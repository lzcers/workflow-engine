//! 单图调度器：认领当前工作流所有运行的任务，执行节点并将图规划交给存储结算。
//!
//! 每个任务通过 [`NodeContext::fork`] 创建执行上下文，所有 run 共享并发额度。
//! 任务完成、就绪通知和轮询都会触发认领；停止后等待已认领任务完成结算。

use std::{future::Future, sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{sleep, timeout},
};
use tracing::{Instrument, error, info, warn};

use super::graph::{Graph, Successor};
use super::node::{NodeContext, NodeError};
use super::task::{FanoutPlan, TaskRecord, TaskStore, store::LEASE_DURATION};

const DEFAULT_MAX_CONCURRENCY: usize = 8;

/// 当前图的调度器，负责认领、并发执行与结算，依赖关系由存储过滤。
pub struct Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + ?Sized,
{
    store: Arc<Store>,
    graph: Arc<Graph<Ctx>>,
    poll: Duration,
    max_concurrency: usize,
    wake: Arc<Notify>,
}

impl<Ctx, Store> Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + Send + ?Sized + 'static,
{
    /// 由固定的图与共享任务存储构造调度器，按 workflow_id 认领所有运行的任务。
    pub fn new(graph: Arc<Graph<Ctx>>, store: Arc<Store>, poll: Duration) -> Self {
        Self {
            store,
            graph,
            poll,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            wake: Arc::new(Notify::new()),
        }
    }

    /// 覆盖当前工作流所有 run 共享的并发结算上限（默认 8，至少 1）。
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 挂接任务就绪通知，有空槽时立即唤醒认领。
    pub fn with_wake(mut self, wake: Arc<Notify>) -> Self {
        self.wake = wake;
        self
    }

    /// 持续驱动当前图，shutdown 完成后停止认领并等待全部在途任务结算。
    ///
    /// 调用方负责确保只有一个运行循环；Workflow 对重复启动返回错误。
    /// 正常停止不会取消业务节点；直接中止此 future 会取消在途节点且无法等待清理。
    /// 调度循环 panic 时取消并等待全部在途任务，再传播 panic。
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut in_flight = JoinSet::new();
        let outcome =
            std::panic::AssertUnwindSafe(self.run_loop(&mut in_flight, &worker_id, shutdown))
                .catch_unwind()
                .await;
        if let Err(panic) = outcome {
            in_flight.shutdown().await;
            std::panic::resume_unwind(panic);
        }
        while let Some(result) = in_flight.join_next().await {
            if let Err(e) = result {
                error!(error = %e, "settle task join failed");
            }
        }
    }

    async fn run_loop(
        &self,
        in_flight: &mut JoinSet<()>,
        worker_id: &str,
        shutdown: impl Future<Output = ()>,
    ) {
        tokio::pin!(shutdown);
        loop {
            // 认领事务不因停止信号被中断：已提交的认领先执行，再等待结算后退出。
            tokio::select! {
                biased;
                _ = &mut shutdown => return,
                _ = std::future::ready(()) => {}
            }
            while let Some(result) = in_flight.try_join_next() {
                if let Err(e) = result {
                    error!(error = %e, "settle task join failed");
                }
            }

            let available = self.max_concurrency - in_flight.len();
            if available > 0 {
                let claim = self
                    .store
                    .claim_many(self.graph.workflow_id(), worker_id, available);
                tokio::pin!(claim);
                let (claimed, stopping) = tokio::select! {
                    biased;
                    _ = &mut shutdown => (claim.await, true),
                    claimed = &mut claim => (claimed, false),
                };
                match claimed {
                    Ok(tasks) if !tasks.is_empty() => {
                        info!(count = tasks.len(), "claim tasks");
                        for task in tasks {
                            in_flight.spawn(settle_safely(
                                self.graph.clone(),
                                self.store.clone(),
                                task,
                            ));
                        }
                        if stopping {
                            return;
                        }
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "worker claim failed"),
                }
                if stopping {
                    return;
                }
            }

            tokio::select! {
                biased;
                _ = &mut shutdown => return,
                result = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Some(Err(e)) = result {
                        error!(error = %e, "settle task join failed");
                    }
                }
                _ = self.wake.notified(), if available > 0 => {}
                _ = sleep(self.poll), if available > 0 => {}
            }
        }
    }
}

/// 节点执行与规划完成后的结果；业务钩子和数据库结算分阶段管理租约。
enum PreparedTask<Ctx> {
    Success {
        context: Ctx,
        output: serde_json::Value,
        successors: Vec<FanoutPlan>,
    },
    Failure {
        message: String,
        successors: Vec<FanoutPlan>,
    },
}

async fn prepare<Ctx>(graph: &Graph<Ctx>, task: &TaskRecord) -> PreparedTask<Ctx>
where
    Ctx: NodeContext + Sync,
{
    let name = task.name.as_str();
    let mut context = graph.context().fork(task).await;
    graph.on_start(name, &mut context, &task.input).await;
    let error = match graph.execute(name, &mut context, task.input.clone()).await {
        Ok(output) => match graph.plan_success(name, &output) {
            Ok(successors) => {
                return PreparedTask::Success {
                    context,
                    output,
                    successors: successors.into_iter().map(FanoutPlan::from).collect(),
                };
            }
            Err(error) => NodeError::permanent(error.to_string()),
        },
        Err(error) => error,
    };
    warn!(error = %error, "task failed");
    graph.on_error(name, &mut context, &error).await;
    let successors = match graph.plan_failure(name, &task.input, &error) {
        Ok(successors) => successors.into_iter().map(FanoutPlan::from).collect(),
        Err(error) => {
            warn!(error = %error, "plan failure successors failed");
            Vec::new()
        }
    };
    PreparedTask::Failure {
        message: error.to_string(),
        successors,
    }
}

/// 续租出错或超时后，不再继续业务执行。释放与结算另由数据库检查认领归属。
async fn renew_claim<S: TaskStore + ?Sized>(store: &S, task: &TaskRecord) -> bool {
    match timeout(LEASE_DURATION / 3, store.renew_claim(task)).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(error = %error, "claim renewal failed; stopping task execution");
            false
        }
        Err(_) => {
            warn!("claim renewal timed out; stopping task execution");
            false
        }
    }
}

async fn prepare_with_lease<Ctx, S>(
    graph: &Graph<Ctx>,
    store: &S,
    task: &TaskRecord,
) -> Option<PreparedTask<Ctx>>
where
    Ctx: NodeContext + Sync,
    S: TaskStore + ?Sized,
{
    // 认领返回后也可能经历进程暂停；开始业务前重新验证租约，拒绝过期任务。
    if !renew_claim(store, task).await {
        return None;
    }
    let heartbeat = async {
        loop {
            sleep(LEASE_DURATION / 3).await;
            if !renew_claim(store, task).await {
                return;
            }
        }
    };
    tokio::select! {
        biased;
        _ = heartbeat => None,
        prepared = prepare(graph, task) => Some(prepared),
    }
}

/// 数据库结算校验 owner、attempt 和租约；提交后的成功回调无需再续租。
async fn settle<Ctx, S>(
    graph: &Graph<Ctx>,
    store: &S,
    task: &TaskRecord,
    prepared: PreparedTask<Ctx>,
) -> Result<(), S::Error>
where
    Ctx: NodeContext + Sync,
    S: TaskStore + ?Sized,
{
    match prepared {
        PreparedTask::Success {
            mut context,
            output,
            successors,
        } => {
            store.settle_success(task, &output, &successors).await?;
            graph.on_success(&task.name, &mut context, &output).await;
            Ok(())
        }
        PreparedTask::Failure {
            message,
            successors,
        } => store.settle_failure(task, &message, &successors).await,
    }
}

/// 记录执行错误与 panic，释放认领供重试；旧认领的释放会被数据库拒绝。
async fn settle_safely<Ctx, S>(graph: Arc<Graph<Ctx>>, store: Arc<S>, task: TaskRecord)
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + Send + ?Sized + 'static,
{
    let span = tracing::info_span!(
        "settle", workflow_id = %task.workflow_id, task_id = task.task_id,
        attempt = task.attempt, worker_id = task.claimed_by.as_deref(), name = task.name.as_str(),
    );
    async {
        let work = async {
            let prepared = prepare_with_lease(graph.as_ref(), store.as_ref(), &task)
                .await
                .ok_or(())?;
            settle(graph.as_ref(), store.as_ref(), &task, prepared)
                .await
                .map_err(|error| {
                    error!(error = %error, "settle transaction failed; releasing claim");
                })
        };
        match std::panic::AssertUnwindSafe(work).catch_unwind().await {
            Ok(Ok(())) => return,
            Ok(Err(())) => {}
            Err(panic) => {
                let detail = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("(non-string panic payload)");
                error!(panic = %detail, "settle task panicked; releasing claim");
            }
        }
        if let Err(error) = store.release_claim(&task).await {
            warn!(error = %error, "could not release task claim");
        }
    }
    .instrument(span)
    .await;
}

/// 存储只接收入队计划，不依赖图类型。
impl From<Successor> for FanoutPlan {
    fn from(s: Successor) -> Self {
        FanoutPlan {
            dst: s.dst,
            task_type: s.task_type.as_str(),
            inputs: s.inputs,
            depends_on_names: s.depends_on_names,
        }
    }
}
