//! 单图调度器：认领当前工作流所有运行的任务，执行节点并将图规划交给存储结算。
//!
//! 调度循环只负责补充执行槽；单个任务依次完成业务执行与后继规划、原子结算和成功回调。
//! 认领租约限定本次处理窗口，结算时由存储再次校验认领；所有 run 共享并发额度。
//! 调度循环随所属实例释放而取消，同时取消在途任务。

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::FutureExt;
use serde_json::Value;
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};
use tracing::{error, info, warn};

use crate::{
    graph::{Graph, Successor},
    node::{NodeContext, NodeError},
    task::{FanoutPlan, TaskRecord, TaskStore},
};

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

    /// 设置当前调度器所有 run 共享的并发任务上限（默认 8，至少 1）。
    /// 执行槽覆盖业务执行、数据库结算和成功回调。
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 挂接任务就绪通知，有空槽时立即唤醒认领。
    pub fn with_wake(mut self, wake: Arc<Notify>) -> Self {
        self.wake = wake;
        self
    }

    /// 持续驱动当前图，直到此 future 被取消。
    ///
    /// 调用方负责确保只有一个运行循环；Workflow 在创建时启动一次。
    /// Workflow 释放时取消此 future；取消会中止在途节点，无法等待结算。
    /// 调度循环 panic 时取消并等待全部在途任务，再传播 panic。
    pub async fn run(&self) {
        // 每次启动都有独立身份，重启后不会沿用上一次的认领归属。
        let worker_id = uuid::Uuid::new_v4().to_string();
        let mut in_flight = JoinSet::new();
        let outcome = AssertUnwindSafe(self.run_loop(&mut in_flight, &worker_id))
            .catch_unwind()
            .await;
        if let Err(panic) = outcome {
            in_flight.shutdown().await;
            std::panic::resume_unwind(panic);
        }
    }

    async fn run_loop(&self, in_flight: &mut JoinSet<()>, worker_id: &str) {
        loop {
            while let Some(result) = in_flight.try_join_next() {
                if let Err(e) = result {
                    error!(error = %e, "settle task join failed");
                }
            }

            let available = self.max_concurrency - in_flight.len();
            if available > 0 && self.claim_and_spawn(in_flight, worker_id, available).await > 0 {
                continue;
            }

            // 满槽时只等待任务结束，不做无效轮询，也不消耗就绪通知。
            tokio::select! {
                biased;
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

    /// 按空槽数认领并启动任务，返回本批数量。
    async fn claim_and_spawn(
        &self,
        in_flight: &mut JoinSet<()>,
        worker_id: &str,
        limit: usize,
    ) -> usize {
        match self
            .store
            .claim_many(self.graph.workflow_id(), worker_id, limit)
            .await
        {
            Ok(tasks) => {
                let count = tasks.len();
                if count > 0 {
                    info!(count, "claim tasks");
                }
                for task in tasks {
                    in_flight.spawn(run_task(self.graph.clone(), self.store.clone(), task));
                }
                count
            }
            Err(error) => {
                error!(error = %error, "worker claim failed");
                0
            }
        }
    }
}

/// 业务执行与后继规划的结果。成功时保留上下文，供提交后的 on_success 使用。
enum ExecutionOutcome<Ctx> {
    Success {
        context: Ctx,
        output: Value,
        successors: Vec<FanoutPlan>,
    },
    Failure {
        context: Option<Ctx>,
        error: NodeError,
        successors: Vec<FanoutPlan>,
    },
}

/// 单个认领的完整生命周期；固定窗口覆盖执行、结算、回调和异常释放。
#[tracing::instrument(
    name = "settle",
    skip_all,
    fields(
        workflow_id = %task.workflow_id,
        task_id = task.task_id,
        attempt = task.attempt,
        worker_id = task.claimed_by.as_deref(),
        name = task.name.as_str(),
    )
)]
async fn run_task<Ctx, S>(graph: Arc<Graph<Ctx>>, store: Arc<S>, task: TaskRecord)
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + Send + ?Sized + 'static,
{
    // 认领到开始处理之间的等待也消耗窗口；过期快照不能再进入业务代码。
    let started_at = Instant::now();
    let Some(remaining) = task
        .lease_until
        .and_then(|until| (until - chrono::Utc::now()).to_std().ok())
        .filter(|remaining| !remaining.is_zero())
    else {
        warn!("claim has no remaining processing window");
        return;
    };
    let deadline = started_at + remaining;

    let work = async {
        let outcome = execute_and_plan(graph.as_ref(), &task).await;

        settle(graph.as_ref(), store.as_ref(), &task, outcome)
            .await
            .map_err(|error| {
                error!(error = %error, "settle transaction failed; releasing claim");
            })
    };
    let lifecycle = async {
        match AssertUnwindSafe(work).catch_unwind().await {
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

        // 释放也校验认领凭据；已过期、被接管或已结算的任务不会被这次清理修改。
        if let Err(error) = store.release_claim(&task).await {
            warn!(error = %error, "could not release task claim");
        }
    };
    // 截止时间优先：到期丢弃本地 future、释放执行槽，由下一次认领重试或处理最终失败。
    // 已提交的结算不会回滚，已发生的外部副作用不会撤回。
    tokio::select! {
        biased;
        _ = sleep_until(deadline) => {
            warn!("claim processing window elapsed; stopping local work");
        }
        _ = lifecycle => {}
    }
}

/// fork 独立上下文并规划结算；预算耗尽的认领只走最终失败流程，不再执行节点。
async fn execute_and_plan<Ctx>(graph: &Graph<Ctx>, task: &TaskRecord) -> ExecutionOutcome<Ctx>
where
    Ctx: NodeContext + Sync,
{
    let name = task.name.as_str();
    if task.attempt > task.max_attempts {
        let error = NodeError::permanent(
            task.error
                .as_deref()
                .expect("exhausted claims must carry their final failure reason"),
        );
        return plan_failure(graph, task, None, error);
    }
    let mut context = graph.context().fork(task).await;
    graph.on_start(name, &mut context, &task.input).await;
    let error = match graph.execute(name, &mut context, task.input.clone()).await {
        Ok(output) => match graph.plan_success(name, &output) {
            Ok(successors) => {
                return ExecutionOutcome::Success {
                    context,
                    output,
                    successors: successors.into_iter().map(FanoutPlan::from).collect(),
                };
            }
            // 无法规划成功后继时，按不可重试的业务错误进入失败路径。
            Err(error) => NodeError::permanent(error.to_string()),
        },
        Err(error) => error,
    };
    plan_failure(graph, task, Some(context), error)
}

/// 业务错误与预算耗尽共用失败后继规划；错误回调在结算成功后执行。
fn plan_failure<Ctx>(
    graph: &Graph<Ctx>,
    task: &TaskRecord,
    context: Option<Ctx>,
    error: NodeError,
) -> ExecutionOutcome<Ctx>
where
    Ctx: NodeContext + Sync,
{
    warn!(error = %error, "task failed");
    let successors = match graph.plan_failure(&task.name, &task.input, &error) {
        Ok(successors) => successors.into_iter().map(FanoutPlan::from).collect(),
        Err(error) => {
            warn!(error = %error, "plan failure successors failed");
            Vec::new()
        }
    };
    ExecutionOutcome::Failure {
        context,
        error,
        successors,
    }
}

/// 原子提交任务状态与后继；存储再次校验 owner、attempt 和固定租约期限。
async fn settle<Ctx, S>(
    graph: &Graph<Ctx>,
    store: &S,
    task: &TaskRecord,
    outcome: ExecutionOutcome<Ctx>,
) -> Result<(), S::Error>
where
    Ctx: NodeContext + Sync,
    S: TaskStore + ?Sized,
{
    match outcome {
        ExecutionOutcome::Success {
            mut context,
            output,
            successors,
        } => {
            store.settle_success(task, &output, &successors).await?;
            // 成功提交后任务已是终态；成功钩子仍受本次处理窗口限制。
            graph.on_success(&task.name, &mut context, &output).await;
            Ok(())
        }
        ExecutionOutcome::Failure {
            context,
            error,
            successors,
        } => {
            store
                .settle_failure(task, &error.to_string(), &successors)
                .await?;
            // 恢复结算不依赖业务上下文；即使 fork 或错误钩子卡住，终态与后继已经持久化。
            let mut context = match context {
                Some(context) => context,
                None => graph.context().fork(task).await,
            };
            graph.on_error(&task.name, &mut context, &error).await;
            Ok(())
        }
    }
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

#[cfg(all(test, feature = "sqlite"))]
#[path = "../tests/unit/scheduler.rs"]
mod tests;
