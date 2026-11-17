//! 单会话调度器：按剩余并发槽认领任务，执行节点并将图规划交给存储结算。
//!
//! 图在构造时固定，每个任务通过 [`NodeContext::fork`] 创建执行上下文。
//! [`JoinSet`] 限制在途任务数；有空槽时，任务完成、就绪通知和轮询都会触发认领。
//! 通知允许丢失或重复，轮询负责发现遗漏的任务与过期租约。

use std::{future::Future, sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::{
    sync::Notify,
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};
use tracing::{Instrument, error, info, warn};

use super::graph::{Graph, Successor};
use super::node::{NodeContext, NodeError};
use super::task::{FanoutPlan, TaskRecord, TaskStore};

const DEFAULT_MAX_CONCURRENCY: usize = 8;

/// 单会话调度器，负责认领、并发执行与结算，依赖关系由存储过滤。
pub struct Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + ?Sized,
{
    store: Arc<Store>,
    graph: Arc<Graph<Ctx>>,
    poll: Duration,
    /// None 常驻；Some(timeout) 在无在途任务且连续空闲达到 timeout 后退出。
    idle_timeout: Option<Duration>,
    max_concurrency: usize,
    wake: Option<Arc<Notify>>,
}

impl<Ctx, Store> Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + Send + ?Sized + 'static,
{
    /// 由固定的图与共享的工作流级任务存储构造调度器，支持具体存储或 trait object。
    pub fn new(
        graph: Arc<Graph<Ctx>>,
        store: Arc<Store>,
        poll: Duration,                 // 多久轮询一次
        idle_timeout: Option<Duration>, // 超时时间
    ) -> Self {
        Self {
            store,
            graph,
            poll,
            idle_timeout,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            wake: None,
        }
    }

    /// 覆盖并发结算上限（默认 8，至少 1）。
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 挂接任务就绪通知；有空槽时立即唤醒认领，None 表示纯轮询。
    pub fn with_wake(mut self, wake: Option<Arc<Notify>>) -> Self {
        self.wake = wake;
        self
    }

    /// 运行到停止信号完成或空闲时限到达，再取消并等待全部在途节点退出。
    ///
    /// 无需外部停止时传入 `std::future::pending()`；没有空闲时限时持续运行。
    /// 调度循环 panic 时同样先等待清理，再传播 panic。
    /// 调用方应完成 shutdown future 并等待返回；直接中止本方法无法等待清理完成。
    pub async fn run(&mut self, shutdown: impl Future<Output = ()>) {
        let mut in_flight = JoinSet::new();
        let outcome = std::panic::AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = shutdown => {},
                _ = self.run_loop(&mut in_flight) => {},
            }
        })
        .catch_unwind()
        .await;
        in_flight.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    async fn run_loop(&self, in_flight: &mut JoinSet<()>) {
        let mut idle_since = None;
        loop {
            while let Some(result) = in_flight.try_join_next() {
                if let Err(e) = result {
                    error!(error = %e, "settle task join failed");
                }
            }

            let available = self.max_concurrency - in_flight.len();
            if available > 0 {
                match self.store.claim_many(available).await {
                    Ok(tasks) if !tasks.is_empty() => {
                        info!(tasks = ?tasks.iter().map(|task| task.name.as_str()).collect::<Vec<_>>(), "claim tasks");
                        idle_since = None;
                        for task in tasks {
                            in_flight.spawn(settle_safely(
                                self.graph.clone(),
                                self.store.clone(),
                                task,
                            ));
                        }
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => error!(error = %e, "worker claim failed"),
                }
            }

            let idle_deadline = if in_flight.is_empty() {
                let since = *idle_since.get_or_insert_with(Instant::now);
                self.idle_timeout.map(|timeout| since + timeout)
            } else {
                idle_since = None;
                None
            };
            if idle_deadline.is_some_and(|deadline| deadline <= Instant::now()) {
                info!("scheduler idle, exiting");
                break;
            }

            // 满载时只等任务完成；空槽才需要通知和轮询，无在途任务才计空闲。
            tokio::select! {
                result = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Some(Err(e)) = result {
                        error!(error = %e, "settle task join failed");
                    }
                }
                _ = async {
                    match &self.wake {
                        Some(wake) => wake.notified().await,
                        None => std::future::pending().await,
                    }
                }, if available > 0 => {}
                _ = sleep(self.poll), if available > 0 => {}
                _ = async {
                    match idle_deadline {
                        Some(deadline) => sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {} // 截止时再认领一次，确认仍无任务后退出。
            }
        }
    }
}

/// 执行节点并原子提交状态与后继；存储错误交给结算入口处理。
async fn settle<Ctx, S>(graph: &Graph<Ctx>, store: &S, task: &TaskRecord) -> Result<(), S::Error>
where
    Ctx: NodeContext + Sync,
    S: TaskStore + ?Sized,
{
    let name = task.name.as_str();
    let mut ctx = graph.context().fork(task).await;
    graph.on_start(name, &mut ctx, &task.input).await;
    match graph.execute(name, &mut ctx, task.input.clone()).await {
        Ok(output) => {
            // 规划失败属于永久节点错误，不能提交成功却遗漏后继。
            let successors = match graph.plan_success(name, &output) {
                Ok(successors) => successors,
                Err(e) => {
                    return settle_failure(
                        graph,
                        store,
                        &mut ctx,
                        task,
                        &NodeError::permanent(e.to_string()),
                    )
                    .await;
                }
            };
            let fanouts = successors
                .into_iter()
                .map(FanoutPlan::from)
                .collect::<Vec<FanoutPlan>>();
            store.settle_success(task, &output, &fanouts).await?;
            // 成功回调发生在状态与后继提交之后。
            graph.on_success(name, &mut ctx, &output).await;
            Ok(())
        }
        Err(e) => settle_failure(graph, store, &mut ctx, task, &e).await,
    }
}

/// 失败回调与路由规划；存储原子提交失败后继或级联已有依赖任务。
async fn settle_failure<Ctx, S>(
    graph: &Graph<Ctx>,
    store: &S,
    ctx: &mut Ctx,
    task: &TaskRecord,
    e: &NodeError,
) -> Result<(), S::Error>
where
    Ctx: NodeContext + Sync,
    S: TaskStore + ?Sized,
{
    let name = task.name.as_str();
    warn!(error = %e, "task failed");
    graph.on_error(name, ctx, e).await;

    let fanouts: Vec<_> = match graph.plan_failure(name, &task.input, e) {
        Ok(successors) => successors.into_iter().map(FanoutPlan::from).collect(),
        Err(err) => {
            warn!(error = %err, "plan failure successors failed");
            Vec::new()
        }
    };
    store.settle_failure(task, &e.to_string(), &fanouts).await
}

/// 统一记录结算错误与 panic，并释放租约供重试（at-least-once 执行）。
async fn settle_safely<Ctx, S>(graph: Arc<Graph<Ctx>>, store: Arc<S>, task: TaskRecord)
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + Send + ?Sized + 'static,
{
    let span = tracing::info_span!("settle", task_id = task.task_id, name = task.name.as_str());
    async {
        info!("settle task");
        match std::panic::AssertUnwindSafe(settle(graph.as_ref(), store.as_ref(), &task))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => return,
            Ok(Err(e)) => error!(error = %e, "settle transaction failed; releasing claim"),
            Err(panic) => {
                let detail = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("(non-string panic payload)");
                error!(panic = %detail, "settle task panicked; releasing claim");
            }
        }
        if let Err(e) = store.release_claim(&task).await {
            error!(error = %e, "release claim after failed settle");
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
