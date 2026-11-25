//! 单图调度器：认领当前工作流所有运行的任务，执行节点并将图规划交给存储结算。
//!
//! 调度循环只负责补充执行槽；单个任务依次完成业务执行与后继规划、原子结算（或可重试退避入队）和回调。
//! 执行及结算期间自动续租，存储校验认领归属；节点自行控制运行超时，所有 run 共享并发额度。
//! 收到停机信号后停止认领新任务并等待在途任务完成；若整个 future 被直接取消则中止在途任务。

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::FutureExt;
use serde_json::Value;
use tokio::{
    sync::{Notify, watch},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};
use tracing::{error, info, warn};

use crate::{
    graph::Graph,
    node::{NodeContext, NodeError},
    task::{MAX_ATTEMPTS, Successor, TaskRecord, TaskStore, store::LEASE_DURATION},
};

pub(crate) const DEFAULT_MAX_CONCURRENCY: usize = 8;
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(LEASE_DURATION.as_secs() / 3);
const LEASE_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// 当前图的调度器，负责认领、并发执行、退避重试与结算，JOIN 就绪状态由存储在结算时更新。
pub(crate) struct Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + ?Sized,
{
    store: Arc<Store>,
    graph: Arc<Graph<Ctx>>,
    poll: Duration,
    max_concurrency: usize,
    wake: Arc<Notify>,
    shutdown: Option<watch::Receiver<bool>>,
}

impl<Ctx, Store> Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + Send + ?Sized + 'static,
{
    /// 由固定的图与共享任务存储构造调度器，按 workflow_id 认领所有运行的任务。
    pub(crate) fn new(graph: Arc<Graph<Ctx>>, store: Arc<Store>, poll: Duration) -> Self {
        Self {
            store,
            graph,
            poll,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            wake: Arc::new(Notify::new()),
            shutdown: None,
        }
    }

    /// 设置当前调度器所有 run 共享的并发任务上限（默认 8，至少 1）。
    /// 执行槽覆盖业务执行、数据库结算和回调。
    pub(crate) fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 挂接任务就绪通知，有空槽时立即唤醒认领。
    pub(crate) fn with_wake(mut self, wake: Arc<Notify>) -> Self {
        self.wake = wake;
        self
    }

    /// 挂接优雅停机信号：收到 `true` 后停止认领新任务并等待在途任务结束。
    pub(crate) fn with_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// 持续驱动当前图，直到收到停机信号并在途任务全部退出，或此 future 被取消。
    ///
    /// 调用方负责确保只有一个运行循环；Workflow 在创建时启动一次。
    /// 调度循环 panic 时取消并等待全部在途任务，再传播 panic。
    pub(crate) async fn run(&self) {
        let mut in_flight = JoinSet::new();
        let outcome = AssertUnwindSafe(self.run_loop(&mut in_flight))
            .catch_unwind()
            .await;
        if let Err(panic) = outcome {
            in_flight.shutdown().await;
            std::panic::resume_unwind(panic);
        }
    }

    async fn run_loop(&self, in_flight: &mut JoinSet<()>) {
        let mut shutdown = self.shutdown.clone();
        loop {
            while let Some(result) = in_flight.try_join_next() {
                if let Err(e) = result {
                    error!(error = %e, "settle task join failed");
                }
            }

            if is_shutting_down(&shutdown) {
                while let Some(result) = in_flight.join_next().await {
                    if let Err(e) = result {
                        error!(error = %e, "settle task join failed");
                    }
                }
                return;
            }

            let available = self.max_concurrency - in_flight.len();
            if available > 0 && self.claim_and_spawn(in_flight, available).await > 0 {
                continue;
            }

            // 满槽时只等待任务结束或停机信号，不做无效轮询，也不消耗就绪通知。
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut shutdown), if shutdown.is_some() => {}
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
    async fn claim_and_spawn(&self, in_flight: &mut JoinSet<()>, limit: usize) -> usize {
        // 从请求发出时计时，比数据库真正写入租约更早，不受应用与数据库时钟偏差影响。
        let lease_deadline = Instant::now() + LEASE_DURATION;
        match self.store.claim_many(self.graph.workflow_id(), limit).await {
            Ok(tasks) => {
                let count = tasks.len();
                if count > 0 {
                    info!(
                        count,
                        workflow_id = self.graph.workflow_id(),
                        node_names = ?tasks.iter().map(|task| task.name.as_str()).collect::<Vec<_>>(),
                        "claim tasks"
                    );
                }
                for task in tasks {
                    in_flight.spawn(run_task(
                        self.graph.clone(),
                        self.store.clone(),
                        task,
                        lease_deadline,
                    ));
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

fn is_shutting_down(shutdown: &Option<watch::Receiver<bool>>) -> bool {
    shutdown
        .as_ref()
        .is_some_and(|rx| *rx.borrow() || rx.has_changed().is_err())
}

async fn wait_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
    let Some(rx) = shutdown.as_mut() else {
        std::future::pending::<()>().await;
        unreachable!()
    };
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn retry_delay(attempt: i32) -> Duration {
    let shift = u32::try_from(attempt.saturating_sub(1))
        .unwrap_or(0)
        .min(10);
    Duration::from_secs(1u64 << shift)
}

/// 业务执行与后继规划的结果。成功时保留上下文，供提交后的 on_success 使用。
enum ExecutionOutcome<Ctx> {
    Success {
        context: Ctx,
        output: Value,
        successors: Vec<Successor>,
    },
    Retry {
        error: NodeError,
        delay: Duration,
    },
    Failure {
        context: Option<Ctx>,
        error: NodeError,
        successors: Vec<Successor>,
    },
}

/// 执行和结算期间维持认领，终态提交或退避入队后停止续租并按需调用回调。
#[tracing::instrument(
    name = "settle",
    skip_all,
    fields(
        workflow_id = %task.workflow_id,
        task_id = task.task_id,
        attempt = task.attempt,
        name = task.name.as_str(),
    )
)]
async fn run_task<Ctx, S>(
    graph: Arc<Graph<Ctx>>,
    store: Arc<S>,
    task: TaskRecord,
    lease_deadline: Instant,
) where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + Send + ?Sized + 'static,
{
    if lease_deadline <= Instant::now() {
        warn!("claim lease elapsed before execution");
        return;
    }
    let work = async {
        let outcome = execute_and_plan(graph.as_ref(), &task).await;
        settle(store.as_ref(), graph.as_ref(), &task, &outcome).await?;
        Ok::<_, S::Error>(outcome)
    };
    let lifecycle = async {
        match AssertUnwindSafe(work).catch_unwind().await {
            Ok(Ok(outcome)) => return Some(outcome),
            Ok(Err(error)) => {
                error!(error = %error, "settle transaction failed; releasing claim");
            }
            Err(panic) => {
                error!(panic = %panic_detail(&panic), "task panicked; releasing claim");
            }
        }
        // 释放校验认领代次；不能释放其他执行者的租约或修改已提交的终态。
        if let Err(error) = store.release_claim(&task).await {
            warn!(error = %error, "could not release task claim");
        }
        None
    };
    let outcome = tokio::select! {
        biased;
        // 失权优先，避免租约到期后再次推进业务。若提交已生效，保留数据库终态；
        // 提交确认与续租失权同时返回时可能跳过 best-effort 回调，不另行结算失败。
        _ = maintain_claim(store.as_ref(), &task, lease_deadline) => None,
        outcome = lifecycle => outcome,
    };
    if let Some(outcome) = outcome {
        // 任务已提交，回调不再占用租约，也没有引擎施加的执行时限。
        if let Err(panic) = AssertUnwindSafe(notify_outcome(graph.as_ref(), &task, outcome))
            .catch_unwind()
            .await
        {
            error!(panic = %panic_detail(&panic), "task callback panicked after settlement");
        }
    }
}

fn panic_detail(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("(non-string panic payload)")
}

/// 失权或最后确认的租约到期时返回，使调用方丢弃未结算的业务 future。
async fn maintain_claim<S: TaskStore + ?Sized>(
    store: &S,
    task: &TaskRecord,
    mut deadline: Instant,
) {
    let mut renew_at = deadline - LEASE_DURATION + LEASE_RENEW_INTERVAL;
    loop {
        let renewal = async {
            sleep_until(renew_at).await;
            let requested_at = Instant::now();
            (requested_at, store.renew_claim(task).await)
        };
        tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                warn!("claim lease elapsed; stopping local work");
                return;
            }
            (requested_at, result) = renewal => {
                match result {
                    Ok(Some(_)) => {
                        // 续租在数据库写入后才成功；请求起点提供保守的单调时钟期限。
                        deadline = requested_at + LEASE_DURATION;
                        renew_at = requested_at + LEASE_RENEW_INTERVAL;
                    }
                    Ok(None) => {
                        warn!("claim no longer owned; stopping local work");
                        return;
                    }
                    Err(error) => {
                        warn!(error = %error, "could not renew claim; retrying within current lease");
                        renew_at = Instant::now() + LEASE_RETRY_INTERVAL;
                    }
                }
            }
        }
    }
}

/// fork 独立上下文并规划结算；预算耗尽的认领只走最终失败流程，不再执行节点。
/// 可重试错误在 `attempt < MAX_ATTEMPTS` 时安排指数退避重试，用完预算后进入失败路径。
async fn execute_and_plan<Ctx>(graph: &Graph<Ctx>, task: &TaskRecord) -> ExecutionOutcome<Ctx>
where
    Ctx: NodeContext + Sync,
{
    let name = task.name.as_str();
    if task.attempt > MAX_ATTEMPTS {
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
                    successors,
                };
            }
            // 无法规划成功后继时，按不可重试的业务错误进入失败路径。
            Err(error) => NodeError::permanent(error.to_string()),
        },
        Err(error) if error.is_retryable() && task.attempt < MAX_ATTEMPTS => {
            let delay = retry_delay(task.attempt);
            warn!(
                error = %error,
                attempt = task.attempt,
                delay_secs = delay.as_secs(),
                "task failed with retryable error; scheduling backoff retry"
            );
            return ExecutionOutcome::Retry { error, delay };
        }
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
        Ok(successors) => successors,
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

/// 原子提交任务状态与后继，或写入退避重试时间；存储再次校验运行状态、attempt 和租约有效期。
async fn settle<Ctx, S>(
    store: &S,
    graph: &Graph<Ctx>,
    task: &TaskRecord,
    outcome: &ExecutionOutcome<Ctx>,
) -> Result<(), S::Error>
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + ?Sized,
{
    let rules = graph.settlement_rules();
    match outcome {
        ExecutionOutcome::Success {
            output, successors, ..
        } => store.settle_success(task, output, successors, rules).await,
        ExecutionOutcome::Retry { error, delay } => {
            store.schedule_retry(task, &error.to_string(), *delay).await
        }
        ExecutionOutcome::Failure {
            error, successors, ..
        } => {
            store
                .settle_failure(task, &error.to_string(), successors, rules)
                .await
        }
    }
}

async fn notify_outcome<Ctx>(graph: &Graph<Ctx>, task: &TaskRecord, outcome: ExecutionOutcome<Ctx>)
where
    Ctx: NodeContext + Sync,
{
    match outcome {
        ExecutionOutcome::Success {
            mut context,
            output,
            ..
        } => {
            graph.on_success(&task.name, &mut context, &output).await;
        }
        ExecutionOutcome::Retry { .. } => {}
        ExecutionOutcome::Failure { context, error, .. } => {
            // 恢复结算不依赖业务上下文；即使 fork 或错误钩子卡住，终态与后继已经持久化。
            let mut context = match context {
                Some(context) => context,
                None => graph.context().fork(task).await,
            };
            graph.on_error(&task.name, &mut context, &error).await;
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "../tests/unit/scheduler.rs"]
mod tests;
