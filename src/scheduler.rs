//! 单图调度器：认领当前工作流任务、维护租约、执行节点并将规划交给存储原子结算。
//!
//! 关键改进：
//! - 轻载不发冗余 `claim_many`：仅当本批认领满槽（`claimed == available`）时立即续查；
//! - 回调与执行槽彻底解耦：结算落库后立即释放执行槽并唤醒下游认领，`NodeContext` 回调在独立 `JoinSet` 中运行，停机时一并排空；
//! - 崩溃与结算异常在 `release_claim` 中保留原始诊断信息并附加短暂退避；超出单次运行任务数上限时立即以永久失败结算。

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
    error::StorageError,
    graph::Graph,
    node::{NodeContext, NodeError, RetryPolicy},
    task::{
        SettlementConfig, Successor, TaskRecord, TaskStore,
        store::{DEFAULT_LEASE_DURATION, DEFAULT_MAX_TASKS_PER_RUN},
    },
};

pub(crate) const DEFAULT_MAX_CONCURRENCY: usize = 8;
const LEASE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const RELEASE_BACKOFF: Duration = Duration::from_millis(200);

/// 当前图的调度器，负责认领、并发执行、退避重试、结算与独立回调排空。
pub(crate) struct Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + ?Sized,
{
    store: Arc<Store>,
    graph: Arc<Graph<Ctx>>,
    poll: Duration,
    shared_poller: bool,
    max_concurrency: usize,
    lease_duration: Duration,
    default_retry: RetryPolicy,
    max_tasks_per_run: usize,
    wake: Arc<Notify>,
    settled: Arc<Notify>,
    shutdown: Option<watch::Receiver<bool>>,
}

impl<Ctx, Store> Scheduler<Ctx, Store>
where
    Ctx: NodeContext + Sync + 'static,
    Store: TaskStore + ?Sized + 'static,
{
    pub(crate) fn new(graph: Arc<Graph<Ctx>>, store: Arc<Store>, poll: Duration) -> Self {
        Self {
            store,
            graph,
            poll,
            shared_poller: false,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            lease_duration: DEFAULT_LEASE_DURATION,
            default_retry: RetryPolicy::default(),
            max_tasks_per_run: DEFAULT_MAX_TASKS_PER_RUN,
            wake: Arc::new(Notify::new()),
            settled: Arc::new(Notify::new()),
            shutdown: None,
        }
    }

    pub(crate) fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    pub(crate) fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration.max(Duration::from_secs(3));
        self
    }

    pub(crate) fn with_default_retry(mut self, policy: RetryPolicy) -> Self {
        self.default_retry = policy;
        self
    }

    pub(crate) fn with_max_tasks_per_run(mut self, limit: usize) -> Self {
        self.max_tasks_per_run = limit;
        self
    }

    pub(crate) fn with_wake(mut self, wake: Arc<Notify>) -> Self {
        self.wake = wake;
        self
    }

    pub(crate) fn with_settled(mut self, settled: Arc<Notify>) -> Self {
        self.settled = settled;
        self
    }

    /// 启用引擎共享轮询器模式：不再单独起本地定时轮询，统一由 `WakeHub` / `LISTEN/NOTIFY` 唤醒。
    pub(crate) fn with_shared_poller(mut self) -> Self {
        self.shared_poller = true;
        self
    }

    pub(crate) fn with_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    fn settlement_config(&self) -> SettlementConfig {
        SettlementConfig {
            default_max_attempts: self.default_retry.effective_max_attempts(),
            max_tasks_per_run: self.max_tasks_per_run,
        }
    }

    /// 持续驱动当前图，直到收到停机信号且在途任务与回调全部退出，或此 future 被取消。
    pub(crate) async fn run(&self) {
        let mut in_flight: JoinSet<Option<(TaskRecord, ExecutionOutcome<Ctx>)>> = JoinSet::new();
        let mut callbacks: JoinSet<()> = JoinSet::new();
        let outcome = AssertUnwindSafe(self.run_loop(&mut in_flight, &mut callbacks))
            .catch_unwind()
            .await;
        if let Err(panic) = outcome {
            in_flight.shutdown().await;
            callbacks.shutdown().await;
            std::panic::resume_unwind(panic);
        }
    }

    fn handle_task_completion(
        &self,
        result: Result<Option<(TaskRecord, ExecutionOutcome<Ctx>)>, tokio::task::JoinError>,
        callbacks: &mut JoinSet<()>,
    ) {
        match result {
            Ok(Some((task, outcome))) => {
                // 结算已落库：唤醒本地 wait_run 与下游认领，并在独立回调集合中执行钩子（不占执行槽）。
                self.settled.notify_waiters();
                if matches!(
                    outcome,
                    ExecutionOutcome::Success { ref successors, .. }
                        | ExecutionOutcome::Failure { ref successors, .. }
                        if !successors.is_empty()
                ) {
                    self.wake.notify_one();
                }
                let graph = self.graph.clone();
                callbacks.spawn(async move {
                    if let Err(panic) =
                        AssertUnwindSafe(notify_outcome(graph.as_ref(), &task, outcome))
                            .catch_unwind()
                            .await
                    {
                        error!(
                            panic = %panic_detail(&panic),
                            "task callback panicked after settlement"
                        );
                    }
                });
            }
            Ok(None) => {}
            Err(e) => {
                error!(error = %e, "settle task join failed");
            }
        }
    }

    async fn run_loop(
        &self,
        in_flight: &mut JoinSet<Option<(TaskRecord, ExecutionOutcome<Ctx>)>>,
        callbacks: &mut JoinSet<()>,
    ) {
        let mut shutdown = self.shutdown.clone();
        loop {
            while let Some(result) = in_flight.try_join_next() {
                self.handle_task_completion(result, callbacks);
            }
            while let Some(result) = callbacks.try_join_next() {
                if let Err(e) = result {
                    error!(error = %e, "callback task join failed");
                }
            }

            if is_shutting_down(&shutdown) {
                while let Some(result) = in_flight.join_next().await {
                    self.handle_task_completion(result, callbacks);
                }
                while let Some(result) = callbacks.join_next().await {
                    if let Err(e) = result {
                        error!(error = %e, "callback task join failed");
                    }
                }
                return;
            }

            let available = self.max_concurrency - in_flight.len();
            if available > 0 {
                let claimed = self.claim_and_spawn(in_flight, available).await;
                // 只有打满本批额度时才立即循环补槽；返回少于额度说明当前无更多可认领任务，
                // 避免顺序流/轻载下每次都多打一倍必然空集的 claim_many 查询。
                if claimed == available {
                    continue;
                }
            }

            let available = self.max_concurrency - in_flight.len();
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut shutdown), if shutdown.is_some() => {}
                result = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Some(result) = result {
                        self.handle_task_completion(result, callbacks);
                    }
                }
                result = callbacks.join_next(), if !callbacks.is_empty() => {
                    if let Some(Err(e)) = result {
                        error!(error = %e, "callback task join failed");
                    }
                }
                _ = self.wake.notified(), if available > 0 => {}
                _ = sleep(self.poll), if available > 0 && !self.shared_poller => {}
            }
        }
    }

    /// 按空槽数认领并启动任务，返回本批数量。
    async fn claim_and_spawn(
        &self,
        in_flight: &mut JoinSet<Option<(TaskRecord, ExecutionOutcome<Ctx>)>>,
        limit: usize,
    ) -> usize {
        let lease_duration = self.lease_duration;
        let lease_deadline = Instant::now() + lease_duration;
        match self
            .store
            .claim_many(self.graph.workflow_id(), limit, lease_duration)
            .await
        {
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
                let config = self.settlement_config();
                for task in tasks {
                    in_flight.spawn(run_task(
                        self.graph.clone(),
                        self.store.clone(),
                        task,
                        lease_deadline,
                        lease_duration,
                        self.default_retry,
                        config,
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

/// 业务执行与后继规划的结果。成功时保留上下文，供提交后的 `on_success` 使用。
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

/// 执行和结算期间维持认领，终态提交或退避入队后立即返回回调载荷，释放执行槽。
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
    lease_duration: Duration,
    default_retry: RetryPolicy,
    config: SettlementConfig,
) -> Option<(TaskRecord, ExecutionOutcome<Ctx>)>
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + ?Sized + 'static,
{
    if lease_deadline <= Instant::now() {
        warn!("claim lease elapsed before execution");
        return None;
    }
    let work = async {
        let outcome = execute_and_plan(graph.as_ref(), &task, default_retry).await;
        match settle(store.as_ref(), graph.as_ref(), &task, &outcome, config).await {
            Ok(()) => Ok(outcome),
            Err(StorageError::RunTaskLimitExceeded {
                workflow_id,
                run_id,
                limit,
            }) => {
                // 单次运行超过任务上限属于确定性护栏触发，直接以永久失败结算，不再无效重试。
                let msg = format!(
                    "run {workflow_id}:{run_id} exceeded max tasks limit ({limit})"
                );
                let err = NodeError::permanent(msg.clone());
                let context = match outcome {
                    ExecutionOutcome::Success { context, .. } => Some(context),
                    ExecutionOutcome::Failure { context, .. } => context,
                    ExecutionOutcome::Retry { .. } => None,
                };
                store
                    .settle_failure(&task, &msg, &[], graph.settlement_rules(), config)
                    .await?;
                Ok(ExecutionOutcome::Failure {
                    context,
                    error: err,
                    successors: Vec::new(),
                })
            }
            Err(err) => Err(err),
        }
    };
    let lifecycle = async {
        let release_reason = match AssertUnwindSafe(work).catch_unwind().await {
            Ok(Ok(outcome)) => return Some(outcome),
            Ok(Err(error)) => {
                error!(error = %error, "settle transaction failed; releasing claim");
                format!("settle failed: {error}")
            }
            Err(panic) => {
                let detail = panic_detail(&panic).to_owned();
                error!(panic = %detail, "task panicked; releasing claim");
                format!("task panicked: {detail}")
            }
        };
        // 释放认领时保留真实错误原因，未耗尽预算时附带短退避防热自旋。
        if let Err(error) = store
            .release_claim(&task, Some(&release_reason), Some(RELEASE_BACKOFF))
            .await
        {
            warn!(error = %error, "could not release task claim");
        }
        None
    };
    let outcome = tokio::select! {
        biased;
        _ = maintain_claim(store.as_ref(), &task, lease_deadline, lease_duration) => None,
        outcome = lifecycle => outcome,
    };
    outcome.map(|outcome| (task, outcome))
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
    lease_duration: Duration,
) {
    let renew_interval = (lease_duration / 3).max(Duration::from_secs(1));
    let mut renew_at = deadline - lease_duration + renew_interval;
    loop {
        let renewal = async {
            sleep_until(renew_at).await;
            let requested_at = Instant::now();
            (requested_at, store.renew_claim(task, lease_duration).await)
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
                        deadline = requested_at + lease_duration;
                        renew_at = requested_at + renew_interval;
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
async fn execute_and_plan<Ctx>(
    graph: &Graph<Ctx>,
    task: &TaskRecord,
    default_retry: RetryPolicy,
) -> ExecutionOutcome<Ctx>
where
    Ctx: NodeContext + Sync,
{
    let name = task.name.as_str();
    let max_attempts = task.max_attempts.max(1);
    if task.attempt > max_attempts {
        let error = NodeError::permanent(
            task.error
                .as_deref()
                .expect("exhausted claims must carry their final failure reason"),
        );
        return plan_failure(graph, task, None, error);
    }
    let mut context = graph.context().fork(task).await;
    context.on_start(task).await;
    let error = match graph.execute(name, &mut context, task.input.clone()).await {
        Ok(output) => match graph.plan_success(name, &output) {
            Ok(successors) => {
                return ExecutionOutcome::Success {
                    context,
                    output,
                    successors,
                };
            }
            Err(error) => NodeError::permanent(error.to_string()),
        },
        Err(error) if error.is_retryable() && task.attempt < max_attempts => {
            let policy = graph.retry_policy_of(name).unwrap_or(default_retry);
            let delay = policy.delay_for(task.task_id, task.attempt);
            warn!(
                error = %error,
                attempt = task.attempt,
                delay_ms = delay.as_millis() as u64,
                "task failed with retryable error; scheduling backoff retry"
            );
            return ExecutionOutcome::Retry { error, delay };
        }
        Err(error) => error,
    };
    plan_failure(graph, task, Some(context), error)
}

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

async fn settle<Ctx, S>(
    store: &S,
    graph: &Graph<Ctx>,
    task: &TaskRecord,
    outcome: &ExecutionOutcome<Ctx>,
    config: SettlementConfig,
) -> Result<(), StorageError>
where
    Ctx: NodeContext + Sync + 'static,
    S: TaskStore + ?Sized,
{
    let rules = graph.settlement_rules();
    match outcome {
        ExecutionOutcome::Success {
            output, successors, ..
        } => {
            store
                .settle_success(task, output, successors, rules, config)
                .await
        }
        ExecutionOutcome::Retry { error, delay } => {
            store.schedule_retry(task, &error.to_string(), *delay).await
        }
        ExecutionOutcome::Failure {
            error, successors, ..
        } => {
            store
                .settle_failure(task, &error.to_string(), successors, rules, config)
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
            context.on_success(task, &output).await;
        }
        ExecutionOutcome::Retry { .. } => {}
        ExecutionOutcome::Failure { context, error, .. } => {
            let mut context = match context {
                Some(context) => context,
                None => graph.context().fork(task).await,
            };
            context.on_error(task, &error).await;
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "../tests/unit/scheduler.rs"]
mod tests;
