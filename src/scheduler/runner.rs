//! 单任务执行生命周期：租约续期防护、节点执行、后继规划、原子结算与回调分发。

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::FutureExt;
use serde_json::Value;
use tokio::time::{Instant, sleep_until};
use tracing::{error, warn};

use crate::{
    error::StorageError,
    graph::Graph,
    node::{NodeContext, NodeError, RetryPolicy},
    task::{SettlementConfig, Successor, TaskRecord, TaskStore},
};

const LEASE_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const RELEASE_BACKOFF: Duration = Duration::from_millis(200);

/// 业务执行与后继规划的结果。成功时保留上下文，供提交后的 `on_success` 使用。
pub(super) enum ExecutionOutcome<Ctx> {
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
pub(super) async fn run_task<Ctx, S>(
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

pub(super) fn panic_detail(panic: &Box<dyn std::any::Any + Send>) -> &str {
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

pub(super) async fn notify_outcome<Ctx>(
    graph: &Graph<Ctx>,
    task: &TaskRecord,
    outcome: ExecutionOutcome<Ctx>,
) where
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
