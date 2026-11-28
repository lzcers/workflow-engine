//! 单图调度器：认领当前工作流任务、维护并发执行槽、独立排空回调与优雅停机。
//!
//! - 轻载不发冗余 `claim_many`：仅当本批认领满槽（`claimed == available`）时立即续查；
//! - 回调与执行槽解耦：结算落库后立即释放执行槽并唤醒下游认领，`NodeContext` 回调在独立 `JoinSet` 中运行，停机时一并排空；
//! - 崩溃与结算异常在 `release_claim` 中保留原始诊断信息并附加短暂退避。

mod runner;
mod wake_hub;

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::{
    sync::{Notify, watch},
    task::JoinSet,
    time::{Instant, sleep},
};
use tracing::{error, info};

use runner::{ExecutionOutcome, notify_outcome, panic_detail, run_task};
pub(crate) use wake_hub::{HubRegistration, WakeHub};

use crate::{
    graph::Graph,
    node::NodeContext,
    task::{TaskRecord, TaskStore, store::LEASE_DURATION},
};

pub(crate) const DEFAULT_MAX_CONCURRENCY: usize = 8;

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
    wake: Arc<Notify>,
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
            wake: Arc::new(Notify::new()),
            shutdown: None,
        }
    }

    pub(crate) fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    pub(crate) fn with_wake(mut self, wake: Arc<Notify>) -> Self {
        self.wake = wake;
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
                // 结算已落库：立即唤醒下游认领，并在独立回调集合中执行钩子（不占执行槽）。
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
        return;
    };
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "../../tests/unit/scheduler.rs"]
mod tests;
