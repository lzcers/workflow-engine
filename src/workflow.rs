//! 单图工作流实例：启动后台调度器、强类型预检提交、运行控制面（查询/取消/等待）与优雅停机。

use std::{any::Any, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

use crate::engine::{HubRegistration, WakeHub};
use crate::graph::ErasedGraph;
use crate::node::RetryPolicy;
use crate::scheduler::{DEFAULT_MAX_CONCURRENCY, Scheduler};
use crate::task::{
    TaskStore,
    store::{DEFAULT_LEASE_DURATION, DEFAULT_MAX_TASKS_PER_RUN},
};
use crate::{EngineError, Graph, NodeContext, RunSnapshot, RunStatus, Task, TaskRecord, Trigger};

/// 后台调度器的并发、轮询、租约、默认重试与单次运行任务数护栏参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerOptions {
    /// 当前工作流所有 run 共享的最大并发执行槽数（默认 8，至少 1）。
    pub max_concurrency: usize,
    /// 空闲且有空槽时的数据库轮询间隔（默认 1 秒；多工作流共用引擎共享轮询器）。
    pub poll_interval: Duration,
    /// 认领与续租的租约有效期（默认 300 秒，至少 3 秒）。
    pub lease_duration: Duration,
    /// 工作流级默认重试策略；节点可通过 `Node::retry_policy` 单独覆盖。
    pub default_retry: RetryPolicy,
    /// 单次运行允许创建的最大任务数上限（默认 10,000，0 表示不限制；防止失败回边环无限循环）。
    pub max_tasks_per_run: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            poll_interval: Duration::from_secs(1),
            lease_duration: DEFAULT_LEASE_DURATION,
            default_retry: RetryPolicy::default(),
            max_tasks_per_run: DEFAULT_MAX_TASKS_PER_RUN,
        }
    }
}

/// 一张固定图的非泛型工作流句柄，由 [`crate::Engine::make`] 或 [`crate::Engine::make_with`] 创建。
pub struct Workflow {
    graph: Arc<dyn ErasedGraph>,
    store: Arc<dyn TaskStore>,
    worker: Option<JoinHandle<()>>,
    wake: Arc<Notify>,
    settled: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    options: WorkerOptions,
    _registration: Option<HubRegistration>,
}

impl Drop for Workflow {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self._registration.take();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

impl Workflow {
    pub(crate) fn new<Ctx>(
        graph: Arc<Graph<Ctx>>,
        store: Arc<dyn TaskStore>,
        hub: &Arc<WakeHub>,
        options: WorkerOptions,
    ) -> Self
    where
        Ctx: NodeContext + Sync + 'static,
    {
        let wake = Arc::new(Notify::new());
        let settled = Arc::new(Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registration = hub.register(
            graph.workflow_id().to_owned(),
            wake.clone(),
            options.poll_interval,
        );
        let scheduler = Scheduler::new(graph.clone(), store.clone(), options.poll_interval)
            .with_max_concurrency(options.max_concurrency)
            .with_lease_duration(options.lease_duration)
            .with_default_retry(options.default_retry)
            .with_max_tasks_per_run(options.max_tasks_per_run)
            .with_wake(wake.clone())
            .with_settled(settled.clone())
            .with_shared_poller()
            .with_shutdown(shutdown_rx);
        let worker = tokio::spawn(async move {
            scheduler.run().await;
        });
        Self {
            graph,
            store,
            worker: Some(worker),
            wake,
            settled,
            shutdown,
            options,
            _registration: Some(registration),
        }
    }

    /// 停止认领新任务，等待当前在途任务与提交后回调全部完成后退出后台调度器。
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        self.wake.notify_one();
        self._registration.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
    }

    /// 后台调度器已终止时为 true，可供宿主报告 worker 健康状态。
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// 访问与调度器共享的原始图，可查询身份、描述、节点类型和触发声明。
    pub fn graph(&self) -> &dyn ErasedGraph {
        self.graph.as_ref()
    }

    /// 取回原始 `Graph<Ctx>` 的借用；上下文类型不匹配时返回 None。
    pub fn graph_as<Ctx: 'static>(&self) -> Option<&Graph<Ctx>> {
        (self.graph() as &dyn Any).downcast_ref()
    }

    /// 获取全部声明了外部触发器的节点名称与配置，按节点名排序（无副作用）。
    pub fn triggers(&self) -> Vec<(&str, Trigger)> {
        self.graph.triggers()
    }

    /// 从指定节点创建并执行一次独立运行；入口处即校验节点存在与 `In` 反序列化兼容性。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        let value = serde_json::to_value(input)?;
        self.graph.validate_input(entry, &value)?;
        let mut task = Task::new(self.graph.workflow_id(), entry, value);
        task.subgraph_path = self
            .graph
            .subgraph_path(entry)
            .ok_or_else(|| EngineError::UnknownNode(entry.to_owned()))?;
        task.max_attempts = Some(
            self.graph
                .retry_policy_of(entry)
                .unwrap_or(self.options.default_retry)
                .effective_max_attempts(),
        );
        let run = self.store.enqueue(task).await?;
        self.wake.notify_one();
        Ok(run)
    }

    /// 查询本次工作流下指定运行的全部任务快照（按 `task_id` 升序）。
    pub async fn list_tasks(&self, run_id: i64) -> Result<Vec<TaskRecord>, EngineError> {
        Ok(self
            .store
            .list_tasks(self.graph.workflow_id(), run_id)
            .await?)
    }

    /// 获取指定运行的汇总状态与任务快照；不存在时返回 `Ok(None)`。
    pub async fn get_run(&self, run_id: i64) -> Result<Option<RunSnapshot>, EngineError> {
        let tasks = self.list_tasks(run_id).await?;
        if tasks.is_empty() {
            return Ok(None);
        }
        let status = RunStatus::from_tasks(&tasks);
        Ok(Some(RunSnapshot {
            workflow_id: self.graph.workflow_id().to_owned(),
            run_id,
            status,
            tasks,
        }))
    }

    /// 取消指定运行的所有活跃任务（`pending`/`collecting`/`running`），返回受影响任务数。
    pub async fn cancel_run(&self, run_id: i64) -> Result<usize, EngineError> {
        let cancelled = self
            .store
            .cancel_run(self.graph.workflow_id(), run_id)
            .await?;
        if cancelled > 0 {
            self.settled.notify_waiters();
        }
        Ok(cancelled)
    }

    /// 等待指定运行进入终态（`Succeeded` / `Failed` / `Cancelled`）并返回最终快照。
    pub async fn wait_run(&self, run_id: i64) -> Result<RunSnapshot, EngineError> {
        let fallback_poll = self.options.poll_interval.min(Duration::from_millis(250));
        loop {
            let notified = self.settled.notified();
            let snapshot = self
                .get_run(run_id)
                .await?
                .ok_or(EngineError::UnknownRun(run_id))?;
            if snapshot.status.is_terminal() {
                return Ok(snapshot);
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(fallback_poll) => {}
            }
        }
    }
}

/// 一次提交的运行句柄；`workflow_id` 与 `run_id` 共同确定运行身份。
#[derive(Debug, Clone)]
pub struct RunHandle {
    pub workflow_id: String,
    pub run_id: i64,
}
