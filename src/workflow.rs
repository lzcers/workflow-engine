//! 单图工作流实例：启动后台调度器、强类型预检提交与优雅停机。

use std::{any::Any, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

use crate::error::EngineError;
use crate::graph::{ErasedGraph, Graph};
use crate::node::{NodeContext, Trigger};
use crate::scheduler::{DEFAULT_MAX_CONCURRENCY, HubRegistration, Scheduler, WakeHub};
pub use crate::task::RunHandle;
use crate::task::{Task, TaskStore};

/// 后台调度器的并发与轮询参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerOptions {
    /// 当前工作流所有 run 共享的最大并发执行槽数（默认 8，至少 1）。
    pub max_concurrency: usize,
    /// 空闲且有空槽时的数据库轮询间隔（默认 1 秒；多工作流共用引擎共享轮询器）。
    pub poll_interval: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// 一张固定图的非泛型工作流句柄，由 [`crate::Engine::make`] 或 [`crate::Engine::make_with`] 创建。
pub struct Workflow {
    graph: Arc<dyn ErasedGraph>,
    store: Arc<dyn TaskStore>,
    worker: Option<JoinHandle<()>>,
    wake: Arc<Notify>,
    shutdown: watch::Sender<bool>,
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
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registration = hub.register(
            graph.workflow_id().to_owned(),
            wake.clone(),
            options.poll_interval,
        );
        let scheduler = Scheduler::new(graph.clone(), store.clone(), options.poll_interval)
            .with_max_concurrency(options.max_concurrency)
            .with_wake(wake.clone())
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
            shutdown,
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

    /// 构造并强校验入口任务，可直接交给 `Database::in_transaction(&mut tx).submit(task)`。
    pub fn prepare_task(&self, entry: &str, input: impl Serialize) -> Result<Task, EngineError> {
        let value = serde_json::to_value(input)?;
        self.graph.task(entry, value)
    }

    /// 从指定节点创建并执行一次独立运行；入口处即校验节点存在与 `In` 反序列化兼容性。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        let task = self.prepare_task(entry, input)?;
        let run = self.store.enqueue(task).await?;
        self.wake.notify_one();
        Ok(run)
    }
}
