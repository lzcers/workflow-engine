//! 单图工作流实例：创建时启动后台调度器，提交独立运行、唤醒认领并支持优雅停机。

use std::{any::Any, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

use crate::error::StorageError;
use crate::graph::ErasedGraph;
use crate::scheduler::{DEFAULT_MAX_CONCURRENCY, Scheduler};
use crate::task::TaskStore;
use crate::{EngineError, Graph, NodeContext, Task, Trigger};

/// 后台调度器的并发与轮询参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerOptions {
    /// 当前工作流所有 run 共享的最大并发执行槽数（默认 8，至少 1）。
    pub max_concurrency: usize,
    /// 空闲且有空槽时的数据库轮询间隔（默认 1 秒）。
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
///
/// 创建时启动后台调度器，处理当前图的存量任务与后续提交；每次 submit 创建独立的 run_id。
/// 与后台调度器共享同一个图对象，通过 graph / graph_as 访问图及其具体上下文。
/// 不同上下文的图返回同一种句柄，可在创建它的 Engine 释放后继续使用。
/// 正常退出可调用 [`Self::shutdown`] 等待在途任务完成；直接丢弃会在 Drop 中取消后台调度器。
pub struct Workflow {
    graph: Arc<dyn ErasedGraph>,
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    worker: Option<JoinHandle<()>>,
    wake: Arc<Notify>,
    shutdown: watch::Sender<bool>,
}

impl Drop for Workflow {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

impl Workflow {
    pub(crate) fn new<Ctx>(
        graph: Arc<Graph<Ctx>>,
        store: Arc<dyn TaskStore<Error = StorageError> + Send>,
        options: WorkerOptions,
    ) -> Self
    where
        Ctx: NodeContext + Sync + 'static,
    {
        let wake = Arc::new(Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let scheduler = Scheduler::new(graph.clone(), store.clone(), options.poll_interval)
            .with_max_concurrency(options.max_concurrency)
            .with_wake(wake.clone())
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
        }
    }

    /// 停止认领新任务，等待当前在途任务完成结算与回调后退出后台调度器。
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        self.wake.notify_one();
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

    /// 取回原始 Graph<Ctx> 的借用；上下文类型不匹配时返回 None。
    pub fn graph_as<Ctx: 'static>(&self) -> Option<&Graph<Ctx>> {
        (self.graph() as &dyn Any).downcast_ref()
    }

    /// 获取全部外部触发节点的名称与注册声明，按节点名排序。
    ///
    /// 宿主负责解释配置、注册和注销触发器，事件到达时调用 submit(name, input)。
    /// 此方法不会注册监听或创建运行。
    pub fn triggers(&self) -> Vec<(&str, Trigger)> {
        self.graph.triggers()
    }

    /// 从指定节点创建并执行一次独立运行；原子分配 run_id 并持久化入口任务。
    ///
    /// input 必须能被该节点的 In 类型反序列化。成功提交立即唤醒认领；
    /// 返回成功表示已持久化并安排执行，不等待节点执行完成。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        let mut task = Task::new(
            self.graph.workflow_id(),
            entry,
            serde_json::to_value(input)?,
        );
        task.subgraph_path = self
            .graph
            .subgraph_path(entry)
            .ok_or_else(|| EngineError::UnknownNode(entry.to_owned()))?;
        let run = self.store.enqueue(task).await?;
        self.wake.notify_one();
        Ok(run)
    }
}

/// 一次提交的运行句柄；workflow_id 与 run_id 共同确定运行身份。
#[derive(Debug, Clone)]
pub struct RunHandle {
    pub workflow_id: String,
    pub run_id: i64,
}
