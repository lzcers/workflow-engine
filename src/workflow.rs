//! 单图工作流实例：提交独立运行并自动驱动后台调度器。

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use serde::Serialize;
use tokio::{sync::Notify, task::JoinHandle};

use crate::{EngineError, Graph, NodeContext, Scheduler, StorageError, TaskStore};

/// 一张固定图的工作流实例，由 [`crate::Engine::make`] 创建。
///
/// 每次 submit 创建独立的 run_id 并自动启动执行，所有运行共享一个后台调度器。
/// 工作流拥有图与存储的共享句柄，可在创建它的 Engine 释放后继续使用。
/// 执行期间需保留此实例；丢弃 Workflow 会取消调度器及在途任务。
pub struct Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    graph: Arc<Graph<Ctx>>,
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    scheduler: Arc<Scheduler<Ctx, dyn TaskStore<Error = StorageError> + Send>>,
    worker: OnceLock<Worker>,
    wake: Arc<Notify>,
}

struct Worker {
    task: JoinHandle<()>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<Ctx> Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    pub(crate) fn new(
        graph: Graph<Ctx>,
        store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    ) -> Self {
        let graph = Arc::new(graph);
        let wake = Arc::new(Notify::new());
        let scheduler = Arc::new(
            Scheduler::new(graph.clone(), store.clone(), Duration::from_secs(1))
                .with_wake(wake.clone()),
        );
        Self {
            graph,
            store,
            scheduler,
            worker: OnceLock::new(),
            wake,
        }
    }

    /// 从指定节点创建并执行一次独立运行；原子分配 run_id 并持久化入口任务。
    ///
    /// input 必须能被该节点的 In 类型反序列化。首次成功提交自动启动后台调度器，
    /// 后续提交立即唤醒认领；所有 run 共享 8 个执行槽和 1 秒轮询周期。
    /// 返回成功表示已持久化并安排执行，不等待节点执行完成。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        if !self.graph.contains_node(entry) {
            return Err(EngineError::UnknownNode(entry.to_owned()));
        }
        let input = serde_json::to_value(input)?;
        let record = self
            .store
            .create_run(self.graph.workflow_id(), entry, &input)
            .await?;
        self.worker.get_or_init(|| {
            let scheduler = self.scheduler.clone();
            Worker {
                task: tokio::spawn(async move {
                    scheduler.run().await;
                }),
            }
        });
        self.wake.notify_one();
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.run_id.expect("create_run must allocate a run ID"),
            task_id: record.task_id,
        })
    }
}

/// 一次提交的运行句柄；workflow_id 与 run_id 共同确定运行身份。
#[derive(Debug, Clone)]
pub struct RunHandle {
    pub workflow_id: String,
    pub run_id: i64,
    pub task_id: i64,
}

impl RunHandle {
    /// 查询本次图运行的状态。当前为空实现，不读取存储或返回实际状态。
    pub async fn status(&self) {}
}
