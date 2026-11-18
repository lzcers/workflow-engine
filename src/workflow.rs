//! 单图工作流实例：创建时启动后台调度器，提交独立运行并唤醒认领。

use std::{sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{sync::Notify, task::JoinHandle};

use crate::error::StorageError;
use crate::scheduler::Scheduler;
use crate::task::TaskStore;
use crate::{EngineError, Graph, NodeContext, Task, Trigger};

/// 一张固定图的工作流实例，由 [`crate::Engine::make`] 创建。
///
/// 创建时启动后台调度器，处理当前图的存量任务与后续提交；每次 submit 创建独立的 run_id。
/// 工作流拥有图与存储的共享句柄，可在创建它的 Engine 释放后继续使用。
/// 执行期间需保留此实例；丢弃 Workflow 会取消调度器及在途任务。
pub struct Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    graph: Arc<Graph<Ctx>>,
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    worker: JoinHandle<()>,
    wake: Arc<Notify>,
}

impl<Ctx> Drop for Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    fn drop(&mut self) {
        self.worker.abort();
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
        let scheduler = Scheduler::new(graph.clone(), store.clone(), Duration::from_secs(1))
            .with_wake(wake.clone());
        let worker = tokio::spawn(async move {
            scheduler.run().await;
        });
        Self {
            graph,
            store,
            worker,
            wake,
        }
    }

    /// 获取全部外部触发节点的名称与注册声明，按节点名排序。
    ///
    /// 宿主负责解释配置、注册和注销触发器，事件到达时调用 submit(name, input)。
    /// 此方法不会注册监听或创建运行。
    pub fn triggers(&self) -> Vec<(&str, Trigger)> {
        self.graph.triggers()
    }

    /// 从指定节点创建并执行一次独立运行；原子分配 run_id 并持久化入口任务及节点类型。
    ///
    /// input 必须能被该节点的 In 类型反序列化。成功提交立即唤醒认领；
    /// 所有 run 共享 8 个执行槽和 1 秒轮询周期。
    /// 返回成功表示已持久化并安排执行，不等待节点执行完成。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        let task_type = self
            .graph
            .task_type_of(entry)
            .ok_or_else(|| EngineError::UnknownNode(entry.to_owned()))?;
        let mut task = Task::new(
            self.graph.workflow_id(),
            entry,
            serde_json::to_value(input)?,
        );
        task.task_type = task_type.as_str().to_owned();
        let record = self.store.enqueue(&task).await?;
        self.wake.notify_one();
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.run_id.expect("enqueue must assign a run ID"),
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
