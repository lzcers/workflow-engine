//! 单图工作流实例：创建时启动后台调度器，提交独立运行并唤醒认领。

use std::{any::Any, sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{sync::Notify, task::JoinHandle};

use crate::database::Database;
use crate::graph::{ErasedGraph, GraphDescription};
use crate::scheduler::Scheduler;
use crate::{EngineError, Graph, NodeContext, Trigger};

/// 一张固定图的非泛型工作流句柄，由 [`crate::Engine::make`] 创建。
///
/// 创建时启动后台调度器，处理当前图的存量任务与后续提交；每次 submit 创建独立的 run_id。
/// 与后台调度器共享同一个图对象，通过 graph / graph_as 访问图及其具体上下文。
/// 不同上下文的图返回同一种句柄，可在创建它的 Engine 释放后继续使用。
/// 执行期间需保留此实例；丢弃 Workflow 会取消调度器及在途任务。
pub struct Workflow {
    graph: Arc<dyn ErasedGraph>,
    description: GraphDescription,
    database: Database,
    worker: JoinHandle<()>,
    wake: Arc<Notify>,
}

impl Drop for Workflow {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Workflow {
    pub(crate) fn new<Ctx>(graph: Graph<Ctx>, database: Database, worker_id: Option<&str>) -> Self
    where
        Ctx: NodeContext + Sync + 'static,
    {
        let graph = Arc::new(graph);
        let description = graph.describe();
        let store = database.execution_store(graph.clone(), worker_id);
        let wake = Arc::new(Notify::new());
        let scheduler =
            Scheduler::new(graph.clone(), store, Duration::from_secs(1)).with_wake(wake.clone());
        let worker = tokio::spawn(async move {
            scheduler.run().await;
        });
        Self {
            graph,
            description,
            database,
            worker,
            wake,
        }
    }

    /// 后台调度器已终止时为 true，可供宿主报告 worker 健康状态。
    pub fn is_finished(&self) -> bool {
        self.worker.is_finished()
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
    /// 所有 run 共享 8 个执行槽和 1 秒轮询周期。
    /// 返回成功表示已持久化并安排执行，不等待节点执行完成。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        let task = self.description.task(entry, serde_json::to_value(input)?)?;
        let run = self.database.submit(task).await?;
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
