//! 可复用的引擎入口：连接数据库、创建共享存储并装配工作流实例。

use std::sync::Arc;

use crate::{Database, Graph, NodeContext, StorageError, TaskStore, Workflow};

/// 引擎初始化或工作流提交失败。
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("no node registered for name={0}")]
    UnknownNode(String),
    #[error("serialize workflow input: {0}")]
    Input(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// 可复用的引擎，持有由数据库 URL 选择的共享任务存储。
///
/// 每次 [`Self::make`] 根据图创建独立的 Workflow 和 Scheduler，首次提交时自动启动调度。
/// 需要启用 `pg` 或 `sqlite` feature。
pub struct Engine {
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
}

impl Engine {
    /// 根据 URL 连接数据库、幂等初始化引擎表并创建任务存储。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self {
            store: database.task_store(),
        })
    }

    /// 根据图定义创建工作流实例及其调度器，支持不同上下文类型的图。
    ///
    /// 各工作流共享存储，分别按图的 workflow_id 认领任务；不同图定义应使用不同的 ID。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow<Ctx>
    where
        Ctx: NodeContext + Sync + 'static,
    {
        Workflow::new(graph, self.store.clone())
    }
}
