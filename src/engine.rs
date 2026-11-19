//! 可复用的引擎入口：连接数据库、创建共享存储并装配工作流实例。

use std::sync::Arc;

use crate::database::Database;
pub use crate::error::StorageError;
use crate::{Graph, NodeContext, Workflow};

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

/// 可复用的引擎，持有由数据库 URL 选择的共享连接池。
///
/// 每次 [`Self::make`] 根据图创建独立的 Workflow 和 Scheduler，并立即启动后台调度。
/// 默认使用 SQLite，也可通过 `pg` feature 启用 PostgreSQL。
pub struct Engine {
    database: Database,
}

impl Engine {
    /// 根据 URL 连接数据库并幂等初始化引擎表。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self { database })
    }

    /// 根据图定义创建工作流实例并启动后台调度器，支持不同上下文类型的图。
    ///
    /// 返回非泛型的 Workflow 句柄，与后台调度器共享原始图对象，保留图访问能力。
    /// 仓储与调度器共享原始图，共享连接池并按 workflow_id 隔离任务；不同图定义应使用不同的 ID。
    /// 创建后即处理存量任务，无需先提交新运行；执行期间需保持返回的 Workflow 存活。
    ///
    /// # Panics
    ///
    /// 必须在 Tokio runtime 中调用，否则启动后台任务时 panic。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        let graph = Arc::new(graph);
        let store = self.database.task_store(graph.clone());
        Workflow::new(graph, store)
    }
}
