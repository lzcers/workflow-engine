//! 可复用的引擎入口：连接数据库，多个工作流共享任务仓储。

use std::sync::Arc;

use crate::database::{Database, DatabasePool};
pub use crate::error::StorageError;
use crate::task::TaskStore;
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

/// 可复用的引擎，为其创建的所有工作流共享同一个任务仓储。
///
/// 每次 [`Self::make`] 根据图创建独立的 Workflow 和 Scheduler，并立即启动后台调度。
/// 默认使用 SQLite，也可通过 `pg` feature 启用 PostgreSQL。
pub struct Engine {
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
}

impl Engine {
    /// 根据 URL 连接数据库并幂等初始化引擎表。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 复用宿主已经初始化的存储连接。
    pub fn from_database(database: Database) -> Self {
        let store: Arc<dyn TaskStore<Error = StorageError> + Send> = match database.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => Arc::new(crate::backend::pgsql::PgTaskRepo::new(pool)),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                Arc::new(crate::backend::sqlite::SqliteTaskRepo::new(pool))
            }
        };
        Self { store }
    }

    /// 根据图定义创建工作流实例并启动后台调度器，支持不同上下文类型的图。
    ///
    /// 返回非泛型的 Workflow 句柄，与后台调度器共享原始图对象，保留图访问能力。
    /// 每个调度器按 workflow_id 认领任务。
    /// 创建后即处理存量任务，无需先提交新运行；执行期间需保持返回的 Workflow 存活。
    ///
    /// # Panics
    ///
    /// 必须在 Tokio runtime 中调用，否则启动后台任务时 panic。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        Workflow::new(Arc::new(graph), self.store.clone())
    }
}
