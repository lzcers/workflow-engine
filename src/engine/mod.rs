//! 可复用的引擎入口：共享存储、共享多工作流轮询器与 PostgreSQL `LISTEN/NOTIFY` 跨进程唤醒。

mod wake_hub;

use std::sync::Arc;

use crate::database::{Database, DatabasePool, PoolOptions};
pub use crate::error::{GraphError, StorageError};
use crate::task::TaskStore;
use crate::workflow::WorkerOptions;
use crate::{Graph, NodeContext, Workflow};

pub(crate) use wake_hub::{HubRegistration, WakeHub};

/// 引擎初始化、提交或运行控制失败。
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("no node registered for name={0}")]
    UnknownNode(String),
    #[error("no run found for run_id={0}")]
    UnknownRun(i64),
    #[error("invalid graph: {0}")]
    InvalidGraph(#[from] GraphError),
    #[error("invalid workflow input: {0}")]
    Input(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// 可复用的引擎，为其创建的所有工作流共享同一个任务仓储与共享唤醒中枢。
pub struct Engine {
    store: Arc<dyn TaskStore>,
    hub: Arc<WakeHub>,
}

impl Engine {
    /// 根据 URL 使用默认连接池参数连接数据库并幂等初始化引擎表。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 使用自定义连接池参数连接数据库并幂等初始化引擎表。
    pub async fn new_with(url: &str, pool_options: PoolOptions) -> Result<Self, EngineError> {
        let database = Database::connect_with(url, pool_options).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 复用宿主已经初始化的存储连接。
    pub fn from_database(database: Database) -> Self {
        #[cfg(feature = "pg")]
        let pg_pool = match &database.0 {
            DatabasePool::Postgres(pool) => Some(pool.clone()),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => None,
        };
        let store: Arc<dyn TaskStore> = match database.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => Arc::new(crate::backend::pgsql::PgTaskRepo::new(pool)),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                Arc::new(crate::backend::sqlite::SqliteTaskRepo::new(pool))
            }
        };
        let hub = WakeHub::new(
            store.clone(),
            #[cfg(feature = "pg")]
            pg_pool,
        );
        Self { store, hub }
    }

    /// 根据图定义创建工作流实例并启动后台调度器，支持不同上下文类型的图。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        self.make_with(graph, WorkerOptions::default())
    }

    /// 使用指定的并发、轮询、租约与重试策略参数创建工作流实例并启动后台调度器。
    pub fn make_with<Ctx>(&self, graph: Graph<Ctx>, options: WorkerOptions) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        Workflow::new(Arc::new(graph), self.store.clone(), &self.hub, options)
    }
}
