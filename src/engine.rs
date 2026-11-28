//! 可复用的引擎入口：装配共享数据库、任务仓储与跨工作流唤醒中枢。

use std::sync::Arc;

use crate::database::Database;
pub use crate::error::{EngineError, GraphError, StorageError};
use crate::graph::Graph;
use crate::node::NodeContext;
use crate::scheduler::WakeHub;
use crate::task::TaskStore;
use crate::workflow::{WorkerOptions, Workflow};

/// 可复用的引擎，为其创建的所有工作流共享同一个数据库、任务仓储与唤醒中枢。
pub struct Engine {
    database: Database,
    store: Arc<dyn TaskStore>,
    hub: Arc<WakeHub>,
}

impl Engine {
    /// 根据 URL 连接数据库并幂等初始化引擎表。
    ///
    /// 需要自定义连接池参数时，先构造连接池并调用 [`Self::from_database`]。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 复用宿主已经初始化的存储连接。
    pub fn from_database(database: Database) -> Self {
        let store = database.task_store();
        let db_for_wake = database.clone();
        let hub = WakeHub::new(store.clone(), move |on_wake| {
            db_for_wake.spawn_wake_listener(on_wake)
        });
        Self {
            database,
            store,
            hub,
        }
    }

    /// 访问底层共享 [`Database`] 句柄（可用于健康检查、优雅关闭连接池或开启宿主业务事务）。
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// 根据图定义创建工作流实例并启动后台调度器，支持不同上下文类型的图。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        self.make_with(graph, WorkerOptions::default())
    }

    /// 使用指定的并发与轮询参数创建工作流实例并启动后台调度器。
    pub fn make_with<Ctx>(&self, graph: Graph<Ctx>, options: WorkerOptions) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        Workflow::new(Arc::new(graph), self.store.clone(), &self.hub, options)
    }
}
