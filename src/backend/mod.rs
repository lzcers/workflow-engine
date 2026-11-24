//! 数据库后端：各自实现共享任务模型与存储契约。

#[cfg(feature = "pg")]
pub mod pgsql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use std::sync::Arc;

use crate::database::{Database, DatabasePool};
use crate::error::StorageError;
use crate::graph::ErasedGraph;
use crate::task::TaskStore;

/// 每张图使用独立仓储，所有仓储共用宿主连接池。
pub(crate) struct TaskStoreFactory(pub(crate) Database);

impl TaskStoreFactory {
    pub(crate) async fn connect(url: &str) -> Result<Self, StorageError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self(database))
    }

    pub(crate) fn bind(
        &self,
        graph: Arc<dyn ErasedGraph>,
        worker_id: Option<&str>,
    ) -> Arc<dyn TaskStore<Error = StorageError> + Send> {
        match (self.0.pool(), worker_id) {
            #[cfg(feature = "pg")]
            (DatabasePool::Postgres(pool), Some(id)) => {
                Arc::new(pgsql::PgTaskRepo::for_worker(pool.clone(), graph, id))
            }
            #[cfg(feature = "pg")]
            (DatabasePool::Postgres(pool), None) => {
                Arc::new(pgsql::PgTaskRepo::new(pool.clone(), graph))
            }
            #[cfg(feature = "sqlite")]
            (DatabasePool::Sqlite(pool), Some(id)) => {
                Arc::new(sqlite::SqliteTaskRepo::for_worker(pool.clone(), graph, id))
            }
            #[cfg(feature = "sqlite")]
            (DatabasePool::Sqlite(pool), None) => {
                Arc::new(sqlite::SqliteTaskRepo::new(pool.clone(), graph))
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/task_fixture.rs"]
pub(crate) mod task_fixture;
