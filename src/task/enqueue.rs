//! 在数据库事务中创建一次运行。任务和入口 scope 随调用方的事务提交。
use std::future::Future;

use serde_json::Value;

use crate::{Database, EngineError, RunHandle, Task, TaskStatus, graph::GraphDescription};

/// PostgreSQL 和 SQLite 的 SQLx 事务都可以作为任务写入端。
pub trait TaskStore {
    /// revision 与执行端的版本匹配。未绑定版本的本地工作流使用 None。
    fn enqueue(
        &mut self,
        graph: &GraphDescription,
        revision: Option<&str>,
        entry: &str,
        input: Value,
    ) -> impl Future<Output = Result<RunHandle, EngineError>> + Send;
}

#[cfg(feature = "pg")]
impl TaskStore for sqlx::Transaction<'_, sqlx::Postgres> {
    async fn enqueue(
        &mut self,
        graph: &GraphDescription,
        revision: Option<&str>,
        entry: &str,
        input: Value,
    ) -> Result<RunHandle, EngineError> {
        use crate::backend::pgsql::{insert_task_at_path, notify_tasks_ready};
        let path = graph.entry_path(entry)?;
        let task = Task::new(&graph.workflow_id, entry, input);
        let record = insert_task_at_path(self, &task, revision, &path, TaskStatus::Pending).await?;
        notify_tasks_ready(self, &task.workflow_id).await?;
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.task_id,
        })
    }
}

#[cfg(feature = "sqlite")]
impl TaskStore for sqlx::Transaction<'_, sqlx::Sqlite> {
    async fn enqueue(
        &mut self,
        graph: &GraphDescription,
        revision: Option<&str>,
        entry: &str,
        input: Value,
    ) -> Result<RunHandle, EngineError> {
        use crate::backend::sqlite::insert_task_at_path;
        let path = graph.entry_path(entry)?;
        let task = Task::new(&graph.workflow_id, entry, input);
        let record = insert_task_at_path(self, &task, revision, &path, TaskStatus::Pending).await?;
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.task_id,
        })
    }
}

impl Database {
    /// 单独入队并提交。有其他数据要一并提交时，直接在事务上调用 TaskStore::enqueue。
    pub async fn enqueue(
        &self,
        graph: &GraphDescription,
        revision: Option<&str>,
        entry: &str,
        input: Value,
    ) -> Result<RunHandle, EngineError> {
        use crate::error::StorageError;
        match self {
            #[cfg(feature = "pg")]
            Self::Postgres(pool) => {
                let mut tx = pool.begin().await.map_err(StorageError::from)?;
                let run = tx.enqueue(graph, revision, entry, input).await?;
                tx.commit().await.map_err(StorageError::from)?;
                Ok(run)
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => {
                let mut tx = pool.begin().await.map_err(StorageError::from)?;
                let run = tx.enqueue(graph, revision, entry, input).await?;
                tx.commit().await.map_err(StorageError::from)?;
                Ok(run)
            }
        }
    }
}
