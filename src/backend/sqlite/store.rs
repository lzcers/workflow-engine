//! [`SqliteTaskRepo`] 直接实现通用引擎 [`crate::task::TaskStore`]。
//!
//! 通过 [`SqliteTaskRepo`] 持久化当前图规划的后继输入，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::SqliteTaskRepo;
use super::scope;
use crate::error::StorageError;
use crate::graph::{ErasedGraph, Successor};
use crate::task::TaskStore;
use crate::{Database, RunHandle, Task, TaskRecord};

impl Database<&mut sqlx::Transaction<'_, sqlx::Sqlite>> {
    /// 提交独立运行的入口。首次 INSERT 在分配 ID 时取得写锁。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        let tx = self.0;
        let run_id = sqlx::query(
            "INSERT INTO tasks (workflow_id, name, status, input_json)
             VALUES (?1, ?2, 'pending', ?3)",
        )
        .bind(&task.workflow_id)
        .bind(&task.name)
        .bind(&task.input)
        .execute(&mut **tx)
        .await?
        .last_insert_rowid();
        let scope_id = scope::enter(
            tx,
            &task.workflow_id,
            Some(run_id),
            run_id,
            None,
            &task.subgraph_path,
            None,
        )
        .await?;
        sqlx::query("UPDATE tasks SET run_id = task_id, scope_id = ?1 WHERE task_id = ?2")
            .bind(scope_id)
            .bind(run_id)
            .execute(&mut **tx)
            .await?;
        Ok(RunHandle {
            workflow_id: task.workflow_id,
            run_id,
        })
    }
}

impl TaskStore for SqliteTaskRepo {
    type Error = StorageError;

    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>> {
        Box::pin(async move { Database::from(self.pool.clone()).submit(task).await })
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(SqliteTaskRepo::claim_many(self, workflow_id, limit))
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(SqliteTaskRepo::renew_claim(self, task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，继承任务最新的 run_id 和 scope_id。
    fn settle_success<'a>(
        &'a self,
        graph: &'a dyn ErasedGraph,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.succeed_and_enqueue(graph, task, output, successors))
    }

    /// 原子失败结算：有后继则入队并标记 superseded，否则标记 failed 并级联已有依赖。
    fn settle_failure<'a>(
        &'a self,
        graph: &'a dyn ErasedGraph,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.fail_and_enqueue(graph, task, message, successors))
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(SqliteTaskRepo::release_claim(self, task))
    }
}
