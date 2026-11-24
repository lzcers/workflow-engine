//! [`SqliteTaskRepo`] 直接实现通用引擎 [`crate::task::TaskStore`]。
//!
//! 通过 [`SqliteTaskRepo`] 持久化当前图规划的后继输入，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::types::Json;

use super::repo::SqliteTaskRepo;
use super::{repo::ROW_COLS, rows::TaskRow, scope};
use crate::error::StorageError;
use crate::graph::Successor;
use crate::task::TaskStore;
use crate::task::scope::ScopeEntries;
use crate::{RunHandle, Task, TaskRecord, TaskStatus};

/// Insert the task and allocate its run and scope in the caller's transaction.
pub(super) async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &Task,
    status: TaskStatus,
    run_id: Option<i64>,
    source_scope_id: Option<i64>,
    parent_task_ids: &[i64],
) -> Result<TaskRecord, StorageError> {
    let task_id: i64 = sqlx::query_scalar(
        "INSERT INTO tasks (workflow_id,name,status,parent_task_ids,input_json)
         VALUES (?1,?2,?3,?4,?5) RETURNING task_id",
    )
    .bind(&task.workflow_id)
    .bind(&task.name)
    .bind(status.as_str())
    .bind(Json(parent_task_ids))
    .bind(&task.input)
    .fetch_one(&mut **tx)
    .await?;
    let run_id = Some(run_id.unwrap_or(task_id));
    let scope_id = scope::route(
        tx,
        &task.workflow_id,
        run_id,
        source_scope_id,
        &task.subgraph_path,
        task_id,
        false,
        &mut ScopeEntries::new(),
    )
    .await?;
    let row = sqlx::query_as::<_, TaskRow>(&format!(
        "UPDATE tasks SET run_id=?1, scope_id=?2 WHERE task_id=?3 RETURNING {ROW_COLS}"
    ))
    .bind(run_id)
    .bind(scope_id)
    .bind(task_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(row.into())
}

impl TaskStore for SqliteTaskRepo {
    type Database = sqlx::Sqlite;
    type Error = StorageError;

    async fn enqueue(
        tx: &mut sqlx::Transaction<'_, Self::Database>,
        task: Task,
    ) -> Result<RunHandle, StorageError> {
        let root = insert_task(tx, &task, TaskStatus::Pending, None, None, &[]).await?;
        Ok(RunHandle {
            workflow_id: root.workflow_id,
            run_id: root.task_id,
        })
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
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.succeed_and_enqueue(task, output, successors)
                .await
                .map(|_| ())
        })
    }

    /// 原子失败结算：有后继则入队并标记 superseded，否则标记 failed 并级联已有依赖。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.fail_and_enqueue(task, message, successors))
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(SqliteTaskRepo::release_claim(self, task))
    }
}
