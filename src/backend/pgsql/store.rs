//! 为 [`PgTaskRepo`] 实现引擎的原子任务存储契约。
//!
//! 通过 [`PgTaskRepo`] 使用当前工作流的上游关系，接收图规划的后继，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::PgTaskRepo;
use super::{repo::ROW_COLS, rows::TaskRow, scope};
use crate::error::StorageError;
use crate::graph::Successor;
use crate::task::ExecutionStore;
use crate::task::scope::ScopeEntries;
use crate::{Task, TaskRecord, TaskStatus};

/// 入口入队与工作流句柄共享同一事务语义，包括深层子图入口的 scope 链。
pub(super) async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task: &Task,
    status: TaskStatus,
    run_id: Option<i64>,
    source_scope_id: Option<i64>,
    parent_task_ids: &[i64],
) -> Result<TaskRecord, StorageError> {
    // 子图入口需要在写入作用域时引用即将创建的任务。
    let task_id: i64 =
        sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('tasks', 'task_id'))")
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
    let sql = format!(
        "INSERT INTO tasks
         (workflow_id, task_id, run_id, name, status, parent_task_ids, input_json, scope_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {ROW_COLS}"
    );
    let row = sqlx::query_as::<_, TaskRow>(&sql)
        .bind(&task.workflow_id)
        .bind(task_id)
        .bind(run_id)
        .bind(&task.name)
        .bind(status.as_str())
        .bind(parent_task_ids)
        .bind(&task.input)
        .bind(scope_id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.into())
}

impl crate::task::store::TaskStore for sqlx::Transaction<'_, sqlx::Postgres> {
    async fn enqueue(&mut self, task: Task) -> Result<TaskRecord, StorageError> {
        let record = insert_task(self, &task, TaskStatus::Pending, None, None, &[]).await?;
        super::repo::notify_tasks_ready(self, &task.workflow_id).await?;
        Ok(record)
    }
}

impl ExecutionStore for PgTaskRepo {
    type Error = StorageError;

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(PgTaskRepo::claim_many(self, workflow_id, limit))
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(PgTaskRepo::renew_claim(self, task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，后继使用任务最新的分组键。
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

    /// 原子失败结算：标记终态，并入队失败后继或级联已有依赖任务。
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
        Box::pin(PgTaskRepo::release_claim(self, task))
    }
}
