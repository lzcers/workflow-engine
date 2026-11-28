//! SQLite 通用工作流仓储实现：所有写事务统一 `BEGIN IMMEDIATE`、零 `julianday()`、原子认领与结算。

use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::{rows::TaskRow, scope, tx::SqliteTx};
use crate::backend::{
    BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR, ROW_COLS, claim_lost,
    common::{SettleOutcome, enqueue_fanouts_and_advance},
};
use crate::error::StorageError;
use crate::task::{
    MAX_ATTEMPTS, RunHandle, SettlementRules, Successor, Task, TaskRecord, TaskStore,
    store::{FanoutSource, LEASE_DURATION},
};

/// SQLite task repository shared across workflow graphs.
#[derive(Clone)]
pub(crate) struct SqliteTaskRepo {
    pub(super) pool: SqlitePool,
}

/// 在给定 SQLite 事务内创建独立运行，同时建立入口所属的隔离作用域。
pub(crate) async fn submit_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: Task,
) -> Result<RunHandle, StorageError> {
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
        run_id,
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

/// 仅当前有效认领可以提交终态；定长 UTC 文本直接比较（无 `julianday` 包装）。
async fn settle_in_tx<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &'a TaskRecord,
    outcome: SettleOutcome<'_>,
) -> Result<FanoutSource<'a>, StorageError> {
    let (status, output, error) = outcome.parts();
    let (run_id, scope_id) = sqlx::query_as(
        r#"
        UPDATE tasks
        SET status = ?3, output_json = ?4, error = ?5, lease_until = NULL, available_at = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE workflow_id = ?1 AND task_id = ?2
          AND status = 'running' AND attempt = ?6
          AND (?3 IN ('failed', 'superseded') OR attempt <= ?7)
          AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        RETURNING run_id, scope_id
        "#,
    )
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .bind(status.as_str())
    .bind(output)
    .bind(error)
    .bind(task.attempt)
    .bind(MAX_ATTEMPTS)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| claim_lost(task))?;
    Ok(FanoutSource {
        workflow_id: &task.workflow_id,
        name: &task.name,
        task_id: task.task_id,
        run_id,
        scope_id,
    })
}

impl SqliteTaskRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn settle(
        &self,
        task: &TaskRecord,
        outcome: SettleOutcome<'_>,
        successors: &[Successor],
        rules: &SettlementRules,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let source = settle_in_tx(&mut tx, task, outcome).await?;
        enqueue_fanouts_and_advance(&mut SqliteTx(&mut tx), &source, successors, rules, None)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl TaskStore for SqliteTaskRepo {
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let run = submit_in_tx(&mut tx, task).await?;
            tx.commit().await?;
            Ok(run)
        })
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let sql = format!(
                r#"
                WITH candidate AS MATERIALIZED (
                    SELECT wt.task_id FROM tasks wt
                    WHERE wt.workflow_id = ?1
                      AND wt.status IN ('pending', 'running')
                      AND (wt.lease_until IS NULL OR wt.lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                      AND (wt.available_at IS NULL OR wt.available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    ORDER BY wt.task_id LIMIT ?3
                )
                UPDATE tasks
                SET status = 'running',
                    error = CASE WHEN attempt >= ?6 THEN COALESCE(error,
                        CASE WHEN lease_until IS NOT NULL THEN ?4 ELSE ?5 END)
                        ELSE NULL END,
                    available_at = NULL,
                    lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds'),
                    attempt = attempt + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id IN (SELECT task_id FROM candidate)
                RETURNING {ROW_COLS}
                "#
            );
            let mut rows = sqlx::query_as::<_, TaskRow>(&sql)
                .bind(workflow_id)
                .bind(LEASE_DURATION.as_secs_f64())
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .bind(LEASE_EXPIRED_ERR)
                .bind(BUDGET_EXHAUSTED_ERR)
                .bind(MAX_ATTEMPTS)
                .fetch_all(&mut *tx)
                .await?;
            tx.commit().await?;
            rows.sort_unstable_by_key(|row| row.task_id);
            Ok(rows.into_iter().map(TaskRecord::from).collect())
        })
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let lease_until = sqlx::query_scalar(
                r#"
                UPDATE tasks
                SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND attempt = ?3
                  AND status = 'running'
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                RETURNING lease_until
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .bind(LEASE_DURATION.as_secs_f64())
            .fetch_optional(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(lease_until)
        })
    }

    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.settle(task, SettleOutcome::Succeeded(output), successors, rules)
                .await
        })
    }

    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.settle(
                task,
                SettleOutcome::failure(message, successors),
                successors,
                rules,
            )
            .await
        })
    }

    fn schedule_retry<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        delay: Duration,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET status = 'pending', error = ?3, lease_until = NULL,
                    available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?4
                  AND attempt < ?6
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(message)
            .bind(task.attempt)
            .bind(delay.as_secs_f64())
            .bind(MAX_ATTEMPTS)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                return Err(claim_lost(task));
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
        error: Option<&'a str>,
        delay: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET lease_until = NULL,
                    error = COALESCE(?4, error),
                    available_at = CASE
                        WHEN ?5 IS NOT NULL AND attempt < ?6
                        THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds')
                        ELSE available_at
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?3
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .bind(error)
            .bind(delay.map(|d| d.as_secs_f64()))
            .bind(MAX_ATTEMPTS)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 0 {
                return Err(claim_lost(task));
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn ready_workflows<'a>(
        &'a self,
        workflow_ids: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            if workflow_ids.is_empty() {
                return Ok(Vec::new());
            }
            let ids = sqlx::query_scalar(
                r#"
                SELECT DISTINCT workflow_id
                FROM tasks
                WHERE workflow_id IN (SELECT value FROM json_each(?1))
                  AND status IN ('pending', 'running')
                  AND (lease_until IS NULL OR lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                  AND (available_at IS NULL OR available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                "#,
            )
            .bind(Json(workflow_ids))
            .fetch_all(&self.pool)
            .await?;
            Ok(ids)
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sqlite.rs"]
mod tests;
