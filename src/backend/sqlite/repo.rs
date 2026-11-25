//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Each task transaction starts with INSERT or UPDATE. That statement acquires the
//! write lock before any reads used to plan fanout or merge JOIN contributions.

use std::{collections::HashMap, time::Duration};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::{rows::TaskRow, scope};
use crate::backend::ROW_COLS;
use crate::error::StorageError;
use crate::task::{
    Dispatch, JoinRule, MAX_ATTEMPTS, SettlementRules, Successor, TaskStore,
    join::{self, RunScope, RunTask},
    scope::{ScopeEntries, route_path},
    store::{FanoutSource, LEASE_DURATION, merge_join_inputs},
};
use crate::{Database, RunHandle, Task, TaskRecord, TaskStatus};

/// SQLite task repository shared across workflow graphs.
#[derive(Clone)]
pub(crate) struct SqliteTaskRepo {
    pub(super) pool: SqlitePool,
}

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
}

fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}

/// 仅当前有效认领可以提交终态；先执行写语句取得写锁再检查数据库时钟。
async fn settle_in_tx<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &'a TaskRecord,
    status: TaskStatus,
    output: Option<&Value>,
    error: Option<&str>,
) -> Result<FanoutSource<'a>, StorageError> {
    let (run_id, scope_id) = sqlx::query_as(&format!(
        r#"
        UPDATE tasks
        SET status = ?3, output_json = ?4, error = ?5, lease_until = NULL, available_at = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE workflow_id = ?1 AND task_id = ?2
          AND status = 'running' AND attempt = ?6
          AND (?3 IN ('failed', 'superseded') OR attempt <= {MAX_ATTEMPTS})
          AND julianday(lease_until) > julianday('now')
        RETURNING run_id, scope_id
        "#
    ))
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .bind(status.as_str())
    .bind(output)
    .bind(error)
    .bind(task.attempt)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| claim_lost(task))?;
    Ok(FanoutSource {
        workflow_id: &task.workflow_id,
        task_id: task.task_id,
        run_id,
        scope_id,
    })
}

/// The source settlement has already acquired the write lock in this transaction.
async fn enqueue_fanouts_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    fanouts: &[Successor],
    rules: &SettlementRules,
) -> Result<(), StorageError> {
    if !fanouts.is_empty() {
        let chain = scope::chain(tx, source.workflow_id, source.scope_id).await?;
        let mut entries = ScopeEntries::new();
        for plan in fanouts {
            if plan.inputs.is_empty() {
                continue;
            }
            let per_input_scope = plan.dispatch == Dispatch::PerInputScope;
            let (parent, entered) =
                route_path(&chain, source.run_id, &plan.subgraph_path, per_input_scope)?;
            match plan.dispatch {
                Dispatch::PerInputScope => {
                    for input in &plan.inputs {
                        let scope_id = scope::enter(
                            tx,
                            source.workflow_id,
                            source.run_id,
                            source.task_id,
                            parent,
                            entered,
                            None,
                        )
                        .await?;
                        insert_task(tx, source, plan.dst, TaskStatus::Pending, scope_id, input)
                            .await?;
                    }
                }
                Dispatch::Shared => {
                    let scope_id = scope::enter(
                        tx,
                        source.workflow_id,
                        source.run_id,
                        source.task_id,
                        parent,
                        entered,
                        Some(&mut entries),
                    )
                    .await?;
                    for input in &plan.inputs {
                        insert_task(tx, source, plan.dst, TaskStatus::Pending, scope_id, input)
                            .await?;
                    }
                }
                Dispatch::Join => {
                    let scope_id = scope::enter(
                        tx,
                        source.workflow_id,
                        source.run_id,
                        source.task_id,
                        parent,
                        entered,
                        Some(&mut entries),
                    )
                    .await?;
                    collect_join(tx, source, plan, scope_id).await?;
                }
            }
        }
    }
    advance_joins(tx, source.workflow_id, source.run_id, &rules.join_rules).await
}

async fn collect_join(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
) -> Result<(), StorageError> {
    let existing = sqlx::query_as::<_, (i64, Json<Vec<i64>>, Value)>(
        "SELECT task_id, parent_task_ids, input_json FROM tasks
         WHERE workflow_id = ?1 AND name = ?2 AND run_id = ?3 AND scope_id IS ?4
           AND status = 'collecting' ORDER BY task_id LIMIT 1",
    )
    .bind(source.workflow_id)
    .bind(plan.dst)
    .bind(source.run_id)
    .bind(scope_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((task_id, Json(mut parents), existing_input)) = existing {
        if !parents.contains(&source.task_id) {
            parents.push(source.task_id);
        }
        let input = merge_join_inputs(Some(existing_input), &plan.inputs)?;
        sqlx::query(
            "UPDATE tasks SET parent_task_ids = ?3, input_json = ?4,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id = ?2",
        )
        .bind(source.workflow_id)
        .bind(task_id)
        .bind(Json(parents))
        .bind(input)
        .execute(&mut **tx)
        .await?;
    } else {
        let input = merge_join_inputs(None, &plan.inputs)?;
        insert_task(
            tx,
            source,
            plan.dst,
            TaskStatus::Collecting,
            scope_id,
            &input,
        )
        .await?;
    }
    Ok(())
}

async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    dst: &str,
    status: TaskStatus,
    scope_id: Option<i64>,
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(source.workflow_id)
    .bind(source.run_id)
    .bind(dst)
    .bind(status.as_str())
    .bind(Json([source.task_id]))
    .bind(input)
    .bind(scope_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// 全部后继写入后，读取当前运行的轻量状态快照，通过共享纯 Rust 规则推进 JOIN 状态。
async fn advance_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: i64,
    rules: &HashMap<String, JoinRule>,
) -> Result<(), StorageError> {
    if rules.is_empty() {
        return Ok(());
    }
    let tasks = sqlx::query_as::<_, RunTask>(
        "SELECT task_id, name, status, scope_id FROM tasks WHERE workflow_id = ?1 AND run_id = ?2",
    )
    .bind(workflow_id)
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    if !tasks.iter().any(|task| task.status == TaskStatus::Collecting) {
        return Ok(());
    }
    let scopes = sqlx::query_as::<_, RunScope>(
        "SELECT scope_id, parent_scope_id, trigger_task_id FROM scopes
         WHERE workflow_id = ?1 AND run_id = ?2",
    )
    .bind(workflow_id)
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;

    let transitions = join::advance_joins(&tasks, &scopes, rules);
    if !transitions.failed.is_empty() {
        sqlx::query(
            "UPDATE tasks
             SET status = 'failed', error = 'upstream dependency failed',
                 lease_until = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
        )
        .bind(workflow_id)
        .bind(Json(&transitions.failed))
        .execute(&mut **tx)
        .await?;
    }
    if !transitions.ready.is_empty() {
        sqlx::query(
            "UPDATE tasks
             SET status = 'pending', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
        )
        .bind(workflow_id)
        .bind(Json(&transitions.ready))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

impl SqliteTaskRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
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
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let mut tx = self.pool.begin().await?;
            let mut rows = sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                WITH candidate AS MATERIALIZED (
                    SELECT wt.task_id FROM tasks wt
                    WHERE wt.workflow_id = ?1
                      AND (wt.lease_until IS NULL OR julianday(wt.lease_until) <= julianday('now'))
                      AND (wt.available_at IS NULL OR julianday(wt.available_at) <= julianday('now'))
                      AND wt.status IN ('pending', 'running')
                    ORDER BY wt.task_id LIMIT ?3
                )
                UPDATE tasks
                SET status = 'running',
                    error = CASE WHEN attempt >= {MAX_ATTEMPTS} THEN COALESCE(error,
                        CASE WHEN lease_until IS NOT NULL
                             THEN 'task lease expired; retry budget exhausted'
                             ELSE 'task retry budget exhausted' END)
                        ELSE NULL END,
                    available_at = NULL,
                    lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds'),
                    attempt = attempt + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id IN (SELECT task_id FROM candidate)
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(LEASE_DURATION.as_secs() as i64)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
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
            let mut tx = self.pool.begin().await?;
            let lease_until = sqlx::query_scalar(
                r#"
                UPDATE tasks
                SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND attempt = ?3
                  AND status = 'running' AND julianday(lease_until) > julianday('now')
                RETURNING lease_until
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .bind(LEASE_DURATION.as_secs() as i64)
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
            let mut tx = self.pool.begin().await?;
            let source =
                settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
            enqueue_fanouts_in_tx(&mut tx, &source, successors, rules).await?;
            tx.commit().await?;
            Ok(())
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
            let mut tx = self.pool.begin().await?;
            let status = if successors.iter().all(|plan| plan.inputs.is_empty()) {
                TaskStatus::Failed
            } else {
                TaskStatus::Superseded
            };
            let source = settle_in_tx(&mut tx, task, status, None, Some(message)).await?;
            enqueue_fanouts_in_tx(&mut tx, &source, successors, rules).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn schedule_retry<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        delay: Duration,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let result = sqlx::query(&format!(
                r#"
                UPDATE tasks
                SET status = 'pending', error = ?3, lease_until = NULL,
                    available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?4
                  AND attempt < {MAX_ATTEMPTS}
                  AND julianday(lease_until) > julianday('now')
                "#
            ))
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(message)
            .bind(task.attempt)
            .bind(delay.as_secs_f64())
            .execute(&self.pool)
            .await?;
            if result.rows_affected() == 0 {
                return Err(claim_lost(task));
            }
            Ok(())
        })
    }

    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let result = sqlx::query(
                r#"
                UPDATE tasks SET lease_until = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?3
                  AND julianday(lease_until) > julianday('now')
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .execute(&self.pool)
            .await?;
            if result.rows_affected() == 0 {
                return Err(claim_lost(task));
            }
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sqlite.rs"]
mod tests;
