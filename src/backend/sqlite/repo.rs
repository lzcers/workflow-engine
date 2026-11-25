//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Each task transaction starts with INSERT or UPDATE. That statement acquires the
//! write lock before any reads used to plan fanout or merge JOIN contributions.

use std::collections::HashMap;

use super::{join, scope};
use crate::task::{
    MAX_ATTEMPTS,
    scope::{ScopeEntries, route_path},
};
use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::graph::{ErasedGraph, JoinRule, Successor};
use crate::{TaskRecord, TaskStatus};

use crate::task::store::{FanoutSource, LEASE_DURATION, check_workflow, merge_join_inputs};

/// Columns suitable for application queries against the same task table.
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    lease_until, attempt, \
    input_json, output_json, error, created_at, updated_at";

/// SQLite task repository shared across workflow graphs.
#[derive(Clone)]
pub struct SqliteTaskRepo {
    pub(super) pool: SqlitePool,
}

fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}

/// The source settlement has already acquired the write lock in this transaction.
async fn enqueue_fanouts_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    graph: &dyn ErasedGraph,
    source: &FanoutSource<'_>,
    fanouts: &[Successor],
) -> Result<(), StorageError> {
    if !fanouts.is_empty() {
        let chain = scope::chain(tx, source.workflow_id, source.scope_id).await?;
        let mut entries = ScopeEntries::new();
        for plan in fanouts {
            if plan.inputs.is_empty() {
                continue;
            }
            let destination =
                graph
                    .subgraph_path(plan.dst)
                    .ok_or_else(|| StorageError::InvalidFanout {
                        detail: format!("unknown successor node {}", plan.dst),
                    })?;
            let (parent, entered) = route_path(
                &chain,
                source.run_id,
                &destination,
                plan.new_scope_per_input,
            )?;
            if plan.new_scope_per_input {
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
                    insert_task(tx, source, plan, scope_id, input).await?;
                }
                continue;
            }
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
            if plan.initial_status == TaskStatus::Collecting {
                collect_join(tx, source, plan, scope_id).await?;
            } else {
                for input in &plan.inputs {
                    insert_task(tx, source, plan, scope_id, input).await?;
                }
            }
        }
    }
    advance_joins(tx, source.workflow_id, source.run_id, graph.join_rules()).await
}

async fn collect_join(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
) -> Result<(), StorageError> {
    let existing = sqlx::query_as::<_, (i64, Json<Vec<i64>>, Value)>(
        "SELECT task_id, parent_task_ids, input_json FROM tasks
         WHERE workflow_id = ?1 AND name = ?2 AND run_id IS ?3 AND scope_id IS ?4
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
        insert_task(tx, source, plan, scope_id, &input).await?;
    }
    Ok(())
}

async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(source.workflow_id)
    .bind(source.run_id)
    .bind(plan.dst)
    .bind(plan.initial_status.as_str())
    .bind(Json([source.task_id]))
    .bind(input)
    .bind(scope_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// 全部后继写入后，在本次运行内传播失败并冻结已收齐的 JOIN 输入。
async fn advance_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: Option<i64>,
    rules: &HashMap<String, JoinRule>,
) -> Result<(), StorageError> {
    if rules.is_empty() {
        return Ok(());
    }
    let collecting = "wt.workflow_id = ?1 AND wt.run_id IS ?2 AND wt.status = 'collecting'";
    let fail = format!(
        "UPDATE tasks AS wt SET status = 'failed', error = 'upstream dependency failed',
            lease_until = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE {collecting} AND ({})",
        join::failed("?3")
    );
    loop {
        let changed = sqlx::query(&fail)
            .bind(workflow_id)
            .bind(run_id)
            .bind(Json(rules))
            .execute(&mut **tx)
            .await?
            .rows_affected();
        if changed == 0 {
            break;
        }
    }
    sqlx::query(&format!(
        "UPDATE tasks AS wt SET status = 'pending', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE {collecting} AND ({})",
        join::ready("?3")
    ))
    .bind(workflow_id)
    .bind(run_id)
    .bind(Json(rules))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl SqliteTaskRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Settle only the current, unexpired claim and enqueue successors in the same commit.
    /// Successors inherit the current database run_id and scope_id, including business updates.
    pub async fn succeed_and_enqueue(
        &self,
        graph: &dyn ErasedGraph,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
    ) -> Result<(), StorageError> {
        check_workflow(graph, &task.workflow_id)?;
        let mut tx = self.pool.begin().await?;
        let (run_id, scope_id) = sqlx::query_as(&format!(
            r#"
            UPDATE tasks
            SET status = 'succeeded', output_json = ?3, error = NULL, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND status = 'running' AND attempt = ?4
              AND attempt <= {MAX_ATTEMPTS}
              AND julianday(lease_until) > julianday('now')
            RETURNING run_id, scope_id
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(output)
        .bind(task.attempt)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let source = FanoutSource {
            workflow_id: &task.workflow_id,
            task_id: task.task_id,
            run_id,
            scope_id,
        };
        enqueue_fanouts_in_tx(&mut tx, graph, &source, fanouts).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Fence the source claim before generating failure successors or cascading failures.
    pub async fn fail_and_enqueue(
        &self,
        graph: &dyn ErasedGraph,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
    ) -> Result<(), StorageError> {
        check_workflow(graph, &task.workflow_id)?;
        let mut tx = self.pool.begin().await?;
        // Settle first so a failure self-edge creates a fresh pending task instead
        // of merging its retry input into the still-running source task.
        let (run_id, scope_id) = sqlx::query_as(
            r#"
            UPDATE tasks
            SET status = ?3, error = ?4, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND status = 'running' AND attempt = ?5
              AND julianday(lease_until) > julianday('now')
            RETURNING run_id, scope_id
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(if fanouts.is_empty() {
            TaskStatus::Failed.as_str()
        } else {
            TaskStatus::Superseded.as_str()
        })
        .bind(error)
        .bind(task.attempt)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let source = FanoutSource {
            workflow_id: &task.workflow_id,
            task_id: task.task_id,
            run_id,
            scope_id,
        };
        enqueue_fanouts_in_tx(&mut tx, graph, &source, fanouts).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Renew only a live claim without advancing its generation. Acquire the write lock
    /// before evaluating database time so a lease that expired while waiting stays expired.
    pub async fn renew_claim(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
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
    }

    /// Release only this still-valid claim so another worker may reclaim the task.
    pub async fn release_claim(&self, task: &TaskRecord) -> Result<(), StorageError> {
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
    }

    /// Claim a bounded batch, including expired tasks that need final failure settlement.
    /// Each claim advances the fencing generation. Claims above the retry budget may only settle failure;
    /// they never execute the node again. The write transaction serializes competing workers.
    pub async fn claim_many(
        &self,
        workflow_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StorageError> {
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
                  AND wt.status IN ('pending', 'running')
                ORDER BY wt.task_id LIMIT ?3
            )
            UPDATE tasks
            SET status = 'running',
                error = CASE WHEN attempt >= {MAX_ATTEMPTS} THEN COALESCE(error,
                    CASE WHEN lease_until IS NOT NULL
                         THEN 'task lease expired; retry budget exhausted'
                         ELSE 'task retry budget exhausted' END)
                    ELSE error END,
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
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sqlite.rs"]
mod tests;
