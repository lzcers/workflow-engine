//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Each task transaction starts with INSERT or UPDATE. That statement acquires the
//! write lock before any reads used to plan fanout or merge JOIN contributions.

use std::{collections::HashMap, sync::Arc};

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

use crate::task::store::{LEASE_DURATION, check_workflow, merge_join_input};

/// Columns suitable for application queries against the same task table.
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    lease_until, attempt, \
    input_json, output_json, error, created_at, updated_at";

/// SQLite task repository bound to a workflow graph and its worker identity.
#[derive(Clone)]
pub struct SqliteTaskRepo {
    pub(super) pool: SqlitePool,
    pub(super) graph: Arc<dyn ErasedGraph>,
    worker_id: Option<Arc<str>>,
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
    source: &TaskRecord,
    fanouts: &[Successor],
) -> Result<(), StorageError> {
    if !fanouts.is_empty() {
        let chain = scope::chain(tx, &source.workflow_id, source.scope_id).await?;
        let mut entries = ScopeEntries::new();
        for plan in fanouts {
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
            for input in &plan.inputs {
                let scope_id = scope::enter(
                    tx,
                    &source.workflow_id,
                    source.run_id,
                    source.task_id,
                    parent,
                    entered,
                    (!plan.new_scope_per_input).then_some(&mut entries),
                )
                .await?;
                enqueue_input(tx, source, plan, scope_id, input).await?;
            }
        }
    }
    fail_blocked_joins(tx, &source.workflow_id, source.run_id, graph.join_rules()).await
}

async fn enqueue_input(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &TaskRecord,
    plan: &Successor,
    scope_id: Option<i64>,
    input: &Value,
) -> Result<(), StorageError> {
    let existing = if plan.initial_status == TaskStatus::Collecting {
        sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=?1 AND name=?2 AND run_id IS ?3 AND scope_id IS ?4 AND status='collecting' ORDER BY task_id LIMIT 1"
        )).bind(&source.workflow_id).bind(plan.dst).bind(source.run_id).bind(scope_id)
          .fetch_optional(&mut **tx).await?
    } else {
        None
    };
    if let Some(existing) = existing {
        let mut parents = existing.parent_task_ids;
        if !parents.contains(&source.task_id) {
            parents.push(source.task_id);
        }
        let mut input_json = existing.input_json;
        merge_join_input(&mut input_json, input)?;
        sqlx::query("UPDATE tasks SET parent_task_ids=?3, input_json=?4, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE workflow_id=?1 AND task_id=?2")
            .bind(&source.workflow_id).bind(existing.task_id).bind(Json(parents)).bind(input_json)
            .execute(&mut **tx).await?;
    } else {
        sqlx::query("INSERT INTO tasks (workflow_id,run_id,name,status,parent_task_ids,input_json,scope_id) VALUES (?1,?2,?3,?4,?5,?6,?7)")
            .bind(&source.workflow_id).bind(source.run_id).bind(plan.dst).bind(plan.initial_status.as_str())
            .bind(Json([source.task_id])).bind(input).bind(scope_id)
            .execute(&mut **tx).await?;
    }

    Ok(())
}

/// 包括晚创建的 JOIN 和间接等待者；不把其他实例的失败传播过来。
async fn fail_blocked_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: Option<i64>,
    rules: &HashMap<String, JoinRule>,
) -> Result<(), StorageError> {
    loop {
        let changed = sqlx::query(&format!(
            "UPDATE tasks AS wt SET status='failed', error='upstream dependency failed', lease_until=NULL, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE wt.workflow_id=?1 AND wt.run_id IS ?2 AND wt.status='collecting' AND ({})", join::failed("?3")
        )).bind(workflow_id).bind(run_id).bind(Json(rules)).execute(&mut **tx).await?.rows_affected();
        if changed == 0 {
            break;
        }
    }
    Ok(())
}

impl SqliteTaskRepo {
    pub fn new(pool: SqlitePool, graph: Arc<dyn ErasedGraph>, worker_id: Option<Arc<str>>) -> Self {
        Self {
            pool,
            graph,
            worker_id,
        }
    }

    /// Settle only the current, unexpired claim and enqueue successors in the same commit.
    /// Successors inherit the current database run_id and scope_id, including business updates.
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
    ) -> Result<TaskRecord, StorageError> {
        check_workflow(self.graph.as_ref(), &task.workflow_id)?;
        let mut tx = self.pool.begin().await?;
        let settled = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            UPDATE tasks
            SET status = 'succeeded', output_json = ?3, error = NULL, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND worker_id IS ?5
              AND status = 'running' AND attempt = ?4
              AND attempt <= {MAX_ATTEMPTS}
              AND julianday(lease_until) > julianday('now')
            RETURNING {ROW_COLS}
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(output)
        .bind(task.attempt)
        .bind(self.worker_id.as_deref())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let settled = TaskRecord::from(settled);
        enqueue_fanouts_in_tx(&mut tx, self.graph.as_ref(), &settled, fanouts).await?;
        tx.commit().await?;
        Ok(settled)
    }

    /// Fence the source claim before generating failure successors or cascading failures.
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
    ) -> Result<(), StorageError> {
        check_workflow(self.graph.as_ref(), &task.workflow_id)?;
        let mut tx = self.pool.begin().await?;
        // Settle first so a failure self-edge creates a fresh pending task instead
        // of merging its retry input into the still-running source task.
        let settled = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            UPDATE tasks
            SET status = ?3, error = ?4, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND worker_id IS ?6
              AND status = 'running' AND attempt = ?5
              AND julianday(lease_until) > julianday('now')
            RETURNING {ROW_COLS}
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(if fanouts.is_empty() {
            TaskStatus::Failed.as_str()
        } else {
            TaskStatus::Superseded.as_str()
        })
        .bind(error)
        .bind(task.attempt)
        .bind(self.worker_id.as_deref())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let settled = TaskRecord::from(settled);
        enqueue_fanouts_in_tx(&mut tx, self.graph.as_ref(), &settled, fanouts).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Renew only a live claim without advancing its generation. Acquire the write lock
    /// before evaluating database time so a lease that expired while waiting stays expired.
    pub async fn renew_claim(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        if self.graph.workflow_id() != task.workflow_id {
            return Ok(None);
        }
        let mut tx = self.pool.begin().await?;
        let lease_until = sqlx::query_scalar(
            r#"
            UPDATE tasks
            SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND worker_id IS ?5
              AND attempt = ?3
              AND status = 'running' AND julianday(lease_until) > julianday('now')
            RETURNING lease_until
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(task.attempt)
        .bind(LEASE_DURATION.as_secs() as i64)
        .bind(self.worker_id.as_deref())
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(lease_until)
    }

    /// Release only this still-valid claim so another worker may reclaim the task.
    pub async fn release_claim(&self, task: &TaskRecord) -> Result<(), StorageError> {
        check_workflow(self.graph.as_ref(), &task.workflow_id)?;
        let result = sqlx::query(
            r#"
            UPDATE tasks SET lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND worker_id IS ?4
              AND status = 'running' AND attempt = ?3
              AND julianday(lease_until) > julianday('now')
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(task.attempt)
        .bind(self.worker_id.as_deref())
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
        check_workflow(self.graph.as_ref(), workflow_id)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let join_ready = join::ready("?4");
        let mut tx = self.pool.begin().await?;
        let mut rows = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            WITH candidate AS MATERIALIZED (
                SELECT wt.task_id FROM tasks wt
                WHERE wt.workflow_id = ?1
                  AND (wt.lease_until IS NULL OR julianday(wt.lease_until) <= julianday('now'))
                  AND (wt.status IN ('pending', 'running') OR (wt.status = 'collecting' AND ({join_ready})))
                ORDER BY wt.task_id LIMIT ?3
            )
            UPDATE tasks
            SET status = 'running', worker_id = ?5,
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
        .bind(Json(self.graph.join_rules()))
        .bind(self.worker_id.as_deref())
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
