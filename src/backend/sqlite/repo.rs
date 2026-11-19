//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Read/modify/write operations acquire SQLite's write lock with `BEGIN IMMEDIATE`
//! before reading. This serializes competing claimers and fan-in updates without
//! relying on PostgreSQL row locks or upgrading a deferred read transaction.

use super::{join, scope};
use crate::task::{
    JoinState,
    scope::{ScopeEntries, join_state},
};
use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::task::FanoutPlan;
use crate::{Task, TaskRecord, TaskStatus};

use crate::task::store::{LEASE_DURATION, merge_join_input};

/// Columns suitable for application queries against the same task table.
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    claimed_by, lease_until, attempt, max_attempts, task_type, join_state, \
    input_json, output_json, error, created_at, updated_at";

/// Workflow task repository. Clones share the same connection pool.
#[derive(Clone)]
pub struct SqliteTaskRepo {
    pool: SqlitePool,
}

/// Allocate a workflow-local task ID inside the caller's transaction.
/// Allocation is rolled back with the transaction.
pub async fn allocate_task_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
) -> Result<i64, StorageError> {
    Ok(sqlx::query_scalar(
        r#"
        INSERT INTO task_counters (workflow_id, next_task_id)
        VALUES (?1, 1)
        ON CONFLICT (workflow_id)
        DO UPDATE SET next_task_id = task_counters.next_task_id + 1
        RETURNING next_task_id
        "#,
    )
    .bind(workflow_id)
    .fetch_one(&mut **tx)
    .await?)
}

fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}

/// Implements the JOIN predecessor/lease gate in [`crate::task::TaskStore::claim_many`]
/// for both claim and recovery. Parent IDs record provenance, not execution dependencies.
fn executable_where() -> String {
    format!(
        "wt.status IN ('pending', 'running') AND (wt.lease_until IS NULL OR julianday(wt.lease_until) <= julianday('now')) AND ({})",
        join::ready()
    )
}

/// The caller must already hold the write lock, normally via `BEGIN IMMEDIATE`.
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &TaskRecord,
    plan: &FanoutPlan,
    max_attempts: i32,
    entries: &mut ScopeEntries,
) -> Result<(), StorageError> {
    if plan.foreach && plan.task_type == "join" {
        return Err(StorageError::InvalidFanout {
            detail: "foreach cannot target a JOIN node".into(),
        });
    }
    for (index, input) in plan.inputs.iter().enumerate() {
        let scope_id = scope::route(
            tx,
            &source.workflow_id,
            source.run_id,
            source.scope_id,
            &plan.source_subgraph_path,
            &plan.destination_subgraph_path,
            source.task_id,
            plan.foreach.then(|| (plan.edge_id.clone(), index)),
            entries,
        )
        .await?;
        let existing = if plan.task_type == "join" {
            sqlx::query_as::<_, TaskRow>(&format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=?1 AND name=?2 AND run_id IS ?3 AND scope_id IS ?4 AND task_type='join' AND status IN ('pending','running') ORDER BY task_id LIMIT 1"
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
            merge_join_input(&mut input_json, input);
            let mut state = existing.join_state;
            state.arrive(&plan.edge_id, source.task_id);
            sqlx::query("UPDATE tasks SET parent_task_ids=?3, input_json=?4, join_state=?5, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE workflow_id=?1 AND task_id=?2")
                .bind(&source.workflow_id).bind(existing.task_id).bind(Json(parents)).bind(input_json).bind(Json(state))
                .execute(&mut **tx).await?;
        } else {
            let mut state = if plan.task_type == "join" {
                let chain = scope::chain(tx, &source.workflow_id, scope_id).await?;
                let mut entry_names = std::collections::HashMap::new();
                for scope in &chain {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        entry_names.entry(scope.trigger_task_id)
                    {
                        let name: String = sqlx::query_scalar(
                            "SELECT name FROM tasks WHERE workflow_id=?1 AND task_id=?2",
                        )
                        .bind(&source.workflow_id)
                        .bind(scope.trigger_task_id)
                        .fetch_one(&mut **tx)
                        .await?;
                        entry.insert(name);
                    }
                }
                join_state(plan, &chain, &entry_names)?
            } else {
                JoinState::default()
            };
            if plan.task_type == "join" {
                state.arrive(&plan.edge_id, source.task_id);
            }
            let task_id = allocate_task_id(tx, &source.workflow_id).await?;
            sqlx::query("INSERT INTO tasks (workflow_id,task_id,run_id,name,status,attempt,max_attempts,task_type,parent_task_ids,join_state,input_json,scope_id) VALUES (?1,?2,?3,?4,'pending',0,?5,?6,?7,?8,?9,?10)")
                .bind(&source.workflow_id).bind(task_id).bind(source.run_id).bind(plan.dst)
                .bind(max_attempts).bind(plan.task_type).bind(Json(vec![source.task_id])).bind(Json(state)).bind(input).bind(scope_id)
                .execute(&mut **tx).await?;
        }
    }

    Ok(())
}

/// 包括晚创建的 JOIN 和间接等待者；不把其他实例的失败传播过来。
async fn fail_blocked_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: Option<i64>,
) -> Result<(), StorageError> {
    loop {
        let changed = sqlx::query(&format!(
            "UPDATE tasks AS wt SET status='failed', error='upstream dependency failed', lease_until=NULL, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE wt.workflow_id=?1 AND wt.run_id IS ?2 AND wt.status='pending' AND ({})", join::failed()
        )).bind(workflow_id).bind(run_id).execute(&mut **tx).await?.rows_affected();
        if changed == 0 {
            break;
        }
    }
    Ok(())
}

impl SqliteTaskRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert a pending task in an application-owned transaction.
    /// Without an explicit run ID, the allocated task ID starts a new run.
    /// Start with `pool.begin_with("BEGIN IMMEDIATE")` when the transaction also reads.
    pub async fn insert_task(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task: &Task,
    ) -> Result<TaskRecord, StorageError> {
        let task_id = allocate_task_id(tx, &task.workflow_id).await?;
        let run_id = Some(task.run_id.unwrap_or(task_id));
        let source_path: Vec<_> = scope::chain(tx, &task.workflow_id, task.scope_id)
            .await?
            .into_iter()
            .map(|scope| scope.subgraph_id)
            .collect();
        let scope_id = scope::route(
            tx,
            &task.workflow_id,
            run_id,
            task.scope_id,
            &source_path,
            &task.subgraph_path,
            task_id,
            None,
            &mut ScopeEntries::new(),
        )
        .await?;
        let state = JoinState::from_names(&task.depends_on_names, scope_id);
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            "INSERT INTO tasks (workflow_id,task_id,run_id,name,status,attempt,max_attempts,parent_task_ids,join_state,input_json,task_type,scope_id) VALUES (?1,?2,?3,?4,'pending',0,?5,?6,?7,?8,?9,?10) RETURNING {ROW_COLS}"
        )).bind(&task.workflow_id).bind(task_id).bind(run_id).bind(&task.name).bind(task.max_attempts)
          .bind(Json(&task.parent_task_ids)).bind(Json(state)).bind(&task.input).bind(&task.task_type).bind(scope_id)
          .fetch_one(&mut **tx).await?;
        if task.task_type == "join" {
            fail_blocked_joins(tx, &task.workflow_id, run_id).await?;
            let row = sqlx::query_as::<_, TaskRow>(&format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=?1 AND task_id=?2"
            ))
            .bind(&task.workflow_id)
            .bind(task_id)
            .fetch_one(&mut **tx)
            .await?;
            return Ok(row.into());
        }
        Ok(row.into())
    }

    /// Enqueue in the supplied run, or atomically create a run using the new task ID.
    pub async fn enqueue(&self, task: &Task) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = Self::insert_task(&mut tx, task).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// Settle only the current, unexpired claim and enqueue successors in the same commit.
    /// Successors inherit the current database run_id and scope_id, including business updates.
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[FanoutPlan],
        max_attempts: i32,
    ) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let settled = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            UPDATE tasks
            SET status = 'succeeded', output_json = ?3, error = NULL, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND status = 'running' AND claimed_by = ?4 AND attempt = ?5
              AND attempt <= max_attempts
              AND julianday(lease_until) > julianday('now')
            RETURNING {ROW_COLS}
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(output)
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &TaskRecord::from(settled.clone()),
                fanout,
                max_attempts,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(&mut tx, &task.workflow_id, settled.run_id).await?;
        tx.commit().await?;
        Ok(settled.into())
    }

    /// Fence the source claim before generating failure successors or cascading failures.
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[FanoutPlan],
        max_attempts: i32,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        // Settle first so a failure self-edge creates a fresh pending task instead
        // of merging its retry input into the still-running source task.
        let settled = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            UPDATE tasks
            SET status = ?3, error = ?4, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND status = 'running' AND claimed_by = ?5 AND attempt = ?6
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
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &TaskRecord::from(settled.clone()),
                fanout,
                max_attempts,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(&mut tx, &task.workflow_id, settled.run_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Renew only a live claim without advancing its generation. Acquire the write lock
    /// before evaluating database time so a lease that expired while waiting stays expired.
    pub async fn renew_claim(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let lease_until = sqlx::query_scalar(
            r#"
            UPDATE tasks
            SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND claimed_by = ?3 AND attempt = ?4
              AND status = 'running' AND julianday(lease_until) > julianday('now')
            RETURNING lease_until
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(&task.claimed_by)
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
              AND status = 'running' AND claimed_by = ?3 AND attempt = ?4
              AND julianday(lease_until) > julianday('now')
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }
        Ok(())
    }

    /// Release leases belonging to a stopped worker in this workflow.
    /// Stop and await that worker first; its old claim snapshots become invalid.
    pub async fn release_claims(
        &self,
        workflow_id: &str,
        worker_id: &str,
    ) -> Result<u64, StorageError> {
        Ok(sqlx::query(
            r#"
            UPDATE tasks SET lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND claimed_by = ?2
              AND status = 'running' AND lease_until IS NOT NULL
            "#,
        )
        .bind(workflow_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Claim a bounded batch, including expired tasks that need final failure settlement.
    /// Each claim advances the fencing generation. Claims above max_attempts may only settle failure;
    /// they never execute the node again. The write transaction serializes competing workers.
    pub async fn claim_many(
        &self,
        workflow_id: &str,
        worker: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        let executable_where = executable_where();
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut rows = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            WITH candidate AS MATERIALIZED (
                SELECT wt.task_id FROM tasks wt
                WHERE wt.workflow_id = ?1 AND {executable_where}
                ORDER BY wt.task_id LIMIT ?4
            )
            UPDATE tasks
            SET status = 'running', claimed_by = ?2,
                error = CASE WHEN attempt >= max_attempts THEN COALESCE(error,
                    CASE WHEN lease_until IS NOT NULL
                         THEN 'task lease expired; retry budget exhausted'
                         ELSE 'task retry budget exhausted' END)
                    ELSE error END,
                lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds'),
                attempt = attempt + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id IN (SELECT task_id FROM candidate)
            RETURNING {ROW_COLS}
            "#
        ))
        .bind(workflow_id)
        .bind(worker)
        .bind(LEASE_DURATION.as_secs() as i64)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.sort_unstable_by_key(|row| row.task_id);
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    pub async fn list_all(&self, workflow_id: &str) -> Result<Vec<TaskRecord>, StorageError> {
        let rows = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 ORDER BY task_id"
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    /// Workflows with claimable execution or final failure settlement work.
    pub async fn executable_workflow_ids(&self) -> Result<Vec<String>, StorageError> {
        let executable_where = executable_where();
        Ok(sqlx::query_scalar(&format!(
            r#"
            SELECT DISTINCT wt.workflow_id FROM tasks wt
            WHERE {executable_where}
            ORDER BY wt.workflow_id
            "#
        ))
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get(
        &self,
        workflow_id: &str,
        task_id: i64,
    ) -> Result<Option<TaskRecord>, StorageError> {
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 AND task_id = ?2"
        ))
        .bind(workflow_id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(TaskRecord::from))
    }

    /// Complete the current claim without creating successors.
    pub async fn succeed(
        &self,
        task: &TaskRecord,
        output: &Value,
    ) -> Result<TaskRecord, StorageError> {
        self.succeed_and_enqueue(task, output, &[], task.max_attempts)
            .await
    }

    pub async fn fail(&self, task: &TaskRecord, error: &str) -> Result<(), StorageError> {
        self.set_terminal(task, TaskStatus::Failed.as_str(), error)
            .await
    }

    pub async fn supersede(&self, task: &TaskRecord, error: &str) -> Result<(), StorageError> {
        self.set_terminal(task, TaskStatus::Superseded.as_str(), error)
            .await
    }

    pub async fn skip(&self, task: &TaskRecord, reason: &str) -> Result<(), StorageError> {
        self.set_terminal(task, TaskStatus::Skipped.as_str(), reason)
            .await
    }

    async fn set_terminal(
        &self,
        task: &TaskRecord,
        status: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            r#"
            UPDATE tasks SET status = ?3, error = ?4, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND status = 'running' AND claimed_by = ?5 AND attempt = ?6
              AND (attempt <= max_attempts OR ?3 IN ('failed', 'superseded'))
              AND julianday(lease_until) > julianday('now')
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(status)
        .bind(error)
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }
        Ok(())
    }
}
