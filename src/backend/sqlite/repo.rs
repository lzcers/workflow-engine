//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Read/modify/write operations acquire SQLite's write lock with `BEGIN IMMEDIATE`
//! before reading. This serializes competing claimers and fan-in updates without
//! relying on PostgreSQL row locks or upgrading a deferred read transaction.

use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::task::FanoutInstances;
use crate::{Task, TaskRecord, TaskStatus};

use crate::task::store::{LEASE_DURATION, merge_join_input};
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted (attempt >= max_attempts)";

/// Columns suitable for application queries against the same task table.
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, producer_task_id, name, instance_id, status, \
    claimed_by, lease_until, attempt, max_attempts, task_type, depends_on_ids, depends_on_names, \
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

/// Implements the dependency/lease gate in [`crate::task::TaskStore::claim_many`]
/// for both claim and recovery. Iterating dependency IDs also blocks missing tasks.
const EXECUTABLE_WHERE: &str = r#"
    wt.status IN ('pending', 'running')
    AND (wt.lease_until IS NULL OR julianday(wt.lease_until) <= julianday('now'))
    AND NOT EXISTS (
        SELECT 1 FROM json_each(wt.depends_on_ids) AS dependency
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks dep
            WHERE dep.workflow_id = wt.workflow_id
              AND dep.task_id = dependency.value
              AND dep.status = 'succeeded'
        )
    )
    AND (
        wt.task_type <> 'join'
        OR NOT EXISTS (
            SELECT 1 FROM json_each(wt.depends_on_names) AS predecessor
            WHERE NOT (
                EXISTS (
                    SELECT 1 FROM tasks st
                    WHERE st.workflow_id = wt.workflow_id
                      AND st.run_id IS wt.run_id
                      AND st.name = predecessor.value
                      AND st.status = 'succeeded'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM tasks st
                    WHERE st.workflow_id = wt.workflow_id
                      AND st.run_id IS wt.run_id
                      AND st.name = predecessor.value
                      AND st.status <> 'succeeded'
                )
            )
        )
    )
"#;

/// The caller must already hold the write lock, normally via `BEGIN IMMEDIATE`.
#[allow(clippy::too_many_arguments)]
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: Option<i64>,
    producer_task_id: Option<i64>,
    name: &str,
    instances: &[(Option<i32>, Value)],
    depends_on_ids: &[i64],
    task_type: &str,
    depends_on_names: &[String],
    max_attempts: i32,
) -> Result<Vec<TaskRecord>, StorageError> {
    let mut rows = Vec::with_capacity(instances.len());
    for (instance_id, input) in instances {
        if let Some(instance_id) = instance_id {
            let task_id = allocate_task_id(tx, workflow_id).await?;
            let row = sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                INSERT INTO tasks
                    (workflow_id, task_id, run_id, name, instance_id, status,
                     attempt, max_attempts, task_type, depends_on_ids, depends_on_names, input_json,
                     producer_task_id)
                VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT DO NOTHING
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(task_id)
            .bind(run_id)
            .bind(name)
            .bind(*instance_id)
            .bind(max_attempts)
            .bind(task_type)
            .bind(Json(depends_on_ids))
            .bind(Json(depends_on_names))
            .bind(input)
            .bind(producer_task_id)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some(row) = row {
                rows.push(row.into());
            }
            continue;
        }

        let existing = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            SELECT {ROW_COLS} FROM tasks
            WHERE workflow_id = ?1 AND name = ?2 AND run_id IS ?3
              AND instance_id IS NULL AND status IN ('pending', 'running')
            ORDER BY task_id LIMIT 1
            "#
        ))
        .bind(workflow_id)
        .bind(name)
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?;

        let row = if let Some(existing) = existing {
            let mut dependencies = existing.depends_on_ids;
            for &dependency in depends_on_ids {
                if !dependencies.contains(&dependency) {
                    dependencies.push(dependency);
                }
            }
            let mut merged_input = existing.input_json;
            if task_type == "join" {
                merge_join_input(&mut merged_input, input);
            }
            sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                UPDATE tasks
                SET depends_on_ids = ?3, input_json = ?4,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(existing.task_id)
            .bind(Json(&dependencies))
            .bind(merged_input)
            .fetch_one(&mut **tx)
            .await?
        } else {
            let failed_predecessor = if task_type == "join" {
                sqlx::query_scalar::<_, bool>(
                    r#"
                    SELECT EXISTS (
                        SELECT 1 FROM tasks
                        WHERE workflow_id = ?1 AND run_id IS ?2
                          AND name IN (SELECT value FROM json_each(?3))
                          AND status = 'failed'
                    )
                    "#,
                )
                .bind(workflow_id)
                .bind(run_id)
                .bind(Json(depends_on_names))
                .fetch_one(&mut **tx)
                .await?
            } else {
                false
            };
            let task_id = allocate_task_id(tx, workflow_id).await?;
            sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                INSERT INTO tasks
                    (workflow_id, task_id, run_id, name, instance_id, status,
                     attempt, max_attempts, task_type, depends_on_ids, depends_on_names, input_json,
                     producer_task_id)
                VALUES (?1, ?2, ?3, ?4, NULL, ?5, 0, ?6, ?7, ?8, ?9, ?10, ?11)
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(task_id)
            .bind(run_id)
            .bind(name)
            .bind(if failed_predecessor {
                TaskStatus::Failed.as_str()
            } else {
                TaskStatus::Pending.as_str()
            })
            .bind(max_attempts)
            .bind(task_type)
            .bind(Json(depends_on_ids))
            .bind(Json(depends_on_names))
            .bind(input)
            .bind(producer_task_id)
            .fetch_one(&mut **tx)
            .await?
        };
        rows.push(row.into());
    }
    Ok(rows)
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
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            INSERT INTO tasks
                (workflow_id, task_id, run_id, name, instance_id, status,
                 attempt, max_attempts, depends_on_ids, depends_on_names, input_json,
                 producer_task_id)
            VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?7, ?8, ?9, ?10)
            RETURNING {ROW_COLS}
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task_id)
        .bind(task.run_id.unwrap_or(task_id))
        .bind(&task.name)
        .bind(task.instance_id)
        .bind(task.max_attempts)
        .bind(Json(&task.depends_on_ids))
        .bind(Json(&task.depends_on_names))
        .bind(&task.input)
        .bind(task.producer_task_id)
        .fetch_one(&mut **tx)
        .await?;
        Ok(row.into())
    }

    /// Enqueue in the supplied run, or atomically create a run using the new task ID.
    pub async fn enqueue(&self, task: &Task) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = Self::insert_task(&mut tx, task).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// Enqueue all instances atomically. Single instances merge dependencies (and
    /// JOIN input) across producers, preserving the initial producer. Numbered map
    /// instances are deduplicated within each producer's output to the destination.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_fanout(
        &self,
        workflow_id: &str,
        run_id: Option<i64>,
        producer_task_id: Option<i64>,
        name: &str,
        instances: &[(Option<i32>, Value)],
        depends_on_ids: &[i64],
        task_type: &str,
        depends_on_names: &[String],
        max_attempts: i32,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let rows = enqueue_fanout_in_tx(
            &mut tx,
            workflow_id,
            run_id,
            producer_task_id,
            name,
            instances,
            depends_on_ids,
            task_type,
            depends_on_names,
            max_attempts,
        )
        .await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// Settle only the current, unexpired claim and enqueue successors in the same commit.
    /// Successors inherit the current database run_id, including business updates.
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[FanoutInstances],
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
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &task.workflow_id,
                settled.run_id,
                Some(task.task_id),
                &fanout.name,
                &fanout.instances,
                &[settled.task_id],
                &fanout.task_type,
                &fanout.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(settled.into())
    }

    /// Fence the source claim before generating failure successors or cascading failures.
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[FanoutInstances],
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
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &task.workflow_id,
                settled.run_id,
                Some(task.task_id),
                &fanout.name,
                &fanout.instances,
                &[],
                &fanout.task_type,
                &fanout.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        if fanouts.is_empty() {
            sqlx::query(
                r#"
                UPDATE tasks
                SET status = 'failed', error = ?3, lease_until = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND status = 'pending'
                  AND EXISTS (
                      SELECT 1 FROM tasks source
                      WHERE source.workflow_id = tasks.workflow_id AND source.task_id = ?2
                        AND source.run_id IS tasks.run_id
                        AND (source.task_id IN (SELECT value FROM json_each(tasks.depends_on_ids))
                             OR (tasks.task_type = 'join'
                                 AND source.name IN (SELECT value FROM json_each(tasks.depends_on_names))))
                  )
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(format!("predecessor task {} failed", task.task_id))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
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

    /// Claim a bounded batch in one workflow, including expired running tasks. The write transaction
    /// serializes competing workers; each successful claim consumes one attempt.
    pub async fn claim_many(
        &self,
        workflow_id: &str,
        worker: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(&format!(
            r#"
            UPDATE tasks AS wt
            SET status = 'failed', error = ?2, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE wt.workflow_id = ?1 AND {EXECUTABLE_WHERE}
              AND wt.attempt >= wt.max_attempts
            "#
        ))
        .bind(workflow_id)
        .bind(BUDGET_EXHAUSTED_ERR)
        .execute(&mut *tx)
        .await?;

        let mut rows = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            WITH candidate AS MATERIALIZED (
                SELECT wt.task_id FROM tasks wt
                WHERE wt.workflow_id = ?1 AND {EXECUTABLE_WHERE}
                  AND wt.attempt < wt.max_attempts
                ORDER BY wt.task_id LIMIT ?4
            )
            UPDATE tasks
            SET status = 'running', claimed_by = ?2,
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

    /// Workflows with claimable tasks, for the backend's periodic recovery scan.
    pub async fn executable_workflow_ids(&self) -> Result<Vec<String>, StorageError> {
        Ok(sqlx::query_scalar(&format!(
            r#"
            SELECT DISTINCT wt.workflow_id FROM tasks wt
            WHERE {EXECUTABLE_WHERE} AND wt.attempt < wt.max_attempts
            ORDER BY wt.workflow_id
            "#
        ))
        .fetch_all(&self.pool)
        .await?)
    }

    /// Seal exhausted runnable tasks across workflows, including workflows that
    /// cannot appear in recovery scans because none of their tasks have budget.
    pub async fn seal_budget_exhausted(&self) -> Result<u64, StorageError> {
        Ok(sqlx::query(&format!(
            r#"
            UPDATE tasks AS wt
            SET status = 'failed', error = ?1, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE {EXECUTABLE_WHERE} AND wt.attempt >= wt.max_attempts
            "#
        ))
        .bind(BUDGET_EXHAUSTED_ERR)
        .execute(&self.pool)
        .await?
        .rows_affected())
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
