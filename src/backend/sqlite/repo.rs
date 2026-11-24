//! SQLite persistence for workflow tasks, claims, settlement, and fanout.
//!
//! Each task transaction starts with INSERT or UPDATE. That statement acquires the
//! write lock before any reads used to plan fanout or merge JOIN contributions.

use std::{collections::HashMap, sync::Arc};

use super::{join, scope};
use crate::task::{MAX_ATTEMPTS, scope::ScopeEntries};
use serde_json::Value;
use sqlx::{SqlitePool, types::Json};

use super::rows::TaskRow;
#[cfg(test)]
use crate::backend::task_fixture::TaskSeed;
use crate::error::StorageError;
use crate::graph::{ErasedGraph, JoinRule, Successor};
use crate::node::TaskType;
use crate::{TaskRecord, TaskStatus};

use crate::task::store::{LEASE_DURATION, merge_join_input};

/// Columns suitable for application queries against the same task table.
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    lease_until, attempt, \
    input_json, output_json, error, created_at, updated_at";

/// One graph's task repository. Clones share its connection pool and graph.
#[derive(Clone)]
pub struct SqliteTaskRepo {
    pool: SqlitePool,
    graph: Arc<dyn ErasedGraph>,
    worker_id: Option<String>,
}

fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}

/// Implements the JOIN predecessor/lease gate in [`crate::task::ExecutionStore::claim_many`]
/// for both claim and recovery. Parent IDs record provenance, not execution dependencies.
fn executable_where(rules_parameter: &str) -> String {
    format!(
        "(wt.lease_until IS NULL OR julianday(wt.lease_until) <= julianday('now'))
         AND (wt.status IN ('pending', 'running') OR (wt.status = 'collecting' AND ({})))",
        join::ready(rules_parameter)
    )
}

/// The source settlement has already acquired the write lock in this transaction.
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &TaskRecord,
    graph: &dyn ErasedGraph,
    plan: &Successor,
    entries: &mut ScopeEntries,
) -> Result<(), StorageError> {
    let task_type = graph
        .task_type_of(plan.dst)
        .ok_or_else(|| StorageError::InvalidFanout {
            detail: format!("unknown destination node {}", plan.dst),
        })?;
    let destination_path =
        graph
            .subgraph_path(plan.dst)
            .ok_or_else(|| StorageError::InvalidFanout {
                detail: format!("unknown destination node {}", plan.dst),
            })?;
    if plan.new_scope_per_input && task_type == TaskType::Join {
        return Err(StorageError::InvalidFanout {
            detail: "foreach cannot target a JOIN node".into(),
        });
    }
    let initial_status = if task_type == TaskType::Join && source.status == TaskStatus::Succeeded {
        TaskStatus::Collecting
    } else {
        TaskStatus::Pending
    };
    for input in &plan.inputs {
        let scope_id = scope::route(
            tx,
            &source.workflow_id,
            source.run_id,
            source.scope_id,
            &destination_path,
            source.task_id,
            plan.new_scope_per_input,
            entries,
        )
        .await?;
        let existing = if initial_status == TaskStatus::Collecting {
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
                .bind(&source.workflow_id).bind(source.run_id).bind(plan.dst).bind(initial_status.as_str())
                .bind(Json([source.task_id])).bind(input).bind(scope_id)
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
    pub fn new(pool: SqlitePool, graph: Arc<dyn ErasedGraph>) -> Self {
        Self {
            pool,
            graph,
            worker_id: None,
        }
    }

    pub(crate) fn for_worker(
        pool: SqlitePool,
        graph: Arc<dyn ErasedGraph>,
        worker_id: &str,
    ) -> Self {
        Self {
            pool,
            graph,
            worker_id: Some(worker_id.to_owned()),
        }
    }

    fn check_workflow(&self, workflow_id: &str) -> Result<(), StorageError> {
        if workflow_id != self.graph.workflow_id() {
            return Err(StorageError::InvalidFanout {
                detail: format!(
                    "workflow {workflow_id} does not match repository graph {}",
                    self.graph.workflow_id()
                ),
            });
        }
        Ok(())
    }

    /// Insert a task in an application-owned transaction.
    /// Without an explicit run ID, the allocated task ID starts a new run.
    /// Insertion acquires the write lock before routing into the destination scope.
    #[cfg(test)]
    pub(super) async fn insert_task(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        seed: &TaskSeed,
    ) -> Result<TaskRecord, StorageError> {
        let mut task = seed.task.clone();
        self.check_workflow(&task.workflow_id)?;
        let task_type =
            self.graph
                .task_type_of(&task.name)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("unknown destination node {}", task.name),
                })?;
        task.subgraph_path =
            self.graph
                .subgraph_path(&task.name)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("unknown destination node {}", task.name),
                })?;
        let initial_status = if task_type == TaskType::Join
            && (seed.run_id.is_some() || !seed.parent_task_ids.is_empty())
        {
            TaskStatus::Collecting
        } else {
            TaskStatus::Pending
        };
        let row = super::store::insert_task(
            tx,
            &task,
            initial_status,
            seed.run_id,
            seed.scope_id,
            &seed.parent_task_ids,
        )
        .await?;
        if initial_status == TaskStatus::Collecting {
            fail_blocked_joins(tx, &task.workflow_id, row.run_id, self.graph.join_rules()).await?;
            let row = sqlx::query_as::<_, TaskRow>(&format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=?1 AND task_id=?2"
            ))
            .bind(&task.workflow_id)
            .bind(row.task_id)
            .fetch_one(&mut **tx)
            .await?;
            return Ok(row.into());
        }
        Ok(row)
    }

    /// Enqueue in the supplied run, or atomically create a run using the new task ID.
    #[cfg(test)]
    pub(crate) async fn enqueue(&self, seed: &TaskSeed) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = self.insert_task(&mut tx, seed).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// Settle only the current, unexpired claim and enqueue successors in the same commit.
    /// Successors inherit the current database run_id and scope_id, including business updates.
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
    ) -> Result<TaskRecord, StorageError> {
        self.check_workflow(&task.workflow_id)?;
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
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &TaskRecord::from(settled.clone()),
                self.graph.as_ref(),
                fanout,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(
            &mut tx,
            &task.workflow_id,
            settled.run_id,
            self.graph.join_rules(),
        )
        .await?;
        tx.commit().await?;
        Ok(settled.into())
    }

    /// Fence the source claim before generating failure successors or cascading failures.
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
    ) -> Result<(), StorageError> {
        self.check_workflow(&task.workflow_id)?;
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
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &TaskRecord::from(settled.clone()),
                self.graph.as_ref(),
                fanout,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(
            &mut tx,
            &task.workflow_id,
            settled.run_id,
            self.graph.join_rules(),
        )
        .await?;
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
        self.check_workflow(&task.workflow_id)?;
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
        self.check_workflow(workflow_id)?;
        let executable_where = executable_where("?4");
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await?;
        let mut rows = sqlx::query_as::<_, TaskRow>(&format!(
            r#"
            WITH candidate AS MATERIALIZED (
                SELECT wt.task_id FROM tasks wt
                WHERE wt.workflow_id = ?1 AND {executable_where}
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

    pub async fn list_all(&self, workflow_id: &str) -> Result<Vec<TaskRecord>, StorageError> {
        let rows = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 ORDER BY task_id"
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    /// Whether this workflow has claimable execution or final failure settlement work.
    pub async fn executable_workflow_ids(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<String>, StorageError> {
        self.check_workflow(workflow_id)?;
        let executable_where = executable_where("?2");
        Ok(sqlx::query_scalar(&format!(
            r#"
            SELECT DISTINCT wt.workflow_id FROM tasks wt
            WHERE wt.workflow_id = ?1 AND {executable_where}
            ORDER BY wt.workflow_id
            "#
        ))
        .bind(workflow_id)
        .bind(Json(self.graph.join_rules()))
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
        self.succeed_and_enqueue(task, output, &[]).await
    }

    pub async fn fail(&self, task: &TaskRecord, error: &str) -> Result<(), StorageError> {
        self.fail_and_enqueue(task, error, &[]).await
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
        self.check_workflow(&task.workflow_id)?;
        let result = sqlx::query(
            r#"
            UPDATE tasks SET status = ?3, error = ?4, lease_until = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE workflow_id = ?1 AND task_id = ?2
              AND worker_id IS ?7
              AND status = 'running' AND attempt = ?5
              AND (attempt <= ?6 OR ?3 IN ('failed', 'superseded'))
              AND julianday(lease_until) > julianday('now')
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(status)
        .bind(error)
        .bind(task.attempt)
        .bind(MAX_ATTEMPTS)
        .bind(self.worker_id.as_deref())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }
        Ok(())
    }
}
