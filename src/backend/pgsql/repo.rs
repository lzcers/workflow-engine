//! 通用工作流任务 `tasks` 的 PostgreSQL 持久化实现（入队 / 认领 / 续租 / 退避重试 / 原子结算）。

use std::{collections::HashMap, time::Duration};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::PgPool;

use super::{rows::TaskRow, scope};
use crate::error::StorageError;
use crate::task::{
    Dispatch, JoinRule, MAX_ATTEMPTS, SettlementRules, Successor, TaskStore,
    join::{self, RunScope, RunTask},
    scope::{ScopeEntries, route_path},
    store::{FanoutSource, LEASE_DURATION, merge_join_inputs},
};
use crate::{Database, RunHandle, Task, TaskRecord, TaskStatus};

/// 最终失败恢复认领保存的原因，重复恢复时保留第一次记录。
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted";
const LEASE_EXPIRED_ERR: &str = "task lease expired; retry budget exhausted";

pub(crate) const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
     lease_until, attempt, \
     input_json, output_json, error, created_at, updated_at";

impl Database<&mut sqlx::Transaction<'_, sqlx::Postgres>> {
    /// 在宿主事务内创建独立运行，同时建立入口所属的子图作用域。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        let tx = self.0;
        let run_id: i64 =
            sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('tasks', 'task_id'))")
                .fetch_one(&mut **tx)
                .await?;
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
        sqlx::query(
            "INSERT INTO tasks (workflow_id, task_id, run_id, name, status, input_json, scope_id)
             VALUES ($1, $2, $2, $3, 'pending', $4, $5)",
        )
        .bind(&task.workflow_id)
        .bind(run_id)
        .bind(task.name)
        .bind(task.input)
        .bind(scope_id)
        .execute(&mut **tx)
        .await?;
        Ok(RunHandle {
            workflow_id: task.workflow_id,
            run_id,
        })
    }
}

/// 同一次运行的结算与汇合使用同一事务锁，包含尚不存在的 JOIN 行。
/// 必须在任务行锁之前获取；节点业务执行不持有此锁。
async fn lock_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        SELECT pg_advisory_xact_lock(hashtextextended(
            jsonb_build_array('tasks'::regclass::oid, $1::text, $2::bigint)::text, 0
        ))
        "#,
    )
    .bind(workflow_id)
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}

/// 仅当前有效认领可以提交终态。持有行锁后再检查租约，返回最新身份供后继使用。
async fn settle_in_tx<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task: &'a TaskRecord,
    status: TaskStatus,
    output: Option<&Value>,
    error: Option<&str>,
) -> Result<FanoutSource<'a>, StorageError> {
    let (run_id, scope_id) = sqlx::query_as(&format!(
        r#"
        UPDATE tasks
        SET status = $4, output_json = $5, error = $6,
            lease_until = NULL, available_at = NULL, updated_at = clock_timestamp()
        WHERE workflow_id = $1 AND task_id = $2
          AND attempt = $3
          AND status = 'running' AND lease_until > clock_timestamp()
          AND ($4 IN ('failed', 'superseded') OR attempt <= {MAX_ATTEMPTS})
        RETURNING run_id, scope_id
        "#
    ))
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .bind(task.attempt)
    .bind(status.as_str())
    .bind(output)
    .bind(error)
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

/// 事务内按子图边界生成后继，JOIN 按运行、实例和节点名累积。
/// 含跨任务结算规则的图在进入此处前获取运行锁，覆盖 JOIN 首次创建的窗口。
async fn enqueue_fanouts_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
) -> Result<(), StorageError> {
    let existing = sqlx::query_as::<_, (i64, Vec<i64>, Value)>(
        "SELECT task_id, parent_task_ids, input_json FROM tasks
         WHERE workflow_id=$1 AND name=$2 AND run_id=$3
           AND scope_id IS NOT DISTINCT FROM $4 AND status='collecting'
         ORDER BY task_id LIMIT 1 FOR UPDATE",
    )
    .bind(source.workflow_id)
    .bind(plan.dst)
    .bind(source.run_id)
    .bind(scope_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((id, mut parents, input)) = existing {
        if !parents.contains(&source.task_id) {
            parents.push(source.task_id);
        }
        let merged = merge_join_inputs(Some(input), &plan.inputs)?;
        sqlx::query(
            "UPDATE tasks SET parent_task_ids=$3, input_json=$4, updated_at=clock_timestamp()
             WHERE workflow_id=$1 AND task_id=$2",
        )
        .bind(source.workflow_id)
        .bind(id)
        .bind(parents)
        .bind(merged)
        .execute(&mut **tx)
        .await?;
    } else {
        let merged = merge_join_inputs(None, &plan.inputs)?;
        insert_task(
            tx,
            source,
            plan.dst,
            TaskStatus::Collecting,
            scope_id,
            &merged,
        )
        .await?;
    }
    Ok(())
}

async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &FanoutSource<'_>,
    dst: &str,
    status: TaskStatus,
    scope_id: Option<i64>,
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (workflow_id,run_id,name,status,parent_task_ids,input_json,scope_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(source.workflow_id)
    .bind(source.run_id)
    .bind(dst)
    .bind(status.as_str())
    .bind([source.task_id].as_slice())
    .bind(input)
    .bind(scope_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// 写完本次所有后继后，读取当前运行轻量状态快照，通过共享纯 Rust 规则推进 JOIN 状态。
async fn advance_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    rules: &HashMap<String, JoinRule>,
) -> Result<(), StorageError> {
    if rules.is_empty() {
        return Ok(());
    }
    let tasks = sqlx::query_as::<_, RunTask>(
        "SELECT task_id, name, status, scope_id FROM tasks WHERE workflow_id = $1 AND run_id = $2",
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
         WHERE workflow_id = $1 AND run_id = $2",
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
                 lease_until = NULL, updated_at = clock_timestamp()
             WHERE workflow_id = $1 AND task_id = ANY($2)",
        )
        .bind(workflow_id)
        .bind(&transitions.failed)
        .execute(&mut **tx)
        .await?;
    }
    if !transitions.ready.is_empty() {
        sqlx::query(
            "UPDATE tasks
             SET status = 'pending', updated_at = clock_timestamp()
             WHERE workflow_id = $1 AND task_id = ANY($2)",
        )
        .bind(workflow_id)
        .bind(&transitions.ready)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// 共享连接池上的任务仓储。
#[derive(Clone)]
pub struct PgTaskRepo {
    pub(super) pool: PgPool,
}

impl PgTaskRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 先等行锁，再在后续写语句中检查数据库当前时间，避免等锁期间租约已过期。
    async fn lock_task(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &TaskRecord,
    ) -> Result<i64, StorageError> {
        sqlx::query_scalar(
            "SELECT run_id FROM tasks
             WHERE workflow_id = $1 AND task_id = $2
             FOR UPDATE",
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| claim_lost(task))
    }

    /// 无跨任务规则时只锁当前任务；否则必须按最新的运行身份先锁运行，再锁任务。
    async fn lock_settlement(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &TaskRecord,
        rules: &SettlementRules,
    ) -> Result<(), StorageError> {
        if !rules.needs_run_lock() {
            Self::lock_task(tx, task).await?;
            return Ok(());
        }

        let run_id = sqlx::query_scalar::<_, i64>(
            "SELECT run_id FROM tasks WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        lock_run(tx, &task.workflow_id, run_id).await?;
        let current_run = Self::lock_task(tx, task).await?;
        if current_run != run_id {
            return Err(claim_lost(task));
        }
        Ok(())
    }

    async fn settle_success_impl(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
        rules: &SettlementRules,
    ) -> Result<(), StorageError> {
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        Self::lock_settlement(&mut tx, task, rules).await?;
        let settled =
            settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
        enqueue_fanouts_in_tx(&mut tx, &settled, fanouts, rules).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn settle_failure_impl(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
        rules: &SettlementRules,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        Self::lock_settlement(&mut tx, task, rules).await?;
        let status = if fanouts.iter().all(|plan| plan.inputs.is_empty()) {
            TaskStatus::Failed
        } else {
            TaskStatus::Superseded
        };
        let settled = settle_in_tx(&mut tx, task, status, None, Some(error)).await?;
        enqueue_fanouts_in_tx(&mut tx, &settled, fanouts, rules).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn schedule_retry_impl(
        &self,
        task: &TaskRecord,
        error: &str,
        delay: Duration,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        Self::lock_task(&mut tx, task).await?;
        let result = sqlx::query(&format!(
            r#"
            UPDATE tasks
            SET status = 'pending', error = $4, lease_until = NULL,
                available_at = clock_timestamp() + make_interval(secs => $5),
                updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND attempt = $3
              AND status = 'running' AND attempt < {MAX_ATTEMPTS}
              AND lease_until > clock_timestamp()
            "#
        ))
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(task.attempt)
        .bind(error)
        .bind(delay.as_secs_f64())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }
        tx.commit().await?;
        Ok(())
    }

    async fn renew_claim_impl(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let mut tx = self.pool.begin().await?;
        match Self::lock_task(&mut tx, task).await {
            Ok(_) => {}
            Err(StorageError::ClaimLost { .. }) => return Ok(None),
            Err(error) => return Err(error),
        }
        let lease_until = sqlx::query_scalar(
            r#"
            UPDATE tasks
            SET lease_until = clock_timestamp() + make_interval(secs => $4),
                updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND attempt = $3
              AND status = 'running' AND lease_until > clock_timestamp()
            RETURNING lease_until
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(task.attempt)
        .bind(LEASE_DURATION.as_secs() as f64)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(lease_until)
    }

    async fn release_claim_impl(&self, task: &TaskRecord) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        Self::lock_task(&mut tx, task).await?;
        let result = sqlx::query(
            r#"
            UPDATE tasks SET lease_until = NULL, updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND attempt = $3
              AND status = 'running' AND lease_until > clock_timestamp()
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(task.attempt)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }

        tx.commit().await?;
        Ok(())
    }

    async fn claim_many_impl(
        &self,
        workflow_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await?;

        let sql = format!(
            r#"
            WITH candidate AS (
                SELECT wt.task_id
                FROM tasks wt
                WHERE wt.workflow_id = $1
                  AND (wt.lease_until IS NULL OR wt.lease_until <= clock_timestamp())
                  AND (wt.available_at IS NULL OR wt.available_at <= clock_timestamp())
                  AND wt.status IN ('pending', 'running')
                ORDER BY wt.task_id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks
            SET status = 'running',
                error = CASE WHEN attempt >= {MAX_ATTEMPTS} THEN
                    COALESCE(error, CASE WHEN lease_until IS NOT NULL THEN $4 ELSE $5 END)
                    ELSE NULL END,
                available_at = NULL,
                lease_until = clock_timestamp() + make_interval(secs => $2),
                attempt = attempt + 1,
                updated_at = clock_timestamp()
            WHERE workflow_id = $1
              AND task_id IN (SELECT task_id FROM candidate)
            RETURNING {ROW_COLS}
            "#
        );
        let mut rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .bind(LEASE_DURATION.as_secs() as f64)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .bind(LEASE_EXPIRED_ERR)
            .bind(BUDGET_EXHAUSTED_ERR)
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;
        rows.sort_unstable_by_key(|row| row.task_id);
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }
}

impl TaskStore for PgTaskRepo {
    type Error = StorageError;

    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>> {
        Box::pin(async move { Database::from(self.pool.clone()).submit(task).await })
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(self.claim_many_impl(workflow_id, limit))
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(self.renew_claim_impl(task))
    }

    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.settle_success_impl(task, output, successors, rules))
    }

    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.settle_failure_impl(task, message, successors, rules))
    }

    fn schedule_retry<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        delay: Duration,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.schedule_retry_impl(task, message, delay))
    }

    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.release_claim_impl(task))
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_repo.rs"]
mod tests;
