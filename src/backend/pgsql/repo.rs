//! 通用工作流任务 `tasks` 的 PostgreSQL 仓储实现：提交、认领、续租、作用域级锁结算与 NOTIFY。

use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::PgPool;

use super::{rows::TaskRow, scope, tx::PgTx};
use crate::backend::{
    BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR, PG_NOTIFY_CHANNEL, ROW_COLS, claim_lost,
    common::enqueue_fanouts_and_advance,
};
use crate::error::StorageError;
use crate::task::{
    DEFAULT_MAX_ATTEMPTS, SettlementConfig, SettlementRules, Successor, TaskStore,
    scope::ScopeRecord, store::FanoutSource,
};
use crate::{Database, RunHandle, Task, TaskRecord, TaskStatus};

async fn notify_workflow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(PG_NOTIFY_CHANNEL)
        .bind(workflow_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl Database<&mut sqlx::Transaction<'_, sqlx::Postgres>> {
    /// 在宿主事务内创建独立运行，同时建立入口所属的隔离作用域并发送 PG NOTIFY。
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
        let max_attempts = task.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1);
        sqlx::query(
            "INSERT INTO tasks (workflow_id, task_id, run_id, name, status, input_json, scope_id, max_attempts)
             VALUES ($1, $2, $2, $3, 'pending', $4, $5, $6)",
        )
        .bind(&task.workflow_id)
        .bind(run_id)
        .bind(task.name)
        .bind(task.input)
        .bind(scope_id)
        .bind(max_attempts)
        .execute(&mut **tx)
        .await?;
        notify_workflow(tx, &task.workflow_id).await?;
        Ok(RunHandle {
            workflow_id: task.workflow_id,
            run_id,
        })
    }
}

/// 仅当前有效认领可以提交终态；使用任务行上的 `max_attempts` 列做动态预算校验。
async fn settle_in_tx<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task: &'a TaskRecord,
    status: TaskStatus,
    output: Option<&Value>,
    error: Option<&str>,
) -> Result<FanoutSource<'a>, StorageError> {
    let (run_id, scope_id) = sqlx::query_as(
        r#"
        UPDATE tasks
        SET status = $4, output_json = $5, error = $6,
            lease_until = NULL, available_at = NULL, updated_at = clock_timestamp()
        WHERE workflow_id = $1 AND task_id = $2
          AND attempt = $3
          AND status = 'running' AND lease_until > clock_timestamp()
          AND ($4 IN ('failed', 'superseded') OR attempt <= max_attempts)
        RETURNING run_id, scope_id
        "#,
    )
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
        name: &task.name,
        task_id: task.task_id,
        run_id,
        scope_id,
    })
}

/// 共享连接池上的 PostgreSQL 任务仓储。
#[derive(Clone)]
pub(crate) struct PgTaskRepo {
    pub(super) pool: PgPool,
}

impl PgTaskRepo {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

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

    /// 精细作用域级事务锁：
    /// - 无受影响 JOIN 时完全免 advisory lock；
    /// - 仅按受影响作用域深度收集锁键并严格升序（0 < ancestor < child）一次性加锁，防死锁且释放同级子图并发。
    async fn lock_settlement(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &TaskRecord,
        rules: &SettlementRules,
    ) -> Result<Option<Vec<ScopeRecord>>, StorageError> {
        let depths = rules.affected_scope_depths(&task.name);
        if depths.is_empty() {
            Self::lock_task(tx, task).await?;
            return Ok(None);
        }

        let need_chain = task.scope_id.is_some() && depths.iter().any(|&d| d > 0);
        let chain = if need_chain {
            Some(scope::chain(tx, &task.workflow_id, task.scope_id).await?)
        } else {
            None
        };

        let mut lock_keys: Vec<i64> = Vec::with_capacity(depths.len());
        for d in depths {
            if d == 0 {
                lock_keys.push(0);
            } else if let Some(ref c) = chain
                && d <= c.len()
            {
                lock_keys.push(c[d - 1].scope_id);
            }
        }
        lock_keys.sort_unstable();
        lock_keys.dedup();

        if !lock_keys.is_empty() {
            sqlx::query(
                r#"
                SELECT pg_advisory_xact_lock(hashtextextended(
                    jsonb_build_array('tasks'::regclass::oid, $1::text, $2::bigint, s)::text, 0
                ))
                FROM unnest($3::bigint[]) AS t(s)
                ORDER BY s
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.run_id)
            .bind(&lock_keys)
            .execute(&mut **tx)
            .await?;
        }

        let current_run = Self::lock_task(tx, task).await?;
        if current_run != task.run_id {
            return Err(claim_lost(task));
        }
        Ok(chain)
    }
}

impl TaskStore for PgTaskRepo {
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>> {
        Box::pin(async move { Database::from(self.pool.clone()).submit(task).await })
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
        lease_duration: Duration,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(async move {
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
                      AND wt.status IN ('pending', 'running')
                      AND (wt.lease_until IS NULL OR wt.lease_until <= clock_timestamp())
                      AND (wt.available_at IS NULL OR wt.available_at <= clock_timestamp())
                    ORDER BY wt.task_id
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE tasks
                SET status = 'running',
                    error = CASE WHEN attempt >= max_attempts THEN
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
                .bind(lease_duration.as_secs_f64())
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .bind(LEASE_EXPIRED_ERR)
                .bind(BUDGET_EXHAUSTED_ERR)
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
        lease_duration: Duration,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(async move {
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
            .bind(lease_duration.as_secs_f64())
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
        config: SettlementConfig,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
                .await?;
            let chain = Self::lock_settlement(&mut tx, task, rules).await?;
            let settled =
                settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
            let created_pending = enqueue_fanouts_and_advance(
                &mut PgTx(&mut tx),
                &settled,
                successors,
                rules,
                config,
                chain,
            )
            .await?;
            if created_pending {
                notify_workflow(&mut tx, &task.workflow_id).await?;
            }
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
        config: SettlementConfig,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut tx = self
                .pool
                .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
                .await?;
            let chain = Self::lock_settlement(&mut tx, task, rules).await?;
            let status = if successors.iter().all(|plan| plan.inputs.is_empty()) {
                TaskStatus::Failed
            } else {
                TaskStatus::Superseded
            };
            let settled = settle_in_tx(&mut tx, task, status, None, Some(message)).await?;
            let created_pending = enqueue_fanouts_and_advance(
                &mut PgTx(&mut tx),
                &settled,
                successors,
                rules,
                config,
                chain,
            )
            .await?;
            if created_pending {
                notify_workflow(&mut tx, &task.workflow_id).await?;
            }
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
            let mut tx = self.pool.begin().await?;
            Self::lock_task(&mut tx, task).await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET status = 'pending', error = $4, lease_until = NULL,
                    available_at = clock_timestamp() + make_interval(secs => $5),
                    updated_at = clock_timestamp()
                WHERE workflow_id = $1 AND task_id = $2
                  AND attempt = $3
                  AND status = 'running' AND attempt < max_attempts
                  AND lease_until > clock_timestamp()
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .bind(message)
            .bind(delay.as_secs_f64())
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
            let mut tx = self.pool.begin().await?;
            Self::lock_task(&mut tx, task).await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET lease_until = NULL,
                    error = COALESCE($4, error),
                    available_at = CASE
                        WHEN $5::double precision IS NOT NULL AND attempt < max_attempts
                        THEN clock_timestamp() + make_interval(secs => $5)
                        ELSE available_at
                    END,
                    updated_at = clock_timestamp()
                WHERE workflow_id = $1 AND task_id = $2
                  AND attempt = $3
                  AND status = 'running' AND lease_until > clock_timestamp()
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(task.attempt)
            .bind(error)
            .bind(delay.map(|d| d.as_secs_f64()))
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
                WHERE workflow_id = ANY($1)
                  AND status IN ('pending', 'running')
                  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
                  AND (available_at IS NULL OR available_at <= clock_timestamp())
                "#,
            )
            .bind(workflow_ids)
            .fetch_all(&self.pool)
            .await?;
            Ok(ids)
        })
    }

    fn list_tasks<'a>(
        &'a self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(async move {
            let sql = format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 AND run_id = $2 ORDER BY task_id"
            );
            let rows = sqlx::query_as::<_, TaskRow>(&sql)
                .bind(workflow_id)
                .bind(run_id)
                .fetch_all(&self.pool)
                .await?;
            Ok(rows.into_iter().map(TaskRecord::from).collect())
        })
    }

    fn cancel_run<'a>(
        &'a self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<usize, StorageError>> {
        Box::pin(async move {
            let mut tx = self.pool.begin().await?;
            let cancelled: Vec<i64> = sqlx::query_scalar(
                r#"
                UPDATE tasks
                SET status = 'cancelled', lease_until = NULL, available_at = NULL,
                    updated_at = clock_timestamp()
                WHERE workflow_id = $1 AND run_id = $2
                  AND status IN ('pending', 'collecting', 'running')
                RETURNING task_id
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .fetch_all(&mut *tx)
            .await?;
            if !cancelled.is_empty() {
                sqlx::query(
                    "DELETE FROM join_contributions WHERE workflow_id = $1 AND task_id = ANY($2)",
                )
                .bind(workflow_id)
                .bind(&cancelled)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(cancelled.len())
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_repo.rs"]
mod tests;
