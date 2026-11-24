//! 通用工作流任务 `tasks` 的持久化操作（入队 / 认领 / 结算 / 级联）。
//!
//! 本模块不含任何业务节点名或业务 JSON 语义；业务侧专属查询（按 name / JSONB 字段的
//! 业务语义）应留在消费方，通过 [`crate::Database::submit`] 或业务事务提交任务。

use std::{collections::HashMap, sync::Arc};

use super::{join, scope};
use crate::graph::{ErasedGraph, JoinRule, Successor};
use crate::task::{
    MAX_ATTEMPTS,
    scope::{ScopeEntries, route_path},
};
use serde_json::Value;
use sqlx::{PgPool, types::Json};

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::task::store::{LEASE_DURATION, check_workflow, merge_join_inputs};
use crate::{TaskRecord, TaskStatus};

/// 最终失败恢复认领保存的原因，重复恢复时保留第一次记录。
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted";
const LEASE_EXPIRED_ERR: &str = "task lease expired; retry budget exhausted";

/// 任务行 SELECT / RETURNING 列（顺序与表结构一致）。`pub`：消费方的业务查询
/// 与同一张表交互时复用同一列序。
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
     lease_until, attempt, \
     input_json, output_json, error, created_at, updated_at";

/// 同一次运行的结算与汇合使用同一事务锁，包含尚不存在的 JOIN 行。
/// 必须在任务行锁之前获取；节点业务执行不持有此锁。
async fn lock_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: Option<i64>,
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
async fn settle_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task: &TaskRecord,
    status: TaskStatus,
    output: Option<&Value>,
    error: Option<&str>,
) -> Result<TaskRecord, StorageError> {
    let row = sqlx::query_as::<_, TaskRow>(&format!(
        r#"
        UPDATE tasks
        SET status = $4, output_json = $5, error = $6,
            lease_until = NULL, updated_at = clock_timestamp()
        WHERE workflow_id = $1 AND task_id = $2
          AND attempt = $3
          AND status = 'running' AND lease_until > clock_timestamp()
          AND ($4 IN ('failed', 'superseded') OR attempt <= {MAX_ATTEMPTS})
        RETURNING {ROW_COLS}
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
    Ok(row.into())
}

/// 事务内按子图边界生成后继，JOIN 按运行、实例和节点名累积。
/// 调用方先获取运行锁，覆盖 JOIN 首次创建的窗口；本方法不自行开事务或提交。
async fn enqueue_fanouts_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph: &dyn ErasedGraph,
    source: &TaskRecord,
    fanouts: &[Successor],
) -> Result<(), StorageError> {
    if !fanouts.is_empty() {
        let chain = scope::chain(tx, &source.workflow_id, source.scope_id).await?;
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
                        &source.workflow_id,
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
                &source.workflow_id,
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
    fail_blocked_joins(tx, &source.workflow_id, source.run_id, graph.join_rules()).await
}

async fn collect_join(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &TaskRecord,
    plan: &Successor,
    scope_id: Option<i64>,
) -> Result<(), StorageError> {
    let existing = sqlx::query_as::<_, (i64, Vec<i64>, Value)>(
        "SELECT task_id, parent_task_ids, input_json FROM tasks
         WHERE workflow_id=$1 AND name=$2 AND run_id IS NOT DISTINCT FROM $3
           AND scope_id IS NOT DISTINCT FROM $4 AND status='collecting'
         ORDER BY task_id LIMIT 1 FOR UPDATE",
    )
    .bind(&source.workflow_id)
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
        .bind(&source.workflow_id)
        .bind(id)
        .bind(parents)
        .bind(merged)
        .execute(&mut **tx)
        .await?;
    } else {
        let merged = merge_join_inputs(None, &plan.inputs)?;
        insert_task(tx, source, plan, scope_id, &merged).await?;
    }
    Ok(())
}

async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &TaskRecord,
    plan: &Successor,
    scope_id: Option<i64>,
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (workflow_id,run_id,name,status,parent_task_ids,input_json,scope_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(&source.workflow_id)
    .bind(source.run_id)
    .bind(plan.dst)
    .bind(plan.initial_status.as_str())
    .bind([source.task_id].as_slice())
    .bind(input)
    .bind(scope_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// 包括晚创建的 JOIN 和间接等待者；不把其他实例的失败传播过来。
async fn fail_blocked_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: Option<i64>,
    rules: &HashMap<String, JoinRule>,
) -> Result<(), StorageError> {
    if rules.is_empty() {
        return Ok(());
    }
    loop {
        let changed = sqlx::query(&format!(
            "UPDATE tasks AS wt SET status='failed', error='upstream dependency failed', lease_until=NULL, updated_at=clock_timestamp() WHERE wt.workflow_id=$1 AND wt.run_id IS NOT DISTINCT FROM $2 AND wt.status='collecting' AND ({})", join::failed("$3")
        )).bind(workflow_id).bind(run_id).bind(Json(rules)).execute(&mut **tx).await?.rows_affected();
        if changed == 0 {
            break;
        }
    }
    Ok(())
}

/// 共享连接池上的任务仓储，可绑定认领任务的 worker 身份。
#[derive(Clone)]
pub struct PgTaskRepo {
    pub(super) pool: PgPool,
    worker_id: Option<Arc<str>>,
}

impl PgTaskRepo {
    pub fn new(pool: PgPool, worker_id: Option<Arc<str>>) -> Self {
        Self { pool, worker_id }
    }

    /// 先等行锁，再在后续写语句中检查数据库当前时间，避免等锁期间租约已过期。
    async fn lock_task(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &TaskRecord,
    ) -> Result<Option<i64>, StorageError> {
        sqlx::query_scalar(
            "SELECT run_id FROM tasks
         WHERE workflow_id = $1 AND task_id = $2
           AND worker_id IS NOT DISTINCT FROM $3
         FOR UPDATE",
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(self.worker_id.as_deref())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| claim_lost(task))
    }

    /// 读取业务回填后的运行身份，再按统一顺序获取运行锁和任务锁。
    async fn lock_settlement(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &TaskRecord,
    ) -> Result<(), StorageError> {
        let run_id = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT run_id FROM tasks WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| claim_lost(task))?;
        lock_run(tx, &task.workflow_id, run_id).await?;
        let current_run = self.lock_task(tx, task).await?;
        if current_run != run_id {
            return Err(claim_lost(task));
        }
        Ok(())
    }

    /// 当前认领成功结算与边界路由、后继入队在同一事务提交。
    pub async fn succeed_and_enqueue(
        &self,
        graph: &dyn ErasedGraph,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
    ) -> Result<TaskRecord, StorageError> {
        check_workflow(graph, &task.workflow_id)?;
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        self.lock_settlement(&mut tx, task).await?;
        let settled =
            settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
        enqueue_fanouts_in_tx(&mut tx, graph, &settled, fanouts).await?;
        tx.commit().await?;
        Ok(settled)
    }

    /// 当前认领先结算，再原子生成失败后继或级联失败；过期执行者没有写权限。
    pub async fn fail_and_enqueue(
        &self,
        graph: &dyn ErasedGraph,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
    ) -> Result<(), StorageError> {
        check_workflow(graph, &task.workflow_id)?;
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        self.lock_settlement(&mut tx, task).await?;
        let status = if fanouts.is_empty() {
            TaskStatus::Failed
        } else {
            TaskStatus::Superseded
        };
        let settled = settle_in_tx(&mut tx, task, status, None, Some(error)).await?;
        enqueue_fanouts_in_tx(&mut tx, graph, &settled, fanouts).await?;
        tx.commit().await?;
        Ok(())
    }

    /// 续租不改变认领代次；先等行锁，再检查数据库时间，过期认领不能复活。
    pub async fn renew_claim(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let mut tx = self.pool.begin().await?;
        match self.lock_task(&mut tx, task).await {
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

    /// 仅释放当前有效认领；旧 worker 不能释放新 owner 的租约。
    pub async fn release_claim(&self, task: &TaskRecord) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        self.lock_task(&mut tx, task).await?;
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

    /// 认领当前工作流的就绪任务，使用可续期租约和递增的 attempt 隔离执行者。
    /// `FOR UPDATE SKIP LOCKED` 保证多 worker 不会同时认领同一行。
    ///
    /// 返回的 attempt <= MAX_ATTEMPTS 时执行业务；超出预算的认领只负责最终失败结算。
    /// 恢复认领保存首次失败原因，允许恢复结算本身在崩溃或事务失败后重试。
    pub async fn claim_many(
        &self,
        graph: &dyn ErasedGraph,
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
                  AND (wt.lease_until IS NULL OR wt.lease_until <= NOW())
                  AND (wt.status IN ('pending', 'running') OR (wt.status = 'collecting' AND ({join_ready})))
                ORDER BY wt.task_id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks
            SET status = 'running', worker_id = $7,
                error = CASE WHEN attempt >= {MAX_ATTEMPTS} THEN
                    COALESCE(error, CASE WHEN lease_until IS NOT NULL THEN $4 ELSE $5 END)
                    ELSE error END,
                lease_until = clock_timestamp() + make_interval(secs => $2),
                attempt = attempt + 1,
                updated_at = NOW()
            WHERE workflow_id = $1
              AND task_id IN (SELECT task_id FROM candidate)
            RETURNING {ROW_COLS}
            "#,
            join_ready = join::ready("$6"),
        );
        let mut rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(graph.workflow_id())
            .bind(LEASE_DURATION.as_secs() as f64)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .bind(LEASE_EXPIRED_ERR)
            .bind(BUDGET_EXHAUSTED_ERR)
            .bind(Json(graph.join_rules()))
            .bind(self.worker_id.as_deref())
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;
        rows.sort_unstable_by_key(|row| row.task_id);
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_repo.rs"]
mod tests;
