//! 通用工作流任务 `tasks` 的持久化操作（入队 / 认领 / 结算 / 级联）。
//!
//! 本模块不含任何业务节点名或业务 JSON 语义；业务侧专属查询（按 name / JSONB 字段的
//! 业务语义）应留在消费方，经 [`PgTaskRepo::insert_task`] / [`notify_tasks_ready`]
//! 等公开事务原语与本模块组合。

use std::{collections::HashMap, sync::Arc};

use super::{join, scope};
use crate::graph::{ErasedGraph, Successor, TaskType};
use crate::task::scope::{ScopeEntries, dependency_names};
use serde_json::Value;
use sqlx::{PgPool, types::Json};

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::task::store::{LEASE_DURATION, merge_join_input};
use crate::{Task, TaskRecord, TaskStatus};

/// 最终失败恢复认领保存的原因，重复恢复时保留第一次记录。
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted";
const LEASE_EXPIRED_ERR: &str = "task lease expired; retry budget exhausted";

/// 任务行 SELECT / RETURNING 列（顺序与表结构一致）。`pub`：消费方的业务查询
/// 与同一张表交互时复用同一列序。
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
     claimed_by, lease_until, attempt, max_attempts, task_type, depends_on_names, \
     input_json, output_json, error, created_at, updated_at";

/// 任务就绪通知通道：任务变为可认领时（入队 / 扇出 / 释放租约）在**同一事务**内
/// `pg_notify` 本通道，payload 为 `workflow_id`；PG 事务性 NOTIFY 只在 COMMIT 时投递，
/// 故收到通知时任务行必已提交可见。通知是提示（可丢失/可重复），丢失由调度器兜底轮询覆盖。
pub const TASK_READY_CHANNEL: &str = "tasks_ready";

/// 事务内发送任务就绪通知（随 COMMIT 投递，回滚不投递）。`pub`：消费方业务入队路径
/// （如 advance 链根任务）在同一事务内复用。
pub async fn notify_tasks_ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
) -> Result<(), StorageError> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(TASK_READY_CHANNEL)
        .bind(workflow_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// 事务内原子分配一个工作流级自增 task_id（`task_counters` 计数表，首次使用自动建行）。
///
/// 引擎自有的 id 分配源，不依赖任何消费方业务表（如 `world_sessions`）。
pub async fn allocate_task_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
) -> Result<i64, StorageError> {
    let task_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO task_counters (workflow_id, next_task_id)
        VALUES ($1, 1)
        ON CONFLICT (workflow_id)
        DO UPDATE SET next_task_id = task_counters.next_task_id + 1
        RETURNING next_task_id
        "#,
    )
    .bind(workflow_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(task_id)
}

/// 同一次运行的结算与汇合使用同一事务锁，包含尚不存在的 JOIN 行。
/// 必须在任务行锁与 ID 计数器锁之前获取；节点业务执行不持有此锁。
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

/// 先等行锁，再在后续写语句中检查数据库当前时间，避免等锁期间租约已过期。
async fn lock_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task: &TaskRecord,
) -> Result<TaskRow, StorageError> {
    sqlx::query_as::<_, TaskRow>(&format!(
        "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 AND task_id = $2 FOR UPDATE"
    ))
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| claim_lost(task))
}

/// 读取业务回填后的运行身份，再按统一顺序获取运行锁和任务锁。
async fn lock_settlement(
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
    let current = lock_task(tx, task).await?;
    if current.run_id != run_id {
        return Err(claim_lost(task));
    }
    Ok(())
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
        SET status = $5, output_json = $6, error = $7,
            lease_until = NULL, updated_at = clock_timestamp()
        WHERE workflow_id = $1 AND task_id = $2
          AND claimed_by = $3 AND attempt = $4
          AND status = 'running' AND lease_until > clock_timestamp()
          AND ($5 IN ('failed', 'superseded') OR attempt <= max_attempts)
        RETURNING {ROW_COLS}
        "#
    ))
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .bind(&task.claimed_by)
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
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph: &dyn ErasedGraph,
    source: &TaskRecord,
    plan: &Successor,
    max_attempts: i32,
    entries: &mut ScopeEntries,
) -> Result<(), StorageError> {
    let task_type = graph
        .task_type_of(plan.dst)
        .ok_or_else(|| StorageError::InvalidFanout {
            detail: format!("unknown successor node {}", plan.dst),
        })?;
    let destination_path =
        graph
            .subgraph_path(plan.dst)
            .ok_or_else(|| StorageError::InvalidFanout {
                detail: format!("unknown successor node {}", plan.dst),
            })?;
    if plan.new_scope_per_input && task_type == TaskType::Join {
        return Err(StorageError::InvalidFanout {
            detail: "foreach cannot target a JOIN node".into(),
        });
    }
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
        let existing = if task_type == TaskType::Join {
            sqlx::query_as::<_, TaskRow>(&format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=$1 AND name=$2 AND run_id IS NOT DISTINCT FROM $3 AND scope_id IS NOT DISTINCT FROM $4 AND task_type='join' AND status IN ('pending','running') ORDER BY task_id LIMIT 1 FOR UPDATE"
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
            sqlx::query("UPDATE tasks SET parent_task_ids=$3, input_json=$4, updated_at=clock_timestamp() WHERE workflow_id=$1 AND task_id=$2")
                .bind(&source.workflow_id).bind(existing.task_id).bind(parents).bind(input_json)
                .execute(&mut **tx).await?;
        } else {
            let dependencies =
                if task_type == TaskType::Join && source.status == TaskStatus::Succeeded {
                    let chain = scope::chain(tx, &source.workflow_id, scope_id).await?;
                    let mut entry_names = std::collections::HashMap::new();
                    for scope in &chain {
                        if let std::collections::hash_map::Entry::Vacant(entry) =
                            entry_names.entry(scope.trigger_task_id)
                        {
                            let name: String = sqlx::query_scalar(
                                "SELECT name FROM tasks WHERE workflow_id=$1 AND task_id=$2",
                            )
                            .bind(&source.workflow_id)
                            .bind(scope.trigger_task_id)
                            .fetch_one(&mut **tx)
                            .await?;
                            entry.insert(name);
                        }
                    }
                    dependency_names(graph, plan.dst, &chain, &entry_names)?
                } else {
                    Vec::new()
                };
            let task_id = allocate_task_id(tx, &source.workflow_id).await?;
            sqlx::query("INSERT INTO tasks (workflow_id,task_id,run_id,name,status,attempt,max_attempts,task_type,parent_task_ids,depends_on_names,input_json,scope_id) VALUES ($1,$2,$3,$4,'pending',0,$5,$6,$7,$8,$9,$10)")
                .bind(&source.workflow_id).bind(task_id).bind(source.run_id).bind(plan.dst)
                .bind(max_attempts).bind(task_type.as_str()).bind(vec![source.task_id]).bind(dependencies).bind(input).bind(scope_id)
                .execute(&mut **tx).await?;
        }
    }
    notify_tasks_ready(tx, &source.workflow_id).await?;
    Ok(())
}

/// 包括晚创建的 JOIN 和间接等待者；不把其他实例的失败传播过来。
async fn fail_blocked_joins(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: Option<i64>,
    upstreams: &HashMap<String, Vec<String>>,
) -> Result<(), StorageError> {
    loop {
        let changed = sqlx::query(&format!(
            "UPDATE tasks AS wt SET status='failed', error='upstream dependency failed', lease_until=NULL, updated_at=clock_timestamp() WHERE wt.workflow_id=$1 AND wt.run_id IS NOT DISTINCT FROM $2 AND wt.status='pending' AND ({})", join::failed("$3")
        )).bind(workflow_id).bind(run_id).bind(Json(upstreams)).execute(&mut **tx).await?.rows_affected();
        if changed == 0 {
            break;
        }
    }
    Ok(())
}

/// 当前工作流的 pgsql 仓储；克隆共享连接池和原始图。
#[derive(Clone)]
pub struct PgTaskRepo {
    pool: PgPool,
    graph: Arc<dyn ErasedGraph>,
    pub(super) max_attempts: i32,
}

/// 实现 [`crate::task::TaskStore::claim_many`] 的可执行契约，`wt` 为 `tasks` 别名。
///
/// 由认领（[`PgTaskRepo::claim_many`]）与恢复扫描（[`PgTaskRepo::executable_workflow_ids`]）
/// 认领和恢复共用：pending/running、无有效租约、JOIN 前驱成功且上游生产结束。
/// 查询按工作流限定，并使用仓储创建时绑定的上游范围。
/// 预算耗尽行也须认领，以便调度器通过统一失败流程结算。
fn executable_where(upstream_parameter: &str) -> String {
    format!(
        "wt.status IN ('pending', 'running') AND (wt.lease_until IS NULL OR wt.lease_until <= NOW()) AND ({})",
        join::ready(upstream_parameter)
    )
}

impl PgTaskRepo {
    pub fn new(pool: PgPool, graph: Arc<dyn ErasedGraph>) -> Self {
        Self {
            pool,
            graph,
            max_attempts: 3,
        }
    }

    /// 覆盖结算时创建的后继任务的最大尝试次数；直接入队沿用 Task 的预算。
    pub fn with_max_attempts(mut self, n: i32) -> Self {
        self.max_attempts = n;
        self
    }

    fn check_workflow(&self, workflow_id: &str) -> Result<(), StorageError> {
        if workflow_id != self.graph.workflow_id() {
            return Err(StorageError::InvalidFanout {
                detail: format!(
                    "workflow {workflow_id} does not match bound graph {}",
                    self.graph.workflow_id()
                ),
            });
        }
        Ok(())
    }

    /// 事务内插入任务行：分配工作流级 task_id 并插入 pending 行，返回完整行。
    /// 未指定 run_id 时使用新分配的 task_id 创建独立运行；指定时保留原分组。
    pub async fn insert_task(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &Task,
    ) -> Result<TaskRecord, StorageError> {
        self.check_workflow(&task.workflow_id)?;
        let task_type =
            self.graph
                .task_type_of(&task.name)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("unknown task node {}", task.name),
                })?;
        let destination_path =
            self.graph
                .subgraph_path(&task.name)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("unknown task node {}", task.name),
                })?;
        // 与结算保持相同锁序：run -> task/counter，直接 JOIN 入队也可能传播依赖失败。
        if task.run_id.is_some() {
            lock_run(tx, &task.workflow_id, task.run_id).await?;
        }
        let task_id = allocate_task_id(tx, &task.workflow_id).await?;
        let run_id = Some(task.run_id.unwrap_or(task_id));
        let scope_id = scope::route(
            tx,
            &task.workflow_id,
            run_id,
            task.scope_id,
            &destination_path,
            task_id,
            false,
            &mut ScopeEntries::new(),
        )
        .await?;
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            "INSERT INTO tasks (workflow_id,task_id,run_id,name,status,attempt,max_attempts,parent_task_ids,depends_on_names,input_json,task_type,scope_id) VALUES ($1,$2,$3,$4,'pending',0,$5,$6,$7,$8,$9,$10) RETURNING {ROW_COLS}"
        )).bind(&task.workflow_id).bind(task_id).bind(run_id).bind(&task.name).bind(task.max_attempts)
          .bind(&task.parent_task_ids).bind(&task.depends_on_names).bind(&task.input).bind(task_type.as_str()).bind(scope_id)
          .fetch_one(&mut **tx).await?;
        if task_type == TaskType::Join {
            fail_blocked_joins(tx, &task.workflow_id, run_id, self.graph.join_upstreams()).await?;
            let row = sqlx::query_as::<_, TaskRow>(&format!(
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id=$1 AND task_id=$2"
            ))
            .bind(&task.workflow_id)
            .bind(task_id)
            .fetch_one(&mut **tx)
            .await?;
            return Ok(row.into());
        }
        Ok(row.into())
    }

    /// 入队一个任务：原子分配工作流级 task_id 并插入 pending 行，随事务提交任务就绪通知。
    /// run_id 为空时创建独立运行，否则加入指定运行；任务与运行身份在同一事务提交。
    pub async fn enqueue(&self, task: &Task) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = self.insert_task(&mut tx, task).await?;
        notify_tasks_ready(&mut tx, &task.workflow_id).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// 当前认领成功结算与边界路由、后继入队在同一事务提交。
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[Successor],
        max_attempts: i32,
    ) -> Result<TaskRecord, StorageError> {
        self.check_workflow(&task.workflow_id)?;
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        lock_settlement(&mut tx, task).await?;
        let settled =
            settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                self.graph.as_ref(),
                &settled,
                fanout,
                max_attempts,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(
            &mut tx,
            &task.workflow_id,
            settled.run_id,
            self.graph.join_upstreams(),
        )
        .await?;
        tx.commit().await?;
        Ok(settled)
    }

    /// 当前认领先结算，再原子生成失败后继或级联失败；过期执行者没有写权限。
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[Successor],
        max_attempts: i32,
    ) -> Result<(), StorageError> {
        self.check_workflow(&task.workflow_id)?;
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        lock_settlement(&mut tx, task).await?;
        let status = if fanouts.is_empty() {
            TaskStatus::Failed
        } else {
            TaskStatus::Superseded
        };
        let settled = settle_in_tx(&mut tx, task, status, None, Some(error)).await?;
        let mut entries = ScopeEntries::new();
        for fanout in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                self.graph.as_ref(),
                &settled,
                fanout,
                max_attempts,
                &mut entries,
            )
            .await?;
        }
        fail_blocked_joins(
            &mut tx,
            &task.workflow_id,
            settled.run_id,
            self.graph.join_upstreams(),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 续租不改变认领代次；先等行锁，再检查数据库时间，过期认领不能复活。
    pub async fn renew_claim(
        &self,
        task: &TaskRecord,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let mut tx = self.pool.begin().await?;
        match lock_task(&mut tx, task).await {
            Ok(_) => {}
            Err(StorageError::ClaimLost { .. }) => return Ok(None),
            Err(error) => return Err(error),
        }
        let lease_until = sqlx::query_scalar(
            r#"
            UPDATE tasks
            SET lease_until = clock_timestamp() + make_interval(secs => $5),
                updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND claimed_by = $3 AND attempt = $4
              AND status = 'running' AND lease_until > clock_timestamp()
            RETURNING lease_until
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(&task.claimed_by)
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
        lock_task(&mut tx, task).await?;
        let result = sqlx::query(
            r#"
            UPDATE tasks SET lease_until = NULL, updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND claimed_by = $3 AND attempt = $4
              AND status = 'running' AND lease_until > clock_timestamp()
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
        }
        notify_tasks_ready(&mut tx, &task.workflow_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// 只可在指定 worker 已停止后使用，释放它在当前工作流的认领，不影响其他 worker。
    pub async fn release_claims(
        &self,
        workflow_id: &str,
        worker_id: &str,
    ) -> Result<u64, StorageError> {
        Ok(sqlx::query(
            r#"
            UPDATE tasks SET lease_until = NULL, updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND claimed_by = $2
              AND status = 'running' AND lease_until IS NOT NULL
            "#,
        )
        .bind(workflow_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// 认领当前工作流的就绪任务，使用可续期租约和递增的 attempt 隔离执行者。
    /// `FOR UPDATE SKIP LOCKED` 保证多 worker 不会同时认领同一行。
    ///
    /// 返回的 attempt <= max_attempts 时执行业务；超出预算的认领只负责最终失败结算。
    /// 恢复认领保存首次失败原因，允许恢复结算本身在崩溃或事务失败后重试。
    pub async fn claim_many(
        &self,
        workflow_id: &str,
        worker: &str,
        limit: usize,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        self.check_workflow(workflow_id)?;
        let executable_where = executable_where("$7");
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
                  AND {executable_where}
                ORDER BY wt.task_id
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks
            SET status = 'running',
                claimed_by = $2,
                error = CASE WHEN attempt >= max_attempts THEN
                    COALESCE(error, CASE WHEN lease_until IS NOT NULL THEN $5 ELSE $6 END)
                    ELSE error END,
                lease_until = clock_timestamp() + make_interval(secs => $3),
                attempt = attempt + 1,
                updated_at = NOW()
            WHERE workflow_id = $1
              AND task_id IN (SELECT task_id FROM candidate)
            RETURNING {ROW_COLS}
            "#
        );
        let mut rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .bind(worker)
            .bind(LEASE_DURATION.as_secs() as f64)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .bind(LEASE_EXPIRED_ERR)
            .bind(BUDGET_EXHAUSTED_ERR)
            .bind(Json(self.graph.join_upstreams()))
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;
        rows.sort_unstable_by_key(|row| row.task_id);
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    /// 列出该 session 全部任务（按 task_id 升序），供归档导出等消费方查询。
    pub async fn list_all(&self, workflow_id: &str) -> Result<Vec<TaskRecord>, StorageError> {
        let sql = format!("SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 ORDER BY task_id");
        let rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    /// 存在可认领任务的工作流 id 列表，供应用恢复扫描（如启动时与监听重连后）：
    /// 包含需要最终失败结算的预算耗尽任务，兜底通知丢失和进程重启。
    pub async fn executable_workflow_ids(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<String>, StorageError> {
        self.check_workflow(workflow_id)?;
        let executable_where = executable_where("$2");
        let sql = format!(
            r#"
            SELECT DISTINCT wt.workflow_id
            FROM tasks wt
            WHERE wt.workflow_id = $1 AND {executable_where}
            "#
        );
        let rows: Vec<(String,)> = sqlx::query_as(&sql)
            .bind(workflow_id)
            .bind(Json(self.graph.join_upstreams()))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(workflow_id,)| workflow_id).collect())
    }

    pub async fn get(
        &self,
        workflow_id: &str,
        task_id: i64,
    ) -> Result<Option<TaskRecord>, StorageError> {
        let sql = format!("SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 AND task_id = $2");
        let row = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(TaskRow::into))
    }

    /// 将当前有效认领标记成功，不生成后继。
    pub async fn succeed(
        &self,
        task: &TaskRecord,
        output: &Value,
    ) -> Result<TaskRecord, StorageError> {
        self.succeed_and_enqueue(task, output, &[], 3).await
    }

    pub async fn fail(&self, task: &TaskRecord, error: &str) -> Result<(), StorageError> {
        self.fail_and_enqueue(task, error, &[], 3).await
    }

    pub async fn supersede(&self, task: &TaskRecord, error: &str) -> Result<(), StorageError> {
        self.set_terminal(task, TaskStatus::Superseded, error).await
    }

    pub async fn skip(&self, task: &TaskRecord, reason: &str) -> Result<(), StorageError> {
        self.set_terminal(task, TaskStatus::Skipped, reason).await
    }

    async fn set_terminal(
        &self,
        task: &TaskRecord,
        status: TaskStatus,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        lock_settlement(&mut tx, task).await?;
        settle_in_tx(&mut tx, task, status, None, Some(error)).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_repo.rs"]
mod tests;
