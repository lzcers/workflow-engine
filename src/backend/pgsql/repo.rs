//! 通用工作流任务 `tasks` 的持久化操作（入队 / 认领 / 结算 / 级联）。
//!
//! 本模块不含任何业务节点名或业务 JSON 语义；业务侧专属查询（按 name / JSONB 字段的
//! 业务语义）应留在消费方，经 [`PgTaskRepo::insert_task`] / [`notify_tasks_ready`]
//! 等公开事务原语与本模块组合。

use serde_json::Value;
use sqlx::PgPool;

use super::rows::TaskRow;
use crate::error::StorageError;
use crate::task::{
    FanoutPlan,
    store::{LEASE_DURATION, merge_join_input},
};
use crate::{Task, TaskRecord, TaskStatus};

/// 最终失败恢复认领保存的原因，重复恢复时保留第一次记录。
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted";
const WINDOW_EXPIRED_ERR: &str = "task execution window expired; retry budget exhausted";

/// 任务行 SELECT / RETURNING 列（顺序与表结构一致）。`pub`：消费方的业务查询
/// 与同一张表交互时复用同一列序。
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, parent_task_id, name, status, \
     claimed_by, lease_until, attempt, max_attempts, task_type, depends_on_ids, depends_on_names, \
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

/// 事务内入队后继：普通任务逐条创建，JOIN 按运行和节点名累积依赖与输入。
/// 调用方先获取运行锁，覆盖 JOIN 首次创建的窗口；本方法不自行开事务或提交。
#[allow(clippy::too_many_arguments)]
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: Option<i64>,
    parent_task_id: Option<i64>,
    name: &str,
    inputs: &[Value],
    depends_on_ids: &[i64],
    task_type: &str,
    depends_on_names: &[String],
    max_attempts: i32,
) -> Result<Vec<TaskRecord>, StorageError> {
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        let existing = if task_type == "join" {
            sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                SELECT {ROW_COLS} FROM tasks
                WHERE workflow_id = $1 AND name = $2
                  AND run_id IS NOT DISTINCT FROM $3
                  AND task_type = 'join' AND status IN ('pending', 'running')
                ORDER BY task_id LIMIT 1 FOR UPDATE
                "#
            ))
            .bind(workflow_id)
            .bind(name)
            .bind(run_id)
            .fetch_optional(&mut **tx)
            .await?
        } else {
            None
        };

        let row = if let Some(existing) = existing {
            let mut dependencies = existing.depends_on_ids;
            for &dependency in depends_on_ids {
                if !dependencies.contains(&dependency) {
                    dependencies.push(dependency);
                }
            }
            let mut merged_input = existing.input_json;
            merge_join_input(&mut merged_input, input);
            sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                UPDATE tasks
                SET depends_on_ids = $3, input_json = $4::jsonb, updated_at = NOW()
                WHERE workflow_id = $1 AND task_id = $2
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(existing.task_id)
            .bind(&dependencies)
            .bind(merged_input)
            .fetch_one(&mut **tx)
            .await?
        } else {
            let failed_predecessor = if task_type == "join" {
                sqlx::query_scalar::<_, bool>(
                    r#"
                    SELECT EXISTS (
                        SELECT 1 FROM tasks
                        WHERE workflow_id = $1
                          AND run_id IS NOT DISTINCT FROM $2
                          AND name = ANY($3) AND status = 'failed'
                    )
                    "#,
                )
                .bind(workflow_id)
                .bind(run_id)
                .bind(depends_on_names)
                .fetch_one(&mut **tx)
                .await?
            } else {
                false
            };
            let task_id = allocate_task_id(tx, workflow_id).await?;
            sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                INSERT INTO tasks
                    (workflow_id, task_id, run_id, name, status,
                     attempt, max_attempts, task_type, depends_on_ids, depends_on_names,
                     input_json, parent_task_id)
                VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9, $10::jsonb, $11)
                RETURNING {ROW_COLS}
                "#
            ))
            .bind(workflow_id)
            .bind(task_id)
            .bind(run_id)
            .bind(name)
            .bind(if failed_predecessor {
                "failed"
            } else {
                "pending"
            })
            .bind(max_attempts)
            .bind(task_type)
            .bind(depends_on_ids)
            .bind(depends_on_names)
            .bind(input)
            .bind(parent_task_id)
            .fetch_one(&mut **tx)
            .await?
        };
        rows.push(row.into());
    }
    notify_tasks_ready(tx, workflow_id).await?;
    Ok(rows)
}

/// 通用工作流任务的 pgsql 仓储：持有连接池（`Clone` 共享同一池）。
#[derive(Clone)]
pub struct PgTaskRepo {
    pool: PgPool,
}

/// 实现 [`crate::task::TaskStore::claim_many`] 的可执行契约，`wt` 为 `tasks` 别名。
///
/// 由认领（[`PgTaskRepo::claim_many`]）与恢复扫描（[`PgTaskRepo::executable_workflow_ids`]）
/// 共用：pending/running、无租约或租约已过期、直接依赖全部存在且成功、JOIN 静态前驱等齐。
/// 遍历依赖 ID 而非已有任务行，确保缺失依赖也会阻塞。
/// 调用方需自行补 `wt.workflow_id = $1`（认领按工作流）或省略（恢复扫描跨工作流）。
/// 预算耗尽行也须认领，以便调度器通过统一失败流程结算。
const EXECUTABLE_WHERE: &str = r#"
    wt.status IN ('pending', 'running')
    AND (wt.lease_until IS NULL OR wt.lease_until <= NOW())
    AND NOT EXISTS (
        SELECT 1
        FROM unnest(wt.depends_on_ids) AS dependency(task_id)
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks dep
            WHERE dep.workflow_id = wt.workflow_id
              AND dep.task_id = dependency.task_id
              AND dep.status = 'succeeded'
        )
    )
    AND (
        wt.task_type <> 'join'
        OR NOT EXISTS (
            -- JOIN 静态前驱闸门：每个前驱名的**全部实例**都必须已成功。
            -- 「至少一个已成功」且「无未成功实例」，缺一不可：
            --   - 只查「至少一个成功」会在多实例扇出时因首个实例成功而过早放行；
            --   - 只查「无未成功实例」会在前驱尚未入队时因查到空集而过早放行。
            SELECT 1
            FROM unnest(wt.depends_on_names) AS dn(name)
            WHERE NOT (
                EXISTS (
                    SELECT 1
                    FROM tasks st
                    WHERE st.workflow_id = wt.workflow_id
                      AND st.run_id IS NOT DISTINCT FROM wt.run_id
                      AND st.name = dn.name
                      AND st.status = 'succeeded'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM tasks st
                    WHERE st.workflow_id = wt.workflow_id
                      AND st.run_id IS NOT DISTINCT FROM wt.run_id
                      AND st.name = dn.name
                      AND st.status <> 'succeeded'
                )
            )
        )
    )
"#;

impl PgTaskRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 事务内插入任务行：分配工作流级 task_id 并插入 pending 行，返回完整行。
    /// 未指定 run_id 时使用新分配的 task_id 创建独立运行；指定时保留原分组。
    pub async fn insert_task(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &Task,
    ) -> Result<TaskRecord, StorageError> {
        let task_id = allocate_task_id(tx, &task.workflow_id).await?;

        let sql = format!(
            r#"
            INSERT INTO tasks
                (
                    workflow_id, task_id, run_id, name, status,
                    claimed_by, lease_until, attempt, max_attempts, depends_on_ids, depends_on_names,
                    input_json, output_json, error, created_at, updated_at, parent_task_id
                )
            VALUES
                (
                    $1, $2, $3, $4, 'pending',
                    NULL, NULL, 0, $5, $6, $7,
                    $8::jsonb, NULL, NULL, NOW(), NOW(), $9
                )
            RETURNING {ROW_COLS}
            "#
        );
        let row = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(&task.workflow_id)
            .bind(task_id)
            .bind(task.run_id.unwrap_or(task_id))
            .bind(&task.name)
            .bind(task.max_attempts)
            .bind(&task.depends_on_ids)
            .bind(&task.depends_on_names)
            .bind(&task.input)
            .bind(task.parent_task_id)
            .fetch_one(&mut **tx)
            .await?;
        Ok(row.into())
    }

    /// 入队一个任务：原子分配工作流级 task_id 并插入 pending 行，随事务提交任务就绪通知。
    /// run_id 为空时创建独立运行，否则加入指定运行；任务与运行身份在同一事务提交。
    pub async fn enqueue(&self, task: &Task) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = Self::insert_task(&mut tx, task).await?;
        notify_tasks_ready(&mut tx, &task.workflow_id).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// 在同一事务中逐条入队输入；仅 JOIN 按运行和节点名汇合。
    /// JOIN 保留首次创建时的 parent_task_id，累积执行依赖和输入。
    /// 运行级事务锁覆盖首次创建窗口，已有 JOIN 通过行锁合并。
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_fanout(
        &self,
        workflow_id: &str,
        run_id: Option<i64>,
        parent_task_id: Option<i64>,
        name: &str,
        inputs: &[Value],
        depends_on_ids: &[i64],
        task_type: &str,
        depends_on_names: &[String],
        max_attempts: i32,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        lock_run(&mut tx, workflow_id, run_id).await?;
        let rows = enqueue_fanout_in_tx(
            &mut tx,
            workflow_id,
            run_id,
            parent_task_id,
            name,
            inputs,
            depends_on_ids,
            task_type,
            depends_on_names,
            max_attempts,
        )
        .await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// 当前认领成功结算与后继入队在同一事务提交，后继继承最新的 run_id。
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[FanoutPlan],
        max_attempts: i32,
    ) -> Result<TaskRecord, StorageError> {
        // 汇合锁之后的查询必须取得新快照，不继承连接的默认隔离级别。
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        lock_settlement(&mut tx, task).await?;
        let settled =
            settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
        for f in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &task.workflow_id,
                settled.run_id,
                Some(task.task_id),
                f.dst,
                &f.inputs,
                &[task.task_id],
                f.task_type,
                &f.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(settled)
    }

    /// 当前认领先结算，再原子生成失败后继或级联失败；过期执行者没有写权限。
    pub async fn fail_and_enqueue(
        &self,
        task: &TaskRecord,
        error: &str,
        fanouts: &[FanoutPlan],
        max_attempts: i32,
    ) -> Result<(), StorageError> {
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
        for f in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                &task.workflow_id,
                settled.run_id,
                Some(task.task_id),
                f.dst,
                &f.inputs,
                &[],
                f.task_type,
                &f.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        if fanouts.is_empty() {
            sqlx::query(
                r#"
                UPDATE tasks AS dependent
                SET status = 'failed', error = $3, lease_until = NULL, updated_at = clock_timestamp()
                FROM tasks AS source
                WHERE source.workflow_id = $1 AND source.task_id = $2
                  AND dependent.workflow_id = source.workflow_id
                  AND dependent.run_id IS NOT DISTINCT FROM source.run_id
                  AND dependent.status = 'pending'
                  AND (source.task_id = ANY(dependent.depends_on_ids)
                       OR source.name = ANY(dependent.depends_on_names))
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(format!("predecessor task {} failed", task.task_id))
            .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
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

    /// 认领当前工作流的就绪任务，使用固定租约和递增的 attempt 隔离执行者。
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
                  AND {EXECUTABLE_WHERE}
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
            .bind(WINDOW_EXPIRED_ERR)
            .bind(BUDGET_EXHAUSTED_ERR)
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
    pub async fn executable_workflow_ids(&self) -> Result<Vec<String>, StorageError> {
        let sql = format!(
            r#"
            SELECT DISTINCT wt.workflow_id
            FROM tasks wt
            WHERE {EXECUTABLE_WHERE}
            "#
        );
        let rows: Vec<(String,)> = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
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
        self.set_terminal(task, TaskStatus::Failed, error).await
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
