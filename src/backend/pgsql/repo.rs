//! 通用工作流任务 `tasks` 的持久化操作（入队 / 认领 / 结算 / 级联）。
//!
//! 本模块不含任何业务节点名或业务 JSON 语义；业务侧专属查询（按 name / JSONB 字段的
//! 业务语义）应留在消费方，经 [`PgTaskRepo::insert_task`] / [`notify_tasks_ready`]
//! 等公开事务原语与本模块组合。

use serde_json::Value;
use sqlx::PgPool;

use super::rows::TaskRow;
use crate::task::{FanoutInstances, store::LEASE_DURATION};
use crate::{StorageError, Task, TaskRecord, TaskStatus};

/// 任务行级重试预算耗尽时的封存错误信息（`attempt >= max_attempts` 不再被认领）。
const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted (attempt >= max_attempts)";

/// 任务行 SELECT / RETURNING 列（顺序与表结构一致）。`pub`：消费方的业务查询
/// 与同一张表交互时复用同一列序。
pub const ROW_COLS: &str = "workflow_id, task_id, run_id, name, instance_id, status, \
     claimed_by, lease_until, attempt, max_attempts, task_type, depends_on_ids, depends_on_names, \
     input_json, output_json, error, created_at, updated_at";

/// 把一份 `{前驱名: 贡献}` 单键对象并入 JOIN 任务的累积 Map；同名 key 合并为数组。
///
/// `contribution` 通常来自单键输出（如 `json!({ src: value })`）。仅 JOIN 任务在扇入合并时调用。
fn merge_join_input(acc: &mut Value, contribution: &Value) {
    let Value::Object(acc_map) = acc else { return };
    let Some(obj) = contribution.as_object() else {
        return;
    };
    for (k, v) in obj {
        match acc_map.get_mut(k) {
            None => {
                acc_map.insert(k.clone(), v.clone());
            }
            Some(existing) => {
                // 同名 key 已存在：把既有值与新贡献并入同一数组。
                let mut arr = match existing.take() {
                    Value::Array(a) => a,
                    other => vec![other],
                };
                match v {
                    Value::Array(items) => arr.extend(items.clone()),
                    other => arr.push(other.clone()),
                }
                acc_map.insert(k.clone(), Value::Array(arr));
            }
        }
    }
}

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

/// 事务内入队后继实例：调用方先获取运行锁，单实例用行锁合并，多实例独立成行
/// （同键经 `ON CONFLICT DO NOTHING` 去重），返回各行记录。供
/// [`PgTaskRepo::enqueue_fanout`] 与原子结算方法共用，**不自行开事务/提交**。
#[allow(clippy::too_many_arguments)]
async fn enqueue_fanout_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: Option<i64>,
    name: &str,
    instances: &[(Option<i32>, Value)],
    depends_on_ids: &[i64],
    task_type: &str,
    depends_on_names: &[String],
    max_attempts: i32,
) -> Result<Vec<TaskRecord>, StorageError> {
    let mut rows = Vec::with_capacity(instances.len());
    let select_cols = format!("SELECT {ROW_COLS}");
    let return_cols = format!("RETURNING {ROW_COLS}");

    for (instance_id, input) in instances {
        if let Some(instance_id) = instance_id {
            // 多实例：独立插入一行。同键冲突（结算事务已提交但响应丢失后的重执行再入队）
            // 时跳过——既有行保持，计数空跳产生的 id 空洞无害。
            let task_id = allocate_task_id(tx, workflow_id).await?;
            let inserted: Option<TaskRow> = sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                INSERT INTO tasks
                    (workflow_id, task_id, run_id, name, instance_id, status, claimed_by, lease_until,
                     attempt, max_attempts, task_type, depends_on_ids, depends_on_names, input_json,
                     output_json, error, created_at, updated_at)
                VALUES
                    ($1, $2, $3, $4, $5, 'pending', NULL, NULL, 0, $6, $7, $8, $9, $10::jsonb,
                     NULL, NULL, NOW(), NOW())
                ON CONFLICT DO NOTHING
                {return_cols}
                "#
            ))
            .bind(workflow_id)
            .bind(task_id)
            .bind(run_id)
            .bind(name)
            .bind(*instance_id)
            .bind(max_attempts)
            .bind(task_type)
            .bind(depends_on_ids)
            .bind(depends_on_names)
            .bind(input)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some(row) = inserted {
                rows.push(row.into());
            }
        } else {
            // 单实例：扇入合并。
            let existing: Option<TaskRow> = sqlx::query_as::<_, TaskRow>(&format!(
                r#"
                {select_cols}
                FROM tasks
                WHERE workflow_id = $1
                  AND name = $2
                  AND run_id IS NOT DISTINCT FROM $3
                  AND instance_id IS NULL
                  AND status IN ('pending', 'running')
                ORDER BY task_id
                LIMIT 1
                FOR UPDATE
                "#
            ))
            .bind(workflow_id)
            .bind(name)
            .bind(run_id)
            .fetch_optional(&mut **tx)
            .await?;

            let row = if let Some(existing) = existing {
                // 扇入汇聚：并集 depends_on_ids，去重。
                let mut merged = existing.depends_on_ids;
                for &d in depends_on_ids {
                    if !merged.contains(&d) {
                        merged.push(d);
                    }
                }
                // 仅 JOIN 任务做输入合并：把新到达的 `{前驱名: 贡献}` 并入已有 Map
                // （同名 key 合并为数组）。普通节点输入是单一强类型，合并会破坏之，
                // 故保持首个输入不变，只并 depends_on_ids。
                let (set_cols, merged_input) = if task_type == "join" {
                    let mut mi = existing.input_json.clone();
                    merge_join_input(&mut mi, input);
                    (
                        "depends_on_ids = $3, input_json = $4::jsonb, ".to_string(),
                        Some(mi),
                    )
                } else {
                    ("depends_on_ids = $3, ".to_string(), None)
                };
                let sql = format!(
                    r#"
                    UPDATE tasks
                    SET {set_cols}updated_at = NOW()
                    WHERE workflow_id = $1 AND task_id = $2
                    {return_cols}
                    "#
                );
                let mut q = sqlx::query_as::<_, TaskRow>(&sql)
                    .bind(workflow_id)
                    .bind(existing.task_id)
                    .bind(&merged);
                if let Some(mi) = merged_input {
                    q = q.bind(mi);
                }
                q.fetch_one(&mut **tx).await?
            } else {
                // 新建。JOIN 任务（task_type='join'）的静态前驱有已**永久失败**者
                // （如前驱先失败、后继后入队）→ 直接以 failed 落库，否则会因等齐闸门
                // 永远不齐而悬挂。普通节点不设等齐闸门，无需此判定。
                let new_status = if task_type != "join" {
                    "pending".to_string()
                } else {
                    let failed_pre: bool = sqlx::query_scalar(
                        r#"
                        SELECT EXISTS (
                            SELECT 1 FROM tasks
                            WHERE workflow_id = $1
                              AND run_id IS NOT DISTINCT FROM $2
                              AND name = ANY($3)
                              AND status = 'failed'
                        )
                        "#,
                    )
                    .bind(workflow_id)
                    .bind(run_id)
                    .bind(depends_on_names)
                    .fetch_one(&mut **tx)
                    .await?;
                    if failed_pre {
                        "failed".to_string()
                    } else {
                        "pending".to_string()
                    }
                };
                let task_id = allocate_task_id(tx, workflow_id).await?;
                sqlx::query_as::<_, TaskRow>(&format!(
                    r#"
                    INSERT INTO tasks
                        (workflow_id, task_id, run_id, name, instance_id, status, claimed_by, lease_until,
                         attempt, max_attempts, task_type, depends_on_ids, depends_on_names, input_json,
                         output_json, error, created_at, updated_at)
                    VALUES
                        ($1, $2, $3, $4, NULL, $5, NULL, NULL, 0, $6, $7, $8, $9, $10::jsonb,
                         NULL, NULL, NOW(), NOW())
                    {return_cols}
                    "#
                ))
                .bind(workflow_id)
                .bind(task_id)
                .bind(run_id)
                .bind(name)
                .bind(&new_status)
                .bind(max_attempts)
                .bind(task_type)
                .bind(depends_on_ids)
                .bind(depends_on_names)
                .bind(input)
                .fetch_one(&mut **tx)
                .await?
            };
            rows.push(row.into());
        }
    }

    // 任务就绪通知：与本次扇出同一事务，COMMIT 才投递（覆盖普通 fanout 与原子结算）。
    notify_tasks_ready(tx, workflow_id).await?;

    Ok(rows)
}

/// 通用工作流任务的 pgsql 仓储：持有连接池（`Clone` 共享同一池）。
#[derive(Clone)]
pub struct PgTaskRepo {
    pool: PgPool,
}

/// 任务「可执行」闸门（不含 session 过滤与 attempt 预算过滤），`wt` 为 `tasks` 别名。
///
/// 由认领（[`PgTaskRepo::claim_many`]）与恢复扫描（[`PgTaskRepo::executable_workflow_ids`]）
/// 共用：pending/running、租约未过期、直接依赖已全部成功、JOIN 静态前驱等齐。
/// 调用方需自行补 `wt.workflow_id = $1`（认领按工作流）或省略（恢复扫描跨工作流）。
const EXECUTABLE_WHERE: &str = r#"
    wt.status IN ('pending', 'running')
    AND (wt.lease_until IS NULL OR wt.lease_until <= NOW())
    AND NOT EXISTS (
        SELECT 1
        FROM tasks dep
        WHERE dep.workflow_id = wt.workflow_id
          AND dep.task_id = ANY(wt.depends_on_ids)
          AND dep.status <> 'succeeded'
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
    /// `pub`：消费方业务入队路径（如「先封存旧链再插入新链根任务」）在自管事务内复用。
    pub async fn insert_task(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: &Task,
    ) -> Result<TaskRecord, StorageError> {
        let task_id = allocate_task_id(tx, &task.workflow_id).await?;

        let sql = format!(
            r#"
            INSERT INTO tasks
                (
                    workflow_id, task_id, run_id, name, instance_id, status,
                    claimed_by, lease_until, attempt, max_attempts, depends_on_ids, depends_on_names,
                    input_json, output_json, error, created_at, updated_at
                )
            VALUES
                (
                    $1, $2, $3, $4, $5, 'pending',
                    NULL, NULL, 0, $6, $7, $8,
                    $9::jsonb, NULL, NULL, NOW(), NOW()
                )
            RETURNING {ROW_COLS}
            "#
        );
        let row = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(&task.workflow_id)
            .bind(task_id)
            .bind(task.run_id)
            .bind(&task.name)
            .bind(task.instance_id)
            .bind(task.max_attempts)
            .bind(&task.depends_on_ids)
            .bind(&task.depends_on_names)
            .bind(&task.input)
            .fetch_one(&mut **tx)
            .await?;
        Ok(row.into())
    }

    /// 原子创建独立运行，以入口任务的 task_id 作为同一工作流内唯一的 run_id。
    /// 入口任务与运行身份在同一事务提交，其他 worker 不会看到未分组的入口任务。
    pub async fn create_run(
        &self,
        workflow_id: &str,
        entry: &str,
        input: &Value,
        max_attempts: i32,
    ) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let mut row = Self::insert_task(
            &mut tx,
            &Task {
                workflow_id: workflow_id.to_owned(),
                run_id: None,
                name: entry.to_owned(),
                instance_id: None,
                max_attempts,
                depends_on_ids: Vec::new(),
                depends_on_names: Vec::new(),
                input: input.clone(),
            },
        )
        .await?;
        sqlx::query("UPDATE tasks SET run_id = task_id WHERE workflow_id = $1 AND task_id = $2")
            .bind(workflow_id)
            .bind(row.task_id)
            .execute(&mut *tx)
            .await?;
        row.run_id = Some(row.task_id);
        notify_tasks_ready(&mut tx, workflow_id).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// 入队一个任务：原子分配工作流级 task_id 并插入 pending 行，随事务提交任务就绪通知。
    pub async fn enqueue(&self, task: &Task) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = Self::insert_task(&mut tx, task).await?;
        notify_tasks_ready(&mut tx, &task.workflow_id).await?;
        tx.commit().await?;
        Ok(row)
    }

    /// 入队后继任务（统一 fanout 入口）。
    ///
    /// `instances` 为 `(instance_id, input)` 列表：
    /// - `instance_id = None`：**单实例**（扇入合并）。若 `(workflow_id, run_id, name, NULL)`
    ///   已有 pending/running 实例则做扇入合并（把 `depends_on_ids` 并集追加进现存任务的
    ///   `depends_on_ids`，避免对同一后继重复入队）。运行级事务锁覆盖“查无记录再创建”的窗口，
    ///   已有记录通过行锁合并。
    /// - `instance_id = Some(i)`：**多实例**（map 边扇出的第 i 个实例），独立成行、互不合并；
    ///   去重由唯一键 `uq_tasks_fanout_instance` 兜底（同键重入队经
    ///   `ON CONFLICT DO NOTHING` 跳过，覆盖「结算事务已提交但响应丢失」的重执行场景）。
    ///
    /// 全部实例在**同一事务**内分配连续 task_id 并插入/合并，返回每个实例的记录
    /// （供扇出父一次性把实例全集登记进 JOIN 节点的 `depends_on_ids`；冲突跳过的实例不在内）。
    /// `task_type` 为目标节点产出的任务类型（`normal`/`join`）；`depends_on_names` 为目标节点
    /// 的静态前驱节点名全集（通用血缘，仅 `task_type = 'join'` 用作等齐前驱的认领闸门）。
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_fanout(
        &self,
        workflow_id: &str,
        run_id: Option<i64>,
        name: &str,
        instances: &[(Option<i32>, Value)],
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

    /// 当前认领成功结算与后继入队在同一事务提交，后继继承最新的 run_id。
    pub async fn succeed_and_enqueue(
        &self,
        task: &TaskRecord,
        output: &Value,
        fanouts: &[FanoutInstances],
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
                &f.name,
                &f.instances,
                &[task.task_id],
                &f.task_type,
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
        fanouts: &[FanoutInstances],
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
                &f.name,
                &f.instances,
                &[],
                &f.task_type,
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

    /// 为仍有效的当前认领续租，不改变认领代次；过期租约不能复活。
    pub async fn renew_claim(&self, task: &TaskRecord) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        lock_task(&mut tx, task).await?;
        let result = sqlx::query(
            r#"
            UPDATE tasks
            SET lease_until = clock_timestamp() + make_interval(secs => $5),
                updated_at = clock_timestamp()
            WHERE workflow_id = $1 AND task_id = $2
              AND claimed_by = $3 AND attempt = $4
              AND status = 'running' AND lease_until > clock_timestamp()
            "#,
        )
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .bind(&task.claimed_by)
        .bind(task.attempt)
        .bind(LEASE_DURATION.as_secs() as f64)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(claim_lost(task));
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

    /// 从指定工作流认领一批可执行任务：pending 或已过期的 running 均视为可认领；同一批内互相无依赖
    /// （`NOT EXISTS` 依赖闸门过滤掉依赖未成功的任务）。
    /// 用 `FOR UPDATE SKIP LOCKED` 保证多 worker 不冲突，`LIMIT $n` 限制单批认领数量。
    ///
    /// 任务级重试预算（`attempt`/`max_attempts`）在此作为**通用兜底**生效：
    /// - `attempt < max_attempts` 的任务才被认领（每次认领 `attempt + 1`）；
    /// - `attempt >= max_attempts` 的可执行任务不再认领，而是在同一事务内**封存为 `failed`**
    ///   （写入 [`BUDGET_EXHAUSTED_ERR`]），避免它们永远停留在 `pending` 造成悬挂。
    ///
    /// 注意：这是**任务行级**的通用护栏（防同一任务被反复认领/超时重入），与消费方节点
    /// 的业务级重试预算（经失败边入队**新任务行**）是两套独立计数。
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

        // 可执行任务的通用闸门（不含 attempt 预算过滤）见 [`EXECUTABLE_WHERE`]；
        // 此处补上工作流过滤，seal / claim 两个 UPDATE 共用同一条闸门。

        // 1) 封存：可执行但重试预算已耗尽（attempt >= max_attempts）的任务 → 置为 failed，
        //    避免它们因预算满而不再被认领、却永远停留在 pending/running 造成悬挂。
        let seal_sql = format!(
            r#"
            UPDATE tasks AS wt
            SET status = 'failed',
                error = $2,
                lease_until = NULL,
                updated_at = NOW()
            WHERE wt.workflow_id = $1
              AND {EXECUTABLE_WHERE}
              AND wt.attempt >= wt.max_attempts
            "#
        );
        sqlx::query(&seal_sql)
            .bind(workflow_id)
            .bind(BUDGET_EXHAUSTED_ERR)
            .execute(&mut *tx)
            .await?;

        // 2) 认领：在可执行任务中，仅预算未耗尽（attempt < max_attempts）的进入认领。
        let sql = format!(
            r#"
            WITH candidate AS (
                SELECT wt.task_id
                FROM tasks wt
                WHERE wt.workflow_id = $1
                  AND {EXECUTABLE_WHERE}
                  AND wt.attempt < wt.max_attempts
                ORDER BY wt.task_id
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks
            SET status = 'running',
                claimed_by = $2,
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
    /// 兜底 NOTIFY 丢失（监听断线窗口 / 进程重启）。
    pub async fn executable_workflow_ids(&self) -> Result<Vec<String>, StorageError> {
        let sql = format!(
            r#"
            SELECT DISTINCT wt.workflow_id
            FROM tasks wt
            WHERE {EXECUTABLE_WHERE}
              AND wt.attempt < wt.max_attempts
            "#
        );
        let rows: Vec<(String,)> = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(workflow_id,)| workflow_id).collect())
    }

    /// 全量封存预算耗尽的可执行任务（`attempt >= max_attempts`），跨所有工作流。
    ///
    /// 与 [`PgTaskRepo::claim_many`] 内的封存步骤同一条闸门（依赖齐备、租约过期），
    /// 只是不按工作流过滤。恢复扫描（启动 / 监听重连）先执行本方法：
    /// 认领闸门与 [`PgTaskRepo::executable_workflow_ids`] 都要求 `attempt < max_attempts`，
    /// 若某工作流只剩预算耗尽行，它既不会被认领、恢复扫描也找不到它——worker 永不
    /// 拉起，`claim_many` 内的封存也就永远不执行，行会永久停留在 pending/running
    /// （进程重启后尤其如此）。把封存提前到恢复阶段即可清扫这类残留，让消费方快照
    /// （`operation`）尽快看到终态失败。
    pub async fn seal_budget_exhausted(&self) -> Result<u64, StorageError> {
        let sql = format!(
            r#"
            UPDATE tasks AS wt
            SET status = 'failed',
                error = $1,
                lease_until = NULL,
                updated_at = NOW()
            WHERE {EXECUTABLE_WHERE}
              AND wt.attempt >= wt.max_attempts
            "#
        );
        let res = sqlx::query(&sql)
            .bind(BUDGET_EXHAUSTED_ERR)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
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
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use sqlx::postgres::PgListener;

    use super::super::test_support::connect_test;
    use super::BUDGET_EXHAUSTED_ERR;
    use super::{PgTaskRepo, TASK_READY_CHANNEL, merge_join_input};
    use crate::task::FanoutInstances;
    use crate::{StorageError, Task, TaskRecord, TaskStatus};

    /// 收取一条**本工作流**的任务就绪通知（2s 超时）：通知通道是集群级共享的，
    /// 并发测试可能互相收到对方的 payload（生产监听器同样按 payload 分发），
    /// 故跳过其他工作流的 payload 直到命中期望工作流。
    async fn recv_ready(listener: &mut PgListener, workflow_id: &str) -> String {
        loop {
            match tokio::time::timeout(Duration::from_secs(2), listener.recv()).await {
                Ok(Ok(notification)) => {
                    assert_eq!(notification.channel(), TASK_READY_CHANNEL);
                    if notification.payload() == workflow_id {
                        return notification.payload().to_string();
                    }
                }
                Ok(Err(e)) => panic!("listener recv failed: {e}"),
                Err(_) => panic!("timed out waiting for task ready notification: {workflow_id}"),
            }
        }
    }

    /// 原子成功结算：succeed + 全部后继（单实例 + 多实例）在同一事务内落库，
    /// 后继 run_id 取回填后的源任务 run_id、depends_on_ids 指向源任务。
    #[tokio::test]
    async fn succeed_and_enqueue_settles_and_fans_out_atomically() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "succeed_and_enqueue_atomic";
        repo.enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "image_prompt".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({"prompts": ["a", "b"]}),
        })
        .await
        .unwrap();
        let source = repo
            .claim_many(workflow_id, "worker", 5)
            .await
            .unwrap()
            .remove(0);

        let settled = repo
            .succeed_and_enqueue(
                &source,
                &json!({"prompts": ["a", "b"]}),
                &[
                    FanoutInstances {
                        name: "finalize_node".to_string(),
                        task_type: "normal".to_string(),
                        instances: vec![(None, json!({}))],
                        depends_on_names: vec![],
                    },
                    FanoutInstances {
                        name: "image_generate".to_string(),
                        task_type: "normal".to_string(),
                        instances: vec![
                            (Some(0), json!({"prompt": "a"})),
                            (Some(1), json!({"prompt": "b"})),
                        ],
                        depends_on_names: vec![],
                    },
                ],
                3,
            )
            .await
            .unwrap();

        // 源任务已成功结算且租约释放。
        assert_eq!(settled.status, TaskStatus::Succeeded);
        assert_eq!(settled.output, Some(json!({"prompts": ["a", "b"]})));
        assert!(settled.lease_until.is_none());

        // 三个后继全部落库，依赖均指向源任务。
        let rows = repo.list_all(workflow_id).await.unwrap();
        assert_eq!(rows.len(), 4);
        let finalize: Vec<_> = rows.iter().filter(|t| t.name == "finalize_node").collect();
        assert_eq!(finalize.len(), 1);
        assert_eq!(finalize[0].instance_id, None);
        assert_eq!(finalize[0].depends_on_ids, vec![source.task_id]);
        let generates: Vec<_> = rows.iter().filter(|t| t.name == "image_generate").collect();
        assert_eq!(generates.len(), 2);
        assert_eq!(generates[0].instance_id, Some(0));
        assert_eq!(generates[1].instance_id, Some(1));
        for g in &generates {
            assert_eq!(g.depends_on_ids, vec![source.task_id]);
        }
    }

    /// 多实例唯一键去重：同键重入队（模拟「结算事务已提交但响应丢失」后的重执行）经
    /// `ON CONFLICT DO NOTHING` 跳过，不产生重复实例行。
    #[tokio::test]
    async fn multi_instance_fanout_dedups_conflicting_keys() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "multi_instance_dedup";
        let instances: Vec<(Option<i32>, serde_json::Value)> = vec![
            (Some(0), json!({"prompt": "a"})),
            (Some(1), json!({"prompt": "b"})),
        ];
        let first = repo
            .enqueue_fanout(
                workflow_id,
                None,
                "image_generate",
                &instances,
                &[7],
                "normal",
                &[],
                3,
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 2);

        // 同键重入队：全部冲突跳过，返回空。
        let second = repo
            .enqueue_fanout(
                workflow_id,
                None,
                "image_generate",
                &instances,
                &[7],
                "normal",
                &[],
                3,
            )
            .await
            .unwrap();
        assert!(second.is_empty());

        let rows = repo.list_all(workflow_id).await.unwrap();
        let generates: Vec<_> = rows.iter().filter(|t| t.name == "image_generate").collect();
        assert_eq!(generates.len(), 2);
    }

    /// 原子失败结算：有后继 → `superseded` 且后继入队（不依赖源任务）；
    /// 无后继 → `failed` 并级联已有依赖，级联失败时源任务终态一起回滚。
    #[tokio::test]
    async fn fail_and_enqueue_marks_superseded_or_failed_atomically() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "fail_and_enqueue_atomic";
        // 有后继：superseded + 后继入队（depends_on_ids 为空）。
        repo.enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "commit_scene".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({"proposal": {}}),
        })
        .await
        .unwrap();
        let retried = repo
            .claim_many(workflow_id, "worker", 1)
            .await
            .unwrap()
            .remove(0);
        repo.fail_and_enqueue(
            &retried,
            "validation failed",
            &[FanoutInstances {
                name: "scene_proposal".to_string(),
                task_type: "normal".to_string(),
                instances: vec![(None, json!({"Retry": {}}))],
                depends_on_names: vec![],
            }],
            3,
        )
        .await
        .unwrap();
        let settled = repo
            .get(workflow_id, retried.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, TaskStatus::Superseded);
        assert_eq!(settled.error.as_deref(), Some("validation failed"));
        let successor = repo
            .list_all(workflow_id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.name == "scene_proposal" && t.task_id != retried.task_id)
            .unwrap();
        assert!(successor.depends_on_ids.is_empty());

        // 无后继：failed。
        let failed = repo
            .enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id: None,
                name: "audio_generate".to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: vec![],
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap();
        let failed = repo
            .claim_many(workflow_id, "worker", 10)
            .await
            .unwrap()
            .into_iter()
            .find(|task| task.task_id == failed.task_id)
            .unwrap();
        let mut dependents = Vec::new();
        for (name, run_id, by_name) in [
            ("audio_dependent", None, false),
            ("audio_join", None, true),
            ("other_run", Some(1), true),
        ] {
            let dependent = repo
                .enqueue(&Task {
                    workflow_id: workflow_id.to_string(),
                    run_id,
                    name: name.to_string(),
                    instance_id: None,
                    max_attempts: 3,
                    depends_on_ids: if by_name {
                        vec![]
                    } else {
                        vec![failed.task_id]
                    },
                    depends_on_names: if by_name {
                        vec![failed.name.clone()]
                    } else {
                        vec![]
                    },
                    input: json!({}),
                })
                .await
                .unwrap();
            dependents.push(dependent);
        }

        // 级联写入失败必须回滚源任务的失败状态。
        sqlx::query(
            "ALTER TABLE tasks ADD CONSTRAINT reject_cascade \
             CHECK (name <> 'audio_join' OR status <> 'failed')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            repo.fail_and_enqueue(&failed, "budget exhausted", &[], 3,)
                .await
                .is_err()
        );
        for task in std::iter::once(&failed).chain(&dependents) {
            let unchanged = repo.get(workflow_id, task.task_id).await.unwrap().unwrap();
            assert_eq!(
                unchanged.status,
                if task.task_id == failed.task_id {
                    TaskStatus::Running
                } else {
                    TaskStatus::Pending
                }
            );
        }
        sqlx::query("ALTER TABLE tasks DROP CONSTRAINT reject_cascade")
            .execute(database.pool())
            .await
            .unwrap();

        repo.fail_and_enqueue(&failed, "budget exhausted", &[], 3)
            .await
            .unwrap();
        let settled = repo
            .get(workflow_id, failed.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
        for dependent in dependents {
            let settled = repo
                .get(workflow_id, dependent.task_id)
                .await
                .unwrap()
                .unwrap();
            let expected = if dependent.run_id.is_none() {
                TaskStatus::Failed
            } else {
                TaskStatus::Pending
            };
            assert_eq!(settled.status, expected);
        }
    }

    /// 释放租约后任务立即可被重新认领（无需等待租约过期），attempt 照常递增。
    #[tokio::test]
    async fn release_claim_makes_task_reclaimable_immediately() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "release_claim_immediate";
        let task = repo
            .enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id: None,
                name: "narration".to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: vec![],
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap();

        let claimed = repo.claim_many(workflow_id, "worker", 5).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(claimed[0].lease_until.is_some());

        repo.release_claim(&claimed[0]).await.unwrap();

        // 租约已释放：立即重认领成功，attempt 递增。
        let reclaimed = repo.claim_many(workflow_id, "worker", 5).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].task_id, task.task_id);
        assert_eq!(reclaimed[0].attempt, 2);
    }

    /// 批量释放：同一工作流的在跑任务全部立即回到可认领，且只影响本工作流。
    #[tokio::test]
    async fn release_claims_returns_every_running_task_in_the_workflow() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        async fn enqueue(repo: &PgTaskRepo, workflow_id: &str) -> TaskRecord {
            repo.enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id: None,
                name: "narration".to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: vec![],
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap()
        }
        let ours = enqueue(&repo, "release_claims_all").await;
        let other = enqueue(&repo, "release_claims_other").await;
        assert_eq!(
            repo.claim_many("release_claims_all", "worker", 5)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            repo.claim_many("release_claims_other", "worker", 5)
                .await
                .unwrap()
                .len(),
            1
        );

        assert_eq!(
            repo.release_claims("release_claims_all", "worker")
                .await
                .unwrap(),
            1
        );
        let reclaimed = repo
            .claim_many("release_claims_all", "worker", 5)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].task_id, ours.task_id);
        assert_eq!(reclaimed[0].attempt, 2, "重认领照常递增 attempt");

        // 其它工作流不受影响：它的任务租约还在（认领不到，状态仍是在跑）。
        assert!(
            repo.claim_many("release_claims_other", "worker", 5)
                .await
                .unwrap()
                .is_empty()
        );
        let untouched = repo
            .get("release_claims_other", other.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(untouched.status, TaskStatus::Running);
        assert!(untouched.lease_until.is_some());
    }

    /// 恢复扫描只返回「存在可认领任务」的工作流（与 `claim_many` 同款闸门）：
    /// 可认领（pending 无依赖）命中；被依赖阻塞、已全部成功、重试预算耗尽的工作流不命中。
    #[tokio::test]
    async fn executable_workflow_ids_only_returns_claimable_sessions() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        async fn insert_task(
            repo: &PgTaskRepo,
            workflow_id: &str,
            name: &str,
            deps: Vec<i64>,
        ) -> TaskRecord {
            repo.enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id: None,
                name: name.to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: deps,
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap()
        }

        // 可认领：pending 且无依赖 → 命中。
        let claimable = "exec_sess_claimable";
        insert_task(&repo, claimable, "narration", vec![]).await;

        // 被依赖阻塞：后继 pending 但前驱未成功 → 不命中。先把前驱认领掉
        // （running + 租约未过期 → 前驱自身也不可认领），否则前驱的 pending 行
        // 本身就会让工作流命中，测试的「阻塞」前提不成立。
        let blocked = "exec_sess_blocked";
        let dep = insert_task(&repo, blocked, "commit_scene", vec![]).await;
        insert_task(&repo, blocked, "narration", vec![dep.task_id]).await;
        repo.claim_many(blocked, "worker", 5).await.unwrap();

        // 已全部成功：无 pending/running → 不命中。
        let done = "exec_sess_done";
        insert_task(&repo, done, "narration", vec![]).await;
        let finished = repo.claim_many(done, "worker", 1).await.unwrap().remove(0);
        repo.succeed(&finished, &json!({})).await.unwrap();

        // 预算耗尽：attempt >= max_attempts（认领三次后不再被认领）→ 不命中。
        let exhausted = "exec_sess_exhausted";
        insert_task(&repo, exhausted, "narration", vec![]).await;
        for _ in 0..3 {
            let claim = repo
                .claim_many(exhausted, "worker", 5)
                .await
                .unwrap()
                .remove(0);
            repo.release_claim(&claim).await.unwrap();
        }

        let mut sessions = repo.executable_workflow_ids().await.unwrap();
        sessions.sort();
        assert_eq!(
            sessions,
            vec![claimable.to_string()],
            "只应命中可认领会话，阻塞/成功/耗尽均不命中"
        );
    }

    /// 「恢复封存」清扫预算耗尽的残留行：租约过期、依赖齐备、attempt >= max_attempts
    /// 的任务被全量置为 `failed`；有剩余预算的任务不受影响。
    ///
    /// 对应线上事故形态：任务经租约过期被反复重认领刷满 attempt，进程重启后恢复扫描
    /// 找不到该工作流（同款 `attempt < max_attempts` 闸门），`claim_many` 内的封存
    /// 永不执行，行永久停在 running。
    #[tokio::test]
    async fn recovery_seal_marks_budget_exhausted_rows_failed() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());

        // 预算耗尽残留：认领 3 次刷满 attempt，模拟「租约过期被杀、无人再碰」。
        let stale = "recovery_seal_stale";
        let stale_task = repo
            .enqueue(&Task {
                workflow_id: stale.to_string(),
                run_id: None,
                name: "scene_proposal".to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: vec![],
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap();
        for attempt in 0..3 {
            repo.claim_many(stale, "worker", 5).await.unwrap();
            if attempt < 2 {
                sqlx::query(
                    "UPDATE tasks SET lease_until = NOW() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2",
                )
                .bind(stale)
                .bind(stale_task.task_id)
                .execute(database.pool())
                .await
                .unwrap();
            }
        }
        let still_running = repo.get(stale, stale_task.task_id).await.unwrap().unwrap();
        assert_eq!(still_running.status, TaskStatus::Running);
        assert_eq!(still_running.attempt, 3);
        assert!(
            still_running.lease_until.is_some(),
            "行仍持有有效租约（进程存活时封存不应误伤在途执行）"
        );

        // 先模拟租约过期（如进程重启后），恢复封存才应命中。
        sqlx::query(
            "UPDATE tasks SET lease_until = NOW() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(stale)
        .bind(stale_task.task_id)
        .execute(database.pool())
        .await
        .unwrap();

        // 对照工作流：有剩余预算（pending, attempt=0），封存不该动它。
        let fresh = "recovery_seal_fresh";
        repo.enqueue(&Task {
            workflow_id: fresh.to_string(),
            run_id: None,
            name: "narration".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap();

        let sealed = repo.seal_budget_exhausted().await.unwrap();
        assert_eq!(sealed, 1, "只应封存滞留在可执行态的预算耗尽行");

        let settled = repo.get(stale, stale_task.task_id).await.unwrap().unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
        assert_eq!(
            settled.error.as_deref(),
            Some(BUDGET_EXHAUSTED_ERR),
            "封存错误信息与 claim_many 内封存保持一致"
        );
        assert!(settled.lease_until.is_none(), "封存应清空租约");

        // 预算充足的工作流不受影响，仍可认领。
        let claimed = repo.claim_many(fresh, "worker", 5).await.unwrap();
        assert_eq!(claimed.len(), 1);
    }

    /// 事务性通知：同一事务内 `pg_notify` 随 COMMIT 投递、ROLLBACK 不投递。
    /// （pg_notify 是集群级通道、不受测试 schema 隔离，测试用唯一通道名避免串扰。）
    #[tokio::test]
    async fn task_ready_notify_fires_only_on_commit() {
        let Some(database) = connect_test().await else {
            return;
        };
        let channel = format!("test_ready_notify_{}", uuid::Uuid::new_v4().simple());
        let mut listener = PgListener::connect(database.database_url()).await.unwrap();
        listener.listen(&channel).await.unwrap();

        // 回滚：不投递。
        let mut tx = database.pool().begin().await.unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind("rolled-back")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), listener.recv())
                .await
                .is_err()
        );

        // 提交：投递。
        let mut tx = database.pool().begin().await.unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind("committed")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let notification = tokio::time::timeout(Duration::from_secs(2), listener.recv())
            .await
            .expect("timed out waiting for committed notification")
            .unwrap();
        assert_eq!(notification.payload(), "committed");
    }

    /// 任务就绪通知发送点：入队 / 扇出 / 释放租约各自随提交投递，payload = workflow_id。
    #[tokio::test]
    async fn task_ready_notifications_carry_workflow_id() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "task_ready_notifications";
        let mut listener = PgListener::connect(database.database_url()).await.unwrap();
        listener.listen(TASK_READY_CHANNEL).await.unwrap();

        // 1) 入队（HTTP 推进根任务）。
        let root = repo
            .enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id: None,
                name: "scene_proposal".to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: vec![],
                depends_on_names: vec![],
                input: json!({}),
            })
            .await
            .unwrap();
        assert_eq!(recv_ready(&mut listener, workflow_id).await, workflow_id);

        // 2) 扇出后继（原子结算路径同样经此通知）。
        repo.enqueue_fanout(
            workflow_id,
            None,
            "commit_scene",
            &[(None, json!({}))],
            &[root.task_id],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
        assert_eq!(recv_ready(&mut listener, workflow_id).await, workflow_id);

        // 3) 释放租约（原子结算回滚后的立即重入）。
        let claimed = repo
            .claim_many(workflow_id, "worker", 5)
            .await
            .unwrap()
            .remove(0);
        repo.release_claim(&claimed).await.unwrap();
        assert_eq!(recv_ready(&mut listener, workflow_id).await, workflow_id);
    }

    async fn seed(
        repo: &PgTaskRepo,
        workflow_id: &str,
        name: &str,
        run_id: Option<i64>,
    ) -> TaskRecord {
        repo.enqueue(&Task {
            workflow_id: workflow_id.into(),
            run_id,
            name: name.into(),
            instance_id: None,
            max_attempts: 5,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap()
    }

    async fn reject_stale_writes(repo: &PgTaskRepo, task: &TaskRecord) {
        let fanouts = [FanoutInstances {
            name: "must-not-exist".into(),
            task_type: "normal".into(),
            instances: vec![(None, json!({}))],
            depends_on_names: vec![],
        }];
        assert!(matches!(
            repo.renew_claim(task).await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.release_claim(task).await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.succeed_and_enqueue(task, &json!("stale"), &fanouts, 3)
                .await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.fail_and_enqueue(task, "stale", &fanouts, 3).await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.fail_and_enqueue(task, "stale", &[], 3).await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.succeed(task, &json!("stale")).await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.fail(task, "stale").await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.supersede(task, "stale").await,
            Err(StorageError::ClaimLost { .. })
        ));
        assert!(matches!(
            repo.skip(task, "stale").await,
            Err(StorageError::ClaimLost { .. })
        ));
    }

    #[tokio::test]
    async fn expired_and_replaced_claims_cannot_write_or_release_a_new_owner() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let workflow_id = "claim-fencing";
        let root = seed(&repo, workflow_id, "root", Some(7)).await;
        let old = repo
            .claim_many(workflow_id, "worker-a", 1)
            .await
            .unwrap()
            .remove(0);
        let mut dependent = Task {
            workflow_id: workflow_id.into(),
            run_id: Some(7),
            name: "dependent".into(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![root.task_id],
            depends_on_names: vec![],
            input: json!({}),
        };
        repo.enqueue(&dependent).await.unwrap();
        sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2")
            .bind(workflow_id).bind(old.task_id).execute(database.pool()).await.unwrap();
        reject_stale_writes(&repo, &old).await;
        let current = repo
            .claim_many(workflow_id, "worker-b", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(current.attempt, old.attempt + 1);
        reject_stale_writes(&repo, &old).await;
        // Even a matching owner cannot reuse the preceding claim generation.
        let mut wrong_attempt = current.clone();
        wrong_attempt.attempt = old.attempt;
        reject_stale_writes(&repo, &wrong_attempt).await;
        let mut wrong_owner = current.clone();
        wrong_owner.claimed_by = Some("worker-a".into());
        reject_stale_writes(&repo, &wrong_owner).await;
        let rows = repo.list_all(workflow_id).await.unwrap();
        assert_eq!(rows.len(), 2, "stale settlement cannot create successors");
        assert_eq!(
            rows[1].status,
            TaskStatus::Pending,
            "stale failure cannot cascade"
        );
        let unchanged = repo
            .get(workflow_id, current.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.claimed_by, current.claimed_by);
        assert_eq!(unchanged.attempt, current.attempt);
        assert_eq!(unchanged.lease_until, current.lease_until);
        sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() + interval '30 seconds' WHERE workflow_id = $1 AND task_id = $2")
            .bind(workflow_id).bind(current.task_id).execute(database.pool()).await.unwrap();
        repo.renew_claim(&current).await.unwrap();
        let renewed = repo
            .get(workflow_id, current.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renewed.attempt, current.attempt);
        assert!(renewed.lease_until.unwrap() > chrono::Utc::now() + chrono::Duration::seconds(200));
        repo.succeed(&current, &json!("current")).await.unwrap();
        reject_stale_writes(&repo, &current).await;
        let settled = repo
            .get(workflow_id, current.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.output, Some(json!("current")));
        // Bulk cleanup is restricted to the stopped worker, including within one workflow.
        dependent.name = "another".into();
        dependent.depends_on_ids.clear();
        repo.enqueue(&dependent).await.unwrap();
        let a = repo
            .claim_many(workflow_id, "worker-a", 1)
            .await
            .unwrap()
            .remove(0);
        let b = repo
            .claim_many(workflow_id, "worker-b", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            repo.release_claims(workflow_id, "worker-a").await.unwrap(),
            1
        );
        assert!(matches!(
            repo.renew_claim(&a).await,
            Err(StorageError::ClaimLost { .. })
        ));
        repo.renew_claim(&b).await.unwrap();
    }

    async fn wait_for_lock_waiters(pool: &sqlx::PgPool, expected: i64) {
        let application: String = sqlx::query_scalar("SELECT current_setting('application_name')")
            .fetch_one(pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'"
                ).bind(&application).fetch_one(pool).await.unwrap();
                if waiting >= expected { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("workers must reach their concurrent lock waits");
    }

    #[tokio::test]
    async fn concurrent_first_fanins_merge_once_even_with_opposite_successor_order() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(database.pool())
            .await
            .unwrap();
        // Different first schemas must still lock the same physical tasks table;
        // a connection's default isolation must not keep a pre-lock JOIN snapshot.
        let alternate_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _| {
                let schema = schema.clone();
                Box::pin(async move {
                    sqlx::query(&format!(r#"SET search_path TO pg_catalog, "{schema}""#))
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SELECT set_config('application_name', $1, false)")
                        .bind(&schema)
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET default_transaction_isolation TO 'repeatable read'")
                        .execute(&mut *connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(database.database_url())
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_schema()")
                .fetch_one(&alternate_pool)
                .await
                .unwrap(),
            "pg_catalog"
        );
        let workflow_id = "concurrent-join";
        seed(&repo, workflow_id, "left", Some(42)).await;
        seed(&repo, workflow_id, "right", Some(42)).await;
        let left = repo
            .claim_many(workflow_id, "worker-a", 1)
            .await
            .unwrap()
            .remove(0);
        let right = repo
            .claim_many(workflow_id, "worker-b", 1)
            .await
            .unwrap()
            .remove(0);
        // Freeze ID allocation: both producers reach the missing-JOIN window concurrently.
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT next_task_id FROM task_counters WHERE workflow_id = $1 FOR UPDATE")
            .bind(workflow_id)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let mut workers = tokio::task::JoinSet::new();
        for (source, targets, repo) in [
            (left.clone(), ["join-a", "join-b"], repo.clone()),
            (
                right.clone(),
                ["join-b", "join-a"],
                PgTaskRepo::new(alternate_pool.clone()),
            ),
        ] {
            workers.spawn(async move {
                let fanouts: Vec<_> = targets
                    .into_iter()
                    .map(|name| FanoutInstances {
                        name: name.into(),
                        task_type: "join".into(),
                        instances: vec![(None, json!({ source.name.clone(): source.task_id }))],
                        depends_on_names: vec!["left".into(), "right".into()],
                    })
                    .collect();
                repo.succeed_and_enqueue(&source, &json!(source.task_id), &fanouts, 3)
                    .await
            });
        }
        wait_for_lock_waiters(database.pool(), 2).await;
        blocker.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = workers.join_next().await {
                result.unwrap().unwrap();
            }
        })
        .await
        .expect("opposite successor order must not deadlock");
        let rows = repo.list_all(workflow_id).await.unwrap();
        assert_eq!(rows.len(), 4, "two source tasks and exactly two JOIN tasks");
        for join in rows.iter().filter(|task| task.task_type == "join") {
            assert_eq!(
                join.input,
                json!({"left": left.task_id, "right": right.task_id})
            );
            let mut dependencies = join.depends_on_ids.clone();
            dependencies.sort_unstable();
            assert_eq!(dependencies, [left.task_id, right.task_id]);
        }
        assert_eq!(
            repo.claim_many(workflow_id, "join-worker", 10)
                .await
                .unwrap()
                .len(),
            2
        );
        alternate_pool.close().await;
    }

    #[tokio::test]
    async fn a_lease_that_expires_while_waiting_for_a_row_lock_cannot_be_used() {
        let Some(database) = connect_test().await else {
            return;
        };
        let repo = PgTaskRepo::new(database.pool().clone());
        for operation in 0..4 {
            let workflow_id = format!("lease-lock-wait-{operation}");
            seed(&repo, &workflow_id, "root", None).await;
            let claim = repo
                .claim_many(&workflow_id, "worker", 1)
                .await
                .unwrap()
                .remove(0);
            sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() + interval '300 milliseconds' WHERE workflow_id = $1")
                .bind(&workflow_id).execute(database.pool()).await.unwrap();
            let mut blocker = database.pool().begin().await.unwrap();
            sqlx::query("SELECT task_id FROM tasks WHERE workflow_id = $1 FOR UPDATE")
                .bind(&workflow_id)
                .execute(&mut *blocker)
                .await
                .unwrap();
            let worker_repo = repo.clone();
            let worker = tokio::spawn(async move {
                match operation {
                    0 => worker_repo.renew_claim(&claim).await,
                    1 => worker_repo.release_claim(&claim).await,
                    2 => worker_repo.succeed(&claim, &json!({})).await.map(|_| ()),
                    _ => {
                        worker_repo
                            .fail_and_enqueue(&claim, "expired", &[], 3)
                            .await
                    }
                }
            });
            wait_for_lock_waiters(database.pool(), 1).await;
            tokio::time::sleep(Duration::from_millis(350)).await;
            blocker.commit().await.unwrap();
            assert!(matches!(
                worker.await.unwrap(),
                Err(StorageError::ClaimLost { .. })
            ));
            let rows = repo.list_all(&workflow_id).await.unwrap();
            assert_eq!(rows[0].status, TaskStatus::Running);
            assert!(rows[0].output.is_none());
        }
    }

    #[test]
    fn join_input_merges_distinct_keys() {
        let mut acc = json!({"narration": {"run_id": 1, "ended": false}});
        merge_join_input(
            &mut acc,
            &json!({"action_options": {"run_id": 1, "options": []}}),
        );
        assert_eq!(
            acc,
            json!({
                "narration": {"run_id": 1, "ended": false},
                "action_options": {"run_id": 1, "options": []},
            })
        );
    }

    #[test]
    fn join_input_merges_same_key_into_array() {
        // 同名前驱多次贡献（多实例扇入）→ 合并为数组。
        let mut acc = json!({"image": {"prompt": "a"}});
        merge_join_input(&mut acc, &json!({"image": {"prompt": "b"}}));
        assert_eq!(
            acc,
            json!({
                "image": [
                    {"prompt": "a"},
                    {"prompt": "b"},
                ]
            })
        );
    }
}
