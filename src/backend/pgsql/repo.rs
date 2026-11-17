//! 通用工作流任务 `tasks` 的持久化操作（入队 / 认领 / 结算 / 级联）。
//!
//! 本模块不含任何业务节点名或业务 JSON 语义；业务侧专属查询（按 name / JSONB 字段的
//! 业务语义）应留在消费方，经 [`PgTaskRepo::insert_task`] / [`notify_tasks_ready`]
//! 等公开事务原语与本模块组合。

use serde_json::Value;
use sqlx::PgPool;

use super::rows::TaskRow;
use crate::{FanoutInstances, StorageError, Task, TaskRecord, TaskStatus};

/// 认领租约时长（running 超时后回收为可认领）。
const LEASE_SECONDS: i64 = 300;

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

/// 事务内入队后继实例：单实例扇入合并（`FOR UPDATE` 去重）/ 多实例独立成行
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
    ///   `depends_on_ids`，避免对同一后继重复入队），用 `FOR UPDATE` 锁定命中行保证并发安全；
    ///   否则新建。
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
        let mut tx = self.pool.begin().await?;
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

    /// 原子化成功结算：`succeed` + 全部后继入队**同一事务**，返回回填后的任务行。
    ///
    /// 一次提交内完成「任务置 succeeded + 全部后继入队」：进程中途崩溃时二者要么都在、
    /// 要么都不在，杜绝「任务已成功但后继从未入队」的静默停摆。`RETURNING` 顺带拿回最新行
    /// （含节点 `execute` 内业务回填的 `run_id`），后继 `run_id` 据此落库，
    /// 替代「提交后二次回读」。
    pub async fn succeed_and_enqueue(
        &self,
        workflow_id: &str,
        task_id: i64,
        output: &Value,
        fanouts: &[FanoutInstances],
        max_attempts: i32,
    ) -> Result<TaskRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let return_cols = format!("RETURNING {ROW_COLS}");
        let settled: TaskRow = sqlx::query_as(&format!(
            r#"
            UPDATE tasks
            SET status = 'succeeded', output_json = $3::jsonb, error = NULL,
                lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            {return_cols}
            "#
        ))
        .bind(workflow_id)
        .bind(task_id)
        .bind(output)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StorageError::MissingTask {
            workflow_id: workflow_id.to_string(),
            task_id,
        })?;

        // 成功后继依赖源任务（`depends_on_ids = [settled.task_id]`）。
        for f in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                workflow_id,
                settled.run_id,
                &f.name,
                &f.instances,
                &[settled.task_id],
                &f.task_type,
                &f.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(settled.into())
    }

    /// 原子失败结算：有后继时标记 `superseded` 并入队；无后继时标记 `failed` 并级联
    /// 同一 workflow、同一 run 内依赖源任务的已有 pending 任务。
    /// 失败后继不依赖源任务（`depends_on_ids = []`）。
    #[allow(clippy::too_many_arguments)]
    pub async fn fail_and_enqueue(
        &self,
        workflow_id: &str,
        task_id: i64,
        run_id: Option<i64>,
        error: &str,
        fanouts: &[FanoutInstances],
        max_attempts: i32,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        for f in fanouts {
            enqueue_fanout_in_tx(
                &mut tx,
                workflow_id,
                run_id,
                &f.name,
                &f.instances,
                &[],
                &f.task_type,
                &f.depends_on_names,
                max_attempts,
            )
            .await?;
        }
        let status = if fanouts.is_empty() {
            TaskStatus::Failed
        } else {
            TaskStatus::Superseded
        };
        sqlx::query(
            r#"
            UPDATE tasks
            SET status = $3, error = $4, lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(status.as_str())
        .bind(error)
        .execute(&mut *tx)
        .await?;
        if fanouts.is_empty() {
            sqlx::query(
                r#"
                UPDATE tasks AS dependent
                SET status = 'failed', error = $3, lease_until = NULL, updated_at = NOW()
                FROM tasks AS source
                WHERE source.workflow_id = $1 AND source.task_id = $2
                  AND dependent.workflow_id = source.workflow_id
                  AND dependent.run_id IS NOT DISTINCT FROM source.run_id
                  AND dependent.status = 'pending'
                  AND (source.task_id = ANY(dependent.depends_on_ids)
                       OR source.name = ANY(dependent.depends_on_names))
                "#,
            )
            .bind(workflow_id)
            .bind(task_id)
            .bind(format!("predecessor task {task_id} failed"))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务在下一轮认领即可
    /// 重入，不必等待租约过期。同一事务内发送任务就绪通知，调度器被立即唤醒重认领。
    pub async fn release_claim(&self, workflow_id: &str, task_id: i64) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE tasks
            SET lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;
        notify_tasks_ready(&mut tx, workflow_id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// 释放某个工作流**全部**在跑任务的租约：worker 被逐出（重启）后调用。
    ///
    /// `claim_many` 把 `lease_until IS NULL` 的 running 行同样视为可认领，因此释放后这些任务
    /// 立即回到可认领状态，不必等租约到期（默认 300s）；被中断的那一次认领已经消耗过一次
    /// attempt，重试照常计数，预算耗尽的任务由认领闸门封存为 failed。
    /// 返回被释放的任务数。
    pub async fn release_claims(&self, workflow_id: &str) -> Result<u64, StorageError> {
        let result = sqlx::query(
            r#"
            UPDATE tasks
            SET lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND status = 'running' AND lease_until IS NOT NULL
            "#,
        )
        .bind(workflow_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// 认领一批可执行任务：pending 或已过期的 running 均视为可认领；同一批内互相无依赖
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
        let mut tx = self.pool.begin().await?;

        // 可执行任务的通用闸门（不含 attempt 预算过滤）见 [`EXECUTABLE_WHERE`]；
        // 此处补上 session 过滤，seal / claim 两个 UPDATE 共用同一条闸门。

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
                lease_until = NOW() + make_interval(secs => $3),
                attempt = attempt + 1,
                updated_at = NOW()
            WHERE workflow_id = $1
              AND task_id IN (SELECT task_id FROM candidate)
            RETURNING {ROW_COLS}
            "#
        );
        let rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .bind(worker)
            .bind(LEASE_SECONDS)
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await?;

        tx.commit().await?;
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

    /// 结算为成功：写产物并回读回填后的任务行（含节点 `execute` 内业务回填的 `run_id`），
    /// 供调度器把后继挂在正确分组键上。任务行不存在时报 [`StorageError::MissingTask`]。
    pub async fn succeed(
        &self,
        workflow_id: &str,
        task_id: i64,
        output: &Value,
    ) -> Result<TaskRecord, StorageError> {
        let return_cols = format!("RETURNING {ROW_COLS}");
        let row: Option<TaskRow> = sqlx::query_as(&format!(
            r#"
            UPDATE tasks
            SET status = 'succeeded', output_json = $3::jsonb, error = NULL,
                lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            {return_cols}
            "#
        ))
        .bind(workflow_id)
        .bind(task_id)
        .bind(output)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TaskRecord::from).ok_or(StorageError::MissingTask {
            workflow_id: workflow_id.to_string(),
            task_id,
        })
    }

    /// 结算为失败：记录错误并释放租约（可由回收逻辑改为可重试）。
    pub async fn fail(
        &self,
        workflow_id: &str,
        task_id: i64,
        error: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'failed', error = $3, lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 结算为被后继重试取代：记录原因，但不对外暴露为失败。
    pub async fn supersede(
        &self,
        workflow_id: &str,
        task_id: i64,
        error: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'superseded', error = $3, lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 结算为跳过（消费方语义：如决定不生成某产物）。
    pub async fn skip(
        &self,
        workflow_id: &str,
        task_id: i64,
        reason: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            UPDATE tasks
            SET status = 'skipped', error = $3, lease_until = NULL, updated_at = NOW()
            WHERE workflow_id = $1 AND task_id = $2
            "#,
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
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
    use crate::{FanoutInstances, Task, TaskRecord, TaskStatus};

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
        let source = repo
            .enqueue(&Task {
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
        repo.claim_many(workflow_id, "worker", 5).await.unwrap();

        let settled = repo
            .succeed_and_enqueue(
                workflow_id,
                source.task_id,
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
        let retried = repo
            .enqueue(&Task {
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
        repo.fail_and_enqueue(
            workflow_id,
            retried.task_id,
            None,
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
            repo.fail_and_enqueue(
                workflow_id,
                failed.task_id,
                None,
                "budget exhausted",
                &[],
                3,
            )
            .await
            .is_err()
        );
        for task in std::iter::once(&failed).chain(&dependents) {
            let unchanged = repo.get(workflow_id, task.task_id).await.unwrap().unwrap();
            assert_eq!(unchanged.status, TaskStatus::Pending);
        }
        sqlx::query("ALTER TABLE tasks DROP CONSTRAINT reject_cascade")
            .execute(database.pool())
            .await
            .unwrap();

        repo.fail_and_enqueue(
            workflow_id,
            failed.task_id,
            None,
            "budget exhausted",
            &[],
            3,
        )
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

        repo.release_claim(workflow_id, task.task_id).await.unwrap();

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

        assert_eq!(repo.release_claims("release_claims_all").await.unwrap(), 1);
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
        let finished = insert_task(&repo, done, "narration", vec![]).await;
        repo.succeed(done, finished.task_id, &json!({}))
            .await
            .unwrap();

        // 预算耗尽：attempt >= max_attempts（认领三次后不再被认领）→ 不命中。
        let exhausted = "exec_sess_exhausted";
        insert_task(&repo, exhausted, "narration", vec![]).await;
        for _ in 0..3 {
            repo.claim_many(exhausted, "worker", 5).await.unwrap();
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
        repo.claim_many(workflow_id, "worker", 5).await.unwrap();
        repo.release_claim(workflow_id, root.task_id).await.unwrap();
        assert_eq!(recv_ready(&mut listener, workflow_id).await, workflow_id);
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
