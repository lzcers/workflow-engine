//! 通用工作流任务 `tasks` 的 PostgreSQL 持久化实现：薄 SQL 方言 + 批量写入 + 作用域级锁 + NOTIFY。

use std::{collections::HashMap, time::Duration};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::PgPool;

use super::{rows::TaskRow, scope};
use crate::backend::{
    BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR, PG_NOTIFY_CHANNEL, ROW_COLS, claim_lost,
    common::{BackendTx, NewTaskRow, enqueue_fanouts_and_advance},
};
use crate::error::StorageError;
use crate::task::{
    DEFAULT_MAX_ATTEMPTS, SettlementConfig, SettlementRules, Successor, TaskStore,
    join::{RunScope, RunTask},
    scope::{ScopeEntries, ScopeRecord},
    store::FanoutSource,
};
use crate::{Database, RunHandle, Task, TaskRecord, TaskStatus};

#[derive(sqlx::FromRow)]
struct RunTaskRow {
    task_id: i64,
    name: String,
    #[sqlx(try_from = "String")]
    status: TaskStatus,
    scope_id: Option<i64>,
    contributed_slots: Vec<String>,
}

impl From<RunTaskRow> for RunTask {
    fn from(row: RunTaskRow) -> Self {
        Self {
            task_id: row.task_id,
            name: row.name,
            status: row.status,
            scope_id: row.scope_id,
            contributed_slots: row.contributed_slots,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ScopedTreeRow {
    scope_id: i64,
    parent_scope_id: Option<i64>,
    trigger_task_id: i64,
    is_descendant: bool,
}

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

struct PgTx<'a, 'c>(&'a mut sqlx::Transaction<'c, sqlx::Postgres>);

impl<'c> BackendTx for PgTx<'_, 'c> {
    fn count_run_tasks<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<usize, StorageError>> {
        Box::pin(async move {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tasks WHERE workflow_id = $1 AND run_id = $2",
            )
            .bind(workflow_id)
            .bind(run_id)
            .fetch_one(&mut **self.0)
            .await?;
            Ok(usize::try_from(count).unwrap_or(usize::MAX))
        })
    }

    fn load_scope_chain<'a>(
        &'a mut self,
        workflow_id: &'a str,
        scope_id: Option<i64>,
    ) -> BoxFuture<'a, Result<Vec<ScopeRecord>, StorageError>> {
        Box::pin(async move { scope::chain(self.0, workflow_id, scope_id).await })
    }

    fn enter_shared_scope<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [std::sync::Arc<str>],
        shared: &'a mut ScopeEntries,
    ) -> BoxFuture<'a, Result<Option<i64>, StorageError>> {
        Box::pin(async move {
            scope::enter(
                self.0,
                workflow_id,
                run_id,
                trigger_task_id,
                parent,
                entered,
                Some(shared),
            )
            .await
        })
    }

    fn enter_isolated_scopes_batch<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [std::sync::Arc<str>],
        count: usize,
    ) -> BoxFuture<'a, Result<Vec<i64>, StorageError>> {
        Box::pin(async move {
            scope::enter_isolated_batch(
                self.0,
                workflow_id,
                run_id,
                trigger_task_id,
                parent,
                entered,
                count,
            )
            .await
        })
    }

    fn insert_tasks_batch<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        rows: &'a [NewTaskRow<'a>],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if rows.is_empty() {
                return Ok(());
            }
            let names: Vec<&str> = rows.iter().map(|r| r.dst).collect();
            let statuses: Vec<&str> = rows.iter().map(|r| r.status.as_str()).collect();
            let inputs: Vec<&Value> = rows.iter().map(|r| r.input).collect();
            let scopes: Vec<Option<i64>> = rows.iter().map(|r| r.scope_id).collect();
            let max_attempts: Vec<i32> = rows.iter().map(|r| r.max_attempts).collect();
            sqlx::query(
                r#"
                INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id, max_attempts)
                SELECT $1, $2, t.name, t.status, $3, t.input, t.scope_id, t.max_attempts
                FROM unnest($4::text[], $5::text[], $6::jsonb[], $7::bigint[], $8::int[])
                     AS t(name, status, input, scope_id, max_attempts)
                "#,
            )
            .bind(source.workflow_id)
            .bind(source.run_id)
            .bind([source.task_id].as_slice())
            .bind(&names)
            .bind(&statuses)
            .bind(&inputs)
            .bind(&scopes)
            .bind(&max_attempts)
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }

    fn find_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<(i64, Vec<i64>, Value)>, StorageError>> {
        Box::pin(async move {
            let row = sqlx::query_as::<_, (i64, Vec<i64>, Value)>(
                "SELECT task_id, parent_task_ids, input_json FROM tasks
                 WHERE workflow_id = $1 AND run_id = $2 AND status = 'collecting'
                   AND scope_id IS NOT DISTINCT FROM $3 AND name = $4
                 ORDER BY task_id LIMIT 1 FOR UPDATE",
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(scope_id)
            .bind(name)
            .fetch_optional(&mut **self.0)
            .await?;
            Ok(row)
        })
    }

    fn insert_collecting_join<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        dst: &'a str,
        scope_id: Option<i64>,
        input: &'a Value,
        max_attempts: i32,
    ) -> BoxFuture<'a, Result<i64, StorageError>> {
        Box::pin(async move {
            let id = sqlx::query_scalar(
                "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id, max_attempts)
                 VALUES ($1, $2, $3, 'collecting', $4, $5, $6, $7)
                 RETURNING task_id",
            )
            .bind(source.workflow_id)
            .bind(source.run_id)
            .bind(dst)
            .bind([source.task_id].as_slice())
            .bind(input)
            .bind(scope_id)
            .bind(max_attempts)
            .fetch_one(&mut **self.0)
            .await?;
            Ok(id)
        })
    }

    fn update_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_id: i64,
        parents: &'a [i64],
        input: &'a Value,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE tasks SET parent_task_ids = $3, input_json = $4, updated_at = clock_timestamp()
                 WHERE workflow_id = $1 AND task_id = $2",
            )
            .bind(workflow_id)
            .bind(task_id)
            .bind(parents)
            .bind(input)
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }

    fn append_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        join_task_id: i64,
        slot_key: &'a str,
        source_task_id: i64,
        values: &'a [Value],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if values.is_empty() {
                return Ok(());
            }
            sqlx::query(
                r#"
                INSERT INTO join_contributions (workflow_id, task_id, slot_key, source_task_id, value_json)
                SELECT $1, $2, $3, $4, t.val
                FROM unnest($5::jsonb[]) WITH ORDINALITY AS t(val, ord)
                ORDER BY t.ord
                "#,
            )
            .bind(workflow_id)
            .bind(join_task_id)
            .bind(slot_key)
            .bind(source_task_id)
            .bind(values)
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }

    fn find_collecting_scopes<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        check_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<Vec<Option<i64>>, StorageError>> {
        Box::pin(async move {
            let include_root = check_scopes.contains(&None);
            let non_null: Vec<i64> = check_scopes.iter().filter_map(|s| *s).collect();
            let rows: Vec<Option<i64>> = sqlx::query_scalar(
                r#"
                SELECT DISTINCT scope_id
                FROM tasks
                WHERE workflow_id = $1 AND run_id = $2 AND status = 'collecting'
                  AND (($3 AND scope_id IS NULL) OR scope_id = ANY($4))
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(include_root)
            .bind(&non_null)
            .fetch_all(&mut **self.0)
            .await?;
            Ok(rows)
        })
    }

    fn load_join_snapshot<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        collecting_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<(Vec<RunTask>, Vec<RunScope>), StorageError>> {
        Box::pin(async move {
            if collecting_scopes.contains(&None) {
                let tasks: Vec<RunTask> = sqlx::query_as::<_, RunTaskRow>(
                    r#"
                    SELECT task_id, name, status, scope_id,
                           CASE WHEN status = 'collecting'
                                THEN ARRAY(SELECT jsonb_object_keys(input_json))
                                ELSE '{}'::text[]
                           END AS contributed_slots
                    FROM tasks
                    WHERE workflow_id = $1 AND run_id = $2
                    "#,
                )
                .bind(workflow_id)
                .bind(run_id)
                .fetch_all(&mut **self.0)
                .await?
                .into_iter()
                .map(RunTask::from)
                .collect();
                let scopes = sqlx::query_as::<_, RunScope>(
                    "SELECT scope_id, parent_scope_id, trigger_task_id FROM scopes
                     WHERE workflow_id = $1 AND run_id = $2",
                )
                .bind(workflow_id)
                .bind(run_id)
                .fetch_all(&mut **self.0)
                .await?;
                return Ok((tasks, scopes));
            }

            // 仅加载相关子图实例的后代与祖先链，避免大扇出时扫描无关兄弟作用域。
            let seed_ids: Vec<i64> = collecting_scopes.iter().filter_map(|s| *s).collect();
            let tree = sqlx::query_as::<_, ScopedTreeRow>(
                r#"
                WITH RECURSIVE
                descendants AS (
                    SELECT scope_id, parent_scope_id, trigger_task_id
                    FROM scopes
                    WHERE workflow_id = $1 AND run_id = $2 AND scope_id = ANY($3)
                    UNION ALL
                    SELECT s.scope_id, s.parent_scope_id, s.trigger_task_id
                    FROM scopes s
                    JOIN descendants d ON s.workflow_id = $1 AND s.run_id = $2 AND s.parent_scope_id = d.scope_id
                ),
                ancestors AS (
                    SELECT scope_id, parent_scope_id, trigger_task_id
                    FROM scopes
                    WHERE workflow_id = $1 AND run_id = $2 AND scope_id = ANY($3)
                    UNION ALL
                    SELECT s.scope_id, s.parent_scope_id, s.trigger_task_id
                    FROM scopes s
                    JOIN ancestors a ON s.workflow_id = $1 AND s.run_id = $2 AND s.scope_id = a.parent_scope_id
                )
                SELECT scope_id, parent_scope_id, trigger_task_id, bool_or(is_desc) AS is_descendant
                FROM (
                    SELECT scope_id, parent_scope_id, trigger_task_id, TRUE AS is_desc FROM descendants
                    UNION ALL
                    SELECT scope_id, parent_scope_id, trigger_task_id, FALSE AS is_desc FROM ancestors
                ) combined
                GROUP BY scope_id, parent_scope_id, trigger_task_id
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(&seed_ids)
            .fetch_all(&mut **self.0)
            .await?;

            let mut descendant_scopes = Vec::new();
            let mut trigger_ids = Vec::new();
            let mut scopes = Vec::with_capacity(tree.len());
            for row in tree {
                if row.is_descendant {
                    descendant_scopes.push(row.scope_id);
                }
                trigger_ids.push(row.trigger_task_id);
                scopes.push(RunScope {
                    scope_id: row.scope_id,
                    parent_scope_id: row.parent_scope_id,
                    trigger_task_id: row.trigger_task_id,
                });
            }
            trigger_ids.sort_unstable();
            trigger_ids.dedup();

            let tasks: Vec<RunTask> = sqlx::query_as::<_, RunTaskRow>(
                r#"
                SELECT task_id, name, status, scope_id,
                       CASE WHEN status = 'collecting'
                            THEN ARRAY(SELECT jsonb_object_keys(input_json))
                            ELSE '{}'::text[]
                       END AS contributed_slots
                FROM tasks
                WHERE workflow_id = $1 AND run_id = $2
                  AND (scope_id = ANY($3) OR task_id = ANY($4))
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(&descendant_scopes)
            .bind(&trigger_ids)
            .fetch_all(&mut **self.0)
            .await?
            .into_iter()
            .map(RunTask::from)
            .collect();
            Ok((tasks, scopes))
        })
    }

    fn fail_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        failed: &'a [(i64, &'static str)],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let mut by_reason: HashMap<&str, Vec<i64>> = HashMap::new();
            let mut all_ids = Vec::with_capacity(failed.len());
            for &(task_id, reason) in failed {
                by_reason.entry(reason).or_default().push(task_id);
                all_ids.push(task_id);
            }
            for (reason, ids) in by_reason {
                sqlx::query(
                    "UPDATE tasks
                     SET status = 'failed', error = $3,
                         lease_until = NULL, updated_at = clock_timestamp()
                     WHERE workflow_id = $1 AND task_id = ANY($2)",
                )
                .bind(workflow_id)
                .bind(&ids)
                .bind(reason)
                .execute(&mut **self.0)
                .await?;
            }
            sqlx::query(
                "DELETE FROM join_contributions WHERE workflow_id = $1 AND task_id = ANY($2)",
            )
            .bind(workflow_id)
            .bind(&all_ids)
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }

    fn load_task_inputs<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, Value>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query_as::<_, (i64, Value)>(
                "SELECT task_id, input_json FROM tasks
                 WHERE workflow_id = $1 AND task_id = ANY($2)",
            )
            .bind(workflow_id)
            .bind(task_ids)
            .fetch_all(&mut **self.0)
            .await?;
            Ok(rows.into_iter().collect())
        })
    }

    fn load_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, HashMap<String, Vec<Value>>>, StorageError>> {
        Box::pin(async move {
            let rows = sqlx::query_as::<_, (i64, String, Value)>(
                "SELECT task_id, slot_key, value_json FROM join_contributions
                 WHERE workflow_id = $1 AND task_id = ANY($2)
                 ORDER BY task_id, slot_key, seq",
            )
            .bind(workflow_id)
            .bind(task_ids)
            .fetch_all(&mut **self.0)
            .await?;
            let mut grouped: HashMap<i64, HashMap<String, Vec<Value>>> = HashMap::new();
            for (task_id, slot_key, value) in rows {
                grouped
                    .entry(task_id)
                    .or_default()
                    .entry(slot_key)
                    .or_default()
                    .push(value);
            }
            Ok(grouped)
        })
    }

    fn mark_joins_pending<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE tasks
                 SET status = 'pending', updated_at = clock_timestamp()
                 WHERE workflow_id = $1 AND task_id = ANY($2)",
            )
            .bind(workflow_id)
            .bind(task_ids)
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }

    fn materialize_and_ready_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        materialized: &'a [(i64, Value)],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if materialized.is_empty() {
                return Ok(());
            }
            let ids: Vec<i64> = materialized.iter().map(|(id, _)| *id).collect();
            let inputs: Vec<&Value> = materialized.iter().map(|(_, v)| v).collect();
            sqlx::query(
                r#"
                UPDATE tasks t
                SET status = 'pending', input_json = v.input, updated_at = clock_timestamp()
                FROM unnest($2::bigint[], $3::jsonb[]) AS v(task_id, input)
                WHERE t.workflow_id = $1 AND t.task_id = v.task_id
                "#,
            )
            .bind(workflow_id)
            .bind(&ids)
            .bind(&inputs)
            .execute(&mut **self.0)
            .await?;
            sqlx::query(
                "DELETE FROM join_contributions WHERE workflow_id = $1 AND task_id = ANY($2)",
            )
            .bind(workflow_id)
            .bind(&ids)
            .execute(&mut **self.0)
            .await?;
            Ok(())
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
