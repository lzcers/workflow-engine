//! SQLite 通用工作流持久化实现：所有写事务统一 `BEGIN IMMEDIATE`、零 `julianday()`、批量写入。

use std::{collections::HashMap, time::Duration};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::{QueryBuilder, SqlitePool, types::Json};

use super::{rows::TaskRow, scope};
use crate::backend::{
    BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR, ROW_COLS, claim_lost,
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

const TASK_BATCH_CHUNK: usize = 100;
const CONTRIB_BATCH_CHUNK: usize = 150;

#[derive(sqlx::FromRow)]
struct RunTaskRow {
    task_id: i64,
    name: String,
    #[sqlx(try_from = "String")]
    status: TaskStatus,
    scope_id: Option<i64>,
    collecting_input: Option<Value>,
}

impl From<RunTaskRow> for RunTask {
    fn from(row: RunTaskRow) -> Self {
        let contributed_slots = match row.collecting_input {
            Some(Value::Object(map)) => map.into_iter().map(|(k, _)| k).collect(),
            _ => Vec::new(),
        };
        Self {
            task_id: row.task_id,
            name: row.name,
            status: row.status,
            scope_id: row.scope_id,
            contributed_slots,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ScopedTreeRow {
    scope_id: i64,
    parent_scope_id: Option<i64>,
    trigger_task_id: i64,
    is_descendant: i64,
}

/// SQLite task repository shared across workflow graphs.
#[derive(Clone)]
pub(crate) struct SqliteTaskRepo {
    pub(super) pool: SqlitePool,
}

impl Database<&mut sqlx::Transaction<'_, sqlx::Sqlite>> {
    /// 提交独立运行的入口；调用方使用 `BEGIN IMMEDIATE` 事务可避免读升写触发 `SQLITE_BUSY`。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        let tx = self.0;
        let max_attempts = task.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1);
        let run_id = sqlx::query(
            "INSERT INTO tasks (workflow_id, name, status, input_json, max_attempts)
             VALUES (?1, ?2, 'pending', ?3, ?4)",
        )
        .bind(&task.workflow_id)
        .bind(&task.name)
        .bind(&task.input)
        .bind(max_attempts)
        .execute(&mut **tx)
        .await?
        .last_insert_rowid();
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
        sqlx::query("UPDATE tasks SET run_id = task_id, scope_id = ?1 WHERE task_id = ?2")
            .bind(scope_id)
            .bind(run_id)
            .execute(&mut **tx)
            .await?;
        Ok(RunHandle {
            workflow_id: task.workflow_id,
            run_id,
        })
    }
}

struct SqliteTx<'a, 'c>(&'a mut sqlx::Transaction<'c, sqlx::Sqlite>);

impl<'c> BackendTx for SqliteTx<'_, 'c> {
    fn count_run_tasks<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<usize, StorageError>> {
        Box::pin(async move {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tasks WHERE workflow_id = ?1 AND run_id = ?2",
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
            let parent_json = serde_json::to_string(&[source.task_id]).unwrap();
            for chunk in rows.chunks(TASK_BATCH_CHUNK) {
                let mut qb = QueryBuilder::<sqlx::Sqlite>::new(
                    "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id, max_attempts) ",
                );
                qb.push_values(chunk, |mut b, row| {
                    b.push_bind(source.workflow_id)
                        .push_bind(source.run_id)
                        .push_bind(row.dst)
                        .push_bind(row.status.as_str())
                        .push_bind(&parent_json)
                        .push_bind(row.input)
                        .push_bind(row.scope_id)
                        .push_bind(row.max_attempts);
                });
                qb.build().execute(&mut **self.0).await?;
            }
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
            let row = sqlx::query_as::<_, (i64, Json<Vec<i64>>, Value)>(
                "SELECT task_id, parent_task_ids, input_json FROM tasks
                 WHERE workflow_id = ?1 AND run_id = ?2 AND status = 'collecting'
                   AND scope_id IS ?3 AND name = ?4
                 ORDER BY task_id LIMIT 1",
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(scope_id)
            .bind(name)
            .fetch_optional(&mut **self.0)
            .await?;
            Ok(row.map(|(id, Json(parents), input)| (id, parents, input)))
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
            let id = sqlx::query(
                "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id, max_attempts)
                 VALUES (?1, ?2, ?3, 'collecting', ?4, ?5, ?6, ?7)",
            )
            .bind(source.workflow_id)
            .bind(source.run_id)
            .bind(dst)
            .bind(Json([source.task_id]))
            .bind(input)
            .bind(scope_id)
            .bind(max_attempts)
            .execute(&mut **self.0)
            .await?
            .last_insert_rowid();
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
                "UPDATE tasks SET parent_task_ids = ?3, input_json = ?4,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE workflow_id = ?1 AND task_id = ?2",
            )
            .bind(workflow_id)
            .bind(task_id)
            .bind(Json(parents))
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
            for chunk in values.chunks(CONTRIB_BATCH_CHUNK) {
                let mut qb = QueryBuilder::<sqlx::Sqlite>::new(
                    "INSERT INTO join_contributions (workflow_id, task_id, slot_key, source_task_id, value_json) ",
                );
                qb.push_values(chunk, |mut b, value| {
                    b.push_bind(workflow_id)
                        .push_bind(join_task_id)
                        .push_bind(slot_key)
                        .push_bind(source_task_id)
                        .push_bind(value);
                });
                qb.build().execute(&mut **self.0).await?;
            }
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
                WHERE workflow_id = ?1 AND run_id = ?2 AND status = 'collecting'
                  AND ((?3 AND scope_id IS NULL) OR scope_id IN (SELECT value FROM json_each(?4)))
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(include_root)
            .bind(Json(&non_null))
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
                           CASE WHEN status = 'collecting' THEN input_json ELSE NULL END AS collecting_input
                    FROM tasks
                    WHERE workflow_id = ?1 AND run_id = ?2
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
                     WHERE workflow_id = ?1 AND run_id = ?2",
                )
                .bind(workflow_id)
                .bind(run_id)
                .fetch_all(&mut **self.0)
                .await?;
                return Ok((tasks, scopes));
            }

            let seed_ids: Vec<i64> = collecting_scopes.iter().filter_map(|s| *s).collect();
            let tree = sqlx::query_as::<_, ScopedTreeRow>(
                r#"
                WITH RECURSIVE
                descendants(scope_id, parent_scope_id, trigger_task_id) AS (
                    SELECT scope_id, parent_scope_id, trigger_task_id
                    FROM scopes
                    WHERE workflow_id = ?1 AND run_id = ?2
                      AND scope_id IN (SELECT value FROM json_each(?3))
                    UNION ALL
                    SELECT s.scope_id, s.parent_scope_id, s.trigger_task_id
                    FROM scopes s
                    JOIN descendants d ON s.workflow_id = ?1 AND s.run_id = ?2 AND s.parent_scope_id = d.scope_id
                ),
                ancestors(scope_id, parent_scope_id, trigger_task_id) AS (
                    SELECT scope_id, parent_scope_id, trigger_task_id
                    FROM scopes
                    WHERE workflow_id = ?1 AND run_id = ?2
                      AND scope_id IN (SELECT value FROM json_each(?3))
                    UNION ALL
                    SELECT s.scope_id, s.parent_scope_id, s.trigger_task_id
                    FROM scopes s
                    JOIN ancestors a ON s.workflow_id = ?1 AND s.run_id = ?2 AND s.scope_id = a.parent_scope_id
                )
                SELECT scope_id, parent_scope_id, trigger_task_id, MAX(is_desc) AS is_descendant
                FROM (
                    SELECT scope_id, parent_scope_id, trigger_task_id, 1 AS is_desc FROM descendants
                    UNION ALL
                    SELECT scope_id, parent_scope_id, trigger_task_id, 0 AS is_desc FROM ancestors
                )
                GROUP BY scope_id, parent_scope_id, trigger_task_id
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(Json(&seed_ids))
            .fetch_all(&mut **self.0)
            .await?;

            let mut descendant_scopes = Vec::new();
            let mut trigger_ids = Vec::new();
            let mut scopes = Vec::with_capacity(tree.len());
            for row in tree {
                if row.is_descendant != 0 {
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
                       CASE WHEN status = 'collecting' THEN input_json ELSE NULL END AS collecting_input
                FROM tasks
                WHERE workflow_id = ?1 AND run_id = ?2
                  AND (
                      scope_id IN (SELECT value FROM json_each(?3))
                      OR task_id IN (SELECT value FROM json_each(?4))
                  )
                "#,
            )
            .bind(workflow_id)
            .bind(run_id)
            .bind(Json(&descendant_scopes))
            .bind(Json(&trigger_ids))
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
                     SET status = 'failed', error = ?3,
                         lease_until = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
                )
                .bind(workflow_id)
                .bind(Json(&ids))
                .bind(reason)
                .execute(&mut **self.0)
                .await?;
            }
            sqlx::query(
                "DELETE FROM join_contributions
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
            )
            .bind(workflow_id)
            .bind(Json(&all_ids))
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
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
            )
            .bind(workflow_id)
            .bind(Json(task_ids))
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
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))
                 ORDER BY task_id, slot_key, seq",
            )
            .bind(workflow_id)
            .bind(Json(task_ids))
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
                 SET status = 'pending', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
            )
            .bind(workflow_id)
            .bind(Json(task_ids))
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
            let mut ids = Vec::with_capacity(materialized.len());
            for (task_id, input) in materialized {
                ids.push(*task_id);
                sqlx::query(
                    "UPDATE tasks
                     SET status = 'pending', input_json = ?3,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE workflow_id = ?1 AND task_id = ?2",
                )
                .bind(workflow_id)
                .bind(task_id)
                .bind(input)
                .execute(&mut **self.0)
                .await?;
            }
            sqlx::query(
                "DELETE FROM join_contributions
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
            )
            .bind(workflow_id)
            .bind(Json(&ids))
            .execute(&mut **self.0)
            .await?;
            Ok(())
        })
    }
}

/// 仅当前有效认领可以提交终态；定长 UTC 文本直接比较（无 `julianday` 包装）。
async fn settle_in_tx<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &'a TaskRecord,
    status: TaskStatus,
    output: Option<&Value>,
    error: Option<&str>,
) -> Result<FanoutSource<'a>, StorageError> {
    let (run_id, scope_id) = sqlx::query_as(
        r#"
        UPDATE tasks
        SET status = ?3, output_json = ?4, error = ?5, lease_until = NULL, available_at = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE workflow_id = ?1 AND task_id = ?2
          AND status = 'running' AND attempt = ?6
          AND (?3 IN ('failed', 'superseded') OR attempt <= max_attempts)
          AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        RETURNING run_id, scope_id
        "#,
    )
    .bind(&task.workflow_id)
    .bind(task.task_id)
    .bind(status.as_str())
    .bind(output)
    .bind(error)
    .bind(task.attempt)
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

impl SqliteTaskRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl TaskStore for SqliteTaskRepo {
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let sql = format!(
                r#"
                WITH candidate AS MATERIALIZED (
                    SELECT wt.task_id FROM tasks wt
                    WHERE wt.workflow_id = ?1
                      AND wt.status IN ('pending', 'running')
                      AND (wt.lease_until IS NULL OR wt.lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                      AND (wt.available_at IS NULL OR wt.available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                    ORDER BY wt.task_id LIMIT ?3
                )
                UPDATE tasks
                SET status = 'running',
                    error = CASE WHEN attempt >= max_attempts THEN COALESCE(error,
                        CASE WHEN lease_until IS NOT NULL THEN ?4 ELSE ?5 END)
                        ELSE NULL END,
                    available_at = NULL,
                    lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds'),
                    attempt = attempt + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id IN (SELECT task_id FROM candidate)
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let lease_until = sqlx::query_scalar(
                r#"
                UPDATE tasks
                SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND attempt = ?3
                  AND status = 'running'
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let source =
                settle_in_tx(&mut tx, task, TaskStatus::Succeeded, Some(output), None).await?;
            enqueue_fanouts_and_advance(
                &mut SqliteTx(&mut tx),
                &source,
                successors,
                rules,
                config,
                None,
            )
            .await?;
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let status = if successors.iter().all(|plan| plan.inputs.is_empty()) {
                TaskStatus::Failed
            } else {
                TaskStatus::Superseded
            };
            let source = settle_in_tx(&mut tx, task, status, None, Some(message)).await?;
            enqueue_fanouts_and_advance(
                &mut SqliteTx(&mut tx),
                &source,
                successors,
                rules,
                config,
                None,
            )
            .await?;
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET status = 'pending', error = ?3, lease_until = NULL,
                    available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?4
                  AND attempt < max_attempts
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                "#,
            )
            .bind(&task.workflow_id)
            .bind(task.task_id)
            .bind(message)
            .bind(task.attempt)
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let result = sqlx::query(
                r#"
                UPDATE tasks
                SET lease_until = NULL,
                    error = COALESCE(?4, error),
                    available_at = CASE
                        WHEN ?5 IS NOT NULL AND attempt < max_attempts
                        THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds')
                        ELSE available_at
                    END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND task_id = ?2
                  AND status = 'running' AND attempt = ?3
                  AND lease_until > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
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
                WHERE workflow_id IN (SELECT value FROM json_each(?1))
                  AND status IN ('pending', 'running')
                  AND (lease_until IS NULL OR lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                  AND (available_at IS NULL OR available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                "#,
            )
            .bind(Json(workflow_ids))
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
                "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 AND run_id = ?2 ORDER BY task_id"
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
            let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
            let cancelled: Vec<i64> = sqlx::query_scalar(
                r#"
                UPDATE tasks
                SET status = 'cancelled', lease_until = NULL, available_at = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE workflow_id = ?1 AND run_id = ?2
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
                    "DELETE FROM join_contributions
                     WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
                )
                .bind(workflow_id)
                .bind(Json(&cancelled))
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(cancelled.len())
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sqlite.rs"]
mod tests;
