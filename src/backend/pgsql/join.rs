//! PostgreSQL 的 `collecting` JOIN 与 `join_contributions` SQL 原语。

use std::collections::HashMap;

use serde_json::Value;

use super::rows::{RunTaskRow, ScopedTreeRow};
use crate::{
    error::StorageError,
    task::{
        join::{RunScope, RunTask},
        store::FanoutSource,
    },
};

pub(super) async fn find_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    scope_id: Option<i64>,
    name: &str,
) -> Result<Option<(i64, Vec<i64>, Value)>, StorageError> {
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
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row)
}

pub(super) async fn insert_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &FanoutSource<'_>,
    dst: &str,
    scope_id: Option<i64>,
    input: &Value,
    max_attempts: i32,
) -> Result<i64, StorageError> {
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
    .fetch_one(&mut **tx)
    .await?;
    Ok(id)
}

pub(super) async fn update_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    task_id: i64,
    parents: &[i64],
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE tasks SET parent_task_ids = $3, input_json = $4, updated_at = clock_timestamp()
         WHERE workflow_id = $1 AND task_id = $2",
    )
    .bind(workflow_id)
    .bind(task_id)
    .bind(parents)
    .bind(input)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn append_contributions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    join_task_id: i64,
    slot_key: &str,
    source_task_id: i64,
    values: &[Value],
) -> Result<(), StorageError> {
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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn find_collecting_scopes(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    check_scopes: &[Option<i64>],
) -> Result<Vec<Option<i64>>, StorageError> {
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
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

pub(super) async fn load_snapshot(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    collecting_scopes: &[Option<i64>],
) -> Result<(Vec<RunTask>, Vec<RunScope>), StorageError> {
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
        .fetch_all(&mut **tx)
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
        .fetch_all(&mut **tx)
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
    .fetch_all(&mut **tx)
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
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(RunTask::from)
    .collect();
    Ok((tasks, scopes))
}

pub(super) async fn fail(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    failed: &[(i64, &'static str)],
) -> Result<(), StorageError> {
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
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query("DELETE FROM join_contributions WHERE workflow_id = $1 AND task_id = ANY($2)")
        .bind(workflow_id)
        .bind(&all_ids)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn load_inputs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<HashMap<i64, Value>, StorageError> {
    let rows = sqlx::query_as::<_, (i64, Value)>(
        "SELECT task_id, input_json FROM tasks
         WHERE workflow_id = $1 AND task_id = ANY($2)",
    )
    .bind(workflow_id)
    .bind(task_ids)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().collect())
}

pub(super) async fn load_contributions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<HashMap<i64, HashMap<String, Vec<Value>>>, StorageError> {
    let rows = sqlx::query_as::<_, (i64, String, Value)>(
        "SELECT task_id, slot_key, value_json FROM join_contributions
         WHERE workflow_id = $1 AND task_id = ANY($2)
         ORDER BY task_id, slot_key, seq",
    )
    .bind(workflow_id)
    .bind(task_ids)
    .fetch_all(&mut **tx)
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
}

pub(super) async fn mark_pending(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE tasks
         SET status = 'pending', updated_at = clock_timestamp()
         WHERE workflow_id = $1 AND task_id = ANY($2)",
    )
    .bind(workflow_id)
    .bind(task_ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn materialize_and_ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    materialized: &[(i64, Value)],
) -> Result<(), StorageError> {
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
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM join_contributions WHERE workflow_id = $1 AND task_id = ANY($2)")
        .bind(workflow_id)
        .bind(&ids)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
