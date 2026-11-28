//! SQLite 的 `collecting` JOIN 与 `join_contributions` SQL 原语。

use std::collections::HashMap;

use serde_json::Value;
use sqlx::{QueryBuilder, types::Json};

use super::rows::{RunTaskRow, ScopedTreeRow};
use crate::{
    error::StorageError,
    task::{
        join::{RunScope, RunTask},
        store::FanoutSource,
    },
};

const CONTRIB_BATCH_CHUNK: usize = 150;

pub(super) async fn find_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: i64,
    scope_id: Option<i64>,
    name: &str,
) -> Result<Option<(i64, Vec<i64>, Value)>, StorageError> {
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
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|(id, Json(parents), input)| (id, parents, input)))
}

pub(super) async fn insert_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    source: &FanoutSource<'_>,
    dst: &str,
    scope_id: Option<i64>,
    input: &Value,
    max_attempts: i32,
) -> Result<i64, StorageError> {
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
    .execute(&mut **tx)
    .await?
    .last_insert_rowid();
    Ok(id)
}

pub(super) async fn update_collecting(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    task_id: i64,
    parents: &[i64],
    input: &Value,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE tasks SET parent_task_ids = ?3, input_json = ?4,
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE workflow_id = ?1 AND task_id = ?2",
    )
    .bind(workflow_id)
    .bind(task_id)
    .bind(Json(parents))
    .bind(input)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn append_contributions(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    join_task_id: i64,
    slot_key: &str,
    source_task_id: i64,
    values: &[Value],
) -> Result<(), StorageError> {
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
        qb.build().execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn find_collecting_scopes(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
        WHERE workflow_id = ?1 AND run_id = ?2 AND status = 'collecting'
          AND ((?3 AND scope_id IS NULL) OR scope_id IN (SELECT value FROM json_each(?4)))
        "#,
    )
    .bind(workflow_id)
    .bind(run_id)
    .bind(include_root)
    .bind(Json(&non_null))
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

pub(super) async fn load_snapshot(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: i64,
    collecting_scopes: &[Option<i64>],
) -> Result<(Vec<RunTask>, Vec<RunScope>), StorageError> {
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
        .fetch_all(&mut **tx)
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
        .fetch_all(&mut **tx)
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
    .fetch_all(&mut **tx)
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
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(RunTask::from)
    .collect();
    Ok((tasks, scopes))
}

pub(super) async fn fail(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
             SET status = 'failed', error = ?3,
                 lease_until = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
        )
        .bind(workflow_id)
        .bind(Json(&ids))
        .bind(reason)
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(
        "DELETE FROM join_contributions
         WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
    )
    .bind(workflow_id)
    .bind(Json(&all_ids))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn load_inputs(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<HashMap<i64, Value>, StorageError> {
    let rows = sqlx::query_as::<_, (i64, Value)>(
        "SELECT task_id, input_json FROM tasks
         WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
    )
    .bind(workflow_id)
    .bind(Json(task_ids))
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().collect())
}

pub(super) async fn load_contributions(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<HashMap<i64, HashMap<String, Vec<Value>>>, StorageError> {
    let rows = sqlx::query_as::<_, (i64, String, Value)>(
        "SELECT task_id, slot_key, value_json FROM join_contributions
         WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))
         ORDER BY task_id, slot_key, seq",
    )
    .bind(workflow_id)
    .bind(Json(task_ids))
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
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    task_ids: &[i64],
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE tasks
         SET status = 'pending', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
    )
    .bind(workflow_id)
    .bind(Json(task_ids))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn materialize_and_ready(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    materialized: &[(i64, Value)],
) -> Result<(), StorageError> {
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
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(
        "DELETE FROM join_contributions
         WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
    )
    .bind(workflow_id)
    .bind(Json(&ids))
    .execute(&mut **tx)
    .await?;
    Ok(())
}
