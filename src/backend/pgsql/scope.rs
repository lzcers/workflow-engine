//! scope 实例批量创建与单条 `WITH RECURSIVE` 父级恢复，随任务结算事务提交。

use std::sync::Arc;

use crate::{
    error::StorageError,
    task::scope::{ScopeEntries, ScopeRecord},
};

/// 单条递归 CTE 加载从最外层到当前 `scope_id` 的完整父子链。
pub(super) async fn chain(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    scope_id: Option<i64>,
) -> Result<Vec<ScopeRecord>, StorageError> {
    let Some(id) = scope_id else {
        return Ok(Vec::new());
    };
    let rows = sqlx::query_as::<_, ScopeRecord>(
        r#"
        WITH RECURSIVE anc(workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id, depth) AS (
            SELECT workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id, 0
            FROM scopes
            WHERE workflow_id = $1 AND scope_id = $2
            UNION ALL
            SELECT s.workflow_id, s.scope_id, s.run_id, s.subgraph_id, s.parent_scope_id, s.trigger_task_id, anc.depth + 1
            FROM scopes s
            JOIN anc ON s.workflow_id = anc.workflow_id AND s.scope_id = anc.parent_scope_id
        )
        SELECT workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id
        FROM anc
        ORDER BY depth DESC
        "#,
    )
    .bind(workflow_id)
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    if rows.is_empty() {
        return Err(StorageError::InvalidFanout {
            detail: format!("unknown scope {id}"),
        });
    }
    Ok(rows)
}

/// 创建路线中新进入的共享作用域；普通边共享同一结算内已创建的实例。
pub(super) async fn enter(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    trigger_task_id: i64,
    mut parent: Option<i64>,
    entered: &[impl AsRef<str>],
    mut shared: Option<&mut ScopeEntries>,
) -> Result<Option<i64>, StorageError> {
    for subgraph in entered {
        let subgraph_id = subgraph.as_ref();
        let key = (parent, subgraph_id.to_owned());
        if let Some(id) = shared.as_ref().and_then(|entries| entries.get(&key)) {
            parent = Some(*id);
            continue;
        }
        let id = sqlx::query_scalar(
            "INSERT INTO scopes (workflow_id, run_id, subgraph_id, parent_scope_id, trigger_task_id)
             VALUES ($1, $2, $3, $4, $5) RETURNING scope_id",
        )
        .bind(workflow_id)
        .bind(run_id)
        .bind(subgraph_id)
        .bind(parent)
        .bind(trigger_task_id)
        .fetch_one(&mut **tx)
        .await?;
        if let Some(entries) = shared.as_deref_mut() {
            entries.insert(key, id);
        }
        parent = Some(id);
    }
    Ok(parent)
}

/// 为 `->!` / `->+` 批量创建 `count` 条独立作用域链（每层仅 1 次 `UNNEST` 往返）。
pub(super) async fn enter_isolated_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    trigger_task_id: i64,
    parent: Option<i64>,
    entered: &[Arc<str>],
    count: usize,
) -> Result<Vec<i64>, StorageError> {
    let mut parents: Vec<Option<i64>> = vec![parent; count];
    let mut leaf_ids = Vec::new();
    for subgraph in entered {
        let ids: Vec<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO scopes (workflow_id, run_id, subgraph_id, parent_scope_id, trigger_task_id)
            SELECT $1, $2, $3, t.p, $4
            FROM unnest($5::bigint[]) WITH ORDINALITY AS t(p, ord)
            ORDER BY t.ord
            RETURNING scope_id
            "#,
        )
        .bind(workflow_id)
        .bind(run_id)
        .bind(subgraph.as_ref())
        .bind(trigger_task_id)
        .bind(&parents)
        .fetch_all(&mut **tx)
        .await?;
        parents = ids.iter().copied().map(Some).collect();
        leaf_ids = ids;
    }
    Ok(leaf_ids)
}
