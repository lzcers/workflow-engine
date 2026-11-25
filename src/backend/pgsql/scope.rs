//! scope 实例创建与父级恢复，随任务结算事务提交。

use crate::{
    error::StorageError,
    task::scope::{ScopeEntries, ScopeRecord},
};

pub(super) async fn chain(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    mut scope_id: Option<i64>,
) -> Result<Vec<ScopeRecord>, StorageError> {
    let mut chain = Vec::new();
    while let Some(id) = scope_id {
        let scope = sqlx::query_as::<_, ScopeRecord>(
            "SELECT workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id FROM scopes WHERE workflow_id = $1 AND scope_id = $2"
        ).bind(workflow_id).bind(id).fetch_optional(&mut **tx).await?
          .ok_or_else(|| StorageError::InvalidFanout { detail: format!("unknown scope {id}") })?;
        scope_id = scope.parent_scope_id;
        chain.push(scope);
    }
    chain.reverse();
    Ok(chain)
}

/// 创建路线中新进入的作用域。普通边共享同一结算内已创建的实例。
pub(super) async fn enter(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    run_id: i64,
    trigger_task_id: i64,
    mut parent: Option<i64>,
    entered: &[String],
    mut shared: Option<&mut ScopeEntries>,
) -> Result<Option<i64>, StorageError> {
    for subgraph_id in entered {
        let key = (parent, subgraph_id.clone());
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
