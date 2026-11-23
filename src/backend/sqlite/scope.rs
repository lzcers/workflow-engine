//! scope 实例创建与父级恢复，随任务结算事务提交。

use crate::{
    error::StorageError,
    task::scope::{ScopeEntries, ScopeRecord},
};

async fn chain(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    mut scope_id: Option<i64>,
) -> Result<Vec<ScopeRecord>, StorageError> {
    let mut chain = Vec::new();
    while let Some(id) = scope_id {
        let scope = sqlx::query_as::<_, ScopeRecord>(
            "SELECT workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id FROM scopes WHERE workflow_id = ?1 AND scope_id = ?2"
        ).bind(workflow_id).bind(id).fetch_optional(&mut **tx).await?
          .ok_or_else(|| StorageError::InvalidFanout { detail: format!("unknown scope {id}") })?;
        scope_id = scope.parent_scope_id;
        chain.push(scope);
    }
    chain.reverse();
    Ok(chain)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn route(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    workflow_id: &str,
    run_id: Option<i64>,
    source_scope_id: Option<i64>,
    destination_path: &[String],
    trigger_task_id: i64,
    foreach: bool,
    entries: &mut ScopeEntries,
) -> Result<Option<i64>, StorageError> {
    let chain = chain(tx, workflow_id, source_scope_id).await?;
    let (mut parent, entered) =
        crate::task::scope::route_path(&chain, run_id, destination_path, foreach)?;
    for subgraph_id in entered {
        let key = (parent, subgraph_id.clone());
        if !foreach && let Some(&id) = entries.get(&key) {
            parent = Some(id);
            continue;
        }
        let id = sqlx::query_scalar(
            "INSERT INTO scopes (workflow_id, run_id, subgraph_id, parent_scope_id, trigger_task_id)
             VALUES (?1,?2,?3,?4,?5) RETURNING scope_id",
        )
        .bind(workflow_id)
        .bind(run_id)
        .bind(subgraph_id)
        .bind(parent)
        .bind(trigger_task_id)
        .fetch_one(&mut **tx)
        .await?;
        if !foreach {
            entries.insert(key, id);
        }
        parent = Some(id);
    }
    Ok(parent)
}
