//! scope 实例创建与父级恢复，随任务结算事务提交。

use super::repo::allocate_task_id;
use crate::{
    error::StorageError,
    task::scope::{ScopeEntries, ScopeRecord},
};

pub(super) async fn chain(
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
    source_path: &[String],
    destination_path: &[String],
    trigger_task_id: i64,
    item: Option<(String, usize)>,
    entries: &mut ScopeEntries,
) -> Result<Option<i64>, StorageError> {
    let chain = chain(tx, workflow_id, source_scope_id).await?;
    if chain.iter().any(|scope| scope.run_id != run_id)
        || chain
            .iter()
            .map(|scope| &scope.subgraph_id)
            .ne(source_path.iter())
    {
        return Err(StorageError::InvalidFanout {
            detail: "source scope does not match run or subgraph path".into(),
        });
    }
    let common = source_path
        .iter()
        .zip(destination_path)
        .take_while(|(a, b)| a == b)
        .count();
    if item.is_some() && common == destination_path.len() {
        return Err(StorageError::InvalidFanout {
            detail: "foreach must enter a subgraph".into(),
        });
    }
    let mut scope_id = common.checked_sub(1).map(|index| chain[index].scope_id);
    for subgraph_id in &destination_path[common..] {
        let key = (scope_id, subgraph_id.clone(), item.clone());
        scope_id = Some(if let Some(id) = entries.get(&key) {
            *id
        } else {
            let id = allocate_task_id(tx, workflow_id).await?;
            sqlx::query("INSERT INTO scopes (workflow_id, scope_id, run_id, subgraph_id, parent_scope_id, trigger_task_id) VALUES (?1,?2,?3,?4,?5,?6)")
                .bind(workflow_id).bind(id).bind(run_id).bind(subgraph_id).bind(scope_id).bind(trigger_task_id)
                .execute(&mut **tx).await?;
            entries.insert(key, id);
            id
        });
    }
    Ok(scope_id)
}
