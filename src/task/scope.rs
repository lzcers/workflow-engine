//! 子图实例的父子关系；不赋予 scope 独立执行生命周期。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::StorageError;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScopeRecord {
    pub workflow_id: String,
    pub scope_id: i64,
    pub run_id: i64,
    pub subgraph_id: String,
    pub parent_scope_id: Option<i64>,
    pub trigger_task_id: i64,
}

/// 同一次原子结算中的普通跨入边共享入口；foreach 逐项创建实例，不使用缓存。
pub(crate) type ScopeEntries = HashMap<(Option<i64>, String), i64>;

/// 返回继续执行所在的祖先实例，以及需要新进入的子图路径。
pub(crate) fn route_path<'a>(
    chain: &[ScopeRecord],
    run_id: i64,
    destination: &'a [String],
    foreach: bool,
) -> Result<(Option<i64>, &'a [String]), StorageError> {
    if chain.iter().any(|scope| scope.run_id != run_id) {
        return Err(StorageError::InvalidFanout {
            detail: "source scope does not match run".into(),
        });
    }
    let common = chain
        .iter()
        .zip(destination)
        .take_while(|(scope, id)| scope.subgraph_id == **id)
        .count();
    let parent = chain[..common].last().map(|scope| scope.scope_id);
    let entered = &destination[common..];
    if foreach && entered.is_empty() {
        return Err(StorageError::InvalidFanout {
            detail: "foreach must enter a subgraph".into(),
        });
    }
    Ok((parent, entered))
}
