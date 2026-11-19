//! 子图实例的父子关系与 JOIN 来源选择；不赋予 scope 独立执行生命周期。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::StorageError;
use crate::graph::ErasedGraph;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScopeRecord {
    pub workflow_id: String,
    pub scope_id: i64,
    pub run_id: Option<i64>,
    pub subgraph_id: String,
    pub parent_scope_id: Option<i64>,
    pub trigger_task_id: i64,
}

/// 同一次原子结算中的普通跨入边共享入口；foreach 逐项创建实例，不使用缓存。
pub(crate) type ScopeEntries = HashMap<(Option<i64>, String), i64>;

/// 保留当前子图调用的有效前驱名称。不同外部入口分别调用同一子图时，不互相等待。
pub(crate) fn dependency_names(
    graph: &dyn ErasedGraph,
    destination: &str,
    chain: &[ScopeRecord],
    entry_names: &HashMap<i64, String>,
) -> Result<Vec<String>, StorageError> {
    let unknown_node = |name: &str| StorageError::InvalidFanout {
        detail: format!("unknown node {name}"),
    };
    let target = graph
        .subgraph_path(destination)
        .ok_or_else(|| unknown_node(destination))?;
    let predecessors = graph
        .predecessors_of(destination)
        .ok_or_else(|| unknown_node(destination))?;
    let mut names = Vec::new();
    for name in predecessors {
        let path = graph
            .subgraph_path(&name)
            .ok_or_else(|| unknown_node(&name))?;
        if !path.starts_with(&target) {
            let common = path.iter().zip(&target).take_while(|(a, b)| a == b).count();
            let entry = chain
                .get(common)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("JOIN source {name} has no matching entry event"),
                })?;
            let entry_name = entry_names.get(&entry.trigger_task_id).ok_or_else(|| {
                StorageError::InvalidFanout {
                    detail: format!("missing JOIN entry task {}", entry.trigger_task_id),
                }
            })?;
            if entry_name != &name {
                continue;
            }
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}
