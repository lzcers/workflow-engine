//! 子图实例的父子关系与 JOIN 来源选择；不赋予 scope 独立执行生命周期。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::store::FanoutPlan;
use crate::error::StorageError;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScopeRecord {
    pub workflow_id: String,
    pub scope_id: i64,
    pub run_id: Option<i64>,
    pub subgraph_id: String,
    pub parent_scope_id: Option<i64>,
    pub trigger_task_id: i64,
}

/// 同层按 scope 匹配，外层入口绑定精确任务，子图输出按父实例收集。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDependency {
    pub edge_id: Option<String>,
    pub name: String,
    pub scope_id: Option<i64>,
    pub task_id: Option<i64>,
    pub descendants: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JoinState {
    pub expected: Vec<JoinDependency>,
    pub arrivals: HashMap<String, Vec<i64>>,
    pub upstream: Vec<JoinDependency>,
}

impl JoinState {
    pub(crate) fn from_names(names: &[String], scope_id: Option<i64>) -> Self {
        Self {
            expected: names
                .iter()
                .map(|name| JoinDependency {
                    edge_id: None,
                    name: name.clone(),
                    scope_id,
                    task_id: None,
                    descendants: false,
                })
                .collect(),
            ..Self::default()
        }
    }

    pub(crate) fn arrive(&mut self, edge_id: &str, task_id: i64) {
        let tasks = self.arrivals.entry(edge_id.to_owned()).or_default();
        if !tasks.contains(&task_id) {
            tasks.push(task_id);
        }
    }
}

/// 缓存仅存在于一次原子结算中。普通边共享入口；foreach 按边及项区分。
pub(crate) type ScopeEntries = HashMap<(Option<i64>, String, Option<(String, usize)>), i64>;

pub(crate) fn join_state(
    plan: &FanoutPlan,
    chain: &[ScopeRecord],
    entry_names: &HashMap<i64, String>,
) -> Result<JoinState, StorageError> {
    let target = &plan.destination_subgraph_path;
    let current = chain.last().map(|scope| scope.scope_id);
    let select = |name: &str, edge_id: Option<String>| -> Result<JoinDependency, StorageError> {
        let path = plan.predecessor_subgraph_paths.get(name).ok_or_else(|| {
            StorageError::InvalidFanout {
                detail: format!("missing subgraph path for JOIN source {name}"),
            }
        })?;
        let common = path.iter().zip(target).take_while(|(a, b)| a == b).count();
        let (scope_id, task_id, descendants) = if path == target {
            (current, None, false)
        } else if path.starts_with(target) {
            (current, None, true)
        } else if edge_id.is_some() {
            // 进入这个实例的外部输入只能来自该次入口事件，不能混入其他调用。
            let entry = chain
                .get(common)
                .ok_or_else(|| StorageError::InvalidFanout {
                    detail: format!("JOIN source {name} has no matching entry event"),
                })?;
            (entry.parent_scope_id, Some(entry.trigger_task_id), false)
        } else {
            // 外部上游只作为生产结束屏障，不作为数据贡献者。
            let parent = common.checked_sub(1).map(|index| chain[index].scope_id);
            (parent, None, path.len() > common)
        };
        Ok(JoinDependency {
            edge_id,
            name: name.to_owned(),
            scope_id,
            task_id,
            descendants,
        })
    };
    let mut expected = Vec::new();
    for (edge, name) in &plan.join_incoming_edges {
        let dependency = select(name, Some(edge.clone()))?;
        if let Some(task_id) = dependency.task_id {
            let entry_name =
                entry_names
                    .get(&task_id)
                    .ok_or_else(|| StorageError::InvalidFanout {
                        detail: format!("missing JOIN entry task {task_id}"),
                    })?;
            // 同一子图可被不同外部节点调用；只等待创建当前实例的入口边。
            if entry_name != name {
                continue;
            }
        }
        expected.push(dependency);
    }
    expected.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
    let mut upstream = Vec::new();
    for name in &plan.join_upstream_names {
        let path = plan.predecessor_subgraph_paths.get(name).ok_or_else(|| {
            StorageError::InvalidFanout {
                detail: format!("missing subgraph path for JOIN upstream {name}"),
            }
        })?;
        // 外部上游在本次入口创建前已完成，不等待其他子图调用中的同名任务。
        if path.starts_with(target) {
            upstream.push(select(name, None)?);
        }
    }
    Ok(JoinState {
        expected,
        upstream,
        arrivals: HashMap::new(),
    })
}
