//! 引擎统一定义的任务存储契约、后继分发计划与静态结算规则。

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RunHandle, Task, TaskRecord};
use crate::error::StorageError;

/// 认领与续租的统一租约有效期。
pub(crate) const LEASE_DURATION: Duration = Duration::from_secs(300);

/// JOIN 槽位的合并方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeMode {
    /// 单值写入键值。
    Set,
    /// 始终以 JSON 数组保存，多次贡献逐项追加。
    Append,
}

/// 后继任务在结算事务内的落库与作用域分发方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// 在共享作用域内为每条输入创建独立的 `pending` 任务。
    Shared,
    /// 为 `->!` / `->+` 的每条输入建立独立子图作用域并创建 `pending` 任务。
    PerInputScope,
    /// 在共享作用域内合并至状态为 `collecting` 的 JOIN 任务的指定槽位。
    Join {
        key: &'static str,
        merge: MergeMode,
    },
}

/// 一条边的后继输入及目标信息；由存储层原子路由、创建作用域和任务。
#[derive(Debug, Clone)]
pub struct Successor {
    pub dst: Arc<str>,
    /// 从根到目标所属子图的完整子图 ID，根节点为空。
    pub subgraph_path: Arc<[Arc<str>]>,
    pub inputs: Vec<Value>,
    pub dispatch: Dispatch,
}

/// 跨边界进入子图内 JOIN 的外部直接前驱，用于通过 `trigger_task_id` 精确绑定入口事件。
#[derive(Debug, Clone)]
pub(crate) struct JoinExternalEntry {
    pub name: String,
    pub entry_depth: usize,
}

/// JOIN 的作用域深度、必填槽位、可选聚合槽位、外部入口与全部反向可达上游，在图构建后固定。
#[derive(Debug, Clone)]
pub(crate) struct JoinRule {
    #[cfg(feature = "pg")]
    pub scope_depth: usize,
    pub required_slots: Vec<String>,
    /// 未收到贡献时需预置为空数组 `[]` 的可选聚合槽位（`#[join(collect, optional)]`）。
    pub optional_append_slots: Vec<String>,
    pub external_entries: Vec<JoinExternalEntry>,
    pub upstreams: Vec<String>,
}

/// 图构建时预计算的静态结算规则，由存储层在结算事务内消费。
#[derive(Debug, Default, Clone)]
pub(crate) struct SettlementRules {
    pub join_rules: HashMap<Arc<str>, JoinRule>,
    /// 每个节点结算时可能影响的 JOIN 节点名列表（用于精细锁粒度与无 JOIN 短路）。
    pub affected_joins: HashMap<Arc<str>, Vec<Arc<str>>>,
}

impl SettlementRules {
    pub(crate) fn affected_joins_for(&self, node: &str) -> &[Arc<str>] {
        self.affected_joins
            .get(node)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// 返回结算 `node` 时可能影响的既有作用域深度（升序去重，供 PG 精细 advisory lock 使用）。
    #[cfg(feature = "pg")]
    pub(crate) fn affected_scope_depths(&self, node: &str) -> Vec<usize> {
        let mut depths: Vec<usize> = self
            .affected_joins_for(node)
            .iter()
            .filter_map(|join_name| self.join_rules.get(join_name.as_ref()))
            .map(|rule| rule.scope_depth)
            .collect();
        depths.sort_unstable();
        depths.dedup();
        depths
    }
}

/// 后继路由使用源任务的身份，以及结算时数据库中的运行和作用域。
pub(crate) struct FanoutSource<'a> {
    pub workflow_id: &'a str,
    pub name: &'a str,
    pub task_id: i64,
    pub run_id: i64,
    pub scope_id: Option<i64>,
}

/// 在 `collecting` 阶段把来源贡献合并进 JOIN 的累加器对象 `input_json`。
///
/// 首次创建 `collecting` 行时（`existing == None`），会为 `optional_append_slots` 预置 `[]`，
/// 确保未收到贡献的可选聚合槽位在反序列化 `Vec<T>` 时自动得到空列表。
pub(crate) fn merge_join_slot(
    existing: Option<Value>,
    optional_append_slots: &[String],
    key: &str,
    merge: MergeMode,
    inputs: &[Value],
) -> Result<Value, StorageError> {
    let mut acc = match existing {
        Some(value) => value,
        None => {
            let mut map = serde_json::Map::new();
            for opt_key in optional_append_slots {
                map.insert(opt_key.clone(), Value::Array(Vec::new()));
            }
            Value::Object(map)
        }
    };
    let Value::Object(map) = &mut acc else {
        return Err(StorageError::InvalidFanout {
            detail: "JOIN accumulator must be a JSON object".into(),
        });
    };
    match merge {
        MergeMode::Set => {
            if let Some(value) = inputs.last() {
                map.insert(key.to_owned(), value.clone());
            }
        }
        MergeMode::Append => {
            if !inputs.is_empty() {
                let entry = map
                    .entry(key.to_owned())
                    .or_insert_with(|| Value::Array(Vec::new()));
                let Value::Array(items) = entry else {
                    return Err(StorageError::InvalidFanout {
                        detail: format!("JOIN append slot `{key}` must be a JSON array"),
                    });
                };
                items.extend(inputs.iter().cloned());
            }
        }
    }
    Ok(acc)
}

/// 引擎与调度器共用的内部任务存储契约，统一使用 [`StorageError`]。
pub(crate) trait TaskStore: Send + Sync {
    /// 提交已准备好入口路径的独立任务，事务提交成功后返回运行身份。
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>>;

    /// 在指定工作流的全部运行中认领一批可执行任务，最多 `limit` 个，按 `task_id` 升序。
    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>>;

    /// 延长仍有效的认领至数据库当前时间 + [`LEASE_DURATION`]，返回新截止时间。
    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>>;

    /// 释放当前有效认领；可附带错误说明与可选短退避，保留崩溃/结算错误现场。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
        error: Option<&'a str>,
        delay: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 在同一事务内写入产物、标记成功、批量入队后继并推进受影响的 collecting JOIN。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 在同一事务内结算失败（`superseded` 或 `failed`）、入队失败后继并推进受影响的 collecting JOIN。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 将可重试失败的任务放回 `pending` 并设置下次可认领时间 `available_at`。
    fn schedule_retry<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        delay: Duration,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 共享轮询器批量探测哪些已注册工作流存在立即可认领的任务（命中 `idx_tasks_claim` 部分索引）。
    fn ready_workflows<'a>(
        &'a self,
        workflow_ids: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>>;
}

#[cfg(test)]
#[path = "../../tests/unit/task_store.rs"]
mod tests;
