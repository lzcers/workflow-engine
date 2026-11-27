//! 引擎统一定义的任务存储契约与后继输入规则。
//!
//! 各数据库后端复用本模块的纯数据转换，并通过各自的 SQL、锁与事务落实 [`TaskStore`] 契约。

use std::{collections::HashMap, time::Duration};

use futures::future::BoxFuture;
use serde_json::Value;

use super::{Task, TaskRecord};
use crate::{RunHandle, error::StorageError, node::MergeMode};

/// 认领和每次续租的存活期限，不限制节点的总执行时长。
pub(crate) const LEASE_DURATION: Duration = Duration::from_secs(300);

/// 后继任务在结算事务内的落库与作用域分发方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// 在共享作用域内为每条输入创建独立的 `pending` 任务。
    Shared,
    /// 为 `->+` 的每条输入建立独立子图作用域并创建 `pending` 任务。
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
    pub dst: &'static str,
    /// 从根到目标所属子图的完整子图 ID，根节点为空。
    pub subgraph_path: Vec<String>,
    pub inputs: Vec<Value>,
    pub dispatch: Dispatch,
}

/// JOIN 的直接前驱、仍可能创建输入的全部上游与聚合槽位名，在图构建后固定。
#[derive(Debug, Clone)]
pub(crate) struct JoinRule {
    pub predecessors: Vec<JoinPredecessor>,
    pub upstreams: Vec<String>,
    /// 需要预置为空数组的 `MergeMode::Append` 槽位键。
    pub append_keys: Vec<String>,
}

/// 外部前驱只对应触发当前子图实例的入口任务。深度从最外层子图的 0 开始。
#[derive(Debug, Clone)]
pub(crate) struct JoinPredecessor {
    pub name: String,
    pub entry_depth: Option<usize>,
    pub optional: bool,
}

/// 图构建时预计算的静态结算规则，由存储层在结算事务内消费。
#[derive(Debug, Default, Clone)]
pub(crate) struct SettlementRules {
    pub join_rules: HashMap<String, JoinRule>,
}

#[cfg(feature = "pg")]
impl SettlementRules {
    /// 是否包含需要运行级串行锁的跨任务规则。
    pub(crate) fn needs_run_lock(&self) -> bool {
        !self.join_rules.is_empty()
    }
}

/// 后继路由使用源任务的身份，以及结算时数据库中的运行和作用域。
pub(crate) struct FanoutSource<'a> {
    pub workflow_id: &'a str,
    pub task_id: i64,
    pub run_id: i64,
    pub scope_id: Option<i64>,
}

/// 按槽位合并模式把整批贡献合入 JOIN 输入对象；新建累加器时预置聚合槽位为空数组。
pub(crate) fn merge_join_inputs(
    existing: Option<Value>,
    append_keys: &[String],
    key: &str,
    merge: MergeMode,
    inputs: &[Value],
) -> Result<Value, StorageError> {
    let mut acc = existing.unwrap_or_else(|| {
        let mut map = serde_json::Map::with_capacity(append_keys.len() + 1);
        for append_key in append_keys {
            map.insert(append_key.clone(), Value::Array(Vec::new()));
        }
        Value::Object(map)
    });
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
            let entry = map
                .entry(key.to_owned())
                .or_insert_with(|| Value::Array(Vec::new()));
            let Value::Array(items) = entry else {
                return Err(StorageError::InvalidFanout {
                    detail: format!("JOIN append slot `{key}` is not a JSON array"),
                });
            };
            items.extend(inputs.iter().cloned());
        }
    }
    Ok(acc)
}

/// 引擎与调度器共用的任务存储，负责入口入队、认领、续租、结算和释放认领。
///
/// 同一引擎的工作流共用仓储，按 workflow_id 认领任务，结算时消费预计算的 [`SettlementRules`]。
/// 调度器把图规划结果（成功后继 / 失败后继）和静态规则统一交给本存储落库，自身不触碰任何业务字段；
/// 具体存储实现使用统一的 [`TaskRecord`] 快照，并将操作限制在任务所属的工作流内。
///
/// 认领资格与结算语义由本契约统一规定，后端负责事务实现；结算必须原子提交任务状态
/// 与全部后继。成功和失败结算均在写完后继后推进本次运行内的 collecting JOIN，
/// 即使本次没有后继也要检查。收齐输入的 JOIN 转为 pending，依赖永久失败的 JOIN 转为 failed。
pub(crate) trait TaskStore: Sync {
    /// 存储层错误类型：允许不同存储实现定义各自的错误，同时保证引擎层可统一传播
    /// （`Send + Sync` 供调度器在协程间传播与打日志）。
    type Error: std::error::Error + Send + Sync + 'static;

    /// 提交已准备好入口路径的独立任务，事务提交成功后返回运行身份。
    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, Self::Error>>;

    /// 在指定工作流的全部运行中认领一批待处理任务（执行业务或处理最终失败），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表，按任务 ID 排序。`limit` 为 0 时直接返回空列表。
    ///
    /// 认领无有效租约的 pending 或 running 任务，两者的输入均已确定。
    ///
    /// 资格检查与写入认领状态必须原子执行，避免多个 worker 同时认领同一个任务。
    /// 每次认领递增 attempt 并写入 300 秒租约。
    /// 返回的 attempt <= MAX_ATTEMPTS 时执行业务；超过上限时只做最终失败结算，不再执行节点。
    /// 首次认领耗尽任务时必须持久化 error：末次租约过期记录租约失效，无租约则记录预算耗尽。
    /// 后续认领保留该原因，使最终失败处理在崩溃或结算失败后仍可恢复。
    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, Self::Error>>;

    /// 将仍有效的认领延长至数据库当前时间 + LEASE_DURATION，返回新截止时间。
    ///
    /// 原子校验 workflow_id、task_id、attempt、running 状态和尚未过期的租约。
    /// 失权、已结算或过期返回 None，不能复活过期认领；数据库错误单独返回 Err。
    /// 续租不递增 attempt，也适用于超过业务尝试预算的最终失败结算认领。
    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, Self::Error>>;

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务在下一轮认领即可
    /// 重入，不必等待租约过期。仅当前有效认领可以释放，不能释放新执行者的租约。
    fn release_claim<'a>(&'a self, task: &'a TaskRecord) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内写入产物、标记成功并入队全部成功后继。
    ///
    /// 必须校验当前有效认领且 attempt <= MAX_ATTEMPTS；旧执行者和仅用于最终失败的认领不能提交成功。
    /// 成功后继继承运行身份，作用域按子图边界进入、继承或退出。
    /// 同一次结算的普通跨入边共享实例，`->+` 按边及元素建立独立实例。
    /// scope 创建、任务结算、JOIN 输入合并与后继创建在同一事务提交。
    /// 普通任务逐项创建；JOIN 按 workflow、run、scope 和节点名累积。
    /// 依赖永久失败会令已有及晚创建的 JOIN 失败，不影响其他实例。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内结算失败，并入队失败后继或级联已有依赖任务。
    ///
    /// `successors` 存在非空输入时标记 `superseded`；否则标记 `failed`，并将同一工作流、
    /// 同一 run 中按前驱名称和作用域关系依赖该实例来源的 collecting JOIN 标记失败。
    /// 必须先校验当前有效认领，旧执行者不能创建后继或级联失败。
    /// 失败后继同样将源任务 ID 写入 parent_task_ids，每条失败路由输入创建一条直接执行的后继。
    /// 任一步失败都必须整体回滚，调度器会释放租约并重试。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
        rules: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 将可重试失败的任务放回 pending 并设置下次可认领时间（退避延迟），清空租约并保留本次错误。
    ///
    /// 必须校验当前有效认领且 `attempt < MAX_ATTEMPTS`；已失权或预算耗尽的认领不能安排退避重试。
    fn schedule_retry<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        delay: Duration,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

#[cfg(test)]
#[path = "../../tests/unit/task_store.rs"]
mod tests;
