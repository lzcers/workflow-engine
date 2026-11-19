//! 引擎统一定义的任务存储契约与后继输入规则。
//!
//! 各数据库后端复用本模块的纯数据转换，并通过各自的 SQL、锁与事务落实 [`TaskStore`] 契约。

use futures::future::BoxFuture;
use std::{collections::HashMap, time::Duration};

use serde_json::Value;

use super::{Task, TaskRecord};

/// 认领和每次续租的存活期限，不限制节点的总执行时长。
pub const LEASE_DURATION: Duration = Duration::from_secs(300);

/// 一条后继入队规划：由调度器从图规划结果（`Graph::plan_success` / `plan_failure`）
/// 映射而来。`TaskStore` 只接收本数据形态，不依赖 `Graph` 类型。
#[derive(Debug, Clone)]
pub struct FanoutPlan {
    /// 目标节点名。
    pub dst: &'static str,
    /// 目标节点产出的任务类型（`normal` / `join` / `event`），落库到 `task_type` 列。
    pub task_type: &'static str,
    pub edge_id: String,
    pub foreach: bool,
    pub source_subgraph_path: Vec<String>,
    pub destination_subgraph_path: Vec<String>,
    pub inputs: Vec<Value>,
    pub predecessor_subgraph_paths: HashMap<String, Vec<String>>,
    pub join_incoming_edges: HashMap<String, String>,
    pub join_upstream_names: Vec<String>,
}

/// 将前驱贡献合并到 JOIN 的累积对象；同名 key 合为数组，已有数组及新贡献数组按元素追加。
/// 任一输入不是对象时不合并。后端须在持锁事务内读取累积值、调用此函数并写回。
pub(crate) fn merge_join_input(acc: &mut Value, contribution: &Value) {
    let Value::Object(acc_map) = acc else { return };
    let Some(obj) = contribution.as_object() else {
        return;
    };
    for (key, value) in obj {
        match acc_map.get_mut(key) {
            None => {
                acc_map.insert(key.clone(), value.clone());
            }
            Some(existing) => {
                let mut values = match existing.take() {
                    Value::Array(values) => values,
                    value => vec![value],
                };
                match value {
                    Value::Array(items) => values.extend(items.clone()),
                    value => values.push(value.clone()),
                }
                *existing = Value::Array(values);
            }
        }
    }
}

/// 引擎与调度器共用的任务持久化抽象：提交、认领、续租、原子结算和释放认领。
///
/// 存储可供多个工作流共享；认领时显式传入工作流身份，提交与结算沿用任务的身份。
/// 调度器把图规划结果（成功后继 / 失败后继）统一交给本存储落库，自身不触碰任何业务字段；
/// 具体存储实现使用统一的 [`TaskRecord`] 快照，并将操作限制在任务所属的工作流内。
///
/// 认领资格与结算语义由本契约统一规定，后端负责事务实现；结算必须原子提交任务状态
/// 与全部后继，失败则整体回滚。认领与恢复扫描必须使用一致的可执行性规则。
pub trait TaskStore: Sync {
    /// 存储层错误类型：允许不同存储实现定义各自的错误，同时保证引擎层可统一传播
    /// （`Send + Sync` 供调度器在协程间传播与打日志）。
    type Error: std::error::Error + Send + Sync + 'static;

    /// 原子入队任务并确定运行身份，返回已持久化的任务快照。
    /// run_id 为 None 时，以新分配的 task_id 创建独立运行；Some(id) 则保留指定运行。
    /// 按静态子图路径创建入口实例，任务仅保存 scope_id。
    /// 节点类型、来源任务、静态前驱和尝试预算沿用输入；任务及其运行、作用域身份必须在同一事务内提交。
    fn enqueue<'a>(&'a self, task: &'a Task) -> BoxFuture<'a, Result<TaskRecord, Self::Error>>;

    /// 在指定工作流的全部运行中认领一批待处理任务（执行业务或处理最终失败），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表，按任务 ID 排序。`limit` 为 0 时直接返回空列表。
    ///
    /// 待处理任务必须同时满足：
    /// - 状态为 pending / running，未持有租约或租约已过期。
    /// - JOIN 的预期来源必须全部成功并沿预期入边贡献输入；子图内仍可能生产来源的任务
    ///   全部结束后才可认领。祖先或兄弟入口绑定精确触发任务，子图输出按父实例收集。
    /// `parent_task_ids` 只记录来源，不参与认领资格判断。
    ///
    /// 资格检查与写入认领状态必须原子执行，避免多个 worker 同时认领同一个任务。
    /// worker_id 必须唯一标识此次执行器生命周期；每次认领递增 attempt 并写入 300 秒租约。
    /// 返回的 attempt <= max_attempts 时执行业务；超过上限时只做最终失败结算，不再执行节点。
    /// 首次认领耗尽任务时必须持久化 error：末次租约过期记录租约失效，无租约则记录预算耗尽。
    /// 后续认领保留该原因，使最终失败处理在崩溃或结算失败后仍可恢复。
    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        worker_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, Self::Error>>;

    /// 将仍有效的认领延长至数据库当前时间 + LEASE_DURATION，返回新截止时间。
    ///
    /// 原子校验 workflow_id、task_id、claimed_by、attempt、running 状态和尚未过期的租约。
    /// 失权、已结算或过期返回 None，不能复活过期认领；数据库错误单独返回 Err。
    /// 续租不递增 attempt，也适用于超过业务尝试预算的最终失败结算认领。
    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, Self::Error>>;

    /// 在同一事务内写入产物、标记成功并入队全部成功后继。
    ///
    /// 必须校验当前有效认领且 attempt <= max_attempts；旧执行者和仅用于最终失败的认领不能提交成功。
    /// 成功后继继承运行身份，作用域按子图边界进入、继承或退出。
    /// 同一次结算的普通跨入边共享实例；foreach 按边及元素建立独立实例。
    /// scope 创建、任务结算、JOIN 的预期来源及实际输入合并与后继创建在同一事务提交。
    /// 普通任务逐项创建；JOIN 按 workflow、run、scope 和节点名累积。
    /// 依赖永久失败会令已有及晚创建的 JOIN 失败，不影响其他实例。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内结算失败，并入队失败后继或级联已有依赖任务。
    ///
    /// `successors` 非空时标记 `superseded`；为空时标记 `failed`，并将同一工作流、
    /// 同一 run 中按 join_state 依赖该实例来源的 pending JOIN 标记失败。
    /// 必须先校验当前有效认领，旧执行者不能创建后继或级联失败。
    /// 失败后继同样将源任务 ID 写入 parent_task_ids，使用与成功后继相同的创建和 JOIN 汇合规则。
    /// 任一步失败都必须整体回滚，调度器会释放租约并重试。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务在下一轮认领即可
    /// 重入，不必等待租约过期。仅当前有效认领可以释放，不能释放新执行者的租约。
    fn release_claim<'a>(&'a self, task: &'a TaskRecord) -> BoxFuture<'a, Result<(), Self::Error>>;
}

#[cfg(test)]
#[path = "../../tests/unit/task_store.rs"]
mod tests;
