//! 引擎统一定义的任务存储契约与后继输入规则。
//!
//! 各数据库后端复用本模块的纯数据转换，并通过各自的 SQL、锁与事务落实 [`TaskStore`] 契约。

use futures::future::BoxFuture;
use std::time::Duration;

use serde_json::Value;

use super::{Task, TaskRecord};

/// 每次认领的固定租约时长，同时作为本次任务执行的时间预算。
pub const LEASE_DURATION: Duration = Duration::from_secs(300);

/// 一条后继入队规划：由调度器从图规划结果（`Graph::plan_success` / `plan_failure`）
/// 映射而来。`TaskStore` 只接收本数据形态，不依赖 `Graph` 类型。
#[derive(Debug, Clone)]
pub struct FanoutPlan {
    /// 目标节点名。
    pub dst: &'static str,
    /// 目标节点产出的任务类型（`normal` / `join`），落库到 `task_type` 列。
    pub task_type: &'static str,
    /// 喂给目标节点的输入列表：普通节点每条输入创建独立任务，JOIN 在同一运行内累积输入。
    /// 空列表不创建任务。
    pub inputs: Vec<Value>,
    /// 目标节点的静态前驱节点名全集（仅 `task_type = join` 时用作等齐前驱的认领闸门）。
    pub depends_on_names: Vec<String>,
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

/// 引擎与调度器共用的任务持久化抽象：提交、认领、原子结算和释放认领。
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
    /// 节点类型、来源任务、静态前驱和尝试预算沿用输入；任务与非空 run_id 必须在同一事务内提交。
    fn enqueue<'a>(&'a self, task: &'a Task) -> BoxFuture<'a, Result<TaskRecord, Self::Error>>;

    /// 在指定工作流的全部运行中认领一批待处理任务（执行业务或处理最终失败），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表，按任务 ID 排序。`limit` 为 0 时直接返回空列表。
    ///
    /// 待处理任务必须同时满足：
    /// - 状态为 pending / running，未持有租约或租约已过期。
    /// - JOIN 要求记录中 `depends_on_names` 的每个前驱，在同一 `workflow_id + run_id`
    ///   下至少存在一个实例，且全部实例均已成功；同为 NULL 的 run_id 视为同组。
    ///   skipped / failed / superseded 均不算成功。前驱集合为空时不增加此项限制
    ///   （图规划的失败后继当前使用空集合）；普通任务不使用前驱名作认领闸门。
    /// `parent_task_ids` 只记录来源，不参与认领资格判断。
    ///
    /// 资格检查与写入认领状态必须原子执行，避免多个 worker 同时认领同一个任务。
    /// worker_id 必须唯一标识此次执行器生命周期；每次认领递增 attempt 并写入固定 300 秒租约。
    /// 返回的 attempt <= max_attempts 时执行业务；超过上限时只做最终失败结算，不再执行节点。
    /// 首次认领耗尽任务时必须持久化 error：末次租约过期记录执行窗口超时，无租约则记录预算耗尽。
    /// 后续认领保留该原因，使最终失败处理在崩溃或结算失败后仍可恢复。
    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        worker_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, Self::Error>>;

    /// 在同一事务内写入产物、标记成功并入队全部成功后继。
    ///
    /// 必须校验当前有效认领且 attempt <= max_attempts；旧执行者和仅用于最终失败的认领不能提交成功。
    /// 成功后继记录源任务 ID，并继承结算时最新的任务身份（如 `run_id`）。
    /// 普通后继每条输入创建独立任务。JOIN 按同一 workflow、run、节点名汇入已有
    /// pending / running JOIN，parent_task_ids 取去重并集，输入通过 [`merge_join_input`] 累积。
    /// 新任务的 parent_task_ids 为仅包含源任务 ID 的数组。
    /// 前驱成功、来源累积、JOIN 输入合并与后继创建必须在同一事务提交，且并发首次
    /// 汇入不能创建重复 JOIN。认领校验与原子结算保证同一父任务不会重复生成后继。
    /// 新建 JOIN 如已有静态前驱永久 failed，应直接标记 failed。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内结算失败，并入队失败后继或级联已有依赖任务。
    ///
    /// `successors` 非空时标记 `superseded`；为空时标记 `failed`，并将同一工作流、
    /// 同一 run 内以源节点名为静态前驱的已有 pending JOIN 标记失败。
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
