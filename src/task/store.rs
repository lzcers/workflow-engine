//! 调度器与存储之间的认领、结算及后继入队契约。

use std::future::Future;

use serde_json::Value;

use super::TaskRecord;

/// 一条后继入队规划：由调度器从图规划结果（`Graph::plan_success` / `plan_failure`）
/// 映射而来。`TaskStore` 只接收本数据形态，不依赖 `Graph` 类型。
#[derive(Debug, Clone)]
pub struct FanoutPlan {
    /// 目标节点名。
    pub dst: &'static str,
    /// 目标节点产出的任务类型（`normal` / `join`），落库到 `task_type` 列。
    pub task_type: &'static str,
    /// 喂给目标节点的输入列表：`len == 1` 单实例（扇入合并）；`len > 1` map 边扇出的
    /// 多实例（每个 input 各建一个独立任务实例，互不合并）。
    pub inputs: Vec<Value>,
    /// 目标节点的静态前驱节点名全集（仅 `task_type = join` 时用作等齐前驱的认领闸门）。
    pub depends_on_names: Vec<String>,
}

/// 一条后继入队计划（原子结算方法专用）：目标节点名、任务类型、实例列表、静态前驱名集。
/// 各持久化后端在结算事务中使用同一模型。
#[derive(Debug, Clone)]
pub struct FanoutInstances {
    /// 目标节点名。
    pub name: String,
    /// 目标节点产出的任务类型（`normal` / `join`），落库到 `task_type` 列。
    pub task_type: String,
    /// `(instance_id, input)` 实例列表：`None` = 单实例（扇入合并）；`Some(i)` = 第 i 个多实例。
    pub instances: Vec<(Option<i32>, Value)>,
    /// 目标节点的静态前驱节点名全集（仅 `task_type = 'join'` 用作等齐前驱的认领闸门）。
    pub depends_on_names: Vec<String>,
}

/// 调度器的任务持久化抽象：认领、原子结算和释放认领。
///
/// 调度器把图规划结果（成功后继 / 失败后继）统一交给本存储落库，自身不触碰任何业务字段；
/// 具体存储实现（如绑定 `workflow_id + repo` 的工作流级 store）使用统一的 [`TaskRecord`] 快照。
///
/// 所有方法均由存储实现提供；结算必须原子提交任务状态与全部后继，失败则整体回滚。
pub trait TaskStore: Sync {
    /// 存储层错误类型：允许不同存储实现定义各自的错误，同时保证引擎层可统一传播
    /// （`Send + Sync` 供调度器在协程间传播与打日志）。
    type Error: std::error::Error + Send + Sync + 'static;

    /// 认领一批可执行任务（同一会话内互相无依赖），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表。调度器据此把一批无依赖任务并发结算。
    fn claim_many(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<TaskRecord>, Self::Error>> + Send;

    /// 在同一事务内写入产物、标记成功并入队全部成功后继。
    ///
    /// 成功后继依赖源任务，并继承结算时最新的任务身份（如 `run_id`）。
    fn settle_success(
        &self,
        task: &TaskRecord,
        output: &Value,
        successors: &[FanoutPlan],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 在同一事务内结算失败，并入队失败后继或级联已有依赖任务。
    ///
    /// `successors` 非空时标记 `superseded`，失败后继不依赖源任务；为空时标记 `failed`，
    /// 并将同一工作流、同一 run 内直接依赖该任务 ID 或节点名的已有 pending 任务标记失败。
    /// 任一步失败都必须整体回滚，调度器会释放租约并重试。
    fn settle_failure(
        &self,
        task: &TaskRecord,
        message: &str,
        successors: &[FanoutPlan],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务在下一轮认领即可
    /// 重入，不必等待租约过期。
    fn release_claim(
        &self,
        task: &TaskRecord,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
