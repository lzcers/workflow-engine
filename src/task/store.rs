//! 引擎、工作流实例与调度器共用的任务存储契约。

use futures::future::BoxFuture;
use std::time::Duration;

use serde_json::Value;

use super::{Task, TaskRecord};

/// 认领与续租使用相同的租约时长；执行期间由调度器定期续租。
pub const LEASE_DURATION: Duration = Duration::from_secs(300);

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

/// 引擎与调度器共用的任务持久化抽象：提交、认领、原子结算和释放认领。
///
/// 存储可供多个工作流共享；认领时显式传入工作流身份，提交与结算沿用任务的身份。
/// 调度器把图规划结果（成功后继 / 失败后继）统一交给本存储落库，自身不触碰任何业务字段；
/// 具体存储实现使用统一的 [`TaskRecord`] 快照，并将操作限制在任务所属的工作流内。
///
/// 所有方法均由存储实现提供；结算必须原子提交任务状态与全部后继，失败则整体回滚。
pub trait TaskStore: Sync {
    /// 存储层错误类型：允许不同存储实现定义各自的错误，同时保证引擎层可统一传播
    /// （`Send + Sync` 供调度器在协程间传播与打日志）。
    type Error: std::error::Error + Send + Sync + 'static;

    /// 原子创建独立运行及其入口任务；返回记录的 run_id 必须是新分配的非空身份。
    fn create_run<'a>(
        &'a self,
        workflow_id: &'a str,
        entry: &'a str,
        input: &'a Value,
    ) -> BoxFuture<'a, Result<TaskRecord, Self::Error>>;

    /// 提交初始任务，返回已持久化的任务快照；工作流身份来自任务。
    fn enqueue<'a>(&'a self, task: &'a Task) -> BoxFuture<'a, Result<TaskRecord, Self::Error>>;

    /// 在指定工作流的全部运行中认领一批可执行任务（互相无依赖），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表，按任务 ID 排序。`limit` 为 0 时直接返回空列表。
    /// worker_id 必须唯一标识此次执行器生命周期；每次认领递增 attempt 并写入租约。
    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        worker_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, Self::Error>>;

    /// 仅当前有效认领可以续租；校验状态、持有人、attempt 和未过期的租约。
    /// 续租不增加 attempt；认领已失效时返回错误，调度器必须停止执行。
    fn renew_claim<'a>(&'a self, task: &'a TaskRecord) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内写入产物、标记成功并入队全部成功后继。
    ///
    /// 必须校验当前有效认领，旧执行者不能提交结果或创建后继。
    /// 成功后继依赖源任务，并继承结算时最新的任务身份（如 `run_id`）。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// 在同一事务内结算失败，并入队失败后继或级联已有依赖任务。
    ///
    /// `successors` 非空时标记 `superseded`，失败后继不依赖源任务；为空时标记 `failed`，
    /// 并将同一工作流、同一 run 内直接依赖该任务 ID 或节点名的已有 pending 任务标记失败。
    /// 必须先校验当前有效认领，旧执行者不能创建后继或级联失败。
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
