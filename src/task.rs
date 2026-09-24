//! 通用任务抽象：调度器只见任务的最小形态，与具体持久化/业务字段解耦。
//!
//! - [`Task`]：调度器眼中唯一可见的任务记录形态（身份 + 节点名 + 输入负载）。
//!   业务字段（如 `commit_id`）不进入本 trait——调度器不解读任务语义，只把它作为
//!   不透明快照经 [`crate::node::NodeContext::fork`] 注入上下文。
//! - [`TaskStore`]：任务持久化抽象（认领 / 结算 / 入队后继）。实现应**会话级**
//!   构造（绑定会话作用域与具体存储），故 `claim` 不再需要会话参数。

use std::future::Future;

use serde_json::Value;

/// 存储层可见的任务失败载荷：仅携带落库所需的消息文本。
///
/// 错误种类（可重试 / 永久）属于图路由词汇（[`crate::node::NodeError`]），已在
/// 调度器规划失败后继时消费完毕；本类型刻意只投影消息，使 `task` 层不依赖 `node` 层
/// （`TaskStore` 实现只与字符串打交道）。将来若存储需要按错误种类落库 / 自治重试，
/// 再向后兼容地补字段即可。
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct TaskError {
    /// 失败消息（落库到 `tasks.error` 列）。
    pub message: String,
}

impl TaskError {
    /// 由消息构造。
    pub fn new(message: impl Into<String>) -> Self {
        TaskError {
            message: message.into(),
        }
    }
}

/// 一条后继入队规划：由调度器从图规划结果（`Graph::plan_success` / `plan_failure`）
/// 映射而来。`TaskStore` 只接收本数据形态（不依赖 `Graph` 类型），字段与既有
/// [`TaskStore::enqueue_fanout`] 的原始参数一一对应。
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

/// 调度器眼中唯一可见的任务形态：身份 + 节点名 + 输入负载。
pub trait Task: Clone + Send {
    /// 全局任务身份（会话内唯一，供日志与依赖追踪）。
    fn task_id(&self) -> i64;
    /// 节点名（调度器据此查图执行）。
    fn name(&self) -> &str;
    /// 任务输入负载。
    fn input(&self) -> &Value;
}

/// 调度器的任务持久化抽象：认领 / 结算 / 入队后继。
///
/// 调度器把图规划结果（成功后继 / 失败后继）统一交给本存储落库，自身不触碰任何业务字段；
/// 具体存储实现（如绑定 `workflow_id + repo` 的工作流级 store）负责解读 `Task` 快照。
///
/// `Self: Sync` 与 `Self::Task: Sync` 供默认实现把共享引用带过 `.await`
/// （与调度器 `S: TaskStore + Send + Sync` 的使用约束一致）。
pub trait TaskStore: Sync {
    /// 具体任务记录类型（实现 [`Task`]）。
    type Task: Task + Sync;

    /// 存储层错误类型：允许不同存储实现定义各自的错误，同时保证引擎层可统一传播
    /// （`Send + Sync` 供调度器在协程间传播与打日志）。
    type Error: std::error::Error + Send + Sync + 'static;

    /// 认领一批可执行任务（同一会话内互相无依赖），最多 `limit` 个；数量不足或无可认领
    /// 时返回少于 `limit` 甚至空列表。调度器据此把一批无依赖任务并发结算。
    fn claim_many(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Self::Task>, Self::Error>> + Send;

    /// 结算为成功：写产物并返回回填后的任务（供上下文同步身份，如 `commit_id`）。
    fn succeed(
        &self,
        task: &Self::Task,
        output: &Value,
    ) -> impl Future<Output = Result<Self::Task, Self::Error>> + Send;

    /// 结算为失败 / 被后继取代：`has_successors = true` 时标记 `superseded`（失败已被下游
    /// 后继接管，对外不暴露为失败），否则标记 `failed`。
    ///
    /// `has_successors` 由调度层根据「失败规划是否产出后继」计算——引擎不区分后继是重试、
    /// 兜底还是错误分支，只知道自己是否产出了后继。
    fn fail(
        &self,
        task: &Self::Task,
        err: &TaskError,
        has_successors: bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 入队后继任务（统一 fanout 入口，成功后继与失败后继共用）。
    ///
    /// `inputs` 为喂给 `name` 节点的输入列表：
    /// - `len == 1`：单实例（扇入合并，多前驱汇聚到同一后继）；
    /// - `len > 1`：map 边扇出的多实例，每个 `input` 各建一个独立任务实例，互不合并。
    ///
    /// 实例身份由 `instance_id` 区分（单实例为 NULL）。`task_type` 为目标节点的任务类型
    /// （普通节点 / JOIN 门），落库到 `task_type` 列；`depends_on_names` 为目标节点的静态
    /// 前驱节点名全集（通用血缘，仅 `task_type = Join` 时用作等齐前驱的认领闸门）；
    /// `depends_on_ids` 为后继任务的直接依赖任务 id 集（认领闸门要求其全部 `succeeded`）——
    /// 成功后继依赖 `from_task`，失败后继**不依赖** `from_task`（否则会被认领闸门永久阻塞）。
    fn enqueue_fanout(
        &self,
        from_task: &Self::Task,
        name: &str,
        task_type: &str,
        inputs: &[Value],
        depends_on_names: &[String],
        depends_on_ids: &[i64],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 前驱永久失败后，级联失败所有直接依赖它的 pending 任务，
    /// 避免它们因前驱已永久失败而永远无法被认领（悬挂）。返回被级联标记为失败的任务数。
    ///
    /// 默认空实现（不级联）；需要级联语义的存储应覆盖此方法。
    fn cascade_failed_predecessor(
        &self,
        _task: &Self::Task,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        async move { Ok(0) }
    }

    /// 结算为成功并在**同一事务**内入队全部成功后继（原子化 [`TaskStore::succeed`] + fanout）。
    ///
    /// 返回回填后的任务记录（如 `commit_scene` 结算时附着的 `commit_id`），供调用方同步
    /// 上下文身份。一次提交内「任务置 succeeded + 全部后继入队」要么都在、要么都不在，
    /// 杜绝进程在 `succeed` 后、后继入队前崩溃导致的「已成功但后继从未入队」静默停摆。
    ///
    /// 默认实现用 [`TaskStore::succeed`] + [`TaskStore::enqueue_fanout`] 组合（非原子，
    /// 保留旧行为）；绑定单一关系库的存储实现应覆盖本方法，在单个事务内完成二者。
    fn succeed_and_enqueue(
        &self,
        task: &Self::Task,
        output: &Value,
        fanouts: &[FanoutPlan],
    ) -> impl Future<Output = Result<Self::Task, Self::Error>> + Send {
        async move {
            let settled = self.succeed(task, output).await?;
            for f in fanouts {
                self.enqueue_fanout(
                    &settled,
                    f.dst,
                    f.task_type,
                    &f.inputs,
                    &f.depends_on_names,
                    &[settled.task_id()],
                )
                .await?;
            }
            Ok(settled)
        }
    }

    /// 结算为失败并在**同一事务**内入队全部失败后继（原子化 [`TaskStore::fail`] + fanout）。
    ///
    /// `has_successors` 由调度层根据 `fanouts` 是否非空计算——有后继接管时标记
    /// `superseded`（对外不暴露为失败），否则标记 `failed`。失败后继不依赖源任务
    /// （`depends_on_ids = []`），否则会被认领闸门永久阻塞。
    ///
    /// 默认实现先逐个入队失败后继（各自事务）再 [`TaskStore::fail`]，非原子；绑定单一
    /// 关系库的存储实现应覆盖本方法，在单个事务内完成二者。
    fn fail_and_enqueue(
        &self,
        task: &Self::Task,
        err: &TaskError,
        has_successors: bool,
        fanouts: &[FanoutPlan],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            for f in fanouts {
                self.enqueue_fanout(
                    task,
                    f.dst,
                    f.task_type,
                    &f.inputs,
                    &f.depends_on_names,
                    &[],
                )
                .await?;
            }
            self.fail(task, err, has_successors).await
        }
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务在下一轮认领即可
    /// 重入，不必等待租约过期。
    ///
    /// 默认空实现（租约仅由过期回收）；持有租约列的存储实现应覆盖为
    /// `UPDATE ... SET lease_until = NULL`。
    fn release_claim(
        &self,
        _task: &Self::Task,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(()) }
    }
}
