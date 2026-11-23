//! 成功边将 JSON 对象贡献汇入等待中的 JOIN。存储按对象键合并输入，
//! 重复的键累积为数组。失败后继和独立运行入口各自携带一次执行的输入。

use serde_json::Value;

use super::{Node, NodeError, TaskType};

/// JOIN 收到认领时的输入快照，并将其作为节点输出。
///
/// - `TASK_TYPE` 为 [`TaskType::Join`]，[`super::super::graph::Graph`] 据此计算静态前驱集合。
///   存储按作用域入口筛选本次调用的前驱，检查来源完成，并等待图中仍可能产生来源的任务结束。
/// - `execute` 保持纯函数：把输入的合并 `Map` 原样透传，作为本节点输出。
pub struct JoinNode;

impl<Ctx: Send> Node<Ctx> for JoinNode {
    type In = Value;
    type Out = Value;

    /// JOIN 汇聚门：任务类型为 [`TaskType::Join`]，存储据此等齐同作用域的全部静态前驱。
    const TASK_TYPE: TaskType = TaskType::Join;

    async fn execute(&self, _ctx: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}
