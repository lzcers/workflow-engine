//! 显式 JOIN 汇聚节点：把多前驱的输入合并为 `Map {name: value}` 后透传。
//!
//! JOIN 节点是引擎的**通用能力**：业务侧无需自建汇聚逻辑，只需在图上声明无数条
//! 成功边汇入一个 JOIN 节点名，再经 JOIN 节点把合并结果路由给下游。
//!
//! 汇聚语义完全由**存储层**承担：每个前驱成功时，存储把 `{前驱名: 贡献}` 增量并入
//! JOIN 任务的 `input_json`（同名 key 合并为数组）；JOIN 节点本身保持纯函数——收到
//! 的输入就是已合并好的 `Map`，直接透传作为输出。下游既可以用 `Map` 直接消费，
//! 也可接一条 `map` 边把 `Map` 换算成强类型输入。
//!
//! JOIN 的「等待同一运行、作用域内全部静态前驱完成」由 [`super::TaskType::Join`] 通知[`super::super::graph::Graph`]
//! 与存储（`join_state` 的预期来源和入边贡献）共同保证，节点本身不关心等待逻辑。

use serde_json::Value;

use super::{Node, NodeError, TaskType};

/// 显式 JOIN 汇聚节点：透传已由存储合并好的 `Map {name: value}`。
///
/// - `TASK_TYPE` 为 [`TaskType::Join`]，[`super::super::graph::Graph`] 据此为其声明静态前驱集合
///   以及入边身份，存储据以实现「全部预期贡献到齐才放行」的认领闸门。
/// - `execute` 保持纯函数：把输入的合并 `Map` 原样透传，作为本节点输出。
pub struct JoinNode;

impl<Ctx: Send> Node<Ctx> for JoinNode {
    type In = Value;
    type Out = Value;

    /// JOIN 汇聚门：任务类型为 [`TaskType::Join`]，存储据此等齐同作用域的全部静态前驱。
    const TASK_TYPE: TaskType = TaskType::Join;

    async fn execute(&self, _ctx: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        // 汇聚已由存储层增量完成：输入即合并后的 `Map {name: value}`，直接透传。
        Ok(input)
    }
}
