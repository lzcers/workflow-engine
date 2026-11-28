//! 图引擎：节点注册表 + 邻接索引边集合，负责运行期执行与后继规划。

mod builder;
mod compile;
mod description;
mod erased;
mod validate;

use std::{collections::HashMap, sync::Arc};

use serde_json::Value;

use super::node::AnyNode;
use crate::{
    EngineError,
    task::{Dispatch, SettlementRules, Successor},
};

pub use crate::edge::{
    Arrow, Edge, EdgeError, EdgeKind, EdgeMode, direct, isolated_direct, isolated_expand,
    shared_expand,
};
pub use crate::node::{
    JoinInput, JoinNode, JoinSlot, MergeMode, Node, NodeContext, NodeError, NodeKey,
    NodeRole as Role, RetryPolicy, SlotSpec, TaskType, Trigger,
};
pub use builder::{GraphBuilder, SubgraphBuilder};
pub use description::{EdgeDescription, GraphDescription, NodeDescription, SubgraphDescription};
pub use erased::ErasedGraph;

/// 节点实例、展示信息与预计算的运行时隔离作用域路径共用一条记录。
struct GraphNode<Ctx> {
    node: Box<dyn AnyNode<Ctx>>,
    title: String,
    description: String,
    subgraph_id: Option<String>,
    scope_path: Arc<[Arc<str>]>,
}

/// 图引擎：节点注册表 + 按源节点索引的成功/失败边集合。
pub struct Graph<Ctx> {
    workflow_id: String,
    title: String,
    context: Ctx,
    nodes: HashMap<Arc<str>, GraphNode<Ctx>>,
    subgraphs: Vec<SubgraphDescription>,
    success_edges: Vec<Edge>,
    failure_edges: Vec<Edge>,
    success_adj: HashMap<Arc<str>, Vec<usize>>,
    failure_adj: HashMap<Arc<str>, Vec<usize>>,
    settlement_rules: SettlementRules,
}

fn qualified_parent(id: &str) -> Option<&str> {
    id.rsplit_once('.').map(|(parent, _)| parent)
}

/// 已校验的子图 ID 由各级子图全名逐级限定。
fn subgraph_path(group: Option<&str>) -> impl Iterator<Item = &str> {
    group.into_iter().flat_map(|id| {
        id.match_indices('.')
            .map(move |(separator, _)| &id[..separator])
            .chain(std::iter::once(id))
    })
}

impl<Ctx> Graph<Ctx> {
    pub fn builder(
        workflow_id: impl Into<String>,
        title: impl Into<String>,
        context: Ctx,
    ) -> GraphBuilder<Ctx>
    where
        Ctx: Send,
    {
        GraphBuilder::new(workflow_id, title, context)
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn context(&self) -> &Ctx {
        &self.context
    }

    pub fn task_type_of(&self, name: &str) -> Option<TaskType> {
        self.nodes
            .get(name)
            .map(|entry| entry.node.role().task_type())
    }

    pub fn retry_policy_of(&self, name: &str) -> Option<RetryPolicy> {
        self.nodes
            .get(name)
            .and_then(|entry| entry.node.retry_policy())
    }

    /// 入口提交前的强类型输入预校验（直接借用 `&Value`，零额外克隆）。
    pub fn validate_input(&self, name: &str, input: &Value) -> Result<(), EngineError> {
        let entry = self
            .nodes
            .get(name)
            .ok_or_else(|| EngineError::UnknownNode(name.to_owned()))?;
        entry.node.validate_input(input).map_err(EngineError::Input)
    }

    /// 按节点名排序的外部触发声明；只读取声明，不建立监听。
    pub fn triggers(&self) -> Vec<(&str, Trigger)> {
        let mut triggers: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(name, entry)| entry.node.trigger().map(|t| (name.as_ref(), t)))
            .collect();
        triggers.sort_by(|left, right| left.0.cmp(right.0));
        triggers
    }

    /// 执行建图时注册的节点实例，不触发上下文钩子。
    pub async fn execute(&self, name: &str, ctx: &mut Ctx, input: Value) -> Result<Value, NodeError>
    where
        Ctx: Send,
    {
        let node = self
            .nodes
            .get(name)
            .ok_or_else(|| NodeError::permanent(format!("no node registered for name={name}")))?;
        node.node.execute(name, ctx, input).await
    }

    /// 节点的运行时隔离作用域路径；根节点或仅属于纯视觉分组的节点为 Some([])，不存在为 None。
    pub fn subgraph_path(&self, name: &str) -> Option<Vec<String>> {
        let entry = self.nodes.get(name)?;
        Some(entry.scope_path.iter().map(|s| s.to_string()).collect())
    }

    /// O(出度) 规划成功后继，直接复用预计算的隔离作用域路径与目标重试预算。
    pub fn plan_success(&self, name: &str, out: &Value) -> Result<Vec<Successor>, EdgeError> {
        let Some(indices) = self.success_adj.get(name) else {
            return Ok(Vec::new());
        };
        let mut plans = Vec::with_capacity(indices.len());
        for &idx in indices {
            let edge = &self.success_edges[idx];
            let inputs = edge.plan_success(out)?;
            if inputs.is_empty() {
                continue;
            }
            plans.push(self.build_successor(edge, inputs));
        }
        Ok(plans)
    }

    /// O(出度) 规划失败后继，指向 JOIN 槽位的失败边作为降级贡献合入同作用域的 collecting JOIN。
    pub fn plan_failure(
        &self,
        name: &str,
        input: &Value,
        err: &NodeError,
    ) -> Result<Vec<Successor>, EdgeError> {
        let Some(indices) = self.failure_adj.get(name) else {
            return Ok(Vec::new());
        };
        let mut plans = Vec::with_capacity(indices.len());
        for &idx in indices {
            let edge = &self.failure_edges[idx];
            let inputs = edge.plan_failure(input, err)?;
            if inputs.is_empty() {
                continue;
            }
            plans.push(self.build_successor(edge, inputs));
        }
        Ok(plans)
    }

    fn build_successor(&self, edge: &Edge, inputs: Vec<Value>) -> Successor {
        let dst_entry = &self.nodes[edge.dst()];
        let dispatch = match edge.slot() {
            Some(slot) => Dispatch::Join {
                key: slot.key,
                merge: slot.merge,
            },
            None => edge.mode().task_dispatch(),
        };
        Successor {
            dst: edge.dst_arc(),
            subgraph_path: dst_entry.scope_path.clone(),
            inputs,
            dispatch,
            max_attempts: dst_entry
                .node
                .retry_policy()
                .map(RetryPolicy::effective_max_attempts),
        }
    }

    pub(crate) fn settlement_rules(&self) -> &SettlementRules {
        &self.settlement_rules
    }
}

impl EdgeMode {
    fn task_dispatch(self) -> Dispatch {
        match self {
            Self::Direct | Self::SharedExpand => Dispatch::Shared,
            Self::IsolatedDirect | Self::IsolatedExpand => Dispatch::PerInputScope,
        }
    }
}
