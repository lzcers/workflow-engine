//! 图引擎：节点注册表 + 边集合，负责执行、后继规划、静态校验。
//!
//! 构图提供两种方式：
//! - 命令式：[`Graph::builder`] 通过 [`success`]、[`failure`] 及带转换的构造函数注册边，在 `build()` 时校验端口类型；
//! - 声明式：[`crate::workflow!`] 宏，在编译期完成节点接线类型检查。

use serde_json::Value;
use std::collections::{HashMap, HashSet};

use super::node::{AnyNode, TypeFingerprint};
use crate::task::{Dispatch, JoinPredecessor, JoinRule, SettlementRules, Successor};

mod builder;
mod description;
mod erased;

pub use crate::edge::{
    Arrow, EdgeError, EdgeMode, FailureEdge, SuccessEdge, direct, failure, failure_transform,
    isolated_expand, shared_expand, success, success_transform,
};
pub use crate::node::{JoinNode, Node, NodeContext, NodeError, NodeKey, TaskType, Trigger};
pub use builder::GraphBuilder;
pub use description::{
    EdgeDescription, EdgeKind, GraphDescription, NodeDescription, SubgraphDescription,
};
pub use erased::ErasedGraph;

/// 节点实例与注册时的展示信息共用一条记录，保证描述对应实际执行的节点。
struct GraphNode<Ctx> {
    node: Box<dyn AnyNode<Ctx>>,
    title: String,
    description: String,
    subgraph_id: Option<String>,
}

/// 图引擎：节点注册表 + 成功/失败边集合，持有上下文与节点实例，负责执行、后继规划、静态校验。
///
/// 成功边与失败边分置两个集合，对应两条独立路由维度：前向主 DAG 与后向回退图。
pub struct Graph<Ctx> {
    workflow_id: String,
    title: String,
    context: Ctx,
    nodes: HashMap<&'static str, GraphNode<Ctx>>,
    subgraphs: Vec<SubgraphDescription>,
    success_edges: Vec<SuccessEdge>,
    failure_edges: Vec<FailureEdge>,
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

    /// 工作流的稳定身份，用于关联任务与运行。
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// 工作流的展示标题。
    pub fn title(&self) -> &str {
        &self.title
    }

    /// 图持有的上下文快照；任务从这里 fork 独立的瞬态字段。
    pub fn context(&self) -> &Ctx {
        &self.context
    }

    /// 入队时使用目标节点声明的任务类型；不存在的入口由调用方报告。
    pub fn task_type_of(&self, name: &str) -> Option<TaskType> {
        self.nodes.get(name).map(|entry| entry.node.task_type())
    }

    /// 按节点名排序的外部触发声明。节点名可直接作为 Workflow::submit 的入口。
    /// 只读取声明，不注册触发器或创建任务。
    pub fn triggers(&self) -> Vec<(&str, Trigger)> {
        let mut triggers: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(&name, entry)| entry.node.trigger().map(|trigger| (name, trigger)))
            .collect();
        triggers.sort_by(|left, right| left.0.cmp(right.0));
        triggers
    }

    /// 执行节点前发布任务开始事件。
    pub(crate) async fn on_start(&self, name: &str, ctx: &mut Ctx, input: &Value) {
        if let Some(node) = self.nodes.get(name) {
            node.node.on_start(ctx, input).await;
        }
    }

    /// 成功落库后发布任务成功事件。
    pub(crate) async fn on_success(&self, name: &str, ctx: &mut Ctx, output: &Value) {
        if let Some(node) = self.nodes.get(name) {
            node.node.on_success(ctx, output).await;
        }
    }

    /// 执行失败时发布任务失败事件。
    pub(crate) async fn on_error(&self, name: &str, ctx: &mut Ctx, error: &NodeError) {
        if let Some(node) = self.nodes.get(name) {
            node.node.on_error(ctx, error).await;
        }
    }

    /// 执行建图时注册的节点实例，不触发生命周期钩子。
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

    /// 节点所属子图路径；根节点为 Some([])，不存在的节点为 None。
    pub fn subgraph_path(&self, name: &str) -> Option<Vec<String>> {
        let group = self.nodes.get(name)?.subgraph_id.as_deref();
        Some(subgraph_path(group).map(str::to_owned).collect())
    }

    /// 每条成功边独立规划，`->+` 的每份输入各自进入一个子图实例。
    pub fn plan_success(&self, name: &str, out: &Value) -> Result<Vec<Successor>, EdgeError> {
        let mut plans = Vec::new();
        for edge in self.success_edges.iter().filter(|edge| edge.src() == name) {
            let inputs = edge.plan_success(out)?;
            if inputs.is_empty() {
                continue;
            }
            let dispatch = if self.settlement_rules.join_rules.contains_key(edge.dst()) {
                if inputs.iter().any(|input| !input.is_object()) {
                    return Err(EdgeError::BadDstIn {
                        src: edge.src(),
                        detail: format!(
                            "JOIN {} requires a JSON object for each contribution",
                            edge.dst()
                        ),
                    });
                }
                Dispatch::Join
            } else {
                edge.mode().task_dispatch()
            };
            plans.push(Successor {
                dst: edge.dst(),
                subgraph_path: self
                    .subgraph_path(edge.dst())
                    .expect("validated edge destination"),
                inputs,
                dispatch,
            });
        }
        Ok(plans)
    }

    /// 失败边也按子图边界进入或退出；失败路由不建立成功 JOIN 等待条件。
    pub fn plan_failure(
        &self,
        name: &str,
        input: &Value,
        err: &NodeError,
    ) -> Result<Vec<Successor>, EdgeError> {
        let mut plans = Vec::new();
        for edge in self.failure_edges.iter().filter(|edge| edge.src() == name) {
            let inputs = edge.plan_failure(input, err)?;
            if inputs.is_empty() {
                continue;
            }
            plans.push(Successor {
                dst: edge.dst(),
                subgraph_path: self
                    .subgraph_path(edge.dst())
                    .expect("validated edge destination"),
                inputs,
                dispatch: edge.mode().task_dispatch(),
            });
        }
        Ok(plans)
    }

    /// 存储在结算时使用这些预计算的静态规则，按作用域匹配来源并推进就绪或失败状态。
    pub(crate) fn settlement_rules(&self) -> &SettlementRules {
        &self.settlement_rules
    }

    fn compute_settlement_rules(&self) -> SettlementRules {
        let join_rules = self
            .nodes
            .iter()
            .filter(|(_, entry)| entry.node.task_type() == TaskType::Join)
            .map(|(&name, _)| (name.to_owned(), self.join_rule(name)))
            .collect();
        SettlementRules { join_rules }
    }

    fn join_rule(&self, dst: &str) -> JoinRule {
        let target = self.subgraph_path(dst).expect("validated JOIN node");
        let mut pending = self.predecessors_of(dst).expect("validated JOIN node");
        let predecessors = pending
            .iter()
            .map(|name| {
                let source = self
                    .subgraph_path(name)
                    .expect("validated JOIN predecessor");
                let entry_depth = (!source.starts_with(&target)).then(|| {
                    source
                        .iter()
                        .zip(&target)
                        .take_while(|(a, b)| a == b)
                        .count()
                });
                JoinPredecessor {
                    name: name.clone(),
                    entry_depth,
                }
            })
            .collect();
        let mut seen = HashSet::new();
        while let Some(name) = pending.pop() {
            if !seen.insert(name.clone()) {
                continue;
            }
            pending.extend(
                self.success_edges
                    .iter()
                    .filter(|edge| edge.dst() == name)
                    .map(|edge| edge.src().to_owned()),
            );
        }
        let mut upstreams: Vec<_> = seen.into_iter().collect();
        upstreams.sort();
        JoinRule {
            predecessors,
            upstreams,
        }
    }

    /// 节点的静态前驱节点名集合：返回所有成功边汇入 `dst` 的源节点名全集。
    ///
    /// 不存在的节点为 None，存在但没有成功前驱的节点为 Some([])。
    /// JOIN 的有效等待集合还需由存储结合当前作用域入口筛选。
    pub fn predecessors_of(&self, dst: &str) -> Option<Vec<String>> {
        self.nodes.get(dst)?;
        let mut set: HashSet<&'static str> = HashSet::new();
        for e in self.success_edges.iter().filter(|e| e.dst() == dst) {
            set.insert(e.src());
        }
        let mut names: Vec<_> = set.into_iter().map(str::to_owned).collect();
        names.sort();
        Some(names)
    }

    /// 静态校验：元数据非空、子图归属与 `->+` 边界合法、Event 触发声明一致且无入边、
    /// success 子图无环、failure 回边不自环。
    fn validate(&self) -> Result<(), String> {
        if self.workflow_id.trim().is_empty() {
            return Err("workflow_id must not be empty".into());
        }
        if self.title.trim().is_empty() {
            return Err("workflow title must not be empty".into());
        }
        self.validate_subgraphs()?;
        for (name, entry) in &self.nodes {
            match (entry.node.task_type(), entry.node.trigger()) {
                (TaskType::Event, None) => {
                    return Err(format!("event node {name} must declare a trigger"));
                }
                (TaskType::Normal | TaskType::Join, Some(_)) => {
                    return Err(format!("non-event node {name} must not declare a trigger"));
                }
                _ => {}
            }
        }
        // 所有边端点已在 validate_subgraphs 中校验存在；此处统一检查 Event 目标限制与端口类型指纹。
        for (kind, src, dst, edge_src_ty, node_src_ty, src_role, edge_dst_ty) in self
            .success_edges
            .iter()
            .map(|e| {
                (
                    "success",
                    e.src(),
                    e.dst(),
                    e.src_out_type(),
                    self.nodes[e.src()].node.out_type(),
                    "输出",
                    e.dst_in_type(),
                )
            })
            .chain(self.failure_edges.iter().map(|e| {
                (
                    "failure",
                    e.src(),
                    e.dst(),
                    e.src_in_type(),
                    self.nodes[e.src()].node.in_type(),
                    "输入",
                    e.dst_in_type(),
                )
            }))
        {
            let dst_node = &self.nodes[dst].node;
            if dst_node.task_type() == TaskType::Event {
                return Err(format!("{kind} edge must not target event node {dst}"));
            }
            if !readable(edge_src_ty, node_src_ty) {
                return Err(format!(
                    "edge {src} -> {dst}: transform 入参类型 {} 与节点 {src} {src_role}类型 {} 不匹配",
                    edge_src_ty.name(),
                    node_src_ty.name()
                ));
            }
            if !writable(edge_dst_ty, dst_node.in_type()) {
                return Err(format!(
                    "edge {src} -> {dst}: transform 出参类型 {} 与节点 {dst} 输入类型 {} 不匹配",
                    edge_dst_ty.name(),
                    dst_node.in_type().name()
                ));
            }
        }

        // success 子图无环（拓扑排序）
        let mut indegree: HashMap<&'static str, usize> =
            self.nodes.keys().map(|&k| (k, 0)).collect();
        let mut succ: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
        for e in self.success_edges.iter() {
            *indegree.entry(e.dst()).or_insert(0) += 1;
            succ.entry(e.src()).or_default().push(e.dst());
        }
        let mut queue: Vec<&'static str> = indegree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&k, _)| k)
            .collect();
        let mut visited = 0;
        while let Some(n) = queue.pop() {
            visited += 1;
            if let Some(next) = succ.get(n) {
                for &m in next {
                    let d = indegree.get_mut(m).unwrap();
                    *d -= 1;
                    if *d == 0 {
                        queue.push(m);
                    }
                }
            }
        }
        if visited != self.nodes.len() {
            return Err("success subgraph contains a cycle".into());
        }

        // failure 回边允许指向任意上游做局部重试，但禁止指回自身（自环会无限失败重试）
        for e in self.failure_edges.iter() {
            if e.src() == e.dst() {
                return Err(format!(
                    "failure edge src={} dst={} must not loop back to itself",
                    e.src(),
                    e.dst()
                ));
            }
        }
        Ok(())
    }
}

impl EdgeMode {
    fn task_dispatch(self) -> Dispatch {
        match self {
            Self::Direct | Self::SharedExpand => Dispatch::Shared,
            Self::IsolatedExpand => Dispatch::PerInputScope,
        }
    }
}

/// 边「读取」节点值方向的指纹兼容判定：边把节点序列化值反序列化为 `edge_ty`。
///
/// 类型一致即兼容；`edge_ty` 为 `Value` 时恒兼容（`from_value::<Value>` 必成功）。
/// 反之节点侧为 `Value` 不构成通配——反序列化为具体类型取决于内容，运行时可能失败。
fn readable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || edge_ty.id() == TypeFingerprint::of::<Value>().id()
}

/// 边「写入」节点值方向的指纹兼容判定：节点把边产出反序列化为 `node_ty`。
///
/// 类型一致即兼容；`node_ty` 为 `Value` 时恒兼容（`from_value::<Value>` 必成功）——
/// 这是具体类型载荷汇入 `JoinNode`（`In = Value`）这类 JSON 边界模式的依据。
fn writable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || node_ty.id() == TypeFingerprint::of::<Value>().id()
}
