//! 图引擎：节点注册表 + 邻接索引边集合，负责执行、后继规划、静态校验与结算规则预计算。

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde_json::Value;

use super::node::{AnyNode, NodeRole, TypeFingerprint};
use crate::{
    EngineError,
    error::GraphError,
    task::{Dispatch, JoinExternalEntry, JoinRule, SettlementRules, Successor},
};

mod builder;
mod description;
mod erased;

pub use crate::edge::{
    Arrow, Edge, EdgeError, EdgeKind, EdgeMode, direct, isolated_direct, isolated_expand,
    shared_expand,
};
pub use crate::node::{
    JoinInput, JoinNode, JoinSlot, MergeMode, Node, NodeContext, NodeError, NodeKey, NodeRole as Role,
    RetryPolicy, SlotSpec, TaskType, Trigger,
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

    /// 校验通过后预计算作用域路径、邻接索引与静态结算规则。
    fn finalize(&mut self) {
        let isolated_set: HashSet<&str> = self
            .subgraphs
            .iter()
            .filter(|s| s.isolated)
            .map(|s| s.id.as_str())
            .collect();
        for entry in self.nodes.values_mut() {
            let path: Vec<Arc<str>> = subgraph_path(entry.subgraph_id.as_deref())
                .filter(|id| isolated_set.contains(id))
                .map(Arc::from)
                .collect();
            entry.scope_path = Arc::from(path);
        }

        self.success_adj.clear();
        for (idx, edge) in self.success_edges.iter().enumerate() {
            self.success_adj
                .entry(edge.src_arc())
                .or_default()
                .push(idx);
        }
        self.failure_adj.clear();
        for (idx, edge) in self.failure_edges.iter().enumerate() {
            self.failure_adj
                .entry(edge.src_arc())
                .or_default()
                .push(idx);
        }

        self.settlement_rules = self.compute_settlement_rules();
    }

    fn compute_settlement_rules(&self) -> SettlementRules {
        // 反向邻接表：O(V + E) 替代全表扫描。
        let mut incoming_all: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in self.success_edges.iter().chain(&self.failure_edges) {
            incoming_all
                .entry(edge.dst())
                .or_default()
                .push(edge.src());
        }

        let mut join_rules = HashMap::new();
        let mut affected_joins: HashMap<Arc<str>, Vec<Arc<str>>> = HashMap::new();

        for (name, entry) in &self.nodes {
            let NodeRole::Join(specs) = entry.node.role() else {
                continue;
            };
            let rule = self.build_join_rule(name, &entry.scope_path, &specs, &incoming_all);
            for upstream in &rule.upstreams {
                affected_joins
                    .entry(Arc::from(upstream.as_str()))
                    .or_default()
                    .push(name.clone());
            }
            join_rules.insert(name.clone(), rule);
        }
        for joins in affected_joins.values_mut() {
            joins.sort();
            joins.dedup();
        }
        SettlementRules {
            join_rules,
            affected_joins,
        }
    }

    fn build_join_rule(
        &self,
        dst: &str,
        target_scope: &[Arc<str>],
        specs: &[SlotSpec],
        incoming_all: &HashMap<&str, Vec<&str>>,
    ) -> JoinRule {
        let required_slots: Vec<String> = specs
            .iter()
            .filter(|spec| spec.required)
            .map(|spec| spec.key.to_owned())
            .collect();
        let append_keys: Vec<String> = specs
            .iter()
            .filter(|spec| spec.merge == MergeMode::Append)
            .map(|spec| spec.key.to_owned())
            .collect();

        // 识别所有跨边界进入本 JOIN 的直接前驱（包含成功边与失败边），记录公共前缀深度。
        let mut ext_seen: HashMap<&str, usize> = HashMap::new();
        if let Some(srcs) = incoming_all.get(dst) {
            for &src in srcs {
                let src_scope = &self.nodes[src].scope_path;
                if !src_scope.starts_with(target_scope) {
                    let depth = src_scope
                        .iter()
                        .zip(target_scope)
                        .take_while(|(a, b)| a == b)
                        .count();
                    ext_seen.insert(src, depth);
                }
            }
        }
        let mut external_entries: Vec<JoinExternalEntry> = ext_seen
            .into_iter()
            .map(|(name, entry_depth)| JoinExternalEntry {
                name: name.to_owned(),
                entry_depth,
            })
            .collect();
        external_entries.sort_by(|a, b| a.name.cmp(&b.name));

        // 反向 BFS 覆盖所有仍可能为该 JOIN 产出贡献的上游（排除 dst 自身）。
        let mut stack: Vec<&str> = incoming_all
            .get(dst)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
            .to_vec();
        let mut seen: HashSet<&str> = HashSet::new();
        while let Some(node) = stack.pop() {
            if node == dst || !seen.insert(node) {
                continue;
            }
            if let Some(preds) = incoming_all.get(node) {
                stack.extend(preds.iter().copied());
            }
        }
        let mut upstreams: Vec<String> = seen.into_iter().map(str::to_owned).collect();
        upstreams.sort();

        JoinRule {
            scope_depth: target_scope.len(),
            required_slots,
            append_keys,
            external_entries,
            upstreams,
        }
    }

    /// 静态校验：元数据非空、子图归属与隔离边界合法、JOIN 槽位完整且类型匹配、
    /// success 子图无环、failure 回边不自环。
    fn validate(&self) -> Result<(), GraphError> {
        if self.workflow_id.trim().is_empty() {
            return Err(GraphError::EmptyWorkflowId);
        }
        if self.title.trim().is_empty() {
            return Err(GraphError::EmptyTitle);
        }
        self.validate_subgraphs()?;

        let mut join_specs: HashMap<&str, Vec<SlotSpec>> = HashMap::new();
        for (name, entry) in &self.nodes {
            if let NodeRole::Join(specs) = entry.node.role() {
                let mut keys = HashSet::new();
                for spec in &specs {
                    if !keys.insert(spec.key) {
                        return Err(GraphError::InvalidJoin {
                            node: name.to_string(),
                            detail: format!("duplicate slot `{}`", spec.key),
                        });
                    }
                }
                join_specs.insert(name.as_ref(), specs);
            }
        }

        let isolated_set: HashSet<&str> = self
            .subgraphs
            .iter()
            .filter(|s| s.isolated)
            .map(|s| s.id.as_str())
            .collect();
        let mut covered_slots: HashMap<(&str, &str), usize> = HashMap::new();
        let mut in_scope_unconditional_set: HashMap<(&str, &str), usize> = HashMap::new();

        for edge in self.success_edges.iter().chain(&self.failure_edges) {
            let src = edge.src();
            let dst = edge.dst();
            let src_node = &self.nodes[src];
            let dst_node = &self.nodes[dst];
            let (node_src_ty, role_label) = match edge.kind() {
                EdgeKind::Success => (src_node.node.out_type(), "output"),
                EdgeKind::Failure => (src_node.node.in_type(), "input"),
            };
            if !readable(edge.src_type(), node_src_ty) {
                return Err(GraphError::InvalidEdge {
                    src: src.to_owned(),
                    dst: dst.to_owned(),
                    detail: format!(
                        "edge reads {} which does not match source {role_label} type {}",
                        edge.src_type().name(),
                        node_src_ty.name()
                    ),
                });
            }

            match (dst_node.node.role().task_type(), edge.slot()) {
                (TaskType::Join, None) => {
                    return Err(GraphError::InvalidEdge {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                        detail: format!("edge targeting JOIN node must specify a slot ({dst}::field)"),
                    });
                }
                (TaskType::Join, Some(edge_slot)) => {
                    let specs = &join_specs[dst];
                    let Some(spec) = specs.iter().find(|s| s.key == edge_slot.key) else {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!("unknown JOIN slot `{}`", edge_slot.key),
                        });
                    };
                    if spec.merge != edge_slot.merge {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!("slot `{}` merge mode mismatch", edge_slot.key),
                        });
                    }
                    if !writable(edge.dst_type(), spec.item_type) {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "slot `{}` contribution type {} does not match {}",
                                edge_slot.key,
                                edge.dst_type().name(),
                                spec.item_type.name()
                            ),
                        });
                    }
                    if spec.merge == MergeMode::Set && edge.mode().is_expand() {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "single-value JOIN slot `{}` does not accept list expansion",
                                edge_slot.key
                            ),
                        });
                    }
                    *covered_slots.entry((dst, spec.key)).or_insert(0) += 1;

                    // 同作用域内多条无条件成功边禁止共写同一个单值 Set 槽位；条件边（XOR-Merge）允许。
                    if edge.kind() == EdgeKind::Success
                        && spec.merge == MergeMode::Set
                        && !edge.has_condition()
                    {
                        let src_scopes: Vec<_> = subgraph_path(src_node.subgraph_id.as_deref())
                            .filter(|id| isolated_set.contains(id))
                            .collect();
                        let dst_scopes: Vec<_> = subgraph_path(dst_node.subgraph_id.as_deref())
                            .filter(|id| isolated_set.contains(id))
                            .collect();
                        if src_scopes.starts_with(&dst_scopes) {
                            let count = in_scope_unconditional_set
                                .entry((dst, spec.key))
                                .or_insert(0);
                            *count += 1;
                            if *count > 1 {
                                return Err(GraphError::InvalidJoin {
                                    node: dst.to_owned(),
                                    detail: format!(
                                        "slot `{}` has multiple unconditional in-scope writers; mark it #[join(collect)] or guard with when(...)",
                                        spec.key
                                    ),
                                });
                            }
                        }
                    }
                }
                (TaskType::Normal, Some(edge_slot)) => {
                    return Err(GraphError::InvalidEdge {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                        detail: format!(
                            "slot syntax `::{}` is only valid for JOIN nodes",
                            edge_slot.key
                        ),
                    });
                }
                (TaskType::Normal, None) => {
                    if !writable(edge.dst_type(), dst_node.node.in_type()) {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "edge output type {} does not match destination input type {}",
                                edge.dst_type().name(),
                                dst_node.node.in_type().name()
                            ),
                        });
                    }
                }
            }
        }

        let mut join_names: Vec<&str> = join_specs.keys().copied().collect();
        join_names.sort_unstable();
        for name in join_names {
            for spec in &join_specs[name] {
                if spec.required && !covered_slots.contains_key(&(name, spec.key)) {
                    return Err(GraphError::InvalidJoin {
                        node: name.to_owned(),
                        detail: format!("missing incoming edge for required slot `{}`", spec.key),
                    });
                }
            }
        }

        // success 子图无环（Kahn 拓扑排序）
        let mut indegree: HashMap<&str, usize> =
            self.nodes.keys().map(|k| (k.as_ref(), 0)).collect();
        let mut succ: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.success_edges {
            *indegree.entry(e.dst()).or_insert(0) += 1;
            succ.entry(e.src()).or_default().push(e.dst());
        }
        let mut queue: Vec<&str> = indegree
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
            return Err(GraphError::SuccessCycle);
        }

        // failure 回边禁止指回自身（非自环循环由运行期 max_tasks_per_run 护栏限制）。
        for e in &self.failure_edges {
            if e.src() == e.dst() {
                return Err(GraphError::FailureSelfLoop {
                    src: e.src().to_owned(),
                    dst: e.dst().to_owned(),
                });
            }
        }
        Ok(())
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

/// 边「读取」节点值方向的指纹兼容判定：`edge_ty == node_ty` 或 `edge_ty == Value`。
fn readable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || edge_ty.id() == TypeFingerprint::of::<Value>().id()
}

/// 边「写入」节点值方向的指纹兼容判定：`edge_ty == node_ty` 或 `node_ty == Value`。
fn writable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || node_ty.id() == TypeFingerprint::of::<Value>().id()
}
