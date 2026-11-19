//! 图引擎：节点注册表 + 边集合，负责执行、后继规划、静态校验。
//!
//! 节点名字作为 [`Graph`] 的 `HashMap` key 在注册时外部绑定；边
//! 通过 `src`/`dst` 名字引用节点。
//!
//! [`GraphBuilder`] 负责注册节点、追加边、组装并校验。

use serde_json::Value;
use std::collections::{HashMap, HashSet};

use super::edge::{AnyFailureEdge, AnySuccessEdge};
use super::node::{AnyNode, TypeFingerprint};

mod builder;
mod description;
mod erased;

pub use crate::edge::{
    EdgeError, EdgeImpl, ErrorWhenBuilder, FailureEdge, FailureEdgeImpl, SuccessEdge, WhenBuilder,
    foreach, map, on_error, when, when_error,
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
/// 成功边与失败边分置两个集合，
/// 对应两种独立路由维度：前向主 DAG 与后向回退图。
pub struct Graph<Ctx> {
    workflow_id: String,
    title: String,
    context: Ctx,
    nodes: HashMap<&'static str, GraphNode<Ctx>>,
    subgraphs: Vec<SubgraphDescription>,
    success_edges: Vec<Box<dyn AnySuccessEdge>>,
    failure_edges: Vec<Box<dyn AnyFailureEdge>>,
}

/// 一条实际边的后继规划，保留边身份及逐项输入顺序。
#[derive(Debug)]
pub struct Successor {
    pub edge_id: String,
    pub dst: &'static str,
    pub task_type: TaskType,
    pub foreach: bool,
    pub source_subgraph_path: Vec<String>,
    pub destination_subgraph_path: Vec<String>,
    pub inputs: Vec<Value>,
    pub depends_on_names: Vec<String>,
    /// 包含直接前驱及全部传递上游，用于解析跨子图依赖。
    pub predecessor_subgraph_paths: HashMap<String, Vec<String>>,
    pub join_incoming_edges: HashMap<String, String>,
    pub join_upstream_names: Vec<String>,
}

impl<Ctx> Graph<Ctx> {
    /// 构建工作流图；稳定 ID 和展示标题由图定义统一声明。
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
    pub async fn on_start(&self, name: &str, ctx: &mut Ctx, input: &Value) {
        if let Some(node) = self.nodes.get(name) {
            node.node.on_start(ctx, input).await;
        }
    }

    /// 成功落库后发布任务成功事件。
    pub async fn on_success(&self, name: &str, ctx: &mut Ctx, output: &Value) {
        if let Some(node) = self.nodes.get(name) {
            node.node.on_success(ctx, output).await;
        }
    }

    /// 执行失败时发布任务失败事件。
    pub async fn on_error(&self, name: &str, ctx: &mut Ctx, error: &NodeError) {
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

    /// 节点所属子图的完整路径，按外层到内层排列。
    pub fn subgraph_path(&self, name: &str) -> Vec<String> {
        let mut path = Vec::new();
        let mut current = self
            .nodes
            .get(name)
            .and_then(|node| node.subgraph_id.as_deref());
        while let Some(id) = current {
            path.push(id.to_owned());
            current = self
                .subgraphs
                .iter()
                .find(|subgraph| subgraph.id == id)
                .and_then(|subgraph| subgraph.parent_id.as_deref());
        }
        path.reverse();
        path
    }

    /// 所有成功边分别规划；不同 foreach 边不合并，也不按下标隐式配对。
    pub fn plan_success(&self, name: &str, out: &Value) -> Result<Vec<Successor>, EdgeError> {
        let mut plans = Vec::new();
        for (index, edge) in self
            .success_edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| edge.src() == name)
        {
            let inputs = edge.plan_success(out)?;
            if inputs.is_empty() {
                continue;
            }
            let dst = edge.dst();
            let task_type = self.task_type_of(dst).expect("validated edge destination");
            let depends_on_names = self.predecessors_of(dst);
            let join_upstream_names = if task_type == TaskType::Join {
                self.upstream_names(dst)
            } else {
                Vec::new()
            };
            let predecessor_subgraph_paths = depends_on_names
                .iter()
                .chain(&join_upstream_names)
                .map(|name| (name.clone(), self.subgraph_path(name)))
                .collect();
            let join_incoming_edges = if task_type == TaskType::Join {
                self.success_edges
                    .iter()
                    .enumerate()
                    .filter(|(_, edge)| edge.dst() == dst)
                    .map(|(index, edge)| (format!("success:{index}"), edge.src().to_owned()))
                    .collect()
            } else {
                HashMap::new()
            };
            plans.push(Successor {
                edge_id: format!("success:{index}"),
                dst,
                task_type,
                foreach: edge.is_foreach(),
                source_subgraph_path: self.subgraph_path(name),
                destination_subgraph_path: self.subgraph_path(dst),
                inputs,
                depends_on_names,
                predecessor_subgraph_paths,
                join_incoming_edges,
                join_upstream_names,
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
        for (index, edge) in self
            .failure_edges
            .iter()
            .enumerate()
            .filter(|(_, edge)| edge.src() == name)
        {
            let inputs = edge.plan_failure(input, err)?;
            if inputs.is_empty() {
                continue;
            }
            plans.push(Successor {
                edge_id: format!("failure:{index}"),
                dst: edge.dst(),
                task_type: self
                    .task_type_of(edge.dst())
                    .expect("validated edge destination"),
                foreach: false,
                source_subgraph_path: self.subgraph_path(name),
                destination_subgraph_path: self.subgraph_path(edge.dst()),
                inputs,
                depends_on_names: Vec::new(),
                predecessor_subgraph_paths: HashMap::new(),
                join_incoming_edges: HashMap::new(),
                join_upstream_names: Vec::new(),
            });
        }
        Ok(plans)
    }

    fn upstream_names(&self, dst: &'static str) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut pending = self.predecessors_of(dst);
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
        let mut names: Vec<_> = seen.into_iter().collect();
        names.sort();
        names
    }

    /// 节点的静态前驱节点名集合：返回所有成功边汇入 `dst` 的源节点名全集。
    ///
    /// 通用血缘语义：普通节点也记录自己的静态前驱；是否用其做「等齐前驱」闸门
    /// 由目标节点的 `task_type` 决定（仅 [`TaskType::Join`] 生效）。
    fn predecessors_of(&self, dst: &'static str) -> Vec<String> {
        let mut set: HashSet<&'static str> = HashSet::new();
        for e in self.success_edges.iter().filter(|e| e.dst() == dst) {
            set.insert(e.src());
        }
        {
            let mut names: Vec<_> = set.into_iter().map(str::to_owned).collect();
            names.sort();
            names
        }
    }

    /// 静态校验：节点名唯一、边两端节点存在、边类型接线指纹一致、success 子图无环、
    /// failure 回边不自环，Event 触发声明一致且无入边。
    ///
    /// 类型接线指纹校验是命令式建图（非 DSL）的兜底：DSL 边有编译期检查，命令式边只有
    /// 闭包自洽的类型参数，与注册节点 `In`/`Out` 的错线此前要到任务结算时才以 serde 错误
    /// 暴露，这里在 build 期提前拒绝；`Value` 可作为任意载荷的读取或写入边界。
    pub fn validate(&self) -> Result<(), String> {
        if self.workflow_id.trim().is_empty() {
            return Err("workflow_id must not be empty".into());
        }
        if self.title.trim().is_empty() {
            return Err("workflow title must not be empty".into());
        }
        self.describe().validate_subgraphs()?;
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
        // 节点名与边引用一致性 + 边类型接线指纹（成功边）
        for e in self.success_edges.iter() {
            let Some(src) = self.nodes.get(e.src()) else {
                return Err(format!("edge src={} not registered", e.src()));
            };
            let Some(dst) = self.nodes.get(e.dst()) else {
                return Err(format!("edge dst={} not registered", e.dst()));
            };
            if dst.node.task_type() == TaskType::Event {
                return Err(format!(
                    "success edge must not target event node {}",
                    e.dst()
                ));
            }
            if e.is_foreach() && dst.node.task_type() == TaskType::Join {
                return Err(format!(
                    "foreach edge must not target join node {}",
                    e.dst()
                ));
            }
            // 边把源节点输出反序列化为 SrcOut：要求与源节点 Out 一致（或边载荷为 Value）
            if !readable(e.src_out_type(), src.node.out_type()) {
                return Err(format!(
                    "edge {} -> {}: transform 入参类型 {} 与节点 {} 输出类型 {} 不匹配",
                    e.src(),
                    e.dst(),
                    e.src_out_type().name(),
                    e.src(),
                    src.node.out_type().name()
                ));
            }
            // 目标节点把边产出反序列化为自身 In：要求与目标节点 In 一致（或节点输入为 Value）
            if !writable(e.dst_in_type(), dst.node.in_type()) {
                return Err(format!(
                    "edge {} -> {}: transform 出参类型 {} 与节点 {} 输入类型 {} 不匹配",
                    e.src(),
                    e.dst(),
                    e.dst_in_type().name(),
                    e.dst(),
                    dst.node.in_type().name()
                ));
            }
        }
        // 节点名与边引用一致性 + 边类型接线指纹（失败边）
        for e in self.failure_edges.iter() {
            let Some(src) = self.nodes.get(e.src()) else {
                return Err(format!("edge src={} not registered", e.src()));
            };
            let Some(dst) = self.nodes.get(e.dst()) else {
                return Err(format!("edge dst={} not registered", e.dst()));
            };
            if dst.node.task_type() == TaskType::Event {
                return Err(format!(
                    "failure edge must not target event node {}",
                    e.dst()
                ));
            }
            // 边把失败节点输入反序列化为 SrcIn：要求与源节点 In 一致（或边载荷为 Value）
            if !readable(e.src_in_type(), src.node.in_type()) {
                return Err(format!(
                    "edge {} -> {}: transform 入参类型 {} 与节点 {} 输入类型 {} 不匹配",
                    e.src(),
                    e.dst(),
                    e.src_in_type().name(),
                    e.src(),
                    src.node.in_type().name()
                ));
            }
            // 目标节点把边产出反序列化为自身 In：与成功边同一规则
            if !writable(e.dst_in_type(), dst.node.in_type()) {
                return Err(format!(
                    "edge {} -> {}: transform 出参类型 {} 与节点 {} 输入类型 {} 不匹配",
                    e.src(),
                    e.dst(),
                    e.dst_in_type().name(),
                    e.dst(),
                    dst.node.in_type().name()
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

/// 边「读取」节点值方向的指纹兼容判定：边把节点序列化值反序列化为 `edge_ty`。
///
/// 类型一致即兼容；`edge_ty` 为 `Value` 时恒兼容（`from_value::<Value>` 必成功）。
/// 反之节点侧为 `Value` 不构成通配——反序列化为具体类型取决于内容，运行时可能失败，
/// 因此仍要求一致（DSL 的 JOIN 出边即按此纪律写成 `&Value` 入参）。
fn readable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || edge_ty.id() == TypeFingerprint::of::<Value>().id()
}

/// 边「写入」节点值方向的指纹兼容判定：节点把边产出反序列化为 `node_ty`。
///
/// 类型一致即兼容；`node_ty` 为 `Value` 时恒兼容（`from_value::<Value>` 必成功）——
/// 这是 `i32` 载荷汇入 `JoinNode`（`In = Value`）这类 JSON 边界模式的依据。反之边侧为
/// `Value` 不构成通配：节点反序列化为具体类型取决于内容，运行时可能失败。
fn writable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || node_ty.id() == TypeFingerprint::of::<Value>().id()
}
