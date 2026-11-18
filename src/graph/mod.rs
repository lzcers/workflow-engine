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

pub use crate::edge::{
    EdgeError, EdgeImpl, ErrorWhenBuilder, FailureEdge, FailureEdgeImpl, SuccessEdge, WhenBuilder,
    map, on_error, when, when_error,
};
pub use crate::node::{JoinNode, Node, NodeContext, NodeError, NodeKey, TaskType, Trigger};
pub use builder::GraphBuilder;
pub use description::{EdgeDescription, EdgeKind, GraphDescription, NodeDescription};

/// 节点实例与注册时的展示信息共用一条记录，保证描述对应实际执行的节点。
struct GraphNode<Ctx> {
    node: Box<dyn AnyNode<Ctx>>,
    title: String,
    description: String,
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
    success_edges: Vec<Box<dyn AnySuccessEdge>>,
    failure_edges: Vec<Box<dyn AnyFailureEdge>>,
}

/// 一条成功后继规划：目标节点名 + 输入列表（按 dst 聚合）。
#[derive(Debug)]
pub struct Successor {
    /// 目标节点名。
    pub dst: &'static str,
    /// 目标节点产出的任务类型（普通节点 / JOIN 门），入队时落库到 `task_type` 列。
    pub task_type: TaskType,
    /// 喂给 `dst` 的输入列表。普通节点每条输入创建独立任务，JOIN 在同一运行内累积输入。
    pub inputs: Vec<Value>,
    /// 目标节点的静态前驱节点名全集（通用血缘，`String` 以便直接落库）。是否用其做
    /// 「等齐前驱」闸门由 `task_type` 决定（仅 [`TaskType::Join`] 生效）。
    pub depends_on_names: Vec<String>,
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
    pub(crate) fn task_type_of(&self, name: &str) -> Option<TaskType> {
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

    /// 规划 `name` 节点的成功后继：遍历其出成功边，`when` 命中后经 `map` 产出 dst 输入，
    /// 并按 dst 聚合为批量后继，是否汇合由目标节点类型决定。
    ///
    /// 返回 [`Successor`] 列表：`when` 不命中或 `map` 返回空则无该项；无 `map` 时透传 `out`。
    /// 纯函数，不触碰 DB——只回答「该入队哪些后继、各喂什么输入（单条 or 多条）」。
    ///
    /// 规划失败（[`EdgeError`]，多为图装配/类型接线错误）为确定性错误，应终止结算。
    pub fn plan_success(&self, name: &str, out: &Value) -> Result<Vec<Successor>, EdgeError> {
        // 按目标节点名字分组收集经过转换后的结果
        let mut order: Vec<&'static str> = Vec::new();
        let mut by_dst: HashMap<&'static str, Vec<Value>> = HashMap::new();

        // 遍历所有成功路径的边，拿到这些边的目标。
        for e in self.success_edges.iter().filter(|e| e.src() == name) {
            let dst = e.dst();
            // 目标节点不在收集的分组里？说明这是新的目标节点
            if !by_dst.contains_key(dst) {
                order.push(dst);
            }
            // 对于 image_propmt |map|-> image_generator 经过 map 转换后会是一个 Vec<Input>
            // 这意味着扇出多实例了
            // {image_generator: Vec[Input]}
            by_dst.entry(dst).or_default().extend(e.plan_success(out)?);
        }
        Ok(order
            .into_iter()
            .filter_map(|dst| {
                let inputs = by_dst.remove(dst)?;
                if inputs.is_empty() {
                    return None;
                }
                Some(Successor {
                    dst,
                    inputs,
                    task_type: self.task_type_of(dst).expect("validated edge destination"),
                    depends_on_names: self.predecessors_of(dst),
                })
            })
            .collect())
    }

    /// 规划 `name` 节点的失败后继：遍历其失败边，`when` 命中后经 `map` 产出 dst 输入，
    /// 并按 dst 聚合为批量后继——与 [`Graph::plan_success`] 完全对称。失败边不约束为回退
    /// 操作，可做错误分支、兜底降级等一般路由；无 `map` 时透传失败节点输入。
    ///
    /// 返回 [`Successor`] 列表：`when` 不命中或 `map` 返回空则无该项。
    /// `task_type` 如实反映 dst 节点的类型（同 [`Graph::plan_success`]）；
    /// `depends_on_names` **保持为空**——这是当前存储层的能力约束：认领闸门只支持
    /// 「等齐全部前驱**成功**」的 JOIN 语义，不支持「等齐失败/终态」。若把失败源节点名填入
    /// `depends_on_names`，失败后继会因源节点状态为 `failed`/`superseded` 而永远无法被认领
    /// （悬挂）。等存储层新增「等齐失败」闸门后，可改为填充失败源名全集，以支持按源汇聚的
    /// 通用错误处理节点。
    ///
    /// 规划失败（[`EdgeError`]，图装配/类型接线错误）为确定性错误，调用方应告警并按无后继处理。
    pub fn plan_failure(
        &self,
        name: &str,
        input: &Value,
        err: &NodeError,
    ) -> Result<Vec<Successor>, EdgeError> {
        // 按目标节点名字分组收集（同 plan_success）。
        let mut order: Vec<&'static str> = Vec::new();
        let mut by_dst: HashMap<&'static str, Vec<Value>> = HashMap::new();
        for e in self.failure_edges.iter().filter(|e| e.src() == name) {
            let dst = e.dst();
            if !by_dst.contains_key(dst) {
                order.push(dst);
            }
            by_dst
                .entry(dst)
                .or_default()
                .extend(e.plan_failure(input, err)?);
        }
        Ok(order
            .into_iter()
            .filter_map(|dst| {
                let inputs = by_dst.remove(dst)?;
                if inputs.is_empty() {
                    return None;
                }
                Some(Successor {
                    dst,
                    inputs,
                    // task_type 如实反映 dst 节点类型：失败后继可能汇聚到 JOIN/错误处理节点，
                    task_type: self.task_type_of(dst).expect("validated edge destination"),
                    // 保持为空（当前约束）：认领闸门只支持「等齐前驱成功」，不支持「等齐失败」；
                    // 填入失败源名会让后继因前驱已失败而永久悬挂。详见方法级注释。
                    depends_on_names: Vec::new(),
                })
            })
            .collect())
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
        set.into_iter().map(|s| s.to_string()).collect()
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
