//! 图构建器：注册节点、追加边、组装并静态校验。

use super::super::edge::{AnyFailureEdge, AnySuccessEdge, FailureEdge, SuccessEdge};
use super::super::node::{AnyNode, Node, NodeKey};
use super::Graph;

/// 图构建器：注册节点、追加边、组装并校验。
pub struct GraphBuilder<Ctx> {
    context: Ctx,
    nodes: Vec<(&'static str, Box<dyn AnyNode<Ctx>>)>,
    success_edges: Vec<Box<dyn AnySuccessEdge>>,
    failure_edges: Vec<Box<dyn AnyFailureEdge>>,
}

impl<Ctx: Send> GraphBuilder<Ctx> {
    pub fn new(context: Ctx) -> Self {
        Self {
            context,
            nodes: Vec::new(),
            success_edges: Vec::new(),
            failure_edges: Vec::new(),
        }
    }

    /// 注册构造完成的节点实例。任务状态放在执行局部变量或 fork 后的上下文中。
    pub fn node<N: Node<Ctx> + 'static>(mut self, name: &'static str, node: N) -> Self {
        self.nodes.push((name, Box::new(node)));
        self
    }

    /// 注册实例并返回类型化标识，供 DSL 从实例推导边两端的类型。
    pub fn node_with_key<N: Node<Ctx> + 'static>(
        self,
        name: &'static str,
        node: N,
    ) -> (Self, NodeKey<Ctx, N>) {
        (self.node(name, node), NodeKey::new(name))
    }

    /// 追加一条成功边（条件/转换，前向 DAG，擦除为 [`AnySuccessEdge`] 存储）。
    pub fn edge<E: SuccessEdge + 'static>(mut self, e: E) -> Self {
        self.success_edges.push(Box::new(e));
        self
    }

    /// 追加一条失败边（条件/转换，后向回退图，擦除为 [`AnyFailureEdge`] 存储）。
    pub fn error_edge<E: FailureEdge + 'static>(mut self, e: E) -> Self {
        self.failure_edges.push(Box::new(e));
        self
    }

    /// 组装为 [`Graph`] 并静态校验（拓扑 + 边类型接线指纹；命令式边无编译期检查，
    /// 类型错线在此 build 期提前拒绝）。
    pub fn build(self) -> Result<Graph<Ctx>, String> {
        let graph = Graph {
            context: self.context,
            nodes: self.nodes.into_iter().collect(),
            success_edges: self.success_edges,
            failure_edges: self.failure_edges,
        };
        graph.validate()?;
        Ok(graph)
    }
}
