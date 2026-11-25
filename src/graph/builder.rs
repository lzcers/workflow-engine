//! 图构建器：注册节点、追加边、组装并静态校验。

use super::super::edge::{AnyFailureEdge, AnySuccessEdge, FailureEdge, SuccessEdge};
use super::super::node::{Node, NodeKey};
use super::{Graph, GraphNode, SubgraphDescription};
use std::collections::HashSet;

/// 图构建器：注册节点、追加边、组装并校验。
pub struct GraphBuilder<Ctx> {
    workflow_id: String,
    title: String,
    context: Ctx,
    nodes: Vec<(&'static str, GraphNode<Ctx>)>,
    subgraphs: Vec<SubgraphDescription>,
    success_edges: Vec<Box<dyn AnySuccessEdge>>,
    failure_edges: Vec<Box<dyn AnyFailureEdge>>,
}

impl<Ctx: Send> GraphBuilder<Ctx> {
    pub fn new(workflow_id: impl Into<String>, title: impl Into<String>, context: Ctx) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            title: title.into(),
            context,
            nodes: Vec::new(),
            subgraphs: Vec::new(),
            success_edges: Vec::new(),
            failure_edges: Vec::new(),
        }
    }

    /// 注册构造完成的节点实例。任务状态放在执行局部变量或 fork 后的上下文中。
    pub fn node<N: Node<Ctx> + 'static>(self, name: &'static str, node: N) -> Self {
        self.node_with_description(name, node, name, "")
    }

    /// 注册节点并附加展示标题与说明。节点身份仍由 `name` 决定，与任务的 name 对应。
    pub fn node_with_description<N: Node<Ctx> + 'static>(
        mut self,
        name: &'static str,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.nodes.push((
            name,
            GraphNode {
                node: Box::new(node),
                title: title.into(),
                description: description.into(),
                subgraph_id: None,
            },
        ));
        self
    }

    /// 注册实例并返回类型化标识，供 DSL 从实例推导边两端的类型。
    pub fn node_with_key<N: Node<Ctx> + 'static>(
        self,
        name: &'static str,
        node: N,
    ) -> (Self, NodeKey<Ctx, N>) {
        self.node_with_description_and_key(name, node, name, "")
    }

    /// 注册带展示信息的实例，并保留类型化标识用于编译期接线检查。
    pub fn node_with_description_and_key<N: Node<Ctx> + 'static>(
        self,
        name: &'static str,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> (Self, NodeKey<Ctx, N>) {
        (
            self.node_with_description(name, node, title, description),
            NodeKey::new(name),
        )
    }

    /// 声明静态子图，不创建可执行节点。ID 是完整点路径。
    pub fn subgraph(
        mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        parent_id: Option<&str>,
    ) -> Self {
        self.subgraphs.push(SubgraphDescription {
            id: id.into(),
            title: title.into(),
            parent_id: parent_id.map(str::to_owned),
        });
        self
    }

    /// 注册子图成员并返回类型化标识；归属与边端点独立。
    pub fn node_in_subgraph_with_key<N: Node<Ctx> + 'static>(
        mut self,
        name: &'static str,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
        subgraph_id: Option<&str>,
    ) -> (Self, NodeKey<Ctx, N>) {
        self.nodes.push((
            name,
            GraphNode {
                node: Box::new(node),
                title: title.into(),
                description: description.into(),
                subgraph_id: subgraph_id.map(str::to_owned),
            },
        ));
        (self, NodeKey::new(name))
    }

    /// 追加一条成功边（条件/转换，前向 DAG）。
    pub fn edge<E: SuccessEdge + 'static>(mut self, e: E) -> Self {
        self.success_edges.push(Box::new(e));
        self
    }

    /// 追加一条失败边（条件/转换，后向回退图）。
    pub fn error_edge<E: FailureEdge + 'static>(mut self, e: E) -> Self {
        self.failure_edges.push(Box::new(e));
        self
    }

    /// 组装为 [`Graph`] 并静态校验（拓扑 + 边类型接线指纹；命令式边无编译期检查，
    /// 类型错线在此 build 期提前拒绝）。
    pub fn build(self) -> Result<Graph<Ctx>, String> {
        let mut names = HashSet::new();
        for (name, _) in &self.nodes {
            if !names.insert(*name) {
                return Err(format!("duplicate node name: {name}"));
            }
        }
        let mut graph = Graph {
            workflow_id: self.workflow_id,
            title: self.title,
            context: self.context,
            nodes: self.nodes.into_iter().collect(),
            subgraphs: self.subgraphs,
            success_edges: self.success_edges,
            failure_edges: self.failure_edges,
            settlement_rules: Default::default(),
        };
        graph.validate()?;
        graph.settlement_rules = graph.compute_settlement_rules();
        Ok(graph)
    }
}
