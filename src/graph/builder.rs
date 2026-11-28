//! 图构建器：注册节点、子图与统一边，组装并静态校验。

use std::{collections::HashMap, sync::Arc};

use super::super::edge::{Edge, EdgeKind};
use super::super::node::{Node, NodeKey};
use super::{Graph, GraphNode, SubgraphDescription};
use crate::error::GraphError;

/// 图构建器：注册节点、子图与统一边，组装并静态校验。
pub struct GraphBuilder<Ctx> {
    workflow_id: String,
    title: String,
    context: Ctx,
    nodes: Vec<(Arc<str>, GraphNode<Ctx>)>,
    subgraphs: Vec<SubgraphDescription>,
    success_edges: Vec<Edge>,
    failure_edges: Vec<Edge>,
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

    /// 注册节点实例（支持静态 `&str` 或动态 `String`/`Arc<str>`）。
    pub fn node<N: Node<Ctx> + 'static>(self, name: impl Into<Arc<str>>, node: N) -> Self {
        let name: Arc<str> = name.into();
        let title = name.to_string();
        self.node_with(name, node, title, "")
    }

    /// 注册节点并附加展示标题与说明。
    pub fn node_with<N: Node<Ctx> + 'static>(
        self,
        name: impl Into<Arc<str>>,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.node_in_subgraph_with_key(name, node, title, description, None)
            .0
    }

    /// 注册根节点并返回类型化 [`NodeKey`]，便于命令式强类型建边。
    pub fn node_key<N: Node<Ctx> + 'static>(
        self,
        name: impl Into<Arc<str>>,
        node: N,
    ) -> (Self, NodeKey<Ctx, N>) {
        let name: Arc<str> = name.into();
        let title = name.to_string();
        self.node_key_with(name, node, title, "")
    }

    /// 注册根节点并附加展示标题与说明，返回类型化 [`NodeKey`]。
    pub fn node_key_with<N: Node<Ctx> + 'static>(
        self,
        name: impl Into<Arc<str>>,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> (Self, NodeKey<Ctx, N>) {
        self.node_in_subgraph_with_key(name, node, title, description, None)
    }

    /// 声明子图（对应运行期隔离作用域）。ID 为完整点路径。
    pub fn subgraph(
        self,
        id: impl Into<String>,
        title: impl Into<String>,
        parent_id: Option<&str>,
    ) -> Self {
        self.subgraph_with(id, title, "", parent_id)
    }

    /// 声明子图并附加说明。
    pub fn subgraph_with(
        mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
        parent_id: Option<&str>,
    ) -> Self {
        self.subgraphs.push(SubgraphDescription {
            id: id.into(),
            title: title.into(),
            description: description.into(),
            parent_id: parent_id.map(str::to_owned),
        });
        self
    }

    /// 注册子图成员并返回类型化标识；归属与边端点独立。
    pub fn node_in_subgraph_with_key<N: Node<Ctx> + 'static>(
        mut self,
        name: impl Into<Arc<str>>,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
        subgraph_id: Option<&str>,
    ) -> (Self, NodeKey<Ctx, N>) {
        let name: Arc<str> = name.into();
        self.nodes.push((
            name.clone(),
            GraphNode {
                node: Box::new(node),
                title: title.into(),
                description: description.into(),
                subgraph_id: subgraph_id.map(str::to_owned),
                scope_path: Arc::from([]),
            },
        ));
        (self, NodeKey::new(name))
    }

    /// 追加一条成功边或失败边（统一入口，按 `EdgeKind` 归类）。
    pub fn edge(mut self, edge: Edge) -> Self {
        match edge.kind() {
            EdgeKind::Success => self.success_edges.push(edge),
            EdgeKind::Failure => self.failure_edges.push(edge),
        }
        self
    }

    /// 组装为 [`Graph`] 并静态校验（拓扑、作用域边界、JOIN 槽位与类型指纹）。
    pub fn build(self) -> Result<Graph<Ctx>, GraphError> {
        let mut nodes = HashMap::with_capacity(self.nodes.len());
        for (name, node) in self.nodes {
            if nodes.insert(name.clone(), node).is_some() {
                return Err(GraphError::DuplicateNode(name.to_string()));
            }
        }
        let mut graph = Graph {
            workflow_id: self.workflow_id,
            title: self.title,
            context: self.context,
            nodes,
            subgraphs: self.subgraphs,
            success_edges: self.success_edges,
            failure_edges: self.failure_edges,
            success_adj: HashMap::new(),
            failure_adj: HashMap::new(),
            settlement_rules: Default::default(),
        };
        graph.validate()?;
        graph.finalize();
        Ok(graph)
    }
}
