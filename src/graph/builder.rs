//! 图构建器与可复用子图挂载：支持静态与动态节点名、UI 分组与隔离作用域解耦。

use std::{collections::HashMap, sync::Arc};

use super::super::edge::{Edge, EdgeKind};
use super::super::node::{Node, NodeKey};
use super::{Graph, GraphNode, SubgraphDescription};
use crate::error::GraphError;

/// 图构建器：注册节点、子图/分组与统一边，组装并静态校验。
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
        self.node_in_subgraph_with_key(name, node, title, "", None)
    }

    /// 声明运行期隔离作用域子图（`isolated = true`）。ID 为完整点路径。
    pub fn subgraph(
        self,
        id: impl Into<String>,
        title: impl Into<String>,
        parent_id: Option<&str>,
    ) -> Self {
        self.subgraph_with(id, title, "", parent_id, true)
    }

    /// 声明纯视觉画布分组（`isolated = false`，不创建数据库 `scopes` 行）。
    pub fn group(
        self,
        id: impl Into<String>,
        title: impl Into<String>,
        parent_id: Option<&str>,
    ) -> Self {
        self.subgraph_with(id, title, "", parent_id, false)
    }

    /// 声明子图或视觉分组，完整指定标题、说明、父级与是否隔离作用域。
    pub fn subgraph_with(
        mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
        parent_id: Option<&str>,
        isolated: bool,
    ) -> Self {
        self.subgraphs.push(SubgraphDescription {
            id: id.into(),
            title: title.into(),
            description: description.into(),
            parent_id: parent_id.map(str::to_owned),
            isolated,
        });
        self
    }

    /// 挂载可复用的隔离作用域子图模板，同一构建函数可在图内多次挂载到不同前缀下。
    pub fn mount(
        self,
        id: impl Into<String>,
        title: impl Into<String>,
        build: impl FnOnce(SubgraphBuilder<Ctx>) -> SubgraphBuilder<Ctx>,
    ) -> Self {
        self.mount_inner(id.into(), title.into(), String::new(), None, true, build)
    }

    /// 挂载可复用的纯视觉分组模板（不创建运行时 `scopes` 行）。
    pub fn mount_group(
        self,
        id: impl Into<String>,
        title: impl Into<String>,
        build: impl FnOnce(SubgraphBuilder<Ctx>) -> SubgraphBuilder<Ctx>,
    ) -> Self {
        self.mount_inner(id.into(), title.into(), String::new(), None, false, build)
    }

    fn mount_inner(
        self,
        id: String,
        title: String,
        description: String,
        parent_id: Option<&str>,
        isolated: bool,
        build: impl FnOnce(SubgraphBuilder<Ctx>) -> SubgraphBuilder<Ctx>,
    ) -> Self {
        let builder = self.subgraph_with(id.clone(), title, description, parent_id, isolated);
        build(SubgraphBuilder {
            prefix: id,
            builder,
        })
        .builder
    }

    /// 注册子图/分组成员并返回类型化标识；归属与边端点独立。
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

/// 可复用子图/分组的嵌套构建器：自动用当前前缀限定局部节点名，支持嵌套挂载。
pub struct SubgraphBuilder<Ctx> {
    prefix: String,
    builder: GraphBuilder<Ctx>,
}

impl<Ctx: Send> SubgraphBuilder<Ctx> {
    /// 当前子图的完整限定前缀。
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// 将局部节点名限定为完整点路径（如 `"fetch"` -> `"stage1.fetch"`）。
    pub fn qualify(&self, local_name: &str) -> Arc<str> {
        Arc::from(format!("{}.{local_name}", self.prefix))
    }

    pub fn node<N: Node<Ctx> + 'static>(self, local_name: &str, node: N) -> Self {
        self.node_with(local_name, node, local_name, "")
    }

    pub fn node_with<N: Node<Ctx> + 'static>(
        self,
        local_name: &str,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.node_key_with(local_name, node, title, description).0
    }

    pub fn node_key<N: Node<Ctx> + 'static>(
        self,
        local_name: &str,
        node: N,
    ) -> (Self, NodeKey<Ctx, N>) {
        self.node_key_with(local_name, node, local_name, "")
    }

    pub fn node_key_with<N: Node<Ctx> + 'static>(
        self,
        local_name: &str,
        node: N,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> (Self, NodeKey<Ctx, N>) {
        let full = self.qualify(local_name);
        let (builder, key) = self.builder.node_in_subgraph_with_key(
            full,
            node,
            title,
            description,
            Some(&self.prefix),
        );
        (
            Self {
                prefix: self.prefix,
                builder,
            },
            key,
        )
    }

    /// 在当前子图内挂载嵌套隔离作用域子图。
    pub fn mount(
        self,
        local_id: &str,
        title: impl Into<String>,
        build: impl FnOnce(SubgraphBuilder<Ctx>) -> SubgraphBuilder<Ctx>,
    ) -> Self {
        let full = format!("{}.{local_id}", self.prefix);
        let builder = self.builder.mount_inner(
            full,
            title.into(),
            String::new(),
            Some(&self.prefix),
            true,
            build,
        );
        Self {
            prefix: self.prefix,
            builder,
        }
    }

    /// 在当前子图内挂载嵌套纯视觉分组。
    pub fn mount_group(
        self,
        local_id: &str,
        title: impl Into<String>,
        build: impl FnOnce(SubgraphBuilder<Ctx>) -> SubgraphBuilder<Ctx>,
    ) -> Self {
        let full = format!("{}.{local_id}", self.prefix);
        let builder = self.builder.mount_inner(
            full,
            title.into(),
            String::new(),
            Some(&self.prefix),
            false,
            build,
        );
        Self {
            prefix: self.prefix,
            builder,
        }
    }

    /// 追加边（配合 `node_key` 或 `qualify` 使用）。
    pub fn edge(self, edge: Edge) -> Self {
        Self {
            prefix: self.prefix,
            builder: self.builder.edge(edge),
        }
    }
}
