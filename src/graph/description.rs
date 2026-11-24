//! 执行图的只读描述，不包含上下文、运行状态或节点载荷。

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{Graph, Trigger};

/// 已构建执行图的可序列化拓扑。节点按 ID 排序，边按成功/失败及各自注册顺序输出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDescription {
    pub workflow_id: String,
    pub title: String,
    pub nodes: Vec<NodeDescription>,
    pub edges: Vec<EdgeDescription>,
    pub subgraphs: Vec<SubgraphDescription>,
}

/// 节点信息。输入输出字段为 Rust 类型名，不是 JSON Schema。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescription {
    /// 图中注册的节点名，与任务的 name 对应。
    pub id: String,
    pub title: String,
    pub description: String,
    pub task_type: String,
    pub subgraph_id: Option<String>,
    /// Event 节点的外部触发声明；其他节点为 None。
    pub trigger: Option<Trigger>,
    pub input_type: String,
    pub output_type: String,
}

/// 静态子图定义；ID 为完整点路径，父级归属不依赖当前运行任务。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubgraphDescription {
    pub id: String,
    pub title: String,
    pub parent_id: Option<String>,
}

/// 边的触发类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    Success,
    Failure,
}

/// 一条实际注册的边。相同端点的多条边仍保留为独立记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeDescription {
    /// 当前图内唯一的编号，如 success:0；注册顺序改变后不保证跨版本稳定。
    pub id: String,
    pub source: String,
    pub target: String,
    pub kind: EdgeKind,
    /// 是否通过 map 转换输入。foreach 和透传边为 false。
    pub map: bool,
    /// 成功边是否逐项建立执行作用域；普通边与失败边为 false。
    pub foreach: bool,
}

impl<Ctx> Graph<Ctx> {
    /// 描述实际执行图；不执行节点、条件或转换闭包，不读取或序列化上下文。
    pub fn describe(&self) -> GraphDescription {
        let mut nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(&id, entry)| NodeDescription {
                id: id.to_owned(),
                title: entry.title.clone(),
                description: entry.description.clone(),
                task_type: entry.node.task_type().as_str().to_owned(),
                trigger: entry.node.trigger(),
                subgraph_id: entry.subgraph_id.clone(),
                input_type: entry.node.in_type().name().to_owned(),
                output_type: entry.node.out_type().name().to_owned(),
            })
            .collect();
        nodes.sort_by(|left, right| left.id.cmp(&right.id));

        let edges = self
            .success_edges
            .iter()
            .enumerate()
            .map(|(index, edge)| EdgeDescription {
                id: format!("success:{index}"),
                source: edge.src().to_owned(),
                target: edge.dst().to_owned(),
                kind: EdgeKind::Success,
                map: edge.is_map(),
                foreach: edge.is_foreach(),
            })
            .chain(
                self.failure_edges
                    .iter()
                    .enumerate()
                    .map(|(index, edge)| EdgeDescription {
                        id: format!("failure:{index}"),
                        source: edge.src().to_owned(),
                        target: edge.dst().to_owned(),
                        kind: EdgeKind::Failure,
                        map: edge.is_map(),
                        foreach: false,
                    }),
            )
            .collect();

        let mut subgraphs = self.subgraphs.clone();
        subgraphs.sort_by(|a, b| a.id.cmp(&b.id));
        GraphDescription {
            workflow_id: self.workflow_id.clone(),
            title: self.title.clone(),
            nodes,
            edges,
            subgraphs,
        }
    }
}

impl GraphDescription {
    /// 从持久化的图描述解析独立运行入口及其子图路径。
    pub(crate) fn entry_path(&self, entry: &str) -> Result<Vec<String>, crate::EngineError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.id == entry)
            .ok_or_else(|| crate::EngineError::UnknownNode(entry.to_owned()))?;
        self.validate_subgraphs()
            .map_err(|detail| crate::error::StorageError::InvalidFanout { detail })?;
        Ok(super::subgraph_path(
            &self.subgraphs,
            node.subgraph_id.as_deref(),
        ))
    }

    /// 校验静态归属以及 foreach 的边界。允许普通边直接连接子图内部节点。
    pub fn validate_subgraphs(&self) -> Result<(), String> {
        let mut groups = HashMap::new();
        let node_ids: HashSet<_> = self.nodes.iter().map(|node| node.id.as_str()).collect();
        for group in &self.subgraphs {
            if group.id.is_empty() || group.title.trim().is_empty() {
                return Err("subgraph id and title must not be empty".into());
            }
            if groups.insert(group.id.as_str(), group).is_some()
                || node_ids.contains(group.id.as_str())
            {
                return Err(format!("duplicate subgraph id: {}", group.id));
            }
            if group.id.rsplit_once('.').map(|(parent, _)| parent) != group.parent_id.as_deref() {
                return Err(format!(
                    "subgraph {} must use its full parent-qualified name",
                    group.id
                ));
            }
        }
        let mut paths = HashMap::new();
        for group in &self.subgraphs {
            let mut path = Vec::new();
            let mut seen = HashSet::new();
            let mut current = Some(group.id.as_str());
            while let Some(id) = current {
                if !seen.insert(id) {
                    return Err(format!("subgraph hierarchy contains a cycle at {id}"));
                }
                let group = groups
                    .get(id)
                    .ok_or_else(|| format!("subgraph {id} not registered"))?;
                path.push(id.to_owned());
                current = group.parent_id.as_deref();
            }
            path.reverse();
            paths.insert(group.id.as_str(), path);
        }
        let mut node_paths = HashMap::new();
        for node in &self.nodes {
            let path = match node.subgraph_id.as_deref() {
                Some(id) => paths
                    .get(id)
                    .ok_or_else(|| format!("node {}: subgraph {id} not registered", node.id))?
                    .clone(),
                None => Vec::new(),
            };
            if let Some(id) = node.subgraph_id.as_deref() {
                if node.id.rsplit_once('.').map(|(parent, _)| parent) != Some(id) {
                    return Err(format!(
                        "node {} must use its full subgraph-qualified name",
                        node.id
                    ));
                }
            }
            node_paths.insert(node.id.as_str(), path);
        }
        for edge in &self.edges {
            let source = node_paths
                .get(edge.source.as_str())
                .ok_or_else(|| format!("edge src={} not registered", edge.source))?;
            let destination = node_paths
                .get(edge.target.as_str())
                .ok_or_else(|| format!("edge dst={} not registered", edge.target))?;
            if edge.foreach
                && self
                    .nodes
                    .iter()
                    .any(|node| node.id == edge.target && node.task_type == "join")
            {
                return Err(format!(
                    "foreach edge must not target join node {}",
                    edge.target
                ));
            }
            if edge.foreach && source.starts_with(destination) {
                return Err(format!(
                    "foreach edge {} -> {} must enter a new subgraph",
                    edge.source, edge.target
                ));
            }
        }
        Ok(())
    }
}
