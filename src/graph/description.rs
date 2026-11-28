//! 执行图的只读描述，不包含上下文、运行状态或节点载荷。

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::{EdgeKind, EdgeMode, Graph, RetryPolicy, TaskType, Trigger, qualified_parent, subgraph_path};
use crate::error::GraphError;

fn default_isolated() -> bool {
    true
}

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
    pub task_type: TaskType,
    pub subgraph_id: Option<String>,
    /// 外部触发声明（与入边正交）；无触发器时为 None。
    pub trigger: Option<Trigger>,
    pub input_type: String,
    pub output_type: String,
    /// 节点级重试策略覆盖；None 时沿用工作流默认策略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
}

/// 静态子图或纯视觉分组定义；ID 为完整点路径。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubgraphDescription {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub parent_id: Option<String>,
    /// true 表示运行时隔离作用域（写 `scopes` 表）；false 表示纯 UI 分组。
    #[serde(default = "default_isolated")]
    pub isolated: bool,
}

/// 一条实际注册的边。相同端点的多条边仍保留为独立记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeDescription {
    /// 当前图内唯一的编号，如 success:0；按源码/注册顺序分配。
    pub id: String,
    pub source: String,
    pub target: String,
    /// 目标为 JOIN 节点时的字段槽位名；普通目标为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    pub kind: EdgeKind,
    /// 箭头的分发方式，对应 `->`、`->!`、`->*` 或 `->+`。
    pub mode: EdgeMode,
    /// 是否在分发前通过 `|>` 转换载荷。
    pub has_transform: bool,
    /// 是否附带条件谓词 `when` / `when_error`。
    #[serde(default)]
    pub has_condition: bool,
}

impl<Ctx> Graph<Ctx> {
    /// 描述实际执行图；不执行节点、条件或转换闭包，不读取或序列化上下文。
    pub fn describe(&self) -> GraphDescription {
        let mut nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(id, entry)| NodeDescription {
                id: id.to_string(),
                title: entry.title.clone(),
                description: entry.description.clone(),
                task_type: entry.node.role().task_type(),
                trigger: entry.node.trigger(),
                subgraph_id: entry.subgraph_id.clone(),
                input_type: entry.node.in_type().name().to_owned(),
                output_type: entry.node.out_type().name().to_owned(),
                retry_policy: entry.node.retry_policy(),
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
                slot: edge.slot_key().map(str::to_owned),
                kind: EdgeKind::Success,
                mode: edge.mode(),
                has_transform: edge.has_transform(),
                has_condition: edge.has_condition(),
            })
            .chain(
                self.failure_edges
                    .iter()
                    .enumerate()
                    .map(|(index, edge)| EdgeDescription {
                        id: format!("failure:{index}"),
                        source: edge.src().to_owned(),
                        target: edge.dst().to_owned(),
                        slot: edge.slot_key().map(str::to_owned),
                        kind: EdgeKind::Failure,
                        mode: edge.mode(),
                        has_transform: edge.has_transform(),
                        has_condition: edge.has_condition(),
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

impl SubgraphDescription {
    fn validate(&self) -> Result<(), GraphError> {
        if self.id.is_empty() || self.title.trim().is_empty() {
            return Err(GraphError::EmptySubgraphMetadata);
        }
        if qualified_parent(&self.id) != self.parent_id.as_deref() {
            return Err(GraphError::InvalidQualification {
                kind: "subgraph",
                id: self.id.clone(),
            });
        }
        Ok(())
    }
}

fn validate_node_owner(id: &str, subgraph: Option<&str>) -> Result<(), GraphError> {
    if subgraph.is_some() && qualified_parent(id) != subgraph {
        return Err(GraphError::InvalidQualification {
            kind: "node",
            id: id.to_owned(),
        });
    }
    Ok(())
}

impl GraphDescription {
    /// 从图描述构造可直接提交的入口任务，并解析其运行时隔离作用域路径与重试预算。
    pub fn task(
        &self,
        entry: &str,
        input: serde_json::Value,
    ) -> Result<crate::Task, crate::EngineError> {
        let mut entries = self.nodes.iter().filter(|node| node.id == entry);
        let node = entries
            .next()
            .ok_or_else(|| crate::EngineError::UnknownNode(entry.to_owned()))?;
        if entries.next().is_some() {
            return Err(crate::EngineError::InvalidGraph(GraphError::DuplicateNode(
                entry.to_owned(),
            )));
        }
        let mut task = crate::Task::new(&self.workflow_id, entry, input);
        task.subgraph_path = self
            .entry_path(node)
            .map_err(crate::EngineError::InvalidGraph)?;
        task.max_attempts = node.retry_policy.map(RetryPolicy::effective_max_attempts);
        Ok(task)
    }

    fn entry_path(&self, node: &NodeDescription) -> Result<Vec<String>, GraphError> {
        let mut path = Vec::new();
        let owner = node.subgraph_id.as_deref();
        validate_node_owner(&node.id, owner)?;
        for id in subgraph_path(owner) {
            let mut groups = self.subgraphs.iter().filter(|group| group.id == id);
            let group = groups
                .next()
                .ok_or_else(|| GraphError::UnknownSubgraph(id.to_owned()))?;
            if groups.next().is_some() {
                return Err(GraphError::DuplicateSubgraph(id.to_owned()));
            }
            group.validate()?;
            if group.isolated {
                path.push(id.to_owned());
            }
        }
        Ok(path)
    }

    /// 校验静态归属以及隔离边（`->!` / `->+`）的子图边界。
    pub fn validate_subgraphs(&self) -> Result<(), GraphError> {
        let nodes = self.nodes.iter().map(|node| {
            (
                node.id.as_str(),
                node.subgraph_id.as_deref(),
                node.task_type == TaskType::Join,
            )
        });
        let edges = self
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str(), edge.mode));
        validate_subgraphs(nodes, self.subgraphs.iter(), edges)
    }
}

impl<Ctx> Graph<Ctx> {
    pub(super) fn validate_subgraphs(&self) -> Result<(), GraphError> {
        let mut nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(id, entry)| {
                (
                    id.as_ref(),
                    entry.subgraph_id.as_deref(),
                    entry.node.role().task_type() == TaskType::Join,
                )
            })
            .collect();
        nodes.sort_unstable_by_key(|&(id, _, _)| id);
        let mut groups: Vec<_> = self.subgraphs.iter().collect();
        groups.sort_by_key(|group| &group.id);

        let success = self
            .success_edges
            .iter()
            .map(|e| (e.src(), e.dst(), e.mode()));
        let failure = self
            .failure_edges
            .iter()
            .map(|e| (e.src(), e.dst(), e.mode()));
        validate_subgraphs(
            nodes.iter().copied(),
            groups.iter().copied(),
            success.chain(failure),
        )
    }
}

fn isolated_path<'a>(owner: Option<&'a str>, isolated_set: &HashSet<&str>) -> Vec<&'a str> {
    subgraph_path(owner)
        .filter(|id| isolated_set.contains(id))
        .collect()
}

fn validate_subgraphs<'a>(
    nodes: impl Iterator<Item = (&'a str, Option<&'a str>, bool)> + Clone,
    subgraphs: impl Iterator<Item = &'a SubgraphDescription> + Clone,
    edges: impl Iterator<Item = (&'a str, &'a str, EdgeMode)>,
) -> Result<(), GraphError> {
    let mut owners = HashMap::new();
    for (id, owner, is_join) in nodes.clone() {
        validate_node_owner(id, owner)?;
        if owners.insert(id, (owner, is_join)).is_some() {
            return Err(GraphError::DuplicateNode(id.to_owned()));
        }
    }

    let mut groups = HashSet::new();
    let mut isolated_groups = HashSet::new();
    for group in subgraphs.clone() {
        if !groups.insert(group.id.as_str()) || owners.contains_key(group.id.as_str()) {
            return Err(GraphError::DuplicateSubgraph(group.id.clone()));
        }
        group.validate()?;
        if group.isolated {
            isolated_groups.insert(group.id.as_str());
        }
    }
    for group in subgraphs {
        if let Some(parent) = &group.parent_id
            && !groups.contains(parent.as_str())
        {
            return Err(GraphError::UnknownSubgraph(parent.clone()));
        }
    }
    for (_, owner, _) in nodes {
        if let Some(group) = owner
            && !groups.contains(group)
        {
            return Err(GraphError::UnknownSubgraph(group.to_owned()));
        }
    }
    for (src, dst, mode) in edges {
        let &(source_group, _) = owners
            .get(src)
            .ok_or_else(|| GraphError::UnknownNode(src.to_owned()))?;
        let &(destination_group, is_join) = owners
            .get(dst)
            .ok_or_else(|| GraphError::UnknownNode(dst.to_owned()))?;
        if mode.is_isolated() {
            if is_join {
                return Err(GraphError::InvalidEdge {
                    src: src.to_owned(),
                    dst: dst.to_owned(),
                    detail: "isolated edge (->! / ->+) must not target a JOIN node".into(),
                });
            }
            let src_scopes = isolated_path(source_group, &isolated_groups);
            let dst_scopes = isolated_path(destination_group, &isolated_groups);
            let common = src_scopes
                .iter()
                .zip(&dst_scopes)
                .take_while(|(a, b)| a == b)
                .count();
            if dst_scopes.len() <= common {
                return Err(GraphError::InvalidEdge {
                    src: src.to_owned(),
                    dst: dst.to_owned(),
                    detail: "isolated edge (->! / ->+) must enter a new isolated subgraph".into(),
                });
            }
        }
    }
    Ok(())
}
