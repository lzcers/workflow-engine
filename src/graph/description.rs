//! 执行图的只读描述，不包含上下文、运行状态或节点载荷。

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::{EdgeMode, Graph, TaskType, Trigger, qualified_parent, subgraph_path};

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
    /// 边的路由模式：`passthrough`、`map` 或 `foreach`（失败边只为前两者）。
    pub mode: EdgeMode,
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
                task_type: entry.node.task_type(),
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
                mode: edge.mode(),
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
                        mode: edge.mode(),
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
    fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() || self.title.trim().is_empty() {
            return Err("subgraph id and title must not be empty".into());
        }
        if qualified_parent(&self.id) != self.parent_id.as_deref() {
            return Err(format!(
                "subgraph {} must use its full parent-qualified name",
                self.id
            ));
        }
        Ok(())
    }
}

fn validate_node_owner(id: &str, subgraph: Option<&str>) -> Result<(), String> {
    if subgraph.is_some() && qualified_parent(id) != subgraph {
        return Err(format!(
            "node {id} must use its full subgraph-qualified name"
        ));
    }
    Ok(())
}

impl GraphDescription {
    /// 从图描述构造可直接提交的入口任务，并解析其子图作用域。
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
            return Err(crate::EngineError::InvalidGraph(format!(
                "duplicate node id: {entry}"
            )));
        }
        let mut task = crate::Task::new(&self.workflow_id, entry, input);
        task.subgraph_path = self
            .entry_path(node)
            .map_err(crate::EngineError::InvalidGraph)?;
        Ok(task)
    }

    fn entry_path(&self, node: &NodeDescription) -> Result<Vec<String>, String> {
        let mut path = Vec::new();
        let owner = node.subgraph_id.as_deref();
        validate_node_owner(&node.id, owner)?;
        for id in subgraph_path(owner) {
            let mut groups = self.subgraphs.iter().filter(|group| group.id == id);
            let group = groups
                .next()
                .ok_or_else(|| format!("subgraph {id} not registered"))?;
            if groups.next().is_some() {
                return Err(format!("duplicate subgraph id: {id}"));
            }
            group.validate()?;
            path.push(id.to_owned());
        }
        Ok(path)
    }

    /// 校验静态归属以及 foreach 的边界。允许普通边直接连接子图内部节点。
    pub fn validate_subgraphs(&self) -> Result<(), String> {
        let nodes = self.nodes.iter().map(|node| {
            (
                node.id.as_str(),
                node.subgraph_id.as_deref(),
                node.task_type == TaskType::Join,
            )
        });
        let edges = self.edges.iter().map(|edge| {
            (
                edge.source.as_str(),
                edge.target.as_str(),
                edge.mode == EdgeMode::Foreach,
            )
        });
        validate_subgraphs(nodes, self.subgraphs.iter(), edges)
    }
}

impl<Ctx> Graph<Ctx> {
    pub(super) fn validate_subgraphs(&self) -> Result<(), String> {
        // 沿用按 ID 报告首个拓扑错误的顺序。
        let mut nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(&id, entry)| {
                (
                    id,
                    entry.subgraph_id.as_deref(),
                    entry.node.task_type() == TaskType::Join,
                )
            })
            .collect();
        nodes.sort_unstable_by_key(|&(id, _, _)| id);
        let mut groups: Vec<_> = self.subgraphs.iter().collect();
        groups.sort_by_key(|group| &group.id);

        let success = self
            .success_edges
            .iter()
            .map(|e| (e.src(), e.dst(), e.mode() == EdgeMode::Foreach));
        let failure = self.failure_edges.iter().map(|e| (e.src(), e.dst(), false));
        validate_subgraphs(
            nodes.iter().copied(),
            groups.iter().copied(),
            success.chain(failure),
        )
    }
}

fn validate_subgraphs<'a>(
    nodes: impl Iterator<Item = (&'a str, Option<&'a str>, bool)> + Clone,
    subgraphs: impl Iterator<Item = &'a SubgraphDescription> + Clone,
    edges: impl Iterator<Item = (&'a str, &'a str, bool)>,
) -> Result<(), String> {
    let mut owners = HashMap::new();
    for (id, owner, is_join) in nodes.clone() {
        validate_node_owner(id, owner)?;
        if owners.insert(id, (owner, is_join)).is_some() {
            return Err(format!("duplicate node id: {id}"));
        }
    }

    let mut groups = HashSet::new();
    for group in subgraphs.clone() {
        if !groups.insert(group.id.as_str()) || owners.contains_key(group.id.as_str()) {
            return Err(format!("duplicate subgraph id: {}", group.id));
        }
        group.validate()?;
    }
    // 父 ID 已验证为严格更短的限定名前缀，确认父级存在即可保证父链完整。
    for group in subgraphs {
        if let Some(parent) = &group.parent_id
            && !groups.contains(parent.as_str())
        {
            return Err(format!("subgraph {parent} not registered"));
        }
    }
    for (id, owner, _) in nodes {
        if let Some(group) = owner
            && !groups.contains(group)
        {
            return Err(format!("node {id}: subgraph {group} not registered"));
        }
    }
    for (src, dst, foreach) in edges {
        let &(source_group, _) = owners
            .get(src)
            .ok_or_else(|| format!("edge src={src} not registered"))?;
        let &(destination_group, is_join) = owners
            .get(dst)
            .ok_or_else(|| format!("edge dst={dst} not registered"))?;
        if foreach {
            if is_join {
                return Err(format!("foreach edge must not target join node {dst}"));
            }
            if destination_group
                .is_none_or(|target| subgraph_path(source_group).any(|id| id == target))
            {
                return Err(format!(
                    "foreach edge {src} -> {dst} must enter a new subgraph"
                ));
            }
        }
    }
    Ok(())
}
