//! 执行图的只读描述，不包含上下文、运行状态或节点载荷。

use serde::Serialize;

use super::Graph;

/// 已构建执行图的可序列化拓扑。节点按 ID 排序，边按成功/失败及各自注册顺序输出。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphDescription {
    pub workflow_id: String,
    pub title: String,
    pub nodes: Vec<NodeDescription>,
    pub edges: Vec<EdgeDescription>,
}

/// 节点信息。输入输出字段为 Rust 类型名，不是 JSON Schema。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeDescription {
    /// 图中注册的节点名，与任务的 name 对应。
    pub id: String,
    pub title: String,
    pub description: String,
    pub task_type: String,
    pub input_type: String,
    pub output_type: String,
}

/// 边的触发类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    Success,
    Failure,
}

/// 一条实际注册的边。相同端点的多条边仍保留为独立记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EdgeDescription {
    /// 当前图内唯一的编号，如 success:0；注册顺序改变后不保证跨版本稳定。
    pub id: String,
    pub source: String,
    pub target: String,
    pub kind: EdgeKind,
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
                    }),
            )
            .collect();

        GraphDescription {
            workflow_id: self.workflow_id.clone(),
            title: self.title.clone(),
            nodes,
            edges,
        }
    }
}
