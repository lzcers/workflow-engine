//! 图静态校验：元数据、子图归属与隔离边界、JOIN 槽位与类型指纹、拓扑无环与回边约束。

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::{
    EdgeKind, EdgeMode, Graph, MergeMode, SlotSpec, SubgraphDescription, TaskType,
    qualified_parent, subgraph_path,
};
use crate::{
    error::GraphError,
    node::{NodeRole, TypeFingerprint},
};

impl<Ctx> Graph<Ctx> {
    /// 静态校验：元数据非空、子图归属与隔离边界合法、JOIN 槽位完整且类型匹配、
    /// success 子图无环、failure 回边不自环。
    pub(super) fn validate(&self) -> Result<(), GraphError> {
        if self.workflow_id.trim().is_empty() {
            return Err(GraphError::EmptyWorkflowId);
        }
        if self.title.trim().is_empty() {
            return Err(GraphError::EmptyTitle);
        }
        self.validate_subgraphs()?;

        let mut join_specs: HashMap<&str, Vec<SlotSpec>> = HashMap::new();
        for (name, entry) in &self.nodes {
            if let NodeRole::Join(specs) = entry.node.role() {
                let mut keys = HashSet::new();
                for spec in &specs {
                    if !keys.insert(spec.key) {
                        return Err(GraphError::InvalidJoin {
                            node: name.to_string(),
                            detail: format!("duplicate slot `{}`", spec.key),
                        });
                    }
                }
                join_specs.insert(name.as_ref(), specs);
            }
        }

        let isolated_set: HashSet<&str> = self
            .subgraphs
            .iter()
            .filter(|s| s.isolated)
            .map(|s| s.id.as_str())
            .collect();
        let mut covered_slots: HashMap<(&str, &str), usize> = HashMap::new();
        let mut in_scope_unconditional_set: HashMap<(&str, &str), usize> = HashMap::new();

        for edge in self.success_edges.iter().chain(&self.failure_edges) {
            let src = edge.src();
            let dst = edge.dst();
            let src_node = &self.nodes[src];
            let dst_node = &self.nodes[dst];
            let (node_src_ty, role_label) = match edge.kind() {
                EdgeKind::Success => (src_node.node.out_type(), "output"),
                EdgeKind::Failure => (src_node.node.in_type(), "input"),
            };
            if !readable(edge.src_type(), node_src_ty) {
                return Err(GraphError::InvalidEdge {
                    src: src.to_owned(),
                    dst: dst.to_owned(),
                    detail: format!(
                        "edge reads {} which does not match source {role_label} type {}",
                        edge.src_type().name(),
                        node_src_ty.name()
                    ),
                });
            }

            match (dst_node.node.role().task_type(), edge.slot()) {
                (TaskType::Join, None) => {
                    return Err(GraphError::InvalidEdge {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                        detail: format!(
                            "edge targeting JOIN node must specify a slot ({dst}::field)"
                        ),
                    });
                }
                (TaskType::Join, Some(edge_slot)) => {
                    let specs = &join_specs[dst];
                    let Some(spec) = specs.iter().find(|s| s.key == edge_slot.key) else {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!("unknown JOIN slot `{}`", edge_slot.key),
                        });
                    };
                    if spec.merge != edge_slot.merge {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!("slot `{}` merge mode mismatch", edge_slot.key),
                        });
                    }
                    if !writable(edge.dst_type(), spec.item_type) {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "slot `{}` contribution type {} does not match {}",
                                edge_slot.key,
                                edge.dst_type().name(),
                                spec.item_type.name()
                            ),
                        });
                    }
                    if spec.merge == MergeMode::Set && edge.mode().is_expand() {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "single-value JOIN slot `{}` does not accept list expansion",
                                edge_slot.key
                            ),
                        });
                    }
                    *covered_slots.entry((dst, spec.key)).or_insert(0) += 1;

                    // 同作用域内多条无条件成功边禁止共写同一个单值 Set 槽位；条件边（XOR-Merge）允许。
                    if edge.kind() == EdgeKind::Success
                        && spec.merge == MergeMode::Set
                        && !edge.has_condition()
                    {
                        let src_scopes = isolated_path(src_node.subgraph_id.as_deref(), &isolated_set);
                        let dst_scopes = isolated_path(dst_node.subgraph_id.as_deref(), &isolated_set);
                        if src_scopes.starts_with(&dst_scopes) {
                            let count = in_scope_unconditional_set
                                .entry((dst, spec.key))
                                .or_insert(0);
                            *count += 1;
                            if *count > 1 {
                                return Err(GraphError::InvalidJoin {
                                    node: dst.to_owned(),
                                    detail: format!(
                                        "slot `{}` has multiple unconditional in-scope writers; mark it #[join(collect)] or guard with when(...)",
                                        spec.key
                                    ),
                                });
                            }
                        }
                    }
                }
                (TaskType::Normal, Some(edge_slot)) => {
                    return Err(GraphError::InvalidEdge {
                        src: src.to_owned(),
                        dst: dst.to_owned(),
                        detail: format!(
                            "slot syntax `::{}` is only valid for JOIN nodes",
                            edge_slot.key
                        ),
                    });
                }
                (TaskType::Normal, None) => {
                    if !writable(edge.dst_type(), dst_node.node.in_type()) {
                        return Err(GraphError::InvalidEdge {
                            src: src.to_owned(),
                            dst: dst.to_owned(),
                            detail: format!(
                                "edge output type {} does not match destination input type {}",
                                edge.dst_type().name(),
                                dst_node.node.in_type().name()
                            ),
                        });
                    }
                }
            }
        }

        let mut join_names: Vec<&str> = join_specs.keys().copied().collect();
        join_names.sort_unstable();
        for name in join_names {
            for spec in &join_specs[name] {
                if spec.required && !covered_slots.contains_key(&(name, spec.key)) {
                    return Err(GraphError::InvalidJoin {
                        node: name.to_owned(),
                        detail: format!("missing incoming edge for required slot `{}`", spec.key),
                    });
                }
            }
        }

        // success 子图无环（Kahn 拓扑排序）
        let mut indegree: HashMap<&str, usize> =
            self.nodes.keys().map(|k| (k.as_ref(), 0)).collect();
        let mut succ: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.success_edges {
            *indegree.entry(e.dst()).or_insert(0) += 1;
            succ.entry(e.src()).or_default().push(e.dst());
        }
        let mut queue: Vec<&str> = indegree
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
            return Err(GraphError::SuccessCycle);
        }

        // failure 回边禁止指回自身（非自环循环由运行期 max_tasks_per_run 护栏限制）。
        for e in &self.failure_edges {
            if e.src() == e.dst() {
                return Err(GraphError::FailureSelfLoop {
                    src: e.src().to_owned(),
                    dst: e.dst().to_owned(),
                });
            }
        }
        Ok(())
    }

    fn validate_subgraphs(&self) -> Result<(), GraphError> {
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

pub(super) fn validate_subgraph_desc(desc: &SubgraphDescription) -> Result<(), GraphError> {
    if desc.id.is_empty() || desc.title.trim().is_empty() {
        return Err(GraphError::EmptySubgraphMetadata);
    }
    if qualified_parent(&desc.id) != desc.parent_id.as_deref() {
        return Err(GraphError::InvalidQualification {
            kind: "subgraph",
            id: desc.id.clone(),
        });
    }
    Ok(())
}

pub(super) fn validate_node_owner(id: &str, subgraph: Option<&str>) -> Result<(), GraphError> {
    if subgraph.is_some() && qualified_parent(id) != subgraph {
        return Err(GraphError::InvalidQualification {
            kind: "node",
            id: id.to_owned(),
        });
    }
    Ok(())
}

fn isolated_path<'a>(owner: Option<&'a str>, isolated_set: &HashSet<&str>) -> Vec<&'a str> {
    subgraph_path(owner)
        .filter(|id| isolated_set.contains(id))
        .collect()
}

pub(super) fn validate_subgraphs<'a>(
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
        validate_subgraph_desc(group)?;
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

/// 边「读取」节点值方向的指纹兼容判定：`edge_ty == node_ty` 或 `edge_ty == Value`。
fn readable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || edge_ty.id() == TypeFingerprint::of::<Value>().id()
}

/// 边「写入」节点值方向的指纹兼容判定：`edge_ty == node_ty` 或 `node_ty == Value`。
fn writable(edge_ty: TypeFingerprint, node_ty: TypeFingerprint) -> bool {
    edge_ty.id() == node_ty.id() || node_ty.id() == TypeFingerprint::of::<Value>().id()
}
