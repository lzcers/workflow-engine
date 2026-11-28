//! 图编译期预计算：子图作用域路径、O(出度) 邻接索引与静态 JOIN 结算规则。

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{Graph, MergeMode, SlotSpec, subgraph_path};
use crate::{
    node::NodeRole,
    task::{JoinExternalEntry, JoinRule, SettlementRules},
};

impl<Ctx> Graph<Ctx> {
    /// 校验通过后预计算作用域路径、邻接索引与静态结算规则。
    pub(super) fn finalize(&mut self) {
        for entry in self.nodes.values_mut() {
            let path: Vec<Arc<str>> = subgraph_path(entry.subgraph_id.as_deref())
                .map(Arc::from)
                .collect();
            entry.scope_path = Arc::from(path);
        }

        self.success_adj.clear();
        for (idx, edge) in self.success_edges.iter().enumerate() {
            self.success_adj
                .entry(edge.src_arc())
                .or_default()
                .push(idx);
        }
        self.failure_adj.clear();
        for (idx, edge) in self.failure_edges.iter().enumerate() {
            self.failure_adj
                .entry(edge.src_arc())
                .or_default()
                .push(idx);
        }

        self.settlement_rules = self.compute_settlement_rules();
    }

    fn compute_settlement_rules(&self) -> SettlementRules {
        // 反向邻接表：O(V + E) 替代全表扫描。
        let mut incoming_all: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in self.success_edges.iter().chain(&self.failure_edges) {
            incoming_all
                .entry(edge.dst())
                .or_default()
                .push(edge.src());
        }

        let mut join_rules = HashMap::new();
        let mut affected_joins: HashMap<Arc<str>, Vec<Arc<str>>> = HashMap::new();

        for (name, entry) in &self.nodes {
            let NodeRole::Join(specs) = entry.node.role() else {
                continue;
            };
            let rule = self.build_join_rule(name, &entry.scope_path, &specs, &incoming_all);
            for upstream in &rule.upstreams {
                affected_joins
                    .entry(Arc::from(upstream.as_str()))
                    .or_default()
                    .push(name.clone());
            }
            join_rules.insert(name.clone(), rule);
        }
        for joins in affected_joins.values_mut() {
            joins.sort();
            joins.dedup();
        }
        SettlementRules {
            join_rules,
            affected_joins,
        }
    }

    fn build_join_rule(
        &self,
        dst: &str,
        target_scope: &[Arc<str>],
        specs: &[SlotSpec],
        incoming_all: &HashMap<&str, Vec<&str>>,
    ) -> JoinRule {
        let required_slots: Vec<String> = specs
            .iter()
            .filter(|spec| spec.required)
            .map(|spec| spec.key.to_owned())
            .collect();
        let optional_append_slots: Vec<String> = specs
            .iter()
            .filter(|spec| !spec.required && spec.merge == MergeMode::Append)
            .map(|spec| spec.key.to_owned())
            .collect();

        // 识别所有跨边界进入本 JOIN 的直接前驱（包含成功边与失败边），记录公共前缀深度。
        let mut ext_seen: HashMap<&str, usize> = HashMap::new();
        if let Some(srcs) = incoming_all.get(dst) {
            for &src in srcs {
                let src_scope = &self.nodes[src].scope_path;
                if !src_scope.starts_with(target_scope) {
                    let depth = src_scope
                        .iter()
                        .zip(target_scope)
                        .take_while(|(a, b)| a == b)
                        .count();
                    ext_seen.insert(src, depth);
                }
            }
        }
        let mut external_entries: Vec<JoinExternalEntry> = ext_seen
            .into_iter()
            .map(|(name, entry_depth)| JoinExternalEntry {
                name: name.to_owned(),
                entry_depth,
            })
            .collect();
        external_entries.sort_by(|a, b| a.name.cmp(&b.name));

        // 反向 BFS 覆盖所有仍可能为该 JOIN 产出贡献的上游（排除 dst 自身）。
        let mut stack: Vec<&str> = incoming_all
            .get(dst)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
            .to_vec();
        let mut seen: HashSet<&str> = HashSet::new();
        while let Some(node) = stack.pop() {
            if node == dst || !seen.insert(node) {
                continue;
            }
            if let Some(preds) = incoming_all.get(node) {
                stack.extend(preds.iter().copied());
            }
        }
        let mut upstreams: Vec<String> = seen.into_iter().map(str::to_owned).collect();
        upstreams.sort();

        JoinRule {
            #[cfg(feature = "pg")]
            scope_depth: target_scope.len(),
            required_slots,
            optional_append_slots,
            external_entries,
            upstreams,
        }
    }
}
