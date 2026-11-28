//! Real graph declarations for repository tests that do not run business nodes.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use serde_json::Value;

use crate::{
    Edge, Graph, Node, NodeError, NodeRole,
    graph::{JoinSlot, MergeMode, SlotSpec, TaskType},
};

struct Pass;
impl Node<()> for Pass {
    type In = Value;
    type Out = Value;
    async fn execute(&self, _: &mut (), input: Value) -> Result<Value, NodeError> {
        Ok(input)
    }
}

struct FixtureJoin(Vec<SlotSpec>);
impl Node<()> for FixtureJoin {
    type In = Value;
    type Out = Value;

    fn role(&self) -> NodeRole {
        NodeRole::Join(self.0.clone())
    }

    async fn execute(&self, _: &mut (), input: Value) -> Result<Value, NodeError> {
        Ok(input)
    }
}

pub fn slot_key(source: &'static str) -> &'static str {
    source.rsplit('.').next().unwrap_or(source)
}

pub fn graph(
    workflow_id: &str,
    nodes: &[(&'static str, TaskType, Option<&str>)],
    edges: &[(&'static str, &'static str)],
) -> Arc<Graph<()>> {
    let mut builder = Graph::builder(workflow_id, workflow_id, ());
    let mut subgraphs = BTreeSet::new();
    let mut join_targets: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
    for &(name, task_type, subgraph) in nodes {
        if task_type == TaskType::Join {
            join_targets.insert(name, Vec::new());
        }
        let mut current = subgraph;
        while let Some(id) = current {
            subgraphs.insert(id);
            current = id.rsplit_once('.').map(|(parent, _)| parent);
        }
    }
    for &(source, destination) in edges {
        if let Some(keys) = join_targets.get_mut(destination) {
            let key = slot_key(source);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    for id in subgraphs {
        builder = builder.subgraph(id, id, id.rsplit_once('.').map(|(parent, _)| parent));
    }
    for &(name, task_type, subgraph) in nodes {
        builder = match task_type {
            TaskType::Normal => {
                builder
                    .node_in_subgraph_with_key(name, Pass, name, "", subgraph)
                    .0
            }
            TaskType::Join => {
                let specs = join_targets
                    .get(name)
                    .into_iter()
                    .flatten()
                    .map(|&key| SlotSpec::new::<Value>(key, MergeMode::Append, true))
                    .collect();
                builder
                    .node_in_subgraph_with_key(name, FixtureJoin(specs), name, "", subgraph)
                    .0
            }
        };
    }
    for &(source, destination) in edges {
        if join_targets.contains_key(destination) {
            let key = slot_key(source);
            builder = builder.edge(
                Edge::success::<Value>(source)
                    .to_slot(destination, JoinSlot::<Value, Value>::append(key))
                    .direct(),
            );
        } else {
            builder = builder.edge(Edge::success::<Value>(source).to::<Value>(destination).direct());
        }
    }
    Arc::new(builder.build().unwrap())
}
