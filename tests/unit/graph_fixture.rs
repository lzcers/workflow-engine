//! Real graph declarations for repository tests that do not run business nodes.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use serde_json::{Value, json};

use crate::{
    Graph, Node, NodeError, Trigger,
    graph::{JoinSlot, MergeMode, SlotSpec, TaskType, direct, success, success_join},
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
    const TASK_TYPE: TaskType = TaskType::Join;

    fn join_slots(&self) -> Option<Vec<SlotSpec>> {
        Some(self.0.clone())
    }

    async fn execute(&self, _: &mut (), input: Value) -> Result<Value, NodeError> {
        Ok(input)
    }
}

struct Event;
impl Node<()> for Event {
    type In = Value;
    type Out = Value;
    const TASK_TYPE: TaskType = TaskType::Event;
    fn trigger(&self) -> Option<Trigger> {
        Some(Trigger {
            kind: "test".into(),
            config: json!({}),
        })
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
                    .map(|&key| SlotSpec::new::<Value>(key, MergeMode::Append, true, true))
                    .collect();
                builder
                    .node_in_subgraph_with_key(name, FixtureJoin(specs), name, "", subgraph)
                    .0
            }
            TaskType::Event => {
                builder
                    .node_in_subgraph_with_key(name, Event, name, "", subgraph)
                    .0
            }
        };
    }
    for &(source, destination) in edges {
        if join_targets.contains_key(destination) {
            let key = slot_key(source);
            builder = builder.edge(success_join(
                source,
                destination,
                JoinSlot::<Value, Value>::append(key),
                direct::<Value>(),
                |_| true,
            ));
        } else {
            builder = builder.edge(success(source, destination, direct::<Value>(), |_| true));
        }
    }
    Arc::new(builder.build().unwrap())
}
