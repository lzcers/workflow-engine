//! Real graph declarations for repository tests that do not run business nodes.

use std::{collections::BTreeSet, sync::Arc};

use serde_json::{Value, json};

use crate::{
    Graph, JoinNode, Node, NodeError, Trigger,
    graph::{TaskType, direct, success},
};

struct Pass;
impl Node<()> for Pass {
    type In = Value;
    type Out = Value;
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

pub fn graph(
    workflow_id: &str,
    nodes: &[(&'static str, TaskType, Option<&str>)],
    edges: &[(&'static str, &'static str)],
) -> Arc<Graph<()>> {
    let mut builder = Graph::builder(workflow_id, workflow_id, ());
    let mut subgraphs = BTreeSet::new();
    for (_, _, subgraph) in nodes {
        let mut current = *subgraph;
        while let Some(id) = current {
            subgraphs.insert(id);
            current = id.rsplit_once('.').map(|(parent, _)| parent);
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
                builder
                    .node_in_subgraph_with_key(name, JoinNode, name, "", subgraph)
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
        builder = builder.edge(success(source, destination, direct::<Value>(), |_| true));
    }
    Arc::new(builder.build().unwrap())
}
