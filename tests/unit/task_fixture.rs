//! Test seeds may model a task that already belongs to an existing run.
use std::sync::Arc;

use crate::{
    Task,
    graph::MergeMode,
    task::{Dispatch, Successor},
};
use serde_json::Value;

#[derive(Clone)]
pub(crate) struct TaskSeed {
    pub task: Task,
    pub run_id: Option<i64>,
    pub scope_id: Option<i64>,
    pub parent_task_ids: Vec<i64>,
}

impl TaskSeed {
    pub fn new(workflow_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self {
            task: Task::new(workflow_id, name, input),
            run_id: None,
            scope_id: None,
            parent_task_ids: Vec::new(),
        }
    }
}

pub(crate) fn plan(dst: &str, inputs: Vec<Value>) -> Successor {
    Successor {
        dst: Arc::from(dst),
        subgraph_path: Arc::from([]),
        inputs,
        dispatch: Dispatch::Shared,
        max_attempts: None,
    }
}

pub(crate) fn scoped_plan(dst: &str, subgraph_path: &[&str], inputs: Vec<Value>) -> Successor {
    Successor {
        subgraph_path: Arc::from(
            subgraph_path
                .iter()
                .map(|&id| Arc::<str>::from(id))
                .collect::<Vec<_>>(),
        ),
        ..plan(dst, inputs)
    }
}

pub(crate) fn join_plan(
    dst: &str,
    key: &'static str,
    merge: MergeMode,
    inputs: Vec<Value>,
) -> Successor {
    Successor {
        dispatch: Dispatch::Join { key, merge },
        ..plan(dst, inputs)
    }
}
