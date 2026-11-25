//! Test seeds may model a task that already belongs to an existing run.
use crate::{Task, TaskStatus, task::Successor};
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

pub(crate) fn plan(dst: &'static str, inputs: Vec<Value>) -> Successor {
    Successor {
        dst,
        subgraph_path: Vec::new(),
        inputs,
        new_scope_per_input: false,
        initial_status: TaskStatus::Pending,
    }
}

pub(crate) fn scoped_plan(
    dst: &'static str,
    subgraph_path: &[&str],
    inputs: Vec<Value>,
) -> Successor {
    Successor {
        subgraph_path: subgraph_path.iter().map(|&id| id.to_owned()).collect(),
        ..plan(dst, inputs)
    }
}

pub(crate) fn join_plan(dst: &'static str, inputs: Vec<Value>) -> Successor {
    Successor {
        initial_status: TaskStatus::Collecting,
        ..plan(dst, inputs)
    }
}
