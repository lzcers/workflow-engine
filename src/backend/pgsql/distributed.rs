//! Run a PostgreSQL-backed workflow across independent producer and worker processes.

use std::sync::Arc;

use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};

use super::repo::{PgTaskRepo, insert_task_at_path, notify_tasks_ready};
use crate::engine::StorageError;
use crate::graph::GraphDescription;
use crate::{EngineError, Graph, NodeContext, RunHandle, Task, TaskStatus, Workflow};

pub use super::schema::migrate as initialize;

/// Submit a standalone run from a published graph description. The caller commits the
/// transaction and may include its own scheduling or deduplication records in that commit.
pub async fn submit(
    tx: &mut Transaction<'_, Postgres>,
    graph: &GraphDescription,
    revision: &str,
    entry: &str,
    input: &Value,
) -> Result<RunHandle, EngineError> {
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == entry)
        .ok_or_else(|| EngineError::UnknownNode(entry.to_owned()))?;
    graph
        .validate_subgraphs()
        .map_err(|detail| StorageError::InvalidFanout { detail })?;
    let path = crate::graph::subgraph_path(&graph.subgraphs, node.subgraph_id.as_deref());
    let task = Task::new(&graph.workflow_id, entry, input.clone());
    let record = insert_task_at_path(tx, &task, Some(revision), &path, TaskStatus::Pending).await?;
    notify_tasks_ready(tx, &graph.workflow_id).await?;
    Ok(RunHandle {
        workflow_id: record.workflow_id,
        run_id: record.task_id,
    })
}

/// Start one scheduler that claims only tasks matching the graph and revision.
/// Every claim records this process's worker ID, including claims recovered after lease expiry.
pub fn worker<Ctx>(pool: PgPool, graph: Graph<Ctx>, revision: &str, worker_id: &str) -> Workflow
where
    Ctx: NodeContext + Sync + 'static,
{
    let graph = Arc::new(graph);
    let repo = PgTaskRepo::for_worker(pool, graph.clone(), revision, worker_id);
    Workflow::new(graph, Arc::new(repo))
}
