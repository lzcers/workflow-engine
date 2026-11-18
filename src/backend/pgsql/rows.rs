//! `tasks` 表的行类型（schema 契约镜像）。JSON 列以 `serde_json::Value` 承载。

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;

use crate::{TaskRecord, TaskStatus};

#[derive(Debug, Clone, FromRow)]
pub struct TaskRow {
    pub workflow_id: String,
    pub task_id: i64,
    pub run_id: Option<i64>,
    pub parent_task_ids: Vec<i64>,
    pub name: String,
    #[sqlx(try_from = "String")]
    pub status: TaskStatus,
    pub claimed_by: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub task_type: String,
    pub depends_on_names: Vec<String>,
    pub input_json: Value,
    pub output_json: Option<Value>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TaskRow> for TaskRecord {
    fn from(row: TaskRow) -> Self {
        Self {
            workflow_id: row.workflow_id,
            task_id: row.task_id,
            run_id: row.run_id,
            parent_task_ids: row.parent_task_ids,
            name: row.name,
            status: row.status,
            claimed_by: row.claimed_by,
            lease_until: row.lease_until,
            attempt: row.attempt,
            max_attempts: row.max_attempts,
            task_type: row.task_type,
            depends_on_names: row.depends_on_names,
            input: row.input_json,
            output: row.output_json,
            error: row.error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}
