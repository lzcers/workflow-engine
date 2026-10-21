//! `tasks` 表的行类型（schema 契约镜像）。JSON 列以 `serde_json::Value` 承载。

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct TaskRow {
    pub workflow_id: String,
    pub task_id: i64,
    pub run_id: Option<i64>,
    pub name: String,
    pub instance_id: Option<i32>,
    pub status: String,
    pub claimed_by: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub task_type: String,
    #[sqlx(json)]
    pub depends_on_ids: Vec<i64>,
    #[sqlx(json)]
    pub depends_on_names: Vec<String>,
    pub input_json: Value,
    pub output_json: Option<Value>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
