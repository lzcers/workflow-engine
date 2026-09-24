//! 任务类型化记录：`tasks` 行的类型化视图，实现引擎 [`crate::Task`]。
//!
//! `run_id` 是通用闸门的「代际分组」维度：扇入去重与 JOIN 等齐前驱都按同组
//! （`IS NOT DISTINCT FROM`）判定；业务侧可把自己的血缘键（如 world 的 commit_id）
//! 映射到该列，引擎不解读其内容。

use serde_json::Value;

use crate::Task;

use super::rows::TaskRow;

#[derive(Debug, Clone)]
pub struct PgTask {
    pub workflow_id: String,
    pub task_id: i64,
    /// 分组键（代际分组）：同组任务共享扇入去重与 JOIN 等齐前驱的判定域
    /// （`IS NOT DISTINCT FROM`）；业务侧可承载血缘语义（如 world 的 commit_id）。
    pub run_id: Option<i64>,
    pub name: String,
    /// NULL=单实例；Some(i)=map 边扇出的第 i 个实例（i ∈ 0..N-1）。
    pub instance_id: Option<i32>,
    pub status: String,
    pub claimed_by: Option<String>,
    pub lease_until: Option<chrono::DateTime<chrono::Utc>>,
    pub attempt: i32,
    pub max_attempts: i32,
    /// 任务类型（`normal`/`join`）：由节点 `TASK_TYPE` 声明，决定存储的等齐前驱闸门。
    pub task_type: String,
    /// 血缘/溯源链路：父任务 id 集合（空集 = 无依赖）。
    pub depends_on_ids: Vec<i64>,
    /// 静态前驱节点名全集（通用血缘；仅 `task_type = 'join'` 用作等齐前驱的认领闸门）。
    pub depends_on_names: Vec<String>,
    /// 任务输入因 name 而异，暂保留为通用 JSON。
    pub input: Value,
    /// 节点输出直接以 JSON 落库（由节点类型化 `In`/`Out` 序列化），因 name 而异。
    pub output: Option<Value>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// 入队任务的输入。
#[derive(Debug, Clone)]
pub struct NewTask {
    pub workflow_id: String,
    pub run_id: Option<i64>,
    pub name: String,
    /// NULL=单实例；Some(i)=map 边扇出的第 i 个实例。
    pub instance_id: Option<i32>,
    pub max_attempts: i32,
    pub depends_on_ids: Vec<i64>,
    /// JOIN 静态前驱节点名全集（普通节点默认空集）。
    pub depends_on_names: Vec<String>,
    pub input: Value,
}

impl From<TaskRow> for PgTask {
    fn from(row: TaskRow) -> Self {
        Self {
            workflow_id: row.workflow_id,
            task_id: row.task_id,
            run_id: row.run_id,
            name: row.name,
            instance_id: row.instance_id,
            status: row.status,
            claimed_by: row.claimed_by,
            lease_until: row.lease_until,
            attempt: row.attempt,
            max_attempts: row.max_attempts,
            task_type: row.task_type,
            depends_on_ids: row.depends_on_ids,
            depends_on_names: row.depends_on_names,
            input: row.input_json,
            output: row.output_json,
            error: row.error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl Task for PgTask {
    fn task_id(&self) -> i64 {
        self.task_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn input(&self) -> &Value {
        &self.input
    }
}
