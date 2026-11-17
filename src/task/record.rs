//! 数据库无关的任务记录、入队输入和状态。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 持久化任务状态，在所有后端使用相同的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    /// 失败已由后继任务接管。
    Superseded,
}

impl TaskStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Superseded => "superseded",
        }
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = serde::de::value::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::deserialize(serde::de::value::StrDeserializer::new(value))
    }
}

impl TryFrom<String> for TaskStatus {
    type Error = serde::de::value::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// 已创建任务的快照，包含引擎分配的身份、执行状态和结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub workflow_id: String,
    pub task_id: i64,
    /// 分组键（代际分组）：同组任务共享扇入去重与 JOIN 等齐前驱的判定域
    /// 业务侧可承载血缘语义。
    pub run_id: Option<i64>,
    pub name: String,
    /// NULL=单实例；Some(i)=map 边扇出的第 i 个实例（i ∈ 0..N-1）。
    pub instance_id: Option<i32>,
    pub status: TaskStatus,
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

/// 待入队的任务内容；身份、执行状态和结果由引擎写入 [`TaskRecord`]。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
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
