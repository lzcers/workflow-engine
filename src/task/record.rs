//! 数据库无关的任务记录、入队输入、状态与运行快照。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 持久化任务状态，在所有后端使用相同的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// 输入已确定，等待认领。
    Pending,
    /// JOIN 正在收集输入，结算事务确认就绪后转为 Pending。
    Collecting,
    Running,
    Succeeded,
    Failed,
    /// 失败已由后继任务接管。
    Superseded,
    /// 运行被主动取消。
    Cancelled,
}

impl TaskStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Collecting => "collecting",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Collecting | Self::Running)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "collecting" => Ok(Self::Collecting),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown task status: {value}")),
        }
    }
}

impl TryFrom<String> for TaskStatus {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// 一次运行的汇总状态，由该运行下全部任务快照推导。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn from_tasks(tasks: &[TaskRecord]) -> Self {
        if tasks.iter().any(|t| t.status.is_active()) {
            Self::Running
        } else if tasks.iter().any(|t| t.status == TaskStatus::Cancelled) {
            Self::Cancelled
        } else if tasks.iter().any(|t| t.status == TaskStatus::Failed) {
            Self::Failed
        } else {
            Self::Succeeded
        }
    }

    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// 一次运行的完整状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub workflow_id: String,
    pub run_id: i64,
    pub status: RunStatus,
    pub tasks: Vec<TaskRecord>,
}

/// 已创建任务的快照，包含引擎分配的身份、执行状态和结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub workflow_id: String,
    pub task_id: i64,
    /// 一次完整运行的身份；进入新的执行作用域时保持不变。
    pub run_id: i64,
    /// 当前子图执行实例；父子关系保存在 scopes 表，None 为运行根作用域。
    pub scope_id: Option<i64>,
    /// 来源任务按贡献顺序去重。独立入口为空。
    /// 失败后继同样记录来源；此字段不要求来源任务成功。
    pub parent_task_ids: Vec<i64>,
    pub name: String,
    pub status: TaskStatus,
    /// 读取快照时的租约截止时间；执行与结算期间由调度器续租，不代表节点运行时限。
    pub lease_until: Option<chrono::DateTime<chrono::Utc>>,
    /// 下次允许被认领的时间；None 表示立即可认领，退避重试时为未来时间。
    pub available_at: Option<chrono::DateTime<chrono::Utc>>,
    /// 每次认领递增的代次，用于写入校验；超过 max_attempts 后只处理最终失败。
    pub attempt: i32,
    /// 该任务允许的最大业务执行次数（含首次认领）。
    pub max_attempts: i32,
    /// 任务输入因 name 而异，保留为通用 JSON。
    pub input: Value,
    /// 节点输出以 JSON 落库，尚未写入时为 None。
    pub output: Option<Value>,
    pub error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// 待入队的任务内容；身份、执行状态和结果由引擎写入 [`TaskRecord`]。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    pub workflow_id: String,
    /// 从根到入口所属隔离作用域的完整子图 ID，根节点为空。
    #[serde(default)]
    pub subgraph_path: Vec<String>,
    pub name: String,
    pub input: Value,
    /// 可选覆盖该入口任务的最大业务执行次数；None 时使用引擎默认上限。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i32>,
}

impl Task {
    /// 创建根作用域的独立运行入口。子图入口可通过 GraphDescription::task 构造。
    pub fn new(workflow_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            subgraph_path: Vec::new(),
            name: name.into(),
            input,
            max_attempts: None,
        }
    }
}
