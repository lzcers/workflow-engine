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
    /// 分组键（代际分组）：同组任务共享 JOIN 汇合与等齐前驱的判定域
    /// 业务侧可承载血缘语义。
    pub run_id: Option<i64>,
    /// 首次创建该任务的父任务 ID；入口为 None。JOIN 保留首个父任务，全部执行依赖见 depends_on_ids。
    pub parent_task_id: Option<i64>,
    pub name: String,
    pub status: TaskStatus,
    /// 最近一次认领的执行器身份；Scheduler 每次启动生成新的 UUID。
    pub claimed_by: Option<String>,
    /// 本次认领的固定处理截止时间，覆盖执行与结算，不续租。
    pub lease_until: Option<chrono::DateTime<chrono::Utc>>,
    /// 每次认领递增的代次，用于写入校验；超过 max_attempts 后只处理最终失败。
    pub attempt: i32,
    /// 业务执行的最大尝试次数；最终失败的恢复认领不再执行业务。
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
    /// Some(id) 加入指定运行；None 在入队时以新任务的 task_id 创建独立运行。
    pub run_id: Option<i64>,
    /// 父任务 ID；独立入口为 None，不替代执行依赖 depends_on_ids。
    pub parent_task_id: Option<i64>,
    pub name: String,
    pub max_attempts: i32,
    pub depends_on_ids: Vec<i64>,
    /// JOIN 静态前驱节点名全集（普通节点默认空集）。
    pub depends_on_names: Vec<String>,
    pub input: Value,
}

impl Task {
    /// 创建无依赖的独立运行入口；可在入队前设置 run_id、父任务、依赖和尝试预算。
    pub fn new(workflow_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: None,
            parent_task_id: None,
            name: name.into(),
            max_attempts: 3,
            depends_on_ids: Vec::new(),
            depends_on_names: Vec::new(),
            input,
        }
    }
}
