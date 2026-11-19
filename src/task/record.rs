//! 数据库无关的任务记录、入队输入和状态。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::JoinState;

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
    /// 一次完整运行的身份；进入新的执行作用域时保持不变。
    pub run_id: Option<i64>,
    /// 当前子图执行实例；父子关系保存在 scopes 表，None 为运行根作用域。
    pub scope_id: Option<i64>,
    /// 来源任务 ID 集合；入口为空，普通后继记录源任务，JOIN 累积并去重所有贡献者。
    /// 失败后继同样记录来源；此字段不要求来源任务成功。
    pub parent_task_ids: Vec<i64>,
    pub name: String,
    pub status: TaskStatus,
    /// 最近一次认领的执行器身份；Scheduler 每次启动生成新的 UUID。
    pub claimed_by: Option<String>,
    /// 读取快照时的租约截止时间；执行与结算期间由调度器续租，不代表节点运行时限。
    pub lease_until: Option<chrono::DateTime<chrono::Utc>>,
    /// 每次认领递增的代次，用于写入校验；超过 max_attempts 后只处理最终失败。
    pub attempt: i32,
    /// 业务执行的最大尝试次数；最终失败的恢复认领不再执行业务。
    pub max_attempts: i32,
    /// 节点声明的任务类型（`normal`/`join`/`event`）；仅 JOIN 额外检查静态前驱。
    pub task_type: String,
    /// JOIN 的预期来源、实际贡献和生产结束屏障。
    pub join_state: JoinState,
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
    /// 直接入队的父实例，None 为根作用域。
    pub scope_id: Option<i64>,
    /// 目标节点的静态子图路径，仅在入队时使用，不存入任务表。
    pub subgraph_path: Vec<String>,
    /// 来源任务 ID 集合；独立入口为空，不作为认领时的成功条件。
    pub parent_task_ids: Vec<i64>,
    pub name: String,
    /// 节点声明的任务类型（`normal`/`join`/`event`），入队时原样保存。
    pub task_type: String,
    pub max_attempts: i32,
    /// JOIN 静态前驱节点名全集（普通节点默认空集）。
    pub depends_on_names: Vec<String>,
    pub input: Value,
}

impl Task {
    /// 创建普通类型的独立运行入口；入队前可设置节点类型、运行、作用域、来源、静态前驱和尝试预算。
    pub fn new(workflow_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: None,
            scope_id: None,
            subgraph_path: Vec::new(),
            parent_task_ids: Vec::new(),
            name: name.into(),
            task_type: "normal".to_owned(),
            max_attempts: 3,
            depends_on_names: Vec::new(),
            input,
        }
    }
}
