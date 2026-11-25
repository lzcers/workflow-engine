//! 数据库无关的任务记录、入队输入和状态。

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
    /// 每次认领递增的代次，用于写入校验；超过统一上限后只处理最终失败。
    pub attempt: i32,
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
#[serde(deny_unknown_fields)]
pub struct Task {
    pub workflow_id: String,
    /// 从根到入口所属子图的完整子图 ID，根节点为空。
    #[serde(default)]
    pub subgraph_path: Vec<String>,
    pub name: String,
    pub input: Value,
}

impl Task {
    /// 创建根作用域的独立运行入口。子图入口可通过 GraphDescription::task 构造。
    pub fn new(workflow_id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            subgraph_path: Vec::new(),
            name: name.into(),
            input,
        }
    }
}
