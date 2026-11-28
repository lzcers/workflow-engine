//! 数据库无关的任务内容、记录、状态与运行快照。

pub(crate) mod join;
mod record;
pub(crate) mod scope;
pub(crate) mod store;

/// 每条任务默认的业务最大执行次数（包含首次认领），可通过节点或工作流 `RetryPolicy` 覆盖。
pub const DEFAULT_MAX_ATTEMPTS: i32 = 3;

pub use record::{RunSnapshot, RunStatus, Task, TaskRecord, TaskStatus};
pub use store::{Dispatch, Successor};
pub(crate) use store::{
    JoinExternalEntry, JoinRule, SettlementConfig, SettlementRules, TaskStore,
};
