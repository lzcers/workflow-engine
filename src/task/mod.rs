//! 数据库无关的任务内容、记录、状态与内部存储契约。

pub(crate) mod join;
mod record;
pub(crate) mod scope;
pub(crate) mod store;

/// 每条任务的统一最大业务执行次数（包含首次认领）。
pub const MAX_ATTEMPTS: i32 = 3;

pub use record::{RunHandle, Task, TaskRecord, TaskStatus};
pub use store::{Dispatch, MergeMode, Successor};
pub(crate) use store::{JoinExternalEntry, JoinRule, SettlementRules, TaskStore};
