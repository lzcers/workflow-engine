//! 数据库无关的任务内容、记录与状态。

pub(crate) mod join;
mod record;
pub(crate) mod scope;
pub(crate) mod store;

/// 每条任务最多执行业务的次数，包含首次认领。
pub const MAX_ATTEMPTS: i32 = 3;

pub use record::{Task, TaskRecord, TaskStatus};
pub use scope::ScopeRecord;
pub use store::{Dispatch, JoinPredecessor, JoinRule, SettlementRules, Successor};
pub(crate) use store::TaskStore;
