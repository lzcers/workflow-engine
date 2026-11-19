//! 数据库无关的任务内容、记录与状态。

mod record;
pub(crate) mod scope;
pub(crate) mod store;

pub use record::{Task, TaskRecord, TaskStatus};
pub use scope::ScopeRecord;
pub(crate) use store::TaskStore;
