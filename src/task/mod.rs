//! 数据库无关的任务内容、记录与状态。

mod record;
pub(crate) mod store;

pub use record::{Task, TaskRecord, TaskStatus};
pub(crate) use store::{FanoutInstances, FanoutPlan, TaskStore};
