//! 数据库无关的任务记录与调度存储契约。

pub mod record;
pub mod store;

pub use record::{Task, TaskRecord, TaskStatus};
pub use store::{FanoutInstances, FanoutPlan, TaskError, TaskStore};
