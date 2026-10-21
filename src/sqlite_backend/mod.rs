//! SQLite 通用工作流后端，由 `sqlite` feature 启用。
//!
//! [`migrate`] 建立引擎自有表，随后用 [`SqliteTaskRepo`] 入队、查询任务，
//! 用绑定工作流的 [`SqliteTaskStore`] 驱动 [`crate::Scheduler`]。
//! 数组与 JSON 输入输出存为 JSON 文本，时间存为 UTC 文本。
//! 写事务通过 `BEGIN IMMEDIATE` 串行化，保证并发认领和扇入合并的原子性。
//!
//! SQLite 没有 LISTEN/NOTIFY；[`run_task_ready_listener`] 定期扫描数据库，
//! 为就绪工作流创建或唤醒 worker，支持其他连接提交任务和进程重启后的恢复。

mod listener;
mod repo;
mod rows;
mod schema;
mod store;
mod task;

#[cfg(test)]
mod tests;

pub use listener::run_task_ready_listener;
pub use repo::{FanoutInstances, ROW_COLS, SqliteError, SqliteResult, SqliteTaskRepo, status};
pub use rows::TaskRow;
pub use schema::migrate;
pub use store::{SqliteStoreError, SqliteTaskStore};
pub use task::{NewTask, SqliteTask};
