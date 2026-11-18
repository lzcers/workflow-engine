//! SQLite 通用工作流后端，由 `sqlite` feature 启用。
//!
//! [`migrate`] 建立引擎自有表，随后用 [`SqliteTaskRepo`] 入队、查询任务，
//! 用可共享的 [`SqliteTaskStore`] 驱动 [`crate::scheduler::Scheduler`]，调度器按图身份认领任务。
//! 数组与 JSON 输入输出存为 JSON 文本，时间存为 UTC 文本。
//! 写事务通过 `BEGIN IMMEDIATE` 串行化，保证并发认领和扇入合并的原子性。
//!
//! SQLite 没有 LISTEN/NOTIFY；宿主应用定期调用
//! [`SqliteTaskRepo::seal_budget_exhausted`] 和 [`SqliteTaskRepo::executable_workflow_ids`]，
//! 为就绪工作流创建或唤醒调度器，发现其他连接提交的任务并恢复到期租约。

mod repo;
mod rows;
mod schema;
mod store;

#[cfg(test)]
#[path = "../../../tests/unit/sqlite.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/unit/sqlite_scheduler.rs"]
mod scheduler_tests;

pub use repo::SqliteTaskRepo;
pub use schema::migrate;
pub use store::SqliteTaskStore;
