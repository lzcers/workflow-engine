//! SQLite 通用工作流后端，由 `sqlite` feature 启用。
//!
//! [`migrate`] 建立引擎自有表，随后用 [`SqliteTaskRepo`] 入队、查询任务，
//! 仓储直接实现任务存储接口，驱动 [`crate::scheduler::Scheduler`] 按图身份认领任务。
//! 数组与 JSON 输入输出存为 JSON 文本，时间存为 UTC 文本。
//! 任务事务通过 `BEGIN IMMEDIATE` 获取写锁，保证并发认领和 JOIN 汇合的原子性。
//!
//! SQLite 没有 LISTEN/NOTIFY；工作流通过引擎共享轮询器发现其他连接提交的任务。
//! 执行期间自动续租，租约失效后可重新认领；预算耗尽的任务取得新的结算认领，
//! 由调度器规划失败后继并原子结算，恢复时不再执行节点。

mod repo;
mod rows;
mod schema;
mod scope;
mod tx;

#[cfg(test)]
#[path = "../../../tests/unit/sqlite_scheduler.rs"]
mod scheduler_tests;

pub(crate) use repo::{SqliteTaskRepo, submit_in_tx};
pub(crate) use schema::migrate;
