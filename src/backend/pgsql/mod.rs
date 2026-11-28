//! PostgreSQL 通用工作流实现：把 [`crate::task::TaskStore`] 落到 `tasks` / `scopes` 表。
//!
//! 本模块由 `pg` feature 启用，仅依赖 [`crate::task`] 与引擎自有 schema（[`migrate`] 幂等执行）。
//! 引擎表不持有指向业务表的 FK；消费方可在执行 [`migrate`] 后自行以 `ALTER TABLE` 补充业务约束。

mod repo;
mod rows;
mod schema;
mod scope;
mod tx;
mod wake;

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_support.rs"]
mod test_support;

/// PostgreSQL 跨进程唤醒通道名（仅内部用于 `NOTIFY`/`LISTEN`）。
pub(super) const PG_NOTIFY_CHANNEL: &str = "workflow_engine_wake";

pub(crate) use repo::{PgTaskRepo, submit_in_tx};
pub(crate) use schema::migrate;
pub(crate) use wake::spawn_wake_listener;
