//! PostgreSQL 通用工作流实现：把 [`crate::TaskStore`] 落到 `tasks` 表。
//!
//! 本模块由 `pg` feature 门控（默认构建不含任何数据库依赖）。模块只依赖通用引擎
//! （[`crate::task`] / [`crate::graph`]）与**引擎自有 schema**（[`migrate`] 幂等执行）：
//!
//! - `tasks`：任务行（列见 [`rows::TaskRow`]），承载认领 / 扇出 / 原子结算 /
//!   级联 / JOIN 闸门所需的全部字段；
//! - `task_counters`：工作流级任务 id 自增计数表（引擎自有的 id 分配源）。
//!
//! 引擎表不持有指向业务表的 FK；消费方在自己的业务表之外执行 [`migrate`] 建立引擎表，
//! 再以 `ALTER TABLE` 补充业务完整性约束（如 FK 到自己的业务表）。
//!
//! 宿主应用监听 [`TASK_READY_CHANNEL`] 并按 payload 唤醒调度器，恢复时调用
//! [`PgTaskRepo::seal_budget_exhausted`] 和 [`PgTaskRepo::executable_workflow_ids`]。
//!
//! 子模块划分：
//! - [`repo`]：[`PgTaskRepo`]，通用 SQL 持久化操作（与业务查询解耦）；
//! - [`store`]：[`PgTaskStore`]，[`crate::TaskStore`] 的 pgsql 实现；
//! - [`crate::task::record`] / [`rows`]：数据库任务行与公共 [`crate::TaskRecord`]；
//! - [`schema`]：引擎自有 DDL 与迁移入口。

mod repo;
mod rows;
mod schema;
mod store;

#[cfg(test)]
mod test_support;

pub use repo::{PgTaskRepo, ROW_COLS, TASK_READY_CHANNEL, notify_tasks_ready};
pub use rows::TaskRow;
pub use schema::migrate;
pub use store::PgTaskStore;
