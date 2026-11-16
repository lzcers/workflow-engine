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
//!
//! 子模块划分：
//! - [`repo`]：[`PgTaskRepo`]，通用 SQL 持久化操作（与业务查询解耦）；
//! - [`store`]：[`PgTaskStore`]，[`crate::TaskStore`] 的 pgsql 实现；
//! - [`listener`]：任务就绪监听与恢复扫描；
//! - [`record`] / [`rows`]：任务行与记录类型（实现 [`crate::Task`]）；
//! - [`schema`]：引擎自有 DDL 与迁移入口。

mod listener;
mod repo;
mod rows;
mod schema;
mod store;
mod task;

#[cfg(test)]
mod test_support;

pub use listener::run_task_ready_listener;
pub use repo::{
    FanoutInstances, PgError, PgResult, PgTaskRepo, ROW_COLS, TASK_READY_CHANNEL,
    notify_tasks_ready, status,
};
pub use rows::TaskRow;
pub use schema::migrate;
pub use store::{PgStoreError, PgTaskStore};
pub use task::{NewTask, PgTask};
