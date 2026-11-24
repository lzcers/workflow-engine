//! PostgreSQL 通用工作流实现：把 [`crate::task::ExecutionStore`] 落到 `tasks` 表。
//!
//! 本模块由 `pg` feature 启用。模块只依赖通用引擎
//! （[`crate::task`] / [`crate::graph`]）与**引擎自有 schema**（[`migrate`] 幂等执行）：
//!
//! - `tasks`：任务行（列见 [`rows::TaskRow`]），承载认领 / 扇出 / 原子结算 /
//!   级联 / JOIN 闸门所需的全部字段；
//! - `scopes`：由数据库编号的子图执行实例，保存入口任务与父作用域。
//!
//! 引擎表不持有指向业务表的 FK；消费方在自己的业务表之外执行 [`migrate`] 建立引擎表，
//! 再以 `ALTER TABLE` 补充业务完整性约束（如 FK 到自己的业务表）。
//!
//! 每个 Workflow 的调度器轮询本图任务，接管到期认领；预算耗尽后重新认领仅用于
//! 最终失败结算，失败状态与后继入队或依赖失败传播在同一事务内完成。
//!
//! 子模块划分：
//! - [`repo`]：[`PgTaskRepo`]，通用 SQL 持久化操作（与业务查询解耦）；
//! - [`store`]：为 [`PgTaskRepo`] 实现 [`crate::task::ExecutionStore`]；
//! - [`crate::task::record`] / [`rows`]：数据库任务行与公共 [`crate::TaskRecord`]；
//! - [`schema`]：引擎自有 DDL 与迁移入口。

mod join;
mod repo;
mod rows;
mod schema;
mod scope;
mod store;

#[cfg(test)]
#[path = "../../../tests/unit/pgsql_support.rs"]
mod test_support;

pub use repo::PgTaskRepo;
pub use schema::migrate;
