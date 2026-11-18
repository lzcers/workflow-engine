//! 通用工作流引擎：与业务解耦的节点 trait + 图引擎。
//!
//! - [`node`]：强类型 [`Node`] trait 与类型擦除（[`node::AnyNode`]）。
//! - [`edge`]：边模型（触发时机、守卫、扇出）与擦除辅助。
//! - [`graph`]：图引擎（[`Graph`]），负责节点注册、边规划、执行与静态校验。
//! - [`task`]：任务内容 [`Task`]、记录 [`TaskRecord`] 与持久化契约 [`TaskStore`]。
//! - [`scheduler`]：通用调度器（[`Scheduler`]），认领任务、结算并规划后继。
//! - `engine`（`pg` / `sqlite` feature）：可复用的引擎入口（`Engine`），初始化存储并创建工作流。
//! - `workflow`（`pg` / `sqlite` feature）：单图实例（`Workflow`），持有调度器，通过 `run` / `stop` 启停；提交任务返回 `RunHandle`。
//! - [`backend::pgsql`]（`pg` feature）：PostgreSQL 通用实现（`PgTaskRepo` / `PgTaskStore` /
//!   任务就绪通知与恢复查询）。默认关闭，引擎核心保持零数据库依赖。
//! - [`backend::sqlite`]（`sqlite` feature）：SQLite 通用实现（`SqliteTaskRepo` /
//!   `SqliteTaskStore`），支持原子结算、租约恢复与就绪工作流查询。
//!
//! worker 生命周期、通知监听与定期恢复扫描由宿主应用管理。
//!
//! 节点**不携带名字**：阶段名作为 [`Graph`] 的 `HashMap` key 在注册实例时外部绑定，
//! 同一节点实现可复用、可在不同名字下注册。

#[cfg(any(feature = "pg", feature = "sqlite", test))]
mod error;
#[cfg(any(feature = "pg", feature = "sqlite", test))]
pub use error::StorageError;

#[cfg(any(feature = "pg", feature = "sqlite"))]
pub mod database;
#[cfg(any(feature = "pg", feature = "sqlite"))]
pub use database::Database;

pub mod backend;
pub mod dsl;
pub mod edge;
#[cfg(any(feature = "pg", feature = "sqlite"))]
pub mod engine;
pub mod graph;
pub mod node;
pub mod scheduler;
pub mod task;
#[cfg(any(feature = "pg", feature = "sqlite"))]
pub mod workflow;

#[cfg(any(feature = "pg", feature = "sqlite"))]
pub use engine::{Engine, EngineError};
pub use graph::Graph;
pub use node::{JoinNode, Node, NodeContext, NodeError};
pub use scheduler::Scheduler;
pub use task::{Task, TaskRecord, TaskStatus, TaskStore};
#[cfg(any(feature = "pg", feature = "sqlite"))]
pub use workflow::{RunHandle, Workflow};
