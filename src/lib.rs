//! 通用工作流引擎：与业务解耦的节点 trait + 图引擎。
//!
//! - [`graph`]：图定义、节点接口、边构造与图描述，入口为 [`Graph`]。
//! - [`engine`]：[`Engine`] 初始化存储并创建工作流。
//! - [`workflow`](mod@workflow)：[`Workflow`] 提交运行并返回 [`RunHandle`]。
//! - [`task`]：任务内容 [`Task`]、记录 [`TaskRecord`] 与状态 [`TaskStatus`]。
//! - [`dsl`]：通过 [`workflow!`] 宏声明图，并在编译期校验节点接线。
//!
//! 数据库连接、持久化后端与调度器由引擎内部管理。
//! 默认使用 SQLite；通过 `pg` / `sqlite` feature 选择后端，至少启用一个。
//!
//! 节点不携带名字：节点名在注册实例时外部绑定，同一节点实现可在不同名字下复用。

#[cfg(not(any(feature = "pg", feature = "sqlite")))]
compile_error!("workflow-engine requires at least one database backend: enable `pg` or `sqlite`");

mod backend;
mod database;
mod edge;
mod error;
mod node;
mod scheduler;

pub mod dsl;
pub mod engine;
pub mod graph;
pub mod task;
pub mod workflow;

pub use database::{Database, DatabasePool};
pub use engine::{Engine, EngineError};
pub use graph::{Graph, JoinNode, Node, NodeContext, NodeError, Trigger};
pub use task::{Task, TaskRecord, TaskStatus};
pub use workflow::{RunHandle, Workflow};
