//! 通用工作流引擎：强类型节点、对称流式边、槽位驱动 JOIN 与 PG/SQLite 双后端。
//!
//! - [`graph`]：图定义、节点接口、统一流式边构造、可复用子图与图描述，入口为 [`Graph`]。
//! - [`engine`]：[`Engine`] 初始化存储与共享唤醒中枢，创建工作流实例。
//! - [`workflow`](mod@workflow)：[`Workflow`] 提交运行、查询/取消/等待运行并返回 [`RunHandle`]。
//! - [`task`]：任务内容 [`Task`]、快照 [`TaskRecord`]、状态 [`TaskStatus`] 与运行快照 [`RunSnapshot`]。
//! - [`dsl`]：通过 [`workflow!`] 宏声明图，并在编译期完成强类型接线校验。

#[cfg(test)]
extern crate self as workflow_engine;

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

pub use database::{Database, DatabasePool, PoolOptions};
pub use engine::{Engine, EngineError, GraphError};
pub use graph::{
    Edge, Graph, JoinInput, JoinNode, Node, NodeContext, NodeError, RetryPolicy, Role as NodeRole,
    Trigger,
};
pub use task::{RunSnapshot, RunStatus, Task, TaskRecord, TaskStatus};
pub use workflow::{RunHandle, WorkerOptions, Workflow};
pub use workflow_engine_macros::JoinInput;
