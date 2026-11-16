//! 通用工作流引擎：与业务解耦的节点 trait + 图引擎。
//!
//! - [`node`]：强类型 [`Node`] trait 与类型擦除（[`AnyNode`]）。
//! - [`edge`]：边模型（触发时机、守卫、扇出）与擦除辅助。
//! - [`graph`]：图引擎（[`Graph`]），负责节点注册、边规划、执行与静态校验。
//! - [`task`]：任务抽象（[`Task`]/[`TaskStore`]），把调度与具体持久化解耦。
//! - [`scheduler`]：通用调度器（[`Scheduler`]），认领任务、结算并规划后继。
//! - [`pgsql_backend`]（`pg` feature）：PostgreSQL 通用实现（`PgTaskRepo` / `PgTaskStore` /
//!   任务就绪通知与恢复查询）。默认关闭，引擎核心保持零数据库依赖。
//! - [`sqlite_backend`]（`sqlite` feature）：SQLite 通用实现（`SqliteTaskRepo` /
//!   `SqliteTaskStore`），支持原子结算、租约恢复与就绪工作流查询。
//!
//! worker 生命周期、通知监听与定期恢复扫描由宿主应用管理。
//!
//! 节点**不携带名字**：阶段名作为 [`Graph`] 的 `HashMap` key 在注册实例时外部绑定，
//! 同一节点实现可复用、可在不同名字下注册。

pub mod dsl;
pub mod edge;
pub mod graph;
pub mod node;
#[cfg(any(feature = "pg", test))]
pub mod pgsql_backend;
pub mod scheduler;
#[cfg(any(feature = "sqlite", test))]
pub mod sqlite_backend;
pub mod task;

pub use edge::{
    AnyFailureEdge, AnySuccessEdge, EdgeError, ErrorWhenBuilder, FailureEdge, FailureEdgeImpl,
    SuccessEdge, WhenBuilder, map, map_checked, on_error, on_error_checked, passthrough_checked,
    when, when_checked, when_error, when_error_checked,
};
pub use graph::{EdgeDescription, EdgeKind, Graph, GraphDescription, NodeDescription};
pub use node::{AnyNode, JoinNode, Node, NodeContext, NodeError, NodeKey};
pub use scheduler::Scheduler;
pub use task::{FanoutPlan, Task, TaskError, TaskStore};
