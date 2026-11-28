//! 声明式图 DSL：通过 [`workflow!`] 宏把 `nodes` / `subgraph` / `group` / `edges` 展开为
//! [`super::graph::Graph`] 构建链，并在编译期完成强类型接线校验。
//!
//! 每条边的写法为 `源 箭头 目标[::槽位] [触发条件] [|> 转换函数]`：
//! - `a -> b`：单值直连（共享作用域）。
//! - `a ->! sub.b`：单值独立调用（为目标子图建立独立作用域）。
//! - `a ->* b`：展开 `Vec<T>`（共享作用域）。
//! - `a ->+ sub.b`：展开 `Vec<T>`，每项进入独立子图作用域。
//! - `a -> join::field`：指向强类型 JOIN 节点的字段槽位（必填/可选完全由 `#[derive(JoinInput)]` 字段定义决定）。
//! - `when(cond)` / `on_error` / `when_error(cond)` 筛选成功或失败路由。
//! - `|> f` 在条件命中时对整份载荷转换一次，再按箭头分发。
//! - `subgraph name { ... }` 声明运行期隔离作用域；`group name { ... }` 声明纯视觉分组（不写 `scopes` 表）。

#[doc(hidden)]
pub use crate::edge::{Edge, direct, isolated_direct, isolated_expand, shared_expand};

/// 声明编译期类型检查的工作流。
///
#[doc = include_str!("../tests/doctests/workflow.md")]
#[macro_export]
macro_rules! workflow {
    ($($tokens:tt)*) => {
        $crate::dsl::expand_workflow!($crate; $($tokens)*)
    };
}

pub use workflow;
#[doc(hidden)]
pub use workflow_engine_macros::expand_workflow;
