//! 声明式图 DSL：通过 [`workflow!`] 宏把 `nodes` + `edges` 描述展开为 [`super::graph::Graph`]
//! 构建链，并做**编译期**静态校验。
//!
//! 节点名用标识符而非字符串，注册时从实例推导 [`crate::graph::NodeKey`] 的类型。
//! 编译时按源节点、箭头和目标节点约束条件与转换函数的签名。
//! 成功边读取源节点输出，失败边读取源节点输入与错误。
//!
//! 每条边的写法为 `源 箭头 目标 [触发条件] [|> 转换函数]`。
//! - `a -> b` 整份传递。转换结果为 `Vec<T>` 时，下游接收一份列表。
//! - `a ->* b` 共享作用域展开。源输出或转换结果必须为 `Vec<B::In>`。
//! - `a ->+ sub.b` 独立展开。每个元素进入新的子图实例。
//! - `when(cond)` 筛选成功输出，`on_error` 匹配失败，`when_error(cond)` 筛选失败。
//! - `|> f` 在条件命中时对整份载荷转换一次，然后按箭头分发。
//!   成功转换接收 `&Src::Out`，失败转换接收 `&Src::In` 与 `&NodeError`。
//!
//! `nodes` 直接接收实例表达式：`name => Type::new(...)` 或 `name => instance`。
//! 上下文同样直接传入实例，节点类型和上下文类型都由编译器推导。
//! 节点前可写 `#[title = "标题"]`、`#[description = "说明"]`，属性各自可选、顺序不限。
//! 值接受可转换为 `String` 的表达式；省略时标题为节点名，说明为空。
//!
//! 多源 JOIN 通过 `nodes` 注册，例如 `finalize_join => JoinNode`。以普通边将各来源接入，
//! 可用 `a -> finalize_join when(cond) |> |out| json!({ "a": out })` 构造一份贡献。
//!
//! 子图使用 `subgraph name { nodes { ... } subgraph child { ... } edges { ... } }` 嵌套定义。
//! 支持交错声明 nodes 块与 subgraph，并保留构造表达式求值顺序。外部可使用 `analysis.daily` 直接连接内部节点。
//!
//! 图必须声明 `workflow_id` 和 `title`，随后传入 `context` 实例、节点与边。

// 宏在调用方展开，因此接线函数必须可访问；它们属于 DSL 的实现入口。
#[doc(hidden)]
pub use crate::edge::{
    direct, failure_checked, failure_transform_checked, isolated_expand, shared_expand,
    success_checked, success_transform_checked,
};

/// 声明类型检查的工作流。子图内包含节点、嵌套子图和边；跨边界使用完整点路径。
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
