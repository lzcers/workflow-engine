//! 声明式图 DSL：通过 [`workflow!`] 宏把 `nodes` + `edges` 描述展开为 [`super::graph::Graph`]
//! 构建链，并做**编译期**静态校验。
//!
//! 节点名用标识符而非字符串，注册时从实例推导 [`crate::graph::NodeKey`] 的类型。
//! 边通过类型化标识调用检查版构造函数（[`crate::dsl::map_checked`] 等），编译期强制：
//! - 转换函数入参 == 上游节点 `Out`、出参 == 下游节点 `In`（失败边为上游 `In` + 错误 → 下游 `In`）；
//! - `when(cond)` / `when_error(cond)` 透传边强制上游输出/输入类型 == 下游输入类型；
//! - 未在 `nodes` 块注册的节点名是编译错误。
//! 类型错线此前要到运行时才以 serde 错误暴露，现在在编译期即报错并指向对应边。
//! 检查由**期望签名**驱动：`map`/`when` 的闭包参数与返回类型由 `F: Fn(&SrcOut) -> ...`
//! 约束反推，无需显式标注——`when(|o| o.image_enabled)` 中 `o` 自动为 `&CommitOutput`。
//!
//! 边语法（`SRC -> DST` 后跟一条子句（可省略），每条以逗号结尾）：
//! - `|> fn`：无条件成功转换边。
//! - `foreach`：把源输出 Vec 逐项传给目标子图，每项建立独立作用域。
//! - `src -> dst`（裸边，无子句）：无条件成功透传边（编译期强制 SRC 的 `Out` == DST 的 `In`）。
//! - `on_error fn`：无条件失败边（失败输入 + 错误 → `to` 输入，不约束为回退）。
//! - `when(cond) |> fn`：成功条件边 + 转换。
//! - `when(cond)`：成功条件边 + 透传（编译期强制 SRC 的 `Out` == DST 的 `In`）。
//! - `when_error(cond) |> fn` / `when_error(cond)`：失败条件边 + 转换 / 透传（按错误分支）。
//!
//! `nodes` 直接接收实例表达式：`name => Type::new(...)` 或 `name => instance`。
//! 上下文同样直接传入实例，节点类型和上下文类型都由编译器推导。
//! 节点前可写 `#[title = "标题"]`、`#[description = "说明"]`，属性各自可选、顺序不限。
//! 值接受可转换为 `String` 的表达式；省略时标题为节点名，说明为空。
//!
//! **多源 JOIN 不用方括号语法糖**：JOIN 是业务节点，在 `nodes` 块显式注册
//! （如 `finalize_join => JoinNode`），再经普通边把各源汇入 JOIN，由 JOIN 经子句接下游。
//! 需要条件门的源用 `when(cond) |> |o| vec![json!({ src: o })]` 构造（条件不命中则 JOIN
//! 不会收到该源贡献，JOIN 门因此不会被创建）。JOIN 的输出类型为 `Value`，接下游的转换
//! 函数入参需写成 `&Value`，在函数内部反序列化为强类型视图。
//!
//! 子图使用 `subgraph name { nodes { ... } subgraph child { ... } edges { ... } }` 嵌套定义。
//! 支持交错声明 nodes 块与 subgraph，并保留构造表达式求值顺序。外部可使用 `analysis.daily` 直接连接内部节点。
//!
//! 图必须声明 `workflow_id` 和 `title`，随后传入 `context` 实例、节点与边。

// 宏在调用方展开，因此接线函数必须可访问；它们属于 DSL 的实现入口。
#[doc(hidden)]
pub use crate::edge::{
    foreach_checked, map_checked, on_error_checked, passthrough_checked, when_checked,
    when_error_checked,
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
