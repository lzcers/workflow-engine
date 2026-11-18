//! 声明式图 DSL：通过 [`workflow!`] 宏把 `nodes` + `edges` 描述展开为 [`super::graph::Graph`]
//! 构建链，并做**编译期**静态校验。
//!
//! 节点名用标识符而非字符串，注册时从实例推导 [`crate::node::NodeKey`] 的类型。
//! 边通过类型化标识调用检查版构造函数（[`crate::edge::map_checked`] 等），编译期强制：
//! - 转换函数入参 == 上游节点 `Out`、出参 == 下游节点 `In`（失败边为上游 `In` + 错误 → 下游 `In`）；
//! - `when(cond)` / `when_error(cond)` 透传边强制上游输出/输入类型 == 下游输入类型；
//! - 未在 `nodes` 块注册的节点名是编译错误。
//! 类型错线此前要到运行时才以 serde 错误暴露，现在在编译期即报错并指向对应边。
//! 检查由**期望签名**驱动：`map`/`when` 的闭包参数与返回类型由 `F: Fn(&SrcOut) -> ...`
//! 约束反推，无需显式标注——`when(|o| o.image_enabled)` 中 `o` 自动为 `&CommitOutput`。
//!
//! 边语法（`SRC -> DST` 后跟一条子句（可省略），每条以逗号结尾）：
//! - `|> fn`：无条件成功转换边。
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
//! 图必须声明 `workflow_id` 和 `title`，随后传入 `context` 实例、节点与边。

/// 把工作流身份、`nodes { name => instance }` 与 `edges { ... }` 展开为
/// `Graph::<Ctx>::builder(workflow_id, title, ctx)...build()`，
/// 返回 `Result<Graph<Ctx>, String>`。
///
/// - `workflow_id` / `title`：必填的稳定 ID 与展示标题，接受可转换为 `String` 的表达式。
/// - `nodes`：`name => instance` 注册构造完成的节点，所有构建表达式只求值一次。
///   可用 `#[title = expr]` / `#[description = expr]` 附加图描述，各最多出现一次。
/// - `edges`：`src -> dst map/on_error/when(cond) map <expr>`，表达式为建边纯函数；
///   边经类型检查版构造函数（[`crate::edge::map_checked`] 等）装配，编译期校验类型接线。
///
#[doc = include_str!("../tests/doctests/workflow.md")]
#[macro_export]
macro_rules! workflow {
    (
        workflow_id: $workflow_id:expr;
        title: $title:expr;
        context: $context:expr;
        nodes { $($(#[$($metadata:tt)*])* $node_name:ident => $instance:expr),* $(,)? }
        edges { $($edge:tt)* }
    ) => {{
        let __builder = $crate::Graph::builder($workflow_id, $title, $context);
        $(
            #[allow(unused_variables)]
            let (__builder, $node_name) = $crate::dsl::workflow!(
                @node __builder; $node_name => $instance; $(#[$($metadata)*])*
            );
        )*
        $crate::dsl::workflow!(@edges __builder; $($edge)*)
    }};

    // 展示属性仅影响节点描述；所有分支都返回同一个类型化 NodeKey。
    (@node $b:expr; $name:ident => $instance:expr;) => {
        $b.node_with_key(stringify!($name), $instance)
    };
    (@node $b:expr; $name:ident => $instance:expr;
        #[title = $title:expr] #[description = $description:expr]
    ) => {
        $b.node_with_description_and_key(stringify!($name), $instance, $title, $description)
    };
    (@node $b:expr; $name:ident => $instance:expr;
        #[description = $description:expr] #[title = $title:expr]
    ) => {
        $crate::dsl::workflow!(@node $b; $name => $instance;
            #[title = $title] #[description = $description]
        )
    };
    (@node $b:expr; $name:ident => $instance:expr; #[title = $title:expr]) => {
        $b.node_with_description_and_key(stringify!($name), $instance, $title, "")
    };
    (@node $b:expr; $name:ident => $instance:expr; #[description = $description:expr]) => {
        $b.node_with_description_and_key(stringify!($name), $instance, stringify!($name), $description)
    };
    (@node $b:expr; $name:ident => $instance:expr; $($invalid:tt)+) => {
        compile_error!(concat!(
            "invalid metadata for node `", stringify!($name),
            "`: expected at most one #[title = expression] and one #[description = expression]"
        ))
    };

    // 多源 AND 汇合不作为 DSL 语法糖：JOIN 是业务节点，在 `nodes` 块显式注册
    // （如 `finalize_join => JoinNode`），源经普通边汇入 JOIN。已移除的方括号语法
    // `[A, B, C] -> D` 不再支持。

    // 成功转换边：SRC -> DST |> fn（map_checked：转换入参 == SRC 的 Out，出参 == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge($crate::edge::map_checked(
                $src, $dst, $transform
            ))
            ; $($rest)*
        )
    };

    // 失败边：SRC -> DST on_error fn（on_error_checked：入参 == SRC 的 In + 错误，出参 == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident on_error $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge($crate::edge::on_error_checked(
                $src, $dst, $transform
            ))
            ; $($rest)*
        )
    };

    // 失败条件 + 转换边：SRC -> DST when_error(cond) |> fn（按错误/输入分支）
    (@edges $b:expr; $src:ident -> $dst:ident when_error($cond:expr) |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge(
                $crate::edge::when_error_checked($src, $dst.name(), $cond)
                    .map_checked($dst, $transform)
            )
            ; $($rest)*
        )
    };

    // 失败条件 + 透传边：SRC -> DST when_error(cond)（编译期强制 SRC 的 In == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident when_error($cond:expr), $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge(
                $crate::edge::when_error_checked($src, $dst.name(), $cond)
                    .passthrough_checked($dst)
            )
            ; $($rest)*
        )
    };

    // 条件 + 转换边：SRC -> DST when(cond) |> fn
    (@edges $b:expr; $src:ident -> $dst:ident when($cond:expr) |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge(
                $crate::edge::when_checked($src, $dst.name(), $cond)
                    .map_checked($dst, $transform)
            )
            ; $($rest)*
        )
    };

    // 条件 + 透传边：SRC -> DST when(cond)（编译期强制 SRC 的 Out == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident when($cond:expr), $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge(
                $crate::edge::when_checked($src, $dst.name(), $cond)
                    .passthrough_checked($dst)
            )
            ; $($rest)*
        )
    };

    // 成功透传边：SRC -> DST（无条件，编译期强制 SRC 的 Out == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge($crate::edge::passthrough_checked(
                $src, $dst
            ))
            ; $($rest)*
        )
    };

    // 收尾：无剩余边，build
    (@edges $b:expr;) => {
        $b.build()
    };
}

// 在 `dsl` 模块下以 `pub use` 重新导出宏，使消费方 `use workflow_engine::dsl;` 后可用
// `dsl::workflow!`。其余宏仅被 `workflow!`、`dsl` 内部宏互引，本 crate 编译期内不被直接调用。
#[allow(unused_imports)]
pub use workflow;

#[cfg(test)]
#[path = "../tests/unit/dsl.rs"]
mod tests;
