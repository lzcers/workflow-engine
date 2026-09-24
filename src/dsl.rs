//! 声明式图 DSL：通过 [`workflow!`] 宏把 `nodes` + `edges` 描述展开为 [`super::graph::Graph`]
//! 构建链，并做**编译期**静态校验。
//!
//! 节点名用标识符而非字符串，注册时从实例推导 [`crate::NodeKey`] 的类型。
//! 边通过类型化标识调用检查版构造函数（[`super::map_checked`] 等），编译期强制：
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
//!
//! **多源 JOIN 不用方括号语法糖**：JOIN 是业务节点，在 `nodes` 块显式注册
//! （如 `finalize_join => JoinNode`），再经普通边把各源汇入 JOIN，由 JOIN 经子句接下游。
//! 需要条件门的源用 `when(cond) |> |o| vec![json!({ src: o })]` 构造（条件不命中则 JOIN
//! 不会收到该源贡献，JOIN 门因此不会被创建）。JOIN 的输出类型为 `Value`，接下游的转换
//! 函数入参需写成 `&Value`，在函数内部反序列化为强类型视图。
//!
//! 上下文实例作为首个参数传入：`workflow!(context: ctx; nodes { ... } edges { ... })`。

/// 把 `nodes { name => instance }` 与 `edges { ... }` 展开为 `Graph::<Ctx>::builder(ctx)...build()`，
/// 返回 `Result<Graph<Ctx>, String>`。
///
/// - `nodes`：`name => instance` 注册构造完成的节点，每个表达式只求值一次。
/// - `edges`：`src -> dst map/on_error/when(cond) map <expr>`，表达式为建边纯函数；
///   边经类型检查版构造函数（[`super::map_checked`] 等）装配，编译期校验类型接线。
///
/// 省略类型声明仍会拒绝输入输出不匹配的透传边：
/// ```compile_fail
/// use workflow_engine::{Node, NodeError, workflow};
/// struct Number;
/// impl Node<()> for Number {
///     type In = i32;
///     type Out = i32;
///     async fn execute(&self, _: &mut (), input: i32) -> Result<i32, NodeError> { Ok(input) }
/// }
/// struct Text;
/// impl Node<()> for Text {
///     type In = String;
///     type Out = String;
///     async fn execute(&self, _: &mut (), input: String) -> Result<String, NodeError> { Ok(input) }
/// }
/// let _ = workflow!(context: (); nodes { a => Number, b => Text } edges { a -> b, });
/// ```
/// 转换闭包的返回值也必须匹配目标节点输入：
/// ```compile_fail
/// use workflow_engine::{Node, NodeError, workflow};
/// struct Number;
/// impl Node<()> for Number {
///     type In = i32;
///     type Out = i32;
///     async fn execute(&self, _: &mut (), input: i32) -> Result<i32, NodeError> { Ok(input) }
/// }
/// let _ = workflow!(
///     context: ();
///     nodes { a => Number, b => Number }
///     edges { a -> b |> |_| vec![String::from("wrong type")], }
/// );
/// ```
#[macro_export]
macro_rules! workflow {
    (
        context: $context:expr;
        nodes { $($node_name:ident => $instance:expr),* $(,)? }
        edges { $($edge:tt)* }
    ) => {{
        let __builder = $crate::Graph::builder($context);
        $(
            #[allow(unused_variables)]
            let (__builder, $node_name) = __builder.node_with_key(stringify!($node_name), $instance);
        )*
        $crate::dsl::workflow!(@edges __builder; $($edge)*)
    }};

    // 多源 AND 汇合不作为 DSL 语法糖：JOIN 是业务节点，在 `nodes` 块显式注册
    // （如 `finalize_join => JoinNode`），源经普通边汇入 JOIN。已移除的方括号语法
    // `[A, B, C] -> D` 不再支持。

    // 成功转换边：SRC -> DST |> fn（map_checked：转换入参 == SRC 的 Out，出参 == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge($crate::map_checked(
                $src, $dst, $transform
            ))
            ; $($rest)*
        )
    };

    // 失败边：SRC -> DST on_error fn（on_error_checked：入参 == SRC 的 In + 错误，出参 == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident on_error $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge($crate::on_error_checked(
                $src, $dst, $transform
            ))
            ; $($rest)*
        )
    };

    // 失败条件 + 转换边：SRC -> DST when_error(cond) |> fn（按错误/输入分支）
    (@edges $b:expr; $src:ident -> $dst:ident when_error($cond:expr) |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge(
                $crate::when_error_checked($src, $dst.name(), $cond)
                    .map_checked($dst, $transform)
            )
            ; $($rest)*
        )
    };

    // 失败条件 + 透传边：SRC -> DST when_error(cond)（编译期强制 SRC 的 In == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident when_error($cond:expr), $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.error_edge(
                $crate::when_error_checked($src, $dst.name(), $cond)
                    .passthrough_checked($dst)
            )
            ; $($rest)*
        )
    };

    // 条件 + 转换边：SRC -> DST when(cond) |> fn
    (@edges $b:expr; $src:ident -> $dst:ident when($cond:expr) |> $transform:expr, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge(
                $crate::when_checked($src, $dst.name(), $cond)
                    .map_checked($dst, $transform)
            )
            ; $($rest)*
        )
    };

    // 条件 + 透传边：SRC -> DST when(cond)（编译期强制 SRC 的 Out == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident when($cond:expr), $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge(
                $crate::when_checked($src, $dst.name(), $cond)
                    .passthrough_checked($dst)
            )
            ; $($rest)*
        )
    };

    // 成功透传边：SRC -> DST（无条件，编译期强制 SRC 的 Out == DST 的 In）
    (@edges $b:expr; $src:ident -> $dst:ident, $($rest:tt)*) => {
        $crate::dsl::workflow!(@edges
            $b.edge($crate::passthrough_checked(
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
mod tests {
    use crate::node::Node;
    use crate::{Graph, NodeError};
    use std::future::Future;

    struct Ctx;

    struct PassThrough;
    impl Node<Ctx> for PassThrough {
        type In = serde_json::Value;
        type Out = serde_json::Value;
        fn execute(
            &self,
            _ctx: &mut Ctx,
            input: Self::In,
        ) -> impl Future<Output = Result<Self::Out, crate::node::NodeError>> + Send {
            async move { Ok(input) }
        }
    }

    /// `on_error fn`（无条件）/ `when_error(cond) |> fn`（条件）失败边 DSL 语法：都能展开并规划。
    /// 节点名与边端点均为标识符（类型级 key），经 checked 构造函数做编译期类型接线校验。
    #[test]
    fn failure_edge_dsl_syntax() {
        let g: Graph<Ctx> = workflow!(
            context: Ctx;
            nodes {
                a => PassThrough,
                b => PassThrough,
                c => PassThrough,
            }
            edges {
                a -> b |> |_: &serde_json::Value| vec![serde_json::json!(1)],
                b -> a on_error |_: &serde_json::Value, _: &NodeError| vec![serde_json::json!(2)],
                b -> c when_error(|_: &serde_json::Value, err: &NodeError| err.is_retryable()) |>
                    |input: &serde_json::Value, _: &NodeError| vec![input.clone()],
            }
        )
        .unwrap();

        // 可重试错误：无条件 on_error 与条件 when_error 都命中 → 路由到 a、c
        let retry = g
            .plan_failure("b", &serde_json::json!(1), &NodeError::retryable("boom"))
            .unwrap();
        assert_eq!(retry.len(), 2);
        let mut dsts: Vec<_> = retry.iter().map(|s| s.dst).collect();
        dsts.sort();
        assert_eq!(dsts, vec!["a", "c"]);
        // 永久错误：when_error 条件不命中 → 只走无条件 on_error
        let skip = g
            .plan_failure("b", &serde_json::json!(1), &NodeError::permanent("boom"))
            .unwrap();
        assert_eq!(skip.len(), 1);
        assert_eq!(skip[0].dst, "a");
    }

    /// 裸成功透传边 `src -> dst`（无子句）：无条件把上游输出原样喂给下游，
    /// 编译期强制上游 `Out` == 下游 `In`（两侧均为 `Value` 时自然满足）。
    #[test]
    fn bare_success_passthrough() {
        let g: Graph<Ctx> = workflow!(
            context: Ctx;
            nodes {
                a => PassThrough,
                b => PassThrough,
            }
            edges {
                a -> b,
            }
        )
        .unwrap();

        let succ = g.plan_success("a", &serde_json::json!({"k": 1})).unwrap();
        assert_eq!(succ.len(), 1);
        assert_eq!(succ[0].dst, "b");
        assert_eq!(succ[0].inputs, vec![serde_json::json!({"k": 1})]);
    }

    #[tokio::test]
    async fn infers_instance_and_context_types_and_evaluates_each_expression_once() {
        struct OffsetContext(i32);
        struct OffsetNode(i32);
        impl Node<OffsetContext> for OffsetNode {
            type In = i32;
            type Out = i32;
            async fn execute(&self, ctx: &mut OffsetContext, input: i32) -> Result<i32, NodeError> {
                Ok(input + self.0 + ctx.0)
            }
        }
        let mut constructions = 0;
        // Neither node nor context implements Clone/Copy; expressions may move owned values.
        let destination = OffsetNode(4);
        let graph = workflow!(
            context: { constructions += 1; OffsetContext(3) };
            nodes {
                source => { constructions += 1; OffsetNode(2) },
                destination => destination,
            }
            edges {
                source -> destination when(|output| *output > 0) |> |output| vec![*output + 1],
                destination -> source when_error(|input, error| *input > 0 && error.is_retryable()),
            }
        )
        .unwrap();
        assert_eq!(constructions, 2);
        assert_eq!(graph.context().0, 3);
        let mut context = OffsetContext(graph.context().0);
        let output = graph
            .execute("source", &mut context, serde_json::json!(5))
            .await
            .unwrap();
        assert_eq!(output, serde_json::json!(10));
        let successors = graph.plan_success("source", &output).unwrap();
        assert_eq!(successors[0].inputs, vec![serde_json::json!(11)]);
        let failure = graph
            .plan_failure("destination", &output, &NodeError::retryable("retry"))
            .unwrap();
        assert_eq!(failure[0].inputs, vec![output]);
    }
}
