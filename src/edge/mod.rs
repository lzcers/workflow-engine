//! 边模型：把 `src` 的成功输出或失败路由到 `dst`。
//!
//! 与 `Node`/`AnyNode` 完全对称的分层结构，但**成功边与失败边是两种独立类型**：
//! - 成功边（[`SuccessEdge`]，载体 [`EdgeImpl`]）：吃 `src` 的**输出**，条件命中后透传或
//!   经 `map` 扇出为 `dst` 输入，或经 `foreach` 逐项建立独立作用域，构成前向主 DAG。
//! - 失败边（[`FailureEdge`]，载体 [`FailureEdgeImpl`]）：吃失败节点的**输入 + 错误**，
//!   条件命中后透传或经 `map` 扇出为 `dst` 输入，构成后向回退图。
//!
//! 拆成两种类型而非「一个 trait + `is_success` 布尔」的伪 union：两者载荷（输出 vs
//! 输入+错误）、图方向（前向 vs 后向）、校验约束（无环 vs 不自环）、调度阶段
//! （`settle_success` vs `settle_failure`）都不同，本就是两条独立路由维度。
//! 每种 trait 只承载它需要的成员，不再有恒为 `None` 的占位方法。
//!
//! 普通成功边与失败边都支持 `when`（条件门）+ `map`（转换，可扇出，无 `map` 即透传）；
//! 逐项建立作用域的 `foreach` 只用于成功边。失败边**不约束为回退操作**，
//! 回退重试只是 `map` 的一种用法；也可做按错误类型的错误分支、兜底降级等一般路由。
//!
//! 子模块划分：
//! - [`success`]：成功边 trait 与擦除（[`SuccessEdge`]/[`AnySuccessEdge`]）。
//! - [`failure`]：失败边 trait 与擦除（[`FailureEdge`]/[`AnyFailureEdge`]）。
//! - [`builder`]：具体载体（[`EdgeImpl`]/[`FailureEdgeImpl`]/[`WhenBuilder`]/[`ErrorWhenBuilder`]）与命名建边函数。

mod builder;
mod failure;
mod success;

pub use builder::{
    EdgeImpl, ErrorWhenBuilder, FailureEdgeImpl, WhenBuilder, foreach, foreach_checked, map,
    map_checked, on_error, on_error_checked, passthrough_checked, when, when_checked, when_error,
    when_error_checked, when_error_map_checked, when_map_checked,
};
pub use failure::{AnyFailureEdge, FailureEdge};
pub use success::{AnySuccessEdge, SuccessEdge};

/// 边规划错误：确定性错误（反序列化/序列化类型不匹配），而非可重试的瞬时错误。
///
/// 规划失败意味着图装配/类型接线错误，重试无意义，应由调度层终止结算并告警。
#[derive(Debug, Clone, thiserror::Error)]
pub enum EdgeError {
    /// 成功边把 `src` 输出反序列化为 `SrcOut` 失败。
    #[error("edge {src}: bad src output: {detail}")]
    BadSrcOut { src: &'static str, detail: String },
    /// 失败边把失败节点输入反序列化为 `SrcIn` 失败。
    #[error("edge {src}: bad src input: {detail}")]
    BadSrcIn { src: &'static str, detail: String },
    /// 转换结果序列化为 `Value` 失败。
    #[error("edge {src}: serialize dst input: {detail}")]
    BadDstIn { src: &'static str, detail: String },
}
