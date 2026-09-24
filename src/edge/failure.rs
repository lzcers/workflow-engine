//! 失败边：把 `src` 的失败（**输入 + 错误**）路由为 `dst` 输入，对称于成功边。
//!
//! 失败边构成后向回退图，提供强类型 [`FailureEdge`] 接口与对 `Value` 规划的擦除接口
//! [`AnyFailureEdge`]。构造经 [`super::on_error`]（无条件）/ [`super::when_error`]
//! （条件）完成。
//!
//! **不把失败边约束为「回退/重试」操作**：与成功边完全对称，失败边只是把失败载荷
//! （失败节点输入 + 错误）经 `when` 门 + `map` 转换路由到任意 `dst` 输入。回退重试只是
//! `map` 的一种用法（重建重试输入）；同样可做按错误类型的错误分支、兜底降级等一般路由。

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::super::node::{NodeError, TypeFingerprint};
use super::EdgeError;

/// 失败边强类型接口：`src` 失败时用其**输入 `SrcIn` + 错误**规划 `dst` 输入。
///
/// 对称于成功边 [`super::success::SuccessEdge`]：`when` 为失败条件（`None` 为无条件），
/// `map` 为失败转换（`None` 则透传失败输入）。`map` 返回空即不产出下游（预算耗尽/放弃）。
pub trait FailureEdge: Send + Sync {
    /// 失败节点输入类型。
    type SrcIn: Serialize + DeserializeOwned + 'static;
    /// 目标节点输入类型。
    type DstIn: Serialize + 'static;
    /// 源节点名。
    fn src(&self) -> &'static str;
    /// 目标节点名。
    fn dst(&self) -> &'static str;
    /// 失败条件（`None` 为无条件）。
    fn when(&self) -> Option<&(dyn Fn(&Self::SrcIn, &NodeError) -> bool + Send + Sync)>;
    /// 失败转换（`None` 则透传失败节点输入）。
    fn map(&self) -> Option<&(dyn Fn(&Self::SrcIn, &NodeError) -> Vec<Self::DstIn> + Send + Sync)>;
}

// ---- 擦除接口：对 `Value` 规划后继（纯函数，不触碰 `Ctx`，比 `AnyNode` 更简单）----

/// 擦除后的失败边接口。边以 [`super::super::graph::Graph`] 的 `Vec<Box<dyn AnyFailureEdge>>` 持有。
pub trait AnyFailureEdge: Send + Sync {
    /// 源节点名。
    fn src(&self) -> &'static str;
    /// 目标节点名。
    fn dst(&self) -> &'static str;
    /// 失败规划：条件命中后经 `map` 产出 `dst` 输入（已序列化为 `Value`），
    /// 对称于成功边的 [`super::success::AnySuccessEdge::plan_success`]。
    ///
    /// - `Ok(Vec)` 为空 = 正常剪枝（`when` 不命中 / `map` 返回空，**不产出任何下游实例**）。
    /// - `Err` = 确定性路由错误（输入类型不匹配 / 序列化失败），应终止结算。
    fn plan_failure(&self, input: &Value, err: &NodeError) -> Result<Vec<Value>, EdgeError>;

    /// 边声明的失败节点输入类型指纹，供 build 期与源节点 `In` 对齐。
    fn src_in_type(&self) -> TypeFingerprint;

    /// 边声明的目标输入类型指纹，供 build 期与目标节点 `In` 对齐。
    fn dst_in_type(&self) -> TypeFingerprint;
}

/// 任意 `E: FailureEdge` 一律擦除为 [`AnyFailureEdge`]。
impl<E> AnyFailureEdge for E
where
    E: FailureEdge,
{
    fn src(&self) -> &'static str {
        FailureEdge::src(self)
    }
    fn dst(&self) -> &'static str {
        FailureEdge::dst(self)
    }

    fn src_in_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<E::SrcIn>()
    }

    fn dst_in_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<E::DstIn>()
    }

    // 空输出语义对称于成功边：无 `map` 时透传把失败输入也作为一条输入（值可能为空，
    // 如 `""`），与「有 `map` 且返回空 `Vec`（不产出任何下游实例）」语义不同——前者是
    // 「有输入但值为空」，后者是「无输入」。剪枝只发生在后者与 `when` 不命中。
    fn plan_failure(&self, input: &Value, err: &NodeError) -> Result<Vec<Value>, EdgeError> {
        let input: E::SrcIn =
            serde_json::from_value(input.clone()).map_err(|e| EdgeError::BadSrcIn {
                src: self.src(),
                detail: e.to_string(),
            })?;
        if let Some(cond) = self.when() {
            if !cond(&input, err) {
                return Ok(Vec::new());
            }
        }
        match self.map() {
            Some(m) => m(&input, err)
                .into_iter()
                .map(|input| {
                    serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                        src: self.src(),
                        detail: e.to_string(),
                    })
                })
                .collect(),
            // 无 map 则透传失败节点输入（要求 `DstIn` 能从 `SrcIn` 序列化还原，即两者同型）。
            None => Ok(vec![serde_json::to_value(&input).map_err(|e| {
                EdgeError::BadDstIn {
                    src: self.src(),
                    detail: e.to_string(),
                }
            })?]),
        }
    }
}
