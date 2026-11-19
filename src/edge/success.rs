//! 成功边：把 `src` 的成功输出 `SrcOut` 路由为 `dst` 输入 `DstIn`。
//!
//! 成功边是前向主 DAG 的构成单元，提供强类型 [`SuccessEdge`] 接口与对 `Value` 规划的
//! 擦除接口 [`AnySuccessEdge`]。构造经 [`super::when`] / [`super::map`] / [`super::foreach`] 完成。

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::super::node::TypeFingerprint;
use super::EdgeError;

/// 成功边强类型接口：`src` 输出 `SrcOut` → `dst` 输入 `DstIn`。
///
/// 对称于 [`super::super::node::Node`]。构造经 [`super::when`] / [`super::map`] 完成。
pub trait SuccessEdge: Send + Sync {
    /// 源节点输出类型。
    type SrcOut: Serialize + DeserializeOwned + 'static;
    /// 目标节点输入类型。
    type DstIn: Serialize + 'static;
    /// 源节点名。
    fn src(&self) -> &'static str;
    /// 目标节点名。
    fn dst(&self) -> &'static str;
    /// 每条输入是否为目标任务建立独立作用域；普通转换和透传边继承上游作用域。
    fn creates_scope(&self) -> bool {
        false
    }
    /// 成功条件（`None` 为无条件）。
    fn when(&self) -> Option<&(dyn Fn(&Self::SrcOut) -> bool + Send + Sync)>;
    /// 成功转换（`None` 则透传 `src` 输出）。
    /// 存着多任务扇出的场景，所以 -> Vec<Self::DstIn> ，比如 gen_image_prompts 生成多条
    /// 提示词，要为每条提示词创建下游任务。
    fn map(&self) -> Option<&(dyn Fn(&Self::SrcOut) -> Vec<Self::DstIn> + Send + Sync)>;
}

// ---- 擦除接口：对 `Value` 规划后继（纯函数，不触碰 `Ctx`，比 `AnyNode` 更简单）----

/// 擦除后的成功边接口。边以 [`super::super::graph::Graph`] 的 `Vec<Box<dyn AnySuccessEdge>>` 持有。
pub trait AnySuccessEdge: Send + Sync {
    /// 源节点名。
    fn src(&self) -> &'static str;
    /// 目标节点名。
    fn dst(&self) -> &'static str;
    /// 每条输入是否为目标任务建立独立作用域。
    fn creates_scope(&self) -> bool;
    /// 成功规划：条件命中经 `map` 产出 `dst` 输入（已序列化为 `Value`）。
    ///
    /// - `Ok(Vec)` 为空 = 正常剪枝（`when` 不命中 / `map` 返回空，**不产出任何下游实例**）。
    /// - `Err` = 确定性路由错误（输出类型不匹配 / 序列化失败），应终止结算。
    fn plan_success(&self, out: &Value) -> Result<Vec<Value>, EdgeError>;

    /// 边声明的源输出类型指纹，供 build 期与源节点 `Out` 对齐（命令式路径无编译期检查）。
    fn src_out_type(&self) -> TypeFingerprint;

    /// 边声明的目标输入类型指纹，供 build 期与目标节点 `In` 对齐。
    fn dst_in_type(&self) -> TypeFingerprint;
}

/// 任意 `E: SuccessEdge` 一律擦除为 [`AnySuccessEdge`]。
impl<E> AnySuccessEdge for E
where
    E: SuccessEdge,
{
    fn src(&self) -> &'static str {
        SuccessEdge::src(self)
    }
    fn dst(&self) -> &'static str {
        SuccessEdge::dst(self)
    }
    fn creates_scope(&self) -> bool {
        SuccessEdge::creates_scope(self)
    }

    fn src_out_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<E::SrcOut>()
    }

    fn dst_in_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<E::DstIn>()
    }

    // 空输出语义：无 `map` 时透传把空输出也作为一条输入（值可能为空，如 `""`），
    // 与「有 `map` 且返回空 `Vec`（不产出任何下游实例）」语义不同——前者是「有输入但
    // 值为空」，后者是「无输入」。剪枝只发生在后者与 `when` 不命中。
    fn plan_success(&self, out: &Value) -> Result<Vec<Value>, EdgeError> {
        let out: E::SrcOut =
            serde_json::from_value(out.clone()).map_err(|e| EdgeError::BadSrcOut {
                src: self.src(),
                detail: e.to_string(),
            })?;
        if let Some(cond) = self.when() {
            if !cond(&out) {
                return Ok(Vec::new());
            }
        }
        match self.map() {
            Some(m) => m(&out)
                .into_iter()
                .map(|input| {
                    serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                        src: self.src(),
                        detail: e.to_string(),
                    })
                })
                .collect(),
            // 无 map 则透传 `src` 输出（要求 `DstIn` 能从 `SrcOut` 序列化还原，即两者同型）。
            None => Ok(vec![serde_json::to_value(&out).map_err(|e| {
                EdgeError::BadDstIn {
                    src: self.src(),
                    detail: e.to_string(),
                }
            })?]),
        }
    }
}
