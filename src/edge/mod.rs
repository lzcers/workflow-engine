//! 边模型：把 `src` 的成功输出或失败路由到 `dst`。
//!
//! **成功边（[`SuccessEdge`]）与失败边（[`FailureEdge`]）是两种独立的具体类型**：
//! - 成功边：吃 `src` 的**输出**，条件命中后透传、经 `map` 扇出为 `dst` 输入，或经 `foreach`
//!   逐项建立独立作用域，构成前向主 DAG。
//! - 失败边：吃失败节点的**输入 + 错误**，条件命中后透传或经 `map` 扇出为 `dst` 输入，
//!   构成后向回退图（不约束为回退，也可做按错误类型分流、降级兜底等一般路由）。
//!
//! 构造期即完成类型检查与规划闭包擦除，图中直接持有 `Vec<SuccessEdge>` / `Vec<FailureEdge>`，
//! 无多余中间 trait 或双重装箱。

mod builder;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::node::{NodeError, TypeFingerprint};

pub use builder::{
    ErrorWhenBuilder, WhenBuilder, foreach, foreach_checked, map, map_checked, on_error,
    on_error_checked, passthrough_checked, when, when_checked, when_error, when_error_checked,
    when_error_map_checked, when_map_checked,
};

type SuccessPlanner = Box<dyn Fn(&Value) -> Result<Vec<Value>, EdgeError> + Send + Sync>;
type FailurePlanner =
    Box<dyn Fn(&Value, &NodeError) -> Result<Vec<Value>, EdgeError> + Send + Sync>;

/// 边的路由与分发模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeMode {
    /// 原样透传源载荷（可带 `when` 条件门）。
    Passthrough,
    /// 经转换函数映射/扇出输入，共享作用域。
    Map,
    /// 逐项拆分并为每个元素建立独立子图作用域（仅成功边）。
    Foreach,
}

/// 成功边：构造期擦除具体载荷类型，保留路由模式与类型指纹供校验与描述。
pub struct SuccessEdge {
    src: &'static str,
    dst: &'static str,
    mode: EdgeMode,
    src_out_type: TypeFingerprint,
    dst_in_type: TypeFingerprint,
    planner: SuccessPlanner,
}

impl SuccessEdge {
    pub(crate) fn new<SrcOut, DstIn>(
        src: &'static str,
        dst: &'static str,
        mode: EdgeMode,
        when: Option<Box<dyn Fn(&SrcOut) -> bool + Send + Sync>>,
        map: Option<Box<dyn Fn(&SrcOut) -> Vec<DstIn> + Send + Sync>>,
    ) -> Self
    where
        SrcOut: Serialize + DeserializeOwned + 'static,
        DstIn: Serialize + 'static,
    {
        let planner: SuccessPlanner = Box::new(move |out: &Value| {
            let out: SrcOut =
                serde_json::from_value(out.clone()).map_err(|e| EdgeError::BadSrcOut {
                    src,
                    detail: e.to_string(),
                })?;
            if let Some(cond) = &when
                && !cond(&out)
            {
                return Ok(Vec::new());
            }
            match &map {
                Some(transform) => transform(&out)
                    .into_iter()
                    .map(|input| {
                        serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                            src,
                            detail: e.to_string(),
                        })
                    })
                    .collect(),
                None => Ok(vec![serde_json::to_value(&out).map_err(|e| {
                    EdgeError::BadDstIn {
                        src,
                        detail: e.to_string(),
                    }
                })?]),
            }
        });
        Self {
            src,
            dst,
            mode,
            src_out_type: TypeFingerprint::of::<SrcOut>(),
            dst_in_type: TypeFingerprint::of::<DstIn>(),
            planner,
        }
    }

    pub fn src(&self) -> &'static str {
        self.src
    }

    pub fn dst(&self) -> &'static str {
        self.dst
    }

    pub fn mode(&self) -> EdgeMode {
        self.mode
    }

    pub fn plan_success(&self, out: &Value) -> Result<Vec<Value>, EdgeError> {
        (self.planner)(out)
    }

    pub(crate) fn src_out_type(&self) -> TypeFingerprint {
        self.src_out_type
    }

    pub(crate) fn dst_in_type(&self) -> TypeFingerprint {
        self.dst_in_type
    }
}

/// 失败边：构造期擦除具体载荷类型，保留路由模式与类型指纹供校验与描述。
pub struct FailureEdge {
    src: &'static str,
    dst: &'static str,
    mode: EdgeMode,
    src_in_type: TypeFingerprint,
    dst_in_type: TypeFingerprint,
    planner: FailurePlanner,
}

impl FailureEdge {
    pub(crate) fn new<SrcIn, DstIn>(
        src: &'static str,
        dst: &'static str,
        when: Option<Box<dyn Fn(&SrcIn, &NodeError) -> bool + Send + Sync>>,
        map: Option<Box<dyn Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync>>,
    ) -> Self
    where
        SrcIn: Serialize + DeserializeOwned + 'static,
        DstIn: Serialize + 'static,
    {
        let mode = if map.is_some() {
            EdgeMode::Map
        } else {
            EdgeMode::Passthrough
        };
        let planner: FailurePlanner = Box::new(move |input: &Value, err: &NodeError| {
            let input: SrcIn =
                serde_json::from_value(input.clone()).map_err(|e| EdgeError::BadSrcIn {
                    src,
                    detail: e.to_string(),
                })?;
            if let Some(cond) = &when
                && !cond(&input, err)
            {
                return Ok(Vec::new());
            }
            match &map {
                Some(transform) => transform(&input, err)
                    .into_iter()
                    .map(|input| {
                        serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                            src,
                            detail: e.to_string(),
                        })
                    })
                    .collect(),
                None => Ok(vec![serde_json::to_value(&input).map_err(|e| {
                    EdgeError::BadDstIn {
                        src,
                        detail: e.to_string(),
                    }
                })?]),
            }
        });
        Self {
            src,
            dst,
            mode,
            src_in_type: TypeFingerprint::of::<SrcIn>(),
            dst_in_type: TypeFingerprint::of::<DstIn>(),
            planner,
        }
    }

    pub fn src(&self) -> &'static str {
        self.src
    }

    pub fn dst(&self) -> &'static str {
        self.dst
    }

    pub fn mode(&self) -> EdgeMode {
        self.mode
    }

    pub fn plan_failure(&self, input: &Value, err: &NodeError) -> Result<Vec<Value>, EdgeError> {
        (self.planner)(input, err)
    }

    pub(crate) fn src_in_type(&self) -> TypeFingerprint {
        self.src_in_type
    }

    pub(crate) fn dst_in_type(&self) -> TypeFingerprint {
        self.dst_in_type
    }
}

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
