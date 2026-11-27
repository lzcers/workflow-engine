//! 边模型将成功节点的输出或失败节点的输入交给下游。
//! 条件命中后，先执行可选的数据转换，再按箭头分发。

mod failure;
mod success;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use failure::{
    FailureEdge, failure, failure_checked, failure_transform, failure_transform_checked,
};
pub use success::{
    SuccessEdge, success, success_checked, success_transform, success_transform_checked,
};

/// 边的路由与分发模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeMode {
    /// `->` 整份传递一个目标输入。
    Direct,
    /// `->*` 共享作用域展开列表。
    SharedExpand,
    /// `->+` 按列表元素建立独立子图作用域。
    IsolatedExpand,
}

/// 定义整份载荷如何分发给目标，类型参数分别为载荷类型和单份目标输入类型。
pub struct Arrow<Payload, DstIn> {
    mode: EdgeMode,
    inputs: fn(Payload) -> Vec<DstIn>,
}

/// `->` 整份传递载荷，包括空列表。
pub fn direct<T>() -> Arrow<T, T> {
    Arrow {
        mode: EdgeMode::Direct,
        inputs: |value| vec![value],
    }
}

/// `->*` 将列表展开为同一目标作用域中的多份输入。
pub fn shared_expand<T>() -> Arrow<Vec<T>, T> {
    Arrow {
        mode: EdgeMode::SharedExpand,
        inputs: std::convert::identity,
    }
}

/// `->+` 将列表逐项传入目标子图，每项建立独立作用域。
pub fn isolated_expand<T>() -> Arrow<Vec<T>, T> {
    Arrow {
        mode: EdgeMode::IsolatedExpand,
        inputs: std::convert::identity,
    }
}

fn serialize_inputs<DstIn: Serialize>(
    src: &'static str,
    inputs: impl IntoIterator<Item = DstIn>,
) -> Result<Vec<Value>, EdgeError> {
    inputs
        .into_iter()
        .map(|input| {
            serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                src,
                detail: e.to_string(),
            })
        })
        .collect()
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
