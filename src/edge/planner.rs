//! 类型状态标记、擦除后的边规划器与零拷贝热路径辅助。

use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use super::{EdgeError, EdgeKind, arrow::EdgeMode};
use crate::node::NodeError;

// Builder 类型状态标记
pub struct Unconditional;
pub struct Condition<F>(pub(super) F);
pub struct Identity;
pub struct Transform<F>(pub(super) F);

type SuccessPlannerFn = Box<dyn Fn(&Value) -> Result<Vec<Value>, EdgeError> + Send + Sync>;
type FailurePlannerFn =
    Box<dyn Fn(&Value, &NodeError) -> Result<Vec<Value>, EdgeError> + Send + Sync>;

/// 内部规划器包装（对外部不透明）。
#[doc(hidden)]
pub struct BuiltPlanner {
    pub(super) has_condition: bool,
    pub(super) has_transform: bool,
    pub(super) planner: EdgePlanner,
}

impl BuiltPlanner {
    pub(super) fn pass_through(mode: EdgeMode) -> Self {
        let planner = if mode.is_expand() {
            EdgePlanner::ExpandPassThrough
        } else {
            EdgePlanner::DirectPassThrough
        };
        Self {
            has_condition: false,
            has_transform: false,
            planner,
        }
    }

    pub(super) fn success(
        has_condition: bool,
        has_transform: bool,
        f: impl Fn(&Value) -> Result<Vec<Value>, EdgeError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            has_condition,
            has_transform,
            planner: EdgePlanner::Success(Box::new(f)),
        }
    }

    pub(super) fn failure(
        has_condition: bool,
        has_transform: bool,
        f: impl Fn(&Value, &NodeError) -> Result<Vec<Value>, EdgeError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            has_condition,
            has_transform,
            planner: EdgePlanner::Failure(Box::new(f)),
        }
    }
}

pub(super) enum EdgePlanner {
    /// 无条件且无转换的单值直连：直接透传，零 serde 往返。
    DirectPassThrough,
    /// 无条件且无转换的列表展开：直接解包 JSON 数组，零元素 serde 往返。
    ExpandPassThrough,
    Success(SuccessPlannerFn),
    Failure(FailurePlannerFn),
}

impl EdgePlanner {
    pub(super) fn plan_success(&self, src: &Arc<str>, out: &Value) -> Result<Vec<Value>, EdgeError> {
        match self {
            Self::DirectPassThrough => Ok(vec![out.clone()]),
            Self::ExpandPassThrough => pass_through_value(src, out, true, EdgeKind::Success),
            Self::Success(f) => f(out),
            Self::Failure(_) => Ok(Vec::new()),
        }
    }

    pub(super) fn plan_failure(
        &self,
        src: &Arc<str>,
        input: &Value,
        err: &NodeError,
    ) -> Result<Vec<Value>, EdgeError> {
        match self {
            Self::DirectPassThrough => Ok(vec![input.clone()]),
            Self::ExpandPassThrough => pass_through_value(src, input, true, EdgeKind::Failure),
            Self::Failure(f) => f(input, err),
            Self::Success(_) => Ok(Vec::new()),
        }
    }
}

pub(super) fn pass_through_value(
    src: &Arc<str>,
    value: &Value,
    is_expand: bool,
    kind: EdgeKind,
) -> Result<Vec<Value>, EdgeError> {
    if !is_expand {
        return Ok(vec![value.clone()]);
    }
    match value {
        Value::Array(items) => Ok(items.clone()),
        _ => Err(match kind {
            EdgeKind::Success => EdgeError::BadSrcOut {
                src: src.clone(),
                detail: "expected JSON array for expand arrow".into(),
            },
            EdgeKind::Failure => EdgeError::BadSrcIn {
                src: src.clone(),
                detail: "expected JSON array for expand arrow".into(),
            },
        }),
    }
}

pub(super) fn serialize_inputs<DstIn: Serialize>(
    src: &Arc<str>,
    inputs: impl IntoIterator<Item = DstIn>,
) -> Result<Vec<Value>, EdgeError> {
    inputs
        .into_iter()
        .map(|input| {
            serde_json::to_value(input).map_err(|e| EdgeError::BadDstIn {
                src: src.clone(),
                detail: e.to_string(),
            })
        })
        .collect()
}
