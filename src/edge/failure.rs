//! 失败边根据源节点的原始输入和错误规划后继。

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{Arrow, EdgeError, EdgeMode, serialize_inputs};
use crate::node::{Node, NodeError, NodeKey, TypeFingerprint};

type FailurePlanner =
    Box<dyn Fn(&Value, &NodeError) -> Result<Vec<Value>, EdgeError> + Send + Sync>;

/// 失败边：构造期擦除具体载荷类型，保留路由模式与类型指纹供校验与描述。
pub struct FailureEdge {
    src: &'static str,
    dst: &'static str,
    mode: EdgeMode,
    has_transform: bool,
    src_in_type: TypeFingerprint,
    dst_in_type: TypeFingerprint,
    planner: FailurePlanner,
}

impl FailureEdge {
    fn new<SrcIn, DstIn, F>(
        src: &'static str,
        dst: &'static str,
        mode: EdgeMode,
        has_transform: bool,
        plan: F,
    ) -> Self
    where
        SrcIn: DeserializeOwned + 'static,
        DstIn: Serialize + 'static,
        F: Fn(SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
    {
        let planner: FailurePlanner = Box::new(move |input: &Value, err: &NodeError| {
            let input: SrcIn =
                serde_json::from_value(input.clone()).map_err(|e| EdgeError::BadSrcIn {
                    src,
                    detail: e.to_string(),
                })?;
            serialize_inputs(src, plan(input, err))
        });
        Self {
            src,
            dst,
            mode,
            has_transform,
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

    pub fn has_transform(&self) -> bool {
        self.has_transform
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

/// 条件命中时沿箭头分发失败节点的原始输入。
pub fn failure<SrcIn, DstIn>(
    from: &'static str,
    to: &'static str,
    arrow: Arrow<SrcIn, DstIn>,
    condition: impl Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
) -> FailureEdge
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
{
    FailureEdge::new(from, to, arrow.mode, false, move |input, error| {
        if condition(&input, error) {
            (arrow.inputs)(input)
        } else {
            Vec::new()
        }
    })
}

/// 条件命中时用失败输入与错误执行一次转换，再沿箭头分发。
pub fn failure_transform<SrcIn, Payload, DstIn>(
    from: &'static str,
    to: &'static str,
    arrow: Arrow<Payload, DstIn>,
    condition: impl Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
    transform: impl Fn(&SrcIn, &NodeError) -> Payload + Send + Sync + 'static,
) -> FailureEdge
where
    SrcIn: DeserializeOwned + 'static,
    Payload: 'static,
    DstIn: Serialize + 'static,
{
    FailureEdge::new(from, to, arrow.mode, true, move |input, error| {
        if condition(&input, error) {
            (arrow.inputs)(transform(&input, error))
        } else {
            Vec::new()
        }
    })
}

pub fn failure_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    arrow: Arrow<Src::In, Dst::In>,
    condition: impl Fn(&Src::In, &NodeError) -> bool + Send + Sync + 'static,
) -> FailureEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    failure(from.name(), to.name(), arrow, condition)
}

pub fn failure_transform_checked<Src, Dst, Ctx, Payload: 'static>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    arrow: Arrow<Payload, Dst::In>,
    condition: impl Fn(&Src::In, &NodeError) -> bool + Send + Sync + 'static,
    transform: impl Fn(&Src::In, &NodeError) -> Payload + Send + Sync + 'static,
) -> FailureEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    failure_transform(from.name(), to.name(), arrow, condition, transform)
}
