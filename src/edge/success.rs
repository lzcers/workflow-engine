//! 成功边在源节点成功后检查条件、转换输出并规划后继。

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{Arrow, EdgeError, EdgeMode, serialize_inputs};
use crate::node::{Node, NodeKey, TypeFingerprint};

type SuccessPlanner = Box<dyn Fn(&Value) -> Result<Vec<Value>, EdgeError> + Send + Sync>;

/// 成功边：构造期擦除具体载荷类型，保留路由模式与类型指纹供校验与描述。
pub struct SuccessEdge {
    src: &'static str,
    dst: &'static str,
    mode: EdgeMode,
    has_transform: bool,
    src_out_type: TypeFingerprint,
    dst_in_type: TypeFingerprint,
    planner: SuccessPlanner,
}

impl SuccessEdge {
    fn new<SrcOut, DstIn, F>(
        src: &'static str,
        dst: &'static str,
        mode: EdgeMode,
        has_transform: bool,
        plan: F,
    ) -> Self
    where
        SrcOut: DeserializeOwned + 'static,
        DstIn: Serialize + 'static,
        F: Fn(SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
    {
        let planner: SuccessPlanner = Box::new(move |out: &Value| {
            let out: SrcOut =
                serde_json::from_value(out.clone()).map_err(|e| EdgeError::BadSrcOut {
                    src,
                    detail: e.to_string(),
                })?;
            serialize_inputs(src, plan(out))
        });
        Self {
            src,
            dst,
            mode,
            has_transform,
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

    pub fn has_transform(&self) -> bool {
        self.has_transform
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

/// 条件命中时沿箭头分发成功节点的原始输出。
pub fn success<SrcOut, DstIn>(
    from: &'static str,
    to: &'static str,
    arrow: Arrow<SrcOut, DstIn>,
    condition: impl Fn(&SrcOut) -> bool + Send + Sync + 'static,
) -> SuccessEdge
where
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
{
    SuccessEdge::new(from, to, arrow.mode, false, move |out| {
        if condition(&out) {
            (arrow.inputs)(out)
        } else {
            Vec::new()
        }
    })
}

/// 条件命中时对整份成功输出执行一次转换，再沿箭头分发。
pub fn success_transform<SrcOut, Payload, DstIn>(
    from: &'static str,
    to: &'static str,
    arrow: Arrow<Payload, DstIn>,
    condition: impl Fn(&SrcOut) -> bool + Send + Sync + 'static,
    transform: impl Fn(&SrcOut) -> Payload + Send + Sync + 'static,
) -> SuccessEdge
where
    SrcOut: DeserializeOwned + 'static,
    Payload: 'static,
    DstIn: Serialize + 'static,
{
    SuccessEdge::new(from, to, arrow.mode, true, move |out| {
        if condition(&out) {
            (arrow.inputs)(transform(&out))
        } else {
            Vec::new()
        }
    })
}

pub fn success_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    arrow: Arrow<Src::Out, Dst::In>,
    condition: impl Fn(&Src::Out) -> bool + Send + Sync + 'static,
) -> SuccessEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    success(from.name(), to.name(), arrow, condition)
}

pub fn success_transform_checked<Src, Dst, Ctx, Payload: 'static>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    arrow: Arrow<Payload, Dst::In>,
    condition: impl Fn(&Src::Out) -> bool + Send + Sync + 'static,
    transform: impl Fn(&Src::Out) -> Payload + Send + Sync + 'static,
) -> SuccessEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    success_transform(from.name(), to.name(), arrow, condition, transform)
}
