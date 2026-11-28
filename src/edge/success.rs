//! 成功边的源端点、流式 Builder 与零拷贝 Planner 特化。

use std::{marker::PhantomData, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{
    Edge, EdgeError, EdgeKind,
    arrow::{Arrow, direct, isolated_direct, isolated_expand, shared_expand},
    planner::{
        BuiltPlanner, Condition, Identity, Transform, Unconditional, pass_through_value,
        serialize_inputs,
    },
};
use crate::node::{EdgeSlot, JoinInput, JoinSlot, Node, NodeKey, TypeFingerprint};

pub struct CheckedSuccessFrom<Ctx, Src: Node<Ctx>> {
    src: Arc<str>,
    marker: PhantomData<fn(Ctx) -> Src>,
}

impl<Ctx, Src: Node<Ctx>> CheckedSuccessFrom<Ctx, Src> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }

    pub fn to<Dst: Node<Ctx>>(
        self,
        dst: &NodeKey<Ctx, Dst>,
    ) -> SuccessEdgeBuilder<Src::Out, Src::Out, Dst::In, Unconditional, Identity> {
        SuccessEdgeBuilder::new(self.src, dst.arc_name(), None)
    }

    pub fn to_slot<Dst, SlotItem>(
        self,
        dst: &NodeKey<Ctx, Dst>,
        slot: JoinSlot<Dst::In, SlotItem>,
    ) -> SuccessEdgeBuilder<Src::Out, Src::Out, SlotItem, Unconditional, Identity>
    where
        Dst: Node<Ctx>,
        Dst::In: JoinInput,
        SlotItem: Serialize + 'static,
    {
        SuccessEdgeBuilder::new(self.src, dst.arc_name(), Some(slot.edge_slot()))
    }
}

pub struct SuccessFrom<SrcOut> {
    src: Arc<str>,
    marker: PhantomData<fn() -> SrcOut>,
}

impl<SrcOut: DeserializeOwned + 'static> SuccessFrom<SrcOut> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }

    pub fn to<DstIn: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
    ) -> SuccessEdgeBuilder<SrcOut, SrcOut, DstIn, Unconditional, Identity> {
        SuccessEdgeBuilder::new(self.src, dst.into(), None)
    }

    pub fn to_slot<JoinIn, SlotItem: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
        slot: JoinSlot<JoinIn, SlotItem>,
    ) -> SuccessEdgeBuilder<SrcOut, SrcOut, SlotItem, Unconditional, Identity> {
        SuccessEdgeBuilder::new(self.src, dst.into(), Some(slot.edge_slot()))
    }
}

pub struct SuccessEdgeBuilder<SrcOut, Payload, DstIn, Cond, Trans> {
    src: Arc<str>,
    dst: Arc<str>,
    slot: Option<EdgeSlot>,
    cond: Cond,
    trans: Trans,
    marker: PhantomData<fn(SrcOut, Payload) -> DstIn>,
}

impl<SrcOut, DstIn> SuccessEdgeBuilder<SrcOut, SrcOut, DstIn, Unconditional, Identity> {
    fn new(src: Arc<str>, dst: Arc<str>, slot: Option<EdgeSlot>) -> Self {
        Self {
            src,
            dst,
            slot,
            cond: Unconditional,
            trans: Identity,
            marker: PhantomData,
        }
    }
}

impl<SrcOut, Payload, DstIn, Trans>
    SuccessEdgeBuilder<SrcOut, Payload, DstIn, Unconditional, Trans>
{
    /// 附加成功触发条件；自动标记 `has_condition = true`（DSL 与命令式完全一致）。
    pub fn when<F>(self, cond: F) -> SuccessEdgeBuilder<SrcOut, Payload, DstIn, Condition<F>, Trans>
    where
        F: Fn(&SrcOut) -> bool + Send + Sync + 'static,
    {
        SuccessEdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: Condition(cond),
            trans: self.trans,
            marker: PhantomData,
        }
    }
}

impl<SrcOut, DstIn, Cond> SuccessEdgeBuilder<SrcOut, SrcOut, DstIn, Cond, Identity> {
    /// 附加转换函数，在条件命中后对整份成功输出调用一次。
    pub fn map<NewPayload, F>(
        self,
        transform: F,
    ) -> SuccessEdgeBuilder<SrcOut, NewPayload, DstIn, Cond, Transform<F>>
    where
        F: Fn(&SrcOut) -> NewPayload + Send + Sync + 'static,
        NewPayload: 'static,
    {
        SuccessEdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: self.cond,
            trans: Transform(transform),
            marker: PhantomData,
        }
    }
}

impl<SrcOut, Payload, DstIn, Cond, Trans> SuccessEdgeBuilder<SrcOut, Payload, DstIn, Cond, Trans>
where
    SrcOut: DeserializeOwned + 'static,
    Payload: 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildSuccessPlanner<SrcOut, Payload, DstIn>,
{
    pub fn via(self, arrow: Arrow<Payload, DstIn>) -> Edge {
        let built = (self.cond, self.trans).build(self.src.clone(), arrow);
        Edge::from_built(
            EdgeKind::Success,
            self.src,
            self.dst,
            self.slot,
            arrow.mode,
            TypeFingerprint::of::<SrcOut>(),
            TypeFingerprint::of::<DstIn>(),
            built,
        )
    }
}

impl<SrcOut, DstIn, Cond, Trans> SuccessEdgeBuilder<SrcOut, DstIn, DstIn, Cond, Trans>
where
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildSuccessPlanner<SrcOut, DstIn, DstIn>,
{
    pub fn direct(self) -> Edge {
        self.via(direct())
    }

    pub fn isolated(self) -> Edge {
        self.via(isolated_direct())
    }
}

impl<SrcOut, DstIn, Cond, Trans> SuccessEdgeBuilder<SrcOut, Vec<DstIn>, DstIn, Cond, Trans>
where
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildSuccessPlanner<SrcOut, Vec<DstIn>, DstIn>,
{
    pub fn expand(self) -> Edge {
        self.via(shared_expand())
    }

    pub fn expand_isolated(self) -> Edge {
        self.via(isolated_expand())
    }
}

// ============================================================================
// 内部 Planner 特化：无转换时跳过重序列化，所有反序列化均借用 &Value 不克隆
// ============================================================================

#[doc(hidden)]
pub trait BuildSuccessPlanner<SrcOut, Payload, DstIn> {
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner;
}

impl<SrcOut, DstIn> BuildSuccessPlanner<SrcOut, SrcOut, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcOut, DstIn>) -> BuiltPlanner {
        BuiltPlanner::pass_through(arrow.mode)
    }
}

impl<SrcOut, DstIn, C> BuildSuccessPlanner<SrcOut, SrcOut, DstIn> for (Condition<C>, Identity)
where
    SrcOut: DeserializeOwned + 'static,
    C: Fn(&SrcOut) -> bool + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<SrcOut, DstIn>) -> BuiltPlanner {
        let (Condition(cond), _) = self;
        let is_expand = arrow.mode.is_expand();
        BuiltPlanner::success(true, false, move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed) {
                return Ok(Vec::new());
            }
            pass_through_value(&src, out, is_expand, EdgeKind::Success)
        })
    }
}

impl<SrcOut, Payload: 'static, DstIn, T> BuildSuccessPlanner<SrcOut, Payload, DstIn>
    for (Unconditional, Transform<T>)
where
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    T: Fn(&SrcOut) -> Payload + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner {
        let (_, Transform(trans)) = self;
        BuiltPlanner::success(false, true, move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            serialize_inputs(&src, (arrow.inputs)(trans(&typed)))
        })
    }
}

impl<SrcOut, Payload: 'static, DstIn, C, T> BuildSuccessPlanner<SrcOut, Payload, DstIn>
    for (Condition<C>, Transform<T>)
where
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    C: Fn(&SrcOut) -> bool + Send + Sync + 'static,
    T: Fn(&SrcOut) -> Payload + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner {
        let (Condition(cond), Transform(trans)) = self;
        BuiltPlanner::success(true, true, move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed) {
                return Ok(Vec::new());
            }
            serialize_inputs(&src, (arrow.inputs)(trans(&typed)))
        })
    }
}
