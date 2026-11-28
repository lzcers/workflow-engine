//! 失败边的源端点、流式 Builder 与零拷贝 Planner 特化（与成功边完全对称）。

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
use crate::node::{EdgeSlot, JoinInput, JoinSlot, Node, NodeError, NodeKey, TypeFingerprint};

pub struct CheckedFailureFrom<Ctx, Src: Node<Ctx>> {
    src: Arc<str>,
    marker: PhantomData<fn(Ctx) -> Src>,
}

impl<Ctx, Src: Node<Ctx>> CheckedFailureFrom<Ctx, Src> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }

    pub fn to<Dst: Node<Ctx>>(
        self,
        dst: &NodeKey<Ctx, Dst>,
    ) -> FailureEdgeBuilder<Src::In, Src::In, Dst::In, Unconditional, Identity> {
        FailureEdgeBuilder::new(self.src, dst.arc_name(), None)
    }

    pub fn to_slot<Dst, SlotItem>(
        self,
        dst: &NodeKey<Ctx, Dst>,
        slot: JoinSlot<Dst::In, SlotItem>,
    ) -> FailureEdgeBuilder<Src::In, Src::In, SlotItem, Unconditional, Identity>
    where
        Dst: Node<Ctx>,
        Dst::In: JoinInput,
        SlotItem: Serialize + 'static,
    {
        FailureEdgeBuilder::new(self.src, dst.arc_name(), Some(slot.edge_slot()))
    }
}

pub struct FailureFrom<SrcIn> {
    src: Arc<str>,
    marker: PhantomData<fn() -> SrcIn>,
}

impl<SrcIn: DeserializeOwned + 'static> FailureFrom<SrcIn> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }

    pub fn to<DstIn: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
    ) -> FailureEdgeBuilder<SrcIn, SrcIn, DstIn, Unconditional, Identity> {
        FailureEdgeBuilder::new(self.src, dst.into(), None)
    }

    pub fn to_slot<JoinIn, SlotItem: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
        slot: JoinSlot<JoinIn, SlotItem>,
    ) -> FailureEdgeBuilder<SrcIn, SrcIn, SlotItem, Unconditional, Identity> {
        FailureEdgeBuilder::new(self.src, dst.into(), Some(slot.edge_slot()))
    }
}

pub struct FailureEdgeBuilder<SrcIn, Payload, DstIn, Cond, Trans> {
    src: Arc<str>,
    dst: Arc<str>,
    slot: Option<EdgeSlot>,
    cond: Cond,
    trans: Trans,
    marker: PhantomData<fn(SrcIn, Payload) -> DstIn>,
}

impl<SrcIn, DstIn> FailureEdgeBuilder<SrcIn, SrcIn, DstIn, Unconditional, Identity> {
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

impl<SrcIn, Payload, DstIn, Trans>
    FailureEdgeBuilder<SrcIn, Payload, DstIn, Unconditional, Trans>
{
    /// 附加失败触发条件，接收原始输入与错误。
    pub fn when<F>(self, cond: F) -> FailureEdgeBuilder<SrcIn, Payload, DstIn, Condition<F>, Trans>
    where
        F: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
    {
        FailureEdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: Condition(cond),
            trans: self.trans,
            marker: PhantomData,
        }
    }
}

impl<SrcIn, DstIn, Cond> FailureEdgeBuilder<SrcIn, SrcIn, DstIn, Cond, Identity> {
    /// 附加失败转换函数，接收原始输入与错误。
    pub fn map<NewPayload, F>(
        self,
        transform: F,
    ) -> FailureEdgeBuilder<SrcIn, NewPayload, DstIn, Cond, Transform<F>>
    where
        F: Fn(&SrcIn, &NodeError) -> NewPayload + Send + Sync + 'static,
        NewPayload: 'static,
    {
        FailureEdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: self.cond,
            trans: Transform(transform),
            marker: PhantomData,
        }
    }
}

impl<SrcIn, Payload, DstIn, Cond, Trans> FailureEdgeBuilder<SrcIn, Payload, DstIn, Cond, Trans>
where
    SrcIn: DeserializeOwned + 'static,
    Payload: 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildFailurePlanner<SrcIn, Payload, DstIn>,
{
    pub fn via(self, arrow: Arrow<Payload, DstIn>) -> Edge {
        let built = (self.cond, self.trans).build(self.src.clone(), arrow);
        Edge::from_built(
            EdgeKind::Failure,
            self.src,
            self.dst,
            self.slot,
            arrow.mode,
            TypeFingerprint::of::<SrcIn>(),
            TypeFingerprint::of::<DstIn>(),
            built,
        )
    }
}

impl<SrcIn, DstIn, Cond, Trans> FailureEdgeBuilder<SrcIn, DstIn, DstIn, Cond, Trans>
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildFailurePlanner<SrcIn, DstIn, DstIn>,
{
    pub fn direct(self) -> Edge {
        self.via(direct())
    }

    pub fn isolated(self) -> Edge {
        self.via(isolated_direct())
    }
}

impl<SrcIn, DstIn, Cond, Trans> FailureEdgeBuilder<SrcIn, Vec<DstIn>, DstIn, Cond, Trans>
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildFailurePlanner<SrcIn, Vec<DstIn>, DstIn>,
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
pub trait BuildFailurePlanner<SrcIn, Payload, DstIn> {
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner;
}

impl<SrcIn, DstIn> BuildFailurePlanner<SrcIn, SrcIn, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcIn, DstIn>) -> BuiltPlanner {
        BuiltPlanner::pass_through(arrow.mode)
    }
}

impl<SrcIn, DstIn, C> BuildFailurePlanner<SrcIn, SrcIn, DstIn> for (Condition<C>, Identity)
where
    SrcIn: DeserializeOwned + 'static,
    C: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<SrcIn, DstIn>) -> BuiltPlanner {
        let (Condition(cond), _) = self;
        let is_expand = arrow.mode.is_expand();
        BuiltPlanner::failure(true, false, move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed, err) {
                return Ok(Vec::new());
            }
            pass_through_value(&src, input, is_expand, EdgeKind::Failure)
        })
    }
}

impl<SrcIn, Payload: 'static, DstIn, T> BuildFailurePlanner<SrcIn, Payload, DstIn>
    for (Unconditional, Transform<T>)
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    T: Fn(&SrcIn, &NodeError) -> Payload + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner {
        let (_, Transform(trans)) = self;
        BuiltPlanner::failure(false, true, move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            serialize_inputs(&src, (arrow.inputs)(trans(&typed, err)))
        })
    }
}

impl<SrcIn, Payload: 'static, DstIn, C, T> BuildFailurePlanner<SrcIn, Payload, DstIn>
    for (Condition<C>, Transform<T>)
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    C: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
    T: Fn(&SrcIn, &NodeError) -> Payload + Send + Sync + 'static,
{
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner {
        let (Condition(cond), Transform(trans)) = self;
        BuiltPlanner::failure(true, true, move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed, err) {
                return Ok(Vec::new());
            }
            serialize_inputs(&src, (arrow.inputs)(trans(&typed, err)))
        })
    }
}
