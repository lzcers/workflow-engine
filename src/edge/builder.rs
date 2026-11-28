//! 统一的流式边构造器与零拷贝 Planner 特化（成功边 / 失败边共享同一状态机）。

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

/// 成功边路由类型标记。
pub struct Success;
/// 失败边路由类型标记。
pub struct Failure;

// ============================================================================
// 源端点构造器（Checked / Dynamic × Success / Failure）
// ============================================================================

pub struct CheckedFrom<Kind, Ctx, Src: Node<Ctx>> {
    src: Arc<str>,
    marker: PhantomData<fn(Kind, Ctx) -> Src>,
}

impl<Kind, Ctx, Src: Node<Ctx>> CheckedFrom<Kind, Ctx, Src> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }
}

impl<Ctx, Src: Node<Ctx>> CheckedFrom<Success, Ctx, Src> {
    pub fn to<Dst: Node<Ctx>>(
        self,
        dst: &NodeKey<Ctx, Dst>,
    ) -> EdgeBuilder<Success, Src::Out, Src::Out, Dst::In, Unconditional, Identity> {
        EdgeBuilder::new(self.src, dst.arc_name(), None)
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_slot<Dst, SlotItem>(
        self,
        dst: &NodeKey<Ctx, Dst>,
        slot: JoinSlot<Dst::In, SlotItem>,
    ) -> EdgeBuilder<Success, Src::Out, Src::Out, SlotItem, Unconditional, Identity>
    where
        Dst: Node<Ctx>,
        Dst::In: JoinInput,
        SlotItem: Serialize + 'static,
    {
        EdgeBuilder::new(self.src, dst.arc_name(), Some(slot.edge_slot()))
    }
}

impl<Ctx, Src: Node<Ctx>> CheckedFrom<Failure, Ctx, Src> {
    pub fn to<Dst: Node<Ctx>>(
        self,
        dst: &NodeKey<Ctx, Dst>,
    ) -> EdgeBuilder<Failure, Src::In, Src::In, Dst::In, Unconditional, Identity> {
        EdgeBuilder::new(self.src, dst.arc_name(), None)
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_slot<Dst, SlotItem>(
        self,
        dst: &NodeKey<Ctx, Dst>,
        slot: JoinSlot<Dst::In, SlotItem>,
    ) -> EdgeBuilder<Failure, Src::In, Src::In, SlotItem, Unconditional, Identity>
    where
        Dst: Node<Ctx>,
        Dst::In: JoinInput,
        SlotItem: Serialize + 'static,
    {
        EdgeBuilder::new(self.src, dst.arc_name(), Some(slot.edge_slot()))
    }
}

pub struct EdgeFrom<Kind, Src> {
    src: Arc<str>,
    marker: PhantomData<fn(Kind) -> Src>,
}

impl<Kind, Src: DeserializeOwned + 'static> EdgeFrom<Kind, Src> {
    pub(super) fn new(src: Arc<str>) -> Self {
        Self {
            src,
            marker: PhantomData,
        }
    }

    pub fn to<DstIn: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
    ) -> EdgeBuilder<Kind, Src, Src, DstIn, Unconditional, Identity> {
        EdgeBuilder::new(self.src, dst.into(), None)
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_slot<JoinIn, SlotItem: Serialize + 'static>(
        self,
        dst: impl Into<Arc<str>>,
        slot: JoinSlot<JoinIn, SlotItem>,
    ) -> EdgeBuilder<Kind, Src, Src, SlotItem, Unconditional, Identity> {
        EdgeBuilder::new(self.src, dst.into(), Some(slot.edge_slot()))
    }
}

// ============================================================================
// 统一流式 EdgeBuilder：共享分发终点，按 Success / Failure 特化 when 与 map
// ============================================================================

pub struct EdgeBuilder<Kind, Src, Payload, DstIn, Cond, Trans> {
    src: Arc<str>,
    dst: Arc<str>,
    slot: Option<EdgeSlot>,
    cond: Cond,
    trans: Trans,
    marker: PhantomData<fn(Kind, Src, Payload) -> DstIn>,
}

impl<Kind, Src, DstIn> EdgeBuilder<Kind, Src, Src, DstIn, Unconditional, Identity> {
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
    EdgeBuilder<Success, SrcOut, Payload, DstIn, Unconditional, Trans>
{
    /// 附加成功触发条件；自动标记 `has_condition = true`（DSL 与命令式完全一致）。
    pub fn when<F>(
        self,
        cond: F,
    ) -> EdgeBuilder<Success, SrcOut, Payload, DstIn, Condition<F>, Trans>
    where
        F: Fn(&SrcOut) -> bool + Send + Sync + 'static,
    {
        EdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: Condition(cond),
            trans: self.trans,
            marker: PhantomData,
        }
    }
}

impl<SrcOut, DstIn, Cond> EdgeBuilder<Success, SrcOut, SrcOut, DstIn, Cond, Identity> {
    /// 附加转换函数，在条件命中后对整份成功输出调用一次。
    pub fn map<NewPayload, F>(
        self,
        transform: F,
    ) -> EdgeBuilder<Success, SrcOut, NewPayload, DstIn, Cond, Transform<F>>
    where
        F: Fn(&SrcOut) -> NewPayload + Send + Sync + 'static,
        NewPayload: 'static,
    {
        EdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: self.cond,
            trans: Transform(transform),
            marker: PhantomData,
        }
    }
}

impl<SrcIn, Payload, DstIn, Trans>
    EdgeBuilder<Failure, SrcIn, Payload, DstIn, Unconditional, Trans>
{
    /// 附加失败触发条件，接收原始输入与错误。
    pub fn when<F>(self, cond: F) -> EdgeBuilder<Failure, SrcIn, Payload, DstIn, Condition<F>, Trans>
    where
        F: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
    {
        EdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: Condition(cond),
            trans: self.trans,
            marker: PhantomData,
        }
    }
}

impl<SrcIn, DstIn, Cond> EdgeBuilder<Failure, SrcIn, SrcIn, DstIn, Cond, Identity> {
    /// 附加失败转换函数，接收原始输入与错误。
    pub fn map<NewPayload, F>(
        self,
        transform: F,
    ) -> EdgeBuilder<Failure, SrcIn, NewPayload, DstIn, Cond, Transform<F>>
    where
        F: Fn(&SrcIn, &NodeError) -> NewPayload + Send + Sync + 'static,
        NewPayload: 'static,
    {
        EdgeBuilder {
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            cond: self.cond,
            trans: Transform(transform),
            marker: PhantomData,
        }
    }
}

impl<Kind, Src, Payload, DstIn, Cond, Trans> EdgeBuilder<Kind, Src, Payload, DstIn, Cond, Trans>
where
    Src: DeserializeOwned + 'static,
    Payload: 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildPlanner<Kind, Src, Payload, DstIn>,
{
    pub fn via(self, arrow: Arrow<Payload, DstIn>) -> Edge {
        let built = (self.cond, self.trans).build(self.src.clone(), arrow);
        Edge::from_built(
            self.src,
            self.dst,
            self.slot,
            arrow.mode,
            TypeFingerprint::of::<Src>(),
            TypeFingerprint::of::<DstIn>(),
            built,
        )
    }
}

impl<Kind, Src, DstIn, Cond, Trans> EdgeBuilder<Kind, Src, DstIn, DstIn, Cond, Trans>
where
    Src: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildPlanner<Kind, Src, DstIn, DstIn>,
{
    pub fn direct(self) -> Edge {
        self.via(direct())
    }

    pub fn isolated(self) -> Edge {
        self.via(isolated_direct())
    }
}

impl<Kind, Src, DstIn, Cond, Trans> EdgeBuilder<Kind, Src, Vec<DstIn>, DstIn, Cond, Trans>
where
    Src: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    (Cond, Trans): BuildPlanner<Kind, Src, Vec<DstIn>, DstIn>,
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
pub trait BuildPlanner<Kind, Src, Payload, DstIn> {
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner;
}

impl<SrcOut, DstIn> BuildPlanner<Success, SrcOut, SrcOut, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcOut, DstIn>) -> BuiltPlanner {
        BuiltPlanner::pass_through(EdgeKind::Success, arrow.mode)
    }
}

impl<SrcOut, DstIn, C> BuildPlanner<Success, SrcOut, SrcOut, DstIn> for (Condition<C>, Identity)
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

impl<SrcOut, Payload: 'static, DstIn, T> BuildPlanner<Success, SrcOut, Payload, DstIn>
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

impl<SrcOut, Payload: 'static, DstIn, C, T> BuildPlanner<Success, SrcOut, Payload, DstIn>
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

impl<SrcIn, DstIn> BuildPlanner<Failure, SrcIn, SrcIn, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcIn, DstIn>) -> BuiltPlanner {
        BuiltPlanner::pass_through(EdgeKind::Failure, arrow.mode)
    }
}

impl<SrcIn, DstIn, C> BuildPlanner<Failure, SrcIn, SrcIn, DstIn> for (Condition<C>, Identity)
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

impl<SrcIn, Payload: 'static, DstIn, T> BuildPlanner<Failure, SrcIn, Payload, DstIn>
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

impl<SrcIn, Payload: 'static, DstIn, C, T> BuildPlanner<Failure, SrcIn, Payload, DstIn>
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
