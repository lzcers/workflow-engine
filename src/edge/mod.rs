//! 统一的边模型：成功边与失败边共享完全对称的流式构造、路由模式与零拷贝热路径。
//!
//! 四个正交维度：
//! 1. 触发维度：[`Edge::from`] / [`Edge::success`]（成功边）与 [`Edge::on_error`] / [`Edge::failure`]（失败边）
//! 2. 目标维度：普通节点 `.to(...)` 与 JOIN 强类型槽位 `.to_slot(..., slot)`
//! 3. 过滤与转换：可选 `.when(pred)` 与 `.map(transform)`
//! 4. 分发箭头（`1|N × Shared|Isolated` 完整 2×2 对称矩阵）：
//!    - `->`（[`direct`] / `.direct()`）：单值，共享作用域
//!    - `->!`（[`isolated_direct`] / `.isolated()`）：单值，建立独立子图作用域
//!    - `->*`（[`shared_expand`] / `.expand()`）：展开 `Vec<T>`，共享作用域
//!    - `->+`（[`isolated_expand`] / `.expand_isolated()`）：展开 `Vec<T>`，每项建立独立子图作用域

use std::{marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::node::{EdgeSlot, JoinInput, JoinSlot, Node, NodeError, NodeKey, TypeFingerprint};

/// 边的触发类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    Success,
    Failure,
}

/// 边的路由与分发模式（单值/列表 × 共享/隔离作用域的完整 2×2 对称矩阵）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeMode {
    /// `->` 整份传递一个目标输入（共享作用域）。
    Direct,
    /// `->!` 整份传递一个目标输入（进入独立新子图作用域）。
    IsolatedDirect,
    /// `->*` 共享作用域展开列表。
    SharedExpand,
    /// `->+` 按列表元素建立独立子图作用域。
    IsolatedExpand,
}

impl EdgeMode {
    pub const fn is_isolated(self) -> bool {
        matches!(self, Self::IsolatedDirect | Self::IsolatedExpand)
    }

    pub const fn is_expand(self) -> bool {
        matches!(self, Self::SharedExpand | Self::IsolatedExpand)
    }
}

/// 定义整份载荷如何分发给目标，类型参数分别为载荷类型和单份目标输入类型。
pub struct Arrow<Payload, DstIn> {
    mode: EdgeMode,
    inputs: fn(Payload) -> Vec<DstIn>,
}

impl<Payload, DstIn> Copy for Arrow<Payload, DstIn> {}
impl<Payload, DstIn> Clone for Arrow<Payload, DstIn> {
    fn clone(&self) -> Self {
        *self
    }
}

/// `->` 整份传递载荷（包括空列表），共享作用域。
pub fn direct<T>() -> Arrow<T, T> {
    Arrow {
        mode: EdgeMode::Direct,
        inputs: |value| vec![value],
    }
}

/// `->!` 整份传递载荷，为目标子图建立独立作用域。
pub fn isolated_direct<T>() -> Arrow<T, T> {
    Arrow {
        mode: EdgeMode::IsolatedDirect,
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

/// 边规划错误：确定性错误（反序列化/序列化类型不匹配），重试无意义。
#[derive(Debug, Clone, thiserror::Error)]
pub enum EdgeError {
    /// 成功边把 `src` 输出反序列化为 `SrcOut` 失败。
    #[error("edge {src}: bad src output: {detail}")]
    BadSrcOut { src: Arc<str>, detail: String },
    /// 失败边把失败节点输入反序列化为 `SrcIn` 失败。
    #[error("edge {src}: bad src input: {detail}")]
    BadSrcIn { src: Arc<str>, detail: String },
    /// 转换结果序列化为 `Value` 失败。
    #[error("edge {src}: serialize dst input: {detail}")]
    BadDstIn { src: Arc<str>, detail: String },
}

type SuccessPlannerFn = Box<dyn Fn(&Value) -> Result<Vec<Value>, EdgeError> + Send + Sync>;
type FailurePlannerFn =
    Box<dyn Fn(&Value, &NodeError) -> Result<Vec<Value>, EdgeError> + Send + Sync>;

/// 内部规划器包装（对外部不透明）。
#[doc(hidden)]
pub struct BuiltPlanner {
    has_condition: bool,
    has_transform: bool,
    planner: EdgePlanner,
}

enum EdgePlanner {
    /// 无条件且无转换的单值直连：直接透传，零 serde 往返。
    DirectPassThrough,
    /// 无条件且无转换的列表展开：直接解包 JSON 数组，零元素 serde 往返。
    ExpandPassThrough,
    Success(SuccessPlannerFn),
    Failure(FailurePlannerFn),
}

/// 统一的图边：构造期擦除具体载荷类型，保留触发类型、路由模式、目标槽位与端口指纹。
pub struct Edge {
    kind: EdgeKind,
    src: Arc<str>,
    dst: Arc<str>,
    slot: Option<EdgeSlot>,
    mode: EdgeMode,
    has_transform: bool,
    has_condition: bool,
    src_type: TypeFingerprint,
    dst_type: TypeFingerprint,
    planner: EdgePlanner,
}

impl Edge {
    /// 从类型化 `NodeKey` 出发构造成功边（编译期检查类型接线）。
    pub fn from<Ctx, Src: Node<Ctx>>(src: &NodeKey<Ctx, Src>) -> CheckedSuccessFrom<Ctx, Src> {
        CheckedSuccessFrom {
            src: src.arc_name(),
            marker: PhantomData,
        }
    }

    /// 从类型化 `NodeKey` 出发构造失败边（编译期检查类型接线）。
    pub fn on_error<Ctx, Src: Node<Ctx>>(src: &NodeKey<Ctx, Src>) -> CheckedFailureFrom<Ctx, Src> {
        CheckedFailureFrom {
            src: src.arc_name(),
            marker: PhantomData,
        }
    }

    /// 从动态/静态节点名出发构造成功边（建图期检查类型指纹）。
    pub fn success<SrcOut>(src: impl Into<Arc<str>>) -> SuccessFrom<SrcOut>
    where
        SrcOut: DeserializeOwned + 'static,
    {
        SuccessFrom {
            src: src.into(),
            marker: PhantomData,
        }
    }

    /// 从动态/静态节点名出发构造失败边（建图期检查类型指纹）。
    pub fn failure<SrcIn>(src: impl Into<Arc<str>>) -> FailureFrom<SrcIn>
    where
        SrcIn: DeserializeOwned + 'static,
    {
        FailureFrom {
            src: src.into(),
            marker: PhantomData,
        }
    }

    pub fn kind(&self) -> EdgeKind {
        self.kind
    }

    pub fn src(&self) -> &str {
        &self.src
    }

    pub(crate) fn src_arc(&self) -> Arc<str> {
        self.src.clone()
    }

    pub fn dst(&self) -> &str {
        &self.dst
    }

    pub(crate) fn dst_arc(&self) -> Arc<str> {
        self.dst.clone()
    }

    pub(crate) fn slot(&self) -> Option<EdgeSlot> {
        self.slot
    }

    pub fn slot_key(&self) -> Option<&'static str> {
        self.slot.map(|slot| slot.key)
    }

    pub fn mode(&self) -> EdgeMode {
        self.mode
    }

    pub fn has_transform(&self) -> bool {
        self.has_transform
    }

    pub fn has_condition(&self) -> bool {
        self.has_condition
    }

    pub(crate) fn src_type(&self) -> TypeFingerprint {
        self.src_type
    }

    pub(crate) fn dst_type(&self) -> TypeFingerprint {
        self.dst_type
    }

    pub fn plan_success(&self, out: &Value) -> Result<Vec<Value>, EdgeError> {
        match &self.planner {
            EdgePlanner::DirectPassThrough => Ok(vec![out.clone()]),
            EdgePlanner::ExpandPassThrough => match out {
                Value::Array(items) => Ok(items.clone()),
                _ => Err(EdgeError::BadSrcOut {
                    src: self.src.clone(),
                    detail: "expected JSON array for expand arrow".into(),
                }),
            },
            EdgePlanner::Success(f) => f(out),
            EdgePlanner::Failure(_) => Ok(Vec::new()),
        }
    }

    pub fn plan_failure(&self, input: &Value, err: &NodeError) -> Result<Vec<Value>, EdgeError> {
        match &self.planner {
            EdgePlanner::DirectPassThrough => Ok(vec![input.clone()]),
            EdgePlanner::ExpandPassThrough => match input {
                Value::Array(items) => Ok(items.clone()),
                _ => Err(EdgeError::BadSrcIn {
                    src: self.src.clone(),
                    detail: "expected JSON array for expand arrow".into(),
                }),
            },
            EdgePlanner::Failure(f) => f(input, err),
            EdgePlanner::Success(_) => Ok(Vec::new()),
        }
    }
}

// ============================================================================
// 源端点构造器（Checked / Dynamic × Success / Failure 完全对称）
// ============================================================================

pub struct CheckedSuccessFrom<Ctx, Src: Node<Ctx>> {
    src: Arc<str>,
    marker: PhantomData<fn(Ctx) -> Src>,
}

impl<Ctx, Src: Node<Ctx>> CheckedSuccessFrom<Ctx, Src> {
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

pub struct CheckedFailureFrom<Ctx, Src: Node<Ctx>> {
    src: Arc<str>,
    marker: PhantomData<fn(Ctx) -> Src>,
}

impl<Ctx, Src: Node<Ctx>> CheckedFailureFrom<Ctx, Src> {
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

pub struct SuccessFrom<SrcOut> {
    src: Arc<str>,
    marker: PhantomData<fn() -> SrcOut>,
}

impl<SrcOut: DeserializeOwned + 'static> SuccessFrom<SrcOut> {
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

pub struct FailureFrom<SrcIn> {
    src: Arc<str>,
    marker: PhantomData<fn() -> SrcIn>,
}

impl<SrcIn: DeserializeOwned + 'static> FailureFrom<SrcIn> {
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

// ============================================================================
// 状态标记与对称流式 Builder（零 serde 透传 + 借用 &Value 反序列化）
// ============================================================================

pub struct Unconditional;
pub struct Condition<F>(F);
pub struct Identity;
pub struct Transform<F>(F);

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
        Edge {
            kind: EdgeKind::Success,
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            mode: arrow.mode,
            has_transform: built.has_transform,
            has_condition: built.has_condition,
            src_type: TypeFingerprint::of::<SrcOut>(),
            dst_type: TypeFingerprint::of::<DstIn>(),
            planner: built.planner,
        }
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
        Edge {
            kind: EdgeKind::Failure,
            src: self.src,
            dst: self.dst,
            slot: self.slot,
            mode: arrow.mode,
            has_transform: built.has_transform,
            has_condition: built.has_condition,
            src_type: TypeFingerprint::of::<SrcIn>(),
            dst_type: TypeFingerprint::of::<DstIn>(),
            planner: built.planner,
        }
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
pub trait BuildSuccessPlanner<SrcOut, Payload, DstIn> {
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner;
}

impl<SrcOut, DstIn> BuildSuccessPlanner<SrcOut, SrcOut, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcOut, DstIn>) -> BuiltPlanner {
        let planner = if arrow.mode.is_expand() {
            EdgePlanner::ExpandPassThrough
        } else {
            EdgePlanner::DirectPassThrough
        };
        BuiltPlanner {
            has_condition: false,
            has_transform: false,
            planner,
        }
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
        let planner = EdgePlanner::Success(Box::new(move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed) {
                return Ok(Vec::new());
            }
            if is_expand {
                match out {
                    Value::Array(items) => Ok(items.clone()),
                    _ => Err(EdgeError::BadSrcOut {
                        src: src.clone(),
                        detail: "expected JSON array for expand arrow".into(),
                    }),
                }
            } else {
                Ok(vec![out.clone()])
            }
        }));
        BuiltPlanner {
            has_condition: true,
            has_transform: false,
            planner,
        }
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
        let planner = EdgePlanner::Success(Box::new(move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            serialize_inputs(&src, (arrow.inputs)(trans(&typed)))
        }));
        BuiltPlanner {
            has_condition: false,
            has_transform: true,
            planner,
        }
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
        let planner = EdgePlanner::Success(Box::new(move |out: &Value| {
            let typed = SrcOut::deserialize(out).map_err(|e| EdgeError::BadSrcOut {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed) {
                return Ok(Vec::new());
            }
            serialize_inputs(&src, (arrow.inputs)(trans(&typed)))
        }));
        BuiltPlanner {
            has_condition: true,
            has_transform: true,
            planner,
        }
    }
}

#[doc(hidden)]
pub trait BuildFailurePlanner<SrcIn, Payload, DstIn> {
    fn build(self, src: Arc<str>, arrow: Arrow<Payload, DstIn>) -> BuiltPlanner;
}

impl<SrcIn, DstIn> BuildFailurePlanner<SrcIn, SrcIn, DstIn> for (Unconditional, Identity) {
    fn build(self, _: Arc<str>, arrow: Arrow<SrcIn, DstIn>) -> BuiltPlanner {
        let planner = if arrow.mode.is_expand() {
            EdgePlanner::ExpandPassThrough
        } else {
            EdgePlanner::DirectPassThrough
        };
        BuiltPlanner {
            has_condition: false,
            has_transform: false,
            planner,
        }
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
        let planner = EdgePlanner::Failure(Box::new(move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed, err) {
                return Ok(Vec::new());
            }
            if is_expand {
                match input {
                    Value::Array(items) => Ok(items.clone()),
                    _ => Err(EdgeError::BadSrcIn {
                        src: src.clone(),
                        detail: "expected JSON array for expand arrow".into(),
                    }),
                }
            } else {
                Ok(vec![input.clone()])
            }
        }));
        BuiltPlanner {
            has_condition: true,
            has_transform: false,
            planner,
        }
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
        let planner = EdgePlanner::Failure(Box::new(move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            serialize_inputs(&src, (arrow.inputs)(trans(&typed, err)))
        }));
        BuiltPlanner {
            has_condition: false,
            has_transform: true,
            planner,
        }
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
        let planner = EdgePlanner::Failure(Box::new(move |input: &Value, err: &NodeError| {
            let typed = SrcIn::deserialize(input).map_err(|e| EdgeError::BadSrcIn {
                src: src.clone(),
                detail: e.to_string(),
            })?;
            if !cond(&typed, err) {
                return Ok(Vec::new());
            }
            serialize_inputs(&src, (arrow.inputs)(trans(&typed, err)))
        }));
        BuiltPlanner {
            has_condition: true,
            has_transform: true,
            planner,
        }
    }
}

fn serialize_inputs<DstIn: Serialize>(
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
