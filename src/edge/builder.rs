//! 边具体载体与命名建边辅助函数。
//!
//! 提供两个具体载体（[`EdgeImpl`] 实现 [`SuccessEdge`]，[`FailureEdgeImpl`] 实现
//! [`FailureEdge`]）以及两套命名建边函数：
//! - 命令式：成功边 [`when`]（返回 [`WhenBuilder`]）/ [`map`] / [`foreach`]，失败边 [`on_error`]（无条件）/
//!   [`when_error`]（返回 [`ErrorWhenBuilder`]）。类型参数直接来自转换/条件闭包的签名；
//!   与注册节点的接线一致性由 build 期的类型指纹校验兜底（无编译期检查）。
//! - DSL 的 checked 函数由两端的 [`NodeKey`] 约束条件与转换的签名。透传要求上游载荷
//!   与目标节点输入同型，转换必须返回目标节点的输入列表。

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use super::super::node::{Node, NodeError, NodeKey};
use super::{FailureEdge, SuccessEdge};

/// 成功边具体载体：存 `when`/`map` 闭包，实现 [`SuccessEdge`]。
pub struct EdgeImpl<SrcOut, DstIn> {
    src: &'static str,
    dst: &'static str,
    is_foreach: bool,
    when: Option<Box<dyn Fn(&SrcOut) -> bool + Send + Sync>>,
    map: Option<Box<dyn Fn(&SrcOut) -> Vec<DstIn> + Send + Sync>>,
    marker: PhantomData<fn(SrcOut) -> DstIn>,
}

impl<SrcOut, DstIn> SuccessEdge for EdgeImpl<SrcOut, DstIn>
where
    SrcOut: Serialize + DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
{
    type SrcOut = SrcOut;
    type DstIn = DstIn;
    fn src(&self) -> &'static str {
        self.src
    }
    fn dst(&self) -> &'static str {
        self.dst
    }
    fn is_foreach(&self) -> bool {
        self.is_foreach
    }
    fn when(&self) -> Option<&(dyn Fn(&SrcOut) -> bool + Send + Sync)> {
        self.when.as_deref()
    }
    fn map(&self) -> Option<&(dyn Fn(&SrcOut) -> Vec<DstIn> + Send + Sync)> {
        self.map.as_deref()
    }
}

/// 失败边具体载体：存 `when`/`map` 闭包，实现 [`FailureEdge`]，对称于 [`EdgeImpl`]。
pub struct FailureEdgeImpl<SrcIn, DstIn> {
    src: &'static str,
    dst: &'static str,
    when: Option<Box<dyn Fn(&SrcIn, &NodeError) -> bool + Send + Sync>>,
    map: Option<Box<dyn Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync>>,
    marker: PhantomData<fn(SrcIn) -> DstIn>,
}

impl<SrcIn, DstIn> FailureEdge for FailureEdgeImpl<SrcIn, DstIn>
where
    SrcIn: Serialize + DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
{
    type SrcIn = SrcIn;
    type DstIn = DstIn;
    fn src(&self) -> &'static str {
        self.src
    }
    fn dst(&self) -> &'static str {
        self.dst
    }
    fn when(&self) -> Option<&(dyn Fn(&SrcIn, &NodeError) -> bool + Send + Sync)> {
        self.when.as_deref()
    }
    fn map(&self) -> Option<&(dyn Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync)> {
        self.map.as_deref()
    }
}

/// [`when`] 的构建器：先声明成功条件，再决定透传或追加转换。
pub struct WhenBuilder<SrcOut> {
    src: &'static str,
    dst: &'static str,
    condition: Box<dyn Fn(&SrcOut) -> bool + Send + Sync>,
    marker: PhantomData<SrcOut>,
}

impl<SrcOut> WhenBuilder<SrcOut> {
    /// 纯条件边：`src` 成功后输出满足条件才触发 `dst`，透传输出作为 `dst` 输入。
    pub fn passthrough(self) -> EdgeImpl<SrcOut, SrcOut> {
        EdgeImpl {
            src: self.src,
            dst: self.dst,
            is_foreach: false,
            when: Some(self.condition),
            map: None,
            marker: PhantomData,
        }
    }

    /// 追加成功转换：条件命中后把输出扇出为 `dst` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> EdgeImpl<SrcOut, DstIn>
    where
        F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
    {
        EdgeImpl {
            src: self.src,
            dst: self.dst,
            is_foreach: false,
            when: Some(self.condition),
            map: Some(Box::new(transform)),
            marker: PhantomData,
        }
    }
}

/// [`when_error`] 的构建器：先声明失败条件，再决定透传或追加转换。对称于 [`WhenBuilder`]。
pub struct ErrorWhenBuilder<SrcIn> {
    src: &'static str,
    dst: &'static str,
    condition: Box<dyn Fn(&SrcIn, &NodeError) -> bool + Send + Sync>,
    marker: PhantomData<SrcIn>,
}

impl<SrcIn> ErrorWhenBuilder<SrcIn> {
    /// 纯条件失败边：`from` 失败且条件命中才触发 `to`，透传失败输入作为 `to` 输入
    /// （要求 `SrcIn` 能从自身还原为 `DstIn`，即重试同输入）。
    pub fn passthrough(self) -> FailureEdgeImpl<SrcIn, SrcIn> {
        FailureEdgeImpl {
            src: self.src,
            dst: self.dst,
            when: Some(self.condition),
            map: None,
            marker: PhantomData,
        }
    }

    /// 追加失败转换：条件命中后把失败输入 + 错误扇出为 `to` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> FailureEdgeImpl<SrcIn, DstIn>
    where
        F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
    {
        FailureEdgeImpl {
            src: self.src,
            dst: self.dst,
            when: Some(self.condition),
            map: Some(Box::new(transform)),
            marker: PhantomData,
        }
    }
}

// ---- 命名建边辅助 ----

/// 成功条件边：`from` 成功后输出满足 `condition` 才触发 `to`。
///
/// 返回 [`WhenBuilder`]，可 `.map(...)` 追加转换或 `.passthrough()` 透传。
pub fn when<SrcOut, F>(from: &'static str, to: &'static str, condition: F) -> WhenBuilder<SrcOut>
where
    F: Fn(&SrcOut) -> bool + Send + Sync + 'static,
{
    WhenBuilder {
        src: from,
        dst: to,
        condition: Box::new(condition),
        marker: PhantomData,
    }
}

/// 成功条件边，透传类型必须与目标输入一致。
pub fn when_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::Out) -> bool + Send + Sync + 'static,
) -> EdgeImpl<Src::Out, Dst::In>
where
    Src: Node<Ctx>,
    Dst: Node<Ctx, In = Src::Out>,
{
    when(from.name(), to.name(), condition).passthrough()
}

/// 成功条件边，按两端节点的类型检查转换。
pub fn when_map_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::Out) -> bool + Send + Sync + 'static,
    transform: impl Fn(&Src::Out) -> Vec<Dst::In> + Send + Sync + 'static,
) -> EdgeImpl<Src::Out, Dst::In>
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    when(from.name(), to.name(), condition).map(transform)
}

/// 成功转换边：`from` 成功后经 `transform` 转换触发 `to`（扇出，空即不继续）。
pub fn map<SrcOut, DstIn, F>(
    from: &'static str,
    to: &'static str,
    transform: F,
) -> EdgeImpl<SrcOut, DstIn>
where
    F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
{
    EdgeImpl {
        src: from,
        dst: to,
        is_foreach: false,
        when: None,
        map: Some(Box::new(transform)),
        marker: PhantomData,
    }
}

/// 成功转换边（类型检查版）：`from` 成功后经 `transform` 转换触发 `to`（扇出，空即不继续）。
///
/// 与 [`map`] 的差别：节点类型从传入的 [`NodeKey`] 推导，编译期强制
/// `transform: Fn(&K1::Out) -> Vec<K2::In>`——入参必须是上游节点的输出类型、
/// 出参必须是下游节点的输入类型。类型错线在编译期即报错（此前要到运行时才以
/// [`crate::edge::EdgeError::BadSrcOut`] 或目标节点反序列化失败暴露）。
/// DSL（[`crate::dsl::workflow!`]）用本函数装配边；命令式用法继续用 [`map`]。
pub fn map_checked<K1, K2, Ctx, F>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
    transform: F,
) -> EdgeImpl<<K1 as Node<Ctx>>::Out, <K2 as Node<Ctx>>::In>
where
    K1: Node<Ctx>,
    K2: Node<Ctx>,
    F: Fn(&<K1 as Node<Ctx>>::Out) -> Vec<<K2 as Node<Ctx>>::In> + Send + Sync + 'static,
{
    map(from.name(), to.name(), transform)
}

/// 将数组逐项传给目标子图，每个元素建立独立作用域。
/// 子图定义确定边界；空数组不创建任务，组内后继继承实例。
pub fn foreach<T>(from: &'static str, to: &'static str) -> EdgeImpl<Vec<T>, T>
where
    T: Clone + 'static,
{
    EdgeImpl {
        src: from,
        dst: to,
        is_foreach: true,
        when: None,
        map: Some(Box::new(|items| items.clone())),
        marker: PhantomData,
    }
}

/// 逐项分组边的类型检查入口；源输出必须是目标输入组成的 Vec。
pub fn foreach_checked<K1, K2, Ctx>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
) -> EdgeImpl<Vec<K2::In>, K2::In>
where
    K1: Node<Ctx, Out = Vec<K2::In>>,
    K2: Node<Ctx>,
    K2::In: Clone,
{
    foreach(from.name(), to.name())
}

/// 失败边（无条件）：`from` 失败后，用喂给失败节点的**输入 + 错误**构造 `to` 输入。
///
/// 对称于成功边的 [`map`]：无条件触发（无 `when` 门），经 `transform` 扇出 `to` 输入；
/// `transform` 返回空即不产出下游（预算耗尽/放弃）。失败边**不约束为回退操作**——
/// 回退重试只是其一种用法（把失败节点输入 + 错误重建为重试输入，如把 `commit_scene` 的
/// 输入 + 校验错误拼成 `scene_proposal` 重试输入）；需要先按错误类型/状态门控时改用
/// [`when_error`]。
pub fn on_error<SrcIn, DstIn, F>(
    from: &'static str,
    to: &'static str,
    transform: F,
) -> FailureEdgeImpl<SrcIn, DstIn>
where
    F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
{
    FailureEdgeImpl {
        src: from,
        dst: to,
        when: None,
        map: Some(Box::new(transform)),
        marker: PhantomData,
    }
}

/// 失败边（无条件，类型检查版）：`from` 失败后，用喂给失败节点的**输入 + 错误**构造 `to` 输入。
///
/// 与 [`on_error`] 的差别：节点类型从传入的 [`NodeKey`] 推导，编译期强制
/// `transform: Fn(&K1::In, &NodeError) -> Vec<K2::In>`——入参必须是失败节点的
/// 输入类型、出参必须是 `to` 节点的输入类型。DSL（[`crate::dsl::workflow!`]）用本函数装配
/// 边；命令式用法继续用 [`on_error`]。
pub fn on_error_checked<K1, K2, Ctx, F>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
    transform: F,
) -> FailureEdgeImpl<<K1 as Node<Ctx>>::In, <K2 as Node<Ctx>>::In>
where
    K1: Node<Ctx>,
    K2: Node<Ctx>,
    F: Fn(&<K1 as Node<Ctx>>::In, &NodeError) -> Vec<<K2 as Node<Ctx>>::In> + Send + Sync + 'static,
{
    on_error(from.name(), to.name(), transform)
}

/// 失败条件边：`from` 失败且条件命中才触发 `to`。对称于成功边的 [`when`]。
///
/// 返回 [`ErrorWhenBuilder`]，可 `.map(...)` 追加转换或 `.passthrough()` 透传。
/// 条件可基于失败节点输入与错误，典型场景是按错误类型分支——可重试错误路由到重试节点，
/// 永久错误路由到兜底降级节点。
pub fn when_error<SrcIn, F>(
    from: &'static str,
    to: &'static str,
    condition: F,
) -> ErrorWhenBuilder<SrcIn>
where
    F: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
{
    ErrorWhenBuilder {
        src: from,
        dst: to,
        condition: Box::new(condition),
        marker: PhantomData,
    }
}

/// 失败条件边，透传的失败节点输入必须与目标输入一致。
pub fn when_error_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::In, &NodeError) -> bool + Send + Sync + 'static,
) -> FailureEdgeImpl<Src::In, Dst::In>
where
    Src: Node<Ctx>,
    Dst: Node<Ctx, In = Src::In>,
{
    when_error(from.name(), to.name(), condition).passthrough()
}

/// 失败条件边，按两端节点的类型检查错误转换。
pub fn when_error_map_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::In, &NodeError) -> bool + Send + Sync + 'static,
    transform: impl Fn(&Src::In, &NodeError) -> Vec<Dst::In> + Send + Sync + 'static,
) -> FailureEdgeImpl<Src::In, Dst::In>
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    when_error(from.name(), to.name(), condition).map(transform)
}

/// 成功透传边（类型检查版）：无条件把 `from` 的输出原样喂给 `to`（不剪枝、不转换）。
///
/// 等式约束 `K2::In = K1::Out` 编译期强制下游输入类型 == 上游输出类型。
/// DSL 的裸边语法 `src -> dst,` 展开为本函数。
pub fn passthrough_checked<K1, K2, Ctx>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
) -> EdgeImpl<<K1 as Node<Ctx>>::Out, <K1 as Node<Ctx>>::Out>
where
    K1: Node<Ctx>,
    K2: Node<Ctx, In = <K1 as Node<Ctx>>::Out>,
{
    EdgeImpl {
        src: from.name(),
        dst: to.name(),
        is_foreach: false,
        when: None,
        map: None,
        marker: PhantomData,
    }
}
