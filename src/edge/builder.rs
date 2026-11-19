//! 边具体载体与命名建边辅助函数。
//!
//! 提供两个具体载体（[`EdgeImpl`] 实现 [`SuccessEdge`]，[`FailureEdgeImpl`] 实现
//! [`FailureEdge`]）以及两套命名建边函数：
//! - 命令式：成功边 [`when`]（返回 [`WhenBuilder`]）/ [`map`] / [`foreach`]，失败边 [`on_error`]（无条件）/
//!   [`when_error`]（返回 [`ErrorWhenBuilder`]）。类型参数直接来自转换/条件闭包的签名；
//!   与注册节点的接线一致性由 build 期的类型指纹校验兜底（无编译期检查）。
//! - 类型检查版（DSL 专用）：[`map_checked`]/[`foreach_checked`]/[`when_checked`]/[`on_error_checked`]/
//!   [`when_error_checked`]，接收由节点实例推导的 [`NodeKey`]，编译期强制转换函数入参 ==
//!   上游节点 `Out`（失败边为 `In`）、出参 == 下游节点 `In`；`when`/`when_error` 不接转换时
//!   用 `.passthrough_checked()` 透传并强制上游输出类型 == 下游输入类型；无条件的成功透传
//!   用 [`passthrough_checked`]（DSL 裸边语法 `src -> dst`）。
//! `when(cond)`、`when_error(cond)` 不接转换时用 `.passthrough()` 实现纯透传。成功边与失败边一一对称。

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use super::super::node::{Node, NodeError, NodeKey};
use super::{FailureEdge, SuccessEdge};

/// 成功边具体载体：存 `when`/`map` 闭包，实现 [`SuccessEdge`]。
pub struct EdgeImpl<SrcOut, DstIn> {
    src: &'static str,
    dst: &'static str,
    creates_scope: bool,
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
    fn creates_scope(&self) -> bool {
        self.creates_scope
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
            creates_scope: false,
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
            creates_scope: false,
            when: Some(self.condition),
            map: Some(Box::new(transform)),
            marker: PhantomData,
        }
    }

    /// 追加成功转换（类型检查版）：条件命中后把输出扇出为 `dst` 输入。
    ///
    /// 接收 `dst` 节点的 [`NodeKey`]，编译期强制
    /// `transform: Fn(&SrcOut) -> Vec<D::In>`——出参必须恰为下游节点输入类型。
    pub fn map_checked<D, Ctx, F>(
        self,
        to: NodeKey<Ctx, D>,
        transform: F,
    ) -> EdgeImpl<SrcOut, <D as Node<Ctx>>::In>
    where
        D: Node<Ctx>,
        F: Fn(&SrcOut) -> Vec<<D as Node<Ctx>>::In> + Send + Sync + 'static,
    {
        EdgeImpl {
            src: self.src,
            dst: to.name(),
            creates_scope: false,
            when: Some(self.condition),
            map: Some(Box::new(transform)),
            marker: PhantomData,
        }
    }

    /// 纯条件边（类型检查版）：条件命中后透传输出作为 `dst` 输入。
    ///
    /// 等式约束 `D::In = SrcOut` 编译期强制上游输出类型 == 下游输入类型
    /// （否则到运行时才以 serde 错误暴露）。
    pub fn passthrough_checked<D, Ctx>(mut self, to: NodeKey<Ctx, D>) -> EdgeImpl<SrcOut, SrcOut>
    where
        D: Node<Ctx, In = SrcOut>,
    {
        self.dst = to.name();
        self.passthrough()
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

    /// 追加失败转换（类型检查版）：条件命中后把失败输入 + 错误扇出为 `to` 输入。
    ///
    /// 接收 `to` 节点的 [`NodeKey`]，编译期强制
    /// `transform: Fn(&SrcIn, &NodeError) -> Vec<D::In>`——出参必须恰为下游节点输入类型。
    pub fn map_checked<D, Ctx, F>(
        self,
        to: NodeKey<Ctx, D>,
        transform: F,
    ) -> FailureEdgeImpl<SrcIn, <D as Node<Ctx>>::In>
    where
        D: Node<Ctx>,
        F: Fn(&SrcIn, &NodeError) -> Vec<<D as Node<Ctx>>::In> + Send + Sync + 'static,
    {
        FailureEdgeImpl {
            src: self.src,
            dst: to.name(),
            when: Some(self.condition),
            map: Some(Box::new(transform)),
            marker: PhantomData,
        }
    }

    /// 纯条件失败边（类型检查版）：条件命中后透传失败输入作为 `to` 输入。
    ///
    /// 等式约束 `D::In = SrcIn` 编译期强制上游输入类型 == 下游输入类型。
    pub fn passthrough_checked<D, Ctx>(
        mut self,
        to: NodeKey<Ctx, D>,
    ) -> FailureEdgeImpl<SrcIn, SrcIn>
    where
        D: Node<Ctx, In = SrcIn>,
    {
        self.dst = to.name();
        self.passthrough()
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

/// 成功条件边（类型检查版）：`from` 成功后输出满足 `condition` 才触发 `to`。
///
/// 与 [`when`] 的差别：接收 `from` 节点的 [`NodeKey`]，编译期强制
/// `condition: Fn(&K::Out) -> bool`——条件入参必须恰为上游节点输出类型。
/// 返回 [`WhenBuilder`]，DSL 再接 `.map_checked()`/`.passthrough_checked()` 完成类型检查。
pub fn when_checked<K, Ctx, F>(
    from: NodeKey<Ctx, K>,
    to: &'static str,
    condition: F,
) -> WhenBuilder<<K as Node<Ctx>>::Out>
where
    K: Node<Ctx>,
    F: Fn(&<K as Node<Ctx>>::Out) -> bool + Send + Sync + 'static,
{
    WhenBuilder {
        src: from.name(),
        dst: to,
        condition: Box::new(condition),
        marker: PhantomData,
    }
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
        creates_scope: false,
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
    EdgeImpl {
        src: from.name(),
        dst: to.name(),
        creates_scope: false,
        when: None,
        map: Some(Box::new(transform)),
        marker: PhantomData,
    }
}

/// 将数组逐项传给目标节点，每个目标任务以自身 task_id 建立独立作用域。
/// 后继任务继承该作用域；空数组不创建任务。与 [`map`] 不同，本边明确建立分组边界。
pub fn foreach<T>(from: &'static str, to: &'static str) -> EdgeImpl<Vec<T>, T>
where
    T: Clone + 'static,
{
    EdgeImpl {
        src: from,
        dst: to,
        creates_scope: true,
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
    FailureEdgeImpl {
        src: from.name(),
        dst: to.name(),
        when: None,
        map: Some(Box::new(transform)),
        marker: PhantomData,
    }
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

/// 失败条件边（类型检查版）：`from` 失败且条件命中才触发 `to`。对称于 [`when_checked`]。
///
/// 与 [`when_error`] 的差别：接收 `from` 节点的 [`NodeKey`]，编译期强制
/// `condition: Fn(&K::In, &NodeError) -> bool`——条件入参必须恰为失败节点输入类型。
/// 返回 [`ErrorWhenBuilder`]，DSL 再接 `.map_checked()`/`.passthrough_checked()` 完成类型检查。
pub fn when_error_checked<K, Ctx, F>(
    from: NodeKey<Ctx, K>,
    to: &'static str,
    condition: F,
) -> ErrorWhenBuilder<<K as Node<Ctx>>::In>
where
    K: Node<Ctx>,
    F: Fn(&<K as Node<Ctx>>::In, &NodeError) -> bool + Send + Sync + 'static,
{
    ErrorWhenBuilder {
        src: from.name(),
        dst: to,
        condition: Box::new(condition),
        marker: PhantomData,
    }
}

/// 成功透传边（类型检查版）：无条件把 `from` 的输出原样喂给 `to`（不剪枝、不转换）。
///
/// 与 [`WhenBuilder::passthrough_checked`] 的差别：无任何条件，`from` 成功即透传。
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
        creates_scope: false,
        when: None,
        map: None,
        marker: PhantomData,
    }
}
