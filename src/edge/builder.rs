//! 命名建边辅助函数：直接构造 [`SuccessEdge`] 与 [`FailureEdge`]。
//!
//! - 命令式：成功边 [`when`]（返回 [`WhenBuilder`]）/ [`map`] / [`foreach`]，失败边 [`on_error`] /
//!   [`when_error`]（返回 [`ErrorWhenBuilder`]）；类型接线一致性由 build 期的类型指纹校验兜底。
//! - DSL 的 `*_checked` 函数由两端的 [`NodeKey`] 在编译期强制约束输入输出类型。

use serde::{Serialize, de::DeserializeOwned};

use super::super::node::{Node, NodeError, NodeKey};
use super::{EdgeMode, FailureEdge, SuccessEdge};

/// [`when`] 的构建器：先声明成功条件，再决定透传或追加转换。
pub struct WhenBuilder<SrcOut> {
    src: &'static str,
    dst: &'static str,
    condition: Box<dyn Fn(&SrcOut) -> bool + Send + Sync>,
}

impl<SrcOut> WhenBuilder<SrcOut>
where
    SrcOut: Serialize + DeserializeOwned + 'static,
{
    /// 纯条件边：`src` 成功后输出满足条件才触发 `dst`，透传输出作为 `dst` 输入。
    pub fn passthrough(self) -> SuccessEdge {
        SuccessEdge::new::<SrcOut, SrcOut>(
            self.src,
            self.dst,
            EdgeMode::Passthrough,
            Some(self.condition),
            None,
        )
    }

    /// 追加成功转换：条件命中后把输出扇出为 `dst` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> SuccessEdge
    where
        DstIn: Serialize + 'static,
        F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
    {
        SuccessEdge::new(
            self.src,
            self.dst,
            EdgeMode::Map,
            Some(self.condition),
            Some(Box::new(transform)),
        )
    }
}

/// [`when_error`] 的构建器：先声明失败条件，再决定透传或追加转换。
pub struct ErrorWhenBuilder<SrcIn> {
    src: &'static str,
    dst: &'static str,
    condition: Box<dyn Fn(&SrcIn, &NodeError) -> bool + Send + Sync>,
}

impl<SrcIn> ErrorWhenBuilder<SrcIn>
where
    SrcIn: Serialize + DeserializeOwned + 'static,
{
    /// 纯条件失败边：`from` 失败且条件命中才触发 `to`，透传失败输入作为 `to` 输入。
    pub fn passthrough(self) -> FailureEdge {
        FailureEdge::new::<SrcIn, SrcIn>(self.src, self.dst, Some(self.condition), None)
    }

    /// 追加失败转换：条件命中后把失败输入 + 错误扇出为 `to` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> FailureEdge
    where
        DstIn: Serialize + 'static,
        F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
    {
        FailureEdge::new(
            self.src,
            self.dst,
            Some(self.condition),
            Some(Box::new(transform)),
        )
    }
}

/// 成功条件边：`from` 成功后输出满足 `condition` 才触发 `to`。
pub fn when<SrcOut, F>(from: &'static str, to: &'static str, condition: F) -> WhenBuilder<SrcOut>
where
    F: Fn(&SrcOut) -> bool + Send + Sync + 'static,
{
    WhenBuilder {
        src: from,
        dst: to,
        condition: Box::new(condition),
    }
}

/// 成功条件边，透传类型必须与目标输入一致。
pub fn when_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::Out) -> bool + Send + Sync + 'static,
) -> SuccessEdge
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
) -> SuccessEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    when(from.name(), to.name(), condition).map(transform)
}

/// 成功转换边：`from` 成功后经 `transform` 转换触发 `to`（扇出，空即不继续）。
pub fn map<SrcOut, DstIn, F>(from: &'static str, to: &'static str, transform: F) -> SuccessEdge
where
    SrcOut: Serialize + DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
{
    SuccessEdge::new(from, to, EdgeMode::Map, None, Some(Box::new(transform)))
}

/// 成功转换边（类型检查版）。
pub fn map_checked<K1, K2, Ctx, F>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
    transform: F,
) -> SuccessEdge
where
    K1: Node<Ctx>,
    K2: Node<Ctx>,
    F: Fn(&<K1 as Node<Ctx>>::Out) -> Vec<<K2 as Node<Ctx>>::In> + Send + Sync + 'static,
{
    map(from.name(), to.name(), transform)
}

/// 将数组逐项传给目标子图，每个元素建立独立作用域。
pub fn foreach<T>(from: &'static str, to: &'static str) -> SuccessEdge
where
    T: Serialize + DeserializeOwned + Clone + 'static,
{
    SuccessEdge::new::<Vec<T>, T>(
        from,
        to,
        EdgeMode::Foreach,
        None,
        Some(Box::new(|items| items.clone())),
    )
}

/// 逐项分组边的类型检查入口；源输出必须是目标输入组成的 Vec。
pub fn foreach_checked<K1, K2, Ctx>(from: NodeKey<Ctx, K1>, to: NodeKey<Ctx, K2>) -> SuccessEdge
where
    K1: Node<Ctx, Out = Vec<K2::In>>,
    K2: Node<Ctx>,
    K2::In: Clone,
{
    foreach::<K2::In>(from.name(), to.name())
}

/// 失败边（无条件）：`from` 失败后，用失败节点的输入 + 错误构造 `to` 输入。
pub fn on_error<SrcIn, DstIn, F>(from: &'static str, to: &'static str, transform: F) -> FailureEdge
where
    SrcIn: Serialize + DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
{
    FailureEdge::new(from, to, None, Some(Box::new(transform)))
}

/// 失败边（无条件，类型检查版）。
pub fn on_error_checked<K1, K2, Ctx, F>(
    from: NodeKey<Ctx, K1>,
    to: NodeKey<Ctx, K2>,
    transform: F,
) -> FailureEdge
where
    K1: Node<Ctx>,
    K2: Node<Ctx>,
    F: Fn(&<K1 as Node<Ctx>>::In, &NodeError) -> Vec<<K2 as Node<Ctx>>::In> + Send + Sync + 'static,
{
    on_error(from.name(), to.name(), transform)
}

/// 失败条件边：`from` 失败且条件命中才触发 `to`。
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
    }
}

/// 失败条件边，透传的失败节点输入必须与目标输入一致。
pub fn when_error_checked<Src, Dst, Ctx>(
    from: NodeKey<Ctx, Src>,
    to: NodeKey<Ctx, Dst>,
    condition: impl Fn(&Src::In, &NodeError) -> bool + Send + Sync + 'static,
) -> FailureEdge
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
) -> FailureEdge
where
    Src: Node<Ctx>,
    Dst: Node<Ctx>,
{
    when_error(from.name(), to.name(), condition).map(transform)
}

/// 成功透传边（类型检查版）：无条件把 `from` 的输出原样喂给 `to`。
pub fn passthrough_checked<K1, K2, Ctx>(from: NodeKey<Ctx, K1>, to: NodeKey<Ctx, K2>) -> SuccessEdge
where
    K1: Node<Ctx>,
    K2: Node<Ctx, In = <K1 as Node<Ctx>>::Out>,
{
    SuccessEdge::new::<K1::Out, K1::Out>(from.name(), to.name(), EdgeMode::Passthrough, None, None)
}
