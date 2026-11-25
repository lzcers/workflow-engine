//! 命名建边辅助函数：直接构造 [`SuccessEdge`] 与 [`FailureEdge`]。
//!
//! - 命令式：成功边 [`passthrough`] / [`when`]（返回 [`WhenBuilder`]）/ [`map`] / [`foreach`]，
//!   失败边 [`on_error`] / [`when_error`]（返回 [`ErrorWhenBuilder`]）；
//!   类型接线一致性由 build 期的类型指纹校验兜底。
//! - DSL 的 `*_checked` 函数由两端的 [`NodeKey`] 在编译期强制约束输入输出类型。

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use super::super::node::{Node, NodeError, NodeKey};
use super::{EdgeMode, FailureEdge, SuccessEdge};

/// [`when`] 的构建器：先声明成功条件，再决定透传或追加转换。
pub struct WhenBuilder<SrcOut, C> {
    src: &'static str,
    dst: &'static str,
    condition: C,
    marker: PhantomData<fn(&SrcOut) -> bool>,
}

impl<SrcOut, C> WhenBuilder<SrcOut, C>
where
    SrcOut: DeserializeOwned + 'static,
    C: Fn(&SrcOut) -> bool + Send + Sync + 'static,
{
    /// 纯条件边：`src` 成功后输出满足条件才触发 `dst`，透传输出作为 `dst` 输入。
    pub fn passthrough(self) -> SuccessEdge
    where
        SrcOut: Serialize,
    {
        let cond = self.condition;
        SuccessEdge::new::<SrcOut, SrcOut, _>(self.src, self.dst, EdgeMode::Passthrough, move |out| {
            if cond(&out) { vec![out] } else { Vec::new() }
        })
    }

    /// 追加成功转换：条件命中后把输出扇出为 `dst` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> SuccessEdge
    where
        DstIn: Serialize + 'static,
        F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
    {
        let cond = self.condition;
        SuccessEdge::new::<SrcOut, DstIn, _>(self.src, self.dst, EdgeMode::Map, move |out| {
            if cond(&out) {
                transform(&out)
            } else {
                Vec::new()
            }
        })
    }
}

/// [`when_error`] 的构建器：先声明失败条件，再决定透传或追加转换。
pub struct ErrorWhenBuilder<SrcIn, C> {
    src: &'static str,
    dst: &'static str,
    condition: C,
    marker: PhantomData<fn(&SrcIn, &NodeError) -> bool>,
}

impl<SrcIn, C> ErrorWhenBuilder<SrcIn, C>
where
    SrcIn: DeserializeOwned + 'static,
    C: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
{
    /// 纯条件失败边：`from` 失败且条件命中才触发 `to`，透传失败输入作为 `to` 输入。
    pub fn passthrough(self) -> FailureEdge
    where
        SrcIn: Serialize,
    {
        let cond = self.condition;
        FailureEdge::new::<SrcIn, SrcIn, _>(
            self.src,
            self.dst,
            EdgeMode::Passthrough,
            move |input, err| {
                if cond(&input, err) {
                    vec![input]
                } else {
                    Vec::new()
                }
            },
        )
    }

    /// 追加失败转换：条件命中后把失败输入 + 错误扇出为 `to` 输入。
    pub fn map<DstIn, F>(self, transform: F) -> FailureEdge
    where
        DstIn: Serialize + 'static,
        F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
    {
        let cond = self.condition;
        FailureEdge::new::<SrcIn, DstIn, _>(
            self.src,
            self.dst,
            EdgeMode::Map,
            move |input, err| {
                if cond(&input, err) {
                    transform(&input, err)
                } else {
                    Vec::new()
                }
            },
        )
    }
}

/// 成功透传边：无条件把 `from` 的输出原样喂给 `to`。
pub fn passthrough<T>(from: &'static str, to: &'static str) -> SuccessEdge
where
    T: Serialize + DeserializeOwned + 'static,
{
    SuccessEdge::new::<T, T, _>(from, to, EdgeMode::Passthrough, |out| vec![out])
}

/// 成功透传边（类型检查版）：无条件把 `from` 的输出原样喂给 `to`。
pub fn passthrough_checked<K1, K2, Ctx>(from: NodeKey<Ctx, K1>, to: NodeKey<Ctx, K2>) -> SuccessEdge
where
    K1: Node<Ctx>,
    K2: Node<Ctx, In = <K1 as Node<Ctx>>::Out>,
{
    passthrough::<K1::Out>(from.name(), to.name())
}

/// 成功条件边：`from` 成功后输出满足 `condition` 才触发 `to`。
pub fn when<SrcOut, F>(from: &'static str, to: &'static str, condition: F) -> WhenBuilder<SrcOut, F>
where
    F: Fn(&SrcOut) -> bool + Send + Sync + 'static,
{
    WhenBuilder {
        src: from,
        dst: to,
        condition,
        marker: PhantomData,
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
    SrcOut: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    F: Fn(&SrcOut) -> Vec<DstIn> + Send + Sync + 'static,
{
    SuccessEdge::new::<SrcOut, DstIn, _>(from, to, EdgeMode::Map, move |out| transform(&out))
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
    T: Serialize + DeserializeOwned + 'static,
{
    SuccessEdge::new::<Vec<T>, T, _>(from, to, EdgeMode::Foreach, |items| items)
}

/// 逐项分组边的类型检查入口；源输出必须是目标输入组成的 Vec。
pub fn foreach_checked<K1, K2, Ctx>(from: NodeKey<Ctx, K1>, to: NodeKey<Ctx, K2>) -> SuccessEdge
where
    K1: Node<Ctx, Out = Vec<K2::In>>,
    K2: Node<Ctx>,
{
    foreach::<K2::In>(from.name(), to.name())
}

/// 失败边（无条件）：`from` 失败后，用失败节点的输入 + 错误构造 `to` 输入。
pub fn on_error<SrcIn, DstIn, F>(from: &'static str, to: &'static str, transform: F) -> FailureEdge
where
    SrcIn: DeserializeOwned + 'static,
    DstIn: Serialize + 'static,
    F: Fn(&SrcIn, &NodeError) -> Vec<DstIn> + Send + Sync + 'static,
{
    FailureEdge::new::<SrcIn, DstIn, _>(from, to, EdgeMode::Map, move |input, err| {
        transform(&input, err)
    })
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
) -> ErrorWhenBuilder<SrcIn, F>
where
    F: Fn(&SrcIn, &NodeError) -> bool + Send + Sync + 'static,
{
    ErrorWhenBuilder {
        src: from,
        dst: to,
        condition,
        marker: PhantomData,
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
