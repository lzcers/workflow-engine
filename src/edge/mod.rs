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

mod arrow;
mod builder;
mod planner;

use std::sync::Arc;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub use arrow::{Arrow, EdgeMode, direct, isolated_direct, isolated_expand, shared_expand};

use builder::{CheckedFrom, EdgeFrom, Failure, Success};
use planner::{BuiltPlanner, EdgePlanner};

use crate::node::{EdgeSlot, Node, NodeError, NodeKey, TypeFingerprint};

/// 边的触发类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    Success,
    Failure,
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
    pub fn from<Ctx, Src: Node<Ctx>>(src: &NodeKey<Ctx, Src>) -> CheckedFrom<Success, Ctx, Src> {
        CheckedFrom::new(src.arc_name())
    }

    /// 从类型化 `NodeKey` 出发构造失败边（编译期检查类型接线）。
    pub fn on_error<Ctx, Src: Node<Ctx>>(
        src: &NodeKey<Ctx, Src>,
    ) -> CheckedFrom<Failure, Ctx, Src> {
        CheckedFrom::new(src.arc_name())
    }

    /// 从动态/静态节点名出发构造成功边（建图期检查类型指纹）。
    pub fn success<SrcOut>(src: impl Into<Arc<str>>) -> EdgeFrom<Success, SrcOut>
    where
        SrcOut: DeserializeOwned + 'static,
    {
        EdgeFrom::new(src.into())
    }

    /// 从动态/静态节点名出发构造失败边（建图期检查类型指纹）。
    pub fn failure<SrcIn>(src: impl Into<Arc<str>>) -> EdgeFrom<Failure, SrcIn>
    where
        SrcIn: DeserializeOwned + 'static,
    {
        EdgeFrom::new(src.into())
    }

    pub(super) fn from_built(
        src: Arc<str>,
        dst: Arc<str>,
        slot: Option<EdgeSlot>,
        mode: EdgeMode,
        src_type: TypeFingerprint,
        dst_type: TypeFingerprint,
        built: BuiltPlanner,
    ) -> Self {
        Self {
            kind: built.kind,
            src,
            dst,
            slot,
            mode,
            has_transform: built.has_transform,
            has_condition: built.has_condition,
            src_type,
            dst_type,
            planner: built.planner,
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
        self.planner.plan_success(&self.src, out)
    }

    pub fn plan_failure(&self, input: &Value, err: &NodeError) -> Result<Vec<Value>, EdgeError> {
        self.planner.plan_failure(&self.src, input, err)
    }
}
