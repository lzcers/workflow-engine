//! 强类型 JOIN 输入槽位与通用汇合透传节点。
//!
//! 业务结构体通过 `#[derive(JoinInput)]` 声明字段槽位：
//! - 普通字段 `T`：单值必填槽（`MergeMode::Set`，`required = true`，禁止 `->*`）
//! - `Option<T>` 或 `#[join(optional)] T`：单值可选槽（`MergeMode::Set`，`required = false`，禁止 `->*`）
//! - `#[join(collect)] Vec<T>`：聚合必填槽（`MergeMode::Append`，`required = true`，允许 `->*`）
//! - `#[join(collect, optional)] Vec<T>`：聚合可选槽（`MergeMode::Append`，`required = false`，允许 `->*`）
//!
//! JOIN 就绪完全以**槽位满足度**与**作用域内上游状态**裁决：
//! 1. 实时失败传播：有效外部入口或本作用域（含后代）内本轮存在 `failed` 上游 → 立即标 `failed`；
//! 2. 活跃上游等待：仍有有效外部入口或本作用域（含后代）内上游处于活跃态（`pending`/`collecting`/`running`）→ 保持 `collecting`；
//! 3. 槽位验收：所有上游终结后，检查全部必填槽位（`required == true`）是否已收到有效贡献 → 任一缺失则标 `failed`，全部满足则物化 `Append` 槽位并转 `pending`。
//!
//! 槽位语义天然支持条件分支 XOR-Merge（多条 `when(...)` 边汇聚到同一必填槽 `T`）、
//! 同节点混合写入必填与可选槽，以及 `on_error` 降级边兜底必填槽。

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use super::{Node, NodeError, NodeRole, TypeFingerprint};
pub use crate::task::MergeMode;

/// 单个 JOIN 输入字段的静态规格，由 `#[derive(JoinInput)]` 自动生成。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotSpec {
    pub key: &'static str,
    pub merge: MergeMode,
    /// 必填槽位在建图期须有入边覆盖，运行期所有上游终结后须至少收到一次贡献。
    pub required: bool,
    pub(crate) item_type: TypeFingerprint,
}

impl SlotSpec {
    #[doc(hidden)]
    pub fn new<T: 'static>(key: &'static str, merge: MergeMode, required: bool) -> Self {
        Self {
            key,
            merge,
            required,
            item_type: TypeFingerprint::of::<T>(),
        }
    }
}

/// 类型擦除后的边目标槽位信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgeSlot {
    pub key: &'static str,
    pub merge: MergeMode,
}

/// 指向 `In` 中某个字段的编译期强类型槽位句柄，贡献项类型为 `Item`。
pub struct JoinSlot<In, Item> {
    key: &'static str,
    merge: MergeMode,
    marker: PhantomData<fn(In) -> Item>,
}

impl<In, Item> Copy for JoinSlot<In, Item> {}
impl<In, Item> Clone for JoinSlot<In, Item> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<In, Item> JoinSlot<In, Item> {
    /// 单值槽位（`T` 或 `Option<T>`）。
    pub const fn set(key: &'static str) -> Self {
        Self {
            key,
            merge: MergeMode::Set,
            marker: PhantomData,
        }
    }

    /// 聚合槽位（`#[join(collect)] Vec<T>`）。
    pub const fn append(key: &'static str) -> Self {
        Self {
            key,
            merge: MergeMode::Append,
            marker: PhantomData,
        }
    }

    pub const fn key(&self) -> &'static str {
        self.key
    }

    pub const fn merge(&self) -> MergeMode {
        self.merge
    }

    pub(crate) const fn edge_slot(&self) -> EdgeSlot {
        EdgeSlot {
            key: self.key,
            merge: self.merge,
        }
    }
}

/// 可作为 JOIN 节点输入的强类型结构体契约，通常由 `#[derive(JoinInput)]` 实现。
pub trait JoinInput: Serialize + DeserializeOwned + 'static {
    /// 按字段名暴露 [`JoinSlot`] 的零大小方法表。
    type Slots: Copy;

    fn slots() -> Self::Slots;

    fn slot_specs() -> Vec<SlotSpec>;
}

/// 通用强类型 JOIN 汇聚透传节点：收齐 `T` 的各个字段槽位后原样向下游输出 `T`。
pub struct JoinNode<T>(PhantomData<fn() -> T>);

impl<T> JoinNode<T> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for JoinNode<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Ctx: Send, T: JoinInput + Send> Node<Ctx> for JoinNode<T> {
    type In = T;
    type Out = T;

    fn role(&self) -> NodeRole {
        NodeRole::Join(T::slot_specs())
    }

    async fn execute(&self, _ctx: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}
