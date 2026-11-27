//! 强类型 JOIN 输入槽位与通用汇合透传节点。
//!
//! 业务结构体通过 `#[derive(JoinInput)]` 声明字段槽位：
//! - 普通字段 `T`：单值必填槽（`MergeMode::Set`）
//! - `Option<T>`：单值可选槽（`MergeMode::Set`，允许 `optional` 边跳过）
//! - `#[join(collect)] Vec<T>`：多源/多实例聚合槽（`MergeMode::Append`，始终存为 JSON 数组）

use std::marker::PhantomData;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{Node, NodeError, TaskType, TypeFingerprint};

/// JOIN 槽位的合并方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeMode {
    /// 单值写入键值。
    Set,
    /// 始终以 JSON 数组保存，多次贡献逐项追加。
    Append,
}

/// 单个 JOIN 输入字段的静态规格，由 `#[derive(JoinInput)]` 自动生成。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotSpec {
    pub key: &'static str,
    pub merge: MergeMode,
    /// 是否要求图中至少有一条入边指向该槽位。
    pub required: bool,
    /// 是否允许 `optional` 边跳过而不产生反序列化缺字段错误（`Option<T>` 或 `#[join(collect)]`）。
    pub allows_optional: bool,
    pub(crate) item_type: TypeFingerprint,
}

impl SlotSpec {
    #[doc(hidden)]
    pub fn new<T: 'static>(
        key: &'static str,
        merge: MergeMode,
        required: bool,
        allows_optional: bool,
    ) -> Self {
        Self {
            key,
            merge,
            required,
            allows_optional,
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
    /// 单值必填槽位（`T`）。
    pub const fn set(key: &'static str) -> Self {
        Self {
            key,
            merge: MergeMode::Set,
            marker: PhantomData,
        }
    }

    /// 单值可选槽位（`Option<T>`）。
    pub const fn optional_set(key: &'static str) -> Self {
        Self {
            key,
            merge: MergeMode::Set,
            marker: PhantomData,
        }
    }

    /// 聚合必填槽位（`#[join(collect)] Vec<T>`）。
    pub const fn append(key: &'static str) -> Self {
        Self {
            key,
            merge: MergeMode::Append,
            marker: PhantomData,
        }
    }

    /// 聚合可选槽位（`#[join(collect, optional)] Vec<T>`）。
    pub const fn optional_append(key: &'static str) -> Self {
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

/// 可作为 `TaskType::Join` 节点输入的强类型结构体契约，通常由 `#[derive(JoinInput)]` 实现。
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

    const TASK_TYPE: TaskType = TaskType::Join;

    fn join_slots(&self) -> Option<Vec<SlotSpec>> {
        Some(T::slot_specs())
    }

    async fn execute(&self, _ctx: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}
