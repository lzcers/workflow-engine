//! 强类型 JOIN 输入槽位与通用汇合透传节点。
//!
//! 业务结构体通过 `#[derive(JoinInput)]` 声明字段槽位：
//! - 普通字段 `T`：单值必填槽（`MergeMode::Set`，建图期须有入边，禁止 `optional` 边与 `->*`）
//! - `Option<T>`：单值可选槽（`MergeMode::Set`，建图期可无入边，允许 `optional` 边，禁止 `->*`）
//! - `#[join(collect)] Vec<T>`：聚合必填槽（`MergeMode::Append`，始终预置为 JSON 数组 `[]`，建图期须有入边，允许 `optional` 边与 `->*`）
//! - `#[join(collect, optional)] Vec<T>`：聚合可选槽（`MergeMode::Append`，建图期可无入边，允许 `optional` 边与 `->*`）
//!
//! # JOIN 上游场景全览
//!
//! 运行时按 `(workflow_id, run_id, scope_id, name)` 查找或创建状态为 `collecting` 的 JOIN 累加器；
//! 每次上游结算事务（无论是否产生后继）都会按以下三步裁决本次运行内的 `collecting` JOIN：
//! 1. **实时失败传播**：任一有效外部入口或本作用域（含后代）内的上游处于 `failed` → 立即标 `failed`；
//! 2. **活跃上游等待**：仍有任一有效外部入口或本作用域（含后代）内的上游处于活跃态（`pending`/`collecting`/`running`，排除自身）→ 保持 `collecting`；
//! 3. **终态贡献验收**：所有上游终结后，逐个检查有效直接成功前驱是否至少存在一条有效贡献
//!    （状态为 `succeeded` 或经 `on_error` 直接贡献到本 JOIN 的 `superseded`，且任务 ID 在 `parent_task_ids` 中）→
//!    必需前驱有缺失则标 `failed`（杜绝僵尸 `collecting`），全部满足则转 `pending` 固化输入。
//!
//! ## 1. 入边拓扑与合并模式
//! - **单源直连（`a -> join::slot`）**：整份输出写入目标槽位（`Set` 覆盖，`Append` 追加一项）。
//! - **共享展开入槽（`a ->* join::slot`）**：将 `Vec<T>` 展开为多份贡献并在同一次结算中批量追加，
//!   **仅允许指向 `Append` 槽位**（建图期拒绝指向 `Set` 槽位）。
//! - **独立展开入槽（`a ->+ sub.join::slot`）**：**建图期禁止**；`->+` 须先进入子图内非 JOIN 节点，再在子图内部汇合。
//! - **多前驱写不同槽位**：最典型多源汇合；首个到达创建 `collecting` 记录并预置全部 `Append` 槽位为 `[]`，
//!   后续到达按结算顺序合入并去重追加 `parent_task_ids`。
//! - **多前驱写同一槽位**：
//!   - `Append` 槽位：允许任意多源/多实例追加，元素顺序取决于结算到达时序（不保证前驱顺序）。
//!   - `Set` 槽位：本作用域（含后代子图，`src_path.starts_with(&dst_path)`）内**禁止**多条无条件且非 `optional` 的成功边共写同一槽位；
//!     允许通过 `when(...)` 条件分流、`optional`、多个外部备选入口、或同一前驱的成功边 + `on_error` 降级边写入
//!     （运行时若多写者同时命中，后结算者覆盖先结算者）。
//! - **同一前驱多条成功边指向同一 JOIN**：可写入不同槽位或按条件分流，但同一 `(src, dst)` 的多条成功边**禁止混用** `optional` 与非 `optional`；
//!   单次结算内按边依次合并，`parent_task_ids` 按源任务 ID 去重。
//! - **上游扇出多实例（1 对 N 汇聚）**：上游经 `->*` 产生多个同名直接前驱实例（或经 `->+` 产生多个子图实例退出到父层 JOIN）时，
//!   JOIN 等待全部实例终结后再验收；`Append` 收齐全部贡献，`Set` 保留最后结算者的值。
//! - **间接上游与延迟生产**：`upstreams` 沿成功边与失败边反向遍历全图（排除自身）；只要本作用域（含后代）内仍有间接上游在途，
//!   即使已存在的直接前驱都已完成，JOIN 仍继续等待后续可能被产出的前驱实例。
//! - **链式 / 级联 JOIN（`join1 -> join2`）**：上游 `join1` 处于 `collecting` 时属于活跃上游，`join2` 继续等待；
//!   单次结算事务内通过不动点循环级联传播失败与缺前驱终态（`join1` 失败会当场将 `collecting` 态的 `join2` 一并标 `failed`）。
//! - **无上游 / 直接提交入口**：通过 `Workflow::submit` 或 `Database::submit` 直接以 JOIN 为入口时，
//!   跳过 `collecting` 直接以 `pending` 入队并按提交载荷执行；不同 `submit` 对应不同 `run_id`，彼此隔离，不能跨次提交补齐同一 JOIN。
//!
//! ## 2. 作用域与子图边界
//! - **同作用域前驱（`src_path == dst_path`）**：在当前 `scope_id` 及其后代作用域内检查直接前驱与上游；
//!   不同子图实例（不同 `scope_id`）的 JOIN 完全隔离，各自独立收集、就绪与失败级联。
//! - **子图退出汇入父层 JOIN（`src_path` 为 `dst_path` 的严格后代）**：父层 JOIN 的本地作用域集合包含自身 `scope_id` 及全部后代子图实例，
//!   会等待所有子图实例中的相关上游终结并汇聚其跨出贡献。
//! - **外部前驱跨入子图内 JOIN（`!src_path.starts_with(&dst_path)`）**：通过作用域链上的 `trigger_task_id`（按公共前缀深度 `entry_depth` 定位）
//!   精准绑定触发该子图实例的单个外部任务，不等待也不混入其他外部调用或兄弟实例；
//!   同一外部任务单次结算内跨入同一子图的多条 `->` / `->*` 边共享同一 `scope_id`。
//! - **多外部备选入口指向同一子图内 JOIN**：仅将名称与当前实例 `trigger_task_id` 匹配的外部前驱纳入验收，
//!   其余未触发本实例的外部入口自动忽略（其运行或失败不影响本实例），子图内部本地前驱仍全部验收。
//! - **独立跨入边界与手动直入子图**：两个不同外部节点分别跨入同一子图时会各自新建 `scope_id`，不会落入同一子图内 JOIN
//!   （若需合并两者应先在子图外 JOIN）；通过 `submit("sub.entry", ...)` 手动直入子图时，`trigger_task_id` 为入口节点自身，
//!   外部入口前驱不匹配会被跳过，仅验收子图内本地前驱。
//!
//! ## 3. 条件跳过、空扇出与 `optional`
//! - **`when` 条件未命中或空展开（`->*` / `->+` 收到空列表 `[]`）**：均不产生后继，也不会写入 JOIN 或其 `parent_task_ids`；
//!   若跳过发生在更上游，直接前驱任务甚至不会被创建。
//! - **成功边标记 `optional`（目标槽位须为 `Option<T>` 或 `#[join(collect)] Vec<T>`）**：
//!   - 已有其他前驱创建 `collecting` JOIN 时，该前驱无贡献不阻塞 JOIN；终结后 `Option<T>` 反序列化为 `None`，`#[join(collect)] Vec<T>` 为预置的 `[]`。
//!   - 运行时是否允许跳过仅由**边上的 `optional` 修饰符**决定；即使槽位为 `Option<T>`，入边未标 `optional` 时前驱跳过仍会判为必需前驱缺失。
//!   - `optional` 仅容忍「跳过 / 无贡献」，**不容忍上游异常 `failed`**；上游一旦 `failed` 仍会触发实时失败传播。
//! - **成功边未标记 `optional`（默认必需前驱）**：所有上游终结后若该前驱无任何有效贡献，立即将 `collecting` JOIN 标为 `failed`
//!   （`upstream dependency did not contribute`）。
//! - **全部入边均被跳过（零贡献）**：从未有任何贡献到达时不会创建 JOIN 任务行，JOIN 不会执行。
//! - **多实例前驱部分命中、部分跳过**：只要该前驱至少有一个同作用域实例有效贡献（进入 `parent_task_ids`），
//!   其余实例未命中 `when` 仍视为必需前驱已满足；全部实例均未贡献则按上述跳过规则裁决。
//!
//! ## 4. 上游失败、重试、降级与恢复
//! - **可重试错误（`NodeError::Retryable` 且 `attempt < MAX_ATTEMPTS`）或租约失效重认领**：
//!   源任务退避置回 `pending` 或保持 `running` 等待重认领，未进入终态，JOIN 继续保持 `collecting` 等待。
//! - **上游永久失败且无失败后继（`status = failed`）**：
//!   - 覆盖 `NodeError::Permanent`、耗尽重试预算（`MAX_ATTEMPTS = 3`）、成功后继规划出错、以及租约过期后预算耗尽的恢复结算。
//!   - 立即触发实时失败传播，将同作用域依赖它的 `collecting` JOIN 标为 `failed`（`upstream dependency failed`），不等其他在途分支。
//!   - **先失败、后到达**：若某上游先 `failed`，后续其他分支才首次创建 `collecting` JOIN，同一次结算事务内的推进检查会当场将其标为 `failed`。
//!   - **不触发 JOIN 自身失败边**：在 `collecting` 阶段被级联标 `failed` 的 JOIN 未进入调度执行，不会触发挂在 JOIN 自身的 `on_error` 边或 `Node::on_error` 回调
//!     （仅转为 `pending` 并被认领执行后失败时才会触发）。
//! - **上游失败被 `on_error` / `when_error` 接管（源任务标 `superseded`，不触发实时失败传播）**：
//!   - **直连降级入槽（`src -> join::slot on_error`）**：失败边向本 JOIN 写入降级值并将源任务 ID 记入 `parent_task_ids`；
//!     终态验收时「`superseded` 且位于 `parent_task_ids`」计为有效贡献，JOIN 正常就绪。
//!   - **回跳重做 / 恢复链路（`src -> recovery on_error` 且恢复路径可再达 JOIN）**：恢复链路位于 `upstreams` 反向可达集合内且处于活跃态，
//!     JOIN 继续等待；恢复链路产出新前驱成功贡献后 JOIN 正常就绪，若恢复链路失败或仍无贡献则 JOIN 标 `failed`。
//!   - **分流到无关旁路（`src -> alert on_error` 且旁路不可达 JOIN）**：旁路不在 `upstreams` 内，JOIN 不等待旁路，也不因 `superseded` 立即失败；
//!     待所有上游终结后，该 `superseded` 任务不在 `parent_task_ids` 中（不计有效贡献）：若无其他实例贡献，必需前驱导致 JOIN 标 `failed`
//!     （`upstream dependency did not contribute`），`optional` 前驱则放行；若该前驱另有其他实例成功贡献，则必需前驱已满足，JOIN 正常就绪。
//!   - **仅失败边连入 JOIN（无对应成功边）**：建图期计入槽位覆盖且 `upstreams` 追踪其上游，但 `predecessors` 仅收录成功边；
//!     若上游成功未走失败边，JOIN 不会在推进阶段报缺前驱（普通必填槽位 `T` 将在执行反序列化输入时因缺字段返回永久错误）。
//!
//! ## 5. 并发结算、空后继唤醒与多轮汇合
//! - **并发首次到达不重复建行**：PostgreSQL 对含 JOIN 的图在结算前先取运行级事务锁（`pg_advisory_xact_lock` + `READ COMMITTED`），
//!   SQLite 依赖写事务锁串行化，保证多前驱并发首次到达只创建一条 `collecting` JOIN 且避免死锁。
//! - **末位上游空后继唤醒**：即便最后终结的上游（如 `when` 跳过、空展开或间接上游收尾）没有任何后继输出，
//!   结算事务末尾仍会执行 JOIN 推进，及时将等待中的 JOIN 转为 `pending` 或 `failed`。
//! - **就绪固化与多轮汇合**：JOIN 转为 `pending` 后输入即固定（自身重试或租约恢复沿用同一输入）。
//!   若后续因失败回边重跑上游再次产生贡献，因仅匹配 `status = 'collecting'`，会新建下一轮 `collecting` JOIN，不修改旧实例；
//!   但若同一作用域内历史残留 `failed` 上游，新一轮 `collecting` JOIN 仍会被实时失败传播判为 `failed`。

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
