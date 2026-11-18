//! 节点抽象：强类型输入/输出 + 类型擦除。
//!
//! [`Node`] 是业务节点实现的接口，`In`/`Out` 支持序列化以便落库（对应 DB `task.input`/`task.output`）。
//! 节点**不携带名字**（无 `PHASE` 常量），阶段名由 [`crate::graph::Graph`]
//! 作为 `HashMap` 的 key 在注册实例时外部绑定。
//!
//! 节点错误统一为 [`NodeError`]：区分「可重试」与「永久失败」，供调度层决定退避重试还是立即失败。
//!
//! 子模块划分：
//! - [`join`]：显式 JOIN 汇聚节点（[`JoinNode`]），引擎通用能力，把多前驱输入合并透传。

use futures::future::BoxFuture;
use std::any::TypeId;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::task::TaskRecord;

mod join;

pub use join::JoinNode;

/// `Node` 的上下文类型参数：`Ctx` 业务侧实例化为 `NodeContext`。
/// 错误统一为 [`NodeError`]。
///
/// `In`/`Out` 的 `'static` 边界（不含非静态借用，非「永生」）由 build 期接线指纹校验
/// 要求：`TypeId::of` 只对 `'static` 类型可用。节点载荷经
/// `Value` 落库，本就不含借用，此约束不构成实际限制。
pub trait Node<Ctx>: Send + Sync {
    /// 任务输入类型，对应 DB `task.input`。
    type In: Serialize + DeserializeOwned + 'static;
    /// 任务输出类型，对应 DB `task.output`。
    type Out: Serialize + DeserializeOwned + 'static;
    /// 该节点产出的任务类型。默认普通节点；[`JoinNode`] 覆盖为 [`TaskType::Join`]。
    const TASK_TYPE: TaskType = TaskType::Normal;
    /// 异步执行：消费输入，产出序列化输出。失败返回 [`NodeError`]。
    ///
    /// 返回 `impl Future + Send` 以便擦除后可在 tokio 任务中跨线程移动。
    fn execute(
        &self,
        ctx: &mut Ctx,
        input: Self::In,
    ) -> impl Future<Output = Result<Self::Out, NodeError>> + Send;

    // ---- 任务生命周期钩子（可选覆盖，不依赖节点实例） ----
    //
    // 钩子通过当前任务上下文发布事件。

    /// 任务开始回调（当前任务已注入 `ctx`），先于节点执行。
    ///
    /// 节点可在此自声明媒体类型并发布事件，`input` 为任务的原始输入。
    fn on_start(_ctx: &mut Ctx, _input: &Value) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// 执行成功且落库后回调。`out` 为该节点序列化输出（`Value`）
    fn on_success(_ctx: &mut Ctx, _out: &Value) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// 执行失败后回调。`err` 为节点错误，发生在任务落库为失败之前。
    fn on_error(_ctx: &mut Ctx, _err: &NodeError) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// 节点注册时返回的类型化标识，携带名字与由实例推导的节点、上下文类型。
/// DSL 用它接线，编译器据此校验转换函数与节点的输入输出是否一致。
pub struct NodeKey<Ctx, N> {
    name: &'static str,
    marker: PhantomData<fn(Ctx) -> N>,
}

impl<Ctx, N> Copy for NodeKey<Ctx, N> {}
impl<Ctx, N> Clone for NodeKey<Ctx, N> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Ctx, N> NodeKey<Ctx, N> {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    pub fn name(self) -> &'static str {
        self.name
    }
}

/// 类型指纹：`TypeId`（判等依据）+ 类型名（报错展示）。
///
/// 命令式建图路径（非 DSL）没有编译期接线检查，[`crate::graph::Graph::validate`] 在
/// build 期用本指纹把「边声明的载荷类型」与「节点注册的 `In`/`Out` 类型」对齐，把类型
/// 错线提前暴露为确定性装配错误（此前要到任务结算时才以 serde 错误暴露）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeFingerprint {
    /// `TypeId`：判等依据。
    id: TypeId,
    /// `std::any::type_name`：报错展示。
    name: &'static str,
}

impl TypeFingerprint {
    /// 取 `T` 的类型指纹。
    pub fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// `TypeId`，供兼容判定。
    pub fn id(&self) -> TypeId {
        self.id
    }

    /// 类型名，供报错展示。
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// 擦除后的节点接口：对 `Value` 执行，隐藏具体 `In`/`Out`。
///
/// 图直接持有构造完成的节点实例。
pub trait AnyNode<Ctx>: Send + Sync {
    fn task_type(&self) -> TaskType;
    fn in_type(&self) -> TypeFingerprint;
    fn out_type(&self) -> TypeFingerprint;
    fn on_start<'a>(&'a self, ctx: &'a mut Ctx, input: &'a Value) -> BoxFuture<'a, ()>;
    fn on_success<'a>(&'a self, ctx: &'a mut Ctx, output: &'a Value) -> BoxFuture<'a, ()>;
    fn on_error<'a>(&'a self, ctx: &'a mut Ctx, error: &'a NodeError) -> BoxFuture<'a, ()>;

    /// `name` 为节点名，仅用于错误信息。
    fn execute<'a>(
        &'a self,
        name: &'a str,
        ctx: &'a mut Ctx,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, NodeError>> + Send + 'a>>;
}

/// 调度器对节点上下文的最小契约：调度器只依赖此接口驱动节点，不感知具体上下文类型。
/// 每次执行都接收统一的 [`TaskRecord`]，由业务上下文按需读取任务信息。
pub trait NodeContext: Send {
    /// 从共享基座按任务复制一份上下文（共享字段借用同一 `Arc`，仅瞬态字段随任务注入）。
    ///
    /// 异步：每个被认领的任务在执行前都会走一次，因此这是业务侧准备或刷新任务所需上下文的
    /// 天然位置（例如重新读取配置、冻结快照）。失败由实现自行降级（沿用上一份可用上下文）：
    /// 任务已经认领，返回错误只会白烧一次尝试预算。
    fn fork(&self, task: &TaskRecord) -> impl Future<Output = Self> + Send;
}

impl<Ctx, N> AnyNode<Ctx> for N
where
    Ctx: Send,
    N: Node<Ctx> + 'static,
{
    fn task_type(&self) -> TaskType {
        N::TASK_TYPE
    }
    fn in_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<N::In>()
    }
    fn out_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<N::Out>()
    }
    fn on_start<'a>(&'a self, ctx: &'a mut Ctx, input: &'a Value) -> BoxFuture<'a, ()> {
        Box::pin(N::on_start(ctx, input))
    }
    fn on_success<'a>(&'a self, ctx: &'a mut Ctx, output: &'a Value) -> BoxFuture<'a, ()> {
        Box::pin(N::on_success(ctx, output))
    }
    fn on_error<'a>(&'a self, ctx: &'a mut Ctx, error: &'a NodeError) -> BoxFuture<'a, ()> {
        Box::pin(N::on_error(ctx, error))
    }

    fn execute<'a>(
        &'a self,
        name: &'a str,
        ctx: &'a mut Ctx,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, NodeError>> + Send + 'a>> {
        Box::pin(async move {
            let input: N::In = serde_json::from_value(input)
                .map_err(|e| NodeError::permanent(format!("bad {} input: {e}", name)))?;
            let out = N::execute(self, ctx, input).await?;
            serde_json::to_value(out)
                .map_err(|e| NodeError::permanent(format!("serialize {} output: {e}", name)))
        })
    }
}

/// 图节点统一的错误类型：区分「可重试」与「永久失败」。
///
/// - [`NodeError::Retryable`]：瞬时错误（如 LLM 调用超时、网络抖动），按退避重试。
/// - [`NodeError::Permanent`]：确定性错误（如序列化失败、意外输出），重试无意义。
///
/// `From<String>` 默认映射为 [`NodeError::Retryable`]，使经 `?` 传播的 exec 层错误
/// （多为瞬时调用）无需显式标注；确定性错误请在构造处用 [`NodeError::permanent`] 显式声明。
#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeError {
    /// 可重试的瞬时错误。
    #[error("{0}")]
    Retryable(String),
    /// 永久失败的确定性错误。
    #[error("{0}")]
    Permanent(String),
}

impl NodeError {
    /// 构造一个可重试错误。
    pub fn retryable(msg: impl Into<String>) -> Self {
        NodeError::Retryable(msg.into())
    }
    /// 构造一个永久失败错误。
    pub fn permanent(msg: impl Into<String>) -> Self {
        NodeError::Permanent(msg.into())
    }
    /// 是否可重试。
    pub fn is_retryable(&self) -> bool {
        matches!(self, NodeError::Retryable(_))
    }
}

/// 默认把 `String` 错误视为可重试（`?` 传播的 exec 层错误以瞬时为主）。
impl From<String> for NodeError {
    fn from(s: String) -> Self {
        NodeError::Retryable(s)
    }
}

/// 节点产出的任务类型。该类型决定存储层如何调度该任务（如 JOIN 门需等齐静态前驱）。
///
/// 由节点经 [`Node::TASK_TYPE`] 常量声明，作为**类型级自描述**（编译期确定），
/// 而非运行时询问。DB `tasks.task_type` 列以 [`TaskType::as_str`] 的字符串承载。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    /// 普通节点：无需等齐前驱，fan-in 任一前驱成功即触发。
    Normal,
    /// JOIN 汇聚门：需等全部静态前驱（`depends_on_names`）成功后才放行。
    Join,
}

impl TaskType {
    /// 落库用的字符串表示（对应 `tasks.task_type` 列值）。
    pub const fn as_str(self) -> &'static str {
        match self {
            TaskType::Normal => "normal",
            TaskType::Join => "join",
        }
    }
}
