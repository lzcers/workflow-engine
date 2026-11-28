//! 节点抽象：强类型输入/输出、角色声明、外部触发声明与上下文生命周期钩子。
//!
//! [`Node`] 是业务节点实现的接口，`In`/`Out` 支持序列化以便落库（对应 DB `task.input`/`task.output`）。
//! 节点**不携带名字**，节点名由 [`crate::graph::Graph`] 在注册实例时外部绑定。
//! 跨节点的观测钩子（`on_start` / `on_success` / `on_error`）统一收敛在 [`NodeContext`] 上。

use std::{any::TypeId, future::Future, marker::PhantomData, sync::Arc};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::task::TaskRecord;

mod join;

pub(crate) use join::EdgeSlot;
pub use join::{JoinInput, JoinNode, JoinSlot, MergeMode, SlotSpec};

/// 外部触发器的注册声明。种类与配置由宿主解释，引擎只负责描述和提交后的任务执行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    /// 宿主识别的触发器种类，例如 `cron` 或 `webhook`。
    pub kind: String,
    /// 对应种类的注册配置，例如 cron 表达式与时区。
    pub config: Value,
}

/// 节点在图中的执行角色（以单一 ADT 消除非法组合状态）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    /// 普通节点：任一入边到达即独立执行。
    Normal,
    /// JOIN 汇聚节点：按槽位收集同作用域贡献，等待上游终结且必填槽位满足后执行。
    Join(Vec<SlotSpec>),
}

impl NodeRole {
    pub fn task_type(&self) -> TaskType {
        match self {
            Self::Normal => TaskType::Normal,
            Self::Join(_) => TaskType::Join,
        }
    }
}

/// 节点声明的执行任务类型，供图描述输出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    /// 普通节点：无需等齐前驱，fan-in 任一前驱成功即触发。
    Normal,
    /// JOIN 汇聚门：按槽位和作用域收集来源并等待上游终结。
    Join,
}

/// 业务节点接口：所有方法均以 `&self` 保持对称。
pub trait Node<Ctx>: Send + Sync {
    /// 任务输入类型，对应 DB `task.input`。
    type In: Serialize + DeserializeOwned + 'static;
    /// 任务输出类型，对应 DB `task.output`。
    type Out: Serialize + DeserializeOwned + 'static;

    /// 节点执行角色；默认普通节点，JOIN 节点返回 `NodeRole::Join(Self::In::slot_specs())`。
    fn role(&self) -> NodeRole {
        NodeRole::Normal
    }

    /// 外部触发器的静态注册声明（无副作用，与入边正交）。
    fn trigger(&self) -> Option<Trigger> {
        None
    }

    /// 异步执行：消费输入，产出序列化输出。失败返回 [`NodeError`]。
    fn execute(
        &self,
        ctx: &mut Ctx,
        input: Self::In,
    ) -> impl Future<Output = Result<Self::Out, NodeError>> + Send;
}

/// 节点注册时返回的类型化标识，携带动态/静态名称与推导的节点、上下文类型。
/// 供 `workflow!` 宏和类型化建边接口在编译期校验边两端的输入输出类型。
pub struct NodeKey<Ctx, N> {
    name: Arc<str>,
    marker: PhantomData<fn(Ctx) -> N>,
}

impl<Ctx, N> Clone for NodeKey<Ctx, N> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            marker: PhantomData,
        }
    }
}

impl<Ctx, N> NodeKey<Ctx, N> {
    pub(crate) fn new(name: Arc<str>) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn arc_name(&self) -> Arc<str> {
        self.name.clone()
    }
}

impl<Ctx, N> NodeKey<Ctx, N>
where
    N: Node<Ctx>,
    N::In: JoinInput,
{
    /// 返回目标 JOIN 节点的强类型字段槽位表，供 `workflow!` 宏和类型化建边按字段接线。
    pub fn slots(&self) -> <N::In as JoinInput>::Slots {
        <N::In as JoinInput>::slots()
    }
}

/// 节点/边端口的编译期类型指纹：`TypeId` 做等值比对，`type_name` 用于错误信息与图描述。
#[derive(Clone, Copy, Debug)]
pub(crate) struct TypeFingerprint {
    id: TypeId,
    name: &'static str,
}

impl PartialEq for TypeFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for TypeFingerprint {}

impl TypeFingerprint {
    pub(crate) fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    pub(crate) fn id(self) -> TypeId {
        self.id
    }

    pub(crate) fn name(self) -> &'static str {
        self.name
    }
}

/// 擦除后的节点接口：对 `Value` 执行，隐藏具体 `In`/`Out`。
pub(crate) trait AnyNode<Ctx>: Send + Sync {
    fn role(&self) -> NodeRole;
    fn trigger(&self) -> Option<Trigger>;
    fn in_type(&self) -> TypeFingerprint;
    fn out_type(&self) -> TypeFingerprint;
    fn validate_input(&self, input: &Value) -> Result<(), serde_json::Error>;

    /// `name` 为节点名，仅用于错误信息。
    fn execute<'a>(
        &'a self,
        name: &'a str,
        ctx: &'a mut Ctx,
        input: Value,
    ) -> BoxFuture<'a, Result<Value, NodeError>>;
}

/// 调度器对任务上下文的契约：承载每任务上下文派发与统一生命周期观测钩子。
///
/// 钩子在 `NodeContext` 上实现一次即覆盖整张图的所有节点，避免在每个 `Node` 实现中重复样板。
pub trait NodeContext: Send {
    /// 从共享基座按任务派发独立上下文（共享字段借用同一 `Arc`，瞬态字段随任务注入）。
    fn fork(&self, task: &TaskRecord) -> impl Future<Output = Self> + Send;

    /// 任务开始执行前回调。
    fn on_start(&mut self, _task: &TaskRecord) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// 成功结算落库后回调（不占用执行槽，不阻塞后继认领）。
    fn on_success(
        &mut self,
        _task: &TaskRecord,
        _output: &Value,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// 最终失败（`failed`/`superseded`）结算落库后回调（不占用执行槽）。
    fn on_error(
        &mut self,
        _task: &TaskRecord,
        _error: &NodeError,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

impl<Ctx, N> AnyNode<Ctx> for N
where
    Ctx: Send,
    N: Node<Ctx> + 'static,
{
    fn role(&self) -> NodeRole {
        N::role(self)
    }

    fn trigger(&self) -> Option<Trigger> {
        N::trigger(self)
    }

    fn in_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<N::In>()
    }

    fn out_type(&self) -> TypeFingerprint {
        TypeFingerprint::of::<N::Out>()
    }

    fn validate_input(&self, input: &Value) -> Result<(), serde_json::Error> {
        <N::In as Deserialize>::deserialize(input).map(|_| ())
    }

    fn execute<'a>(
        &'a self,
        name: &'a str,
        ctx: &'a mut Ctx,
        input: Value,
    ) -> BoxFuture<'a, Result<Value, NodeError>> {
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
    pub fn retryable(msg: impl Into<String>) -> Self {
        NodeError::Retryable(msg.into())
    }

    pub fn permanent(msg: impl Into<String>) -> Self {
        NodeError::Permanent(msg.into())
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, NodeError::Retryable(_))
    }
}

/// 默认把 `String` 错误视为可重试（`?` 传播的外部调用错误以瞬时为主）。
impl From<String> for NodeError {
    fn from(s: String) -> Self {
        NodeError::Retryable(s)
    }
}
