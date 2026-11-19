//! 执行作用域入口：透传输入，每个任务及其下游使用独立的 JOIN 判定域。
//!
//! 存储在任务入队时将 `scope_id` 设为该任务的 `task_id`；后继继承该值，
//! 直到再次进入 Scope 节点或经过 foreach 边。`run_id` 保持不变，因此整个运行仍可统一观测。
//! 进入同一 JOIN 的所有分支必须位于同一作用域内。

use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};

use super::{Node, NodeError, TaskType};

/// 为每份输入建立独立执行作用域的纯透传节点。
pub struct ScopeNode<T> {
    marker: PhantomData<fn() -> T>,
}

impl<T> ScopeNode<T> {
    pub const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T> Default for ScopeNode<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Ctx, T> Node<Ctx> for ScopeNode<T>
where
    Ctx: Send,
    T: Serialize + DeserializeOwned + Send + 'static,
{
    type In = T;
    type Out = T;

    const TASK_TYPE: TaskType = TaskType::Scope;

    async fn execute(&self, _ctx: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}
