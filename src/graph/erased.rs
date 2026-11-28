//! 保留原始图对象，只隐藏上下文类型的查询与校验接口。

use std::any::Any;

use serde_json::Value;

use super::{Graph, GraphDescription, TaskType, Trigger};
use crate::{error::EngineError, task::Task};

/// 工作流持有的原始图接口，可通过 [`crate::Workflow::graph`] 访问。
pub trait ErasedGraph: Any + Send + Sync {
    /// 工作流的稳定身份。
    fn workflow_id(&self) -> &str;

    /// 工作流的展示标题。
    fn title(&self) -> &str;

    /// 描述图的节点、边、子图和触发声明。
    fn describe(&self) -> GraphDescription;

    /// 节点声明的任务类型；节点不存在时返回 None。
    fn task_type_of(&self, name: &str) -> Option<TaskType>;

    /// 节点所属子图作用域路径；不存在为 None，根节点为 Some([])。
    fn subgraph_path(&self, name: &str) -> Option<Vec<String>>;

    /// 在入队前按目标节点的 `In` 类型预校验输入 JSON。
    fn validate_input(&self, name: &str, input: &Value) -> Result<(), EngineError>;

    /// 构造并强校验入口任务（O(1) 无全图克隆），供宿主事务提交使用。
    fn task(&self, entry: &str, input: Value) -> Result<Task, EngineError>;

    /// 按节点名排序的外部触发声明。
    fn triggers(&self) -> Vec<(&str, Trigger)>;
}

impl<Ctx: Send + Sync + 'static> ErasedGraph for Graph<Ctx> {
    fn workflow_id(&self) -> &str {
        Graph::workflow_id(self)
    }

    fn title(&self) -> &str {
        Graph::title(self)
    }

    fn describe(&self) -> GraphDescription {
        Graph::describe(self)
    }

    fn task_type_of(&self, name: &str) -> Option<TaskType> {
        Graph::task_type_of(self, name)
    }

    fn subgraph_path(&self, name: &str) -> Option<Vec<String>> {
        Graph::subgraph_path(self, name)
    }

    fn validate_input(&self, name: &str, input: &Value) -> Result<(), EngineError> {
        Graph::validate_input(self, name, input)
    }

    fn task(&self, entry: &str, input: Value) -> Result<Task, EngineError> {
        Graph::task(self, entry, input)
    }

    fn triggers(&self) -> Vec<(&str, Trigger)> {
        Graph::triggers(self)
    }
}
