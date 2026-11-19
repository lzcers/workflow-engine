//! 保留原始图对象，只隐藏上下文类型的查询接口。

use std::{any::Any, collections::HashMap};

use super::{Graph, GraphDescription, TaskType, Trigger};

/// 工作流持有的原始图接口，可通过 [`crate::Workflow::graph`] 访问。
///
/// 查询直接读取原始图；需要上下文或其他具体图操作时，使用
/// [`crate::Workflow::graph_as`] 取回 `Graph<Ctx>`。
pub trait ErasedGraph: Any + Send + Sync {
    /// 工作流的稳定身份。
    fn workflow_id(&self) -> &str;

    /// 工作流的展示标题。
    fn title(&self) -> &str;

    /// 描述图的节点、边和触发声明。
    fn describe(&self) -> GraphDescription;

    /// 节点声明的任务类型；节点不存在时返回 None。
    fn task_type_of(&self, name: &str) -> Option<TaskType>;

    /// 静态子图路径；不存在的节点为 None，根节点为 Some([])。
    fn subgraph_path(&self, name: &str) -> Option<Vec<String>>;

    /// 直接成功前驱；不存在的节点为 None。
    fn predecessors_of(&self, name: &str) -> Option<Vec<String>>;

    /// 图构建时计算的 JOIN 传递上游，不在仓储中重复保存。
    fn join_upstreams(&self) -> &HashMap<String, Vec<String>>;

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

    fn predecessors_of(&self, name: &str) -> Option<Vec<String>> {
        Graph::predecessors_of(self, name)
    }

    fn join_upstreams(&self) -> &HashMap<String, Vec<String>> {
        Graph::join_upstreams(self)
    }

    fn triggers(&self) -> Vec<(&str, Trigger)> {
        Graph::triggers(self)
    }
}
