//! 子图实例的父子关系；不赋予 scope 独立执行生命周期。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ScopeRecord {
    pub workflow_id: String,
    pub scope_id: i64,
    pub run_id: Option<i64>,
    pub subgraph_id: String,
    pub parent_scope_id: Option<i64>,
    pub trigger_task_id: i64,
}

/// 同一次原子结算中的普通跨入边共享入口；foreach 逐项创建实例，不使用缓存。
pub(crate) type ScopeEntries = HashMap<(Option<i64>, String), i64>;
