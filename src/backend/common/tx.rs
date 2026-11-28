//! 后端事务 SQL 方言原语契约：PG 与 SQLite 分别实现此最小原语集。

use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use serde_json::Value;

use crate::{
    error::StorageError,
    task::{
        TaskStatus,
        join::{RunScope, RunTask},
        scope::{ScopeEntries, ScopeRecord},
        store::FanoutSource,
    },
};

/// 待批量插入的一条任务行。
pub(crate) struct NewTaskRow<'a> {
    pub dst: &'a str,
    pub status: TaskStatus,
    pub scope_id: Option<i64>,
    pub input: &'a Value,
    pub max_attempts: i32,
}

/// 单个后端事务内的最小 SQL 原语集。
pub(crate) trait BackendTx {
    /// 统计本次运行已创建的任务总数（仅在配置 `max_tasks_per_run > 0` 且需新建任务时调用）。
    fn count_run_tasks<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<usize, StorageError>>;

    /// 单条 `WITH RECURSIVE` 加载当前作用域到根的完整父子链（从最外层到最内层）。
    fn load_scope_chain<'a>(
        &'a mut self,
        workflow_id: &'a str,
        scope_id: Option<i64>,
    ) -> BoxFuture<'a, Result<Vec<ScopeRecord>, StorageError>>;

    /// 沿路径逐级进入共享作用域，利用 `shared` 缓存在单次结算内复用新建实例。
    fn enter_shared_scope<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [Arc<str>],
        shared: &'a mut ScopeEntries,
    ) -> BoxFuture<'a, Result<Option<i64>, StorageError>>;

    /// 为 `->!` / `->+` 的 `count` 份输入批量创建 `count` 条独立作用域链，返回每份输入的叶 `scope_id`。
    fn enter_isolated_scopes_batch<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [Arc<str>],
        count: usize,
    ) -> BoxFuture<'a, Result<Vec<i64>, StorageError>>;

    /// 批量插入一批任务行（PG 使用 `UNNEST`，SQLite 使用多行 `VALUES`）。
    fn insert_tasks_batch<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        rows: &'a [NewTaskRow<'a>],
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 查找并锁定指定 `(workflow_id, run_id, scope_id, name)` 下状态为 `collecting` 的 JOIN 行。
    fn find_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<(i64, Vec<i64>, Value)>, StorageError>>;

    /// 新建一条 `collecting` JOIN 任务行，返回其 `task_id`。
    fn insert_collecting_join<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        dst: &'a str,
        scope_id: Option<i64>,
        input: &'a Value,
        max_attempts: i32,
    ) -> BoxFuture<'a, Result<i64, StorageError>>;

    /// 更新已有 `collecting` JOIN 的 `parent_task_ids` 与累加器对象 `input_json`。
    fn update_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_id: i64,
        parents: &'a [i64],
        input: &'a Value,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 向 `join_contributions` 批量 O(1) 追加 `Append` 槽位的贡献条目。
    fn append_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        join_task_id: i64,
        slot_key: &'a str,
        source_task_id: i64,
        values: &'a [Value],
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 短路探测：返回 `check_scopes` 中实际存在 `collecting` JOIN 的作用域集合（空则跳过全量加载）。
    fn find_collecting_scopes<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        check_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<Vec<Option<i64>>, StorageError>>;

    /// 按需加载与 `collecting_scopes` 相关的最小任务和作用域快照：
    /// 若含根作用域 `None` 则加载整次运行；否则仅加载对应子树作用域及外部触发任务。
    fn load_join_snapshot<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        collecting_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<(Vec<RunTask>, Vec<RunScope>), StorageError>>;

    /// 批量标记级联失败的 JOIN 并清理其 `join_contributions`。
    fn fail_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        failed: &'a [(i64, &'static str)],
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 读取待就绪 JOIN 当前的累加器对象 `input_json`。
    fn load_task_inputs<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, Value>, StorageError>>;

    /// 批量读取待就绪 JOIN 在 `join_contributions` 中按 `seq` 排序的贡献条目。
    fn load_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, HashMap<String, Vec<Value>>>, StorageError>>;

    /// 批量将无 `Append` 物化需求的 JOIN 直接转为 `pending`。
    fn mark_joins_pending<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// 将物化好完整 `input_json` 的 JOIN 转为 `pending` 并清理其 `join_contributions`。
    fn materialize_and_ready_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        materialized: &'a [(i64, Value)],
    ) -> BoxFuture<'a, Result<(), StorageError>>;
}
