//! 后端事务 SQL 方言原语契约：PG 与 SQLite 分别实现此最小原语集（静态分发，零装箱）。

use std::{future::Future, sync::Arc};

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
}

/// 单个后端事务内的最小 SQL 原语集。
pub(crate) trait BackendTx {
    /// 单条 `WITH RECURSIVE` 加载当前作用域到根的完整父子链（从最外层到最内层）。
    fn load_scope_chain(
        &mut self,
        workflow_id: &str,
        scope_id: Option<i64>,
    ) -> impl Future<Output = Result<Vec<ScopeRecord>, StorageError>> + Send;

    /// 沿路径逐级进入共享作用域，利用 `shared` 缓存在单次结算内复用新建实例。
    fn enter_shared_scope(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &[Arc<str>],
        shared: &mut ScopeEntries,
    ) -> impl Future<Output = Result<Option<i64>, StorageError>> + Send;

    /// 为 `->!` / `->+` 的 `count` 份输入批量创建 `count` 条独立作用域链，返回每份输入的叶 `scope_id`。
    fn enter_isolated_scopes_batch(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &[Arc<str>],
        count: usize,
    ) -> impl Future<Output = Result<Vec<i64>, StorageError>> + Send;

    /// 批量插入一批任务行（PG 使用 `UNNEST`，SQLite 使用多行 `VALUES`）。
    fn insert_tasks_batch(
        &mut self,
        source: &FanoutSource<'_>,
        rows: &[NewTaskRow<'_>],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// 查找并锁定指定 `(workflow_id, run_id, scope_id, name)` 下状态为 `collecting` 的 JOIN 行。
    fn find_collecting_join(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &str,
    ) -> impl Future<Output = Result<Option<(i64, Vec<i64>, Value)>, StorageError>> + Send;

    /// 新建一条 `collecting` JOIN 任务行。
    fn insert_collecting_join(
        &mut self,
        source: &FanoutSource<'_>,
        dst: &str,
        scope_id: Option<i64>,
        input: &Value,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// 更新已有 `collecting` JOIN 的 `parent_task_ids` 与累加器对象 `input_json`。
    fn update_collecting_join(
        &mut self,
        workflow_id: &str,
        task_id: i64,
        parents: &[i64],
        input: &Value,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// 短路探测：当前运行是否存在 `collecting` 状态的 JOIN 行。
    fn has_collecting_join(
        &mut self,
        workflow_id: &str,
        run_id: i64,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;

    /// 加载当前运行的最小任务和作用域快照，供纯 Rust JOIN 推进器判定。
    fn load_run_snapshot(
        &mut self,
        workflow_id: &str,
        run_id: i64,
    ) -> impl Future<Output = Result<(Vec<RunTask>, Vec<RunScope>), StorageError>> + Send;

    /// 批量标记级联失败的 JOIN。
    fn fail_joins(
        &mut self,
        workflow_id: &str,
        failed: &[(i64, &'static str)],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// 批量将就绪的 JOIN 转为 `pending`。
    fn mark_joins_pending(
        &mut self,
        workflow_id: &str,
        task_ids: &[i64],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}
