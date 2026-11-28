//! PG 与 SQLite 共享的事务编排层：统一后继批量入队、O(1) Append 收集、作用域隔离推进与任务数护栏。
//!
//! 后端只需实现薄 SQL 方言原语 [`BackendTx`]，彻底消除双后端业务编排重复与语义漂移。

use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use serde_json::Value;

use crate::{
    error::StorageError,
    node::MergeMode,
    task::{
        Dispatch, JoinRule, SettlementConfig, SettlementRules, Successor, TaskStatus,
        join::{self, RunScope, RunTask},
        scope::{ScopeEntries, ScopeRecord, route_path},
        store::{FanoutSource, materialize_join_input, record_join_slot},
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

/// 统一的事务内后继扇出 + 槽位收集 + 作用域化 JOIN 推进；返回本次是否有新任务进入 `pending`。
pub(crate) async fn enqueue_fanouts_and_advance<Tx: BackendTx>(
    tx: &mut Tx,
    source: &FanoutSource<'_>,
    fanouts: &[Successor],
    rules: &SettlementRules,
    config: SettlementConfig,
    preloaded_chain: Option<Vec<ScopeRecord>>,
) -> Result<bool, StorageError> {
    let affected_depths = rules.affected_scope_depths(source.name);
    let has_fanouts = fanouts.iter().any(|p| !p.inputs.is_empty());
    let mut created_pending = false;

    if !has_fanouts && affected_depths.is_empty() {
        return Ok(false);
    }

    let chain = match preloaded_chain {
        Some(chain) => chain,
        None => tx.load_scope_chain(source.workflow_id, source.scope_id).await?,
    };

    // 单次运行任务总数护栏（防止失败回边环或失控扇出打爆数据库）。
    let planned_new_tasks: usize = fanouts
        .iter()
        .map(|plan| match plan.dispatch {
            Dispatch::Shared | Dispatch::PerInputScope => plan.inputs.len(),
            Dispatch::Join { .. } => usize::from(!plan.inputs.is_empty()),
        })
        .sum();
    if config.max_tasks_per_run > 0 && planned_new_tasks > 0 {
        let existing_count = tx.count_run_tasks(source.workflow_id, source.run_id).await?;
        if existing_count.saturating_add(planned_new_tasks) > config.max_tasks_per_run {
            return Err(StorageError::RunTaskLimitExceeded {
                workflow_id: source.workflow_id.to_owned(),
                run_id: source.run_id,
                limit: config.max_tasks_per_run,
            });
        }
    }

    let mut newly_touched_join_scopes: Vec<Option<i64>> = Vec::new();
    if has_fanouts {
        let mut shared_entries = ScopeEntries::new();
        let mut pending_batch: Vec<(Arc<str>, Option<i64>, &Value, i32)> = Vec::new();

        for plan in fanouts {
            if plan.inputs.is_empty() {
                continue;
            }
            let max_attempts = plan
                .max_attempts
                .unwrap_or(config.default_max_attempts)
                .max(1);
            let per_input_scope = plan.dispatch == Dispatch::PerInputScope;
            let (parent, entered) =
                route_path(&chain, source.run_id, &plan.subgraph_path, per_input_scope)?;

            match plan.dispatch {
                Dispatch::PerInputScope => {
                    let scopes = tx
                        .enter_isolated_scopes_batch(
                            source.workflow_id,
                            source.run_id,
                            source.task_id,
                            parent,
                            entered,
                            plan.inputs.len(),
                        )
                        .await?;
                    for (scope_id, input) in scopes.into_iter().zip(&plan.inputs) {
                        pending_batch.push((plan.dst.clone(), Some(scope_id), input, max_attempts));
                    }
                }
                Dispatch::Shared => {
                    let scope_id = tx
                        .enter_shared_scope(
                            source.workflow_id,
                            source.run_id,
                            source.task_id,
                            parent,
                            entered,
                            &mut shared_entries,
                        )
                        .await?;
                    for input in &plan.inputs {
                        pending_batch.push((plan.dst.clone(), scope_id, input, max_attempts));
                    }
                }
                Dispatch::Join { key, merge } => {
                    let scope_id = tx
                        .enter_shared_scope(
                            source.workflow_id,
                            source.run_id,
                            source.task_id,
                            parent,
                            entered,
                            &mut shared_entries,
                        )
                        .await?;
                    collect_join(tx, source, plan, scope_id, key, merge, max_attempts).await?;
                    newly_touched_join_scopes.push(scope_id);
                }
            }
        }

        if !pending_batch.is_empty() {
            let rows: Vec<NewTaskRow<'_>> = pending_batch
                .iter()
                .map(|(dst, scope_id, input, max_attempts)| NewTaskRow {
                    dst: dst.as_ref(),
                    status: TaskStatus::Pending,
                    scope_id: *scope_id,
                    input,
                    max_attempts: *max_attempts,
                })
                .collect();
            tx.insert_tasks_batch(source, &rows).await?;
            created_pending = true;
        }
    }

    if !rules.join_rules.is_empty() {
        let mut check_scopes = newly_touched_join_scopes;
        for depth in affected_depths {
            if depth == 0 {
                check_scopes.push(None);
            } else if depth <= chain.len() {
                check_scopes.push(Some(chain[depth - 1].scope_id));
            }
        }
        check_scopes.sort_unstable();
        check_scopes.dedup();
        if !check_scopes.is_empty() {
            let readied = advance_joins_scoped(
                tx,
                source.workflow_id,
                source.run_id,
                &check_scopes,
                &rules.join_rules,
            )
            .await?;
            created_pending |= readied;
        }
    }

    Ok(created_pending)
}

async fn collect_join<Tx: BackendTx>(
    tx: &mut Tx,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
    key: &str,
    merge: MergeMode,
    max_attempts: i32,
) -> Result<(), StorageError> {
    let existing = tx
        .find_collecting_join(source.workflow_id, source.run_id, scope_id, plan.dst.as_ref())
        .await?;
    let join_task_id = if let Some((id, mut parents, input)) = existing {
        if !parents.contains(&source.task_id) {
            parents.push(source.task_id);
        }
        let updated = record_join_slot(Some(input), key, merge, &plan.inputs)?;
        tx.update_collecting_join(source.workflow_id, id, &parents, &updated)
            .await?;
        id
    } else {
        let initial = record_join_slot(None, key, merge, &plan.inputs)?;
        tx.insert_collecting_join(
            source,
            plan.dst.as_ref(),
            scope_id,
            &initial,
            max_attempts,
        )
        .await?
    };

    if merge == MergeMode::Append && !plan.inputs.is_empty() {
        tx.append_join_contributions(
            source.workflow_id,
            join_task_id,
            key,
            source.task_id,
            &plan.inputs,
        )
        .await?;
    }
    Ok(())
}

async fn advance_joins_scoped<Tx: BackendTx>(
    tx: &mut Tx,
    workflow_id: &str,
    run_id: i64,
    check_scopes: &[Option<i64>],
    rules: &HashMap<Arc<str>, JoinRule>,
) -> Result<bool, StorageError> {
    let collecting_scopes = tx
        .find_collecting_scopes(workflow_id, run_id, check_scopes)
        .await?;
    if collecting_scopes.is_empty() {
        return Ok(false);
    }

    let (tasks, scopes) = tx
        .load_join_snapshot(workflow_id, run_id, &collecting_scopes)
        .await?;
    let transitions = join::advance_joins(&tasks, &scopes, rules);

    if !transitions.failed.is_empty() {
        tx.fail_joins(workflow_id, &transitions.failed).await?;
    }

    if transitions.ready.is_empty() {
        return Ok(false);
    }

    let mut plain_ready = Vec::new();
    let mut append_ready = Vec::new();
    for (task_id, name) in transitions.ready {
        let has_append = rules
            .get(name.as_str())
            .is_some_and(|rule| !rule.append_keys.is_empty());
        if has_append {
            append_ready.push((task_id, name));
        } else {
            plain_ready.push(task_id);
        }
    }

    if !plain_ready.is_empty() {
        tx.mark_joins_pending(workflow_id, &plain_ready).await?;
    }

    if !append_ready.is_empty() {
        let ids: Vec<i64> = append_ready.iter().map(|(id, _)| *id).collect();
        let mut inputs = tx.load_task_inputs(workflow_id, &ids).await?;
        let mut all_contribs = tx.load_join_contributions(workflow_id, &ids).await?;
        let mut materialized = Vec::with_capacity(append_ready.len());
        for (task_id, name) in append_ready {
            let existing = inputs
                .remove(&task_id)
                .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
            let contribs = all_contribs.remove(&task_id).unwrap_or_default();
            let append_keys = rules
                .get(name.as_str())
                .map(|r| r.append_keys.as_slice())
                .unwrap_or(&[]);
            let final_input = materialize_join_input(existing, append_keys, contribs)?;
            materialized.push((task_id, final_input));
        }
        tx.materialize_and_ready_joins(workflow_id, &materialized)
            .await?;
    }

    Ok(true)
}
