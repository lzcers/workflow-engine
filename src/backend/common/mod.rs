//! PG 与 SQLite 共享的事务编排层：统一后继批量入队、槽位合并与 JOIN 推进。
//!
//! 后端只需实现薄 SQL 方言原语 [`BackendTx`]，消除双后端业务编排重复与语义漂移。

mod tx;

use std::sync::Arc;

use serde_json::Value;

pub(crate) use tx::{BackendTx, NewTaskRow};

use crate::{
    error::StorageError,
    task::{
        Dispatch, MergeMode, SettlementRules, Successor, TaskStatus, join,
        scope::{ScopeEntries, ScopeRecord, route_path},
        store::{FanoutSource, merge_join_slot},
    },
};

/// 结算终态载荷：用单一 ADT 消除 `(status, output, error)` 的非法组合。
pub(crate) enum SettleOutcome<'a> {
    Succeeded(&'a Value),
    Failed {
        status: TaskStatus,
        message: &'a str,
    },
}

impl<'a> SettleOutcome<'a> {
    /// 根据失败后继是否为空构造失败终态：有后继接管为 `Superseded`，否则为 `Failed`。
    pub(crate) fn failure(message: &'a str, successors: &[Successor]) -> Self {
        let status = if successors.iter().all(|plan| plan.inputs.is_empty()) {
            TaskStatus::Failed
        } else {
            TaskStatus::Superseded
        };
        Self::Failed { status, message }
    }

    pub(crate) fn parts(self) -> (TaskStatus, Option<&'a Value>, Option<&'a str>) {
        match self {
            Self::Succeeded(output) => (TaskStatus::Succeeded, Some(output), None),
            Self::Failed { status, message } => (status, None, Some(message)),
        }
    }
}

/// 统一的事务内后继扇出 + 槽位收集 + JOIN 推进；返回本次是否有新任务进入 `pending`。
pub(crate) async fn enqueue_fanouts_and_advance<Tx: BackendTx>(
    tx: &mut Tx,
    source: &FanoutSource<'_>,
    fanouts: &[Successor],
    rules: &SettlementRules,
    preloaded_chain: Option<Vec<ScopeRecord>>,
) -> Result<bool, StorageError> {
    let has_affected_joins = !rules.affected_joins_for(source.name).is_empty();
    let has_fanouts = fanouts.iter().any(|p| !p.inputs.is_empty());
    let mut created_pending = false;
    let mut touched_join = false;

    if !has_fanouts && !has_affected_joins {
        return Ok(false);
    }

    if has_fanouts {
        let chain = match preloaded_chain {
            Some(chain) => chain,
            None => tx.load_scope_chain(source.workflow_id, source.scope_id).await?,
        };
        let mut shared_entries = ScopeEntries::new();
        let mut pending_batch: Vec<(Arc<str>, Option<i64>, &Value)> = Vec::new();

        for plan in fanouts {
            if plan.inputs.is_empty() {
                continue;
            }
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
                        pending_batch.push((plan.dst.clone(), Some(scope_id), input));
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
                        pending_batch.push((plan.dst.clone(), scope_id, input));
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
                    let optional_append_slots = rules
                        .join_rules
                        .get(plan.dst.as_ref())
                        .map(|r| r.optional_append_slots.as_slice())
                        .unwrap_or(&[]);
                    collect_join(tx, source, plan, scope_id, optional_append_slots, key, merge)
                        .await?;
                    touched_join = true;
                }
            }
        }

        if !pending_batch.is_empty() {
            let rows: Vec<NewTaskRow<'_>> = pending_batch
                .iter()
                .map(|(dst, scope_id, input)| NewTaskRow {
                    dst: dst.as_ref(),
                    status: TaskStatus::Pending,
                    scope_id: *scope_id,
                    input,
                })
                .collect();
            tx.insert_tasks_batch(source, &rows).await?;
            created_pending = true;
        }
    }

    if !rules.join_rules.is_empty()
        && (has_affected_joins || touched_join)
        && tx
            .has_collecting_join(source.workflow_id, source.run_id)
            .await?
    {
        let (tasks, scopes) = tx
            .load_run_snapshot(source.workflow_id, source.run_id)
            .await?;
        let transitions = join::advance_joins(&tasks, &scopes, &rules.join_rules);

        if !transitions.failed.is_empty() {
            tx.fail_joins(source.workflow_id, &transitions.failed).await?;
        }
        if !transitions.ready.is_empty() {
            let ready_ids: Vec<i64> = transitions.ready.into_iter().map(|(id, _)| id).collect();
            tx.mark_joins_pending(source.workflow_id, &ready_ids).await?;
            created_pending = true;
        }
    }

    Ok(created_pending)
}

async fn collect_join<Tx: BackendTx>(
    tx: &mut Tx,
    source: &FanoutSource<'_>,
    plan: &Successor,
    scope_id: Option<i64>,
    optional_append_slots: &[String],
    key: &str,
    merge: MergeMode,
) -> Result<(), StorageError> {
    let existing = tx
        .find_collecting_join(
            source.workflow_id,
            source.run_id,
            scope_id,
            plan.dst.as_ref(),
        )
        .await?;
    if let Some((id, mut parents, input)) = existing {
        if !parents.contains(&source.task_id) {
            parents.push(source.task_id);
        }
        let updated = merge_join_slot(Some(input), optional_append_slots, key, merge, &plan.inputs)?;
        tx.update_collecting_join(source.workflow_id, id, &parents, &updated)
            .await?;
    } else {
        let initial = merge_join_slot(None, optional_append_slots, key, merge, &plan.inputs)?;
        tx.insert_collecting_join(source, plan.dst.as_ref(), scope_id, &initial)
            .await?;
    }
    Ok(())
}
