//! 纯 Rust 的槽位驱动 JOIN 状态推进：单趟不动点 + 作用域缓存 + 多轮历史失败隔离。
//!
//! 裁决规则：
//! 1. **实时失败传播**：匹配到的外部入口或本作用域（含后代）内**本轮**存在 `failed` 上游 → 立即级联失败；
//! 2. **活跃上游等待**：匹配到的外部入口或本作用域（含后代）内仍有上游处于活跃态（`pending`/`collecting`/`running`，排除自身）→ 继续等待；
//! 3. **槽位终态验收**：所有上游终结后，逐个检查 `rule.required_slots` 是否均已收到贡献（存在于 `contributed_slots`）→
//!    任一必填槽缺失则立即标 `failed`，全部满足则转 `pending` 并物化 `Append` 槽位。

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{JoinRule, TaskStatus};
use crate::backend::{UPSTREAM_FAILED_ERR, UPSTREAM_UNSATISFIED_ERR};

/// 参与 JOIN 判定的最小任务快照（不搬运普通任务的 `input_json`）。
#[derive(Debug, Clone)]
pub(crate) struct RunTask {
    pub task_id: i64,
    pub name: String,
    pub status: TaskStatus,
    pub scope_id: Option<i64>,
    /// 仅对 `collecting` JOIN 填充：已收到至少一次贡献的槽位键名。
    pub contributed_slots: Vec<String>,
}

/// 参与 JOIN 作用域匹配的子图实例快照。
#[derive(Debug, Clone, Copy, sqlx::FromRow)]
pub(crate) struct RunScope {
    pub scope_id: i64,
    pub parent_scope_id: Option<i64>,
    pub trigger_task_id: i64,
}

/// 本次结算需要写回数据库的 JOIN 状态变更。
#[derive(Debug, Default)]
pub(crate) struct JoinTransitions {
    pub failed: Vec<(i64, &'static str)>,
    pub ready: Vec<(i64, String)>,
}

enum Verdict {
    Waiting,
    Ready,
    Failed(&'static str),
}

struct RunIndex<'a> {
    task_by_id: HashMap<i64, usize>,
    tasks_by_name: HashMap<&'a str, Vec<usize>>,
    scope_by_id: HashMap<i64, RunScope>,
    children_by_parent: HashMap<Option<i64>, Vec<i64>>,
    local_scopes_cache: RefCell<HashMap<Option<i64>, Arc<HashSet<Option<i64>>>>>,
    entry_triggers_cache: RefCell<HashMap<Option<i64>, Arc<Vec<i64>>>>,
}

impl<'a> RunIndex<'a> {
    fn build(tasks: &'a [RunTask], scopes: &[RunScope]) -> Self {
        let mut task_by_id = HashMap::with_capacity(tasks.len());
        let mut tasks_by_name: HashMap<&'a str, Vec<usize>> = HashMap::new();
        for (idx, task) in tasks.iter().enumerate() {
            task_by_id.insert(task.task_id, idx);
            tasks_by_name.entry(task.name.as_str()).or_default().push(idx);
        }

        let mut scope_by_id = HashMap::with_capacity(scopes.len());
        let mut children_by_parent: HashMap<Option<i64>, Vec<i64>> = HashMap::new();
        for &scope in scopes {
            scope_by_id.insert(scope.scope_id, scope);
            children_by_parent
                .entry(scope.parent_scope_id)
                .or_default()
                .push(scope.scope_id);
        }

        Self {
            task_by_id,
            tasks_by_name,
            scope_by_id,
            children_by_parent,
            local_scopes_cache: RefCell::new(HashMap::new()),
            entry_triggers_cache: RefCell::new(HashMap::new()),
        }
    }

    fn local_scopes(&self, root: Option<i64>) -> Arc<HashSet<Option<i64>>> {
        if let Some(cached) = self.local_scopes_cache.borrow().get(&root) {
            return cached.clone();
        }
        let mut visited = HashSet::new();
        let mut stack = vec![root];
        while let Some(scope_id) = stack.pop() {
            if !visited.insert(scope_id) {
                continue;
            }
            if let Some(children) = self.children_by_parent.get(&scope_id) {
                for &child in children {
                    stack.push(Some(child));
                }
            }
        }
        let result = Arc::new(visited);
        self.local_scopes_cache
            .borrow_mut()
            .insert(root, result.clone());
        result
    }

    /// 从最外层隔离作用域（depth = 0）到当前作用域的触发任务 ID 链。
    fn entry_triggers(&self, root: Option<i64>) -> Arc<Vec<i64>> {
        if let Some(cached) = self.entry_triggers_cache.borrow().get(&root) {
            return cached.clone();
        }
        let mut triggers = Vec::new();
        let mut seen = HashSet::new();
        let mut scope_id = root;
        while let Some(id) = scope_id {
            if !seen.insert(id) {
                break;
            }
            let Some(scope) = self.scope_by_id.get(&id) else {
                break;
            };
            triggers.push(scope.trigger_task_id);
            scope_id = scope.parent_scope_id;
        }
        triggers.reverse();
        let result = Arc::new(triggers);
        self.entry_triggers_cache
            .borrow_mut()
            .insert(root, result.clone());
        result
    }

    /// 匹配触发当前子图实例的外部直接入口任务索引。
    fn matched_external_entries(
        &self,
        scope_id: Option<i64>,
        rule: &JoinRule,
        tasks: &[RunTask],
    ) -> Vec<usize> {
        if rule.external_entries.is_empty() {
            return Vec::new();
        }
        let entries = self.entry_triggers(scope_id);
        let mut matched = Vec::new();
        for ext in &rule.external_entries {
            let Some(&trigger_task_id) = entries.get(ext.entry_depth) else {
                continue;
            };
            let Some(&idx) = self.task_by_id.get(&trigger_task_id) else {
                continue;
            };
            if tasks[idx].name == ext.name {
                matched.push(idx);
            }
        }
        matched
    }

    /// 计算多轮汇合的失败隔离截断 ID：若同作用域内已有更早进入终态的同名 JOIN，
    /// 则忽略早于上一轮 JOIN 创建的历史 `failed` 上游，防止失败回边重跑时被历史失败毒化。
    fn failure_cutoff(&self, join_task: &RunTask, tasks: &[RunTask], statuses: &[TaskStatus]) -> i64 {
        let Some(indices) = self.tasks_by_name.get(join_task.name.as_str()) else {
            return 0;
        };
        indices
            .iter()
            .filter_map(|&idx| {
                let candidate = &tasks[idx];
                (candidate.scope_id == join_task.scope_id
                    && candidate.task_id < join_task.task_id
                    && !statuses[idx].is_active())
                .then_some(candidate.task_id)
            })
            .max()
            .unwrap_or(0)
    }

    fn evaluate(
        &self,
        join_idx: usize,
        rule: &JoinRule,
        tasks: &[RunTask],
        statuses: &[TaskStatus],
    ) -> Verdict {
        let join_task = &tasks[join_idx];
        let matched_entries = self.matched_external_entries(join_task.scope_id, rule, tasks);
        let local_scopes = self.local_scopes(join_task.scope_id);
        let cutoff = self.failure_cutoff(join_task, tasks, statuses);

        // 1) 实时失败传播：外部入口或本作用域内本轮上游失败则立即级联失败。
        if matched_entries
            .iter()
            .any(|&idx| statuses[idx] == TaskStatus::Failed)
        {
            return Verdict::Failed(UPSTREAM_FAILED_ERR);
        }
        if rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        idx != join_idx
                            && tasks[idx].task_id > cutoff
                            && statuses[idx] == TaskStatus::Failed
                            && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        }) {
            return Verdict::Failed(UPSTREAM_FAILED_ERR);
        }

        // 2) 上游活跃检查：仍有任务在途（pending/collecting/running）则继续等待。
        if matched_entries.iter().any(|&idx| statuses[idx].is_active()) {
            return Verdict::Waiting;
        }
        if rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        idx != join_idx
                            && statuses[idx].is_active()
                            && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        }) {
            return Verdict::Waiting;
        }

        // 3) 所有上游均已终结：按必填槽位是否收到有效贡献做终态裁决。
        for req_slot in &rule.required_slots {
            if !join_task.contributed_slots.iter().any(|k| k == req_slot) {
                return Verdict::Failed(UPSTREAM_UNSATISFIED_ERR);
            }
        }
        Verdict::Ready
    }
}

/// 单趟不动点推进：失败级联到达不动点的最后一轮直接产出 `ready`，不再重复评估第二遍。
pub(crate) fn advance_joins(
    tasks: &[RunTask],
    scopes: &[RunScope],
    rules: &HashMap<Arc<str>, JoinRule>,
) -> JoinTransitions {
    if rules.is_empty() || !tasks.iter().any(|t| t.status == TaskStatus::Collecting) {
        return JoinTransitions::default();
    }

    let mut statuses: Vec<TaskStatus> = tasks.iter().map(|t| t.status).collect();
    let mut transitions = JoinTransitions::default();
    let index = RunIndex::build(tasks, scopes);

    loop {
        let mut newly_failed = Vec::new();
        let mut ready = Vec::new();
        for (idx, task) in tasks.iter().enumerate() {
            if statuses[idx] != TaskStatus::Collecting {
                continue;
            }
            let Some(rule) = rules.get(task.name.as_str()) else {
                continue;
            };
            match index.evaluate(idx, rule, tasks, &statuses) {
                Verdict::Failed(reason) => newly_failed.push((idx, reason)),
                Verdict::Ready => ready.push((task.task_id, task.name.clone())),
                Verdict::Waiting => {}
            }
        }
        if newly_failed.is_empty() {
            transitions.ready = ready;
            break;
        }
        for (idx, reason) in newly_failed {
            statuses[idx] = TaskStatus::Failed;
            transitions.failed.push((tasks[idx].task_id, reason));
        }
    }

    transitions
}
