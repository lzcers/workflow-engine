//! 纯 Rust 的 JOIN 状态推进：在结算事务内依据当前 run 的任务、贡献者与作用域快照判定失败传播与就绪。
//!
//! 规则：
//! 1. 任一入口前驱或本作用域内上游失败（`failed`）→ 立刻级联失败。
//! 2. 仍有任一上游处于活跃态（`pending`/`collecting`/`running`，排除自身）→ 继续等待。
//! 3. 所有上游终结后逐个验收前驱：
//!    - `succeeded` 或已通过 `on_error` 直接向本 JOIN 贡献的 `superseded`（位于 `parent_task_ids`）均计为有效贡献；
//!    - `required`（非 `optional`）前驱若无任何有效贡献，立即标 `failed`，杜绝僵尸 `collecting`；
//!    - 所有必需前驱均满足时转 `pending`。

use std::collections::{HashMap, HashSet};

use super::{JoinRule, TaskStatus};
use crate::backend::{UPSTREAM_FAILED_ERR, UPSTREAM_UNSATISFIED_ERR};

/// 当前运行中参与 JOIN 判定的最小任务快照。
#[derive(Debug, Clone)]
pub(crate) struct RunTask {
    pub task_id: i64,
    pub name: String,
    pub status: TaskStatus,
    pub scope_id: Option<i64>,
    pub parent_task_ids: Vec<i64>,
}

/// 当前运行中参与 JOIN 作用域匹配的子图实例快照。
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
    pub ready: Vec<i64>,
}

enum SourceCheck<'a> {
    Local { name: &'a str, optional: bool },
    Entry { task_id: i64, optional: bool },
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
        }
    }

    fn local_scopes(&self, root: Option<i64>) -> HashSet<Option<i64>> {
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
        visited
    }

    /// 从最外层子图（depth = 0）到当前作用域的触发任务 ID 链。
    fn entry_triggers(&self, mut scope_id: Option<i64>) -> Vec<i64> {
        let mut triggers = Vec::new();
        let mut seen = HashSet::new();
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
        triggers
    }

    fn required_sources<'r>(
        &self,
        scope_id: Option<i64>,
        rule: &'r JoinRule,
        tasks: &[RunTask],
    ) -> Vec<SourceCheck<'r>> {
        let entries = self.entry_triggers(scope_id);
        let mut sources = Vec::with_capacity(rule.predecessors.len());
        for predecessor in &rule.predecessors {
            match predecessor.entry_depth {
                None => sources.push(SourceCheck::Local {
                    name: predecessor.name.as_str(),
                    optional: predecessor.optional,
                }),
                Some(depth) => {
                    let Some(&trigger_task_id) = entries.get(depth) else {
                        continue;
                    };
                    let Some(&idx) = self.task_by_id.get(&trigger_task_id) else {
                        continue;
                    };
                    if tasks[idx].name == predecessor.name {
                        sources.push(SourceCheck::Entry {
                            task_id: trigger_task_id,
                            optional: predecessor.optional,
                        });
                    }
                }
            }
        }
        sources
    }

    fn evaluate(
        &self,
        join_idx: usize,
        rule: &JoinRule,
        tasks: &[RunTask],
        statuses: &[TaskStatus],
    ) -> Verdict {
        let join_task = &tasks[join_idx];
        let sources = self.required_sources(join_task.scope_id, rule, tasks);
        let local_scopes = self.local_scopes(join_task.scope_id);

        // 1) 实时失败传播：外部入口或本地作用域上游失败则立即失败。
        if sources.iter().any(|src| match *src {
            SourceCheck::Entry { task_id, .. } => self
                .task_by_id
                .get(&task_id)
                .is_some_and(|&idx| statuses[idx] == TaskStatus::Failed),
            SourceCheck::Local { .. } => false,
        }) {
            return Verdict::Failed(UPSTREAM_FAILED_ERR);
        }
        if rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        idx != join_idx
                            && statuses[idx] == TaskStatus::Failed
                            && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        }) {
            return Verdict::Failed(UPSTREAM_FAILED_ERR);
        }

        // 2) 上游活跃检查：仍有任务在途（pending/collecting/running）则继续等待。
        if sources.iter().any(|src| match *src {
            SourceCheck::Entry { task_id, .. } => self.task_by_id.get(&task_id).is_some_and(|&idx| {
                matches!(
                    statuses[idx],
                    TaskStatus::Pending | TaskStatus::Collecting | TaskStatus::Running
                )
            }),
            SourceCheck::Local { .. } => false,
        }) {
            return Verdict::Waiting;
        }
        if rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        idx != join_idx
                            && matches!(
                                statuses[idx],
                                TaskStatus::Pending | TaskStatus::Collecting | TaskStatus::Running
                            )
                            && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        }) {
            return Verdict::Waiting;
        }

        // 3) 所有上游均已终结：按实际贡献者 parent_task_ids 做终态裁决，杜绝僵尸 collecting。
        let contributed: HashSet<i64> = join_task.parent_task_ids.iter().copied().collect();
        for src in &sources {
            match *src {
                SourceCheck::Entry { task_id, optional } => {
                    let satisfied = self.task_by_id.get(&task_id).is_some_and(|&idx| {
                        matches!(
                            statuses[idx],
                            TaskStatus::Succeeded | TaskStatus::Superseded
                        ) && contributed.contains(&task_id)
                    });
                    if !satisfied && !optional {
                        return Verdict::Failed(UPSTREAM_UNSATISFIED_ERR);
                    }
                }
                SourceCheck::Local { name, optional } => {
                    let mut has_contribution = false;
                    if let Some(indices) = self.tasks_by_name.get(name) {
                        for &idx in indices {
                            if !local_scopes.contains(&tasks[idx].scope_id) {
                                continue;
                            }
                            if matches!(
                                statuses[idx],
                                TaskStatus::Succeeded | TaskStatus::Superseded
                            ) && contributed.contains(&tasks[idx].task_id)
                            {
                                has_contribution = true;
                            }
                        }
                    }
                    if !has_contribution && !optional {
                        return Verdict::Failed(UPSTREAM_UNSATISFIED_ERR);
                    }
                }
            }
        }
        Verdict::Ready
    }
}

/// 先循环传播失败与缺失前驱终态到不动点（覆盖链式 JOIN），再将收齐输入的 JOIN 推进为 `pending`。
pub(crate) fn advance_joins(
    tasks: &[RunTask],
    scopes: &[RunScope],
    rules: &HashMap<String, JoinRule>,
) -> JoinTransitions {
    if rules.is_empty() || !tasks.iter().any(|t| t.status == TaskStatus::Collecting) {
        return JoinTransitions::default();
    }

    let mut statuses: Vec<TaskStatus> = tasks.iter().map(|t| t.status).collect();
    let mut transitions = JoinTransitions::default();
    let index = RunIndex::build(tasks, scopes);

    loop {
        let mut newly_failed = Vec::new();
        for (idx, task) in tasks.iter().enumerate() {
            if statuses[idx] != TaskStatus::Collecting {
                continue;
            }
            let Some(rule) = rules.get(&task.name) else {
                continue;
            };
            if let Verdict::Failed(reason) = index.evaluate(idx, rule, tasks, &statuses) {
                newly_failed.push((idx, reason));
            }
        }
        if newly_failed.is_empty() {
            break;
        }
        for (idx, reason) in newly_failed {
            statuses[idx] = TaskStatus::Failed;
            transitions.failed.push((tasks[idx].task_id, reason));
        }
    }

    for (idx, task) in tasks.iter().enumerate() {
        if statuses[idx] != TaskStatus::Collecting {
            continue;
        }
        let Some(rule) = rules.get(&task.name) else {
            continue;
        };
        if matches!(index.evaluate(idx, rule, tasks, &statuses), Verdict::Ready) {
            transitions.ready.push(task.task_id);
        }
    }

    transitions
}
