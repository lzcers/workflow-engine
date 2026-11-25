//! 纯 Rust 的 JOIN 状态推进：在结算事务内依据当前 run 的任务与作用域快照判定失败传播与就绪。
//!
//! 所有数据库后端共用本模块，只需查询当前 run 的轻量任务/作用域行并批量回写状态，
//! 不再在各数据库方言中维护重复的递归 CTE SQL。

use std::collections::{HashMap, HashSet};

use super::{JoinRule, TaskStatus};

/// 当前运行中参与 JOIN 判定的最小任务快照。
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct RunTask {
    pub task_id: i64,
    pub name: String,
    #[sqlx(try_from = "String")]
    pub status: TaskStatus,
    pub scope_id: Option<i64>,
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
    pub failed: Vec<i64>,
    pub ready: Vec<i64>,
}

enum RequiredSource<'a> {
    Local(&'a str),
    Entry { task_id: i64 },
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
    ) -> Vec<RequiredSource<'r>> {
        let entries = self.entry_triggers(scope_id);
        let mut required = Vec::with_capacity(rule.predecessors.len());
        for predecessor in &rule.predecessors {
            match predecessor.entry_depth {
                None => required.push(RequiredSource::Local(predecessor.name.as_str())),
                Some(depth) => {
                    let Some(&trigger_task_id) = entries.get(depth) else {
                        continue;
                    };
                    let Some(&idx) = self.task_by_id.get(&trigger_task_id) else {
                        continue;
                    };
                    if tasks[idx].name == predecessor.name {
                        required.push(RequiredSource::Entry {
                            task_id: trigger_task_id,
                        });
                    }
                }
            }
        }
        required
    }

    fn is_failed(
        &self,
        scope_id: Option<i64>,
        rule: &JoinRule,
        tasks: &[RunTask],
        statuses: &[TaskStatus],
    ) -> bool {
        let required = self.required_sources(scope_id, rule, tasks);
        if required.is_empty() {
            return false;
        }
        if required.iter().any(|req| match *req {
            RequiredSource::Entry { task_id } => self
                .task_by_id
                .get(&task_id)
                .is_some_and(|&idx| statuses[idx] == TaskStatus::Failed),
            RequiredSource::Local(_) => false,
        }) {
            return true;
        }
        let local_scopes = self.local_scopes(scope_id);

        rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        statuses[idx] == TaskStatus::Failed
                            && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        })
    }

    fn is_ready(
        &self,
        scope_id: Option<i64>,
        rule: &JoinRule,
        tasks: &[RunTask],
        statuses: &[TaskStatus],
    ) -> bool {
        let required = self.required_sources(scope_id, rule, tasks);
        if required.is_empty() {
            return true;
        }
        let local_scopes = self.local_scopes(scope_id);

        let upstream_active = rule.upstreams.iter().any(|upstream| {
            self.tasks_by_name
                .get(upstream.as_str())
                .is_some_and(|indices| {
                    indices.iter().any(|&idx| {
                        matches!(
                            statuses[idx],
                            TaskStatus::Pending | TaskStatus::Collecting | TaskStatus::Running
                        ) && local_scopes.contains(&tasks[idx].scope_id)
                    })
                })
        });
        if upstream_active {
            return false;
        }

        for req in &required {
            match *req {
                RequiredSource::Entry { task_id } => {
                    let Some(&idx) = self.task_by_id.get(&task_id) else {
                        return false;
                    };
                    if statuses[idx] != TaskStatus::Succeeded {
                        return false;
                    }
                }
                RequiredSource::Local(name) => {
                    let Some(indices) = self.tasks_by_name.get(name) else {
                        return false;
                    };
                    let mut matched = false;
                    for &idx in indices {
                        let status = statuses[idx];
                        if status == TaskStatus::Superseded
                            || !local_scopes.contains(&tasks[idx].scope_id)
                        {
                            continue;
                        }
                        matched = true;
                        if status != TaskStatus::Succeeded {
                            return false;
                        }
                    }
                    if !matched {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// 先循环传播依赖失败到不动点（覆盖链式 JOIN），再将收齐输入的 JOIN 推进为 `pending`。
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
            if index.is_failed(task.scope_id, rule, tasks, &statuses) {
                newly_failed.push(idx);
            }
        }
        if newly_failed.is_empty() {
            break;
        }
        for idx in newly_failed {
            statuses[idx] = TaskStatus::Failed;
            transitions.failed.push(tasks[idx].task_id);
        }
    }

    for (idx, task) in tasks.iter().enumerate() {
        if statuses[idx] != TaskStatus::Collecting {
            continue;
        }
        let Some(rule) = rules.get(&task.name) else {
            continue;
        };
        if index.is_ready(task.scope_id, rule, tasks, &statuses) {
            transitions.ready.push(task.task_id);
        }
    }

    transitions
}
