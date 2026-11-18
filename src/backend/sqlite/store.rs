//! [`SqliteTaskStore`]：通用引擎 [`crate::TaskStore`] 的 SQLite 实现。
//!
//! 通过 [`SqliteTaskRepo`] 供多个工作流共享，收编通用引擎的
//! [`crate::task::FanoutPlan`] → [`crate::task::FanoutInstances`] 折叠，不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::SqliteTaskRepo;
use crate::task::{FanoutInstances, FanoutPlan};
use crate::{StorageError, Task, TaskRecord, TaskStore};

/// 共享任务存储：通过 `SqliteTaskRepo` 把 [`TaskStore`] 落到 `tasks` 表。
#[derive(Clone)]
pub struct SqliteTaskStore {
    repo: SqliteTaskRepo,
    max_attempts: i32,
}

impl SqliteTaskStore {
    /// 绑定仓储；新任务默认 `max_attempts = 3`。
    pub fn new(repo: SqliteTaskRepo) -> Self {
        Self {
            repo,
            max_attempts: 3,
        }
    }

    /// 覆盖新建任务的最大尝试次数。
    pub fn with_max_attempts(mut self, n: i32) -> Self {
        self.max_attempts = n;
        self
    }
}

/// 把 `inputs` 折叠为 `(instance_id, input)` 实例列表：`len == 1` → 单实例（`None`，
/// 扇入合并）；`len > 1` → map 边扇出的多实例（各占一个独立 `instance_id`）。
/// 成功后继和失败后继共用此实例模型。
fn fold_instances(inputs: &[Value]) -> Vec<(Option<i32>, Value)> {
    if inputs.len() == 1 {
        vec![(None, inputs[0].clone())]
    } else {
        inputs
            .iter()
            .enumerate()
            .map(|(i, input)| (Some(i as i32), input.clone()))
            .collect()
    }
}

/// 把引擎侧 [`FanoutPlan`] 折叠为仓储层入队计划。
fn to_fanout_instances(f: &FanoutPlan) -> FanoutInstances {
    FanoutInstances {
        name: f.dst.to_string(),
        task_type: f.task_type.to_string(),
        instances: fold_instances(&f.inputs),
        depends_on_names: f.depends_on_names.clone(),
    }
}

impl TaskStore for SqliteTaskStore {
    type Error = StorageError;

    fn create_run<'a>(
        &'a self,
        workflow_id: &'a str,
        entry: &'a str,
        input: &'a Value,
    ) -> BoxFuture<'a, Result<TaskRecord, StorageError>> {
        Box::pin(
            self.repo
                .create_run(workflow_id, entry, input, self.max_attempts),
        )
    }

    fn enqueue<'a>(&'a self, task: &'a Task) -> BoxFuture<'a, Result<TaskRecord, StorageError>> {
        Box::pin(self.repo.enqueue(task))
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        worker_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(self.repo.claim_many(workflow_id, worker_id, limit))
    }

    fn renew_claim<'a>(&'a self, task: &'a TaskRecord) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.repo.renew_claim(task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，后继使用任务最新的分组键。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let fanouts: Vec<FanoutInstances> =
                successors.iter().map(to_fanout_instances).collect();
            self.repo
                .succeed_and_enqueue(task, output, &fanouts, self.max_attempts)
                .await
                .map(|_| ())
        })
    }

    /// 原子失败结算：有后继则入队并标记 superseded，否则标记 failed 并级联已有依赖。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let fanouts: Vec<FanoutInstances> =
                successors.iter().map(to_fanout_instances).collect();
            self.repo
                .fail_and_enqueue(task, message, &fanouts, self.max_attempts)
                .await
        })
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.repo.release_claim(task))
    }
}
