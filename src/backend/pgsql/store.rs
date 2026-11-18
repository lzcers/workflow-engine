//! [`PgTaskStore`]：通用引擎 [`crate::task::TaskStore`] 的 pgsql 实现。
//!
//! 通过 [`PgTaskRepo`] 供多个工作流共享，复用任务模块定义的后继实例转换规则，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::PgTaskRepo;
use crate::error::StorageError;
use crate::task::{FanoutInstances, FanoutPlan, TaskStore};
use crate::{Task, TaskRecord};

/// 共享任务存储：通过 `PgTaskRepo` 把 [`TaskStore`] 落到 `tasks` 表。
#[derive(Clone)]
pub struct PgTaskStore {
    repo: PgTaskRepo,
    max_attempts: i32,
}

impl PgTaskStore {
    /// 绑定仓储；结算时新建的后继任务默认 `max_attempts = 3`。
    pub fn new(repo: PgTaskRepo) -> Self {
        Self {
            repo,
            max_attempts: 3,
        }
    }

    /// 覆盖结算时新建后继任务的最大尝试次数；直接入队沿用 Task 的配置。
    pub fn with_max_attempts(mut self, n: i32) -> Self {
        self.max_attempts = n;
        self
    }
}

impl TaskStore for PgTaskStore {
    type Error = StorageError;

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
                successors.iter().map(FanoutInstances::from).collect();
            self.repo
                .succeed_and_enqueue(task, output, &fanouts, self.max_attempts)
                .await
                .map(|_| ())
        })
    }

    /// 原子失败结算：标记终态，并入队失败后继或级联已有依赖任务。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            let fanouts: Vec<FanoutInstances> =
                successors.iter().map(FanoutInstances::from).collect();
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
