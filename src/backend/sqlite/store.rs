//! [`SqliteTaskStore`]：通用引擎 [`crate::task::TaskStore`] 的 SQLite 实现。
//!
//! 通过 [`SqliteTaskRepo`] 供多个工作流共享，直接持久化图规划的后继输入，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::SqliteTaskRepo;
use crate::error::StorageError;
use crate::task::{FanoutPlan, TaskStore};
use crate::{Task, TaskRecord};

/// 共享任务存储：通过 `SqliteTaskRepo` 把 [`TaskStore`] 落到 `tasks` 表。
#[derive(Clone)]
pub struct SqliteTaskStore {
    repo: SqliteTaskRepo,
    max_attempts: i32,
}

impl SqliteTaskStore {
    /// 绑定仓储；后继任务默认 `max_attempts = 3`，入队任务使用 Task 中的预算。
    pub fn new(repo: SqliteTaskRepo) -> Self {
        Self {
            repo,
            max_attempts: 3,
        }
    }

    /// 覆盖结算时创建的后继任务的最大尝试次数。
    pub fn with_max_attempts(mut self, n: i32) -> Self {
        self.max_attempts = n;
        self
    }
}

impl TaskStore for SqliteTaskStore {
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

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(self.repo.renew_claim(task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，继承任务最新的 run_id 和 scope_id。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [FanoutPlan],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.repo
                .succeed_and_enqueue(task, output, successors, self.max_attempts)
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
            self.repo
                .fail_and_enqueue(task, message, successors, self.max_attempts)
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
