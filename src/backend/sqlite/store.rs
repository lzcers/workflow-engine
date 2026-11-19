//! [`SqliteTaskRepo`] 直接实现通用引擎 [`crate::task::TaskStore`]。
//!
//! 通过 [`SqliteTaskRepo`] 持久化当前图规划的后继输入，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::SqliteTaskRepo;
use crate::error::StorageError;
use crate::graph::Successor;
use crate::task::TaskStore;
use crate::{Task, TaskRecord};

impl TaskStore for SqliteTaskRepo {
    type Error = StorageError;

    fn enqueue<'a>(&'a self, task: &'a Task) -> BoxFuture<'a, Result<TaskRecord, StorageError>> {
        Box::pin(SqliteTaskRepo::enqueue(self, task))
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        worker_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(SqliteTaskRepo::claim_many(
            self,
            workflow_id,
            worker_id,
            limit,
        ))
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(SqliteTaskRepo::renew_claim(self, task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，继承任务最新的 run_id 和 scope_id。
    fn settle_success<'a>(
        &'a self,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.succeed_and_enqueue(task, output, successors, self.max_attempts)
                .await
                .map(|_| ())
        })
    }

    /// 原子失败结算：有后继则入队并标记 superseded，否则标记 failed 并级联已有依赖。
    fn settle_failure<'a>(
        &'a self,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.fail_and_enqueue(task, message, successors, self.max_attempts)
                .await
        })
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(SqliteTaskRepo::release_claim(self, task))
    }
}
