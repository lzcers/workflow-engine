//! 为 [`PgTaskRepo`] 实现引擎的原子任务存储契约。
//!
//! 通过 [`PgTaskRepo`] 使用当前工作流的上游关系，接收图规划的后继，
//! 不感知任何业务字段。

use futures::future::BoxFuture;
use serde_json::Value;

use super::repo::PgTaskRepo;
use super::scope;
use crate::error::StorageError;
use crate::graph::{ErasedGraph, Successor};
use crate::task::TaskStore;
use crate::{Database, RunHandle, Task, TaskRecord};

impl Database<&mut sqlx::Transaction<'_, sqlx::Postgres>> {
    /// 在宿主事务内创建独立运行，同时建立入口所属的子图作用域。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        let tx = self.0;
        let run_id: i64 =
            sqlx::query_scalar("SELECT nextval(pg_get_serial_sequence('tasks', 'task_id'))")
                .fetch_one(&mut **tx)
                .await?;
        let scope_id = scope::enter(
            tx,
            &task.workflow_id,
            Some(run_id),
            run_id,
            None,
            &task.subgraph_path,
            None,
        )
        .await?;
        sqlx::query(
            "INSERT INTO tasks (workflow_id, task_id, run_id, name, status, input_json, scope_id)
             VALUES ($1, $2, $2, $3, 'pending', $4, $5)",
        )
        .bind(&task.workflow_id)
        .bind(run_id)
        .bind(task.name)
        .bind(task.input)
        .bind(scope_id)
        .execute(&mut **tx)
        .await?;
        Ok(RunHandle {
            workflow_id: task.workflow_id,
            run_id,
        })
    }
}

impl TaskStore for PgTaskRepo {
    type Error = StorageError;

    fn enqueue(&self, task: Task) -> BoxFuture<'_, Result<RunHandle, StorageError>> {
        Box::pin(async move { Database::from(self.pool.clone()).submit(task).await })
    }

    fn claim_many<'a>(
        &'a self,
        workflow_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(PgTaskRepo::claim_many(self, workflow_id, limit))
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(PgTaskRepo::renew_claim(self, task))
    }

    /// 原子成功结算：写入输出并入队全部成功后继，后继使用任务最新的分组键。
    fn settle_success<'a>(
        &'a self,
        graph: &'a dyn ErasedGraph,
        task: &'a TaskRecord,
        output: &'a Value,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.succeed_and_enqueue(graph, task, output, successors))
    }

    /// 原子失败结算：标记终态，并入队失败后继或级联已有依赖任务。
    fn settle_failure<'a>(
        &'a self,
        graph: &'a dyn ErasedGraph,
        task: &'a TaskRecord,
        message: &'a str,
        successors: &'a [Successor],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.fail_and_enqueue(graph, task, message, successors))
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    fn release_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(PgTaskRepo::release_claim(self, task))
    }
}
