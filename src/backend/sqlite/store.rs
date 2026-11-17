//! [`SqliteTaskStore`]：通用引擎 [`crate::TaskStore`] 的 SQLite 实现。
//!
//! 工作流级构造（绑定 `workflow_id` 作用域与 [`SqliteTaskRepo`]），收编通用引擎的
//! [`crate::FanoutPlan`] → [`crate::FanoutInstances`] 折叠，不感知任何业务字段。

use serde_json::Value;

use super::repo::SqliteTaskRepo;
use crate::{FanoutInstances, FanoutPlan, StorageError, TaskRecord, TaskStore};

/// 工作流级任务存储：绑定 `workflow_id + SqliteTaskRepo`，把 [`TaskStore`] 落到 `tasks` 表。
#[derive(Clone)]
pub struct SqliteTaskStore {
    repo: SqliteTaskRepo,
    workflow_id: String,
    max_attempts: i32,
}

impl SqliteTaskStore {
    /// 绑定工作流作用域与仓储；新任务默认 `max_attempts = 3`。
    pub fn new(repo: SqliteTaskRepo, workflow_id: String) -> Self {
        Self {
            repo,
            workflow_id,
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

    async fn claim_many(&self, limit: usize) -> Result<Vec<TaskRecord>, StorageError> {
        self.repo
            .claim_many(&self.workflow_id, "worker", limit)
            .await
    }

    /// 原子成功结算：写入输出并入队全部成功后继，后继使用任务最新的分组键。
    async fn settle_success(
        &self,
        task: &TaskRecord,
        output: &Value,
        successors: &[FanoutPlan],
    ) -> Result<(), StorageError> {
        let fanouts: Vec<FanoutInstances> = successors.iter().map(to_fanout_instances).collect();
        self.repo
            .succeed_and_enqueue(
                &self.workflow_id,
                task.task_id,
                output,
                &fanouts,
                self.max_attempts,
            )
            .await
            .map(|_| ())
    }

    /// 原子失败结算：有后继则入队并标记 superseded，否则标记 failed 并级联已有依赖。
    async fn settle_failure(
        &self,
        task: &TaskRecord,
        message: &str,
        successors: &[FanoutPlan],
    ) -> Result<(), StorageError> {
        let fanouts: Vec<FanoutInstances> = successors.iter().map(to_fanout_instances).collect();
        self.repo
            .fail_and_enqueue(
                &self.workflow_id,
                task.task_id,
                task.run_id,
                message,
                &fanouts,
                self.max_attempts,
            )
            .await
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    async fn release_claim(&self, task: &TaskRecord) -> Result<(), StorageError> {
        self.repo
            .release_claim(&self.workflow_id, task.task_id)
            .await
    }
}
