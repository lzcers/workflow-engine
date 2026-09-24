//! [`PgTaskStore`]：通用引擎 [`crate::TaskStore`] 的 pgsql 实现。
//!
//! 工作流级构造（绑定 `workflow_id` 作用域与 [`PgTaskRepo`]），收编通用引擎的
//! [`crate::FanoutPlan`] → [`super::repo::FanoutInstances`] 折叠，不感知任何业务字段。

use serde_json::Value;

use super::repo::{FanoutInstances, PgError, PgTaskRepo};
pub use super::task::PgTask;
use crate::{FanoutPlan, TaskError, TaskStore};

/// 工作流级任务存储的持久化错误：包装仓储层错误 + 结算后回填缺失的确定性错误。
#[derive(Debug, thiserror::Error)]
pub enum PgStoreError {
    /// 底层仓储（postgres）操作失败。
    #[error("repository error: {0}")]
    Repo(#[from] PgError),
    /// 成功结算后任务回读不到（节点身份回填缺失）。
    #[error("task {workflow_id}:{task_id} missing after succeed")]
    Missing { workflow_id: String, task_id: i64 },
}

/// 工作流级任务存储：绑定 `workflow_id + PgTaskRepo`，把 [`TaskStore`] 落到 `tasks` 表。
#[derive(Clone)]
pub struct PgTaskStore {
    repo: PgTaskRepo,
    workflow_id: String,
    max_attempts: i32,
}

impl PgTaskStore {
    /// 绑定工作流作用域与仓储；新任务默认 `max_attempts = 3`。
    pub fn new(repo: PgTaskRepo, workflow_id: String) -> Self {
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
/// 与 [`TaskStore::enqueue_fanout`] 的既有实例模型一致，成功/失败后继共用。
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

impl TaskStore for PgTaskStore {
    type Task = PgTask;
    type Error = PgStoreError;

    async fn claim_many(&self, limit: usize) -> Result<Vec<PgTask>, PgStoreError> {
        self.repo
            .claim_many(&self.workflow_id, "worker", limit)
            .await
            .map_err(PgStoreError::Repo)
    }

    async fn succeed(&self, task: &PgTask, output: &Value) -> Result<PgTask, PgStoreError> {
        // RETURNING 一步拿回节点回填的最新行（如业务侧 `attach_commit` 附着的分组键），
        // 无需二次回读；行缺失视为确定性错误。
        self.repo
            .succeed(&self.workflow_id, task.task_id, output)
            .await
            .map_err(|e| match e {
                PgError::MissingTask {
                    workflow_id,
                    task_id,
                } => PgStoreError::Missing {
                    workflow_id,
                    task_id,
                },
                other => PgStoreError::Repo(other),
            })
    }

    async fn fail(
        &self,
        task: &PgTask,
        err: &TaskError,
        has_successors: bool,
    ) -> Result<(), PgStoreError> {
        // 有后继接管失败 → 标记 superseded（对外不暴露为失败，前端看到后继在推进）；
        // 无后继 → 标记 failed。
        if has_successors {
            self.repo
                .supersede(&self.workflow_id, task.task_id, &err.to_string())
                .await
                .map_err(PgStoreError::Repo)
        } else {
            self.repo
                .fail(&self.workflow_id, task.task_id, &err.to_string())
                .await
                .map_err(PgStoreError::Repo)
        }
    }

    async fn enqueue_fanout(
        &self,
        task: &PgTask,
        name: &str,
        task_type: &str,
        inputs: &[Value],
        depends_on_names: &[String],
        depends_on_ids: &[i64],
    ) -> Result<(), PgStoreError> {
        // 统一 fanout 实例模型（成功/失败后继共用）：单实例（len==1）→ instance_id=None
        // （扇入合并）；多实例 → 各占一个独立 instance_id。扇出父一次性登记实例全集：
        // 父的 task_id 与其全部实例共同构成 JOIN 依赖。
        let instances = fold_instances(inputs);
        self.repo
            .enqueue_fanout(
                &self.workflow_id,
                task.run_id,
                name,
                &instances,
                depends_on_ids,
                task_type,
                depends_on_names,
                self.max_attempts,
            )
            .await
            .map(|_| ())
            .map_err(PgStoreError::Repo)
    }

    /// 原子成功结算：succeed + 全部成功后继入队同一事务，返回回填后的任务记录
    /// （如业务节点结算时附着的分组键）。
    async fn succeed_and_enqueue(
        &self,
        task: &PgTask,
        output: &Value,
        fanouts: &[FanoutPlan],
    ) -> Result<PgTask, PgStoreError> {
        let fanouts: Vec<FanoutInstances> = fanouts.iter().map(to_fanout_instances).collect();
        self.repo
            .succeed_and_enqueue(
                &self.workflow_id,
                task.task_id,
                output,
                &fanouts,
                self.max_attempts,
            )
            .await
            .map_err(|e| match e {
                PgError::MissingTask {
                    workflow_id,
                    task_id,
                } => PgStoreError::Missing {
                    workflow_id,
                    task_id,
                },
                other => PgStoreError::Repo(other),
            })
    }

    /// 原子失败结算：fail/superseded + 全部失败后继入队同一事务。
    async fn fail_and_enqueue(
        &self,
        task: &PgTask,
        err: &TaskError,
        has_successors: bool,
        fanouts: &[FanoutPlan],
    ) -> Result<(), PgStoreError> {
        let fanouts: Vec<FanoutInstances> = fanouts.iter().map(to_fanout_instances).collect();
        self.repo
            .fail_and_enqueue(
                &self.workflow_id,
                task.task_id,
                task.run_id,
                &err.to_string(),
                has_successors,
                &fanouts,
                self.max_attempts,
            )
            .await
            .map_err(PgStoreError::Repo)
    }

    /// 释放任务租约：原子结算事务失败（已回滚）后由调度器调用，让任务下一轮认领即可重入。
    async fn release_claim(&self, task: &PgTask) -> Result<(), PgStoreError> {
        self.repo
            .release_claim(&self.workflow_id, task.task_id)
            .await
            .map_err(PgStoreError::Repo)
    }

    async fn cascade_failed_predecessor(&self, task: &PgTask) -> Result<usize, PgStoreError> {
        self.repo
            .cascade_failed_predecessor(
                &self.workflow_id,
                task.task_id,
                &task.name,
                &format!("predecessor task {} failed", task.task_id),
            )
            .await
            .map_err(PgStoreError::Repo)
    }
}
