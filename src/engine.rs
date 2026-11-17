//! 单图控制平面：持有调度器，提交任务并返回运行句柄。

use std::{future::Future, sync::Arc, time::Duration};

use tokio::sync::{Mutex, Notify};

use crate::{Database, Graph, NodeContext, Scheduler, StorageError, Task, TaskStore};

/// 引擎提交或启动失败。
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("workflow mismatch: expected {expected}, got {actual}")]
    WorkflowMismatch { expected: String, actual: String },
    #[error("no node registered for name={0}")]
    UnknownNode(String),
    #[error("engine scheduler is already running")]
    AlreadyRunning,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// 一张固定图的控制入口；构造时创建调度器，调用 [`Self::run`] 才开始执行。
///
/// 从 [`Database`] 创建绑定图身份的内部存储，提交与调度共享同一个实例。
/// 可以在调度运行期间继续提交。需要启用 `pg` 或 `sqlite` feature。
pub struct Engine<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    graph: Arc<Graph<Ctx>>,
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    scheduler: Mutex<Scheduler<Ctx, dyn TaskStore<Error = StorageError> + Send>>,
    wake: Arc<Notify>,
}

impl<Ctx> Engine<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    /// 接收完整的图与数据库连接，内部绑定工作流并创建唯一调度器。
    /// 数据库连接和表初始化由调用方通过 [`Database`] 完成。
    pub fn new(
        graph: Graph<Ctx>,
        database: Database,
        poll: Duration,
        idle_timeout: Option<Duration>,
    ) -> Self {
        let store = database.task_store(graph.workflow_id());
        let graph = Arc::new(graph);
        let wake = Arc::new(Notify::new());
        let scheduler = Scheduler::new(graph.clone(), store.clone(), poll, idle_timeout)
            .with_wake(Some(wake.clone()));
        Self {
            graph,
            store,
            scheduler: Mutex::new(scheduler),
            wake,
        }
    }

    /// 向当前图提交任务；持久化成功后唤醒调度器并返回句柄。
    ///
    /// `name` 指定图内入口节点；`run_id` 沿用任务提供的分组键，不自动分配。
    /// 独立运行应使用不同的 `run_id`；`None` 与其他未指定 run 的任务共享分组。
    pub async fn submit(&self, task: &Task) -> Result<RunHandle, EngineError> {
        if task.workflow_id != self.graph.workflow_id() {
            return Err(EngineError::WorkflowMismatch {
                expected: self.graph.workflow_id().to_owned(),
                actual: task.workflow_id.clone(),
            });
        }
        if !self.graph.contains_node(&task.name) {
            return Err(EngineError::UnknownNode(task.name.clone()));
        }
        let record = self.store.enqueue(task).await?;
        self.wake.notify_one();
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.run_id,
            task_id: record.task_id,
        })
    }

    /// 驱动当前图直到停止信号完成或达到空闲时限，并等待在途任务清理。
    /// 同一引擎只允许一个调度循环；可以在本方法返回后重新启动。
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> Result<(), EngineError> {
        let mut scheduler = self
            .scheduler
            .try_lock()
            .map_err(|_| EngineError::AlreadyRunning)?;
        scheduler.run(shutdown).await;
        Ok(())
    }
}

/// 一次提交的运行句柄；记录工作流、运行分组与提交任务的身份。
#[derive(Debug, Clone)]
pub struct RunHandle {
    pub workflow_id: String,
    pub run_id: Option<i64>,
    pub task_id: i64,
}

impl RunHandle {
    /// 查询本次图运行的状态。当前为空实现，不读取存储或返回实际状态。
    pub async fn status(&self) {}
}
