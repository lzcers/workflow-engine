//! 单图工作流实例：持有调度器，提交独立运行，并控制整个工作流的启停。

use std::{sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{
    sync::{Mutex, Notify, oneshot},
    task::JoinHandle,
};

use crate::{EngineError, Graph, NodeContext, Scheduler, StorageError, TaskStore};

/// 一张固定图的工作流实例，由 [`crate::Engine::make`] 创建。
///
/// 每次 submit 创建独立的 run_id；run 启动调度器，执行当前图所有运行的任务。
/// 工作流拥有图与存储的共享句柄，可在创建它的 Engine 释放后继续使用。
pub struct Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    graph: Arc<Graph<Ctx>>,
    store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    scheduler: Arc<Scheduler<Ctx, dyn TaskStore<Error = StorageError> + Send>>,
    worker: Mutex<Option<Worker>>,
    wake: Arc<Notify>,
}

struct Worker {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<Ctx> Workflow<Ctx>
where
    Ctx: NodeContext + Sync + 'static,
{
    pub(crate) fn new(
        graph: Graph<Ctx>,
        store: Arc<dyn TaskStore<Error = StorageError> + Send>,
    ) -> Self {
        let graph = Arc::new(graph);
        let wake = Arc::new(Notify::new());
        let scheduler = Arc::new(
            Scheduler::new(graph.clone(), store.clone(), Duration::from_secs(1))
                .with_wake(wake.clone()),
        );
        Self {
            graph,
            store,
            scheduler,
            worker: Mutex::new(None),
            wake,
        }
    }

    /// 从指定节点创建一次独立运行；原子分配 run_id 并持久化入口任务。
    ///
    /// input 必须能被该节点的 In 类型反序列化。返回成功仅表示已持久化。
    /// 调度器已启动时立即唤醒，否则任务等待下一次 run。
    pub async fn submit(
        &self,
        entry: &str,
        input: impl Serialize,
    ) -> Result<RunHandle, EngineError> {
        if !self.graph.contains_node(entry) {
            return Err(EngineError::UnknownNode(entry.to_owned()));
        }
        let input = serde_json::to_value(input)?;
        let record = self
            .store
            .create_run(self.graph.workflow_id(), entry, &input)
            .await?;
        self.wake.notify_one();
        Ok(RunHandle {
            workflow_id: record.workflow_id,
            run_id: record.run_id.expect("create_run must allocate a run ID"),
            task_id: record.task_id,
        })
    }

    /// 启动当前工作流的后台调度器并立即返回，执行所有已提交及后续提交的 run。
    ///
    /// 所有 run 共享 8 个执行槽和 1 秒轮询周期；本进程提交会立即唤醒认领。
    /// 重复启动返回 AlreadyRunning。空闲时保持运行，调用 stop 停止后可再次启动。
    pub async fn run(&self) -> Result<(), EngineError> {
        let mut worker = self.worker.lock().await;
        if worker.is_some() {
            return Err(EngineError::AlreadyRunning);
        }
        let (shutdown, stopped) = oneshot::channel();
        let scheduler = self.scheduler.clone();
        let task = tokio::spawn(async move {
            scheduler
                .run(async {
                    let _ = stopped.await;
                })
                .await;
        });
        *worker = Some(Worker {
            shutdown: Some(shutdown),
            task,
        });
        Ok(())
    }

    /// 停止认领新任务，并等待已认领任务及其结算完成；未执行的任务留待再次 run。
    ///
    /// 重复停止无副作用。节点长时间不返回时，本方法会持续等待。
    /// 应等待 stop 完成后再释放实例；直接丢弃 Workflow 会取消后台任务，无法等待结算。
    pub async fn stop(&self) -> Result<(), EngineError> {
        let mut worker = self.worker.lock().await;
        if let Some(running) = worker.as_mut() {
            if let Some(shutdown) = running.shutdown.take() {
                let _ = shutdown.send(());
            }
            let result = (&mut running.task).await;
            worker.take();
            result?;
        }
        Ok(())
    }
}

/// 一次提交的运行句柄；workflow_id 与 run_id 共同确定运行身份。
#[derive(Debug, Clone)]
pub struct RunHandle {
    pub workflow_id: String,
    pub run_id: i64,
    pub task_id: i64,
}

impl RunHandle {
    /// 查询本次图运行的状态。当前为空实现，不读取存储或返回实际状态。
    pub async fn status(&self) {}
}
