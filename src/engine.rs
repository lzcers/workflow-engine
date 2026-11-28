//! 可复用的引擎入口：共享存储、共享多工作流轮询器与 PostgreSQL `LISTEN/NOTIFY` 跨进程唤醒。

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::error;

use crate::database::{Database, DatabasePool, PoolOptions};
pub use crate::error::{GraphError, StorageError};
use crate::task::TaskStore;
use crate::workflow::WorkerOptions;
use crate::{Graph, NodeContext, Workflow};

/// 引擎初始化、提交或运行控制失败。
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("no node registered for name={0}")]
    UnknownNode(String),
    #[error("no run found for run_id={0}")]
    UnknownRun(i64),
    #[error("invalid graph: {0}")]
    InvalidGraph(#[from] GraphError),
    #[error("invalid workflow input: {0}")]
    Input(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

struct HubEntry {
    id: u64,
    workflow_id: String,
    wake: Arc<Notify>,
    poll_interval: Duration,
}

#[derive(Default)]
struct HubState {
    next_id: u64,
    entries: Vec<HubEntry>,
    poller: Option<JoinHandle<()>>,
    #[cfg(feature = "pg")]
    pg_listener: Option<JoinHandle<()>>,
}

/// 跨工作流共享的唤醒中枢：
/// - 单个共享轮询器通过 `ready_workflows` 一次部分索引查询覆盖全部已注册工作流；
/// - PostgreSQL 下同时维护单个 `PgListener` 实现亚毫秒级跨进程唤醒；
/// - 随最后一个 `Workflow` 释放/停机自动清理后台协程，支持 `Engine` 先于 `Workflow` 释放。
pub(crate) struct WakeHub {
    store: Arc<dyn TaskStore>,
    #[cfg(feature = "pg")]
    pg_pool: Option<sqlx::PgPool>,
    state: Mutex<HubState>,
    config_changed: Arc<Notify>,
}

pub(crate) struct HubRegistration {
    hub: Arc<WakeHub>,
    id: u64,
}

impl Drop for HubRegistration {
    fn drop(&mut self) {
        self.hub.unregister(self.id);
    }
}

impl WakeHub {
    fn new(
        store: Arc<dyn TaskStore>,
        #[cfg(feature = "pg")] pg_pool: Option<sqlx::PgPool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            #[cfg(feature = "pg")]
            pg_pool,
            state: Mutex::new(HubState::default()),
            config_changed: Arc::new(Notify::new()),
        })
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        workflow_id: String,
        wake: Arc<Notify>,
        poll_interval: Duration,
    ) -> HubRegistration {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id += 1;
        state.entries.push(HubEntry {
            id,
            workflow_id,
            wake,
            poll_interval: poll_interval.max(Duration::from_millis(5)),
        });
        if state.poller.is_none() {
            let weak = Arc::downgrade(self);
            state.poller = Some(tokio::spawn(shared_poll_loop(weak)));
        }
        #[cfg(feature = "pg")]
        if state.pg_listener.is_none()
            && let Some(pool) = self.pg_pool.clone()
        {
            let weak = Arc::downgrade(self);
            state.pg_listener = Some(tokio::spawn(pg_listen_loop(pool, weak)));
        }
        drop(state);
        self.config_changed.notify_one();
        HubRegistration {
            hub: self.clone(),
            id,
        }
    }

    fn unregister(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        state.entries.retain(|e| e.id != id);
        if state.entries.is_empty() {
            if let Some(handle) = state.poller.take() {
                handle.abort();
            }
            #[cfg(feature = "pg")]
            if let Some(handle) = state.pg_listener.take() {
                handle.abort();
            }
        }
        drop(state);
        self.config_changed.notify_one();
    }

    fn snapshot_poll(&self) -> (Vec<String>, Duration) {
        let state = self.state.lock().unwrap();
        let mut seen = HashSet::with_capacity(state.entries.len());
        let mut ids = Vec::with_capacity(state.entries.len());
        let mut min_poll = Duration::from_secs(60);
        for entry in &state.entries {
            if seen.insert(entry.workflow_id.as_str()) {
                ids.push(entry.workflow_id.clone());
            }
            if entry.poll_interval < min_poll {
                min_poll = entry.poll_interval;
            }
        }
        (ids, min_poll)
    }

    fn notify_workflow(&self, workflow_id: &str) {
        let wakes: Vec<Arc<Notify>> = {
            let state = self.state.lock().unwrap();
            state
                .entries
                .iter()
                .filter(|e| e.workflow_id == workflow_id)
                .map(|e| e.wake.clone())
                .collect()
        };
        for wake in wakes {
            wake.notify_one();
        }
    }
}

async fn shared_poll_loop(hub_weak: Weak<WakeHub>) {
    loop {
        let Some(hub) = hub_weak.upgrade() else {
            return;
        };
        let (workflow_ids, interval) = hub.snapshot_poll();
        if workflow_ids.is_empty() {
            return;
        }
        let changed = hub.config_changed.clone();
        drop(hub);

        tokio::select! {
            _ = changed.notified() => continue,
            _ = sleep(interval) => {}
        }

        let Some(hub) = hub_weak.upgrade() else {
            return;
        };
        let (workflow_ids, _) = hub.snapshot_poll();
        if workflow_ids.is_empty() {
            return;
        }
        match hub.store.ready_workflows(&workflow_ids).await {
            Ok(ready) => {
                for wf_id in ready {
                    hub.notify_workflow(&wf_id);
                }
            }
            Err(error) => {
                error!(error = %error, "shared poller query failed");
            }
        }
    }
}

#[cfg(feature = "pg")]
async fn pg_listen_loop(pool: sqlx::PgPool, hub_weak: Weak<WakeHub>) {
    use crate::backend::PG_NOTIFY_CHANNEL;
    use sqlx::postgres::PgListener;
    use tracing::warn;

    loop {
        if hub_weak.upgrade().is_none() {
            return;
        }
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(error) => {
                warn!(error = %error, "pg listener connect failed; retrying");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if let Err(error) = listener.listen(PG_NOTIFY_CHANNEL).await {
            warn!(error = %error, "pg listen channel failed; retrying");
            sleep(Duration::from_secs(2)).await;
            continue;
        }
        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let Some(hub) = hub_weak.upgrade() else {
                        return;
                    };
                    hub.notify_workflow(notification.payload());
                }
                Err(error) => {
                    warn!(error = %error, "pg listener disconnected; reconnecting");
                    sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }
}

/// 可复用的引擎，为其创建的所有工作流共享同一个任务仓储与共享唤醒中枢。
pub struct Engine {
    store: Arc<dyn TaskStore>,
    hub: Arc<WakeHub>,
}

impl Engine {
    /// 根据 URL 使用默认连接池参数连接数据库并幂等初始化引擎表。
    pub async fn new(url: &str) -> Result<Self, EngineError> {
        let database = Database::connect(url).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 使用自定义连接池参数连接数据库并幂等初始化引擎表。
    pub async fn new_with(url: &str, pool_options: PoolOptions) -> Result<Self, EngineError> {
        let database = Database::connect_with(url, pool_options).await?;
        database.initialize().await?;
        Ok(Self::from_database(database))
    }

    /// 复用宿主已经初始化的存储连接。
    pub fn from_database(database: Database) -> Self {
        #[cfg(feature = "pg")]
        let pg_pool = match &database.0 {
            DatabasePool::Postgres(pool) => Some(pool.clone()),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => None,
        };
        let store: Arc<dyn TaskStore> = match database.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => Arc::new(crate::backend::pgsql::PgTaskRepo::new(pool)),
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                Arc::new(crate::backend::sqlite::SqliteTaskRepo::new(pool))
            }
        };
        let hub = WakeHub::new(
            store.clone(),
            #[cfg(feature = "pg")]
            pg_pool,
        );
        Self { store, hub }
    }

    /// 根据图定义创建工作流实例并启动后台调度器，支持不同上下文类型的图。
    pub fn make<Ctx>(&self, graph: Graph<Ctx>) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        self.make_with(graph, WorkerOptions::default())
    }

    /// 使用指定的并发、轮询、租约与重试策略参数创建工作流实例并启动后台调度器。
    pub fn make_with<Ctx>(&self, graph: Graph<Ctx>, options: WorkerOptions) -> Workflow
    where
        Ctx: NodeContext + Sync + 'static,
    {
        Workflow::new(Arc::new(graph), self.store.clone(), &self.hub, options)
    }
}
