//! 跨工作流共享的唤醒中枢：单轮询器合并查询 + 可选的后端推送唤醒监听（如 PG `LISTEN/NOTIFY`）。

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard, Weak},
    time::Duration,
};

use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::error;

use crate::task::TaskStore;

type WakeCallback = Arc<dyn Fn(&str) -> bool + Send + Sync>;
type ListenerSpawner = Box<dyn Fn(WakeCallback) -> Option<JoinHandle<()>> + Send + Sync>;

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
    listener: Option<JoinHandle<()>>,
}

/// 跨工作流共享的唤醒中枢：
/// - 单个共享轮询器通过 `ready_workflows` 一次部分索引查询覆盖全部已注册工作流；
/// - 若底层存储支持推送唤醒（如 PostgreSQL `LISTEN/NOTIFY`），同时维护单个共享监听任务；
/// - 随最后一个 `Workflow` 释放/停机自动清理后台协程，支持 `Engine` 先于 `Workflow` 释放。
pub(crate) struct WakeHub {
    store: Arc<dyn TaskStore>,
    spawn_listener: ListenerSpawner,
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
    pub(crate) fn new(
        store: Arc<dyn TaskStore>,
        spawn_listener: impl Fn(WakeCallback) -> Option<JoinHandle<()>> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            spawn_listener: Box::new(spawn_listener),
            state: Mutex::new(HubState::default()),
            config_changed: Arc::new(Notify::new()),
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        workflow_id: String,
        wake: Arc<Notify>,
        poll_interval: Duration,
    ) -> HubRegistration {
        let mut state = self.lock_state();
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
        if state.listener.is_none() {
            let weak = Arc::downgrade(self);
            state.listener = (self.spawn_listener)(Arc::new(move |wf_id| {
                let Some(hub) = weak.upgrade() else {
                    return false;
                };
                hub.notify_workflow(wf_id);
                true
            }));
        }
        drop(state);
        self.config_changed.notify_one();
        HubRegistration {
            hub: self.clone(),
            id,
        }
    }

    fn unregister(&self, id: u64) {
        let mut state = self.lock_state();
        state.entries.retain(|e| e.id != id);
        if state.entries.is_empty() {
            if let Some(handle) = state.poller.take() {
                handle.abort();
            }
            if let Some(handle) = state.listener.take() {
                handle.abort();
            }
        }
        drop(state);
        self.config_changed.notify_one();
    }

    fn snapshot_poll(&self) -> (Vec<String>, Duration) {
        let state = self.lock_state();
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
            let state = self.lock_state();
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
