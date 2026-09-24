//! 通用调度器池 [`SchedulerPool`]：按字符串键管理多个 worker 的生命周期。
//!
//! 与业务、存储、图均无关：池只负责「起 worker、记槽位（句柄 + 唤醒信号）、
//! get-or-create 复用与惰性清理、逐出（配置变更后按新配置重建）、把 worker 的唤醒信号
//! 交回上层、退出时记录存活时长」。是否唤醒由上层组合（对返回的信号调 `notify_one()`），
//! 池不承担通知分发。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::Notify;
use tracing::info;

/// 单个 worker 槽位：句柄（供惰性清理）+ 唤醒信号（任务就绪通知据此唤醒）。
struct Slot {
    handle: tokio::task::JoinHandle<()>,
    /// 唤醒信号：`Notify` 许可上限为 1，多次通知天然合并为一次唤醒。
    wake: Arc<Notify>,
}

/// worker 工厂：键 + 唤醒信号 → 待运行的 future（盒装擦除，避免池子随工厂泛型化）。
type Spawn = dyn Fn(String, Arc<Notify>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

/// 按字符串键管理调度器生命周期的通用池。
///
/// - [`SchedulerPool::ensure_worker`]：get-or-create——保证键的 worker 在运行，返回其唤醒信号；
/// - 惰性清理：`ensure_worker` 顺带清掉已结束的槽位，注册表不会长期累积；
/// - [`SchedulerPool::evict`]：中止并移除键的 worker，供「配置变更后重建」使用；
/// - 生命周期观测：worker 退出时记录带存活时长的日志（与工厂内日志配对，便于排查）。
///
/// 工厂闭包在 `ensure_worker` 内被同步调用，应只负责构造 future；任何初始化重活（如物化
/// 运行时、构建执行器）都应放进 future 内，spawn 后才执行。
#[derive(Clone)]
pub struct SchedulerPool {
    slots: Arc<Mutex<HashMap<String, Slot>>>,
    spawn: Arc<Spawn>,
}

impl SchedulerPool {
    /// 由 worker 工厂构造池：`spawn(key, wake)` 返回该键 worker 要运行的 future。
    pub fn new<F, Fut>(spawn: F) -> Self
    where
        F: Fn(String, Arc<Notify>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
            spawn: Arc::new(move |key, wake| Box::pin(spawn(key, wake))),
        }
    }

    /// 确保键的 worker 在运行，返回其唤醒信号：已有活跃 worker 则复用，否则经工厂新建并
    /// spawn。get-or-create 在单次锁内完成，避免「先查后建」在并发下的重复 spawn。
    ///
    /// 是否唤醒由上层组合：想唤醒就对返回值调 `notify_one()`，只想拉起就丢弃返回值。
    pub fn ensure_worker(&self, key: &str) -> Arc<Notify> {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slots.retain(|_, slot| !slot.handle.is_finished());
        if let Some(slot) = slots.get(key) {
            return slot.wake.clone(); // 复用活跃 worker
        }
        self.spawn_worker(&mut slots, key)
    }

    /// 逐出键的 worker：中止其任务、等它真正停下，然后移除槽位（下次 `ensure_worker` 重建）。
    ///
    /// 必须等到中止生效才返回：逐出的用途是「让 worker 重新按当前配置拉起」，若旧 worker
    /// 仍在跑，它可能与新 worker 竞争认领任务（被旧 worker 抢走的任务在新 worker 里不可见，
    /// 只能等租约过期）。等待中止后，调用方可以安全地释放该工作流的任务租约再重建 worker。
    ///
    /// 返回是否确实逐出了一个活跃 worker（无活跃 worker 时是 no-op）。
    pub async fn evict(&self, key: &str) -> bool {
        let slot = {
            let mut slots = self
                .slots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slots.remove(key)
        };
        let Some(slot) = slot else {
            return false;
        };
        slot.handle.abort();
        // abort 在下一个 await 点生效；句柄被 abort 后 await 返回 `Cancelled`，属预期结果。
        let _ = slot.handle.await;
        true
    }

    /// 经工厂新建并 spawn 键的 worker（调用方需已持锁），返回其唤醒信号。
    fn spawn_worker(&self, slots: &mut HashMap<String, Slot>, key: &str) -> Arc<Notify> {
        let wake = Arc::new(Notify::new());
        // 先取启动时刻再 spawn：worker 可能立即结束，时刻必须早于退出日志。
        let started_at = Instant::now();
        let key_for_log = key.to_string();
        let future = (self.spawn)(key.to_string(), wake.clone());
        let handle = tokio::task::spawn(async move {
            future.await;
            info!(key = %key_for_log, elapsed = ?started_at.elapsed(), "scheduler worker exited");
        });
        slots.insert(
            key.to_string(),
            Slot {
                handle,
                wake: wake.clone(),
            },
        );
        wake
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// 轮询等待计数到达期望值（测试内确定性等待 worker 退出）。
    async fn wait_until(counter: &AtomicUsize, expected: usize) {
        for _ in 0..10_000 {
            if counter.load(Ordering::SeqCst) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not reached: {expected}");
    }

    /// 活跃 worker 被复用；退出后槽位被惰性清理，再次 `ensure_worker` 会重新 spawn。
    #[tokio::test]
    async fn ensure_reuses_live_worker_then_respawns_after_exit() {
        let spawned = Arc::new(AtomicUsize::new(0));
        let exited = Arc::new(AtomicUsize::new(0));
        let pool = SchedulerPool::new({
            let spawned = spawned.clone();
            let exited = exited.clone();
            move |_key: String, wake: Arc<Notify>| {
                let spawned = spawned.clone();
                let exited = exited.clone();
                async move {
                    spawned.fetch_add(1, Ordering::SeqCst);
                    wake.notified().await; // 被池唤醒后退出（与生产同款路径）
                    exited.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        pool.ensure_worker("a");
        wait_until(&spawned, 1).await;
        pool.ensure_worker("a");
        assert_eq!(spawned.load(Ordering::SeqCst), 1, "活跃 worker 应被复用");

        pool.ensure_worker("a").notify_one(); // 复用并唤醒 → 退出
        wait_until(&exited, 1).await;

        pool.ensure_worker("a");
        wait_until(&spawned, 2).await;
        assert_eq!(spawned.load(Ordering::SeqCst), 2, "退出后应重新 spawn");
    }

    /// 逐出中止在跑的 worker 并移除槽位：无活跃 worker 时是 no-op，逐出后 `ensure_worker`
    /// 必须新建（旧 worker 的唤醒信号不再能唤醒新 worker）。
    #[tokio::test]
    async fn evict_aborts_live_worker_and_forces_a_fresh_spawn() {
        let spawned = Arc::new(AtomicUsize::new(0));
        let aborted = Arc::new(AtomicUsize::new(0));
        let pool = SchedulerPool::new({
            let spawned = spawned.clone();
            let aborted = aborted.clone();
            move |_key: String, wake: Arc<Notify>| {
                let spawned = spawned.clone();
                let aborted = aborted.clone();
                async move {
                    spawned.fetch_add(1, Ordering::SeqCst);
                    wake.notified().await; // 逐出会在此处中止，退出计数不应增加
                    aborted.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        assert!(!pool.evict("a").await, "无活跃 worker 时逐出是 no-op");

        pool.ensure_worker("a");
        wait_until(&spawned, 1).await;
        assert!(pool.evict("a").await, "应逐出活跃 worker");
        assert_eq!(aborted.load(Ordering::SeqCst), 0, "中止不应走正常退出路径");

        let wake = pool.ensure_worker("a");
        wait_until(&spawned, 2).await;
        assert_eq!(spawned.load(Ordering::SeqCst), 2, "逐出后应重新 spawn");

        // 新 worker 可被唤醒并正常退出：说明返回的是新槽位的唤醒信号。
        wake.notify_one();
        wait_until(&aborted, 1).await;
    }

    /// `ensure_worker` get-or-create：无活跃 worker 时新建并 spawn 并返回唤醒信号；
    /// worker 存活期间复用不重复 spawn，`notify_one` 即唤醒。
    #[tokio::test]
    async fn ensure_worker_get_or_create_returns_wake_signal() {
        let spawned = Arc::new(AtomicUsize::new(0));
        let exited = Arc::new(AtomicUsize::new(0));
        let pool = SchedulerPool::new({
            let spawned = spawned.clone();
            let exited = exited.clone();
            move |_key: String, wake: Arc<Notify>| {
                let spawned = spawned.clone();
                let exited = exited.clone();
                async move {
                    spawned.fetch_add(1, Ordering::SeqCst);
                    wake.notified().await;
                    exited.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        // 无活跃 worker：新建并 spawn，返回的唤醒信号可唤醒它。
        let wake1 = pool.ensure_worker("a");
        wait_until(&spawned, 1).await;

        // 存活期间重复调用：复用不重复 spawn，且返回同一唤醒信号。
        let wake2 = pool.ensure_worker("a");
        assert_eq!(spawned.load(Ordering::SeqCst), 1, "存活期应复用");
        assert!(Arc::ptr_eq(&wake1, &wake2), "应返回同一唤醒信号");

        // 对返回的信号 notify_one 即唤醒 → 退出。
        wake1.notify_one();
        wait_until(&exited, 1).await;

        // 退出后可重建（wait_until 覆盖 is_finished 滞后）。
        pool.ensure_worker("a");
        wait_until(&spawned, 2).await;
    }
}
