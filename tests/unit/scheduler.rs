//! Lease renewal keeps long-running work alive and fences off lost claims.

use std::{
    collections::VecDeque,
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::future::BoxFuture;
use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
use tokio::{
    sync::{Notify, mpsc},
    task::{JoinHandle, JoinSet},
    time::{Instant, advance, sleep, timeout},
};

use super::{Scheduler, notify_outcome, run_task};
use crate::backend::task_fixture::TaskSeed;
use crate::{
    Edge, Graph, Node, NodeContext, NodeError, TaskRecord, TaskStatus,
    backend::sqlite::{SqliteTaskRepo, migrate},
    error::StorageError,
    task::{MAX_ATTEMPTS, SettlementRules, Successor, TaskStore, store::LEASE_DURATION},
};

struct Context {
    task: Option<TaskRecord>,
    events: mpsc::UnboundedSender<(&'static str, String)>,
    pool: SqlitePool,
}

impl Context {
    fn record(&self, stage: &'static str) {
        self.events
            .send((stage, self.task.as_ref().unwrap().name.clone()))
            .unwrap();
    }
}

impl NodeContext for Context {
    async fn fork(&self, task: &TaskRecord) -> Self {
        self.events.send(("fork", task.name.clone())).unwrap();
        Self {
            task: Some(task.clone()),
            events: self.events.clone(),
            pool: self.pool.clone(),
        }
    }

    async fn on_start(&mut self, _task: &TaskRecord) {
        self.record("start");
    }

    async fn on_success(&mut self, _task: &TaskRecord, _: &Value) {
        self.record("success");
    }

    async fn on_error(&mut self, task: &TaskRecord, error: &NodeError) {
        let status: String =
            sqlx::query_scalar("SELECT status FROM tasks WHERE workflow_id=? AND task_id=?")
                .bind(&task.workflow_id)
                .bind(task.task_id)
                .fetch_one(&self.pool)
                .await
                .unwrap();
        assert!(matches!(status.as_str(), "failed" | "superseded"));
        self.events.send(("error", error.to_string())).unwrap();
    }
}

struct PendingNode;

impl Node<Context> for PendingNode {
    type In = i32;
    type Out = i32;

    async fn execute(&self, ctx: &mut Context, _: i32) -> Result<i32, NodeError> {
        ctx.record("execute");
        pending().await
    }
}

struct SuccessNode;

impl Node<Context> for SuccessNode {
    type In = i32;
    type Out = i32;

    async fn execute(&self, ctx: &mut Context, input: i32) -> Result<i32, NodeError> {
        ctx.record("execute");
        Ok(input + 1)
    }
}

async fn memory_pool() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    pool
}

async fn set_deadline(
    pool: &SqlitePool,
    repo: &SqliteTaskRepo,
    task: &TaskRecord,
    deadline: chrono::DateTime<chrono::Utc>,
) -> TaskRecord {
    sqlx::query("UPDATE tasks SET lease_until = ? WHERE workflow_id = ? AND task_id = ?")
        .bind(deadline.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .bind(&task.workflow_id)
        .bind(task.task_id)
        .execute(pool)
        .await
        .unwrap();
    repo.get(&task.workflow_id, task.task_id)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn lost_full_slot_recovers_through_failure_edge_without_executing_again() {
    let pool = memory_pool().await;
    let (events, mut receiver) = mpsc::unbounded_channel();
    let graph = Arc::new(
        Graph::builder(
            "timeout",
            "Timeout recovery",
            Context {
                task: None,
                events,
                pool: pool.clone(),
            },
        )
        .node("pending", PendingNode)
        .node("recovery", SuccessNode)
        .edge(
            Edge::failure::<i32>("pending")
                .to::<i32>("recovery")
                .map(|input: &i32, error| {
                    assert!(error.to_string().contains("lease expired"));
                    *input
                })
                .direct(),
        )
        .build()
        .unwrap(),
    );
    let repo = SqliteTaskRepo::new(pool.clone());
    let input = TaskSeed::new("timeout", "pending", json!(41));
    repo.seed_task(graph.as_ref(), &input).await.unwrap();
    for _ in 1..MAX_ATTEMPTS {
        let previous = repo
            .claim_many(graph.workflow_id(), 1)
            .await
            .unwrap()
            .remove(0);
        repo.release_claim(&previous, None, None).await.unwrap();
    }
    let claim = repo
        .claim_many(graph.workflow_id(), 1)
        .await
        .unwrap()
        .remove(0);
    let claim = set_deadline(
        &pool,
        &repo,
        &claim,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;
    let store = Arc::new(repo.clone());
    let scheduler = Scheduler::new(graph.clone(), store.clone(), Duration::from_millis(2))
        .with_max_concurrency(1);
    let mut in_flight = JoinSet::new();
    let mut callbacks = JoinSet::new();
    in_flight.spawn(run_task(
        graph,
        store,
        claim.clone(),
        Instant::now() + Duration::from_millis(100),
    ));
    let seen = timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = scheduler.run_loop(&mut in_flight, &mut callbacks) => panic!("scheduler exited"),
            seen = async {
                let mut seen = Vec::new();
                loop {
                    let event = receiver.recv().await.unwrap();
                    let done = event == ("success", "recovery".to_owned());
                    seen.push(event);
                    if done {
                        return seen;
                    }
                }
            } => seen,
        }
    })
    .await
    .expect("the lost claim must release the slot so recovery can execute");
    in_flight.shutdown().await;
    callbacks.shutdown().await;

    assert!(
        seen.iter()
            .filter(|event| **event == ("execute", "pending".to_owned()))
            .count()
            <= 1
    );
    assert!(
        seen.iter()
            .filter(|event| **event == ("start", "pending".to_owned()))
            .count()
            <= 1
    );
    assert_eq!(seen.iter().filter(|event| event.0 == "error").count(), 1);
    let tasks = repo.list_all("timeout").await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].task_id, claim.task_id);
    assert_eq!(tasks[0].status, TaskStatus::Superseded);
    assert_eq!(tasks[0].attempt, MAX_ATTEMPTS + 1);
    assert_eq!(tasks[1].status, TaskStatus::Succeeded);
    assert_eq!(tasks[1].output, Some(json!(42)));
    assert_eq!(tasks[1].parent_task_ids, [claim.task_id]);
    pool.close().await;
}

#[tokio::test]
async fn already_expired_snapshot_never_enters_business_or_settlement() {
    let pool = memory_pool().await;
    let (events, mut receiver) = mpsc::unbounded_channel();
    let graph = Arc::new(
        Graph::builder(
            "expired",
            "Expired claim",
            Context {
                task: None,
                events,
                pool: pool.clone(),
            },
        )
        .node("pending", PendingNode)
        .build()
        .unwrap(),
    );
    let repo = SqliteTaskRepo::new(pool.clone());
    repo.seed_task(
        graph.as_ref(),
        &TaskSeed::new("expired", "pending", json!(0)),
    )
    .await
    .unwrap();
    let claim = repo
        .claim_many(graph.workflow_id(), 1)
        .await
        .unwrap()
        .remove(0);
    let claim = set_deadline(
        &pool,
        &repo,
        &claim,
        chrono::Utc::now() - chrono::Duration::seconds(1),
    )
    .await;
    let outcome = timeout(
        Duration::from_secs(1),
        run_task(
            graph,
            Arc::new(repo.clone()),
            claim.clone(),
            Instant::now() - Duration::from_secs(1),
        ),
    )
    .await
    .expect("an expired snapshot must exit before calling fork or a node hook");
    assert!(outcome.is_none());
    assert!(receiver.try_recv().is_err());
    let stored = repo.get("expired", claim.task_id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(stored).unwrap(),
        serde_json::to_value(claim).unwrap()
    );
    pool.close().await;
}

#[derive(Clone, Copy)]
enum Renewal {
    Healthy,
    Error,
    Lost,
    Hanging,
}

struct Store {
    renewals: AtomicUsize,
    script: Mutex<VecDeque<Renewal>>,
    settlement_delay: Duration,
    settlements: AtomicUsize,
    committed: AtomicBool,
    releases: AtomicUsize,
}

impl Store {
    fn new(script: impl IntoIterator<Item = Renewal>, settlement_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            renewals: AtomicUsize::new(0),
            script: Mutex::new(script.into_iter().collect()),
            settlement_delay,
            settlements: AtomicUsize::new(0),
            committed: AtomicBool::new(false),
            releases: AtomicUsize::new(0),
        })
    }
}

impl TaskStore for Store {
    fn enqueue(&self, _: crate::Task) -> BoxFuture<'_, Result<crate::RunHandle, StorageError>> {
        Box::pin(async { unreachable!("run_task receives an existing claim") })
    }

    fn claim_many<'a>(
        &'a self,
        _: &'a str,
        _: usize,
    ) -> BoxFuture<'a, Result<Vec<TaskRecord>, StorageError>> {
        Box::pin(async { unreachable!("run_task receives an existing claim") })
    }

    fn renew_claim<'a>(
        &'a self,
        task: &'a TaskRecord,
    ) -> BoxFuture<'a, Result<Option<chrono::DateTime<chrono::Utc>>, StorageError>> {
        Box::pin(async move {
            assert_eq!(task.attempt, 1, "renewal must not create another attempt");
            self.renewals.fetch_add(1, Ordering::SeqCst);
            if self.committed.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let result = self
                .script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Renewal::Healthy);
            match result {
                Renewal::Healthy => Ok(Some(chrono::Utc::now() + chrono::Duration::seconds(300))),
                Renewal::Error => Err(StorageError::Other("database unavailable".into())),
                Renewal::Lost => Ok(None),
                Renewal::Hanging => pending().await,
            }
        })
    }

    fn settle_success<'a>(
        &'a self,
        _: &'a TaskRecord,
        _: &'a Value,
        _: &'a [Successor],
        _: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.settlements.fetch_add(1, Ordering::SeqCst);
            if !self.settlement_delay.is_zero() {
                sleep(self.settlement_delay).await;
            }
            self.committed.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn settle_failure<'a>(
        &'a self,
        _: &'a TaskRecord,
        _: &'a str,
        _: &'a [Successor],
        _: &'a SettlementRules,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async { panic!("lease loss must not be recorded as a business failure") })
    }

    fn schedule_retry<'a>(
        &'a self,
        _: &'a TaskRecord,
        _: &'a str,
        _: Duration,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async { panic!("lease loss must not be recorded as a retry") })
    }

    fn release_claim<'a>(
        &'a self,
        _: &'a TaskRecord,
        _: Option<&'a str>,
        _: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn ready_workflows<'a>(
        &'a self,
        _: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[derive(Default)]
struct Probe {
    started: Notify,
    executions: AtomicUsize,
    completed_executions: AtomicUsize,
    cancelled: AtomicBool,
    hooks: AtomicUsize,
}

struct Cancellation {
    probe: Arc<Probe>,
    completed: bool,
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        if !self.completed {
            self.probe.cancelled.store(true, Ordering::SeqCst);
        }
    }
}

struct TimedNode {
    execution: Duration,
}

#[derive(Clone)]
struct TimedContext {
    probe: Arc<Probe>,
    callback: Duration,
}

impl NodeContext for TimedContext {
    async fn fork(&self, _: &TaskRecord) -> Self {
        self.clone()
    }

    async fn on_success(&mut self, _: &TaskRecord, _: &Value) {
        self.probe.hooks.fetch_add(1, Ordering::SeqCst);
        if !self.callback.is_zero() {
            sleep(self.callback).await;
        }
    }
}

impl Node<TimedContext> for TimedNode {
    type In = i32;
    type Out = i32;

    async fn execute(&self, ctx: &mut TimedContext, input: i32) -> Result<i32, NodeError> {
        let mut cancellation = Cancellation {
            probe: ctx.probe.clone(),
            completed: false,
        };
        ctx.probe.executions.fetch_add(1, Ordering::SeqCst);
        ctx.probe.started.notify_one();
        sleep(self.execution).await;
        ctx.probe
            .completed_executions
            .fetch_add(1, Ordering::SeqCst);
        cancellation.completed = true;
        Ok(input)
    }
}

fn claim() -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        workflow_id: "renewal".into(),
        task_id: 1,
        run_id: 1,
        scope_id: None,
        parent_task_ids: vec![],
        name: "node".into(),
        status: TaskStatus::Running,
        lease_until: Some(now + chrono::Duration::seconds(300)),
        available_at: None,
        attempt: 1,
        input: json!(7),
        output: None,
        error: None,
        created_at: now,
        updated_at: now,
    }
}

async fn start_probe(
    store: Arc<Store>,
    execution_seconds: u64,
    callback_seconds: u64,
) -> (JoinHandle<()>, Arc<Probe>) {
    let probe = Arc::new(Probe::default());
    let callback = Duration::from_secs(callback_seconds);
    let graph = Arc::new(
        Graph::builder(
            "renewal",
            "Lease renewal",
            TimedContext {
                probe: probe.clone(),
                callback,
            },
        )
        .node(
            "node",
            TimedNode {
                execution: Duration::from_secs(execution_seconds),
            },
        )
        .build()
        .unwrap(),
    );
    let handle = tokio::spawn(async move {
        if let Some((task, outcome)) = run_task(
            graph.clone(),
            store,
            claim(),
            Instant::now() + LEASE_DURATION,
        )
        .await
        {
            notify_outcome(graph.as_ref(), &task, outcome).await;
        }
    });
    probe.started.notified().await;
    (handle, probe)
}

async fn elapse(seconds: u64) {
    advance(Duration::from_secs(seconds)).await;
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn long_execution_renews_repeatedly_without_restarting_or_timing_out() {
    let store = Store::new([], Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 650, 0).await;
    for renewals in 1..=6 {
        elapse(100).await;
        assert!(!handle.is_finished());
        assert_eq!(store.renewals.load(Ordering::SeqCst), renewals);
    }
    elapse(50).await;
    handle.await.unwrap();
    assert_eq!(probe.executions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.hooks.load(Ordering::SeqCst), 1);
    assert!(!probe.cancelled.load(Ordering::SeqCst));
    assert!(store.committed.load(Ordering::SeqCst));
    elapse(300).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 6);
}

#[tokio::test(start_paused = true)]
async fn lost_claim_cancels_execution_without_settlement_or_release() {
    let store = Store::new([Renewal::Lost], Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 1000, 0).await;
    elapse(100).await;
    handle.await.unwrap();
    assert!(probe.cancelled.load(Ordering::SeqCst));
    assert_eq!(store.settlements.load(Ordering::SeqCst), 0);
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn transient_renewal_error_retries_within_lease_and_recovers() {
    let store = Store::new([Renewal::Error, Renewal::Healthy], Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 350, 0).await;
    elapse(100).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 1);
    assert!(!probe.cancelled.load(Ordering::SeqCst));
    elapse(5).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 2);
    elapse(100).await;
    elapse(100).await;
    elapse(45).await;
    handle.await.unwrap();
    assert!(store.committed.load(Ordering::SeqCst));
    assert!(!probe.cancelled.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn persistent_errors_cancel_before_ready_business_can_run_at_the_lease_deadline() {
    let store = Store::new(std::iter::repeat_n(Renewal::Error, 100), Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 300, 0).await;
    elapse(100).await;
    for _ in 0..39 {
        elapse(5).await;
        assert!(!handle.is_finished());
    }
    elapse(5).await;
    handle.await.unwrap();
    assert!(probe.cancelled.load(Ordering::SeqCst));
    assert_eq!(probe.completed_executions.load(Ordering::SeqCst), 0);
    assert!(!store.committed.load(Ordering::SeqCst));
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn hanging_renewal_cannot_extend_the_last_confirmed_lease() {
    let store = Store::new([Renewal::Hanging], Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 1000, 0).await;
    elapse(100).await;
    assert_eq!(store.renewals.load(Ordering::SeqCst), 1);
    elapse(199).await;
    assert!(!handle.is_finished());
    elapse(1).await;
    handle.await.unwrap();
    assert!(probe.cancelled.load(Ordering::SeqCst));
    assert_eq!(store.settlements.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn settlement_keeps_renewing_until_commit() {
    let store = Store::new([], Duration::from_secs(399));
    let (handle, probe) = start_probe(store.clone(), 1, 0).await;
    elapse(1).await;
    assert_eq!(store.settlements.load(Ordering::SeqCst), 1);
    elapse(99).await;
    elapse(100).await;
    elapse(100).await;
    assert!(!handle.is_finished());
    assert_eq!(store.renewals.load(Ordering::SeqCst), 3);
    elapse(100).await;
    handle.await.unwrap();
    assert!(store.committed.load(Ordering::SeqCst));
    assert_eq!(probe.hooks.load(Ordering::SeqCst), 1);
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn committed_callback_has_no_engine_timeout_or_renewal() {
    let store = Store::new([], Duration::ZERO);
    let (handle, probe) = start_probe(store.clone(), 1, 1000).await;
    elapse(1).await;
    assert!(store.committed.load(Ordering::SeqCst));
    assert_eq!(probe.hooks.load(Ordering::SeqCst), 1);
    elapse(600).await;
    assert!(!handle.is_finished());
    assert_eq!(store.renewals.load(Ordering::SeqCst), 0);
    elapse(400).await;
    handle.await.unwrap();
    assert_eq!(store.settlements.load(Ordering::SeqCst), 1);
}
