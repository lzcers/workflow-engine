//! Fixed processing windows release slots and recover through the graph's failure path.

use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
use tokio::{sync::mpsc, task::JoinSet, time::timeout};

use super::{Scheduler, run_task};
use crate::{
    Graph, Node, NodeContext, NodeError, Task, TaskRecord, TaskStatus,
    backend::sqlite::{SqliteTaskRepo, SqliteTaskStore, migrate},
    edge::on_error,
};

struct Context {
    task: Option<TaskRecord>,
    events: mpsc::UnboundedSender<(&'static str, String)>,
    repo: SqliteTaskRepo,
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
            repo: self.repo.clone(),
        }
    }
}

struct PendingNode;

impl Node<Context> for PendingNode {
    type In = i32;
    type Out = i32;

    async fn on_start(ctx: &mut Context, _: &Value) {
        ctx.record("start");
    }

    async fn execute(&self, ctx: &mut Context, _: i32) -> Result<i32, NodeError> {
        ctx.record("execute");
        pending().await
    }

    async fn on_error(ctx: &mut Context, error: &NodeError) {
        let task = ctx.task.as_ref().unwrap();
        let stored = ctx
            .repo
            .get(&task.workflow_id, task.task_id)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            stored.status,
            TaskStatus::Failed | TaskStatus::Superseded
        ));
        ctx.events.send(("error", error.to_string())).unwrap();
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

    async fn on_success(ctx: &mut Context, _: &Value) {
        ctx.record("success");
    }
}

async fn memory_repo() -> (SqlitePool, SqliteTaskRepo) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let repo = SqliteTaskRepo::new(pool.clone());
    (pool, repo)
}

async fn set_deadline(
    pool: &SqlitePool,
    repo: &SqliteTaskRepo,
    task: &TaskRecord,
    deadline: chrono::DateTime<chrono::Utc>,
) -> TaskRecord {
    sqlx::query("UPDATE tasks SET lease_until = ? WHERE workflow_id = ? AND task_id = ?")
        .bind(deadline)
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
async fn expired_full_slot_recovers_through_failure_edge_without_executing_again() {
    let (pool, repo) = memory_repo().await;
    let (events, mut receiver) = mpsc::unbounded_channel();
    let graph = Arc::new(
        Graph::builder(
            "timeout",
            "Timeout recovery",
            Context {
                task: None,
                events,
                repo: repo.clone(),
            },
        )
        .node("pending", PendingNode)
        .node("recovery", SuccessNode)
        .error_edge(on_error("pending", "recovery", |input: &i32, error| {
            assert!(error.to_string().contains("execution window expired"));
            vec![*input]
        }))
        .build()
        .unwrap(),
    );
    let mut input = Task::new("timeout", "pending", json!(41));
    input.max_attempts = 1;
    repo.enqueue(&input).await.unwrap();
    let claim = repo
        .claim_many("timeout", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    let claim = set_deadline(
        &pool,
        &repo,
        &claim,
        chrono::Utc::now() + chrono::Duration::milliseconds(100),
    )
    .await;
    let store = Arc::new(SqliteTaskStore::new(repo.clone()));
    let scheduler = Scheduler::new(graph.clone(), store.clone(), Duration::from_millis(2))
        .with_max_concurrency(1);
    let mut in_flight = JoinSet::new();
    in_flight.spawn(run_task(graph, store, claim.clone()));
    let seen = timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = scheduler.run_loop(&mut in_flight, "worker") => panic!("scheduler exited"),
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
    .expect("the full slot must expire so recovery and its successor can execute");
    in_flight.shutdown().await;

    assert_eq!(
        seen.iter()
            .filter(|event| **event == ("execute", "pending".to_owned()))
            .count(),
        1
    );
    assert_eq!(
        seen.iter()
            .filter(|event| **event == ("start", "pending".to_owned()))
            .count(),
        1
    );
    assert_eq!(seen.iter().filter(|event| event.0 == "error").count(), 1);
    let tasks = repo.list_all("timeout").await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].task_id, claim.task_id);
    assert_eq!(tasks[0].status, TaskStatus::Superseded);
    assert_eq!(tasks[0].attempt, 2);
    assert_eq!(tasks[1].status, TaskStatus::Succeeded);
    assert_eq!(tasks[1].output, Some(json!(42)));
    assert_eq!(tasks[1].parent_task_id, Some(claim.task_id));
    assert!(tasks[1].depends_on_ids.is_empty());
    pool.close().await;
}

#[tokio::test]
async fn already_expired_snapshot_never_enters_business_or_settlement() {
    let (pool, repo) = memory_repo().await;
    let (events, mut receiver) = mpsc::unbounded_channel();
    let graph = Arc::new(
        Graph::builder(
            "expired",
            "Expired claim",
            Context {
                task: None,
                events,
                repo: repo.clone(),
            },
        )
        .node("pending", PendingNode)
        .build()
        .unwrap(),
    );
    repo.enqueue(&Task::new("expired", "pending", json!(0)))
        .await
        .unwrap();
    let claim = repo
        .claim_many("expired", "worker", 1)
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
    timeout(
        Duration::from_secs(1),
        run_task(
            graph,
            Arc::new(SqliteTaskStore::new(repo.clone())),
            claim.clone(),
        ),
    )
    .await
    .expect("an expired snapshot must exit before calling fork or a node hook");
    assert!(receiver.try_recv().is_err());
    let stored = repo.get("expired", claim.task_id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(stored).unwrap(),
        serde_json::to_value(claim).unwrap()
    );
    pool.close().await;
}

#[tokio::test]
async fn success_hook_timeout_keeps_committed_success_and_ends_local_work() {
    struct PendingSuccess;

    impl Node<Context> for PendingSuccess {
        type In = i32;
        type Out = i32;

        async fn execute(&self, ctx: &mut Context, input: i32) -> Result<i32, NodeError> {
            ctx.record("execute");
            Ok(input)
        }

        async fn on_success(ctx: &mut Context, _: &Value) {
            ctx.record("success");
            pending().await
        }
    }

    let (pool, repo) = memory_repo().await;
    let (events, mut receiver) = mpsc::unbounded_channel();
    let graph = Arc::new(
        Graph::builder(
            "callback",
            "Slow callback",
            Context {
                task: None,
                events,
                repo: repo.clone(),
            },
        )
        .node("success", PendingSuccess)
        .build()
        .unwrap(),
    );
    repo.enqueue(&Task::new("callback", "success", json!(7)))
        .await
        .unwrap();
    let claim = repo
        .claim_many("callback", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    let claim = set_deadline(
        &pool,
        &repo,
        &claim,
        chrono::Utc::now() + chrono::Duration::milliseconds(100),
    )
    .await;
    timeout(
        Duration::from_secs(2),
        run_task(
            graph,
            Arc::new(SqliteTaskStore::new(repo.clone())),
            claim.clone(),
        ),
    )
    .await
    .expect("a pending success hook must not hold the slot beyond the claim deadline");
    let mut seen = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        seen.push(event);
    }
    assert!(seen.contains(&("success", "success".to_owned())));
    let stored = repo.get("callback", claim.task_id).await.unwrap().unwrap();
    assert_eq!(stored.status, TaskStatus::Succeeded);
    assert_eq!(stored.output, Some(json!(7)));
    assert_eq!(stored.attempt, 1);
    assert!(
        repo.claim_many("callback", "next-worker", 1)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
}

#[tokio::test]
async fn blocked_context_fork_does_not_prevent_final_failure_recovery() {
    struct BlockedContext {
        forks: Arc<AtomicUsize>,
        executions: Arc<AtomicUsize>,
    }

    impl NodeContext for BlockedContext {
        async fn fork(&self, _: &TaskRecord) -> Self {
            self.forks.fetch_add(1, Ordering::SeqCst);
            pending().await
        }
    }

    struct NodeAfterFork;

    impl Node<BlockedContext> for NodeAfterFork {
        type In = i32;
        type Out = i32;

        async fn execute(&self, ctx: &mut BlockedContext, input: i32) -> Result<i32, NodeError> {
            ctx.executions.fetch_add(1, Ordering::SeqCst);
            Ok(input)
        }
    }

    let (pool, repo) = memory_repo().await;
    let forks = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let graph = Arc::new(
        Graph::builder(
            "blocked-fork",
            "Blocked context",
            BlockedContext {
                forks: forks.clone(),
                executions: executions.clone(),
            },
        )
        .node("root", NodeAfterFork)
        .build()
        .unwrap(),
    );
    let mut task = Task::new("blocked-fork", "root", json!(0));
    task.max_attempts = 1;
    repo.enqueue(&task).await.unwrap();
    let claim = repo
        .claim_many("blocked-fork", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    let claim = set_deadline(
        &pool,
        &repo,
        &claim,
        chrono::Utc::now() + chrono::Duration::milliseconds(100),
    )
    .await;
    let store = Arc::new(SqliteTaskStore::new(repo.clone()));
    let scheduler = Scheduler::new(graph.clone(), store.clone(), Duration::from_millis(2))
        .with_max_concurrency(1);
    let mut in_flight = JoinSet::new();
    in_flight.spawn(run_task(graph, store, claim.clone()));
    let failed = timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = scheduler.run_loop(&mut in_flight, "worker") => panic!("scheduler exited"),
            failed = async {
                loop {
                    let task = repo.get("blocked-fork", claim.task_id).await.unwrap().unwrap();
                    if task.status == TaskStatus::Failed {
                        return task;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } => failed,
        }
    })
    .await
    .expect("failure recovery must commit even when context creation never returns");
    in_flight.shutdown().await;
    assert_eq!(failed.attempt, 2);
    assert!(failed.error.unwrap().contains("execution window expired"));
    assert!(forks.load(Ordering::SeqCst) >= 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    pool.close().await;
}
