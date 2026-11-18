//! DSL 图在 SQLite 内部存储和调度器上的扇出、JOIN 与唤醒行为。

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::{Notify, mpsc};

use crate::{
    backend::sqlite::{SqliteTaskRepo, SqliteTaskStore, migrate},
    database::Database,
    dsl,
    graph::{JoinNode, Node, NodeContext, NodeError},
    scheduler::Scheduler,
    task::{Task, TaskRecord, TaskStatus},
};

#[derive(Default)]
struct Context {
    task: Option<TaskRecord>,
    completed: Option<mpsc::UnboundedSender<(TaskRecord, Value)>>,
}

impl NodeContext for Context {
    async fn fork(&self, task: &TaskRecord) -> Self {
        Self {
            task: Some(task.clone()),
            completed: self.completed.clone(),
        }
    }
}

struct AddOne;

impl Node<Context> for AddOne {
    type In = i32;
    type Out = i32;

    async fn execute(&self, ctx: &mut Context, input: i32) -> Result<i32, NodeError> {
        let task = ctx
            .task
            .as_ref()
            .expect("scheduler supplies a task snapshot");
        assert_eq!(task.input, json!(input));
        assert_eq!(task.status, TaskStatus::Running);
        Ok(input + 1)
    }

    async fn on_success(ctx: &mut Context, output: &Value) {
        if let Some(completed) = &ctx.completed {
            completed
                .send((ctx.task.as_ref().unwrap().clone(), output.clone()))
                .unwrap();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Collected {
    worker: Vec<i32>,
    summary: i32,
}

struct Sum;

impl Node<Context> for Sum {
    type In = Collected;
    type Out = i32;

    async fn execute(&self, _: &mut Context, input: Collected) -> Result<i32, NodeError> {
        Ok(input.worker.into_iter().sum::<i32>() + input.summary)
    }
}

#[tokio::test]
async fn dsl_workflow_persists_fanout_join_and_final_output() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    database.initialize().await.unwrap();
    let pool = match &database {
        Database::Sqlite(pool) => pool.clone(),
        #[cfg(feature = "pg")]
        Database::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
    };
    let repo = SqliteTaskRepo::new(pool);

    // start ─┬─ worker × 2 ─┬─ collect (JOIN) ─ total
    //        └─ summary ────┘
    let graph = dsl::workflow! {
        workflow_id: "dsl-workflow";
        title: "扇出与汇合";
        context: Context::default();
        nodes {
            start => AddOne,
            worker => AddOne,
            summary => AddOne,
            collect => JoinNode,
            total => Sum,
        }
        edges {
            start -> worker |> |output| vec![*output, *output + 10],
            start -> summary,
            worker -> collect |> |output| vec![json!({"worker": output})],
            summary -> collect |> |output| vec![json!({"summary": output})],
            collect -> total |> |output: &Value| vec![
                serde_json::from_value::<Collected>(output.clone())
                    .expect("JOIN contains both worker outputs and the summary"),
            ],
        }
    }
    .unwrap();

    let root = repo
        .create_run("dsl-workflow", "start", &json!(0), 3)
        .await
        .unwrap();
    let other_root = repo
        .create_run("dsl-workflow", "start", &json!(100), 3)
        .await
        .unwrap();
    assert_eq!(root.run_id, Some(root.task_id));
    assert_eq!(other_root.run_id, Some(other_root.task_id));
    assert_ne!(root.run_id, other_root.run_id);
    let store = SqliteTaskStore::new(repo.clone()).with_max_attempts(5);
    let scheduler = Scheduler::new(Arc::new(graph), Arc::new(store), Duration::from_millis(2))
        .with_max_concurrency(3);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = scheduler.run() => panic!("scheduler must run until cancelled"),
            _ = async {
                loop {
                    let tasks = repo.list_all("dsl-workflow").await.unwrap();
                    if tasks.len() == 12
                        && tasks
                            .iter()
                            .all(|task| task.status == TaskStatus::Succeeded)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } => {}
        }
    })
    .await
    .expect("both runs finish before cancelling the scheduler");

    let all_tasks = repo.list_all(&root.workflow_id).await.unwrap();
    let tasks: Vec<_> = all_tasks
        .iter()
        .filter(|task| task.run_id == root.run_id)
        .collect();
    assert_eq!(
        tasks.len(),
        6,
        "root, two workers, summary, JOIN, and total"
    );
    for task in &tasks {
        assert_eq!(task.status, TaskStatus::Succeeded, "{task:?}");
        assert_eq!(task.attempt, 1);
        assert!(task.error.is_none());
        assert!(task.lease_until.is_none());
        if task.task_id != root.task_id {
            assert_eq!(task.max_attempts, 5);
        }
    }
    assert_eq!(tasks[0].output, Some(json!(1)));

    let mut workers: Vec<_> = tasks.iter().filter(|task| task.name == "worker").collect();
    workers.sort_unstable_by_key(|task| task.instance_id);
    assert_eq!(workers.len(), 2);
    for (worker, instance_id, input, output) in [(workers[0], 0, 1, 2), (workers[1], 1, 11, 12)] {
        assert_eq!(worker.instance_id, Some(instance_id));
        assert_eq!(worker.input, json!(input));
        assert_eq!(worker.output, Some(json!(output)));
        assert_eq!(worker.depends_on_ids, [root.task_id]);
    }

    let summary = tasks.iter().find(|task| task.name == "summary").unwrap();
    assert_eq!(summary.input, json!(1));
    assert_eq!(summary.output, Some(json!(2)));
    assert_eq!(summary.depends_on_ids, [root.task_id]);

    let join = tasks.iter().find(|task| task.name == "collect").unwrap();
    assert_eq!(join.task_type, "join");
    assert_eq!(join.instance_id, None);
    let mut predecessors = join.depends_on_names.clone();
    predecessors.sort();
    assert_eq!(predecessors, ["summary", "worker"]);
    let mut dependency_ids = join.depends_on_ids.clone();
    dependency_ids.sort_unstable();
    let mut expected_ids = vec![workers[0].task_id, workers[1].task_id, summary.task_id];
    expected_ids.sort_unstable();
    assert_eq!(dependency_ids, expected_ids);
    let mut collected: Collected = serde_json::from_value(join.input.clone()).unwrap();
    collected.worker.sort_unstable();
    assert_eq!(collected.worker, [2, 12]);
    assert_eq!(collected.summary, 2);
    assert_eq!(join.output.as_ref(), Some(&join.input));

    let total = tasks.iter().find(|task| task.name == "total").unwrap();
    assert_eq!(total.depends_on_ids, [join.task_id]);
    assert_eq!(total.input, join.input);
    assert_eq!(total.output, Some(json!(16)));

    let other_tasks: Vec<_> = all_tasks
        .iter()
        .filter(|task| task.run_id == other_root.run_id)
        .collect();
    assert_eq!(other_tasks.len(), 6);
    for task in &other_tasks {
        assert!(
            task.depends_on_ids
                .iter()
                .all(|id| { other_tasks.iter().any(|other| other.task_id == *id) })
        );
    }
    let other_total = other_tasks
        .iter()
        .find(|task| task.name == "total")
        .unwrap();
    assert_eq!(other_total.output, Some(json!(316)));
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());
    database.close().await;
}

struct SignalNode {
    started: Arc<Notify>,
    release: Option<Arc<Notify>>,
}

impl Node<Context> for SignalNode {
    type In = i32;
    type Out = i32;

    async fn execute(&self, _: &mut Context, input: i32) -> Result<i32, NodeError> {
        self.started.notify_one();
        if let Some(release) = &self.release {
            release.notified().await;
        }
        Ok(input)
    }
}

#[tokio::test]
async fn scheduler_wakes_for_new_work_while_another_node_is_running() {
    let (released_tx, mut released_rx) = mpsc::unbounded_channel();
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .after_release(move |_, _| {
            let released_tx = released_tx.clone();
            Box::pin(async move {
                let _ = released_tx.send(());
                Ok(true)
            })
        })
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&db).await.unwrap();
    let repo = SqliteTaskRepo::new(db.clone());
    let mut task = Task {
        workflow_id: "scheduler-wake".into(),
        run_id: None,
        name: "slow".into(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!(0),
    };
    repo.enqueue(&task).await.unwrap();

    let slow_started = Arc::new(Notify::new());
    let fast_started = Arc::new(Notify::new());
    let release_slow = Arc::new(Notify::new());
    let graph = dsl::workflow! {
        workflow_id: "scheduler-wake";
        title: "调度唤醒";
        context: Context::default();
        nodes {
            slow => SignalNode { started: slow_started.clone(), release: Some(release_slow.clone()) },
            fast => SignalNode { started: fast_started.clone(), release: None },
        }
        edges {}
    }
    .unwrap();
    let wake = Arc::new(Notify::new());
    let scheduler = Scheduler::new(
        Arc::new(graph),
        Arc::new(SqliteTaskStore::new(repo.clone())),
        Duration::from_secs(60),
    )
    .with_max_concurrency(2)
    .with_wake(wake.clone());

    // 清空建库和入队产生的归还事件。单连接池保证此后仅观察调度器的认领事务。
    let connection = db.acquire().await.unwrap();
    while released_rx.try_recv().is_ok() {}
    drop(connection);
    released_rx.recv().await.unwrap();

    let worker = tokio::spawn(async move {
        scheduler.run().await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        slow_started.notified().await;
        // 第一次认领 slow；第二次认领为空，此时仍有一个空槽，调度器进入等待。
        released_rx.recv().await.unwrap();
        released_rx.recv().await.unwrap();
    })
    .await
    .expect("scheduler starts the slow node and finishes its next claim");

    task.name = "fast".into();
    repo.enqueue(&task).await.unwrap();
    wake.notify_one();
    let started = tokio::time::timeout(Duration::from_secs(5), fast_started.notified()).await;

    release_slow.notify_one();
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let tasks = repo.list_all("scheduler-wake").await.unwrap();
            if tasks.len() == 2
                && tasks
                    .iter()
                    .all(|task| task.status == TaskStatus::Succeeded)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        started.is_ok(),
        "new work must start before the 60-second poll"
    );
    settled.expect("both nodes settle before cancelling the scheduler");
    db.close().await;
}
