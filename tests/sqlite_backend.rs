//! 通过公开 API 验证 DSL 图在 SQLite 上的完整调度、扇出和多源 JOIN。

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use workflow_engine::{
    Database, Engine, EngineError, Graph, JoinNode, Node, NodeContext, NodeError, Scheduler, Task,
    TaskRecord, TaskStatus,
    backend::sqlite::{SqliteTaskRepo, SqliteTaskStore, migrate},
    dsl,
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
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.run(async {
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
        }),
    )
    .await
    .expect("both runs finish before shutdown");

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

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker = tokio::spawn(async move {
        scheduler
            .run(async move {
                shutdown_rx.await.unwrap();
            })
            .await;
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
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("shutdown waits for both nodes to finish")
        .unwrap();
    assert!(
        started.is_ok(),
        "new work must start before the 60-second poll"
    );
    assert!(
        repo.list_all("scheduler-wake")
            .await
            .unwrap()
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded)
    );
    db.close().await;
}

#[tokio::test]
async fn workflows_with_different_contexts_start_and_stop_independently() {
    struct OtherContext(Context);

    impl NodeContext for OtherContext {
        async fn fork(&self, task: &TaskRecord) -> Self {
            Self(self.0.fork(task).await)
        }
    }

    impl Node<OtherContext> for AddOne {
        type In = i32;
        type Out = i32;

        async fn execute(&self, ctx: &mut OtherContext, input: i32) -> Result<i32, NodeError> {
            <Self as Node<Context>>::execute(self, &mut ctx.0, input).await
        }

        async fn on_success(ctx: &mut OtherContext, output: &Value) {
            <Self as Node<Context>>::on_success(&mut ctx.0, output).await;
        }
    }

    let engine = Engine::new("sqlite::memory:").await.unwrap();
    let (completed, mut results) = mpsc::unbounded_channel();
    let first_graph = Graph::builder(
        "first",
        "First",
        Context {
            completed: Some(completed.clone()),
            ..Context::default()
        },
    )
    .node("start", AddOne)
    .node("alternate", AddOne)
    .build()
    .unwrap();
    let second_graph = Graph::builder(
        "second",
        "Second",
        OtherContext(Context {
            completed: Some(completed),
            ..Context::default()
        }),
    )
    .node("start", AddOne)
    .build()
    .unwrap();
    let first = engine.make(first_graph);
    let second = engine.make(second_graph);
    drop(engine);
    let first_run = first.submit("start", 10).await.unwrap();
    let another_run = first.submit("alternate", 20).await.unwrap();
    let second_run = second.submit("start", 30).await.unwrap();
    assert_eq!(first_run.workflow_id, "first");
    assert_eq!(second_run.workflow_id, "second");
    assert_eq!(first_run.run_id, first_run.task_id);
    assert_eq!(second_run.run_id, second_run.task_id);
    assert_ne!(first_run.run_id, another_run.run_id);
    assert_eq!(first_run.task_id, 1);
    assert_eq!(second_run.task_id, 1);
    assert!(matches!(
        first.submit("missing", 0).await,
        Err(EngineError::UnknownNode(name)) if name == "missing"
    ));
    assert!(matches!(
        results.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    tokio::time::timeout(Duration::from_secs(5), first.run())
        .await
        .expect("run starts the scheduler and returns immediately")
        .unwrap();
    let mut outputs = tokio::time::timeout(Duration::from_secs(5), async {
        let mut outputs = Vec::new();
        for _ in 0..2 {
            let (task, output) = results.recv().await.unwrap();
            outputs.push((
                task.workflow_id,
                task.run_id.unwrap(),
                task.name,
                output.as_i64().unwrap(),
            ));
        }
        outputs
    })
    .await
    .expect("one Workflow run drives both of its submitted runs");
    outputs.sort();
    assert_eq!(
        outputs,
        [
            ("first".into(), first_run.run_id, "start".into(), 11),
            ("first".into(), another_run.run_id, "alternate".into(), 21),
        ]
    );
    assert!(matches!(
        results.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        first.run().await,
        Err(EngineError::AlreadyRunning)
    ));

    second.run().await.unwrap();
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("the second workflow starts only after its own run call")
        .unwrap();
    assert_eq!(task.workflow_id, "second");
    assert_eq!(task.run_id, Some(second_run.run_id));
    assert_eq!(output, json!(31));

    first.stop().await.unwrap();
    let paused_run = first.submit("alternate", 40).await.unwrap();
    let running_run = second.submit("start", 50).await.unwrap();
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("stopping the first workflow does not stop the second")
        .unwrap();
    assert_eq!(task.workflow_id, "second");
    assert_eq!(task.run_id, Some(running_run.run_id));
    assert_eq!(output, json!(51));
    assert!(matches!(
        results.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    first.run().await.unwrap();
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("restart executes the run submitted while stopped")
        .unwrap();
    assert_eq!(task.workflow_id, "first");
    assert_eq!(task.run_id, Some(paused_run.run_id));
    assert_eq!(task.name, "alternate");
    assert_eq!(output, json!(41));
    first.stop().await.unwrap();
    first.stop().await.unwrap();
    second.stop().await.unwrap();
}

#[tokio::test]
async fn workflow_shares_slots_across_runs_and_drains_in_flight_tasks_before_stopping() {
    struct Block {
        started: mpsc::UnboundedSender<i32>,
        release: Arc<Semaphore>,
    }

    impl Node<Context> for Block {
        type In = i32;
        type Out = i32;

        async fn execute(&self, _: &mut Context, input: i32) -> Result<i32, NodeError> {
            self.started.send(input).unwrap();
            self.release.acquire().await.unwrap().forget();
            Ok(input)
        }

        async fn on_success(ctx: &mut Context, output: &Value) {
            ctx.completed
                .as_ref()
                .unwrap()
                .send((ctx.task.as_ref().unwrap().clone(), output.clone()))
                .unwrap();
        }
    }

    let engine = Engine::new("sqlite::memory:").await.unwrap();
    let (started, mut starts) = mpsc::unbounded_channel();
    let (completed, mut completions) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let graph = Graph::builder(
        "shared-slots",
        "Shared slots",
        Context {
            completed: Some(completed),
            ..Context::default()
        },
    )
    .node(
        "start",
        Block {
            started,
            release: release.clone(),
        },
    )
    .build()
    .unwrap();
    let workflow = engine.make(graph);
    for input in 0..12 {
        workflow.submit("start", input).await.unwrap();
    }
    workflow.run().await.unwrap();
    let mut inputs = tokio::time::timeout(Duration::from_secs(5), async {
        let mut inputs = Vec::new();
        for _ in 0..8 {
            inputs.push(starts.recv().await.unwrap());
        }
        inputs
    })
    .await
    .expect("all runs share the workflow's eight execution slots");
    let ninth = tokio::time::timeout(Duration::from_millis(100), starts.recv()).await;
    assert!(ninth.is_err(), "the ninth run must wait for a shared slot");
    release.add_permits(1);
    inputs.push(
        tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .expect("completion frees a slot for the next run")
            .unwrap(),
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(100), workflow.stop())
            .await
            .is_err(),
        "stop must wait for all eight in-flight nodes"
    );
    assert!(
        matches!(workflow.run().await, Err(EngineError::AlreadyRunning)),
        "cancelling the stop waiter must retain the draining worker"
    );
    release.add_permits(8);
    tokio::time::timeout(Duration::from_secs(5), workflow.stop())
        .await
        .expect("a second stop waits for the original in-flight tasks to settle")
        .unwrap();
    let mut completed_runs = Vec::new();
    for _ in 0..9 {
        let (task, _) = completions
            .try_recv()
            .expect("task settled before stop returned");
        completed_runs.push(task.run_id.unwrap());
    }
    assert!(matches!(
        starts.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        completions.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    workflow.submit("start", 99).await.unwrap();
    assert!(matches!(
        starts.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    release.add_permits(4);
    workflow.run().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..4 {
            inputs.push(starts.recv().await.unwrap());
            let (task, _) = completions.recv().await.unwrap();
            completed_runs.push(task.run_id.unwrap());
        }
    })
    .await
    .expect("restart executes the remaining runs and the new submission");
    workflow.stop().await.unwrap();
    inputs.sort_unstable();
    assert_eq!(inputs, (0..12).chain([99]).collect::<Vec<_>>());
    completed_runs.sort_unstable();
    completed_runs.dedup();
    assert_eq!(completed_runs.len(), 13);
}
