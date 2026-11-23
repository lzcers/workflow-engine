//! 通过公开 API 验证工作流创建恢复、提交执行与并发额度。

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::sync::{Semaphore, mpsc};
use workflow_engine::{
    Engine, EngineError, Graph, TaskRecord, TaskStatus, Trigger,
    graph::{Node, NodeContext, NodeError, TaskType},
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

#[tokio::test]
async fn event_discovery_and_external_submissions_preserve_type_and_independent_runs() {
    struct ScheduledInput;

    impl Node<Context> for ScheduledInput {
        type In = i32;
        type Out = i32;
        const TASK_TYPE: TaskType = TaskType::Event;

        fn trigger(&self) -> Option<Trigger> {
            Some(Trigger {
                kind: "cron".into(),
                config: json!({"expression": "0 9 * * *", "timezone": "Asia/Shanghai"}),
            })
        }

        async fn execute(&self, _: &mut Context, input: i32) -> Result<i32, NodeError> {
            Ok(input + 10)
        }

        async fn on_success(ctx: &mut Context, output: &Value) {
            <AddOne as Node<Context>>::on_success(ctx, output).await;
        }
    }

    let engine = Engine::new("sqlite::memory:").await.unwrap();
    let (completed, mut results) = mpsc::unbounded_channel();
    let graph = workflow_engine::workflow! {
        workflow_id: "external-events";
        title: "External events";
        context: Context { completed: Some(completed), ..Context::default() };
        nodes {
            tick => ScheduledInput,
            process => AddOne,
        }
        edges {
            tick -> process,
        }
    }
    .unwrap();
    let workflow = engine.make(graph);
    let triggers = workflow.triggers();
    assert_eq!(triggers.len(), 1);
    let (entry, trigger) = &triggers[0];
    assert_eq!(*entry, "tick");
    assert_eq!(trigger.kind, "cron");
    assert_eq!(trigger.config["expression"], "0 9 * * *");

    // The external registration uses the discovered entry for each delivered event.
    let first = workflow.submit(entry, 10).await.unwrap();
    let second = workflow.submit(entry, 20).await.unwrap();
    let manual = workflow.submit("process", 30).await.unwrap();
    assert_ne!(first.run_id, second.run_id);
    assert_ne!(manual.run_id, first.run_id);
    assert_ne!(manual.run_id, second.run_id);

    let mut observed = tokio::time::timeout(Duration::from_secs(5), async {
        let mut observed = Vec::new();
        for _ in 0..5 {
            let (task, output) = results.recv().await.unwrap();
            observed.push((
                task.run_id.unwrap(),
                task.name,
                task.parent_task_ids,
                task.input.as_i64().unwrap(),
                output.as_i64().unwrap(),
            ));
        }
        observed
    })
    .await
    .expect("each event executes its entry and successor, alongside a manual submission");
    observed.sort();
    let mut expected = vec![
        (first.run_id, "tick".into(), vec![], 10, 20),
        (first.run_id, "process".into(), vec![first.run_id], 20, 21),
        (second.run_id, "tick".into(), vec![], 20, 30),
        (second.run_id, "process".into(), vec![second.run_id], 30, 31),
        (manual.run_id, "process".into(), vec![], 30, 31),
    ];
    expected.sort();
    assert_eq!(observed, expected);
}

#[tokio::test]
async fn make_recovers_persisted_tasks_without_submit() {
    let path = std::env::temp_dir().join(format!("workflow-recovery-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let engine = Engine::new(&url).await.unwrap();
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    sqlx::query(
        "INSERT INTO tasks (workflow_id, run_id, task_id, name, status, input_json,
                            attempt, lease_until)
         VALUES ('recovery', 1, 1, 'start', 'pending', '10', 0, NULL),
                ('recovery', 2, 2, 'start', 'running', '20', 1,
                 '2000-01-01T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    drop(engine);

    let engine = Engine::new(&url).await.unwrap();
    let (completed, mut results) = mpsc::unbounded_channel();
    let graph = Graph::builder(
        "recovery",
        "Recovery",
        Context {
            completed: Some(completed),
            ..Context::default()
        },
    )
    .node("start", AddOne)
    .build()
    .unwrap();
    let workflow = engine.make(graph);
    let mut recovered = tokio::time::timeout(Duration::from_secs(5), async {
        let mut recovered = Vec::new();
        for _ in 0..2 {
            let (task, output) = results.recv().await.unwrap();
            assert_eq!(task.workflow_id, "recovery");
            recovered.push((
                task.task_id,
                task.run_id,
                task.attempt,
                output.as_i64().unwrap(),
            ));
        }
        recovered
    })
    .await
    .expect("make starts pending and expired running tasks without a new submit");
    recovered.sort_unstable();
    assert_eq!(recovered, [(1, Some(1), 1, 11), (2, Some(2), 2, 21)]);

    drop(workflow);
    drop(engine);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn workflows_with_different_contexts_execute_on_submit_independently() {
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
    let first_description = first_graph.describe();
    let [first, second] = [engine.make(first_graph), engine.make(second_graph)];
    drop(engine);
    assert_eq!(first.graph().workflow_id(), "first");
    assert_eq!(first.graph().title(), "First");
    assert_eq!(first.graph().describe(), first_description);
    assert_eq!(first.graph().task_type_of("start"), Some(TaskType::Normal));
    assert_eq!(first.graph().task_type_of("missing"), None);
    let first_graph = first.graph_as::<Context>().expect("original context type");
    assert_eq!(first_graph.describe(), first_description);
    assert!(first_graph.context().completed.is_some());
    assert!(first.graph_as::<OtherContext>().is_none());
    assert!(second.graph_as::<Context>().is_none());
    let second_graph = second
        .graph_as::<OtherContext>()
        .expect("second workflow retains its own graph type");
    assert_eq!(second_graph.workflow_id(), "second");
    assert!(second_graph.context().0.completed.is_some());
    let first_run = first.submit("start", 10).await.unwrap();
    let another_run = first.submit("alternate", 20).await.unwrap();
    let second_run = second.submit("start", 30).await.unwrap();
    assert_eq!(first_run.workflow_id, "first");
    assert_eq!(second_run.workflow_id, "second");
    assert_ne!(first_run.run_id, another_run.run_id);
    assert_ne!(first_run.run_id, second_run.run_id);
    assert_ne!(another_run.run_id, second_run.run_id);
    assert!(matches!(
        first.submit("missing", 0).await,
        Err(EngineError::UnknownNode(name)) if name == "missing"
    ));
    let mut outputs = tokio::time::timeout(Duration::from_secs(5), async {
        let mut outputs = Vec::new();
        for _ in 0..3 {
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
    .expect("submitting to either workflow automatically executes its runs");
    outputs.sort();
    assert_eq!(
        outputs,
        [
            ("first".into(), first_run.run_id, "start".into(), 11),
            ("first".into(), another_run.run_id, "alternate".into(), 21),
            ("second".into(), second_run.run_id, "start".into(), 31),
        ]
    );
    assert!(matches!(
        results.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let next_run = first.submit("alternate", 40).await.unwrap();
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("a subsequent submit wakes the existing scheduler")
        .unwrap();
    assert_eq!(task.workflow_id, "first");
    assert_eq!(task.run_id, Some(next_run.run_id));
    assert_eq!(task.name, "alternate");
    assert_eq!(output, json!(41));

    drop(first);
    let next_run = second.submit("start", 50).await.unwrap();
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("dropping the first workflow does not stop the second")
        .unwrap();
    assert_eq!(task.workflow_id, "second");
    assert_eq!(task.run_id, Some(next_run.run_id));
    assert_eq!(output, json!(51));
}

#[tokio::test]
async fn workflow_shares_slots_across_concurrent_submissions() {
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
    let runs = tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all((0..12).map(|input| workflow.submit("start", input))),
    )
    .await
    .expect("concurrent submissions return without waiting for blocked nodes");
    let mut submitted_runs: Vec<_> = runs.into_iter().map(|run| run.unwrap().run_id).collect();
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

    release.add_permits(11);
    let mut completed_runs = tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..3 {
            inputs.push(starts.recv().await.unwrap());
        }
        let mut completed_runs = Vec::new();
        for _ in 0..12 {
            let (task, _) = completions.recv().await.unwrap();
            completed_runs.push(task.run_id.unwrap());
        }
        completed_runs
    })
    .await
    .expect("all submitted runs execute and settle as slots become available");
    inputs.sort_unstable();
    assert_eq!(inputs, (0..12).collect::<Vec<_>>());
    submitted_runs.sort_unstable();
    completed_runs.sort_unstable();
    assert_eq!(completed_runs, submitted_runs);
    submitted_runs.dedup();
    assert_eq!(submitted_runs.len(), 12);

    let next_run = workflow.submit("start", 99).await.unwrap();
    let input = tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .expect("the scheduler executes another submission after becoming idle")
        .unwrap();
    assert_eq!(input, 99);
    release.add_permits(1);
    let (task, output) = tokio::time::timeout(Duration::from_secs(5), completions.recv())
        .await
        .expect("the later run settles without an explicit start")
        .unwrap();
    assert_eq!(task.run_id, Some(next_run.run_id));
    assert_eq!(output, json!(99));
}
