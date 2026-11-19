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
        .enqueue(&Task::new("dsl-workflow", "start", json!(0)))
        .await
        .unwrap();
    let other_root = repo
        .enqueue(&Task::new("dsl-workflow", "start", json!(100)))
        .await
        .unwrap();
    assert_eq!(root.run_id, Some(root.task_id));
    assert!(root.parent_task_ids.is_empty());
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
    workers.sort_unstable_by_key(|task| task.task_id);
    assert_eq!(workers.len(), 2);
    for (worker, input, output) in [(workers[0], 1, 2), (workers[1], 11, 12)] {
        assert_eq!(worker.input, json!(input));
        assert_eq!(worker.output, Some(json!(output)));
        assert_eq!(worker.parent_task_ids, [root.task_id]);
    }

    let summary = tasks.iter().find(|task| task.name == "summary").unwrap();
    assert_eq!(summary.input, json!(1));
    assert_eq!(summary.output, Some(json!(2)));
    assert_eq!(summary.parent_task_ids, [root.task_id]);

    assert_eq!(
        tasks.iter().filter(|task| task.name == "collect").count(),
        1
    );
    let join = tasks.iter().find(|task| task.name == "collect").unwrap();
    assert_eq!(join.task_type, "join");
    let mut predecessors = join
        .join_state
        .expected
        .iter()
        .map(|dependency| dependency.name.clone())
        .collect::<Vec<_>>();
    predecessors.sort();
    assert_eq!(predecessors, ["summary", "worker"]);
    let mut parent_ids = join.parent_task_ids.clone();
    parent_ids.sort_unstable();
    let mut expected_ids = vec![workers[0].task_id, workers[1].task_id, summary.task_id];
    expected_ids.sort_unstable();
    assert_eq!(parent_ids, expected_ids);
    let mut collected: Collected = serde_json::from_value(join.input.clone()).unwrap();
    collected.worker.sort_unstable();
    assert_eq!(collected.worker, [2, 12]);
    assert_eq!(collected.summary, 2);
    assert_eq!(join.output.as_ref(), Some(&join.input));

    let total = tasks.iter().find(|task| task.name == "total").unwrap();
    assert_eq!(total.parent_task_ids, [join.task_id]);
    assert_eq!(total.input, join.input);
    assert_eq!(total.output, Some(json!(16)));

    let other_tasks: Vec<_> = all_tasks
        .iter()
        .filter(|task| task.run_id == other_root.run_id)
        .collect();
    assert_eq!(other_tasks.len(), 6);
    for task in &other_tasks {
        assert!(
            task.parent_task_ids
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

#[tokio::test]
async fn nested_fanout_preserves_each_parent_and_single_input_successor() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    database.initialize().await.unwrap();
    let pool = match &database {
        Database::Sqlite(pool) => pool.clone(),
        #[cfg(feature = "pg")]
        Database::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
    };
    let repo = SqliteTaskRepo::new(pool);
    let (completed, mut completions) = mpsc::unbounded_channel();
    let graph = dsl::workflow! {
        workflow_id: "nested-fanout";
        title: "两层扇出";
        context: Context { task: None, completed: Some(completed) };
        nodes {
            start => AddOne,
            a => AddOne,
            b => AddOne,
            c => AddOne,
        }
        edges {
            start -> a |> |output| vec![*output, *output + 10],
            a -> b |> |output| vec![*output, *output + 100],
            b -> c,
        }
    }
    .unwrap();
    let root = repo
        .enqueue(&Task::new("nested-fanout", "start", json!(0)))
        .await
        .unwrap();
    let scheduler = Scheduler::new(
        Arc::new(graph),
        Arc::new(SqliteTaskStore::new(repo.clone())),
        Duration::from_millis(2),
    )
    .with_max_concurrency(3);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = scheduler.run() => panic!("scheduler must run until cancelled"),
            _ = async {
                for _ in 0..11 {
                    completions.recv().await.expect("every task completes");
                }
            } => {}
        }
    })
    .await
    .expect("root, two A tasks, four B tasks and four C tasks all execute");

    let tasks = repo.list_all("nested-fanout").await.unwrap();
    assert_eq!(tasks.len(), 11);
    assert!(tasks.iter().all(|task| task.run_id == root.run_id
        && task.status == TaskStatus::Succeeded
        && task.attempt == 1));
    let mut parents: Vec<_> = tasks.iter().filter(|task| task.name == "a").collect();
    parents.sort_unstable_by_key(|task| task.task_id);
    assert_eq!(parents.len(), 2);
    assert_eq!(tasks.iter().filter(|task| task.name == "b").count(), 4);
    assert_eq!(tasks.iter().filter(|task| task.name == "c").count(), 4);
    for (parent, parent_input) in [(parents[0], 1), (parents[1], 11)] {
        assert_eq!(parent.parent_task_ids, [root.task_id]);
        assert_eq!(parent.input, json!(parent_input));
        let mut children: Vec<_> = tasks
            .iter()
            .filter(|task| task.name == "b" && task.parent_task_ids == [parent.task_id])
            .collect();
        children.sort_unstable_by_key(|task| task.task_id);
        assert_eq!(children.len(), 2, "each A must produce its own two B tasks");
        for (child, input) in [
            (children[0], parent_input + 1),
            (children[1], parent_input + 101),
        ] {
            assert_eq!(child.parent_task_ids, [parent.task_id]);
            assert_eq!(child.input, json!(input));
            assert_eq!(child.output, Some(json!(input + 1)));
            let successors: Vec<_> = tasks
                .iter()
                .filter(|task| task.name == "c" && task.parent_task_ids == [child.task_id])
                .collect();
            assert_eq!(successors.len(), 1);
            assert_eq!(successors[0].parent_task_ids, [child.task_id]);
            assert_eq!(successors[0].input, json!(input + 1));
            assert_eq!(successors[0].output, Some(json!(input + 2)));
        }
    }
    database.close().await;
}

struct SignalNode {
    started: Arc<Notify>,
    release: Option<Arc<Notify>>,
}

impl Node<Context> for SignalNode {
    type In = i32;
    type Out = Vec<i32>;

    async fn execute(&self, _: &mut Context, input: i32) -> Result<Vec<i32>, NodeError> {
        self.started.notify_one();
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if input < 0 {
            return Err(NodeError::permanent("source failed"));
        }
        Ok(vec![input])
    }
}

struct Item;

impl Node<Context> for Item {
    type In = i32;
    type Out = i32;

    async fn execute(&self, _: &mut Context, input: i32) -> Result<i32, NodeError> {
        Ok(input)
    }
}

#[tokio::test]
async fn scopes_isolate_shared_join_and_reports_after_out_of_order_sources() {
    for use_foreach in [false, true] {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        database.initialize().await.unwrap();
        let pool = match &database {
            Database::Sqlite(pool) => pool.clone(),
            #[cfg(feature = "pg")]
            Database::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
        };
        let repo = SqliteTaskRepo::new(pool);
        let starts: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        let releases: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        macro_rules! report_graph {
            ($item_node:expr; $($clause:tt)*) => {
                dsl::workflow! {
                    workflow_id: "scoped-reports";
                    title: "独立作用域复用计算与报表";
                    context: Context::default();
                    nodes {
                        start => AddOne,
                        source_a => SignalNode { started: starts[0].clone(), release: Some(releases[0].clone()) },
                        source_b => SignalNode { started: starts[1].clone(), release: Some(releases[1].clone()) },
                        source_c => SignalNode { started: starts[2].clone(), release: Some(releases[2].clone()) },
                    }
                    subgraph analysis {
                        nodes {
                        per_item => $item_node,
                        worker => AddOne,
                        collect => JoinNode,
                        report => Sum,
                        }
                    }
                    edges {
                        start -> source_a,
                        start -> source_b |> |output| vec![*output + 10],
                        start -> source_c |> |output| vec![*output + 20],
                        source_a -> analysis.per_item $($clause)*,
                        source_b -> analysis.per_item $($clause)*,
                        source_c -> analysis.per_item $($clause)*,
                        analysis.per_item -> analysis.worker,
                        analysis.per_item -> analysis.collect |> |output| vec![json!({"summary": output})],
                        analysis.worker -> analysis.collect |> |output| vec![json!({"worker": [output]})],
                        analysis.collect -> analysis.report |> |output: &Value| vec![
                            serde_json::from_value::<Collected>(output.clone()).unwrap(),
                        ],
                    }
                }
            };
        }
        let graph = if use_foreach {
            report_graph!(Item; foreach)
        } else {
            report_graph!(Item; |> |output| output.clone())
        }
        .unwrap();
        let root = repo
            .enqueue(&Task::new("scoped-reports", "start", json!(0)))
            .await
            .unwrap();
        let scheduler = Scheduler::new(
            Arc::new(graph),
            Arc::new(SqliteTaskStore::new(repo.clone())),
            Duration::from_millis(2),
        )
        .with_max_concurrency(8);

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scheduler.run() => panic!("scheduler must run until cancelled"),
                _ = async {
                    tokio::join!(starts[0].notified(), starts[1].notified(), starts[2].notified());
                    // The next source stays running until the preceding report has completed.
                    // Shared nodes must not wait for or collect another source's future scope.
                    for (index, source) in [2, 0, 1].into_iter().enumerate() {
                        releases[source].notify_one();
                        loop {
                            let tasks = repo.list_all("scoped-reports").await.unwrap();
                            let reports = tasks.iter().filter(|task|
                                task.name == "analysis.report" && task.status == TaskStatus::Succeeded
                            ).count();
                            if reports == index + 1 {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                } => {}
            }
        })
        .await
        .expect("each scope produces its own report before the next source is released");

        let tasks = repo.list_all("scoped-reports").await.unwrap();
        assert_eq!(tasks.len(), 16);
        assert!(tasks.iter().all(|task| task.run_id == root.run_id
            && task.status == TaskStatus::Succeeded
            && task.attempt == 1));
        let scopes: Vec<_> = tasks
            .iter()
            .filter(|task| task.name == "analysis.per_item")
            .collect();
        assert_eq!(scopes.len(), 3);
        for scope in scopes {
            assert_eq!(scope.task_type, "normal");
            assert!(scope.scope_id.is_some());
            let source = tasks
                .iter()
                .find(|task| scope.parent_task_ids == [task.task_id])
                .unwrap();
            assert_eq!(source.scope_id, None);
            assert_eq!(source.output, Some(json!([scope.input])));
            let scoped: Vec<_> = tasks
                .iter()
                .filter(|task| task.scope_id == scope.scope_id)
                .collect();
            assert_eq!(scoped.len(), 4, "scope, worker, JOIN and report");
            let input = scope.input.as_i64().unwrap();
            let worker = scoped
                .iter()
                .find(|task| task.name == "analysis.worker")
                .unwrap();
            assert_eq!(worker.parent_task_ids, [scope.task_id]);
            assert_eq!(worker.input, scope.input);
            assert_eq!(worker.output, Some(json!(input + 1)));

            let join = scoped
                .iter()
                .find(|task| task.name == "analysis.collect")
                .unwrap();
            let mut predecessors = join
                .join_state
                .expected
                .iter()
                .map(|dependency| dependency.name.clone())
                .collect::<Vec<_>>();
            predecessors.sort();
            assert_eq!(predecessors, ["analysis.per_item", "analysis.worker"]);
            let mut parents = join.parent_task_ids.clone();
            parents.sort_unstable();
            assert_eq!(parents, [scope.task_id, worker.task_id]);
            assert_eq!(join.input, json!({"summary": input, "worker": [input + 1]}));

            let report = scoped
                .iter()
                .find(|task| task.name == "analysis.report")
                .unwrap();
            assert_eq!(report.parent_task_ids, [join.task_id]);
            assert_eq!(report.input, join.input);
            assert_eq!(report.output, Some(json!(input * 2 + 1)));
        }
        let mut reports: Vec<_> = tasks
            .iter()
            .filter(|task| task.name == "analysis.report")
            .collect();
        reports.sort_unstable_by_key(|task| task.task_id);
        assert_eq!(
            reports
                .iter()
                .map(|task| task.output.clone().unwrap())
                .collect::<Vec<_>>(),
            [json!(43), json!(3), json!(23)],
            "reports retain their source pairing despite out-of-order source completion",
        );
        database.close().await;
    }
}

#[tokio::test]
async fn shared_join_selects_its_external_entry_and_isolates_source_failure() {
    for fail_source_a in [false, true] {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        database.initialize().await.unwrap();
        let pool = match &database {
            Database::Sqlite(pool) => pool.clone(),
            #[cfg(feature = "pg")]
            Database::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
        };
        let repo = SqliteTaskRepo::new(pool);
        let starts: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        let releases: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        let graph = dsl::workflow! {
            workflow_id: "external-entry-join";
            title: "跨组入口与 JOIN 隔离";
            context: Context::default();
            nodes {
                start => AddOne,
                source_a => SignalNode { started: starts[0].clone(), release: Some(releases[0].clone()) },
                source_b => SignalNode { started: starts[1].clone(), release: Some(releases[1].clone()) },
                source_c => SignalNode { started: starts[2].clone(), release: Some(releases[2].clone()) },
            }
            subgraph analysis {
                nodes { worker => AddOne, collect => JoinNode, report => Sum }
                edges {
                    worker -> collect |> |output| vec![json!({"worker": [output]})],
                    collect -> report |> |output: &Value| vec![serde_json::from_value::<Collected>(output.clone()).unwrap()],
                }
            }
            edges {
                start -> source_a |> move |output| vec![if fail_source_a { -*output } else { *output }],
                start -> source_b |> |output| vec![*output + 10],
                start -> source_c |> |output| vec![*output + 20],
                source_a -> analysis.collect |> |output| vec![json!({"summary": output[0]})],
                source_a -> analysis.worker |> |output| output.clone(),
                source_b -> analysis.worker |> |output| output.clone(),
                source_b -> analysis.collect |> |output| vec![json!({"summary": output[0]})],
                source_c -> analysis.collect |> |output| vec![json!({"summary": output[0]})],
                source_c -> analysis.worker |> |output| output.clone(),
            }
        }
        .unwrap();
        let root = repo
            .enqueue(&Task::new("external-entry-join", "start", json!(0)))
            .await
            .unwrap();
        let scheduler = Scheduler::new(
            Arc::new(graph),
            Arc::new(SqliteTaskStore::new(repo.clone())),
            Duration::from_millis(2),
        )
        .with_max_concurrency(8);

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scheduler.run() => panic!("scheduler must run until cancelled"),
                _ = async {
                    tokio::join!(starts[0].notified(), starts[1].notified(), starts[2].notified());
                    let mut expected_reports = 0;
                    for source in [2, 0, 1] {
                        releases[source].notify_one();
                        let source_fails = fail_source_a && source == 0;
                        expected_reports += usize::from(!source_fails);
                        loop {
                            let tasks = repo.list_all("external-entry-join").await.unwrap();
                            let reports = tasks.iter().filter(|task|
                                task.name == "analysis.report" && task.status == TaskStatus::Succeeded
                            ).count();
                            let failed = tasks.iter().any(|task|
                                task.name == "source_a" && task.status == TaskStatus::Failed
                            );
                            if reports == expected_reports && (!source_fails || failed) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                } => {}
            }
        })
        .await
        .expect("a report must not wait for another external entry or inherit its failure");

        let tasks = repo.list_all("external-entry-join").await.unwrap();
        assert_eq!(tasks.len(), if fail_source_a { 10 } else { 13 });
        assert!(
            tasks
                .iter()
                .all(|task| task.run_id == root.run_id && task.attempt == 1)
        );
        for task in &tasks {
            let expected = if fail_source_a && task.name == "source_a" {
                TaskStatus::Failed
            } else {
                TaskStatus::Succeeded
            };
            assert_eq!(task.status, expected, "{task:?}");
        }
        let mut scope_ids = Vec::new();
        for worker in tasks.iter().filter(|task| task.name == "analysis.worker") {
            assert!(worker.scope_id.is_some());
            assert!(!scope_ids.contains(&worker.scope_id));
            scope_ids.push(worker.scope_id);
            let source = tasks
                .iter()
                .find(|task| worker.parent_task_ids == [task.task_id])
                .unwrap();
            let join = tasks
                .iter()
                .find(|task| task.name == "analysis.collect" && task.scope_id == worker.scope_id)
                .unwrap();
            assert_eq!(join.join_state.expected.len(), 2);
            let external = join
                .join_state
                .expected
                .iter()
                .find(|dependency| dependency.name == source.name)
                .unwrap();
            assert_eq!(external.task_id, Some(source.task_id));
            assert!(
                join.join_state
                    .expected
                    .iter()
                    .any(|dependency| dependency.name == worker.name
                        && dependency.scope_id == worker.scope_id
                        && dependency.task_id.is_none())
            );
            let mut parents = join.parent_task_ids.clone();
            parents.sort_unstable();
            assert_eq!(parents, [source.task_id, worker.task_id]);
            let input = worker.input.as_i64().unwrap();
            assert_eq!(join.input, json!({"summary": input, "worker": [input + 1]}));
            let report = tasks
                .iter()
                .find(|task| task.name == "analysis.report" && task.scope_id == worker.scope_id)
                .unwrap();
            assert_eq!(report.parent_task_ids, [join.task_id]);
            assert_eq!(report.output, Some(json!(input * 2 + 1)));
        }
        database.close().await;
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
        task_type: "normal".into(),
        workflow_id: "scheduler-wake".into(),
        run_id: None,
        scope_id: None,
        subgraph_path: vec![],
        parent_task_ids: vec![],
        name: "slow".into(),
        max_attempts: 3,
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
