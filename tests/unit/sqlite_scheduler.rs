//! DSL 图在 SQLite 内部存储和调度器上的扇出、JOIN 与单次认领唤醒行为。

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::{Notify, mpsc};

use crate::backend::task_fixture::TaskSeed;
use crate::{
    JoinInput,
    backend::sqlite::{SqliteTaskRepo, migrate},
    database::{Database, DatabasePool},
    dsl,
    graph::{JoinNode, Node, NodeContext, NodeError},
    scheduler::Scheduler,
    task::{TaskRecord, TaskStatus, TaskStore, store::DEFAULT_LEASE_DURATION},
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

    async fn on_success(&mut self, task: &TaskRecord, output: &Value) {
        if let Some(completed) = &self.completed {
            let _ = completed.send((task.clone(), output.clone()));
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
}

#[derive(Debug, Serialize, Deserialize, JoinInput)]
struct Collected {
    #[join(collect)]
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
    let pool = match database.pool() {
        DatabasePool::Sqlite(pool) => pool.clone(),
        #[cfg(feature = "pg")]
        DatabasePool::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
    };

    let graph = dsl::workflow! {
        workflow_id: "dsl-workflow";
        title: "扇出与汇合";
        context: Context::default();
        nodes {
            start => AddOne,
            worker => AddOne,
            summary => AddOne,
            collect => JoinNode::<Collected>::new(),
            total => Sum,
        }
        edges {
            start ->* worker |> |output| vec![*output, *output + 10],
            start -> summary,
            worker -> collect::worker,
            summary -> collect::summary,
            collect -> total,
        }
    }
    .unwrap();

    let graph = Arc::new(graph);
    let repo = SqliteTaskRepo::new(pool);
    let root = repo
        .seed_task(
            graph.as_ref(),
            &TaskSeed::new("dsl-workflow", "start", json!(0)),
        )
        .await
        .unwrap();
    let other_root = repo
        .seed_task(
            graph.as_ref(),
            &TaskSeed::new("dsl-workflow", "start", json!(100)),
        )
        .await
        .unwrap();
    assert_eq!(root.run_id, root.task_id);
    assert!(root.parent_task_ids.is_empty());
    assert_eq!(other_root.run_id, other_root.task_id);
    assert_ne!(root.run_id, other_root.run_id);
    let scheduler = Scheduler::new(
        graph.clone(),
        Arc::new(repo.clone()),
        Duration::from_millis(2),
    )
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
                .all(|id| other_tasks.iter().any(|other| other.task_id == *id))
        );
    }
    let other_total = other_tasks
        .iter()
        .find(|task| task.name == "total")
        .unwrap();
    assert_eq!(other_total.output, Some(json!(316)));
    assert!(
        repo.claim_many(graph.workflow_id(), 1, DEFAULT_LEASE_DURATION)
            .await
            .unwrap()
            .is_empty()
    );
    database.close().await;
}

#[tokio::test]
async fn nested_fanout_preserves_each_parent_and_single_input_successor() {
    let database = Database::connect("sqlite::memory:").await.unwrap();
    database.initialize().await.unwrap();
    let pool = match database.pool() {
        DatabasePool::Sqlite(pool) => pool.clone(),
        #[cfg(feature = "pg")]
        DatabasePool::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
    };
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
            start ->* a |> |output| vec![*output, *output + 10],
            a ->* b |> |output| vec![*output, *output + 100],
            b -> c,
        }
    }
    .unwrap();
    let graph = Arc::new(graph);
    let repo = SqliteTaskRepo::new(pool);
    let root = repo
        .seed_task(
            graph.as_ref(),
            &TaskSeed::new("nested-fanout", "start", json!(0)),
        )
        .await
        .unwrap();
    let scheduler = Scheduler::new(
        graph.clone(),
        Arc::new(repo.clone()),
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
    for isolated in [false, true] {
        let database = Database::connect("sqlite::memory:").await.unwrap();
        database.initialize().await.unwrap();
        let pool = match database.pool() {
            DatabasePool::Sqlite(pool) => pool.clone(),
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
        };
        let starts: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        let releases: Vec<_> = (0..3).map(|_| Arc::new(Notify::new())).collect();
        macro_rules! report_graph {
            ($($arrow:tt)+) => {
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
                            per_item => Item,
                            worker => AddOne,
                            collect => JoinNode::<Collected>::new(),
                            report => Sum,
                        }
                    }
                    edges {
                        start -> source_a,
                        start -> source_b |> |output| *output + 10,
                        start -> source_c |> |output| *output + 20,
                        source_a $($arrow)+ analysis.per_item,
                        source_b $($arrow)+ analysis.per_item,
                        source_c $($arrow)+ analysis.per_item,
                        analysis.per_item -> analysis.worker,
                        analysis.per_item -> analysis.collect::summary,
                        analysis.worker -> analysis.collect::worker,
                        analysis.collect -> analysis.report,
                    }
                }
            };
        }
        let graph = if isolated {
            report_graph!(->+)
        } else {
            report_graph!(->*)
        }
        .unwrap();
        let graph = Arc::new(graph);
        let repo = SqliteTaskRepo::new(pool);
        let root = repo
            .seed_task(
                graph.as_ref(),
                &TaskSeed::new("scoped-reports", "start", json!(0)),
            )
            .await
            .unwrap();
        let scheduler = Scheduler::new(
            graph.clone(),
            Arc::new(repo.clone()),
            Duration::from_millis(2),
        )
        .with_max_concurrency(8);

        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = scheduler.run() => panic!("scheduler must run until cancelled"),
                _ = async {
                    tokio::join!(starts[0].notified(), starts[1].notified(), starts[2].notified());
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
        let pool = match database.pool() {
            DatabasePool::Sqlite(pool) => pool.clone(),
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(_) => panic!("SQLite URL must select the SQLite backend"),
        };
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
                nodes { worker => AddOne, collect => JoinNode::<Collected>::new(), report => Sum }
                edges {
                    worker -> collect::worker,
                    collect -> report,
                }
            }
            edges {
                start -> source_a |> move |output| if fail_source_a { -*output } else { *output },
                start -> source_b |> |output| *output + 10,
                start -> source_c |> |output| *output + 20,
                source_a -> analysis.collect::summary |> |output| output[0],
                source_a ->* analysis.worker,
                source_b ->* analysis.worker,
                source_b -> analysis.collect::summary |> |output| output[0],
                source_c -> analysis.collect::summary |> |output| output[0],
                source_c ->* analysis.worker,
            }
        }
        .unwrap();
        let graph = Arc::new(graph);
        let repo = SqliteTaskRepo::new(pool);
        let root = repo
            .seed_task(
                graph.as_ref(),
                &TaskSeed::new("external-entry-join", "start", json!(0)),
            )
            .await
            .unwrap();
        let scheduler = Scheduler::new(
            graph.clone(),
            Arc::new(repo.clone()),
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
async fn scheduler_wakes_for_new_work_without_redundant_empty_claim() {
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
    let mut task = TaskSeed::new("scheduler-wake", "slow", json!(0));
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
    let graph = Arc::new(graph);
    let repo = SqliteTaskRepo::new(db.clone());
    repo.seed_task(graph.as_ref(), &task).await.unwrap();
    let wake = Arc::new(Notify::new());
    let scheduler = Scheduler::new(
        graph.clone(),
        Arc::new(repo.clone()),
        Duration::from_secs(60),
    )
    .with_max_concurrency(2)
    .with_wake(wake.clone());

    let connection = db.acquire().await.unwrap();
    while released_rx.try_recv().is_ok() {}
    drop(connection);
    released_rx.recv().await.unwrap();

    let worker = tokio::spawn(async move {
        scheduler.run().await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        slow_started.notified().await;
        // 认领数量 1 < 可用槽 2 时直接进入等待，不再多发一次多余空认领。
        released_rx.recv().await.unwrap();
    })
    .await
    .expect("scheduler starts the slow node with a single claim");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), released_rx.recv())
            .await
            .is_err(),
        "must not issue a redundant second claim_many when batch < available"
    );

    task.task.name = "fast".into();
    repo.seed_task(graph.as_ref(), &task).await.unwrap();
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
