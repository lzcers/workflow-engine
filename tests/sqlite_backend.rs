//! 通过公开 API 验证 DSL 图在 SQLite 上的完整调度、扇出和多源 JOIN。

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;
use workflow_engine::{
    JoinNode, Node, NodeContext, NodeError, Scheduler, dsl,
    sqlite_backend::{NewTask, SqliteTask, SqliteTaskRepo, SqliteTaskStore, migrate, status},
};

#[derive(Default)]
struct Context {
    task: Option<SqliteTask>,
}

impl NodeContext<SqliteTask> for Context {
    async fn fork(&self, task: &SqliteTask) -> Self {
        Self {
            task: Some(task.clone()),
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
        assert_eq!(task.status, status::RUNNING);
        Ok(input + 1)
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
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&db).await.unwrap();
    let repo = SqliteTaskRepo::new(db.clone());

    // start ─┬─ worker × 2 ─┬─ collect (JOIN) ─ total
    //        └─ summary ────┘
    let graph = dsl::workflow! {
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
        .enqueue(&NewTask {
            workflow_id: "dsl-workflow".into(),
            run_id: Some(7),
            name: "start".into(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!(0),
        })
        .await
        .unwrap();
    let store = SqliteTaskStore::new(repo.clone(), root.workflow_id.clone()).with_max_attempts(5);
    let scheduler = Scheduler::new(
        Arc::new(graph),
        store,
        Duration::from_millis(2),
        Duration::from_millis(10),
    )
    .with_max_concurrency(3);
    tokio::time::timeout(Duration::from_secs(5), scheduler.run())
        .await
        .expect("workflow finishes and scheduler exits after becoming idle");

    let tasks = repo.list_all(&root.workflow_id).await.unwrap();
    assert_eq!(
        tasks.len(),
        6,
        "root, two workers, summary, JOIN, and total"
    );
    for task in &tasks {
        assert_eq!(task.status, status::SUCCEEDED, "{task:?}");
        assert_eq!(task.attempt, 1);
        assert_eq!(task.run_id, root.run_id);
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
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());
    db.close().await;
}
