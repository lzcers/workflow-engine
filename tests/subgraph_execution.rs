//! 真实存储上的嵌套进入、退出和 JOIN 隔离；同一图验证 SQLite 与 PostgreSQL。
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc};
use workflow_engine::task::ScopeRecord;
use workflow_engine::{
    Engine, Graph, JoinNode, Node, NodeContext, NodeError, TaskRecord, workflow,
};

struct Context {
    task: Option<TaskRecord>,
    results: mpsc::UnboundedSender<(TaskRecord, Value)>,
}
impl NodeContext for Context {
    async fn fork(&self, task: &TaskRecord) -> Self {
        Self {
            task: Some(task.clone()),
            results: self.results.clone(),
        }
    }
}
struct Pass<T>(PhantomData<T>);
fn pass<T>() -> Pass<T> {
    Pass(PhantomData)
}
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Node<Context> for Pass<T> {
    type In = T;
    type Out = T;
    async fn execute(&self, _: &mut Context, input: T) -> Result<T, NodeError> {
        Ok(input)
    }
    async fn on_success(ctx: &mut Context, output: &Value) {
        let _ = ctx
            .results
            .send((ctx.task.as_ref().unwrap().clone(), output.clone()));
    }
}
fn nested_graph(context: Context) -> Graph<Context> {
    workflow! {
        workflow_id: "nested-subgraphs";
        title: "Nested subgraphs";
        context: context;
        nodes { start => pass::<Vec<i32>>(), collect => JoinNode, finish => pass::<Value>() }
        subgraph analysis {
            nodes { daily => pass::<i32>(), joined => JoinNode, report => pass::<Value>() }
            subgraph indicators {
                nodes { left => pass::<i32>(), right => pass::<i32>(), joined => JoinNode }
                edges {
                    left -> joined |> |value| vec![json!({"left": value})],
                    right -> joined |> |value| vec![json!({"right": value})],
                }
            }
            edges {
                daily -> indicators.left,
                daily -> indicators.right,
                daily -> joined |> |value| vec![json!({"daily": value})],
                indicators.joined -> joined |> |value| vec![json!({"indicators": value})],
                joined -> report,
            }
        }
        edges {
            start -> analysis.daily foreach,
            analysis.report -> collect |> |value| vec![json!({"reports": value})],
            collect -> finish,
        }
    }
    .unwrap()
}
async fn read_scopes(url: &str, workflow_id: &str) -> Vec<ScopeRecord> {
    let sql = "SELECT workflow_id,scope_id,run_id,subgraph_id,parent_scope_id,trigger_task_id FROM scopes WHERE workflow_id=$1 ORDER BY scope_id";
    #[cfg(feature = "sqlite")]
    if url.starts_with("sqlite:") {
        let pool = sqlx::SqlitePool::connect(url).await.unwrap();
        let scopes = sqlx::query_as(sql)
            .bind(workflow_id)
            .fetch_all(&pool)
            .await
            .unwrap();
        pool.close().await;
        return scopes;
    }
    #[cfg(feature = "pg")]
    if url.starts_with("postgres") {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        let scopes = sqlx::query_as(sql)
            .bind(workflow_id)
            .fetch_all(&pool)
            .await
            .unwrap();
        pool.close().await;
        return scopes;
    }
    panic!("unsupported test backend")
}
async fn execute_sql(url: &str, sql: &str) {
    #[cfg(feature = "sqlite")]
    if url.starts_with("sqlite:") {
        let pool = sqlx::SqlitePool::connect(url).await.unwrap();
        sqlx::raw_sql(sql).execute(&pool).await.unwrap();
        pool.close().await;
        return;
    }
    #[cfg(feature = "pg")]
    if url.starts_with("postgres") {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        sqlx::raw_sql(sql).execute(&pool).await.unwrap();
        pool.close().await;
        return;
    }
    panic!("unsupported test backend")
}
async fn recover_nested(url: &str) {
    let workflow_id = format!("scope-recovery-{}", uuid::Uuid::new_v4());
    let engine = Engine::new(url).await.unwrap();
    let fixture = format!(
        "INSERT INTO tasks(workflow_id,name,status,input_json,attempt,lease_until)
         VALUES('{workflow_id}','outer.inner.start','running','8',1,'2000-01-01T00:00:00Z');
         INSERT INTO scopes(workflow_id,run_id,subgraph_id,trigger_task_id)
         SELECT workflow_id,task_id,'outer',task_id FROM tasks WHERE workflow_id='{workflow_id}';
         INSERT INTO scopes(workflow_id,run_id,subgraph_id,parent_scope_id,trigger_task_id)
         SELECT workflow_id,run_id,'outer.inner',scope_id,trigger_task_id FROM scopes
         WHERE workflow_id='{workflow_id}' AND subgraph_id='outer';
         UPDATE tasks SET run_id=task_id, scope_id=(
             SELECT scope_id FROM scopes WHERE workflow_id='{workflow_id}' AND subgraph_id='outer.inner'
         ) WHERE workflow_id='{workflow_id}';"
    );
    execute_sql(url, &fixture).await;
    let scopes = read_scopes(url, &workflow_id).await;
    drop(engine);
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let graph = workflow! {
        workflow_id: workflow_id.clone(); title:"Scope recovery";
        context:Context{task:None,results};
        nodes { finish=>pass::<i32>() }
        subgraph outer {
            nodes {report=>pass::<i32>()}
            subgraph inner { nodes {start=>pass::<i32>()} edges{} }
            edges {inner.start->report,}
        }
        edges {outer.report->finish,}
    }
    .unwrap();
    let _workflow = engine.make(graph);
    let records = tokio::time::timeout(Duration::from_secs(8), async {
        let mut records = Vec::new();
        while records.len() < 3 {
            records.push(receiver.recv().await.unwrap());
        }
        records
    })
    .await
    .expect("restart must restore ancestors from scopes and resume the old task");
    assert_eq!(
        records.iter().map(|r| r.0.scope_id).collect::<Vec<_>>(),
        vec![Some(scopes[1].scope_id), Some(scopes[0].scope_id), None]
    );
    assert_eq!(records[0].0.attempt, 2);
    assert_eq!(records[0].0.task_id, scopes[1].trigger_task_id);
    assert_eq!(
        read_scopes(url, &workflow_id).await.len(),
        2,
        "recovery must reuse existing scopes"
    );
}

async fn nested(url: &str) {
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let workflow = engine.make(nested_graph(Context {
        task: None,
        results,
    }));
    let run = workflow.submit("start", vec![1, 2, 3]).await.unwrap();
    let records = tokio::time::timeout(Duration::from_secs(12), async {
        let mut records = Vec::new();
        loop {
            let result = receiver.recv().await.unwrap();
            let end = result.0.name == "finish";
            records.push(result);
            if end {
                break records;
            }
        }
    })
    .await
    .expect("all three nested invocations must complete and collect");
    let reports: Vec<_> = records
        .iter()
        .filter(|(task, _)| task.name == "analysis.report")
        .collect();
    assert_eq!(reports.len(), 3);
    let mut ids = Vec::new();
    for (task, output) in reports {
        assert_eq!(task.run_id, run.run_id);
        assert_eq!(output["daily"], output["indicators"]["left"]);
        assert_eq!(output["daily"], output["indicators"]["right"]);
        ids.push(task.scope_id.unwrap());
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3);
    let (finished, output) = records.last().unwrap();
    assert_eq!(finished.scope_id, None);
    assert_eq!(output["reports"].as_array().unwrap().len(), 3);
    let scopes = read_scopes(url, "nested-subgraphs").await;
    let scopes: Vec<_> = scopes
        .iter()
        .filter(|s| s.run_id == run.run_id)
        .collect();
    assert_eq!(
        scopes.len(),
        6,
        "two ordinary edges share one nested invocation per ETF"
    );
    for scope in scopes {
        if scope.subgraph_id == "analysis" {
            assert_eq!(scope.parent_scope_id, None);
        } else {
            assert_eq!(scope.subgraph_id, "analysis.indicators");
            assert!(ids.contains(&scope.parent_scope_id.unwrap()));
        }
    }
    // Manual entry directly into a nested node creates both ancestor instances.
    let direct = workflow
        .submit("analysis.indicators.left", 7)
        .await
        .unwrap();
    let scopes = read_scopes(url, "nested-subgraphs").await;
    let direct_scopes: Vec<_> = scopes
        .iter()
        .filter(|s| s.run_id == direct.run_id)
        .collect();
    assert_eq!(direct_scopes.len(), 2);
    assert_eq!(
        direct_scopes[1].parent_scope_id,
        Some(direct_scopes[0].scope_id)
    );
}
async fn entry_and_foreach(url: &str) {
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let graph = workflow! {
        workflow_id:"entry-subgraphs";
        title:"Entry correlation";
        context:Context{task:None,results};
        nodes {start=>pass::<Vec<i32>>(),outside=>pass::<Value>()}
        subgraph batch {
            nodes {daily=>pass::<i32>()}
            subgraph signals {
                nodes {mfi=>pass::<i32>(),joined=>JoinNode,report=>pass::<Value>()}
                edges {mfi->joined |> |value|vec![json!({"mfi":value})], joined->report,}
            }
            edges {
                daily->signals.mfi,
                daily->signals.joined |> |value|vec![json!({"daily":value})],
            }
        }
        edges {
            start->batch.daily foreach,
            start->batch.daily foreach,
            batch.signals.report->outside,
        }
    }
    .unwrap();
    let workflow = engine.make(graph);
    let run = workflow.submit("start", vec![4, 5]).await.unwrap();
    let outputs = tokio::time::timeout(Duration::from_secs(12), async {
        let mut outputs = Vec::new();
        while outputs.len() < 4 {
            let item = receiver.recv().await.unwrap();
            if item.0.name == "outside" {
                outputs.push(item);
            }
        }
        outputs
    })
    .await
    .expect("parallel foreach edges must create independent invocations");
    for (task, value) in outputs {
        assert_eq!(task.scope_id, None);
        assert_eq!(value["daily"], value["mfi"]);
    }
    let scopes = read_scopes(url, "entry-subgraphs").await;
    assert_eq!(
        scopes
            .iter()
            .filter(|scope| scope.run_id == run.run_id)
            .count(),
        8
    );
}

struct PausedProducer(Arc<Semaphore>);
impl Node<Context> for PausedProducer {
    type In = i32;
    type Out = i32;

    async fn execute(&self, context: &mut Context, input: i32) -> Result<i32, NodeError> {
        let _ = context
            .results
            .send((context.task.as_ref().unwrap().clone(), Value::Null));
        self.0.acquire().await.unwrap().forget();
        Ok(input)
    }
}

async fn waits_for_later_upstream_tasks(url: &str) {
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let graph = workflow! {
        workflow_id: format!("late-upstream-{}", uuid::Uuid::new_v4());
        title: "JOIN waits for tasks that have not been produced";
        context: Context { task: None, results };
        nodes { start => pass::<i32>(), joined => JoinNode, finish => pass::<Value>() }
        subgraph batch {
            nodes { prepare => PausedProducer(release.clone()), worker => pass::<i32>() }
            edges { prepare -> worker, }
        }
        edges {
            start -> batch.worker |> |_| vec![1],
            start -> batch.prepare |> |_| vec![2],
            batch.worker -> joined |> |value| vec![json!({"values": value})],
            joined -> finish,
        }
    }
    .unwrap();
    let workflow = engine.make(graph);
    let (other_results, mut other_receiver) = mpsc::unbounded_channel();
    let other_release = Arc::new(Semaphore::new(0));
    let other_graph = workflow! {
        workflow_id: format!("unrelated-upstream-{}", uuid::Uuid::new_v4());
        title: "Same JOIN name with a different upstream graph";
        context: Context { task: None, results: other_results };
        nodes { start => pass::<i32>(), joined => JoinNode, finish => pass::<Value>() }
        subgraph batch {
            nodes { prepare => PausedProducer(other_release.clone()), worker => pass::<i32>() }
            edges {}
        }
        edges {
            start -> batch.worker,
            start -> batch.prepare,
            batch.worker -> joined |> |value| vec![json!({"values": value})],
            joined -> finish,
        }
    }
    .unwrap();
    let other_workflow = engine.make(other_graph);
    workflow.submit("start", 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut prepared, mut completed) = (false, false);
        while !prepared || !completed {
            let (task, _) = receiver.recv().await.unwrap();
            assert_ne!(
                task.name, "finish",
                "JOIN ran before the producer completed"
            );
            prepared |= task.name == "batch.prepare";
            completed |= task.name == "batch.worker";
        }
    })
    .await
    .expect("the first worker must finish while the second worker is not created yet");
    other_workflow.submit("start", 3).await.unwrap();
    let other_output = tokio::time::timeout(Duration::from_secs(8), async {
        let mut prepared = false;
        let mut output = None;
        while !prepared || output.is_none() {
            let (task, value) = other_receiver.recv().await.unwrap();
            prepared |= task.name == "batch.prepare";
            if task.name == "finish" {
                output = Some(value);
            }
        }
        output.unwrap()
    })
    .await
    .expect("another workflow's JOIN must ignore its unrelated blocked producer");
    assert_eq!(other_output, json!({"values": 3}));
    assert!(
        tokio::time::timeout(Duration::from_millis(1200), receiver.recv())
            .await
            .is_err(),
        "JOIN must keep collecting across a scheduler poll while its producer is running"
    );
    release.add_permits(1);
    let output = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let (task, output) = receiver.recv().await.unwrap();
            if task.name == "finish" {
                break output;
            }
        }
    })
    .await
    .expect("JOIN must complete after both worker instances finish");
    let mut values: Vec<i32> = serde_json::from_value(output["values"].clone()).unwrap();
    values.sort();
    assert_eq!(values, [1, 2]);
    other_release.add_permits(1);
}

// Both graphs use the same JOIN name but wait for different predecessors.
async fn shared_store_uses_each_graphs_join_rules(url: &str) {
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let make = |id: &str, entry: &'static str| {
        let context = Context {
            task: None,
            results: results.clone(),
        };
        let graph = Graph::builder(id, id, context)
            .node(entry, pass::<Vec<i32>>())
            .node("joined", JoinNode)
            .node("finish", pass::<Value>())
            .edge(workflow_engine::graph::map(
                entry,
                "joined",
                |values: &Vec<i32>| values.iter().map(|v| json!({"value": v})).collect(),
            ))
            .edge(workflow_engine::graph::map(
                "joined",
                "finish",
                |value: &Value| vec![value.clone()],
            ))
            .build()
            .unwrap();
        engine.make(graph)
    };
    let orders = make("shared-orders", "charge");
    let billing = make("shared-billing", "invoice");
    orders.submit("charge", [1, 2]).await.unwrap();
    billing.submit("invoice", [3, 4]).await.unwrap();
    let mut outputs = tokio::time::timeout(Duration::from_secs(8), async {
        let mut outputs = Vec::new();
        while outputs.len() < 2 {
            let (task, output) = receiver.recv().await.unwrap();
            if task.name == "finish" {
                outputs.push((task.workflow_id, output));
            }
        }
        outputs
    })
    .await
    .expect("both graphs must claim their own JOIN");
    outputs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        outputs,
        [
            ("shared-billing".into(), json!({"value": [3, 4]})),
            ("shared-orders".into(), json!({"value": [1, 2]})),
        ]
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_nested_scopes_and_entry_correlation() {
    let path = std::env::temp_dir().join(format!("subgraph-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    nested(&url).await;
    entry_and_foreach(&url).await;
    recover_nested(&url).await;
    waits_for_later_upstream_tasks(&url).await;
    retry_and_recovery_join(&url).await;
    shared_store_uses_each_graphs_join_rules(&url).await;
    let _ = std::fs::remove_file(path);
}
#[cfg(feature = "pg")]
#[tokio::test]
async fn postgres_nested_scopes_and_entry_correlation() {
    let Ok(url) = std::env::var("WORKFLOW_ENGINE_TEST_DATABASE_URL") else {
        return;
    };
    nested(&url).await;
    entry_and_foreach(&url).await;
    recover_nested(&url).await;
    waits_for_later_upstream_tasks(&url).await;
    retry_and_recovery_join(&url).await;
    shared_store_uses_each_graphs_join_rules(&url).await;
}

struct RetryWork(Arc<Semaphore>);
impl Node<Context> for RetryWork {
    type In = i32;
    type Out = i32;

    async fn execute(&self, _: &mut Context, input: i32) -> Result<i32, NodeError> {
        if input == 0 {
            return Err(NodeError::permanent("return to prepare"));
        }
        self.0.acquire().await.unwrap().forget();
        Ok(input)
    }
}

struct RecoveryJoin {
    allow_retry: Arc<Semaphore>,
    release_recovery: Arc<Semaphore>,
}
impl Node<Context> for RecoveryJoin {
    type In = Value;
    type Out = Value;
    const TASK_TYPE: workflow_engine::graph::TaskType = workflow_engine::graph::TaskType::Join;

    async fn execute(&self, _: &mut Context, input: Value) -> Result<Value, NodeError> {
        if input.get("recovery").is_some() {
            // The successful contribution must arrive while this claim is still running.
            self.allow_retry.add_permits(1);
            self.release_recovery.acquire().await.unwrap().forget();
        }
        Ok(input)
    }
}

async fn retry_and_recovery_join(url: &str) {
    let engine = Engine::new(url).await.unwrap();
    let (results, mut receiver) = mpsc::unbounded_channel();
    let allow_retry = Arc::new(Semaphore::new(0));
    let release_recovery = Arc::new(Semaphore::new(0));
    let graph = workflow! {
        workflow_id: format!("retry-and-recovery-{}", uuid::Uuid::new_v4());
        title: "Successful retry and simultaneous recovery at one JOIN";
        context: Context { task: None, results };
        nodes {
            prepare => pass::<i32>(), work => RetryWork(allow_retry.clone()),
            joined => RecoveryJoin { allow_retry, release_recovery: release_recovery.clone() },
            finish => pass::<Value>()
        }
        edges {
            prepare -> work,
            work -> prepare on_error |_, _| vec![1],
            work -> joined on_error |_, _| vec![json!({"recovery": true})],
            work -> joined |> |value| vec![json!({"work": value})],
            joined -> finish,
        }
    }
    .unwrap();
    let workflow = engine.make(graph);
    workflow.submit("prepare", 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        for expected in [json!({"work": 1}), json!({"recovery": true})] {
            loop {
                let (task, output) = receiver.recv().await.unwrap();
                if task.name == "finish" {
                    assert_eq!(output, expected);
                    break;
                }
            }
            release_recovery.add_permits(1);
        }
    })
    .await
    .expect("the successful retry and recovery invocation must each reach finish");
}
