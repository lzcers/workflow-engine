//! 真实存储上的嵌套进入、退出和 JOIN 隔离；同一图验证 SQLite 与 PostgreSQL。
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{marker::PhantomData, time::Duration};
use tokio::sync::mpsc;
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
        return sqlx::query_as(sql)
            .bind(workflow_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    }
    #[cfg(feature = "pg")]
    if url.starts_with("postgres") {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        return sqlx::query_as(sql)
            .bind(workflow_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    }
    panic!("unsupported test backend")
}
async fn execute_sql(url: &str, sql: &str) {
    #[cfg(feature = "sqlite")]
    if url.starts_with("sqlite:") {
        let pool = sqlx::SqlitePool::connect(url).await.unwrap();
        sqlx::raw_sql(sql).execute(&pool).await.unwrap();
        return;
    }
    #[cfg(feature = "pg")]
    if url.starts_with("postgres") {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        sqlx::raw_sql(sql).execute(&pool).await.unwrap();
        return;
    }
    panic!("unsupported test backend")
}
async fn recover_nested(url: &str) {
    let workflow_id = format!("scope-recovery-{}", uuid::Uuid::new_v4());
    let engine = Engine::new(url).await.unwrap();
    execute_sql(url, &"INSERT INTO task_counters(workflow_id,next_task_id) VALUES('scope-recovery',3);
        INSERT INTO scopes(workflow_id,scope_id,run_id,subgraph_id,parent_scope_id,trigger_task_id)
        VALUES('scope-recovery',1,3,'outer',NULL,3),('scope-recovery',2,3,'outer.inner',1,3);
        INSERT INTO tasks(workflow_id,task_id,run_id,scope_id,name,status,input_json,attempt,claimed_by,lease_until)
        VALUES('scope-recovery',3,3,2,'outer.inner.start','running','8',1,'interrupted','2000-01-01T00:00:00Z');".replace("scope-recovery", &workflow_id)).await;
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
        vec![Some(2), Some(1), None]
    );
    assert_eq!(records[0].0.attempt, 2);
    assert_eq!(records[0].0.task_id, 3);
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
        assert_eq!(task.run_id, Some(run.run_id));
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
        .filter(|s| s.run_id == Some(run.run_id))
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
        .filter(|s| s.run_id == Some(direct.run_id))
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
            .filter(|scope| scope.run_id == Some(run.run_id))
            .count(),
        8
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
}
