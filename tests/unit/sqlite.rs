use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};

use serde_json::json;
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, types::Json};

use super::{SqliteTaskRepo, SqliteTx, TaskRow, scope};
use crate::backend::{ROW_COLS, common::enqueue_fanouts_and_advance, sqlite::migrate};
#[path = "graph_fixture.rs"]
mod graph_fixture;
use crate::Database;
use crate::backend::task_fixture::{TaskSeed, join_plan, plan, scoped_plan};
use crate::error::StorageError;
use crate::graph::{Graph, MergeMode, TaskType};
use crate::task::{
    DEFAULT_MAX_ATTEMPTS, Dispatch, SettlementConfig, Successor, TaskStore,
    store::{DEFAULT_LEASE_DURATION, FanoutSource},
};
use crate::{TaskRecord, TaskStatus};
use TaskType::{Join, Normal};
use graph_fixture::graph as make_graph;

const LEASE: Duration = DEFAULT_LEASE_DURATION;

fn cfg() -> SettlementConfig {
    SettlementConfig::default()
}

impl SqliteTaskRepo {
    pub(crate) async fn list_all(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        let rows = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 ORDER BY task_id"
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    pub(crate) async fn get(
        &self,
        workflow_id: &str,
        task_id: i64,
    ) -> Result<Option<TaskRecord>, StorageError> {
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE workflow_id = ?1 AND task_id = ?2"
        ))
        .bind(workflow_id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(TaskRecord::from))
    }

    /// Construct existing runs, scope ancestry and partially collected JOINs for storage tests.
    pub(crate) async fn seed_task<Ctx>(
        &self,
        graph: &Graph<Ctx>,
        seed: &TaskSeed,
    ) -> Result<TaskRecord, StorageError> {
        let task = &seed.task;
        if task.workflow_id != graph.workflow_id() {
            return Err(StorageError::InvalidFanout {
                detail: format!(
                    "workflow {} does not match seed graph {}",
                    task.workflow_id,
                    graph.workflow_id()
                ),
            });
        }
        let path: Vec<Arc<str>> = graph
            .subgraph_path(&task.name)
            .ok_or_else(|| StorageError::InvalidFanout {
                detail: format!("unknown task node {}", task.name),
            })?
            .into_iter()
            .map(Arc::from)
            .collect();
        let rules = graph.settlement_rules();
        let collecting = rules.join_rules.contains_key(task.name.as_str())
            && (seed.run_id.is_some() || !seed.parent_task_ids.is_empty());
        let status = if collecting {
            TaskStatus::Collecting
        } else {
            TaskStatus::Pending
        };

        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let max_attempts = task.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1);
        let task_id = sqlx::query(
            "INSERT INTO tasks (workflow_id, name, status, parent_task_ids, input_json, max_attempts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&task.workflow_id)
        .bind(&task.name)
        .bind(status.as_str())
        .bind(Json(&seed.parent_task_ids))
        .bind(&task.input)
        .bind(max_attempts)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        let run_id = seed.run_id.unwrap_or(task_id);
        let chain = scope::chain(&mut tx, &task.workflow_id, seed.scope_id).await?;
        let (parent, entered) = crate::task::scope::route_path(&chain, run_id, &path, false)?;
        let scope_id = scope::enter(
            &mut tx,
            &task.workflow_id,
            run_id,
            task_id,
            parent,
            entered,
            None,
        )
        .await?;
        sqlx::query("UPDATE tasks SET run_id = ?1, scope_id = ?2 WHERE task_id = ?3")
            .bind(run_id)
            .bind(scope_id)
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
        if collecting {
            // 直接按目标 JOIN 作用域检查是否应立即推进（如上游已失败或已全部终结）。
            let source = FanoutSource {
                workflow_id: &task.workflow_id,
                name: &task.name,
                task_id,
                run_id,
                scope_id,
            };
            let mut single_rules = rules.clone();
            single_rules
                .affected_joins
                .entry(Arc::from(task.name.as_str()))
                .or_default()
                .push(Arc::from(task.name.as_str()));
            enqueue_fanouts_and_advance(
                &mut SqliteTx(&mut tx),
                &source,
                &[],
                &single_rules,
                cfg(),
                None,
            )
            .await?;
        }
        let row = sqlx::query_as::<_, TaskRow>(&format!(
            "SELECT {ROW_COLS} FROM tasks WHERE task_id = ?1"
        ))
        .bind(task_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row.into())
    }
}

async fn memory_repo(
    workflow_id: &str,
    nodes: &[(&'static str, TaskType, Option<&str>)],
    edges: &[(&'static str, &'static str)],
) -> (SqlitePool, SqliteTaskRepo, Arc<Graph<()>>) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let repo = SqliteTaskRepo::new(pool.clone());
    (pool, repo, make_graph(workflow_id, nodes, edges))
}

fn new_task(workflow_id: &str, name: &str) -> TaskSeed {
    TaskSeed::new(workflow_id, name, json!({}))
}

async fn get(repo: &SqliteTaskRepo, task: &TaskRecord) -> TaskRecord {
    repo.get(&task.workflow_id, task.task_id)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn migration_is_idempotent_and_task_values_round_trip() {
    let (pool, repo, declared) = memory_repo(
        "workflow",
        &[
            ("root", Normal, None),
            ("second", Normal, None),
            ("rollback", Normal, None),
            ("next", Normal, None),
            ("analysis.entry", Normal, Some("analysis")),
            ("invalid_scope", Normal, None),
        ],
        &[],
    )
    .await;
    let graph = declared.as_ref();
    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('tasks')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for removed in ["task_type", "depends_on_names", "claimed_by", "join_state"] {
        assert!(!columns.iter().any(|column| column == removed));
    }
    assert!(columns.iter().any(|column| column == "max_attempts"));

    let mut input = new_task("workflow", "root");
    input.run_id = Some(42);
    input.task.input = json!({"text": "工作流 'quoted'", "nested": [null, true, {"n": 3}]});
    let first = repo.seed_task(graph, &input).await.unwrap();

    migrate(&pool).await.unwrap();
    let second = repo
        .seed_task(graph, &new_task("workflow", "second"))
        .await
        .unwrap();
    let other_graph = make_graph("other", &[("root", Normal, None)], &[]);
    let other = repo
        .seed_task(other_graph.as_ref(), &new_task("other", "root"))
        .await
        .unwrap();
    assert_ne!(first.task_id, second.task_id);
    assert_eq!(second.run_id, second.task_id);
    assert_ne!(other.task_id, first.task_id);
    assert_ne!(other.task_id, second.task_id);
    assert_eq!(other.run_id, other.task_id);
    assert_eq!(first.name, "root");
    assert!(first.parent_task_ids.is_empty());
    assert_eq!(first.input, input.task.input);
    assert_eq!(first.status, TaskStatus::Pending);
    assert_eq!(first.attempt, 0);
    assert_eq!(first.max_attempts, DEFAULT_MAX_ATTEMPTS);
    assert!(first.lease_until.is_none());
    assert!(first.created_at <= first.updated_at);

    let rows = repo.list_all("workflow").await.unwrap();
    assert_eq!(
        rows.iter().map(|t| t.task_id).collect::<Vec<_>>(),
        [first.task_id, second.task_id]
    );
    assert_eq!(rows[0].input, input.task.input);
    assert_eq!(rows[0].run_id, 42);
    assert_eq!(rows[1].run_id, second.run_id);
    assert!(repo.get("missing", 1).await.unwrap().is_none());

    let output = json!([null, {"result": "完成"}]);
    let claimed = repo.claim_many("workflow", 1, LEASE).await.unwrap().remove(0);
    repo.settle_success(&claimed, &output, &[], graph.settlement_rules(), cfg())
        .await
        .unwrap();
    let settled = get(&repo, &first).await;
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(settled.output, Some(output));

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let rolled_back = Database::in_transaction(&mut tx)
        .submit(crate::Task::new("workflow", "rollback", json!({})))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        repo.get("workflow", rolled_back.run_id)
            .await
            .unwrap()
            .is_none()
    );
    let next = repo
        .seed_task(graph, &new_task("workflow", "next"))
        .await
        .unwrap();
    assert_eq!(next.run_id, next.task_id);
    assert_ne!(next.run_id, second.run_id);

    for name in ["undeclared", "analysis"] {
        assert!(matches!(
            repo.seed_task(graph, &new_task("workflow", name)).await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }

    let scoped = repo
        .seed_task(
            graph,
            &TaskSeed {
                run_id: Some(first.run_id),
                ..new_task("workflow", "analysis.entry")
            },
        )
        .await
        .unwrap();
    for (run_id, scope_id) in [(Some(first.run_id), Some(i64::MAX)), (Some(43), scoped.scope_id)] {
        let invalid = TaskSeed {
            run_id,
            scope_id,
            ..new_task("workflow", "invalid_scope")
        };
        assert!(matches!(
            repo.seed_task(graph, &invalid).await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }
}

#[tokio::test]
async fn settlement_promotes_a_waiting_join_when_the_last_source_emits_no_successors() {
    let (_, repo, declared) = memory_repo(
        "deps",
        &[
            ("root", Normal, None),
            ("child", Join, None),
            ("missing_dependency", Join, None),
            ("absent", Normal, None),
        ],
        &[("root", "child"), ("absent", "missing_dependency")],
    )
    .await;
    let graph = declared.as_ref();
    let root = repo
        .seed_task(graph, &new_task("deps", "root"))
        .await
        .unwrap();
    let mut last_source = new_task("deps", "root");
    last_source.run_id = Some(root.run_id);
    let last_source = repo.seed_task(graph, &last_source).await.unwrap();
    let mut missing = new_task("deps", "missing_dependency");
    missing.run_id = Some(root.run_id);
    let missing = repo.seed_task(graph, &missing).await.unwrap();

    assert!(repo.claim_many("deps", 0, LEASE).await.unwrap().is_empty());
    let claimed = repo.claim_many("deps", 10, LEASE).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].task_id, root.task_id);
    assert_eq!(claimed[0].attempt, 1);
    assert!(claimed[0].lease_until.is_some());
    assert!(repo.claim_many("deps", 10, LEASE).await.unwrap().is_empty());

    let rules = graph.settlement_rules();
    let input = json!(1);
    repo.settle_success(
        &claimed[0],
        &input,
        &[join_plan("child", "root", MergeMode::Set, vec![input.clone()])],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    let child = repo
        .list_all("deps")
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.name == "child")
        .unwrap();
    assert_eq!(child.status, TaskStatus::Collecting);
    assert!(repo.claim_many("deps", 10, LEASE).await.unwrap().is_empty());

    assert_eq!(claimed[1].task_id, last_source.task_id);
    repo.settle_success(&claimed[1], &json!(null), &[], rules, cfg())
        .await
        .unwrap();
    let ready = get(&repo, &child).await;
    assert_eq!(ready.status, TaskStatus::Pending);
    assert_eq!(ready.attempt, 0);
    assert_eq!(ready.input, json!({"root": 1}));
    assert_eq!(ready.parent_task_ids, [root.task_id]);
    let claimed = repo.claim_many("deps", 10, LEASE).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, child.task_id);
    assert_eq!(claimed[0].status, TaskStatus::Running);
    assert_eq!(get(&repo, &missing).await.status, TaskStatus::Failed);

    let entry = repo
        .seed_task(graph, &new_task("deps", "missing_dependency"))
        .await
        .unwrap();
    assert_eq!(entry.status, TaskStatus::Pending);
    let claimed = repo.claim_many("deps", 10, LEASE).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, entry.task_id);
    assert_eq!(get(&repo, &missing).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn exhausted_tasks_are_claimed_for_fenced_final_failure_settlement() {
    let (pool, repo, declared) = memory_repo(
        "retry",
        &[("root", Normal, None), ("child", Join, None)],
        &[("root", "child")],
    )
    .await;
    let graph = declared.as_ref();
    let task = repo
        .seed_task(graph, &new_task("retry", "root"))
        .await
        .unwrap();
    let mut child = new_task("retry", "child");
    child.run_id = Some(task.run_id);
    child.parent_task_ids = vec![task.task_id];
    let child = repo.seed_task(graph, &child).await.unwrap();
    let first = repo.claim_many("retry", 1, LEASE).await.unwrap();
    assert_eq!(first[0].attempt, 1);
    repo.release_claim(&first[0], None, None).await.unwrap();
    let second = repo.claim_many("retry", 1, LEASE).await.unwrap();
    assert_eq!(second[0].attempt, 2);

    sqlx::query("UPDATE tasks SET lease_until = ? WHERE workflow_id = ? AND task_id = ?")
        .bind(
            (chrono::Utc::now() - chrono::Duration::seconds(10))
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
        )
        .bind("retry")
        .bind(task.task_id)
        .execute(&pool)
        .await
        .unwrap();
    let third = repo.claim_many("retry", 1, LEASE).await.unwrap();
    assert_eq!(third[0].attempt, 3);
    assert!(repo.claim_many("retry", 1, LEASE).await.unwrap().is_empty());
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);

    sqlx::query(
        "UPDATE tasks SET lease_until = '2000-01-01T00:00:00.000Z' WHERE workflow_id = 'retry'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let recovery = repo.claim_many("retry", 1, LEASE).await.unwrap().remove(0);
    assert_eq!(recovery.attempt, 4);
    assert_eq!(recovery.status, TaskStatus::Running);
    assert_eq!(
        recovery.error.as_deref(),
        Some("task lease expired; retry budget exhausted")
    );
    let rules = graph.settlement_rules();
    for result in [
        repo.settle_success(&recovery, &json!("too late"), &[], rules, cfg())
            .await,
        repo.settle_failure(&third[0], "old worker", &[], rules, cfg())
            .await,
    ] {
        assert!(matches!(result, Err(StorageError::ClaimLost { .. })));
    }

    repo.release_claim(&recovery, None, None).await.unwrap();
    let next_recovery = repo.claim_many("retry", 1, LEASE).await.unwrap().remove(0);
    assert_eq!(next_recovery.attempt, 5);
    assert_eq!(next_recovery.error, recovery.error);
    assert!(matches!(
        repo.settle_failure(&recovery, "stale recovery", &[], rules, cfg())
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.settle_failure(
        &next_recovery,
        next_recovery.error.as_deref().unwrap(),
        &[],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    let exhausted = get(&repo, &task).await;
    assert_eq!(exhausted.status, TaskStatus::Failed);
    assert_eq!(exhausted.attempt, 5);
    assert!(exhausted.lease_until.is_none());
    assert_eq!(exhausted.error, recovery.error);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Failed);
    assert!(repo.claim_many("retry", 1, LEASE).await.unwrap().is_empty());

    let declared = make_graph("zero", &[("root", Normal, None)], &[]);
    let graph = declared.as_ref();
    let zero = new_task("zero", "root");
    let zero = repo.seed_task(graph, &zero).await.unwrap();
    for _ in 0..DEFAULT_MAX_ATTEMPTS {
        let previous = repo.claim_many("zero", 1, LEASE).await.unwrap().remove(0);
        repo.release_claim(&previous, None, None).await.unwrap();
    }
    let recovery = repo.claim_many("zero", 1, LEASE).await.unwrap().remove(0);
    assert_eq!(recovery.attempt, DEFAULT_MAX_ATTEMPTS + 1);
    assert_eq!(
        recovery.error.as_deref(),
        Some("task retry budget exhausted")
    );
    repo.settle_failure(
        &recovery,
        recovery.error.as_deref().unwrap(),
        &[],
        graph.settlement_rules(),
        cfg(),
    )
    .await
    .unwrap();
    assert_eq!(get(&repo, &zero).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn join_merges_across_parents_and_waits_for_all_predecessors() {
    let (_, repo, declared) = memory_repo(
        "join",
        &[
            ("image", Normal, None),
            ("text", Normal, None),
            ("collect", Join, None),
        ],
        &[("image", "collect"), ("text", "collect")],
    )
    .await;
    let graph = declared.as_ref();
    let mut images = Vec::new();
    for input in [json!("a"), json!("b")] {
        let mut image = new_task("join", "image");
        image.run_id = Some(1);
        image.task.input = input;
        images.push(repo.seed_task(graph, &image).await.unwrap());
    }

    let mut text = new_task("join", "text");
    text.run_id = Some(1);
    let text = repo.seed_task(graph, &text).await.unwrap();

    let mut wrong_run = new_task("join", "text");
    wrong_run.run_id = Some(2);
    let wrong_run = repo.seed_task(graph, &wrong_run).await.unwrap();
    let mut claimed = repo.claim_many("join", 10, LEASE).await.unwrap();
    let text_claim = claimed.remove(
        claimed
            .iter()
            .position(|task| task.task_id == text.task_id)
            .unwrap(),
    );
    let wrong_claim = claimed.remove(
        claimed
            .iter()
            .position(|task| task.task_id == wrong_run.task_id)
            .unwrap(),
    );
    let rules = graph.settlement_rules();
    repo.settle_success(&wrong_claim, &json!({}), &[], rules, cfg())
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|t| t.name == "image"));
    assert_eq!(
        claimed.iter().map(|task| task.task_id).collect::<Vec<_>>(),
        images.iter().map(|task| task.task_id).collect::<Vec<_>>()
    );

    repo.settle_success(
        &claimed[0],
        &json!("a"),
        &[join_plan("collect", "image", MergeMode::Append, vec![json!("a")])],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    let join = repo
        .list_all("join")
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.name == "collect")
        .unwrap();
    assert!(repo.claim_many("join", 10, LEASE).await.unwrap().is_empty());
    repo.settle_success(
        &claimed[1],
        &json!("b"),
        &[join_plan("collect", "image", MergeMode::Append, vec![json!("b")])],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    let merged = get(&repo, &join).await;
    // collecting 阶段 Append 贡献以 O(1) 存于 join_contributions，tasks.input_json 仅存槽位占位标记。
    assert_eq!(merged.input, json!({"image": []}));
    assert_eq!(
        merged.parent_task_ids,
        [claimed[0].task_id, claimed[1].task_id]
    );
    assert!(
        repo.claim_many("join", 10, LEASE).await.unwrap().is_empty(),
        "another run cannot satisfy a missing predecessor"
    );

    assert_eq!(text_claim.task_id, text.task_id);
    repo.settle_success(
        &text_claim,
        &json!("text"),
        &[join_plan("collect", "text", MergeMode::Set, vec![json!("text")])],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    let claimed = repo.claim_many("join", 10, LEASE).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, join.task_id);
    assert_eq!(
        claimed[0].input,
        json!({"image": ["a", "b"], "text": "text"})
    );
    assert_eq!(
        claimed[0].parent_task_ids,
        [images[0].task_id, images[1].task_id, text.task_id]
    );
    assert_eq!(
        repo.list_all("join")
            .await
            .unwrap()
            .iter()
            .filter(|task| task.name == "collect")
            .count(),
        1,
    );
}

#[tokio::test]
async fn fanin_accumulates_join_inputs_and_isolates_numbered_runs() {
    let (pool, repo, declared) = memory_repo(
        "fanin",
        &[("image", Normal, None), ("collect", Join, None)],
        &[("image", "collect")],
    )
    .await;
    let graph = declared.as_ref();
    sqlx::query(
        "CREATE TRIGGER reject_repeated_join_write BEFORE UPDATE OF input_json ON tasks
         WHEN OLD.status = 'collecting' AND NEW.status = 'collecting' AND OLD.parent_task_ids = NEW.parent_task_ids
         BEGIN SELECT RAISE(ABORT, 'one write per source plan'); END",
    )
    .execute(&pool)
    .await
    .unwrap();
    for run_id in [Some(1), Some(2), Some(0)] {
        for input in [json!("first"), json!("second")] {
            let mut source = new_task("fanin", "image");
            source.run_id = run_id;
            source.task.input = input;
            repo.seed_task(graph, &source).await.unwrap();
        }
    }
    let sources = repo.claim_many("fanin", 10, LEASE).await.unwrap();
    let rules = graph.settlement_rules();
    for source in &sources {
        let contribution = join_plan(
            "collect",
            "image",
            MergeMode::Append,
            vec![source.input.clone(), json!("tail")],
        );
        repo.settle_success(source, &source.input, &[contribution], rules, cfg())
            .await
            .unwrap();
    }
    let joins = repo.claim_many("fanin", 10, LEASE).await.unwrap();
    assert_eq!(joins.len(), 3);
    assert_eq!(
        joins.iter().map(|join| join.run_id).collect::<HashSet<_>>(),
        HashSet::from([1, 2, 0])
    );
    for join in joins {
        assert_eq!(
            join.input,
            json!({"image": ["first", "tail", "second", "tail"]})
        );
        assert_eq!(join.parent_task_ids.len(), 2);
    }
    let remaining_contribs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM join_contributions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining_contribs, 0, "materialized contributions must be cleaned up");
}

#[tokio::test]
async fn success_and_all_fanouts_commit_or_roll_back_together() {
    let (pool, repo, declared) = memory_repo(
        "atomic_success",
        &[
            ("root", Normal, None),
            ("child", Normal, None),
            ("analysis.child", Normal, Some("analysis")),
            ("invalid", Normal, None),
        ],
        &[],
    )
    .await;
    let graph = declared.as_ref();
    let task = repo
        .seed_task(graph, &new_task("atomic_success", "root"))
        .await
        .unwrap();

    let claimed = repo
        .claim_many("atomic_success", 1, LEASE)
        .await
        .unwrap()
        .remove(0);
    let rules = graph.settlement_rules();
    let valid = plan("child", vec![json!("a"), json!("b")]);
    let scoped = Successor {
        dispatch: Dispatch::PerInputScope,
        ..scoped_plan("analysis.child", &["analysis"], vec![json!("a"), json!("b")])
    };
    let invalid = plan("invalid", vec![json!({})]);
    sqlx::query("CREATE TRIGGER reject_invalid BEFORE INSERT ON tasks WHEN NEW.name = 'invalid' BEGIN SELECT RAISE(ABORT, 'invalid successor'); END")
        .execute(&pool).await.unwrap();
    assert!(
        repo.settle_success(
            &claimed,
            &json!({"done": true}),
            &[valid.clone(), invalid],
            rules,
            cfg(),
        )
        .await
        .is_err()
    );
    let unchanged = get(&repo, &task).await;
    assert_eq!(unchanged.status, TaskStatus::Running);
    assert!(unchanged.output.is_none());
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 1);
    assert!(matches!(
        repo.settle_success(
            &claimed,
            &json!({}),
            &[Successor {
                dispatch: Dispatch::PerInputScope,
                ..plan("child", vec![json!({})])
            }],
            rules,
            cfg(),
        )
        .await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 1);

    sqlx::query("UPDATE tasks SET run_id = 9 WHERE workflow_id = 'atomic_success'")
        .execute(&pool)
        .await
        .unwrap();
    repo.settle_success(
        &claimed,
        &json!({"done": true}),
        &[valid.clone(), scoped],
        rules,
        cfg(),
    )
    .await
    .unwrap();
    assert!(matches!(
        repo.settle_success(&claimed, &json!({"done": true}), &[valid], rules, cfg())
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 5);
    let settled = get(&repo, &claimed).await;
    assert_eq!(settled.run_id, 9);
    assert_eq!(settled.scope_id, None);
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert!(settled.lease_until.is_none());
    let children = repo.claim_many("atomic_success", 10, LEASE).await.unwrap();
    assert_eq!(children.len(), 4);
    assert_ne!(children[0].task_id, children[1].task_id);
    assert_eq!(
        children.iter().map(|task| &task.input).collect::<Vec<_>>(),
        [&json!("a"), &json!("b"), &json!("a"), &json!("b")]
    );
    for (index, child) in children.into_iter().enumerate() {
        assert_eq!(child.run_id, 9);
        assert_eq!(child.scope_id.is_some(), index >= 2);
        assert_eq!(child.parent_task_ids, [task.task_id]);
    }
}

#[tokio::test]
async fn failure_and_recovery_fanouts_are_atomic_and_do_not_depend_on_failed_parent() {
    let (pool, repo, declared) = memory_repo(
        "atomic_failure",
        &[
            ("analysis.root", Normal, Some("analysis")),
            ("analysis.recovery", Join, Some("analysis")),
            ("analysis.success", Normal, Some("analysis")),
            ("invalid", Normal, None),
        ],
        &[("analysis.success", "analysis.recovery")],
    )
    .await;
    let graph = declared.as_ref();
    let mut input = new_task("atomic_failure", "analysis.root");
    input.run_id = Some(4);
    let task = repo.seed_task(graph, &input).await.unwrap();

    for _ in 1..DEFAULT_MAX_ATTEMPTS {
        let previous = repo
            .claim_many("atomic_failure", 1, LEASE)
            .await
            .unwrap()
            .remove(0);
        repo.release_claim(&previous, None, None).await.unwrap();
    }
    let expired = repo
        .claim_many("atomic_failure", 1, LEASE)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00.000Z' WHERE workflow_id = 'atomic_failure'")
        .execute(&pool).await.unwrap();
    let claimed = repo
        .claim_many("atomic_failure", 1, LEASE)
        .await
        .unwrap()
        .remove(0);
    assert!(claimed.attempt > DEFAULT_MAX_ATTEMPTS);
    assert_eq!(expired.scope_id, task.scope_id);
    assert_eq!(claimed.scope_id, task.scope_id);
    let rules = graph.settlement_rules();
    let recovery = scoped_plan("analysis.recovery", &["analysis"], vec![json!({"retry": true})]);
    let invalid = plan("invalid", vec![json!({})]);
    sqlx::query("CREATE TRIGGER reject_invalid BEFORE INSERT ON tasks WHEN NEW.name = 'invalid' BEGIN SELECT RAISE(ABORT, 'invalid successor'); END")
        .execute(&pool).await.unwrap();
    let message = claimed.error.as_deref().unwrap();
    assert!(matches!(
        repo.settle_failure(&expired, message, std::slice::from_ref(&recovery), rules, cfg())
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(
        repo.settle_failure(&claimed, message, &[recovery.clone(), invalid], rules, cfg())
            .await
            .is_err()
    );
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);
    assert_eq!(repo.list_all("atomic_failure").await.unwrap().len(), 1);

    sqlx::query("INSERT INTO scopes(workflow_id,scope_id,run_id,subgraph_id,parent_scope_id,trigger_task_id) VALUES ('atomic_failure',21,4,'analysis',NULL,?1)")
        .bind(task.task_id).execute(&pool).await.unwrap();
    sqlx::query("UPDATE tasks SET scope_id = 21 WHERE workflow_id = 'atomic_failure'")
        .execute(&pool)
        .await
        .unwrap();

    repo.settle_failure(&claimed, message, &[recovery], rules, cfg())
        .await
        .unwrap();
    let settled = get(&repo, &task).await;
    assert_eq!(settled.status, TaskStatus::Superseded);
    assert_eq!(settled.error.as_deref(), Some(message));
    assert!(settled.lease_until.is_none());
    assert!(matches!(
        repo.settle_failure(&claimed, message, &[], rules, cfg()).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(repo.list_all("atomic_failure").await.unwrap().len(), 2);
    let recovery = repo
        .claim_many("atomic_failure", 10, LEASE)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(recovery.name, "analysis.recovery");
    assert_eq!(recovery.run_id, 4);
    assert_eq!(recovery.scope_id, Some(21));
    assert_eq!(recovery.parent_task_ids, [task.task_id]);
    repo.settle_failure(&recovery, message, &[], rules, cfg())
        .await
        .unwrap();
    assert_eq!(get(&repo, &recovery).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn failure_fanout_to_the_same_name_creates_a_fresh_task() {
    let (_, repo, declared) = memory_repo("same_name", &[("retry", Normal, None)], &[]).await;
    let graph = declared.as_ref();
    repo.seed_task(graph, &new_task("same_name", "retry"))
        .await
        .unwrap();

    let source = repo.claim_many("same_name", 1, LEASE).await.unwrap().remove(0);
    repo.settle_failure(
        &source,
        "retry requested",
        &[plan("retry", vec![json!({"retry": true})])],
        graph.settlement_rules(),
        cfg(),
    )
    .await
    .unwrap();
    assert_eq!(get(&repo, &source).await.status, TaskStatus::Superseded);
    let successors = repo.claim_many("same_name", 1, LEASE).await.unwrap();
    assert_eq!(successors.len(), 1);
    assert_ne!(successors[0].task_id, source.task_id);
    assert_eq!(successors[0].input, json!({"retry": true}));
    assert_eq!(successors[0].parent_task_ids, [source.task_id]);
}

#[tokio::test]
async fn release_and_settlement_stay_in_their_workflow() {
    let (_, repo, declared) = memory_repo("ours", &[("root", Normal, None)], &[]).await;
    let graph = declared.as_ref();
    let ours = repo
        .seed_task(graph, &new_task("ours", "root"))
        .await
        .unwrap();
    let other_graph = make_graph("theirs", &[("root", Normal, None)], &[]);
    let theirs = repo
        .seed_task(other_graph.as_ref(), &new_task("theirs", "root"))
        .await
        .unwrap();

    let claim = repo.claim_many("ours", 1, LEASE).await.unwrap().remove(0);
    assert_eq!(get(&repo, &theirs).await.status, TaskStatus::Pending);
    repo.claim_many("theirs", 1, LEASE).await.unwrap();
    repo.release_claim(&claim, None, None).await.unwrap();
    let reclaimed = repo.claim_many("ours", 10, LEASE).await.unwrap().remove(0);
    assert_eq!(reclaimed.attempt, 2);
    assert!(repo.claim_many("theirs", 1, LEASE).await.unwrap().is_empty());

    repo.settle_success(&reclaimed, &json!("ours"), &[], graph.settlement_rules(), cfg())
        .await
        .unwrap();
    assert_eq!(get(&repo, &ours).await.status, TaskStatus::Succeeded);
    assert_eq!(get(&repo, &theirs).await.status, TaskStatus::Running);
    assert_eq!(get(&repo, &theirs).await.output, None);
}

#[tokio::test]
async fn failed_predecessors_cascade_and_late_joins_are_failed_immediately() {
    let (pool, repo, declared) = memory_repo(
        "cascade",
        &[
            ("root", Normal, None),
            ("child", Normal, None),
            ("join", Join, None),
            ("late_join", Join, None),
        ],
        &[("root", "join"), ("root", "late_join")],
    )
    .await;
    let graph = declared.as_ref();
    let root = repo
        .seed_task(graph, &new_task("cascade", "root"))
        .await
        .unwrap();
    let mut child = new_task("cascade", "child");
    child.run_id = Some(root.run_id);
    child.parent_task_ids = vec![root.task_id];
    let child = repo.seed_task(graph, &child).await.unwrap();
    let join = repo
        .seed_task(
            graph,
            &TaskSeed {
                run_id: Some(root.run_id),
                ..new_task("cascade", "join")
            },
        )
        .await
        .unwrap();
    let mut other_root = new_task("cascade", "root");
    other_root.run_id = Some(0);
    repo.seed_task(graph, &other_root).await.unwrap();
    let other_run = repo
        .seed_task(
            graph,
            &TaskSeed {
                run_id: Some(0),
                ..new_task("cascade", "join")
            },
        )
        .await
        .unwrap();
    let other_graph = make_graph("other", &[("root", Normal, None)], &[]);
    let unrelated = repo
        .seed_task(other_graph.as_ref(), &new_task("other", "root"))
        .await
        .unwrap();

    let root = repo.claim_many("cascade", 1, LEASE).await.unwrap().remove(0);
    let message = "permanent failure";

    sqlx::query(
        r#"
        CREATE TRIGGER reject_cascade BEFORE UPDATE OF status ON tasks
        WHEN NEW.workflow_id = 'cascade' AND OLD.name = 'join' AND NEW.status = 'failed'
        BEGIN
            SELECT RAISE(ABORT, 'injected cascade failure');
        END
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let rules = graph.settlement_rules();
    assert!(
        repo.settle_failure(&root, message, &[], rules, cfg())
            .await
            .is_err()
    );
    let unchanged = get(&repo, &root).await;
    assert_eq!(unchanged.status, TaskStatus::Running);
    assert_eq!(unchanged.lease_until, root.lease_until);
    assert!(unchanged.error.is_none());
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Pending);
    assert_eq!(get(&repo, &join).await.status, TaskStatus::Collecting);
    sqlx::query("DROP TRIGGER reject_cascade")
        .execute(&pool)
        .await
        .unwrap();

    repo.settle_failure(&root, message, &[], rules, cfg())
        .await
        .unwrap();
    assert_eq!(get(&repo, &root).await.status, TaskStatus::Failed);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Pending);
    assert_eq!(get(&repo, &join).await.status, TaskStatus::Failed);
    assert_eq!(
        get(&repo, &other_run).await.status,
        TaskStatus::Collecting,
        "name-based cascading stays in its run"
    );
    assert_eq!(get(&repo, &unrelated).await.status, TaskStatus::Pending);

    let late = repo
        .seed_task(
            graph,
            &TaskSeed {
                run_id: Some(root.run_id),
                ..new_task("cascade", "late_join")
            },
        )
        .await
        .unwrap();
    assert_eq!(late.status, TaskStatus::Failed);
    let claims = repo.claim_many("cascade", 10, LEASE).await.unwrap();
    assert_eq!(claims.len(), 2);
    assert!(claims.iter().any(|task| task.task_id == child.task_id));
}

#[tokio::test]
async fn cascade_matches_runs_only_for_join_predecessors() {
    let runs = [10, 20, 30, 0];
    for source_run in runs {
        let (_, repo, declared) = memory_repo(
            "cascade_runs",
            &[
                ("root", Normal, None),
                ("child", Normal, None),
                ("completed", Normal, None),
                ("join", Join, None),
            ],
            &[("root", "join")],
        )
        .await;
        let graph = declared.as_ref();
        let mut root = new_task("cascade_runs", "root");
        root.run_id = Some(source_run);
        let root = repo.seed_task(graph, &root).await.unwrap();
        let mut targets = Vec::new();

        for run_id in runs {
            if run_id != source_run {
                let mut other_root = new_task("cascade_runs", "root");
                other_root.run_id = Some(run_id);
                repo.seed_task(graph, &other_root).await.unwrap();
            }
            let mut child = new_task("cascade_runs", "child");
            child.run_id = Some(run_id);
            child.parent_task_ids = vec![root.task_id];
            targets.push(repo.seed_task(graph, &child).await.unwrap());
            targets.push(
                repo.seed_task(
                    graph,
                    &TaskSeed {
                        run_id: Some(run_id),
                        ..new_task("cascade_runs", "join")
                    },
                )
                .await
                .unwrap(),
            );
        }

        let mut other_workflow = new_task("other", "child");
        other_workflow.run_id = Some(source_run);
        other_workflow.parent_task_ids = vec![root.task_id];
        let other_graph = make_graph("other", &[("child", Normal, None)], &[]);
        let other_workflow = repo
            .seed_task(other_graph.as_ref(), &other_workflow)
            .await
            .unwrap();

        let mut completed = new_task("cascade_runs", "completed");
        completed.run_id = Some(source_run);
        let completed = repo.seed_task(graph, &completed).await.unwrap();
        let claims = repo.claim_many("cascade_runs", 10, LEASE).await.unwrap();
        let root_claim = claims
            .iter()
            .find(|task| task.task_id == root.task_id)
            .unwrap();
        let completed_claim = claims
            .iter()
            .find(|task| task.task_id == completed.task_id)
            .unwrap();
        let rules = graph.settlement_rules();
        repo.settle_success(completed_claim, &json!("done"), &[], rules, cfg())
            .await
            .unwrap();

        repo.settle_failure(root_claim, "permanent failure", &[], rules, cfg())
            .await
            .unwrap();

        for target in targets {
            let expected = if target.name != "join" {
                TaskStatus::Running
            } else if target.run_id == source_run {
                TaskStatus::Failed
            } else {
                TaskStatus::Collecting
            };
            assert_eq!(
                get(&repo, &target).await.status,
                expected,
                "source run {source_run:?}, target {} in run {:?}",
                target.name,
                target.run_id,
            );
        }
        assert_eq!(
            get(&repo, &other_workflow).await.status,
            TaskStatus::Pending
        );
        let completed = get(&repo, &completed).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);
        assert_eq!(completed.output, Some(json!("done")));
    }
}

#[tokio::test]
async fn renewal_extends_the_live_claim_without_changing_its_generation() {
    for previous_attempts in [0, DEFAULT_MAX_ATTEMPTS] {
        let workflow_id = format!("renew-{previous_attempts}");
        let (pool, repo, declared) = memory_repo(
            &workflow_id,
            &[("analysis.root", Normal, Some("analysis"))],
            &[],
        )
        .await;
        let graph = declared.as_ref();

        let task = new_task(&workflow_id, "analysis.root");
        let pending = repo.seed_task(graph, &task).await.unwrap();
        for _ in 0..previous_attempts {
            let previous = repo.claim_many(&workflow_id, 1, LEASE).await.unwrap().remove(0);
            repo.release_claim(&previous, None, None).await.unwrap();
        }
        assert!(repo.renew_claim(&pending, LEASE).await.unwrap().is_none());
        let claim = repo.claim_many(&workflow_id, 1, LEASE).await.unwrap().remove(0);
        sqlx::query("UPDATE tasks SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+10 seconds') WHERE workflow_id = ?")
            .bind(&workflow_id).execute(&pool).await.unwrap();
        let shortened = get(&repo, &claim).await.lease_until.unwrap();
        let renewed = repo.renew_claim(&claim, LEASE).await.unwrap().unwrap();
        assert!(renewed > shortened + chrono::Duration::seconds(280));
        let current = get(&repo, &claim).await;
        assert_eq!(current.lease_until, Some(renewed));
        assert_eq!(current.attempt, claim.attempt);
        assert_eq!(current.error, claim.error);
        assert_eq!(current.scope_id, pending.scope_id);
        assert!(repo.claim_many(&workflow_id, 1, LEASE).await.unwrap().is_empty());
        let mut wrong_workflow = claim.clone();
        wrong_workflow.workflow_id = "missing".into();
        assert!(repo.renew_claim(&wrong_workflow, LEASE).await.unwrap().is_none());
        let mut wrong_task = claim.clone();
        wrong_task.task_id += 1;
        assert!(repo.renew_claim(&wrong_task, LEASE).await.unwrap().is_none());
        let rules = graph.settlement_rules();
        if previous_attempts == DEFAULT_MAX_ATTEMPTS {
            repo.settle_failure(&claim, "budget exhausted", &[], rules, cfg())
                .await
                .unwrap();
        } else {
            repo.settle_success(&claim, &json!("done"), &[], rules, cfg())
                .await
                .unwrap();
        }
        assert!(repo.renew_claim(&claim, LEASE).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn stale_claims_cannot_mutate_a_reclaimed_task_or_create_successors() {
    let (pool, repo, declared) = memory_repo(
        "fencing",
        &[
            ("root", Normal, None),
            ("child", Normal, None),
            ("forbidden", Normal, None),
        ],
        &[],
    )
    .await;
    let graph = declared.as_ref();
    let root = repo
        .seed_task(graph, &new_task("fencing", "root"))
        .await
        .unwrap();
    let mut child = new_task("fencing", "child");
    child.run_id = Some(root.run_id);
    child.parent_task_ids = vec![root.task_id];
    let child = repo.seed_task(graph, &child).await.unwrap();
    let first = repo.claim_many("fencing", 1, LEASE).await.unwrap().remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00.000Z' WHERE workflow_id = 'fencing' AND task_id = ?")
        .bind(root.task_id).execute(&pool).await.unwrap();
    let second = repo.claim_many("fencing", 1, LEASE).await.unwrap().remove(0);
    assert_eq!(second.attempt, first.attempt + 1);
    let before = serde_json::to_value(&second).unwrap();

    assert!(repo.renew_claim(&first, LEASE).await.unwrap().is_none());
    let rules = graph.settlement_rules();
    let fanout = plan("forbidden", vec![json!("stale")]);
    for result in [
        repo.release_claim(&first, None, None).await,
        repo.settle_success(
            &first,
            &json!("stale"),
            std::slice::from_ref(&fanout),
            rules,
            cfg(),
        )
        .await,
        repo.settle_failure(&first, "stale", std::slice::from_ref(&fanout), rules, cfg())
            .await,
        repo.settle_failure(&first, "stale", &[], rules, cfg()).await,
        repo.settle_success(&first, &json!("stale"), &[], rules, cfg())
            .await,
    ] {
        assert!(matches!(result, Err(StorageError::ClaimLost { .. })));
    }
    assert_eq!(
        serde_json::to_value(get(&repo, &second).await).unwrap(),
        before
    );
    assert_eq!(repo.list_all("fencing").await.unwrap().len(), 2);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Pending);
    repo.settle_success(&second, &json!("current"), &[], rules, cfg())
        .await
        .unwrap();
    assert_eq!(get(&repo, &second).await.output, Some(json!("current")));
    assert!(repo.renew_claim(&second, LEASE).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(&second, None, None).await,
        Err(StorageError::ClaimLost { .. })
    ));
}

#[tokio::test]
async fn transactional_submit_and_reclaim_within_a_workflow() {
    let id = "published";
    let nodes = [
        ("analysis.entry", Normal, Some("analysis")),
        ("finish", Normal, None),
    ];
    let edges = [("analysis.entry", "finish")];
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let other_id = "published-other";
    let declared = make_graph(id, &nodes, &edges);
    let rules = declared.settlement_rules();
    let task = declared
        .describe()
        .task("analysis.entry", json!(42))
        .unwrap();
    let first = SqliteTaskRepo::new(pool.clone());
    let replacement = SqliteTaskRepo::new(pool.clone());

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let run = Database::in_transaction(&mut tx)
        .submit(task.clone())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(first.get(id, run.run_id).await.unwrap().is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM scopes")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let run = Database::in_transaction(&mut tx)
        .submit(task)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(first.claim_many(other_id, 1, LEASE).await.unwrap().is_empty());
    let root = first.claim_many(id, 1, LEASE).await.unwrap().pop().unwrap();
    assert_eq!(root.task_id, run.run_id);
    assert!(root.scope_id.is_some());
    assert_eq!(root.input, json!(42));

    first.release_claim(&root, None, None).await.unwrap();
    let recovered = replacement.claim_many(id, 1, LEASE).await.unwrap().pop().unwrap();
    assert_eq!(recovered.attempt, root.attempt + 1);
    assert!(
        first
            .settle_failure(&root, "stale claim", &[], rules, cfg())
            .await
            .is_err()
    );
    replacement
        .settle_success(
            &recovered,
            &json!(42),
            &[plan("finish", vec![json!(42)])],
            rules,
            cfg(),
        )
        .await
        .unwrap();
    assert!(first.claim_many(other_id, 1, LEASE).await.unwrap().is_empty());
    let child = first.claim_many(id, 1, LEASE).await.unwrap().pop().unwrap();
    assert_eq!(child.parent_task_ids, [root.task_id]);
    assert_eq!(child.run_id, run.run_id);
    assert_eq!(child.scope_id, None);
}

#[tokio::test]
async fn lease_expiry_rejects_settlement_even_without_takeover() {
    let (pool, repo, declared) = memory_repo("expiry", &[("root", Normal, None)], &[]).await;
    let graph = declared.as_ref();
    repo.seed_task(graph, &new_task("expiry", "root"))
        .await
        .unwrap();
    let claim = repo.claim_many("expiry", 1, LEASE).await.unwrap().remove(0);
    assert!(repo.claim_many("expiry", 1, LEASE).await.unwrap().is_empty());
    assert_eq!(get(&repo, &claim).await.lease_until, claim.lease_until);

    sqlx::query(
        "UPDATE tasks SET lease_until = '2000-01-01T00:00:00.000Z' WHERE workflow_id = 'expiry'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let rules = graph.settlement_rules();
    assert!(repo.renew_claim(&claim, LEASE).await.unwrap().is_none());
    for result in [
        repo.release_claim(&claim, None, None).await,
        repo.settle_success(&claim, &json!("expired"), &[], rules, cfg())
            .await,
        repo.settle_failure(&claim, "expired", &[], rules, cfg()).await,
    ] {
        assert!(
            matches!(result, Err(StorageError::ClaimLost { .. })),
            "expiry alone invalidates a claim even without takeover"
        );
    }
    let expired = get(&repo, &claim).await;
    assert_eq!(expired.attempt, claim.attempt);
    assert_eq!(expired.status, TaskStatus::Running);
    assert!(expired.output.is_none());
}

#[tokio::test]
async fn releasing_a_claim_only_releases_the_selected_task() {
    let (_, repo, declared) = memory_repo(
        "worker-release",
        &[("first", Normal, None), ("second", Normal, None)],
        &[],
    )
    .await;
    let graph = declared.as_ref();
    repo.seed_task(graph, &new_task("worker-release", "first"))
        .await
        .unwrap();
    repo.seed_task(graph, &new_task("worker-release", "second"))
        .await
        .unwrap();
    let first = repo
        .claim_many("worker-release", 1, LEASE)
        .await
        .unwrap()
        .remove(0);
    let second = repo
        .claim_many("worker-release", 1, LEASE)
        .await
        .unwrap()
        .remove(0);
    repo.release_claim(&first, None, None).await.unwrap();
    assert!(matches!(
        repo.release_claim(&first, None, None).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(get(&repo, &second).await.lease_until, second.lease_until);
    let reclaimed = repo.claim_many("worker-release", 10, LEASE).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].task_id, first.task_id);
}

struct DatabaseFile(PathBuf);

impl DatabaseFile {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("workflow-engine-{}.sqlite", uuid::Uuid::new_v4())))
    }

    async fn connect(&self) -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&self.0)
                    .create_if_missing(true)
                    .busy_timeout(Duration::from_secs(10)),
            )
            .await
            .unwrap()
    }
}

impl Drop for DatabaseFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut path = self.0.as_os_str().to_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(path));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_pools_claim_without_duplicates_and_persist_after_reopening() {
    let file = DatabaseFile::new();
    let pool_a = file.connect().await;
    migrate(&pool_a).await.unwrap();
    let pool_b = file.connect().await;
    let declared = make_graph(
        "concurrent",
        &[("worker", Normal, None), ("after_reopen", Normal, None)],
        &[],
    );
    let graph = declared.as_ref();
    let repo_a = SqliteTaskRepo::new(pool_a.clone());
    let repo_b = SqliteTaskRepo::new(pool_b.clone());
    for _ in 0..20 {
        let (first, second) = tokio::join!(
            repo_a.enqueue(crate::Task::new("concurrent", "worker", json!({}))),
            repo_b.enqueue(crate::Task::new("concurrent", "worker", json!({}))),
        );
        assert_ne!(first.unwrap().run_id, second.unwrap().run_id);
    }

    let (a, b, c, d) = tokio::join!(
        repo_a.claim_many("concurrent", 10, LEASE),
        repo_b.claim_many("concurrent", 10, LEASE),
        repo_a.claim_many("concurrent", 10, LEASE),
        repo_b.claim_many("concurrent", 10, LEASE),
    );
    let tasks: Vec<_> = [a, b, c, d].into_iter().flat_map(Result::unwrap).collect();
    assert_eq!(tasks.len(), 40);
    assert_eq!(
        tasks
            .iter()
            .map(|t| t.task_id)
            .collect::<HashSet<_>>()
            .len(),
        40
    );
    assert!(
        tasks
            .iter()
            .all(|t| t.attempt == 1 && t.status == TaskStatus::Running)
    );
    let task_id = tasks[0].task_id;
    repo_a
        .settle_success(
            &tasks[0],
            &json!({"saved": true}),
            &[],
            graph.settlement_rules(),
            cfg(),
        )
        .await
        .unwrap();

    let fanin_graph = make_graph(
        "concurrent_fanin",
        &[("worker", Normal, None), ("collect", Join, None)],
        &[("worker", "collect")],
    );
    for n in 1..=4 {
        let mut source = new_task("concurrent_fanin", "worker");
        source.run_id = Some(1);
        source.task.input = json!(n);
        repo_a
            .seed_task(fanin_graph.as_ref(), &source)
            .await
            .unwrap();
    }
    let sources = repo_a.claim_many("concurrent_fanin", 4, LEASE).await.unwrap();
    let results = futures::future::join_all(sources.iter().enumerate().map(|(index, source)| {
        let repo = if index % 2 == 0 { &repo_a } else { &repo_b };
        let rules = fanin_graph.settlement_rules();
        async move {
            repo.settle_success(
                source,
                &source.input,
                &[join_plan(
                    "collect",
                    "worker",
                    MergeMode::Append,
                    vec![source.input.clone()],
                )],
                rules,
                cfg(),
            )
            .await
            .unwrap();
        }
    }))
    .await;
    assert_eq!(results.len(), 4);
    let merged: Vec<_> = repo_a
        .list_all("concurrent_fanin")
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.name == "collect")
        .collect();
    assert_eq!(
        merged.len(),
        1,
        "concurrent fan-in shares a single destination"
    );
    assert_eq!(
        merged[0]
            .parent_task_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>(),
        sources
            .iter()
            .map(|source| source.task_id)
            .collect::<HashSet<_>>()
    );
    let mut values: Vec<_> = merged[0].input["worker"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_i64().unwrap())
        .collect();
    values.sort_unstable();
    assert_eq!(
        values,
        [1, 2, 3, 4],
        "concurrent contributions are not lost"
    );
    pool_a.close().await;
    pool_b.close().await;

    let reopened = file.connect().await;
    migrate(&reopened).await.unwrap();
    let repo = SqliteTaskRepo::new(reopened.clone());
    let rows = repo.list_all("concurrent").await.unwrap();
    assert_eq!(rows.len(), 40);
    let saved = repo.get("concurrent", task_id).await.unwrap().unwrap();
    assert_eq!(saved.status, TaskStatus::Succeeded);
    assert_eq!(saved.output, Some(json!({"saved": true})));
    for running in rows
        .iter()
        .filter(|task| task.status == TaskStatus::Running)
    {
        repo.release_claim(running, None, None).await.unwrap();
    }
    assert_eq!(
        repo.claim_many("concurrent", 40, LEASE).await.unwrap().len(),
        39
    );
    let next = repo
        .seed_task(graph, &new_task("concurrent", "after_reopen"))
        .await
        .unwrap();
    assert!(next.task_id > merged[0].task_id);
    reopened.close().await;
}
