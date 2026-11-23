use std::{collections::HashSet, path::PathBuf, time::Duration};

use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use super::{SqliteTaskRepo, migrate};
#[path = "graph_fixture.rs"]
mod graph_fixture;
use crate::error::StorageError;
use crate::graph::{Successor, TaskType};
use crate::task::MAX_ATTEMPTS;
use crate::task::TaskStore;
use crate::{Task, TaskRecord, TaskStatus};
use TaskType::{Join, Normal};
use graph_fixture::graph;

async fn memory_repo(
    workflow_id: &str,
    nodes: &[(&'static str, TaskType, Option<&str>)],
    edges: &[(&'static str, &'static str)],
) -> (SqlitePool, SqliteTaskRepo) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let repo = SqliteTaskRepo::new(pool.clone(), graph(workflow_id, nodes, edges));
    (pool, repo)
}

fn new_task(workflow_id: &str, name: &str) -> Task {
    Task::new(workflow_id, name, json!({}))
}

async fn get(repo: &SqliteTaskRepo, task: &TaskRecord) -> TaskRecord {
    repo.get(&task.workflow_id, task.task_id)
        .await
        .unwrap()
        .unwrap()
}

fn plan(dst: &'static str, inputs: Vec<Value>) -> Successor {
    Successor {
        new_scope_per_input: false,

        dst,
        inputs,
    }
}

#[tokio::test]
async fn migration_is_idempotent_and_task_values_round_trip() {
    let (pool, repo) = memory_repo(
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
    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('tasks')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for removed in [
        "task_type",
        "depends_on_names",
        "claimed_by",
        "max_attempts",
    ] {
        assert!(!columns.iter().any(|column| column == removed));
    }
    assert!(!columns.iter().any(|name| name == "join_state"));
    let store = repo.clone();
    let mut input = new_task("workflow", "root");
    input.run_id = Some(42);
    input.input = json!({"text": "工作流 'quoted'", "nested": [null, true, {"n": 3}]});
    let first = store.enqueue(&input).await.unwrap();

    migrate(&pool).await.unwrap();
    let second = store
        .enqueue(&new_task("workflow", "second"))
        .await
        .unwrap();
    let other_repo =
        SqliteTaskRepo::new(pool.clone(), graph("other", &[("root", Normal, None)], &[]));
    let other = other_repo
        .enqueue(&new_task("other", "root"))
        .await
        .unwrap();
    assert!(matches!(
        repo.enqueue(&new_task("other", "root")).await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert!(matches!(
        repo.claim_many("other", 1).await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert_ne!(first.task_id, second.task_id);
    assert_eq!(second.run_id, Some(second.task_id));
    assert_ne!(other.task_id, first.task_id);
    assert_ne!(other.task_id, second.task_id);
    assert_eq!(other.run_id, Some(other.task_id));
    assert_eq!(first.name, "root");
    assert!(first.parent_task_ids.is_empty());
    assert_eq!(first.input, input.input);
    assert_eq!(first.status, TaskStatus::Pending);
    assert_eq!(first.attempt, 0);
    assert!(first.lease_until.is_none());
    assert!(first.created_at <= first.updated_at);

    let rows = repo.list_all("workflow").await.unwrap();
    assert_eq!(
        rows.iter().map(|t| t.task_id).collect::<Vec<_>>(),
        [first.task_id, second.task_id]
    );
    assert_eq!(rows[0].input, input.input);
    assert_eq!(rows[0].run_id, Some(42));
    assert_eq!(rows[1].run_id, second.run_id);
    assert!(repo.get("missing", 1).await.unwrap().is_none());

    let output = json!([null, {"result": "完成"}]);
    let claimed = repo.claim_many("workflow", 1).await.unwrap().remove(0);
    let settled = repo.succeed(&claimed, &output).await.unwrap();
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(get(&repo, &first).await.output, Some(output));

    let mut tx = pool.begin().await.unwrap();
    let rolled_back = repo
        .insert_task(&mut tx, &new_task("workflow", "rollback"))
        .await
        .unwrap();
    assert_eq!(rolled_back.run_id, Some(rolled_back.task_id));
    tx.rollback().await.unwrap();
    assert!(
        repo.get("workflow", rolled_back.task_id)
            .await
            .unwrap()
            .is_none()
    );
    let next = store.enqueue(&new_task("workflow", "next")).await.unwrap();
    assert_eq!(next.run_id, Some(next.task_id));
    assert_ne!(next.run_id, second.run_id);

    for name in ["undeclared", "analysis"] {
        assert!(matches!(
            repo.enqueue(&new_task("workflow", name)).await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }

    let scoped = repo
        .enqueue(&Task {
            run_id: first.run_id,

            ..new_task("workflow", "analysis.entry")
        })
        .await
        .unwrap();
    for (run_id, scope_id) in [(first.run_id, Some(i64::MAX)), (Some(43), scoped.scope_id)] {
        let invalid = Task {
            run_id,
            scope_id,
            ..new_task("workflow", "invalid_scope")
        };
        assert!(matches!(
            repo.enqueue(&invalid).await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }
}

#[tokio::test]
async fn join_claims_require_all_named_predecessors_including_missing_tasks() {
    let (_, repo) = memory_repo(
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
    let root = repo.enqueue(&new_task("deps", "root")).await.unwrap();
    let mut child = new_task("deps", "child");
    child.run_id = root.run_id;

    child.parent_task_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    assert_eq!(child.run_id, root.run_id);
    assert_eq!(child.parent_task_ids, [root.task_id]);
    assert_eq!(child.status, TaskStatus::Collecting);
    let mut missing = new_task("deps", "missing_dependency");
    missing.run_id = root.run_id;

    let missing = repo.enqueue(&missing).await.unwrap();

    assert!(repo.claim_many("deps", 0).await.unwrap().is_empty());
    let claimed = repo.claim_many("deps", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, root.task_id);
    assert_eq!(claimed[0].attempt, 1);
    assert!(claimed[0].lease_until.is_some());
    assert!(repo.claim_many("deps", 10).await.unwrap().is_empty());
    assert!(
        repo.executable_workflow_ids("deps")
            .await
            .unwrap()
            .is_empty()
    );

    repo.succeed(&claimed[0], &json!(null)).await.unwrap();
    assert_eq!(
        repo.executable_workflow_ids("deps").await.unwrap(),
        ["deps"]
    );
    let claimed = repo.claim_many("deps", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, child.task_id);
    assert_eq!(claimed[0].status, TaskStatus::Running);
    assert_eq!(get(&repo, &missing).await.status, TaskStatus::Collecting);

    // A direct JOIN entry starts its own run with the supplied input.
    let entry = repo
        .enqueue(&new_task("deps", "missing_dependency"))
        .await
        .unwrap();
    assert_eq!(entry.status, TaskStatus::Pending);
    let claimed = repo.claim_many("deps", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, entry.task_id);
    assert_eq!(get(&repo, &missing).await.status, TaskStatus::Collecting);
}

#[tokio::test]
async fn exhausted_tasks_are_claimed_for_fenced_final_failure_settlement() {
    let (pool, repo) = memory_repo(
        "retry",
        &[("root", Normal, None), ("child", Join, None)],
        &[("root", "child")],
    )
    .await;
    let task = repo.enqueue(&new_task("retry", "root")).await.unwrap();
    let mut child = new_task("retry", "child");
    child.run_id = task.run_id;

    child.parent_task_ids = vec![task.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let first = repo.claim_many("retry", 1).await.unwrap();
    assert_eq!(first[0].attempt, 1);
    repo.release_claim(&first[0]).await.unwrap();
    let second = repo.claim_many("retry", 1).await.unwrap();
    assert_eq!(second[0].attempt, 2);

    sqlx::query("UPDATE tasks SET lease_until = ? WHERE workflow_id = ? AND task_id = ?")
        .bind(chrono::Utc::now() - chrono::Duration::seconds(10))
        .bind("retry")
        .bind(task.task_id)
        .execute(&pool)
        .await
        .unwrap();
    let third = repo.claim_many("retry", 1).await.unwrap();
    assert_eq!(third[0].attempt, 3);
    assert!(repo.claim_many("retry", 1).await.unwrap().is_empty());
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);

    sqlx::query(
        "UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'retry'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        repo.executable_workflow_ids("retry").await.unwrap(),
        ["retry"]
    );
    let recovery = repo.claim_many("retry", 1).await.unwrap().remove(0);
    assert_eq!(recovery.attempt, 4);
    assert_eq!(recovery.status, TaskStatus::Running);
    assert_eq!(
        recovery.error.as_deref(),
        Some("task lease expired; retry budget exhausted")
    );
    for result in [
        repo.succeed(&recovery, &json!("too late"))
            .await
            .map(|_| ()),
        repo.skip(&recovery, "skip final failure").await,
        repo.fail_and_enqueue(&third[0], "old worker", &[]).await,
    ] {
        assert!(matches!(result, Err(StorageError::ClaimLost { .. })));
    }

    // Recovery can itself be interrupted; it must retain the original failure reason.
    repo.release_claim(&recovery).await.unwrap();
    let next_recovery = repo.claim_many("retry", 1).await.unwrap().remove(0);
    assert_eq!(next_recovery.attempt, 5);
    assert_eq!(next_recovery.error, recovery.error);
    assert!(matches!(
        repo.fail_and_enqueue(&recovery, "stale recovery", &[])
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.fail_and_enqueue(&next_recovery, next_recovery.error.as_deref().unwrap(), &[])
        .await
        .unwrap();
    let exhausted = get(&repo, &task).await;
    assert_eq!(exhausted.status, TaskStatus::Failed);
    assert_eq!(exhausted.attempt, 5);
    assert!(exhausted.lease_until.is_none());
    assert_eq!(exhausted.error, recovery.error);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Failed);
    assert!(
        repo.executable_workflow_ids("retry")
            .await
            .unwrap()
            .is_empty()
    );

    let repo = SqliteTaskRepo::new(pool.clone(), graph("zero", &[("root", Normal, None)], &[]));
    let zero = new_task("zero", "root");
    let zero = repo.enqueue(&zero).await.unwrap();
    for _ in 0..MAX_ATTEMPTS {
        let previous = repo.claim_many("zero", 1).await.unwrap().remove(0);
        repo.release_claim(&previous).await.unwrap();
    }
    let recovery = repo.claim_many("zero", 1).await.unwrap().remove(0);
    assert_eq!(recovery.attempt, MAX_ATTEMPTS + 1);

    assert_eq!(
        recovery.error.as_deref(),
        Some("task retry budget exhausted")
    );
    repo.fail_and_enqueue(&recovery, recovery.error.as_deref().unwrap(), &[])
        .await
        .unwrap();
    assert_eq!(get(&repo, &zero).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn join_merges_across_parents_and_waits_for_all_predecessors() {
    let (_, repo) = memory_repo(
        "join",
        &[
            ("image", Normal, None),
            ("text", Normal, None),
            ("collect", Join, None),
        ],
        &[("image", "collect"), ("text", "collect")],
    )
    .await;
    let mut images = Vec::new();
    for input in [json!("a"), json!("b")] {
        let mut image = new_task("join", "image");
        image.run_id = Some(1);
        image.input = input;
        images.push(repo.enqueue(&image).await.unwrap());
    }
    let store = repo.clone();
    let mut wrong_run = new_task("join", "text");
    wrong_run.run_id = Some(2);
    let wrong_run = repo.enqueue(&wrong_run).await.unwrap();
    let mut claimed = repo.claim_many("join", 10).await.unwrap();
    let wrong_claim = claimed.remove(
        claimed
            .iter()
            .position(|task| task.task_id == wrong_run.task_id)
            .unwrap(),
    );
    repo.succeed(&wrong_claim, &json!({})).await.unwrap();
    assert_eq!(
        claimed.len(),
        2,
        "both image instances execute independently"
    );
    assert!(claimed.iter().all(|t| t.name == "image"));
    assert_eq!(
        claimed.iter().map(|task| task.task_id).collect::<Vec<_>>(),
        images.iter().map(|task| task.task_id).collect::<Vec<_>>()
    );

    let contribution = |input| plan("collect", vec![input]);
    store
        .settle_success(
            &claimed[0],
            &json!("a"),
            &[contribution(json!({"image": "a"}))],
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
    assert!(repo.claim_many("join", 10).await.unwrap().is_empty());
    store
        .settle_success(
            &claimed[1],
            &json!("b"),
            &[contribution(json!({"image": "b"}))],
        )
        .await
        .unwrap();
    let merged = get(&repo, &join).await;
    assert_eq!(merged.input, json!({"image": ["a", "b"]}));
    assert_eq!(
        merged.parent_task_ids,
        [claimed[0].task_id, claimed[1].task_id]
    );
    assert!(
        repo.claim_many("join", 10).await.unwrap().is_empty(),
        "another run cannot satisfy a missing predecessor"
    );

    let mut text = new_task("join", "text");
    text.run_id = Some(1);
    let text = repo.enqueue(&text).await.unwrap();
    let text_claim = repo.claim_many("join", 10).await.unwrap().remove(0);
    assert_eq!(text_claim.task_id, text.task_id);
    store
        .settle_success(
            &text_claim,
            &json!("text"),
            &[contribution(json!({"text": "text"}))],
        )
        .await
        .unwrap();
    let claimed = repo.claim_many("join", 10).await.unwrap();
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
    let (_, repo) = memory_repo(
        "fanin",
        &[("image", Normal, None), ("collect", Join, None)],
        &[("image", "collect")],
    )
    .await;
    for run_id in [Some(1), Some(2), Some(0)] {
        for input in [
            json!({"image": "first"}),
            json!({"image": ["second", "third"]}),
        ] {
            let mut source = new_task("fanin", "image");
            source.run_id = run_id;
            source.input = input;
            repo.enqueue(&source).await.unwrap();
        }
    }
    let sources = repo.claim_many("fanin", 10).await.unwrap();
    for source in &sources {
        let contribution = plan("collect", vec![source.input.clone()]);
        repo.succeed_and_enqueue(source, &source.input, &[contribution])
            .await
            .unwrap();
    }
    let joins = repo.claim_many("fanin", 10).await.unwrap();
    assert_eq!(joins.len(), 3);
    assert_eq!(
        joins.iter().map(|join| join.run_id).collect::<HashSet<_>>(),
        HashSet::from([Some(1), Some(2), Some(0)])
    );
    for join in joins {
        assert_eq!(join.input, json!({"image": ["first", "second", "third"]}));
        assert_eq!(join.parent_task_ids.len(), 2);
    }
}

#[tokio::test]
async fn success_and_all_fanouts_commit_or_roll_back_together() {
    let (pool, repo) = memory_repo(
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
    let task = repo
        .enqueue(&new_task("atomic_success", "root"))
        .await
        .unwrap();
    let store = repo.clone();
    let claimed = store
        .claim_many("atomic_success", 1)
        .await
        .unwrap()
        .remove(0);
    let valid = plan("child", vec![json!("a"), json!("b")]);
    let scoped = Successor {
        dst: "analysis.child",
        new_scope_per_input: true,

        ..valid.clone()
    };
    let invalid = plan("invalid", vec![json!({})]);
    sqlx::query("CREATE TRIGGER reject_invalid BEFORE INSERT ON tasks WHEN NEW.name = 'invalid' BEGIN SELECT RAISE(ABORT, 'invalid successor'); END")
        .execute(&pool).await.unwrap();
    assert!(
        store
            .settle_success(&claimed, &json!({"done": true}), &[valid.clone(), invalid])
            .await
            .is_err()
    );
    let unchanged = get(&repo, &task).await;
    assert_eq!(unchanged.status, TaskStatus::Running);
    assert!(unchanged.output.is_none());
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 1);
    assert!(matches!(
        store
            .settle_success(&claimed, &json!({}), &[plan("undeclared", vec![json!({})])])
            .await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 1);

    // Successors inherit the latest run and scope, not the claim's older snapshot.
    sqlx::query("UPDATE tasks SET run_id = 9 WHERE workflow_id = 'atomic_success'")
        .execute(&pool)
        .await
        .unwrap();
    store
        .settle_success(&claimed, &json!({"done": true}), &[valid.clone(), scoped])
        .await
        .unwrap();
    assert!(matches!(
        store
            .settle_success(&claimed, &json!({"done": true}), &[valid])
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 5);
    let settled = get(&repo, &claimed).await;
    assert_eq!(settled.run_id, Some(9));
    assert_eq!(settled.scope_id, None);
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert!(settled.lease_until.is_none());
    let children = store.claim_many("atomic_success", 10).await.unwrap();
    assert_eq!(children.len(), 4);
    assert_ne!(children[0].task_id, children[1].task_id);
    assert_eq!(
        children.iter().map(|task| &task.input).collect::<Vec<_>>(),
        [&json!("a"), &json!("b"), &json!("a"), &json!("b")]
    );
    for (index, child) in children.into_iter().enumerate() {
        assert_eq!(child.run_id, Some(9));
        assert_eq!(child.scope_id.is_some(), index >= 2);
        assert_eq!(child.parent_task_ids, [task.task_id]);
    }
}

#[tokio::test]
async fn failure_and_recovery_fanouts_are_atomic_and_do_not_depend_on_failed_parent() {
    let (pool, repo) = memory_repo(
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
    let mut input = new_task("atomic_failure", "analysis.root");
    input.run_id = Some(4);

    let task = repo.enqueue(&input).await.unwrap();
    let store = repo.clone();
    for _ in 1..MAX_ATTEMPTS {
        let previous = store
            .claim_many("atomic_failure", 1)
            .await
            .unwrap()
            .remove(0);
        store.release_claim(&previous).await.unwrap();
    }
    let expired = store
        .claim_many("atomic_failure", 1)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'atomic_failure'")
        .execute(&pool).await.unwrap();
    let claimed = store
        .claim_many("atomic_failure", 1)
        .await
        .unwrap()
        .remove(0);
    assert!(claimed.attempt > MAX_ATTEMPTS);
    assert_eq!(expired.scope_id, task.scope_id);
    assert_eq!(claimed.scope_id, task.scope_id);
    let recovery = plan("analysis.recovery", vec![json!({"retry": true})]);
    let invalid = plan("invalid", vec![json!({})]);
    sqlx::query("CREATE TRIGGER reject_invalid BEFORE INSERT ON tasks WHEN NEW.name = 'invalid' BEGIN SELECT RAISE(ABORT, 'invalid successor'); END")
        .execute(&pool).await.unwrap();
    let message = claimed.error.as_deref().unwrap();
    assert!(matches!(
        store
            .settle_failure(&expired, message, std::slice::from_ref(&recovery))
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(
        store
            .settle_failure(&claimed, message, &[recovery.clone(), invalid])
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

    store
        .settle_failure(&claimed, message, &[recovery])
        .await
        .unwrap();
    let settled = get(&repo, &task).await;
    assert_eq!(settled.status, TaskStatus::Superseded);
    assert_eq!(settled.error.as_deref(), Some(message));
    assert!(settled.lease_until.is_none());
    assert!(matches!(
        store.settle_failure(&claimed, message, &[]).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(repo.list_all("atomic_failure").await.unwrap().len(), 2);
    let recovery = store
        .claim_many("atomic_failure", 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(recovery.name, "analysis.recovery");
    assert_eq!(recovery.run_id, Some(4));
    assert_eq!(recovery.scope_id, Some(21));
    assert_eq!(recovery.parent_task_ids, [task.task_id]);
    store.settle_failure(&recovery, message, &[]).await.unwrap();
    assert_eq!(get(&repo, &recovery).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn failure_fanout_to_the_same_name_creates_a_fresh_task() {
    let (_, repo) = memory_repo("same_name", &[("retry", Normal, None)], &[]).await;
    repo.enqueue(&new_task("same_name", "retry")).await.unwrap();
    let store = repo.clone();
    let source = store.claim_many("same_name", 1).await.unwrap().remove(0);
    store
        .settle_failure(
            &source,
            "retry requested",
            &[plan("retry", vec![json!({"retry": true})])],
        )
        .await
        .unwrap();
    assert_eq!(get(&repo, &source).await.status, TaskStatus::Superseded);
    let successors = store.claim_many("same_name", 1).await.unwrap();
    assert_eq!(successors.len(), 1);
    assert_ne!(successors[0].task_id, source.task_id);
    assert_eq!(successors[0].input, json!({"retry": true}));
    assert_eq!(successors[0].parent_task_ids, [source.task_id]);
}

#[tokio::test]
async fn release_and_settlement_stay_in_their_workflow() {
    let (pool, repo) = memory_repo("ours", &[("root", Normal, None)], &[]).await;
    let ours = repo.enqueue(&new_task("ours", "root")).await.unwrap();
    let their_repo = SqliteTaskRepo::new(pool, graph("theirs", &[("root", Normal, None)], &[]));
    let theirs = their_repo
        .enqueue(&new_task("theirs", "root"))
        .await
        .unwrap();
    let store = repo.clone();
    let claim = store.claim_many("ours", 1).await.unwrap().remove(0);
    assert_eq!(get(&repo, &theirs).await.status, TaskStatus::Pending);
    their_repo.claim_many("theirs", 1).await.unwrap();
    repo.release_claim(&claim).await.unwrap();
    let reclaimed = store.claim_many("ours", 10).await.unwrap().remove(0);
    assert_eq!(reclaimed.attempt, 2);
    assert!(their_repo.claim_many("theirs", 1).await.unwrap().is_empty());

    store
        .settle_success(&reclaimed, &json!("ours"), &[])
        .await
        .unwrap();
    assert_eq!(get(&repo, &ours).await.status, TaskStatus::Succeeded);
    assert_eq!(get(&repo, &theirs).await.status, TaskStatus::Running);
    assert_eq!(get(&repo, &theirs).await.output, None);
}

#[tokio::test]
async fn failed_predecessors_cascade_and_late_joins_are_failed_immediately() {
    let (pool, repo) = memory_repo(
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
    let root = repo.enqueue(&new_task("cascade", "root")).await.unwrap();
    let mut child = new_task("cascade", "child");
    child.run_id = root.run_id;
    child.parent_task_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let join = repo
        .enqueue(&Task {
            run_id: root.run_id,

            ..new_task("cascade", "join")
        })
        .await
        .unwrap();
    let other_run = repo
        .enqueue(&Task {
            run_id: Some(0),

            ..new_task("cascade", "join")
        })
        .await
        .unwrap();
    let other_repo =
        SqliteTaskRepo::new(pool.clone(), graph("other", &[("root", Normal, None)], &[]));
    let unrelated = other_repo
        .enqueue(&new_task("other", "root"))
        .await
        .unwrap();
    let store = repo.clone();
    let root = store.claim_many("cascade", 1).await.unwrap().remove(0);
    let message = "permanent failure";

    // A failed cascade must also roll back the source's terminal state and lease.
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
    assert!(store.settle_failure(&root, message, &[]).await.is_err());
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

    store.settle_failure(&root, message, &[]).await.unwrap();
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
        .enqueue(&Task {
            run_id: root.run_id,

            ..new_task("cascade", "late_join")
        })
        .await
        .unwrap();
    assert_eq!(late.status, TaskStatus::Failed);
    let claims = repo.claim_many("cascade", 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task_id, child.task_id);
}

#[tokio::test]
async fn cascade_matches_runs_only_for_join_predecessors() {
    let runs = [Some(10), Some(20), Some(30), Some(0)];
    for source_run in runs {
        let (pool, repo) = memory_repo(
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
        let mut root = new_task("cascade_runs", "root");
        root.run_id = source_run;
        let root = repo.enqueue(&root).await.unwrap();
        let mut targets = Vec::new();

        for run_id in runs {
            let mut child = new_task("cascade_runs", "child");
            child.run_id = run_id;
            child.parent_task_ids = vec![root.task_id];
            targets.push(repo.enqueue(&child).await.unwrap());
            targets.push(
                repo.enqueue(&Task {
                    run_id: run_id,

                    ..new_task("cascade_runs", "join")
                })
                .await
                .unwrap(),
            );
        }

        let mut other_workflow = new_task("other", "child");
        other_workflow.run_id = source_run;
        other_workflow.parent_task_ids = vec![root.task_id];
        let other_repo = SqliteTaskRepo::new(
            pool.clone(),
            graph("other", &[("child", Normal, None)], &[]),
        );
        let other_workflow = other_repo.enqueue(&other_workflow).await.unwrap();

        let mut completed = new_task("cascade_runs", "completed");
        completed.run_id = source_run;
        let completed = repo.enqueue(&completed).await.unwrap();
        let claims = repo.claim_many("cascade_runs", 10).await.unwrap();
        let root_claim = claims
            .iter()
            .find(|task| task.task_id == root.task_id)
            .unwrap();
        let completed_claim = claims
            .iter()
            .find(|task| task.task_id == completed.task_id)
            .unwrap();
        repo.succeed(completed_claim, &json!("done")).await.unwrap();

        let store = repo.clone();
        store
            .settle_failure(root_claim, "permanent failure", &[])
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
    for previous_attempts in [0, MAX_ATTEMPTS] {
        let workflow_id = format!("renew-{previous_attempts}");
        let (pool, repo) = memory_repo(
            &workflow_id,
            &[("analysis.root", Normal, Some("analysis"))],
            &[],
        )
        .await;
        let store = repo.clone();
        let task = new_task(&workflow_id, "analysis.root");

        let pending = repo.enqueue(&task).await.unwrap();
        for _ in 0..previous_attempts {
            let previous = repo.claim_many(&workflow_id, 1).await.unwrap().remove(0);
            repo.release_claim(&previous).await.unwrap();
        }
        assert!(store.renew_claim(&pending).await.unwrap().is_none());
        let claim = repo.claim_many(&workflow_id, 1).await.unwrap().remove(0);
        sqlx::query("UPDATE tasks SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+10 seconds') WHERE workflow_id = ?")
            .bind(&workflow_id).execute(&pool).await.unwrap();
        let shortened = get(&repo, &claim).await.lease_until.unwrap();
        // The caller's lease snapshot may be stale after any prior renewal.
        let renewed = store.renew_claim(&claim).await.unwrap().unwrap();
        assert!(renewed > shortened + chrono::Duration::seconds(280));
        let current = get(&repo, &claim).await;
        assert_eq!(current.lease_until, Some(renewed));
        assert_eq!(current.attempt, claim.attempt);
        assert_eq!(current.error, claim.error);
        assert_eq!(current.scope_id, pending.scope_id);
        assert!(repo.claim_many(&workflow_id, 1).await.unwrap().is_empty());
        let mut wrong_workflow = claim.clone();
        wrong_workflow.workflow_id = "missing".into();
        assert!(store.renew_claim(&wrong_workflow).await.unwrap().is_none());
        let mut wrong_task = claim.clone();
        wrong_task.task_id += 1;
        assert!(store.renew_claim(&wrong_task).await.unwrap().is_none());
        if previous_attempts == MAX_ATTEMPTS {
            store
                .settle_failure(&claim, "budget exhausted", &[])
                .await
                .unwrap();
        } else {
            store
                .settle_success(&claim, &json!("done"), &[])
                .await
                .unwrap();
        }
        assert!(store.renew_claim(&claim).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn stale_claims_cannot_mutate_a_reclaimed_task_or_create_successors() {
    let (pool, repo) = memory_repo(
        "fencing",
        &[
            ("root", Normal, None),
            ("child", Normal, None),
            ("forbidden", Normal, None),
        ],
        &[],
    )
    .await;
    let root = repo.enqueue(&new_task("fencing", "root")).await.unwrap();
    let mut child = new_task("fencing", "child");
    child.run_id = root.run_id;
    child.parent_task_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let first = repo.claim_many("fencing", 1).await.unwrap().remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'fencing' AND task_id = ?")
        .bind(root.task_id).execute(&pool).await.unwrap();
    let second = repo.claim_many("fencing", 1).await.unwrap().remove(0);
    assert_eq!(second.attempt, first.attempt + 1);
    let before = serde_json::to_value(&second).unwrap();
    let store = repo.clone();
    assert!(store.renew_claim(&first).await.unwrap().is_none());
    let fanout = plan("forbidden", vec![json!("stale")]);
    for result in [
        repo.release_claim(&first).await,
        store
            .settle_success(&first, &json!("stale"), std::slice::from_ref(&fanout))
            .await,
        store
            .settle_failure(&first, "stale", std::slice::from_ref(&fanout))
            .await,
        store.settle_failure(&first, "stale", &[]).await,
        repo.succeed(&first, &json!("stale")).await.map(|_| ()),
        repo.fail(&first, "stale").await,
        repo.supersede(&first, "stale").await,
        repo.skip(&first, "stale").await,
    ] {
        assert!(matches!(result, Err(StorageError::ClaimLost { .. })));
    }
    assert_eq!(
        serde_json::to_value(get(&repo, &second).await).unwrap(),
        before
    );
    assert_eq!(repo.list_all("fencing").await.unwrap().len(), 2);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Pending);
    store
        .settle_success(&second, &json!("current"), &[])
        .await
        .unwrap();
    assert_eq!(get(&repo, &second).await.output, Some(json!("current")));
    assert!(store.renew_claim(&second).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(&second).await,
        Err(StorageError::ClaimLost { .. })
    ));
}

#[tokio::test]
async fn lease_expiry_rejects_settlement_even_without_takeover() {
    let (pool, repo) = memory_repo("expiry", &[("root", Normal, None)], &[]).await;
    repo.enqueue(&new_task("expiry", "root")).await.unwrap();
    let claim = repo.claim_many("expiry", 1).await.unwrap().remove(0);
    assert!(repo.claim_many("expiry", 1).await.unwrap().is_empty());
    assert_eq!(get(&repo, &claim).await.lease_until, claim.lease_until);

    sqlx::query(
        "UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'expiry'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(repo.renew_claim(&claim).await.unwrap().is_none());
    for result in [
        repo.release_claim(&claim).await,
        repo.succeed(&claim, &json!("expired")).await.map(|_| ()),
        repo.fail_and_enqueue(&claim, "expired", &[]).await,
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
    let (_, repo) = memory_repo(
        "worker-release",
        &[("first", Normal, None), ("second", Normal, None)],
        &[],
    )
    .await;
    repo.enqueue(&new_task("worker-release", "first"))
        .await
        .unwrap();
    repo.enqueue(&new_task("worker-release", "second"))
        .await
        .unwrap();
    let first = repo
        .claim_many("worker-release", 1)
        .await
        .unwrap()
        .remove(0);
    let second = repo
        .claim_many("worker-release", 1)
        .await
        .unwrap()
        .remove(0);
    repo.release_claim(&first).await.unwrap();
    assert!(matches!(
        repo.release_claim(&first).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(get(&repo, &second).await.lease_until, second.lease_until);
    let reclaimed = repo.claim_many("worker-release", 10).await.unwrap();
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
    let declared = graph(
        "concurrent",
        &[("worker", Normal, None), ("after_reopen", Normal, None)],
        &[],
    );
    let repo_a = SqliteTaskRepo::new(pool_a.clone(), declared.clone());
    let repo_b = SqliteTaskRepo::new(pool_b.clone(), declared.clone());
    for _ in 0..40 {
        repo_a
            .enqueue(&new_task("concurrent", "worker"))
            .await
            .unwrap();
    }

    let (a, b, c, d) = tokio::join!(
        repo_a.claim_many("concurrent", 10),
        repo_b.claim_many("concurrent", 10),
        repo_a.claim_many("concurrent", 10),
        repo_b.claim_many("concurrent", 10),
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
        .succeed(&tasks[0], &json!({"saved": true}))
        .await
        .unwrap();

    let fanin_graph = graph(
        "concurrent_fanin",
        &[("worker", Normal, None), ("collect", Join, None)],
        &[("worker", "collect")],
    );
    let repo_a = SqliteTaskRepo::new(pool_a.clone(), fanin_graph.clone());
    let repo_b = SqliteTaskRepo::new(pool_b.clone(), fanin_graph);
    for n in 1..=4 {
        let mut source = new_task("concurrent_fanin", "worker");
        source.run_id = Some(1);
        source.input = json!(n);
        repo_a.enqueue(&source).await.unwrap();
    }
    let sources = repo_a.claim_many("concurrent_fanin", 4).await.unwrap();
    let results = futures::future::join_all(sources.iter().enumerate().map(|(index, source)| {
        let repo = if index % 2 == 0 { &repo_a } else { &repo_b };
        async move {
            repo.succeed_and_enqueue(
                source,
                &source.input,
                &[plan("collect", vec![json!({"worker": source.input})])],
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
    let repo = SqliteTaskRepo::new(reopened.clone(), declared);
    let rows = repo.list_all("concurrent").await.unwrap();
    assert_eq!(rows.len(), 40);
    let saved = repo.get("concurrent", task_id).await.unwrap().unwrap();
    assert_eq!(saved.status, TaskStatus::Succeeded);
    assert_eq!(saved.output, Some(json!({"saved": true})));
    for running in rows
        .iter()
        .filter(|task| task.status == TaskStatus::Running)
    {
        repo.release_claim(running).await.unwrap();
    }
    assert_eq!(repo.claim_many("concurrent", 40).await.unwrap().len(), 39);
    let next = repo
        .enqueue(&new_task("concurrent", "after_reopen"))
        .await
        .unwrap();
    assert!(next.task_id > merged[0].task_id);
    reopened.close().await;
}
