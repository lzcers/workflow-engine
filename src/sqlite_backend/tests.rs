use std::{collections::HashSet, path::PathBuf, time::Duration};

use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use super::{NewTask, SqliteError, SqliteTask, SqliteTaskRepo, SqliteTaskStore, migrate, status};
use crate::{FanoutPlan, Task, TaskError, TaskStore};

async fn memory_repo() -> (SqlitePool, SqliteTaskRepo) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    let repo = SqliteTaskRepo::new(pool.clone());
    (pool, repo)
}

fn new_task(workflow_id: &str, name: &str) -> NewTask {
    NewTask {
        workflow_id: workflow_id.to_owned(),
        run_id: None,
        name: name.to_owned(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({}),
    }
}

async fn get(repo: &SqliteTaskRepo, task: &SqliteTask) -> SqliteTask {
    repo.get(&task.workflow_id, task.task_id)
        .await
        .unwrap()
        .unwrap()
}

fn plan(dst: &'static str, task_type: &'static str, inputs: Vec<Value>) -> FanoutPlan {
    FanoutPlan {
        dst,
        task_type,
        inputs,
        depends_on_names: vec![],
    }
}

#[tokio::test]
async fn migration_is_idempotent_and_task_values_round_trip() {
    let (pool, repo) = memory_repo().await;
    let mut input = new_task("workflow", "root");
    input.run_id = Some(42);
    input.input = json!({"text": "工作流 'quoted'", "nested": [null, true, {"n": 3}]});
    let first = repo.enqueue(&input).await.unwrap();

    migrate(&pool).await.unwrap();
    let second = repo.enqueue(&new_task("workflow", "second")).await.unwrap();
    let other = repo.enqueue(&new_task("other", "root")).await.unwrap();
    assert_eq!(first.task_id, 1);
    assert_eq!(second.task_id, 2);
    assert_eq!(other.task_id, 1, "task IDs are scoped to each workflow");
    assert_eq!(first.task_id(), first.task_id);
    assert_eq!(first.name(), "root");
    assert_eq!(first.input(), &input.input);
    assert_eq!(first.status, status::PENDING);
    assert_eq!(first.task_type, "normal");
    assert_eq!(first.attempt, 0);
    assert!(first.lease_until.is_none());
    assert!(first.created_at <= first.updated_at);

    let rows = repo.list_all("workflow").await.unwrap();
    assert_eq!(rows.iter().map(|t| t.task_id).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(rows[0].input, input.input);
    assert_eq!(rows[0].run_id, Some(42));
    assert!(repo.get("missing", 1).await.unwrap().is_none());

    let output = json!([null, {"result": "完成"}]);
    let settled = repo
        .succeed("workflow", first.task_id, &output)
        .await
        .unwrap();
    assert_eq!(settled.status, status::SUCCEEDED);
    assert_eq!(get(&repo, &first).await.output, Some(output));

    let mut tx = pool.begin().await.unwrap();
    let rolled_back = SqliteTaskRepo::insert_task(&mut tx, &new_task("workflow", "rollback"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        repo.get("workflow", rolled_back.task_id)
            .await
            .unwrap()
            .is_none()
    );
    let next = repo.enqueue(&new_task("workflow", "next")).await.unwrap();
    assert_eq!(
        next.task_id, rolled_back.task_id,
        "allocation rolls back with insertion"
    );
}

#[tokio::test]
async fn claims_require_all_dependencies_including_missing_ids() {
    let (_, repo) = memory_repo().await;
    let root = repo.enqueue(&new_task("deps", "root")).await.unwrap();
    let mut child = new_task("deps", "child");
    child.depends_on_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let mut missing = new_task("deps", "missing_dependency");
    missing.depends_on_ids = vec![999];
    let missing = repo.enqueue(&missing).await.unwrap();

    assert!(
        repo.claim_many("deps", "worker", 0)
            .await
            .unwrap()
            .is_empty()
    );
    let claimed = repo.claim_many("deps", "worker-a", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, root.task_id);
    assert_eq!(claimed[0].claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(claimed[0].attempt, 1);
    assert!(claimed[0].lease_until.is_some());
    assert!(
        repo.claim_many("deps", "worker-b", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());

    repo.succeed("deps", root.task_id, &json!(null))
        .await
        .unwrap();
    assert_eq!(repo.executable_workflow_ids().await.unwrap(), ["deps"]);
    let claimed = repo.claim_many("deps", "worker-b", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, child.task_id);
    assert_eq!(get(&repo, &missing).await.status, status::PENDING);
}

#[tokio::test]
async fn released_and_expired_leases_retry_until_the_budget_is_exhausted() {
    let (pool, repo) = memory_repo().await;
    let task = repo.enqueue(&new_task("retry", "root")).await.unwrap();
    let first = repo.claim_many("retry", "first", 1).await.unwrap();
    assert_eq!(first[0].attempt, 1);
    repo.release_claim("retry", task.task_id).await.unwrap();
    let second = repo.claim_many("retry", "second", 1).await.unwrap();
    assert_eq!(second[0].attempt, 2);

    sqlx::query("UPDATE tasks SET lease_until = ? WHERE workflow_id = ? AND task_id = ?")
        .bind(chrono::Utc::now() - chrono::Duration::seconds(10))
        .bind("retry")
        .bind(task.task_id)
        .execute(&pool)
        .await
        .unwrap();
    let third = repo.claim_many("retry", "third", 1).await.unwrap();
    assert_eq!(third[0].attempt, 3);
    assert_eq!(
        repo.seal_budget_exhausted().await.unwrap(),
        0,
        "an active final attempt is protected"
    );
    assert!(
        repo.claim_many("retry", "fourth", 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(get(&repo, &task).await.status, status::RUNNING);

    repo.release_claim("retry", task.task_id).await.unwrap();
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());
    assert_eq!(repo.seal_budget_exhausted().await.unwrap(), 1);
    let exhausted = get(&repo, &task).await;
    assert_eq!(exhausted.status, status::FAILED);
    assert_eq!(exhausted.attempt, 3);
    assert!(exhausted.lease_until.is_none());
    assert!(exhausted.error.unwrap().contains("budget exhausted"));

    let mut zero = new_task("zero", "root");
    zero.max_attempts = 0;
    let zero = repo.enqueue(&zero).await.unwrap();
    assert!(
        repo.claim_many("zero", "worker", 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(get(&repo, &zero).await.status, status::FAILED);
}

#[tokio::test]
async fn join_waits_for_every_named_predecessor_and_all_its_instances() {
    let (_, repo) = memory_repo().await;
    let images = repo
        .enqueue_fanout(
            "join",
            Some(1),
            "image",
            &[(Some(0), json!("a")), (Some(1), json!("b"))],
            &[],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
    let join = repo
        .enqueue_fanout(
            "join",
            Some(1),
            "collect",
            &[(None, json!({}))],
            &[],
            "join",
            &["image".into(), "text".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let mut wrong_run = new_task("join", "text");
    wrong_run.run_id = Some(2);
    let wrong_run = repo.enqueue(&wrong_run).await.unwrap();
    repo.succeed("join", wrong_run.task_id, &json!({}))
        .await
        .unwrap();
    let claimed = repo.claim_many("join", "worker", 10).await.unwrap();
    assert_eq!(
        claimed.len(),
        2,
        "the JOIN is blocked before predecessors finish"
    );
    assert!(claimed.iter().all(|t| t.name == "image"));

    repo.succeed("join", images[0].task_id, &json!({}))
        .await
        .unwrap();
    assert!(
        repo.claim_many("join", "worker", 10)
            .await
            .unwrap()
            .is_empty()
    );
    repo.succeed("join", images[1].task_id, &json!({}))
        .await
        .unwrap();
    assert!(
        repo.claim_many("join", "worker", 10)
            .await
            .unwrap()
            .is_empty(),
        "another run cannot satisfy a missing predecessor"
    );

    let mut text = new_task("join", "text");
    text.run_id = Some(1);
    let text = repo.enqueue(&text).await.unwrap();
    repo.succeed("join", text.task_id, &json!({}))
        .await
        .unwrap();
    let claimed = repo.claim_many("join", "worker", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, join.task_id);
}

#[tokio::test]
async fn fanin_accumulates_join_inputs_and_isolates_null_and_numbered_runs() {
    let (_, repo) = memory_repo().await;
    let mut ids = vec![];
    for run_id in [Some(1), None, Some(2)] {
        let first = repo
            .enqueue_fanout(
                "fanin",
                run_id,
                "collect",
                &[(None, json!({"image": "first"}))],
                &[11],
                "join",
                &["image".into()],
                3,
            )
            .await
            .unwrap()
            .remove(0);
        let merged = repo
            .enqueue_fanout(
                "fanin",
                run_id,
                "collect",
                &[(None, json!({"image": ["second", "third"]}))],
                &[11, 12],
                "join",
                &["image".into()],
                3,
            )
            .await
            .unwrap()
            .remove(0);
        assert_eq!(merged.task_id, first.task_id);
        assert_eq!(merged.run_id, run_id);
        assert_eq!(merged.depends_on_ids, [11, 12]);
        assert_eq!(merged.input, json!({"image": ["first", "second", "third"]}));
        ids.push(merged.task_id);
    }
    assert_eq!(ids.into_iter().collect::<HashSet<_>>().len(), 3);
    assert_eq!(repo.list_all("fanin").await.unwrap().len(), 3);

    let first = repo
        .enqueue_fanout(
            "fanin",
            None,
            "normal",
            &[(None, json!({"first": true}))],
            &[1],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let merged = repo
        .enqueue_fanout(
            "fanin",
            None,
            "normal",
            &[(None, json!({"second": true}))],
            &[2, 1],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    assert_eq!(merged.task_id, first.task_id);
    assert_eq!(
        merged.input, first.input,
        "normal nodes keep their typed initial input"
    );
    assert_eq!(merged.depends_on_ids, [1, 2]);
}

#[tokio::test]
async fn map_enqueue_is_idempotent_even_after_settlement_and_with_null_run_ids() {
    let (_, repo) = memory_repo().await;
    let instances = [
        (Some(0), json!({"value": 0})),
        (Some(1), json!({"value": 1})),
    ];
    for run_id in [None, Some(-1), Some(0), Some(1)] {
        let inserted = repo
            .enqueue_fanout("map", run_id, "image", &instances, &[], "normal", &[], 3)
            .await
            .unwrap();
        assert_eq!(inserted.len(), 2);
        repo.succeed("map", inserted[0].task_id, &json!({}))
            .await
            .unwrap();
        let duplicate = repo
            .enqueue_fanout("map", run_id, "image", &instances, &[], "normal", &[], 3)
            .await
            .unwrap();
        assert!(duplicate.is_empty());
        assert_eq!(get(&repo, &inserted[0]).await.status, status::SUCCEEDED);
    }
    assert_eq!(repo.list_all("map").await.unwrap().len(), 8);
}

#[tokio::test]
async fn success_and_all_fanouts_commit_or_roll_back_together() {
    let (pool, repo) = memory_repo().await;
    let task = repo
        .enqueue(&new_task("atomic_success", "root"))
        .await
        .unwrap();
    let store = SqliteTaskStore::new(repo.clone(), "atomic_success".into()).with_max_attempts(7);
    let claimed = store.claim_many(1).await.unwrap().remove(0);
    let valid = plan("child", "normal", vec![json!("a"), json!("b")]);
    let invalid = plan("invalid", "unsupported", vec![json!({})]);
    assert!(
        store
            .succeed_and_enqueue(&claimed, &json!({"done": true}), &[valid.clone(), invalid])
            .await
            .is_err()
    );
    let unchanged = get(&repo, &task).await;
    assert_eq!(unchanged.status, status::RUNNING);
    assert!(unchanged.output.is_none());
    assert_eq!(repo.list_all("atomic_success").await.unwrap().len(), 1);

    // A node can attach its run ID during execution; successors must use the latest row.
    sqlx::query("UPDATE tasks SET run_id = 9 WHERE workflow_id = 'atomic_success'")
        .execute(&pool)
        .await
        .unwrap();
    let settled = store
        .succeed_and_enqueue(&claimed, &json!({"done": true}), &[valid])
        .await
        .unwrap();
    assert_eq!(settled.run_id, Some(9));
    assert_eq!(settled.status, status::SUCCEEDED);
    assert!(settled.lease_until.is_none());
    let children = store.claim_many(10).await.unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(
        children
            .iter()
            .map(|t| t.instance_id)
            .collect::<HashSet<_>>(),
        HashSet::from([Some(0), Some(1)])
    );
    for child in children {
        assert_eq!(child.run_id, Some(9));
        assert_eq!(child.depends_on_ids, [task.task_id]);
        assert_eq!(child.max_attempts, 7);
    }
}

#[tokio::test]
async fn failure_and_recovery_fanouts_are_atomic_and_do_not_depend_on_failed_parent() {
    let (_, repo) = memory_repo().await;
    let mut input = new_task("atomic_failure", "root");
    input.run_id = Some(4);
    let task = repo.enqueue(&input).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone(), "atomic_failure".into());
    let claimed = store.claim_many(1).await.unwrap().remove(0);
    let recovery = plan("recovery", "normal", vec![json!({"retry": true})]);
    let invalid = plan("invalid", "unsupported", vec![json!({})]);
    let error = TaskError::new("execution failed");
    assert!(
        store
            .fail_and_enqueue(&claimed, &error, true, &[recovery.clone(), invalid])
            .await
            .is_err()
    );
    assert_eq!(get(&repo, &task).await.status, status::RUNNING);
    assert_eq!(repo.list_all("atomic_failure").await.unwrap().len(), 1);

    store.release_claim(&claimed).await.unwrap();
    let reclaimed = store.claim_many(1).await.unwrap().remove(0);
    assert_eq!(reclaimed.attempt, 2);
    store
        .fail_and_enqueue(&reclaimed, &error, true, &[recovery])
        .await
        .unwrap();
    let settled = get(&repo, &task).await;
    assert_eq!(settled.status, status::SUPERSEDED);
    assert_eq!(settled.error.as_deref(), Some("execution failed"));
    assert!(settled.lease_until.is_none());
    let recovery = store.claim_many(10).await.unwrap().remove(0);
    assert_eq!(recovery.name, "recovery");
    assert_eq!(recovery.instance_id, None);
    assert_eq!(recovery.run_id, Some(4));
    assert!(recovery.depends_on_ids.is_empty());
    store
        .fail_and_enqueue(&recovery, &error, false, &[])
        .await
        .unwrap();
    assert_eq!(get(&repo, &recovery).await.status, status::FAILED);
}

#[tokio::test]
async fn failure_fanout_to_the_same_name_creates_a_fresh_task() {
    let (_, repo) = memory_repo().await;
    repo.enqueue(&new_task("same_name", "retry")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone(), "same_name".into());
    let source = store.claim_many(1).await.unwrap().remove(0);
    store
        .fail_and_enqueue(
            &source,
            &TaskError::new("retry requested"),
            true,
            &[plan("retry", "normal", vec![json!({"retry": true})])],
        )
        .await
        .unwrap();
    assert_eq!(get(&repo, &source).await.status, status::SUPERSEDED);
    let successors = store.claim_many(1).await.unwrap();
    assert_eq!(successors.len(), 1);
    assert_ne!(successors[0].task_id, source.task_id);
    assert_eq!(successors[0].input, json!({"retry": true}));
    assert!(successors[0].depends_on_ids.is_empty());
}

#[tokio::test]
async fn release_all_claims_and_store_operations_stay_in_their_workflow() {
    let (_, repo) = memory_repo().await;
    let ours = repo.enqueue(&new_task("ours", "root")).await.unwrap();
    let theirs = repo.enqueue(&new_task("theirs", "root")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone(), "ours".into());
    assert_eq!(store.claim_many(10).await.unwrap().len(), 1);
    assert_eq!(get(&repo, &theirs).await.status, status::PENDING);
    repo.claim_many("theirs", "other-worker", 1).await.unwrap();
    assert_eq!(repo.release_claims("ours").await.unwrap(), 1);
    assert_eq!(store.claim_many(10).await.unwrap()[0].attempt, 2);
    assert!(
        repo.claim_many("theirs", "third-worker", 1)
            .await
            .unwrap()
            .is_empty()
    );

    store.succeed(&ours, &json!("ours")).await.unwrap();
    assert_eq!(get(&repo, &ours).await.status, status::SUCCEEDED);
    assert_eq!(get(&repo, &theirs).await.status, status::RUNNING);
    assert_eq!(get(&repo, &theirs).await.output, None);
}

#[tokio::test]
async fn failed_predecessors_cascade_and_late_joins_are_failed_immediately() {
    let (_, repo) = memory_repo().await;
    let root = repo.enqueue(&new_task("cascade", "root")).await.unwrap();
    let mut child = new_task("cascade", "child");
    child.depends_on_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let join = repo
        .enqueue_fanout(
            "cascade",
            None,
            "join",
            &[(None, json!({}))],
            &[],
            "join",
            &["root".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let other_run = repo
        .enqueue_fanout(
            "cascade",
            Some(0),
            "join",
            &[(None, json!({}))],
            &[],
            "join",
            &["root".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let unrelated = repo.enqueue(&new_task("other", "root")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone(), "cascade".into());
    store
        .fail(&root, &TaskError::new("permanent failure"), false)
        .await
        .unwrap();
    assert_eq!(store.cascade_failed_predecessor(&root).await.unwrap(), 2);
    assert_eq!(get(&repo, &child).await.status, status::FAILED);
    assert_eq!(get(&repo, &join).await.status, status::FAILED);
    assert_eq!(
        get(&repo, &other_run).await.status,
        status::PENDING,
        "name-based cascading stays in its run"
    );
    assert_eq!(get(&repo, &unrelated).await.status, status::PENDING);

    let late = repo
        .enqueue_fanout(
            "cascade",
            None,
            "late_join",
            &[(None, json!({}))],
            &[],
            "join",
            &["root".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    assert_eq!(late.status, status::FAILED);
    assert!(
        repo.claim_many("cascade", "worker", 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn settling_a_missing_task_returns_a_typed_error() {
    let (_, repo) = memory_repo().await;
    assert!(matches!(
        repo.succeed("missing", 123, &json!({})).await,
        Err(SqliteError::MissingTask { workflow_id, task_id: 123 }) if workflow_id == "missing"
    ));
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
    let repo_a = SqliteTaskRepo::new(pool_a.clone());
    let repo_b = SqliteTaskRepo::new(pool_b.clone());
    for i in 0..40 {
        repo_a
            .enqueue(&new_task("concurrent", &format!("task-{i}")))
            .await
            .unwrap();
    }

    let (a, b, c, d) = tokio::join!(
        repo_a.claim_many("concurrent", "a", 10),
        repo_b.claim_many("concurrent", "b", 10),
        repo_a.claim_many("concurrent", "c", 10),
        repo_b.claim_many("concurrent", "d", 10),
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
            .all(|t| t.attempt == 1 && t.status == status::RUNNING)
    );
    let task_id = tasks[0].task_id;
    repo_a
        .succeed("concurrent", task_id, &json!({"saved": true}))
        .await
        .unwrap();

    let contributions = futures::future::join_all(
        [(&repo_a, 1), (&repo_b, 2), (&repo_a, 3), (&repo_b, 4)]
            .into_iter()
            .map(|(repo, n)| async move {
                repo.enqueue_fanout(
                    "concurrent_fanin",
                    None,
                    "collect",
                    &[(None, json!({"worker": n}))],
                    &[n],
                    "join",
                    &["worker".into()],
                    3,
                )
                .await
                .unwrap()
            }),
    )
    .await;
    let merged_ids: HashSet<_> = contributions.iter().map(|rows| rows[0].task_id).collect();
    assert_eq!(
        merged_ids.len(),
        1,
        "concurrent fan-in shares a single destination"
    );
    let merged = repo_a.list_all("concurrent_fanin").await.unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0]
            .depends_on_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>(),
        HashSet::from([1, 2, 3, 4])
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
    assert_eq!(saved.status, status::SUCCEEDED);
    assert_eq!(saved.output, Some(json!({"saved": true})));
    assert_eq!(repo.release_claims("concurrent").await.unwrap(), 39);
    assert_eq!(
        repo.claim_many("concurrent", "reopened", 40)
            .await
            .unwrap()
            .len(),
        39
    );
    let next = repo
        .enqueue(&new_task("concurrent", "after_reopen"))
        .await
        .unwrap();
    assert_eq!(next.task_id, 41, "the workflow counter survives reopen");
    reopened.close().await;
}
