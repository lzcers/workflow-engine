use std::{collections::HashSet, path::PathBuf, time::Duration};

use serde_json::{Value, json};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use super::{SqliteTaskRepo, SqliteTaskStore, migrate};
use crate::error::StorageError;
use crate::task::{FanoutPlan, TaskStore};
use crate::{Task, TaskRecord, TaskStatus};

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

fn new_task(workflow_id: &str, name: &str) -> Task {
    Task::new(workflow_id, name, json!({}))
}

async fn get(repo: &SqliteTaskRepo, task: &TaskRecord) -> TaskRecord {
    repo.get(&task.workflow_id, task.task_id)
        .await
        .unwrap()
        .unwrap()
}

fn plan(dst: &'static str, task_type: &'static str, inputs: Vec<Value>) -> FanoutPlan {
    FanoutPlan {
        creates_scope: false,
        dst,
        task_type,
        inputs,
        depends_on_names: vec![],
    }
}

#[tokio::test]
async fn migration_is_idempotent_and_task_values_round_trip() {
    let (pool, repo) = memory_repo().await;
    let store = SqliteTaskStore::new(repo.clone());
    let mut input = new_task("workflow", "root");
    input.run_id = Some(42);
    input.input = json!({"text": "工作流 'quoted'", "nested": [null, true, {"n": 3}]});
    let first = store.enqueue(&input).await.unwrap();

    migrate(&pool).await.unwrap();
    let second = store
        .enqueue(&new_task("workflow", "second"))
        .await
        .unwrap();
    let other = repo.enqueue(&new_task("other", "root")).await.unwrap();
    assert_eq!(first.task_id, 1);
    assert_eq!(second.task_id, 2);
    assert_eq!(second.run_id, Some(second.task_id));
    assert_eq!(other.task_id, 1, "task IDs are scoped to each workflow");
    assert_eq!(other.run_id, Some(other.task_id));
    assert_eq!(first.name, "root");
    assert!(first.parent_task_ids.is_empty());
    assert_eq!(first.input, input.input);
    assert_eq!(first.status, TaskStatus::Pending);
    assert_eq!(first.task_type, "normal");
    assert_eq!(first.attempt, 0);
    assert!(first.lease_until.is_none());
    assert!(first.created_at <= first.updated_at);

    let rows = repo.list_all("workflow").await.unwrap();
    assert_eq!(rows.iter().map(|t| t.task_id).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(rows[0].input, input.input);
    assert_eq!(rows[0].run_id, Some(42));
    assert_eq!(rows[1].run_id, second.run_id);
    assert!(repo.get("missing", 1).await.unwrap().is_none());

    let output = json!([null, {"result": "完成"}]);
    let claimed = repo
        .claim_many("workflow", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    let settled = repo.succeed(&claimed, &output).await.unwrap();
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(get(&repo, &first).await.output, Some(output));

    let mut tx = pool.begin().await.unwrap();
    let rolled_back = SqliteTaskRepo::insert_task(&mut tx, &new_task("workflow", "rollback"))
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
    assert_eq!(
        next.task_id, rolled_back.task_id,
        "allocation rolls back with insertion"
    );
    assert_eq!(next.run_id, Some(next.task_id));
    assert_ne!(next.run_id, second.run_id);
}

#[tokio::test]
async fn join_claims_require_all_named_predecessors_including_missing_tasks() {
    let (_, repo) = memory_repo().await;
    let root = repo.enqueue(&new_task("deps", "root")).await.unwrap();
    let mut child = new_task("deps", "child");
    child.run_id = root.run_id;
    child.max_attempts = 5;
    child.task_type = "join".into();
    child.parent_task_ids = vec![root.task_id];
    child.depends_on_names = vec!["root".into()];
    let child = repo.enqueue(&child).await.unwrap();
    assert_eq!(child.run_id, root.run_id);
    assert_eq!(child.max_attempts, 5);
    assert_eq!(child.parent_task_ids, [root.task_id]);
    assert_eq!(child.depends_on_names, ["root"]);
    let mut missing = new_task("deps", "missing_dependency");
    missing.run_id = root.run_id;
    missing.task_type = "join".into();
    missing.depends_on_names = vec!["absent".into()];
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

    repo.succeed(&claimed[0], &json!(null)).await.unwrap();
    assert_eq!(repo.executable_workflow_ids().await.unwrap(), ["deps"]);
    let claimed = repo.claim_many("deps", "worker-b", 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].task_id, child.task_id);
    assert_eq!(get(&repo, &missing).await.status, TaskStatus::Pending);
}

#[tokio::test]
async fn scoped_join_inputs_claims_and_failures_are_isolated() {
    for (task_type, creates_scope) in [("scope", false), ("normal", true)] {
        let (_, repo) = memory_repo().await;
        let mut root = new_task("scopes", "entry");
        root.task_type = "scope".into();
        root.scope_id = Some(999);
        let root = repo.enqueue(&root).await.unwrap();
        assert_eq!(root.scope_id, Some(root.task_id));
        let root = repo
            .claim_many("scopes", "worker", 1)
            .await
            .unwrap()
            .remove(0);
        repo.succeed_and_enqueue(
            &root,
            &json!(null),
            &[FanoutPlan {
                creates_scope,
                ..plan(
                    "instrument",
                    task_type,
                    vec![json!("A"), json!("B"), json!("C")],
                )
            }],
            3,
        )
        .await
        .unwrap();
        let scopes = repo.claim_many("scopes", "worker", 10).await.unwrap();
        assert_eq!(scopes.len(), 3);
        let join_plan = |input| FanoutPlan {
            creates_scope: false,
            dst: "collect",
            task_type: "join",
            inputs: vec![input],
            depends_on_names: vec!["instrument".into(), "mfi".into()],
        };
        for scope in &scopes {
            assert_eq!(scope.run_id, root.run_id);
            assert_eq!(scope.scope_id, Some(scope.task_id));
            assert_ne!(scope.scope_id, root.scope_id);
            repo.succeed_and_enqueue(
                scope,
                &scope.input,
                &[
                    plan("mfi", "normal", vec![scope.input.clone()]),
                    join_plan(json!({"bars": scope.input})),
                ],
                3,
            )
            .await
            .unwrap();
        }
        let joins: Vec<_> = repo
            .list_all("scopes")
            .await
            .unwrap()
            .into_iter()
            .filter(|task| task.name == "collect")
            .collect();
        assert_eq!(joins.len(), 3);
        for (join, scope) in joins.iter().zip(&scopes) {
            assert_eq!(join.scope_id, scope.scope_id);
            assert_eq!(join.input, json!({"bars": scope.input}));
            assert_eq!(join.parent_task_ids, [scope.task_id]);
        }

        let mut signals = repo.claim_many("scopes", "worker", 10).await.unwrap();
        assert_eq!(signals.len(), 3);
        assert!(signals.iter().all(|task| task.name == "mfi"));
        let previous = signals[0].clone();
        repo.release_claim(&previous).await.unwrap();
        signals[0] = repo
            .claim_many("scopes", "restarted", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(signals[0].task_id, previous.task_id);
        assert_eq!(signals[0].scope_id, previous.scope_id);
        assert_eq!(signals[0].attempt, previous.attempt + 1);
        repo.succeed_and_enqueue(
            &signals[0],
            &signals[0].input,
            &[join_plan(json!({"signals": signals[0].input}))],
            3,
        )
        .await
        .unwrap();
        repo.fail_and_enqueue(&signals[2], "C failed", &[], 3)
            .await
            .unwrap();
        assert_eq!(get(&repo, &joins[2]).await.status, TaskStatus::Failed);
        assert_eq!(get(&repo, &joins[1]).await.status, TaskStatus::Pending);

        // A is complete and claimable while B remains in flight and C has failed.
        let ready = repo.claim_many("scopes", "worker", 10).await.unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, joins[0].task_id);
        assert_eq!(ready[0].input, json!({"bars": "A", "signals": "A"}));
        assert_eq!(
            ready[0].parent_task_ids,
            [scopes[0].task_id, signals[0].task_id]
        );
        repo.succeed_and_enqueue(
            &ready[0],
            &ready[0].input,
            &[plan("report", "normal", vec![ready[0].input.clone()])],
            3,
        )
        .await
        .unwrap();
        let report = repo
            .claim_many("scopes", "worker", 10)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(report.name, "report");
        assert_eq!(report.scope_id, scopes[0].scope_id);
        repo.succeed(&report, &json!("<html>A</html>"))
            .await
            .unwrap();

        // A failed task only poisons joins created later in its own scope.
        for index in [1, 2] {
            let late = repo
                .enqueue_fanout(
                    "scopes",
                    root.run_id,
                    scopes[index].scope_id,
                    &[],
                    "late_join",
                    &[json!({})],
                    "join",
                    false,
                    &["mfi".into()],
                    3,
                )
                .await
                .unwrap()
                .remove(0);
            assert_eq!(
                late.status,
                if index == 2 {
                    TaskStatus::Failed
                } else {
                    TaskStatus::Pending
                }
            );
        }
        assert!(repo.executable_workflow_ids().await.unwrap().is_empty());
        repo.succeed_and_enqueue(
            &signals[1],
            &signals[1].input,
            &[join_plan(json!({"signals": signals[1].input}))],
            3,
        )
        .await
        .unwrap();
        let ready = repo.claim_many("scopes", "worker", 10).await.unwrap();
        assert_eq!(ready.len(), 2);
        assert!(ready.iter().all(|task| task.scope_id == scopes[1].scope_id));
        assert_eq!(
            get(&repo, &joins[1]).await.input,
            json!({"bars": "B", "signals": "B"})
        );
    }
}

#[tokio::test]
async fn exhausted_tasks_are_claimed_for_fenced_final_failure_settlement() {
    let (pool, repo) = memory_repo().await;
    let task = repo.enqueue(&new_task("retry", "root")).await.unwrap();
    let mut child = new_task("retry", "child");
    child.run_id = task.run_id;
    child.task_type = "join".into();
    child.depends_on_names = vec!["root".into()];
    child.parent_task_ids = vec![task.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let first = repo.claim_many("retry", "first", 1).await.unwrap();
    assert_eq!(first[0].attempt, 1);
    repo.release_claim(&first[0]).await.unwrap();
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
    assert!(
        repo.claim_many("retry", "fourth", 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(get(&repo, &task).await.status, TaskStatus::Running);

    sqlx::query(
        "UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'retry'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(repo.executable_workflow_ids().await.unwrap(), ["retry"]);
    let recovery = repo
        .claim_many("retry", "recovery", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(recovery.attempt, 4);
    assert_eq!(recovery.status, TaskStatus::Running);
    assert_eq!(
        recovery.error.as_deref(),
        Some("task lease expired; retry budget exhausted")
    );
    let mut forged_budget = recovery.clone();
    forged_budget.max_attempts = 10;
    for result in [
        repo.succeed(&forged_budget, &json!("too late"))
            .await
            .map(|_| ()),
        repo.skip(&recovery, "skip final failure").await,
        repo.fail_and_enqueue(&third[0], "old worker", &[], 3).await,
    ] {
        assert!(matches!(result, Err(StorageError::ClaimLost { .. })));
    }

    // Recovery can itself be interrupted; it must retain the original failure reason.
    repo.release_claim(&recovery).await.unwrap();
    let next_recovery = repo
        .claim_many("retry", "next-recovery", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(next_recovery.attempt, 5);
    assert_eq!(next_recovery.error, recovery.error);
    assert!(matches!(
        repo.fail_and_enqueue(&recovery, "stale recovery", &[], 3)
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.fail_and_enqueue(
        &next_recovery,
        next_recovery.error.as_deref().unwrap(),
        &[],
        3,
    )
    .await
    .unwrap();
    let exhausted = get(&repo, &task).await;
    assert_eq!(exhausted.status, TaskStatus::Failed);
    assert_eq!(exhausted.attempt, 5);
    assert!(exhausted.lease_until.is_none());
    assert_eq!(exhausted.error, recovery.error);
    assert_eq!(get(&repo, &child).await.status, TaskStatus::Failed);
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());

    let mut zero = new_task("zero", "root");
    zero.max_attempts = 0;
    let zero = repo.enqueue(&zero).await.unwrap();
    let recovery = repo
        .claim_many("zero", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(recovery.attempt, 1);
    assert_eq!(recovery.max_attempts, 0);
    assert_eq!(
        recovery.error.as_deref(),
        Some("task retry budget exhausted")
    );
    repo.fail_and_enqueue(&recovery, recovery.error.as_deref().unwrap(), &[], 3)
        .await
        .unwrap();
    assert_eq!(get(&repo, &zero).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn join_merges_across_parents_and_waits_for_all_predecessors() {
    let (_, repo) = memory_repo().await;
    let images = repo
        .enqueue_fanout(
            "join",
            Some(1),
            None,
            &[],
            "image",
            &[json!("a"), json!("b")],
            "normal",
            false,
            &[],
            3,
        )
        .await
        .unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    let mut wrong_run = new_task("join", "text");
    wrong_run.run_id = Some(2);
    let wrong_run = repo.enqueue(&wrong_run).await.unwrap();
    let mut claimed = repo.claim_many("join", "worker", 10).await.unwrap();
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

    let contribution = |input| FanoutPlan {
        creates_scope: false,
        dst: "collect",
        task_type: "join",
        inputs: vec![input],
        depends_on_names: vec!["image".into(), "text".into()],
    };
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
    assert!(
        repo.claim_many("join", "worker", 10)
            .await
            .unwrap()
            .is_empty()
    );
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
        repo.claim_many("join", "worker", 10)
            .await
            .unwrap()
            .is_empty(),
        "another run cannot satisfy a missing predecessor"
    );

    let mut text = new_task("join", "text");
    text.run_id = Some(1);
    let text = repo.enqueue(&text).await.unwrap();
    let text_claim = repo
        .claim_many("join", "worker", 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(text_claim.task_id, text.task_id);
    store
        .settle_success(
            &text_claim,
            &json!("text"),
            &[contribution(json!({"text": "text"}))],
        )
        .await
        .unwrap();
    let claimed = repo.claim_many("join", "worker", 10).await.unwrap();
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
async fn fanin_accumulates_join_inputs_and_isolates_null_and_numbered_runs() {
    let (_, repo) = memory_repo().await;
    let mut ids = vec![];
    for run_id in [Some(1), Some(2), None, Some(0)] {
        let first = repo
            .enqueue_fanout(
                "fanin",
                run_id,
                None,
                &[11],
                "collect",
                &[json!({"image": "first"})],
                "join",
                false,
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
                None,
                &[11, 12],
                "collect",
                &[json!({"image": "second"}), json!({"image": ["third"]})],
                "join",
                false,
                &["image".into()],
                3,
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(merged.task_id, first.task_id);
        assert_eq!(merged.run_id, run_id);
        assert_eq!(merged.parent_task_ids, [11, 12]);
        assert_eq!(merged.input, json!({"image": ["first", "second", "third"]}));
        ids.push(merged.task_id);
    }
    assert_eq!(ids.into_iter().collect::<HashSet<_>>().len(), 4);
    assert_eq!(repo.list_all("fanin").await.unwrap().len(), 4);

    let first = repo
        .enqueue_fanout(
            "fanin",
            None,
            None,
            &[1],
            "normal",
            &[json!({"first": true})],
            "normal",
            false,
            &[],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let second = repo
        .enqueue_fanout(
            "fanin",
            None,
            None,
            &[2, 1],
            "normal",
            &[json!({"second": true})],
            "normal",
            false,
            &[],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    assert_ne!(second.task_id, first.task_id);
    assert_eq!(first.input, json!({"first": true}));
    assert_eq!(second.input, json!({"second": true}));
    assert_eq!(first.parent_task_ids, [1]);
    assert_eq!(second.parent_task_ids, [2, 1]);
}

#[tokio::test]
async fn foreach_fanout_handles_empty_single_and_multiple_inputs() {
    let (_, repo) = memory_repo().await;
    for creates_scope in [false, true] {
        for size in [0, 1, 3] {
            let inputs: Vec<_> = (0..size).map(|item| json!(item)).collect();
            let tasks = repo
                .enqueue_fanout(
                    "foreach",
                    Some(42),
                    Some(7),
                    &[1],
                    "worker",
                    &inputs,
                    "normal",
                    creates_scope,
                    &[],
                    3,
                )
                .await
                .unwrap();
            assert_eq!(tasks.len(), inputs.len());
            for (task, input) in tasks.iter().zip(&inputs) {
                assert_eq!(task.input, *input);
                assert_eq!(task.run_id, Some(42));
                assert_eq!(task.parent_task_ids, [1]);
                assert_eq!(task.task_type, "normal");
                assert_eq!(
                    task.scope_id,
                    Some(if creates_scope { task.task_id } else { 7 })
                );
            }
        }
    }
    assert_eq!(repo.list_all("foreach").await.unwrap().len(), 8);
    for inputs in [vec![], vec![json!({})]] {
        assert!(matches!(
            repo.enqueue_fanout(
                "foreach",
                Some(42),
                Some(7),
                &[1],
                "collect",
                &inputs,
                "join",
                true,
                &[],
                3,
            )
            .await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }
    assert_eq!(repo.list_all("foreach").await.unwrap().len(), 8);
}

#[tokio::test]
async fn success_and_all_fanouts_commit_or_roll_back_together() {
    let (pool, repo) = memory_repo().await;
    let task = repo
        .enqueue(&new_task("atomic_success", "root"))
        .await
        .unwrap();
    let store = SqliteTaskStore::new(repo.clone()).with_max_attempts(7);
    let claimed = store
        .claim_many("atomic_success", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    let valid = plan("child", "normal", vec![json!("a"), json!("b")]);
    let scoped = FanoutPlan {
        creates_scope: true,
        ..valid.clone()
    };
    let invalid = plan("invalid", "unsupported", vec![json!({})]);
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

    // Successors inherit the latest run and scope, not the claim's older snapshot.
    sqlx::query("UPDATE tasks SET run_id = 9, scope_id = 19 WHERE workflow_id = 'atomic_success'")
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
    assert_eq!(settled.scope_id, Some(19));
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert!(settled.lease_until.is_none());
    let children = store
        .claim_many("atomic_success", "worker", 10)
        .await
        .unwrap();
    assert_eq!(children.len(), 4);
    assert_ne!(children[0].task_id, children[1].task_id);
    assert_eq!(
        children.iter().map(|task| &task.input).collect::<Vec<_>>(),
        [&json!("a"), &json!("b"), &json!("a"), &json!("b")]
    );
    for (index, child) in children.into_iter().enumerate() {
        assert_eq!(child.run_id, Some(9));
        assert_eq!(
            child.scope_id,
            Some(if index < 2 { 19 } else { child.task_id })
        );
        assert_eq!(child.parent_task_ids, [task.task_id]);
        assert_eq!(child.max_attempts, 7);
    }
}

#[tokio::test]
async fn failure_and_recovery_fanouts_are_atomic_and_do_not_depend_on_failed_parent() {
    let (pool, repo) = memory_repo().await;
    let mut input = new_task("atomic_failure", "root");
    input.run_id = Some(4);
    input.scope_id = Some(20);
    input.max_attempts = 1;
    let task = repo.enqueue(&input).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    let expired = store
        .claim_many("atomic_failure", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'atomic_failure'")
        .execute(&pool).await.unwrap();
    let claimed = store
        .claim_many("atomic_failure", "recovery-worker", 1)
        .await
        .unwrap()
        .remove(0);
    assert!(claimed.attempt > claimed.max_attempts);
    assert_eq!(expired.scope_id, Some(20));
    assert_eq!(claimed.scope_id, Some(20));
    let recovery = plan("recovery", "normal", vec![json!({"retry": true})]);
    let invalid = plan("invalid", "unsupported", vec![json!({})]);
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
        .claim_many("atomic_failure", "worker", 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(recovery.name, "recovery");
    assert_eq!(recovery.run_id, Some(4));
    assert_eq!(recovery.scope_id, Some(21));
    assert_eq!(recovery.parent_task_ids, [task.task_id]);
    store.settle_failure(&recovery, message, &[]).await.unwrap();
    assert_eq!(get(&repo, &recovery).await.status, TaskStatus::Failed);
}

#[tokio::test]
async fn failure_fanout_to_the_same_name_creates_a_fresh_task() {
    let (_, repo) = memory_repo().await;
    repo.enqueue(&new_task("same_name", "retry")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    let source = store
        .claim_many("same_name", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    store
        .settle_failure(
            &source,
            "retry requested",
            &[plan("retry", "normal", vec![json!({"retry": true})])],
        )
        .await
        .unwrap();
    assert_eq!(get(&repo, &source).await.status, TaskStatus::Superseded);
    let successors = store.claim_many("same_name", "worker", 1).await.unwrap();
    assert_eq!(successors.len(), 1);
    assert_ne!(successors[0].task_id, source.task_id);
    assert_eq!(successors[0].input, json!({"retry": true}));
    assert_eq!(successors[0].parent_task_ids, [source.task_id]);
}

#[tokio::test]
async fn release_all_claims_and_store_operations_stay_in_their_workflow() {
    let (_, repo) = memory_repo().await;
    let ours = repo.enqueue(&new_task("ours", "root")).await.unwrap();
    let theirs = repo.enqueue(&new_task("theirs", "root")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    assert_eq!(
        store.claim_many("ours", "worker", 10).await.unwrap().len(),
        1
    );
    assert_eq!(get(&repo, &theirs).await.status, TaskStatus::Pending);
    repo.claim_many("theirs", "other-worker", 1).await.unwrap();
    assert_eq!(repo.release_claims("ours", "worker").await.unwrap(), 1);
    let reclaimed = store
        .claim_many("ours", "worker", 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(reclaimed.attempt, 2);
    assert!(
        repo.claim_many("theirs", "third-worker", 1)
            .await
            .unwrap()
            .is_empty()
    );

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
    let (pool, repo) = memory_repo().await;
    let root = repo.enqueue(&new_task("cascade", "root")).await.unwrap();
    let mut child = new_task("cascade", "child");
    child.run_id = root.run_id;
    child.parent_task_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let join = repo
        .enqueue_fanout(
            "cascade",
            root.run_id,
            None,
            &[],
            "join",
            &[json!({})],
            "join",
            false,
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
            None,
            &[],
            "join",
            &[json!({})],
            "join",
            false,
            &["root".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    let unrelated = repo.enqueue(&new_task("other", "root")).await.unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    let root = store
        .claim_many("cascade", "worker", 1)
        .await
        .unwrap()
        .remove(0);
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
    assert_eq!(get(&repo, &join).await.status, TaskStatus::Pending);
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
        TaskStatus::Pending,
        "name-based cascading stays in its run"
    );
    assert_eq!(get(&repo, &unrelated).await.status, TaskStatus::Pending);

    let late = repo
        .enqueue_fanout(
            "cascade",
            root.run_id,
            None,
            &[],
            "late_join",
            &[json!({})],
            "join",
            false,
            &["root".into()],
            3,
        )
        .await
        .unwrap()
        .remove(0);
    assert_eq!(late.status, TaskStatus::Failed);
    let claims = repo.claim_many("cascade", "worker", 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task_id, child.task_id);
}

#[tokio::test]
async fn cascade_matches_runs_only_for_join_predecessors() {
    let runs = [Some(1), Some(2), Some(3), Some(0)];
    for source_run in runs {
        let (_, repo) = memory_repo().await;
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
                repo.enqueue_fanout(
                    "cascade_runs",
                    run_id,
                    None,
                    &[],
                    "join",
                    &[json!({})],
                    "join",
                    false,
                    &["root".into()],
                    3,
                )
                .await
                .unwrap()
                .remove(0),
            );
        }

        let mut other_workflow = new_task("other", "child");
        other_workflow.run_id = source_run;
        other_workflow.parent_task_ids = vec![root.task_id];
        let other_workflow = repo.enqueue(&other_workflow).await.unwrap();

        let mut completed = new_task("cascade_runs", "completed");
        completed.run_id = source_run;
        let completed = repo.enqueue(&completed).await.unwrap();
        let claims = repo.claim_many("cascade_runs", "worker", 10).await.unwrap();
        let root_claim = claims
            .iter()
            .find(|task| task.task_id == root.task_id)
            .unwrap();
        let completed_claim = claims
            .iter()
            .find(|task| task.task_id == completed.task_id)
            .unwrap();
        repo.succeed(completed_claim, &json!("done")).await.unwrap();

        let store = SqliteTaskStore::new(repo.clone());
        store
            .settle_failure(root_claim, "permanent failure", &[])
            .await
            .unwrap();

        for target in targets {
            let expected = if target.task_type != "join" {
                TaskStatus::Running
            } else if target.run_id == source_run {
                TaskStatus::Failed
            } else {
                TaskStatus::Pending
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
    let (pool, repo) = memory_repo().await;
    let store = SqliteTaskStore::new(repo.clone());
    for max_attempts in [3, 0] {
        let workflow_id = format!("renew-{max_attempts}");
        let mut task = new_task(&workflow_id, "root");
        task.max_attempts = max_attempts;
        task.scope_id = Some(10);
        let pending = repo.enqueue(&task).await.unwrap();
        assert!(store.renew_claim(&pending).await.unwrap().is_none());
        let claim = repo
            .claim_many(&workflow_id, "worker", 1)
            .await
            .unwrap()
            .remove(0);
        sqlx::query("UPDATE tasks SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+10 seconds') WHERE workflow_id = ?")
            .bind(&workflow_id).execute(&pool).await.unwrap();
        let shortened = get(&repo, &claim).await.lease_until.unwrap();
        // The caller's lease snapshot may be stale after any prior renewal.
        let renewed = store.renew_claim(&claim).await.unwrap().unwrap();
        assert!(renewed > shortened + chrono::Duration::seconds(280));
        let current = get(&repo, &claim).await;
        assert_eq!(current.lease_until, Some(renewed));
        assert_eq!(current.attempt, claim.attempt);
        assert_eq!(current.claimed_by, claim.claimed_by);
        assert_eq!(current.error, claim.error);
        assert_eq!(current.scope_id, Some(10));
        assert!(
            repo.claim_many(&workflow_id, "other", 1)
                .await
                .unwrap()
                .is_empty()
        );
        let mut wrong_workflow = claim.clone();
        wrong_workflow.workflow_id = "missing".into();
        assert!(store.renew_claim(&wrong_workflow).await.unwrap().is_none());
        let mut wrong_task = claim.clone();
        wrong_task.task_id += 1;
        assert!(store.renew_claim(&wrong_task).await.unwrap().is_none());
        if max_attempts == 0 {
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
    let (pool, repo) = memory_repo().await;
    let root = repo.enqueue(&new_task("fencing", "root")).await.unwrap();
    let mut child = new_task("fencing", "child");
    child.run_id = root.run_id;
    child.parent_task_ids = vec![root.task_id];
    let child = repo.enqueue(&child).await.unwrap();
    let first = repo
        .claim_many("fencing", "worker-a", 1)
        .await
        .unwrap()
        .remove(0);
    sqlx::query("UPDATE tasks SET lease_until = '2000-01-01T00:00:00Z' WHERE workflow_id = 'fencing' AND task_id = ?")
        .bind(root.task_id).execute(&pool).await.unwrap();
    let second = repo
        .claim_many("fencing", "worker-b", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(second.attempt, first.attempt + 1);
    let before = serde_json::to_value(&second).unwrap();
    let store = SqliteTaskStore::new(repo.clone());
    assert!(store.renew_claim(&first).await.unwrap().is_none());
    let fanout = plan("forbidden", "normal", vec![json!("stale")]);
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
    // Both the owner and the attempt are checked independently.
    let mut wrong_attempt = second.clone();
    wrong_attempt.attempt = first.attempt;
    assert!(store.renew_claim(&wrong_attempt).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(&wrong_attempt).await,
        Err(StorageError::ClaimLost { .. })
    ));
    let mut wrong_owner = second.clone();
    wrong_owner.claimed_by = first.claimed_by.clone();
    assert!(store.renew_claim(&wrong_owner).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(&wrong_owner).await,
        Err(StorageError::ClaimLost { .. })
    ));
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
    let (pool, repo) = memory_repo().await;
    repo.enqueue(&new_task("expiry", "root")).await.unwrap();
    let claim = repo
        .claim_many("expiry", "worker", 1)
        .await
        .unwrap()
        .remove(0);
    assert!(
        repo.claim_many("expiry", "other-worker", 1)
            .await
            .unwrap()
            .is_empty()
    );
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
        repo.fail_and_enqueue(&claim, "expired", &[], 3).await,
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
async fn releasing_a_stopped_worker_does_not_release_another_workers_claims() {
    let (_, repo) = memory_repo().await;
    repo.enqueue(&new_task("worker-release", "first"))
        .await
        .unwrap();
    repo.enqueue(&new_task("worker-release", "second"))
        .await
        .unwrap();
    let first = repo
        .claim_many("worker-release", "a", 1)
        .await
        .unwrap()
        .remove(0);
    let second = repo
        .claim_many("worker-release", "b", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(repo.release_claims("worker-release", "a").await.unwrap(), 1);
    assert!(matches!(
        repo.release_claim(&first).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert_eq!(get(&repo, &second).await.lease_until, second.lease_until);
    let reclaimed = repo.claim_many("worker-release", "c", 10).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].task_id, first.task_id);
    assert_eq!(get(&repo, &second).await.claimed_by.as_deref(), Some("b"));
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
            .all(|t| t.attempt == 1 && t.status == TaskStatus::Running)
    );
    let task_id = tasks[0].task_id;
    repo_a
        .succeed(&tasks[0], &json!({"saved": true}))
        .await
        .unwrap();

    let contributions = futures::future::join_all(
        [(&repo_a, 1), (&repo_b, 2), (&repo_a, 3), (&repo_b, 4)]
            .into_iter()
            .map(|(repo, n)| async move {
                repo.enqueue_fanout(
                    "concurrent_fanin",
                    None,
                    None,
                    &[n],
                    "collect",
                    &[json!({"worker": n})],
                    "join",
                    false,
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
            .parent_task_ids
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
    assert_eq!(saved.status, TaskStatus::Succeeded);
    assert_eq!(saved.output, Some(json!({"saved": true})));
    let mut released = 0;
    for worker in ["a", "b", "c", "d"] {
        released += repo.release_claims("concurrent", worker).await.unwrap();
    }
    assert_eq!(released, 39);
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
