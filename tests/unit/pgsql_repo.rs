use std::time::Duration;

use serde_json::{Value, json};
use sqlx::postgres::PgListener;

use super::super::test_support::connect_test;
use super::{BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR};
use super::{PgTaskRepo, TASK_READY_CHANNEL};
use crate::error::StorageError;
use crate::graph::{
    Successor,
    TaskType::{Event, Join, Normal},
};
use crate::task::MAX_ATTEMPTS;
use crate::{Task, TaskRecord, TaskStatus};

#[path = "graph_fixture.rs"]
mod graph_fixture;
use graph_fixture::graph;

fn plan(dst: &'static str, inputs: Vec<Value>) -> Successor {
    Successor {
        dst,
        inputs,
        new_scope_per_input: false,
    }
}

/// 收取一条**本工作流**的任务就绪通知（2s 超时）：通知通道是集群级共享的，
/// 并发测试可能互相收到对方的 payload（生产监听器同样按 payload 分发），
/// 故跳过其他工作流的 payload 直到命中期望工作流。
async fn recv_ready(listener: &mut PgListener, workflow_id: &str) {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), listener.recv()).await {
            Ok(Ok(notification)) => {
                assert_eq!(notification.channel(), TASK_READY_CHANNEL);
                if notification.payload() == workflow_id {
                    return;
                }
            }
            Ok(Err(e)) => panic!("listener recv failed: {e}"),
            Err(_) => panic!("timed out waiting for task ready notification: {workflow_id}"),
        }
    }
}

#[tokio::test]
async fn enqueue_creates_independent_runs_or_preserves_explicit_task_fields() {
    let Some(database) = connect_test().await else {
        return;
    };
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='tasks'",
    )
    .fetch_all(database.pool())
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
    let workflow_id = "enqueue-runs";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("root", Event, None),
                ("child", Normal, None),
                ("analysis.entry", Normal, Some("analysis")),
                ("invalid_scope", Normal, None),
            ],
            &[("root", "child"), ("root", "analysis.entry")],
        ),
    );
    let task = Task::new(workflow_id, "root", json!({"input": 1}));
    let first = repo.enqueue(&task).await.unwrap();
    let second = repo.enqueue(&task).await.unwrap();
    assert_eq!(first.run_id, Some(first.task_id));
    assert_eq!(second.run_id, Some(second.task_id));
    assert_ne!(first.run_id, second.run_id);

    let child = Task {
        run_id: first.run_id,
        scope_id: None,
        parent_task_ids: vec![first.task_id],
        ..Task::new(workflow_id, "child", json!({"input": 2}))
    };
    let inserted = repo.enqueue(&child).await.unwrap();
    let stored = repo
        .get(workflow_id, inserted.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.run_id, first.run_id);
    assert_eq!(stored.scope_id, child.scope_id);
    assert_eq!(stored.name, child.name);
    assert_eq!(stored.parent_task_ids, child.parent_task_ids);
    assert_eq!(stored.input, child.input);
    assert_eq!(stored.status, TaskStatus::Pending);
    assert_eq!(stored.attempt, 0);

    let scoped = repo
        .enqueue(&Task {
            run_id: first.run_id,
            ..Task::new(workflow_id, "analysis.entry", json!("item"))
        })
        .await
        .unwrap();
    assert_eq!(scoped.run_id, first.run_id);
    assert!(scoped.scope_id.is_some());
    let parent: Option<i64> = sqlx::query_scalar(
        "SELECT parent_scope_id FROM scopes WHERE workflow_id=$1 AND scope_id=$2",
    )
    .bind(workflow_id)
    .bind(scoped.scope_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(parent, None);
    for (run_id, scope_id) in [
        (first.run_id, Some(i64::MAX)),
        (second.run_id, scoped.scope_id),
    ] {
        let invalid = Task {
            run_id,
            scope_id,
            ..Task::new(workflow_id, "invalid_scope", json!({}))
        };
        assert!(matches!(
            repo.enqueue(&invalid).await,
            Err(StorageError::InvalidFanout { .. })
        ));
    }
}

/// 原子成功结算：succeed + 全部后继（单条及批量输入）在同一事务内落库，
/// 后继继承源任务 run_id、parent_task_ids 记录源任务。
#[tokio::test]
async fn succeed_and_enqueue_settles_and_fans_out_atomically() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "succeed_and_enqueue_atomic";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("image_prompt", Normal, None),
                ("finalize_node", Normal, None),
                ("images.image_generate", Normal, Some("images")),
            ],
            &[
                ("image_prompt", "finalize_node"),
                ("image_prompt", "images.image_generate"),
            ],
        ),
    );
    repo.enqueue(&Task {
        workflow_id: workflow_id.to_string(),
        run_id: None,
        scope_id: None,
        name: "image_prompt".to_string(),
        parent_task_ids: vec![],
        input: json!({"prompts": ["a", "b"]}),
    })
    .await
    .unwrap();
    let source = repo.claim_many(workflow_id, 5).await.unwrap().remove(0);

    let settled = repo
        .succeed_and_enqueue(
            &source,
            &json!({"prompts": ["a", "b"]}),
            &[
                plan("finalize_node", vec![json!({})]),
                plan(
                    "images.image_generate",
                    vec![json!({"prompt": "a"}), json!({"prompt": "b"})],
                ),
                Successor {
                    new_scope_per_input: true,
                    ..plan("images.image_generate", vec![json!({"prompt": "c"})])
                },
            ],
        )
        .await
        .unwrap();

    // 源任务已成功结算且租约释放。
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(settled.output, Some(json!({"prompts": ["a", "b"]})));
    assert!(settled.lease_until.is_none());
    reject_stale_writes(&repo, &source).await;

    // 同一目标的普通跨入 map 共享实例，foreach 的元素建立独立实例。
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(source.run_id, Some(source.task_id));
    assert!(rows.iter().all(|task| task.run_id == source.run_id));
    let finalize: Vec<_> = rows.iter().filter(|t| t.name == "finalize_node").collect();
    assert_eq!(finalize.len(), 1);
    assert_eq!(finalize[0].parent_task_ids, vec![source.task_id]);
    let generates: Vec<_> = rows
        .iter()
        .filter(|t| t.name == "images.image_generate")
        .collect();
    assert_eq!(generates.len(), 3);
    for g in &generates {
        assert_eq!(g.parent_task_ids, vec![source.task_id]);
        assert!(g.scope_id.is_some());
        if g.input["prompt"] == "c" {
            assert_ne!(g.scope_id, generates[0].scope_id);
        } else {
            assert_eq!(g.scope_id, generates[0].scope_id);
        }
    }
}

/// 普通节点逐条创建任务，即使输入、父任务和运行身份相同。
#[tokio::test]
async fn normal_fanout_creates_each_input_independently() {
    let Some(database) = connect_test().await else {
        return;
    };
    for (case, (foreach, count)) in [(false, 2), (true, 0), (true, 1), (true, 3)]
        .into_iter()
        .enumerate()
    {
        let workflow_id = format!("normal-fanout-{case}");
        let repo = PgTaskRepo::new(
            database.pool().clone(),
            graph(
                &workflow_id,
                &[
                    ("root", Normal, None),
                    ("image_generate", Normal, None),
                    ("images.image_generate", Normal, Some("images")),
                ],
                &[
                    ("root", "image_generate"),
                    ("root", "images.image_generate"),
                ],
            ),
        );
        let mut previous = vec![];
        for _ in 0..2 {
            let root = repo
                .enqueue(&Task::new(&workflow_id, "root", json!({})))
                .await
                .unwrap();
            let claims = repo.claim_many(&workflow_id, 20).await.unwrap();
            let source = claims
                .iter()
                .find(|task| task.task_id == root.task_id)
                .unwrap();
            let inputs: Vec<_> = (0..count).map(|item| json!({"item": item})).collect();
            let destination = if foreach {
                "images.image_generate"
            } else {
                "image_generate"
            };
            let mut fanout = plan(destination, inputs.clone());
            fanout.new_scope_per_input = foreach;
            repo.succeed_and_enqueue(source, &json!({}), &[fanout])
                .await
                .unwrap();
            let rows = repo.list_all(&workflow_id).await.unwrap();
            let generated: Vec<_> = rows
                .iter()
                .filter(|task| task.parent_task_ids == [root.task_id])
                .collect();
            assert_eq!(generated.len(), count);
            for (task, input) in generated.iter().zip(&inputs) {
                assert_eq!(&task.input, input);
                assert_eq!(task.run_id, root.run_id);
                assert_eq!(task.scope_id.is_some(), foreach);
                assert!(!previous.contains(&task.task_id));
            }
            if foreach {
                let scopes: std::collections::HashSet<_> =
                    generated.iter().map(|task| task.scope_id).collect();
                assert_eq!(scopes.len(), count);
            }
            previous.extend(generated.iter().map(|task| task.task_id));
        }
    }
}

#[tokio::test]
async fn join_merges_different_parents_and_waits_for_all_tasks() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "join-parents";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[("source", Normal, None), ("collect", Join, None)],
            &[("source", "collect")],
        ),
    );
    seed(&repo, workflow_id, "source", Some(42)).await;
    seed(&repo, workflow_id, "source", Some(42)).await;
    let sources = repo.claim_many(workflow_id, 2).await.unwrap();
    assert_eq!(sources.len(), 2);
    for (index, source) in sources.iter().enumerate() {
        let contribution = json!({"source": source.task_id});
        repo.succeed_and_enqueue(
            source,
            &contribution,
            &[plan(
                "collect",
                vec![contribution.clone(), json!({"extra": source.task_id})],
            )],
        )
        .await
        .unwrap();
        if index == 0 {
            assert!(repo.claim_many(workflow_id, 1).await.unwrap().is_empty());
        }
    }
    let rows = repo.list_all(workflow_id).await.unwrap();
    let joins: Vec<_> = rows.iter().filter(|task| task.name == "collect").collect();
    assert_eq!(
        joins.len(),
        1,
        "different parents must contribute to the same JOIN"
    );
    let join = joins[0];
    assert_eq!(
        join.parent_task_ids,
        [sources[0].task_id, sources[1].task_id]
    );
    assert_eq!(
        join.input,
        json!({
            "source": [sources[0].task_id, sources[1].task_id],
            "extra": [sources[0].task_id, sources[1].task_id],
        })
    );
    let claims = repo.claim_many(workflow_id, 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task_id, join.task_id);
}

/// 原子失败结算：有后继 → `superseded` 且后继入队（不依赖源任务）；
/// 无后继 → `failed` 并级联等待该前驱的 JOIN，级联失败时源任务终态一起回滚。
#[tokio::test]
async fn fail_and_enqueue_marks_superseded_or_failed_atomically() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "fail_and_enqueue_atomic";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("commit_scene", Normal, None),
                ("scene_proposal", Normal, None),
                ("audio_generate", Normal, None),
                ("audio_dependent", Normal, None),
                ("audio_join", Join, None),
                ("other_run", Join, None),
            ],
            &[
                ("audio_generate", "audio_join"),
                ("audio_generate", "other_run"),
            ],
        ),
    );
    // 有后继：superseded + 后继入队，并保留源任务。
    repo.enqueue(&Task {
        workflow_id: workflow_id.to_string(),
        run_id: None,
        scope_id: None,
        name: "commit_scene".to_string(),
        parent_task_ids: vec![],
        input: json!({"proposal": {}}),
    })
    .await
    .unwrap();
    let retried = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.fail_and_enqueue(
        &retried,
        "validation failed",
        &[plan("scene_proposal", vec![json!({"Retry": {}})])],
    )
    .await
    .unwrap();
    let settled = repo
        .get(workflow_id, retried.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.status, TaskStatus::Superseded);
    assert_eq!(settled.error.as_deref(), Some("validation failed"));
    let successor = repo
        .list_all(workflow_id)
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.name == "scene_proposal" && t.task_id != retried.task_id)
        .unwrap();
    assert_eq!(successor.parent_task_ids, [retried.task_id]);

    // 无后继：failed。
    let failed = repo
        .enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            scope_id: None,
            name: "audio_generate".to_string(),
            parent_task_ids: vec![],
            input: json!({}),
        })
        .await
        .unwrap();
    let failed = repo
        .claim_many(workflow_id, 10)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == failed.task_id)
        .unwrap();
    let mut dependents = Vec::new();
    for (name, run_id, is_join) in [
        ("audio_dependent", failed.run_id, false),
        ("audio_join", failed.run_id, true),
        ("other_run", retried.run_id, true),
    ] {
        let dependent = repo
            .enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id,
                scope_id: None,
                name: name.to_string(),
                parent_task_ids: if is_join {
                    vec![]
                } else {
                    vec![failed.task_id]
                },
                input: json!({}),
            })
            .await
            .unwrap();
        dependents.push(dependent);
    }

    // 级联写入失败必须回滚源任务的失败状态。
    sqlx::query(
        "ALTER TABLE tasks ADD CONSTRAINT reject_cascade \
         CHECK (name <> 'audio_join' OR status <> 'failed')",
    )
    .execute(database.pool())
    .await
    .unwrap();
    assert!(
        repo.fail_and_enqueue(&failed, "budget exhausted", &[])
            .await
            .is_err()
    );
    for task in std::iter::once(&failed).chain(&dependents) {
        let unchanged = repo.get(workflow_id, task.task_id).await.unwrap().unwrap();
        assert_eq!(
            unchanged.status,
            if task.task_id == failed.task_id {
                TaskStatus::Running
            } else {
                TaskStatus::Pending
            }
        );
    }
    sqlx::query("ALTER TABLE tasks DROP CONSTRAINT reject_cascade")
        .execute(database.pool())
        .await
        .unwrap();

    repo.fail_and_enqueue(&failed, "budget exhausted", &[])
        .await
        .unwrap();
    let settled = repo
        .get(workflow_id, failed.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.status, TaskStatus::Failed);
    for dependent in dependents {
        let settled = repo
            .get(workflow_id, dependent.task_id)
            .await
            .unwrap()
            .unwrap();
        let expected = if dependent.name == "audio_join" && dependent.run_id == failed.run_id {
            TaskStatus::Failed
        } else {
            TaskStatus::Pending
        };
        assert_eq!(settled.status, expected);
    }
}

/// 释放认领只影响指定工作流中的任务。
#[tokio::test]
async fn releasing_a_claim_stays_in_its_workflow() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph("release_ours", &[("narration", Normal, None)], &[]),
    );
    let other_repo = PgTaskRepo::new(
        database.pool().clone(),
        graph("release_other", &[("narration", Normal, None)], &[]),
    );
    async fn enqueue(repo: &PgTaskRepo, workflow_id: &str) -> TaskRecord {
        repo.enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            scope_id: None,
            name: "narration".to_string(),
            parent_task_ids: vec![],
            input: json!({}),
        })
        .await
        .unwrap()
    }
    let ours = enqueue(&repo, "release_ours").await;
    let other = enqueue(&other_repo, "release_other").await;
    let our_claim = repo.claim_many("release_ours", 1).await.unwrap().remove(0);
    let other_claim = other_repo
        .claim_many("release_other", 1)
        .await
        .unwrap()
        .remove(0);
    assert!(matches!(
        repo.claim_many("release_other", 1).await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert!(matches!(
        repo.succeed(&other_claim, &json!("wrong graph")).await,
        Err(StorageError::InvalidFanout { .. })
    ));
    assert!(matches!(
        repo.fail(&other_claim, "wrong graph").await,
        Err(StorageError::InvalidFanout { .. })
    ));

    repo.release_claim(&our_claim).await.unwrap();
    let reclaimed = repo.claim_many("release_ours", 5).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].task_id, ours.task_id);
    assert_eq!(reclaimed[0].attempt, 2, "重认领照常递增 attempt");

    // 其它工作流不受影响：它的任务租约还在（认领不到，状态仍是在跑）。
    assert!(
        other_repo
            .claim_many("release_other", 5)
            .await
            .unwrap()
            .is_empty()
    );
    let untouched = repo
        .get("release_other", other.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(untouched.status, TaskStatus::Running);
    assert!(untouched.lease_until.is_some());
}

/// 恢复扫描只返回「存在可认领任务」的工作流（与 `claim_many` 同款闸门）：
/// 待执行和待最终失败结算的工作流均命中；JOIN 被前驱阻塞或已全部成功的工作流不命中。
#[tokio::test]
async fn executable_workflow_ids_only_returns_claimable_sessions() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repository = |workflow_id| {
        PgTaskRepo::new(
            database.pool().clone(),
            graph(
                workflow_id,
                &[
                    ("narration", Normal, None),
                    ("commit_scene", Normal, None),
                    ("collect", Join, None),
                ],
                &[("commit_scene", "collect")],
            ),
        )
    };
    async fn insert_task(repo: &PgTaskRepo, workflow_id: &str, name: &str) -> TaskRecord {
        repo.enqueue(&Task::new(workflow_id, name, json!({})))
            .await
            .unwrap()
    }

    // 可认领：pending 且无依赖 → 命中。
    let claimable = "exec_sess_claimable";
    let claimable_repo = repository(claimable);
    insert_task(&claimable_repo, claimable, "narration").await;

    // JOIN 被前驱阻塞：后继 pending 但前驱未成功 → 不命中。先把前驱认领掉
    // （running + 租约未过期 → 前驱自身也不可认领），否则前驱的 pending 行
    // 本身就会让工作流命中，测试的「阻塞」前提不成立。
    let blocked = "exec_sess_blocked";
    let blocked_repo = repository(blocked);
    let dep = insert_task(&blocked_repo, blocked, "commit_scene").await;
    let mut join = Task::new(blocked, "collect", json!({}));
    join.run_id = dep.run_id;
    blocked_repo.enqueue(&join).await.unwrap();
    assert_eq!(blocked_repo.claim_many(blocked, 5).await.unwrap().len(), 1);

    // 已全部成功：无 pending/running → 不命中。
    let done = "exec_sess_done";
    let done_repo = repository(done);
    insert_task(&done_repo, done, "narration").await;
    let finished = done_repo.claim_many(done, 1).await.unwrap().remove(0);
    done_repo.succeed(&finished, &json!({})).await.unwrap();

    // 预算耗尽后仍需认领，以通过图的统一失败流程结算。
    let exhausted = "exec_sess_exhausted";
    let exhausted_repo = repository(exhausted);
    insert_task(&exhausted_repo, exhausted, "narration").await;
    for _ in 0..3 {
        let claim = exhausted_repo
            .claim_many(exhausted, 5)
            .await
            .unwrap()
            .remove(0);
        exhausted_repo.release_claim(&claim).await.unwrap();
    }

    let mut sessions = Vec::new();
    for (workflow_id, repo) in [
        (claimable, &claimable_repo),
        (blocked, &blocked_repo),
        (done, &done_repo),
        (exhausted, &exhausted_repo),
    ] {
        sessions.extend(repo.executable_workflow_ids(workflow_id).await.unwrap());
    }
    sessions.sort();
    assert_eq!(
        sessions,
        vec![claimable.to_string(), exhausted.to_string()],
        "可执行业务或需最终失败结算的工作流都应被发现"
    );
}

/// 预算耗尽后重新认领，只允许原子失败结算；旧认领不能修改恢复者或创建失败后继。
#[tokio::test]
async fn exhausted_claims_recover_through_atomic_failure_settlement() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "exhausted-recovery";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("source", Normal, None),
                ("handle_failure", Normal, None),
                ("invalid", Normal, None),
            ],
            &[],
        ),
    );
    let input = Task::new(workflow_id, "source", json!({}));
    let root = repo.enqueue(&input).await.unwrap();
    for _ in 1..MAX_ATTEMPTS {
        let previous = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
        repo.release_claim(&previous).await.unwrap();
    }
    let old = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(old.attempt, MAX_ATTEMPTS);
    assert!(repo.claim_many(workflow_id, 1).await.unwrap().is_empty());
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1")
        .bind(workflow_id).execute(database.pool()).await.unwrap();
    assert_eq!(
        repo.executable_workflow_ids(workflow_id).await.unwrap(),
        [workflow_id]
    );
    let recovery = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(recovery.task_id, root.task_id);
    assert_eq!(recovery.attempt, MAX_ATTEMPTS + 1);
    assert_eq!(recovery.status, TaskStatus::Running);
    assert_eq!(recovery.error.as_deref(), Some(LEASE_EXPIRED_ERR));
    reject_stale_writes(&repo, &old).await;
    assert!(matches!(
        repo.succeed(&recovery, &json!("forbidden")).await,
        Err(StorageError::ClaimLost { .. })
    ));

    let fanout = plan("handle_failure", vec![json!({"reason": "timeout"})]);
    let mut invalid = fanout.clone();
    invalid.dst = "invalid";
    sqlx::query(
        "ALTER TABLE tasks ADD CONSTRAINT reject_recovery_successor CHECK (name <> 'invalid')",
    )
    .execute(database.pool())
    .await
    .unwrap();
    assert!(
        repo.fail_and_enqueue(&recovery, LEASE_EXPIRED_ERR, &[fanout.clone(), invalid])
            .await
            .is_err()
    );
    let unchanged = repo.get(workflow_id, root.task_id).await.unwrap().unwrap();
    assert_eq!(unchanged.status, TaskStatus::Running);
    assert_eq!(unchanged.attempt, recovery.attempt);
    assert_eq!(unchanged.lease_until, recovery.lease_until);
    assert_eq!(repo.list_all(workflow_id).await.unwrap().len(), 1);
    sqlx::query("ALTER TABLE tasks DROP CONSTRAINT reject_recovery_successor")
        .execute(database.pool())
        .await
        .unwrap();

    // 失败结算自身也允许重新认领，首次保存的超时原因不能被释放租约覆盖。
    repo.release_claim(&recovery).await.unwrap();
    let next = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(next.attempt, MAX_ATTEMPTS + 2);
    assert_eq!(next.error.as_deref(), Some(LEASE_EXPIRED_ERR));
    reject_stale_writes(&repo, &recovery).await;
    repo.fail_and_enqueue(&next, LEASE_EXPIRED_ERR, &[fanout])
        .await
        .unwrap();
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, TaskStatus::Superseded);
    assert_eq!(rows[0].error.as_deref(), Some(LEASE_EXPIRED_ERR));
    assert!(rows[0].lease_until.is_none());
    assert_eq!(rows[1].status, TaskStatus::Pending);
    assert_eq!(rows[1].parent_task_ids, [root.task_id]);
    reject_stale_writes(&repo, &next).await;

    // 无执行预算的任务也通过失败结算传播到等待它的 JOIN，不会被直接封存。
    let zero_repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            "zero-budget",
            &[("source", Normal, None), ("dependent", Join, None)],
            &[("source", "dependent")],
        ),
    );
    let input = Task::new("zero-budget", "source", json!({}));
    let zero = zero_repo.enqueue(&input).await.unwrap();
    let mut child = Task::new("zero-budget", "dependent", json!({}));
    child.run_id = zero.run_id;
    let child = zero_repo.enqueue(&child).await.unwrap();
    for _ in 0..MAX_ATTEMPTS {
        let previous = zero_repo
            .claim_many("zero-budget", 1)
            .await
            .unwrap()
            .remove(0);
        zero_repo.release_claim(&previous).await.unwrap();
    }
    let claim = zero_repo
        .claim_many("zero-budget", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(claim.attempt, MAX_ATTEMPTS + 1);
    assert_eq!(claim.error.as_deref(), Some(BUDGET_EXHAUSTED_ERR));
    zero_repo
        .fail_and_enqueue(&claim, BUDGET_EXHAUSTED_ERR, &[])
        .await
        .unwrap();
    for task_id in [zero.task_id, child.task_id] {
        let settled = zero_repo
            .get("zero-budget", task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
    }
}

/// 任务就绪通知发送点：入队 / 扇出 / 释放租约各自随提交投递，payload = workflow_id。
#[tokio::test]
async fn task_ready_notifications_carry_workflow_id() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "task_ready_notifications";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("scene_proposal", Normal, None),
                ("commit_scene", Normal, None),
            ],
            &[],
        ),
    );
    let mut listener = PgListener::connect(database.database_url()).await.unwrap();
    listener.listen(TASK_READY_CHANNEL).await.unwrap();

    // 1) 入队（HTTP 推进根任务）。
    let root = repo
        .enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            scope_id: None,
            name: "scene_proposal".to_string(),
            parent_task_ids: vec![],
            input: json!({}),
        })
        .await
        .unwrap();
    recv_ready(&mut listener, workflow_id).await;

    // 2) 扇出后继（原子结算路径同样经此通知）。
    let source = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(source.task_id, root.task_id);
    repo.succeed_and_enqueue(
        &source,
        &json!({}),
        &[plan("commit_scene", vec![json!({})])],
    )
    .await
    .unwrap();
    recv_ready(&mut listener, workflow_id).await;

    // 3) 释放租约（原子结算回滚后的立即重入）。
    let claimed = repo.claim_many(workflow_id, 5).await.unwrap().remove(0);
    repo.release_claim(&claimed).await.unwrap();
    recv_ready(&mut listener, workflow_id).await;
}

async fn seed(repo: &PgTaskRepo, workflow_id: &str, name: &str, run_id: Option<i64>) -> TaskRecord {
    repo.enqueue(&Task {
        workflow_id: workflow_id.into(),
        run_id,
        scope_id: None,
        name: name.into(),
        parent_task_ids: vec![],
        input: json!({}),
    })
    .await
    .unwrap()
}

async fn reject_stale_writes(repo: &PgTaskRepo, task: &TaskRecord) {
    let fanouts = [plan("must-not-exist", vec![json!({})])];
    assert!(repo.renew_claim(task).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(task).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.succeed_and_enqueue(task, &json!("stale"), &fanouts)
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.fail_and_enqueue(task, "stale", &fanouts).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.fail_and_enqueue(task, "stale", &[]).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.succeed(task, &json!("stale")).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.fail(task, "stale").await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.supersede(task, "stale").await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.skip(task, "stale").await,
        Err(StorageError::ClaimLost { .. })
    ));
}

#[tokio::test]
async fn renewal_extends_the_live_claim_without_changing_its_generation() {
    let Some(database) = connect_test().await else {
        return;
    };
    for previous_attempts in [0, MAX_ATTEMPTS] {
        let workflow_id = format!("renew-{previous_attempts}");
        let repo = PgTaskRepo::new(
            database.pool().clone(),
            graph(&workflow_id, &[("root", Normal, None)], &[]),
        );
        let task = Task::new(&workflow_id, "root", json!({}));

        let pending = repo.enqueue(&task).await.unwrap();
        for _ in 0..previous_attempts {
            let previous = repo.claim_many(&workflow_id, 1).await.unwrap().remove(0);
            repo.release_claim(&previous).await.unwrap();
        }
        assert!(repo.renew_claim(&pending).await.unwrap().is_none());
        let claim = repo.claim_many(&workflow_id, 1).await.unwrap().remove(0);
        let shortened: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "UPDATE tasks SET lease_until = clock_timestamp() + interval '10 seconds' WHERE workflow_id = $1 RETURNING lease_until"
        ).bind(&workflow_id).fetch_one(database.pool()).await.unwrap();
        let renewed = repo.renew_claim(&claim).await.unwrap().unwrap();
        assert!(renewed > shortened + chrono::Duration::seconds(280));
        let current = repo
            .get(&workflow_id, claim.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.lease_until, Some(renewed));
        assert_eq!(current.attempt, claim.attempt);
        assert_eq!(current.error, claim.error);
        assert!(repo.claim_many(&workflow_id, 1).await.unwrap().is_empty());
        let mut wrong_workflow = claim.clone();
        wrong_workflow.workflow_id = "missing".into();
        assert!(repo.renew_claim(&wrong_workflow).await.unwrap().is_none());
        let mut wrong_task = claim.clone();
        wrong_task.task_id += 1;
        assert!(repo.renew_claim(&wrong_task).await.unwrap().is_none());
        if previous_attempts == MAX_ATTEMPTS {
            repo.fail_and_enqueue(&claim, "budget exhausted", &[])
                .await
                .unwrap();
        } else {
            repo.succeed(&claim, &json!("done")).await.unwrap();
        }
        assert!(repo.renew_claim(&claim).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn expired_and_replaced_claims_cannot_write_or_release_a_new_owner() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "claim-fencing";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("root", Normal, None),
                ("dependent", Join, None),
                ("another", Join, None),
            ],
            &[("root", "dependent"), ("root", "another")],
        ),
    );
    seed(&repo, workflow_id, "root", Some(7)).await;
    let old = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let mut dependent = Task {
        workflow_id: workflow_id.into(),
        run_id: Some(7),
        scope_id: None,
        name: "dependent".into(),
        parent_task_ids: vec![],
        input: json!({}),
    };
    repo.enqueue(&dependent).await.unwrap();
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2")
        .bind(workflow_id).bind(old.task_id).execute(database.pool()).await.unwrap();
    reject_stale_writes(&repo, &old).await;
    let current = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(current.attempt, old.attempt + 1);
    reject_stale_writes(&repo, &old).await;
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 2, "stale settlement cannot create successors");
    assert_eq!(
        rows[1].status,
        TaskStatus::Pending,
        "stale failure cannot cascade"
    );
    let unchanged = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.attempt, current.attempt);
    assert_eq!(unchanged.lease_until, current.lease_until);
    repo.succeed(&current, &json!("current")).await.unwrap();
    reject_stale_writes(&repo, &current).await;
    let settled = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.output, Some(json!("current")));
    // Releasing one task leaves the other claim valid.
    dependent.name = "another".into();
    repo.enqueue(&dependent).await.unwrap();
    let a = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let b = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.release_claim(&a).await.unwrap();
    assert!(matches!(
        repo.release_claim(&a).await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.succeed(&b, &json!("unaffected")).await.unwrap();
}

async fn wait_for_lock_waiters(pool: &sqlx::PgPool, expected: i64) {
    let application: String = sqlx::query_scalar("SELECT current_setting('application_name')")
        .fetch_one(pool)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'"
            ).bind(&application).fetch_one(pool).await.unwrap();
            if waiting >= expected { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("workers must reach their concurrent lock waits");
}

#[tokio::test]
async fn concurrent_first_fanins_merge_once_even_with_opposite_successor_order() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "concurrent-join";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("left", Normal, None),
                ("right", Normal, None),
                ("join-a", Join, None),
                ("join-b", Join, None),
            ],
            &[
                ("left", "join-a"),
                ("right", "join-a"),
                ("left", "join-b"),
                ("right", "join-b"),
            ],
        ),
    );
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(database.pool())
        .await
        .unwrap();
    // Different first schemas must still lock the same physical tasks table;
    // a connection's default isolation must not keep a pre-lock JOIN snapshot.
    let alternate_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query(&format!(r#"SET search_path TO pg_catalog, "{schema}""#))
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SELECT set_config('application_name', $1, false)")
                    .bind(&schema)
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET default_transaction_isolation TO 'repeatable read'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database.database_url())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_schema()")
            .fetch_one(&alternate_pool)
            .await
            .unwrap(),
        "pg_catalog"
    );
    seed(&repo, workflow_id, "left", Some(42)).await;
    seed(&repo, workflow_id, "right", Some(42)).await;
    let left = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let right = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    // Freeze ID allocation: both parents reach the missing-JOIN window concurrently.
    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT next_task_id FROM task_counters WHERE workflow_id = $1 FOR UPDATE")
        .bind(workflow_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut workers = tokio::task::JoinSet::new();
    for (source, targets, repo) in [
        (left.clone(), ["join-a", "join-b"], repo.clone()),
        (
            right.clone(),
            ["join-b", "join-a"],
            PgTaskRepo::new(alternate_pool.clone(), repo.graph.clone()),
        ),
    ] {
        workers.spawn(async move {
            let fanouts: Vec<_> = targets
                .into_iter()
                .map(|name| plan(name, vec![json!({ source.name.clone(): source.task_id })]))
                .collect();
            repo.succeed_and_enqueue(&source, &json!(source.task_id), &fanouts)
                .await
        });
    }
    wait_for_lock_waiters(database.pool(), 2).await;
    blocker.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await
    .expect("opposite successor order must not deadlock");
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 4, "two source tasks and exactly two JOIN tasks");
    for join in rows
        .iter()
        .filter(|task| matches!(task.name.as_str(), "join-a" | "join-b"))
    {
        assert_eq!(
            join.input,
            json!({"left": left.task_id, "right": right.task_id})
        );
        let mut parents = join.parent_task_ids.clone();
        parents.sort_unstable();
        assert_eq!(parents, [left.task_id, right.task_id]);
    }
    assert_eq!(repo.claim_many(workflow_id, 10).await.unwrap().len(), 2);
    alternate_pool.close().await;
}

#[tokio::test]
async fn a_lease_that_expires_while_waiting_for_a_row_lock_cannot_be_used() {
    let Some(database) = connect_test().await else {
        return;
    };
    for operation in 0..4 {
        let workflow_id = format!("lease-lock-wait-{operation}");
        let repo = PgTaskRepo::new(
            database.pool().clone(),
            graph(&workflow_id, &[("root", Normal, None)], &[]),
        );
        seed(&repo, &workflow_id, "root", None).await;
        let claim = repo.claim_many(&workflow_id, 1).await.unwrap().remove(0);
        sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() + interval '300 milliseconds' WHERE workflow_id = $1")
            .bind(&workflow_id).execute(database.pool()).await.unwrap();
        let mut blocker = database.pool().begin().await.unwrap();
        sqlx::query("SELECT task_id FROM tasks WHERE workflow_id = $1 FOR UPDATE")
            .bind(&workflow_id)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let worker_repo = repo.clone();
        let worker = tokio::spawn(async move {
            let result = match operation {
                0 => worker_repo.release_claim(&claim).await,
                1 => worker_repo.succeed(&claim, &json!({})).await.map(|_| ()),
                2 => worker_repo.fail_and_enqueue(&claim, "expired", &[]).await,
                _ => return worker_repo.renew_claim(&claim).await.unwrap().is_none(),
            };
            matches!(result, Err(StorageError::ClaimLost { .. }))
        });
        wait_for_lock_waiters(database.pool(), 1).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        blocker.commit().await.unwrap();
        assert!(worker.await.unwrap());
        let rows = repo.list_all(&workflow_id).await.unwrap();
        assert_eq!(rows[0].status, TaskStatus::Running);
        assert!(rows[0].output.is_none());
    }
}

#[tokio::test]
async fn scoped_fanout_joins_and_failures_stay_within_their_branch() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "scoped-join";
    let repo = PgTaskRepo::new(
        database.pool().clone(),
        graph(
            workflow_id,
            &[
                ("root", Normal, None),
                ("analysis.entry", Normal, Some("analysis")),
                ("analysis.left", Normal, Some("analysis")),
                ("analysis.right", Normal, Some("analysis")),
                ("analysis.join", Join, Some("analysis")),
                ("analysis.report", Normal, Some("analysis")),
            ],
            &[
                ("root", "analysis.entry"),
                ("analysis.entry", "analysis.left"),
                ("analysis.entry", "analysis.right"),
                ("analysis.left", "analysis.join"),
                ("analysis.right", "analysis.join"),
                ("analysis.join", "analysis.report"),
            ],
        ),
    );
    seed(&repo, workflow_id, "root", Some(42)).await;
    let root = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.succeed_and_enqueue(
        &root,
        &json!(["a", "b", "c"]),
        &[Successor {
            new_scope_per_input: true,
            ..plan("analysis.entry", vec![json!("a"), json!("b"), json!("c")])
        }],
    )
    .await
    .unwrap();
    let scopes = repo.claim_many(workflow_id, 3).await.unwrap();
    for scope in &scopes {
        let fanouts =
            ["analysis.left", "analysis.right"].map(|dst| plan(dst, vec![scope.input.clone()]));
        repo.succeed_and_enqueue(scope, &scope.input, &fanouts)
            .await
            .unwrap();
    }
    let branches = repo.claim_many(workflow_id, 6).await.unwrap();
    assert_eq!(branches.len(), 6);
    assert!(branches.iter().all(|task| task.run_id == Some(42)));
    for scope in &scopes {
        assert_eq!(
            branches
                .iter()
                .filter(|task| task.scope_id == scope.scope_id)
                .count(),
            2,
        );
    }
    let branch = |item: &str, name: &str| {
        branches
            .iter()
            .find(|task| task.input == json!(item) && task.name == name)
            .unwrap()
    };
    let contribution = |task: &TaskRecord| {
        plan(
            "analysis.join",
            vec![json!({task.name.rsplit('.').next().unwrap(): task.input})],
        )
    };
    for item in ["a", "b"] {
        let left = branch(item, "analysis.left");
        repo.succeed_and_enqueue(left, &left.input, &[contribution(left)])
            .await
            .unwrap();
    }
    assert!(repo.claim_many(workflow_id, 10).await.unwrap().is_empty());

    // Reclaiming the same task must retain its branch, including after a restart.
    let right_a = branch("a", "analysis.right");
    repo.release_claim(right_a).await.unwrap();
    let reclaimed = repo.claim_many(workflow_id, 10).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    let right_a = &reclaimed[0];
    assert_eq!(right_a.task_id, branch("a", "analysis.right").task_id);
    assert_eq!(right_a.scope_id, branch("a", "analysis.right").scope_id);
    repo.succeed_and_enqueue(right_a, &right_a.input, &[contribution(right_a)])
        .await
        .unwrap();
    let ready = repo.claim_many(workflow_id, 10).await.unwrap();
    assert_eq!(
        ready.len(),
        1,
        "branch a must run while branches b and c are unfinished"
    );
    assert_eq!(ready[0].scope_id, right_a.scope_id);
    assert_eq!(ready[0].input, json!({"left": "a", "right": "a"}));
    repo.succeed_and_enqueue(
        &ready[0],
        &ready[0].input,
        &[plan("analysis.report", vec![ready[0].input.clone()])],
    )
    .await
    .unwrap();

    // Branch b already has a waiting JOIN; branch c's JOIN arrives after failure.
    for item in ["b", "c"] {
        repo.fail_and_enqueue(branch(item, "analysis.right"), "failed signal", &[])
            .await
            .unwrap();
    }
    let left_c = branch("c", "analysis.left");
    repo.succeed_and_enqueue(left_c, &left_c.input, &[contribution(left_c)])
        .await
        .unwrap();
    let rows = repo.list_all(workflow_id).await.unwrap();
    let joins: Vec<_> = rows
        .iter()
        .filter(|task| task.name == "analysis.join")
        .collect();
    assert_eq!(joins.len(), 3);
    for join in joins {
        let expected = if join.scope_id == scopes[0].scope_id {
            TaskStatus::Succeeded
        } else {
            TaskStatus::Failed
        };
        assert_eq!(join.status, expected);
    }
    let report = rows
        .iter()
        .find(|task| task.name == "analysis.report")
        .unwrap();
    assert_eq!(report.scope_id, scopes[0].scope_id);
    assert_eq!(report.input, json!({"left": "a", "right": "a"}));
    let ready = repo.claim_many(workflow_id, 10).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, report.task_id);
}
