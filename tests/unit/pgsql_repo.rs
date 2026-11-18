use std::time::Duration;

use serde_json::json;
use sqlx::postgres::PgListener;

use super::super::test_support::connect_test;
use super::BUDGET_EXHAUSTED_ERR;
use super::{PgTaskRepo, TASK_READY_CHANNEL};
use crate::error::StorageError;
use crate::task::FanoutInstances;
use crate::{Task, TaskRecord, TaskStatus};

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
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "enqueue-runs";
    let task = Task::new(workflow_id, "root", json!({"input": 1}));
    let first = repo.enqueue(&task).await.unwrap();
    let second = repo.enqueue(&task).await.unwrap();
    assert_eq!(first.run_id, Some(first.task_id));
    assert_eq!(second.run_id, Some(second.task_id));
    assert_ne!(first.run_id, second.run_id);
    assert_eq!(first.max_attempts, 3);

    let child = Task {
        run_id: first.run_id,
        instance_id: Some(5),
        max_attempts: 7,
        depends_on_ids: vec![first.task_id],
        depends_on_names: vec![first.name.clone()],
        ..Task::new(workflow_id, "child", json!({"input": 2}))
    };
    let inserted = repo.enqueue(&child).await.unwrap();
    let stored = repo
        .get(workflow_id, inserted.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.run_id, first.run_id);
    assert_eq!(stored.name, child.name);
    assert_eq!(stored.instance_id, child.instance_id);
    assert_eq!(stored.max_attempts, child.max_attempts);
    assert_eq!(stored.depends_on_ids, child.depends_on_ids);
    assert_eq!(stored.depends_on_names, child.depends_on_names);
    assert_eq!(stored.input, child.input);
    assert_eq!(stored.status, TaskStatus::Pending);
    assert_eq!(stored.attempt, 0);
}

/// 原子成功结算：succeed + 全部后继（单实例 + 多实例）在同一事务内落库，
/// 后继继承源任务 run_id、depends_on_ids 指向源任务。
#[tokio::test]
async fn succeed_and_enqueue_settles_and_fans_out_atomically() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "succeed_and_enqueue_atomic";
    repo.enqueue(&Task {
        workflow_id: workflow_id.to_string(),
        run_id: None,
        name: "image_prompt".to_string(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({"prompts": ["a", "b"]}),
    })
    .await
    .unwrap();
    let source = repo
        .claim_many(workflow_id, "worker", 5)
        .await
        .unwrap()
        .remove(0);

    let settled = repo
        .succeed_and_enqueue(
            &source,
            &json!({"prompts": ["a", "b"]}),
            &[
                FanoutInstances {
                    name: "finalize_node".to_string(),
                    task_type: "normal".to_string(),
                    instances: vec![(None, json!({}))],
                    depends_on_names: vec![],
                },
                FanoutInstances {
                    name: "image_generate".to_string(),
                    task_type: "normal".to_string(),
                    instances: vec![
                        (Some(0), json!({"prompt": "a"})),
                        (Some(1), json!({"prompt": "b"})),
                    ],
                    depends_on_names: vec![],
                },
            ],
            3,
        )
        .await
        .unwrap();

    // 源任务已成功结算且租约释放。
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(settled.output, Some(json!({"prompts": ["a", "b"]})));
    assert!(settled.lease_until.is_none());

    // 三个后继全部落库，依赖均指向源任务。
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(source.run_id, Some(source.task_id));
    assert!(rows.iter().all(|task| task.run_id == source.run_id));
    let finalize: Vec<_> = rows.iter().filter(|t| t.name == "finalize_node").collect();
    assert_eq!(finalize.len(), 1);
    assert_eq!(finalize[0].instance_id, None);
    assert_eq!(finalize[0].depends_on_ids, vec![source.task_id]);
    let generates: Vec<_> = rows.iter().filter(|t| t.name == "image_generate").collect();
    assert_eq!(generates.len(), 2);
    assert_eq!(generates[0].instance_id, Some(0));
    assert_eq!(generates[1].instance_id, Some(1));
    for g in &generates {
        assert_eq!(g.depends_on_ids, vec![source.task_id]);
    }
}

/// 多实例唯一键去重：同键重入队（模拟「结算事务已提交但响应丢失」后的重执行）经
/// `ON CONFLICT DO NOTHING` 跳过，不产生重复实例行。
#[tokio::test]
async fn multi_instance_fanout_dedups_conflicting_keys() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "multi_instance_dedup";
    let instances: Vec<(Option<i32>, serde_json::Value)> = vec![
        (Some(0), json!({"prompt": "a"})),
        (Some(1), json!({"prompt": "b"})),
    ];
    let first = repo
        .enqueue_fanout(
            workflow_id,
            None,
            "image_generate",
            &instances,
            &[7],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 2);

    // 同键重入队：全部冲突跳过，返回空。
    let second = repo
        .enqueue_fanout(
            workflow_id,
            None,
            "image_generate",
            &instances,
            &[7],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
    assert!(second.is_empty());

    let rows = repo.list_all(workflow_id).await.unwrap();
    let generates: Vec<_> = rows.iter().filter(|t| t.name == "image_generate").collect();
    assert_eq!(generates.len(), 2);
}

/// 原子失败结算：有后继 → `superseded` 且后继入队（不依赖源任务）；
/// 无后继 → `failed` 并级联已有依赖，级联失败时源任务终态一起回滚。
#[tokio::test]
async fn fail_and_enqueue_marks_superseded_or_failed_atomically() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "fail_and_enqueue_atomic";
    // 有后继：superseded + 后继入队（depends_on_ids 为空）。
    repo.enqueue(&Task {
        workflow_id: workflow_id.to_string(),
        run_id: None,
        name: "commit_scene".to_string(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({"proposal": {}}),
    })
    .await
    .unwrap();
    let retried = repo
        .claim_many(workflow_id, "worker", 1)
        .await
        .unwrap()
        .remove(0);
    repo.fail_and_enqueue(
        &retried,
        "validation failed",
        &[FanoutInstances {
            name: "scene_proposal".to_string(),
            task_type: "normal".to_string(),
            instances: vec![(None, json!({"Retry": {}}))],
            depends_on_names: vec![],
        }],
        3,
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
    assert!(successor.depends_on_ids.is_empty());

    // 无后继：failed。
    let failed = repo
        .enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "audio_generate".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap();
    let failed = repo
        .claim_many(workflow_id, "worker", 10)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == failed.task_id)
        .unwrap();
    let mut dependents = Vec::new();
    for (name, run_id, by_name) in [
        ("audio_dependent", failed.run_id, false),
        ("audio_join", failed.run_id, true),
        ("other_run", retried.run_id, true),
    ] {
        let dependent = repo
            .enqueue(&Task {
                workflow_id: workflow_id.to_string(),
                run_id,
                name: name.to_string(),
                instance_id: None,
                max_attempts: 3,
                depends_on_ids: if by_name {
                    vec![]
                } else {
                    vec![failed.task_id]
                },
                depends_on_names: if by_name {
                    vec![failed.name.clone()]
                } else {
                    vec![]
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
        repo.fail_and_enqueue(&failed, "budget exhausted", &[], 3,)
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

    repo.fail_and_enqueue(&failed, "budget exhausted", &[], 3)
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
        let expected = if dependent.run_id == failed.run_id {
            TaskStatus::Failed
        } else {
            TaskStatus::Pending
        };
        assert_eq!(settled.status, expected);
    }
}

/// 批量释放：同一工作流的在跑任务全部立即回到可认领，且只影响本工作流。
#[tokio::test]
async fn release_claims_returns_every_running_task_in_the_workflow() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    async fn enqueue(repo: &PgTaskRepo, workflow_id: &str) -> TaskRecord {
        repo.enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "narration".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap()
    }
    let ours = enqueue(&repo, "release_claims_all").await;
    let other = enqueue(&repo, "release_claims_other").await;
    assert_eq!(
        repo.claim_many("release_claims_all", "worker", 5)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        repo.claim_many("release_claims_other", "worker", 5)
            .await
            .unwrap()
            .len(),
        1
    );

    assert_eq!(
        repo.release_claims("release_claims_all", "worker")
            .await
            .unwrap(),
        1
    );
    let reclaimed = repo
        .claim_many("release_claims_all", "worker", 5)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].task_id, ours.task_id);
    assert_eq!(reclaimed[0].attempt, 2, "重认领照常递增 attempt");

    // 其它工作流不受影响：它的任务租约还在（认领不到，状态仍是在跑）。
    assert!(
        repo.claim_many("release_claims_other", "worker", 5)
            .await
            .unwrap()
            .is_empty()
    );
    let untouched = repo
        .get("release_claims_other", other.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(untouched.status, TaskStatus::Running);
    assert!(untouched.lease_until.is_some());
}

#[tokio::test]
async fn claims_require_all_dependencies_including_missing_ids() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let instances = [(Some(0), json!(null)), (Some(1), json!(null))];
    // 另一个工作流的同 ID 成功记录不能满足本工作流的依赖。
    let other = repo
        .enqueue_fanout(
            "other_dependencies",
            None,
            "source",
            &[
                (Some(0), json!(null)),
                (Some(1), json!(null)),
                (Some(2), json!(null)),
            ],
            &[],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
    for claim in repo
        .claim_many("other_dependencies", "worker", 10)
        .await
        .unwrap()
    {
        repo.succeed(&claim, &json!(null)).await.unwrap();
    }
    let dependency_ids = vec![other[1].task_id, other[2].task_id];
    let workflow_id = "missing_dependencies";
    let child = repo
        .enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "child".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: dependency_ids.clone(),
            depends_on_names: vec![],
            input: json!(null),
        })
        .await
        .unwrap();
    assert!(
        repo.claim_many(workflow_id, "worker", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());

    let sources = repo
        .enqueue_fanout(
            workflow_id,
            child.run_id,
            "source",
            &instances,
            &[],
            "normal",
            &[],
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        sources.iter().map(|task| task.task_id).collect::<Vec<_>>(),
        dependency_ids
    );
    let claims = repo.claim_many(workflow_id, "worker", 10).await.unwrap();
    assert_eq!(claims.len(), 2);
    repo.succeed(&claims[0], &json!(null)).await.unwrap();
    assert!(
        repo.claim_many(workflow_id, "worker", 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(repo.executable_workflow_ids().await.unwrap().is_empty());

    repo.succeed(&claims[1], &json!(null)).await.unwrap();
    assert_eq!(repo.executable_workflow_ids().await.unwrap(), [workflow_id]);
    let claims = repo.claim_many(workflow_id, "worker", 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].task_id, child.task_id);
}

/// 恢复扫描只返回「存在可认领任务」的工作流（与 `claim_many` 同款闸门）：
/// 可认领（pending 无依赖）命中；被依赖阻塞、已全部成功、重试预算耗尽的工作流不命中。
#[tokio::test]
async fn executable_workflow_ids_only_returns_claimable_sessions() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    async fn insert_task(
        repo: &PgTaskRepo,
        workflow_id: &str,
        name: &str,
        deps: Vec<i64>,
    ) -> TaskRecord {
        repo.enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: name.to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: deps,
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap()
    }

    // 可认领：pending 且无依赖 → 命中。
    let claimable = "exec_sess_claimable";
    insert_task(&repo, claimable, "narration", vec![]).await;

    // 被依赖阻塞：后继 pending 但前驱未成功 → 不命中。先把前驱认领掉
    // （running + 租约未过期 → 前驱自身也不可认领），否则前驱的 pending 行
    // 本身就会让工作流命中，测试的「阻塞」前提不成立。
    let blocked = "exec_sess_blocked";
    let dep = insert_task(&repo, blocked, "commit_scene", vec![]).await;
    insert_task(&repo, blocked, "narration", vec![dep.task_id]).await;
    repo.claim_many(blocked, "worker", 5).await.unwrap();

    // 已全部成功：无 pending/running → 不命中。
    let done = "exec_sess_done";
    insert_task(&repo, done, "narration", vec![]).await;
    let finished = repo.claim_many(done, "worker", 1).await.unwrap().remove(0);
    repo.succeed(&finished, &json!({})).await.unwrap();

    // 预算耗尽：attempt >= max_attempts（认领三次后不再被认领）→ 不命中。
    let exhausted = "exec_sess_exhausted";
    insert_task(&repo, exhausted, "narration", vec![]).await;
    for _ in 0..3 {
        let claim = repo
            .claim_many(exhausted, "worker", 5)
            .await
            .unwrap()
            .remove(0);
        repo.release_claim(&claim).await.unwrap();
    }

    let mut sessions = repo.executable_workflow_ids().await.unwrap();
    sessions.sort();
    assert_eq!(
        sessions,
        vec![claimable.to_string()],
        "只应命中可认领会话，阻塞/成功/耗尽均不命中"
    );
}

/// 「恢复封存」清扫预算耗尽的残留行：租约过期、依赖齐备、attempt >= max_attempts
/// 的任务被全量置为 `failed`；有剩余预算的任务不受影响。
///
/// 对应线上事故形态：任务经租约过期被反复重认领刷满 attempt，进程重启后恢复扫描
/// 找不到该工作流（同款 `attempt < max_attempts` 闸门），`claim_many` 内的封存
/// 永不执行，行永久停在 running。
#[tokio::test]
async fn recovery_seal_marks_budget_exhausted_rows_failed() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());

    // 预算耗尽残留：认领 3 次刷满 attempt，模拟「租约过期被杀、无人再碰」。
    let stale = "recovery_seal_stale";
    let stale_task = repo
        .enqueue(&Task {
            workflow_id: stale.to_string(),
            run_id: None,
            name: "scene_proposal".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap();
    for attempt in 0..3 {
        repo.claim_many(stale, "worker", 5).await.unwrap();
        if attempt < 2 {
            sqlx::query(
                "UPDATE tasks SET lease_until = NOW() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2",
            )
            .bind(stale)
            .bind(stale_task.task_id)
            .execute(database.pool())
            .await
            .unwrap();
        }
    }
    let still_running = repo.get(stale, stale_task.task_id).await.unwrap().unwrap();
    assert_eq!(still_running.status, TaskStatus::Running);
    assert_eq!(still_running.attempt, 3);
    assert!(
        still_running.lease_until.is_some(),
        "行仍持有有效租约（进程存活时封存不应误伤在途执行）"
    );

    // 先模拟租约过期（如进程重启后），恢复封存才应命中。
    sqlx::query(
        "UPDATE tasks SET lease_until = NOW() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2",
    )
    .bind(stale)
    .bind(stale_task.task_id)
    .execute(database.pool())
    .await
    .unwrap();

    // 对照工作流：有剩余预算（pending, attempt=0），封存不该动它。
    let fresh = "recovery_seal_fresh";
    repo.enqueue(&Task {
        workflow_id: fresh.to_string(),
        run_id: None,
        name: "narration".to_string(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({}),
    })
    .await
    .unwrap();

    let sealed = repo.seal_budget_exhausted().await.unwrap();
    assert_eq!(sealed, 1, "只应封存滞留在可执行态的预算耗尽行");

    let settled = repo.get(stale, stale_task.task_id).await.unwrap().unwrap();
    assert_eq!(settled.status, TaskStatus::Failed);
    assert_eq!(
        settled.error.as_deref(),
        Some(BUDGET_EXHAUSTED_ERR),
        "封存错误信息与 claim_many 内封存保持一致"
    );
    assert!(settled.lease_until.is_none(), "封存应清空租约");

    // 预算充足的工作流不受影响，仍可认领。
    let claimed = repo.claim_many(fresh, "worker", 5).await.unwrap();
    assert_eq!(claimed.len(), 1);
}

/// 任务就绪通知发送点：入队 / 扇出 / 释放租约各自随提交投递，payload = workflow_id。
#[tokio::test]
async fn task_ready_notifications_carry_workflow_id() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "task_ready_notifications";
    let mut listener = PgListener::connect(database.database_url()).await.unwrap();
    listener.listen(TASK_READY_CHANNEL).await.unwrap();

    // 1) 入队（HTTP 推进根任务）。
    let root = repo
        .enqueue(&Task {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: "scene_proposal".to_string(),
            instance_id: None,
            max_attempts: 3,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!({}),
        })
        .await
        .unwrap();
    recv_ready(&mut listener, workflow_id).await;

    // 2) 扇出后继（原子结算路径同样经此通知）。
    repo.enqueue_fanout(
        workflow_id,
        root.run_id,
        "commit_scene",
        &[(None, json!({}))],
        &[root.task_id],
        "normal",
        &[],
        3,
    )
    .await
    .unwrap();
    recv_ready(&mut listener, workflow_id).await;

    // 3) 释放租约（原子结算回滚后的立即重入）。
    let claimed = repo
        .claim_many(workflow_id, "worker", 5)
        .await
        .unwrap()
        .remove(0);
    repo.release_claim(&claimed).await.unwrap();
    recv_ready(&mut listener, workflow_id).await;
}

async fn seed(repo: &PgTaskRepo, workflow_id: &str, name: &str, run_id: Option<i64>) -> TaskRecord {
    repo.enqueue(&Task {
        workflow_id: workflow_id.into(),
        run_id,
        name: name.into(),
        instance_id: None,
        max_attempts: 5,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({}),
    })
    .await
    .unwrap()
}

async fn reject_stale_writes(repo: &PgTaskRepo, task: &TaskRecord) {
    let fanouts = [FanoutInstances {
        name: "must-not-exist".into(),
        task_type: "normal".into(),
        instances: vec![(None, json!({}))],
        depends_on_names: vec![],
    }];
    assert!(matches!(
        repo.renew_claim(task).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.release_claim(task).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.succeed_and_enqueue(task, &json!("stale"), &fanouts, 3)
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.fail_and_enqueue(task, "stale", &fanouts, 3).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.fail_and_enqueue(task, "stale", &[], 3).await,
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
async fn expired_and_replaced_claims_cannot_write_or_release_a_new_owner() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    let workflow_id = "claim-fencing";
    let root = seed(&repo, workflow_id, "root", Some(7)).await;
    let old = repo
        .claim_many(workflow_id, "worker-a", 1)
        .await
        .unwrap()
        .remove(0);
    let mut dependent = Task {
        workflow_id: workflow_id.into(),
        run_id: Some(7),
        name: "dependent".into(),
        instance_id: None,
        max_attempts: 3,
        depends_on_ids: vec![root.task_id],
        depends_on_names: vec![],
        input: json!({}),
    };
    repo.enqueue(&dependent).await.unwrap();
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2")
        .bind(workflow_id).bind(old.task_id).execute(database.pool()).await.unwrap();
    reject_stale_writes(&repo, &old).await;
    let current = repo
        .claim_many(workflow_id, "worker-b", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(current.attempt, old.attempt + 1);
    reject_stale_writes(&repo, &old).await;
    // Even a matching owner cannot reuse the preceding claim generation.
    let mut wrong_attempt = current.clone();
    wrong_attempt.attempt = old.attempt;
    reject_stale_writes(&repo, &wrong_attempt).await;
    let mut wrong_owner = current.clone();
    wrong_owner.claimed_by = Some("worker-a".into());
    reject_stale_writes(&repo, &wrong_owner).await;
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
    assert_eq!(unchanged.claimed_by, current.claimed_by);
    assert_eq!(unchanged.attempt, current.attempt);
    assert_eq!(unchanged.lease_until, current.lease_until);
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() + interval '30 seconds' WHERE workflow_id = $1 AND task_id = $2")
        .bind(workflow_id).bind(current.task_id).execute(database.pool()).await.unwrap();
    repo.renew_claim(&current).await.unwrap();
    let renewed = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.attempt, current.attempt);
    assert!(renewed.lease_until.unwrap() > chrono::Utc::now() + chrono::Duration::seconds(200));
    repo.succeed(&current, &json!("current")).await.unwrap();
    reject_stale_writes(&repo, &current).await;
    let settled = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.output, Some(json!("current")));
    // Bulk cleanup is restricted to the stopped worker, including within one workflow.
    dependent.name = "another".into();
    dependent.depends_on_ids.clear();
    repo.enqueue(&dependent).await.unwrap();
    let a = repo
        .claim_many(workflow_id, "worker-a", 1)
        .await
        .unwrap()
        .remove(0);
    let b = repo
        .claim_many(workflow_id, "worker-b", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        repo.release_claims(workflow_id, "worker-a").await.unwrap(),
        1
    );
    assert!(matches!(
        repo.renew_claim(&a).await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.renew_claim(&b).await.unwrap();
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
    let repo = PgTaskRepo::new(database.pool().clone());
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
    let workflow_id = "concurrent-join";
    seed(&repo, workflow_id, "left", Some(42)).await;
    seed(&repo, workflow_id, "right", Some(42)).await;
    let left = repo
        .claim_many(workflow_id, "worker-a", 1)
        .await
        .unwrap()
        .remove(0);
    let right = repo
        .claim_many(workflow_id, "worker-b", 1)
        .await
        .unwrap()
        .remove(0);
    // Freeze ID allocation: both producers reach the missing-JOIN window concurrently.
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
            PgTaskRepo::new(alternate_pool.clone()),
        ),
    ] {
        workers.spawn(async move {
            let fanouts: Vec<_> = targets
                .into_iter()
                .map(|name| FanoutInstances {
                    name: name.into(),
                    task_type: "join".into(),
                    instances: vec![(None, json!({ source.name.clone(): source.task_id }))],
                    depends_on_names: vec!["left".into(), "right".into()],
                })
                .collect();
            repo.succeed_and_enqueue(&source, &json!(source.task_id), &fanouts, 3)
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
    for join in rows.iter().filter(|task| task.task_type == "join") {
        assert_eq!(
            join.input,
            json!({"left": left.task_id, "right": right.task_id})
        );
        let mut dependencies = join.depends_on_ids.clone();
        dependencies.sort_unstable();
        assert_eq!(dependencies, [left.task_id, right.task_id]);
    }
    assert_eq!(
        repo.claim_many(workflow_id, "join-worker", 10)
            .await
            .unwrap()
            .len(),
        2
    );
    alternate_pool.close().await;
}

#[tokio::test]
async fn a_lease_that_expires_while_waiting_for_a_row_lock_cannot_be_used() {
    let Some(database) = connect_test().await else {
        return;
    };
    let repo = PgTaskRepo::new(database.pool().clone());
    for operation in 0..4 {
        let workflow_id = format!("lease-lock-wait-{operation}");
        seed(&repo, &workflow_id, "root", None).await;
        let claim = repo
            .claim_many(&workflow_id, "worker", 1)
            .await
            .unwrap()
            .remove(0);
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
            match operation {
                0 => worker_repo.renew_claim(&claim).await,
                1 => worker_repo.release_claim(&claim).await,
                2 => worker_repo.succeed(&claim, &json!({})).await.map(|_| ()),
                _ => {
                    worker_repo
                        .fail_and_enqueue(&claim, "expired", &[], 3)
                        .await
                }
            }
        });
        wait_for_lock_waiters(database.pool(), 1).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        blocker.commit().await.unwrap();
        assert!(matches!(
            worker.await.unwrap(),
            Err(StorageError::ClaimLost { .. })
        ));
        let rows = repo.list_all(&workflow_id).await.unwrap();
        assert_eq!(rows[0].status, TaskStatus::Running);
        assert!(rows[0].output.is_none());
    }
}
