use std::time::Duration;

use serde_json::json;

use super::super::test_support::connect_test;
use super::{BUDGET_EXHAUSTED_ERR, LEASE_EXPIRED_ERR};
use super::{PgTaskRepo, ROW_COLS, TaskRow, advance_joins, scope};
use crate::backend::task_fixture::{TaskSeed, join_plan, plan, scoped_plan};
use crate::error::StorageError;
use crate::graph::{
    Graph,
    TaskType::{Event, Join, Normal},
};
use crate::task::{Dispatch, MAX_ATTEMPTS, SettlementRules, Successor, TaskStore};
use crate::{TaskRecord, TaskStatus};

#[path = "graph_fixture.rs"]
mod graph_fixture;
use graph_fixture::graph as build_graph;

impl PgTaskRepo {
    /// Seed an arbitrary run and scope for scheduler and settlement tests.
    async fn seed_task<Ctx>(
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
        let path = graph.subgraph_path(&task.name).unwrap();
        let rules = graph.settlement_rules();
        let collecting = rules.join_rules.contains_key(&task.name)
            && (seed.run_id.is_some() || !seed.parent_task_ids.is_empty());
        let status = if collecting {
            TaskStatus::Collecting
        } else {
            TaskStatus::Pending
        };

        let mut tx = self.pool.begin().await?;
        let task_id: i64 = sqlx::query_scalar(
            "INSERT INTO tasks (workflow_id, name, status, input_json, parent_task_ids)
             VALUES ($1, $2, $3, $4, $5) RETURNING task_id",
        )
        .bind(&task.workflow_id)
        .bind(&task.name)
        .bind(status.as_str())
        .bind(&task.input)
        .bind(&seed.parent_task_ids)
        .fetch_one(&mut *tx)
        .await?;
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
        sqlx::query("UPDATE tasks SET run_id=$2, scope_id=$3 WHERE task_id=$1")
            .bind(task_id)
            .bind(run_id)
            .bind(scope_id)
            .execute(&mut *tx)
            .await?;
        if collecting {
            advance_joins(&mut tx, &task.workflow_id, run_id, &rules.join_rules).await?;
        }
        tx.commit().await?;
        Ok(self.get(&task.workflow_id, task_id).await?.unwrap())
    }
    async fn list_all(&self, workflow_id: &str) -> Result<Vec<TaskRecord>, StorageError> {
        let sql = format!("SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 ORDER BY task_id");
        let rows = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(TaskRecord::from).collect())
    }

    async fn get(
        &self,
        workflow_id: &str,
        task_id: i64,
    ) -> Result<Option<TaskRecord>, StorageError> {
        let sql = format!("SELECT {ROW_COLS} FROM tasks WHERE workflow_id = $1 AND task_id = $2");
        let row = sqlx::query_as::<_, TaskRow>(&sql)
            .bind(workflow_id)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(TaskRow::into))
    }
}

#[tokio::test]
async fn submitting_entries_allocates_independent_runs_and_persists_their_inputs() {
    let Some(db) = connect_test().await else {
        return;
    };
    let workflow_id = "enqueue-runs";
    let pool = db.pool();
    let graph = build_graph(workflow_id, &[("root", Event, None)], &[]);
    let task = graph.describe().task("root", json!({"input": 1})).unwrap();
    let database = crate::Database::from(pool.clone());
    let first = database.submit(task.clone()).await.unwrap();
    let second = database.submit(task.clone()).await.unwrap();
    assert_ne!(first.run_id, second.run_id);
    let repo = PgTaskRepo::new(pool.clone());
    for run in [first, second] {
        let row = repo.get(workflow_id, run.run_id).await.unwrap().unwrap();
        assert_eq!(row.run_id, row.task_id);
        assert_eq!(row.workflow_id, run.workflow_id);
        assert_eq!(row.scope_id, None);
        assert!(row.parent_task_ids.is_empty());
        assert_eq!(row.input, task.input);
        assert_eq!(row.status, TaskStatus::Pending);
        assert_eq!(row.attempt, 0);
    }
}

#[tokio::test]
async fn submitted_tasks_follow_workflow_and_claim_attempt() {
    let Some(database) = connect_test().await else {
        return;
    };
    let pool = database.pool();
    let workflow_id = "distributed";
    let graph = build_graph(
        workflow_id,
        &[
            ("analysis.entry", Normal, Some("analysis")),
            ("finish", Normal, None),
        ],
        &[("analysis.entry", "finish")],
    );
    let mut tx = pool.begin().await.unwrap();
    use crate::Database;
    let task = graph.describe().task("analysis.entry", json!(1)).unwrap();
    let submitted = Database::in_transaction(&mut tx)
        .submit(task)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let other_id = "distributed-other";
    let rules = graph.settlement_rules();
    let repo = PgTaskRepo::new(pool.clone());
    let replacement = PgTaskRepo::new(pool.clone());
    assert!(repo.claim_many(other_id, 1).await.unwrap().is_empty());
    let root = repo
        .claim_many(workflow_id, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(root.run_id, submitted.run_id);
    assert!(root.scope_id.is_some());
    assert!(
        replacement
            .claim_many(workflow_id, 1)
            .await
            .unwrap()
            .is_empty()
    );
    repo.release_claim(&root).await.unwrap();
    let recovered = replacement
        .claim_many(workflow_id, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovered.attempt, root.attempt + 1);
    assert!(repo.renew_claim(&root).await.unwrap().is_none());
    assert!(repo.release_claim(&root).await.is_err());
    assert!(
        repo.settle_success(&root, &json!(1), &[], rules)
            .await
            .is_err()
    );
    replacement
        .settle_success(&recovered, &json!(1), &[plan("finish", vec![json!(1)])], rules)
        .await
        .unwrap();
    assert!(repo.claim_many(other_id, 1).await.unwrap().is_empty());
    let child = repo
        .claim_many(workflow_id, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(child.run_id, root.run_id);
    assert_eq!(child.scope_id, None);
}

/// 原子成功结算：succeed + 全部后继（单条及批量输入）在同一事务内落库，
/// 后继继承源任务 run_id、parent_task_ids 记录源任务。
#[tokio::test]
async fn succeed_and_enqueue_settles_and_fans_out_atomically() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "succeed_and_enqueue_atomic";
    let graph = build_graph(
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
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    repo.seed_task(
        graph.as_ref(),
        &TaskSeed::new(workflow_id, "image_prompt", json!({"prompts": ["a", "b"]})),
    )
    .await
    .unwrap();
    let source = repo.claim_many(workflow_id, 5).await.unwrap().remove(0);

    let rules = graph.settlement_rules();
    repo.settle_success(
        &source,
        &json!({"prompts": ["a", "b"]}),
        &[
            plan("finalize_node", vec![json!({})]),
            scoped_plan(
                "images.image_generate",
                &["images"],
                vec![json!({"prompt": "a"}), json!({"prompt": "b"})],
            ),
            Successor {
                dispatch: Dispatch::PerInputScope,
                ..scoped_plan(
                    "images.image_generate",
                    &["images"],
                    vec![json!({"prompt": "c"})],
                )
            },
        ],
        rules,
    )
    .await
    .unwrap();

    // 源任务已成功结算且租约释放。
    let rows = repo.list_all(workflow_id).await.unwrap();
    let settled = rows
        .iter()
        .find(|task| task.task_id == source.task_id)
        .unwrap();
    assert_eq!(settled.status, TaskStatus::Succeeded);
    assert_eq!(settled.output, Some(json!({"prompts": ["a", "b"]})));
    assert!(settled.lease_until.is_none());
    reject_stale_writes(&repo, rules, &source).await;

    // 同一目标的普通跨入 map 共享实例，foreach 的元素建立独立实例。
    assert_eq!(rows.len(), 5);
    assert_eq!(source.run_id, source.task_id);
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
        let graph = build_graph(
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
        );
        let repo = PgTaskRepo::new(database.pool().clone());
        let mut previous = vec![];
        for _ in 0..2 {
            let root = repo
                .seed_task(
                    graph.as_ref(),
                    &TaskSeed::new(&workflow_id, "root", json!({})),
                )
                .await
                .unwrap();
            let claims = repo.claim_many(&workflow_id, 20).await.unwrap();
            let source = claims
                .iter()
                .find(|task| task.task_id == root.task_id)
                .unwrap();
            let inputs: Vec<_> = (0..count).map(|item| json!({"item": item})).collect();
            let mut fanout = if foreach {
                scoped_plan("images.image_generate", &["images"], inputs.clone())
            } else {
                plan("image_generate", inputs.clone())
            };
            if foreach {
                fanout.dispatch = Dispatch::PerInputScope;
            }
            repo.settle_success(source, &json!({}), &[fanout], graph.settlement_rules())
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
    let graph = build_graph(
        workflow_id,
        &[("source", Normal, None), ("collect", Join, None)],
        &[("source", "collect")],
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    seed(&repo, graph.as_ref(), "source", Some(42)).await;
    seed(&repo, graph.as_ref(), "source", Some(42)).await;
    // Both keys arrive in one plan. A per-input INSERT or UPDATE exposes a partial contribution.
    sqlx::query(
        "ALTER TABLE tasks ADD CHECK (
            name <> 'collect' OR (input_json->'source') IS NOT DISTINCT FROM (input_json->'extra')
        )",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let sources = repo.claim_many(workflow_id, 2).await.unwrap();
    assert_eq!(sources.len(), 2);
    let rules = graph.settlement_rules();
    for (index, source) in sources.iter().enumerate() {
        let contribution = json!({"source": source.task_id});
        repo.settle_success(
            source,
            &contribution,
            &[join_plan(
                "collect",
                vec![contribution.clone(), json!({"extra": source.task_id})],
            )],
            rules,
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
    assert_eq!(join.status, TaskStatus::Pending);
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
    let graph = build_graph(
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
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    // 有后继：superseded + 后继入队，并保留源任务。
    repo.seed_task(
        graph.as_ref(),
        &TaskSeed::new(workflow_id, "commit_scene", json!({"proposal": {}})),
    )
    .await
    .unwrap();
    let rules = graph.settlement_rules();
    let retried = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.settle_failure(
        &retried,
        "validation failed",
        &[plan("scene_proposal", vec![json!({"Retry": {}})])],
        rules,
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
        .seed_task(
            graph.as_ref(),
            &TaskSeed::new(workflow_id, "audio_generate", json!({})),
        )
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
        ("audio_dependent", Some(failed.run_id), false),
        ("audio_join", Some(failed.run_id), true),
        ("other_run", Some(retried.run_id), true),
    ] {
        let dependent = repo
            .seed_task(
                graph.as_ref(),
                &TaskSeed {
                    run_id,
                    parent_task_ids: if is_join {
                        vec![]
                    } else {
                        vec![failed.task_id]
                    },
                    ..TaskSeed::new(workflow_id, name, json!({}))
                },
            )
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
        repo.settle_failure(&failed, "budget exhausted", &[], rules)
            .await
            .is_err()
    );
    for task in std::iter::once(&failed).chain(&dependents) {
        let unchanged = repo.get(workflow_id, task.task_id).await.unwrap().unwrap();
        assert_eq!(unchanged.status, task.status);
    }
    sqlx::query("ALTER TABLE tasks DROP CONSTRAINT reject_cascade")
        .execute(database.pool())
        .await
        .unwrap();

    repo.settle_failure(&failed, "budget exhausted", &[], rules)
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
            dependent.status
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
    let graph = build_graph("release_ours", &[("narration", Normal, None)], &[]);
    let repo = PgTaskRepo::new(database.pool().clone());
    let other_graph = build_graph("release_other", &[("narration", Normal, None)], &[]);
    let ours = seed(&repo, graph.as_ref(), "narration", None).await;
    let other = seed(&repo, other_graph.as_ref(), "narration", None).await;
    let our_claim = repo.claim_many("release_ours", 1).await.unwrap().remove(0);
    let _other_claim = repo.claim_many("release_other", 1).await.unwrap().remove(0);

    repo.release_claim(&our_claim).await.unwrap();
    let reclaimed = repo.claim_many("release_ours", 5).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].task_id, ours.task_id);
    assert_eq!(reclaimed[0].attempt, 2, "重认领照常递增 attempt");

    // 其它工作流不受影响：它的任务租约还在（认领不到，状态仍是在跑）。
    assert!(
        repo.claim_many("release_other", 5)
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

/// 预算耗尽后重新认领，只允许原子失败结算；旧认领不能修改恢复者或创建失败后继。
#[tokio::test]
async fn exhausted_claims_recover_through_atomic_failure_settlement() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "exhausted-recovery";
    let graph = build_graph(
        workflow_id,
        &[
            ("source", Normal, None),
            ("handle_failure", Normal, None),
            ("invalid", Normal, None),
        ],
        &[],
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    let input = TaskSeed::new(workflow_id, "source", json!({}));
    let root = repo.seed_task(graph.as_ref(), &input).await.unwrap();
    for _ in 1..MAX_ATTEMPTS {
        let previous = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
        repo.release_claim(&previous).await.unwrap();
    }
    let old = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(old.attempt, MAX_ATTEMPTS);
    assert!(repo.claim_many(workflow_id, 1).await.unwrap().is_empty());
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1")
        .bind(workflow_id).execute(database.pool()).await.unwrap();
    let recovery = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(recovery.task_id, root.task_id);
    assert_eq!(recovery.attempt, MAX_ATTEMPTS + 1);
    assert_eq!(recovery.status, TaskStatus::Running);
    assert_eq!(recovery.error.as_deref(), Some(LEASE_EXPIRED_ERR));
    let rules = graph.settlement_rules();
    reject_stale_writes(&repo, rules, &old).await;
    assert!(matches!(
        repo.settle_success(&recovery, &json!("forbidden"), &[], rules)
            .await,
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
        repo.settle_failure(
            &recovery,
            LEASE_EXPIRED_ERR,
            &[fanout.clone(), invalid],
            rules,
        )
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
    reject_stale_writes(&repo, rules, &recovery).await;
    repo.settle_failure(&next, LEASE_EXPIRED_ERR, &[fanout], rules)
        .await
        .unwrap();
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, TaskStatus::Superseded);
    assert_eq!(rows[0].error.as_deref(), Some(LEASE_EXPIRED_ERR));
    assert!(rows[0].lease_until.is_none());
    assert_eq!(rows[1].status, TaskStatus::Pending);
    assert_eq!(rows[1].parent_task_ids, [root.task_id]);
    reject_stale_writes(&repo, rules, &next).await;

    // 无执行预算的任务也通过失败结算传播到等待它的 JOIN，不会被直接封存。
    let graph = build_graph(
        "zero-budget",
        &[("source", Normal, None), ("dependent", Join, None)],
        &[("source", "dependent")],
    );
    let input = TaskSeed::new("zero-budget", "source", json!({}));
    let zero = repo.seed_task(graph.as_ref(), &input).await.unwrap();
    let mut child = TaskSeed::new("zero-budget", "dependent", json!({}));
    child.run_id = Some(zero.run_id);
    let child = repo.seed_task(graph.as_ref(), &child).await.unwrap();
    for _ in 0..MAX_ATTEMPTS {
        let previous = repo.claim_many("zero-budget", 1).await.unwrap().remove(0);
        repo.release_claim(&previous).await.unwrap();
    }
    let claim = repo.claim_many("zero-budget", 1).await.unwrap().remove(0);
    assert_eq!(claim.attempt, MAX_ATTEMPTS + 1);
    assert_eq!(claim.error.as_deref(), Some(BUDGET_EXHAUSTED_ERR));
    repo.settle_failure(
        &claim,
        BUDGET_EXHAUSTED_ERR,
        &[],
        graph.settlement_rules(),
    )
    .await
    .unwrap();
    for task_id in [zero.task_id, child.task_id] {
        let settled = repo.get("zero-budget", task_id).await.unwrap().unwrap();
        assert_eq!(settled.status, TaskStatus::Failed);
    }
}

async fn seed(
    repo: &PgTaskRepo,
    graph: &Graph<()>,
    name: &str,
    run_id: Option<i64>,
) -> TaskRecord {
    repo.seed_task(
        graph,
        &TaskSeed {
            run_id,
            ..TaskSeed::new(graph.workflow_id(), name, json!({}))
        },
    )
    .await
    .unwrap()
}

async fn reject_stale_writes(repo: &PgTaskRepo, rules: &SettlementRules, task: &TaskRecord) {
    let fanouts = [plan("must-not-exist", vec![json!({})])];
    assert!(repo.renew_claim(task).await.unwrap().is_none());
    assert!(matches!(
        repo.release_claim(task).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.settle_success(task, &json!("stale"), &fanouts, rules)
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.settle_failure(task, "stale", &fanouts, rules).await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.settle_success(task, &json!("stale"), &[], rules)
            .await,
        Err(StorageError::ClaimLost { .. })
    ));
    assert!(matches!(
        repo.settle_failure(task, "stale", &[], rules).await,
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
        let graph = build_graph(&workflow_id, &[("root", Normal, None)], &[]);
        let repo = PgTaskRepo::new(database.pool().clone());
        let task = TaskSeed::new(&workflow_id, "root", json!({}));

        let pending = repo.seed_task(graph.as_ref(), &task).await.unwrap();
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
        let rules = graph.settlement_rules();
        if previous_attempts == MAX_ATTEMPTS {
            repo.settle_failure(&claim, "budget exhausted", &[], rules)
                .await
                .unwrap();
        } else {
            repo.settle_success(&claim, &json!("done"), &[], rules)
                .await
                .unwrap();
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
    let graph = build_graph(
        workflow_id,
        &[
            ("root", Normal, None),
            ("dependent", Join, None),
            ("another", Join, None),
        ],
        &[("root", "dependent"), ("root", "another")],
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    seed(&repo, graph.as_ref(), "root", Some(7)).await;
    let old = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let mut dependent = TaskSeed {
        run_id: Some(7),
        ..TaskSeed::new(workflow_id, "dependent", json!({}))
    };
    repo.seed_task(graph.as_ref(), &dependent).await.unwrap();
    sqlx::query("UPDATE tasks SET lease_until = clock_timestamp() - interval '1 second' WHERE workflow_id = $1 AND task_id = $2")
        .bind(workflow_id).bind(old.task_id).execute(database.pool()).await.unwrap();
    let rules = graph.settlement_rules();
    reject_stale_writes(&repo, rules, &old).await;
    let current = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    assert_eq!(current.attempt, old.attempt + 1);
    reject_stale_writes(&repo, rules, &old).await;
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows.len(), 2, "stale settlement cannot create successors");
    assert_eq!(
        rows[1].status,
        TaskStatus::Collecting,
        "stale failure cannot cascade"
    );
    let unchanged = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.attempt, current.attempt);
    assert_eq!(unchanged.lease_until, current.lease_until);
    repo.settle_success(&current, &json!("current"), &[], rules)
        .await
        .unwrap();
    reject_stale_writes(&repo, rules, &current).await;
    let settled = repo
        .get(workflow_id, current.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.output, Some(json!("current")));
    // Releasing one task leaves the other claim valid.
    dependent.task.name = "another".into();
    repo.seed_task(graph.as_ref(), &dependent).await.unwrap();
    let a = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let b = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.release_claim(&a).await.unwrap();
    assert!(matches!(
        repo.release_claim(&a).await,
        Err(StorageError::ClaimLost { .. })
    ));
    repo.settle_success(&b, &json!("unaffected"), &[], rules)
        .await
        .unwrap();
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
async fn tasks_without_joins_can_settle_independently_in_the_same_run() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "independent-settlement";
    let graph = build_graph(workflow_id, &[("work", Normal, None)], &[]);
    let repo = PgTaskRepo::new(database.pool().clone());
    let first = seed(&repo, graph.as_ref(), "work", None).await;
    seed(&repo, graph.as_ref(), "work", Some(first.run_id)).await;
    let claims = repo.claim_many(workflow_id, 2).await.unwrap();
    assert_eq!(claims.len(), 2);

    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id = $1 FOR UPDATE")
        .bind(claims[0].task_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let rules = graph.settlement_rules();
    let blocked = repo.settle_failure(&claims[0], "failed", &[], rules);
    let independent = async {
        wait_for_lock_waiters(database.pool(), 1).await;
        tokio::time::timeout(
            Duration::from_secs(5),
            repo.settle_success(&claims[1], &json!("done"), &[], rules),
        )
        .await
        .expect("second task must settle while the first task is still locked")
        .unwrap();
        blocker.commit().await.unwrap();
    };
    let (result, ()) = tokio::join!(blocked, independent);
    result.unwrap();
    let rows = repo.list_all(workflow_id).await.unwrap();
    assert_eq!(rows[0].status, TaskStatus::Failed);
    assert_eq!(rows[1].status, TaskStatus::Succeeded);
}

#[tokio::test]
async fn concurrent_first_fanins_merge_once_even_with_opposite_successor_order() {
    let Some(database) = connect_test().await else {
        return;
    };
    let workflow_id = "concurrent-join";
    let graph = build_graph(
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
    );
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
    seed(&repo, graph.as_ref(), "left", Some(42)).await;
    seed(&repo, graph.as_ref(), "right", Some(42)).await;
    let left = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    let right = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    // Hold the source rows until both settlements are waiting to create the first JOIN.
    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE workflow_id = $1 FOR UPDATE")
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
        let rules = graph.settlement_rules().clone();
        workers.spawn(async move {
            let fanouts: Vec<_> = targets
                .into_iter()
                .map(|name| join_plan(name, vec![json!({ source.name.clone(): source.task_id })]))
                .collect();
            repo.settle_success(&source, &json!(source.task_id), &fanouts, &rules)
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
        assert_eq!(join.status, TaskStatus::Pending);
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
        let graph = build_graph(&workflow_id, &[("root", Normal, None)], &[]);
        let repo = PgTaskRepo::new(database.pool().clone());
        seed(&repo, graph.as_ref(), "root", None).await;
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
        let rules = graph.settlement_rules().clone();
        let worker = tokio::spawn(async move {
            let result = match operation {
                0 => worker_repo.release_claim(&claim).await,
                1 => {
                    worker_repo
                        .settle_success(&claim, &json!({}), &[], &rules)
                        .await
                }
                2 => {
                    worker_repo
                        .settle_failure(&claim, "expired", &[], &rules)
                        .await
                }
                _ => {
                    return worker_repo.renew_claim(&claim).await.unwrap().is_none();
                }
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
    let graph = build_graph(
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
    );
    let repo = PgTaskRepo::new(database.pool().clone());
    seed(&repo, graph.as_ref(), "root", Some(42)).await;
    let rules = graph.settlement_rules();
    let root = repo.claim_many(workflow_id, 1).await.unwrap().remove(0);
    repo.settle_success(
        &root,
        &json!(["a", "b", "c"]),
        &[Successor {
            dispatch: Dispatch::PerInputScope,
            ..scoped_plan(
                "analysis.entry",
                &["analysis"],
                vec![json!("a"), json!("b"), json!("c")],
            )
        }],
        rules,
    )
    .await
    .unwrap();
    let scopes = repo.claim_many(workflow_id, 3).await.unwrap();
    for scope in &scopes {
        let fanouts = ["analysis.left", "analysis.right"]
            .map(|dst| scoped_plan(dst, &["analysis"], vec![scope.input.clone()]));
        repo.settle_success(scope, &scope.input, &fanouts, rules)
            .await
            .unwrap();
    }
    let branches = repo.claim_many(workflow_id, 6).await.unwrap();
    assert_eq!(branches.len(), 6);
    assert!(branches.iter().all(|task| task.run_id == 42));
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
    let contribution = |task: &TaskRecord| Successor {
        dispatch: Dispatch::Join,
        ..scoped_plan(
            "analysis.join",
            &["analysis"],
            vec![json!({task.name.rsplit('.').next().unwrap(): task.input})],
        )
    };
    for item in ["a", "b"] {
        let left = branch(item, "analysis.left");
        repo.settle_success(left, &left.input, &[contribution(left)], rules)
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
    repo.settle_success(right_a, &right_a.input, &[contribution(right_a)], rules)
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
    repo.settle_success(
        &ready[0],
        &ready[0].input,
        &[scoped_plan(
            "analysis.report",
            &["analysis"],
            vec![ready[0].input.clone()],
        )],
        rules,
    )
    .await
    .unwrap();

    // Branch b already has a waiting JOIN; branch c's JOIN arrives after failure.
    for item in ["b", "c"] {
        repo.settle_failure(branch(item, "analysis.right"), "failed signal", &[], rules)
            .await
            .unwrap();
    }
    let left_c = branch("c", "analysis.left");
    repo.settle_success(left_c, &left_c.input, &[contribution(left_c)], rules)
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
