//! SQLite 任务就绪轮询。

use std::time::Duration;

use super::SqliteTaskRepo;
use crate::SchedulerPool;

/// 启动时立即恢复，随后按 `poll` 周期封存预算耗尽的任务并唤醒就绪工作流。
///
/// 扫描数据库，因此能发现其他连接/进程写入的任务，也能恢复租约到期的任务。
/// 可将此 future 交给 `tokio::spawn`，停止服务时中止其句柄。
/// `poll` 为零时按 1ms 处理，避免忙循环。
pub async fn run_task_ready_listener(repo: SqliteTaskRepo, pool: SchedulerPool, poll: Duration) {
    let mut interval = tokio::time::interval(poll.max(Duration::from_millis(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = repo.seal_budget_exhausted().await {
            tracing::warn!(%error, "sqlite recovery failed to seal exhausted tasks");
        }
        match repo.executable_workflow_ids().await {
            Ok(workflow_ids) => {
                for workflow_id in workflow_ids {
                    pool.ensure_worker(&workflow_id).notify_one();
                }
            }
            Err(error) => tracing::warn!(%error, "sqlite task ready scan failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::sync::mpsc;

    use super::*;
    use crate::sqlite_backend::{NewTask, migrate, status};

    #[tokio::test]
    async fn polling_recovers_existing_and_later_work_and_seals_exhaustion() {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        migrate(&db).await.unwrap();
        let repo = SqliteTaskRepo::new(db.clone());
        let task = |workflow_id: &str, max_attempts| NewTask {
            workflow_id: workflow_id.into(),
            run_id: None,
            name: "root".into(),
            instance_id: None,
            max_attempts,
            depends_on_ids: vec![],
            depends_on_names: vec![],
            input: json!(null),
        };
        repo.enqueue(&task("existing", 3)).await.unwrap();
        let exhausted = repo.enqueue(&task("exhausted", 0)).await.unwrap();
        let (send, mut recv) = mpsc::unbounded_channel();
        let workers = SchedulerPool::new(move |id, wake| {
            let send = send.clone();
            async move {
                wake.notified().await;
                send.send(id).unwrap();
                std::future::pending::<()>().await;
            }
        });
        let listener = tokio::spawn(run_task_ready_listener(
            repo.clone(),
            workers.clone(),
            Duration::from_millis(10),
        ));
        let first = tokio::time::timeout(Duration::from_secs(2), recv.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first, "existing");
        assert_eq!(
            repo.get("exhausted", exhausted.task_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            status::FAILED
        );
        repo.enqueue(&task("later", 3)).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), recv.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second, "later");
        listener.abort();
        let _ = listener.await;
        workers.evict("existing").await;
        workers.evict("later").await;
        db.close().await;
    }
}
