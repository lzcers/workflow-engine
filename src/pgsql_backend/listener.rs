//! LISTEN/NOTIFY 任务就绪监听与恢复扫描。
//!
//! 与业务无关：按 payload（scope 键）唤醒 worker、连接成功即恢复扫描。
//! worker 生命周期管理由 [`crate::SchedulerPool`] 承担，是否唤醒由上层组合。

use super::repo::{PgTaskRepo, TASK_READY_CHANNEL};
use crate::SchedulerPool;

/// 任务就绪监听断线/连接失败后的重试退避。
const LISTENER_RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// 进程级任务就绪监听：单连接 `LISTEN/NOTIFY`，按 payload（workflow_id）唤醒对应调度器。
///
/// 通知是提示语义（可丢失、可重复）：断线期间的丢失由重连后的恢复扫描兜底，重复/竞态由
/// 「空认领即回去等」吸收，因此本任务只需尽力而为，无需保证送达。
/// 每个 payload 都先 `ensure` 再 `notify`：该工作流尚无 worker（如本进程未服务过它）时
/// 由 `ensure` 拉起，已有 worker 时 `notify` 唤醒——入队与拉活由同一信号驱动。
/// 恢复扫描在**每次连接成功**（含首次启动与断线重连）后各执行一次，兜底进程重启残留
/// 与断线窗口内丢失、且不会重放的通知——这是唯一的恢复路径。
pub async fn run_task_ready_listener(
    database_url: String,
    pool: SchedulerPool,
    recovery_repo: PgTaskRepo,
) {
    loop {
        let mut listener = match sqlx::postgres::PgListener::connect(&database_url).await {
            Ok(listener) => listener,
            Err(e) => {
                tracing::error!(error = %e, "task ready listener connect failed; retrying");
                tokio::time::sleep(LISTENER_RECONNECT_BACKOFF).await;
                continue;
            }
        };
        if let Err(e) = listener.listen(TASK_READY_CHANNEL).await {
            tracing::error!(error = %e, "task ready listener listen failed; retrying");
            tokio::time::sleep(LISTENER_RECONNECT_BACKOFF).await;
            continue;
        }
        // 连接成功（首次启动或断线重连）即恢复扫描：把当前「可认领」的工作流 worker 拉起，
        // 兜底监听（重）连接前丢失的通知（进程重启残留 / 断线窗口）。在独立协程执行，
        // 不阻塞本监听器立即消费通知；`ensure_worker` 幂等，重复/并发扫描无害。
        tokio::task::spawn(recover_stalled_sessions(
            recovery_repo.clone(),
            pool.clone(),
        ));
        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let workflow_id = notification.payload();
                    // 确保 worker 在运行并唤醒：get-or-create 返回唤醒信号，组合 notify_one。
                    pool.ensure_worker(workflow_id).notify_one();
                }
                Err(e) => {
                    tracing::warn!(error = %e, "task ready listener connection lost; reconnecting");
                    break;
                }
            }
        }
    }
}

/// 恢复扫描：先全量封存「预算耗尽但可执行」的残留任务，再拉起有可认领任务的工作流。
///
/// 由任务就绪监听器在**每次连接成功**（首次启动与断线重连）时 spawn 一次。只扫可认领
/// 工作流（同 `claim_many` 闸门），不会把被依赖/JOIN 闸门阻塞（如等待外部输入）的工作流
/// 拉活造成 worker 空转。
///
/// 封存一步必须先于可认领扫描：预算耗尽行（`attempt >= max_attempts`）被认领闸门与
/// 本扫描共同排除，若不在此封存，其所在工作流在进程重启后既不会被拉起 worker、
/// 也等不到 `claim_many` 内的封存，行会永久停留在 pending/running（悬挂）。
async fn recover_stalled_sessions(repo: PgTaskRepo, pool: SchedulerPool) {
    match repo.seal_budget_exhausted().await {
        Ok(sealed) if sealed > 0 => {
            tracing::info!(sealed, "recovery sealed budget-exhausted tasks");
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "recovery seal budget-exhausted tasks failed");
        }
    }
    match repo.executable_workflow_ids().await {
        Ok(workflow_ids) => {
            for workflow_id in workflow_ids {
                pool.ensure_worker(&workflow_id);
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "recovery scan query failed");
        }
    }
}
