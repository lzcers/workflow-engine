//! PostgreSQL `LISTEN/NOTIFY` 跨进程唤醒监听器（完整封装连接重试与通道细节）。

use std::{sync::Arc, time::Duration};

use sqlx::{PgPool, postgres::PgListener};
use tokio::{task::JoinHandle, time::sleep};
use tracing::warn;

use super::PG_NOTIFY_CHANNEL;

/// 启动后台 `PgListener` 监听任务；每当收到唤醒通知调用 `on_wake(workflow_id)`，
/// 若回调返回 `false`（调用方已释放）则自动退出监听循环。
pub(crate) fn spawn_wake_listener(
    pool: PgPool,
    on_wake: Arc<dyn Fn(&str) -> bool + Send + Sync>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut listener = match PgListener::connect_with(&pool).await {
                Ok(listener) => listener,
                Err(error) => {
                    warn!(error = %error, "pg listener connect failed; retrying");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            if let Err(error) = listener.listen(PG_NOTIFY_CHANNEL).await {
                warn!(error = %error, "pg listen channel failed; retrying");
                sleep(Duration::from_secs(2)).await;
                continue;
            }
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        if !on_wake(notification.payload()) {
                            return;
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "pg listener disconnected; reconnecting");
                        sleep(Duration::from_secs(1)).await;
                        break;
                    }
                }
            }
        }
    })
}
