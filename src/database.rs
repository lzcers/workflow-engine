//! 数据库连接、引擎表初始化、健康检查与事务任务提交入口。

use std::{sync::Arc, time::Duration};

use crate::error::StorageError;
use crate::task::{RunHandle, Task, TaskStore};

/// 工作流数据库。默认共享连接池，也可以借用宿主事务来提交任务。
#[derive(Clone)]
pub struct Database<C = DatabasePool>(C);

/// 通过数据库 URL 选择的共享连接池。
#[derive(Clone)]
pub enum DatabasePool {
    #[cfg(feature = "pg")]
    Postgres(sqlx::PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

#[cfg(feature = "pg")]
impl From<sqlx::PgPool> for Database {
    fn from(pool: sqlx::PgPool) -> Self {
        Self(DatabasePool::Postgres(pool))
    }
}

#[cfg(feature = "sqlite")]
impl From<sqlx::SqlitePool> for Database {
    fn from(pool: sqlx::SqlitePool) -> Self {
        Self(DatabasePool::Sqlite(pool))
    }
}

#[cfg(feature = "pg")]
impl Database<&mut sqlx::Transaction<'_, sqlx::Postgres>> {
    /// 在宿主事务内创建独立运行，同时建立入口所属的隔离作用域并发送 PG NOTIFY。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        crate::backend::pgsql::submit_in_tx(self.0, task).await
    }
}

#[cfg(feature = "sqlite")]
impl Database<&mut sqlx::Transaction<'_, sqlx::Sqlite>> {
    /// 提交独立运行的入口；调用方使用 `BEGIN IMMEDIATE` 事务可避免读升写触发 `SQLITE_BUSY`。
    pub async fn submit(self, task: Task) -> Result<RunHandle, StorageError> {
        crate::backend::sqlite::submit_in_tx(self.0, task).await
    }
}

impl Database {
    /// 在宿主已有的 SQLx 事务上提交任务，借用在提交任务后结束。
    ///
    /// 使用 SQLite 且同一事务内先读后写时，建议通过 `pool.begin_with("BEGIN IMMEDIATE")` 开启事务。
    pub fn in_transaction<'a, 'c, DB: sqlx::Database>(
        tx: &'a mut sqlx::Transaction<'c, DB>,
    ) -> Database<&'a mut sqlx::Transaction<'c, DB>> {
        Database(tx)
    }

    /// 访问共享的 SQLx 连接池，可用于开启包含宿主业务写入的事务。
    pub fn pool(&self) -> &DatabasePool {
        &self.0
    }

    /// 根据 URL 连接 PostgreSQL 或 SQLite，不创建引擎表、不启动 worker。
    ///
    /// 需要自定义连接池参数时，直接构造 `PgPool`/`SqlitePool` 并调用 `Database::from(pool)`。
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let timeout = Duration::from_secs(5);
        match url.split_once(':').map(|(scheme, _)| scheme) {
            Some("postgres" | "postgresql") => {
                #[cfg(feature = "pg")]
                {
                    let pool = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(10)
                        .acquire_timeout(timeout)
                        .connect(url)
                        .await?;
                    Ok(Self::from(pool))
                }
                #[cfg(not(feature = "pg"))]
                {
                    Err(StorageError::BackendDisabled {
                        backend: "postgresql",
                    })
                }
            }
            Some("sqlite") => {
                #[cfg(feature = "sqlite")]
                {
                    use sqlx::sqlite::{
                        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
                    };

                    let options: SqliteConnectOptions = url.parse()?;
                    let database_and_params = url
                        .trim_start_matches("sqlite://")
                        .trim_start_matches("sqlite:");
                    let (database_name, params) = database_and_params
                        .split_once('?')
                        .unwrap_or((database_and_params, ""));
                    let mut transient = database_name == ":memory:"
                        || options.get_filename().as_os_str().is_empty()
                        || matches!(
                            options.get_filename().to_str(),
                            Some(":memory:" | "file::memory:")
                        );
                    let mut explicit_mode = false;
                    let mut read_only = false;
                    let mut immutable = false;
                    for (key, value) in form_urlencoded::parse(params.as_bytes()) {
                        match key.as_ref() {
                            "mode" => {
                                explicit_mode = true;
                                transient |= value == "memory";
                                read_only |= value == "ro";
                            }
                            "immutable" => immutable = matches!(value.as_ref(), "true" | "1"),
                            _ => {}
                        }
                    }
                    let mut options = options.busy_timeout(timeout);
                    let mut pool_options = SqlitePoolOptions::new().acquire_timeout(timeout);
                    if transient {
                        pool_options = pool_options
                            .max_connections(1)
                            .idle_timeout(None)
                            .max_lifetime(None)
                            .test_before_acquire(false);
                    } else {
                        pool_options = pool_options.max_connections(4);
                        if !read_only && !immutable {
                            options = options.journal_mode(SqliteJournalMode::Wal);
                            if !explicit_mode {
                                options = options.create_if_missing(true);
                            }
                        }
                    }
                    Ok(Self::from(pool_options.connect_with(options).await?))
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(StorageError::BackendDisabled { backend: "sqlite" })
                }
            }
            _ => Err(StorageError::UnsupportedScheme),
        }
    }

    /// 持久化一个独立运行的入口任务，返回已提交的运行身份。
    pub async fn submit(&self, task: Task) -> Result<RunHandle, StorageError> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                let run = Self::in_transaction(&mut tx).submit(task).await?;
                tx.commit().await?;
                Ok(run)
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                let run = Self::in_transaction(&mut tx).submit(task).await?;
                tx.commit().await?;
                Ok(run)
            }
        }
    }

    /// 幂等创建引擎自有表。调用方显式选择初始化时机。
    pub async fn initialize(&self) -> Result<(), StorageError> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => crate::backend::pgsql::migrate(pool).await,
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => crate::backend::sqlite::migrate(pool).await,
        }
    }

    pub(crate) fn task_store(&self) -> Arc<dyn TaskStore> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                Arc::new(crate::backend::pgsql::PgTaskRepo::new(pool.clone()))
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                Arc::new(crate::backend::sqlite::SqliteTaskRepo::new(pool.clone()))
            }
        }
    }

    /// 按底层后端能力启动跨进程唤醒监听器（PostgreSQL 启动 `PgListener`，SQLite 返回 `None`）。
    pub(crate) fn spawn_wake_listener(
        &self,
        on_wake: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                Some(crate::backend::pgsql::spawn_wake_listener(pool.clone(), on_wake))
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(_) => {
                let _ = on_wake;
                None
            }
        }
    }

    /// 验证连接池能够执行查询，不修改数据或检查业务任务状态。
    pub async fn check(&self) -> Result<(), StorageError> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
        }
        Ok(())
    }

    /// 关闭共享连接池，等待已借出的连接归还。
    pub async fn close(&self) {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => pool.close().await,
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => pool.close().await,
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
#[path = "../tests/unit/database.rs"]
mod tests;
