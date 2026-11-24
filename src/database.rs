//! 数据库连接、引擎表初始化与任务存储创建；后端选择由连接 URL 决定。

use std::time::Duration;

use crate::error::StorageError;
use crate::{RunHandle, Task};

/// 工作流数据库。默认共享连接池，也可以借用宿主事务来提交任务。
#[derive(Clone)]
pub struct Database<C = DatabasePool>(pub(crate) C);

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

impl Database {
    /// 在宿主已有的 SQLx 事务上提交任务，借用在提交任务后结束。
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
    /// PostgreSQL 使用最多 10 个连接；SQLite 可写文件使用 WAL 和最多 4 个连接。
    /// SQLite 内存及临时数据库保留一个不自动回收的连接，避免连接回收后数据库消失。
    /// 两种后端的连接获取超时均为 5 秒，SQLite 写锁等待也为 5 秒。
    /// SQLite 文件默认在不存在时创建；显式的 mode 和 immutable URL 参数保持原意。
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        match url.split_once(':').map(|(scheme, _)| scheme) {
            Some("postgres" | "postgresql") => {
                #[cfg(feature = "pg")]
                {
                    let pool = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(10)
                        .acquire_timeout(Duration::from_secs(5))
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
                    // SQLx 会把原始 :memory: 改为生成的文件名；编码后的特殊文件名
                    // 和空文件名则由 SQLite 自身解释为只属于连接的临时数据库。
                    let mut transient = database_name == ":memory:"
                        || options.get_filename().as_os_str().is_empty()
                        || matches!(
                            options.get_filename().to_str(),
                            Some(":memory:" | "file::memory:")
                        );
                    let mut explicit_mode = false;
                    let mut read_only = false;
                    let mut immutable = false;
                    // SQLx 已验证 URL；这里只读取连接池策略所需的参数，保留其连接选项。
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
                    let mut options = options.busy_timeout(Duration::from_secs(5));
                    let mut pool_options =
                        SqlitePoolOptions::new().acquire_timeout(Duration::from_secs(5));
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
                let mut tx = pool.begin().await?;
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
