//! 数据库连接、引擎表初始化与任务存储创建；后端选择由连接 URL 决定。

use std::{sync::Arc, time::Duration};

#[cfg(feature = "pg")]
use crate::backend::pgsql::{PgTaskRepo, PgTaskStore};
#[cfg(feature = "sqlite")]
use crate::backend::sqlite::{SqliteTaskRepo, SqliteTaskStore};
use crate::{StorageError, TaskStore};

/// 工作流存储的共享连接句柄。
///
/// 克隆句柄共享同一个连接池；调用 [`Self::close`] 会关闭所有克隆使用的池。
/// 后端连接池用于需要与业务 SQL 共用事务的集成，普通初始化与健康检查不需要解包。
#[derive(Clone)]
pub enum Database {
    #[cfg(feature = "pg")]
    Postgres(sqlx::PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}

impl Database {
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
                    Ok(Self::Postgres(pool))
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
                    Ok(Self::Sqlite(pool_options.connect_with(options).await?))
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(StorageError::BackendDisabled { backend: "sqlite" })
                }
            }
            _ => Err(StorageError::UnsupportedScheme),
        }
    }

    /// 按连接时选定的后端创建可供多个工作流共享的存储，与当前数据库共享连接池。
    /// 由 Engine 内部调用，不重新连接或初始化表。
    pub(crate) fn task_store(&self) -> Arc<dyn TaskStore<Error = StorageError> + Send> {
        match self {
            #[cfg(feature = "pg")]
            Self::Postgres(pool) => Arc::new(PgTaskStore::new(PgTaskRepo::new(pool.clone()))),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => Arc::new(SqliteTaskStore::new(SqliteTaskRepo::new(pool.clone()))),
        }
    }

    /// 幂等创建引擎自有表。调用方显式选择初始化时机。
    pub async fn initialize(&self) -> Result<(), StorageError> {
        match self {
            #[cfg(feature = "pg")]
            Self::Postgres(pool) => crate::backend::pgsql::migrate(pool).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => crate::backend::sqlite::migrate(pool).await,
        }
    }

    /// 验证连接池能够执行查询，不修改数据或检查业务任务状态。
    pub async fn check(&self) -> Result<(), StorageError> {
        match self {
            #[cfg(feature = "pg")]
            Self::Postgres(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => {
                sqlx::query("SELECT 1").execute(pool).await?;
            }
        }
        Ok(())
    }

    /// 关闭共享连接池，等待已借出的连接归还。
    pub async fn close(&self) {
        match self {
            #[cfg(feature = "pg")]
            Self::Postgres(pool) => pool.close().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => pool.close().await,
        }
    }
}
