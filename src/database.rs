//! 数据库连接、引擎表初始化、连接池配置与运行控制面查询/取消接口。

use std::time::Duration;

use crate::error::StorageError;
use crate::task::TaskStore;
use crate::{RunHandle, RunSnapshot, RunStatus, Task, TaskRecord};

/// 数据库连接池配置参数；未显式传入时使用引擎默认值（PG 10 / SQLite 4，超时 5 秒）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolOptions {
    /// 最大连接数（SQLite 内存/临时库固定为 1）。
    pub max_connections: u32,
    /// 获取连接超时时间（SQLite 同时作为 `busy_timeout`）。
    pub acquire_timeout: Duration,
}

impl PoolOptions {
    pub const fn new(max_connections: u32, acquire_timeout: Duration) -> Self {
        Self {
            max_connections: if max_connections == 0 { 1 } else { max_connections },
            acquire_timeout,
        }
    }
}

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

    /// 根据 URL 使用默认连接池参数连接 PostgreSQL 或 SQLite，不创建引擎表、不启动 worker。
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        Self::connect_internal(url, None).await
    }

    /// 使用自定义连接池参数连接 PostgreSQL 或 SQLite。
    pub async fn connect_with(url: &str, options: PoolOptions) -> Result<Self, StorageError> {
        Self::connect_internal(url, Some(options)).await
    }

    async fn connect_internal(
        url: &str,
        pool_cfg: Option<PoolOptions>,
    ) -> Result<Self, StorageError> {
        let timeout = pool_cfg.map_or(Duration::from_secs(5), |c| c.acquire_timeout);
        match url.split_once(':').map(|(scheme, _)| scheme) {
            Some("postgres" | "postgresql") => {
                #[cfg(feature = "pg")]
                {
                    let max_conns = pool_cfg.map_or(10, |c| c.max_connections.max(1));
                    let pool = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(max_conns)
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
                        let max_conns = pool_cfg.map_or(4, |c| c.max_connections.max(1));
                        pool_options = pool_options.max_connections(max_conns);
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

    /// 查询指定运行下全部任务快照（按 `task_id` 升序）。
    pub async fn list_tasks(
        &self,
        workflow_id: &str,
        run_id: i64,
    ) -> Result<Vec<TaskRecord>, StorageError> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                crate::backend::pgsql::PgTaskRepo::new(pool.clone())
                    .list_tasks(workflow_id, run_id)
                    .await
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                crate::backend::sqlite::SqliteTaskRepo::new(pool.clone())
                    .list_tasks(workflow_id, run_id)
                    .await
            }
        }
    }

    /// 获取指定运行的汇总状态与任务快照；不存在该运行时返回 `Ok(None)`。
    pub async fn get_run(
        &self,
        workflow_id: &str,
        run_id: i64,
    ) -> Result<Option<RunSnapshot>, StorageError> {
        let tasks = self.list_tasks(workflow_id, run_id).await?;
        if tasks.is_empty() {
            return Ok(None);
        }
        let status = RunStatus::from_tasks(&tasks);
        Ok(Some(RunSnapshot {
            workflow_id: workflow_id.to_owned(),
            run_id,
            status,
            tasks,
        }))
    }

    /// 取消指定运行的所有活跃任务（`pending`/`collecting`/`running`），返回被取消的任务数。
    pub async fn cancel_run(&self, workflow_id: &str, run_id: i64) -> Result<usize, StorageError> {
        match &self.0 {
            #[cfg(feature = "pg")]
            DatabasePool::Postgres(pool) => {
                crate::backend::pgsql::PgTaskRepo::new(pool.clone())
                    .cancel_run(workflow_id, run_id)
                    .await
            }
            #[cfg(feature = "sqlite")]
            DatabasePool::Sqlite(pool) => {
                crate::backend::sqlite::SqliteTaskRepo::new(pool.clone())
                    .cancel_run(workflow_id, run_id)
                    .await
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
