//! SQLite 引擎自有 schema 与迁移入口。

use sqlx::SqlitePool;

use crate::error::StorageError;

const SCHEMA_DDL: &str = include_str!("../../../migrations/sqlite/0001_workflow_tasks.sql");

/// 原子执行幂等 DDL，可在每次启动时调用。
pub async fn migrate(pool: &SqlitePool) -> Result<(), StorageError> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::raw_sql(SCHEMA_DDL).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
