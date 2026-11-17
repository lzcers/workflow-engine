//! 引擎自有 schema（DDL 归属）与迁移入口。
//!
//! 消费方在启动时先执行 [`migrate`]，再执行自己的业务 schema。
//! 引擎表（`tasks` / `task_counters`）不持有指向业务表的 FK；业务完整性由
//! 消费方在自己的迁移里以 `ALTER TABLE` 补充。

use sqlx::PgPool;

use crate::StorageError;

/// 引擎 schema DDL（幂等，每次启动执行：`CREATE ... IF NOT EXISTS` 建表及索引，
/// `COMMENT ON` 同步表和字段说明）。
pub const SCHEMA_DDL: &str = include_str!("../../../migrations/pgsql/0001_workflow_tasks.sql");

/// 执行引擎 schema 迁移（幂等，可每次启动执行）。
pub async fn migrate(pool: &PgPool) -> Result<(), StorageError> {
    sqlx::raw_sql(SCHEMA_DDL).execute(pool).await?;
    Ok(())
}
