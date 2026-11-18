//! 测试用 pgsql 设施：隔离 schema + 引擎 schema 迁移。
//!
//! 测试库经环境变量 `WORKFLOW_ENGINE_TEST_DATABASE_URL`（回退 `DATABASE_URL`）连接；
//! 未配置时测试跳过（返回 `None`）。独立 schema 隔离，并允许同一测试使用多个数据库连接。
//! [`connect_test`] 会执行引擎自有迁移（[`super::schema::migrate`]）。

use sqlx::{PgPool, postgres::PgPoolOptions};

/// 测试库连接：隔离 schema（已执行引擎迁移）。
pub struct TestDb {
    pool: PgPool,
    database_url: String,
}

impl TestDb {
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }
}

/// 连接测试库并执行引擎迁移（隔离 schema + 多连接池）；无测试库环境变量时返回 `None`。
pub async fn connect_test() -> Option<TestDb> {
    let Ok(database_url) = std::env::var("WORKFLOW_ENGINE_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
    else {
        return None;
    };
    let schema_name = format!("workflow_engine_test_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("failed to connect postgres test database");
    sqlx::query(&format!(r#"CREATE SCHEMA "{schema_name}""#))
        .execute(&admin_pool)
        .await
        .expect("failed to create postgres test schema");
    admin_pool.close().await;

    let pg_pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |conn, _meta| {
            let schema_name = schema_name.clone();
            Box::pin(async move {
                sqlx::query(&format!(r#"SET search_path TO "{schema_name}""#))
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SELECT set_config('application_name', $1, false)")
                    .bind(&schema_name)
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .expect("failed to connect isolated postgres test schema");

    super::schema::migrate(&pg_pool)
        .await
        .expect("failed to run engine schema migration");

    Some(TestDb {
        pool: pg_pool,
        database_url,
    })
}
