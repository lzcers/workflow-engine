//! 数据库后端：各自实现共享任务模型与存储契约。

#[cfg(feature = "pg")]
pub mod pgsql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(test)]
#[path = "../../tests/unit/task_fixture.rs"]
pub(crate) mod task_fixture;
