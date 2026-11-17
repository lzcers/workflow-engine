//! 数据库后端：各自实现共享任务模型与存储契约。

#[cfg(any(feature = "pg", test))]
pub mod pgsql;
#[cfg(any(feature = "sqlite", test))]
pub mod sqlite;
