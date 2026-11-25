//! 数据库后端：各自实现共享任务模型与存储契约。

#[cfg(feature = "pg")]
pub(crate) mod pgsql;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

#[cfg(test)]
#[path = "../../tests/unit/task_fixture.rs"]
pub(crate) mod task_fixture;

pub(crate) const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    lease_until, attempt, \
    input_json, output_json, error, created_at, updated_at";
