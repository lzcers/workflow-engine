//! 数据库后端：通过公共事务编排层 [`common`] 复用后继扇出、JOIN 槽位收集与推进逻辑。

pub(crate) mod common;
#[cfg(feature = "pg")]
pub(crate) mod pgsql;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

#[cfg(test)]
#[path = "../../tests/unit/graph_fixture.rs"]
pub(crate) mod graph_fixture;
#[cfg(test)]
#[path = "../../tests/unit/task_fixture.rs"]
pub(crate) mod task_fixture;

use crate::{error::StorageError, task::TaskRecord};

pub(crate) const ROW_COLS: &str = "workflow_id, task_id, run_id, scope_id, parent_task_ids, name, status, \
    lease_until, available_at, attempt, \
    input_json, output_json, error, created_at, updated_at";

/// 最终失败恢复写入数据库的统一原因文本。
pub(crate) const BUDGET_EXHAUSTED_ERR: &str = "task retry budget exhausted";
pub(crate) const LEASE_EXPIRED_ERR: &str = "task lease expired; retry budget exhausted";

pub(crate) fn claim_lost(task: &TaskRecord) -> StorageError {
    StorageError::ClaimLost {
        workflow_id: task.workflow_id.clone(),
        task_id: task.task_id,
    }
}
