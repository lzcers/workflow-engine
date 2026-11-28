//! SQLite 事务方言实现：为共享编排层实现 [`BackendTx`]。

use std::{collections::HashMap, sync::Arc};

use serde_json::Value;
use sqlx::{QueryBuilder, types::Json};

use super::{rows::RunTaskRow, scope};
use crate::{
    backend::common::{BackendTx, NewTaskRow},
    error::StorageError,
    task::{
        join::{RunScope, RunTask},
        scope::{ScopeEntries, ScopeRecord},
        store::FanoutSource,
    },
};

const TASK_BATCH_CHUNK: usize = 100;

pub(super) struct SqliteTx<'a, 'c>(pub(super) &'a mut sqlx::Transaction<'c, sqlx::Sqlite>);

impl<'c> BackendTx for SqliteTx<'_, 'c> {
    async fn load_scope_chain(
        &mut self,
        workflow_id: &str,
        scope_id: Option<i64>,
    ) -> Result<Vec<ScopeRecord>, StorageError> {
        scope::chain(self.0, workflow_id, scope_id).await
    }

    async fn enter_shared_scope(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &[Arc<str>],
        shared: &mut ScopeEntries,
    ) -> Result<Option<i64>, StorageError> {
        scope::enter(
            self.0,
            workflow_id,
            run_id,
            trigger_task_id,
            parent,
            entered,
            Some(shared),
        )
        .await
    }

    async fn enter_isolated_scopes_batch(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &[Arc<str>],
        count: usize,
    ) -> Result<Vec<i64>, StorageError> {
        scope::enter_isolated_batch(
            self.0,
            workflow_id,
            run_id,
            trigger_task_id,
            parent,
            entered,
            count,
        )
        .await
    }

    async fn insert_tasks_batch(
        &mut self,
        source: &FanoutSource<'_>,
        rows: &[NewTaskRow<'_>],
    ) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        let parent_json = format!("[{}]", source.task_id);
        for chunk in rows.chunks(TASK_BATCH_CHUNK) {
            let mut qb = QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id) ",
            );
            qb.push_values(chunk, |mut b, row| {
                b.push_bind(source.workflow_id)
                    .push_bind(source.run_id)
                    .push_bind(row.dst)
                    .push_bind(row.status.as_str())
                    .push_bind(&parent_json)
                    .push_bind(row.input)
                    .push_bind(row.scope_id);
            });
            qb.build().execute(&mut **self.0).await?;
        }
        Ok(())
    }

    async fn find_collecting_join(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &str,
    ) -> Result<Option<(i64, Vec<i64>, Value)>, StorageError> {
        let row = sqlx::query_as::<_, (i64, Json<Vec<i64>>, Value)>(
            "SELECT task_id, parent_task_ids, input_json FROM tasks
             WHERE workflow_id = ?1 AND run_id = ?2 AND status = 'collecting'
               AND scope_id IS ?3 AND name = ?4
             ORDER BY task_id LIMIT 1",
        )
        .bind(workflow_id)
        .bind(run_id)
        .bind(scope_id)
        .bind(name)
        .fetch_optional(&mut **self.0)
        .await?;
        Ok(row.map(|(id, Json(parents), input)| (id, parents, input)))
    }

    async fn insert_collecting_join(
        &mut self,
        source: &FanoutSource<'_>,
        dst: &str,
        scope_id: Option<i64>,
        input: &Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id)
             VALUES (?1, ?2, ?3, 'collecting', ?4, ?5, ?6)",
        )
        .bind(source.workflow_id)
        .bind(source.run_id)
        .bind(dst)
        .bind(Json([source.task_id]))
        .bind(input)
        .bind(scope_id)
        .execute(&mut **self.0)
        .await?;
        Ok(())
    }

    async fn update_collecting_join(
        &mut self,
        workflow_id: &str,
        task_id: i64,
        parents: &[i64],
        input: &Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE tasks SET parent_task_ids = ?3, input_json = ?4,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id = ?2",
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(Json(parents))
        .bind(input)
        .execute(&mut **self.0)
        .await?;
        Ok(())
    }

    async fn has_collecting_join(
        &mut self,
        workflow_id: &str,
        run_id: i64,
    ) -> Result<bool, StorageError> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM tasks
                WHERE workflow_id = ?1 AND run_id = ?2 AND status = 'collecting'
             )",
        )
        .bind(workflow_id)
        .bind(run_id)
        .fetch_one(&mut **self.0)
        .await?;
        Ok(exists != 0)
    }

    async fn load_run_snapshot(
        &mut self,
        workflow_id: &str,
        run_id: i64,
    ) -> Result<(Vec<RunTask>, Vec<RunScope>), StorageError> {
        let tasks: Vec<RunTask> = sqlx::query_as::<_, RunTaskRow>(
            r#"
            SELECT task_id, name, status, scope_id,
                   CASE WHEN status = 'collecting' THEN input_json ELSE NULL END AS collecting_input
            FROM tasks
            WHERE workflow_id = ?1 AND run_id = ?2
            "#,
        )
        .bind(workflow_id)
        .bind(run_id)
        .fetch_all(&mut **self.0)
        .await?
        .into_iter()
        .map(RunTask::from)
        .collect();
        let scopes = sqlx::query_as::<_, RunScope>(
            "SELECT scope_id, parent_scope_id, trigger_task_id FROM scopes
             WHERE workflow_id = ?1 AND run_id = ?2",
        )
        .bind(workflow_id)
        .bind(run_id)
        .fetch_all(&mut **self.0)
        .await?;
        Ok((tasks, scopes))
    }

    async fn fail_joins(
        &mut self,
        workflow_id: &str,
        failed: &[(i64, &'static str)],
    ) -> Result<(), StorageError> {
        let mut by_reason: HashMap<&str, Vec<i64>> = HashMap::new();
        for &(task_id, reason) in failed {
            by_reason.entry(reason).or_default().push(task_id);
        }
        for (reason, ids) in by_reason {
            sqlx::query(
                "UPDATE tasks
                 SET status = 'failed', error = ?3,
                     lease_until = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
            )
            .bind(workflow_id)
            .bind(Json(&ids))
            .bind(reason)
            .execute(&mut **self.0)
            .await?;
        }
        Ok(())
    }

    async fn mark_joins_pending(
        &mut self,
        workflow_id: &str,
        task_ids: &[i64],
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE tasks
             SET status = 'pending', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE workflow_id = ?1 AND task_id IN (SELECT value FROM json_each(?2))",
        )
        .bind(workflow_id)
        .bind(Json(task_ids))
        .execute(&mut **self.0)
        .await?;
        Ok(())
    }
}
