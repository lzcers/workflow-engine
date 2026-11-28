//! PostgreSQL 事务方言实现：为共享编排层实现 [`BackendTx`]。

use std::{collections::HashMap, sync::Arc};

use serde_json::Value;

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

pub(super) struct PgTx<'a, 'c>(pub(super) &'a mut sqlx::Transaction<'c, sqlx::Postgres>);

impl<'c> BackendTx for PgTx<'_, 'c> {
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
        let names: Vec<&str> = rows.iter().map(|r| r.dst).collect();
        let statuses: Vec<&str> = rows.iter().map(|r| r.status.as_str()).collect();
        let inputs: Vec<&Value> = rows.iter().map(|r| r.input).collect();
        let scopes: Vec<Option<i64>> = rows.iter().map(|r| r.scope_id).collect();
        sqlx::query(
            r#"
            INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id)
            SELECT $1, $2, t.name, t.status, $3, t.input, t.scope_id
            FROM unnest($4::text[], $5::text[], $6::jsonb[], $7::bigint[])
                 AS t(name, status, input, scope_id)
            "#,
        )
        .bind(source.workflow_id)
        .bind(source.run_id)
        .bind([source.task_id].as_slice())
        .bind(&names)
        .bind(&statuses)
        .bind(&inputs)
        .bind(&scopes)
        .execute(&mut **self.0)
        .await?;
        Ok(())
    }

    async fn find_collecting_join(
        &mut self,
        workflow_id: &str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &str,
    ) -> Result<Option<(i64, Vec<i64>, Value)>, StorageError> {
        let row = sqlx::query_as::<_, (i64, Vec<i64>, Value)>(
            "SELECT task_id, parent_task_ids, input_json FROM tasks
             WHERE workflow_id = $1 AND run_id = $2 AND status = 'collecting'
               AND scope_id IS NOT DISTINCT FROM $3 AND name = $4
             ORDER BY task_id LIMIT 1 FOR UPDATE",
        )
        .bind(workflow_id)
        .bind(run_id)
        .bind(scope_id)
        .bind(name)
        .fetch_optional(&mut **self.0)
        .await?;
        Ok(row)
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
             VALUES ($1, $2, $3, 'collecting', $4, $5, $6)",
        )
        .bind(source.workflow_id)
        .bind(source.run_id)
        .bind(dst)
        .bind([source.task_id].as_slice())
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
            "UPDATE tasks SET parent_task_ids = $3, input_json = $4, updated_at = clock_timestamp()
             WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(workflow_id)
        .bind(task_id)
        .bind(parents)
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
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM tasks
                WHERE workflow_id = $1 AND run_id = $2 AND status = 'collecting'
             )",
        )
        .bind(workflow_id)
        .bind(run_id)
        .fetch_one(&mut **self.0)
        .await?;
        Ok(exists)
    }

    async fn load_run_snapshot(
        &mut self,
        workflow_id: &str,
        run_id: i64,
    ) -> Result<(Vec<RunTask>, Vec<RunScope>), StorageError> {
        let tasks: Vec<RunTask> = sqlx::query_as::<_, RunTaskRow>(
            r#"
            SELECT task_id, name, status, scope_id,
                   CASE WHEN status = 'collecting'
                        THEN ARRAY(SELECT jsonb_object_keys(input_json))
                        ELSE '{}'::text[]
                   END AS contributed_slots
            FROM tasks
            WHERE workflow_id = $1 AND run_id = $2
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
             WHERE workflow_id = $1 AND run_id = $2",
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
                 SET status = 'failed', error = $3,
                     lease_until = NULL, updated_at = clock_timestamp()
                 WHERE workflow_id = $1 AND task_id = ANY($2)",
            )
            .bind(workflow_id)
            .bind(&ids)
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
             SET status = 'pending', updated_at = clock_timestamp()
             WHERE workflow_id = $1 AND task_id = ANY($2)",
        )
        .bind(workflow_id)
        .bind(task_ids)
        .execute(&mut **self.0)
        .await?;
        Ok(())
    }
}
