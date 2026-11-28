//! SQLite 事务方言适配器：为共享编排层实现 [`BackendTx`]。

use std::{collections::HashMap, sync::Arc};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::QueryBuilder;

use super::{join, scope};
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
    fn count_run_tasks<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
    ) -> BoxFuture<'a, Result<usize, StorageError>> {
        Box::pin(async move {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tasks WHERE workflow_id = ?1 AND run_id = ?2",
            )
            .bind(workflow_id)
            .bind(run_id)
            .fetch_one(&mut **self.0)
            .await?;
            Ok(usize::try_from(count).unwrap_or(usize::MAX))
        })
    }

    fn load_scope_chain<'a>(
        &'a mut self,
        workflow_id: &'a str,
        scope_id: Option<i64>,
    ) -> BoxFuture<'a, Result<Vec<ScopeRecord>, StorageError>> {
        Box::pin(async move { scope::chain(self.0, workflow_id, scope_id).await })
    }

    fn enter_shared_scope<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [Arc<str>],
        shared: &'a mut ScopeEntries,
    ) -> BoxFuture<'a, Result<Option<i64>, StorageError>> {
        Box::pin(async move {
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
        })
    }

    fn enter_isolated_scopes_batch<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        trigger_task_id: i64,
        parent: Option<i64>,
        entered: &'a [Arc<str>],
        count: usize,
    ) -> BoxFuture<'a, Result<Vec<i64>, StorageError>> {
        Box::pin(async move {
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
        })
    }

    fn insert_tasks_batch<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        rows: &'a [NewTaskRow<'a>],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            if rows.is_empty() {
                return Ok(());
            }
            let parent_json = serde_json::to_string(&[source.task_id]).unwrap();
            for chunk in rows.chunks(TASK_BATCH_CHUNK) {
                let mut qb = QueryBuilder::<sqlx::Sqlite>::new(
                    "INSERT INTO tasks (workflow_id, run_id, name, status, parent_task_ids, input_json, scope_id, max_attempts) ",
                );
                qb.push_values(chunk, |mut b, row| {
                    b.push_bind(source.workflow_id)
                        .push_bind(source.run_id)
                        .push_bind(row.dst)
                        .push_bind(row.status.as_str())
                        .push_bind(&parent_json)
                        .push_bind(row.input)
                        .push_bind(row.scope_id)
                        .push_bind(row.max_attempts);
                });
                qb.build().execute(&mut **self.0).await?;
            }
            Ok(())
        })
    }

    fn find_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        scope_id: Option<i64>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<(i64, Vec<i64>, Value)>, StorageError>> {
        Box::pin(async move { join::find_collecting(self.0, workflow_id, run_id, scope_id, name).await })
    }

    fn insert_collecting_join<'a>(
        &'a mut self,
        source: &'a FanoutSource<'a>,
        dst: &'a str,
        scope_id: Option<i64>,
        input: &'a Value,
        max_attempts: i32,
    ) -> BoxFuture<'a, Result<i64, StorageError>> {
        Box::pin(async move {
            join::insert_collecting(self.0, source, dst, scope_id, input, max_attempts).await
        })
    }

    fn update_collecting_join<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_id: i64,
        parents: &'a [i64],
        input: &'a Value,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move { join::update_collecting(self.0, workflow_id, task_id, parents, input).await })
    }

    fn append_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        join_task_id: i64,
        slot_key: &'a str,
        source_task_id: i64,
        values: &'a [Value],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            join::append_contributions(
                self.0,
                workflow_id,
                join_task_id,
                slot_key,
                source_task_id,
                values,
            )
            .await
        })
    }

    fn find_collecting_scopes<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        check_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<Vec<Option<i64>>, StorageError>> {
        Box::pin(async move { join::find_collecting_scopes(self.0, workflow_id, run_id, check_scopes).await })
    }

    fn load_join_snapshot<'a>(
        &'a mut self,
        workflow_id: &'a str,
        run_id: i64,
        collecting_scopes: &'a [Option<i64>],
    ) -> BoxFuture<'a, Result<(Vec<RunTask>, Vec<RunScope>), StorageError>> {
        Box::pin(async move { join::load_snapshot(self.0, workflow_id, run_id, collecting_scopes).await })
    }

    fn fail_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        failed: &'a [(i64, &'static str)],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move { join::fail(self.0, workflow_id, failed).await })
    }

    fn load_task_inputs<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, Value>, StorageError>> {
        Box::pin(async move { join::load_inputs(self.0, workflow_id, task_ids).await })
    }

    fn load_join_contributions<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<HashMap<i64, HashMap<String, Vec<Value>>>, StorageError>> {
        Box::pin(async move { join::load_contributions(self.0, workflow_id, task_ids).await })
    }

    fn mark_joins_pending<'a>(
        &'a mut self,
        workflow_id: &'a str,
        task_ids: &'a [i64],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move { join::mark_pending(self.0, workflow_id, task_ids).await })
    }

    fn materialize_and_ready_joins<'a>(
        &'a mut self,
        workflow_id: &'a str,
        materialized: &'a [(i64, Value)],
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move { join::materialize_and_ready(self.0, workflow_id, materialized).await })
    }
}
