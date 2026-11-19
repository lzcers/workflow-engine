//! JOIN 的 SQL 闸门；认领、恢复扫描及失败传播共用来源匹配规则。

const SOURCE_MATCHES: &str = r#"st.workflow_id = wt.workflow_id AND st.run_id IS NOT DISTINCT FROM wt.run_id
        AND st.name = (dep.value->>'name')
        AND ((((dep.value->>'task_id'))::bigint IS NOT NULL AND st.task_id = ((dep.value->>'task_id'))::bigint) OR
             (((dep.value->>'task_id'))::bigint IS NULL AND (
                (NOT ((dep.value->>'descendants'))::boolean AND st.scope_id IS NOT DISTINCT FROM ((dep.value->>'scope_id'))::bigint) OR
                (((dep.value->>'descendants'))::boolean AND st.scope_id IN (
                    WITH RECURSIVE children(scope_id) AS (
                        SELECT scope_id FROM scopes
                        WHERE workflow_id = wt.workflow_id AND run_id IS NOT DISTINCT FROM wt.run_id
                          AND parent_scope_id IS NOT DISTINCT FROM ((dep.value->>'scope_id'))::bigint
                        UNION ALL
                        SELECT child.scope_id FROM scopes child JOIN children parent
                          ON child.parent_scope_id = parent.scope_id
                        WHERE child.workflow_id = wt.workflow_id AND child.run_id IS NOT DISTINCT FROM wt.run_id
                    ) SELECT scope_id FROM children
                ))
             )))"#;

pub(super) fn ready() -> String {
    format!(
        r#"wt.task_type <> 'join' OR (
       NOT EXISTS (SELECT 1 FROM jsonb_array_elements(wt.join_state->'upstream') dep(value)
                   JOIN tasks st ON {matches}
                   WHERE st.status IN ('pending', 'running'))
       AND NOT EXISTS (
           SELECT 1 FROM jsonb_array_elements(wt.join_state->'expected') dep(value)
           WHERE NOT EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND st.status = 'succeeded')
              OR EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND
                   (st.status <> 'succeeded' OR ((dep.value->>'edge_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(wt.join_state->'arrivals'->((dep.value->>'edge_id'))) a(value) WHERE a.value::bigint = st.task_id))))
       )
    )"#,
        matches = SOURCE_MATCHES
    )
}

pub(super) fn failed() -> String {
    format!(
        r#"wt.task_type = 'join' AND (
        EXISTS (SELECT 1 FROM jsonb_array_elements(wt.join_state->'expected') dep(value) JOIN tasks st ON {matches} WHERE st.status = 'failed')
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(wt.join_state->'upstream') dep(value) JOIN tasks st ON {matches} WHERE st.status = 'failed')
    )"#,
        matches = SOURCE_MATCHES
    )
}
