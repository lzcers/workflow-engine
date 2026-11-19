//! JOIN 的 SQL 闸门；认领、恢复扫描及失败传播共用来源匹配规则。

const SOURCE_MATCHES: &str = r#"st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id
        AND st.name = json_extract(dep.value, '$.name')
        AND ((json_extract(dep.value, '$.task_id') IS NOT NULL AND st.task_id = json_extract(dep.value, '$.task_id')) OR
             (json_extract(dep.value, '$.task_id') IS NULL AND (
                (NOT json_extract(dep.value, '$.descendants') AND st.scope_id IS json_extract(dep.value, '$.scope_id')) OR
                (json_extract(dep.value, '$.descendants') AND st.scope_id IN (
                    WITH RECURSIVE children(scope_id) AS (
                        SELECT scope_id FROM scopes
                        WHERE workflow_id = wt.workflow_id AND run_id IS wt.run_id
                          AND parent_scope_id IS json_extract(dep.value, '$.scope_id')
                        UNION ALL
                        SELECT child.scope_id FROM scopes child JOIN children parent
                          ON child.parent_scope_id = parent.scope_id
                        WHERE child.workflow_id = wt.workflow_id AND child.run_id IS wt.run_id
                    ) SELECT scope_id FROM children
                ))
             )))"#;

pub(super) fn ready() -> String {
    format!(
        r#"wt.task_type <> 'join' OR (
       NOT EXISTS (SELECT 1 FROM json_each(wt.join_state, '$.upstream') dep
                   JOIN tasks st ON {matches}
                   WHERE st.status IN ('pending', 'running'))
       AND NOT EXISTS (
           SELECT 1 FROM json_each(wt.join_state, '$.expected') dep
           WHERE NOT EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND st.status = 'succeeded')
              OR EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND
                   (st.status <> 'succeeded' OR (json_extract(dep.value, '$.edge_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM json_each(wt.join_state, '$.arrivals') a, json_each(a.value) item WHERE a.key = json_extract(dep.value, '$.edge_id') AND item.value = st.task_id))))
       )
    )"#,
        matches = SOURCE_MATCHES
    )
}

pub(super) fn failed() -> String {
    format!(
        r#"wt.task_type = 'join' AND (
        EXISTS (SELECT 1 FROM json_each(wt.join_state, '$.expected') dep JOIN tasks st ON {matches} WHERE st.status = 'failed')
        OR EXISTS (SELECT 1 FROM json_each(wt.join_state, '$.upstream') dep JOIN tasks st ON {matches} WHERE st.status = 'failed')
    )"#,
        matches = SOURCE_MATCHES
    )
}
