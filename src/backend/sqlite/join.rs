//! JOIN 条件从执行图和当前调用的作用域推导，认领与失败传播共用来源选择。

fn matches_rule(rules: &str, condition: &str) -> String {
    format!(
        r#"EXISTS (
            SELECT 1 FROM json_each({rules}) AS rule
            WHERE rule.key = wt.name
              AND EXISTS (
                WITH RECURSIVE
                local_scopes(scope_id) AS (
                    SELECT wt.scope_id
                    UNION ALL
                    SELECT child.scope_id FROM scopes child JOIN local_scopes parent
                      ON child.parent_scope_id IS parent.scope_id
                    WHERE child.workflow_id = wt.workflow_id AND child.run_id IS wt.run_id
                ),
                ancestors(parent_scope_id, trigger_task_id, distance) AS (
                    SELECT parent_scope_id, trigger_task_id, 0 FROM scopes
                    WHERE workflow_id = wt.workflow_id AND run_id IS wt.run_id AND scope_id = wt.scope_id
                    UNION ALL
                    SELECT parent.parent_scope_id, parent.trigger_task_id, child.distance + 1
                    FROM scopes parent JOIN ancestors child ON parent.scope_id = child.parent_scope_id
                    WHERE parent.workflow_id = wt.workflow_id AND parent.run_id IS wt.run_id
                ),
                entries AS (
                    SELECT trigger_task_id, row_number() OVER (ORDER BY distance DESC) - 1 AS depth
                    FROM ancestors
                ),
                predecessors AS (
                    SELECT json_extract(value, '$.name') AS name, json_extract(value, '$.entry_depth') AS depth
                    FROM json_each(rule.value, '$.predecessors')
                ),
                required AS (
                    SELECT p.name, e.trigger_task_id FROM predecessors p
                    LEFT JOIN entries e ON e.depth = p.depth
                    LEFT JOIN tasks t ON t.workflow_id = wt.workflow_id AND t.task_id = e.trigger_task_id
                    WHERE p.depth IS NULL OR p.name = t.name
                ),
                sources AS (
                    SELECT st.status FROM required r LEFT JOIN tasks st
                      ON st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id
                        AND st.name = r.name AND st.status <> 'superseded'
                        AND (st.task_id = r.trigger_task_id OR (r.trigger_task_id IS NULL AND EXISTS (
                            SELECT 1 FROM local_scopes WHERE scope_id IS st.scope_id
                        )))
                ),
                upstreams AS (
                    SELECT st.status FROM json_each(rule.value, '$.upstreams') upstream JOIN tasks st
                      ON st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id AND st.name = upstream.value
                    WHERE EXISTS (SELECT 1 FROM local_scopes WHERE scope_id IS st.scope_id)
                )
                SELECT 1 WHERE EXISTS (SELECT 1 FROM required) AND ({condition})
              )
        )"#
    )
}

/// 检查收集中的 JOIN 是否已等齐上游。
pub(super) fn ready(rules: &str) -> String {
    let blocked = matches_rule(
        rules,
        r#"EXISTS (SELECT 1 FROM upstreams WHERE status IN ('pending','collecting','running'))
           OR EXISTS (SELECT 1 FROM sources WHERE status IS NOT 'succeeded')"#,
    );
    format!("NOT {blocked}")
}

pub(super) fn failed(rules: &str) -> String {
    matches_rule(
        rules,
        "EXISTS (SELECT 1 FROM upstreams WHERE status = 'failed')
         OR EXISTS (SELECT 1 FROM sources WHERE status = 'failed')",
    )
}
