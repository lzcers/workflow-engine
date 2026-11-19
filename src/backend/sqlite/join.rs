//! JOIN 按前驱名称及作用域查询；上游生产范围由执行图传入，不写入任务表。

/// 组内及后代实例。节点完整名称继续限制具体来源，不混入其他节点。
const LOCAL_SCOPE: &str = r#"(st.scope_id IS wt.scope_id OR st.scope_id IN (
    WITH RECURSIVE children(scope_id) AS (
        SELECT scope_id FROM scopes
        WHERE workflow_id = wt.workflow_id AND run_id IS wt.run_id
          AND parent_scope_id IS wt.scope_id
        UNION ALL
        SELECT child.scope_id FROM scopes child JOIN children parent
          ON child.parent_scope_id = parent.scope_id
        WHERE child.workflow_id = wt.workflow_id AND child.run_id IS wt.run_id
    ) SELECT scope_id FROM children
))"#;

/// 外部输入只对应当前实例入口链中的具体任务，不匹配其他调用的同名入口。
const ENTRY_TASK: &str = r#"st.task_id IN (
    WITH RECURSIVE ancestors(scope_id, parent_scope_id, trigger_task_id) AS (
        SELECT scope_id, parent_scope_id, trigger_task_id FROM scopes
        WHERE workflow_id = wt.workflow_id AND run_id IS wt.run_id AND scope_id = wt.scope_id
        UNION ALL
        SELECT parent.scope_id, parent.parent_scope_id, parent.trigger_task_id
        FROM scopes parent JOIN ancestors child ON parent.scope_id = child.parent_scope_id
        WHERE parent.workflow_id = wt.workflow_id AND parent.run_id IS wt.run_id
    ) SELECT trigger_task_id FROM ancestors
)"#;

fn source_matches() -> String {
    format!(
        "st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id AND st.name = dep.value AND ({LOCAL_SCOPE} OR {ENTRY_TASK})"
    )
}

/// 参数是当前执行图的 JOIN 名 -> 全部上游名称映射。
pub(super) fn ready(upstream_parameter: &str) -> String {
    format!(
        r#"wt.task_type <> 'join' OR json_array_length(wt.depends_on_names) = 0 OR (
            NOT EXISTS (
                SELECT 1 FROM json_each({upstream_parameter}) graph, json_each(graph.value) upstream
                JOIN tasks st ON st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id
                  AND st.name = upstream.value AND {LOCAL_SCOPE}
                WHERE graph.key = wt.name AND st.status IN ('pending', 'running')
            )
            AND NOT EXISTS (
                SELECT 1 FROM json_each(wt.depends_on_names) dep
                WHERE NOT EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND st.status = 'succeeded')
                   OR EXISTS (SELECT 1 FROM tasks st WHERE {matches} AND st.status <> 'succeeded')
            )
        )"#,
        matches = source_matches()
    )
}

pub(super) fn failed(upstream_parameter: &str) -> String {
    format!(
        r#"wt.task_type = 'join' AND json_array_length(wt.depends_on_names) > 0 AND (
            EXISTS (SELECT 1 FROM json_each(wt.depends_on_names) dep
                    JOIN tasks st ON {matches} WHERE st.status = 'failed')
            OR EXISTS (
                SELECT 1 FROM json_each({upstream_parameter}) graph, json_each(graph.value) upstream
                JOIN tasks st ON st.workflow_id = wt.workflow_id AND st.run_id IS wt.run_id
                  AND st.name = upstream.value AND {LOCAL_SCOPE}
                WHERE graph.key = wt.name AND st.status = 'failed'
            )
        )"#,
        matches = source_matches()
    )
}
