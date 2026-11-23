//! 用图中的 JOIN 规则和当前运行的作用域、任务状态判定就绪与失败。

/// collecting 状态的 JOIN 等待全部有效前驱成功，以及仍可能生产输入的上游结束。
pub(super) fn ready(rules: &str) -> String {
    format!(
        "NOT ({})",
        blocked(
            rules,
            "source.status IS DISTINCT FROM 'succeeded'",
            "upstream.status IN ('pending', 'collecting', 'running')"
        )
    )
}

pub(super) fn failed(rules: &str) -> String {
    blocked(
        rules,
        "source.status = 'failed'",
        "upstream.status = 'failed'",
    )
}

fn blocked(rules: &str, source_status: &str, upstream_status: &str) -> String {
    format!(
        r#"{rules}::jsonb ? wt.name
        AND EXISTS (
            WITH RECURSIVE
            children(scope_id) AS (
                SELECT wt.scope_id
                UNION ALL
                SELECT s.scope_id FROM scopes s JOIN children c
                  ON s.parent_scope_id IS NOT DISTINCT FROM c.scope_id
                WHERE s.workflow_id = wt.workflow_id AND s.run_id IS NOT DISTINCT FROM wt.run_id
            ),
            ancestors(parent_scope_id, trigger_task_id, distance) AS (
                SELECT parent_scope_id, trigger_task_id, 0 FROM scopes
                WHERE workflow_id = wt.workflow_id AND scope_id = wt.scope_id
                UNION ALL
                SELECT s.parent_scope_id, s.trigger_task_id, a.distance + 1
                FROM scopes s JOIN ancestors a ON s.scope_id = a.parent_scope_id
                WHERE s.workflow_id = wt.workflow_id
            ),
            entries AS (
                SELECT trigger_task_id, row_number() OVER (ORDER BY distance DESC) - 1 AS depth
                FROM ancestors
            ),
            deps AS (
                SELECT p.name, entry.task_id FROM jsonb_to_recordset(
                    {rules}::jsonb -> wt.name -> 'predecessors'
                ) AS p(name text, entry_depth int)
                LEFT JOIN entries e ON e.depth = p.entry_depth
                LEFT JOIN tasks entry ON entry.workflow_id = wt.workflow_id
                    AND entry.run_id IS NOT DISTINCT FROM wt.run_id
                    AND entry.task_id = e.trigger_task_id AND entry.name = p.name
                WHERE p.entry_depth IS NULL OR entry.task_id IS NOT NULL
            )
            SELECT 1 WHERE EXISTS (SELECT 1 FROM deps) AND (
                EXISTS (
                    SELECT 1 FROM deps
                    LEFT JOIN tasks source ON source.workflow_id = wt.workflow_id
                        AND source.run_id IS NOT DISTINCT FROM wt.run_id
                        AND source.name = deps.name AND source.status <> 'superseded' AND (
                            source.task_id = deps.task_id OR (deps.task_id IS NULL AND EXISTS (
                                SELECT 1 FROM children WHERE scope_id IS NOT DISTINCT FROM source.scope_id
                            ))
                        )
                    WHERE {source_status}
                ) OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements_text({rules}::jsonb -> wt.name -> 'upstreams') AS u(name)
                    JOIN tasks upstream ON upstream.workflow_id = wt.workflow_id
                        AND upstream.run_id IS NOT DISTINCT FROM wt.run_id AND upstream.name = u.name
                    WHERE {upstream_status} AND EXISTS (
                        SELECT 1 FROM children WHERE scope_id IS NOT DISTINCT FROM upstream.scope_id
                    )
                )
            )
        )"#
    )
}
