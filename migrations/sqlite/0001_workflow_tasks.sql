-- SQLite 引擎自有 schema；JSON 数组以 TEXT 存储，时间为定长 UTC 文本（YYYY-MM-DDTHH:MM:SS.fffZ）。
-- 由 sqlite::migrate 在 BEGIN IMMEDIATE 事务内幂等执行，不依赖业务表。

CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT NOT NULL,
    run_id INTEGER NOT NULL DEFAULT 0,
    scope_id INTEGER,
    task_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (task_id > 0),
    parent_task_ids TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(parent_task_ids) AND json_type(parent_task_ids) = 'array'),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'collecting', 'running', 'succeeded', 'failed', 'superseded')
    ),
    lease_until TEXT,
    available_at TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    input_json TEXT NOT NULL CHECK (json_valid(input_json))
        CHECK (status <> 'collecting' OR json_type(input_json) = 'object'),
    output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 仅索引活跃/待认领行并按 task_id 排序，避免历史终态膨胀 B-tree 与排序开销。
CREATE INDEX IF NOT EXISTS idx_tasks_claim
ON tasks(workflow_id, task_id)
WHERE status IN ('pending', 'running');

-- 按运行、状态、作用域和节点名查询：加速 JOIN 汇合与推进短路。
CREATE INDEX IF NOT EXISTS idx_tasks_run_status
ON tasks(workflow_id, run_id, status, scope_id, name);

-- 子图的执行实例，仅保存身份和父子关系，无独立任务状态或调度生命周期。
CREATE TABLE IF NOT EXISTS scopes (
    workflow_id TEXT NOT NULL,
    scope_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (scope_id > 0),
    run_id INTEGER NOT NULL,
    subgraph_id TEXT NOT NULL,
    parent_scope_id INTEGER,
    trigger_task_id INTEGER NOT NULL,
    UNIQUE (workflow_id, scope_id),
    FOREIGN KEY (workflow_id, parent_scope_id) REFERENCES scopes(workflow_id, scope_id)
);

CREATE INDEX IF NOT EXISTS idx_scopes_parent
ON scopes(workflow_id, run_id, parent_scope_id);
