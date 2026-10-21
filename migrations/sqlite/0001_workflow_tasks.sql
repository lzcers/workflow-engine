-- SQLite 引擎自有 schema；JSON 数组以 TEXT 存储，时间为 UTC 文本。
-- 由 sqlite_backend::migrate 在事务内幂等执行，不依赖业务表。
CREATE TABLE IF NOT EXISTS task_counters (
    workflow_id  TEXT PRIMARY KEY NOT NULL,
    next_task_id INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT NOT NULL,
    run_id INTEGER,
    task_id INTEGER NOT NULL CHECK (task_id > 0),
    name TEXT NOT NULL,
    instance_id INTEGER,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'skipped', 'superseded')
    ),
    claimed_by TEXT,
    lease_until TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    depends_on_ids TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(depends_on_ids) AND json_type(depends_on_ids) = 'array'),
    task_type TEXT NOT NULL DEFAULT 'normal' CHECK (task_type IN ('normal', 'join')),
    depends_on_names TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(depends_on_names) AND json_type(depends_on_names) = 'array'),
    input_json TEXT NOT NULL CHECK (json_valid(input_json)),
    output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workflow_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(workflow_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_tasks_recovery ON tasks(status, workflow_id);
CREATE INDEX IF NOT EXISTS idx_tasks_predecessors ON tasks(workflow_id, run_id, name, status);

-- 分开约束 NULL 与非 NULL 的 run，避免把 NULL 与有效 run_id（如 0）合并。
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_fanout_instance
ON tasks(workflow_id, run_id, name, instance_id)
WHERE instance_id IS NOT NULL AND run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_fanout_instance_without_run
ON tasks(workflow_id, name, instance_id)
WHERE instance_id IS NOT NULL AND run_id IS NULL;
