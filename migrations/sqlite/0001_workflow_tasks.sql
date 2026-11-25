-- SQLite 引擎自有 schema；JSON 数组以 TEXT 存储，时间为 UTC 文本。
-- 由 sqlite_backend::migrate 在事务内幂等执行，不依赖业务表。

-- 工作流任务实例及其执行状态；同一次 run 可包含多个任务。
CREATE TABLE IF NOT EXISTS tasks (
    -- 工作流标识，用于隔离调度与查询。
    workflow_id TEXT NOT NULL,
    -- 工作流运行身份；后继继承源任务的值。
    -- 提交入口时以新任务 ID 创建运行；直接写入的 NULL 在同一工作流内按同组处理。
    run_id INTEGER,
    -- 运行内的 JOIN 隔离域；实例及父子关系保存在 scopes 表，进入子图创建实例，退出恢复父实例。
    -- NULL 表示运行根作用域；跨边界 JOIN 按 scopes 父子关系及入口任务匹配来源。
    scope_id INTEGER,
    -- 数据库生成的任务主键，供来源引用、认领和结算使用。
    task_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (task_id > 0),
    -- 来源任务 ID JSON 数组；入口为空，成功和失败后继均记录源任务，JOIN 累积并去重所有贡献者。
    -- 引用同一 workflow_id 下的 task_id。
    parent_task_ids TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(parent_task_ids) AND json_type(parent_task_ids) = 'array'),
    -- 工作流图中注册的节点名；调度器据此选择执行节点，同一节点可产生多条任务记录。
    name TEXT NOT NULL,
    -- pending 可直接认领，collecting 收集 JOIN 输入，running 已认领，succeeded 成功，failed 永久失败，
    -- skipped 跳过，superseded 失败已由后继接管。
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'collecting', 'running', 'succeeded', 'failed', 'skipped', 'superseded')
    ),
    -- 本次认领的租约截止时间（UTC 文本），执行及结算期间持续续租；NULL 表示无有效租约，结算或释放时清空。
    -- running 任务在租约释放或到期后可重新认领。
    lease_until TEXT,
    -- 认领代次，从 0 开始，每次认领加 1；超过引擎重试预算 后只处理最终失败，不再执行业务。
    attempt INTEGER NOT NULL DEFAULT 0,
    -- 节点输入的 JSON 文本；成功汇合任务按对象键合并贡献。
    input_json TEXT NOT NULL CHECK (json_valid(input_json))
        CHECK (status <> 'collecting' OR json_type(input_json) = 'object'),
    -- 节点成功输出的 JSON 文本；结构由节点输出类型决定，尚未写入输出时为 SQL NULL。
    output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
    -- 失败、跳过或被后继接管的原因，也记录重试预算耗尽及依赖失败原因；成功结算时清空。
    error TEXT,
    -- 任务记录创建时间，由数据库时钟写入 UTC 文本。
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    -- 最近更新时间（UTC 文本）；仓储在认领、结算、来源合并或租约变更时显式更新，无触发器。
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(workflow_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_tasks_predecessors ON tasks(workflow_id, run_id, scope_id, name, status);

-- 子图的执行实例，仅保存身份和父子关系，无独立任务状态或调度生命周期。
CREATE TABLE IF NOT EXISTS scopes (
    workflow_id TEXT NOT NULL,
    scope_id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (scope_id > 0),
    run_id INTEGER,
    subgraph_id TEXT NOT NULL,
    parent_scope_id INTEGER,
    trigger_task_id INTEGER NOT NULL,
    UNIQUE (workflow_id, scope_id),
    FOREIGN KEY (workflow_id, parent_scope_id) REFERENCES scopes(workflow_id, scope_id)
);
CREATE INDEX IF NOT EXISTS idx_scopes_parent ON scopes(workflow_id, run_id, parent_scope_id);
