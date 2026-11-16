-- SQLite 引擎自有 schema；JSON 数组以 TEXT 存储，时间为 UTC 文本。
-- 由 sqlite_backend::migrate 在事务内幂等执行，不依赖业务表。
-- 工作流级任务 ID 计数器；分配与任务插入共用事务，回滚时计数同步回滚。
CREATE TABLE IF NOT EXISTS task_counters (
    -- 工作流标识；每个工作流独立维护 task_id 计数。
    workflow_id  TEXT PRIMARY KEY NOT NULL,
    -- 最近一次分配的 task_id；分配时先加 1 再返回，首次返回 1，去重跳过插入时可能留下空号。
    next_task_id INTEGER NOT NULL DEFAULT 0
);

-- 工作流任务实例及其执行状态；同一次 run 可包含多个任务。
CREATE TABLE IF NOT EXISTS tasks (
    -- 工作流标识；用于隔离任务、分配 task_id 和限定调度范围，与 task_id 组成主键。
    workflow_id TEXT NOT NULL,
    -- 运行分组键；用于扇入去重、JOIN 等齐前驱及失败级联，后继继承源任务的值。
    -- 由调用方指定或回填；NULL 表示尚未指定，同一工作流内的 NULL 按同组处理。
    run_id INTEGER,
    -- 工作流内从 1 开始分配的任务记录 ID；跨 run 递增，不保证连续或跨工作流唯一。
    -- 用于任务定位、依赖引用和状态结算；同一任务重新认领时保持不变。
    task_id INTEGER NOT NULL CHECK (task_id > 0),
    -- 工作流图中注册的节点名；调度器据此选择执行节点，同一节点可产生多条任务记录。
    name TEXT NOT NULL,
    -- 节点扇出的实例序号；NULL 表示单实例，多实例从 0 开始编号。
    -- 与 workflow_id、run_id、name 一起用于扇出去重。
    instance_id INTEGER,
    -- pending 待认领，running 已认领，succeeded 成功，failed 永久失败，
    -- skipped 跳过，superseded 失败已由后继接管。
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'skipped', 'superseded')
    ),
    -- 最近一次认领任务的 worker 标识；未认领时为 NULL，结算后保留，不代表租约仍有效。
    claimed_by TEXT,
    -- 租约到期时间（UTC 文本）；NULL 表示无有效租约，结算或释放时清空。
    -- pending/running 任务在无租约或租约到期后可按依赖和次数预算重新认领。
    lease_until TEXT,
    -- 该任务行已被认领的次数；从 0 开始，每次认领加 1，包括首次及租约回收后的重新执行。
    attempt INTEGER NOT NULL DEFAULT 0,
    -- 该任务行允许被认领的总次数（含首次）；耗尽且再次满足执行条件时封存为 failed。
    -- 失败边新建的任务单独计数。
    max_attempts INTEGER NOT NULL DEFAULT 3,
    -- 直接依赖的任务 ID JSON 数组，引用同一 workflow_id 下的 task_id。
    -- 用于依赖就绪判定、扇入合并、溯源及失败级联；空数组表示无直接依赖。
    depends_on_ids TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(depends_on_ids) AND json_type(depends_on_ids) = 'array'),
    -- 节点声明的任务类型：normal 普通节点；join 汇合节点，额外检查静态前驱是否全部就绪。
    task_type TEXT NOT NULL DEFAULT 'normal' CHECK (task_type IN ('normal', 'join')),
    -- 静态前驱节点名的 JSON 数组，用于血缘记录及 JOIN 失败级联。
    -- join 认领时要求同一 workflow_id、run_id 下每个前驱至少有一个实例且全部实例成功。
    depends_on_names TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(depends_on_names) AND json_type(depends_on_names) = 'array'),
    -- 节点输入的 JSON 文本；结构由节点输入类型决定，JOIN 输入按前驱节点名汇聚。
    input_json TEXT NOT NULL CHECK (json_valid(input_json)),
    -- 节点成功输出的 JSON 文本；结构由节点输出类型决定，尚未写入输出时为 SQL NULL。
    output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
    -- 失败、跳过或被后继接管的原因，也记录重试预算耗尽及依赖失败原因；成功结算时清空。
    error TEXT,
    -- 任务记录创建时间，由数据库时钟写入 UTC 文本。
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    -- 最近更新时间（UTC 文本）；仓储在认领、结算、依赖合并或租约变更时显式更新，无触发器。
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
