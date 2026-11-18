-- SQLite 引擎自有 schema；JSON 数组以 TEXT 存储，时间为 UTC 文本。
-- 由 sqlite_backend::migrate 在事务内幂等执行，不依赖业务表。
-- 工作流级任务 ID 计数器；分配与任务插入共用事务，回滚时计数同步回滚。
CREATE TABLE IF NOT EXISTS task_counters (
    -- 工作流标识；每个工作流独立维护 task_id 计数。
    workflow_id  TEXT PRIMARY KEY NOT NULL,
    -- 最近一次分配的 task_id；分配时先加 1 再返回，首次返回 1。
    next_task_id INTEGER NOT NULL DEFAULT 0
);

-- 工作流任务实例及其执行状态；同一次 run 可包含多个任务。
CREATE TABLE IF NOT EXISTS tasks (
    -- 工作流标识；用于隔离任务、分配 task_id 和限定调度范围，与 task_id 组成主键。
    workflow_id TEXT NOT NULL,
    -- 运行分组键；用于 JOIN 汇合、等齐前驱及失败级联，后继继承源任务的值。
    -- enqueue 未指定时以新任务 ID 创建运行，指定时保留；直接写入的 NULL 在同一工作流内按同组处理。
    run_id INTEGER,
    -- 工作流内从 1 开始分配的任务记录 ID；跨 run 递增，不保证连续或跨工作流唯一。
    -- 用于任务定位、依赖引用和状态结算；同一任务重新认领时保持不变。
    task_id INTEGER NOT NULL CHECK (task_id > 0),
    -- 来源任务 ID JSON 数组；入口为空，成功和失败后继均记录源任务，JOIN 累积并去重所有贡献者。
    -- 引用同一 workflow_id 下的 task_id，仅用于溯源，不要求来源任务成功。
    parent_task_ids TEXT NOT NULL DEFAULT '[]'
        CHECK (json_valid(parent_task_ids) AND json_type(parent_task_ids) = 'array'),
    -- 工作流图中注册的节点名；调度器据此选择执行节点，同一节点可产生多条任务记录。
    name TEXT NOT NULL,
    -- pending 待认领，running 已认领，succeeded 成功，failed 永久失败，
    -- skipped 跳过，superseded 失败已由后继接管。
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'skipped', 'superseded')
    ),
    -- 最近一次认领任务的 worker 标识；未认领时为 NULL，结算后保留，不代表租约仍有效。
    claimed_by TEXT,
    -- 本次认领的固定处理截止时间（UTC 文本），不续租；NULL 表示无有效租约，结算或释放时清空。
    -- pending/running 任务在无租约或到期后可按依赖重新认领。
    lease_until TEXT,
    -- 认领代次，从 0 开始，每次认领加 1；超过 max_attempts 后只处理最终失败，不再执行业务。
    attempt INTEGER NOT NULL DEFAULT 0,
    -- 该任务行的业务执行次数上限（含首次）；耗尽后重新认领并通过统一失败流程结算。
    -- 失败边新建的任务单独计数。
    max_attempts INTEGER NOT NULL DEFAULT 3,
    -- 节点声明的任务类型：normal 普通节点；event 外部事件入口；join 汇合节点，额外检查静态前驱。
    task_type TEXT NOT NULL DEFAULT 'normal' CHECK (task_type IN ('normal', 'join', 'event')),
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
    -- 最近更新时间（UTC 文本）；仓储在认领、结算、来源合并或租约变更时显式更新，无触发器。
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workflow_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(workflow_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_tasks_recovery ON tasks(status, workflow_id);
CREATE INDEX IF NOT EXISTS idx_tasks_predecessors ON tasks(workflow_id, run_id, name, status);
