-- workflow-engine（pg feature）引擎自有 schema：幂等 DDL，每次启动执行。
-- 引擎表不持有指向消费方业务表的 FK；业务完整性（FK 到 world_sessions / world_nodes
-- 等）由消费方在自己的迁移里以 ALTER TABLE 补充。
-- 消费方迁移顺序：先本文件（engine schema），再业务 schema。

-- 工作流级任务 id 计数器：引擎自有的 id 分配源，不依赖任何业务表。
-- 独立成表的原因：id 分配必须与任务插入同事务（回滚时增量一起回滚），
-- PG SEQUENCE 是非事务的且无法按 workflow 动态管理；计数行按 workflow_id 首次使用
-- 自动 upsert 建行，引擎不需要任何前置行存在。
CREATE TABLE IF NOT EXISTS task_counters (
    workflow_id  TEXT PRIMARY KEY,
    next_task_id BIGINT NOT NULL DEFAULT 0
);

-- 通用任务表：run_id 为「一次 run」的分组键（扇入去重 / JOIN 等齐前驱按同 run 判定），
-- 业务可把自己的血缘键（如 world 的 commit_id）映射到该列，引擎不解读其内容；
-- enqueue 未指定分组时以新任务 ID 创建运行；直接写入的 NULL 在同一 workflow 下按同组处理。
CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT   NOT NULL,
    run_id      BIGINT,
    task_id     BIGINT NOT NULL CHECK (task_id > 0),
    producer_task_id BIGINT CHECK (producer_task_id > 0),
    name        TEXT   NOT NULL,
    instance_id INT,
    status      TEXT   NOT NULL,
    claimed_by  TEXT,
    lease_until TIMESTAMPTZ,
    attempt     INT    NOT NULL DEFAULT 0,
    max_attempts INT   NOT NULL DEFAULT 3,
    depends_on_ids BIGINT[] NOT NULL DEFAULT '{}',
    task_type      TEXT   NOT NULL DEFAULT 'normal',
    depends_on_names TEXT[] NOT NULL DEFAULT '{}',
    input_json  JSONB  NOT NULL,
    output_json JSONB,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_id, task_id)
);

-- 写入数据库元数据，供数据库客户端查看；每次执行 schema 时同步更新说明。
COMMENT ON TABLE task_counters IS '工作流级任务 ID 计数器；分配与任务插入共用事务，回滚时计数同步回滚。';
COMMENT ON COLUMN task_counters.workflow_id IS '工作流标识；每个工作流独立维护 task_id 计数。';
COMMENT ON COLUMN task_counters.next_task_id IS '最近一次分配的 task_id，初始值为 0；分配时先加 1 再返回，首次返回 1，去重跳过插入时可能留下空号。';

COMMENT ON TABLE tasks IS '工作流任务实例及其执行状态；以 (workflow_id, task_id) 唯一定位一条任务，同一次 run 可包含多个任务。';
COMMENT ON COLUMN tasks.workflow_id IS '工作流标识；用于隔离任务、分配 task_id 和限定调度范围，与 task_id 组成主键。';
COMMENT ON COLUMN tasks.run_id IS '运行分组键；同一工作流内用于扇入去重、JOIN 等齐前驱及失败级联，后继继承源任务的值；enqueue 未指定时以新任务 ID 创建运行，指定时保留；直接写入的 NULL 在同一工作流内按同组处理。';
COMMENT ON COLUMN tasks.task_id IS '工作流内从 1 开始分配的任务记录 ID；跨 run 递增，不保证连续或跨工作流唯一，用于任务定位、依赖引用和状态结算；同一任务重新认领时保持不变。';
COMMENT ON COLUMN tasks.name IS '工作流图中注册的节点名；调度器据此选择执行节点，同一节点可产生多条任务记录。';
COMMENT ON COLUMN tasks.producer_task_id IS '首次创建任务的源任务 ID；入口可为 NULL，汇合保留首个创建者，全部前驱由 depends_on_ids 记录。多实例去重按创建者隔离，单实例汇合不按创建者隔离。';
COMMENT ON COLUMN tasks.instance_id IS '节点扇出的实例序号；NULL 表示单实例，每个创建者的多实例从 0 开始编号，与 workflow_id、run_id、producer_task_id、name 一起用于扇出去重。';
COMMENT ON COLUMN tasks.status IS '任务状态：pending 待认领，running 已认领，succeeded 成功，failed 永久失败，skipped 跳过，superseded 失败已由后继接管。';
COMMENT ON COLUMN tasks.claimed_by IS '最近一次认领任务的 worker 标识；未认领时为 NULL，任务结算后保留，不代表租约仍有效。';
COMMENT ON COLUMN tasks.lease_until IS '认领租约的到期时间；NULL 表示无有效租约，pending/running 任务在无租约或租约到期后可按依赖和次数预算重新认领；结算或释放时清空。';
COMMENT ON COLUMN tasks.attempt IS '该任务行已被认领的次数；初始为 0，每次认领加 1，包括首次执行及租约回收后的重新执行。';
COMMENT ON COLUMN tasks.max_attempts IS '该任务行允许被认领的总次数，默认 3（含首次执行）；预算耗尽且再次满足执行条件时封存为 failed，失败边新建任务单独计数。';
COMMENT ON COLUMN tasks.depends_on_ids IS '直接依赖的任务 ID 数组，引用同一 workflow_id 下的 task_id；用于依赖就绪判定、扇入合并、溯源及失败级联，空数组表示无直接依赖。';
COMMENT ON COLUMN tasks.depends_on_names IS '静态前驱节点名数组；join 认领时要求同一 workflow_id、run_id 下每个前驱至少有一个实例且全部实例成功，同时用于血缘记录及失败级联。';
COMMENT ON COLUMN tasks.task_type IS '节点声明的任务类型：normal 为普通节点，join 为汇合节点；join 额外检查 depends_on_names 中的静态前驱是否全部就绪。';
COMMENT ON COLUMN tasks.input_json IS '节点输入的 JSON 值；结构由节点输入类型决定，JOIN 输入按前驱节点名汇聚。';
COMMENT ON COLUMN tasks.output_json IS '节点成功输出的 JSON 值；结构由节点输出类型决定，尚未写入输出时为 SQL NULL。';
COMMENT ON COLUMN tasks.error IS '失败、跳过或被后继接管的原因文本，也记录重试预算耗尽及依赖失败原因；成功结算时清空。';
COMMENT ON COLUMN tasks.created_at IS '任务记录创建时间，由数据库时钟写入。';
COMMENT ON COLUMN tasks.updated_at IS '任务记录最近更新时间；由仓储在认领、结算、依赖合并或租约变更时显式更新，无自动更新触发器。';

-- 调度器轮询：就绪 + 过期 running。
CREATE INDEX IF NOT EXISTS idx_tasks_claim
ON tasks(workflow_id, status, lease_until);

-- 恢复扫描：按 status 快速定位候选工作流。
CREATE INDEX IF NOT EXISTS idx_tasks_pending_sessions
ON tasks(status, workflow_id)
WHERE status = 'pending';

-- 多实例扇出去重键：同一 (workflow, run, producer, name, instance) 的多实例行唯一。
-- 覆盖「原子结算事务已提交但响应丢失」的重执行场景。用 COALESCE(run_id, 0) 兜底
-- NULL（PG 唯一索引默认 NULL 互不相等）。部分索引只覆盖 instance_id IS NOT NULL 的
-- 多实例行；单实例行走 FOR UPDATE 扇入合并去重，不在此键内。
-- 未指定创建者使用独立的 0 键空间；有效任务 ID 必须为正数。
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_fanout_instance
ON tasks (workflow_id, COALESCE(run_id, 0), COALESCE(producer_task_id, 0), name, instance_id)
WHERE instance_id IS NOT NULL;
