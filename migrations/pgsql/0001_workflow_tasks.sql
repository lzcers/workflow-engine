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

-- 通用任务表：run_id 标识一次运行，scope_id 隔离运行内的分支（JOIN 按同 run 和 scope 判定），
-- 业务可把自己的血缘键（如 world 的 commit_id）映射到该列，引擎不解读其内容；
-- enqueue 未指定分组时以新任务 ID 创建运行；直接写入的 NULL 在同一 workflow 下按同组处理。
CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT   NOT NULL,
    run_id      BIGINT,
    scope_id    BIGINT,
    task_id     BIGINT NOT NULL CHECK (task_id > 0),
    parent_task_ids BIGINT[] NOT NULL DEFAULT '{}',
    name        TEXT   NOT NULL,
    status      TEXT   NOT NULL,
    claimed_by  TEXT,
    lease_until TIMESTAMPTZ,
    attempt     INT    NOT NULL DEFAULT 0,
    max_attempts INT   NOT NULL DEFAULT 3,
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
COMMENT ON COLUMN task_counters.next_task_id IS '最近一次分配的 task_id，初始值为 0；分配时先加 1 再返回，首次返回 1。';

COMMENT ON TABLE tasks IS '工作流任务实例及其执行状态；以 (workflow_id, task_id) 唯一定位一条任务，同一次 run 可包含多个任务。';
COMMENT ON COLUMN tasks.workflow_id IS '工作流标识；用于隔离任务、分配 task_id 和限定调度范围，与 task_id 组成主键。';
COMMENT ON COLUMN tasks.run_id IS '运行分组键；与 scope_id 一起限定 JOIN 汇合、前驱等待及失败级联，后继继承源任务的值；enqueue 未指定时以新任务 ID 创建运行，指定时保留；直接写入的 NULL 在同一工作流内按同组处理。';
COMMENT ON COLUMN tasks.scope_id IS '运行内的执行分组；实例及父子关系保存在 scopes 表；进入子图创建实例，退出恢复父实例，NULL 为运行根作用域。';
COMMENT ON COLUMN tasks.parent_task_ids IS '来源任务 ID 数组，引用同一 workflow_id 下的 task_id；入口为空，成功和失败后继均记录源任务，JOIN 累积并去重所有贡献者；仅用于溯源，不要求来源任务成功。';
COMMENT ON COLUMN tasks.task_id IS '工作流内从 1 开始分配的任务记录 ID；跨 run 递增，不保证连续或跨工作流唯一，用于任务定位、依赖引用和状态结算；同一任务重新认领时保持不变。';
COMMENT ON COLUMN tasks.task_type IS '节点声明的任务类型：normal 为普通节点，event 为外部事件入口，join 为汇合节点；JOIN 按 depends_on_names 和 scopes 父子关系等待来源成功及图中的上游生产结束。';
COMMENT ON COLUMN tasks.name IS '工作流图中注册的节点名；调度器据此选择执行节点，同一节点可产生多条任务记录。';
COMMENT ON COLUMN tasks.status IS '任务状态：pending 待认领，running 已认领，succeeded 成功，failed 永久失败，skipped 跳过，superseded 失败已由后继接管。';
COMMENT ON COLUMN tasks.claimed_by IS '最近一次认领任务的 worker 标识；未认领时为 NULL，任务结算后保留，不代表租约仍有效。';
COMMENT ON COLUMN tasks.lease_until IS '本次认领的租约截止时间，执行及结算期间持续续租；NULL 表示无有效租约，pending/running 任务在无租约或到期后可按依赖重新认领；结算或释放时清空。';
COMMENT ON COLUMN tasks.attempt IS '认领代次；初始为 0，每次认领加 1，用于写入校验；超过 max_attempts 的认领只处理最终失败，不再执行业务。';
COMMENT ON COLUMN tasks.max_attempts IS '该任务行的业务执行次数上限，默认 3（含首次）；耗尽后重新认领并通过统一失败流程结算，失败边新建任务单独计数。';
COMMENT ON COLUMN tasks.depends_on_names IS 'JOIN 所需的前驱节点名；通过 scopes 父子关系确定实例范围，上游生产结束条件由执行图提供。';
COMMENT ON COLUMN tasks.input_json IS '节点输入的 JSON 值；结构由节点输入类型决定，JOIN 输入按前驱节点名汇聚。';
COMMENT ON COLUMN tasks.output_json IS '节点成功输出的 JSON 值；结构由节点输出类型决定，尚未写入输出时为 SQL NULL。';
COMMENT ON COLUMN tasks.error IS '失败、跳过或被后继接管的原因文本，也记录重试预算耗尽及依赖失败原因；成功结算时清空。';
COMMENT ON COLUMN tasks.created_at IS '任务记录创建时间，由数据库时钟写入。';
COMMENT ON COLUMN tasks.updated_at IS '任务记录最近更新时间；由仓储在认领、结算、来源合并或租约变更时显式更新，无自动更新触发器。';

-- 调度器轮询：就绪 + 过期 running。
CREATE INDEX IF NOT EXISTS idx_tasks_claim
ON tasks(workflow_id, status, lease_until);

-- 恢复扫描：按 status 快速定位候选工作流。
CREATE INDEX IF NOT EXISTS idx_tasks_pending_sessions
ON tasks(status, workflow_id)
WHERE status = 'pending';

-- JOIN 分组合并、前驱就绪与失败检查。
CREATE INDEX IF NOT EXISTS idx_tasks_scope_name
ON tasks(workflow_id, run_id, scope_id, name, status);

-- 子图的执行实例，仅保存身份和父子关系，无独立任务状态或调度生命周期。
CREATE TABLE IF NOT EXISTS scopes (
    workflow_id TEXT NOT NULL,
    scope_id BIGINT NOT NULL CHECK (scope_id > 0),
    run_id BIGINT,
    subgraph_id TEXT NOT NULL,
    parent_scope_id BIGINT,
    trigger_task_id BIGINT NOT NULL,
    PRIMARY KEY (workflow_id, scope_id),
    FOREIGN KEY (workflow_id, parent_scope_id) REFERENCES scopes(workflow_id, scope_id)
);
CREATE INDEX IF NOT EXISTS idx_scopes_parent ON scopes(workflow_id, run_id, parent_scope_id);
