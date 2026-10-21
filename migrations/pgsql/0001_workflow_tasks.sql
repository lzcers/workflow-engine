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
-- run 产出节点前为 NULL。
CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT   NOT NULL,
    run_id      BIGINT,
    task_id     BIGINT NOT NULL CHECK (task_id > 0),
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

-- 调度器轮询：就绪 + 过期 running。
CREATE INDEX IF NOT EXISTS idx_tasks_claim
ON tasks(workflow_id, status, lease_until);

-- 恢复扫描：按 status 快速定位候选工作流。
CREATE INDEX IF NOT EXISTS idx_tasks_pending_sessions
ON tasks(status, workflow_id)
WHERE status = 'pending';

-- 多实例扇出去重键：同一 (workflow, run, name, instance) 的多实例行唯一。
-- 覆盖「原子结算事务已提交但响应丢失」的重执行场景。用 COALESCE(run_id, 0) 兜底
-- NULL（PG 唯一索引默认 NULL 互不相等）。部分索引只覆盖 instance_id IS NOT NULL 的
-- 多实例行；单实例行走 FOR UPDATE 扇入合并去重，不在此键内。
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_fanout_instance
ON tasks (workflow_id, COALESCE(run_id, 0), name, instance_id)
WHERE instance_id IS NOT NULL;
