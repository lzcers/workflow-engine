# Scheduler 设计与调度流程

本文描述当前调度器、共享唤醒中枢与双后端原子结算的运行机制。

核心代码：[Engine / WakeHub](../src/engine.rs)、[Workflow](../src/workflow.rs)、[Scheduler](../src/scheduler.rs)、[公共事务编排](../src/backend/common.rs)、[槽位 JOIN 推进](../src/task/join.rs)。

## 职责与并发模型

| 组件 | 职责 |
| --- | --- |
| `Engine` / `WakeHub` | 持有共享 `TaskStore`、跨工作流共享的单轮询器（`ready_workflows` 一次部分索引查询覆盖全部工作流）与 PostgreSQL 共享 `PgListener` |
| `Workflow` | 入口强类型预检提交、运行控制面（`get_run`/`list_tasks`/`cancel_run`/`wait_run`）、优雅停机与取消 |
| `Scheduler` | 空槽认领（`claimed == available` 才立即续查，消除轻载双倍认领）、租约续期、指数退避+抖动、结算提交与独立回调排空 |
| `common::enqueue_fanouts_and_advance` | PG/SQLite 共用的事务编排：任务上限护栏、批量写入任务与作用域、O(1) `join_contributions` 追加、短路与子树化 JOIN 推进 |
| `Graph` | 邻接索引 `O(出度)` 规划、零拷贝透传边、`NodeContext` 统一生命周期钩子 |

## 调度主循环与回调解耦

1. `Scheduler::run` 维护两个独立的 `JoinSet`：
   - `in_flight`：跟踪正在执行与结算的任务，决定并发槽配额 `available = max_concurrency - in_flight.len()`；
   - `callbacks`：执行结算提交后的 `NodeContext::on_success` / `on_error`，**不占用执行槽**，停机时与 `in_flight` 一并排空。
2. 每次认领 `claim_many(workflow_id, available, lease_duration)`：
   - 仅当 `claimed == available` 时立即 `continue` 补槽；若 `claimed < available` 则直接进入 `select!` 等待，不再发必然空集的二次查询。
3. 任务完成原子结算后立即从 `in_flight` 回收，释放执行槽，并触发 `settled.notify_waiters()`（即时唤醒 `wait_run`）和 `wake.notify_one()`（若有后继立即认领）。
4. 任务 panic 或结算失败调用 `release_claim` 时保留原始错误说明并在未耗尽预算时附带短退避；超出 `max_tasks_per_run` 护栏则直接以永久失败结算。
5. `cancel_run` 将指定运行所有活跃任务置为 `cancelled` 并清理 `join_contributions`；正在执行的旧 worker 在下次续租或结算时因状态不再为 `running` 而自动失权退出。

## 作用域精细锁与 O(1) Append 物化

- 结算时通过 `SettlementRules::affected_scope_depths` 仅锁定受当前节点影响的已有作用域（PG 按 `0 < parent < child` 严格升序一次性 `pg_advisory_xact_lock`），无受影响 JOIN 时零 advisory lock；独立子图实例完全并行结算。
- `Append` 槽位在 `collecting` 阶段向 `join_contributions` 做 O(1) 批量追加，`tasks.input_json` 只保留槽位键占位标记。
- `advance_joins` 先按受影响作用域短路探测是否存在 `collecting` 行；存在时仅加载对应子树作用域与外部触发任务，单趟不动点推进，就绪时一次性从 `join_contributions` 物化数组到 `tasks.input_json` 并删除临时条目。
