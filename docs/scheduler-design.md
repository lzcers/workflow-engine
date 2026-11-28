# Scheduler 设计与调度流程

本文描述当前调度器、共享唤醒中枢与双后端原子结算的运行机制。

核心代码：[Engine](../src/engine.rs)、[WakeHub](../src/scheduler/wake_hub.rs)、[Workflow](../src/workflow.rs)、[Scheduler](../src/scheduler/mod.rs)、[公共事务编排](../src/backend/common/mod.rs)、[槽位 JOIN 推进](../src/task/join.rs)。

## 职责与并发模型

| 组件 | 职责 |
| --- | --- |
| `Engine` / `WakeHub` | 持有共享 `Database` 与 `TaskStore`、跨工作流共享的单轮询器（`ready_workflows` 一次部分索引查询覆盖全部工作流）与 PostgreSQL 共享 `PgListener` |
| `Workflow` | 入口强类型预检提交（`submit` / `prepare_task`）与优雅停机 `shutdown` |
| `Scheduler` | 空槽认领（`claimed == available` 才立即续查，消除轻载双倍认领）、租约续期、指数退避+抖动、结算提交与独立回调排空 |
| `common::enqueue_fanouts_and_advance` | PG/SQLite 共用的事务编排：批量写入任务与作用域、累加器槽位合并、短路与单趟不动点 JOIN 推进 |
| `Graph` | 邻接索引 `O(出度)` 规划、零拷贝透传边、`NodeContext` 统一生命周期钩子 |

## 调度主循环

1. **回收已完成任务**：`in_flight.try_join_next()` 结算完成后立即释放执行槽，若有后继则通知 `wake`，同时把 `NodeContext` 回调（`on_success`/`on_error`）派发到独立的 `callbacks` `JoinSet` 中，避免慢回调阻塞下一批认领。
2. **优雅停机**：收到 `shutdown` 信号后停止认领新任务，先排空 `in_flight`，再排空 `callbacks` 后退出。
3. **按空槽认领**：`available = max_concurrency - in_flight.len()`；仅当 `claimed == available` 时立即 `continue` 续查，避免轻载多打一倍空查询。
4. **等待事件**：`tokio::select!` 监听 `shutdown`、`in_flight` 完成、`callbacks` 完成、`wake`（来自本地提交、本地结算或 `WakeHub` / PG `LISTEN/NOTIFY`）。

## 单任务生命周期（`run_task`）

1. **租约护卫**：与 `maintain_claim` 并发 `select!`，每 `LEASE_DURATION / 3` 续租一次；续租返回 `None` 或超时立即取消本地未来。
2. **执行与重试**：
   - `attempt > MAX_ATTEMPTS`：不再执行业务节点，直接走失败后继规划与结算；
   - 可重试错误且 `attempt < MAX_ATTEMPTS`：按指数退避 + 抖动调用 `schedule_retry` 写入 `available_at`；
   - 成功或永久失败：规划后继并调用 `settle_success` / `settle_failure` 原子落库。
3. **异常处理**：结算事务失败或节点 panic 时，在 `release_claim` 中写入真实诊断原因并附加短退避 `RELEASE_BACKOFF`。
