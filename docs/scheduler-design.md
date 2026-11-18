# Scheduler 设计与调度流程

本文描述当前实现：`Engine::make(graph)` 创建工作流实例并立即启动调度器，
`Workflow::submit` 创建独立运行并唤醒认领。`Scheduler::run()` 持续执行，生命周期由所属实例管理。

核心代码：[Scheduler](../src/scheduler.rs)、[Workflow](../src/workflow.rs)、
[TaskStore](../src/task/store.rs)、[Graph](../src/graph/mod.rs)。

## 职责与运行范围

一个 Scheduler 绑定一张固定的 Graph，按 `workflow_id` 认领该图所有运行的任务。
数据库保存待执行任务和依赖关系；调度器只在有空槽时认领，不在内存中维护另一份待执行队列。

| 组件 | 职责 |
| --- | --- |
| `Workflow` | 构造时启动后台调度器、提交入口任务、唤醒认领、随实例释放取消后台任务 |
| `Scheduler` | 计算空闲槽、批量认领、按固定窗口执行或处理最终失败、调用存储结算及异常释放 |
| `Graph` | 提供节点与上下文，执行节点钩子和业务逻辑，规划成功或失败后继 |
| `TaskStore` | 原子创建运行，筛选可认领任务，校验认领归属，原子提交状态与后继 |
| 数据库后端 | 实现依赖与 JOIN 闸门、租约、尝试预算和并发事务控制 |

`run_id` 标识一次提交产生的运行，`worker_id` 标识一次调度循环。一次调度循环生成一个
UUID 作为 `worker_id`，可以处理同一 `workflow_id` 下多个不同 `run_id` 的任务。
不同运行的扇入与 JOIN 判定由存储隔离。

并发上限属于 Scheduler 实例。一个 Workflow 的所有运行共享默认 8 个槽；若为同一个
`workflow_id` 创建多个实例，每个实例都有自己的额度，8 不是跨实例或跨进程的全局上限。
存储通过认领凭据防止旧执行者覆盖新认领的结果。

## 实例启动与提交

`Engine::make(graph)` 构造 Workflow 和 Scheduler，并启动一次后台 `scheduler.run()`。
因此 `make` 必须在 Tokio runtime 内调用。调度器开始认领该图已有的可执行任务，无需先提交新运行。
每次 `Workflow::submit(entry, input)` 按以下顺序执行：

1. 检查入口节点存在，将输入序列化为 JSON。节点 `In` 类型的反序列化检查留到实际执行阶段。
2. 构造未指定 `run_id` 的 Task，调用 `TaskStore::enqueue`，在同一事务内分配运行身份并保存入口任务。
3. 调用共享 `Notify::notify_one()` 唤醒认领，返回包含 `workflow_id`、`run_id` 和入口 `task_id` 的 `RunHandle`。

成功返回表示入口任务已持久化并安排执行，不等待节点完成。提交不会创建调度器，
提交失败也不影响已经启动的调度器。所有运行共享构造时启动的同一个后台任务。

存储统一使用 `enqueue(Task)` 入队：`run_id = None` 时以新任务的 `task_id` 创建独立运行，
`Some(id)` 时加入指定运行，依赖、实例编号和尝试预算沿用 Task 的字段。
运行身份在 INSERT 时直接写入，不需要先插入任务再回填。

Workflow 持有图与存储的共享句柄，Engine 释放不影响它。执行期间需要保留 Workflow；
实例释放会通过 `Workflow::drop` 中止后台任务，继而取消调度器的在途任务。
`RunHandle` 只保存运行身份，持有它不会延长 Workflow 的生命周期。

## 主调度循环：按空槽补充任务

`run()` 创建 `worker_id` 和 `JoinSet<()>`，然后进入 `run_loop`。
`JoinSet` 跟踪本实例已认领且尚未回收的 task future，其长度用于计算并发额度。

```mermaid
flowchart TD
    Start["run(): 创建 worker_id 和 JoinSet"] --> Reap["回收已结束任务，记录 JoinError"]
    Reap --> Slots["available = max_concurrency - in_flight.len()"]
    Slots --> HasSlot{"有空闲槽？"}
    HasSlot -- 是 --> Claim["claim_many(workflow_id, worker_id, available)"]
    Claim --> HasTasks{"认领到任务？"}
    HasTasks -- 是 --> Spawn["每个任务 spawn run_task，加入 JoinSet"]
    Spawn --> Reap
    HasTasks -- "空列表或存储错误" --> Wait["等待在途任务结束、Notify 或轮询间隔"]
    HasSlot -- 否 --> Full["只等待在途任务结束"]
    Wait --> Reap
    Full --> Reap
```

每轮先用 `try_join_next` 回收全部已结束任务，再计算空槽。认领数量最多为 `available`，
返回任务后逐个 spawn，随后立即进入下一轮补槽；不会等待整批任务都执行完。
`claim_many` 返回空列表或报错时进入等待，存储错误会记录日志，不退出调度循环。

等待阶段由 `tokio::select!` 驱动：

| 等待事件 | 启用条件 | 作用 |
| --- | --- | --- |
| `in_flight.join_next()` | 至少有一个在途任务 | 回收任务并释放执行槽，继续认领 |
| `wake.notified()` | 有空槽 | 本进程新提交后立即尝试认领 |
| `sleep(poll)` | 有空槽 | 再次查询数据库，发现其他来源入队或租约过期后可认领的任务 |

同时就绪时，选择顺序是任务完成、通知、轮询。满槽时不轮询，也不消耗就绪通知；
有槽时，即使其他节点很慢，通知仍可唤醒认领。Notify 只是唤醒信号，可以合并多次通知，
任务是否存在和可执行始终以数据库为准。
Scheduler 本身不建立 PostgreSQL LISTEN；其他进程提交的任务由轮询发现。

Workflow 使用 1 秒轮询间隔和 8 个执行槽，Scheduler 的构造与配置由库内部管理。
轮询间隔是进入等待后的休眠时长，
不是每秒固定扫描一次，也不是每个任务启动前都要等待的延迟。

一个槽覆盖整个 `run_task`：上下文准备、业务执行、图规划、数据库结算、成功或错误回调，
以及异常时的认领释放。槽位计数不等同于数据库中 `running` 状态的行数。

## 认领与依赖就绪

调度器把 `workflow_id`、`worker_id` 和数量上限交给 `TaskStore::claim_many`。
当前 PostgreSQL / SQLite 后端负责筛选待执行或租约已过期的任务、检查依赖和 JOIN
是否就绪，再原子写入 `running`、持有人、固定 300 秒租约及递增后的 `attempt`。

`attempt` 是参与写入校验的认领代次，每次认领都递增。`max_attempts` 限制业务执行次数：

- 认领返回的 `attempt <= max_attempts`：执行节点及其生命周期钩子。
- 认领返回的 `attempt > max_attempts`：只处理最终失败，跳过 `on_start` 和节点 `execute`。
  这类认领同样有独立执行窗口和有效持有人，以便安全规划、提交失败后继。
- `max_attempts = 0`：首次认领即进入最终失败处理，不执行业务节点。

认领事务首次发现预算耗尽时记录最终失败原因：旧租约已到期则记录执行窗口超时；
提前释放的认领或零预算任务记录预算耗尽。后续恢复认领保留该原因。
预算耗尽的行继续参与认领和恢复查询，由统一失败结算处理，不直接封存为 `failed`。

任务按存储的认领顺序进入执行槽，完成顺序取决于各节点耗时；调度器不提供按 run
轮转的公平队列。后继通过结算事务入库，满足依赖后由下一次认领取得，不在当前任务里
递归执行。其他槽可以继续推进快分支，慢分支保持在途。

## 单个任务：执行、结算与回调

每个 `run_task` 在本次认领的固定窗口内完成执行、后继规划、结算和回调。
正常执行通过 `execute_and_plan` 返回包含执行结果与后继计划的 `ExecutionOutcome`；
预算耗尽的认领直接进入同一 `plan_failure` 辅助函数，复用同步失败边规划。
失败结果保留 `NodeError` 和可选上下文：正常业务失败沿用执行上下文，恢复处理在失败提交后才创建回调上下文。

```mermaid
flowchart TD
    Claim["收到已认领的 TaskRecord"] --> Deadline{"固定窗口仍有效？"}
    Deadline -- 否 --> Expired["结束本次尝试，释放执行槽"]
    Deadline -- 是 --> Budget
    subgraph Window["共用原 lease_until 截止时间"]
        Budget{"attempt 超过 max_attempts？"}
        Budget -- 否 --> Execute
        Execute["fork → on_start → execute"]
        Execute -- "执行成功" --> PlanSuccess["plan_success"]
        Execute -- "节点返回错误" --> PlanFailure["plan_failure"]
        PlanSuccess -- "规划失败，转永久错误" --> PlanFailure
        Budget -- 是 --> Recover["读取最终失败原因"]
        Recover --> PlanFailure
        PlanSuccess -- "规划成功" --> Success["settle_success 原子提交"]
        PlanFailure --> Failure["settle_failure 原子提交"]
        Success -- "提交成功" --> Callback["on_success"]
        Failure -- "提交成功" --> ErrorCallback["复用上下文或 fork → on_error"]
        Success -- "提交失败" --> Release["尝试 release_claim"]
        Failure -- "提交失败" --> Release
    end
    Callback --> Done["任务结束，槽位可回收"]
    ErrorCallback --> Done
    Release --> Done
    Window -- "窗口到期" --> Expired
    Expired --> Reclaim["未结算任务等待下一次认领"]
    Reclaim --> Claim
```

### 执行阶段

窗口有效且还有执行预算时，按顺序执行 `NodeContext::fork(task)`、`on_start` 和节点 `execute`。
每个任务获得独立上下文，图和节点实例仍通过共享句柄复用。

执行成功后，`Graph::plan_success` 生成后继，并转换为存储使用的 `FanoutPlan`。
若成功后继规划出错，则转为永久业务错误，进入同一失败路径。

执行或成功后继规划失败时，用原始输入和错误调用 `Graph::plan_failure`，
保留执行上下文供失败提交后的 `on_error` 使用。失败后继规划本身出错时记录日志，使用空后继列表结算失败。
执行阶段生成结果与入队计划，不提交任务状态。

固定窗口规则如下：

- 认领时按数据库时间设置 `lease_until = 当前时间 + LEASE_DURATION`，时长为 300 秒，不续租。
- 本地等待使用这次认领的剩余窗口，调度等待与进程暂停已经消耗的时间不会重置。
- 窗口覆盖 `fork`、`on_start`、`execute`、后继规划、`on_error`、结算、`on_success` 和异常认领释放。
- 到期后取消本地 future 并释放执行槽；未提交结算的任务由后续认领继续处理。
  即使所有执行槽都在等待永不完成的异步操作，到期后也能腾出槽位进行恢复。
- 本地计时负责停止执行，存储使用数据库当前时间作最终权限校验；到期结果不会被接受。

当返回的认领代次超过 `max_attempts`，调度器在 `fork` 之前转入恢复处理，
将持久化的最终失败原因作为 `NodeError::Permanent` 交给 `Graph::plan_failure`，使用同一
`settle_failure` 原子提交最终结果。只有提交成功后才 `fork` 回调上下文并调用 `on_error`，
因此上下文准备或错误回调卡住不会阻止最终状态与失败后继落库。
最后一次执行者已崩溃也不影响这条恢复路径。恢复处理本身受新认领的固定窗口约束，
提交前未完成时可再次认领，仅重复失败处理，业务执行次数不再增加。

### 结算阶段

存储必须在结算事务中再次验证任务状态、`claimed_by`、`attempt`
以及数据库时间下的租约有效性；旧认领不能提交结果或创建后继。
仅用于最终失败处理的认领也不能提交成功状态。

| 执行结果 | 原子存储操作 | 提交后的动作 |
| --- | --- | --- |
| 成功 | `settle_success` 写入输出、标记 `succeeded`、入队全部成功后继 | 调用 `on_success`，然后结束任务 |
| 失败且后继计划非空 | `settle_failure` 标记 `superseded`，入队失败后继 | 调用 `on_error`，然后结束任务 |
| 失败且后继计划为空 | `settle_failure` 标记 `failed`，级联同一运行中已有的直接依赖 pending 任务 | 调用 `on_error`，然后结束任务 |

状态变更和后继入队在同一事务内提交，任一步失败整体回滚。
`on_error` 和 `on_success` 都在对应结算提交成功之后调用。
正常业务失败沿用执行时的上下文；恢复处理没有执行上下文，因此失败提交后才调用 `fork`。
回调仍占执行槽，并受原执行窗口限制；后继已经入库，可能由其他空槽或其他 worker
在回调结束前执行。因此两个回调都不构成后继任务的依赖屏障，回调失败或超时也不会撤回
已经提交的状态与后继。

## 异常、取消与恢复

| 情况 | 当前处理 |
| --- | --- |
| 认领报错 | 记录错误，按主循环的事件或轮询机制再次尝试 |
| 执行窗口到期 | 取消本地 future、释放槽位，未结算任务由后续认领重试或处理最终失败 |
| 预算耗尽 | 取得新认领，规划失败后继并原子结算，提交后再创建回调上下文并调用错误钩子 |
| 结算报错 | 记录错误并尝试释放认领，再次认领后按剩余预算执行或处理最终失败 |
| 单任务执行、结算或回调 panic | `run_task` 捕获 panic，记录错误并尝试释放认领 |
| 认领释放失败 | 记录警告；后续由存储的租约与恢复机制处理 |
| 调度主循环 panic | 取消并等待 JoinSet 中的任务退出，再传播 panic |
| Workflow 释放或 run future 被取消 | 取消在途任务，不等待结算，也不会自动执行异步认领释放 |

释放认领同样校验当前有效持有人和 `attempt`。已经过期、被接管或结算到终态的任务，
不会被旧执行者的清理操作改回待执行。回调 panic 时，已提交的状态和后继不会回滚。
结算与回调之间可能发生进程退出或窗口到期，因此 `on_success` 和 `on_error` 都不保证必达，
也没有独立的补发机制。

`NodeError::Retryable` 不会在当前调度器里自动触发退避或重新执行同一任务；业务错误会
经过失败边规划与失败结算。因结算异常、任务 panic 或租约过期而重新认领时会消耗新的
`attempt`；代次超过 `max_attempts` 后仅处理最终失败。失败边产生的新任务有自己的
尝试预算，不属于原任务的重新认领。

实例取消时已经发生的外部副作用不会撤回，未结算任务的认领可能保留到租约过期。
重新认领可能重复执行业务逻辑，外部写入应按任务身份设计幂等；数据库原子结算不提供
外部副作用的 exactly-once 保证。Tokio 的取消在异步任务让出执行权时生效。

进程重启后，连接同一持久化数据库并为相同 `workflow_id` 重新调用 `Engine::make(graph)`，
新调度器即可处理该图的存量任务，无需新的 `submit`。遗留的 `running` 任务保持原状态，
旧租约过期后由认领事务更新持有人、创建新的固定窗口并递增 `attempt`。有执行预算则
从节点开头重新执行，预算已耗尽则进入最终失败处理。恢复时不会提前释放其他 worker 的
有效租约，旧 worker 返回后也无权修改新认领或已经结算的任务。

Scheduler 只轮询自己绑定的图。应用需要为要恢复的图创建并保留 Workflow 实例；
引擎不会从数据库自动发现或重建尚未实例化的图。SQLite 内存及临时数据库不支持跨进程恢复。
当前 Workflow 不自动重启发生 panic 的调度器。

## 代码导航

| 实现位置 | 阅读重点 |
| --- | --- |
| [src/workflow.rs](../src/workflow.rs) | 构造时启动、`submit`、就绪通知、`Workflow::drop` |
| [src/scheduler.rs](../src/scheduler.rs) | `run_loop` → `claim_and_spawn` → `run_task`，固定窗口内 `execute_and_plan` / `plan_failure` → `settle` |
| [src/task/store.rs](../src/task/store.rs) | 存储契约、`FanoutPlan`、租约时长 |
| [src/graph/mod.rs](../src/graph/mod.rs) | 节点执行、成功与失败后继规划 |
| [src/backend/pgsql/repo.rs](../src/backend/pgsql/repo.rs) | PostgreSQL 认领、结算与恢复查询 |
| [src/backend/sqlite/repo.rs](../src/backend/sqlite/repo.rs) | SQLite 认领、结算与恢复查询 |
| [tests/sqlite_backend.rs](../tests/sqlite_backend.rs) | 提交自动执行、共享槽、通知唤醒、扇出与 JOIN 的现有验证 |
