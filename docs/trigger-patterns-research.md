# 外部事件触发：一手资料研究

研究日期：2026-11-17。本文是设计依据和建议，不代表本项目已经实现触发器、定时调度或运行恢复消息。

## 已核对的成熟产品模式

| 问题 | 已验证事实 | 对本项目的启示（设计推论） |
| --- | --- | --- |
| 触发器何时生效 | n8n 的生产 Webhook 在工作流发布时注册；Schedule Trigger 也要求发布工作流。[Webhook](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.webhook)、[Schedule Trigger](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.scheduletrigger) | 触发源的启停属于工作流的启用生命周期，不属于某一次节点任务的执行。 |
| 统一抽象是否意味着统一监听方式 | n8n 源码将 Webhook、轮询、常驻监听、定时触发分开处理；Webhook 通过注册路由工作，不要求为它保持持续执行。[ActiveWorkflowManager](https://github.com/n8n-io/n8n/blob/master/packages/cli/src/active-workflow-manager.ts) | 可以统一“事件进入后如何启动运行”，不必强迫 HTTP handler 和 cron 循环实现相同的常驻 `run()` 方法。 |
| 定时计划与单次执行是什么关系 | Temporal Schedule 有独立身份，按时间启动 Workflow Execution；暂停 Schedule 不会暂停或影响它已经启动的执行。[Schedule](https://docs.temporal.io/schedule) | 一个 cron 定义对应多次独立运行；启用定时器时不需要先创建一个永远等待的节点任务。 |
| 如何处理定时可靠性 | n8n durable scheduler 把计划和即将到来的触发持久化，由实例认领；崩溃后回收认领。过期触发按 misfire 策略处理。[Durable scheduler](https://docs.n8n.io/deploy/host-n8n/configure-n8n/durable-scheduler) | 仅在进程中放一个 timer 不足以提供重启恢复。持久化时间游标与入队原子性必须有明确责任方；不需要照搬 n8n 的全部阶段和实体。 |
| 新执行与已有执行如何区分 | Temporal Signal 是向运行中工作流发送异步写消息；Signal-With-Start 明确规定：存在对应运行则发送，否则启动再发送。[Workflow message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing)、[Sending messages](https://docs.temporal.io/sending-messages) | `start` 和 `resume/signal` 应有不同契约，不能仅因传输都是 Webhook 就当成同一种启动行为。当前需求可以只实现 start。 |
| 图中间的 Webhook 等待如何表示 | n8n Wait 暂停当前执行；On Webhook Call 使用运行时生成、每次执行独有的恢复 URL。[Wait](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.wait) | 中途等待必须定位已有运行/等待点，处理相关性与持久化等待；不应复用“每事件新建运行”的开始触发器语义。 |

Temporal 还将重叠执行策略和迟到补发窗口作为 Schedule 行为显式定义；例如 Skip、BufferOne 和 AllowAll 是不同的业务语义，而非底层任务并发数。[Schedule policies](https://docs.temporal.io/schedule#policies)

## 与当前代码的对应

- 本项目把节点定义为一次 `execute(input) -> output`，Scheduler 从存储认领任务后执行；因此长时间监听外部事件会占用一个在途任务槽位。[Node](../src/node/mod.rs)、[Scheduler](../src/scheduler.rs)
- 宿主已经负责 worker 生命周期、唤醒与恢复扫描；触发源的 HTTP 路由和 cron 驱动可延续这个责任边界。[README](../README.md)
- SQLite 后端已经提供 `insert_task(tx, NewTask)`，可在宿主事务中插入入口任务；独立 `enqueue()` 只是插入并提交，不等同于事件去重。[SqliteTaskRepo](../src/backend/sqlite/repo.rs)
- 当前 `run_id` 是可承载业务血缘的分组键，JOIN 汇合与等齐前驱都依赖它；并不是已定义好的不可变工作流执行身份。每个新事件必须获得隔离的分组范围，不能让所有触发事件都落入相同的 `run_id = NULL` 分组；若业务会改变血缘，应先确定独立执行身份的边界。[TaskRecord](../src/task/record.rs)
- 已有 Tokio、serde、SQLx 和可选 chrono；当前依赖中没有专门的 cron 解析器或 HTTP 服务框架。应用已有 HTTP 服务时，应直接接入其路由；cron 计算应选成熟库，具体包需要另行核对维护情况。[Cargo.toml](../Cargo.toml)

## 建议的最小闭环

保持现有任务调度核心：外部触发源接收事件 → 验证与确定目标入口 → 在同一事务内完成事件去重与入口任务插入 → 提交后唤醒现有 Scheduler → 按既有图执行后继。

若图上需要显示 Cron/Webhook 开始节点，它可以代表入口声明及事件载荷的短暂处理。监听和定时计划由宿主管理，不放进普通 `Node::execute` 中等待。统一的是一次触发的启动边界，传输适配保持各自实现。

先明确三个契约：每次触发的隔离身份、重复事件的幂等键、cron 迟到与重叠的固定行为。只有实际出现图中间等待需求时，再引入持久化等待和关联恢复语义。以上均为针对本项目的设计推论，而非外部产品必须照搬的实现。

## 当前讨论范围的收敛

根据本轮讨论，暂不设计持久化，不引入独立 Trigger 实体、TriggerSpec 或注册体系。上面的持久化内容仅保留为研究背景，不是当前实施范围。

建议在现有 `TaskType` 中增加 `Event`，表示节点的输入来自外部事件。CronNode、WebhookNode 仍实现同一个 `Node`，使用原来的 nodes/edges 注册方式。为让节点自行提供事件，在 Node 上增加可选 `events()` 方法，使用已有 futures 的 `BoxStream` 输出该节点的强类型输入；Event 节点必须提供事件流，否则启用时报告装配错误。默认方法返回 None，普通节点和 JoinNode 不订阅事件流。

调度器启用时订阅 Event 节点，等待事件不占普通任务并发槽。事件到达后创建入口任务，再经过现有 `NodeContext::fork(task)`、`execute` 和后继规划。Node 的事件流使用构造时注入的依赖，不需要虚构一个尚未发生的任务上下文。Graph 只允许 Event 节点作为入口，不允许成功边或失败边指向它；描述继续使用 task_type。订阅在运行期创建，不在 build/describe 时创建，并随所属调度器停止而释放。

现有 TaskStore 没有通用的根任务入队方法，落地时需要补齐这个操作；该缺口独立于是否选择数据库持久化。当前建议仍是设计，未实现这些接口。
