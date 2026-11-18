# 外部事件触发：一手资料研究

研究日期：2026-11-17；实现方案更新于 2026-11-18。项目支持图内触发声明及外部提交，具体触发器注册、定时调度与监听由宿主实现。

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

## 当前实现

图负责声明触发条件，宿主负责具体触发器的注册、注销与监听，引擎通过现有提交路径执行事件。

- Event 节点实现同一个 `Node`，声明 `TASK_TYPE = TaskType::Event`，使用原来的 nodes/edges 注册方式。[Node](../src/node/mod.rs)
- `Node::trigger()` 返回 `Option<Trigger>`，普通节点默认返回 `None`。`Trigger { kind: String, config: Value }` 是描述数据，不具有独立身份或运行生命周期。种类和具体配置由宿主解释。
- Event 节点必须有触发声明，其他节点必须没有；成功边和失败边都不能指向 Event 节点。构图时验证这些规则。[Graph](../src/graph/mod.rs)
- `Graph::describe()` 的节点包含 `task_type` 与 `trigger`。`Graph::triggers()` 和 `Workflow::triggers()` 返回按节点名排序的 `(node_id, Trigger)` 列表，供宿主注册；这些查询均不会建立监听。[Description](../src/graph/description.rs)、[Workflow](../src/workflow.rs)
- `trigger()` 只读取固定的节点配置，不依赖任务上下文。图中不保存注册句柄，也不提供 `activate` 或 `events` 接口。

完整路径：宿主读取触发声明 → 注册触发器 → 接收并验证事件 → `workflow.submit(node_id, input)` → 持久化入口任务并唤醒 Scheduler → 执行 Event 节点及其后继。

`Workflow::submit` 在同一事务内确定独立 `run_id`、保存输入及节点声明的 `task_type`。Event 任务创建后按普通任务执行，`execute` 可以透传或处理事件；Scheduler 不等待外部事件。`Workflow` 管理自身调度器生命周期，宿主管理外部触发源生命周期。[TaskStore](../src/task/store.rs)、[Scheduler](../src/scheduler.rs)

## 责任边界

每次成功提交创建独立运行，普通节点仍允许手动提交。多个触发入口的事件不能通过多次 `submit` 合并成同一次运行，也不能补齐同一次运行的 JOIN。

cron 表达式校验、时区处理、迟到和重叠策略、消息确认、注册所有权及事件去重均由宿主负责。`submit` 返回成功表示入口已持久化，不表示工作流完成，也不提供外部事件去重。上面的可靠调度研究是宿主实现的参考，不代表引擎提供了这些保证。

本次只实现启动新运行，不引入图中间等待、关联恢复或消息投递到既有运行的语义。HTTP 服务、cron 库和消息客户端由宿主选用，无需向引擎增加这些依赖。
