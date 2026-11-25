# workflow-engine

独立的**通用工作流引擎**：强类型节点、图引擎、通用调度器和 PostgreSQL / SQLite 后端。
引擎不包含业务字段，适合由应用自行定义节点与任务上下文。

## 定位

- 公共 API 围绕 `graph`、`engine`、`workflow`、`task` 和 `dsl` 组织；节点、上下文与建边接口属于图定义。
- 通过 `Engine` 使用数据库，默认启用 SQLite；`pg` / `sqlite` feature 只选择后端，至少启用一个，也可同时启用。
- 通过 `Engine::new(url).await` 创建可复用引擎，再用 `engine.make(graph)` 创建非泛型 `Workflow` 句柄并启动后台调度。
- `Workflow::submit` 创建独立运行并唤醒调度器；数据库连接、任务存储、后端仓储和调度器由库内部管理。

## 公共模块

| 模块 | 职责 |
| --- | --- |
| [`graph`](src/graph) | `Graph`、构图接口、节点 `Node`、上下文 `NodeContext`、`JoinNode`、成功/失败边与图描述 |
| [`engine`](src/engine.rs) | `Engine` 按 URL 初始化共享连接池，通过 `make` 为不同的图创建工作流实例；`EngineError` 描述初始化与提交错误 |
| [`workflow`](src/workflow.rs) | 单图 `Workflow` 提交独立运行并自动执行，返回 `RunHandle` |
| [`task`](src/task) | 任务输入 `Task`、任务快照 `TaskRecord` 与状态 `TaskStatus` |
| [`dsl`](src/dsl.rs) | `workflow!` 宏将 `nodes { … }` 和 `edges { … }` 展开为图构建链，并做编译期类型接线校验 |

常用类型可直接从 crate 根导入，例如 `use workflow_engine::{Engine, Graph, Node, NodeContext};`。
图描述、节点标识与命令式建边接口通过 `workflow_engine::graph` 访问。

## 公共任务模型

PostgreSQL 和 SQLite 共用 `TaskRecord`、`Task`、`TaskStatus`。业务上下文实现
`NodeContext`，`fork` 接收 `&TaskRecord`，不依赖数据库任务类型。`TaskStatus` 使用枚举，序列化为
`pending`、`collecting`、`running`、`succeeded`、`failed`、`superseded`；数据库中的未知状态
会返回解码错误。任务模型由两个后端共享。
`Task` 表示独立运行的入口，包含 `workflow_id`、节点名、输入和子图路径。根节点用 `Task::new(workflow_id, name, input)`，图描述的 `task(entry, input)` 可为任意已注册节点解析路径。
`Database::submit(task)` 将入口持久化，以新任务 ID 作为 `run_id`。执行端结算时，后继继承该运行 ID。
`TaskRecord` 是带身份、状态和结果的任务快照。业务侧可以在节点上下文和生命周期回调中读取当前任务。每条任务最多执行业务 3 次。

`parent_task_ids` 按贡献顺序记录实际来源，入口为空。普通后继每条输入对应一个新任务。
任务终态与后继创建在同一事务提交，认领代次阻止重复结算。

成功边向 JOIN 提交 JSON 对象，按对象键合并，同名键的多次贡献累积为数组。
引擎按 `workflow_id + run_id + scope_id + name` 查找状态为 `collecting` 的 JOIN，合并输入并去重记录来源 ID。
每次上游结算写完后继后，存储按作用域检查此次运行中仍在收集的 JOIN。前驱就绪时转为 `pending` 并固定输入。租约过期时使用同一输入重试，晚到的成功贡献开启下一次汇合。
直接提交到 JOIN 的独立运行入口，以及失败边指向 JOIN 时产生的每条后继，均以 `pending` 入队，使用各自的输入执行。
即使本次没有产生后继，上游结算也可能使 JOIN 就绪。

同一 Engine 创建的工作流共享任务存储和连接池。调度器按 workflow_id 认领任务，结算时传入当前图预计算的结算规则与后继列表。

## 子图与执行作用域

子图是 DSL 的静态嵌套结构，包含节点和边，不是一个执行节点：

```rust,ignore
nodes { etfs => EtfCodesNode }
subgraph analysis {
    nodes { daily => DailyNode, signal => SignalNode, report_join => JoinNode, report => ReportNode }
    edges {
        daily -> signal,
        daily -> report_join |> |bars| vec![json!({ "bars": bars })],
        signal -> report_join |> |signals| vec![json!({ "signals": signals })],
        report_join -> report,
    }
}
edges { etfs -> analysis.daily foreach }
```

跨边界边直接引用真实节点的完整点路径。子图可以嵌套；例如内层 `analysis.detail` 的
父子图为 `analysis`。`nodes` 块可以与 `subgraph` 交错，节点构造表达式按声明顺序求值。
宏通过 `workflow-engine-macros` 解析子图定义；边仍通过类型化 NodeKey 在编译期检查。

任务只保存当前 `scope_id`，`scopes` 表保存每次子图调用的 `subgraph_id`、父 scope 和
入口任务。根作用域为 NULL。进入子图创建实例，内部继承实例，退出时恢复目标层的父实例；
输出立即触发普通后继，不等待整个子图完成。重试及进程恢复沿用已有实例。

同一来源任务一次结算中的普通跨入边共享目标子图实例，不同来源任务不会自动合并。
`foreach` 将源输出 `Vec<T>` 逐项传给目标，要求跨入显式子图，每个元素建立独立实例；
不同 foreach 边不按下标隐式配对。`map` 只转换或扇出数据，不为每条输入单独分组。
空数组不创建任务，`foreach` 不直接指向 JOIN。

JOIN 在同一实例内聚合来源，结算时按作用域入口筛选本次调用的有效前驱。边界路由、scope 创建、
源任务结算和后继入队在同一事务中完成。子图定义通过 `GraphDescription.subgraphs` 输出，
节点通过 `subgraph_id` 声明归属；画布折叠不改变真实节点和边。

等待条件按有效前驱名对应的任务状态判断，不逐条记录入边是否到达。跨子图退出时，JOIN
查询所属作用域及其后代中的同名任务，并等待仍可能创建这些任务的上游结束；跨子图进入时，
通过作用域的 `trigger_task_id` 识别本次调用的外部来源，避免混入其他调用。

组内 JOIN 等待每个必需的前驱名至少有一次成功，忽略已由失败后继接管的历史任务。
条件分支如果使必需前驱从未创建，JOIN 会持续等待。
外部入口贡献绑定创建该实例的具体任务，不自动关联不同入口事件。
多个外部入口是不同子图调用的备选来源，当前实例只等待真实入口对应的贡献；组内必需前驱仍须全部完成。
父层 JOIN 可收集其子图实例的输出，并等待相关上游生产结束；普通跨出边仍立即继续。

DDL 只初始化当前 schema，不迁移旧表；旧 schema 数据库需要另行重建。

## 引擎与工作流实例

`Engine::new(url).await` 连接数据库、幂等创建引擎表并持有共享连接池。
`engine.make(graph)`（或 `engine.make_with(graph, WorkerOptions { .. })`）接收 `Graph<Ctx>` 并返回非泛型的 `Workflow`，构造时立即启动一个后台调度器，
因此必须在 Tokio runtime 内调用。调度器会认领当前图已有的可执行任务，无需先提交新运行。
同一 Engine 可以创建不同上下文类型的工作流。每个 `Workflow` 与自己的调度器共享同一个 `Graph<Ctx>`，节点执行保留类型约束。宿主可以直接将不同上下文的工作流放入同一集合，
并通过 `workflow.graph()` 查询原始图的身份、描述、节点类型、子图路径和触发声明。
节点查询对未知名称返回 `None`；提交入口时要求节点属于当前工作流的图，后继在图规划阶段填入目标子图路径。
需要具体上下文或其他图操作时，调用 `workflow.graph_as::<Ctx>()` 获取 `Option<&Graph<Ctx>>`；
类型不匹配返回 `None`。图访问直接读取共享对象，不保存独立的节点类型或触发配置副本。
不同图定义应使用不同的 `workflow_id`。

```rust,no_run
use serde_json::Value;
use workflow_engine::{Engine, Graph, NodeContext};

async fn execute<Ctx>(
    graph: Graph<Ctx>,
    input: Value,
) -> Result<(), Box<dyn std::error::Error>>
where
    Ctx: NodeContext + Sync + 'static,
{
    let engine = Engine::new("sqlite://workflow.db").await?;
    let workflow = engine.make(graph);
    let description = workflow.graph().describe();
    let context: &Ctx = workflow.graph_as::<Ctx>().expect("original context type").context();
    // 创建 run_id 并唤醒调度器，返回时不等待运行结束。
    let run = workflow.submit("entry", input).await?;
    println!("submitted run {}", run.run_id);

    // 执行期间保持 workflow 存活。
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

`submit(entry, input)` 检查节点名并序列化输入，在同一事务内分配 `run_id` 并保存入口任务。
入口由每次提交指定，输入需要符合该节点的 `In` 类型。每次提交都是独立运行：
`workflow_id + run_id` 确定运行身份，后继继承 `run_id`，不同运行的 JOIN 汇合相互隔离。
选定入口需要能触达后续 JOIN 所需的前驱；多次 submit 不能补齐同一次运行的多个入口。
`RunHandle` 保存 `workflow_id` 和 `run_id`。入口任务的 ID 即 `run_id`。
提交成功后立即唤醒认领，返回时不等待运行结束。调度器执行当前图下所有已提交及后续
提交的运行，空闲时持续等待。每个 Workflow 的所有运行共享一个调度器、8 个执行槽和
1 秒轮询周期，提交不会再创建调度器。

执行期间需要保持 Workflow 存活。正常退出可调用 `workflow.shutdown().await` 停止认领新任务并等待在途任务完成；
直接丢弃 Workflow 会通过 Drop 取消后台调度器和在途节点，不等待结算完成；已持久化的任务仍保留在数据库。Engine 释放后 Workflow 仍可使用。

进程重启后，连接同一持久化数据库并为相同 `workflow_id` 重新 `make(graph)`，即可恢复
存量任务，无需新的 `submit`。遗留的 `running` 任务等待旧租约过期后重新认领并从节点
开头执行，每条任务最多执行业务 3 次。预算耗尽后取得的新认领只处理最终失败，
通过失败边生成后继，或标记失败并传播依赖失败。引擎只调度已创建实例的图，不会从
数据库自动发现并重建其他图。SQLite 内存及临时数据库不支持跨进程恢复。

上述示例使用默认 SQLite 后端，PostgreSQL 使用 `pg` feature 和对应连接 URL。

### 从控制面入队，在独立进程执行

多机部署使用共享 PostgreSQL，单机进程也可连接同一个 SQLite 文件。进程可以通过 `Database::connect(url)` 建立连接，或用已有 SQLx 连接池构造 `Database::from(pool)`，然后调用 `database.initialize().await` 建表。

每个 `workflow_id` 对应一个不可变定义。向根节点提交独立运行时，只需构造一个任务。

```rust,ignore
use workflow_engine::Task;

let run = database.submit(Task::new(workflow_id, "start", input)).await?;
```

控制面接受用户选择的节点时，可用 `description.task(entry, input)?` 校验节点并填好子图路径，再将这个任务交给存储。存储分配 run ID，并为子图入口创建运行时的作用域。

cron 需要同时记录本次触发已处理。通过 `Database::in_transaction(&mut tx)` 复用已有的 SQLx 事务，任务和游标一起提交或回滚。

```rust,ignore
use workflow_engine::Database;

let mut tx = pool.begin().await?;
let run = Database::in_transaction(&mut tx).submit(task).await?;
advance_schedule(&mut tx, next_at).await?;
tx.commit().await?;
```

执行进程加载实际的图并启动调度器。

```rust,ignore
let engine = Engine::from_database(database);
let workflow = engine.make(graph);
```

宿主持有 `Workflow` 以维持调度器生命周期。多个进程可同时认领同一 `workflow_id` 下的任务，每次认领都会递增该任务的 `attempt`。工作流定义变化时使用新的 `workflow_id`，旧运行由加载旧图的进程完成。进程内的 `Workflow::submit` 通过共享的 `TaskStore::enqueue` 提交，成功后立即唤醒本地调度器。Server 直接调用 `Database::submit`，两条路径复用相同的数据库写入。

## 关键机制

完整调度流程见 [Scheduler 设计文档](docs/scheduler-design.md)，包含主循环与单任务流程图、
执行槽分配、自动续租与原子结算，以及实例取消和恢复的责任边界。

- **节点不携带名字**：节点名作为 `Graph` 的 `HashMap` key 在注册实例时外部绑定，同一节点
  实现可复用、可在不同名字下注册。
- **编译期类型接线**：DSL 边经类型检查版构造函数装配，由 trait solver 强制「转换函数入参 ==
  上游节点 `Out`、出参 == 下游节点 `In`」，类型错线在编译期即报错，而非运行时 serde 错误。
- **at-least-once 结算**：成功/失败结算与后继入队在同一事务原子提交；事务失败整体回滚并
  释放租约，任务下一轮被重认领（重执行一次的代价即引擎既有的 at-least-once 语义），杜绝
  「已成功但后继从未入队」的静默停摆。
  无失败后继时，源任务失败与已有直接依赖任务的失败级联也在同一事务内完成。
- **自动续租**：每次认领递增 `attempt`，
  并按数据库时间设置 300 秒的 `lease_until`。上下文准备、业务执行和结算期间每 100 秒
  续租一次，续租不增加 `attempt`。租约确认失效，或到期前未能确认续租成功时，取消本地
  future 并释放执行槽，未结算任务允许重新认领。续租、结算和释放均校验当前状态、
  attempt 和数据库时间下的租约有效性，过期租约不能复活，旧执行者不能提交结果。
  引擎不限制节点运行时长；业务超时由节点自行控制。终态提交后停止续租，回调仍占执行槽，
  由节点自行控制回调时长，并随 Workflow 释放而取消。
- **统一最终失败**：`attempt` 是认领代次，引擎统一限制业务执行次数。
  `attempt > MAX_ATTEMPTS` 的认领只进行最终失败处理，不调用 `on_start` 和 `execute`。
  最后一次租约过期时记录租约失效原因，提前释放记录预算耗尽原因；恢复认领保留原因。
  调度器同步规划失败后继，再原子提交 `superseded` 与失败后继，或提交 `failed` 与依赖失败传播。
  失败提交后才调用 `on_error`；正常业务失败沿用执行上下文，恢复处理此时才 `fork` 回调上下文，
  避免上下文准备或回调卡住最终结算。恢复过程在提交前崩溃或结算失败后可重新认领继续处理，
  不依赖原 worker 返回，也不会再次执行业务节点。
- **并发汇合**。PostgreSQL 对含 JOIN 的图先取得该 run 的事务级 advisory lock，再锁定源任务行，
  以便并发结算时只创建一条汇合记录。无 JOIN 的图只锁定源任务行，同一 run 的独立任务可以并发结算。
  SQLite 使用已有的写事务锁。
  数据库认领保护不提供外部副作用的 exactly-once 保证，节点调用外部系统仍需幂等。
- **并发闸门 = 在途任务计数**（`JoinSet` 跟踪，`len <= max_concurrency`）：认领后立即
  spawn 结算并回到认领循环，快任务的后续立即被捡走，慢的 AI 任务仍在后台并发执行。
- **JOIN 不是语法糖**：多源 AND 汇合是业务节点，在 `nodes` 块显式注册 `JoinNode`，再经
  普通边把各源汇入；JOIN 输出类型为 `Value`，接下游的转换函数在内部反序列化为强类型视图。

## 数据库配置

按连接 URL 启用后端 feature：PostgreSQL 使用 `pg` 和 `postgres://` / `postgresql://`，
SQLite 使用 `sqlite` 和 `sqlite:`。默认启用 SQLite，至少需要一个后端；两个后端都关闭时编译报错。
对应 feature 未启用或协议不支持时，`Engine::new` 返回错误。
连接与初始化错误通过 `EngineError::Storage` 传播，具体错误类型为 `engine::StorageError`，保留底层错误来源。

```toml
# 默认 SQLite
workflow-engine = { path = "../workflow-engine" }
```

`Engine::new("sqlite://workflow.db").await` 自动连接并初始化引擎表。
SQLite 可写文件连接默认启用 WAL 和 5 秒锁等待，未指定 `mode` 时允许创建文件；显式的
`mode` 和 `immutable` 参数保持原意。内存及临时数据库使用单连接且不按空闲时间或存活时间回收。
PostgreSQL 使用最多 10 个连接和 5 秒连接池获取超时。

引擎使用 `tasks` 和 `scopes` 两张表，各自使用数据库生成的整数主键。编号在各自表内唯一，允许跳号。建表 SQL 分别位于
[`migrations/pgsql/0001_workflow_tasks.sql`](migrations/pgsql/0001_workflow_tasks.sql) 和
[`migrations/sqlite/0001_workflow_tasks.sql`](migrations/sqlite/0001_workflow_tasks.sql)，由 Engine 幂等执行。
任务认领、续租、失效认领恢复、后继入队及 JOIN 汇合由内部调度和存储实现负责。

## 使用

本地作为相邻项目的依赖时，在应用的 `Cargo.toml` 中启用：

```toml
workflow-engine = { path = "../workflow-engine", default-features = false, features = ["pg"] }
```

需要同时支持两个后端时，保留默认 feature 并添加 `features = ["pg"]`。

构建一张图有两种方式（见 [`graph`](src/graph) 和 [`dsl`](src/dsl.rs)）：

1. 命令式 `Graph::<Ctx>::builder(workflow_id, title, ctx).node(..).edge(..)…build()`；
2. 声明式 `dsl::workflow!(workflow_id: id; title: title; context: ctx; nodes { … } edges { … })`。

图直接持有工作流身份、标题、上下文与构造完成的节点。
`workflow_id` 与 `title` 必填且不能仅含空白；构图后通过 `graph.workflow_id()`、
`graph.title()` 只读访问。`workflow_id` 是任务关联的稳定身份，标题只用于展示。`.node(name, instance)` 对应 DSL 的
`name => instance`；节点与上下文类型由实例推导，unit struct 可直接写 `name => NodeType`。
节点实例跨任务复用，单次执行的可变状态保存在局部变量或 `NodeContext::fork` 产生的任务上下文中。
正常执行顺序为 `on_start → execute → 原子结算 → on_success/on_error`；
成功与错误回调都在提交后调用，不保证必达，也不构成后继任务的依赖屏障。
回调失败或随 Workflow 释放被取消不会回滚已提交状态与后继。引擎不为节点或回调设置运行超时。
`Graph::execute` 单独调用时不触发事件钩子。

`graph.describe()` 从同一个执行图导出可序列化的 `GraphDescription { workflow_id, title, nodes, edges }`，
适合供应用注册表和前端画布读取。描述不执行节点、条件或转换闭包，也不包含上下文和任务载荷。
节点按注册名排序，`id` 与任务的 `name` 对应；`task_type` 为 `normal`、`join` 或 `event`，
`trigger` 为 Event 节点的外部注册声明，其他节点为 `null`。
`input_type` / `output_type` 是 Rust 类型名，不是字段级 JSON Schema。

节点默认以注册名为标题、说明为空。DSL 可在节点实例前声明展示信息：

```rust,ignore
let graph = dsl::workflow! {
    workflow_id: "stock_indicators";
    title: "股票指标";
    context: context;
    nodes {
        #[title = "计算指标"]
        #[description = "根据行情计算技术指标"]
        calculate => node,
    }
    edges {}
}?;
let description = graph.describe();
```

节点的 `title` 和 `description` 都可省略，顺序不限，每个属性最多出现一次；未知属性会在编译期报错。
属性值接受可转换为 `String` 的表达式，每个表达式仅求值一次。展示信息与节点实例一起注册，
节点 ID、执行行为和边的编译期类型检查保持一致。

命令式构图也可在注册实例时指定相同信息：

```rust,ignore
let graph = Graph::builder("stock_indicators", "股票指标", context)
    .node_with_description("calculate", node, "计算指标", "根据行情计算技术指标")
    .build()?;
let description = graph.describe();
```

节点可通过 `.node(...)`、`.node_with_key(...)` 或 DSL 注册；重复节点名在 `build()` 时返回错误。
成功边和失败边分别以 `kind: "success"` / `"failure"` 标识，平行边独立保留。
每条边通过 `mode` 枚举标记路由模式：`passthrough`（透传，含带条件的透传边）、`map`（显式转换）或 `foreach`（逐项进入独立子图作用域，仅成功边）。
边 ID 为 `success:0`、`failure:0` 等当前图内编号；注册顺序变化后不保证跨版本稳定。
工作流身份和标题只在图中定义；应用注册表直接索引完整的图描述，不重复指定身份和名称。
运行状态仍由任务存储管理，无需另外维护一份画布拓扑。

图是固定的拓扑定义，易变依赖可在 `NodeContext::fork` 中按任务刷新。
有空闲执行槽时，即使其他节点仍在执行，当前 Workflow 的新提交也会立即触发认领。

## 外部事件入口

Event 节点仍实现 `Node` 并使用相同的 nodes/edges 注册方式。通过
`TASK_TYPE = TaskType::Event` 声明入口角色，`trigger()` 返回 `Trigger { kind, config }`。
`kind` 和 JSON 配置的含义由宿主定义；引擎不解析 cron、不注册 HTTP 路由或订阅消息。

```rust
use serde_json::{Value, json};
use workflow_engine::{Node, NodeError, Trigger, graph::TaskType};

struct CronEntry {
    expression: String,
    timezone: String,
}

impl<Ctx: Send> Node<Ctx> for CronEntry {
    type In = Value;
    type Out = Value;
    const TASK_TYPE: TaskType = TaskType::Event;

    fn trigger(&self) -> Option<Trigger> {
        Some(Trigger {
            kind: "cron".into(),
            config: json!({
                "expression": self.expression,
                "timezone": self.timezone,
            }),
        })
    }

    async fn execute(&self, _ctx: &mut Ctx, input: Value) -> Result<Value, NodeError> {
        Ok(input)
    }
}
```

`workflow.triggers()` 返回按节点名排序的 `Vec<(&str, Trigger)>`，构图后也可通过
`graph.triggers()` 读取相同声明。宿主遍历 `(node_id, trigger)` 并完成注册；事件到达后
调用 `workflow.submit(node_id, payload).await?`，输入需要符合该节点的 `In`。
每次提交创建独立运行，入口节点类型取自绑定图，后继沿正常调度与结算路径执行。
普通节点仍可通过 `submit` 手动启动。

`trigger()` 只读取固定配置，必须无副作用；构图、描述和查询都不会建立监听。
Event 节点必须返回声明，其他节点必须返回 `None`，成功边和失败边都不能指向 Event 节点。
这些规则在 `build()` 时校验；具体触发配置的有效性由宿主注册器校验。
触发器注册、注销、消息确认和事件去重由宿主管理，`submit` 不提供外部事件去重。
多个入口的事件各自启动运行，不能用多次提交补齐同一次运行的 JOIN。

## 开发

测试代码统一放在 `tests/`：顶层 `.rs` 文件是集成测试，`unit/` 存放单元测试与
测试辅助代码，`doctests/` 存放编译失败示例。单元测试通过 `#[cfg(test)]` + `#[path]`
在所属模块中加载，保留私有成员访问；文档测试通过 `include_str!` 嵌入 API 文档。

```bash
# 类型检查 / 测试
cargo check
cargo check --features sqlite
cargo test --all-features
# Engine / Workflow 公开 API 集成测试
cargo test --features sqlite --test sqlite_backend
```

PostgreSQL 测试默认通过 [Cargo 配置](.cargo/config.toml) 连接本地专用库
`postgresql://workflow_engine_test@127.0.0.1:5432/workflow_engine_test`。
首次配置本地 PostgreSQL 时，以有权限的用户创建测试角色和数据库：

```bash
createuser -h 127.0.0.1 -p 5432 --no-superuser --no-createdb --no-createrole workflow_engine_test
createdb -h 127.0.0.1 -p 5432 --owner=workflow_engine_test workflow_engine_test
cargo test --all-features --lib backend::pgsql::
```

每项测试在专用库中创建独立 schema。CI 或其他环境通过显式设置
`WORKFLOW_ENGINE_TEST_DATABASE_URL` 覆盖连接地址；本地地址沿用 PostgreSQL 的回环连接认证配置。
SQLite 测试使用内存数据库与临时文件，无需外部服务，覆盖并发认领、持久化重开和原子回滚。
[`tests/sqlite_backend.rs`](tests/sqlite_backend.rs) 通过公开 API 验证不同上下文工作流的自动执行与并发提交共享执行槽。
[`tests/unit/sqlite_scheduler.rs`](tests/unit/sqlite_scheduler.rs) 使用 `dsl::workflow!` 构建
扇出 → 多源 JOIN → 汇总图，在库内部验证任务依赖、输入输出与最终落库状态。
