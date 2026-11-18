# workflow-engine

独立的**通用工作流引擎**：强类型节点、图引擎、通用调度器和可选 PostgreSQL / SQLite 后端。
引擎不包含业务字段，适合由应用自行定义节点与任务上下文。

## 定位

- 图与调度核心（`node` / `graph` / `edge` / `task` / `scheduler`）**零数据库依赖**；
- 上层通过 `Engine::new(url).await` 创建可复用引擎，再用 `engine.make(graph)`
  创建 `Workflow<Ctx>` 实例，无需构造仓储或实现 `TaskStore`；二者随 `pg` / `sqlite` feature 启用；
- PostgreSQL 通用实现由 `pg` feature 门控（`workflow-engine = { features = ["pg"] }`）；
- SQLite 通用实现由 `sqlite` feature 门控；两个后端可独立启用，也可同时启用；
- 调度器使用统一的任务快照 `TaskRecord`，依赖任务存储 `TaskStore`、图引擎 `Graph`、
  节点上下文 `NodeContext`。不解读任务语义，只驱动 `claim → settle` 循环并把图规划结果
  交给存储落库。
- worker 生命周期、任务就绪监听、恢复扫描、配置刷新与重启编排由宿主应用负责；
  引擎提供 `Scheduler`、任务就绪通知与恢复查询、`release_claims` 等存储原语。

## 公共任务模型与连接入口

PostgreSQL 和 SQLite 共用 `TaskRecord`、`Task`、`TaskStatus`。业务上下文实现
`NodeContext`，`fork` 接收 `&TaskRecord`，不依赖数据库任务类型。`TaskStatus` 使用枚举，序列化为
`pending`、`running`、`succeeded`、`failed`、`skipped`、`superseded`；数据库中的未知状态
会返回解码错误。公共模型在未启用数据库 feature 时也可使用。
`Task` 表示待入队内容，`TaskRecord` 表示带身份、状态和结果的任务快照；`TaskStore` 统一使用 `TaskRecord`。

启用 `pg` 或 `sqlite` 后，`Engine::new(url).await` 内部完成数据库连接、引擎表初始化与存储创建。
需要单独操作数据库时，可使用底层 `Database` 连接入口：

```rust,no_run
use workflow_engine::Database;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let database = Database::connect(&std::env::var("DATABASE_URL")?).await?;
database.initialize().await?;
database.check().await?;
// 在此使用连接；构造和初始化不会启动 Scheduler。
database.close().await;
# Ok(())
# }
```

`connect` 根据 `postgres://` / `postgresql://` / `sqlite:` 选择后端；对应 feature 未启用
或协议不支持时明确报错。`initialize` 复用后端现有的幂等建表操作，`check` 只检查连接，
`close` 关闭共享连接池，所有克隆句柄都会受到影响。连接、初始化、仓储和任务存储统一返回
`StorageError`，保留底层错误来源。

`Database` 内部按 URL 已确定的后端创建对应 `TaskStore`，复用现有连接池。
存储可供多个工作流共享，认领时显式指定工作流身份，结算使用任务的身份。
此工厂仅供库内部调用，上层使用 `Engine` 时无需选择或构造存储。

SQLite 可写文件连接默认启用 WAL 和 5 秒锁等待，未指定 `mode` 时允许创建文件；显式的
`mode` 和 `immutable` 参数保持原意。内存及临时数据库使用单连接且不按空闲时间或存活时间回收。
PostgreSQL 使用最多 10 个连接和 5 秒连接池获取超时。
数据库原生连接池可通过 `Database::Postgres(pool)` / `Database::Sqlite(pool)` 接入后端仓储
和业务自管事务；运行提交、观测查询及 worker 管理仍使用各自现有接口。

## 核心模块

| 模块 | 职责 |
| --- | --- |
| [`node`](src/node) | 强类型 [`Node`](src/node/mod.rs) trait、节点上下文 [`NodeContext`](src/node/mod.rs)、类型擦除 `AnyNode`、`JoinNode`（多源 AND 汇合） |
| [`edge`](src/edge) | 边模型：成功/失败边、触发时机（`when` / `when_error`）、守卫、扇出；类型检查版构造函数 `map_checked` / `when_checked` / `on_error_checked` / `passthrough_checked` |
| [`graph`](src/graph) | [`Graph`](src/graph/mod.rs) 图引擎：节点注册、边规划（`plan_success` / `plan_failure`）、执行（`execute`）、静态校验、图描述（`describe`） |
| [`dsl`](src/dsl.rs) | 声明式图 DSL：`workflow!` 宏把 `nodes { … }` + `edges { … }` 展开为 Graph 构建链，并做**编译期**类型接线校验 |
| [`task`](src/task) | `record` 定义公共任务记录和输入，`store` 定义认领、结算与后继入队契约 |
| [`database`](src/database.rs) | 统一连接、初始化、健康检查与关闭入口，内部按后端创建任务存储 |
| [`scheduler`](src/scheduler.rs) | 通用单会话调度器 [`Scheduler`](src/scheduler.rs)：批量认领无依赖任务 → 多协程并发结算 |
| [`engine`](src/engine.rs)（`pg` / `sqlite`） | 可复用的 `Engine`：按 URL 初始化共享存储，通过 `make` 为不同的图创建工作流实例 |
| [`workflow`](src/workflow.rs)（`pg` / `sqlite`） | 单图 `Workflow<Ctx>`：持有调度器，提交独立运行，通过 `run` / `stop` 控制当前图所有运行的执行 |
| [`backend::pgsql`](src/backend/pgsql)（`pg`） | PostgreSQL 通用实现：`PgTaskRepo` / `PgTaskStore` / 任务就绪通知与恢复查询 |
| [`backend::sqlite`](src/backend/sqlite)（`sqlite`） | SQLite 通用实现：`SqliteTaskRepo` / `SqliteTaskStore` / 就绪工作流查询与租约恢复 |

## 引擎与工作流实例

`Engine::new(url).await` 连接数据库、幂等创建引擎表并持有共享任务存储。
`engine.make(graph)` 根据图定义创建 `Workflow<Ctx>`，内部已实例化该图的 Scheduler。
同一 Engine 可以创建不同上下文类型的工作流；它们共享连接池，各自控制调度生命周期。
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
    let run = workflow.submit("entry", input).await?;
    // 当前为空方法，不返回实际运行状态。
    run.status().await;

    // 启动后台调度器后立即返回；执行此图下所有 run_id 的任务。
    workflow.run().await?;
    tokio::signal::ctrl_c().await?;
    workflow.stop().await?;
    Ok(())
}
```

`submit(entry, input)` 检查节点名并序列化输入，在同一事务内分配 `run_id` 和保存入口任务。
入口由每次提交指定，输入需要符合该节点的 `In` 类型。每次提交都是独立运行：
`workflow_id + run_id` 确定运行身份，后继继承 `run_id`，不同运行的扇入与 JOIN 相互隔离。
选定入口需要能触达后续 JOIN 所需的前驱；多次 submit 不能补齐同一次运行的多个入口。
RunHandle 保存 `workflow_id`、`run_id` 和入口任务的 `task_id`，`status()` 暂为空实现。
成功提交仅表示任务已持久化，尚未启动的工作流不会自行执行。

`workflow.run().await` 启动当前实例的后台 Scheduler 并立即返回，不等待运行结束。
它执行当前图下所有已提交及后续提交的运行，空闲时持续等待。每个 Workflow 默认共享
8 个执行槽和 1 秒轮询周期，本进程提交会立即唤醒认领。同一实例重复启动返回
`EngineError::AlreadyRunning`；不同实例可以独立启停。

`workflow.stop().await` 停止认领并等待在途节点完成结算，之后可以再次 `run()`。
已提交但未执行的任务、停止过程中产生的后继任务仍保留在数据库。停止不会取消正在执行的
业务节点，因此长时间不返回的节点也会使停止持续等待。直接丢弃 Workflow 会取消后台任务，
不能等待结算完成；需要完整停止时先等待 `stop()`。Engine 释放后 Workflow 仍可使用。

`TaskStore` 保留为底层 Scheduler 的扩展契约；异步方法返回 `BoxFuture`，可作为 trait object。
`Database` 按 URL 选择后端并创建共享存储，使用 Engine 无需接触 TaskStore。
上述示例启用 `sqlite` feature，PostgreSQL 使用 `pg` feature 和对应连接 URL。

## 关键机制

- **节点不携带名字**：节点名作为 `Graph` 的 `HashMap` key 在注册实例时外部绑定，同一节点
  实现可复用、可在不同名字下注册。
- **编译期类型接线**：DSL 边经类型检查版构造函数装配，由 trait solver 强制「转换函数入参 ==
  上游节点 `Out`、出参 == 下游节点 `In`」，类型错线在编译期即报错，而非运行时 serde 错误。
- **at-least-once 结算**：成功/失败结算与后继入队在同一事务原子提交；事务失败整体回滚并
  释放租约，任务下一轮被重认领（重执行一次的代价即引擎既有的 at-least-once 语义），杜绝
  「已成功但后继从未入队」的静默停摆。
  无失败后继时，源任务失败与已有直接依赖任务的失败级联也在同一事务内完成。
- **认领归属与续租**：每次启动 Scheduler 都分配唯一 worker ID，认领时递增 `attempt`。
  节点执行前先验证并续租，执行期间每 100 秒续一次 300 秒租约；续租失败或超过 100 秒
  未响应时停止该节点 future。成功、失败、释放及续租均校验当前状态、worker、attempt
  和数据库时间下的租约有效性，失效认领返回 `StorageError::ClaimLost`，不能修改新认领的任务。
  续租覆盖上下文准备、节点执行与失败回调；成功提交后的回调不再续租。
- **并发汇合**：PostgreSQL 在同一次 run 的结算和后继入队事务前取得 advisory transaction lock，
  获锁后再查询、创建或合并 JOIN，避免并发首次创建重复记录。锁仅覆盖数据库结算阶段，
  节点业务执行仍可并行；不同 run 使用不同的运行锁。SQLite 使用已有的写事务锁。
  数据库认领保护不提供外部副作用的 exactly-once 保证，节点调用外部系统仍需幂等。
- **并发闸门 = 在途任务计数**（`JoinSet` 跟踪，`len <= max_concurrency`）：认领后立即
  spawn 结算并回到认领循环，快任务的后续立即被捡走，慢的 AI 任务仍在后台并发执行。
- **JOIN 不是语法糖**：多源 AND 汇合是业务节点，在 `nodes` 块显式注册 `JoinNode`，再经
  普通边把各源汇入；JOIN 输出类型为 `Value`，接下游的转换函数在内部反序列化为强类型视图。

## 数据库（`pg` feature）

引擎自有 schema（迁移见 [`migrations/pgsql/0001_workflow_tasks.sql`](migrations/pgsql/0001_workflow_tasks.sql)，
幂等 DDL，启动时执行）：

- `tasks`：任务行（`workflow_id` / `run_id` / `task_id` / `status` / `claimed_by` /
  `lease_until` / `attempt` / `depends_on_ids` / `input_json` / `output_json` …），承载认领、
  扇出、原子结算、级联、JOIN 闸门所需的全部字段。
- `task_counters`：工作流级任务 id 自增计数表（引擎自有的 id 分配源，与任务插入同事务）。

引擎表**不持有指向业务表的 FK**；消费方在自己的业务表之外执行 `backend::pgsql::migrate`
建立引擎表，再以 `ALTER TABLE` 补充业务完整性约束（如 FK 到自己的业务表）。

任务就绪通知在事务提交后投递到 `TASK_READY_CHANNEL`，payload 为 `workflow_id`；
业务自管入队事务可调用 `notify_tasks_ready`。宿主应用负责 LISTEN/NOTIFY 连接，
收到通知后启动或唤醒对应的 `Scheduler`。启动或监听重连后的恢复流程由宿主调用
`repo.seal_budget_exhausted()`，再通过 `repo.executable_workflow_ids()` 找出待启动的工作流。

## 数据库（`sqlite` feature）

SQLite 后端使用同样的 `tasks` / `task_counters` 模型，迁移独立保存在
[`migrations/sqlite/0001_workflow_tasks.sql`](migrations/sqlite/0001_workflow_tasks.sql)。
数组和 JSON 负载存为 JSON 文本，时间存为 UTC 文本。`migrate` 在事务内幂等建表。
支持任务认领、租约回收、重试预算、成功/失败与后继入队原子提交、单实例扇入、map 去重、
JOIN 前驱等齐和失败级联；`run_id = NULL` 是独立分组，不会与其他 run 合并。

```toml
workflow-engine = { path = "../workflow-engine", features = ["sqlite"] }
sqlx = { version = "0.8.6", default-features = false, features = ["runtime-tokio", "sqlite"] }
```

```rust,no_run
use serde_json::json;
use workflow_engine::{Database, Task};
use workflow_engine::backend::sqlite::{SqliteTaskRepo, SqliteTaskStore};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let database = Database::connect("sqlite://workflow.db").await?;
database.initialize().await?;
let db = match &database {
    Database::Sqlite(pool) => pool.clone(),
    #[cfg(feature = "pg")]
    Database::Postgres(_) => unreachable!("SQLite URL selects SQLite"),
};

let repo = SqliteTaskRepo::new(db);
repo.enqueue(&Task {
    workflow_id: "demo".into(),
    run_id: None,
    name: "start".into(),
    instance_id: None,
    max_attempts: 3,
    depends_on_ids: vec![],
    depends_on_names: vec![],
    input: json!({"message": "hello"}),
}).await?;
let store = SqliteTaskStore::new(repo);
// 将 Arc::new(store) 和 Arc<Graph<Ctx>> 交给 Scheduler::new；Ctx 实现 NodeContext。
# let _ = store;
# Ok(())
# }
```

写事务使用 `BEGIN IMMEDIATE`，同一数据库的多个 worker 会串行完成认领与结算；节点执行
仍可并发。文件数据库建议启用 WAL 与 `busy_timeout`，内存测试建议用单连接池
（`max_connections(1)` + `sqlite::memory:`）。业务自管事务调用 `SqliteTaskRepo::insert_task`
前若需要读取数据库，也应使用 `pool.begin_with("BEGIN IMMEDIATE")`，避免读事务升级锁冲突。

SQLite 使用轮询发现就绪任务。单个已启动的 `Scheduler` 自带轮询；跨工作流发现由宿主
在启动时和定时扫描中先调用 `repo.seal_budget_exhausted()`，再调用
`repo.executable_workflow_ids()`，为每个返回的工作流启动或唤醒对应的 `Scheduler`，
多个调度器可共享同一个 `SqliteTaskStore`。这样可以发现其他连接或进程的提交，并恢复租约到期的任务。
SQLite 后端不提供 worker 池、扫描循环或跨进程重启通知。主动重建由宿主协调：先阻止
该 worker 的其他启动入口，停止旧 worker 并等待其清理完成，再调用
`repo.release_claims(workflow_id, worker_id)` 仅释放它的租约，随后重建并恢复启动入口。

## 使用

本地作为相邻项目的依赖时，在应用的 `Cargo.toml` 中启用：

```toml
workflow-engine = { path = "../workflow-engine", features = ["pg"] }
```

构建一张图有两种方式（见 [`graph`](src/graph) 和 [`dsl`](src/dsl.rs)）：

1. 命令式 `Graph::<Ctx>::builder(workflow_id, title, ctx).node(..).edge(..)…build()`；
2. 声明式 `dsl::workflow!(workflow_id: id; title: title; context: ctx; nodes { … } edges { … })`。

图直接持有工作流身份、标题、上下文与构造完成的节点。
`workflow_id` 与 `title` 必填且不能仅含空白；构图后通过 `graph.workflow_id()`、
`graph.title()` 只读访问。`workflow_id` 是任务关联的稳定身份，标题只用于展示。`.node(name, instance)` 对应 DSL 的
`name => instance`；节点与上下文类型由实例推导，unit struct 可直接写 `name => NodeType`。
节点实例跨任务复用，单次执行的可变状态保存在局部变量或 `NodeContext::fork` 产生的任务上下文中。
调度顺序为 `on_start → execute → on_success/on_error`；`Graph::execute` 单独调用时不触发事件钩子。

`graph.describe()` 从同一个执行图导出可序列化的 `GraphDescription { workflow_id, title, nodes, edges }`，
适合供应用注册表和前端画布读取。描述不执行节点、条件或转换闭包，也不包含上下文和任务载荷。
节点按注册名排序，`id` 与任务的 `name` 对应；`task_type` 为 `normal` 或 `join`，
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
边 ID 为 `success:0`、`failure:0` 等当前图内编号；注册顺序变化后不保证跨版本稳定。
工作流身份和标题只在图中定义；应用注册表直接索引完整的图描述，不重复指定身份和名称。
运行状态仍由任务存储管理，无需另外维护一份画布拓扑。

直接使用调度器时，`Scheduler::new(graph, store, poll)` 接收 `Arc<Graph<Ctx>>`、
`Arc<Store>` 和轮询周期，支持具体存储与 `dyn TaskStore`。
`Scheduler::run(shutdown).await` 持续运行到停止信号完成，并等待已认领任务结算后返回；
上层 Workflow 将这个循环放在后台任务中，并通过 `run()` / `stop()` 管理其生命周期。
有空闲执行槽时，即使其他节点仍在执行，就绪通知也会立即触发认领。

图是固定的拓扑定义，易变依赖可在 `NodeContext::fork` 中按任务刷新。
直接中止 Scheduler future 会取消在途节点，无法等待清理；调度循环 panic 时则会取消并
等待在途任务退出后传播 panic，Workflow 的 `stop()` 会返回后台任务的错误。

## 开发

测试代码统一放在 `tests/`：顶层 `.rs` 文件是集成测试，`unit/` 存放单元测试与
测试辅助代码，`doctests/` 存放编译失败示例。单元测试通过 `#[cfg(test)]` + `#[path]`
在所属模块中加载，保留私有成员访问；文档测试通过 `include_str!` 嵌入 API 文档。

```bash
# 类型检查 / 测试
cargo check
cargo check --features sqlite
cargo test --all-features
# DSL 图 + SQLite 后端的端到端集成测试
cargo test --features sqlite --test sqlite_backend
```

PostgreSQL 集成测试需要 `WORKFLOW_ENGINE_TEST_DATABASE_URL`（或 `DATABASE_URL`）；
未设置时，这些测试会跳过数据库连接。
SQLite 测试使用内存数据库与临时文件，无需外部服务，覆盖并发认领、持久化重开和原子回滚。
[`tests/sqlite_backend.rs`](tests/sqlite_backend.rs) 使用 `dsl::workflow!` 构建
扇出 → 多源 JOIN → 汇总图，通过公开 API 验证任务依赖、输入输出与最终落库状态。
