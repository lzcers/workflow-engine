# workflow-engine

独立的**通用工作流引擎**：强类型节点、图引擎、通用调度器和可选 PostgreSQL / SQLite 后端。
引擎不包含业务字段，适合由应用自行定义节点与任务上下文。

## 定位

- 引擎核心（`node` / `graph` / `edge` / `task` / `scheduler`）**零数据库依赖**；
- PostgreSQL 通用实现由 `pg` feature 门控（`workflow-engine = { features = ["pg"] }`）；
- SQLite 通用实现由 `sqlite` feature 门控；两个后端可独立启用，也可同时启用；
- 调度器只依赖四个抽象：任务快照 `Task`、任务存储 `TaskStore`、图引擎 `Graph`、
  节点上下文 `NodeContext`。不解读任务语义，只驱动 `claim → settle` 循环并把图规划结果
  交给存储落库。
- worker 生命周期、任务就绪监听、恢复扫描、配置刷新与重启编排由宿主应用负责；
  引擎提供 `Scheduler`、任务就绪通知与恢复查询、`release_claims` 等存储原语。

## 核心模块

| 模块 | 职责 |
| --- | --- |
| [`node`](src/node) | 强类型 [`Node`](src/node/mod.rs) trait、节点上下文 [`NodeContext`](src/node/mod.rs)、类型擦除 `AnyNode`、`JoinNode`（多源 AND 汇合） |
| [`edge`](src/edge) | 边模型：成功/失败边、触发时机（`when` / `when_error`）、守卫、扇出；类型检查版构造函数 `map_checked` / `when_checked` / `on_error_checked` / `passthrough_checked` |
| [`graph`](src/graph) | [`Graph`](src/graph/mod.rs) 图引擎：节点注册、边规划（`plan_success` / `plan_failure`）、执行（`execute`）、静态校验、图描述（`describe`） |
| [`dsl`](src/dsl.rs) | 声明式图 DSL：`workflow!` 宏把 `nodes { … }` + `edges { … }` 展开为 Graph 构建链，并做**编译期**类型接线校验 |
| [`task`](src/task.rs) | `Task` / `TaskStore` 抽象，把调度与具体持久化解耦 |
| [`scheduler`](src/scheduler.rs) | 通用单会话调度器 [`Scheduler`](src/scheduler.rs)：批量认领无依赖任务 → 多协程并发结算 |
| [`pgsql_backend`](src/pgsql_backend)（`pg`） | PostgreSQL 通用实现：`PgTaskRepo` / `PgTaskStore` / 任务就绪通知与恢复查询 |
| [`sqlite_backend`](src/sqlite_backend)（`sqlite`） | SQLite 通用实现：`SqliteTaskRepo` / `SqliteTaskStore` / 就绪工作流查询与租约恢复 |

## 关键机制

- **节点不携带名字**：节点名作为 `Graph` 的 `HashMap` key 在注册实例时外部绑定，同一节点
  实现可复用、可在不同名字下注册。
- **编译期类型接线**：DSL 边经类型检查版构造函数装配，由 trait solver 强制「转换函数入参 ==
  上游节点 `Out`、出参 == 下游节点 `In`」，类型错线在编译期即报错，而非运行时 serde 错误。
- **at-least-once 结算**：成功/失败结算与后继入队在同一事务原子提交；事务失败整体回滚并
  释放租约，任务下一轮被重认领（重执行一次的代价即引擎既有的 at-least-once 语义），杜绝
  「已成功但后继从未入队」的静默停摆。
  无失败后继时，源任务失败与已有直接依赖任务的失败级联也在同一事务内完成。
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

引擎表**不持有指向业务表的 FK**；消费方在自己的业务表之外执行 `pgsql_backend::migrate`
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
use std::time::Duration;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use workflow_engine::sqlite_backend::{migrate, NewTask, SqliteTaskRepo, SqliteTaskStore};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let options = SqliteConnectOptions::new()
    .filename("workflow.db")
    .create_if_missing(true)
    .journal_mode(SqliteJournalMode::Wal)
    .busy_timeout(Duration::from_secs(5));
let db = SqlitePoolOptions::new()
    .max_connections(4)
    .connect_with(options)
    .await?;
migrate(&db).await?;

let repo = SqliteTaskRepo::new(db);
repo.enqueue(&NewTask {
    workflow_id: "demo".into(),
    run_id: None,
    name: "start".into(),
    instance_id: None,
    max_attempts: 3,
    depends_on_ids: vec![],
    depends_on_names: vec![],
    input: json!({"message": "hello"}),
}).await?;
let store = SqliteTaskStore::new(repo, "demo".into());
// 将 store 和 Arc<Graph<Ctx>> 交给 Scheduler::new；Ctx 实现 NodeContext<SqliteTask>。
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
`repo.executable_workflow_ids()`，为每个返回的工作流构造 `SqliteTaskStore` 并启动或唤醒
对应的 `Scheduler`。这样可以发现其他连接或进程的提交，并恢复租约到期的任务。
SQLite 后端不提供 worker 池、扫描循环或跨进程重启通知。主动重建由宿主协调：先阻止
该工作流的其他启动入口，停止旧 worker 并等待其清理完成，再调用
`repo.release_claims(workflow_id)` 释放租约，随后重建并恢复启动入口。

## 使用

本地作为相邻项目的依赖时，在应用的 `Cargo.toml` 中启用：

```toml
workflow-engine = { path = "../workflow-engine", features = ["pg"] }
```

构建一张图有两种方式（见 [`graph`](src/graph/tests.rs) 和 [`dsl`](src/dsl.rs) 中的测试）：

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

`Scheduler::new(graph, store, poll, idle_timeout)` 直接接收 `Arc<Graph<Ctx>>`：图是固定的拓扑
真相源，会话配置等易变依赖由上下文在 `NodeContext::fork`（异步）里按任务刷新——「配置变更对
下一个认领的任务生效」。刷新失败由业务侧降级为沿用上一份已生效配置，不阻塞认领。

`idle_timeout` 为 `None` 时，Scheduler 常驻等待任务，空闲或数据库查询失败不会让它退出；
`Some(duration)` 用于需要空闲回收的动态 worker，空闲截止时间独立于轮询间隔，退出前再确认
没有可认领任务。有空闲并发槽时，即使其他节点仍在执行，就绪通知也会立即触发认领。
停止常驻调度器时，调用
`run_until(shutdown)` 并通过 `shutdown` future 发送停止信号；方法会取消并等待全部在途
节点任务退出后返回。调度循环 panic 时也会先完成这一步清理，再传播 panic，供上层池观察。
调用方需等待 `run_until` 完成，直接 abort 或丢弃其 future 无法等待清理结束。
`run()` 用于依靠空闲时限退出，或由运行时终止的调度器。

## 开发

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
