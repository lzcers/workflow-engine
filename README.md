# workflow-engine

独立的**通用工作流引擎**：强类型节点、图引擎、通用调度器和 PostgreSQL / SQLite 后端。
引擎不包含业务字段，适合由应用自行定义节点与任务上下文。

## 定位

- 公共 API 围绕 `graph`、`engine`、`workflow`、`task` 和 `dsl` 组织；节点、上下文与建边接口属于图定义。
- 通过 `Engine` 使用数据库，默认启用 SQLite；`pg` / `sqlite` feature 只选择后端，至少启用一个，也可同时启用。
- 通过 `Engine::new(url).await` 创建可复用引擎，再用 `engine.make(graph)` 创建 `Workflow<Ctx>` 实例。
- `Workflow::submit` 创建独立运行并自动启动后台调度；数据库连接、任务存储、后端仓储和调度器由库内部管理。

## 公共模块

| 模块 | 职责 |
| --- | --- |
| [`graph`](src/graph) | `Graph`、构图接口、节点 `Node`、上下文 `NodeContext`、`JoinNode`、成功/失败边与图描述 |
| [`engine`](src/engine.rs) | `Engine` 按 URL 初始化共享存储，通过 `make` 为不同的图创建工作流实例；`EngineError` 描述初始化与提交错误 |
| [`workflow`](src/workflow.rs) | 单图 `Workflow<Ctx>` 提交独立运行并自动执行，返回 `RunHandle` |
| [`task`](src/task) | 任务输入 `Task`、任务快照 `TaskRecord` 与状态 `TaskStatus` |
| [`dsl`](src/dsl.rs) | `workflow!` 宏将 `nodes { … }` 和 `edges { … }` 展开为图构建链，并做编译期类型接线校验 |

常用类型可直接从 crate 根导入，例如 `use workflow_engine::{Engine, Graph, Node, NodeContext};`。
图描述、节点标识与命令式建边接口通过 `workflow_engine::graph` 访问。

## 公共任务模型

PostgreSQL 和 SQLite 共用 `TaskRecord`、`Task`、`TaskStatus`。业务上下文实现
`NodeContext`，`fork` 接收 `&TaskRecord`，不依赖数据库任务类型。`TaskStatus` 使用枚举，序列化为
`pending`、`running`、`succeeded`、`failed`、`skipped`、`superseded`；数据库中的未知状态
会返回解码错误。任务模型由两个后端共享。
`Task` 表示待入队内容，`TaskRecord` 表示带身份、状态和结果的任务快照。
业务侧通过 `Workflow::submit` 提交运行，在节点上下文和生命周期回调中读取当前任务。

## 引擎与工作流实例

`Engine::new(url).await` 连接数据库、幂等创建引擎表并持有共享任务存储。
`engine.make(graph)` 根据图定义创建 `Workflow<Ctx>`，首次成功提交时启动后台执行。
同一 Engine 可以创建不同上下文类型的工作流；它们共享连接池，各自拥有独立的调度器。
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
    // 创建 run_id 并启动后台执行，返回时不等待运行结束。
    let run = workflow.submit("entry", input).await?;
    // 当前为空方法，不返回实际运行状态。
    run.status().await;

    // 执行期间保持 workflow 存活。
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

`submit(entry, input)` 检查节点名并序列化输入，在同一事务内分配 `run_id` 和保存入口任务。
入口由每次提交指定，输入需要符合该节点的 `In` 类型。每次提交都是独立运行：
`workflow_id + run_id` 确定运行身份，后继继承 `run_id`，不同运行的扇入与 JOIN 相互隔离。
选定入口需要能触达后续 JOIN 所需的前驱；多次 submit 不能补齐同一次运行的多个入口。
RunHandle 保存 `workflow_id`、`run_id` 和入口任务的 `task_id`，`status()` 暂为空实现。
首次成功提交自动启动当前实例的后台调度器，后续提交立即唤醒认领，返回时不等待运行
结束。调度器执行当前图下所有已提交及后续提交的运行，空闲时持续等待。每个 Workflow
的所有运行共享 8 个执行槽和 1 秒轮询周期，并发提交也只启动一个调度器。

执行期间需要保持 Workflow 存活。丢弃 Workflow 会取消后台调度器和在途节点，不能等待
结算完成；已持久化的任务仍保留在数据库。Engine 释放后 Workflow 仍可使用。

上述示例使用默认 SQLite 后端，PostgreSQL 使用 `pg` feature 和对应连接 URL。

## 关键机制

完整调度流程见 [Scheduler 设计文档](docs/scheduler-design.md)，包含主循环与单任务流程图、
执行槽分配、续租与原子结算，以及实例取消和恢复的责任边界。

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

两个后端均使用引擎自有的 `tasks` / `task_counters` 表，建表 SQL 分别位于
[`migrations/pgsql/0001_workflow_tasks.sql`](migrations/pgsql/0001_workflow_tasks.sql) 和
[`migrations/sqlite/0001_workflow_tasks.sql`](migrations/sqlite/0001_workflow_tasks.sql)，由 Engine 幂等执行。
任务认领、租约续期、重试、后继入队及 JOIN 汇合由内部调度和存储实现负责。

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

图是固定的拓扑定义，易变依赖可在 `NodeContext::fork` 中按任务刷新。
有空闲执行槽时，即使其他节点仍在执行，当前 Workflow 的新提交也会立即触发认领。

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

PostgreSQL 集成测试需要 `WORKFLOW_ENGINE_TEST_DATABASE_URL`（或 `DATABASE_URL`）；
未设置时，这些测试会跳过数据库连接。
SQLite 测试使用内存数据库与临时文件，无需外部服务，覆盖并发认领、持久化重开和原子回滚。
[`tests/sqlite_backend.rs`](tests/sqlite_backend.rs) 通过公开 API 验证不同上下文工作流的自动执行与并发提交共享执行槽。
[`tests/unit/sqlite_scheduler.rs`](tests/unit/sqlite_scheduler.rs) 使用 `dsl::workflow!` 构建
扇出 → 多源 JOIN → 汇总图，在库内部验证任务依赖、输入输出与最终落库状态。
