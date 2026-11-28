# workflow-engine

独立的**通用工作流引擎**：强类型节点、对称流式边模型、槽位驱动 JOIN、可复用子图与 PostgreSQL / SQLite 双后端。
引擎不包含业务字段，适合由应用自行定义节点与任务上下文。

## 定位

- 公共 API 围绕 `graph`、`engine`、`workflow`、`task` 和 `dsl` 组织。
- 通过 `Engine` 使用数据库，默认启用 SQLite；`pg` / `sqlite` feature 至少启用一个，也可同时启用。
- `Engine::new(url).await`（或 `Engine::new_with(url, PoolOptions)`）初始化连接池与共享唤醒中枢（单轮询器覆盖多工作流，PostgreSQL 同时启用 `LISTEN/NOTIFY` 跨进程唤醒）。
- `engine.make(graph)`（或 `engine.make_with(graph, WorkerOptions)`）创建非泛型 `Workflow` 句柄并启动后台调度；支持 `submit`、`get_run`、`list_tasks`、`cancel_run`、`wait_run` 与 `shutdown`。

## 公共模块

| 模块 | 职责 |
| --- | --- |
| [`graph`](src/graph) | `Graph`、`GraphBuilder`、`SubgraphBuilder`、节点 `Node`/`NodeRole`/`RetryPolicy`、上下文 `NodeContext`、`JoinNode`、统一流式边 `Edge` 与图描述 |
| [`engine`](src/engine.rs) | `Engine` 初始化共享存储与共享唤醒中枢（`WakeHub` / PG `LISTEN/NOTIFY`），通过 `make` / `make_with` 创建工作流实例；`EngineError` / `GraphError` |
| [`workflow`](src/workflow.rs) | 单图 `Workflow` 入口预检提交、运行控制面（`get_run`/`list_tasks`/`cancel_run`/`wait_run`）、`shutdown()` 优雅停机与 `WorkerOptions` |
| [`task`](src/task) | 任务输入 `Task`、快照 `TaskRecord`、状态 `TaskStatus`（含 `Cancelled`）、运行快照 `RunSnapshot`/`RunStatus` |
| [`dsl`](src/dsl.rs) | `workflow!` 宏将 `nodes` / `subgraph` / `group` / `edges` 展开为对称流式构图链，并做编译期类型接线校验 |

## 公共任务模型与运行控制面

- PostgreSQL 与 SQLite 共用 `TaskRecord`、`Task`、`TaskStatus`（`pending` / `collecting` / `running` / `succeeded` / `failed` / `superseded` / `cancelled`）、`RunSnapshot`、`RunStatus` 和 `RetryPolicy`。
- `TaskRecord` 完整透出 `available_at` 与每任务独立的 `max_attempts`（默认 3，可由 `WorkerOptions::default_retry` 或 `Node::retry_policy()` 覆盖）。
- `Database` 与 `Workflow` 对称提供 `get_run`、`list_tasks`、`cancel_run`；`Workflow` 另提供基于结算事件唤醒的 `wait_run`。
- `Workflow::submit` 在入队前即按目标节点 `In` 类型预校验输入 JSON，非法载荷直接失败不落库空跑。
- 生命周期钩子（`on_start` / `on_success` / `on_error`）收敛在 `NodeContext` 上实现一次即覆盖全图；提交后回调运行在独立 `JoinSet`，不占用并发执行槽，`shutdown()` 时统一排空。

## 强类型槽位驱动 JOIN

业务结构体通过 `#[derive(JoinInput)]` 声明槽位（必填/可选完全由结构体定义单一决定，边上无需重复声明 `optional`）：

- 普通字段 `T`：单值必填槽（`MergeMode::Set`，`required = true`）
- `Option<T>` 或 `#[join(optional)] T`：单值可选槽（`MergeMode::Set`，`required = false`）
- `#[join(collect)] Vec<T>`：聚合必填槽（`MergeMode::Append`，`required = true`）
- `#[join(collect, optional)] Vec<T>`：聚合可选槽（`MergeMode::Append`，`required = false`）

支持通过 `#[join(crate = my_engine)]` 自定义重导出路径。

JOIN 就绪按**槽位满足度**与**作用域内上游状态**裁决：
1. 实时失败传播：有效外部入口或本作用域（含后代）本轮存在 `failed` 上游时立即级联失败（多轮汇合自动按上一轮终态 JOIN 截断，不被历史失败毒化）；
2. 活跃上游等待：仍有上游处于活跃态（`pending`/`collecting`/`running`）时继续等待；
3. 槽位终态验收：所有上游终结后，全部 `required` 槽位均已收到贡献则转 `pending` 并物化 `Append` 槽位，任一缺失则转 `failed`（天然支持 XOR-Merge 条件分支汇聚到必填槽 `T`、同节点混写必填与可选槽及 `on_error` 降级兜底）。

`Append` 槽位在 `collecting` 阶段以 O(1) 行追加至 `join_contributions` 表，就绪转 `pending` 时一次性物化到 `tasks.input_json` 并清理，消除大扇入下的 $O(N^2)$ JSON 写放大。

## 子图、视觉分组与对称箭头

- `subgraph name { ... }` 声明**运行期隔离作用域**（`isolated = true`），进入时写 `scopes` 表，隔离并发子图实例和 JOIN；命令式可通过 `GraphBuilder::mount` / `SubgraphBuilder` 跨处复用同一子图模板。
- `group name { ... }`（命令式 `.group(...)` / `.mount_group(...)`）声明**纯画布视觉分组**（`isolated = false`），不写 `scopes` 表，与运行期作用域解耦。
- 四种箭头构成 `单值/列表 × 共享/隔离作用域` 完整 2×2 对称矩阵：

| 箭头 | 分发给目标的输入 | 作用域语义 | 收到空列表时 |
| --- | --- | --- | --- |
| `->` | 整份传递一个值 | 共享作用域 | 传递一份 `[]` |
| `->!` | 整份传递一个值 | 进入独立新子图作用域 | 传递一份 `[]` |
| `->*` | 展开 `Vec<T>` 每项 | 共享作用域 | 零份输入 |
| `->+` | 展开 `Vec<T>` 每项 | 每项进入独立新子图作用域 | 零份输入 |

- 命令式建边统一使用对称流式 `Edge` API，与 `workflow!` 宏完全等价：
  - 强类型 `NodeKey`：`Edge::from(&a).to(&b).when(...).map(...).direct()` / `Edge::on_error(&a).to_slot(&j, j.slots().x()).direct()`
  - 动态节点名（支持 `Arc<str>` / `String`）：`Edge::success::<SrcOut>("a").to::<DstIn>("b").direct()` / `Edge::failure::<SrcIn>("a")...`
- 无条件无转换的直连/展开边走零 serde 透传快路径；条件/转换边直接借用 `&Value` 反序列化，消除无谓深拷贝。

## 外部事件触发

任何实现 `Node` 的节点均可通过 `fn trigger(&self) -> Option<Trigger>` 声明外部触发配置（与 `NodeRole` 及上游入边完全正交）。宿主通过 `workflow.triggers()` / `graph.triggers()` 读取声明并注册触发器，事件到达后调用 `workflow.submit(node_id, payload)` 或 `Database::in_transaction(&mut tx).submit(task)`（SQLite 先读后写事务建议显式 `BEGIN IMMEDIATE`）。

## 数据库与性能特征

- 引擎表：`tasks`、`scopes`、`join_contributions`（见 [`migrations/pgsql/0001_workflow_tasks.sql`](migrations/pgsql/0001_workflow_tasks.sql) 与 [`migrations/sqlite/0001_workflow_tasks.sql`](migrations/sqlite/0001_workflow_tasks.sql)）。
- 认领索引 `idx_tasks_claim(workflow_id, task_id) WHERE status IN ('pending', 'running')` 仅索引活跃行并原生按 `task_id` 有序；SQLite 写事务统一 `BEGIN IMMEDIATE`，时间列直接比较定长 UTC 文本（无 `julianday` 包装）。
- 扇出任务与独立作用域在 PG 使用 `UNNEST`、在 SQLite 使用分块多行 `VALUES` 批量写入；`scope::chain` 使用单条 `WITH RECURSIVE`。
- PG 结算按受影响 JOIN 的作用域深度做严格升序的精细 `pg_advisory_xact_lock`（无受影响 JOIN 时零 advisory lock），同级并发子图实例互不阻塞；`advance_joins` 先短路探测 `collecting` 作用域，再仅加载相关子树快照。
- 单次运行默认设置 `max_tasks_per_run = 10_000` 护栏（可通过 `WorkerOptions` 调整），防止失败回边环无限循环；指数退避自带抖动（jitter）防惊群。

## 开发与验证

```bash
cargo check --all-features
cargo test --all-features
```
