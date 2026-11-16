//! 通用单会话调度器 [`Scheduler`]：批量认领无依赖任务 → 多协程并发结算。
//!
//! 作为**通用基础设施**，调度器只依赖任务快照 [`Task`]、任务存储 [`TaskStore`]、
//! 图引擎 [`Graph`] 与节点上下文契约 [`NodeContext`]。它不持有任何业务字段——
//! `workflow_id`/业务血缘/`repo` 都收敛于具体 `TaskStore` 实现与上下文实现，调度器
//! 不解读任务语义，只驱动 `claim → settle` 循环并把图规划结果交给存储落库。
//!
//! 图在调度器构造时固定：拓扑真相源不随会话配置变化，易变配置（如模型执行器）由
//! 上下文在 [`NodeContext::fork`] 里按任务复制；业务侧若需要刷新配置，也在这一步做
//! （失败自行降级，沿用上一份可用上下文）。
//!
//! 每个 session 由独立 [`Scheduler`] 驱动：`claim_many` 一次认领同一会话内互相无依赖的
//! 一批任务（依赖闸门在存储层过滤），认领到的任务由独立 tokio 协程并发 `settle`；
//! `store` 与图经 `Arc` 跨协程共享。多实例水平拓展时由
//! `FOR UPDATE SKIP LOCKED` 兜底认领，会话级 advisory lock 留待多实例阶段补充。
//!
//! 并发闸门为**在途任务计数**（[`JoinSet`] 跟踪，`len <= max_concurrency`）而非「整批结算
//! 完成才放行」：认领后立即 spawn 结算并回到认领循环，快任务的后续（如 JOIN 门 →
//! `finalize_node`）在下一轮认领立即被捡走，慢的 AI 任务仍在后台并发执行，避免慢任务
//! 拖住整条就绪链。

use std::{future::Future, sync::Arc, time::Duration};

use futures::FutureExt;
use tracing::{Instrument, error, info, warn};

use super::graph::{Graph, Successor};
use super::node::{NodeContext, NodeError};
use super::task::{FanoutPlan, Task, TaskError, TaskStore};

/// 默认单批并发结算上限。
const DEFAULT_MAX_CONCURRENCY: usize = 8;

/// 单会话调度器：批量认领 → 多协程并发结算。
///
/// 泛型参数 `Ctx` 为节点上下文（图持有的基座），`Store` 为任务存储。调度器把 `graph`
/// 作为拓扑真相源，把规划结果（后继 / 重试）交给 `store` 落库，与具体上下文 / 存储类型彻底解耦。
pub struct Scheduler<Ctx, Store>
where
    Ctx: NodeContext<Store::Task> + Send + Sync + 'static,
    Store: TaskStore,
{
    store: Arc<Store>,
    graph: Arc<Graph<Ctx>>,
    poll: Duration,
    /// None 常驻；Some(timeout) 在无在途任务且连续空闲达到 timeout 后退出。
    idle_timeout: Option<Duration>,
    /// 并发闸门：同时在途结算的任务数上限（认领只补足到该上限，非整批结算后才放行）。
    max_concurrency: usize,
    /// 任务就绪唤醒信号（LISTEN/NOTIFY 分发，`None` 则纯轮询）。通知是提示语义：
    /// 可丢失、可重复；被唤醒后认领为空就继续等，兜底轮询保证最终一致。
    wake: Option<Arc<tokio::sync::Notify>>,
}

impl<Ctx, S> Scheduler<Ctx, S>
where
    Ctx: NodeContext<S::Task> + Send + Sync + 'static,
    S: TaskStore + Send + Sync + 'static,
    S::Task: Task + Sync + 'static,
{
    /// 由已构造的图与任务存储构造调度器；idle_timeout 为 None 时持续运行。
    pub fn new(
        graph: Arc<Graph<Ctx>>,
        store: S,
        poll: Duration,
        idle_timeout: Option<Duration>,
    ) -> Self {
        Self {
            store: Arc::new(store),
            graph,
            poll,
            idle_timeout,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            wake: None,
        }
    }

    /// 覆盖并发结算上限（默认 [`DEFAULT_MAX_CONCURRENCY`]，至少 1）。
    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = n.max(1);
        self
    }

    /// 挂接任务就绪唤醒信号（`None` 保持纯轮询）。唤醒后立即认领，`poll` 只作兜底。
    pub fn with_wake(mut self, wake: Option<Arc<tokio::sync::Notify>>) -> Self {
        self.wake = wake;
        self
    }

    /// 认领 → 并发结算；配置 Some(idle_timeout) 时连续空闲达到时限后退出。
    /// None 表示常驻，包括暂时无法认领任务或数据库查询失败时。
    ///
    /// 认领到的任务立即 `spawn` 结算，不等待整批结算完成：每轮按
    /// `max_concurrency - 在途数` 认领补足并发槽位，并优先被「任务完成」事件唤醒
    /// （完成可能带出后继，下一轮认领立即捡走），慢的 AI 任务仍在后台并发执行，避免慢任务
    /// 拖住整条就绪链。空闲等待优先由 `wake` 唤醒（毫秒级），`poll` 兜底租约到期恢复
    /// 与通知丢失。
    pub async fn run(self) {
        self.run_until(std::future::pending()).await;
    }

    /// 运行到停止信号完成或配置的空闲时限到达，再取消并等待全部在途节点任务退出。
    ///
    /// 调度循环 panic 时同样先等待在途任务取消完成，再向上层传播 panic。
    /// 调用方应完成 shutdown future 并等待本方法返回；直接中止本方法无法等待清理完成。
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) {
        let mut in_flight = tokio::task::JoinSet::new();
        let outcome = std::panic::AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = shutdown => {},
                _ = self.run_loop(&mut in_flight) => {},
            }
        })
        .catch_unwind()
        .await;
        in_flight.shutdown().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    async fn run_loop(&self, in_flight: &mut tokio::task::JoinSet<Result<(), S::Error>>) {
        let store = self.store.clone();
        let graph = self.graph.clone();
        let mut idle = Duration::ZERO;
        // 在途任务并发闸门：`JoinSet` 跟踪并发结算协程，`len <= max_concurrency`。
        // 从结构上保证并发上限，同时认领循环不被慢任务阻塞（快任务后继即时被认领）。
        loop {
            // 非阻塞回收已完成在途任务的结算结果（含 panic/join 错误），释放并发槽位。
            while let Some(res) = in_flight.try_join_next() {
                if let Err(e) = res {
                    error!(error = %e, "worker settle task failed (panic/join or store error)");
                }
            }

            let available = self.max_concurrency - in_flight.len();
            if available == 0 {
                // 并发槽位已满：等一个在途任务完成释放槽位后继续认领。
                wait_for_completion::<S>(in_flight, self.poll).await;
                continue;
            }

            // 认领前不再有配置闸门：图是固定的拓扑真相源，易变配置由 `fork` 在执行前刷新
            // （失败则沿用上一份已生效配置，不阻塞认领）。
            match store.claim_many(available).await {
                Ok(tasks) if !tasks.is_empty() => {
                    tracing::info!(
                        "claim tasks: {:?}",
                        &tasks.iter().map(|t| t.name()).collect::<Vec<&str>>()
                    );
                    idle = Duration::ZERO;
                    // 认领到的任务互相无依赖，各自独立协程并发结算。
                    // 经 `settle_safely` 捕获节点 panic：释放租约并通知重入，任务行不停摆等租约。
                    for task in tasks {
                        let store = store.clone();
                        let graph = graph.clone();
                        in_flight.spawn(settle_safely(graph, store, task));
                    }
                }
                Ok(_) => {
                    if in_flight.is_empty() {
                        if self.idle_timeout.is_some_and(|timeout| idle >= timeout) {
                            info!("scheduler idle, exiting");
                            break;
                        }
                        let wait_started = tokio::time::Instant::now();
                        wait_for_work(&self.wake, self.poll).await;
                        idle += wait_started.elapsed();
                    } else {
                        // 无新任务可认领但有在途任务：等一个完成（可能带出后继）立即唤醒，
                        // 不累计空闲（产线仍在推进）。
                        idle = Duration::ZERO;
                        wait_for_completion::<S>(in_flight, self.poll).await;
                    }
                }
                Err(e) => {
                    error!(error = %e, "worker claim failed");
                    if in_flight.is_empty() {
                        if self.idle_timeout.is_some_and(|timeout| idle >= timeout) {
                            break;
                        }
                        let wait_started = tokio::time::Instant::now();
                        wait_for_work(&self.wake, self.poll).await;
                        idle += wait_started.elapsed();
                    } else {
                        idle = Duration::ZERO;
                        wait_for_completion::<S>(in_flight, self.poll).await;
                    }
                }
            }
        }
    }
}

/// 等待「有工作可认领」：任务就绪通知立即唤醒，否则按 `poll` 兜底轮询超时
/// （覆盖租约到期恢复与通知丢失）。通知是提示语义，被唤醒后认领为空（竞态/
/// 已被其他 worker 认走）就进入下一轮继续等，无害。
async fn wait_for_work(wake: &Option<Arc<tokio::sync::Notify>>, poll: Duration) {
    match wake {
        Some(n) => {
            let notified = n.notified();
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(poll) => {}
            }
        }
        None => tokio::time::sleep(poll).await,
    }
}

/// 等待至少一个在途任务完成（`join_next` 返回即释放一个并发槽位），`poll` 为兜底超时，
/// 防止结算协程异常退出导致永久阻塞；完成结果中的 panic/join/存储错误在此记录。
///
/// `join_next` 只会消费恰好一个已完成的协程，其余完成者由调度循环顶部的
/// `try_join_next` 在下一轮非阻塞回收。
async fn wait_for_completion<S>(
    in_flight: &mut tokio::task::JoinSet<Result<(), S::Error>>,
    poll: Duration,
) where
    S: TaskStore,
{
    match tokio::time::timeout(poll, in_flight.join_next()).await {
        Ok(Some(Err(e))) => {
            error!(error = %e, "worker settle task failed (panic/join or store error)")
        }
        _ => {}
    }
}

/// 结算一个已认领的任务：取得节点实例并执行 → 落库 + 后继规划。
///
/// 每个节点注入一份 `Ctx` 拷贝（[`NodeContext::fork`]）：把任务快照透传给上下文，
/// 共享字段借用同一 `Arc` 跨协程安全共享。
async fn settle<Ctx, S>(
    graph: Arc<Graph<Ctx>>,
    store: Arc<S>,
    task: S::Task,
) -> Result<(), S::Error>
where
    Ctx: NodeContext<S::Task> + Send + Sync + 'static,
    S: TaskStore + Send + Sync + 'static,
    S::Task: Task + Sync + 'static,
{
    let name = task.name().to_string();
    let task_id = task.task_id();
    // 模型调用（AgentRunner 的 `AI model request *` 日志）经该 span 继承任务身份：
    // span 覆盖 execute 全程，内部的 tracing 日志自动携带 task_id / name，无需把
    // 任务名逐层传给图引擎。
    let span = tracing::info_span!("", %task_id, %name);

    info!(%task_id, %name, "settle task");

    // 注入当前任务的节点上下文拷贝（任务快照含 task_id 等身份字段）。
    let mut ctx = graph.context().fork(&task).await;

    graph.on_start(&name, &mut ctx, task.input()).await;
    match graph
        .execute(&name, &mut ctx, task.input().clone())
        .instrument(span)
        .await
    {
        Ok(output) => {
            // 先做后继规划：路由类型错误（[`EdgeError`]，图装配/类型接线错误）为确定性
            // 错误，视为节点永久失败，不再结算为成功（否则下游永远等不到后继）。

            let successors = match graph.plan_success(&name, &output) {
                Ok(s) => s,
                Err(e) => {
                    warn!(%task_id, %name, error = %e, "plan successors failed; failing task");
                    return settle_failure(
                        &graph,
                        store.as_ref(),
                        &mut ctx,
                        &task,
                        &NodeError::permanent(e.to_string()),
                    )
                    .await;
                }
            };

            // 原子结算：任务置 succeeded + 全部后继入队同一事务（单输入 → 单实例扇入合并；
            // 多条输入 → map 边扇出多实例，成功后继依赖当前任务 `depends_on_ids = [task_id]`）。
            // 事务失败整体回滚，任务仍可认领——释放租约让它在下一轮立即重入（重执行一次的
            // 代价即引擎既有的 at-least-once 语义），杜绝「已成功但后继从未入队」的静默停摆。
            let fanouts: Vec<FanoutPlan> = successors.iter().map(FanoutPlan::from).collect();
            if let Err(e) = store.succeed_and_enqueue(&task, &output, &fanouts).await {
                error!(%task_id, %name, error = %e, "settle success transaction failed; releasing claim for retry");
                if let Err(re) = store.release_claim(&task).await {
                    error!(%task_id, error = %re, "release claim after failed settle");
                }
                return Err(e);
            }

            // on_success 阶段：回调节点 `on_success`（节点在此发布 `task_succeeded`
            // 及 `node_started`/`node_ready` 等特有事件）。落库与后继已原子提交，事件在其后发布。
            graph.on_success(&name, &mut ctx, &output).await;
            Ok(())
        }
        Err(e) => settle_failure(&graph, store.as_ref(), &mut ctx, &task, &e).await,
    }
}

/// 结算失败：回调节点 `on_error` → 按图上失败边规划后继 → 原子失败结算（fail/superseded
/// + 全部后继入队同一事务）→ 无后继则级联。
///
/// 失败边是通用路由（重试、兜底、错误分支），引擎不区分。有后继时标记 `superseded`、
/// 不级联（下游应由后继驱动）；无后继时标记 `failed` 并级联，避免悬挂。
/// 原子结算事务失败则整体回滚、释放租约，任务重入后重新执行并重走失败路由。
async fn settle_failure<Ctx, S>(
    graph: &Graph<Ctx>,
    store: &S,
    ctx: &mut Ctx,
    task: &S::Task,
    e: &NodeError,
) -> Result<(), S::Error>
where
    Ctx: NodeContext<S::Task> + Send + Sync,
    S: TaskStore,
    S::Task: Task + Sync,
{
    let task_id = task.task_id();
    let name = task.name().to_string();
    warn!(%task_id, %name, error = %e, "task failed");
    // on_error 阶段：回调节点 `on_error`（节点在此发布 `task_failed`）。
    graph.on_error(&name, ctx, e).await;

    // 失败路由规划（路由类型错误视为无后继，告警后按永久失败结算）。
    let fanouts: Vec<FanoutPlan> = match graph.plan_failure(&name, task.input(), e) {
        Ok(successors) => successors.iter().map(FanoutPlan::from).collect(),
        Err(err) => {
            warn!(%task_id, %name, error = %err, "plan failure successors failed");
            Vec::new()
        }
    };
    // 有后继接管失败 → 标记 superseded（不对外暴露为失败），前端看到后继在推进；
    // 预算耗尽/不可重试 → 标记 failed，前端最终看到失败。
    let has_successors = !fanouts.is_empty();

    // 原子失败结算：fail/superseded + 全部失败后继入队同一事务（与成功路径同一实例模型；
    // 失败后继不依赖当前任务 `depends_on_ids = []`，否则会被认领闸门永久阻塞）。
    // 错误在此折叠为存储可见的失败载荷（仅消息，见 `TaskError`），错误种类已在
    // 失败路由规划（`plan_failure`）阶段消费完毕。
    let task_err = TaskError::from(e);
    if let Err(err) = store
        .fail_and_enqueue(task, &task_err, has_successors, &fanouts)
        .await
    {
        warn!(%task_id, error = %err, "fail settle transaction failed; releasing claim for retry");
        if let Err(re) = store.release_claim(task).await {
            error!(%task_id, error = %re, "release claim after failed settle");
        }
        return Err(err);
    }
    // 无后继 → 级联所有直接依赖它的 pending 后继，避免它们因前驱已失败而永远无法被认领（悬挂）。
    if !has_successors {
        match store.cascade_failed_predecessor(task).await {
            Ok(n) if n > 0 => {
                info!(%task_id, %name, cascaded = n, "cascade predecessor failure")
            }
            Ok(_) => {}
            Err(err) => warn!(%task_id, error = %err, "cascade predecessor failure failed"),
        }
    }
    Ok(())
}

/// 结算协程入口：捕获节点 panic（节点 bug 不应让任务行停摆等租约过期），
/// panic 时释放租约并通知重入，任务下一轮立即被重认领（attempt 照常 +1，预算护栏兜底）。
async fn settle_safely<Ctx, S>(
    graph: Arc<Graph<Ctx>>,
    store: Arc<S>,
    task: S::Task,
) -> Result<(), S::Error>
where
    Ctx: NodeContext<S::Task> + Send + Sync + 'static,
    S: TaskStore + Send + Sync + 'static,
    S::Task: Task + Sync + 'static,
{
    let task_id = task.task_id();
    let release_task = task.clone();
    match std::panic::AssertUnwindSafe(settle(graph, store.clone(), task))
        .catch_unwind()
        .await
    {
        Ok(res) => res,
        Err(panic) => {
            let detail = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("(non-string panic payload)");
            error!(%task_id, panic = %detail, "settle task panicked; releasing claim for immediate retry");
            if let Err(e) = store.release_claim(&release_task).await {
                error!(%task_id, error = %e, "release claim after panic failed");
            }
            Ok(())
        }
    }
}

/// 把图规划结果折叠为存储入队计划（`TaskStore` 只见 [`FanoutPlan`]，不依赖 `Graph` 类型）。
impl From<&Successor> for FanoutPlan {
    fn from(s: &Successor) -> Self {
        FanoutPlan {
            dst: s.dst,
            task_type: s.task_type.as_str(),
            inputs: s.inputs.clone(),
            depends_on_names: s.depends_on_names.clone(),
        }
    }
}

/// 把节点错误折叠为存储可见的失败载荷（`TaskStore` 只见 [`TaskError`]，不依赖
/// `node::NodeError`）。错误种类（可重试 / 永久）已在失败路由规划阶段消费，存储层只需消息。
impl From<&NodeError> for TaskError {
    fn from(e: &NodeError) -> Self {
        TaskError::new(e.to_string())
    }
}
