//! [`Graph`] 单元测试：执行路由、静态校验、成功/失败后继规划。

use super::*;
use crate::edge::{map, on_error, when, when_error};
use crate::node::{JoinNode, Node};
use std::future::Future;

struct Ctx;

struct AddOne;
impl Node<Ctx> for AddOne {
    type In = i32;
    type Out = i32;
    fn execute(
        &self,
        _ctx: &mut Ctx,
        input: i32,
    ) -> impl Future<Output = Result<i32, NodeError>> + Send {
        async move { Ok(input + 1) }
    }
}

struct Exploder;
impl Node<Ctx> for Exploder {
    type In = i32;
    type Out = i32;
    fn execute(
        &self,
        _ctx: &mut Ctx,
        input: i32,
    ) -> impl Future<Output = Result<i32, NodeError>> + Send {
        async move {
            if input > 10 {
                Err(NodeError::permanent(format!("too big: {input}")))
            } else {
                Ok(input)
            }
        }
    }
}

fn small_graph() -> Result<Graph<Ctx>, String> {
    Graph::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", Exploder)
        .node("c", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .edge(when("b", "c", |out: &i32| out % 2 == 0).map(|out: &i32| vec![*out]))
        // on_error —— 无条件失败边，按错误内容分支
        .error_edge(on_error("b", "a", |_: &i32, err: &NodeError| {
            if err.to_string().starts_with("too big") {
                vec![41]
            } else {
                Vec::new()
            }
        }))
        .build()
}

#[test]
fn execute_routes_by_external_name() {
    let g = small_graph().unwrap();
    let description = serde_json::to_value(g.describe()).unwrap();
    assert_eq!(description["workflow_id"], g.workflow_id());
    assert_eq!(description["title"], g.title());
    let mut ctx = Ctx;
    let out = tokio::runtime::Runtime::new().unwrap().block_on(g.execute(
        "a",
        &mut ctx,
        serde_json::json!(1),
    ));
    assert_eq!(out.unwrap(), serde_json::json!(2));
}

#[test]
fn build_rejects_empty_workflow_identity_or_title() {
    for (workflow_id, title, expected) in [
        ("", "工作流", "workflow_id must not be empty"),
        (" \t", "工作流", "workflow_id must not be empty"),
        ("test", "", "workflow title must not be empty"),
        ("test", " \n", "workflow title must not be empty"),
    ] {
        let error = Graph::builder(workflow_id, title, Ctx)
            .node("a", AddOne)
            .build()
            .err()
            .expect("empty workflow identity or title must fail build");
        assert_eq!(error, expected);
    }
}

#[test]
fn missing_node_errors() {
    let g = small_graph().unwrap();
    let mut ctx = Ctx;
    let out = tokio::runtime::Runtime::new().unwrap().block_on(g.execute(
        "nope",
        &mut ctx,
        serde_json::json!(1),
    ));
    assert!(out.is_err());
}

#[test]
fn validate_rejects_cycle() {
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .edge(map("b", "a", |_: &i32| vec![1]))
        .build();
    assert!(g.is_err());
}

#[test]
fn validate_allows_failure_to_upstream() {
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .node("c", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .edge(map("b", "c", |_: &i32| vec![1]))
        .error_edge(on_error("c", "b", |_: &i32, _: &NodeError| vec![1]))
        .build();
    assert!(g.is_ok());
}

#[test]
fn validate_rejects_failure_self_loop() {
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(on_error("b", "b", |_: &i32, _: &NodeError| vec![1]))
        .build();
    assert!(g.is_err());
}

#[test]
fn plan_success_applies_when_then_map() {
    let g = small_graph().unwrap();
    // b 输出 42（偶数）→ when 命中 → map 产出 {42}
    let hit = g.plan_success("b", &serde_json::json!(42)).unwrap();
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0].dst, "c");
    assert_eq!(hit[0].inputs, vec![serde_json::json!(42)]);
    // b 输出 41（奇数）→ when 不命中 → 无后继
    let miss = g.plan_success("b", &serde_json::json!(41)).unwrap();
    assert!(miss.is_empty());
}

#[test]
fn plan_success_applies_map_fanout() {
    let g = small_graph().unwrap();
    // a → b 是无条件 map（恒产出输入 1）
    let plans = g.plan_success("a", &serde_json::json!(7)).unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].dst, "b");
    assert_eq!(plans[0].inputs, vec![serde_json::json!(1)]);
}

#[test]
fn plan_success_aggregates_fanout_per_dst() {
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1, 2, 3]))
        .build()
        .unwrap();
    // a → b 的 map 扇出 3 条输入 → 聚合为单个 dst 的 3 实例后继
    let plans = g.plan_success("a", &serde_json::json!(0)).unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].dst, "b");
    assert_eq!(plans[0].inputs.len(), 3);
    // 普通节点也记录通用血缘：depends_on_names = {a}
    assert_eq!(plans[0].depends_on_names, vec!["a"]);
    // 普通节点任务类型为 Normal
    assert_eq!(plans[0].task_type, TaskType::Normal);
}

#[test]
fn plan_success_join_declares_static_predecessors() {
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .node("join", JoinNode)
        .edge(map("a", "join", |_: &i32| vec![1]))
        .edge(map("b", "join", |_: &i32| vec![1]))
        .build()
        .unwrap();
    // 无论从哪个前驱规划，JOIN 门都声明完整静态前驱集
    let from_a = g.plan_success("a", &serde_json::json!(0)).unwrap();
    assert_eq!(from_a[0].depends_on_names.len(), 2);
    // JOIN 门任务类型为 Join
    assert_eq!(from_a[0].task_type, TaskType::Join);
    let from_b = g.plan_success("b", &serde_json::json!(0)).unwrap();
    assert_eq!(from_b[0].depends_on_names.len(), 2);
}

#[test]
fn plan_failure_rebuilds_retry_input() {
    let g = small_graph().unwrap();
    let err = NodeError::permanent("too big: 42");
    // on_error(b → a) 命中 → 用节点输入 + 错误重建重试输入（返回 Successor，与 plan_success 对称）
    let retry = g.plan_failure("b", &serde_json::json!(1), &err).unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].dst, "a");
    assert_eq!(retry[0].inputs, vec![serde_json::json!(41)]);
    // 失败后继是即时派发：恒为 Normal、无等齐闸门/血缘
    assert_eq!(retry[0].task_type, TaskType::Normal);
    assert!(retry[0].depends_on_names.is_empty());
    // 错误前缀不匹配 → 放弃重试
    let skip = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::permanent("other"))
        .unwrap();
    assert!(skip.is_empty());
}

#[test]
fn plan_failure_applies_when_then_map() {
    // 失败条件边（when_error）：条件可基于错误类型/输入，命中后经 map 扇出 dst 输入，
    // 与 plan_success 的 when+map 完全对称——失败边不止「回退」，可按错误分支路由。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .node("c", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(
            when_error("b", "c", |_: &i32, err: &NodeError| err.is_retryable())
                .map(|input: &i32, err: &NodeError| vec![*input + err.to_string().len() as i32]),
        )
        .build()
        .unwrap();
    // 可重试错误 → 条件命中 → map 产出重试输入（1 + "boom".len() == 5）
    let retry = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].dst, "c");
    assert_eq!(retry[0].inputs, vec![serde_json::json!(5)]);
    // 永久错误 → 条件不命中 → 剪枝（无下游）
    let skip = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::permanent("boom"))
        .unwrap();
    assert!(skip.is_empty());
}

#[test]
fn plan_failure_passthrough_failed_input() {
    // 失败条件边透传（when_error(...).passthrough()）：条件命中后把失败输入原样喂给 dst。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(
            when_error("b", "a", |_: &i32, err: &NodeError| err.is_retryable()).passthrough(),
        )
        .build()
        .unwrap();
    // 可重试错误 → 透传失败输入（重试同输入）
    let plans = g
        .plan_failure("b", &serde_json::json!(7), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].dst, "a");
    assert_eq!(plans[0].inputs, vec![serde_json::json!(7)]);
}

#[test]
fn plan_failure_aggregates_fanout_per_dst() {
    // 失败边 map 扇出多条 → 按 dst 聚合为单条 Successor 的多输入（与 plan_success 一致）。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .node("c", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(on_error("b", "a", |_: &i32, _: &NodeError| vec![1, 2, 3]))
        .build()
        .unwrap();
    let plans = g
        .plan_failure("b", &serde_json::json!(0), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].dst, "a");
    assert_eq!(
        plans[0].inputs,
        vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3)
        ]
    );
}

#[test]
fn plan_failure_reflects_dst_task_type() {
    // task_type 如实反映 dst 节点类型（失败边可汇聚到 JOIN/错误处理节点），
    // 不硬编码为普通节点；depends_on_names 因存储层不支持「等齐失败」保持为空。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("join", JoinNode)
        .edge(map("a", "join", |_: &i32| vec![1]))
        .error_edge(on_error("a", "join", |_: &i32, _: &NodeError| vec![1]))
        .build()
        .unwrap();
    let plans = g
        .plan_failure("a", &serde_json::json!(1), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].dst, "join");
    assert_eq!(plans[0].task_type, TaskType::Join);
    assert!(plans[0].depends_on_names.is_empty());
}

#[test]
fn plan_failure_surfaces_type_mismatch_as_error() {
    // 失败边期望 SrcIn: i32；若失败节点输入为字符串，规划应返回 Err 而非静默剪枝
    // （对称于成功边的 plan_success_surfaces_type_mismatch_as_error）。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(on_error("b", "a", |_: &i32, _: &NodeError| vec![1]))
        .build()
        .unwrap();
    let err = g
        .plan_failure(
            "b",
            &serde_json::json!("not-an-int"),
            &NodeError::retryable("boom"),
        )
        .unwrap_err();
    assert!(matches!(err, EdgeError::BadSrcIn { .. }));
}

#[test]
fn validate_rejects_type_mismatched_edge() {
    // 命令式路径无编译期接线检查：边声明载荷与注册节点 In/Out 不一致时，
    // build 期指纹校验拒绝（此前要到任务结算时才以 serde 错误暴露）。
    let result = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne) // Out = i32
        .node("b", AddOne) // In = i32
        .edge(map("a", "b", |_: &String| vec![String::new()])) // SrcOut = String ≠ i32
        .build();
    let err = result.err().expect("type mismatch must fail build");
    assert!(err.contains("a -> b"), "{err}");
    assert!(err.contains("String") && err.contains("i32"), "{err}");
}

#[test]
fn validate_rejects_dst_type_mismatch() {
    // 出参方向：DstIn = String ≠ 目标节点 In = i32。
    let result = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![String::new()]))
        .build();
    let err = result.err().expect("type mismatch must fail build");
    assert!(
        err.contains("出参") && err.contains("String") && err.contains("i32"),
        "{err}"
    );
}

#[test]
fn validate_rejects_failure_edge_type_mismatch() {
    // 失败边入参方向：SrcIn = String ≠ 失败节点 In = i32。
    let result = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .error_edge(on_error("b", "a", |_: &String, _: &NodeError| vec![1]))
        .build();
    let err = result.err().expect("type mismatch must fail build");
    assert!(err.contains("String") && err.contains("i32"), "{err}");
}

#[test]
fn validate_allows_value_boundary_into_join() {
    // Value 边界：具体载荷（i32）汇入 JoinNode（In = Value）恒兼容——节点侧反序列化为
    // Value 必成功，无需类型一致（DSL 的 JOIN 汇入边产出 Value 则是另一侧的兼容方向）。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("join", JoinNode)
        .edge(map("a", "join", |_: &i32| vec![1]))
        .error_edge(on_error("a", "join", |_: &i32, _: &NodeError| vec![1]))
        .build();
    assert!(g.is_ok());
}

#[test]
fn plan_success_surfaces_type_mismatch_as_error() {
    // a → b 的 map 期望 SrcOut: i32；若上游输出为字符串，规划应返回 Err 而非静默剪枝。
    let g = Graph::<Ctx>::builder("test", "测试工作流", Ctx)
        .node("a", AddOne)
        .node("b", AddOne)
        .edge(map("a", "b", |_: &i32| vec![1]))
        .build()
        .unwrap();
    let err = g
        .plan_success("a", &serde_json::json!("not-an-int"))
        .unwrap_err();
    assert!(matches!(err, EdgeError::BadSrcOut { .. }));
}
