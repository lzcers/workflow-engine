use std::future::Future;
use workflow_engine::{
    Graph, Node, NodeError, Trigger,
    graph::{EdgeMode, TaskType},
    task::Dispatch,
    workflow,
};

struct Ctx;

struct PassThrough;
impl Node<Ctx> for PassThrough {
    type In = serde_json::Value;
    type Out = serde_json::Value;
    fn execute(
        &self,
        _ctx: &mut Ctx,
        input: Self::In,
    ) -> impl Future<Output = Result<Self::Out, NodeError>> + Send {
        async move { Ok(input) }
    }
}

struct Values;
impl Node<Ctx> for Values {
    type In = Vec<serde_json::Value>;
    type Out = Vec<serde_json::Value>;

    async fn execute(&self, _: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}

#[test]
fn arrows_route_collections_as_one_input_or_as_individual_items() {
    use serde_json::json;
    let graph = workflow! {
        workflow_id: "collection-routes";
        title: "Collection routes";
        context: Ctx;
        nodes { source => Values, batch => Values }
        subgraph analysis { nodes { item => PassThrough } }
        edges {
            source -> batch,
            source ->* analysis.item,
            source ->+ analysis.item,
            source ->+ analysis.item when(|items| items.len() > 1) |>
                |items| items.iter().rev().cloned().collect(),
            source ->* analysis.item on_error,
            source ->+ analysis.item when_error(|_, err| err.is_retryable()) |>
                |items, _| items.iter().rev().cloned().collect(),
        }
    }
    .unwrap();

    let description = graph.describe();
    assert!(!description.edges[2].has_transform);
    assert!(description.edges[3].has_transform);
    let modes: Vec<_> = description.edges.iter().map(|edge| edge.mode).collect();
    assert_eq!(
        modes,
        [
            EdgeMode::Direct,
            EdgeMode::SharedExpand,
            EdgeMode::IsolatedExpand,
            EdgeMode::IsolatedExpand,
            EdgeMode::SharedExpand,
            EdgeMode::IsolatedExpand,
        ]
    );
    assert_eq!(
        serde_json::to_value(modes).unwrap(),
        json!([
            "direct",
            "shared_expand",
            "isolated_expand",
            "isolated_expand",
            "shared_expand",
            "isolated_expand"
        ])
    );
    let plans = graph.plan_success("source", &json!([1, 2])).unwrap();
    assert_eq!(plans.len(), 4);
    assert_eq!(plans[0].inputs, [json!([1, 2])]);
    assert_eq!(plans[0].dst, "batch");
    for plan in &plans[1..] {
        assert_eq!(plan.dst, "analysis.item");
    }
    assert_eq!(plans[1].dispatch, Dispatch::Shared);
    assert_eq!(plans[1].inputs, [json!(1), json!(2)]);
    assert_eq!(plans[2].dispatch, Dispatch::PerInputScope);
    assert_eq!(plans[2].inputs, plans[1].inputs);
    assert_eq!(plans[3].dispatch, Dispatch::PerInputScope);
    assert_eq!(plans[3].inputs, [json!(2), json!(1)]);

    let empty = graph.plan_success("source", &json!([])).unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].inputs, [json!([])]);
    let singleton = graph.plan_success("source", &json!([1])).unwrap();
    assert_eq!(singleton.len(), 3);

    let retry = graph
        .plan_failure("source", &json!([1, 2]), &NodeError::retryable("retry"))
        .unwrap();
    assert_eq!(retry.len(), 2);
    assert_eq!(retry[0].dispatch, Dispatch::Shared);
    assert_eq!(retry[0].inputs, [json!(1), json!(2)]);
    assert_eq!(retry[1].dispatch, Dispatch::PerInputScope);
    assert_eq!(retry[1].inputs, [json!(2), json!(1)]);
    let permanent = graph
        .plan_failure("source", &json!([1, 2]), &NodeError::permanent("stop"))
        .unwrap();
    assert_eq!(permanent.len(), 1);
    assert_eq!(permanent[0].dispatch, Dispatch::Shared);
}

#[test]
fn isolated_expansion_rejects_join_targets() {
    let graph = workflow! {
        workflow_id: "isolated-join";
        title: "Isolated JOIN";
        context: Ctx;
        nodes { source => Values }
        subgraph analysis { nodes { join => workflow_engine::JoinNode } }
        edges { source ->+ analysis.join }
    };
    assert_eq!(
        graph.err().as_deref(),
        Some("->+ edge must not target join node analysis.join")
    );
}

struct EventNode(Option<Trigger>);

impl Node<Ctx> for EventNode {
    type In = serde_json::Value;
    type Out = serde_json::Value;
    const TASK_TYPE: TaskType = TaskType::Event;

    fn trigger(&self) -> Option<Trigger> {
        self.0.clone()
    }

    async fn execute(&self, _: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
        Ok(input)
    }
}

fn cron_trigger() -> Trigger {
    Trigger {
        kind: "cron".into(),
        config: serde_json::json!({"expression": "0 9 * * *", "timezone": "Asia/Shanghai"}),
    }
}

#[test]
fn event_nodes_are_discovered_and_described_through_existing_dsl() {
    let graph = workflow! {
        workflow_id: "event-description";
        title: "Event description";
        context: Ctx;
        nodes {
            z_tick => EventNode(Some(cron_trigger())),
            process => PassThrough,
            #[title = "支付回调"]
            a_payment => EventNode(Some(Trigger {
                kind: "payment.webhook".into(),
                config: serde_json::json!({"path": "/payments"}),
            })),
        }
        edges {
            z_tick -> process,
            a_payment -> process,
        }
    }
    .unwrap();

    let triggers = graph.triggers();
    assert_eq!(
        triggers.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ["a_payment", "z_tick"]
    );
    assert_eq!(triggers[0].1.kind, "payment.webhook");
    assert_eq!(
        triggers[0].1.config,
        serde_json::json!({"path": "/payments"})
    );
    assert_eq!(triggers[1].1, cron_trigger());

    let description = graph.describe();
    assert_eq!(description.nodes[0].id, "a_payment");
    assert_eq!(description.nodes[0].title, "支付回调");
    assert_eq!(description.nodes[0].task_type, TaskType::Event);
    assert_eq!(description.nodes[0].trigger.as_ref(), Some(&triggers[0].1));
    assert_eq!(description.nodes[1].id, "process");
    assert!(description.nodes[1].trigger.is_none());
    let serialized = serde_json::to_value(&description).unwrap();
    assert_eq!(
        serialized["nodes"][2]["trigger"]["config"],
        cron_trigger().config
    );
    assert!(serialized["nodes"][1]["trigger"].is_null());
}

#[test]
fn event_nodes_require_consistent_trigger_metadata() {
    struct OrdinaryNodeWithTrigger;

    impl Node<Ctx> for OrdinaryNodeWithTrigger {
        type In = serde_json::Value;
        type Out = serde_json::Value;

        fn trigger(&self) -> Option<Trigger> {
            Some(cron_trigger())
        }

        async fn execute(&self, _: &mut Ctx, input: Self::In) -> Result<Self::Out, NodeError> {
            Ok(input)
        }
    }

    assert!(
        Graph::builder("missing-trigger", "Missing trigger", Ctx)
            .node("event", EventNode(None))
            .build()
            .is_err()
    );
    assert!(
        Graph::builder("unexpected-trigger", "Unexpected trigger", Ctx)
            .node("ordinary", OrdinaryNodeWithTrigger)
            .build()
            .is_err()
    );
}

#[test]
fn event_nodes_reject_incoming_success_and_failure_edges() {
    let success = workflow! {
        workflow_id: "event-success-edge";
        title: "Event success edge";
        context: Ctx;
        nodes {
            source => PassThrough,
            event => EventNode(Some(cron_trigger())),
        }
        edges {
            source -> event,
        }
    };
    assert!(success.is_err());

    let failure = workflow! {
        workflow_id: "event-failure-edge";
        title: "Event failure edge";
        context: Ctx;
        nodes {
            source => PassThrough,
            event => EventNode(Some(cron_trigger())),
        }
        edges {
            source -> event on_error,
        }
    };
    assert!(failure.is_err());
}

/// `on_error |> fn`（无条件）/ `when_error(cond) |> fn`（条件）失败边 DSL 语法：都能展开并规划。
/// 节点名与边端点均为标识符（类型级 key），经 checked 构造函数做编译期类型接线校验。
#[test]
fn failure_edge_dsl_syntax() {
    let g: Graph<Ctx> = workflow!(
        workflow_id: "test";
        title: "测试工作流";
        context: Ctx;
        nodes {
            #[title = "源节点"]
            a => PassThrough,
            #[description = "失败后分流"]
            b => PassThrough,
            c => PassThrough,
        }
        edges {
            a -> b |> |_: &serde_json::Value| serde_json::json!(1),
            b -> a on_error |> |_: &serde_json::Value, _: &NodeError| serde_json::json!(2),
            b -> c when_error(|_: &serde_json::Value, err: &NodeError| err.is_retryable()) |>
                |input: &serde_json::Value, _: &NodeError| input.clone(),
        }
    )
    .unwrap();

    let description = g.describe();
    assert_eq!(description.nodes[0].title, "源节点");
    assert_eq!(description.nodes[0].description, "");
    assert_eq!(description.nodes[1].title, "b");
    assert_eq!(description.nodes[1].description, "失败后分流");
    assert_eq!(description.nodes[2].title, "c");
    assert_eq!(description.nodes[2].description, "");

    // 可重试错误：无条件 on_error 与条件 when_error 都命中 → 路由到 a、c
    let retry = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(retry.len(), 2);
    let mut dsts: Vec<_> = retry.iter().map(|s| s.dst).collect();
    dsts.sort();
    assert_eq!(dsts, vec!["a", "c"]);
    // 永久错误：when_error 条件不命中 → 只走无条件 on_error
    let skip = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::permanent("boom"))
        .unwrap();
    assert_eq!(skip.len(), 1);
    assert_eq!(skip[0].dst, "a");
}

#[tokio::test]
async fn infers_instance_and_context_types_and_evaluates_each_expression_once() {
    struct OffsetContext(i32);
    struct OffsetNode(i32);
    impl Node<OffsetContext> for OffsetNode {
        type In = i32;
        type Out = i32;
        async fn execute(&self, ctx: &mut OffsetContext, input: i32) -> Result<i32, NodeError> {
            Ok(input + self.0 + ctx.0)
        }
    }
    let mut constructions = 0;
    // Neither node nor context implements Clone/Copy; expressions may move owned values.
    let destination = OffsetNode(4);
    let workflow_id = String::from("offset_workflow");
    let workflow_title = String::from("偏移工作流");
    let title = String::from("带偏移的源节点");
    let description = String::from("节点和上下文共同计算偏移");
    let graph = workflow!(
        workflow_id: { constructions += 1; workflow_id };
        title: { constructions += 1; workflow_title };
        context: { constructions += 1; OffsetContext(3) };
        nodes {
            #[title = { constructions += 1; title }]
            #[description = { constructions += 1; description }]
            source => { constructions += 1; OffsetNode(2) },
            destination => destination,
        }
        edges {
            source -> destination when(|output| *output > 0) |> |output| *output + 1,
            destination -> source when_error(|input, error| *input > 0 && error.is_retryable()),
        }
    )
    .unwrap();
    assert_eq!(constructions, 6);
    assert_eq!(graph.workflow_id(), "offset_workflow");
    assert_eq!(graph.title(), "偏移工作流");
    assert_eq!(graph.describe().workflow_id, graph.workflow_id());
    assert_eq!(graph.describe().title, graph.title());
    assert_eq!(graph.context().0, 3);
    let source = graph
        .describe()
        .nodes
        .into_iter()
        .find(|node| node.id == "source")
        .unwrap();
    assert_eq!(source.title, "带偏移的源节点");
    assert_eq!(source.description, "节点和上下文共同计算偏移");
    let mut context = OffsetContext(graph.context().0);
    let output = graph
        .execute("source", &mut context, serde_json::json!(5))
        .await
        .unwrap();
    assert_eq!(output, serde_json::json!(10));
    let successors = graph.plan_success("source", &output).unwrap();
    assert_eq!(successors[0].inputs, vec![serde_json::json!(11)]);
    let failure = graph
        .plan_failure("destination", &output, &NodeError::retryable("retry"))
        .unwrap();
    assert_eq!(failure[0].inputs, vec![output]);
}

#[test]
fn nested_subgraphs_preserve_qualified_endpoints_and_complete_join_upstream() {
    let graph = workflow! {
        workflow_id: "nested";
        title: "Nested";
        context: Ctx;
        nodes { source => PassThrough }
        subgraph batch {
            nodes { fetch => PassThrough }
            #[title = "信号分析"]
            subgraph analysis {
                nodes { signal => PassThrough, join => workflow_engine::JoinNode }
                edges { signal -> join }
            }
            edges { fetch -> analysis.signal, fetch -> analysis.join }
        }
        nodes { output => PassThrough }
        edges { source -> batch.fetch, batch.analysis.join -> output }
    }
    .unwrap();
    let description = graph.describe();
    assert_eq!(description.subgraphs.len(), 2);
    assert_eq!(description.subgraphs[1].id, "batch.analysis");
    assert_eq!(description.subgraphs[1].parent_id.as_deref(), Some("batch"));
    assert_eq!(
        graph.subgraph_path("batch.analysis.signal").unwrap(),
        ["batch", "batch.analysis"]
    );
    let error = graph
        .plan_success("batch.fetch", &serde_json::json!(1))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("JOIN batch.analysis.join requires a JSON object")
    );
    let plans = graph
        .plan_success("batch.fetch", &serde_json::json!({}))
        .unwrap();
    assert_eq!(plans.len(), 2);
    assert_eq!(
        graph.subgraph_path(plans[0].dst).unwrap(),
        ["batch", "batch.analysis"]
    );
    let join = &plans[1];
    assert_eq!(
        graph.predecessors_of(join.dst).unwrap(),
        ["batch.analysis.signal", "batch.fetch"]
    );
    assert_eq!(graph.subgraph_path("batch.fetch").unwrap(), ["batch"]);
    assert!(graph.subgraph_path("missing").is_none());
    assert!(graph.predecessors_of("missing").is_none());
    let exit = graph
        .plan_success("batch.analysis.join", &serde_json::json!({}))
        .unwrap();
    assert!(graph.subgraph_path(exit[0].dst).unwrap().is_empty());
}

#[test]
fn described_entry_checks_only_its_declared_scope_chain() {
    let mut description = workflow! {
        workflow_id: "entries";
        title: "Entries";
        context: Ctx;
        nodes { root => PassThrough }
        subgraph outer {
            subgraph inner { nodes { leaf => PassThrough } }
        }
        edges { root -> outer.inner.leaf }
    }
    .unwrap()
    .describe();
    let leaf = "outer.inner.leaf";
    let input = serde_json::Value::Null;

    description.edges[0].target = "unregistered".into();
    assert!(description.validate_subgraphs().is_err());
    assert_eq!(
        description.task(leaf, input.clone()).unwrap().subgraph_path,
        ["outer", "outer.inner"]
    );

    let duplicate = description
        .nodes
        .iter()
        .find(|node| node.id == leaf)
        .unwrap()
        .clone();
    description.nodes.push(duplicate);
    assert!(description.task(leaf, input.clone()).is_err());
    description.nodes.pop();
    description.subgraphs.push(description.subgraphs[0].clone());
    assert!(description.task(leaf, input.clone()).is_err());
    description.subgraphs.pop();
    description.subgraphs[0].parent_id = Some("outer.inner".into());
    assert!(description.task(leaf, input.clone()).is_err());
    description.subgraphs.remove(0);
    assert!(description.task(leaf, input.clone()).is_err());
    assert!(
        description
            .task("root", input.clone())
            .unwrap()
            .subgraph_path
            .is_empty()
    );
    assert!(matches!(
        description.task("unknown", input),
        Err(workflow_engine::EngineError::UnknownNode(_))
    ));
}

#[test]
fn interleaved_subgraph_declarations_preserve_construction_order() {
    let mut order = Vec::new();
    let graph = workflow! {
        workflow_id: "ordered";
        title: "Ordered";
        context: Ctx;
        nodes { before => { order.push(1); PassThrough } }
        subgraph inner {
            nodes { middle => { order.push(2); PassThrough } }
        }
        nodes { after => { order.push(3); PassThrough } }
        edges { before -> inner.middle, inner.middle -> after }
    }
    .unwrap();
    assert_eq!(order, [1, 2, 3]);
    assert_eq!(graph.describe().nodes.len(), 3);
}

#[test]
fn isolated_expansion_requires_an_explicit_subgraph_boundary() {
    let result = workflow! {
        workflow_id: "unbounded";
        title: "Unbounded";
        context: Ctx;
        nodes { source => Values, target => PassThrough }
        edges { source ->+ target }
    };
    assert!(result.err().unwrap().contains("must enter a new subgraph"));
}
