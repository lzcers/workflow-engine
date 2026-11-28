use serde::{Deserialize, Serialize};
use std::future::Future;
use workflow_engine::{
    Edge, Graph, GraphError, JoinInput, JoinNode, Node, NodeError, Trigger,
    graph::{EdgeMode, MergeMode, TaskType},
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
        subgraph analysis { nodes { item => PassThrough, batch_in => Values } }
        edges {
            source -> batch,
            source ->! analysis.batch_in,
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
    assert!(!description.edges[3].has_transform);
    assert!(description.edges[4].has_transform);
    assert!(description.edges[4].has_condition);
    assert!(!description.edges[0].has_condition);
    let modes: Vec<_> = description.edges.iter().map(|edge| edge.mode).collect();
    assert_eq!(
        modes,
        [
            EdgeMode::Direct,
            EdgeMode::IsolatedDirect,
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
            "isolated_direct",
            "shared_expand",
            "isolated_expand",
            "isolated_expand",
            "shared_expand",
            "isolated_expand"
        ])
    );
    let plans = graph.plan_success("source", &json!([1, 2])).unwrap();
    assert_eq!(plans.len(), 5);
    assert_eq!(plans[0].inputs, [json!([1, 2])]);
    assert_eq!(plans[0].dst.as_ref(), "batch");
    assert_eq!(plans[1].dst.as_ref(), "analysis.batch_in");
    assert_eq!(plans[1].dispatch, Dispatch::PerInputScope);
    assert_eq!(plans[1].inputs, [json!([1, 2])]);
    for plan in &plans[2..] {
        assert_eq!(plan.dst.as_ref(), "analysis.item");
    }
    assert_eq!(plans[2].dispatch, Dispatch::Shared);
    assert_eq!(plans[2].inputs, [json!(1), json!(2)]);
    assert_eq!(plans[3].dispatch, Dispatch::PerInputScope);
    assert_eq!(plans[3].inputs, plans[2].inputs);
    assert_eq!(plans[4].dispatch, Dispatch::PerInputScope);
    assert_eq!(plans[4].inputs, [json!(2), json!(1)]);

    let empty = graph.plan_success("source", &json!([])).unwrap();
    assert_eq!(empty.len(), 2);
    assert_eq!(empty[0].inputs, [json!([])]);
    assert_eq!(empty[1].inputs, [json!([])]);
    let singleton = graph.plan_success("source", &json!([1])).unwrap();
    assert_eq!(singleton.len(), 4);

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

#[derive(Serialize, Deserialize, JoinInput)]
struct SingleValueJoin {
    #[join(collect)]
    items: Vec<serde_json::Value>,
}

#[test]
fn isolated_arrows_reject_join_targets() {
    let graph = workflow! {
        workflow_id: "isolated-join";
        title: "Isolated JOIN";
        context: Ctx;
        nodes { source => Values }
        subgraph analysis { nodes { join => JoinNode::<SingleValueJoin>::new() } }
        edges { source ->+ analysis.join::items }
    };
    assert!(matches!(graph, Err(GraphError::InvalidEdge { .. })));
}

struct TriggeredNode(Option<Trigger>);

impl Node<Ctx> for TriggeredNode {
    type In = serde_json::Value;
    type Out = serde_json::Value;

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
fn triggers_are_discovered_and_orthogonal_to_incoming_edges() {
    let graph = workflow! {
        workflow_id: "event-description";
        title: "Event description";
        context: Ctx;
        nodes {
            z_tick => TriggeredNode(Some(cron_trigger())),
            process => PassThrough,
            #[title = "支付回调"]
            a_payment => TriggeredNode(Some(Trigger {
                kind: "payment.webhook".into(),
                config: serde_json::json!({"path": "/payments"}),
            })),
        }
        edges {
            z_tick -> process,
            a_payment -> process,
            // 外部触发元数据与上游入边完全正交：允许上游继续触发带 trigger 的节点。
            process -> a_payment on_error,
        }
    }
    .unwrap();

    let triggers = graph.triggers();
    assert_eq!(
        triggers.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ["a_payment", "z_tick"]
    );
    assert_eq!(triggers[0].1.kind, "payment.webhook");
    assert_eq!(triggers[1].1, cron_trigger());

    let description = graph.describe();
    assert_eq!(description.nodes[0].id, "a_payment");
    assert_eq!(description.nodes[0].title, "支付回调");
    assert_eq!(description.nodes[0].task_type, TaskType::Normal);
    assert_eq!(description.nodes[0].trigger.as_ref(), Some(&triggers[0].1));
    assert_eq!(description.nodes[1].id, "process");
    assert!(description.nodes[1].trigger.is_none());
}

#[test]
fn failure_edge_dsl_syntax_and_source_order() {
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

    let retry = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::retryable("boom"))
        .unwrap();
    assert_eq!(retry.len(), 2);
    let mut dsts: Vec<_> = retry.iter().map(|s| s.dst.as_ref()).collect();
    dsts.sort();
    assert_eq!(dsts, vec!["a", "c"]);
    let skip = g
        .plan_failure("b", &serde_json::json!(1), &NodeError::permanent("boom"))
        .unwrap();
    assert_eq!(skip.len(), 1);
    assert_eq!(skip[0].dst.as_ref(), "a");
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

#[derive(Serialize, Deserialize, JoinInput)]
struct NestedJoin {
    signal: serde_json::Value,
    fetch: serde_json::Value,
    optional_extra: Option<serde_json::Value>,
}

#[test]
fn nested_subgraphs_ui_groups_and_reusable_mount() {
    let graph = workflow! {
        workflow_id: "nested";
        title: "Nested";
        context: Ctx;
        nodes { source => PassThrough }
        #[description = "纯画布视觉分组，不产生 scopes 行"]
        group visual_stage {
            nodes { prep => PassThrough }
        }
        subgraph batch {
            nodes { fetch => PassThrough }
            #[title = "信号分析"]
            #[description = "隔离计算子图"]
            subgraph analysis {
                nodes { signal => PassThrough, join => JoinNode::<NestedJoin>::new() }
                edges { signal -> join::signal }
            }
            edges { fetch -> analysis.signal, fetch -> analysis.join::fetch }
        }
        nodes { output => PassThrough }
        edges {
            source -> visual_stage.prep,
            visual_stage.prep -> batch.fetch,
            batch.analysis.join -> output |> |joined| serde_json::to_value(joined).unwrap(),
        }
    }
    .unwrap();
    let description = graph.describe();
    assert_eq!(description.subgraphs.len(), 3);
    let visual = description
        .subgraphs
        .iter()
        .find(|s| s.id == "visual_stage")
        .unwrap();
    assert!(!visual.isolated);
    assert_eq!(visual.description, "纯画布视觉分组，不产生 scopes 行");
    assert!(graph.subgraph_path("visual_stage.prep").unwrap().is_empty());

    let analysis = description
        .subgraphs
        .iter()
        .find(|s| s.id == "batch.analysis")
        .unwrap();
    assert!(analysis.isolated);
    assert_eq!(analysis.parent_id.as_deref(), Some("batch"));
    assert_eq!(analysis.description, "隔离计算子图");
    assert_eq!(
        graph.subgraph_path("batch.analysis.signal").unwrap(),
        ["batch", "batch.analysis"]
    );
    let plans = graph
        .plan_success("batch.fetch", &serde_json::json!(1))
        .unwrap();
    assert_eq!(plans.len(), 2);
    assert_eq!(
        graph.subgraph_path(plans[0].dst.as_ref()).unwrap(),
        ["batch", "batch.analysis"]
    );
    let join = &plans[1];
    assert_eq!(
        join.dispatch,
        Dispatch::Join {
            key: "fetch",
            merge: MergeMode::Set
        }
    );
    assert_eq!(join.inputs, [serde_json::json!(1)]);
    assert_eq!(graph.subgraph_path("batch.fetch").unwrap(), ["batch"]);
    assert!(graph.subgraph_path("missing").is_none());

    // 命令式可复用子图挂载 + 条件边对称校验（多条命令式 when 边可共写同一必填 Set 槽位）
    let reusable = |sub: workflow_engine::graph::SubgraphBuilder<Ctx>| {
        let (sub, a) = sub.node_key("a", PassThrough);
        let (sub, b) = sub.node_key("b", PassThrough);
        sub.edge(Edge::from(&a).to(&b).direct())
    };
    let dynamic = Graph::builder("dynamic", "Dynamic", Ctx)
        .mount("s1", "Stage 1", reusable)
        .mount("s2", "Stage 2", reusable)
        .node("j", JoinNode::<NestedJoin>::new())
        .edge(
            Edge::success::<serde_json::Value>("s1.b")
                .to_slot("j", NestedJoin::slots().signal())
                .when(|v| v.is_number())
                .direct(),
        )
        .edge(
            Edge::success::<serde_json::Value>("s2.b")
                .to_slot("j", NestedJoin::slots().signal())
                .when(|v| v.is_string())
                .direct(),
        )
        .edge(
            Edge::success::<serde_json::Value>("s1.a")
                .to_slot("j", NestedJoin::slots().fetch())
                .direct(),
        )
        .build()
        .unwrap();
    assert_eq!(dynamic.subgraph_path("s1.a").unwrap(), ["s1"]);
    assert_eq!(dynamic.subgraph_path("s2.b").unwrap(), ["s2"]);
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
fn isolated_arrows_require_an_explicit_isolated_subgraph_boundary() {
    let result = workflow! {
        workflow_id: "unbounded";
        title: "Unbounded";
        context: Ctx;
        nodes { source => Values, target => PassThrough }
        edges { source ->+ target }
    };
    assert!(matches!(result, Err(GraphError::InvalidEdge { .. })));

    let into_group = workflow! {
        workflow_id: "into-group";
        title: "Into UI Group";
        context: Ctx;
        nodes { source => PassThrough }
        group ui { nodes { target => PassThrough } }
        edges { source ->! ui.target }
    };
    assert!(matches!(into_group, Err(GraphError::InvalidEdge { .. })));
}
