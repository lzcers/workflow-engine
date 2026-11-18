use std::future::Future;
use workflow_engine::{Graph, Node, NodeError, workflow};

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

/// `on_error fn`（无条件）/ `when_error(cond) |> fn`（条件）失败边 DSL 语法：都能展开并规划。
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
            a -> b |> |_: &serde_json::Value| vec![serde_json::json!(1)],
            b -> a on_error |_: &serde_json::Value, _: &NodeError| vec![serde_json::json!(2)],
            b -> c when_error(|_: &serde_json::Value, err: &NodeError| err.is_retryable()) |>
                |input: &serde_json::Value, _: &NodeError| vec![input.clone()],
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
            source -> destination when(|output| *output > 0) |> |output| vec![*output + 1],
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
