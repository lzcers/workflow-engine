省略类型声明仍会拒绝输入输出不匹配的透传边：
```compile_fail
use workflow_engine::{Node, NodeError, workflow};
struct Number;
impl Node<()> for Number {
    type In = i32;
    type Out = i32;
    async fn execute(&self, _: &mut (), input: i32) -> Result<i32, NodeError> { Ok(input) }
}
struct Text;
impl Node<()> for Text {
    type In = String;
    type Out = String;
    async fn execute(&self, _: &mut (), input: String) -> Result<String, NodeError> { Ok(input) }
}
let _ = workflow!(
    workflow_id: "example";
    title: "示例工作流";
    context: ();
    nodes {
        #[title = "数字"]
        #[description = "输出数字"]
        a => Number,
        b => Text,
    }
    edges { a -> b, }
);
```
直接边的目标输入为数字时，转换闭包返回数字列表会编译失败：
```compile_fail
use workflow_engine::{Node, NodeError, workflow};
struct Number;
impl Node<()> for Number {
    type In = i32;
    type Out = i32;
    async fn execute(&self, _: &mut (), input: i32) -> Result<i32, NodeError> { Ok(input) }
}
let _ = workflow!(
    workflow_id: "example";
    title: "示例工作流";
    context: ();
    nodes {
        a => Number,
        #[description = "接收数字"]
        #[title = "目标"]
        b => Number,
    }
    edges { a -> b |> |_| vec![1_i32], }
);
```
同一属性不能重复声明：
```compile_fail
use workflow_engine::{JoinNode, workflow};
let _ = workflow!(
    workflow_id: "example";
    title: "示例工作流";
    context: ();
    nodes {
        #[title = "第一个标题"]
        #[title = "第二个标题"]
        join => JoinNode,
    }
    edges {}
);
```
未知属性会在编译期报错，不会被静默忽略：
```compile_fail
use workflow_engine::{JoinNode, workflow};
let _ = workflow!(
    workflow_id: "example";
    title: "示例工作流";
    context: ();
    nodes {
        #[titel = "拼错的标题属性"]
        join => JoinNode,
    }
    edges {}
);
```
