use serde_json::json;

use super::merge_join_input;

#[test]
fn join_input_merges_same_key_into_array() {
    // 同名前驱多次贡献（多实例扇入）→ 合并为数组。
    let mut acc = json!({"image": {"prompt": "a"}});
    merge_join_input(&mut acc, &json!({"image": {"prompt": "b"}})).unwrap();
    assert_eq!(
        acc,
        json!({
            "image": [
                {"prompt": "a"},
                {"prompt": "b"},
            ]
        })
    );
}
