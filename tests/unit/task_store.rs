use serde_json::json;

use crate::node::MergeMode;

use super::merge_join_inputs;

#[test]
fn join_inputs_set_and_append_by_slot_mode() {
    let acc = merge_join_inputs(
        None,
        &["image".to_owned()],
        "image",
        MergeMode::Append,
        &[json!({"prompt": "a"})],
    )
    .unwrap();
    assert_eq!(acc, json!({"image": [{"prompt": "a"}]}));

    let acc = merge_join_inputs(
        Some(acc),
        &["image".to_owned()],
        "image",
        MergeMode::Append,
        &[json!({"prompt": "b"})],
    )
    .unwrap();
    let acc = merge_join_inputs(
        Some(acc),
        &["image".to_owned()],
        "summary",
        MergeMode::Set,
        &[json!(42)],
    )
    .unwrap();
    assert_eq!(
        acc,
        json!({
            "image": [
                {"prompt": "a"},
                {"prompt": "b"},
            ],
            "summary": 42,
        })
    );
}
