use serde_json::json;

use crate::node::MergeMode;

use super::merge_join_slot;

#[test]
fn join_slot_merging_handles_set_append_and_optional_append_defaults() {
    let opt = vec!["optional_tags".to_owned()];
    let acc = merge_join_slot(
        None,
        &opt,
        "image",
        MergeMode::Append,
        &[json!({"prompt": "a"})],
    )
    .unwrap();
    assert_eq!(acc, json!({"image": [{"prompt": "a"}], "optional_tags": []}));

    let acc = merge_join_slot(
        Some(acc),
        &opt,
        "image",
        MergeMode::Append,
        &[json!({"prompt": "b"})],
    )
    .unwrap();
    let acc = merge_join_slot(Some(acc), &opt, "summary", MergeMode::Set, &[json!(42)]).unwrap();
    assert_eq!(
        acc,
        json!({
            "image": [{"prompt": "a"}, {"prompt": "b"}],
            "summary": 42,
            "optional_tags": [],
        })
    );
}
