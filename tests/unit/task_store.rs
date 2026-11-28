use std::collections::HashMap;

use serde_json::json;

use crate::node::MergeMode;

use super::{materialize_join_input, record_join_slot};

#[test]
fn join_slot_recording_and_append_materialization() {
    let acc = record_join_slot(None, "image", MergeMode::Append, &[json!({"prompt": "a"})])
        .unwrap();
    assert_eq!(acc, json!({"image": []}));

    let acc = record_join_slot(Some(acc), "summary", MergeMode::Set, &[json!(42)]).unwrap();
    assert_eq!(acc, json!({"image": [], "summary": 42}));

    let mut contribs = HashMap::new();
    contribs.insert(
        "image".to_owned(),
        vec![json!({"prompt": "a"}), json!({"prompt": "b"})],
    );
    let final_input = materialize_join_input(
        acc,
        &["image".to_owned(), "optional_tags".to_owned()],
        contribs,
    )
    .unwrap();
    assert_eq!(
        final_input,
        json!({
            "image": [{"prompt": "a"}, {"prompt": "b"}],
            "summary": 42,
            "optional_tags": [],
        })
    );
}
