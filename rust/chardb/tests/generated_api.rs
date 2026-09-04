//! `chardb api rust` output must compile against this crate and keep the wire shape.
//! `test/cli/api_rust.test.ts` regenerates the fixture from the app it describes.

use serde_json::json;

#[path = "fixtures/generated_api.rs"]
mod generated;

use generated::{
    ListMessagesArgs, ListMessagesArgsKind, MessagesRow, PostMessageArgs, CLEAR_MESSAGES,
    LIST_MESSAGES, POST_MESSAGE,
};

#[test]
fn generated_handles_keep_references_and_wire_keys() {
    assert_eq!(LIST_MESSAGES.reference(), "src/queries.ts#listMessages");
    assert_eq!(POST_MESSAGE.reference(), "src/api.ts#postMessage");
    assert_eq!(CLEAR_MESSAGES.reference(), "src/api.ts#clearMessages");

    let args = ListMessagesArgs {
        organization_id: "org-1".to_owned(),
        limit: None,
        kind: Some(ListMessagesArgsKind::Pinned),
    };
    assert_eq!(
        serde_json::to_value(&args).unwrap(),
        json!({ "organizationId": "org-1", "kind": "pinned" })
    );

    let post = PostMessageArgs {
        id: "m1".to_owned(),
        organization_id: "org-1".to_owned(),
        body: "hi".to_owned(),
        r#type: None,
        tags: Some(vec!["a".to_owned()]),
    };
    assert_eq!(
        serde_json::to_value(&post).unwrap(),
        json!({ "id": "m1", "organizationId": "org-1", "body": "hi", "type": null, "tags": ["a"] })
    );

    let row: MessagesRow = serde_json::from_value(json!({
        "id": "m1",
        "organizationId": "org-1",
        "body": "hi",
        "createdAt": 7,
        "pinned": false,
        "score": null,
        "meta": { "a": 1 }
    }))
    .unwrap();
    assert_eq!(row.created_at, 7);
    assert_eq!(row.score, None);
    assert_eq!(row.meta, Some(json!({ "a": 1 })));
}
