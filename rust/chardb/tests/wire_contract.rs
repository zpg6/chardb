use chardb::wire::{decode_down, decode_up, encode_up, CdbErrorCode, Down, RefetchReason};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    #[serde(rename = "protocolV")]
    protocol_v: u8,
    up: Vec<serde_json::Value>,
    down: Vec<serde_json::Value>,
}

fn corpus() -> Corpus {
    serde_json::from_str(include_str!("fixtures/wire_v3.json")).unwrap()
}

#[test]
fn shared_typescript_corpus_round_trips_in_rust() {
    let corpus = corpus();
    assert_eq!(corpus.protocol_v, 3);
    assert_eq!(corpus.up.len(), 7);
    assert_eq!(corpus.down.len(), 6);
    for expected in corpus.up {
        let raw = serde_json::to_string(&expected).unwrap();
        let message = decode_up(&raw).unwrap();
        let encoded = encode_up(&message).unwrap();
        let actual: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(actual, expected);
    }
    for message in corpus.down {
        decode_down(&serde_json::to_string(&message).unwrap()).unwrap();
    }
}

#[test]
fn closed_tags_fields_and_safe_ids_are_enforced() {
    assert!(decode_down(r#"{"t":"future"}"#).is_err());
    assert!(
        decode_down(r#"{"t":"snapshot","subId":9007199254740992,"cookie":"c","rows":[]}"#).is_err()
    );
    assert!(decode_down(
        r#"{"t":"welcome","protocolV":3,"baseCookie":"c","region":"x","extra":true}"#
    )
    .is_err());
}

#[test]
fn additive_values_follow_the_typescript_normalization_contract() {
    let error = decode_down(
        r#"{"t":"error","code":"CDB_FUTURE","retryable":true,"correlationId":"corr","docs":"future"}"#,
    )
    .unwrap();
    match error {
        Down::Error {
            code,
            retryable,
            docs,
            ..
        } => {
            assert_eq!(code, CdbErrorCode::Invariant);
            assert!(!retryable);
            assert_eq!(docs, CdbErrorCode::Invariant.docs_url());
        }
        _ => panic!("expected error"),
    }

    let refetch = decode_down(r#"{"t":"mustRefetch","subIds":[],"reason":"future"}"#).unwrap();
    assert!(matches!(
        refetch,
        Down::MustRefetch {
            reason: RefetchReason::Lagged,
            ..
        }
    ));
}
