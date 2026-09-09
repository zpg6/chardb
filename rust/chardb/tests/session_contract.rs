#![cfg(all(feature = "sync", feature = "async"))]

use std::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use chardb::{
    wire::{decode_up, RefetchReason, Up},
    AsyncClient, Client, ClientConfig, ConnectionState, ErrorKind, Mutation, Query,
    SubscriptionEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tungstenite::{
    accept_hdr,
    handshake::server::{Request, Response},
    Message, WebSocket,
};

#[derive(Debug, Deserialize, PartialEq)]
struct Row {
    #[serde(rename = "__key")]
    key: String,
    body: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct MutationAck {
    id: String,
}

#[derive(Serialize)]
struct Args<'a> {
    #[serde(rename = "organizationId")]
    organization_id: &'a str,
}

const MESSAGES: Query<Args<'static>, Row> = Query::new("queries.ts#messages");
const POST: Mutation<Args<'static>, MutationAck> = Mutation::new("mutations.ts#post");
const JSON_MESSAGES: Query<serde_json::Value, Row> = Query::new("queries.ts#messages");
const JSON_POST: Mutation<serde_json::Value, MutationAck> = Mutation::new("mutations.ts#post");

fn jwt(subject: &str, expiry: u64) -> String {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"sub":"{subject}","exp":{expiry}}}"#));
    format!("e30.{payload}.signature")
}

fn read_up(socket: &mut WebSocket<TcpStream>) -> Up {
    loop {
        match socket.read().unwrap() {
            Message::Text(text) => return decode_up(&text).unwrap(),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).unwrap(),
            other => panic!("unexpected client frame: {other:?}"),
        }
    }
}

fn send_json(socket: &mut WebSocket<TcpStream>, value: &serde_json::Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .unwrap();
}

#[allow(clippy::result_large_err)]
fn accept_client(listener: &TcpListener) -> (WebSocket<TcpStream>, String) {
    let stream = listener.accept().unwrap().0;
    let mut request_uri = None;
    let socket = accept_hdr(stream, |request: &Request, response: Response| {
        request_uri = Some(request.uri().to_string());
        Ok(response)
    })
    .unwrap();
    let uri = request_uri.unwrap();
    let client_id = uri
        .split('?')
        .nth(1)
        .and_then(|query| {
            query
                .split('&')
                .find_map(|pair| pair.strip_prefix("clientId="))
        })
        .unwrap()
        .to_owned();
    (socket, client_id)
}

fn spawn_contract_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut first, client_id) = accept_client(&listener);
        match read_up(&mut first) {
            Up::Hello {
                protocol_v,
                client_id: hello_id,
                resume_from_cookie,
                ..
            } => {
                assert_eq!(protocol_v, 3);
                assert_eq!(hello_id, client_id);
                assert_eq!(resume_from_cookie, None);
            }
            other => panic!("expected hello, got {other:?}"),
        }
        send_json(
            &mut first,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        let sub_id = match read_up(&mut first) {
            Up::Subscribe {
                sub_id,
                r#ref,
                args,
                ..
            } => {
                assert_eq!(r#ref, "queries.ts#messages");
                assert_eq!(args, json!({"organizationId":"org-1"}));
                sub_id
            }
            other => panic!("expected subscription, got {other:?}"),
        };
        let snapshot_cookie = format!("{client_id}:1");
        send_json(
            &mut first,
            &json!({"t":"snapshot","subId":sub_id.get(),"cookie":snapshot_cookie,"rows":[{"__key":"message-1","body":"before"}]}),
        );
        assert!(matches!(read_up(&mut first), Up::Ack { cookie } if cookie == snapshot_cookie));
        let mutation_id = match read_up(&mut first) {
            Up::Mutate {
                mutation_id,
                r#ref,
                args,
            } => {
                assert_eq!(r#ref, "mutations.ts#post");
                assert_eq!(args, json!({"organizationId":"org-1"}));
                mutation_id
            }
            other => panic!("expected mutation, got {other:?}"),
        };
        assert_eq!(mutation_id, "stable-mutation-id");
        first.close(None).unwrap();

        let (mut second, reconnect_id) = accept_client(&listener);
        assert_eq!(reconnect_id, client_id);
        match read_up(&mut second) {
            Up::Hello {
                client_id: hello_id,
                resume_from_cookie,
                ..
            } => {
                assert_eq!(hello_id, client_id);
                assert_eq!(
                    resume_from_cookie.as_deref(),
                    Some(snapshot_cookie.as_str())
                );
            }
            other => panic!("expected reconnect hello, got {other:?}"),
        }
        send_json(
            &mut second,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"resumedFromCookie":snapshot_cookie,"region":"test"}),
        );
        assert!(
            matches!(read_up(&mut second), Up::Subscribe { sub_id: replayed, .. } if replayed == sub_id)
        );
        assert!(
            matches!(read_up(&mut second), Up::Mutate { mutation_id: replayed, .. } if replayed == mutation_id)
        );
        send_json(
            &mut second,
            &json!({
                "t":"poke",
                "cookie":format!("{client_id}:2"),
                "patches":[{"op":"edit","subId":sub_id.get(),"rowKey":"message-1","row":{"body":"after"}}],
                "mutResults":[{"mutId":mutation_id,"ok":true,"result":{"id":"message-1"},"cookie":format!("{client_id}:2")}]
            }),
        );
        assert!(matches!(read_up(&mut second), Up::UpdateAuth { .. }));
        send_json(
            &mut second,
            &json!({"t":"mustRefetch","subIds":[],"reason":"authChanged"}),
        );
        let _ = second.close(None);
    });
    (endpoint, server)
}

fn config(endpoint: String) -> ClientConfig {
    let next = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    ClientConfig::new(endpoint, move || {
        let offset = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, std::convert::Infallible>(jwt("user-1", 4_102_444_800 + offset))
    })
}

#[test]
fn blocking_client_matches_reconnect_mutation_and_auth_contract() {
    let (endpoint, server) = spawn_contract_server();
    let client = Client::connect(config(endpoint)).unwrap();
    assert_eq!(client.state(), ConnectionState::Open);
    let mut subscription = client
        .subscribe(
            MESSAGES,
            &Args {
                organization_id: "org-1",
            },
        )
        .unwrap();
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Snapshot { rows } if rows == vec![Row { key: "message-1".into(), body: "before".into() }]
    ));
    let result: MutationAck = client
        .mutate_with_id(
            POST,
            &Args {
                organization_id: "org-1",
            },
            "stable-mutation-id",
        )
        .unwrap();
    assert_eq!(
        result,
        MutationAck {
            id: "message-1".into()
        }
    );
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Update { rows } if rows == vec![Row { key: "message-1".into(), body: "after".into() }]
    ));
    client.refresh_auth().unwrap();
    client.close();
    server.join().unwrap();
}

#[test]
fn async_facade_runs_the_identical_session_contract_without_tokio() {
    let (endpoint, server) = spawn_contract_server();
    futures_lite::future::block_on(async move {
        let client = AsyncClient::connect(config(endpoint)).await.unwrap();
        let mut subscription = client
            .subscribe(
                MESSAGES,
                &Args {
                    organization_id: "org-1",
                },
            )
            .unwrap();
        assert!(matches!(
            subscription.recv().await.unwrap(),
            SubscriptionEvent::Snapshot { .. }
        ));
        let result: MutationAck = client
            .mutate_with_id(
                POST,
                &Args {
                    organization_id: "org-1",
                },
                "stable-mutation-id",
            )
            .await
            .unwrap();
        assert_eq!(result.id, "message-1");
        assert!(matches!(
            subscription.recv().await.unwrap(),
            SubscriptionEvent::Update { .. }
        ));
        client.refresh_auth().await.unwrap();
        client.close();
    });
    server.join().unwrap();
}

#[test]
fn connect_failure_is_bounded_and_reported() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let started = std::time::Instant::now();
    let Err(error) = Client::connect(
        ClientConfig::with_token(format!("ws://{address}/ws"), "token")
            .connect_timeout(std::time::Duration::from_millis(200)),
    ) else {
        panic!("connection unexpectedly succeeded");
    };
    assert_eq!(error.kind(), chardb::ErrorKind::Transport);
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn welcome_timeout_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut socket, _) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        thread::sleep(Duration::from_millis(200));
    });
    let started = Instant::now();
    let Err(error) = Client::connect(config(endpoint).welcome_timeout(Duration::from_millis(50)))
    else {
        panic!("connection unexpectedly received a welcome");
    };
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}

#[test]
fn dispatched_mutation_timeout_reports_unknown_outcome_and_id() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut socket, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        send_json(
            &mut socket,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        assert!(matches!(
            read_up(&mut socket),
            Up::Mutate { mutation_id, .. } if mutation_id == "timeout-id"
        ));
        thread::sleep(Duration::from_millis(200));
    });
    let client =
        Client::connect(config(endpoint).mutation_timeout(Duration::from_millis(50))).unwrap();
    let error = client
        .mutate_with_id(JSON_POST, &json!({}), "timeout-id")
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::MutationOutcomeUnknown);
    assert_eq!(error.mutation_id(), Some("timeout-id"));
    client.close();
    server.join().unwrap();
}

#[test]
fn auth_refresh_acknowledgement_timeout_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut socket, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        send_json(
            &mut socket,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        assert!(matches!(read_up(&mut socket), Up::UpdateAuth { .. }));
        thread::sleep(Duration::from_millis(200));
    });
    let client =
        Client::connect(config(endpoint).auth_refresh_timeout(Duration::from_millis(50))).unwrap();
    let started = Instant::now();
    let error = client.refresh_auth().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert!(started.elapsed() < Duration::from_secs(1));
    server.join().unwrap();
}

#[test]
fn duplicate_pending_mutation_id_never_replaces_the_first_caller() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut socket, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        send_json(
            &mut socket,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        let mutation_id = match read_up(&mut socket) {
            Up::Mutate {
                mutation_id,
                r#ref,
                args,
            } => {
                assert_eq!(r#ref, "mutations.ts#post");
                assert_eq!(args, json!({"body":"first"}));
                mutation_id
            }
            other => panic!("expected mutation, got {other:?}"),
        };
        seen_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(100));
        send_json(
            &mut socket,
            &json!({
                "t":"poke",
                "cookie":format!("{client_id}:1"),
                "patches":[],
                "mutResults":[{"mutId":mutation_id,"ok":true,"result":{"id":"first-result"},"cookie":format!("{client_id}:1")}]
            }),
        );
        thread::sleep(Duration::from_millis(100));
    });
    let client =
        Client::connect(config(endpoint).mutation_timeout(Duration::from_secs(2))).unwrap();
    let first_client = client.clone();
    let first = thread::spawn(move || {
        first_client.mutate_with_id(JSON_POST, &json!({"body":"first"}), "shared-id")
    });
    seen_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second = client
        .mutate_with_id(JSON_POST, &json!({"body":"second"}), "shared-id")
        .unwrap_err();
    assert_eq!(second.kind(), ErrorKind::Configuration);
    assert_eq!(first.join().unwrap().unwrap().id, "first-result");
    client.close();
    server.join().unwrap();
}

#[test]
fn retryable_session_error_reconnects_and_nonretryable_error_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (reconnected_tx, reconnected_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut first, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut first), Up::Hello { .. }));
        send_json(
            &mut first,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        send_json(
            &mut first,
            &json!({"t":"error","code":"CDB_CATALOG_UNAVAILABLE","retryable":true,"correlationId":"retry","docs":"https://chardb.dev/errors/cdb_catalog_unavailable"}),
        );
        let (mut second, reconnect_id) = accept_client(&listener);
        assert_eq!(reconnect_id, client_id);
        assert!(matches!(read_up(&mut second), Up::Hello { .. }));
        send_json(
            &mut second,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        reconnected_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(100));
        send_json(
            &mut second,
            &json!({"t":"error","code":"CDB_INVARIANT","retryable":false,"correlationId":"terminal","docs":"https://chardb.dev/errors/cdb_invariant"}),
        );
    });
    let client = Client::connect(config(endpoint)).unwrap();
    reconnected_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while client.state() != ConnectionState::Closed && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(client.state(), ConnectionState::Closed);
    server.join().unwrap();
}

#[test]
fn retryable_subscription_error_resubscribes_and_delete_row_is_accepted() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut socket, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        send_json(
            &mut socket,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        let sub_id = match read_up(&mut socket) {
            Up::Subscribe { sub_id, .. } => sub_id,
            other => panic!("expected subscription, got {other:?}"),
        };
        send_json(
            &mut socket,
            &json!({"t":"snapshot","subId":sub_id.get(),"cookie":format!("{client_id}:1"),"rows":[{"__key":"row-1","body":"before"}]}),
        );
        assert!(matches!(read_up(&mut socket), Up::Ack { .. }));
        send_json(
            &mut socket,
            &json!({"t":"error","code":"CDB_SHARD_UNAVAILABLE","subId":sub_id.get(),"retryable":true,"correlationId":"retry-sub","docs":"https://chardb.dev/errors/cdb_shard_unavailable"}),
        );
        assert!(
            matches!(read_up(&mut socket), Up::Subscribe { sub_id: retry, .. } if retry == sub_id)
        );
        send_json(
            &mut socket,
            &json!({"t":"snapshot","subId":sub_id.get(),"cookie":format!("{client_id}:2"),"rows":[{"__key":"row-1","body":"after-retry"}]}),
        );
        assert!(matches!(read_up(&mut socket), Up::Ack { .. }));
        send_json(
            &mut socket,
            &json!({"t":"poke","cookie":format!("{client_id}:3"),"patches":[{"op":"del","subId":sub_id.get(),"rowKey":"row-1","row":{"__key":"wrong","body":"ignored"}}]}),
        );
        done_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(100));
    });
    let client = Client::connect(config(endpoint)).unwrap();
    let mut subscription = client.subscribe(JSON_MESSAGES, &json!({})).unwrap();
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Snapshot { rows } if rows[0].body == "before"
    ));
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Retrying { error, retry_in }
            if error.is_retryable() && retry_in == Duration::from_millis(100)
    ));
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Snapshot { rows } if rows[0].body == "after-retry"
    ));
    assert!(matches!(
        subscription.recv().unwrap(),
        SubscriptionEvent::Update { rows } if rows.is_empty()
    ));
    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    client.close();
    server.join().unwrap();
}

#[test]
#[allow(clippy::too_many_lines)]
fn auth_changed_waits_for_every_named_subscription_snapshot_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (advance_tx, advance_rx) = mpsc::sync_channel(2);
    let server = thread::spawn(move || {
        let (mut socket, client_id) = accept_client(&listener);
        assert!(matches!(read_up(&mut socket), Up::Hello { .. }));
        send_json(
            &mut socket,
            &json!({"t":"welcome","protocolV":3,"baseCookie":format!("{client_id}:0"),"region":"test"}),
        );
        let first = match read_up(&mut socket) {
            Up::Subscribe { sub_id, .. } => sub_id,
            other => panic!("expected subscription, got {other:?}"),
        };
        let second = match read_up(&mut socket) {
            Up::Subscribe { sub_id, .. } => sub_id,
            other => panic!("expected subscription, got {other:?}"),
        };
        for (id, suffix) in [(first, 1), (second, 2)] {
            send_json(
                &mut socket,
                &json!({"t":"snapshot","subId":id.get(),"cookie":format!("{client_id}:{suffix}"),"rows":[]}),
            );
            assert!(matches!(read_up(&mut socket), Up::Ack { .. }));
        }
        assert!(matches!(read_up(&mut socket), Up::UpdateAuth { .. }));
        send_json(
            &mut socket,
            &json!({"t":"mustRefetch","subIds":[second.get(),first.get(),second.get()],"reason":"authChanged"}),
        );
        let mut replayed = Vec::new();
        replayed.push(match read_up(&mut socket) {
            Up::Subscribe { sub_id, .. } => sub_id,
            other => panic!("expected subscription replay, got {other:?}"),
        });
        replayed.push(match read_up(&mut socket) {
            Up::Subscribe { sub_id, .. } => sub_id,
            other => panic!("expected subscription replay, got {other:?}"),
        });
        replayed.sort_by_key(|id| id.get());
        assert_eq!(replayed, vec![first, second]);
        ready_tx.send(()).unwrap();
        advance_rx.recv().unwrap();
        send_json(
            &mut socket,
            &json!({"t":"snapshot","subId":first.get(),"cookie":format!("{client_id}:3"),"rows":[]}),
        );
        assert!(matches!(read_up(&mut socket), Up::Ack { .. }));
        advance_rx.recv().unwrap();
        send_json(
            &mut socket,
            &json!({"t":"snapshot","subId":second.get(),"cookie":format!("{client_id}:4"),"rows":[]}),
        );
        assert!(matches!(read_up(&mut socket), Up::Ack { .. }));
        thread::sleep(Duration::from_millis(100));
    });
    let client =
        Client::connect(config(endpoint).auth_refresh_timeout(Duration::from_secs(2))).unwrap();
    let mut first = client
        .subscribe(JSON_MESSAGES, &json!({"which":1}))
        .unwrap();
    let mut second = client
        .subscribe(JSON_MESSAGES, &json!({"which":2}))
        .unwrap();
    assert!(matches!(
        first.recv().unwrap(),
        SubscriptionEvent::Snapshot { .. }
    ));
    assert!(matches!(
        second.recv().unwrap(),
        SubscriptionEvent::Snapshot { .. }
    ));
    let refresh_client = client.clone();
    let (refresh_tx, refresh_rx) = mpsc::sync_channel(1);
    thread::spawn(move || refresh_tx.send(refresh_client.refresh_auth()).unwrap());
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        first.recv().unwrap(),
        SubscriptionEvent::Refetching {
            reason: RefetchReason::AuthChanged
        }
    ));
    assert!(matches!(
        second.recv().unwrap(),
        SubscriptionEvent::Refetching {
            reason: RefetchReason::AuthChanged
        }
    ));
    assert!(refresh_rx.recv_timeout(Duration::from_millis(50)).is_err());
    advance_tx.send(()).unwrap();
    assert!(matches!(
        first.recv().unwrap(),
        SubscriptionEvent::Snapshot { .. }
    ));
    assert!(refresh_rx.recv_timeout(Duration::from_millis(50)).is_err());
    advance_tx.send(()).unwrap();
    assert!(matches!(
        second.recv().unwrap(),
        SubscriptionEvent::Snapshot { .. }
    ));
    refresh_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    client.close();
    server.join().unwrap();
}
