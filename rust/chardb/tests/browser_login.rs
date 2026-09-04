#![cfg(feature = "browser-login")]

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

use chardb_client::{browser_login::BrowserLogin, ErrorKind};
use oauth2::{url::Url, PkceCodeChallenge, PkceCodeVerifier};

fn start() -> BrowserLogin {
    BrowserLogin::start(
        "https://auth.example/authorize?client_id=cli&scope=openid",
        Duration::from_secs(5),
    )
    .unwrap()
}

fn params(login: &BrowserLogin) -> HashMap<String, String> {
    Url::parse(login.authorization_url())
        .unwrap()
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn address(p: &HashMap<String, String>) -> String {
    Url::parse(&p["redirect_uri"])
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0]
        .to_string()
}

fn request(address: &str, target: &str) -> String {
    raw_request(
        address,
        &format!("GET {target} HTTP/1.1\r\nHost: {address}\r\n\r\n"),
    )
}

fn raw_request(address: &str, request: &str) -> String {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    // Rejection may close a socket with unread request bytes.
    let _ = stream.read_to_string(&mut response);
    response
}

#[test]
fn binds_loopback_and_builds_fresh_pkce_requests() {
    let first = start();
    let second = start();
    let a = params(&first);
    let b = params(&second);
    assert!(a["redirect_uri"].starts_with("http://127.0.0.1:"));
    assert_eq!(a["response_type"], "code");
    assert_eq!(a["scope"], "openid");
    assert_eq!(a["code_challenge_method"], "S256");
    assert_ne!(a["state"], b["state"]);
    assert_ne!(a["code_challenge"], b["code_challenge"]);
    assert_ne!(a["redirect_uri"], b["redirect_uri"]);
    assert!(!format!("{first:?}").contains(&a["state"]));
    let addr = address(&a);
    drop(first);
    assert!(TcpStream::connect(addr).is_err());
}

#[test]
fn rejects_unsafe_or_ambiguous_authorization_configuration() {
    for url in [
        "http://auth.example/?client_id=x",
        "javascript:alert(1)",
        "https://user:secret@auth.example/?client_id=x",
        "https://auth.example/?client_id=x#fragment",
        "https://auth.example/",
        "https://auth.example/?client_id=",
        "https://auth.example/?client_id=x&client_id=y",
        "https://auth.example/?client_id=x&state=secret",
        "https://auth.example/?client_id=x&redirect_uri=http://evil.example",
        "https://auth.example/?client_id=x&response_type=token",
        "https://auth.example/?client_id=x&response_mode=fragment",
        "https://auth.example/?client_id=x&code_challenge=secret",
        "https://auth.example/?client_id=x&code_challenge_method=plain",
    ] {
        let error = BrowserLogin::start(url, Duration::from_secs(1)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(!error.to_string().contains("secret"));
    }
    assert!(BrowserLogin::start("https://auth.example/?client_id=x", Duration::ZERO).is_err());
    assert!(BrowserLogin::start("https://auth.example/?client_id=x", Duration::MAX).is_err());
}

#[test]
fn ignores_invalid_callbacks_then_accepts_one_code() {
    let login = start();
    let p = params(&login);
    let addr = address(&p);
    let worker = thread::spawn(move || login.wait());
    let state = &p["state"];
    for target in [
        "/favicon.ico".to_owned(),
        "/callback?code=secret".to_owned(),
        "/callback?state=wrong&code=secret".to_owned(),
        format!("/callback?state={state}&state={state}&code=secret"),
        format!("/callback?state={state}&code=a&code=b"),
        format!("/callback?state={state}&code=a&error=denied"),
        format!("/callback?state={state}&code="),
        format!("/callback?state={state}"),
    ] {
        let response = request(&addr, &target);
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(!response.contains("secret"));
    }
    for raw in [
        format!("POST /callback?state={state}&code=a HTTP/1.1\r\nHost: {addr}\r\n\r\n"),
        format!("GET /callback?state={state}&code=a HTTP/1.1\r\nHost: attacker.example\r\n\r\n"),
        format!(
            "GET /callback?state={state}&code=a HTTP/1.1\r\nHost: {addr}\r\nHost: {addr}\r\n\r\n"
        ),
        format!(
            "GET /callback?state={state}&code=a HTTP/1.1\r\nHost: {addr}\r\nX-Large: {}\r\n\r\n",
            "x".repeat(9000)
        ),
    ] {
        let response = raw_request(&addr, &raw);
        assert!(
            response.starts_with("HTTP/1.1 400")
                || (raw.len() > 8192 && !response.contains("200 OK")),
            "unexpected response: {response:?}"
        );
    }
    let response = request(
        &addr,
        &format!("/callback?state={state}&code=code%2Bwith%2Fescapes"),
    );
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("Cache-Control: no-store"));
    assert!(!response.contains("code+with/escapes"));
    let grant = worker.join().unwrap().unwrap();
    assert_eq!(grant.code.secret(), "code+with/escapes");
    assert_eq!(grant.redirect_uri.as_str(), p["redirect_uri"]);
    assert_eq!(
        PkceCodeChallenge::from_code_verifier_sha256(&grant.pkce_verifier).as_str(),
        p["code_challenge"]
    );
    let debug = format!("{grant:?}");
    assert!(!debug.contains(grant.code.secret()));
    assert!(!debug.contains(grant.pkce_verifier.secret()));
    assert!(TcpStream::connect(addr).is_err());
}

#[test]
fn denial_is_terminal_and_redacted() {
    let login = start();
    let p = params(&login);
    let addr = address(&p);
    let worker = thread::spawn(move || login.wait());
    let response = request(
        &addr,
        &format!(
            "/callback?state={}&error=access_denied&error_description=secret",
            p["state"]
        ),
    );
    let error = worker.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert!(!format!("{error:?}{error}").contains("secret"));
    assert!(!response.contains("secret"));
    assert!(TcpStream::connect(addr).is_err());
}

#[test]
fn idle_and_slow_clients_cannot_extend_the_deadline() {
    for slow in [false, true] {
        let login = BrowserLogin::start(
            "https://auth.example/?client_id=cli",
            Duration::from_millis(150),
        )
        .unwrap();
        let addr = address(&params(&login));
        let _stream = slow.then(|| {
            let mut stream = TcpStream::connect(&addr).unwrap();
            stream.write_all(b"GET /callback HTTP/1.1\r\n").unwrap();
            stream
        });
        let start = Instant::now();
        assert_eq!(login.wait().unwrap_err().kind(), ErrorKind::Timeout);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(TcpStream::connect(addr).is_err());
    }
}

#[test]
#[cfg(feature = "sync")]
fn callback_exchanges_pkce_and_supplies_jwt_to_client() {
    use chardb_client::{
        wire::{decode_up, Up},
        Client, ClientConfig,
    };
    use oauth2::{ureq, TokenResponse};
    use tungstenite::Message;

    let login = start();
    let p = params(&login);
    let addr = address(&p);
    let worker = thread::spawn(move || login.wait());
    request(
        &addr,
        &format!("/callback?state={}&code=single-use-code", p["state"]),
    );
    let grant = worker.join().unwrap().unwrap();
    let token_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let token_url = format!("http://{}/token", token_listener.local_addr().unwrap());
    let jwt = "e30.eyJzdWIiOiJ1c2VyLTEiLCJleHAiOjQxMDI0NDQ4MDB9.signature";
    let issuer = thread::spawn(move || {
        let (mut stream, _) = token_listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut bytes = Vec::new();
        let body = loop {
            let mut chunk = [0; 1024];
            let n = stream.read(&mut chunk).unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&chunk[..n]);
            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut req = httparse::Request::new(&mut headers);
            if let httparse::Status::Complete(offset) = req.parse(&bytes).unwrap() {
                let len: usize = std::str::from_utf8(
                    req.headers
                        .iter()
                        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                        .unwrap()
                        .value,
                )
                .unwrap()
                .parse()
                .unwrap();
                if bytes.len() >= offset + len {
                    assert_eq!(req.method, Some("POST"));
                    assert_eq!(req.path, Some("/token"));
                    break bytes[offset..offset + len].to_vec();
                }
            }
        };
        let fields: HashMap<_, _> = oauth2::url::form_urlencoded::parse(&body)
            .into_owned()
            .collect();
        assert_eq!(fields["grant_type"], "authorization_code");
        assert_eq!(fields["client_id"], "cli");
        assert_eq!(fields["code"], "single-use-code");
        assert_eq!(fields["redirect_uri"], p["redirect_uri"]);
        assert_eq!(
            PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(
                fields["code_verifier"].clone()
            ))
            .as_str(),
            p["code_challenge"]
        );
        let body = format!(r#"{{"access_token":"{jwt}","token_type":"Bearer","expires_in":3600}}"#);
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let http = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(3))
        .build();
    let tokens = grant.exchange("cli", &token_url, &http).unwrap();
    issuer.join().unwrap();
    assert_eq!(tokens.access_token().secret(), jwt);
    let ws = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/ws", ws.local_addr().unwrap());
    let gateway = thread::spawn(move || {
        let (stream, _) = ws.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut socket = tungstenite::accept(stream).unwrap();
        let Up::Hello {
            jwt: received,
            client_id,
            ..
        } = decode_up(socket.read().unwrap().to_text().unwrap()).unwrap()
        else {
            panic!("expected hello")
        };
        assert_eq!(received, jwt);
        socket.send(Message::Text(serde_json::json!({"t":"welcome", "protocolV":3, "baseCookie":format!("{client_id}:0"), "region":"test"}).to_string().into())).unwrap();
        let _ = socket.read();
    });
    let client = Client::connect(ClientConfig::with_token(
        endpoint,
        tokens.access_token().secret().clone(),
    ))
    .unwrap();
    client.close();
    gateway.join().unwrap();
}

#[test]
fn exchange_rejects_plaintext_and_redacts_provider_errors() {
    use chardb_client::browser_login::{AuthorizationCode, BrowserAuthorization, RedirectUrl};
    use oauth2::{http, HttpRequest, HttpResponse};
    let grant = || BrowserAuthorization {
        code: AuthorizationCode::new("secret-code".to_owned()),
        pkce_verifier: PkceCodeVerifier::new("secret-verifier".repeat(4)),
        redirect_uri: RedirectUrl::new("http://127.0.0.1:1234/callback".to_owned()).unwrap(),
    };
    let http = |_: HttpRequest| -> std::result::Result<HttpResponse, std::io::Error> {
        Ok(http::Response::builder()
            .status(400)
            .header("content-type", "application/json")
            .body(
                br#"{"error":"invalid_grant","error_description":"secret-code secret-verifier"}"#
                    .to_vec(),
            )
            .unwrap())
    };
    let error = grant()
        .exchange("cli", "http://auth.example/token", &http)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    let error = grant()
        .exchange("cli", "https://auth.example/token", &http)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert!(!format!("{error:?}{error}").contains("secret"));
}

#[test]
fn accepts_a_callback_delivered_in_separate_writes() {
    let login = start();
    let p = params(&login);
    let addr = address(&p);
    let worker = thread::spawn(move || login.wait());
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .write_all(format!("GET /callback?state={}&code=split HTTP/1.1\r\n", p["state"]).as_bytes())
        .unwrap();
    thread::sleep(Duration::from_millis(50));
    stream
        .write_all(format!("Host: {addr}\r\n\r\n").as_bytes())
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert_eq!(worker.join().unwrap().unwrap().code.secret(), "split");
}
