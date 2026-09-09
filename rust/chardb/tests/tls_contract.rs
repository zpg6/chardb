#![cfg(feature = "sync")]

use std::{net::TcpListener, sync::Arc, thread, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use chardb::{wire::decode_up, Client, ClientConfig};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    ClientConfig as RustlsClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use serde_json::json;
use tungstenite::{accept, Message};

const CERTIFICATE: &str = include_str!("fixtures/localhost-cert.der.b64");
const PRIVATE_KEY: &str = include_str!("fixtures/localhost-key.der.b64");

fn certificate() -> CertificateDer<'static> {
    CertificateDer::from(STANDARD.decode(CERTIFICATE.trim()).unwrap())
}

#[test]
fn custom_rustls_roots_complete_a_real_wss_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server_certificate = certificate();
    let server_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        STANDARD.decode(PRIVATE_KEY.trim()).unwrap(),
    ));
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![server_certificate.clone()], server_key)
        .unwrap();

    let server = thread::spawn(move || {
        let stream = listener.accept().unwrap().0;
        let tls = StreamOwned::new(
            ServerConnection::new(Arc::new(server_config)).unwrap(),
            stream,
        );
        let mut socket = accept(tls).unwrap();
        let Message::Text(hello) = socket.read().unwrap() else {
            panic!("expected text hello");
        };
        let _ = decode_up(&hello).unwrap();
        socket
            .send(Message::Text(
                json!({
                    "t": "welcome",
                    "protocolV": 3,
                    "baseCookie": "tls-client:0",
                    "region": "test"
                })
                .to_string()
                .into(),
            ))
            .unwrap();
    });

    let mut roots = RootCertStore::empty();
    roots.add(certificate()).unwrap();
    let tls_config = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client = Client::connect(
        ClientConfig::with_token(format!("wss://127.0.0.1:{}/ws", address.port()), "token")
            .client_id("tls-client")
            .connect_timeout(Duration::from_secs(2))
            .welcome_timeout(Duration::from_secs(2))
            .tls_config(Arc::new(tls_config)),
    )
    .unwrap();
    client.close();
    server.join().unwrap();
}
