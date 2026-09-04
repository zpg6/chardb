# `CharDB` Rust client

`chardb-client` is the native Rust client for `CharDB` protocol version 3.
Blocking and async callers use one session worker, so reconnects, deadlines,
subscription state, and mutation replay have the same implementation in both
APIs. Repository CI is configured to test native builds on Windows, macOS, and
Linux.

Application calls use typed `Query<Arguments, Row>` and
`Mutation<Arguments, Output>` handles. The raw TypeScript reference lives in
one API declaration instead of every call site. Rust checks the operation kind
and binds the argument and result types at compile time. The client serializes
arguments and validates and decodes server output at runtime. A handle is one
`&'static str`, has no allocation, and writes that string unchanged to protocol
v3's `ref` field.

## Install

This repository includes the crate at `rust/chardb`. It is not public on crates.io yet.

The default build includes the blocking client, the runtime-neutral async
client, and Rustls with Mozilla `WebPKI` roots.

Available features:

- `sync` exports `Client` and `Subscription`.
- `async` exports `AsyncClient` and `AsyncSubscription`. It does not require
  Tokio or another executor.
- `browser-login` adds browser authorization with a temporary loopback listener
  and S256 PKCE. It is opt-in and does not select an HTTP client or executor.
- `introspection` adds JSON Schema 2020-12 descriptions for application
  argument, mutation-result, and subscription-row types through `schemars`.
- `rustls-webpki-roots` uses the bundled public root set and is the default.
- `rustls-native-roots` reads the operating system root store. Use it for
  enterprise or private roots installed on the machine.
- `client` is the shared transport engine. Protocol-only consumers can disable
  default features and use `chardb_client::wire` without networking code.

The crate's minimum supported Rust version is 1.85.

For an application-managed private CA, pinned verifier, or client certificate,
build a `rustls::ClientConfig`, wrap it in `Arc`, and pass it through
`ClientConfig::tls_config`. JWTs are never put in HTTP headers or URLs.

One logical client owns one network thread and one WebSocket. Clones are cheap
`Arc` handles. The socket uses `TCP_NODELAY`, command and event queues are
bounded where growth would follow user traffic, and JSON limit checks count
serialized bytes without allocating a second copy of the data.

### Optional type introspection

Typed Serde results are always available. Enable `introspection` when a tool,
plugin host, dynamic UI, or schema registry also needs to inspect those types at
runtime.

```rust
# #[cfg(feature = "introspection")]
# fn introspection_example() -> Result<(), Box<dyn std::error::Error>> {
use chardb_client::{Query, introspection::{operation_schema, JsonSchema}};

#[derive(JsonSchema)]
struct ListArgs {
    organization_id: String,
}

#[derive(JsonSchema)]
struct Message {
    id: String,
    body: String,
}

const LIST_MESSAGES: Query<ListArgs, Message> =
    Query::new("messages#list");

let contract = operation_schema(LIST_MESSAGES);
let json = serde_json::to_value(contract)?;
# let _ = json;
# Ok(())
# }
```

This is application type metadata. `CharDB` still validates authorization,
routing, and the registered server handle independently.

## Blocking client

The endpoint is the Worker's WebSocket path without a `clientId` query
parameter. The client generates one stable ID, adds it to the URL, and repeats
the same ID in `hello`.

```rust,no_run
# #[cfg(feature = "sync")]
# mod sync_example {
use std::time::Duration;
use chardb_client::{Client, ClientConfig, Mutation, Query, SubscriptionEvent};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ListArgs {
    #[serde(rename = "organizationId")]
    organization_id: String,
}

#[derive(Serialize)]
struct PostArgs {
    #[serde(rename = "organizationId")]
    organization_id: String,
    body: String,
}

#[derive(Deserialize)]
struct Message {
    id: String,
    body: String,
}

#[derive(Deserialize)]
struct Posted {
    id: String,
}

const LIST_MESSAGES: Query<ListArgs, Message> =
    Query::new("messages#list");
const POST_MESSAGE: Mutation<PostArgs, Posted> =
    Mutation::new("messages#create");

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let jwt = std::env::var("CHARDB_JWT")?;
let client = Client::connect(
    ClientConfig::with_token("wss://example.com/ws", jwt)
        .connect_timeout(Duration::from_secs(10))
        .mutation_timeout(Duration::from_secs(60)),
)?;

let args = ListArgs { organization_id: "org-1".to_owned() };
let mut messages = client.subscribe(LIST_MESSAGES, &args)?;

match messages.recv()? {
    SubscriptionEvent::Snapshot { rows } => println!("{} rows", rows.len()),
    SubscriptionEvent::Error(error) => return Err(error.into()),
    _ => {}
}

let posted = client.mutate(POST_MESSAGE, &PostArgs {
    organization_id: "org-1".to_owned(),
    body: "hello".to_owned(),
})?;
println!("posted {}", posted.id);
client.close();
# Ok(())
# }
# }
```

`Client::connect` waits for the authenticated `welcome`, not merely the HTTP
upgrade. No protected operation crosses the socket before that message.

## Async client

The async API uses the same methods and event types. Its futures use channel
wakers and work with any executor.

```rust,no_run
# #[cfg(feature = "async")]
# mod async_example {
use chardb_client::{AsyncClient, ClientConfig, Mutation, Query, SubscriptionEvent};
use serde_json::json;

const LIST_MESSAGES: Query<serde_json::Value, serde_json::Value> =
    Query::new("messages#list");
const POST_MESSAGE: Mutation<serde_json::Value, serde_json::Value> =
    Mutation::new("messages#create");

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let jwt = std::env::var("CHARDB_JWT")?;
let client = AsyncClient::connect(ClientConfig::with_token(
    "wss://example.com/ws",
    jwt,
)).await?;

let mut rows = client.subscribe(
    LIST_MESSAGES,
    &json!({ "organizationId": "org-1" }),
)?;

if let SubscriptionEvent::Snapshot { rows } = rows.recv().await? {
    println!("{} rows", rows.len());
}

let result = client.mutate(
    POST_MESSAGE,
    &json!({ "organizationId": "org-1", "body": "hello" }),
).await?;
# let _ = result;
# Ok(())
# }
# }
```

## Operation handles

Keep handles next to the Rust request and response types they bind. A small
application can use one `api` module:

```rust
use chardb_client::{Mutation, Query};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct ListMessages {
    #[serde(rename = "organizationId")]
    pub organization_id: String,
}

#[derive(Deserialize)]
pub struct Message {
    pub id: String,
    pub body: String,
}

#[derive(Serialize)]
pub struct PostMessage {
    #[serde(rename = "organizationId")]
    pub organization_id: String,
    pub body: String,
}

#[derive(Deserialize)]
pub struct Posted {
    pub id: String,
}

pub const LIST_MESSAGES: Query<ListMessages, Message> =
    Query::new("messages#list");
pub const POST_MESSAGE: Mutation<PostMessage, Posted> =
    Mutation::new("messages#create");
```

`Query::new` and `Mutation::new` apply the wire codec's reference check. Since
handles are normally constants, a missing `#` fails during compilation. The
client method fixes the result type from the handle, so callers do not need a
turbofish or a result annotation. A query handle cannot be passed to `mutate`,
and a mutation handle cannot be passed to `subscribe`.

To migrate from the string API, move each existing reference into one typed
constant, then replace the string argument at each call site:

```rust,ignore
// Before
let rows = client.subscribe::<_, Message>("messages#list", &args)?;

// After
const LIST_MESSAGES: Query<ListArgs, Message> =
    Query::new("messages#list");
let rows = client.subscribe(LIST_MESSAGES, &args)?;
```

The protocol types in `chardb_client::wire` still expose `ref` as a string.
That module represents protocol v3 directly and is the escape hatch for tools
that build messages from a dynamic registry.

Dropping an async mutation future before the worker sends it prevents the send.
Dropping it after a send cannot cancel server execution because protocol v3 has
no mutation-cancel message. The worker keeps the mutation identity until it
settles or reaches its deadline.

## Reconnect and mutation identity

After a network loss the client waits 250 ms, doubles the delay after each
failed attempt, and caps it at 10 seconds. A valid `welcome` resets the delay.
It keeps the client ID, active subscriptions, last cookie, and each unsettled
mutation.

Mutation replay always reuses the exact `(mutId, ref, args)` tuple. The server
deduplicates that tuple. The client never retries a settled server error, even
when its code is marked retryable, and never creates a replacement mutation ID
after an ambiguous send.

The default mutation deadline is 60 seconds and includes reconnect time. If it
expires, the error kind is `MutationOutcomeUnknown`, the code is
`CDB_MUTATION_OUTCOME_UNKNOWN`, and `Error::mutation_id()` returns the ID needed
for reconciliation. Use `mutate_with_id` when the application must persist the
ID before dispatch. `CharDB` currently retains mutation replay records for 24
hours, so a mutation ID is not a permanent idempotency key.

Only one unsettled operation may own a mutation ID in a client session. A
second concurrent `mutate_with_id` call with the same ID fails locally without
replacing or changing the first call.

Cookie-backed rows stay visible for at most 30 seconds after the first
disconnect. If the server does not replay a subscription in that window, the
client clears those rows, emits `SubscriptionEvent::Refetching`, drops the stale
cookie when safe, and asks for a fresh snapshot.

## Authentication

`ClientConfig::new` accepts a synchronous token provider. It runs on the
dedicated network thread, never on an async executor thread. Keep the provider
bounded, and return an owned error message without including the token.

The client decodes `sub` and `exp` only to schedule refresh 60 seconds before
expiry. It does not trust those claims. The Gateway verifies every token. A
refresh must keep the same subject, extend the expiry, and receive the
protocol's `mustRefetch` acknowledgment with reason `authChanged`. The refresh
acknowledgment has its own 10-second default deadline. If that acknowledgment
names subscriptions to refetch, `refresh_auth` completes after each replacement
snapshot has been accepted and acknowledged. A terminal failure of any named
subscription fails the refresh.

Call `refresh_auth` to rotate early. A principal change requires a new client.
`ClientConfig` and client errors never include JWT text in `Debug` or `Display`.

### Browser login for CLI applications

Enable `browser-login` for an interactive native CLI. Authentication remains
required with or without this feature. Services can supply JWTs through the
existing token provider without installing browser-launch dependencies.

`BrowserLogin::start` binds `127.0.0.1` on an ephemeral port, generates state and
an S256 PKCE challenge, and adds the authorization-code parameters to your URL.
`open_browser` launches the system browser. If launching fails, display
`authorization_url()` so the user can open it on the same computer.
`wait` consumes the attempt and closes the listener on success or failure.
Dropping an unused attempt also closes it. The timeout includes browser time.
These methods are blocking; async applications should run them on a blocking
thread. A browser on another computer cannot reach the CLI's loopback listener.

```rust,no_run
# #[cfg(feature = "browser-login")]
# fn login() -> chardb_client::Result<()> {
use chardb_client::browser_login::BrowserLogin;
use std::time::Duration;

let login = BrowserLogin::start(
    "https://auth.example/authorize?client_id=my-cli&scope=openid",
    Duration::from_secs(180),
)?;
if login.open_browser().is_err() {
    eprintln!("Open on this computer: {}", login.authorization_url());
}
let authorization = login.wait()?;
// authorization.exchange(client_id, token_url, &http_client) sends the code,
// redirect URI, and PKCE verifier using an oauth2::SyncHttpClient.
# let _ = authorization;
# Ok(())
# }
```

The auth server must support public clients, S256 PKCE, and the redirect URI
`http://127.0.0.1:{port}/callback` with variable ports. It must issue short-lived,
one-use codes bound to the client, redirect URI, and PKCE challenge. Configure
the exchange HTTP client with a timeout and redirect following disabled.
The SDK rejects non-HTTPS auth endpoints except loopback IPs for local tests.

A Better Auth sign-in page plus its `jwt()` plugin does **not** provide this
authorization-code endpoint. The application must configure a compatible
provider or implement that server contract. The callback returns a code, never
a browser session or JWT. `BrowserAuthorization::exchange` returns the provider's
token response. Pass its access token to `ClientConfig::with_token` only when
it is a JWT with the issuer, audience, and claims your Gateway accepts. Otherwise,
the application must exchange it for a `CharDB` JWT first.

`examples/browser_login.rs` shows the full flow using a test-only `ureq`
dependency. Run it with `cargo run --manifest-path rust/Cargo.toml --features
browser-login --example browser_login` and set `CHARDB_AUTHORIZE_URL`,
`CHARDB_TOKEN_URL`, `CHARDB_OAUTH_CLIENT_ID`, and `CHARDB_WS_URL`. The authorization
URL may include scopes or other provider parameters; the example adds the client
ID. The example connects once and exits. Long-running applications must supply a
token provider that renews credentials. Token persistence, keychain storage, and
refresh policy belong to the application; the browser helper stores nothing.

## TLS and plaintext

Native `wss://` builds use Rustls. There is no fallback to plaintext and no
option that disables certificate or hostname checks.

`ws://` is accepted for `localhost`, `127.0.0.1`, and `::1`. A non-loopback
plaintext endpoint requires `allow_plaintext_non_loopback(true)`, which is
intended for a trusted private network during development. The client does not
implement HTTP proxy tunneling or redirect following.

The configured connect deadline covers address connection attempts, TLS
negotiation, and the WebSocket upgrade. The welcome, mutation, and auth-refresh
waits have separate deadlines. A synchronous token provider and operating
system DNS lookup can run outside those socket deadlines.

## Subscription behavior

Snapshots replace the full row set and receive an `ack`. Duplicate snapshots
receive another `ack` without replacing rows. A `poke` applies all patches
atomically, injects the wire `rowKey` as `__key` for put and edit patches, then
emits the complete current row set as `SubscriptionEvent::Update`.

Dropping a subscription sends `unsub` on a best-effort basis. The event channel
is bounded. If a consumer stops reading and fills it, the worker retires that
local subscription instead of letting network input allocate memory without a
limit.

A retryable subscription error clears stale rows, emits
`SubscriptionEvent::Retrying`, and resends the same subscription after a
bounded exponential delay. A nonretryable subscription error retires only that
subscription and emits a terminal `SubscriptionEvent::Error`.

The public Rust client supports live-query rows and registered mutations. It
does not implement files, vectors, presence, streams, or a Cloudflare Workers
Rust transport. The wire module decodes presence and stream messages only
because their envelopes belong to protocol version 3. Its native transport uses
`std::net::TcpStream` and an operating-system thread; it does not target
`workers-rs` or `wasm32`.

## Limits

The client enforces the same resource bounds as the TypeScript SDK:

- 1 MiB inbound and outbound text messages
- 512 KiB, 4,096 aggregate members, and 99 nesting levels per argument value
- 64 active subscriptions
- 4,096 rows and 512 KiB per subscription
- 4,096 patches and 512 KiB per patch batch
- 8 MiB total retained subscription rows
- 32 unsettled mutations

Rust integers outside JavaScript's safe range are rejected. JSON numbers must
be finite and cannot be negative zero.

## Contract tests

The committed protocol corpus is read by both `src/wire.ts` and the Rust wire
module. It covers every protocol-v3 tag and the additive normalization rules.
Blocking and async clients run the same scripted reconnect, mutation replay,
patch, and empty-list auth-refresh scenario. Additional blocking tests cover
retryable and terminal session errors, subscription retry, row-carrying delete
patches, duplicate in-flight mutation IDs, multi-subscription auth refresh, and
connect, welcome, mutation, and auth-refresh timeout paths. The WSS test uses an
application-supplied test root; it does not exercise public or operating-system
roots.

`examples/workerd_conformance.rs` is also launched by the repository's Gateway
JWT harness when `CHARDB_RUST_CONFORMANCE_BIN` points to the compiled example.
That path runs against the real Miniflare/Workerd Gateway, Catalog, Cdb shard,
JWKS verifier, registered query, and mutation handler on the CI host. It checks
one initial snapshot, one stable-ID mutation, one live update, and the verified
organization, user, and role values returned by that mutation. It does not
exercise Rust reconnect, refresh, TLS, files, vectors, presence, or streams
through Workerd.
