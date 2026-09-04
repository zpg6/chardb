import { useState } from "react";

type CodeLanguage = "rust" | "typescript";

const CODE_TOKEN =
    /\/\/[^\n]*|\/\*[\s\S]*?\*\/|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\b\d+(?:\.\d+)?\b|\b[A-Za-z_][A-Za-z0-9_]*\b/g;
const TYPESCRIPT_KEYWORDS = new Set([
    "as",
    "async",
    "await",
    "const",
    "export",
    "from",
    "function",
    "if",
    "import",
    "new",
    "return",
    "true",
]);
const RUST_KEYWORDS = new Set([
    "as",
    "async",
    "await",
    "break",
    "const",
    "continue",
    "derive",
    "fn",
    "if",
    "let",
    "loop",
    "match",
    "mod",
    "mut",
    "pub",
    "return",
    "struct",
    "use",
]);

function syntaxClass(source: string, token: string, index: number, language: CodeLanguage): string {
    if (token.startsWith("//") || token.startsWith("/*")) return "syntax-comment";
    if (token.startsWith('"') || token.startsWith("'")) return "syntax-string";
    if (/^\d/.test(token)) return "syntax-number";
    if ((language === "rust" ? RUST_KEYWORDS : TYPESCRIPT_KEYWORDS).has(token)) return "syntax-keyword";
    if (/^[A-Z]/.test(token)) return "syntax-type";

    const before = source.slice(0, index).trimEnd().at(-1);
    const after = source
        .slice(index + token.length)
        .trimStart()
        .at(0);
    if (before === ".") return "syntax-property";
    if (after === "(" || after === "!") return "syntax-function";
    return "syntax-variable";
}

function highlightCode(source: string, language: CodeLanguage) {
    const nodes: Array<string | React.JSX.Element> = [];
    let cursor = 0;

    for (const match of source.matchAll(CODE_TOKEN)) {
        const index = match.index ?? 0;
        if (index > cursor) nodes.push(source.slice(cursor, index));
        nodes.push(
            <span className={syntaxClass(source, match[0], index, language)} key={`${index}-${match[0]}`}>
                {match[0]}
            </span>
        );
        cursor = index + match[0].length;
    }
    if (cursor < source.length) nodes.push(source.slice(cursor));
    return nodes;
}

function DatabaseSnippet() {
    return (
        <code>
            {highlightCode(
                `// src/auth.ts
import { defineAuth } from "@chardb/core/server";
import { anonymous } from "better-auth/plugins/anonymous";
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";

export const auth = defineAuth({
  plugins: [anonymous(), organization(), jwt()],
});

// src/schema.ts
import { forOrg } from "@chardb/core/server";
import { text } from "drizzle-orm/sqlite-core";
import { auth } from "./auth.ts";

const { cdbTable } = forOrg(auth);
export const messages = cdbTable("messages", {
  id: text("id").primaryKey(),
  body: text("body").notNull(),
});

// src/worker.ts
import { chardb } from "@chardb/core/server";
import { auth } from "./auth.ts";
import * as api from "./messages.ts";
import { migrations } from "./migrations.ts";
import * as schema from "./schema.ts";

const app = chardb({
  ownership: "organization",
  auth,
  schema,
  api,
  migrations,
});

export default app;
export const { DB, Catalog, Cdb, Gateway, Resharder } = app;`,
                "typescript"
            )}
        </code>
    );
}

function ReactClientSnippet() {
    return (
        <code>
            {highlightCode(
                `import { createChardbReactClient } from "@chardb/react";
import { anonymousClient, jwtClient, organizationClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
import { listMessages } from "./messages.ts";

export const db = createChardbReactClient({
  url: window.location.origin,
  ownership: "organization",
  auth: ({ baseURL }) => createAuthClient({
    baseURL,
    plugins: [anonymousClient(), organizationClient(), jwtClient()],
  }),
});

async function createOrganization() {
  const suffix = crypto.randomUUID().slice(0, 8);
  const created = await db.auth.organization.create({
    name: "My organization",
    slug: "my-organization-" + suffix,
    keepCurrentActiveOrganization: true,
  });
  if (created.data) {
    await db.auth.organization.setActive({ organizationId: created.data.id });
  }
}

function Messages() {
  const identity = db.useIdentity();
  const messages = db.useQuery(listMessages, { limit: 50 });
  if (identity.status === "select-organization") {
    return <button onClick={createOrganization}>Create organization</button>;
  }
  return <p>{messages.data?.length ?? 0} messages</p>;
}

export function App() {
  const session = db.auth.useSession();
  if (!session.data) {
    return <button onClick={() => db.auth.signIn.anonymous()}>Sign in</button>;
  }
  return (
    <db.Provider>
      <Messages />
      <button onClick={() => db.auth.signOut()}>Sign out</button>
    </db.Provider>
  );
}`,
                "typescript"
            )}
        </code>
    );
}

function RustClientSnippet() {
    return (
        <code>
            {highlightCode(
                `// bunx chardb api rust --out src/chardb_api.rs
mod chardb_api;

use chardb_api::{ListMessagesArgs, LIST_MESSAGES};
use chardb_client::{AsyncClient, ClientConfig, Result, SubscriptionEvent};

async fn watch_messages(
  endpoint: &str,
  jwt: String,
  organization_id: String,
) -> Result<()> {
  let client = AsyncClient::connect(
    ClientConfig::with_token(endpoint, jwt),
  ).await?;
  let args = ListMessagesArgs { organization_id, limit: Some(50) };
  let mut messages = client.subscribe(LIST_MESSAGES, &args)?;

  loop {
    match messages.recv().await? {
      SubscriptionEvent::Snapshot { rows }
      | SubscriptionEvent::Update { rows } => {
        for row in rows { println!("{}: {}", row.id, row.body); }
      }
      SubscriptionEvent::Error(error) => return Err(error),
      SubscriptionEvent::Closed => break,
      _ => {}
    }
  }
  client.close();
  Ok(())
}`,
                "rust"
            )}
        </code>
    );
}

const clientSdks = [
    { name: "React", icon: "/brands/react.svg", fileIcon: "/brands/file-react-ts.svg", available: true },
    { name: "Rust", icon: "/brands/rust.svg", fileIcon: "/brands/file-rust.svg", available: true },
    { name: "Python", icon: "/brands/python.svg", fileIcon: "/brands/file-typescript.svg", available: false },
    { name: "Swift", icon: "/brands/swift.svg", fileIcon: "/brands/file-typescript.svg", available: false },
    { name: "Flutter", icon: "/brands/flutter.svg", fileIcon: "/brands/file-typescript.svg", available: false },
    { name: "Expo", icon: "/brands/expo.svg", fileIcon: "/brands/file-typescript.svg", available: false },
] as const;
type ClientSdk = "React" | "Rust";

const moments = [
    {
        label: "Realtime",
        title: "Queries update after the transaction commits.",
        body: "Live handles rerun on the owning shard, so the browser sees committed rows without polling or a second sync service.",
    },
    {
        label: "Files + vectors",
        title: "Rows can point to more than scalar data.",
        body: "File columns route to R2. Vector columns route to Vectorize. The schema keeps the organization boundary and application code keeps the row identity.",
    },
    {
        label: "Local + Cloudflare",
        title: "The same Worker runs on both sides of deploy.",
        body: "Wrangler owns the config. Miniflare and Cloudflare's Vitest integration run it locally. The generated Worker deploys without a separate database service.",
    },
] as const;

export function ProductOverview() {
    const [clientSdk, setClientSdk] = useState<ClientSdk>("React");
    const selectedSdk = clientSdks.find(sdk => sdk.name === clientSdk) ?? clientSdks[0];

    return (
        <>
            <section id="worker-client" className="border-y border-line bg-ink-900/50">
                <div className="mx-auto max-w-page px-5 py-20 sm:px-8 sm:py-28">
                    <div className="max-w-2xl">
                        <p className="eyebrow">Worker → client</p>
                        <h2 className="mt-4 text-3xl font-semibold tracking-tight text-fg sm:text-4xl">
                            Set up your Worker. Use it like an app SDK.
                        </h2>
                        <p className="mt-5 text-base leading-7 text-fg-muted">
                            Pick organization or user ownership once. CharDB carries the successful Better Auth identity
                            through routing, policy, typed handles, and every client query.
                        </p>
                    </div>

                    <div className="connection-stage mt-12">
                        <article className="code-window" aria-label="Worker database setup">
                            <header>
                                <span className="file-dot" />
                                <span className="file-dot" />
                                <span className="file-dot" />
                                <img
                                    className="file-type-icon"
                                    src="/brands/file-typescript.svg"
                                    alt=""
                                    width="18"
                                    height="18"
                                />
                                <code>Worker setup</code>
                                <span className="code-surface">Worker</span>
                            </header>
                            <pre>
                                <DatabaseSnippet />
                            </pre>
                        </article>

                        <div className="connection-rail" aria-label="Authenticated live connection">
                            <span className="connection-line" />
                            <span className="connection-node" aria-hidden="true">
                                ↔
                            </span>
                            <span className="connection-label">Better Auth JWT</span>
                            <span className="connection-label">typed + live</span>
                        </div>

                        <article className="code-window" aria-label="Client SDK consumption">
                            <header>
                                <span className="file-dot" />
                                <span className="file-dot" />
                                <span className="file-dot" />
                                <img
                                    className="file-type-icon"
                                    src={selectedSdk.fileIcon}
                                    alt=""
                                    width="18"
                                    height="18"
                                />
                                <code>{clientSdk === "Rust" ? "src/main.rs" : "src/App.tsx"}</code>
                                <span className="code-surface">Client</span>
                            </header>
                            <div className="client-sdk-tabs" role="tablist" aria-label="Client SDK">
                                {clientSdks.map(sdk => {
                                    return (
                                        <button
                                            type="button"
                                            role="tab"
                                            aria-selected={clientSdk === sdk.name}
                                            disabled={!sdk.available}
                                            onClick={() => sdk.available && setClientSdk(sdk.name)}
                                            key={sdk.name}
                                        >
                                            <img
                                                className="sdk-tab-icon"
                                                src={sdk.icon}
                                                alt=""
                                                width="14"
                                                height="14"
                                            />
                                            {sdk.name}
                                            {!sdk.available ? <span>soon</span> : null}
                                        </button>
                                    );
                                })}
                            </div>
                            <pre>{clientSdk === "Rust" ? <RustClientSnippet /> : <ReactClientSnippet />}</pre>
                        </article>
                    </div>
                </div>
            </section>

            <section id="product" className="mx-auto max-w-page px-5 py-20 sm:px-8 sm:py-28">
                <div className="grid gap-px overflow-hidden rounded-2xl border border-line bg-line lg:grid-cols-3">
                    {moments.map(moment => (
                        <article className="bg-ink-950 p-7 sm:p-8" key={moment.label}>
                            <p className="font-mono text-xs text-accent">{moment.label}</p>
                            <h2 className="mt-8 text-xl font-medium leading-snug text-fg">{moment.title}</h2>
                            <p className="mt-3 text-sm leading-6 text-fg-muted">{moment.body}</p>
                        </article>
                    ))}
                </div>

                <div className="mt-16 flex flex-col items-start justify-between gap-7 border-t border-line pt-8 sm:flex-row sm:items-center">
                    <p className="max-w-xl text-xl leading-8 text-fg">
                        Start with the generated app. Keep the Cloudflare stack you already know.
                    </p>
                    <div className="flex items-center gap-5 text-sm">
                        <a href="/why/" className="text-fg hover:text-accent transition-colors">
                            Why CharDB
                        </a>
                    </div>
                </div>
            </section>
        </>
    );
}
