# CharDB

An auth-native database for Cloudflare Workers.

CharDB turns a Better Auth user or organization into the ownership, authorization, and placement boundary for a sharded SQLite database. It runs on Durable Objects, uses Drizzle schemas and migrations, works through Wrangler and Miniflare, and gives browser clients typed queries, mutations, files, and live updates.

## Build one

Use Bun 1.2.22 or newer and Node.js 22 or newer.

```sh
bunx @chardb/core init my-chardb-app
cd my-chardb-app
bun install
bun run dev
```

The initializer writes a Worker, React app, Better Auth setup, Drizzle schema, migration journal, and `wrangler.toml`. It refuses to merge into a nonempty directory.

The generated app signs in, creates an organization, writes and reads organization-owned rows, receives live updates, uploads an R2-backed image, survives a Worker restart, and runs locally through Wrangler, Miniflare, and Vite.

## Own data with Better Auth

Choose one ownership mode for the application: `organization` or `user`. There is no unowned table escape hatch.

```ts
// src/auth.ts
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { anonymous } from "better-auth/plugins/anonymous";
import { defineAuth } from "@chardb/core/server";

export const auth = defineAuth({
    plugins: [anonymous(), organization(), jwt()],
});

// src/schema.ts
import { text } from "drizzle-orm/sqlite-core";
import { forOrg } from "@chardb/core/server";
import { auth } from "./auth.ts";

const { cdbTable } = forOrg(auth);

export const messages = cdbTable(
    "messages",
    {
        id: text("id").primaryKey(),
        authorId: text("author_id")
            .notNull()
            .references(() => auth.user.id),
        body: text("body").notNull(),
    },
    {
        selfBy: "authorId",
        roles: {
            owner: "*",
            admin: "*",
            member: { read: "*", create: ["id", "body"] },
            self: { update: ["body"], delete: true },
        },
    },
);
```

`forOrg(auth)` adds the managed organization key. CharDB gets it from the verified Better Auth session, refreshes membership and role state through the Catalog, routes the operation to that organization, then applies the table policy inside the same SQLite transaction.

Use `forOrgUser(auth)` when rows belong to a user inside an organization. Use `forUser(auth)` for applications without organizations.

## Define the public API

Queries and mutations are server definitions with stable wire identities. Arguments are validated before routing. No SQL string crosses RPC.

```ts
// src/messages.ts
import { api } from "@chardb/core/server";
import { desc, eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

export const listMessages = api.query({
    ref: "messages#list",
    args: z.object({
        organizationId: z.string(),
        limit: z.number().int().min(1).max(100).default(50),
    }),
    query: (db, args) => db
        .select()
        .from(messages)
        .where(eq(messages.organizationId, args.organizationId))
        .orderBy(desc(messages.id))
        .limit(args.limit),
});

export const postMessage = api.mutation({
    ref: "messages#create",
    authority: "organization",
    partitionKey: "organizationId",
    args: z.object({
        organizationId: z.string(),
        id: z.string(),
        body: z.string().trim().min(1).max(2_000),
    }),
    handler: (ctx, args) => {
        ctx.db.insert(messages).values({ id: args.id, body: args.body }).run();
        return { id: args.id };
    },
});
```

The organization ID is a checked routing input, not authorization. A caller cannot use it to enter another organization.

## Compose the Worker

```ts
// src/worker.ts
import { chardb } from "@chardb/core/server";
import { auth } from "./auth.ts";
import { migrations } from "./migrations.ts";
import * as messages from "./messages.ts";
import * as schema from "./schema.ts";

export const app = chardb({
    ownership: "organization",
    auth,
    schema,
    api: messages,
    migrations,
});

app.get("/health", c => c.json({
    ok: true,
    schemaVersion: migrations.version,
    schemaDigest: migrations.digest,
}));

export default app;
export const { DB, Catalog, Cdb, Gateway, Resharder } = app;
```

`chardb()` mounts Better Auth, the database protocol, live WebSockets, file routes, custom Hono routes, and the Durable Object classes used by `wrangler.toml`.

Worker routes can also use the native `DB` binding with the structured Drizzle client from `@chardb/core`.

Browser imports of registered handles need the CharDB Vite plugin:

```ts
// vite.config.ts
import react from "@vitejs/plugin-react";
import { chardb as chardbVite } from "@chardb/core/vite";
import { defineConfig } from "vite";

export default defineConfig({
    plugins: [react(), chardbVite()],
});
```

## Use it from React

```tsx
import { anonymousClient, organizationClient, jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
import { createChardbReactClient } from "@chardb/react";
import { listMessages, postMessage } from "./messages.ts";

export const db = createChardbReactClient({
    url: window.location.origin,
    ownership: "organization",
    auth: ({ baseURL }) => createAuthClient({
        baseURL,
        plugins: [anonymousClient(), organizationClient(), jwtClient()],
    }),
});

function Messages() {
    const identity = db.useIdentity();
    const messages = db.useQuery(listMessages, { limit: 50 });
    const mutate = db.useMutation(postMessage);

    if (identity.status === "select-organization") {
        return <button onClick={() => void createOrganization()}>Create an organization</button>;
    }

    return (
        <button onClick={() => void mutate({ id: crypto.randomUUID(), body: "hello" })}>
            {messages.data?.length ?? 0} messages
        </button>
    );
}

async function createOrganization() {
    const suffix = crypto.randomUUID().slice(0, 8);
    const created = await db.auth.organization.create({
        name: "My organization",
        slug: `my-organization-${suffix}`,
        keepCurrentActiveOrganization: true,
    });
    if (created.data) {
        await db.auth.organization.setActive({ organizationId: created.data.id });
    }
}

export function App() {
    const session = db.auth.useSession();
    if (!session.data) {
        return <button onClick={() => void db.auth.signIn.anonymous()}>Sign in</button>;
    }
    return (
        <db.Provider>
            <Messages />
            <button onClick={() => void db.auth.signOut()}>Sign out</button>
        </db.Provider>
    );
}
```

The configured public Worker URL connects both Better Auth and CharDB. The React client follows the active Better Auth organization and injects its ID into owned query and mutation arguments, so components supply only business data.

## Files and vectors

Files are typed Drizzle columns backed by R2. The row stores an opaque file ID; upload, attach, replacement, range requests, deletion, and authorization stay tied to the owning row and active organization.

Vectors are typed columns backed by Vectorize. A mutation updates the row, authoritative vector head, and durable delivery outbox in one SQLite transaction. Search filters Vectorize candidates against current SQLite ownership, policy, and version state before returning them. Rows, file metadata, vector heads, outboxes, and tombstones move together when a shard range moves.

Both are opt-in:

```ts
import { file } from "@chardb/core/files";
import { vector } from "@chardb/core/server";

attachment: file("attachment", {
    maxSize: 5 * 1_024 * 1_024,
    contentTypes: ["image/jpeg", "image/png"],
}),
embedding: vector("embedding", {
    dim: 768,
    binding: "VECTORS",
    metric: "cosine",
}),
```

Prepare the required Vectorize metadata index with:

```sh
bunx @chardb/core vectorize prepare
```

## Migrations

```sh
bunx @chardb/core migrations generate --name add_messages
bunx @chardb/core migrate --url https://api.example.com --id deploy-42 --target 2
```

Migration generation is deterministic. It writes immutable, digest-chained JSON and TypeScript snapshots and verifies the complete journal before adding a version. Deployment is resumable, fenced by schema epoch, and applied across the active shard inventory with bounded concurrency.

The public migration path accepts additive SQLite changes that do not require a table rewrite or data cleanup. Destructive and ambiguous changes fail generation.

## Recovery

Capture and restore a coordinated Durable Object recovery point with the CLI:

Use the `CHARDB_ADMIN_TOKEN` already configured for the deployed Worker. Do not generate a different token for recovery.

```sh
bunx @chardb/core backups create \
    --url https://api.example.com \
    --out recovery-2026-09-01.json

bunx @chardb/core backups restore \
    --url https://api.example.com \
    --from recovery-2026-09-01.json
```

Restore verifies the manifest and topology, fences Catalog and every shard, and removes the current derived provider records before it restarts the Durable Objects at their bookmarks. The CLI advances large restores through signed, bounded turns. Rerunning the same restore after a lost response resumes the same operation. A different manifest cannot cross the existing fence. Cloudflare retains native Durable Object PITR history for 30 days. Native PITR is available in deployed Workers, not local Miniflare. Read [Plan ahead](docs/plan-ahead.mdx) before relying on recovery or adding production data.

## Verify the package

- Each upload writes one content-addressed R2 object per unique payload. Ordinary reads verify and stream that retained object without creating a mutable live key. Restore removes every CharDB-owned live key and eagerly rebuilds the files present at the recovery point from verified retained bytes.
- Retained objects have no automatic expiry because they are the authoritative file bytes. Content left by a rejected upload or deleted file is invisible to application reads but remains billable until provider-wide orphan collection is available.
- While every shard is fenced, restore deletes each tracked physical Vectorize record and proves exact-id absence. After SQLite restarts, it requeues the authoritative heads, including pending deletes. Searches continue to validate Vectorize candidates against current ownership, policy, and vector version.

If provider cleanup or rewind fails, CharDB keeps the recovery fence in place. Rerun the same command to resume the durable operation and converge after an unknown result.

## What the release gate proves

The release suite builds one paired `@chardb/core` and `@chardb/react` candidate and uses those exact tarballs throughout. It proves:

- Better Auth organization and user ownership, role changes, revocation, and tenant isolation
- registered mutations, structured Drizzle reads, live queries, reconnects, deduplication, and process restarts
- deterministic migration generation, resumable deployment, schema fencing, and hostile journal rejection
- R2 upload, attach, read, range requests, replacement, deletion, and organization cleanup
- Vectorize delivery, retries, stale-candidate filtering, policy checks, and deletion settlement
- combined row, file, and vector movement across physical shards
- deployed recovery-point creation, traffic fencing, SQLite rewind, retained-file restoration, vector reconciliation, and exact cleanup
- generated-project browser behavior through Vite, Wrangler, Miniflare, and Playwright
- clean package consumption on Linux, macOS, and Windows

Correctness and timing reports are separate. Benchmark artifacts identify the exact package digest, runtime, workload, warmup, sample count, and correctness flags. Comparison rejects mismatched evidence instead of producing a misleading percentage.

Run the complete local correctness gate with:

```sh
bun run test:correctness
```

## Packages

| Package | Purpose |
| --- | --- |
| `@chardb/core` | Worker runtime, browser client, native binding client, CLI, files, vectors, Vite, and shared types |
| `@chardb/react` | React client included in this repository; registry publication is pending |
| `chardb-client` | Rust client included in this repository; registry publication is pending |

Generated projects use `wrangler.toml`. The CLI also reads `wrangler.json` and `wrangler.jsonc`.

## Current boundary

CharDB's first release is experimental. Organization-owned and user-owned SQLite, organization files, organization vectors, live queries, migrations, explicit range movement, coordinated recovery points, provider cleanup, and bounded R2 recovery retention are implemented and tested.

Automatic load-based resharding, vector-search continuation, regional failover controls, cross-partition transactions, and a production availability SLA are not part of this release.

## License

MIT. See [LICENSE](LICENSE).
