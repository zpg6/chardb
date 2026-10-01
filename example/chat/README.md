# chardb chat tutorial

One organization-owned table covers typed create, edit, and delete mutations, live queries, and row permissions on one React screen. Better Auth signs in an anonymous local user. The configured CharDB client reads that session and adds the active organization to database calls.

The files follow the same split an application should use:

```text
src/server/auth.ts       Better Auth organization, anonymous, and JWT plugins
src/server/schema.ts     one forOrg(auth) table
src/server/api.ts        postMessage, editMessage, deleteMessage
src/server/queries.ts    listMessages live query
src/server/migrations/v1.ts  immutable deployed version-one SQL
src/server/migrations/v1-better-auth-17.ts  version-one SQL deployed with Better Auth 1.7
src/server/migrations/v2.ts  Better Auth 1.7's JWT key columns
src/server/migrations/history.ts  explicit deployment history selection
src/server/migrations.ts     packaged migration journal
src/server/worker.ts         chardb() and HTTP routes
src/web/App.tsx          Better Auth organization controls, live list, and message form
```

Keep both version-one SQL files unchanged. Change the current schema in `src/server/schema.ts`, then append a versioned SQL entry to both histories in `src/server/migrations/history.ts`. Static SQL prevents dependency upgrades from changing historical digests. The migration tests compare both complete journals with the current schema.

`CHAT_SCHEMA_HISTORY` selects the journal for the whole deployment. It does not inspect the database or rewrite migration metadata. Match the setting to the version-one schema already stored:

| Version-one schema | `CHAT_SCHEMA_HISTORY` | Version-two change |
| --- | --- | --- |
| Better Auth 1.6, without `jwks.alg` and `jwks.crv` | `better-auth-1.6`, or unset | Adds both nullable columns |
| Better Auth 1.7, with both columns | `better-auth-1.7` | Records a no-op migration |

The Wrangler template sets `better-auth-1.7`. Existing deployments first initialized with Better Auth 1.6 must change that variable to `better-auth-1.6` before deploying this code. New databases can use either history. Keep the chosen setting on later deployments; the histories retain distinct digests even though their version-two tables match. A wrong setting fails the stored migration digest check. An unrecognized value prevents the Worker from starting.

`worker.ts` also exposes a direct read at `GET /api/messages?organizationId=<active-id>`. It uses the same schema and query compiler as the registered live handle:

```ts
const rows = await client(c.env.DB, { jwt, authOrigin: url.origin })
    .select()
    .from(messages)
    .where(eq(messages.organizationId, organizationId))
    .orderBy(desc(messages.createdAt), desc(messages.id))
    .limit(50);
```

The React client owns the Worker URL, Better Auth client, and organization scope:

```tsx
const db = createChardbReactClient({
    url: window.location.origin,
    ownership: "organization",
    auth: ({ baseURL }) => createAuthClient({ baseURL, plugins }),
});

function Messages() {
    return db.useQuery(listMessages, { limit: 50 });
}
```

Run it locally:

```bash
bun run build:react
cd example/chat
npm ci
npm run typecheck
npm run build
npm run dev
```

Set `CHARDB_DEV_PERSIST_TO=/tmp/chardb-chat-fresh` to try a fresh local database without deleting existing state.

`dev` starts Wrangler, reads the packaged schema version from `/health`, then applies that exact migration target. It prints the local URL only after the schema is active. The next `npm run dev` applies version two using the configured history. The Wrangler config declares four same-Worker Durable Object namespaces for CharDB's internal calls. Application code uses only the exported `DB` binding.

The browser uses Better Auth through `db.auth`, including `useSession()`, `useListOrganizations()`, `organization.create()`, and `organization.setActive()`. `db.Provider`, `db.useIdentity()`, `db.useQuery()`, and `db.useMutation()` share that client. The tutorial does not maintain another session or membership store. The local auth configuration accepts an HTTP loopback browser origin only when the Worker request is also HTTP loopback. Production requests never inherit that development exception.

Wrangler sends `/api/auth/*`, `/ws`, and `/_chardb/*` through the Worker before static assets. Use `npm run dev:web` only when you need the separate Vite development server.

The previous multi-tenancy demo now lives in [`conformance/`](./conformance/README.md). It remains a source and stress-test fixture, but the tutorial compiler does not include it.
