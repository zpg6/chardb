# chardb chat tutorial

One organization-owned table covers typed create, edit, and delete mutations, live queries, and row permissions on one React screen. Better Auth signs in an anonymous local user. The configured CharDB client reads that session and adds the active organization to database calls.

The files follow the same split an application should use:

```text
src/server/auth.ts       Better Auth organization, anonymous, and JWT plugins
src/server/schema.ts     one forOrg(auth) table
src/server/api.ts        postMessage, editMessage, deleteMessage
src/server/queries.ts    listMessages live query
src/server/migrations/v1.ts  immutable deployed version-one schema snapshot
src/server/migrations.ts     append-only migration journal
src/server/worker.ts         chardb() and HTTP routes
src/web/App.tsx          Better Auth organization controls, live list, and message form
```

Do not edit `src/server/migrations/v1.ts` after a deployment reaches version one. Change the current schema in `src/server/schema.ts`, then append a versioned SQL entry to `src/server/migrations.ts`. Keeping the deployed snapshot separate prevents a later schema edit from changing the version-one digest.

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

Set `CHARDB_PERSIST_TO=/tmp/chardb-chat-fresh` to try a fresh local database without deleting existing state.

`dev` starts Wrangler, reads the packaged schema version from `/health`, then applies that exact migration target. It prints the local URL only after the schema is active. Appending version two to `src/server/migrations.ts` makes the next `npm run dev` apply version two without another flag. The Wrangler config declares four same-Worker Durable Object namespaces for CharDB's internal calls. Application code uses only the exported `DB` binding.

The browser uses Better Auth through `db.auth`, including `useSession()`, `useListOrganizations()`, `organization.create()`, and `organization.setActive()`. `db.Provider`, `db.useIdentity()`, `db.useQuery()`, and `db.useMutation()` share that client. The tutorial does not maintain another session or membership store. The local auth configuration accepts an HTTP loopback browser origin only when the Worker request is also HTTP loopback. Production requests never inherit that development exception.

Wrangler sends `/api/auth/*`, `/ws`, and `/_chardb/*` through the Worker before static assets. Use `npm run dev:web` only when you need the separate Vite development server.

The previous multi-tenancy demo now lives in [`conformance/`](./conformance/README.md). It remains a source and stress-test fixture, but the tutorial compiler does not include it.

Try it in two tabs with the same session. Send, edit, and delete a message in one tab and watch the other update without a refresh. Switch organizations while editing to check that the draft clears and each organization keeps its own history. Members can edit and delete their own messages; the schema also grants owners and admins moderation rights. The server enforces those rules even for direct API calls.

Run `npm test` to exercise create/edit/delete against SQLite with the CharDB policy wrapper, including another member, another organization, and an admin.
