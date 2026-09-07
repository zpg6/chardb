![CharDB](https://raw.githubusercontent.com/zpg6/chardb/main/landing/public/banner.png)

An auth-native database for Cloudflare Workers.

CharDB turns a Better Auth user or organization into the ownership, authorization, and placement boundary for a sharded SQLite database. It runs on Durable Objects, uses Drizzle schemas and migrations, works through Wrangler and Miniflare, and gives browser clients typed queries, mutations, files, and live updates.

Documentation lives at [docs.chardb.dev](https://docs.chardb.dev). The first release is experimental. Read [Plan ahead](https://docs.chardb.dev/plan-ahead) before storing data you cannot recreate.

## Build one

Bun 1.2.22 or newer and Node.js 22 or newer.

```sh
bunx @chardb/core init my-chardb-app
cd my-chardb-app
bun install
bun run dev
```

The [quickstart](https://docs.chardb.dev/quickstart) walks through the generated app.

## Own rows

Every table names its owner and what each role may do with each column. This is the schema the initializer writes.

```ts
import { file } from "@chardb/core/files";
import { integer, text } from "drizzle-orm/sqlite-core";
import { forOrg } from "@chardb/core/server";
import { auth } from "./auth.ts";

const { cdbTable } = forOrg(auth);

export const messages = cdbTable(
  "messages",
  {
    id: text("id").primaryKey(),
    authorId: text("author_id")
      .notNull()
      .references(() => auth.user.id, { onDelete: "cascade" }),
    body: text("body").notNull(),
    attachment: file("attachment", { maxSize: 5 * 1_024 * 1_024, contentTypes: ["image/jpeg", "image/png"] }),
    createdAt: integer("created_at").notNull(),
  },
  {
    selfBy: "authorId",
    roles: {
      owner: "*",
      admin: "*",
      member: { read: "*", create: ["id", "body", "attachment", "createdAt"] },
      self: { read: "*", update: ["body", "attachment"], delete: true },
    },
  },
);
```

`forOrg(auth)` adds `organizationId` and fills it from the verified session on every insert. `forUser(auth)` and `forOrgUser(auth)` do the same for a user, or for a member inside an organization. [Ownership](https://docs.chardb.dev/ownership) has the details.

## Packages

| Package | Purpose |
| --- | --- |
| `@chardb/core` | Worker runtime, browser client, native binding client, CLI, files, vectors, Vite plugin, and shared types |
| `@chardb/react` | React client |
| `chardb-client` | Rust client |

## License

MIT.
