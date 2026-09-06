import { integer, real, text } from "drizzle-orm/sqlite-core";
import { forOrg } from "../../../../src/server/index.ts";
import { auth } from "./auth.ts";

const { cdbTable } = forOrg(auth);

export const messages = cdbTable(
    "messages",
    {
        id: text("id").primaryKey(),
        body: text("body").notNull(),
        createdAt: integer("created_at").notNull(),
        pinned: integer("pinned", { mode: "boolean" }).notNull(),
        score: real("score"),
        meta: text("meta", { mode: "json" }),
    },
    { roles: { owner: "*", admin: "*", member: { read: "*" } } }
);
