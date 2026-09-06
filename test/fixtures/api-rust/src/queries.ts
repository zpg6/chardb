import { and, desc, eq, inArray } from "drizzle-orm";
import { z } from "zod";
import { api } from "../../../../src/server/index.ts";
import { messages } from "./schema.ts";

export const listMessages = api.query({
    ref: "src/queries.ts#listMessages",
    args: z.object({
        organizationId: z.string(),
        limit: z.number().int().min(0).max(100).default(50),
        kind: z.enum(["all", "pinned"]).optional(),
        scope: z.enum(["self", "team"]).optional(),
        filters: z.record(z.string(), z.string()).optional(),
    }),
    query: (db, args) =>
        db
            .select()
            .from(messages)
            .where(eq(messages.organizationId, args.organizationId))
            .orderBy(desc(messages.createdAt), desc(messages.id))
            .limit(args.limit),
});

export const allMessages = api.query({
    ref: "src/queries.ts#allMessages",
    query: db =>
        db
            .select()
            .from(messages)
            .where(eq(messages.organizationId, "org-fixture"))
            .orderBy(desc(messages.id))
            .limit(10),
});

export const taggedMessages = api.query({
    ref: "src/queries.ts#taggedMessages",
    args: z.object({ organizationId: z.string(), ids: z.array(z.string()).default([]) }),
    query: (db, args) =>
        db
            .select()
            .from(messages)
            .where(and(eq(messages.organizationId, args.organizationId), inArray(messages.id, args.ids)))
            .orderBy(desc(messages.id))
            .limit(5),
});
