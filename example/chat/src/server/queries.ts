import { api } from "@chardb/core/server";
import { desc, eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

export const listMessages = api.query({
    args: z.object({
        organizationId: z.string(),
        limit: z.number().int().min(1).max(100).default(50),
    }),
    query: (db, args) =>
        db
            .select()
            .from(messages)
            .where(eq(messages.organizationId, args.organizationId))
            .orderBy(desc(messages.createdAt), desc(messages.id))
            .limit(args.limit),
});
