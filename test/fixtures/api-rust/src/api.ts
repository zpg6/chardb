import { z } from "zod";
import { api } from "../../../../src/server/index.ts";
import { messages } from "./schema.ts";

export const postMessage = api.mutation({
    ref: "src/api.ts#postMessage",
    authority: "organization",
    args: z.object({
        id: z.string(),
        organizationId: z.string(),
        body: z.string().min(1),
        type: z.string().nullable(),
        tags: z.array(z.string()).optional(),
        dueAt: z.string().nullable().optional(),
    }),
    partitionKey: "organizationId",
    handler: (ctx, args) => {
        ctx.db.insert(messages).values({ id: args.id, body: args.body, createdAt: 0, pinned: false }).run();
        return { id: args.id };
    },
});

export const clearMessages = api.mutation({
    ref: "src/api.ts#clearMessages",
    authority: "organization",
    partitionKey: (args: { organizationId: string }) => args.organizationId,
    handler: () => ({ cleared: true }),
});
