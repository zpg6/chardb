import { CdbError } from "@chardb/core";
import { api } from "@chardb/core/server";
import { eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

export const postMessage = api.mutation({
    authority: "organization",
    args: z.object({
        id: z.string(),
        organizationId: z.string(),
        body: z.string().trim().min(1).max(2_000),
        clientCreatedAt: z.number().int(),
    }),
    partitionKey: "organizationId",
    handler: (ctx, args) => {
        if (!ctx.auth.userId || !ctx.auth.tenantId || ctx.auth.tenantId !== args.organizationId) {
            throw new CdbError({
                code: "CDB_FORBIDDEN",
                message: "active organization does not match the routed partition",
            });
        }
        ctx.db.insert(messages).values({ id: args.id, body: args.body, createdAt: args.clientCreatedAt }).run();
        return { id: args.id };
    },
});

const messageKey = z.object({ organizationId: z.string(), id: z.string().min(1) });

export const editMessage = api.mutation({
    authority: "organization",
    partitionKey: "organizationId",
    args: messageKey.extend({ body: z.string().trim().min(1).max(2_000) }),
    handler: (ctx, args) => {
        ctx.db.update(messages).set({ body: args.body }).where(eq(messages.id, args.id)).run();
        return { id: args.id };
    },
});

export const deleteMessage = api.mutation({
    authority: "organization",
    partitionKey: "organizationId",
    args: messageKey,
    handler: (ctx, args) => {
        ctx.db.delete(messages).where(eq(messages.id, args.id)).run();
        return { id: args.id };
    },
});
