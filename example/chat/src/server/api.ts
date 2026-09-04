import { CdbError } from "@chardb/core";
import { api } from "@chardb/core/server";
import { eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

const messageKey = z.object({ organizationId: z.string(), id: z.string().min(1) });
const body = z.string().trim().min(1).max(2_000);

/** Every mutation names its organization, and that name must be the one the caller is routed to. */
function assertRouted(
    auth: { readonly userId?: string | undefined; readonly tenantId?: string | undefined },
    organizationId: string
): void {
    if (!auth.userId || !auth.tenantId || auth.tenantId !== organizationId) {
        throw new CdbError({
            code: "CDB_FORBIDDEN",
            message: "active organization does not match the routed partition",
        });
    }
}

/**
 * The policy wrapper scopes every write to rows the caller may touch, and the
 * production driver reports no change count, so the row is read back to tell
 * a denied or stale write from a real one.
 */
function denied(verb: string): never {
    throw new CdbError({ code: "CDB_FORBIDDEN", message: `message is missing or not yours to ${verb}` });
}

export const postMessage = api.mutation({
    ref: "src/server/api.ts#postMessage",
    authority: "organization",
    args: messageKey.extend({ body, clientCreatedAt: z.number().int().nonnegative().max(8_640_000_000_000_000) }),
    partitionKey: "organizationId",
    handler: (ctx, args) => {
        assertRouted(ctx.auth, args.organizationId);
        ctx.db.insert(messages).values({ id: args.id, body: args.body, createdAt: args.clientCreatedAt }).run();
        return { id: args.id };
    },
});

export const editMessage = api.mutation({
    ref: "src/server/api.ts#editMessage",
    authority: "organization",
    partitionKey: "organizationId",
    args: messageKey.extend({ body }),
    handler: (ctx, args) => {
        assertRouted(ctx.auth, args.organizationId);
        ctx.db.update(messages).set({ body: args.body }).where(eq(messages.id, args.id)).run();
        const row = ctx.db.select().from(messages).where(eq(messages.id, args.id)).get();
        if (row?.body !== args.body) denied("edit");
        return { id: args.id };
    },
});

export const deleteMessage = api.mutation({
    ref: "src/server/api.ts#deleteMessage",
    authority: "organization",
    partitionKey: "organizationId",
    args: messageKey,
    handler: (ctx, args) => {
        assertRouted(ctx.auth, args.organizationId);
        const target = ctx.db.select().from(messages).where(eq(messages.id, args.id));
        if (!target.get()) denied("delete");
        ctx.db.delete(messages).where(eq(messages.id, args.id)).run();
        if (target.get()) denied("delete");
        return { id: args.id };
    },
});
