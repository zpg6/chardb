import { describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";
import { text } from "drizzle-orm/sqlite-core";
import { z } from "zod";
import { isCdbError } from "../../src/errors.ts";
import { type MutationCtx, createApi, defineMutation, defineQuery } from "../../src/server/define.ts";
import { readRef } from "../../src/server/refs.ts";
import { ChardbRef } from "../../src/types.ts";
import { globalScope } from "../helpers/cdb-table.ts";

describe("defineXxx — function-ref identity", () => {
    test("defineMutation attaches __chardbRef and __chardbKind", async () => {
        const fn = defineMutation(({ db: _ }: { db: unknown }, args: { x: number }) => args.x + 1);
        expect(fn.__chardbKind).toBe("mutation");
        expect(typeof fn.__chardbRef).toBe("string");
        expect(readRef(fn)).toBeDefined();
        const out = await fn({ db: null, auth: { userId: "u", claims: {} } } as never, { x: 1 });
        expect(out).toBe(2);
    });

    test("planned query carries a runtime compiler, is dispatch-only, and accepts an optional ref", async () => {
        const { cdbTable } = globalScope();
        const rows = cdbTable(
            "define_planned_rows",
            { id: text("id").primaryKey(), scope: text("scope").notNull() },
            { partitionBy: "scope", roles: { user: { read: "*" } } }
        );
        let compileRuns = 0;
        const planned = createApi({ rows }).query({
            ref: "api/define#planned",
            query: (db, args: { scope: string }) => {
                compileRuns++;
                return db.select().from(rows).where(eq(rows.scope, args.scope)).orderBy(rows.id).limit(10);
            },
        });
        expect(planned.__chardbCompilePlan?.({ scope: "shared" }).partitionKey).toBe("shared");
        await expect(
            planned({ db: {}, auth: { userId: "user-1", claims: {} } } as never, { scope: "shared" })
        ).rejects.toMatchObject({ code: "CDB_UNSUPPORTED_FEATURE" });
        expect(compileRuns).toBe(1);
        expect(defineQuery({ query: (() => null) as never }).__chardbKind).toBe("query");
        expect(() => defineQuery({ ref: "invalid", query: (() => null) as never })).toThrow("query ref must be");
        expect(() =>
            defineQuery({
                ref: "api/define#mixed",
                query: (() => null) as never,
                intent: (() => null) as never,
            } as never)
        ).toThrow("cannot mix query with intent");
    });

    test("singlePartition: true ⇒ chardb defaults `__chardbIdempotencyTtl` to 24h", () => {
        const fn = defineMutation<unknown, { id: string }, { ok: boolean }>({
            handler: (_ctx: MutationCtx<unknown>, _args) => ({ ok: true }),
            singlePartition: true,
            partitionKey: "id",
        });
        expect((fn as unknown as { __chardbIdempotencyTtl?: string }).__chardbIdempotencyTtl).toBe("24h");
    });

    test("explicit idempotencyTtl wins over the singlePartition default", () => {
        const fn = defineMutation<unknown, { id: string }, { ok: boolean }>({
            handler: (_ctx: MutationCtx<unknown>, _args) => ({ ok: true }),
            singlePartition: true,
            idempotencyTtl: "24h",
            partitionKey: "id",
        });
        // Explicit and default both happen to be "24h" today; the test guards
        // future-proof against a wider TTL enum where the user's choice must
        // override silently.
        expect((fn as unknown as { __chardbIdempotencyTtl: string }).__chardbIdempotencyTtl).toBe("24h");
    });

    test("singlePartition: false ⇒ no idempotency horizon set by default", () => {
        const fn = defineMutation<unknown, { id: string }, { ok: boolean }>({
            handler: (_ctx: MutationCtx<unknown>, _args) => ({ ok: true }),
        });
        expect((fn as unknown as { __chardbIdempotencyTtl?: string }).__chardbIdempotencyTtl).toBeUndefined();
    });

    test("declaring partitionKey implies singlePartition AND the 24h idempotency horizon", () => {
        const fn = defineMutation<unknown, { organizationId: string }, { ok: boolean }>({
            handler: (_ctx: MutationCtx<unknown>, _args) => ({ ok: true }),
            partitionKey: "organizationId",
            // No `singlePartition`, no `idempotencyTtl` — chardb defaults both.
        });
        const internals = fn as unknown as {
            __chardbSinglePartition?: boolean;
            __chardbIdempotencyTtl?: string;
            __chardbPartitionKey?: (args: { organizationId: string }) => unknown;
        };
        expect(internals.__chardbSinglePartition).toBe(true);
        expect(internals.__chardbIdempotencyTtl).toBe("24h");
        expect(internals.__chardbPartitionKey?.({ organizationId: "org-1" })).toBe("org-1");
    });

    test("organization authority is explicit mutation metadata", () => {
        const fn = defineMutation<unknown, { organizationId: string }, null>(() => null, {
            ref: "api/messages#post",
            authority: "organization",
            partitionKey: args => args.organizationId,
        });
        expect((fn as unknown as { __chardbAuthority?: string }).__chardbAuthority).toBe("organization");
        expect(fn.__chardbRef).toBe(ChardbRef("api/messages#post"));
    });

    test("user authority is explicit mutation metadata", () => {
        const mutation = defineMutation({
            ref: "api/preferences#save",
            args: z.object({ userId: z.string() }),
            authority: "user",
            partitionKey: "userId",
            handler: () => null,
        });
        expect((mutation as unknown as { __chardbAuthority?: string }).__chardbAuthority).toBe("user");
    });

    test("global mutation authority requires and preserves placement metadata", () => {
        const mutation = defineMutation({
            ref: "api/settings#save",
            authority: "global",
            partitionKey: (args: { partition: string }) => args.partition,
            handler: () => null,
        });
        expect((mutation as unknown as { __chardbAuthority?: string }).__chardbAuthority).toBe("global");
    });

    test("global declarations reject missing placement metadata at runtime", () => {
        expect(() =>
            defineMutation({
                ref: "api/settings#missingPartition",
                authority: "global",
                handler: () => null,
            } as never)
        ).toThrow("global mutations require an explicit partitionKey extractor");
    });

    test("config mutation refs are stable and validated", () => {
        const mutation = defineMutation({ ref: "api/items#create", handler: () => null });
        expect(mutation.__chardbRef).toBe(ChardbRef("api/items#create"));
        expect(() => defineMutation({ ref: "missing-separator", handler: () => null })).toThrow(/containing #/);
        expect(
            defineMutation({
                authority: "organization",
                partitionKey: (_args: { organizationId: string }) => "org-1",
                handler: () => null,
            }).__chardbKind
        ).toBe("mutation");
    });

    test("explicit singlePartition: false beats the partitionKey-implied default", () => {
        const fn = defineMutation<unknown, { organizationId: string }, { ok: boolean }>({
            handler: (_ctx: MutationCtx<unknown>, _args) => ({ ok: true }),
            partitionKey: "organizationId",
            singlePartition: false,
        });
        expect((fn as unknown as { __chardbSinglePartition?: boolean }).__chardbSinglePartition).toBeUndefined();
    });

    test("synchronous mutation validation rejects caller input with CDB_INVALID_ARGS before the handler", () => {
        let invoked = false;
        const fn = defineMutation({
            args: z.object({ id: z.string() }),
            handler: () => {
                invoked = true;
                return null;
            },
        });

        try {
            fn({ db: null, auth: { userId: "u", claims: {} } } as never, { id: 7 } as never);
            throw new Error("expected validation failure");
        } catch (error) {
            expect(isCdbError(error)).toBe(true);
            if (isCdbError(error)) {
                expect(error.code).toBe("CDB_INVALID_ARGS");
                expect(error.retryable).toBe(false);
            }
        }
        expect(invoked).toBe(false);
    });
});
