import { describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";
import { text } from "drizzle-orm/sqlite-core";
import { z } from "zod";
import { isCdbError } from "../../src/errors.ts";
import { createApi, defineMutation } from "../../src/server/define.ts";
import {
    manifestFromExports,
    resolveMutation,
    resolveQuery,
    routeMutation,
    routeQuery,
    routeValidatedQuery,
} from "../../src/server/manifest.ts";
import { ChardbRef } from "../../src/types.ts";
import { globalScope } from "../helpers/cdb-table.ts";

const { cdbTable } = globalScope();
const rows = cdbTable(
    "manifest_rows",
    { id: text("id").primaryKey(), scope: text("scope").notNull() },
    { partitionBy: "scope", roles: { user: { read: "*" } } }
);
const api = createApi({ rows });
const listRows = api.query({
    ref: "api/manifest#list",
    query: (db, args: { scope: string }) =>
        db.select().from(rows).where(eq(rows.scope, args.scope)).orderBy(rows.id).limit(10),
});
const validatedRows = api.query({
    ref: "api/manifest#validated",
    args: z.object({ scope: z.string() }),
    query: (db, args) => db.select().from(rows).where(eq(rows.scope, args.scope)).orderBy(rows.id).limit(10),
});
const createRow = defineMutation<unknown, { scope: string }, { id: string }>(
    (_ctx, args) => ({ id: `row-${args.scope}` }),
    { ref: "api/manifest#create", authority: "global", singlePartition: true, partitionKey: args => args.scope }
);

describe("manifestFromExports", () => {
    test("collects mutations and planned queries", () => {
        const manifest = manifestFromExports({ createRow, listRows, junk: 42 });
        expect(manifest.mutations.size).toBe(1);
        expect(manifest.queries.size).toBe(1);
        expect(resolveMutation(manifest, createRow.__chardbRef).singlePartition).toBe(true);
        expect(resolveQuery(manifest, listRows.__chardbRef).compilePlan).toBeDefined();
    });

    test("binds unnamed handlers and queries to API export names", () => {
        const first = api.mutation({
            authority: "organization",
            partitionKey: "organizationId",
            args: z.object({ organizationId: z.string() }),
            handler: () => "first",
        });
        const second = api.mutation({
            authority: "organization",
            partitionKey: "organizationId",
            args: z.object({ organizationId: z.string() }),
            handler: () => "second",
        });
        const list = api.query({
            query: (db, args: { scope: string }) =>
                db.select().from(rows).where(eq(rows.scope, args.scope)).orderBy(rows.id).limit(10),
        });
        const manifest = manifestFromExports({ first, second, list });
        expect([...manifest.mutations.keys()]).toEqual([ChardbRef("mutation#first"), ChardbRef("mutation#second")]);
        expect([...manifest.queries.keys()]).toEqual([ChardbRef("query#list")]);
        expect(first.__chardbRef).toBe(ChardbRef("mutation#first"));
        expect(
            resolveMutation(manifest, second.__chardbRef).invoke(
                { db: {}, auth: { userId: "u", claims: {} } },
                { organizationId: "org" }
            )
        ).toBe("second");
        expect(
            routeValidatedQuery(manifest, { ref: list.__chardbRef, args: { scope: "shared" } }, () => "policy")
                .partitionKey
        ).toBe("shared");
        expect(() => manifestFromExports({ renamed: first })).toThrow("already registered as first");
    });

    test("chooses the same alias regardless of export order", () => {
        for (const kind of ["mutation", "query"] as const) {
            const create = () =>
                kind === "mutation"
                    ? api.mutation({ handler: () => null })
                    : api.query({ query: db => db.select().from(rows).where(eq(rows.scope, "shared")) });
            const first = create();
            const second = create();
            const left = manifestFromExports({ save: first, alias: first });
            const right = manifestFromExports({ alias: second, save: second });
            const ref = ChardbRef(`${kind}#alias`);
            expect(first.__chardbRef).toBe(ref);
            expect(second.__chardbRef).toBe(ref);
            expect([...left.mutations.keys(), ...left.queries.keys()]).toEqual([ref]);
            expect([...right.mutations.keys(), ...right.queries.keys()]).toEqual([ref]);
        }
    });

    test("retains a bound name when aliases are added or reordered", () => {
        const save = api.mutation({ handler: () => null });
        manifestFromExports({ save });
        const ref = save.__chardbRef;
        for (const exports of [
            { alias: save, save },
            { save, alias: save },
        ]) {
            expect([...manifestFromExports(exports).mutations.keys()]).toEqual([ref]);
            expect(save.__chardbRef).toBe(ref);
        }
        expect(() => manifestFromExports({ alias: save })).toThrow("already registered as save");
        expect(save.__chardbRef).toBe(ref);
    });

    test("preserves explicit refs across alias containers", () => {
        const save = api.mutation({ ref: "messages#save", handler: () => null });
        for (const exports of [{ save, alias: save }, { alias: save }, { renamed: save }]) {
            expect([...manifestFromExports(exports).mutations.keys()]).toEqual([ChardbRef("messages#save")]);
        }
    });

    test("rejects an explicit ref that collides with an automatic ref", () => {
        const save = api.mutation({ handler: () => null });
        const explicit = api.mutation({ ref: "mutation#save", handler: () => null });
        expect(() => manifestFromExports({ save, explicit })).toThrow("duplicate ref");
    });

    test("routes a compiled plan and includes its plan hash", () => {
        const manifest = manifestFromExports({ listRows });
        const route = routeValidatedQuery(
            manifest,
            { ref: listRows.__chardbRef, args: { scope: "shared" } },
            () => "policy"
        );
        expect(route).toMatchObject({ authority: "global", partitionKey: "shared" });
        expect(route.selectPlan).toEqual(listRows.__chardbCompilePlan?.({ scope: "shared" }).plan);
        expect(route.queryHash).toContain("planHash");
    });

    test("rejects a query marker without a compiled plan", () => {
        const legacy = Object.assign(async () => [], {
            __chardbKind: "query" as const,
            __chardbRef: "api/manifest#legacy" as never,
        });
        expect(() => manifestFromExports({ legacy })).toThrow("query ref has no compiled plan");
    });

    test("rejects distinct handles with the same ref", () => {
        const duplicate = Object.assign(async () => [], {
            __chardbKind: "query" as const,
            __chardbRef: listRows.__chardbRef,
            __chardbCompilePlan: listRows.__chardbCompilePlan,
        });
        expect(() => manifestFromExports({ listRows, duplicate })).toThrow("duplicate ref across query and query");
    });

    test("routes mutations and reports unknown refs", () => {
        const manifest = manifestFromExports({ createRow });
        expect(
            routeMutation(manifest, { ref: createRow.__chardbRef, args: { scope: "shared" } }, () => 7)
        ).toMatchObject({ ok: true, authority: "global", partitionKey: "shared", vshard: 7 });
        try {
            resolveMutation(manifest, "api/manifest#missing" as ChardbRef);
            throw new Error("expected throw");
        } catch (error) {
            expect(isCdbError(error)).toBe(true);
        }
    });

    test("isolates malformed and deeply nested args from a valid planned sibling", async () => {
        const manifest = manifestFromExports({ listRows, validatedRows });
        await expect(
            routeQuery(manifest, { ref: validatedRows.__chardbRef, args: { scope: 7 } }, () => "policy")
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_INVALID_ARGS" } });
        let deep: Record<string, unknown> = {};
        for (let index = 0; index < 40; index++) deep = { child: deep };
        await expect(
            routeQuery(manifest, { ref: validatedRows.__chardbRef, args: deep as never }, () => "policy")
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_INVALID_ARGS" } });
        await expect(
            routeQuery(manifest, { ref: listRows.__chardbRef, args: { scope: "shared" } }, () => "policy")
        ).resolves.toMatchObject({ ok: true, partitionKey: "shared" });
    });
});
