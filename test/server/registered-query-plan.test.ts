import { describe, expect, test } from "bun:test";
import { and, between, desc, eq, gt, gte, inArray, isNotNull, isNull, lt, lte, ne, or, sql } from "drizzle-orm";
import { type QueryBuilder, integer, text } from "drizzle-orm/sqlite-core";
import { type ChardbSelectPlanV1, createBindingSelect } from "../../src/binding-plan.ts";
import { createApi } from "../../src/server/define.ts";
import { manifestFromExports, routeValidatedQuery } from "../../src/server/manifest.ts";
import { compileRegisteredQueryPlan } from "../../src/server/registered-query-plan.ts";
import type { ChardbRef } from "../../src/types.ts";
import { searchVector, vector } from "../../src/vector.ts";
import { forOrg, forUser, globalScope } from "../helpers/cdb-table.ts";

const { cdbTable } = globalScope();
const plannedRows = cdbTable(
    "planned_rows",
    {
        id: text("id").primaryKey(),
        namespace: text("namespace").notNull(),
        channelId: text("channel_id").notNull(),
        createdAt: integer("created_at").notNull(),
    },
    { partitionBy: "namespace", roles: { user: { read: "*" } } }
);

const api = createApi({ plannedRows });
const listPlannedRows = api.query({
    ref: "api/planned#list",
    query: (db, args: { namespace: string; channelId: string; limit: number }) =>
        db
            .select()
            .from(plannedRows)
            .where(and(eq(plannedRows.namespace, args.namespace), eq(plannedRows.channelId, args.channelId)))
            .orderBy(desc(plannedRows.createdAt), desc(plannedRows.id))
            .limit(args.limit),
});

const { cdbTable: userTable } = forUser();
const plannedUserRows = userTable(
    "planned_user_rows",
    { id: text("id").primaryKey(), userId: text("user_id").notNull() },
    { tenantBy: "userId", partitionBy: "userId", roles: { user: { read: "*" } } }
);

const { cdbTable: organizationTable } = forOrg();
const plannedVectorRows = organizationTable(
    "planned_vector_rows",
    {
        id: text("id").primaryKey(),
        organizationId: text("organization_id").notNull(),
        embedding: vector("embedding", { dim: 3, binding: "CDB_PLANNED_VECTORS", metric: "cosine" }),
    },
    {
        tenantBy: "organizationId",
        partitionBy: "organizationId",
        roles: { member: { read: "*" } },
    }
);

const searchPlannedVectors = createApi({ plannedVectorRows }).query({
    ref: "api/planned#vector-search",
    query: (_db, args: { organizationId: string; values: readonly number[]; limit?: number }) =>
        searchVector(plannedVectorRows.embedding, args),
});

describe("registered query plan", () => {
    test("compiles an organization vector search into one descriptor-bound plan", () => {
        const plan = searchPlannedVectors.__chardbCompilePlan?.({
            organizationId: "org-1",
            values: [1 / 3, 2, -4],
            limit: 7,
        });
        if (!plan || plan.kind !== "searchVector") throw new Error("vector fixture compiled a select plan");
        expect(plan).toEqual({
            version: 1,
            kind: "searchVector",
            authority: "organization",
            partitionKey: "org-1",
            intent: {
                kind: "select",
                tables: ["planned_vector_rows"],
                partitionKey: {
                    table: "planned_vector_rows",
                    column: "organization_id",
                    values: ["org-1"],
                },
            },
            resource: {
                kind: "vector",
                version: 1,
                table: "planned_vector_rows",
                column: "embedding",
                primaryKey: "id",
                organizationColumn: "organization_id",
                binding: "CDB_PLANNED_VECTORS",
                dimensions: 3,
                metric: "cosine",
            },
            values: [Math.fround(1 / 3), 2, -4],
            limit: 7,
            planHash: "7a19305ff41e92c2a053a31358bf74440cb816f702bbd0370cf77ec5b6dae3f0",
        });
        const typedArrayPlan = compileRegisteredQueryPlan(
            () =>
                searchVector(plannedVectorRows.embedding, {
                    organizationId: "org-1",
                    values: new Float32Array([1 / 3, 2, -4]),
                    limit: 7,
                }),
            {}
        );
        expect(typedArrayPlan.planHash).toBe(plan.planHash);
        expect(
            searchPlannedVectors.__chardbCompilePlan?.({
                organizationId: "org-2",
                values: [1 / 3, 2, -4],
                limit: 7,
            }).planHash
        ).not.toBe(plan.planHash);

        const manifest = manifestFromExports({ searchPlannedVectors });
        const route = routeValidatedQuery(
            manifest,
            {
                ref: searchPlannedVectors.__chardbRef,
                args: { organizationId: "org-1", values: [1 / 3, 2, -4], limit: 7 },
            },
            tables => `policy:${tables.join(",")}`
        );
        expect(route.selectPlan).toBeUndefined();
        expect(route.vectorPlan).toEqual(plan);
        expect(route).toMatchObject({
            authority: "organization",
            partitionKey: "org-1",
            policyDigest: "policy:planned_vector_rows",
        });

        const descriptor = manifest.queries.get(searchPlannedVectors.__chardbRef);
        const compilePlan = descriptor?.compilePlan;
        if (!descriptor || !compilePlan) throw new Error("vector fixture omitted its compiler");
        (manifest.queries as Map<ChardbRef, typeof descriptor>).set(searchPlannedVectors.__chardbRef, {
            ...descriptor,
            compilePlan: args => {
                const compiled = compilePlan(args);
                if (compiled.kind !== "searchVector") throw new Error("vector fixture compiled a select plan");
                return { ...compiled, values: [0, ...compiled.values.slice(1)] };
            },
        });
        expect(() =>
            routeValidatedQuery(
                manifest,
                {
                    ref: searchPlannedVectors.__chardbRef,
                    args: { organizationId: "org-1", values: [1 / 3, 2, -4], limit: 7 },
                },
                () => "policy"
            )
        ).toThrow("vector plan disagrees with its organization resource metadata");
    });

    test("derives global placement, intervals, full projection, ordering, and a stable hash", () => {
        const plan = listPlannedRows.__chardbCompilePlan?.({ namespace: "public", channelId: "news", limit: 25 });
        expect(plan).toBeDefined();
        expect(plan?.authority).toBe("global");
        expect(plan?.partitionKey).toBe("public");
        expect(plan?.intent.tables).toEqual(["planned_rows"]);
        expect(plan?.intent.partitionKey).toEqual({
            table: "planned_rows",
            column: "namespace",
            values: ["public"],
        });
        expect(plan?.intent.intervals?.map(interval => interval.indexName)).toEqual(["namespace", "channel_id"]);
        expect(plan?.projection).toEqual([
            { key: "id", column: "id" },
            { key: "namespace", column: "namespace" },
            { key: "channelId", column: "channel_id" },
            { key: "createdAt", column: "created_at" },
        ]);
        expect(plan?.orderBy).toEqual([
            { column: "created_at", direction: "desc" },
            { column: "id", direction: "desc" },
        ]);
        expect(plan?.limit).toBe(25);
        expect(listPlannedRows.__chardbCompilePlan?.({ namespace: "public", channelId: "news", limit: 25 })).toEqual(
            plan
        );
    });

    test("emits the same canonical plan as the direct select compiler", async () => {
        const directPlans: ChardbSelectPlanV1[] = [];
        const select = createBindingSelect(async plan => {
            directPlans.push(plan);
            return [];
        });
        await select()
            .from(plannedRows)
            .where(and(eq(plannedRows.namespace, "public"), eq(plannedRows.channelId, "news")) as ReturnType<typeof eq>)
            .orderBy(desc(plannedRows.createdAt), desc(plannedRows.id))
            .limit(25);

        const registered = listPlannedRows.__chardbCompilePlan?.({
            namespace: "public",
            channelId: "news",
            limit: 25,
        });
        expect(registered?.plan).toEqual(directPlans[0]);
        expect(registered?.plan).toEqual({
            version: 1,
            kind: "select",
            table: "planned_rows",
            selection: { kind: "all" },
            where: {
                kind: "and",
                predicates: [
                    { kind: "compare", op: "eq", column: "namespace", value: "public" },
                    { kind: "compare", op: "eq", column: "channel_id", value: "news" },
                ],
            },
            orderBy: [
                { column: "created_at", direction: "desc" },
                { column: "id", direction: "desc" },
            ],
            limit: 25,
            cardinality: "many",
        });
    });

    test("keeps IN, between, and direct ordering identical across compiler frontends", async () => {
        const directPlans: ChardbSelectPlanV1[] = [];
        const select = createBindingSelect(async plan => {
            directPlans.push(plan);
            return [];
        });
        const predicate = and(
            eq(plannedRows.namespace, "public"),
            inArray(plannedRows.channelId, ["news", "alerts"]),
            between(plannedRows.createdAt, 10, 20)
        ) as ReturnType<typeof eq>;
        await select().from(plannedRows).where(predicate).orderBy(plannedRows.id).limit(50);

        const registered = compileRegisteredQueryPlan(
            (db: QueryBuilder) => db.select().from(plannedRows).where(predicate).orderBy(plannedRows.id).limit(50),
            {}
        );
        const direct = directPlans[0];
        if (!direct) throw new Error("direct compiler did not execute");
        if (registered.kind !== "select") throw new Error("select fixture compiled a vector plan");
        expect(registered.plan).toEqual(direct);
    });

    test("uses the canonical compiler for every supported predicate operator", () => {
        const compiled = compileRegisteredQueryPlan(
            (db: QueryBuilder) =>
                db
                    .select()
                    .from(plannedRows)
                    .where(
                        and(
                            eq(plannedRows.namespace, "public"),
                            ne(plannedRows.channelId, "blocked"),
                            gt(plannedRows.createdAt, 0),
                            gte(plannedRows.createdAt, 1),
                            lt(plannedRows.createdAt, 100),
                            lte(plannedRows.createdAt, 99),
                            inArray(plannedRows.channelId, ["news", "alerts"]),
                            between(plannedRows.createdAt, 10, 20),
                            isNull(plannedRows.channelId),
                            isNotNull(plannedRows.id),
                            or(eq(plannedRows.channelId, "news"), eq(plannedRows.channelId, "alerts"))
                        )
                    )
                    .orderBy(plannedRows.id)
                    .limit(50),
            {}
        );

        if (compiled.kind !== "select") throw new Error("select fixture compiled a vector plan");
        expect(compiled.plan.where).toEqual({
            kind: "and",
            predicates: [
                { kind: "compare", op: "eq", column: "namespace", value: "public" },
                { kind: "compare", op: "ne", column: "channel_id", value: "blocked" },
                { kind: "compare", op: "gt", column: "created_at", value: 0 },
                { kind: "compare", op: "gte", column: "created_at", value: 1 },
                { kind: "compare", op: "lt", column: "created_at", value: 100 },
                { kind: "compare", op: "lte", column: "created_at", value: 99 },
                { kind: "in", column: "channel_id", values: ["news", "alerts"] },
                { kind: "between", column: "created_at", lower: 10, upper: 20 },
                { kind: "null", op: "isNull", column: "channel_id" },
                { kind: "null", op: "isNotNull", column: "id" },
                {
                    kind: "or",
                    predicates: [
                        { kind: "compare", op: "eq", column: "channel_id", value: "news" },
                        { kind: "compare", op: "eq", column: "channel_id", value: "alerts" },
                    ],
                },
            ],
        });
    });

    test("manifest routing uses the compiled plan and incorporates its hash", () => {
        const manifest = manifestFromExports({ listPlannedRows });
        const first = routeValidatedQuery(
            manifest,
            { ref: listPlannedRows.__chardbRef, args: { namespace: "public", channelId: "news", limit: 25 } },
            tables => `policy:${tables.join(",")}`
        );
        const second = routeValidatedQuery(
            manifest,
            { ref: listPlannedRows.__chardbRef, args: { namespace: "public", channelId: "other", limit: 25 } },
            tables => `policy:${tables.join(",")}`
        );
        expect(first.authority).toBe("global");
        expect(first.partitionKey).toBe("public");
        expect(first.intent.intervals?.map(interval => interval.indexName)).toEqual(["namespace", "channel_id"]);
        expect(first.queryHash).not.toBe(second.queryHash);
    });

    test("rejects a changed select plan that retains its old compiler hash", () => {
        const manifest = manifestFromExports({ listPlannedRows });
        const descriptor = manifest.queries.get(listPlannedRows.__chardbRef);
        if (!descriptor) throw new Error("planned query fixture omitted its manifest descriptor");
        const compilePlan = descriptor.compilePlan;
        (manifest.queries as Map<ChardbRef, typeof descriptor>).set(listPlannedRows.__chardbRef, {
            ...descriptor,
            compilePlan: args => {
                const compiled = compilePlan(args);
                if (compiled.kind !== "select") throw new Error("select fixture compiled a vector plan");
                return {
                    ...compiled,
                    plan: {
                        ...compiled.plan,
                        where: { kind: "compare", op: "eq", column: "namespace", value: "private" } as const,
                    },
                };
            },
        });

        expect(() =>
            routeValidatedQuery(
                manifest,
                {
                    ref: listPlannedRows.__chardbRef,
                    args: { namespace: "public", channelId: "news", limit: 25 },
                },
                () => "policy"
            )
        ).toThrow("canonical select plan disagrees with its compiler metadata");
    });

    test("derives user authority from table metadata", () => {
        const query = createApi({ plannedUserRows }).query({
            ref: "api/planned#user-list",
            query: (db, args: { userId: string }) =>
                db
                    .select()
                    .from(plannedUserRows)
                    .where(eq(plannedUserRows.userId, args.userId))
                    .orderBy(plannedUserRows.id)
                    .limit(10),
        });
        expect(query.__chardbCompilePlan?.({ userId: "user-1" })).toMatchObject({
            authority: "user",
            partitionKey: "user-1",
        });
    });

    test("rejects promises, projections, raw predicates, multi-partition reads, unstable order, and bad limits", () => {
        expect(() => compileRegisteredQueryPlan(async () => [], { namespace: "public" })).toThrow(
            "return a builder synchronously"
        );
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder, args: { namespace: string }) =>
                    db
                        .select({ id: plannedRows.id })
                        .from(plannedRows)
                        .where(eq(plannedRows.namespace, args.namespace))
                        .orderBy(plannedRows.id)
                        .limit(10),
                { namespace: "public" }
            )
        ).toThrow("explicit projections");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder, args: { namespace: string }) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(and(eq(plannedRows.namespace, args.namespace), sql`random()`))
                        .orderBy(plannedRows.id)
                        .limit(10),
                { namespace: "public" }
            )
        ).toThrow("outside the bounded");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder, args: { namespace: string }) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(
                            and(eq(plannedRows.namespace, args.namespace), sql`${plannedRows.createdAt} + ${1} > ${2}`)
                        )
                        .orderBy(plannedRows.id)
                        .limit(10),
                { namespace: "public" }
            )
        ).toThrow("outside the bounded");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(inArray(plannedRows.namespace, ["a", "b"]))
                        .orderBy(plannedRows.id)
                        .limit(10),
                {}
            )
        ).toThrow("one nonempty string");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(
                            and(
                                eq(plannedRows.namespace, "public"),
                                inArray(
                                    plannedRows.channelId,
                                    Array.from({ length: 101 }, (_, index) => String(index))
                                )
                            )
                        )
                        .orderBy(plannedRows.id)
                        .limit(10),
                {}
            )
        ).toThrow("1 through 100 values");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(eq(plannedRows.namespace, "public"))
                        .orderBy(plannedRows.createdAt)
                        .limit(10),
                {}
            )
        ).toThrow("must end with primary key");
        expect(() =>
            compileRegisteredQueryPlan(
                (db: QueryBuilder) =>
                    db
                        .select()
                        .from(plannedRows)
                        .where(eq(plannedRows.namespace, "public"))
                        .orderBy(plannedRows.id)
                        .limit(101),
                {}
            )
        ).toThrow("1 through 100");
    });
});
