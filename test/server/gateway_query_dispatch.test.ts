import { describe, expect, test } from "bun:test";
import { CdbError } from "../../src/errors.ts";
import {
    type TrustedQueryDispatchDeps,
    dispatchTrustedQuery,
    projectCdbQueryResponse,
} from "../../src/server/do/gateway-auth-dispatch.ts";
import type { CdbQueryRequest } from "../../src/server/rpc.ts";
import { ChardbRef, PrincipalId, type RawJson, ShardId, TenantId } from "../../src/types.ts";
import { vshardOf } from "../../src/vshard.ts";

const args = { organizationId: "org-1", limit: 25 } as const;
const request = {
    principalId: PrincipalId("user-1"),
    ref: "queries.ts#listMessages",
    args,
} as const;

const authority = {
    principalId: PrincipalId("user-1"),
    organizationId: TenantId("org-1"),
    role: "admin,member",
    roles: ["admin", "member"],
    authEpochs: { global: 2, tenant: 3, principal: 4 },
    recoveryGeneration: 0,
} as const;

function workingDeps(): TrustedQueryDispatchDeps {
    return {
        async routeQuery(input) {
            expect(input).toEqual({ ref: request.ref, args });
            return {
                ok: true,
                args: input.args,
                intent: {
                    kind: "select",
                    tables: ["messages"],
                    partitionKey: {
                        table: "messages",
                        column: "organization_id",
                        values: ["org-1"],
                    },
                    joinShape: "colocated",
                },
                policyDigest: "policy-1",
                queryHash: "query-1",
                authority: "organization",
                partitionKey: "org-1",
            };
        },
        catalog: {
            async resolveOrganizationAuthority(input) {
                expect(input).toEqual({ principalId: PrincipalId("user-1"), organizationId: TenantId("org-1") });
                return authority;
            },
            async route(vshard) {
                expect(vshard).toBe(Number(vshardOf(["org-1"])));
                return {
                    shardId: ShardId("shard-a"),
                    schemaEpoch: 9,
                    recoveryGeneration: 0,
                    domainSchemaEpoch: 5,
                };
            },
        },
        cdb(shardId) {
            expect(shardId).toBe("shard-a");
            return {
                async query(input) {
                    expect(input).toEqual<CdbQueryRequest>({
                        ref: ChardbRef("queries.ts#listMessages"),
                        args,
                        placement: { authority: "organization", partitionKey: "org-1" },
                        auth: {
                            userId: "user-1",
                            tenantId: "org-1",
                            role: "admin,member",
                            roles: ["admin", "member"],
                            authEpochs: { global: 2, tenant: 3, principal: 4 },
                            claims: {},
                        },
                        schemaEpoch: 9,
                        recoveryGeneration: 0,
                        domainSchemaEpoch: 5,
                    });
                    return { ok: true, result: [{ id: "message-1" }] };
                },
            };
        },
    };
}

describe("trusted one-shot query dispatch", () => {
    test("refreshes Catalog authority and placement once after a typed stale-route response", async () => {
        const attempts: CdbQueryRequest[] = [];
        let catalogCalls = 0;
        const result = await dispatchTrustedQuery(
            {
                ...workingDeps(),
                catalog: {
                    async resolveOrganizationAuthority() {
                        throw new Error("combined authority-route RPC must be used");
                    },
                    async route() {
                        throw new Error("combined authority-route RPC must be used");
                    },
                    async resolveOrganizationAuthorityRoute() {
                        catalogCalls += 1;
                        return {
                            authority: {
                                ...authority,
                                role: catalogCalls === 1 ? "member" : "admin",
                                roles: [catalogCalls === 1 ? "member" : "admin"],
                            },
                            route: {
                                shardId: ShardId(catalogCalls === 1 ? "source" : "destination"),
                                schemaEpoch: catalogCalls,
                                recoveryGeneration: 0,
                                domainSchemaEpoch: 5,
                            },
                        };
                    },
                },
                cdb: shardId => ({
                    async query(input) {
                        attempts.push(input);
                        return shardId === "source"
                            ? {
                                  ok: false,
                                  error: new CdbError({
                                      code: "CDB_STALE_EPOCH",
                                      message: "source cut over",
                                  }).toJSON(),
                              }
                            : { ok: true, result: [{ id: "destination-row" }] };
                    },
                }),
            },
            request
        );

        expect(result).toEqual({ ok: true, result: [{ id: "destination-row" }] });
        expect(catalogCalls).toBe(2);
        expect(attempts.map(attempt => [attempt.args, attempt.schemaEpoch, attempt.auth.role])).toEqual([
            [request.args, 1, "member"],
            [request.args, 2, "admin"],
        ]);
    });

    test("stops after two stale query attempts and does not retry terminal or unavailable failures", async () => {
        const stale = new CdbError({ code: "CDB_STALE_EPOCH", message: "still stale" }).toJSON();
        let staleCalls = 0;
        const staleResult = await dispatchTrustedQuery(
            {
                ...workingDeps(),
                cdb: () => ({
                    async query() {
                        staleCalls += 1;
                        return { ok: false, error: stale };
                    },
                }),
            },
            request
        );
        expect(staleResult).toEqual({ ok: false, error: stale });
        expect(staleCalls).toBe(2);

        for (const failure of [
            { kind: "terminal", response: new CdbError({ code: "CDB_FORBIDDEN", message: "denied" }).toJSON() },
            { kind: "unavailable", response: null },
        ] as const) {
            let calls = 0;
            const result = await dispatchTrustedQuery(
                {
                    ...workingDeps(),
                    cdb: () => ({
                        async query() {
                            calls += 1;
                            if (failure.response === null) throw new Error("offline");
                            return { ok: false, error: failure.response };
                        },
                    }),
                },
                request
            );
            expect(result).toMatchObject({
                ok: false,
                error: { code: failure.kind === "terminal" ? "CDB_FORBIDDEN" : "CDB_SHARD_UNAVAILABLE" },
            });
            expect(calls).toBe(1);
        }
    });

    test("uses one Catalog RPC for organization authority and placement", async () => {
        const base = workingDeps();
        let combinedCalls = 0;
        const result = await dispatchTrustedQuery(
            {
                ...base,
                catalog: {
                    async resolveOrganizationAuthority() {
                        throw new Error("legacy authority RPC must not run");
                    },
                    async route() {
                        throw new Error("legacy route RPC must not run");
                    },
                    async resolveOrganizationAuthorityRoute(input) {
                        combinedCalls += 1;
                        expect(input).toEqual({
                            principalId: PrincipalId("user-1"),
                            organizationId: TenantId("org-1"),
                            vshard: Number(vshardOf(["org-1"])),
                        });
                        return {
                            authority,
                            route: {
                                shardId: ShardId("shard-a"),
                                schemaEpoch: 9,
                                recoveryGeneration: 0,
                                domainSchemaEpoch: 5,
                            },
                        };
                    },
                },
            },
            request
        );

        expect(result).toEqual({ ok: true, result: [{ id: "message-1" }] });
        expect(combinedCalls).toBe(1);
    });

    test("preserves the retryable migration fence from the combined Catalog boundary", async () => {
        const base = workingDeps();
        let cdbCalls = 0;
        const result = await dispatchTrustedQuery(
            {
                ...base,
                catalog: {
                    async resolveOrganizationAuthority() {
                        throw new Error("legacy authority RPC must not run");
                    },
                    async route() {
                        throw new Error("legacy route RPC must not run");
                    },
                    async resolveOrganizationAuthorityRoute() {
                        throw new CdbError({
                            code: "CDB_STALE_EPOCH",
                            message: "schema migration deploy-7 is in progress",
                        });
                    },
                },
                cdb() {
                    cdbCalls += 1;
                    return base.cdb("shard-a");
                },
            },
            request
        );

        expect(result).toMatchObject({
            ok: false,
            error: {
                code: "CDB_STALE_EPOCH",
                retryable: true,
                message: "schema migration deploy-7 is in progress",
            },
        });
        expect(cdbCalls).toBe(0);
    });

    test("derives organization auth and placement before executing Cdb", async () => {
        await expect(dispatchTrustedQuery(workingDeps(), request)).resolves.toEqual({
            ok: true,
            result: [{ id: "message-1" }],
        });
    });

    test("binds user queries to the verified subject", async () => {
        const deps: TrustedQueryDispatchDeps = {
            routeQuery: async input => ({
                ok: true,
                args: input.args,
                intent: {
                    kind: "select",
                    tables: ["preferences"],
                    partitionKey: { table: "preferences", column: "user_id", values: ["user-1"] },
                },
                policyDigest: "user-policy",
                queryHash: "user-query",
                authority: "user",
                partitionKey: "user-1",
            }),
            catalog: {
                resolveOrganizationAuthority: async () => null,
                resolveUserAuthority: async input => ({
                    principalId: input.principalId,
                    role: "user",
                    roles: ["user"],
                    authEpochs: { global: 2, tenant: 0, principal: 7 },
                    recoveryGeneration: 0,
                }),
                route: async () => ({
                    shardId: ShardId("user-shard"),
                    schemaEpoch: 3,
                    recoveryGeneration: 0,
                    domainSchemaEpoch: 4,
                }),
            },
            cdb: () => ({
                query: async input => {
                    expect(input.auth).toEqual({
                        userId: "user-1",
                        role: "user",
                        roles: ["user"],
                        authEpochs: { global: 2, tenant: 0, principal: 7 },
                        claims: {},
                    });
                    return { ok: true, result: [{ theme: "dark" }] };
                },
            }),
        };

        await expect(dispatchTrustedQuery(deps, request)).resolves.toEqual({
            ok: true,
            result: [{ theme: "dark" }],
        });
        const forgedDeps: TrustedQueryDispatchDeps = {
            ...deps,
            routeQuery: async input => ({
                ok: true,
                args: input.args,
                intent: {
                    kind: "select",
                    tables: ["preferences"],
                    partitionKey: { table: "preferences", column: "user_id", values: ["user-2"] },
                },
                policyDigest: "user-policy",
                queryHash: "forged-user-query",
                authority: "user",
                partitionKey: "user-2",
            }),
        };
        await expect(dispatchTrustedQuery(forgedDeps, request)).resolves.toMatchObject({
            ok: false,
            error: { code: "CDB_FORBIDDEN" },
        });
    });

    test("routes a global partition without binding it to the JWT subject", async () => {
        const deps: TrustedQueryDispatchDeps = {
            routeQuery: async input => ({
                ok: true,
                args: input.args,
                intent: {
                    kind: "select",
                    tables: ["settings"],
                    partitionKey: { table: "settings", column: "scope", values: ["settings-v1"] },
                },
                policyDigest: "global-policy",
                queryHash: "global-query",
                authority: "global",
                partitionKey: "settings-v1",
            }),
            catalog: {
                resolveOrganizationAuthority: async () => null,
                resolveUserAuthority: async input => ({
                    principalId: input.principalId,
                    role: "admin",
                    roles: ["admin"],
                    authEpochs: { global: 6, tenant: 0, principal: 8 },
                    recoveryGeneration: 0,
                }),
                route: async () => ({
                    shardId: ShardId("global-shard"),
                    schemaEpoch: 4,
                    recoveryGeneration: 0,
                    domainSchemaEpoch: 5,
                }),
            },
            cdb: () => ({
                query: async input => {
                    expect(input.placement).toEqual({ authority: "global", partitionKey: "settings-v1" });
                    expect(input.auth.userId).toBe("user-1");
                    expect(input.auth.tenantId).toBeUndefined();
                    return { ok: true, result: [{ enabled: true }] };
                },
            }),
        };

        await expect(dispatchTrustedQuery(deps, request)).resolves.toEqual({
            ok: true,
            result: [{ enabled: true }],
        });
    });

    test("rejects caller-controlled or cross-partition intent before Catalog", async () => {
        let catalogCalls = 0;
        for (const routed of [
            { authority: null, partitionKey: "org-1", values: ["org-1"], joinShape: "colocated" },
            { authority: "organization", partitionKey: "org-1", values: ["org-2"], joinShape: "colocated" },
            { authority: "organization", partitionKey: "org-1", values: ["org-1"], joinShape: "cross-partition" },
        ] as const) {
            const deps: TrustedQueryDispatchDeps = {
                ...workingDeps(),
                routeQuery: async input => ({
                    ok: true,
                    args: input.args,
                    intent: {
                        kind: "select",
                        tables: ["messages"],
                        partitionKey: {
                            table: "messages",
                            column: "organization_id",
                            values: [...routed.values],
                        },
                        joinShape: routed.joinShape,
                    },
                    policyDigest: "policy-1",
                    queryHash: "query-1",
                    authority: routed.authority,
                    partitionKey: routed.partitionKey,
                }),
                catalog: {
                    async resolveOrganizationAuthority() {
                        catalogCalls++;
                        return authority;
                    },
                    async route() {
                        catalogCalls++;
                        return {
                            shardId: ShardId("unused"),
                            schemaEpoch: 1,
                            recoveryGeneration: 0,
                            domainSchemaEpoch: 1,
                        };
                    },
                },
            };
            const result = await dispatchTrustedQuery(deps, request);
            expect(result).toMatchObject({
                ok: false,
                error: { code: routed.authority === null ? "CDB_AUTH_NOT_BOUND" : "CDB_CROSS_PARTITION" },
            });
        }
        expect(catalogCalls).toBe(0);
    });

    test("returns stable failures for missing membership and unavailable boundaries", async () => {
        await expect(
            dispatchTrustedQuery(
                {
                    ...workingDeps(),
                    catalog: {
                        async resolveOrganizationAuthority() {
                            return null;
                        },
                        async route() {
                            throw new Error("must not route");
                        },
                    },
                },
                request
            )
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_FORBIDDEN", retryable: false } });

        await expect(
            dispatchTrustedQuery(
                {
                    ...workingDeps(),
                    catalog: {
                        async resolveOrganizationAuthority() {
                            return authority;
                        },
                        async route() {
                            throw new Error("offline");
                        },
                    },
                },
                request
            )
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_CATALOG_UNAVAILABLE", retryable: true } });

        await expect(
            dispatchTrustedQuery(
                {
                    ...workingDeps(),
                    cdb() {
                        return {
                            async query() {
                                throw new Error("offline");
                            },
                        };
                    },
                },
                request
            )
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_SHARD_UNAVAILABLE", retryable: true } });
    });

    test("owns routed arguments across an async Catalog boundary", async () => {
        const mutable = { organizationId: "org-1", limit: 25 };
        let release!: () => void;
        let started!: () => void;
        const held = new Promise<void>(resolve => {
            release = resolve;
        });
        const routed = new Promise<void>(resolve => {
            started = resolve;
        });
        let observed: RawJson | undefined;
        const deps: TrustedQueryDispatchDeps = {
            ...workingDeps(),
            routeQuery: async () => ({
                ok: true,
                args: mutable,
                intent: {
                    kind: "select",
                    tables: ["messages"],
                    partitionKey: { table: "messages", column: "organization_id", values: ["org-1"] },
                    joinShape: "colocated",
                },
                policyDigest: "policy-1",
                queryHash: "query-1",
                authority: "organization",
                partitionKey: "org-1",
            }),
            catalog: {
                async resolveOrganizationAuthority() {
                    return authority;
                },
                async route() {
                    started();
                    await held;
                    return {
                        shardId: ShardId("shard-a"),
                        schemaEpoch: 1,
                        recoveryGeneration: 0,
                        domainSchemaEpoch: 1,
                    };
                },
            },
            cdb: () => ({
                async query(input) {
                    observed = input.args;
                    return { ok: true, result: null };
                },
            }),
        };
        const pending = dispatchTrustedQuery(deps, request);
        await routed;
        mutable.organizationId = "org-forged";
        mutable.limit = 999;
        release();
        await expect(pending).resolves.toEqual({ ok: true, result: null });
        expect(observed).toEqual(args);
    });

    test("retains the admitted subject and query while routing is pending", async () => {
        const mutableRequest = { ...request, ref: request.ref as string };
        const base = workingDeps();
        const authorizedPrincipals: PrincipalId[] = [];
        const queries: CdbQueryRequest[] = [];
        let release!: () => void;
        const held = new Promise<void>(resolve => {
            release = resolve;
        });
        const pending = dispatchTrustedQuery(
            {
                ...base,
                async routeQuery(input) {
                    await held;
                    return base.routeQuery(input);
                },
                catalog: {
                    ...base.catalog,
                    async resolveOrganizationAuthority(input) {
                        authorizedPrincipals.push(input.principalId);
                        return { ...authority, principalId: input.principalId };
                    },
                },
                cdb: () => ({
                    async query(input) {
                        queries.push(input);
                        return { ok: true, result: [{ id: "message-1" }] };
                    },
                }),
            },
            mutableRequest
        );
        mutableRequest.principalId = PrincipalId("user-2");
        mutableRequest.ref = "queries.ts#privateMessages";
        release();

        await expect(pending).resolves.toEqual({ ok: true, result: [{ id: "message-1" }] });
        expect(authorizedPrincipals).toEqual([request.principalId]);
        expect(queries.map(query => [query.ref, query.auth.userId])).toEqual([[request.ref, request.principalId]]);
    });

    test("validates query RPC envelopes without requiring array results", () => {
        expect(projectCdbQueryResponse({ ok: true, result: { count: 3 } })).toEqual({
            ok: true,
            result: { count: 3 },
        });
        expect(projectCdbQueryResponse({ ok: true, result: new Date() })).toMatchObject({
            ok: false,
            error: { code: "CDB_INVARIANT" },
        });
        expect(
            projectCdbQueryResponse({
                ok: false,
                error: new CdbError({ code: "CDB_FORBIDDEN", message: "no membership" }).toJSON(),
            })
        ).toMatchObject({ ok: false, error: { code: "CDB_FORBIDDEN" } });
    });
});
