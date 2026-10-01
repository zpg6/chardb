import { DurableObject } from "cloudflare:workers";
import { jwt } from "better-auth/plugins/jwt";
import { and, eq, gte } from "drizzle-orm";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { z } from "zod";
import { isCdbError } from "../../src/errors.ts";
import { cdbPolicyDigest } from "../../src/server/cdb-policy.ts";
import { chardb } from "../../src/server/chardb.ts";
import { createApi } from "../../src/server/define.ts";
import { manifestFromExports, routeMutation, routeValidatedQuery } from "../../src/server/manifest.ts";
import type {
    CdbMutationRequest,
    CdbMutationResponse,
    CdbRegisteredQueryRequest,
    CdbSubscriptionRequest,
    GatewayInvalidationRequest,
    GatewayInvalidationResponse,
    LiveSubscriptionId,
} from "../../src/server/rpc.ts";
import { ChardbRef, ClientId, PrincipalId, SubId, TenantId } from "../../src/types.ts";
import { vshardOf } from "../../src/vshard.ts";
import { forUser } from "../helpers/cdb-table.ts";

const authUser = sqliteTable("user", { id: text("id").primaryKey() });
const { cdbTable } = forUser();
const entries = cdbTable(
    "registry_entries",
    {
        id: text("id").primaryKey(),
        ownerId: text("owner_id")
            .notNull()
            .references(() => authUser.id),
        value: integer("value").notNull(),
    },
    { tenantBy: "ownerId", partitionBy: "ownerId", roles: { self: { create: "*", read: "*" } } }
);
const schema = { entries };
const api = createApi(schema);

const putEntry = api.mutation({
    args: z.object({ id: z.string().min(1), value: z.number().int() }),
    handler: function putEntry(ctx, args) {
        if (args.id === "explode") throw new Error("unexpected handler failure");
        ctx.db.insert(entries).values({ id: args.id, ownerId: ctx.auth.userId, value: args.value }).run();
        return {
            saved: { id: args.id, value: args.value },
            actor: {
                userId: ctx.auth.userId,
                tenantId: ctx.auth.tenantId ?? null,
                role: ctx.auth.role ?? null,
                probeClaim: ctx.auth.claims.probe ?? null,
            },
        };
    },
});

const putRoutedEntry = api.mutation({
    ref: "src/probe.ts#putRoutedEntry",
    authority: "user",
    partitionKey: "ownerId",
    args: z.object({ id: z.string().min(1), ownerId: z.string().min(1), value: z.number().int() }),
    handler: function putRoutedEntry(ctx, args) {
        ctx.db.insert(entries).values(args).run();
        return args.id;
    },
});

const inspectEntries = api.mutation({
    args: z.object({ label: z.string() }),
    handler: function inspectEntries(ctx, args) {
        const rows = ctx.db.select().from(entries).orderBy(entries.id).all();
        return {
            label: args.label,
            rows,
            reader: ctx.auth.userId,
        };
    },
});

const rawAfterTyped = api.mutation({
    args: z.object({ id: z.string().min(1), value: z.number().int() }),
    handler: function rawAfterTyped(ctx, args) {
        ctx.db.insert(entries).values({ id: args.id, ownerId: ctx.auth.userId, value: args.value }).run();
        ctx.db.run('DELETE FROM "registry_entries"');
        return args.id;
    },
});

const putPair = api.mutation({
    ref: "src/probe.ts#putPair",
    args: z.object({ id: z.string(), secondId: z.string(), value: z.number().nullable() }),
    handler: (ctx, args) => {
        ctx.db.insert(entries).values({ id: args.id, ownerId: ctx.auth.userId, value: 1 }).run();
        ctx.db
            .insert(entries)
            .values({ id: args.secondId, ownerId: ctx.auth.userId, value: args.value as number })
            .run();
        return args.secondId;
    },
});

const registryEntries = api.query({
    ref: "queries.ts#registryEntries",
    args: z.object({ ownerId: z.string(), minimum: z.number().int() }),
    query: (db, args) =>
        db
            .select()
            .from(entries)
            .where(and(eq(entries.ownerId, args.ownerId), gte(entries.value, args.minimum)))
            .orderBy(entries.id)
            .limit(100),
});

const app = chardb({
    ownership: "user",
    auth: { plugins: [jwt()] },
    schema,
    api: { putEntry, putRoutedEntry, inspectEntries, rawAfterTyped, putPair, registryEntries },
});
const manifest = manifestFromExports({
    putEntry,
    putRoutedEntry,
    inspectEntries,
    rawAfterTyped,
    putPair,
    registryEntries,
});

interface StoredEntry extends Record<string, SqlStorageValue> {
    readonly id: string;
    readonly owner_id: string;
    readonly value: number;
}

interface CountRow extends Record<string, SqlStorageValue> {
    readonly count: number;
}

export class Cdb extends app.Cdb {
    corruptRegisteredQuery(registrationId: string, kind: "malformed" | "mismatch" | "mapping"): void {
        if (kind === "mapping") {
            this.ctx.storage.sql.exec(
                `DELETE FROM _chardb_live_subscription_tables
                 WHERE registration_id = ?`,
                registrationId
            );
            return;
        }
        this.ctx.storage.sql.exec(
            `UPDATE _chardb_live_subscriptions
             SET args_json = ?
             WHERE registration_id = ?`,
            kind === "malformed" ? "{" : '{"minimum":0}',
            registrationId
        );
    }

    async subscribeForProof(request: CdbSubscriptionRequest): Promise<Record<string, unknown>> {
        try {
            return { ok: true, result: await this.subscribe(request) };
        } catch (error) {
            return {
                ok: false,
                error: isCdbError(error)
                    ? error.toJSON()
                    : { code: "CDB_INVARIANT", message: error instanceof Error ? error.message : "unknown failure" },
            };
        }
    }

    inspectAtomicState(): {
        readonly entries: readonly StoredEntry[];
        readonly opLogRows: number;
        readonly changeSeq: number;
        readonly mappings: readonly Record<string, SqlStorageValue>[];
        readonly outbox: readonly Record<string, SqlStorageValue>[];
    } {
        const entries = [
            ...this.ctx.storage.sql.exec<StoredEntry>("SELECT id, owner_id, value FROM registry_entries ORDER BY id"),
        ];
        const count = [...this.ctx.storage.sql.exec<CountRow>("SELECT COUNT(*) AS count FROM _chardb_op_log")][0];
        const clock = [
            ...this.ctx.storage.sql.exec<{ change_seq: number }>(
                "SELECT change_seq FROM _chardb_change_clock WHERE singleton = 1"
            ),
        ][0];
        const mappings = [
            ...this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(
                `SELECT gateway_id, registration_id, table_name
                 FROM _chardb_live_subscription_tables
                 ORDER BY gateway_id, registration_id, table_name`
            ),
        ];
        const outbox = [
            ...this.ctx.storage.sql.exec<Record<string, SqlStorageValue>>(
                `SELECT gateway_id, registration_id, change_seq
                 FROM _chardb_invalidation_outbox
                 ORDER BY gateway_id, registration_id`
            ),
        ];
        return { entries, opLogRows: count?.count ?? 0, changeSeq: clock?.change_seq ?? -1, mappings, outbox };
    }
}

export class Resharder extends app.Resharder {}

export class InvalidationGateway extends DurableObject<Record<string, never>> {
    constructor(state: DurableObjectState, env: Record<string, never>) {
        super(state, env);
        state.blockConcurrencyWhile(async () => {
            this.ctx.storage.sql.exec(
                `CREATE TABLE IF NOT EXISTS invalidation_calls (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   request_json TEXT NOT NULL
                 )`
            );
        });
    }

    async invalidateSubscriptions(request: GatewayInvalidationRequest): Promise<GatewayInvalidationResponse> {
        this.ctx.storage.sql.exec("INSERT INTO invalidation_calls (request_json) VALUES (?)", JSON.stringify(request));
        return {
            gatewayId: request.gatewayId,
            acknowledgements: request.invalidations.map(invalidation => ({
                registrationId: invalidation.subscription.registrationId,
                changeSeq: invalidation.changeSeq,
                status: "accepted",
            })),
        };
    }

    inspectInvalidations(): readonly GatewayInvalidationRequest[] {
        return [
            ...this.ctx.storage.sql.exec<{ request_json: string }>(
                "SELECT request_json FROM invalidation_calls ORDER BY id"
            ),
        ].map(row => JSON.parse(row.request_json) as GatewayInvalidationRequest);
    }
}

interface DispatchBody {
    readonly operation: "put" | "routed" | "inspect" | "raw" | "pair" | "unknown";
    readonly mutId: string;
    readonly args: unknown;
    readonly schemaEpoch?: number;
}

const AUTH = {
    userId: "registry-user",
    tenantId: "registry-org",
    role: "member",
    roles: ["member"],
    claims: { probe: "claim-ok" },
} as const;

function subscriptionRequest(gatewayId: string): CdbSubscriptionRequest {
    return {
        subscription: {
            gatewayId,
            registrationId: "registry-registration",
            connectionId: "registry-connection",
            clientId: ClientId("registry-client"),
            subId: SubId(1),
        },
        principalId: PrincipalId(AUTH.userId),
        organizationId: TenantId(AUTH.tenantId),
        recoveryGeneration: 0,
        schemaEpoch: 1,
        vshard: Number(vshardOf([AUTH.tenantId])),
        domainSchemaEpoch: 1,
        ref: ChardbRef("queries.ts#registryEntries"),
        args: {},
        queryHash: "registry-invalidation-query-hash",
        tables: ["registry_entries"],
        intervals: [],
    };
}

function registeredSubscriptionRequest(gatewayId: string, registrationId: string): CdbSubscriptionRequest {
    const args = { ownerId: AUTH.userId, minimum: 60 };
    const routed = routeValidatedQuery(manifest, { ref: registryEntries.__chardbRef, args }, tables =>
        cdbPolicyDigest(schema, tables)
    );
    return {
        subscription: {
            gatewayId,
            registrationId,
            connectionId: `connection-${registrationId}`,
            clientId: ClientId(`client-${registrationId}`),
            subId: SubId(2),
        },
        principalId: PrincipalId(AUTH.userId),
        organizationId: TenantId(AUTH.userId),
        placement: { authority: "user", partitionKey: AUTH.userId },
        recoveryGeneration: 0,
        schemaEpoch: 1,
        vshard: Number(vshardOf([AUTH.userId])),
        domainSchemaEpoch: 1,
        ref: registryEntries.__chardbRef,
        args: routed.args,
        queryHash: routed.queryHash,
        tables: routed.intent.tables,
        intervals: [],
    };
}

interface RegistryEnv {
    readonly CDB: DurableObjectNamespace;
    readonly CDB_GATEWAY: DurableObjectNamespace;
}

export default {
    async fetch(request: Request, env: RegistryEnv): Promise<Response> {
        const id = env.CDB.idFromName(request.headers.get("x-fixture-shard") ?? "configured-registry");
        const gatewayId = env.CDB_GATEWAY.idFromName("registry-gateway");
        const subscribeRequest = subscriptionRequest(gatewayId.toString());
        const stub = env.CDB.get(id) as unknown as {
            mutate(input: CdbMutationRequest): Promise<CdbMutationResponse>;
            inspectAtomicState(): Promise<unknown>;
            corruptRegisteredQuery(registrationId: string, kind: "malformed" | "mismatch" | "mapping"): Promise<void>;
            subscribeForProof(input: CdbSubscriptionRequest): Promise<Record<string, unknown>>;
            subscribe(input: CdbSubscriptionRequest): Promise<unknown>;
            unsubscribe(input: { subscription: LiveSubscriptionId; recoveryGeneration: number }): Promise<void>;
            queryRegistered(input: CdbRegisteredQueryRequest): Promise<unknown>;
            prepareRoutingFence(input: RoutingFenceIdentity): Promise<unknown>;
            activateRoutingFence(input: RoutingFenceIdentity): Promise<unknown>;
            completeRoutingFenceCleanup(input: RoutingFenceIdentity): Promise<unknown>;
        };
        const url = new URL(request.url);
        if (url.pathname === "/state") return Response.json(await stub.inspectAtomicState());
        if (url.pathname === "/gateway-state") {
            const gateway = env.CDB_GATEWAY.get(gatewayId) as unknown as {
                inspectInvalidations(): Promise<readonly GatewayInvalidationRequest[]>;
            };
            return Response.json(await gateway.inspectInvalidations());
        }
        if (url.pathname === "/subscribe") {
            return Response.json(await stub.subscribe(subscribeRequest));
        }
        if (url.pathname === "/unsubscribe") {
            await stub.unsubscribe({ subscription: subscribeRequest.subscription, recoveryGeneration: 0 });
            return Response.json({ ok: true });
        }
        if (url.pathname.startsWith("/routing-fence/")) {
            const body = { ...((await request.json()) as RoutingFenceIdentity), recoveryGeneration: 0 };
            if (url.pathname === "/routing-fence/prepare") return Response.json(await stub.prepareRoutingFence(body));
            if (url.pathname === "/routing-fence/activate") {
                return Response.json(await stub.activateRoutingFence(body));
            }
            if (url.pathname === "/routing-fence/cleanup") {
                return Response.json(await stub.completeRoutingFenceCleanup(body));
            }
            return Response.json({ ok: false }, { status: 404 });
        }
        if (url.pathname.startsWith("/registered/")) {
            const body = (await request.json()) as {
                readonly registrationId: string;
                readonly forgedIdentity?: boolean;
                readonly forgedPrincipal?: boolean;
                readonly forgedPartition?: boolean;
                readonly forgedAuthority?: boolean;
                readonly corruption?: "malformed" | "mismatch" | "mapping";
            };
            const registered = registeredSubscriptionRequest(gatewayId.toString(), body.registrationId);
            if (url.pathname === "/registered/subscribe") {
                return Response.json(await stub.subscribeForProof(registered));
            }
            if (url.pathname === "/registered/unsubscribe") {
                await stub.unsubscribe({ subscription: registered.subscription, recoveryGeneration: 0 });
                return Response.json({ ok: true });
            }
            if (url.pathname === "/registered/corrupt") {
                if (!body.corruption) return Response.json({ ok: false }, { status: 400 });
                await stub.corruptRegisteredQuery(body.registrationId, body.corruption);
                return Response.json({ ok: true });
            }
            if (url.pathname === "/registered/query") {
                return Response.json(
                    await stub.queryRegistered({
                        subscription: body.forgedIdentity
                            ? { ...registered.subscription, connectionId: "forged-connection" }
                            : registered.subscription,
                        auth: {
                            ...AUTH,
                            userId: body.forgedPrincipal ? "forged-principal" : AUTH.userId,
                            claims: { probe: "fresh-query-auth" },
                        },
                        recoveryGeneration: 0,
                        schemaEpoch: 1,
                        vshard: Number(vshardOf([AUTH.userId])),
                        domainSchemaEpoch: 1,
                        placement: {
                            authority: body.forgedAuthority ? "organization" : "user",
                            partitionKey: body.forgedPartition ? "forged-partition" : AUTH.userId,
                        },
                    })
                );
            }
            return Response.json({ ok: false }, { status: 404 });
        }
        const body = (await request.json()) as DispatchBody;
        const ref =
            body.operation === "put"
                ? putEntry.__chardbRef
                : body.operation === "routed"
                  ? putRoutedEntry.__chardbRef
                  : body.operation === "inspect"
                    ? inspectEntries.__chardbRef
                    : body.operation === "raw"
                      ? rawAfterTyped.__chardbRef
                      : body.operation === "pair"
                        ? putPair.__chardbRef
                        : "src/probe.ts#missing";
        const route = routeMutation(manifest, { ref, args: body.args as CdbMutationRequest["args"] }, () => 0);
        if (!route.ok) return Response.json(route);
        const routedUserId =
            body.operation === "routed" &&
            typeof body.args === "object" &&
            body.args !== null &&
            typeof (body.args as { readonly ownerId?: unknown }).ownerId === "string"
                ? (body.args as { readonly ownerId: string }).ownerId
                : AUTH.userId;
        const requestAuth =
            body.operation === "routed"
                ? { userId: routedUserId, role: AUTH.role, roles: AUTH.roles, claims: AUTH.claims }
                : AUTH;
        const mutationRequest: CdbMutationRequest = {
            principalId: routedUserId,
            mutId: body.mutId,
            ref,
            args: route.args,
            ...(route.authority === null || route.partitionKey === null
                ? {}
                : { placement: { authority: route.authority, partitionKey: route.partitionKey } }),
            auth: requestAuth,
            recoveryGeneration: 0,
            schemaEpoch: body.schemaEpoch ?? 1,
            domainSchemaEpoch: 1,
        };
        return Response.json(await stub.mutate(mutationRequest));
    },
};

interface RoutingFenceIdentity {
    readonly migrationId: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly sourceGeneration: number;
    readonly destinationGeneration: number;
    readonly recoveryGeneration?: number;
}
