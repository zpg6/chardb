/**
 * `Gateway` DO — Hibernatable WebSockets
 * (https://developers.cloudflare.com/durable-objects/api/hibernatable-websockets-api/),
 * subscription registry.
 *
 * Sharded by the full `clientId` through 4,096 pinned-hash Gateway buckets.
 * The per-conn 2 KiB `serializeAttachment` payload carries verified subject,
 * expiry, client id, and resume state so a wake-up can recheck authentication
 * without trusting decode-only JWT claims.
 */

import { DurableObject } from "cloudflare:workers";
import { CdbError, docsUrlFor, isCdbErrorCode, isRetryable, rehydrateCdbRpcError } from "../../errors.ts";
import {
    type ChardbRef,
    ClientId,
    Cookie,
    CorrelationId,
    type MutId,
    PrincipalId,
    type RawJson,
    SubId,
    TenantId,
} from "../../types.ts";
import { stableJson } from "../../util/canonical.ts";
import { vshardOf } from "../../vshard.ts";
import { type CdbIntent, type Down, PROTOCOL_V, type Up, checkProtocolV, decodeUp, encodeWire } from "../../wire.ts";
import { cdbPolicyDigest } from "../cdb-policy.ts";
import type { MutationAuthority } from "../define.ts";
import { withChardbLoopbacks } from "../loopback.ts";
import {
    type ChardbManifest,
    type QueryRouteResponse,
    emptyManifest,
    routeMutation as resolveMutationRoute,
    routeQuery as resolveQueryRoute,
} from "../manifest.ts";
import { assertCdbMutationArgsByteLimit, snapshotCdbQueryArgs } from "../result_limits.ts";
import type {
    CatalogMutationRpc,
    CatalogOrganizationAuthorityRouteRpc,
    CatalogOrganizationAuthorityRpc,
    CatalogRoutingRpc,
    CatalogUserAuthorityRpc,
    CdbErrorWire,
    CdbMutationResponse,
    CdbMutationRpc,
    CdbSubscriptionRequest,
    CdbSubscriptionResponse,
    CdbSubscriptionRpc,
    GatewayInvalidationAck,
    GatewayInvalidationRequest,
    GatewayInvalidationResponse,
    LiveSubscriptionId,
    MutationRouteRequest,
    MutationRouteResponse,
    TrustedMutationAuth,
} from "../rpc.ts";
import { GatewayAlarmScheduler, nextGatewayWorkAt } from "./gateway-alarm-store.ts";
import {
    type GatewayJwtConfig,
    type GatewayJwtVerificationRequest,
    type VerifiedGwAttachment,
    dispatchTrustedMutation,
    isCurrentVerifiedAttachment,
    isVerifiedAttachment,
    mutationFailure,
    resolvePartitionAuthRoute,
    trustedMutationAuthFromAttachment,
    verifyGatewayJwt,
} from "./gateway-auth-dispatch.ts";
import { checkGatewayAuthorityFreshness } from "./gateway-authority-freshness.ts";
import {
    GATEWAY_ABANDONED_REGISTRATION_BATCH_SIZE,
    GATEWAY_ABANDONED_REGISTRATION_CURSOR_KEY,
    GATEWAY_CLEANUP_BASE_RETRY_MS,
    GATEWAY_CLEANUP_BATCH_SIZE,
    GATEWAY_CLEANUP_MAX_ERROR_LENGTH,
    GATEWAY_CLEANUP_MAX_RETRY_COUNT,
    GATEWAY_QUERY_BATCH_SIZE,
    GATEWAY_REGISTRATION_DDL,
    GATEWAY_SUBSCRIBE_RECOVERY_MS,
    type GatewayRegistrationCdbState,
    type GatewayRegistrationKey,
    type GatewayRegistrationLifecycle,
    type GatewaySnapshotReplay,
    type StoredGatewayActiveHead,
    type StoredGatewayCleanupRow,
    activateGatewaySubscription,
    armGatewaySubscriptionRecovery,
    deleteNeverRegisteredGatewaySubscription,
    ensureGatewayRegistrationColumns,
    ensureGatewaySnapshotOutboxColumns,
    gatewayInvalidationInvariant,
    gatewayRetryDelayMs,
    installGatewayRegistration,
    markPendingGatewaySubscriptionAmbiguous,
    pruneGatewaySnapshotReplays,
    resolveGatewaySnapshotReplay,
    resumeGatewayAuthDeferredWork,
    retainCurrentGatewaySnapshotReplay,
    retireCurrentGatewayRegistration,
    retireCurrentGatewayRegistrationsForConnection,
    retireGatewayRegistration,
} from "./gateway-registration-store.ts";
import {
    type GatewayExactSnapshotSocket,
    GatewaySnapshotDelivery,
    type GatewaySnapshotRetirement,
} from "./gateway-snapshot-delivery.ts";
import { GatewaySnapshotMaterializer } from "./gateway-snapshot-materializer.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

const GW_DDL = GATEWAY_REGISTRATION_DDL;
export const MAX_INITIAL_SNAPSHOTS_PER_CONNECTION = 64 as const;
export const MAX_GATEWAY_INVALIDATIONS_PER_REQUEST = 64 as const;
export const GATEWAY_AUTH_REFRESH_GRACE_MS = 60_000 as const;
// Match the Gateway-wide unsettled mutation budget so one isolate has a
// single aggregate concurrency scale for client-originated work.
const GATEWAY_MAX_CURRENT_AND_PENDING_REGISTRATIONS = 256;
// Preserve the platform's original 1 MiB WebSocket message ceiling even on
// runtimes that accept larger frames. File payloads belong on the upload path.
const GATEWAY_MAX_INBOUND_WEBSOCKET_BYTES = 1024 * 1024;
const GATEWAY_TEXT_ENCODER = new TextEncoder();

interface PendingGwAttachment {
    readonly kind: "pending";
    readonly connectionId: string;
    readonly authOrigin: string;
    readonly routedClientId: ClientId | null;
}

interface RejectedGwAttachment {
    readonly kind: "rejected";
    readonly connectionId: string;
    readonly authOrigin: string;
}

interface PendingSubscription {
    readonly connectionId: string;
    readonly subId: SubId;
    readonly capacityKey: string;
    cancelled: boolean;
    queued: boolean;
    readonly resumeReplayAttempt: boolean;
    task: Promise<void>;
}

interface StoredGatewayInstallRecovery {
    readonly principal_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly registration_id: string;
    readonly connection_id: string;
}

type GwAttachment = PendingGwAttachment | RejectedGwAttachment | VerifiedGwAttachment;

function gatewayAuthRefreshDeadlineMs(attachment: VerifiedGwAttachment): number | null {
    const expiresAt = attachment.jwtExp * 1_000;
    const deadline = expiresAt + GATEWAY_AUTH_REFRESH_GRACE_MS;
    return Number.isSafeInteger(deadline) && deadline >= 0 ? deadline : null;
}

export interface GatewayEnv {
    readonly CDB_CATALOG: DurableObjectNamespace;
    readonly CDB_SHARD: DurableObjectNamespace;
}

export interface GatewayRuntimeConfig {
    readonly schema: () => Record<string, unknown>;
    readonly manifest: () => ChardbManifest;
    readonly auth: GatewayJwtConfig | null;
}

/** Parse the Worker-routed client id without trusting the later hello payload. */
export function routedClientIdFromUrl(rawUrl: string): ClientId | null {
    let candidates: string[];
    try {
        candidates = new URL(rawUrl).searchParams.getAll("clientId");
    } catch {
        return null;
    }
    if (candidates.length !== 1) return null;
    const candidate = candidates[0] as string;
    if (
        candidate.length === 0 ||
        candidate.length > 256 ||
        candidate.trim() !== candidate ||
        hasAsciiControlCharacter(candidate)
    ) {
        return null;
    }
    return ClientId(candidate);
}

function hasAsciiControlCharacter(value: string): boolean {
    for (let index = 0; index < value.length; index++) {
        const code = value.charCodeAt(index);
        if (code <= 31 || code === 127) return true;
    }
    return false;
}

/** Build a Down.error envelope with the locked metadata for its code. */
export function gatewayErrorEnvelope(
    code: import("../../errors.ts").CdbErrorCode,
    correlationId: CorrelationId,
    subId?: SubId
): Extract<Down, { readonly t: "error" }> {
    return {
        t: "error",
        code,
        retryable: isRetryable(code),
        correlationId,
        docs: docsUrlFor(code),
        ...(subId !== undefined ? { subId } : {}),
    };
}

/** Build a generation-specific subscription identity from Gateway-owned values. */
export function gatewaySubscriptionId(
    gatewayId: string,
    registrationId: string,
    connectionId: string,
    clientId: ClientId,
    subId: SubId
): LiveSubscriptionId {
    return { gatewayId, registrationId, connectionId, clientId, subId };
}

/** Build the serializable Cdb subscription RPC from server-owned routing data. */
export function cdbSubscriptionRequest(input: {
    readonly gatewayId: string;
    readonly registrationId: string;
    readonly connectionId: string;
    readonly clientId: ClientId;
    readonly subId: SubId;
    readonly principalId: PrincipalId;
    readonly organizationId: TenantId;
    readonly authority?: MutationAuthority;
    readonly schemaEpoch: number;
    readonly recoveryGeneration: number;
    readonly vshard: number;
    readonly domainSchemaEpoch: number;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    readonly queryHash: string;
    readonly intent: CdbIntent;
}): CdbSubscriptionRequest {
    return {
        subscription: gatewaySubscriptionId(
            input.gatewayId,
            input.registrationId,
            input.connectionId,
            input.clientId,
            input.subId
        ),
        principalId: input.principalId,
        organizationId: input.organizationId,
        ...(input.authority === undefined
            ? {}
            : { placement: { authority: input.authority, partitionKey: input.organizationId } }),
        schemaEpoch: input.schemaEpoch,
        recoveryGeneration: input.recoveryGeneration,
        vshard: input.vshard,
        domainSchemaEpoch: input.domainSchemaEpoch,
        ref: input.ref,
        args: input.args,
        queryHash: input.queryHash,
        tables: [...input.intent.tables],
        intervals: (input.intent.intervals ?? []).map(bundle => ({
            table: bundle.table,
            indexName: bundle.indexName,
            intervals: bundle.intervals,
        })),
    };
}

export function projectCdbSubscriptionResponse(value: unknown, expected: LiveSubscriptionId): CdbSubscriptionResponse {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new TypeError("Cdb returned a malformed subscribe response");
    }
    const response = value as Record<string, unknown>;
    const subscription = response.subscription;
    if (typeof subscription !== "object" || subscription === null || Array.isArray(subscription)) {
        throw new TypeError("Cdb returned a malformed subscribe response");
    }
    const identity = subscription as Record<string, unknown>;
    if (
        !hasExactKeys(identity, ["gatewayId", "registrationId", "connectionId", "clientId", "subId"]) ||
        identity.gatewayId !== expected.gatewayId ||
        identity.registrationId !== expected.registrationId ||
        identity.connectionId !== expected.connectionId ||
        identity.clientId !== expected.clientId ||
        identity.subId !== expected.subId
    ) {
        throw new TypeError("Cdb returned a mismatched subscribe response");
    }
    if (response.ok === true) {
        if (
            !hasExactKeys(response, ["ok", "subscription", "changeSeq"]) ||
            !Number.isSafeInteger(response.changeSeq) ||
            (response.changeSeq as number) < 0
        ) {
            throw new TypeError("Cdb returned a malformed subscribe success");
        }
        return response as unknown as Extract<CdbSubscriptionResponse, { readonly ok: true }>;
    }
    if (response.ok !== false || !hasExactKeys(response, ["ok", "registrationState", "subscription", "error"])) {
        throw new TypeError("Cdb returned a malformed subscribe response");
    }
    if (response.registrationState !== "absent") {
        throw new TypeError("Cdb returned a malformed subscribe failure state");
    }
    const error = response.error;
    if (typeof error !== "object" || error === null || Array.isArray(error)) {
        throw new TypeError("Cdb returned a malformed subscribe failure");
    }
    const wire = error as Record<string, unknown>;
    if (
        !hasExactKeys(wire, ["code", "retryable", "message", "correlationId", "docs", "retryAfterMs", "hint"]) ||
        !isCdbErrorCode(wire.code) ||
        wire.retryable !== isRetryable(wire.code) ||
        typeof wire.message !== "string" ||
        wire.docs !== docsUrlFor(wire.code) ||
        (wire.correlationId !== undefined && typeof wire.correlationId !== "string") ||
        (wire.retryAfterMs !== undefined &&
            (!Number.isSafeInteger(wire.retryAfterMs) ||
                (wire.retryAfterMs as number) < 0 ||
                (wire.retryAfterMs as number) > 2_147_483_647)) ||
        (wire.hint !== undefined && typeof wire.hint !== "string")
    ) {
        throw new TypeError("Cdb returned a malformed subscribe failure");
    }
    return response as unknown as Extract<CdbSubscriptionResponse, { readonly ok: false }>;
}

/**
 * Install a new durable generation. The caller must wrap this helper in the
 * Gateway storage transaction so the old generation and head move atomically.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
    const actual = Object.keys(value).sort();
    const expected = [...keys].sort();
    return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function isBoundedIdentity(value: unknown): value is string {
    return (
        typeof value === "string" &&
        value.length > 0 &&
        value.length <= 512 &&
        value.trim() === value &&
        !hasAsciiControlCharacter(value)
    );
}

function validateGatewayInvalidationRequest(value: unknown, expectedGatewayId: string): GatewayInvalidationRequest {
    if (!isRecord(value) || !hasExactKeys(value, ["sourceCdbId", "gatewayId", "invalidations"])) {
        throw gatewayInvalidationInvariant("Gateway invalidation request has an unexpected shape");
    }
    if (
        !isBoundedIdentity(value.sourceCdbId) ||
        !isBoundedIdentity(value.gatewayId) ||
        value.gatewayId !== expectedGatewayId ||
        !Array.isArray(value.invalidations) ||
        value.invalidations.length === 0 ||
        value.invalidations.length > MAX_GATEWAY_INVALIDATIONS_PER_REQUEST
    ) {
        throw gatewayInvalidationInvariant("Gateway invalidation request is malformed or misrouted");
    }

    const seen = new Set<string>();
    for (const item of value.invalidations) {
        if (!isRecord(item) || !hasExactKeys(item, ["subscription", "changeSeq"])) {
            throw gatewayInvalidationInvariant("Gateway invalidation item has an unexpected shape");
        }
        const subscription = item.subscription;
        if (
            !isRecord(subscription) ||
            !hasExactKeys(subscription, ["gatewayId", "registrationId", "connectionId", "clientId", "subId"])
        ) {
            throw gatewayInvalidationInvariant("Gateway invalidation subscription has an unexpected shape");
        }
        if (
            subscription.gatewayId !== expectedGatewayId ||
            !isBoundedIdentity(subscription.registrationId) ||
            !isBoundedIdentity(subscription.connectionId) ||
            !isBoundedIdentity(subscription.clientId) ||
            !Number.isSafeInteger(subscription.subId) ||
            (subscription.subId as number) < 0 ||
            !Number.isSafeInteger(item.changeSeq) ||
            (item.changeSeq as number) <= 0 ||
            seen.has(subscription.registrationId)
        ) {
            throw gatewayInvalidationInvariant("Gateway invalidation identity, sequence, or uniqueness is invalid");
        }
        seen.add(subscription.registrationId);
    }
    return value as unknown as GatewayInvalidationRequest;
}

export class Gateway extends DurableObject<GatewayEnv> {
    private static readonly MAX_UNSETTLED_MUTATIONS_PER_CONNECTION = 32;
    private static readonly MAX_UNSETTLED_MUTATIONS = 256;

    private readonly alarmScheduler: GatewayAlarmScheduler;
    private readonly snapshotDelivery: GatewaySnapshotDelivery;
    private readonly snapshotMaterializer: GatewaySnapshotMaterializer;
    private readonly authOperationClaims = new Map<string, object>();
    private readonly authRefreshBarriers = new Map<string, Promise<boolean>>();
    private readonly authRefreshDrainConnections = new Set<string>();
    private readonly activeOperations = new Map<string, Set<Promise<void>>>();
    private readonly pendingSubscriptions = new Map<string, PendingSubscription>();
    private readonly unsettledMutationsByConnection = new Map<string, number>();
    private gatewayWorkDrain: Promise<void> | null = null;
    private gatewayWorkDrainRequested = false;
    private unsettledMutationCount = 0;

    constructor(state: DurableObjectState, env: GatewayEnv) {
        super(state, withChardbLoopbacks(env, state));
        this.alarmScheduler = new GatewayAlarmScheduler(state.storage);
        const snapshotStorage = {
            sql: adaptSqlStorage(state.storage.sql),
            transactionSync: <T>(callback: () => T): T => state.storage.transactionSync(callback),
        };
        const snapshotSocket = (
            identity: GatewayRegistrationKey & { readonly connectionId: string },
            nowMs: number
        ): GatewayExactSnapshotSocket => this.exactGatewaySocket(identity, nowMs);
        const settleRetired = (
            identity: GatewayRegistrationKey & { readonly connectionId: string },
            settlement: GatewaySnapshotRetirement
        ): void => this.settleRetiredGatewaySubscription(identity, settlement);
        this.snapshotDelivery = new GatewaySnapshotDelivery({
            storage: snapshotStorage,
            nowMs: () => this.gatewayNowMs(),
            scheduleAlarm: requestedAt => this.scheduleGatewayAlarm(requestedAt),
            scheduleWork: nowMs => this.scheduleGatewayWork(nowMs),
            currentPolicyDigest: intentJson => this.currentGatewayPolicyDigest(intentJson),
            checkAuthority: attempt =>
                checkGatewayAuthorityFreshness(
                    {
                        shardNamespace: this.env.CDB_SHARD,
                        routeQuery: request => this.routeQuery(request),
                        catalog: () =>
                            this.catalog() as CatalogRoutingRpc &
                                CatalogOrganizationAuthorityRpc &
                                Partial<CatalogOrganizationAuthorityRouteRpc & CatalogUserAuthorityRpc>,
                    },
                    attempt
                ),
            exactSocket: snapshotSocket,
            settleRetired,
            send: (socket, message) => this.send(socket, message),
        });
        this.snapshotMaterializer = new GatewaySnapshotMaterializer({
            storage: snapshotStorage,
            shardNamespace: this.env.CDB_SHARD,
            gatewayId: this.ctx.id.toString(),
            nowMs: () => this.gatewayNowMs(),
            scheduleAlarm: requestedAt => this.scheduleGatewayAlarm(requestedAt),
            scheduleWork: nowMs => this.scheduleGatewayWork(nowMs),
            currentPolicyDigest: intentJson => this.currentGatewayPolicyDigest(intentJson),
            routeQuery: request => this.routeQuery(request),
            catalog: () =>
                this.catalog() as CatalogRoutingRpc &
                    CatalogOrganizationAuthorityRpc &
                    Partial<CatalogUserAuthorityRpc>,
            exactSocket: snapshotSocket,
            settleRetired,
            stageSnapshot: input => this.snapshotDelivery.stage(input),
        });
        state.blockConcurrencyWhile(() => this.bootstrap());
    }

    protected runtimeManifest(): ChardbManifest {
        return emptyManifest();
    }

    protected runtimePolicyDigest(tableNames: readonly string[]): string | null {
        void tableNames;
        return null;
    }

    private currentGatewayPolicyDigest(intentJson: string): string | null {
        try {
            const intent = JSON.parse(intentJson) as { readonly tables?: unknown };
            if (!Array.isArray(intent.tables) || !intent.tables.every(table => typeof table === "string")) {
                throw new TypeError("persisted Gateway query intent has invalid tables");
            }
            return this.runtimePolicyDigest(intent.tables);
        } catch {
            return "";
        }
    }

    protected jwtConfig(): GatewayJwtConfig | null {
        return null;
    }

    /** Resolve the registered mutation inside this Gateway isolate. */
    routeMutation(request: MutationRouteRequest): MutationRouteResponse {
        return resolveMutationRoute(this.runtimeManifest(), request, vshardOf);
    }

    /** Resolve query routing from the server manifest, never client hints. */
    routeQuery(request: { readonly ref: string; readonly args: RawJson }): Promise<QueryRouteResponse> {
        return resolveQueryRoute(this.runtimeManifest(), request, tables => {
            const digest = this.runtimePolicyDigest(tables);
            if (digest === null) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "Gateway query policy schema is unavailable" });
            }
            return digest;
        });
    }

    protected gatewayNowMs(): number {
        return Date.now();
    }

    /** Serialize every Gateway alarm write and preserve any earlier durable deadline. */
    protected scheduleGatewayAlarm(requestedAt: number): Promise<void> {
        return this.alarmScheduler.scheduleEarlier(requestedAt);
    }

    /** Keep every active head covered by a durable auth-retirement alarm. */
    private async scheduleGatewayAuthRetirement(nowMs: number): Promise<void> {
        const active = adaptSqlStorage(this.ctx.storage.sql).all<StoredGatewayActiveHead>(
            `SELECT g.rowid AS generation_rowid, g.principal_id, g.client_id, g.sub_id,
                    g.registration_id, g.connection_id
             FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.lifecycle = 'active' AND g.cdb_state = 'active'`
        );
        if (active.length === 0) return;
        const attachmentsByConnection = new Map<string, GwAttachment | null>();
        for (const ws of this.ctx.getWebSockets()) {
            const attachment = ws.deserializeAttachment() as GwAttachment | null;
            if (!attachment || attachmentsByConnection.has(attachment.connectionId)) {
                if (attachment) attachmentsByConnection.set(attachment.connectionId, null);
                continue;
            }
            attachmentsByConnection.set(attachment.connectionId, attachment);
        }
        let earliest: number | null = null;
        for (const head of active) {
            const attachment = attachmentsByConnection.get(head.connection_id);
            let deadline: number | null = null;
            if (
                attachment &&
                isVerifiedAttachment(attachment) &&
                attachment.connectionId === head.connection_id &&
                attachment.principalId === head.principal_id &&
                attachment.clientId === head.client_id &&
                attachment.snapshotSubIds?.includes(SubId(head.sub_id))
            ) {
                deadline = gatewayAuthRefreshDeadlineMs(attachment);
            }
            const reconcileAt = deadline === null || deadline <= nowMs ? nowMs + 1 : deadline;
            earliest = earliest === null ? reconcileAt : Math.min(earliest, reconcileAt);
        }
        if (earliest !== null) await this.scheduleGatewayAlarm(earliest);
    }

    private dueGatewayInstallRecoveries(nowMs: number): readonly StoredGatewayInstallRecovery[] {
        return adaptSqlStorage(this.ctx.storage.sql).all<StoredGatewayInstallRecovery>(
            `SELECT g.principal_id, g.client_id, g.sub_id, g.registration_id, g.connection_id
             FROM _gw_registration_generations g
             WHERE g.cdb_state = 'pending'
               AND g.lifecycle IN ('installing', 'retiring')
               AND g.retry_at IS NOT NULL AND g.retry_at <= ?
             ORDER BY g.retry_at, g.registration_id
             LIMIT ?`,
            nowMs,
            GATEWAY_QUERY_BATCH_SIZE
        );
    }

    private exactGatewaySocket(
        identity: GatewayRegistrationKey & { readonly connectionId: string },
        nowMs: number
    ): GatewayExactSnapshotSocket {
        const matching = this.ctx.getWebSockets().filter(ws => {
            const attachment = ws.deserializeAttachment() as GwAttachment | null;
            return attachment?.connectionId === identity.connectionId;
        });
        if (matching.length !== 1) return { status: "terminal" };
        const ws = matching[0];
        if (!ws) return { status: "terminal" };
        const attachment = ws.deserializeAttachment() as GwAttachment | null;
        if (
            !isVerifiedAttachment(attachment) ||
            attachment.connectionId !== identity.connectionId ||
            attachment.principalId !== identity.principalId ||
            attachment.clientId !== identity.clientId ||
            !attachment.snapshotSubIds?.includes(identity.subId)
        ) {
            return { status: "terminal" };
        }
        // Keep registrations while a briefly late updateAuth can still prove
        // the same identity. Protected work remains paused throughout grace.
        const nowSeconds = Math.floor(nowMs / 1_000);
        const refreshDeadline = gatewayAuthRefreshDeadlineMs(attachment);
        if (refreshDeadline === null) {
            this.rejectExpiredGatewaySocket(ws);
            return { status: "terminal" };
        }
        if (attachment.jwtExp <= nowSeconds) {
            if (nowMs < refreshDeadline) return { status: "refreshing", retryAt: refreshDeadline };
            this.rejectExpiredGatewaySocket(ws);
            return { status: "terminal" };
        }
        if (
            this.authRefreshBarriers.has(identity.connectionId) &&
            !this.authRefreshDrainConnections.has(identity.connectionId)
        ) {
            return {
                status: "refreshing",
                retryAt: Math.min(nowMs + GATEWAY_CLEANUP_BASE_RETRY_MS, refreshDeadline),
            };
        }
        if (attachment.jwtNbf !== undefined && attachment.jwtNbf > nowSeconds) {
            return { status: "refreshing", retryAt: Math.ceil(attachment.jwtNbf * 1_000) };
        }
        return { status: "ready", ws, attachment };
    }

    private rejectExpiredGatewaySocket(ws: WebSocket): void {
        try {
            this.rejectAuth(ws, "CDB_FORBIDDEN");
        } catch {
            // The alarm caller still owns durable retirement when the socket
            // cannot receive or complete the terminal close.
        }
    }

    private trackGatewayTask(connectionId: string, task: Promise<void>): Promise<void> {
        let active = this.activeOperations.get(connectionId);
        if (!active) {
            active = new Set();
            this.activeOperations.set(connectionId, active);
        }
        active.add(task);
        const cleanup = () => {
            active?.delete(task);
            if (active?.size === 0) this.activeOperations.delete(connectionId);
        };
        void task.then(cleanup, cleanup);
        return task;
    }

    private async waitForActiveGatewayOperations(connectionId: string): Promise<void> {
        while (true) {
            const active = this.activeOperations.get(connectionId);
            if (!active || active.size === 0) return;
            await Promise.allSettled([...active]);
        }
    }

    private settleRetiredGatewaySubscription(
        identity: GatewayRegistrationKey & { readonly connectionId: string },
        settlement:
            | { readonly kind: "error"; readonly code: import("../../errors.ts").CdbErrorCode }
            | { readonly kind: "refetch"; readonly reason: "shardsChanged" }
    ): void {
        const current = this.exactGatewaySocket(identity, this.gatewayNowMs());
        if (current.status !== "ready") return;
        current.ws.serializeAttachment({
            ...current.attachment,
            snapshotSubIds: (current.attachment.snapshotSubIds ?? []).filter(subId => subId !== identity.subId),
        } satisfies VerifiedGwAttachment);
        if (settlement.kind === "refetch") {
            this.send(current.ws, { t: "mustRefetch", subIds: [identity.subId], reason: settlement.reason });
            return;
        }
        this.sendError(current.ws, settlement.code, identity.subId);
    }

    private dueGatewayCleanupRows(nowMs: number): readonly StoredGatewayCleanupRow[] {
        return adaptSqlStorage(this.ctx.storage.sql).all<StoredGatewayCleanupRow>(
            `SELECT g.principal_id, g.client_id, g.sub_id, g.registration_id,
                    g.connection_id, g.source_cdb_id, g.organization_id, g.recovery_generation, g.retry_count
             FROM _gw_registration_generations g
             WHERE g.lifecycle = 'retiring' AND g.cdb_state = 'retiring'
               AND g.retry_at IS NOT NULL AND g.retry_at <= ?
               AND NOT EXISTS (
                 SELECT 1 FROM _gw_registration_heads h WHERE h.registration_id = g.registration_id
               )
             ORDER BY g.retry_at, g.registration_id
             LIMIT ?`,
            nowMs,
            GATEWAY_CLEANUP_BATCH_SIZE
        );
    }

    private completeGatewayCleanup(row: StoredGatewayCleanupRow): void {
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            sql.exec(
                `DELETE FROM _gw_registration_generations
                 WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
                   AND connection_id = ? AND source_cdb_id = ?
                   AND lifecycle = 'retiring' AND cdb_state = 'retiring'
                   AND NOT EXISTS (
                     SELECT 1 FROM _gw_registration_heads h
                     WHERE h.registration_id = _gw_registration_generations.registration_id
                   )`,
                row.registration_id,
                row.principal_id,
                row.client_id,
                row.sub_id,
                row.connection_id,
                row.source_cdb_id
            );
            if (sql.changes() === 1) return;
            if (
                sql.one<{ registration_id: string }>(
                    "SELECT registration_id FROM _gw_registration_generations WHERE registration_id = ?",
                    row.registration_id
                )
            ) {
                throw gatewayInvalidationInvariant("retired Gateway generation changed before cleanup could complete");
            }
        });
    }

    private completeLegacyGatewayCleanup(row: StoredGatewayCleanupRow): void {
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            sql.exec(
                `DELETE FROM _gw_registration_generations
                 WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
                   AND connection_id = ? AND (source_cdb_id IS NULL OR source_cdb_id = '')
                   AND lifecycle = 'retiring' AND cdb_state = 'retiring'
                   AND NOT EXISTS (
                     SELECT 1 FROM _gw_registration_heads h
                     WHERE h.registration_id = _gw_registration_generations.registration_id
                   )`,
                row.registration_id,
                row.principal_id,
                row.client_id,
                row.sub_id,
                row.connection_id
            );
            if (sql.changes() === 1) return;
            if (
                sql.one<{ registration_id: string }>(
                    "SELECT registration_id FROM _gw_registration_generations WHERE registration_id = ?",
                    row.registration_id
                )
            ) {
                throw gatewayInvalidationInvariant(
                    "legacy retired Gateway generation changed before cleanup could complete"
                );
            }
        });
    }

    private recordGatewayCleanupFailure(row: StoredGatewayCleanupRow, nowMs: number, error: unknown): void {
        const attempts = Math.min(row.retry_count + 1, GATEWAY_CLEANUP_MAX_RETRY_COUNT);
        const message = (error instanceof Error ? error.message : String(error)).slice(
            0,
            GATEWAY_CLEANUP_MAX_ERROR_LENGTH
        );
        const retryAt = nowMs + gatewayRetryDelayMs(attempts);
        this.ctx.storage.transactionSync(() => {
            adaptSqlStorage(this.ctx.storage.sql).exec(
                `UPDATE _gw_registration_generations
                 SET retry_count = ?, retry_at = ?, retry_error = ?, updated_at = ?
                 WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
                   AND connection_id = ?
                   AND lifecycle = 'retiring' AND cdb_state = 'retiring'
                   AND NOT EXISTS (
                     SELECT 1 FROM _gw_registration_heads h
                     WHERE h.registration_id = _gw_registration_generations.registration_id
                   )`,
                attempts,
                retryAt,
                message,
                nowMs,
                row.registration_id,
                row.principal_id,
                row.client_id,
                row.sub_id,
                row.connection_id
            );
        });
    }

    private async cleanupGatewayGeneration(row: StoredGatewayCleanupRow, nowMs: number): Promise<void> {
        try {
            if (!row.source_cdb_id) {
                // These rows predate physical Cdb registration identity, so no
                // remote subscription can exist. Delete only the exact
                // headless legacy generation.
                this.completeLegacyGatewayCleanup(row);
                return;
            }
            const id = this.env.CDB_SHARD.idFromString(row.source_cdb_id);
            const cdb = this.env.CDB_SHARD.get(id) as unknown as CdbSubscriptionRpc;
            const subscription = {
                gatewayId: this.ctx.id.toString(),
                registrationId: row.registration_id,
                connectionId: row.connection_id,
                clientId: ClientId(row.client_id),
                subId: SubId(row.sub_id),
            };
            let recoveryGeneration = row.recovery_generation;
            let outcome: unknown;
            try {
                outcome = await cdb.unsubscribe({ subscription, recoveryGeneration });
            } catch (error) {
                const normalized = rehydrateCdbRpcError(error);
                if (!(normalized instanceof CdbError) || normalized.code !== "CDB_STALE_EPOCH") throw normalized;
                if (!row.organization_id) {
                    throw gatewayInvalidationInvariant("retired Gateway generation omitted its recovery owner");
                }
                const route = await this.catalog().route(Number(vshardOf([row.organization_id])));
                recoveryGeneration = route.recoveryGeneration;
                outcome = await cdb.unsubscribe({ subscription, recoveryGeneration });
            }
            if (outcome !== undefined) throw new Error("Cdb returned a malformed unsubscribe outcome");
            const finalized: unknown = await cdb.finalizeUnsubscribe({
                subscription,
                recoveryGeneration,
            });
            if (finalized !== undefined) throw new Error("Cdb returned a malformed unsubscribe finalization outcome");
            this.completeGatewayCleanup(row);
        } catch (error) {
            this.recordGatewayCleanupFailure(row, nowMs, error);
        }
    }

    private async scheduleGatewayWork(nowMs: number): Promise<void> {
        const dueAt = nextGatewayWorkAt(adaptSqlStorage(this.ctx.storage.sql), [...this.authRefreshDrainConnections]);
        if (dueAt !== null) await this.scheduleGatewayAlarm(Math.max(nowMs + 1, dueAt));
    }

    /** Retire the logical head only in the transaction that owns its cleanup alarm. */
    private async retireGatewayStateWithCleanupAlarm(nowMs: number, retire: () => void): Promise<void> {
        const alarmAt = nowMs + 1;
        await this.alarmScheduler.transactionWithEarlierAlarm(alarmAt, retire);
    }

    /** Best-effort fallback after a close event could not commit retirement. */
    private async scheduleAbandonedGatewayReconciliation(nowMs: number): Promise<void> {
        const alarmAt = nowMs + 1;
        await this.alarmScheduler.transactionWithEarlierAlarm(alarmAt, () => {
            adaptSqlStorage(this.ctx.storage.sql).exec(
                `INSERT INTO _gw_maintenance_state (key, integer_value) VALUES (?, 0)
                 ON CONFLICT (key) DO NOTHING`,
                GATEWAY_ABANDONED_REGISTRATION_CURSOR_KEY
            );
        });
    }

    /**
     * Reconcile one durable page of active heads against exact live socket
     * attachments. The rowid cursor prevents live heads from starving later
     * abandoned heads across bounded alarm passes.
     */
    private reconcileAbandonedGatewayRegistrations(nowMs: number): boolean {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const cursor =
            sql.one<{ integer_value: number }>(
                "SELECT integer_value FROM _gw_maintenance_state WHERE key = ?",
                GATEWAY_ABANDONED_REGISTRATION_CURSOR_KEY
            )?.integer_value ?? 0;
        const rows = sql.all<StoredGatewayActiveHead>(
            `SELECT g.rowid AS generation_rowid, g.principal_id, g.client_id, g.sub_id,
                    g.registration_id, g.connection_id
             FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.rowid > ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
             ORDER BY g.rowid
             LIMIT ?`,
            cursor,
            GATEWAY_ABANDONED_REGISTRATION_BATCH_SIZE + 1
        );
        const candidates = rows.slice(0, GATEWAY_ABANDONED_REGISTRATION_BATCH_SIZE);
        const hasMore = rows.length > GATEWAY_ABANDONED_REGISTRATION_BATCH_SIZE;
        const abandoned = candidates.filter(candidate => {
            const identity = {
                principalId: PrincipalId(candidate.principal_id),
                clientId: ClientId(candidate.client_id),
                subId: SubId(candidate.sub_id),
                registrationId: candidate.registration_id,
                connectionId: candidate.connection_id,
            };
            return this.exactGatewaySocket(identity, nowMs).status === "terminal";
        });
        if (abandoned.length === 0 && !hasMore && cursor === 0) return false;
        this.ctx.storage.transactionSync(() => {
            const transactionSql = adaptSqlStorage(this.ctx.storage.sql);
            for (const candidate of abandoned) {
                retainCurrentGatewaySnapshotReplay(
                    transactionSql,
                    {
                        principalId: PrincipalId(candidate.principal_id),
                        clientId: ClientId(candidate.client_id),
                        subId: SubId(candidate.sub_id),
                    },
                    nowMs,
                    true
                );
                retireGatewayRegistration(
                    transactionSql,
                    {
                        principalId: PrincipalId(candidate.principal_id),
                        clientId: ClientId(candidate.client_id),
                        subId: SubId(candidate.sub_id),
                    },
                    candidate.registration_id,
                    nowMs
                );
            }
            pruneGatewaySnapshotReplays(transactionSql, nowMs);
            const nextCursor = hasMore ? (candidates.at(-1)?.generation_rowid ?? cursor) : 0;
            if (nextCursor !== cursor) {
                transactionSql.exec(
                    `INSERT INTO _gw_maintenance_state (key, integer_value) VALUES (?, ?)
                     ON CONFLICT (key) DO UPDATE SET integer_value = excluded.integer_value`,
                    GATEWAY_ABANDONED_REGISTRATION_CURSOR_KEY,
                    nextCursor
                );
            }
        });
        return hasMore;
    }

    private drainGatewayWork(): Promise<void> {
        this.gatewayWorkDrainRequested = true;
        if (this.gatewayWorkDrain) return this.gatewayWorkDrain;
        const drain = Promise.resolve().then(async () => {
            while (this.gatewayWorkDrainRequested) {
                this.gatewayWorkDrainRequested = false;
                await this.runGatewayWorkPass();
            }
        });
        this.gatewayWorkDrain = drain;
        const clear = (): void => {
            if (this.gatewayWorkDrain === drain) this.gatewayWorkDrain = null;
        };
        drain.then(clear, clear);
        return drain;
    }

    private async runGatewayWorkPass(): Promise<void> {
        const nowMs = this.gatewayNowMs();
        const excludedConnectionIds = [...this.authRefreshDrainConnections];
        this.ctx.storage.transactionSync(() =>
            pruneGatewaySnapshotReplays(adaptSqlStorage(this.ctx.storage.sql), nowMs)
        );
        for (const recovery of this.dueGatewayInstallRecoveries(nowMs)) {
            this.ctx.storage.transactionSync(() => {
                markPendingGatewaySubscriptionAmbiguous(adaptSqlStorage(this.ctx.storage.sql), {
                    principalId: PrincipalId(recovery.principal_id),
                    clientId: ClientId(recovery.client_id),
                    subId: SubId(recovery.sub_id),
                    registrationId: recovery.registration_id,
                    connectionId: recovery.connection_id,
                    nowMs,
                });
            });
        }
        const cleanupRows = this.dueGatewayCleanupRows(nowMs);
        await Promise.allSettled(cleanupRows.map(row => this.cleanupGatewayGeneration(row, nowMs)));

        const queryTasks = this.snapshotMaterializer
            .dueCandidates(nowMs, undefined, excludedConnectionIds)
            .map(candidate =>
                this.trackGatewayTask(candidate.connection_id, this.snapshotMaterializer.runCandidate(candidate, nowMs))
            );
        await Promise.allSettled(queryTasks);

        const sendNowMs = this.gatewayNowMs();
        const sendAttempts = await this.snapshotDelivery.claimDue(sendNowMs, undefined, excludedConnectionIds);
        const sendTasks = sendAttempts.map(attempt =>
            this.trackGatewayTask(attempt.connectionId, this.snapshotDelivery.sendAttempt(attempt))
        );
        await Promise.allSettled(sendTasks);
        await this.scheduleGatewayWork(this.gatewayNowMs());
    }

    /** Start healthy-path work after its durable alarm owns crash recovery. */
    protected startEagerGatewayWork(): void {
        this.ctx.waitUntil(this.drainGatewayWork().catch(() => {}));
    }

    private async drainConnectionGatewayWork(connectionId: string): Promise<void> {
        const queryNowMs = this.gatewayNowMs();
        const queryTasks = this.snapshotMaterializer
            .dueCandidates(queryNowMs, connectionId)
            .map(candidate =>
                this.trackGatewayTask(
                    candidate.connection_id,
                    this.snapshotMaterializer.runCandidate(candidate, queryNowMs)
                )
            );
        await Promise.allSettled(queryTasks);

        const sendNowMs = this.gatewayNowMs();
        const sendAttempts = await this.snapshotDelivery.claimDue(sendNowMs, connectionId);
        const sendTasks = sendAttempts.map(attempt =>
            this.trackGatewayTask(attempt.connectionId, this.snapshotDelivery.sendAttempt(attempt))
        );
        await Promise.allSettled(sendTasks);
        await this.scheduleGatewayWork(this.gatewayNowMs());
    }

    private async bootstrap(): Promise<void> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        sql.exec("PRAGMA foreign_keys = ON");
        for (const stmt of GW_DDL.split(";")
            .map(s => s.trim())
            .filter(Boolean))
            sql.exec(stmt);
        ensureGatewayRegistrationColumns(sql);
        ensureGatewaySnapshotOutboxColumns(sql);
        sql.exec(
            `UPDATE _gw_registration_generations
             SET retry_at = updated_at
             WHERE lifecycle = 'retiring' AND cdb_state = 'retiring' AND retry_at IS NULL`
        );
        const nowMs = this.gatewayNowMs();
        await this.scheduleGatewayWork(nowMs);
        await this.scheduleGatewayAuthRetirement(nowMs);
    }

    override async alarm(): Promise<void> {
        const reconciliationAt = this.gatewayNowMs();
        if (this.reconcileAbandonedGatewayRegistrations(reconciliationAt)) {
            await this.scheduleGatewayAlarm(reconciliationAt + 1);
        }
        await this.drainGatewayWork();
        await this.scheduleGatewayAuthRetirement(this.gatewayNowMs());
    }

    /**
     * Accept Cdb invalidations only for the exact generation that still owns
     * its logical head. Dirty versions remain durable for the alarm runner.
     */
    async invalidateSubscriptions(request: GatewayInvalidationRequest): Promise<GatewayInvalidationResponse> {
        const gatewayId = this.ctx.id.toString();
        const validated = validateGatewayInvalidationRequest(request, gatewayId);
        const updatedAt = this.gatewayNowMs();
        const acknowledgements = this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            return validated.invalidations.map(({ subscription, changeSeq }): GatewayInvalidationAck => {
                const stored = sql.one<{
                    registration_id: string;
                    connection_id: string;
                    client_id: string;
                    sub_id: number;
                    source_cdb_id: string | null;
                    lifecycle: GatewayRegistrationLifecycle;
                    cdb_state: GatewayRegistrationCdbState;
                    head_registration_id: string | null;
                }>(
                    `SELECT g.registration_id, g.connection_id, g.client_id, g.sub_id,
                            g.source_cdb_id, g.lifecycle, g.cdb_state,
                            h.registration_id AS head_registration_id
                     FROM _gw_registration_generations AS g
                     LEFT JOIN _gw_registration_heads AS h
                       ON h.principal_id = g.principal_id
                      AND h.client_id = g.client_id
                      AND h.sub_id = g.sub_id
                     WHERE g.registration_id = ?`,
                    subscription.registrationId
                );
                if (
                    !stored ||
                    stored.head_registration_id !== stored.registration_id ||
                    stored.lifecycle === "retiring" ||
                    stored.cdb_state === "retiring"
                ) {
                    return {
                        registrationId: subscription.registrationId,
                        changeSeq,
                        status: "stale",
                    };
                }
                if (
                    stored.source_cdb_id !== validated.sourceCdbId ||
                    stored.connection_id !== subscription.connectionId ||
                    stored.client_id !== subscription.clientId ||
                    stored.sub_id !== subscription.subId
                ) {
                    throw gatewayInvalidationInvariant(
                        "current Gateway registration conflicts with its Cdb invalidation identity"
                    );
                }
                sql.exec(
                    `UPDATE _gw_registration_generations
                     SET dirty_version = MAX(dirty_version, ?), updated_at = ?
                     WHERE registration_id = ? AND connection_id = ? AND client_id = ? AND sub_id = ?
                       AND source_cdb_id = ?
                       AND lifecycle <> 'retiring' AND cdb_state <> 'retiring'
                       AND EXISTS (
                         SELECT 1 FROM _gw_registration_heads h
                         WHERE h.registration_id = _gw_registration_generations.registration_id
                           AND h.principal_id = _gw_registration_generations.principal_id
                           AND h.client_id = _gw_registration_generations.client_id
                           AND h.sub_id = _gw_registration_generations.sub_id
                       )`,
                    changeSeq,
                    updatedAt,
                    subscription.registrationId,
                    subscription.connectionId,
                    subscription.clientId,
                    subscription.subId,
                    validated.sourceCdbId
                );
                if (sql.changes() !== 1) {
                    throw gatewayInvalidationInvariant(
                        "current Gateway registration changed while accepting a Cdb invalidation"
                    );
                }
                return {
                    registrationId: subscription.registrationId,
                    changeSeq,
                    status: "accepted",
                };
            });
        });

        if (acknowledgements.some(acknowledgement => acknowledgement.status === "accepted")) {
            try {
                await this.scheduleGatewayAlarm(updatedAt + 1);
            } catch (error) {
                throw new CdbError({
                    code: "CDB_SHARD_UNAVAILABLE",
                    message: "Gateway could not durably schedule invalidation work",
                    cause: error,
                });
            }
            this.startEagerGatewayWork();
        }
        return { gatewayId, acknowledgements };
    }

    /**
     * Upgrade an HTTP request to a hibernated WebSocket. Returns a Response.
     */
    override async fetch(request: Request): Promise<Response> {
        const upgrade = request.headers.get("Upgrade");
        if (upgrade !== "websocket") {
            return new Response("expected websocket", { status: 426 });
        }
        const pair = new WebSocketPair();
        const server = pair[1];
        this.ctx.acceptWebSocket(server);
        server.serializeAttachment({
            kind: "pending",
            connectionId: crypto.randomUUID(),
            authOrigin: new URL(request.url).origin,
            routedClientId: routedClientIdFromUrl(request.url),
        } satisfies PendingGwAttachment);
        return new Response(null, { status: 101, webSocket: pair[0] });
    }

    override async webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer): Promise<void> {
        if (typeof raw !== "string") return;
        // UTF-8 is never shorter than the JavaScript code-unit count. Reject
        // obviously large strings before allocating an encoded copy, then
        // measure exact bytes for multibyte input before JSON parsing.
        if (
            raw.length > GATEWAY_MAX_INBOUND_WEBSOCKET_BYTES ||
            GATEWAY_TEXT_ENCODER.encode(raw).byteLength > GATEWAY_MAX_INBOUND_WEBSOCKET_BYTES
        ) {
            ws.close(1009, "message too large");
            return;
        }
        let msg: Up;
        try {
            msg = decodeUp(raw);
        } catch {
            this.sendError(ws, "CDB_UNSUPPORTED_FEATURE");
            return;
        }
        switch (msg.t) {
            case "hello":
                await this.onHello(ws, msg);
                break;
            case "updateAuth":
                await this.onUpdateAuth(ws, msg);
                break;
            case "sub":
                await this.onSub(ws, msg);
                break;
            case "unsub":
                await this.onUnsub(ws, msg);
                break;
            case "mut":
                await this.onMut(ws, msg);
                break;
            case "ack":
                this.onAck(ws, msg);
                break;
            case "ping":
                // Hibernation auto-replies; nothing to do.
                break;
            default:
                msg satisfies never;
        }
    }

    private async cleanupGatewaySocket(ws: WebSocket): Promise<void> {
        const attachment = ws.deserializeAttachment() as GwAttachment | null;
        if (attachment) {
            if (attachment.kind !== "rejected") {
                ws.serializeAttachment({
                    kind: "rejected",
                    connectionId: attachment.connectionId,
                    authOrigin: attachment.authOrigin,
                } satisfies RejectedGwAttachment);
            }
            this.authOperationClaims.delete(attachment.connectionId);
            this.authRefreshBarriers.delete(attachment.connectionId);
            this.authRefreshDrainConnections.delete(attachment.connectionId);
            this.activeOperations.delete(attachment.connectionId);
            for (const pending of this.pendingSubscriptions.values()) {
                if (pending.connectionId === attachment.connectionId) pending.cancelled = true;
            }
            const nowMs = this.gatewayNowMs();
            try {
                await this.retireGatewayStateWithCleanupAlarm(nowMs, () => {
                    retireCurrentGatewayRegistrationsForConnection(
                        adaptSqlStorage(this.ctx.storage.sql),
                        attachment.connectionId,
                        nowMs
                    );
                });
            } catch (error) {
                await this.scheduleAbandonedGatewayReconciliation(nowMs).catch(() => {});
                throw error;
            }
        }
    }

    override async webSocketClose(ws: WebSocket): Promise<void> {
        await this.cleanupGatewaySocket(ws);
    }

    override async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
        await this.cleanupGatewaySocket(ws);
    }

    private async onHello(ws: WebSocket, msg: Extract<Up, { t: "hello" }>): Promise<void> {
        const pending = ws.deserializeAttachment() as GwAttachment | null;
        if (pending?.kind !== "pending" || pending.routedClientId === null || msg.clientId !== pending.routedClientId) {
            this.rejectAuth(ws, "CDB_FORBIDDEN");
            return;
        }
        const claim = this.claimAuthOperation(pending.connectionId);
        if (!claim) {
            this.sendError(ws, "CDB_RATE_LIMITED");
            return;
        }
        try {
            const mismatch = checkProtocolV(msg.protocolV);
            if (mismatch) {
                this.markConnectionRejected(ws, pending.connectionId);
                this.sendThenClose(
                    ws,
                    () => this.send(ws, mismatch),
                    1002,
                    `unsupported chardb protocol ${msg.protocolV}`
                );
                return;
            }
            const attachment = await this.verifyAttachment(
                ws,
                {
                    authOrigin: pending.authOrigin,
                    connectionId: pending.connectionId,
                    clientId: pending.routedClientId,
                    jwt: msg.jwt,
                    ...(msg.resumeFromCookie ? { lastCookie: msg.resumeFromCookie } : {}),
                },
                () => this.authOperationClaims.get(pending.connectionId) === claim
            );
            if (!attachment || this.authOperationClaims.get(pending.connectionId) !== claim) return;
            const current = ws.deserializeAttachment() as GwAttachment | null;
            if (
                current?.kind !== "pending" ||
                current.connectionId !== pending.connectionId ||
                current.authOrigin !== pending.authOrigin ||
                current.routedClientId !== pending.routedClientId
            ) {
                return;
            }
            const baseCookie = Cookie(`${pending.routedClientId}:0`);
            ws.serializeAttachment({
                ...attachment,
                lastCookie: msg.resumeFromCookie ?? baseCookie,
                ...(msg.resumeFromCookie !== undefined ? { resumeRefetchPendingSubIds: [] } : {}),
            } satisfies VerifiedGwAttachment);
            const welcome: Down = {
                t: "welcome",
                protocolV: PROTOCOL_V,
                baseCookie,
                region: "WNAM",
                ...(msg.resumeFromCookie ? { resumedFromCookie: msg.resumeFromCookie } : {}),
            };
            try {
                this.send(ws, welcome);
            } catch (error) {
                this.markConnectionRejected(ws, pending.connectionId);
                this.closePreservingFailure(ws, 1011, "welcome delivery failed", { value: error });
            }
        } finally {
            this.releaseAuthOperation(pending.connectionId, claim);
        }
    }

    private async onUpdateAuth(ws: WebSocket, msg: Extract<Up, { t: "updateAuth" }>): Promise<void> {
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(current)) {
            this.rejectAuth(ws, "CDB_FORBIDDEN");
            return;
        }
        const refreshDeadline = gatewayAuthRefreshDeadlineMs(current);
        if (refreshDeadline === null || this.gatewayNowMs() >= refreshDeadline) {
            this.rejectAuth(ws, "CDB_FORBIDDEN");
            return;
        }
        const connectionId = current.connectionId;
        const claim = this.claimAuthOperation(connectionId);
        if (!claim) {
            this.sendError(ws, "CDB_RATE_LIMITED");
            return;
        }
        const isCurrent = (): boolean => this.authOperationClaims.get(connectionId) === claim;
        this.authRefreshDrainConnections.add(connectionId);
        const barrier = this.performUpdateAuth(ws, connectionId, msg, isCurrent).catch(() => {
            if (isCurrent()) this.rejectAuth(ws, "CDB_CATALOG_UNAVAILABLE");
            return false;
        });
        this.authRefreshBarriers.set(connectionId, barrier);
        let refreshed = false;
        try {
            refreshed = await barrier;
        } finally {
            if (this.authRefreshBarriers.get(connectionId) === barrier) {
                this.authRefreshBarriers.delete(connectionId);
            }
            this.authRefreshDrainConnections.delete(connectionId);
            this.releaseAuthOperation(connectionId, claim);
        }
        if (refreshed) {
            const resumedAt = this.gatewayNowMs();
            const resumed = await this.alarmScheduler
                .transactionWithEarlierAlarm(resumedAt + 1, () =>
                    resumeGatewayAuthDeferredWork(adaptSqlStorage(this.ctx.storage.sql), connectionId, resumedAt)
                )
                .catch(() => false);
            if (resumed) this.startEagerGatewayWork();
        }
    }

    private claimAuthOperation(connectionId: string): object | null {
        if (this.authOperationClaims.has(connectionId)) return null;
        const claim = {};
        this.authOperationClaims.set(connectionId, claim);
        return claim;
    }

    private releaseAuthOperation(connectionId: string, claim: object): void {
        if (this.authOperationClaims.get(connectionId) === claim) {
            this.authOperationClaims.delete(connectionId);
        }
    }

    private async performUpdateAuth(
        ws: WebSocket,
        connectionId: string,
        msg: Extract<Up, { t: "updateAuth" }>,
        isCurrent: () => boolean = () => true
    ): Promise<boolean> {
        if (!isCurrent()) return false;
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(current) || current.connectionId !== connectionId) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
            return false;
        }
        const attachment = await this.verifyAttachment(
            ws,
            {
                authOrigin: current.authOrigin,
                connectionId,
                clientId: current.clientId,
                jwt: msg.jwt,
                ...(current.lastCookie !== undefined ? { lastCookie: current.lastCookie } : {}),
            },
            isCurrent
        );
        if (!isCurrent() || !attachment) return false;
        const refreshDeadline = gatewayAuthRefreshDeadlineMs(current);
        if (refreshDeadline === null || this.gatewayNowMs() >= refreshDeadline) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
            return false;
        }
        if (attachment.principalId !== current.principalId) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
            return false;
        }

        await this.waitForActiveGatewayOperations(connectionId);
        if (!isCurrent()) return false;
        await this.drainConnectionGatewayWork(connectionId);
        if (!isCurrent()) return false;
        this.authRefreshDrainConnections.delete(connectionId);
        await this.waitForActiveGatewayOperations(connectionId);
        if (!isCurrent()) return false;

        const latest = ws.deserializeAttachment() as GwAttachment | null;
        if (
            !isVerifiedAttachment(latest) ||
            latest.connectionId !== connectionId ||
            latest.clientId !== current.clientId
        ) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
            return false;
        }
        const latestRefreshDeadline = gatewayAuthRefreshDeadlineMs(latest);
        if (latestRefreshDeadline === null || this.gatewayNowMs() >= latestRefreshDeadline) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
            return false;
        }
        if (!isCurrent()) return false;
        ws.serializeAttachment({
            ...attachment,
            ...(latest.lastCookie !== undefined ? { lastCookie: latest.lastCookie } : {}),
            ...(latest.snapshotSubIds !== undefined ? { snapshotSubIds: latest.snapshotSubIds } : {}),
            ...(latest.resumeRefetchPendingSubIds !== undefined
                ? { resumeRefetchPendingSubIds: latest.resumeRefetchPendingSubIds }
                : {}),
        } satisfies VerifiedGwAttachment);
        try {
            const refreshed = ws.deserializeAttachment() as GwAttachment | null;
            if (!isVerifiedAttachment(refreshed) || refreshed.connectionId !== connectionId) {
                if (isCurrent()) this.rejectAuth(ws, "CDB_FORBIDDEN");
                return false;
            }
            if (!isCurrent()) return false;
            await this.scheduleGatewayAuthRetirement(this.gatewayNowMs());
            if (!isCurrent()) return false;
            this.send(ws, { t: "mustRefetch", subIds: [], reason: "authChanged" });
            return true;
        } catch {
            if (isCurrent()) {
                this.rejectConnection(ws, "CDB_SHARD_UNAVAILABLE", 1011, "authentication refresh failed");
            }
            return false;
        }
    }

    private async verifyAttachment(
        ws: WebSocket,
        request: Omit<GatewayJwtVerificationRequest, "config" | "catalog">,
        isCurrent: () => boolean = () => true
    ): Promise<VerifiedGwAttachment | null> {
        const config = this.jwtConfig();
        if (!config) {
            if (isCurrent()) this.rejectAuth(ws, "CDB_AUTH_NOT_BOUND");
            return null;
        }
        try {
            return await verifyGatewayJwt({
                ...request,
                config,
                catalog: this.catalog() as unknown as GatewayJwtVerificationRequest["catalog"],
            });
        } catch (error) {
            if (isCurrent()) {
                this.rejectAuth(ws, error instanceof CdbError ? error.code : "CDB_CATALOG_UNAVAILABLE");
            }
            return null;
        }
    }

    private rejectAuth(ws: WebSocket, code: import("../../errors.ts").CdbErrorCode): void {
        this.rejectConnection(ws, code, 1008, code);
    }

    private rejectConnection(
        ws: WebSocket,
        code: import("../../errors.ts").CdbErrorCode,
        closeCode: number,
        reason: string
    ): void {
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (current?.kind === "rejected") return;
        if (current) this.markConnectionRejected(ws, current.connectionId);
        this.sendThenClose(ws, () => this.sendError(ws, code), closeCode, reason);
    }

    private markConnectionRejected(ws: WebSocket, expectedConnectionId: string): void {
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (!current || current.kind === "rejected" || current.connectionId !== expectedConnectionId) return;
        ws.serializeAttachment({
            kind: "rejected",
            connectionId: current.connectionId,
            authOrigin: current.authOrigin,
        } satisfies RejectedGwAttachment);
    }

    private sendThenClose(ws: WebSocket, send: () => void, closeCode: number, reason: string): void {
        let sendFailure: { readonly value: unknown } | null = null;
        try {
            send();
        } catch (error) {
            sendFailure = { value: error };
        }
        this.closePreservingFailure(ws, closeCode, reason, sendFailure);
    }

    private closePreservingFailure(
        ws: WebSocket,
        closeCode: number,
        reason: string,
        priorFailure: { readonly value: unknown } | null
    ): void {
        try {
            ws.close(closeCode, reason);
        } catch (error) {
            if (priorFailure === null) throw error;
        }
        if (priorFailure !== null) throw priorFailure.value;
    }

    private async onSub(ws: WebSocket, msg: Extract<Up, { t: "sub" }>): Promise<void> {
        const attachment = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(attachment) || !isCurrentVerifiedAttachment(attachment)) {
            this.sendError(ws, "CDB_FORBIDDEN", msg.subId);
            return;
        }
        let ownedMsg: Extract<Up, { t: "sub" }>;
        try {
            ownedMsg = { ...msg, args: snapshotCdbQueryArgs(msg.args) };
        } catch (error) {
            this.sendError(ws, error instanceof CdbError ? error.code : "CDB_INVARIANT", msg.subId);
            return;
        }
        const resumeRefetchPendingSubIds = attachment.resumeRefetchPendingSubIds;
        let resumeReplayAttempt = false;
        if (resumeRefetchPendingSubIds !== undefined) {
            if (!resumeRefetchPendingSubIds.includes(msg.subId) && !attachment.snapshotSubIds?.includes(msg.subId)) {
                if (resumeRefetchPendingSubIds.length >= MAX_INITIAL_SNAPSHOTS_PER_CONNECTION) {
                    this.sendError(ws, "CDB_RATE_LIMITED", msg.subId);
                    return;
                }
                resumeReplayAttempt = true;
            }
        }
        const operationKey = `${attachment.connectionId}:${msg.subId}`;
        const previous = this.pendingSubscriptions.get(operationKey);
        if (previous?.queued) {
            this.sendError(ws, "CDB_RATE_LIMITED", msg.subId);
            return;
        }
        const capacityKey = stableJson([attachment.principalId, attachment.clientId, msg.subId]);
        const duplicatePending = [...this.pendingSubscriptions.values()].some(
            pending => !pending.cancelled && pending !== previous && pending.capacityKey === capacityKey
        );
        if (duplicatePending) {
            this.sendError(ws, "CDB_RATE_LIMITED", msg.subId);
            return;
        }
        const deliveredSubIds = new Set(attachment.snapshotSubIds ?? []);
        if (!previous && !deliveredSubIds.has(msg.subId)) {
            const reservedSubIds = new Set(
                [...this.pendingSubscriptions.values()]
                    .filter(
                        pending =>
                            pending.connectionId === attachment.connectionId &&
                            !pending.cancelled &&
                            !deliveredSubIds.has(pending.subId)
                    )
                    .map(pending => pending.subId)
            );
            if (deliveredSubIds.size + reservedSubIds.size >= MAX_INITIAL_SNAPSHOTS_PER_CONNECTION) {
                this.sendError(ws, "CDB_RATE_LIMITED", msg.subId);
                return;
            }
        }
        if (!this.hasRegistrationCapacity(attachment, msg.subId, capacityKey)) {
            this.sendError(ws, "CDB_RATE_LIMITED", msg.subId);
            return;
        }
        const queued = this.authRefreshBarriers.has(attachment.connectionId) || previous !== undefined;
        if (previous) previous.cancelled = true;
        const pending: PendingSubscription = {
            connectionId: attachment.connectionId,
            subId: msg.subId,
            capacityKey,
            cancelled: false,
            queued,
            resumeReplayAttempt,
            task: Promise.resolve(),
        };
        this.pendingSubscriptions.set(operationKey, pending);
        if (queued) {
            await (previous?.task.catch(() => {}) ?? Promise.resolve());
            let succeeded = true;
            while (true) {
                const barrier = this.authRefreshBarriers.get(attachment.connectionId);
                if (!barrier) break;
                if (!(await barrier)) {
                    succeeded = false;
                    break;
                }
            }
            if (succeeded && !pending.cancelled) {
                pending.queued = false;
                await this.admitSubscription(ws, ownedMsg, pending, operationKey);
            } else if (this.pendingSubscriptions.get(operationKey) === pending) {
                this.pendingSubscriptions.delete(operationKey);
            }
            return;
        }
        await this.admitSubscription(ws, ownedMsg, pending, operationKey);
    }

    private hasRegistrationCapacity(attachment: VerifiedGwAttachment, subId: SubId, capacityKey: string): boolean {
        if (
            [...this.pendingSubscriptions.values()].some(
                pending => !pending.cancelled && pending.capacityKey === capacityKey
            )
        ) {
            return true;
        }
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        if (
            sql.one<{ registration_id: string }>(
                `SELECT registration_id FROM _gw_registration_heads
                 WHERE principal_id = ? AND client_id = ? AND sub_id = ?`,
                attachment.principalId,
                attachment.clientId,
                subId
            )
        ) {
            return true;
        }
        const capacityKeys = new Set(
            sql
                .all<{ principal_id: string; client_id: string; sub_id: number }>(
                    `SELECT principal_id, client_id, sub_id FROM _gw_registration_heads
                     LIMIT ?`,
                    GATEWAY_MAX_CURRENT_AND_PENDING_REGISTRATIONS
                )
                .map(row => stableJson([row.principal_id, row.client_id, row.sub_id]))
        );
        if (capacityKeys.size >= GATEWAY_MAX_CURRENT_AND_PENDING_REGISTRATIONS) return false;
        for (const pending of this.pendingSubscriptions.values()) {
            if (!pending.cancelled) capacityKeys.add(pending.capacityKey);
        }
        return capacityKeys.size < GATEWAY_MAX_CURRENT_AND_PENDING_REGISTRATIONS;
    }

    private async admitSubscription(
        ws: WebSocket,
        msg: Extract<Up, { t: "sub" }>,
        pending: PendingSubscription,
        operationKey: string
    ): Promise<void> {
        const att = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(att) || !isCurrentVerifiedAttachment(att)) {
            if (!pending.cancelled) this.sendError(ws, "CDB_FORBIDDEN", msg.subId);
            if (this.pendingSubscriptions.get(operationKey) === pending) {
                this.pendingSubscriptions.delete(operationKey);
            }
            return;
        }
        const task = this.settleSubscription(ws, att, msg, pending);
        pending.task = task;
        let active = this.activeOperations.get(att.connectionId);
        if (!active) {
            active = new Set();
            this.activeOperations.set(att.connectionId, active);
        }
        active.add(task);
        try {
            await task;
        } catch {
            const current = ws.deserializeAttachment() as GwAttachment | null;
            if (
                !pending.cancelled &&
                this.pendingSubscriptions.get(operationKey) === pending &&
                isVerifiedAttachment(current) &&
                isCurrentVerifiedAttachment(current) &&
                current.connectionId === att.connectionId &&
                current.clientId === att.clientId &&
                current.principalId === att.principalId
            ) {
                this.sendError(ws, "CDB_INVARIANT", msg.subId);
            }
        } finally {
            if (this.pendingSubscriptions.get(operationKey) === pending) {
                this.pendingSubscriptions.delete(operationKey);
            }
            active.delete(task);
            if (active.size === 0) this.activeOperations.delete(att.connectionId);
        }
    }

    private async settleSubscription(
        ws: WebSocket,
        att: VerifiedGwAttachment,
        msg: Extract<Up, { t: "sub" }>,
        pending: PendingSubscription
    ): Promise<void> {
        const routedResult = await this.routeQuery({ ref: msg.ref, args: msg.args });
        if (pending.cancelled) return;
        if (!routedResult.ok) {
            this.sendError(ws, routedResult.error.code, msg.subId);
            return;
        }
        let routed: Extract<QueryRouteResponse, { readonly ok: true }>;
        try {
            routed = { ...routedResult, args: snapshotCdbQueryArgs(routedResult.args) };
        } catch (error) {
            this.sendError(ws, error instanceof CdbError ? error.code : "CDB_INVARIANT", msg.subId);
            return;
        }
        if (routed.authority !== "organization" && routed.authority !== "user" && routed.authority !== "global") {
            this.sendError(ws, "CDB_AUTH_NOT_BOUND", msg.subId);
            return;
        }
        const organizationId = routed.partitionKey;
        const partition = routed.intent.partitionKey;
        if (
            !organizationId ||
            !partition ||
            partition.values.length === 0 ||
            routed.intent.joinShape === "cross-partition" ||
            !partition.values.every(value => typeof value === "string" && value === organizationId)
        ) {
            this.sendError(ws, "CDB_CROSS_PARTITION", msg.subId);
            return;
        }
        if (routed.authority === "user" && organizationId !== att.principalId) {
            this.sendError(ws, "CDB_FORBIDDEN", msg.subId);
            return;
        }

        const vshards = new Set(partition.values.map(value => Number(vshardOf([value as string]))));
        if (vshards.size !== 1) {
            this.sendError(ws, "CDB_CROSS_PARTITION", msg.subId);
            return;
        }
        const vshard = [...vshards][0];
        if (vshard === undefined) {
            this.sendError(ws, "CDB_CROSS_PARTITION", msg.subId);
            return;
        }

        const catalog = this.catalog() as CatalogRoutingRpc &
            CatalogOrganizationAuthorityRpc &
            Partial<CatalogOrganizationAuthorityRouteRpc & CatalogUserAuthorityRpc>;
        const projected = await resolvePartitionAuthRoute(
            catalog,
            routed.authority,
            att.principalId,
            organizationId,
            vshard
        );
        if (pending.cancelled) return;
        if (!projected.ok) {
            this.sendError(ws, projected.code, msg.subId);
            return;
        }
        const authEpochs = projected.auth.authEpochs;
        if (!authEpochs) {
            this.sendError(ws, "CDB_CATALOG_UNAVAILABLE", msg.subId);
            return;
        }

        const { shardId, schemaEpoch, domainSchemaEpoch } = projected.route;
        const recoveryGeneration = projected.route.recoveryGeneration;
        if (pending.cancelled) return;
        const currentBeforeInstall = ws.deserializeAttachment() as GwAttachment | null;
        const operationKey = `${att.connectionId}:${msg.subId}`;
        if (
            this.pendingSubscriptions.get(operationKey) !== pending ||
            !isVerifiedAttachment(currentBeforeInstall) ||
            !isCurrentVerifiedAttachment(currentBeforeInstall) ||
            currentBeforeInstall.connectionId !== att.connectionId ||
            currentBeforeInstall.clientId !== att.clientId ||
            currentBeforeInstall.principalId !== att.principalId
        ) {
            return;
        }

        const cdbId = this.env.CDB_SHARD.idFromName(shardId);
        const sourceCdbId = cdbId.toString();
        if (sourceCdbId.length === 0) {
            this.sendError(ws, "CDB_SHARD_UNAVAILABLE", msg.subId);
            return;
        }
        let replaySnapshot: GatewaySnapshotReplay | null = null;
        if (pending.resumeReplayAttempt) {
            const replayAt = this.gatewayNowMs();
            replaySnapshot = this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                retainCurrentGatewaySnapshotReplay(
                    sql,
                    { principalId: att.principalId, clientId: att.clientId, subId: msg.subId },
                    replayAt
                );
                return att.lastCookie === undefined
                    ? null
                    : resolveGatewaySnapshotReplay(sql, {
                          principalId: att.principalId,
                          clientId: att.clientId,
                          subId: msg.subId,
                          cookie: att.lastCookie,
                          organizationId: TenantId(organizationId),
                          ref: msg.ref,
                          args: routed.args,
                          policyDigest: routed.policyDigest,
                          queryHash: routed.queryHash,
                          shardId,
                          sourceCdbId,
                          schemaEpoch,
                          recoveryGeneration,
                          domainSchemaEpoch,
                          authEpochs,
                          nowMs: replayAt,
                      });
            });
            const currentAfterReplayLookup = ws.deserializeAttachment() as GwAttachment | null;
            if (
                pending.cancelled ||
                this.pendingSubscriptions.get(operationKey) !== pending ||
                !isVerifiedAttachment(currentAfterReplayLookup) ||
                !isCurrentVerifiedAttachment(currentAfterReplayLookup) ||
                currentAfterReplayLookup.connectionId !== att.connectionId ||
                currentAfterReplayLookup.clientId !== att.clientId ||
                currentAfterReplayLookup.principalId !== att.principalId
            ) {
                return;
            }
            if (!replaySnapshot) {
                this.send(ws, { t: "mustRefetch", subIds: [msg.subId], reason: "lagged" });
                const resumePending = currentAfterReplayLookup.resumeRefetchPendingSubIds;
                if (resumePending !== undefined && !resumePending.includes(msg.subId)) {
                    ws.serializeAttachment({
                        ...currentAfterReplayLookup,
                        resumeRefetchPendingSubIds: [...resumePending, msg.subId]
                            .sort((left, right) => left - right)
                            .map(SubId),
                    } satisfies VerifiedGwAttachment);
                }
                return;
            }
        }
        const registrationId = crypto.randomUUID();
        const installedAt = this.gatewayNowMs();
        const identity = {
            principalId: att.principalId,
            clientId: att.clientId,
            subId: msg.subId,
            registrationId,
            connectionId: att.connectionId,
        } as const;
        const recoveryAt = installedAt + GATEWAY_SUBSCRIBE_RECOVERY_MS;
        try {
            await this.scheduleGatewayAlarm(recoveryAt);
        } catch {
            if (!pending.cancelled) this.sendError(ws, "CDB_SHARD_UNAVAILABLE", msg.subId);
            return;
        }
        const currentBeforeCommit = ws.deserializeAttachment() as GwAttachment | null;
        if (
            pending.cancelled ||
            this.pendingSubscriptions.get(operationKey) !== pending ||
            !isVerifiedAttachment(currentBeforeCommit) ||
            !isCurrentVerifiedAttachment(currentBeforeCommit) ||
            currentBeforeCommit.connectionId !== att.connectionId ||
            currentBeforeCommit.clientId !== att.clientId ||
            currentBeforeCommit.principalId !== att.principalId
        ) {
            return;
        }
        try {
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                installGatewayRegistration(sql, {
                    ...identity,
                    organizationId: TenantId(organizationId),
                    ref: msg.ref,
                    args: routed.args,
                    intent: routed.intent,
                    policyDigest: routed.policyDigest,
                    queryHash: routed.queryHash,
                    shardId,
                    sourceCdbId,
                    schemaEpoch,
                    recoveryGeneration,
                    domainSchemaEpoch,
                    authEpochs,
                    ...(att.lastCookie === undefined ? {} : { lastCookie: att.lastCookie }),
                    nowMs: installedAt,
                });
                if (!armGatewaySubscriptionRecovery(sql, { ...identity, recoveryAt, nowMs: installedAt })) {
                    throw gatewayInvalidationInvariant("Gateway subscription install could not arm recovery");
                }
            });
        } catch (error) {
            if (error instanceof CdbError && error.code === "CDB_RATE_LIMITED") {
                this.sendError(ws, error.code, msg.subId);
                return;
            }
            throw error;
        }
        const deleteNeverRegistered = (): boolean =>
            this.ctx.storage.transactionSync(() =>
                deleteNeverRegisteredGatewaySubscription(adaptSqlStorage(this.ctx.storage.sql), identity)
            );
        const settleAmbiguous = async (): Promise<void> => {
            const nowMs = this.gatewayNowMs();
            const changed = this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                return (
                    markPendingGatewaySubscriptionAmbiguous(sql, { ...identity, nowMs }) ||
                    retireGatewayRegistration(sql, identity, registrationId, nowMs)
                );
            });
            if (changed) await this.scheduleGatewayWork(nowMs);
        };
        const isCancelledOrStale = (): boolean => {
            const current = ws.deserializeAttachment() as GwAttachment | null;
            return (
                pending.cancelled ||
                this.pendingSubscriptions.get(operationKey) !== pending ||
                !isVerifiedAttachment(current) ||
                !isCurrentVerifiedAttachment(current) ||
                current.connectionId !== att.connectionId ||
                current.clientId !== att.clientId ||
                current.principalId !== att.principalId
            );
        };

        // The prearmed recovery deadline remains durable if this earlier
        // scheduling attempt fails.
        await this.scheduleGatewayWork(installedAt).catch(() => {});
        if (isCancelledOrStale()) {
            deleteNeverRegistered();
            return;
        }

        const request = cdbSubscriptionRequest({
            gatewayId: this.ctx.id.toString(),
            ...identity,
            organizationId: TenantId(organizationId),
            authority: routed.authority,
            schemaEpoch,
            recoveryGeneration,
            vshard,
            domainSchemaEpoch,
            ref: msg.ref,
            args: routed.args,
            queryHash: routed.queryHash,
            intent: routed.intent,
        });
        let response: CdbSubscriptionResponse;
        try {
            const cdb = this.env.CDB_SHARD.get(cdbId) as unknown as CdbSubscriptionRpc;
            response = projectCdbSubscriptionResponse(await cdb.subscribe(request), request.subscription);
        } catch {
            await settleAmbiguous();
            if (!isCancelledOrStale()) this.sendError(ws, "CDB_SHARD_UNAVAILABLE", msg.subId);
            return;
        }
        if (!response.ok) {
            const deleted = deleteNeverRegistered();
            if (deleted && !isCancelledOrStale()) {
                if (response.error.code === "CDB_STALE_EPOCH") {
                    this.send(ws, { t: "mustRefetch", subIds: [msg.subId], reason: "shardsChanged" });
                } else {
                    this.sendError(ws, response.error.code, msg.subId);
                }
            }
            return;
        }
        if (isCancelledOrStale()) {
            await settleAmbiguous();
            return;
        }
        const activatedAt = this.gatewayNowMs();
        const activated = this.ctx.storage.transactionSync(() =>
            activateGatewaySubscription(adaptSqlStorage(this.ctx.storage.sql), {
                ...identity,
                changeSeq: response.changeSeq,
                nowMs: activatedAt,
            })
        );
        if (!activated || isCancelledOrStale()) {
            await settleAmbiguous();
            return;
        }
        const current = ws.deserializeAttachment() as VerifiedGwAttachment;
        const snapshotSubIds = [...new Set([...(current.snapshotSubIds ?? []), msg.subId])]
            .sort((left, right) => left - right)
            .map(SubId);
        const resumeRefetchPendingSubIds = pending.resumeReplayAttempt
            ? current.resumeRefetchPendingSubIds
            : current.resumeRefetchPendingSubIds?.filter(subId => subId !== msg.subId);
        ws.serializeAttachment({
            ...current,
            snapshotSubIds,
            ...(resumeRefetchPendingSubIds !== undefined ? { resumeRefetchPendingSubIds } : {}),
        } satisfies VerifiedGwAttachment);
        if (replaySnapshot) {
            try {
                this.send(ws, {
                    t: "snapshot",
                    subId: replaySnapshot.subId,
                    cookie: replaySnapshot.cookie,
                    rows: replaySnapshot.rows,
                });
            } catch (error) {
                this.markConnectionRejected(ws, att.connectionId);
                this.closePreservingFailure(ws, 1011, "snapshot replay delivery failed", { value: error });
            }
        }
        try {
            await this.scheduleGatewayAuthRetirement(activatedAt);
            await this.scheduleGatewayWork(activatedAt);
        } catch {
            if (!isCancelledOrStale()) this.sendError(ws, "CDB_SHARD_UNAVAILABLE", msg.subId);
        }
    }

    private async onUnsub(ws: WebSocket, msg: Extract<Up, { t: "unsub" }>): Promise<void> {
        const att = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(att) || !isCurrentVerifiedAttachment(att)) return;
        const pending = this.pendingSubscriptions.get(`${att.connectionId}:${msg.subId}`);
        if (pending) pending.cancelled = true;
        const nowMs = this.gatewayNowMs();
        await this.retireGatewayStateWithCleanupAlarm(nowMs, () => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            retireCurrentGatewayRegistration(sql, {
                principalId: att.principalId,
                clientId: att.clientId,
                subId: msg.subId,
                connectionId: att.connectionId,
                nowMs,
            });
            sql.exec(
                `DELETE FROM _gw_snapshot_replay
                 WHERE principal_id = ? AND client_id = ? AND sub_id = ?`,
                att.principalId,
                att.clientId,
                msg.subId
            );
        });
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (
            isVerifiedAttachment(current) &&
            current.connectionId === att.connectionId &&
            current.clientId === att.clientId &&
            current.principalId === att.principalId
        ) {
            const snapshotSubIds = current.snapshotSubIds?.filter(subId => subId !== msg.subId);
            const resumeRefetchPendingSubIds = current.resumeRefetchPendingSubIds?.filter(subId => subId !== msg.subId);
            if (
                snapshotSubIds?.length !== current.snapshotSubIds?.length ||
                resumeRefetchPendingSubIds?.length !== current.resumeRefetchPendingSubIds?.length
            ) {
                ws.serializeAttachment({
                    ...current,
                    ...(snapshotSubIds !== undefined ? { snapshotSubIds } : {}),
                    ...(resumeRefetchPendingSubIds !== undefined ? { resumeRefetchPendingSubIds } : {}),
                } satisfies VerifiedGwAttachment);
            }
        }
    }

    private onAck(ws: WebSocket, msg: Extract<Up, { t: "ack" }>): void {
        const attachment = ws.deserializeAttachment() as GwAttachment | null;
        const nowMs = this.gatewayNowMs();
        if (!isVerifiedAttachment(attachment) || !isCurrentVerifiedAttachment(attachment, Math.floor(nowMs / 1_000))) {
            return;
        }
        const settlement = this.snapshotDelivery.acknowledge({
            principalId: attachment.principalId,
            clientId: attachment.clientId,
            connectionId: attachment.connectionId,
            cookie: msg.cookie,
            nowMs,
        });
        if (!settlement) return;
        if (settlement.kind === "replay") {
            void this.scheduleGatewayWork(nowMs);
            return;
        }
        if (!settlement.acknowledged) return;
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (
            !isVerifiedAttachment(current) ||
            current.connectionId !== attachment.connectionId ||
            current.principalId !== attachment.principalId ||
            current.clientId !== attachment.clientId
        ) {
            return;
        }
        if (
            !settlement.identity.alreadyAcknowledged &&
            (current.lastCookie ?? null) === settlement.identity.attachmentBaseCookie
        ) {
            ws.serializeAttachment({ ...current, lastCookie: msg.cookie } satisfies VerifiedGwAttachment);
        }
        void this.scheduleGatewayWork(nowMs);
    }

    private async onMut(ws: WebSocket, msg: Extract<Up, { t: "mut" }>): Promise<void> {
        const attachment = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(attachment) || !isCurrentVerifiedAttachment(attachment)) {
            await this.admitMutation(ws, msg);
            return;
        }
        try {
            assertCdbMutationArgsByteLimit(msg.args);
        } catch (error) {
            const failure =
                error instanceof CdbError
                    ? error
                    : new CdbError({ code: "CDB_INVARIANT", message: "mutation argument sizing failed" });
            this.sendMutFailure(ws, msg.mutId, failure.toJSON(), attachment.lastCookie ?? Cookie(""));
            return;
        }
        if (!this.reserveMutation(attachment.connectionId)) {
            const current = ws.deserializeAttachment() as GwAttachment | null;
            this.sendMutFailure(
                ws,
                msg.mutId,
                new CdbError({ code: "CDB_RATE_LIMITED", message: "too many unsettled mutations" }).toJSON(),
                (isVerifiedAttachment(current) ? current.lastCookie : attachment.lastCookie) ?? Cookie("")
            );
            return;
        }
        try {
            const barrier = this.authRefreshBarriers.get(attachment.connectionId);
            if (barrier) {
                if (await barrier) await this.admitMutation(ws, msg);
                return;
            }
            await this.admitMutation(ws, msg);
        } finally {
            this.releaseMutation(attachment.connectionId);
        }
    }

    private reserveMutation(connectionId: string): boolean {
        const connectionCount = this.unsettledMutationsByConnection.get(connectionId) ?? 0;
        if (
            connectionCount >= Gateway.MAX_UNSETTLED_MUTATIONS_PER_CONNECTION ||
            this.unsettledMutationCount >= Gateway.MAX_UNSETTLED_MUTATIONS
        ) {
            return false;
        }
        this.unsettledMutationsByConnection.set(connectionId, connectionCount + 1);
        this.unsettledMutationCount += 1;
        return true;
    }

    private releaseMutation(connectionId: string): void {
        const connectionCount = this.unsettledMutationsByConnection.get(connectionId);
        if (connectionCount === undefined || connectionCount <= 0 || this.unsettledMutationCount <= 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "Gateway mutation admission counter underflow" });
        }
        if (connectionCount === 1) this.unsettledMutationsByConnection.delete(connectionId);
        else this.unsettledMutationsByConnection.set(connectionId, connectionCount - 1);
        this.unsettledMutationCount -= 1;
    }

    private async admitMutation(ws: WebSocket, msg: Extract<Up, { t: "mut" }>): Promise<void> {
        const att = ws.deserializeAttachment() as GwAttachment | null;
        if (!isVerifiedAttachment(att)) {
            this.sendMutFailure(
                ws,
                msg.mutId,
                new CdbError({ code: "CDB_FORBIDDEN", message: "verified mutation auth is not bound" }).toJSON(),
                Cookie("")
            );
            return;
        }
        if (!isCurrentVerifiedAttachment(att)) {
            this.sendMutFailure(
                ws,
                msg.mutId,
                new CdbError({ code: "CDB_FORBIDDEN", message: "verified mutation auth is expired" }).toJSON(),
                att.lastCookie ?? Cookie("")
            );
            return;
        }
        const trusted = trustedMutationAuthFromAttachment(att);
        const task = this.settleMut(ws, att, msg, trusted);
        let active = this.activeOperations.get(att.connectionId);
        if (!active) {
            active = new Set();
            this.activeOperations.set(att.connectionId, active);
        }
        active.add(task);
        try {
            await task;
        } catch {
            // The only uncaught failure here is the final WebSocket send. A
            // second send would violate exactly-once settlement.
        } finally {
            active.delete(task);
            if (active.size === 0) this.activeOperations.delete(att.connectionId);
        }
    }

    /**
     * Resolve a mutation through the worker manifest, route the resulting vshard
     * via the Catalog, and call `Cdb.mutate` on the owning shard. The handler
     * intentionally does not re-evaluate the user's mutation body; that runs
     * inside the shard DO under `transactionSync`.
     */
    private async routeMut(msg: Extract<Up, { t: "mut" }>, trusted: TrustedMutationAuth): Promise<CdbMutationResponse> {
        let catalog: CatalogMutationRpc & CatalogOrganizationAuthorityRpc;
        try {
            const catalogId = this.env.CDB_CATALOG.idFromName("global");
            catalog = this.env.CDB_CATALOG.get(catalogId) as unknown as CatalogMutationRpc &
                CatalogOrganizationAuthorityRpc;
        } catch {
            return mutationFailure("CDB_CATALOG_UNAVAILABLE", "Catalog binding unavailable");
        }
        return dispatchTrustedMutation(
            {
                routeMutation: request => this.routeMutation(request),
                catalog,
                cdb: shardId => {
                    const shardDoId = this.env.CDB_SHARD.idFromName(shardId);
                    return this.env.CDB_SHARD.get(shardDoId) as unknown as CdbMutationRpc;
                },
            },
            {
                ref: msg.ref,
                mutId: msg.mutId,
                args: msg.args,
                ...trusted,
            }
        );
    }

    /** Settle one accepted mutation exactly once, including unexpected async failures. */
    private async settleMut(
        ws: WebSocket,
        att: VerifiedGwAttachment,
        msg: Extract<Up, { t: "mut" }>,
        trusted: TrustedMutationAuth
    ): Promise<void> {
        let ack: CdbMutationResponse;
        try {
            ack = await this.routeMut(msg, trusted);
        } catch {
            ack = mutationFailure("CDB_INVARIANT", "mutation dispatch failed unexpectedly");
        }
        const current = ws.deserializeAttachment() as GwAttachment | null;
        if (
            !isVerifiedAttachment(current) ||
            current.connectionId !== att.connectionId ||
            current.clientId !== att.clientId ||
            current.principalId !== att.principalId
        ) {
            return;
        }
        if (ack.ok) {
            const cookie = Cookie(ack.cookie);
            // Another mutation or snapshot may have advanced the socket while
            // this RPC was unresolved. Preserve that delivered watermark; the
            // per-mutation result still carries its own commit cookie.
            const deliveredCookie = current.lastCookie === att.lastCookie ? cookie : (current.lastCookie ?? cookie);
            if (deliveredCookie === cookie) {
                ws.serializeAttachment({ ...current, lastCookie: cookie } satisfies VerifiedGwAttachment);
            }
            this.send(ws, {
                t: "poke",
                cookie: deliveredCookie,
                patches: [],
                mutResults: [{ mutId: msg.mutId, ok: true, result: ack.result, cookie }],
            });
        } else {
            this.sendMutFailure(ws, msg.mutId, ack.error, current.lastCookie ?? Cookie(""));
        }
    }

    private catalog(): CatalogRoutingRpc {
        const id = this.env.CDB_CATALOG.idFromName("global");
        return this.env.CDB_CATALOG.get(id) as unknown as CatalogRoutingRpc;
    }

    private send(ws: WebSocket, down: Down): void {
        ws.send(encodeWire(down));
    }

    private sendError(ws: WebSocket, code: import("../../errors.ts").CdbErrorCode, subId?: SubId): void {
        const corr = CorrelationId(crypto.randomUUID());
        this.send(ws, gatewayErrorEnvelope(code, corr, subId));
    }

    private sendMutFailure(ws: WebSocket, mutId: MutId, error: CdbErrorWire, cookie: Cookie): void {
        this.send(ws, {
            t: "poke",
            cookie,
            patches: [],
            mutResults: [
                {
                    mutId,
                    ok: false,
                    error: {
                        code: error.code,
                        retryable: isRetryable(error.code),
                        docs: docsUrlFor(error.code),
                    },
                },
            ],
        });
    }
}

/** Bind the bundler-built manifest into each Gateway isolate. */
export function configureGatewayRuntime(config: GatewayRuntimeConfig): typeof Gateway {
    return class ConfiguredGateway extends Gateway {
        protected override runtimeManifest(): ChardbManifest {
            return config.manifest();
        }

        protected override runtimePolicyDigest(tableNames: readonly string[]): string {
            return cdbPolicyDigest(config.schema(), tableNames);
        }

        protected override jwtConfig(): GatewayJwtConfig | null {
            return config.auth;
        }
    };
}
