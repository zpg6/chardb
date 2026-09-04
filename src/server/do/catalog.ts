/**
 * `Catalog` DO.
 *
 * Single per logical DB. Holds:
 *   - schema + colocation graph + partition contract digest
 *   - (vshard_lo, vshard_hi) → ShardDO range table (split-only, never merge)
 *   - epoch counters: schema_epoch, auth_epoch_global, auth_epoch_tenant, auth_epoch_principal
 *   - JWKS cache (SWR)
 *   - reference tables (replicated)
 *
 * Epoch bumps are CAS-guarded: every bump runs as `UPDATE … SET epoch=epoch+1`
 * inside `transactionSync`; concurrent admin actions serialize at the DO input
 * gate. Bootstrap blocks new requests until the schema migration has run via
 * `state.blockConcurrencyWhile`
 * (https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile).
 */

import { DurableObject } from "cloudflare:workers";
import {
    type CatalogJwkResolution,
    type CatalogJwkResolutionRequest,
    JWKS_CACHE_TTL_MS,
    JWKS_FAILURE_BACKOFF_INITIAL_MS,
    JWKS_FAILURE_BACKOFF_MAX_MS,
    JWKS_MAX_KID_BYTES,
    JWKS_REFRESH_LEASE_MS,
    JWKS_SUCCESS_COOLDOWN_MS,
    fetchValidatedJwks,
    parseCachedJwk,
} from "../../auth/jwks_cache.ts";
import { CdbError, isCdbError, rehydrateCdbRpcError, throwCdbRpcError } from "../../errors.ts";
import type { PrincipalId, RawJson, ShardId, TenantId } from "../../types.ts";
import { VSHARD_COUNT, vshardOf } from "../../vshard.ts";
import { withChardbLoopbacks } from "../loopback.ts";
import { readCurrentOwnerVectorPurgeStatus } from "../organization-deletion-status.ts";
import { chardbResourceDescriptorsAt, isChardbVectorResourceDescriptor } from "../resource-descriptors.ts";
import { type ChardbMigrationJournal, defineMigrations, pendingMigrations } from "../schema-migrations.ts";
import {
    CATALOG_AUTH_INVALIDATION_BATCH_SIZE,
    CatalogAuthInvalidationStore,
    initializeCatalogAuthInvalidationStore,
} from "./catalog-auth-invalidation-store.ts";
import {
    type CatalogAuthAdapterRpcRequest,
    type CatalogAuthAdapterRpcResult,
    type CatalogAuthEpochChange,
    type CatalogAuthIncrementRequest,
    type CatalogAuthIncrementResult,
    type CatalogAuthMutationRequest,
    type CatalogAuthMutationResult,
    type CatalogAuthQueryRequest,
    type OrganizationAuthority,
    type OrganizationAuthorityRequest,
    type UserAuthority,
    type UserAuthorityRequest,
    bumpCatalogAuthEpoch,
    catalogOrganizationAuthorityAvailable,
    catalogUserAuthorityAvailable,
    countCatalogAuth,
    incrementCatalogAuthWithEffects,
    initializeCatalogAuthorityStorage,
    mutateCatalogAuthWithEffects,
    queryCatalogAuth,
    readCatalogAuthEpoch,
    recordMigratedCatalogAuthoritySchema,
    resolveOrganizationAuthorityFromCatalog,
    resolveUserAuthorityFromCatalog,
} from "./catalog-authority-store.ts";
import {
    type CatalogOrganizationDeletionBarrier,
    type CatalogOrganizationDeletionBarrierIdentity,
    type CatalogOrganizationDeletionBarrierStatus,
    CatalogOrganizationDeletionBarrierStore,
    initializeCatalogOrganizationDeletionBarrierStore,
} from "./catalog-organization-deletion-barrier-store.ts";
import {
    type CatalogOrganizationDeletion,
    type CatalogOrganizationDeletionShard,
    CatalogOrganizationDeletionStore,
    initializeCatalogOrganizationDeletionStore,
} from "./catalog-organization-deletion-store.ts";
import { type CatalogCutoverRequest, type CatalogCutoverResult, CatalogRoutingStore } from "./catalog-routing-store.ts";
import {
    type CatalogSchemaShardState,
    type CatalogSchemaState,
    type CatalogSql,
    activateCatalogSchemaShard,
    applyCatalogSchemaMigrationStep,
    beginCatalogSchemaChange,
    catalogSchemaBaselineExists,
    completeCatalogSchemaMigration,
    initializeCatalogStorage,
    readCatalogSchemaMigrationShards,
    readCatalogSchemaState,
    recordCatalogSchemaShardFailure,
} from "./catalog-schema-store.ts";
import {
    type CatalogTopologyOperation,
    type CatalogTopologyOperationIdentity,
    CatalogTopologyOperationStore,
    initializeCatalogTopologyOperationStore,
} from "./catalog-topology-operation-store.ts";
import type { CdbAuthInvalidationRequest, CdbAuthInvalidationResult } from "./cdb-auth-invalidation-store.ts";
import type { CdbVectorOrganizationPurgeStatus } from "./cdb-vector-organization-deletion-store.ts";
import { DurableAlarmScheduler } from "./gateway-alarm-store.ts";
import { RecoveryAdmissionStore } from "./recovery-admission.ts";
import type { RecoveryAdmissionClock } from "./recovery-coordinator.ts";
import {
    DurableObjectRecovery,
    abortForArmedRecoveryRestore,
    assertRecoveryAvailable,
    assertRecoveryAvailableFor,
    initializeRecoveryStorage,
} from "./recovery.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

export interface CatalogEnv {
    readonly CDB_SHARD?: DurableObjectNamespace;
    readonly CDB_RESHARD?: DurableObjectNamespace;
}

const JWKS_URL_MAX_BYTES = 2_048;

type JwksRefreshOutcome =
    | { readonly ok: true }
    | { readonly ok: false; readonly message: string; readonly retryAfterMs: number };

function projectOrganizationDeletionHandoff(
    deletion: CatalogOrganizationDeletion | null,
    shards: readonly CatalogOrganizationDeletionShard[]
): CatalogOrganizationDeletionStatus["handoff"] {
    if (!deletion) {
        return Object.freeze({ state: "not_started" as const, attempts: 0, completedAt: null, lastError: null });
    }
    const attempts = shards.reduce((sum, shard) => sum + shard.attempts, deletion.attempts);
    if (!Number.isSafeInteger(attempts) || attempts < 0) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "organization deletion handoff attempts are invalid" });
    }
    const lastError =
        deletion.lastError ??
        shards.find(shard => shard.status === "pending" && shard.lastError !== null)?.lastError ??
        null;
    return Object.freeze({
        state: deletion.status,
        attempts,
        completedAt: deletion.completedAt,
        lastError,
    });
}

function normalizeJwksUrl(rawUrl: string): string | null {
    if (new TextEncoder().encode(rawUrl).byteLength > JWKS_URL_MAX_BYTES) return null;
    try {
        const url = new URL(rawUrl);
        if ((url.protocol !== "https:" && url.protocol !== "http:") || url.username || url.password) return null;
        return url.toString();
    } catch {
        return null;
    }
}

function jwksResolutionUnavailable(message: string, retryAfterMs: number): CatalogJwkResolution {
    return { ok: false, message, retryAfterMs: Math.max(1, Math.ceil(retryAfterMs)) };
}

export interface RouteResult {
    readonly shardId: ShardId;
    /** Domain schema epoch. Changes only after every shard activates one packaged migration target. */
    readonly domainSchemaEpoch: number;
    /** Physical routing generation. Changes on vshard split or cutover. */
    readonly schemaEpoch: number;
    /** Internal recovery generation. Changes for every restore operation. */
    readonly recoveryGeneration: number;
}

export interface OrganizationAuthorityRouteRequest extends OrganizationAuthorityRequest {
    readonly vshard: number;
}

export type OrganizationAuthorityRouteResult =
    | { readonly authority: OrganizationAuthority; readonly route: RouteResult }
    | { readonly authority: null };

interface CdbSchemaMigrationRpc {
    prepareSchemaMigration(args: {
        readonly recoveryGeneration: number;
        readonly migrationId: string;
        readonly activeVersion: number;
        readonly activeDigest: string;
        readonly targetVersion: number;
        readonly targetEpoch: number;
        readonly targetDigest: string;
    }): Promise<unknown>;
    applySchemaMigration(args: {
        readonly migrationId: string;
        readonly version: number;
        readonly recoveryGeneration: number;
    }): Promise<unknown>;
    activateSchemaMigration(args: {
        readonly migrationId: string;
        readonly recoveryGeneration: number;
    }): Promise<unknown>;
    baselineSchemaMigration(args: {
        readonly recoveryGeneration: number;
        readonly migrationId: string;
        readonly targetVersion: number;
        readonly targetEpoch: number;
        readonly targetDigest: string;
    }): Promise<unknown>;
}

interface CdbOrganizationDeletionRpc {
    /** Legacy RPC name. The shard treats this as cleanup for every organization-owned resource. */
    deleteOrganizationFiles(args: {
        readonly organizationId: string;
        readonly nowMs: number;
        readonly domainSchemaEpoch: number;
        readonly recoveryGeneration: number;
    }): Promise<{
        readonly organizationId: string;
        readonly accepted: true;
    }>;
    vectorOrganizationPurgeStatus(input: {
        readonly organizationId: string;
        readonly schemaEpoch: number;
        readonly domainSchemaEpoch: number;
        readonly recoveryGeneration: number;
    }): Promise<CdbVectorOrganizationPurgeStatus | null>;
}

export interface CatalogOrganizationDeletionStatus {
    readonly organizationId: string;
    readonly authDeleted: boolean;
    readonly handoffComplete: boolean;
    readonly handoff: {
        readonly state: "not_started" | "pending" | "complete";
        readonly attempts: number;
        readonly completedAt: number | null;
        readonly lastError: string | null;
    };
    readonly vectorPurge: CdbVectorOrganizationPurgeStatus | null;
}

interface CdbAuthInvalidationRpc {
    invalidateAuthScope(args: CdbAuthInvalidationRequest): Promise<CdbAuthInvalidationResult>;
}

const EMPTY_MIGRATION_JOURNAL = defineMigrations([]);

/** Retained for callers compiled against the earlier vector deletion feature gate. */
export function assertCatalogOrganizationDeletionSupported(
    _journal: ChardbMigrationJournal,
    deletedOrganizationIds: readonly string[]
): void {
    void deletedOrganizationIds;
}

export interface CatalogRuntimeConfig {
    readonly migrations: () => ChardbMigrationJournal;
}

interface CatalogTopologyOperationRequest {
    readonly migId: string;
    readonly sourceShard: string;
    readonly destinationShard: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly startEpoch: number;
    readonly recoveryGeneration: number;
}

interface CatalogDerivedTopologyOperationRequest {
    readonly migId: string;
    readonly destinationShard: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly recoveryGeneration: number;
}

const CATALOG_TOPOLOGY_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

function assertDerivedTopologyOperationRequest(args: CatalogDerivedTopologyOperationRequest): void {
    if (!CATALOG_TOPOLOGY_ID.test(args.migId)) {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "topology migration id is invalid" });
    }
    if (!CATALOG_TOPOLOGY_ID.test(args.destinationShard)) {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "topology destination shard is invalid" });
    }
    if (
        !Number.isSafeInteger(args.rangeLo) ||
        !Number.isSafeInteger(args.rangeHi) ||
        args.rangeLo < 0 ||
        args.rangeHi < args.rangeLo ||
        args.rangeHi >= VSHARD_COUNT
    ) {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "topology vshard range is invalid" });
    }
}

interface CatalogOrganizationDeletionBarrierRequest {
    readonly migId: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
}

export class Catalog extends DurableObject<CatalogEnv> {
    private bootstrapped = false;
    private authTablesBootstrapped = false;
    private readonly routingStore: CatalogRoutingStore;
    private readonly alarmScheduler: DurableAlarmScheduler;
    private readonly recovery: DurableObjectRecovery;
    private readonly jwksRefreshes = new Map<string, Promise<JwksRefreshOutcome>>();

    constructor(state: DurableObjectState, env: CatalogEnv) {
        super(state, withChardbLoopbacks(env, state));
        this.routingStore = new CatalogRoutingStore({
            sql: adaptSqlStorage(this.ctx.storage.sql),
            transactionSync: callback => this.ctx.storage.transactionSync(callback),
        });
        this.alarmScheduler = new DurableAlarmScheduler(state.storage);
        this.recovery = new DurableObjectRecovery(state.storage, () => adaptSqlStorage(this.ctx.storage.sql));
        state.blockConcurrencyWhile(async () => this.bootstrap());
    }

    protected migrationJournal(): ChardbMigrationJournal {
        return EMPTY_MIGRATION_JOURNAL;
    }

    private async bootstrap(): Promise<void> {
        if (this.bootstrapped) return;
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const journal = this.migrationJournal();
        const schemaState = initializeCatalogStorage(sql, journal);
        initializeCatalogOrganizationDeletionStore(sql);
        initializeCatalogOrganizationDeletionBarrierStore(sql);
        initializeCatalogTopologyOperationStore(sql);
        initializeCatalogAuthInvalidationStore(sql);
        initializeRecoveryStorage(sql);
        await this.reconcileRecoveryAdmission();
        if (schemaState.status === "active" && schemaState.activeVersion === journal.version) {
            this.ensureAuthTables();
        }
        const nextAuthInvalidation = new CatalogAuthInvalidationStore(sql).nextPendingAt();
        const nextDeletion = this.hasOrganizationCleanupResources()
            ? new CatalogOrganizationDeletionStore(sql).nextPendingAt()
            : null;
        const nextAlarm = this.earliestPending(nextAuthInvalidation, nextDeletion);
        if (nextAlarm !== null) await this.scheduleAlarmNoLaterThan(Math.max(Date.now() + 1, nextAlarm));
        this.bootstrapped = true;
    }

    /**
     * Materialize every synthesized auth table in the singleton Catalog.
     * Central storage supports Better Auth's normal secondary lookups
     * without cross-shard scans or auth-specific GSI maintenance. The DDL
     * is rendered from the synthesized Drizzle schema so plugin tables and
     * column additions flow through automatically.
     */
    private ensureAuthTables(): void {
        if (this.authTablesBootstrapped) return;
        const schemaState = this.readSchemaState();
        if (schemaState.status !== "active" || schemaState.activeVersion !== this.migrationJournal().version) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "Catalog auth schema migration is not active",
                hint: "retry after the schema migration activates",
            });
        }
        this.ctx.storage.transactionSync(() => {
            initializeCatalogAuthorityStorage(adaptSqlStorage(this.ctx.storage.sql));
        });
        this.authTablesBootstrapped = true;
    }

    /** Run a Better Auth model write against Catalog-owned storage. */
    async mutateAuth(args: CatalogAuthMutationRequest, recoveryGeneration: number): Promise<CatalogAuthMutationResult> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        this.ensureAuthTables();
        const hasOrganizationCleanupResources = this.hasOrganizationCleanupResources();
        const nowMs = Date.now();
        const mutate = () => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(recoveryGeneration);
            const deletionStore = new CatalogOrganizationDeletionStore(sql);
            const deletionBarriers = new CatalogOrganizationDeletionBarrierStore(sql);
            if (
                args.model === "organization" &&
                (args.op === "create" || args.op === "update") &&
                typeof args.payload?.id === "string" &&
                deletionStore.isDeleted(args.payload.id)
            ) {
                throw new CdbError({
                    code: "CDB_INVALID_ARGS",
                    message: "organization id was permanently retired after deletion",
                });
            }
            const mutation = mutateCatalogAuthWithEffects(sql, args);
            this.enqueueAuthInvalidations(sql, mutation.authEpochChanges, nowMs);
            for (const organizationId of mutation.deletedOrganizationIds) {
                const vshard = Number(vshardOf([organizationId]));
                deletionBarriers.assertDeletionAllowed(vshard);
                deletionStore.record(organizationId, vshard, nowMs);
                if (hasOrganizationCleanupResources) {
                    deletionStore.recordShards(organizationId, [this.organizationDeletionTarget(vshard)], nowMs);
                } else {
                    deletionStore.complete(organizationId, nowMs);
                }
            }
            return mutation;
        };
        const outcome = await this.alarmScheduler.transactionWithEarlierAlarm(nowMs, mutate);
        return outcome.result;
    }

    /** Atomically apply Better Auth's guarded single-row numeric increment contract. */
    async incrementAuth(
        args: CatalogAuthIncrementRequest,
        recoveryGeneration: number
    ): Promise<CatalogAuthIncrementResult> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        this.ensureAuthTables();
        const nowMs = Date.now();
        const outcome = await this.alarmScheduler.transactionWithEarlierAlarm(nowMs, () => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(recoveryGeneration);
            const increment = incrementCatalogAuthWithEffects(sql, args);
            this.enqueueAuthInvalidations(sql, increment.authEpochChanges, nowMs);
            return increment;
        });
        return outcome.result;
    }

    /** Read Better Auth rows from Catalog-owned storage. */
    async queryAuth(
        args: CatalogAuthQueryRequest,
        recoveryGeneration: number
    ): Promise<readonly Record<string, RawJson>[]> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        this.ensureAuthTables();
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        new RecoveryAdmissionStore(sql).assertRequest(recoveryGeneration);
        return queryCatalogAuth(sql, args);
    }

    /** Count Better Auth rows without materializing them across the Catalog RPC. */
    async countAuth(
        args: Pick<CatalogAuthQueryRequest, "model" | "where">,
        recoveryGeneration: number
    ): Promise<number> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        this.ensureAuthTables();
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        new RecoveryAdmissionStore(sql).assertRequest(recoveryGeneration);
        return countCatalogAuth(sql, args);
    }

    /**
     * Keep expected adapter failures on the caller side of the Workers RPC boundary.
     * Throwing a migration fence through RPC restarts the Durable Object turn in local
     * Workerd even when Better Auth converts that rejection into a closed HTTP response.
     */
    async authAdapterRpc(request: CatalogAuthAdapterRpcRequest): Promise<CatalogAuthAdapterRpcResult> {
        try {
            switch (request.operation) {
                case "mutate":
                    return { ok: true, value: await this.mutateAuth(request.args, request.recoveryGeneration) };
                case "increment":
                    return { ok: true, value: await this.incrementAuth(request.args, request.recoveryGeneration) };
                case "query":
                    return { ok: true, value: await this.queryAuth(request.args, request.recoveryGeneration) };
                case "count":
                    return { ok: true, value: await this.countAuth(request.args, request.recoveryGeneration) };
                default:
                    throw new CdbError({ code: "CDB_INVALID_ARGS", message: "unknown auth adapter operation" });
            }
        } catch (error) {
            if (!isCdbError(error)) throw error;
            return {
                ok: false,
                error: {
                    code: error.code,
                    message: error.message,
                    ...(error.hint === undefined ? {} : { hint: error.hint }),
                },
            };
        }
    }

    /**
     * Resolve organization authority from Catalog-owned Better Auth rows.
     * JWT tenant and role claims are deliberately absent from this boundary:
     * the caller supplies only its verified subject and requested organization.
     */
    async resolveOrganizationAuthority(args: OrganizationAuthorityRequest): Promise<OrganizationAuthority | null> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        if (!args.principalId || !args.organizationId) return null;
        if (!catalogOrganizationAuthorityAvailable()) return null;
        this.ensureAuthTables();
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            if (new CatalogOrganizationDeletionStore(sql).isDeleted(args.organizationId)) return null;
            const authority = resolveOrganizationAuthorityFromCatalog(sql, args);
            return authority ? { ...authority, recoveryGeneration: this.recoveryGeneration() } : null;
        });
    }

    /**
     * Resolve current organization membership and physical placement without an
     * interleaving Catalog turn between the authority and routing reads.
     */
    async resolveOrganizationAuthorityRoute(
        args: OrganizationAuthorityRouteRequest
    ): Promise<OrganizationAuthorityRouteResult> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        if (!Number.isSafeInteger(args.vshard) || args.vshard < 0 || args.vshard >= VSHARD_COUNT) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "virtual shard is out of range" });
        }
        const currentSchemaState = this.readSchemaState();
        if (
            currentSchemaState.status !== "active" ||
            currentSchemaState.activeVersion !== this.migrationJournal().version
        ) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: `schema migration ${currentSchemaState.migrationId ?? "unknown"} is in progress`,
                hint: "retry after the schema migration activates",
            });
        }
        if (!args.principalId || !args.organizationId) return { authority: null };
        if (!catalogOrganizationAuthorityAvailable()) return { authority: null };
        this.ensureAuthTables();
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const schemaState = this.readSchemaState();
            if (schemaState.status !== "active" || schemaState.activeVersion !== this.migrationJournal().version) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: `schema migration ${schemaState.migrationId ?? "unknown"} is in progress`,
                    hint: "retry after the schema migration activates",
                });
            }
            if (new CatalogOrganizationDeletionStore(sql).isDeleted(args.organizationId)) return { authority: null };
            const authority = resolveOrganizationAuthorityFromCatalog(sql, args);
            if (!authority) return { authority: null };
            const recoveryGeneration = this.recoveryGeneration();
            return {
                authority: { ...authority, recoveryGeneration },
                route: {
                    ...this.routingStore.route(args.vshard),
                    domainSchemaEpoch: schemaState.activeEpoch,
                    recoveryGeneration,
                },
            };
        });
    }

    /** Resolve user authority from the Catalog-owned Better Auth user row. */
    async resolveUserAuthority(args: UserAuthorityRequest): Promise<UserAuthority | null> {
        await this.bootstrap();
        this.assertRecoveryAvailable();
        if (!args.principalId) return null;
        if (!catalogUserAuthorityAvailable()) return null;
        this.ensureAuthTables();
        return this.ctx.storage.transactionSync(() => {
            const authority = resolveUserAuthorityFromCatalog(adaptSqlStorage(this.ctx.storage.sql), args);
            return authority ? { ...authority, recoveryGeneration: this.recoveryGeneration() } : null;
        });
    }

    async route(vshard: number): Promise<RouteResult> {
        this.assertRecoveryAvailable();
        const schemaState = this.readSchemaState();
        if (schemaState.status !== "active" || schemaState.activeVersion !== this.migrationJournal().version) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: `schema migration ${schemaState.migrationId ?? "unknown"} is in progress`,
                hint: "retry after the schema migration activates",
            });
        }
        return {
            ...this.routingStore.route(vshard),
            domainSchemaEpoch: schemaState.activeEpoch,
            recoveryGeneration: this.recoveryGeneration(),
        };
    }

    /** Private operational status routed through the current vshard owner. */
    async organizationDeletionPurgeStatus(input: {
        readonly organizationId: string;
    }): Promise<CatalogOrganizationDeletionStatus> {
        try {
            await this.bootstrap();
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const store = new CatalogOrganizationDeletionStore(sql);
            const deletion = store.read(input.organizationId);
            const shards = deletion ? store.shards(input.organizationId) : [];
            const handoff = projectOrganizationDeletionHandoff(deletion, shards);
            if (!deletion) {
                return Object.freeze({
                    organizationId: input.organizationId,
                    authDeleted: false,
                    handoffComplete: false,
                    handoff,
                    vectorPurge: null,
                });
            }
            if (!this.hasVectorCleanupResources()) {
                return Object.freeze({
                    organizationId: deletion.organizationId,
                    authDeleted: true,
                    handoffComplete: deletion.status === "complete",
                    handoff,
                    vectorPurge: null,
                });
            }
            const shardNamespace = this.env.CDB_SHARD;
            if (!shardNamespace) {
                throw new CdbError({ code: "CDB_SHARD_UNAVAILABLE", message: "CDB_SHARD binding is unavailable" });
            }
            const vectorPurge = await readCurrentOwnerVectorPurgeStatus({
                organizationId: deletion.organizationId,
                vshard: deletion.vshard,
                deps: {
                    route: vshard => this.route(vshard),
                    cdb: shardId =>
                        shardNamespace.get(shardNamespace.idFromName(shardId)) as unknown as CdbOrganizationDeletionRpc,
                },
            });
            if (deletion.status === "complete" && vectorPurge === null) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: "completed vector deletion handoff has no current-owner purge tombstone",
                });
            }
            return Object.freeze({
                organizationId: deletion.organizationId,
                authDeleted: true,
                handoffComplete: deletion.status === "complete",
                handoff,
                vectorPurge,
            });
        } catch (error) {
            throwCdbRpcError(rehydrateCdbRpcError(error));
        }
    }

    override async alarm(): Promise<void> {
        if (abortForArmedRecoveryRestore(this.ctx, adaptSqlStorage(this.ctx.storage.sql))) return;
        if (new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).blocksBackgroundWork()) return;
        const nowMs = Date.now();
        await this.deliverAuthInvalidations(nowMs);
        if (this.hasOrganizationCleanupResources()) await this.deliverOrganizationDeletions(nowMs);
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const nextAuthInvalidation = new CatalogAuthInvalidationStore(sql).nextPendingAt();
        const nextDeletion = this.hasOrganizationCleanupResources()
            ? new CatalogOrganizationDeletionStore(sql).nextPendingAt()
            : null;
        const nextAlarm = this.earliestPending(nextAuthInvalidation, nextDeletion);
        if (nextAlarm !== null) await this.scheduleAlarmNoLaterThan(Math.max(nowMs + 1, nextAlarm));
    }

    private async deliverOrganizationDeletions(nowMs: number): Promise<void> {
        const store = new CatalogOrganizationDeletionStore(adaptSqlStorage(this.ctx.storage.sql));
        // Reconcile deletion rows written by the earlier parent-only outbox shape.
        for (const deletion of store.due(nowMs)) {
            try {
                this.ctx.storage.transactionSync(() => {
                    const currentStore = new CatalogOrganizationDeletionStore(adaptSqlStorage(this.ctx.storage.sql));
                    const current = currentStore.read(deletion.organizationId);
                    if (!current || current.status === "complete") return;
                    currentStore.recordShards(
                        deletion.organizationId,
                        [this.organizationDeletionTarget(deletion.vshard)],
                        nowMs
                    );
                });
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                this.ctx.storage.transactionSync(() => {
                    const currentStore = new CatalogOrganizationDeletionStore(adaptSqlStorage(this.ctx.storage.sql));
                    const current = currentStore.read(deletion.organizationId);
                    if (current?.status === "pending") currentStore.defer(deletion.organizationId, nowMs, message);
                });
            }
        }
        const handoffs = store.dueShards(nowMs);
        const outcomes = await Promise.all(
            handoffs.map(async handoff => {
                try {
                    if (!this.env.CDB_SHARD) {
                        throw new CdbError({
                            code: "CDB_SHARD_UNAVAILABLE",
                            message: "CDB_SHARD binding is unavailable",
                        });
                    }
                    const route = await this.route(handoff.vshard);
                    const id = this.env.CDB_SHARD.idFromName(handoff.shardId);
                    const cdb = this.env.CDB_SHARD.get(id) as unknown as CdbOrganizationDeletionRpc;
                    const result = await cdb.deleteOrganizationFiles({
                        organizationId: handoff.organizationId,
                        nowMs,
                        domainSchemaEpoch: route.domainSchemaEpoch,
                        recoveryGeneration: route.recoveryGeneration,
                    });
                    this.assertOrganizationDeletionResult(result, handoff.organizationId);
                    return { handoff, error: null } as const;
                } catch (error) {
                    return { handoff, error: error instanceof Error ? error : new Error(String(error)) } as const;
                }
            })
        );
        this.ctx.storage.transactionSync(() => {
            const currentStore = new CatalogOrganizationDeletionStore(adaptSqlStorage(this.ctx.storage.sql));
            for (const outcome of outcomes) {
                const current = currentStore.read(outcome.handoff.organizationId);
                if (!current || current.status === "complete") continue;
                if (outcome.error === null) {
                    currentStore.completeShard(outcome.handoff.organizationId, outcome.handoff.shardId, nowMs);
                } else {
                    const message = outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
                    currentStore.deferShard(outcome.handoff.organizationId, outcome.handoff.shardId, nowMs, message);
                }
            }
        });
    }

    private enqueueAuthInvalidations(sql: CatalogSql, changes: readonly CatalogAuthEpochChange[], nowMs: number): void {
        const store = new CatalogAuthInvalidationStore(sql);
        const activeTopology = new CatalogTopologyOperationStore(sql).active();
        for (const change of changes) {
            if (change.scope === "global") {
                store.enqueueGlobal(change.epoch, nowMs);
                continue;
            }
            if (change.scope === "principal") {
                store.enqueuePrincipal(change.scopeId, change.epoch, nowMs);
                continue;
            }
            const vshard = Number(vshardOf([change.scopeId]));
            const shardIds = new Set<string>([this.routingStore.route(vshard).shardId]);
            if (activeTopology && activeTopology.rangeLo <= vshard && vshard <= activeTopology.rangeHi) {
                shardIds.add(activeTopology.sourceShard);
                shardIds.add(activeTopology.destinationShard);
            }
            store.enqueueTargets(change.scope, change.scopeId, change.epoch, [...shardIds], nowMs);
        }
    }

    /** The current owner captures pre-cutover tombstones; the range barrier closes the handoff race. */
    private organizationDeletionTarget(vshard: number): string {
        return this.routingStore.route(vshard).shardId;
    }

    private async deliverAuthInvalidations(nowMs: number): Promise<void> {
        const store = new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql));
        const targets = store.dueTargets(nowMs);
        const outcomes = await Promise.all(
            targets.map(async target => {
                try {
                    const request = {
                        scope: target.scope,
                        scopeId: target.scopeId,
                        epoch: target.epoch,
                        recoveryGeneration: this.recoveryGeneration(),
                    };
                    const result = await this.callAuthInvalidation(target.shardId, request);
                    this.assertAuthInvalidationResult(result, request);
                    return { target, error: null } as const;
                } catch (error) {
                    return { target, error: error instanceof Error ? error : new Error(String(error)) } as const;
                }
            })
        );
        this.ctx.storage.transactionSync(() => {
            const current = new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql));
            for (const outcome of outcomes) {
                if (outcome.error === null) current.completeTarget(outcome.target);
                else current.deferTarget(outcome.target, nowMs, outcome.error);
            }
        });

        const principal = store.duePrincipal(nowMs);
        if (principal) {
            const page = this.globalAuthInvalidationShardPage(principal.cursorShardId);
            if (page.length === 0) {
                this.ctx.storage.transactionSync(() =>
                    new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql)).completePrincipal(
                        principal.scopeId,
                        principal.epoch
                    )
                );
            } else {
                const lastShardId = page.at(-1);
                if (!lastShardId) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: "principal auth shard page is empty" });
                }
                const request = {
                    scope: "principal" as const,
                    scopeId: principal.scopeId,
                    epoch: principal.epoch,
                    recoveryGeneration: this.recoveryGeneration(),
                };
                const principalOutcomes = await Promise.all(
                    page.map(async shardId => {
                        try {
                            const result = await this.callAuthInvalidation(shardId, request);
                            this.assertAuthInvalidationResult(result, request);
                            return null;
                        } catch (error) {
                            return error instanceof Error ? error : new Error(String(error));
                        }
                    })
                );
                const failure = principalOutcomes.find(error => error !== null);
                this.ctx.storage.transactionSync(() => {
                    const current = new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql));
                    if (failure) current.deferPrincipal(principal.scopeId, principal.epoch, nowMs, failure);
                    else current.advancePrincipal(principal.scopeId, principal.epoch, lastShardId, nowMs);
                });
            }
        }

        const global = store.dueGlobal(nowMs);
        if (!global) return;
        const page = this.globalAuthInvalidationShardPage(global.cursorShardId);
        if (page.length === 0) {
            this.ctx.storage.transactionSync(() =>
                new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql)).completeGlobal(global.epoch)
            );
            return;
        }
        const lastShardId = page.at(-1);
        if (!lastShardId) throw new CdbError({ code: "CDB_INVARIANT", message: "global auth shard page is empty" });
        const request = {
            scope: "global" as const,
            scopeId: "global",
            epoch: global.epoch,
            recoveryGeneration: this.recoveryGeneration(),
        };
        const globalOutcomes = await Promise.all(
            page.map(async shardId => {
                try {
                    const result = await this.callAuthInvalidation(shardId, request);
                    this.assertAuthInvalidationResult(result, request);
                    return null;
                } catch (error) {
                    return error instanceof Error ? error : new Error(String(error));
                }
            })
        );
        const failure = globalOutcomes.find(error => error !== null);
        this.ctx.storage.transactionSync(() => {
            const current = new CatalogAuthInvalidationStore(adaptSqlStorage(this.ctx.storage.sql));
            if (failure) current.deferGlobal(global.epoch, nowMs, failure);
            else current.advanceGlobal(global.epoch, lastShardId, nowMs);
        });
    }

    private globalAuthInvalidationShardPage(afterExclusive: string | null): readonly string[] {
        const shardIds = new Set<string>(
            this.routingStore.listShardIdsPage(afterExclusive, CATALOG_AUTH_INVALIDATION_BATCH_SIZE)
        );
        const active = new CatalogTopologyOperationStore(adaptSqlStorage(this.ctx.storage.sql)).active();
        if (active) {
            for (const participant of [active.sourceShard, active.destinationShard]) {
                if (afterExclusive === null || participant > afterExclusive) shardIds.add(participant);
            }
        }
        return [...shardIds].sort().slice(0, CATALOG_AUTH_INVALIDATION_BATCH_SIZE);
    }

    private async callAuthInvalidation(
        shardId: string,
        request: CdbAuthInvalidationRequest
    ): Promise<CdbAuthInvalidationResult> {
        if (!this.env.CDB_SHARD) {
            throw new CdbError({ code: "CDB_SHARD_UNAVAILABLE", message: "CDB_SHARD binding is unavailable" });
        }
        const id = this.env.CDB_SHARD.idFromName(shardId);
        return (this.env.CDB_SHARD.get(id) as unknown as CdbAuthInvalidationRpc).invalidateAuthScope(request);
    }

    private assertAuthInvalidationResult(result: CdbAuthInvalidationResult, request: CdbAuthInvalidationRequest): void {
        if (
            !result ||
            result.accepted !== true ||
            result.scope !== request.scope ||
            result.scopeId !== request.scopeId ||
            !Number.isSafeInteger(result.epoch) ||
            result.epoch < request.epoch ||
            result.recoveryGeneration !== request.recoveryGeneration ||
            !Number.isSafeInteger(result.registrations) ||
            result.registrations < 0 ||
            !Number.isSafeInteger(result.changeSeq) ||
            result.changeSeq < 0 ||
            Object.keys(result).length !== 7
        ) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "Cdb auth invalidation response is invalid" });
        }
    }

    private earliestPending(left: number | null, right: number | null): number | null {
        if (left === null) return right;
        if (right === null) return left;
        return Math.min(left, right);
    }

    private hasOrganizationCleanupResources(): boolean {
        return this.migrationJournal().migrations.some(migration =>
            migration.resources.some(resource => resource.kind === "file" || resource.kind === "vector")
        );
    }

    private hasVectorCleanupResources(): boolean {
        return chardbResourceDescriptorsAt(this.migrationJournal().migrations).some(isChardbVectorResourceDescriptor);
    }

    private assertOrganizationDeletionResult(
        result: Awaited<ReturnType<CdbOrganizationDeletionRpc["deleteOrganizationFiles"]>>,
        organizationId: string
    ): void {
        if (
            !result ||
            result.organizationId !== organizationId ||
            result.accepted !== true ||
            Object.keys(result).length !== 2
        ) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "Cdb organization cleanup response is invalid" });
        }
    }

    private async scheduleAlarmNoLaterThan(deadline: number): Promise<void> {
        await this.alarmScheduler.scheduleEarlier(deadline);
    }

    schemaState(): CatalogSchemaState & { readonly recoveryGeneration: number } {
        this.assertRecoveryAvailable();
        return { ...this.readSchemaState(), recoveryGeneration: this.recoveryGeneration() };
    }

    async adminRecoveryBookmark(args: { readonly atMs?: number }): Promise<{
        readonly bookmark: string;
        readonly atMs: number;
    }> {
        return await this.adminMigrationRpc(() => this.recovery.bookmark(args.atMs));
    }

    async adminArmRecoveryRestore(args: {
        readonly bookmark: string;
        readonly armedAt: number;
        readonly operationId: string;
        readonly generation: number;
        readonly schema: { readonly version: number; readonly epoch: number; readonly digest: string };
        readonly routingEpoch: number;
        readonly shardIds: readonly string[];
    }) {
        return await this.adminMigrationRpc(() =>
            this.recovery.arm(
                args.bookmark,
                args.armedAt,
                {
                    operationId: args.operationId,
                    generation: args.generation,
                },
                sql => {
                    const schema = this.readSchemaState();
                    const shardIds = [...this.routingStore.listShardIds()].sort();
                    const unchanged =
                        schema.status === "active" &&
                        schema.activeVersion === args.schema.version &&
                        schema.activeEpoch === args.schema.epoch &&
                        schema.activeDigest === args.schema.digest &&
                        this.routingStore.route(0).schemaEpoch === args.routingEpoch &&
                        JSON.stringify(shardIds) === JSON.stringify([...args.shardIds].sort());
                    if (!unchanged) {
                        throw new CdbError({
                            code: "CDB_STALE_EPOCH",
                            message: "Catalog changed after recovery preflight",
                        });
                    }
                    new CatalogTopologyOperationStore(sql).assertNoActive();
                }
            )
        );
    }

    async adminReleaseRecovery(args: { readonly operationId: string; readonly generation: number }) {
        return await this.adminMigrationRpc(() => this.recovery.release(args.operationId, args.generation));
    }

    async adminCancelRecoveryRestore(args: { readonly bookmark: string }) {
        return await this.adminMigrationRpc(() => this.recovery.cancel(args.bookmark));
    }

    async adminCommitRecoveryRestore(args: { readonly bookmark: string }) {
        return await this.adminMigrationRpc(() => this.recovery.commit(args.bookmark));
    }

    async adminRecoveryRestoreStatus(args: { readonly bookmark: string }) {
        return await this.adminMigrationRpc(() => this.recovery.status(args.bookmark));
    }

    async adminSchemaState(): Promise<CatalogSchemaState> {
        return await this.adminMigrationRpc(() => this.schemaState());
    }

    async adminBeginSchemaMigration(args: {
        readonly migrationId: string;
        readonly targetVersion: number;
    }): Promise<CatalogSchemaState> {
        return await this.adminMigrationRpc(() => this.beginSchemaMigration(args));
    }

    async adminBeginSchemaBaseline(args: {
        readonly migrationId: string;
        readonly targetVersion: number;
    }): Promise<CatalogSchemaState> {
        return await this.adminMigrationRpc(() => this.beginSchemaBaseline(args));
    }

    async adminSchemaMigrationShards(args: {
        readonly migrationId: string;
    }): Promise<readonly CatalogSchemaShardState[]> {
        return await this.adminMigrationRpc(() => this.schemaMigrationShards(args));
    }

    async adminMigrateSchemaShard(args: {
        readonly migrationId: string;
        readonly shardId: string;
    }): Promise<CatalogSchemaShardState> {
        return await this.adminMigrationRpc(() => this.migrateSchemaShard(args));
    }

    async adminApplyCatalogSchemaMigration(args: {
        readonly migrationId: string;
        readonly version: number;
    }): Promise<CatalogSchemaState> {
        return await this.adminMigrationRpc(() => this.applyCatalogSchemaMigration(args));
    }

    async adminCompleteSchemaMigration(args: { readonly migrationId: string }): Promise<CatalogSchemaState> {
        return await this.adminMigrationRpc(() => this.completeSchemaMigration(args));
    }

    private async adminMigrationRpc<T>(operation: () => T | Promise<T>): Promise<T> {
        try {
            return await operation();
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    private assertRecoveryAvailable(): void {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        assertRecoveryAvailable(sql);
        const admission = new RecoveryAdmissionStore(sql).read();
        if (admission.state === "blocked") {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "point-in-time restore is in progress" });
        }
    }

    private recoveryGeneration(): number {
        this.assertRecoveryAvailable();
        return new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).read().generation;
    }

    private async reconcileRecoveryAdmission(): Promise<void> {
        if (!this.env.CDB_RESHARD) {
            throw new CdbError({ code: "CDB_SHARD_UNAVAILABLE", message: "CDB_RESHARD binding is unavailable" });
        }
        const coordinator = this.env.CDB_RESHARD.get(this.env.CDB_RESHARD.idFromName("global")) as unknown as {
            adminRecoveryAdmissionClock(): Promise<RecoveryAdmissionClock>;
        };
        const clock = await coordinator.adminRecoveryAdmissionClock();
        this.ctx.storage.transactionSync(() => {
            new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).reconcile(clock);
        });
    }

    beginSchemaMigration(args: { readonly migrationId: string; readonly targetVersion: number }): CatalogSchemaState {
        return this.beginSchemaChange(args, false);
    }

    beginSchemaBaseline(args: { readonly migrationId: string; readonly targetVersion: number }): CatalogSchemaState {
        return this.beginSchemaChange(args, true);
    }

    private beginSchemaChange(
        args: { readonly migrationId: string; readonly targetVersion: number },
        baseline: boolean
    ): CatalogSchemaState {
        this.assertRecoveryAvailable();
        if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(args.migrationId)) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "schema migration id is invalid" });
        }
        const journal = this.migrationJournal();
        if (
            !Number.isSafeInteger(args.targetVersion) ||
            args.targetVersion < 1 ||
            args.targetVersion !== journal.version
        ) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "schema migration target version is invalid" });
        }
        this.ctx.storage.transactionSync(() => {
            new CatalogTopologyOperationStore(adaptSqlStorage(this.ctx.storage.sql)).assertNoActive();
            beginCatalogSchemaChange(adaptSqlStorage(this.ctx.storage.sql), journal, args, baseline);
        });
        return this.readSchemaState();
    }

    schemaMigrationShards(args: { readonly migrationId: string }): readonly CatalogSchemaShardState[] {
        return readCatalogSchemaMigrationShards(adaptSqlStorage(this.ctx.storage.sql), args);
    }

    async migrateSchemaShard(args: {
        readonly migrationId: string;
        readonly shardId: string;
    }): Promise<CatalogSchemaShardState> {
        const recoveryGeneration = this.recoveryGeneration();
        const current = this.readSchemaState();
        if (
            current.status !== "migrating" ||
            current.migrationId !== args.migrationId ||
            current.targetVersion === null ||
            current.targetEpoch === null ||
            current.targetDigest === null
        ) {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "schema migration does not own Catalog" });
        }
        const stored = this.schemaMigrationShards({ migrationId: args.migrationId }).find(
            shard => shard.shardId === args.shardId
        );
        if (!stored) throw new CdbError({ code: "CDB_INVALID_ARGS", message: "schema migration shard is unknown" });
        if (stored.status === "active") return stored;
        if (!this.env.CDB_SHARD) {
            throw new CdbError({ code: "CDB_SHARD_UNAVAILABLE", message: "CDB_SHARD binding is unavailable" });
        }
        try {
            const id = this.env.CDB_SHARD.idFromName(args.shardId);
            const cdb = this.env.CDB_SHARD.get(id) as unknown as CdbSchemaMigrationRpc;
            const baseline = catalogSchemaBaselineExists(adaptSqlStorage(this.ctx.storage.sql), args.migrationId);
            if (baseline) {
                await cdb.baselineSchemaMigration({
                    recoveryGeneration,
                    migrationId: args.migrationId,
                    targetVersion: current.targetVersion,
                    targetEpoch: current.targetEpoch,
                    targetDigest: current.targetDigest,
                });
            } else {
                await cdb.prepareSchemaMigration({
                    recoveryGeneration,
                    migrationId: args.migrationId,
                    activeVersion: current.activeVersion,
                    activeDigest: current.activeDigest,
                    targetVersion: current.targetVersion,
                    targetEpoch: current.targetEpoch,
                    targetDigest: current.targetDigest,
                });
                for (const migration of pendingMigrations(this.migrationJournal(), current.activeVersion)) {
                    if (migration.version > current.targetVersion) break;
                    await cdb.applySchemaMigration({
                        migrationId: args.migrationId,
                        version: migration.version,
                        recoveryGeneration,
                    });
                }
                await cdb.activateSchemaMigration({ migrationId: args.migrationId, recoveryGeneration });
            }
        } catch (error) {
            const message = (error instanceof Error ? error.message : String(error)).slice(0, 512);
            this.ctx.storage.transactionSync(() => {
                recordCatalogSchemaShardFailure(adaptSqlStorage(this.ctx.storage.sql), args, message);
            });
            throw error;
        }
        this.ctx.storage.transactionSync(() => {
            new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).assertRequest(recoveryGeneration);
            activateCatalogSchemaShard(adaptSqlStorage(this.ctx.storage.sql), args);
        });
        const active = this.schemaMigrationShards({ migrationId: args.migrationId }).find(
            shard => shard.shardId === args.shardId
        );
        if (!active) throw new CdbError({ code: "CDB_INVARIANT", message: "schema migration shard disappeared" });
        return active;
    }

    applyCatalogSchemaMigration(args: { readonly migrationId: string; readonly version: number }): CatalogSchemaState {
        this.assertRecoveryAvailable();
        const journal = this.migrationJournal();
        if (!Number.isSafeInteger(args.version) || args.version < 1 || args.version > journal.version) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "Catalog schema migration version is invalid" });
        }
        this.ctx.storage.transactionSync(() => {
            applyCatalogSchemaMigrationStep(adaptSqlStorage(this.ctx.storage.sql), journal, args);
        });
        return this.readSchemaState();
    }

    completeSchemaMigration(args: { readonly migrationId: string }): CatalogSchemaState {
        this.assertRecoveryAvailable();
        this.ctx.storage.transactionSync(() => {
            completeCatalogSchemaMigration(
                adaptSqlStorage(this.ctx.storage.sql),
                this.migrationJournal(),
                args,
                recordMigratedCatalogAuthoritySchema
            );
        });
        this.authTablesBootstrapped = true;
        return this.readSchemaState();
    }

    /** Return each physical shard that owns at least one current vshard range. */
    async listShardIds(): Promise<readonly ShardId[]> {
        return this.routingStore.listShardIds();
    }

    async adminRecoveryInventory(args: { readonly armedBookmark?: string } = {}): Promise<{
        readonly schema: CatalogSchemaState;
        readonly routingEpoch: number;
        readonly shardIds: readonly ShardId[];
    }> {
        return await this.adminMigrationRpc(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            assertRecoveryAvailableFor(sql, args.armedBookmark);
            new CatalogTopologyOperationStore(sql).assertNoActive();
            const schema = this.readSchemaState();
            if (schema.status !== "active") {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "schema migration blocks a recovery point",
                });
            }
            return {
                schema,
                routingEpoch: this.routingStore.route(0).schemaEpoch,
                shardIds: this.routingStore.listShardIds(),
            };
        });
    }

    /** Claim the Catalog topology before any range data moves. */
    beginDerivedTopologyOperation(args: CatalogDerivedTopologyOperationRequest): CatalogTopologyOperation {
        assertDerivedTopologyOperationRequest(args);
        let operation: CatalogTopologyOperation | null = null;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
            const store = new CatalogTopologyOperationStore(sql);
            const existing = store.read(args.migId);
            if (existing) {
                const schema = this.readSchemaState();
                if (
                    schema.status !== "active" ||
                    schema.activeVersion !== existing.schemaVersion ||
                    schema.activeEpoch !== existing.schemaEpoch ||
                    schema.activeDigest !== existing.schemaDigest
                ) {
                    throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology schema identity changed" });
                }
                operation = store.begin(
                    {
                        ...existing,
                        destinationShard: args.destinationShard,
                        rangeLo: args.rangeLo,
                        rangeHi: args.rangeHi,
                        recoveryGeneration: args.recoveryGeneration,
                    },
                    Date.now()
                );
                return;
            }
            const schema = this.readSchemaState();
            if (schema.status !== "active" || schema.activeVersion !== this.migrationJournal().version) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "schema migration blocks topology operation" });
            }
            const start = this.routingStore.route(args.rangeLo);
            const end = this.routingStore.route(args.rangeHi);
            if (
                start.shardId !== end.shardId ||
                start.schemaEpoch !== end.schemaEpoch ||
                !this.routingStore.ownsRange(args.rangeLo, args.rangeHi, start.shardId)
            ) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "topology range does not have one exact current owner",
                });
            }
            if (args.destinationShard === start.shardId) {
                throw new CdbError({
                    code: "CDB_INVALID_ARGS",
                    message: "topology source and destination must differ",
                });
            }
            operation = store.begin(
                {
                    migrationId: args.migId,
                    sourceShard: start.shardId,
                    destinationShard: args.destinationShard,
                    rangeLo: args.rangeLo,
                    rangeHi: args.rangeHi,
                    startEpoch: start.schemaEpoch,
                    schemaVersion: schema.activeVersion,
                    schemaEpoch: schema.activeEpoch,
                    schemaDigest: schema.activeDigest,
                    recoveryGeneration: args.recoveryGeneration,
                },
                Date.now()
            );
        });
        if (!operation) throw new CdbError({ code: "CDB_INVARIANT", message: "topology operation was not recorded" });
        return operation;
    }

    /** Claim the Catalog topology with an already-derived exact source identity. */
    beginTopologyOperation(args: CatalogTopologyOperationRequest): CatalogTopologyOperation {
        let operation: CatalogTopologyOperation | null = null;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
            const store = new CatalogTopologyOperationStore(sql);
            const existing = store.read(args.migId);
            if (existing) {
                const schema = this.readSchemaState();
                if (
                    schema.status !== "active" ||
                    schema.activeVersion !== existing.schemaVersion ||
                    schema.activeEpoch !== existing.schemaEpoch ||
                    schema.activeDigest !== existing.schemaDigest
                ) {
                    throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology schema identity changed" });
                }
                operation = store.begin(
                    this.topologyIdentity(args, existing.schemaVersion, existing.schemaEpoch, existing.schemaDigest),
                    Date.now()
                );
                return;
            }
            const schema = this.readSchemaState();
            if (schema.status !== "active" || schema.activeVersion !== this.migrationJournal().version) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "schema migration blocks topology operation" });
            }
            const start = this.routingStore.route(args.rangeLo);
            const end = this.routingStore.route(args.rangeHi);
            if (
                start.schemaEpoch !== args.startEpoch ||
                end.schemaEpoch !== args.startEpoch ||
                start.shardId !== args.sourceShard ||
                end.shardId !== args.sourceShard ||
                !this.routingStore.ownsRange(args.rangeLo, args.rangeHi, args.sourceShard)
            ) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "topology source identity does not match current routing",
                });
            }
            operation = store.begin(
                this.topologyIdentity(args, schema.activeVersion, schema.activeEpoch, schema.activeDigest),
                Date.now()
            );
        });
        if (!operation) throw new CdbError({ code: "CDB_INVARIANT", message: "topology operation was not recorded" });
        return operation;
    }

    topologyOperation(args: {
        readonly migrationId: string;
        readonly recoveryGeneration: number;
    }): CatalogTopologyOperation | null {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
        return new CatalogTopologyOperationStore(sql).read(args.migrationId);
    }

    topologyRoutingStatus(args: CatalogTopologyOperationRequest): {
        readonly owner: "source" | "destination";
        readonly schemaEpoch: number;
        readonly operationStatus: CatalogTopologyOperation["status"];
    } {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
        const operation = new CatalogTopologyOperationStore(sql).read(args.migId);
        if (
            !operation ||
            operation.sourceShard !== args.sourceShard ||
            operation.destinationShard !== args.destinationShard ||
            operation.rangeLo !== args.rangeLo ||
            operation.rangeHi !== args.rangeHi ||
            operation.startEpoch !== args.startEpoch ||
            operation.recoveryGeneration !== args.recoveryGeneration
        ) {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology recovery identity does not match" });
        }
        const start = this.routingStore.route(args.rangeLo);
        const end = this.routingStore.route(args.rangeHi);
        const source =
            start.schemaEpoch === args.startEpoch &&
            end.schemaEpoch === args.startEpoch &&
            start.shardId === args.sourceShard &&
            end.shardId === args.sourceShard &&
            this.routingStore.ownsRange(args.rangeLo, args.rangeHi, args.sourceShard);
        const destination =
            start.schemaEpoch === args.startEpoch + 1 &&
            end.schemaEpoch === args.startEpoch + 1 &&
            start.shardId === args.destinationShard &&
            end.shardId === args.destinationShard &&
            this.routingStore.ownsRange(args.rangeLo, args.rangeHi, args.destinationShard);
        if (source === destination) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "topology recovery does not match an exact source or destination owner",
            });
        }
        if ((operation.status === "aborted" && !source) || (operation.status === "completed" && !destination)) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "topology recovery owner contradicts its operation status",
            });
        }
        return {
            owner: source ? "source" : "destination",
            schemaEpoch: start.schemaEpoch,
            operationStatus: operation.status,
        };
    }

    /** Fence new Better Auth organization deletions before the final resource-tail convergence pass. */
    async beginOrganizationDeletionBarrier(
        args: CatalogOrganizationDeletionBarrierRequest & { readonly recoveryGeneration: number }
    ): Promise<CatalogOrganizationDeletionBarrier> {
        const identity = this.deletionBarrierIdentity(args);
        const nowMs = Date.now();
        let barrier: CatalogOrganizationDeletionBarrier | null = null;
        let ready = false;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
            const topology = new CatalogTopologyOperationStore(sql).read(args.migId);
            if (
                !topology ||
                topology.status !== "active" ||
                topology.rangeLo !== args.rangeLo ||
                topology.rangeHi !== args.rangeHi
            ) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "organization deletion barrier does not match an active topology operation",
                });
            }
            const store = new CatalogOrganizationDeletionBarrierStore(sql);
            barrier = store.begin(identity, nowMs);
            ready = store.status(identity).olderDeletionsComplete;
        });
        if (!barrier) throw new CdbError({ code: "CDB_INVARIANT", message: "deletion barrier was not recorded" });
        if (!ready) await this.scheduleAlarmNoLaterThan(nowMs + 1);
        return barrier;
    }

    organizationDeletionBarrierStatus(
        args: CatalogOrganizationDeletionBarrierRequest & { readonly recoveryGeneration: number }
    ): CatalogOrganizationDeletionBarrierStatus {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
        return new CatalogOrganizationDeletionBarrierStore(sql).status(this.deletionBarrierIdentity(args));
    }

    completeTopologyOperation(args: CatalogTopologyOperationRequest): CatalogTopologyOperation {
        let operation: CatalogTopologyOperation | null = null;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
            const store = new CatalogTopologyOperationStore(sql);
            const stored = store.read(args.migId);
            if (!stored) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology operation lease is missing" });
            }
            const identity = this.topologyIdentity(args, stored.schemaVersion, stored.schemaEpoch, stored.schemaDigest);
            const exact = store.begin(identity, Date.now());
            if (exact.status === "completed") {
                operation = exact;
                return;
            }
            if (exact.status === "aborted") {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "aborted topology operation cannot complete" });
            }
            const start = this.routingStore.route(args.rangeLo);
            const end = this.routingStore.route(args.rangeHi);
            if (
                start.schemaEpoch !== args.startEpoch + 1 ||
                end.schemaEpoch !== args.startEpoch + 1 ||
                start.shardId !== args.destinationShard ||
                end.shardId !== args.destinationShard ||
                !this.routingStore.ownsRange(args.rangeLo, args.rangeHi, args.destinationShard)
            ) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "topology completion does not match current routing",
                });
            }
            operation = store.complete(identity, start.schemaEpoch, Date.now());
        });
        if (!operation) throw new CdbError({ code: "CDB_INVARIANT", message: "topology operation did not complete" });
        return operation;
    }

    abortTopologyOperation(args: CatalogTopologyOperationRequest): CatalogTopologyOperation {
        let operation: CatalogTopologyOperation | null = null;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
            const store = new CatalogTopologyOperationStore(sql);
            const stored = store.read(args.migId);
            if (!stored) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology operation lease is missing" });
            }
            const identity = this.topologyIdentity(args, stored.schemaVersion, stored.schemaEpoch, stored.schemaDigest);
            const exact = store.begin(identity, Date.now());
            if (exact.status === "aborted") {
                operation = exact;
                return;
            }
            if (exact.status === "completed") {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "completed topology operation cannot abort" });
            }
            const start = this.routingStore.route(args.rangeLo);
            const end = this.routingStore.route(args.rangeHi);
            if (
                start.schemaEpoch !== args.startEpoch ||
                end.schemaEpoch !== args.startEpoch ||
                start.shardId !== args.sourceShard ||
                end.shardId !== args.sourceShard ||
                !this.routingStore.ownsRange(args.rangeLo, args.rangeHi, args.sourceShard)
            ) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "a cut-over topology operation cannot abort" });
            }
            const nowMs = Date.now();
            operation = store.abort(identity, nowMs);
            const barriers = new CatalogOrganizationDeletionBarrierStore(adaptSqlStorage(this.ctx.storage.sql));
            const barrier = barriers.read(args.migId);
            if (barrier) barriers.abort(this.deletionBarrierIdentity(args), nowMs);
        });
        if (!operation) throw new CdbError({ code: "CDB_INVARIANT", message: "topology operation did not abort" });
        return operation;
    }

    /**
     * Atomic cutover for a vshard range. Combines (a) the range-table edit that
     * reassigns `[lo, hi]` from `fromShard` to `toShard`, (b) a schema-epoch
     * bump that invalidates every cached client route, and (c) an idempotency
     * guard keyed by `migId` so a retry after a crash sees `applied=true` and
     * leaves state unchanged. The whole sequence runs inside a single
     * `transactionSync` so external observers either see the pre-cutover map at
     * `epoch=N` or the post-cutover map at `epoch=N+1`, never a half-applied
     * intermediate. Mirrors the `CatalogCutover` action in `spec/Resharder.tla`.
     */
    async cutover(
        args: CatalogCutoverRequest & { readonly startEpoch?: number; readonly recoveryGeneration: number }
    ): Promise<CatalogCutoverResult> {
        new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).assertRequest(args.recoveryGeneration);
        const schemaState = this.readSchemaState();
        const storedTopology = new CatalogTopologyOperationStore(adaptSqlStorage(this.ctx.storage.sql)).read(
            args.migId
        );
        const schemaMatchesStoredTopology =
            storedTopology !== null &&
            schemaState.activeVersion === storedTopology.schemaVersion &&
            schemaState.activeEpoch === storedTopology.schemaEpoch &&
            schemaState.activeDigest === storedTopology.schemaDigest;
        if (
            schemaState.status !== "active" ||
            (storedTopology === null && schemaState.activeVersion !== this.migrationJournal().version) ||
            (storedTopology !== null && !schemaMatchesStoredTopology)
        ) {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "schema migration blocks routing cutover" });
        }
        return this.routingStore.cutover(args, {
            before: currentEpoch => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                new RecoveryAdmissionStore(sql).assertRequest(args.recoveryGeneration);
                const store = new CatalogTopologyOperationStore(sql);
                const existing = store.read(args.migId);
                if (!existing) {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "topology operation lease is missing",
                    });
                }
                const startEpoch = args.startEpoch ?? existing.startEpoch;
                const identity = this.topologyIdentity(
                    {
                        migId: args.migId,
                        sourceShard: args.fromShard,
                        destinationShard: args.toShard,
                        rangeLo: args.lo,
                        rangeHi: args.hi,
                        startEpoch,
                        recoveryGeneration: args.recoveryGeneration,
                    },
                    existing.schemaVersion,
                    existing.schemaEpoch,
                    existing.schemaDigest
                );
                const exact = store.begin(identity, Date.now());
                if (exact.status === "aborted") {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "aborted topology operation cannot cut over",
                    });
                }
                if (
                    exact.status === "active" &&
                    (schemaState.activeVersion !== exact.schemaVersion ||
                        schemaState.activeEpoch !== exact.schemaEpoch ||
                        schemaState.activeDigest !== exact.schemaDigest ||
                        (currentEpoch !== exact.startEpoch && currentEpoch !== exact.startEpoch + 1))
                ) {
                    throw new CdbError({ code: "CDB_STALE_EPOCH", message: "topology schema identity changed" });
                }
            },
            after: (_newEpoch, _applied) => {
                const store = new CatalogOrganizationDeletionBarrierStore(adaptSqlStorage(this.ctx.storage.sql));
                const barrier = store.read(args.migId);
                if (barrier) {
                    store.release(
                        {
                            migrationId: args.migId,
                            rangeLo: args.lo,
                            rangeHi: args.hi,
                        },
                        Date.now()
                    );
                }
            },
        });
    }

    private deletionBarrierIdentity(
        args: CatalogOrganizationDeletionBarrierRequest
    ): CatalogOrganizationDeletionBarrierIdentity {
        return { migrationId: args.migId, rangeLo: args.rangeLo, rangeHi: args.rangeHi };
    }

    private topologyIdentity(
        args: CatalogTopologyOperationRequest,
        schemaVersion: number,
        schemaEpoch: number,
        schemaDigest: string
    ): CatalogTopologyOperationIdentity {
        return {
            migrationId: args.migId,
            sourceShard: args.sourceShard,
            destinationShard: args.destinationShard,
            rangeLo: args.rangeLo,
            rangeHi: args.rangeHi,
            startEpoch: args.startEpoch,
            schemaVersion,
            schemaEpoch,
            schemaDigest,
            recoveryGeneration: args.recoveryGeneration,
        };
    }

    async bumpAuthEpoch(scope: "global" | "tenant" | "principal", scopeId: string): Promise<number> {
        const nowMs = Date.now();
        return this.alarmScheduler.transactionWithEarlierAlarm(nowMs, () => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const epoch = bumpCatalogAuthEpoch(sql, scope, scopeId);
            this.enqueueAuthInvalidations(sql, [{ scope, scopeId, epoch }], nowMs);
            return epoch;
        });
    }

    private readSchemaState(): CatalogSchemaState {
        return readCatalogSchemaState(adaptSqlStorage(this.ctx.storage.sql));
    }

    authEpoch(args: { tenantId?: TenantId; principalId?: PrincipalId }): {
        global: number;
        tenant: number;
        principal: number;
    } {
        return readCatalogAuthEpoch(adaptSqlStorage(this.ctx.storage.sql), args);
    }

    async resolveJwk(request: CatalogJwkResolutionRequest): Promise<CatalogJwkResolution> {
        const schemaState = this.readSchemaState();
        if (schemaState.status !== "active" || schemaState.activeVersion !== this.migrationJournal().version) {
            return jwksResolutionUnavailable(
                "Catalog schema migration blocks JWKS resolution",
                JWKS_FAILURE_BACKOFF_INITIAL_MS
            );
        }
        const jwksUrl = normalizeJwksUrl(request.jwksUrl);
        if (jwksUrl === null) {
            return jwksResolutionUnavailable("Catalog JWKS URL is invalid", JWKS_FAILURE_BACKOFF_MAX_MS);
        }
        if (request.kid.length === 0 || new TextEncoder().encode(request.kid).byteLength > JWKS_MAX_KID_BYTES) {
            return { ok: true, jwkJson: null };
        }

        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const scoped = this.readScopedJwk(sql, jwksUrl, request.kid);
        if (scoped && scoped.expiresAt > Date.now()) {
            try {
                parseCachedJwk(scoped.jwkJson);
                return { ok: true, jwkJson: scoped.jwkJson };
            } catch {
                // A corrupt cache row is never trusted. Treat it like an
                // expired row and require a successful remote replacement.
            }
        }

        const refreshState = sql.one<{
            next_fetch_at: number;
            refreshing_until: number;
            failure_count: number;
        }>(
            `SELECT next_fetch_at, refreshing_until, failure_count
             FROM catalog_jwks_refresh WHERE jwks_url = ?`,
            jwksUrl
        );

        const existing = this.jwksRefreshes.get(jwksUrl);
        if (existing) return this.finishJwkResolution(existing, jwksUrl, request.kid);

        const now = Date.now();
        if (refreshState && refreshState.refreshing_until > now) {
            return jwksResolutionUnavailable(
                "Catalog JWKS refresh is already in progress",
                refreshState.refreshing_until - now
            );
        }
        if (refreshState && refreshState.next_fetch_at > now) {
            if (refreshState.failure_count > 0) {
                return jwksResolutionUnavailable(
                    "Catalog JWKS refresh is cooling down after a failure",
                    refreshState.next_fetch_at - now
                );
            }
            if (!scoped) return { ok: true, jwkJson: null };
        }

        this.markJwksRefreshLease(sql, jwksUrl, now + JWKS_REFRESH_LEASE_MS);
        const refresh = this.refreshJwks(jwksUrl);
        this.jwksRefreshes.set(jwksUrl, refresh);
        try {
            return await this.finishJwkResolution(refresh, jwksUrl, request.kid);
        } finally {
            if (this.jwksRefreshes.get(jwksUrl) === refresh) this.jwksRefreshes.delete(jwksUrl);
        }
    }

    private async finishJwkResolution(
        refresh: Promise<JwksRefreshOutcome>,
        jwksUrl: string,
        kid: string
    ): Promise<CatalogJwkResolution> {
        const outcome = await refresh;
        if (!outcome.ok) return outcome;
        const row = this.readScopedJwk(adaptSqlStorage(this.ctx.storage.sql), jwksUrl, kid);
        if (!row || row.expiresAt <= Date.now()) return { ok: true, jwkJson: null };
        try {
            parseCachedJwk(row.jwkJson);
            return { ok: true, jwkJson: row.jwkJson };
        } catch {
            return jwksResolutionUnavailable("Catalog stored an invalid JWK", JWKS_FAILURE_BACKOFF_INITIAL_MS);
        }
    }

    private async refreshJwks(jwksUrl: string): Promise<JwksRefreshOutcome> {
        try {
            const fresh = await fetchValidatedJwks((url, init) => globalThis.fetch(url, init), jwksUrl);
            const now = Date.now();
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                sql.exec("DELETE FROM catalog_jwks_v2 WHERE jwks_url = ?", jwksUrl);
                for (const key of fresh.keys) {
                    sql.exec(
                        `INSERT INTO catalog_jwks_v2
                         (jwks_url, kid, jwk_json, fetched_at, expires_at) VALUES (?, ?, ?, ?, ?)`,
                        jwksUrl,
                        key.kid as string,
                        JSON.stringify(key),
                        now,
                        now + JWKS_CACHE_TTL_MS
                    );
                }
                sql.exec(
                    `INSERT INTO catalog_jwks_refresh
                     (jwks_url, next_fetch_at, refreshing_until, failure_count, last_success_at)
                     VALUES (?, ?, 0, 0, ?)
                     ON CONFLICT(jwks_url) DO UPDATE SET
                       next_fetch_at = excluded.next_fetch_at,
                       refreshing_until = 0,
                       failure_count = 0,
                       last_success_at = excluded.last_success_at`,
                    jwksUrl,
                    now + JWKS_SUCCESS_COOLDOWN_MS,
                    now
                );
            });
            return { ok: true };
        } catch (cause) {
            const now = Date.now();
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const previous = sql.one<{ failure_count: number }>(
                "SELECT failure_count FROM catalog_jwks_refresh WHERE jwks_url = ?",
                jwksUrl
            );
            const failureCount = Math.min((previous?.failure_count ?? 0) + 1, 31);
            const retryAfterMs = Math.min(
                JWKS_FAILURE_BACKOFF_MAX_MS,
                JWKS_FAILURE_BACKOFF_INITIAL_MS * 2 ** Math.min(failureCount - 1, 16)
            );
            try {
                sql.exec(
                    `INSERT INTO catalog_jwks_refresh
                     (jwks_url, next_fetch_at, refreshing_until, failure_count, last_success_at)
                     VALUES (?, ?, 0, ?, NULL)
                     ON CONFLICT(jwks_url) DO UPDATE SET
                       next_fetch_at = excluded.next_fetch_at,
                       refreshing_until = 0,
                       failure_count = excluded.failure_count`,
                    jwksUrl,
                    now + retryAfterMs,
                    failureCount
                );
            } catch {
                // The caller still receives a typed fail-closed result. A
                // broken Catalog store cannot promise durable cooldown state.
            }
            const message = cause instanceof Error ? cause.message : "Catalog JWKS refresh failed";
            return jwksResolutionUnavailable(message, retryAfterMs);
        }
    }

    private readScopedJwk(
        sql: CatalogSql,
        jwksUrl: string,
        kid: string
    ): { readonly jwkJson: string; readonly expiresAt: number } | null {
        const row = sql.one<{ jwk_json: string; expires_at: number }>(
            `SELECT jwk_json, expires_at FROM catalog_jwks_v2
             WHERE jwks_url = ? AND kid = ?`,
            jwksUrl,
            kid
        );
        return row ? { jwkJson: row.jwk_json, expiresAt: row.expires_at } : null;
    }

    private markJwksRefreshLease(sql: CatalogSql, jwksUrl: string, refreshingUntil: number): void {
        sql.exec(
            `INSERT INTO catalog_jwks_refresh
             (jwks_url, next_fetch_at, refreshing_until, failure_count, last_success_at)
             VALUES (?, 0, ?, 0, NULL)
             ON CONFLICT(jwks_url) DO UPDATE SET refreshing_until = excluded.refreshing_until`,
            jwksUrl,
            refreshingUntil
        );
    }

    async putJwk(kid: string, jwkJson: string, ttlMs: number): Promise<void> {
        const now = Date.now();
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        sql.exec(
            `INSERT INTO catalog_jwks (kid, jwk_json, fetched_at, expires_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(kid) DO UPDATE SET jwk_json = excluded.jwk_json, fetched_at = excluded.fetched_at, expires_at = excluded.expires_at`,
            kid,
            jwkJson,
            now,
            now + ttlMs
        );
    }

    async getJwk(kid: string): Promise<{ jwkJson: string; expiresAt: number } | null> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const row = sql.one<{ jwk_json: string; expires_at: number }>(
            "SELECT jwk_json, expires_at FROM catalog_jwks WHERE kid = ?",
            kid
        );
        return row ? { jwkJson: row.jwk_json, expiresAt: row.expires_at } : null;
    }
}

export function configureCatalogRuntime(config: CatalogRuntimeConfig): typeof Catalog {
    return class ConfiguredCatalog extends Catalog {
        protected override migrationJournal(): ChardbMigrationJournal {
            return config.migrations();
        }
    };
}
