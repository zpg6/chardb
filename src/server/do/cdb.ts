/**
 * `Cdb` shard DO.
 *
 * Owns one slice of vshards on its SQLite database. All single-partition
 * mutations land here; the op-log wrapper provides at-most-once semantics
 * dedup ledger atomically with each base mutation.
 *
 * This class is a thin runtime over the pure helpers in `chardb/oplog` and
 * `chardb/intervals`. The real exec happens on workerd
 * (https://developers.cloudflare.com/durable-objects/api/sql-storage/);
 * locally the class compiles but is exercised by integration tests under
 * wrangler/miniflare.
 */

import { DurableObject } from "cloudflare:workers";
import { renderSqliteTableDdl } from "../../auth/ddl.ts";
import { synthesizedAuthTableNames } from "../../auth/synthesize.ts";
import { CdbError, isCdbError, isCdbErrorCode, throwCdbRpcError } from "../../errors.ts";
import { intervalSetFromWire } from "../../intervals_wire.ts";
import { SHARD_BOOTSTRAP_DDL, initializeOpLogPlacement, initializeSplitLogAccounting } from "../../oplog/schema.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import type { RangeFilter } from "../../reshard/range.ts";
import type { TableSpec } from "../../reshard/triggers.ts";
import { type ChardbRef, ClientId, type RawJson, SubId } from "../../types.ts";
import { stableJson } from "../../util/canonical.ts";
import { vshardOf } from "../../vshard.ts";
import { cdbPolicyDigest } from "../cdb-policy.ts";
import { collectCdbTables } from "../cdb-table-registry.ts";
import { resolveCdbMeta } from "../cdb-table.ts";
import { initializeExternalReshardCapture, withExternalReshardCapture } from "../external-reshard-capture.ts";
import { renderFileReshardTriggers } from "../file-reshard-triggers.ts";
import { refreshRecoverableFile, rehydrateRecoverableFile } from "../file-retention.ts";
import { renderFileAttachmentTriggerSet } from "../file-triggers.ts";
import { sourceChardbEnv, withChardbLoopbacks } from "../loopback.ts";
import { type ChardbManifest, type QueryRouteResponse, emptyManifest, routeValidatedQuery } from "../manifest.ts";
import {
    type VectorResourceV1,
    assertSchemaResourceJournal,
    cdbVectorResourceId,
    chardbResourceDescriptorsAt,
    collectSchemaFileResourceDescriptors,
    collectSchemaResourceDescriptors,
    isChardbVectorResourceDescriptor,
} from "../resource-descriptors.ts";
import { snapshotCdbQueryArgs } from "../result_limits.ts";
import type {
    CdbBindingPlanRequest,
    CdbMutationRequest,
    CdbMutationResponse,
    CdbPlacement,
    CdbQueryRequest,
    CdbQueryResponse,
    CdbRegisteredQueryRequest,
    CdbSubscriptionRequest,
    CdbSubscriptionResponse,
    GatewayInvalidationAck,
    GatewayInvalidationRequest,
    GatewayInvalidationResponse,
    GatewayInvalidationRpc,
    LiveSubscriptionId,
} from "../rpc.ts";
import { type ChardbMigrationJournal, defineMigrations } from "../schema-migrations.ts";
import { assertVectorReshardCaptureForeignKeys, renderVectorReshardTriggers } from "../vector-reshard-triggers.ts";
import type { OrganizationVectorSearchValidation } from "../vector-search-dispatch.ts";
import {
    type CdbAuthInvalidationRequest,
    type CdbAuthInvalidationResult,
    CdbAuthInvalidationStore,
    initializeCdbAuthInvalidationStore,
} from "./cdb-auth-invalidation-store.ts";
import { resolveCdbFileDownload } from "./cdb-file-download.ts";
import {
    CDB_FILE_RESHARD_PAGE_SIZE,
    type CdbFileReshardDrainCursor,
    type CdbFileReshardIdentity,
    type CdbFileReshardParityPage,
    CdbFileReshardStore,
    type CdbReshardFileRecord,
    type CdbReshardOrganizationTombstone,
    initializeCdbFileReshardStore,
} from "./cdb-file-reshard-store.ts";
import {
    type CdbFileDownloadRequest,
    type CdbFileReadyRequest,
    type CdbFileReserveRequest,
    CdbFileRuntime,
    type CdbOrganizationFileDeletionRequest,
    type CdbOrganizationFileDeletionResult,
} from "./cdb-file-runtime.ts";
import { backfillFilePlacements, initializeFileStore, validateFilePlacementsPage } from "./cdb-file-store.ts";
import { CDB_FILE_PENDING_TTL_MS, CdbFileStore, type StoredFile } from "./cdb-file-store.ts";
import {
    assertFreshReshardDestination,
    assertUnusedVersionZeroReshardDestination,
} from "./cdb-fresh-reshard-destination.ts";
import {
    CDB_LIVE_STORE_DDL,
    INVALIDATION_BASE_RETRY_MS,
    INVALIDATION_MAX_RETRY_MS,
    type StoredInvalidationRow,
    type StoredSubscriptionRow,
    acknowledgeInvalidations as acknowledgeStoredInvalidations,
    assertLiveSubscriptionIdentity,
    assertLiveVectorDependencies,
    assertLiveVectorSubscriptionDependency,
    assertSubscriptionTables,
    enqueueRecoveryGenerationInvalidations,
    enqueueSchemaMigrationInvalidations,
    enqueueVectorResourceInvalidations,
    finalizeRetiredLiveSubscription,
    initializeLiveStore,
    parseStoredSubscription,
    parseStoredSubscriptionRouting,
    persistLiveSubscription,
    persistLiveSubscriptionWithVectorDependency,
    promoteLiveSubscriptionRecoveryGeneration,
    dueInvalidations as readDueInvalidations,
    nextInvalidationAlarmAt as readNextInvalidationAlarmAt,
    recordInvalidationFailures,
    retireLegacyLiveSubscriptions,
    retireLiveSubscription,
    sameSubscriptionIdentity,
    subscriptionInvariant,
} from "./cdb-live-store.ts";
import { executeCdbMutation } from "./cdb-mutation-execution.ts";
import { CdbOpLogRetentionStore } from "./cdb-oplog-retention-store.ts";
import { executeCdbSelectPlan } from "./cdb-query-execution.ts";
import { CdbReshardRuntime, type TailTransaction } from "./cdb-reshard-runtime.ts";
import { CdbVectorMutationContext, bindCdbVectorMutationContext } from "./cdb-vector-mutation.ts";
import {
    CDB_VECTOR_DELETED_ORGANIZATION_INSERT_GUARD,
    CDB_VECTOR_DELETED_ORGANIZATION_WRITE_GUARD,
    CdbVectorOrganizationDeletionStore,
    initializeCdbVectorOrganizationDeletionStore,
    installCdbVectorOrganizationDeletionGuards,
    uninstallCdbVectorOrganizationDeletionGuards,
} from "./cdb-vector-organization-deletion-store.ts";
import { CdbVectorOutboxStore, initializeCdbVectorOutboxStore } from "./cdb-vector-outbox-store.ts";
import { scrubCdbVectorRecoveryPage } from "./cdb-vector-recovery-scrub.ts";
import {
    type CdbVectorReshardDestRequest,
    CdbVectorReshardDestStore,
    type CdbVectorReshardParityRequest,
    initializeCdbVectorReshardDestStore,
} from "./cdb-vector-reshard-dest-store.ts";
import {
    type CdbVectorReshardCursor,
    type CdbVectorReshardIdentity,
    CdbVectorReshardSnapshotReader,
    decodeCdbVectorReshardPage,
    encodeCdbVectorReshardPage,
} from "./cdb-vector-reshard-records.ts";
import {
    type CdbVectorReshardSnapshotRequest,
    CdbVectorReshardSnapshotSessionStore,
    initializeCdbVectorReshardSnapshotSessions,
} from "./cdb-vector-reshard-snapshot-session.ts";
import {
    type CdbVectorReshardSourceDeleteCursor,
    CdbVectorReshardSourceDrainStore,
    type CdbVectorReshardSourcePrepareCursor,
} from "./cdb-vector-reshard-source-drain.ts";
import { CdbVectorRuntime } from "./cdb-vector-runtime.ts";
import { executeRegisteredVectorQueryPlan, resolveCdbVectorSearchMatches } from "./cdb-vector-search.ts";
import type {
    CdbValidatedVectorMatch,
    CdbVectorizeMutationIndex,
    CdbVectorizeSearchIndex,
} from "./cdb-vectorize-adapter.ts";
export type { TailTransaction } from "./cdb-reshard-runtime.ts";
import { renderVectorMutationTriggerSet } from "../vector-triggers.ts";
import {
    CDB_RESHARD_IDENTITY_STORE_DDL,
    type CdbReshardSplitIdentity,
    assertCdbReshardRangeIdentity,
    initializeCdbReshardIdentityStore,
} from "./cdb-reshard-identity-store.ts";
import { assertReshardSourceDomainDrained } from "./cdb-reshard-relational.ts";
import {
    CDB_ROUTING_FENCE_STORE_DDL,
    type CdbRoutingFence,
    type CdbRoutingFenceIdentity,
    initializeCdbRoutingFenceStore,
} from "./cdb-routing-fence-store.ts";
import {
    CDB_SCHEMA_MIGRATION_STORE_DDL,
    type CdbFreshSchemaProvisionRequest,
    type CdbSchemaBaselineRequest,
    type CdbSchemaMigrationActivateRequest,
    type CdbSchemaMigrationApplyRequest,
    type CdbSchemaMigrationPrepareRequest,
    CdbSchemaMigrationStore,
    type CdbSchemaState,
} from "./cdb-schema-migration-store.ts";
import {
    CDB_SPLIT_OPLOG_STORE_DDL,
    type SplitOpLogAckResult,
    type SplitOpLogApplyResult,
    type SplitOpLogBatch,
    initializeSplitOpLogAccounting,
} from "./cdb-split-oplog-store.ts";
import { RecoveryAdmissionStore } from "./recovery-admission.ts";
import type { RecoveryAdmissionClock } from "./recovery-coordinator.ts";
import {
    DurableObjectRecovery,
    abortForArmedRecoveryRestore,
    assertRecoveryAvailable,
    assertRecoveryAvailableFor,
    initializeRecoveryStorage,
    readArmedRecoveryRestore,
} from "./recovery.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

export interface CdbEnv {
    readonly CDB_GATEWAY?: DurableObjectNamespace;
    readonly CDB_RESHARD?: DurableObjectNamespace;
    readonly CDB_FILES?: R2Bucket;
}

export type SubscribeArgs = CdbSubscriptionRequest;

const CDB_LOCAL_DDL = `
CREATE TABLE IF NOT EXISTS _chardb_domain_schema (
  table_name TEXT PRIMARY KEY,
  signature TEXT NOT NULL
);
` as const;

function cdbSqlIdentifier(value: string): string {
    return `"${value.replaceAll('"', '""')}"`;
}

const EMPTY_MIGRATION_JOURNAL = defineMigrations([]);

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
    const actual = Object.keys(value).sort();
    const expected = [...keys].sort();
    return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function validateInvalidationResponse(
    value: unknown,
    gatewayId: string,
    requested: ReadonlyMap<string, number>
): readonly GatewayInvalidationAck[] {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw subscriptionInvariant("Gateway invalidation response must be an object");
    }
    const response = value as Record<string, unknown>;
    if (!hasExactKeys(response, ["gatewayId", "acknowledgements"])) {
        throw subscriptionInvariant("Gateway invalidation response has an unexpected shape");
    }
    if (response.gatewayId !== gatewayId || !Array.isArray(response.acknowledgements)) {
        throw subscriptionInvariant("Gateway invalidation response does not match the requested Gateway");
    }
    const seen = new Set<string>();
    const acknowledgements: GatewayInvalidationAck[] = [];
    for (const value of response.acknowledgements) {
        if (typeof value !== "object" || value === null || Array.isArray(value)) {
            throw subscriptionInvariant("Gateway invalidation acknowledgement must be an object");
        }
        const acknowledgement = value as Record<string, unknown>;
        if (!hasExactKeys(acknowledgement, ["registrationId", "changeSeq", "status"])) {
            throw subscriptionInvariant("Gateway invalidation acknowledgement has an unexpected shape");
        }
        if (
            typeof acknowledgement.registrationId !== "string" ||
            !Number.isSafeInteger(acknowledgement.changeSeq) ||
            (acknowledgement.status !== "accepted" && acknowledgement.status !== "stale") ||
            requested.get(acknowledgement.registrationId) !== acknowledgement.changeSeq ||
            seen.has(acknowledgement.registrationId)
        ) {
            throw subscriptionInvariant("Gateway invalidation acknowledgement does not match the request");
        }
        seen.add(acknowledgement.registrationId);
        acknowledgements.push({
            registrationId: acknowledgement.registrationId,
            changeSeq: acknowledgement.changeSeq as number,
            status: acknowledgement.status,
        });
    }
    return acknowledgements;
}

function domainSchemaMismatch(tableName: string): CdbError {
    return new CdbError({
        code: "CDB_PARTITION_CONTRACT_CHANGED",
        message: `domain table "${tableName}" is unsigned or does not match the configured schema`,
        hint: "add and run an explicit shard schema migration before deploying this schema",
    });
}

export interface CdbRuntimeConfig<TSchema extends Record<string, unknown>> {
    readonly schema: () => TSchema;
    readonly manifest: () => ChardbManifest;
    readonly migrations?: () => ChardbMigrationJournal;
}

/**
 * Cdb shard. Provisioned as `class_name = "Cdb"` by Wrangler migrations.
 */
export class Cdb extends DurableObject<CdbEnv> {
    private readonly schemaMigrations: CdbSchemaMigrationStore;
    private readonly files: CdbFileRuntime;
    private readonly opLogRetention: CdbOpLogRetentionStore;
    private readonly recovery: DurableObjectRecovery;
    private readonly resharding: CdbReshardRuntime;
    private readonly vectors: CdbVectorRuntime;

    constructor(state: DurableObjectState, env: CdbEnv) {
        super(state, withChardbLoopbacks(env, state));
        this.schemaMigrations = new CdbSchemaMigrationStore(this.ctx.storage);
        this.resharding = new CdbReshardRuntime({
            storage: this.ctx.storage,
            schemaMigrations: this.schemaMigrations,
            schema: () => this.mutationSchema(),
            journal: () => this.migrationJournal(),
            invalidationNowMs: () => this.invalidationNowMs(),
            scheduleAlarmNoLaterThan: deadline => this.scheduleAlarmNoLaterThan(deadline),
            allowedApplicationTriggerNames: () => [
                ...collectSchemaFileResourceDescriptors(this.mutationSchema()).flatMap(
                    resource => renderFileAttachmentTriggerSet(resource).names
                ),
                ...this.vectorResources().flatMap(resource => renderVectorMutationTriggerSet(resource).names),
                CDB_VECTOR_DELETED_ORGANIZATION_INSERT_GUARD,
                CDB_VECTOR_DELETED_ORGANIZATION_WRITE_GUARD,
            ],
            prepareDestination: sql => {
                if (this.vectorResources().length === 0) return;
                initializeCdbVectorReshardDestStore(sql);
                this.setVectorMutationTriggers(sql, this.vectorResources(), "uninstall");
                uninstallCdbVectorOrganizationDeletionGuards(sql);
            },
            assertDestinationActivation: (sql, args) => {
                if (this.vectorResources().length === 0) return;
                const session = sql.one<{
                    terminal: number;
                    parity_complete: number;
                    parity_through_lsn: number | null;
                    outcome: string;
                }>(
                    `SELECT terminal, parity_complete, parity_through_lsn, outcome
                     FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?`,
                    args.migId
                );
                const split = sql.one<{
                    applied_lsn: number;
                    bulk_done: number;
                    inbox_rows: number;
                    inbox_closed: number;
                }>(
                    `SELECT applied_lsn, bulk_done, inbox_rows, inbox_closed
                     FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'`,
                    args.migId
                );
                if (
                    !session ||
                    session.terminal !== 1 ||
                    session.parity_complete !== 1 ||
                    session.outcome !== "finalized" ||
                    split?.applied_lsn !== session.parity_through_lsn ||
                    split.bulk_done !== 1 ||
                    split.inbox_rows !== 0 ||
                    split.inbox_closed !== 1
                ) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: "destination vector movement is not finalized at its applied tail watermark",
                    });
                }
                this.assertVectorMutationTriggersInstalled(sql);
            },
        });
        this.opLogRetention = new CdbOpLogRetentionStore(this.ctx.storage);
        this.recovery = new DurableObjectRecovery(state.storage, () => adaptSqlStorage(this.ctx.storage.sql));
        this.files = new CdbFileRuntime({
            storage: this.ctx.storage,
            bucket: this.env.CDB_FILES,
            resources: () => collectSchemaFileResourceDescriptors(this.mutationSchema()),
            assertActiveEpoch: epoch => {
                this.assertActiveSchemaEpoch(epoch);
            },
            assertOwnership: organizationId => this.assertFileOwnership(organizationId),
            metadataTransaction: (organizationId, callback) => this.fileMetadataTransaction(organizationId, callback),
        });
        this.vectors = new CdbVectorRuntime({
            storage: this.ctx.storage,
            resources: () => this.vectorResources(),
            resolveIndex: binding => this.resolveVectorIndex(binding),
            assertDeliveryAdmission: (claim, sql, recoveryBookmark) => {
                assertRecoveryAvailableFor(sql, recoveryBookmark);
                const schema = this.schemaMigrations.state(sql);
                if (schema.status !== "active") {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "vector delivery requires an active schema",
                    });
                }
                this.resharding.assertBackgroundDeliveryAdmission(claim.placementVshard, sql);
                if (claim.operation === "upsert") this.assertOrganizationActive(claim.organizationId, sql);
            },
            organizationDeleted: (organizationId, sql) => new CdbFileStore(sql).isOrganizationDeleted(organizationId),
            recordOrganizationUnprovenDeleteTurn: (organizationId, sql) =>
                new CdbVectorOrganizationDeletionStore(sql, callback => callback()).recordUnprovenTurn(organizationId),
            onDeliverySettled: (claim, _outcome, sql) =>
                enqueueVectorResourceInvalidations(sql, claim.resourceId).registrations > 0,
            captureDeliveryTransaction: (sql, placementVshard, callback) =>
                withExternalReshardCapture(sql, placementVshard, callback),
            nowMs: () => this.invalidationNowMs(),
            scheduleAlarmNoLaterThan: deadline => this.scheduleAlarmNoLaterThan(deadline),
        });
        state.blockConcurrencyWhile(() => this.bootstrap());
    }

    protected mutationSchema(): Record<string, unknown> {
        return {};
    }

    protected mutationManifest(): ChardbManifest {
        return emptyManifest();
    }

    protected migrationJournal(): ChardbMigrationJournal {
        return EMPTY_MIGRATION_JOURNAL;
    }

    private assertFileOwnership(organizationId: string, sql = adaptSqlStorage(this.ctx.storage.sql)): void {
        new CdbFileReshardStore(sql).assertOwnership(Number(vshardOf([organizationId])));
    }

    private hasFileResources(): boolean {
        return collectSchemaFileResourceDescriptors(this.mutationSchema()).length > 0;
    }

    /** Permanent organization tombstones protect every external resource, not only R2 files. */
    private hasOrganizationTombstoneResources(): boolean {
        return this.hasFileResources() || this.vectorResources().length > 0;
    }

    private assertOrganizationActive(organizationId: string, sql: SyncSql): void {
        if (
            !sql.one<{ present: number }>(
                "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = '_chardb_deleted_organizations'"
            )
        ) {
            return;
        }
        if (
            sql.one<{ present: number }>(
                "SELECT 1 AS present FROM _chardb_deleted_organizations WHERE organization_id = ?",
                organizationId
            )
        ) {
            throw new CdbError({ code: "CDB_FORBIDDEN", message: "organization was permanently deleted" });
        }
    }

    /** Stage one bounded local cleanup page before any external alarm work. */
    private stageNextVectorOrganizationDeletion(nowMs: number): boolean {
        if (this.vectorResources().length === 0) return false;
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const pending = new CdbVectorOrganizationDeletionStore(sql, callback => callback()).nextPendingPage();
        if (!pending) return false;
        const transaction = <T>(callback: () => T): T =>
            this.ctx.storage.transactionSync(() => {
                this.assertFileOwnership(pending.organizationId, sql);
                return withExternalReshardCapture(sql, Number(vshardOf([pending.organizationId])), callback);
            });
        new CdbVectorOrganizationDeletionStore(sql, transaction).stageNextPage({
            organizationId: pending.organizationId,
            nowMs,
        });
        return true;
    }

    private vectorResources() {
        return collectSchemaResourceDescriptors(this.mutationSchema()).filter(isChardbVectorResourceDescriptor);
    }

    private vectorResourcesAt(journal: ChardbMigrationJournal, version: number): readonly VectorResourceV1[] {
        return chardbResourceDescriptorsAt(journal.migrations, version).filter(isChardbVectorResourceDescriptor);
    }

    private assertVectorMigrationIdentity(
        previous: readonly VectorResourceV1[],
        next: readonly VectorResourceV1[]
    ): void {
        const nextResourceIds = new Set(next.map(cdbVectorResourceId));
        for (const resource of previous) {
            if (nextResourceIds.has(cdbVectorResourceId(resource))) continue;
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `vector resource ${resource.table}.${resource.column} cannot change or disappear during schema migration`,
                hint: "wait for an exact vector head, outbox, attempt-ledger, and remote-document migration protocol",
            });
        }
    }

    private setVectorMutationTriggers(
        sql: SyncSql,
        resources: readonly VectorResourceV1[],
        mode: "install" | "uninstall"
    ): void {
        for (const resource of resources) {
            for (const statement of renderVectorMutationTriggerSet(resource)[mode]) sql.exec(statement);
        }
    }

    private assertVectorJournalTransitions(journal: ChardbMigrationJournal): void {
        let previous: readonly VectorResourceV1[] = [];
        for (let version = 1; version <= journal.version; version++) {
            const next = this.vectorResourcesAt(journal, version);
            this.assertVectorMigrationIdentity(previous, next);
            previous = next;
        }
    }

    private assertVectorDomainIdentity(sql: SyncSql, resources: readonly VectorResourceV1[]): void {
        for (const resource of resources) {
            const table = cdbSqlIdentifier(resource.table);
            const column = cdbSqlIdentifier(resource.column);
            const primaryKey = cdbSqlIdentifier(resource.primaryKey);
            const organizationColumn = cdbSqlIdentifier(resource.organizationColumn);
            const resourceId = cdbVectorResourceId(resource);
            const missingHead = sql.one<{ present: number }>(
                `SELECT 1 AS present
                 FROM ${table} AS domain
                 LEFT JOIN _chardb_vectors AS head
                   ON head.vector_id = CAST(domain.${column} AS TEXT)
                  AND head.organization_id = domain.${organizationColumn}
                  AND head.resource_id = ?
                  AND head.row_pk = CAST(domain.${primaryKey} AS TEXT)
                  AND head.state IN ('pending', 'ready')
                 WHERE domain.${column} IS NOT NULL AND head.vector_id IS NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM _chardb_deleted_organizations AS deleted
                     WHERE deleted.organization_id = domain.${organizationColumn}
                   )
                 LIMIT 1`,
                resourceId
            );
            if (missingHead) {
                throw new CdbError({
                    code: "CDB_PARTITION_CONTRACT_CHANGED",
                    message: `schema migration left ${resource.table}.${resource.column} without its exact vector head`,
                });
            }
            const missingDomain = sql.one<{ present: number }>(
                `SELECT 1 AS present
                 FROM _chardb_vectors AS head
                 LEFT JOIN ${table} AS domain
                   ON CAST(domain.${primaryKey} AS TEXT) = head.row_pk
                  AND domain.${organizationColumn} = head.organization_id
                  AND CAST(domain.${column} AS TEXT) = head.vector_id
                 WHERE head.resource_id = ? AND head.state IN ('pending', 'ready')
                   AND domain.${primaryKey} IS NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM _chardb_deleted_organizations AS deleted
                     WHERE deleted.organization_id = head.organization_id
                   )
                 LIMIT 1`,
                resourceId
            );
            if (missingDomain) {
                throw new CdbError({
                    code: "CDB_PARTITION_CONTRACT_CHANGED",
                    message: `schema migration orphaned an authoritative ${resource.table}.${resource.column} vector head`,
                });
            }
        }
    }

    /** Trusted same-isolate hook used by configured runtimes and native conformance fixtures. */
    protected resolveVectorIndex(binding: string): CdbVectorizeMutationIndex {
        const descriptor = Object.getOwnPropertyDescriptor(sourceChardbEnv(this.env), binding);
        const value = descriptor && "value" in descriptor ? descriptor.value : undefined;
        if (
            typeof value !== "object" ||
            value === null ||
            typeof (value as { upsert?: unknown }).upsert !== "function" ||
            typeof (value as { deleteByIds?: unknown }).deleteByIds !== "function" ||
            typeof (value as { getByIds?: unknown }).getByIds !== "function"
        ) {
            throw new CdbError({
                code: "CDB_SHARD_UNAVAILABLE",
                message: `configured Vectorize binding ${JSON.stringify(binding)} is unavailable`,
            });
        }
        return value as CdbVectorizeMutationIndex;
    }

    /** Trusted same-isolate lookup for the query-only Vectorize surface. */
    protected resolveVectorSearchIndex(binding: string): CdbVectorizeSearchIndex {
        const descriptor = Object.getOwnPropertyDescriptor(sourceChardbEnv(this.env), binding);
        const value = descriptor && "value" in descriptor ? descriptor.value : undefined;
        if (typeof value !== "object" || value === null || typeof (value as { query?: unknown }).query !== "function") {
            throw new CdbError({
                code: "CDB_SHARD_UNAVAILABLE",
                message: `configured Vectorize binding ${JSON.stringify(binding)} is unavailable for search`,
            });
        }
        return value as CdbVectorizeSearchIndex;
    }

    private fileReshardIdentity(
        args: Pick<Omit<CdbReshardSplitIdentity, "role">, "migId" | "rangeLo" | "rangeHi">
    ): CdbFileReshardIdentity {
        return { migId: args.migId, rangeLo: args.rangeLo, rangeHi: args.rangeHi };
    }

    private assertFileReshardSchema(args: Omit<CdbReshardSplitIdentity, "role">, sql: SyncSql): void {
        const state = this.schemaMigrations.state(sql);
        if (
            state.status !== "active" ||
            state.activeVersion !== args.schemaVersion ||
            state.activeEpoch !== args.schemaEpoch ||
            state.activeDigest !== args.schemaDigest
        ) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "file reshard schema identity does not match the active Cdb schema",
            });
        }
    }

    private vectorReshardIdentity(
        args: Pick<Omit<CdbReshardSplitIdentity, "role">, "migId" | "rangeLo" | "rangeHi">
    ): CdbVectorReshardIdentity {
        return { migId: args.migId, rangeLo: args.rangeLo, rangeHi: args.rangeHi };
    }

    /** Require the vector lifecycle to match the generic split identity and phase. */
    private assertVectorReshardSourceIdentity(
        args: Omit<CdbReshardSplitIdentity, "role">,
        sql: SyncSql,
        phase: "capturing" | "frozen" | "drained"
    ): CdbVectorReshardIdentity {
        assertCdbReshardRangeIdentity(args);
        const schema = this.schemaMigrations.state(sql);
        if (
            schema.status !== "active" ||
            schema.activeVersion !== args.schemaVersion ||
            schema.activeEpoch !== args.schemaEpoch ||
            schema.activeDigest !== args.schemaDigest
        ) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "vector reshard schema identity does not match the active Cdb schema",
            });
        }
        if (!Array.isArray(args.tables)) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "vector reshard table list is invalid" });
        }
        const tablesJson = stableJson([...args.tables].sort((left, right) => left.name.localeCompare(right.name)));
        const identity = sql.one<{
            readonly range_lo: number;
            readonly range_hi: number;
            readonly role: string;
            readonly schema_version: number;
            readonly schema_epoch: number;
            readonly schema_digest: string;
            readonly tables_json: string;
        }>(
            `SELECT range_lo, range_hi, role, schema_version, schema_epoch, schema_digest, tables_json
             FROM _chardb_split_identity WHERE mig_id = ?`,
            args.migId
        );
        if (
            !identity ||
            identity.range_lo !== args.rangeLo ||
            identity.range_hi !== args.rangeHi ||
            identity.role !== "source" ||
            identity.schema_version !== args.schemaVersion ||
            identity.schema_epoch !== args.schemaEpoch ||
            identity.schema_digest !== args.schemaDigest ||
            identity.tables_json !== tablesJson
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} does not match its bound vector source identity`,
            });
        }
        const split = sql.one<{
            readonly range_lo: number;
            readonly range_hi: number;
            readonly role: string;
            readonly capture: number;
            readonly drain_started: number;
            readonly drained: number;
            readonly abort_started: number;
        }>(
            `SELECT range_lo, range_hi, role, capture, drain_started, drained, abort_started
             FROM _chardb_split_state WHERE mig_id = ?`,
            args.migId
        );
        const exactSplit =
            split?.range_lo === args.rangeLo &&
            split.range_hi === args.rangeHi &&
            split.role === "source" &&
            split.abort_started === 0;
        const exactPhase =
            phase === "capturing"
                ? split?.capture === 1 && split.drain_started === 0 && split.drained === 0
                : phase === "frozen"
                  ? split?.capture === 0 && split.drain_started === 1 && split.drained === 0
                  : split?.capture === 0 && split.drained === 1;
        if (!exactSplit || !exactPhase) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} is not in its ${phase} vector source phase`,
            });
        }
        return this.vectorReshardIdentity(args);
    }

    private assertVectorReshardDestIdentity(
        args: Omit<CdbReshardSplitIdentity, "role">,
        sql: SyncSql,
        phase: "bulk" | "parity" | "serving" | "abort"
    ): CdbVectorReshardIdentity {
        assertCdbReshardRangeIdentity(args);
        const schema = this.schemaMigrations.state(sql);
        if (
            schema.status !== "active" ||
            schema.activeVersion !== args.schemaVersion ||
            schema.activeEpoch !== args.schemaEpoch ||
            schema.activeDigest !== args.schemaDigest
        ) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "vector reshard schema identity does not match the active destination schema",
            });
        }
        const tablesJson = stableJson([...args.tables].sort((left, right) => left.name.localeCompare(right.name)));
        const identity = sql.one<{
            range_lo: number;
            range_hi: number;
            role: string;
            schema_version: number;
            schema_epoch: number;
            schema_digest: string;
            tables_json: string;
        }>(
            `SELECT range_lo, range_hi, role, schema_version, schema_epoch, schema_digest, tables_json
             FROM _chardb_split_identity WHERE mig_id = ?`,
            args.migId
        );
        const split = sql.one<{
            range_lo: number;
            range_hi: number;
            role: string;
            bulk_done: number;
            destination_serving: number;
            abort_started: number;
            drained: number;
            inbox_rows: number;
            inbox_closed: number;
        }>(
            `SELECT range_lo, range_hi, role, bulk_done, destination_serving, abort_started, drained,
                    inbox_rows, inbox_closed
             FROM _chardb_split_state WHERE mig_id = ?`,
            args.migId
        );
        const exactIdentity =
            identity?.range_lo === args.rangeLo &&
            identity.range_hi === args.rangeHi &&
            identity.role === "dest" &&
            identity.schema_version === args.schemaVersion &&
            identity.schema_epoch === args.schemaEpoch &&
            identity.schema_digest === args.schemaDigest &&
            identity.tables_json === tablesJson;
        const exactSplit = split?.range_lo === args.rangeLo && split.range_hi === args.rangeHi && split.role === "dest";
        const exactPhase =
            phase === "bulk"
                ? split?.bulk_done === 0 &&
                  split.destination_serving === 0 &&
                  split.abort_started === 0 &&
                  split.drained === 0
                : phase === "parity"
                  ? split?.bulk_done === 1 &&
                    split.destination_serving === 0 &&
                    split.abort_started === 0 &&
                    split.drained === 0 &&
                    split.inbox_rows === 0 &&
                    split.inbox_closed === 1
                  : phase === "serving"
                    ? split?.destination_serving === 1 && split.abort_started === 0
                    : split?.destination_serving === 0 && split.abort_started === 1 && split.drained === 1;
        if (!exactIdentity || !exactSplit || !exactPhase) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} is not in its ${phase} vector destination phase`,
            });
        }
        return this.vectorReshardIdentity(args);
    }

    private uninstallVectorReshardTriggers(sql: SyncSql, migId: string): number {
        const triggers = renderVectorReshardTriggers(migId);
        let removed = 0;
        for (const [index, statement] of triggers.uninstall.entries()) {
            const name = triggers.names[index];
            if (!name) throw new CdbError({ code: "CDB_INVARIANT", message: "vector trigger set is incomplete" });
            if (sql.one("SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?", name)) removed++;
            sql.exec(statement);
        }
        return removed;
    }

    private assertActiveReshardCaptureWireCompatible(
        sql: SyncSql,
        hasOrganizationTombstoneResources: boolean,
        hasVectorResources: boolean
    ): void {
        if (hasOrganizationTombstoneResources) {
            const fileSources = sql.all<{ mig_id: string }>(
                `SELECT cursor.mig_id FROM _chardb_split_file_cursor AS cursor
                 INNER JOIN _chardb_split_state AS state
                   ON state.mig_id = cursor.mig_id AND state.role = 'source'
                 WHERE cursor.role = 'source' AND cursor.outcome = 'active' AND state.capture = 1
                 ORDER BY cursor.mig_id`
            );
            for (const source of fileSources) {
                const triggers = renderFileReshardTriggers(source.mig_id);
                const tombstoneTriggers = triggers.names.filter(name => name.includes("_org_"));
                const rows = tombstoneTriggers.map(name =>
                    sql.one<{ sql: string }>("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?", name)
                );
                if (rows.some(row => !row?.sql.includes("vector_unproven_turns"))) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `active file source ${source.mig_id} uses an incompatible tombstone capture wire; finish or abort it under the prior code before deploying this version`,
                    });
                }
            }
        }
        if (hasVectorResources) {
            const vectorSources = sql.all<{ mig_id: string }>(
                `SELECT session.mig_id
                 FROM _chardb_vector_snapshot_sessions AS session
                 INNER JOIN _chardb_split_state AS state
                   ON state.mig_id = session.mig_id AND state.role = 'source'
                 WHERE session.cleaned = 0 AND state.capture = 1
                 ORDER BY session.mig_id`
            );
            for (const source of vectorSources) {
                assertVectorReshardCaptureForeignKeys(sql);
                const triggers = renderVectorReshardTriggers(source.mig_id);
                const outboxTriggers = triggers.names.filter(name => name.includes("_outbox_"));
                const rows = outboxTriggers.map(name =>
                    sql.one<{ sql: string }>("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?", name)
                );
                if (rows.some(row => !row?.sql.includes("terminal_failure"))) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `active vector source ${source.mig_id} uses an incompatible outbox capture wire; finish or abort it under the prior code before deploying this version`,
                    });
                }
            }
        }
    }

    private assertCachedVectorReshardWireCompatible(sql: SyncSql): void {
        const pages = [
            ...sql.all<{ mig_id: string; encoded: string }>(
                `SELECT mig_id, cached_page_enc AS encoded FROM _chardb_vector_snapshot_sessions
                 WHERE cleaned = 0 AND cached_page_enc IS NOT NULL`
            ),
            ...sql.all<{ mig_id: string; encoded: string }>(
                `SELECT mig_id, last_page_enc AS encoded FROM _chardb_vector_reshard_dest_sessions
                 WHERE last_page_enc IS NOT NULL`
            ),
        ];
        for (const page of pages) {
            try {
                const decoded = decodeCdbVectorReshardPage(page.encoded);
                if (encodeCdbVectorReshardPage(decoded) !== page.encoded) throw new Error("noncanonical page");
            } catch {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migration ${page.mig_id} retains an incompatible vector snapshot wire; finish or abort it under the prior code before deploying this version`,
                });
            }
        }
    }

    reshardSideStateProtocolCapabilitiesV2(): {
        readonly vectorSnapshot: "v2";
        readonly fileTombstones: "v2";
    } {
        return Object.freeze({ vectorSnapshot: "v2" as const, fileTombstones: "v2" as const });
    }

    private setFileAttachmentTriggers(sql: SyncSql, mode: "install" | "uninstall"): number {
        let count = 0;
        for (const resource of collectSchemaFileResourceDescriptors(this.mutationSchema())) {
            const triggers = renderFileAttachmentTriggerSet(resource);
            for (const statement of triggers[mode]) {
                sql.exec(statement);
                count++;
            }
        }
        return count;
    }

    private assertVectorMutationTriggersInstalled(sql: SyncSql): void {
        for (const resource of this.vectorResources()) {
            for (const name of renderVectorMutationTriggerSet(resource).names) {
                if (!sql.one("SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?", name)) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `destination vector mutation trigger ${name} is not installed`,
                    });
                }
            }
        }
    }

    private assertVectorDerivedState(sql: SyncSql): void {
        const capacity = sql.one<{
            singleton: number;
            reconciled: number;
            head_count: number;
            stored_bytes: number;
            outbox_rows: number;
            attempt_rows: number;
        }>("SELECT * FROM _chardb_vector_capacity WHERE singleton = 1");
        const actual = sql.one<{
            head_count: number;
            stored_bytes: number;
            outbox_rows: number;
            attempt_rows: number;
        }>(
            `SELECT (SELECT COUNT(*) FROM _chardb_vectors) AS head_count,
                    (SELECT COALESCE(SUM(COALESCE(length(values_enc), 0) + length(metadata_json)), 0)
                     FROM _chardb_vectors) AS stored_bytes,
                    (SELECT COUNT(*) FROM _chardb_vector_outbox) AS outbox_rows,
                    (SELECT COUNT(*) FROM _chardb_vector_attempts) AS attempt_rows`
        );
        const scheduler = sql.one<{ singleton: number; next_vshard: number }>(
            "SELECT singleton, next_vshard FROM _chardb_vector_scheduler WHERE singleton = 1"
        );
        const sequence = sql.one<{ singleton: number; last_seq: number; max_seq: number }>(
            `SELECT sequence.singleton, sequence.last_seq,
                    COALESCE((SELECT MAX(created_seq) FROM _chardb_vectors), 0) AS max_seq
             FROM _chardb_vector_head_sequence AS sequence WHERE sequence.singleton = 1`
        );
        if (
            !capacity ||
            capacity.singleton !== 1 ||
            capacity.reconciled !== 1 ||
            !actual ||
            capacity.head_count !== actual.head_count ||
            capacity.stored_bytes !== actual.stored_bytes ||
            capacity.outbox_rows !== actual.outbox_rows ||
            capacity.attempt_rows !== actual.attempt_rows ||
            !scheduler ||
            scheduler.singleton !== 1 ||
            !Number.isSafeInteger(scheduler.next_vshard) ||
            scheduler.next_vshard < 0 ||
            scheduler.next_vshard >= 16_384 ||
            !sequence ||
            sequence.singleton !== 1 ||
            !Number.isSafeInteger(sequence.last_seq) ||
            !Number.isSafeInteger(sequence.max_seq) ||
            sequence.last_seq < sequence.max_seq
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: "vector derived state does not match its physical rows",
            });
        }
    }

    private fileSnapshotThroughLsn(sql: SyncSql, migId: string): number {
        return this.resharding.sourceTailHighWatermark(migId, sql);
    }

    private fileMetadataTransaction<T>(organizationId: string, callback: (store: CdbFileStore) => T): T {
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertFileOwnership(organizationId, sql);
            return withExternalReshardCapture(sql, Number(vshardOf([organizationId])), () =>
                callback(new CdbFileStore(sql))
            );
        });
    }

    private async bootstrap(): Promise<void> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const journal = this.migrationJournal();
        if (journal.version > 0) assertSchemaResourceJournal(this.mutationSchema(), journal.migrations);
        this.assertVectorJournalTransitions(journal);
        for (const stmt of `${SHARD_BOOTSTRAP_DDL}\n${CDB_SPLIT_OPLOG_STORE_DDL}\n${CDB_LIVE_STORE_DDL}\n${CDB_LOCAL_DDL}\n${CDB_SCHEMA_MIGRATION_STORE_DDL}\n${CDB_ROUTING_FENCE_STORE_DDL}\n${CDB_RESHARD_IDENTITY_STORE_DDL}`
            .split(";")
            .map(s => s.trim())
            .filter(Boolean)) {
            sql.exec(stmt);
        }
        initializeOpLogPlacement(sql);
        this.ctx.storage.transactionSync(() => initializeSplitLogAccounting(adaptSqlStorage(this.ctx.storage.sql)));
        this.ctx.storage.transactionSync(() => initializeSplitOpLogAccounting(adaptSqlStorage(this.ctx.storage.sql)));
        initializeLiveStore(sql);
        initializeCdbAuthInvalidationStore(sql);
        initializeRecoveryStorage(sql);
        await this.reconcileRecoveryAdmission();
        initializeCdbReshardIdentityStore(sql);
        initializeCdbRoutingFenceStore(sql);
        const hasFileResources = this.hasFileResources();
        const hasVectorResources = this.vectorResources().length > 0;
        if (hasFileResources || hasVectorResources) {
            initializeFileStore(sql);
            initializeCdbFileReshardStore(sql);
        }
        if (hasFileResources || hasVectorResources) initializeExternalReshardCapture(sql);
        if (hasVectorResources) {
            initializeCdbVectorOutboxStore(sql);
            initializeCdbVectorOrganizationDeletionStore(sql);
            initializeCdbVectorReshardSnapshotSessions(sql);
            initializeCdbVectorReshardDestStore(sql);
            this.assertCachedVectorReshardWireCompatible(sql);
        }
        sql.exec("PRAGMA foreign_keys = ON");
        this.assertActiveReshardCaptureWireCompatible(sql, hasFileResources || hasVectorResources, hasVectorResources);
        this.ctx.storage.transactionSync(() => retireLegacyLiveSubscriptions(adaptSqlStorage(this.ctx.storage.sql)));
        const migrationState = this.schemaMigrations.initialize(journal);
        if (migrationState.ensureDomainTables) this.ensureDomainTables();
        const activeVectorResources =
            journal.version === 0
                ? this.vectorResources()
                : this.vectorResourcesAt(journal, this.schemaMigrations.state(sql).activeVersion);
        const destinationSessions = hasVectorResources
            ? sql.all<{
                  mig_id: string;
                  outcome: string | null;
                  role: string;
                  destination_serving: number;
                  abort_started: number;
                  drained: number;
              }>(
                  `SELECT identity.mig_id, session.outcome, identity.role,
                          state.destination_serving, state.abort_started, state.drained
                   FROM _chardb_split_identity AS identity
                   JOIN _chardb_split_state AS state
                     ON state.mig_id = identity.mig_id AND state.role = identity.role
                   LEFT JOIN _chardb_vector_reshard_dest_sessions AS session
                     ON session.mig_id = identity.mig_id
                   WHERE identity.role = 'dest'
                   ORDER BY identity.mig_id`
              )
            : [];
        for (const session of destinationSessions) {
            const serving = session.destination_serving === 1;
            const aborting = session.abort_started === 1;
            const drained = session.drained === 1;
            const valid =
                session.role === "dest" &&
                ((session.outcome === null &&
                    ((!serving && !aborting && !drained) ||
                        (!serving && aborting) ||
                        (serving && !aborting && drained))) ||
                    (session.outcome === "active" && !serving && ((!aborting && !drained) || aborting)) ||
                    (session.outcome === "aborting" && !serving && aborting && drained) ||
                    (session.outcome === "finalized" &&
                        ((!aborting && ((!serving && !drained) || serving)) || (!serving && aborting && drained))) ||
                    (session.outcome === "aborted" && !serving && aborting && drained) ||
                    (session.outcome === "cleaned" && serving && !aborting && drained));
            if (!valid) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `vector destination ${session.mig_id} has an impossible trigger lifecycle state`,
                });
            }
        }
        const stagedDestination = destinationSessions.some(
            session =>
                session.destination_serving === 0 &&
                (session.outcome === "active" ||
                    session.outcome === "aborting" ||
                    (session.outcome === "finalized" && session.abort_started === 1))
        );
        this.setVectorMutationTriggers(sql, activeVectorResources, stagedDestination ? "uninstall" : "install");
        if (hasVectorResources) {
            if (stagedDestination) uninstallCdbVectorOrganizationDeletionGuards(sql);
            else installCdbVectorOrganizationDeletionGuards(sql);
        }
        assertLiveVectorDependencies(sql, activeVectorResources.map(cdbVectorResourceId));
        const cursor = this.ctx.storage.sql.exec<StoredSubscriptionRow>(
            `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                    principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                    ref, args_json, policy_digest, query_hash,
                    tables_json, intervals_json
             FROM _chardb_live_subscriptions
             WHERE state = 'active'
             ORDER BY gateway_id, registration_id`
        );
        for (const row of cursor) {
            let request: CdbSubscriptionRequest;
            try {
                request = parseStoredSubscription(row);
            } catch (error) {
                if (!(error instanceof CdbError) || error.code !== "CDB_INVALID_ARGS") throw error;
                const routing = parseStoredSubscriptionRouting(row);
                assertSubscriptionTables(sql, routing.subscription, [...new Set(routing.tables)].sort());
                this.validateIntervals(routing);
                continue;
            }
            assertSubscriptionTables(sql, request.subscription, [...new Set(request.tables)].sort());
            this.validateIntervals(request);
        }
        const invalidationAlarmAt = readNextInvalidationAlarmAt(sql);
        if (invalidationAlarmAt !== null) await this.scheduleAlarmNoLaterThan(invalidationAlarmAt);
        const retention = this.opLogRetention.maintain(this.invalidationNowMs());
        if (retention.nextAt !== null) await this.scheduleAlarmNoLaterThan(retention.nextAt);
        if (hasVectorResources && !stagedDestination) {
            const pendingOrganizationDeletion = new CdbVectorOrganizationDeletionStore(sql, callback =>
                callback()
            ).nextPendingPage();
            if (pendingOrganizationDeletion) {
                await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
            }
            const nextVectorAt = new CdbVectorOutboxStore(sql).nextDueAt();
            if (nextVectorAt !== null) {
                await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextVectorAt));
            }
        }
    }

    private ensureDomainTables(): void {
        const tables = this.renderDomainTables();
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            for (const ddl of tables) {
                const existing = sql.one<{ sql: string }>(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                    ddl.tableName
                );
                const recorded = sql.one<{ signature: string }>(
                    "SELECT signature FROM _chardb_domain_schema WHERE table_name = ?",
                    ddl.tableName
                );
                if (existing) {
                    this.assertRenderedDomainTable(sql, ddl, existing.sql, recorded?.signature ?? null, true);
                    continue;
                }
                if (recorded) throw domainSchemaMismatch(ddl.tableName);
                sql.exec(ddl.createTable);
                for (const statement of ddl.indexes) sql.exec(statement);
                sql.exec(
                    "INSERT INTO _chardb_domain_schema (table_name, signature) VALUES (?, ?)",
                    ddl.tableName,
                    ddl.signature
                );
            }
        });
    }

    private renderDomainTables() {
        const schema = this.mutationSchema();
        const tables = [...collectCdbTables(schema)].sort((a, b) => a.meta.name.localeCompare(b.meta.name));
        const domainTableNames = new Set(tables.map(entry => entry.meta.name));
        const authTableNames = synthesizedAuthTableNames(schema);
        return tables.map(({ table }) => {
            const meta = resolveCdbMeta(table);
            const authorityColumns = new Set([meta.tenantBy, meta.selfBy].filter(column => column !== undefined));
            return renderSqliteTableDdl(table, {
                errorCode: "CDB_PARTITION_CONTRACT_CHANGED",
                label: "domain DDL",
                hint: "add and run an explicit shard schema migration before deploying this schema",
                includeForeignKey: reference => {
                    if (domainTableNames.has(reference.foreignTableName)) return true;
                    if (
                        authTableNames.has(reference.foreignTableName) ||
                        reference.columns.some(column => authorityColumns.has(column))
                    )
                        return false;
                    throw new CdbError({
                        code: "CDB_NONLOCAL_FK",
                        message: `domain table "${meta.name}" references non-cdbTable "${reference.foreignTableName}"`,
                        hint: "make the referenced domain table a cdbTable or use a tenant/self authority column",
                    });
                },
            });
        });
    }

    private assertRenderedDomainTable(
        sql: SyncSql,
        ddl: ReturnType<typeof renderSqliteTableDdl>,
        actualCreateTable: string,
        recordedSignature: string | null,
        requireRecordedSignature: boolean
    ): void {
        if (
            (requireRecordedSignature && recordedSignature !== ddl.signature) ||
            actualCreateTable !== ddl.createTable
        ) {
            throw domainSchemaMismatch(ddl.tableName);
        }
        for (let index = 0; index < ddl.indexNames.length; index++) {
            const indexName = ddl.indexNames[index];
            const expectedSql = ddl.indexes[index];
            if (!indexName || !expectedSql) throw domainSchemaMismatch(ddl.tableName);
            const present = sql.one<{ sql: string }>(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ? AND tbl_name = ?",
                indexName,
                ddl.tableName
            );
            if (present?.sql !== expectedSql) throw domainSchemaMismatch(ddl.tableName);
        }
    }

    private recordMigratedDomainSchema(sql: SyncSql): void {
        const rendered = this.renderDomainTables();
        const expectedNames = new Set(rendered.map(ddl => ddl.tableName));
        const physical = sql.all<{ name: string }>(
            `SELECT name FROM sqlite_master
             WHERE type = 'table'
               AND substr(name, 1, 8) != '_chardb_'
               AND substr(name, 1, 4) != '_cf_'
               AND substr(name, 1, 12) != '__miniflare_'
               AND substr(name, 1, 7) != 'sqlite_'
             ORDER BY name`
        );
        for (const row of physical) {
            if (!expectedNames.has(row.name)) throw domainSchemaMismatch(row.name);
        }
        for (const ddl of rendered) {
            const existing = sql.one<{ sql: string }>(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                ddl.tableName
            );
            if (!existing) throw domainSchemaMismatch(ddl.tableName);
            this.assertRenderedDomainTable(sql, ddl, existing.sql, null, false);
            sql.exec(
                `INSERT INTO _chardb_domain_schema (table_name, signature) VALUES (?, ?)
                 ON CONFLICT(table_name) DO UPDATE SET signature = excluded.signature`,
                ddl.tableName,
                ddl.signature
            );
        }
        const recorded = sql.all<{ table_name: string }>(
            "SELECT table_name FROM _chardb_domain_schema ORDER BY table_name"
        );
        for (const row of recorded) {
            if (expectedNames.has(row.table_name)) continue;
            const existing = sql.one<{ present: number }>(
                "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ?",
                row.table_name
            );
            if (existing) throw domainSchemaMismatch(row.table_name);
            sql.exec("DELETE FROM _chardb_domain_schema WHERE table_name = ?", row.table_name);
        }
    }

    private assertActiveSchemaEpoch(
        expectedEpoch: number,
        sql = adaptSqlStorage(this.ctx.storage.sql)
    ): CdbSchemaState {
        assertRecoveryAvailable(sql);
        return this.schemaMigrations.assertActiveEpoch(expectedEpoch, () => this.migrationJournal(), sql);
    }

    private assertRecoveryGeneration(expectedGeneration: number, sql = adaptSqlStorage(this.ctx.storage.sql)): void {
        new RecoveryAdmissionStore(sql).assertRequest(expectedGeneration);
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

    private assertRoutingEpoch(schemaEpoch: number, placement: CdbPlacement): void {
        this.resharding.assertRoutingAdmission(schemaEpoch, Number(vshardOf([placement.partitionKey])));
    }

    schemaState(): CdbSchemaState {
        return this.schemaMigrations.state();
    }

    baselineSchemaMigration(args: CdbSchemaBaselineRequest): CdbSchemaState {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertNoActiveReshard();
        const journal = this.migrationJournal();
        const resources = this.vectorResourcesAt(journal, args.targetVersion);
        return this.schemaMigrations.baseline(
            args,
            journal,
            sql => this.recordMigratedDomainSchema(sql),
            sql => {
                this.setVectorMutationTriggers(sql, resources, "install");
                this.assertVectorDomainIdentity(sql, resources);
            }
        );
    }

    /** Provision a never-used destination at the exact schema held by a topology lease. */
    provisionFreshReshardDestination(args: CdbFreshSchemaProvisionRequest): CdbSchemaState {
        this.ctx.storage.transactionSync(() => {
            new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).reconcile({
                generation: args.recoveryGeneration,
                activeOperationId: null,
                activeDigest: null,
            });
        });
        this.assertRecoveryGeneration(args.recoveryGeneration);
        const splitMigId = args.migrationId.startsWith("reshard-dest:")
            ? args.migrationId.slice("reshard-dest:".length)
            : null;
        if (splitMigId) this.resharding.assertFreshDestinationProvisioningAllowed(splitMigId);
        const journal = this.migrationJournal();
        const current = this.schemaMigrations.state();
        if (
            current.status === "active" &&
            current.activeVersion === args.targetVersion &&
            current.activeEpoch === args.targetEpoch &&
            current.activeDigest === args.targetDigest &&
            current.lastMigrationId === args.migrationId
        ) {
            return current;
        }
        return this.schemaMigrations.provisionFresh(
            args,
            journal,
            sql => {
                if (args.targetVersion === 0) {
                    assertUnusedVersionZeroReshardDestination(sql, this.renderDomainTables());
                } else {
                    assertFreshReshardDestination(sql);
                }
            },
            sql => this.recordMigratedDomainSchema(sql),
            sql => {
                const resources = this.vectorResourcesAt(journal, args.targetVersion);
                this.setVectorMutationTriggers(sql, resources, "install");
                this.assertVectorDomainIdentity(sql, resources);
            }
        );
    }

    prepareSchemaMigration(args: CdbSchemaMigrationPrepareRequest): CdbSchemaState {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertNoActiveReshard();
        const journal = this.migrationJournal();
        return this.schemaMigrations.prepare(args, journal);
    }

    applySchemaMigration(args: CdbSchemaMigrationApplyRequest): CdbSchemaState {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertNoActiveReshard();
        const journal = this.migrationJournal();
        const previousResources = this.vectorResourcesAt(journal, args.version - 1);
        const nextResources = this.vectorResourcesAt(journal, args.version);
        this.assertVectorMigrationIdentity(previousResources, nextResources);
        return this.schemaMigrations.apply(args, journal, {
            beforeStatements: sql => this.setVectorMutationTriggers(sql, previousResources, "uninstall"),
            afterStatements: sql => {
                this.setVectorMutationTriggers(sql, nextResources, "install");
                this.assertVectorDomainIdentity(sql, nextResources);
            },
        });
    }

    async activateSchemaMigration(args: CdbSchemaMigrationActivateRequest): Promise<CdbSchemaState> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertNoActiveReshard();
        const state = this.schemaMigrations.activate(
            args,
            () => this.migrationJournal(),
            sql => {
                this.recordMigratedDomainSchema(sql);
                enqueueSchemaMigrationInvalidations(sql);
            }
        );
        const nextAttemptAt = readNextInvalidationAlarmAt(adaptSqlStorage(this.ctx.storage.sql));
        if (nextAttemptAt !== null) {
            await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextAttemptAt));
        }
        return state;
    }

    /** Prepare one immutable source-side routing range fence. */
    prepareRoutingFence(args: CdbRoutingFenceIdentity): CdbRoutingFence {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.prepareRoutingFence(args);
    }

    /** Stop this source and wake every live registration stranded in the moved range. */
    async activateRoutingFence(args: CdbRoutingFenceIdentity): Promise<CdbRoutingFence> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.activateRoutingFence(args);
    }

    /** Project one Catalog auth epoch into the existing live-query invalidation outbox. */
    async invalidateAuthScope(args: CdbAuthInvalidationRequest): Promise<CdbAuthInvalidationResult> {
        const result = this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(args.recoveryGeneration, sql);
            return new CdbAuthInvalidationStore(sql).apply(args, this.invalidationNowMs());
        });
        const nextAttemptAt = readNextInvalidationAlarmAt(adaptSqlStorage(this.ctx.storage.sql));
        if (nextAttemptAt !== null) {
            await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextAttemptAt));
        }
        return result;
    }

    /** Mark source data cleanup complete without reopening the fenced range. */
    completeRoutingFenceCleanup(args: CdbRoutingFenceIdentity): CdbRoutingFence {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.completeRoutingFenceCleanup(args);
    }

    cancelRoutingFenceBeforeCutover(args: CdbRoutingFenceIdentity): CdbRoutingFence | null {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.cancelRoutingFenceBeforeCutover(args);
    }

    /** Inspect the activated source fence that covers one vshard. */
    activeRoutingFence(vshard: number): CdbRoutingFence | null {
        return this.resharding.activeRoutingFence(vshard);
    }

    private validateIntervals(args: { readonly intervals: CdbSubscriptionRequest["intervals"] }): void {
        for (const block of args.intervals) intervalSetFromWire(block.intervals);
    }

    protected invalidationNowMs(): number {
        return Date.now();
    }

    protected invalidationRetryDelayMs(attempts: number): number {
        return Math.min(INVALIDATION_MAX_RETRY_MS, INVALIDATION_BASE_RETRY_MS * 2 ** Math.max(0, attempts - 1));
    }

    private recordInvalidationFailure(rows: readonly StoredInvalidationRow[], nowMs: number, error: unknown): void {
        this.ctx.storage.transactionSync(() => {
            recordInvalidationFailures(
                adaptSqlStorage(this.ctx.storage.sql),
                rows,
                nowMs,
                attempts => this.invalidationRetryDelayMs(attempts),
                error
            );
        });
    }

    private acknowledgeInvalidations(
        gatewayId: string,
        rows: readonly StoredInvalidationRow[],
        acknowledgements: readonly GatewayInvalidationAck[],
        nowMs: number
    ): void {
        const acknowledged = new Set(acknowledgements.map(ack => JSON.stringify([ack.registrationId, ack.changeSeq])));
        this.ctx.storage.transactionSync(() => {
            acknowledgeStoredInvalidations(adaptSqlStorage(this.ctx.storage.sql), gatewayId, acknowledgements);
        });
        const omitted = rows.filter(row => !acknowledged.has(JSON.stringify([row.registration_id, row.change_seq])));
        if (omitted.length > 0) {
            this.recordInvalidationFailure(omitted, nowMs, "Gateway omitted an invalidation acknowledgement");
        }
    }

    private async drainInvalidations(nowMs: number): Promise<void> {
        const rows = readDueInvalidations(adaptSqlStorage(this.ctx.storage.sql), nowMs);
        const groups = new Map<string, StoredInvalidationRow[]>();
        for (const row of rows) {
            const group = groups.get(row.gateway_id) ?? [];
            group.push(row);
            groups.set(row.gateway_id, group);
        }

        const outcomes = await Promise.all(
            [...groups].map(async ([gatewayId, group]) => {
                try {
                    if (!this.env.CDB_GATEWAY) throw new Error("CDB_GATEWAY binding is unavailable");
                    const id = this.env.CDB_GATEWAY.idFromString(gatewayId);
                    const gateway = this.env.CDB_GATEWAY.get(id) as unknown as GatewayInvalidationRpc;
                    const request: GatewayInvalidationRequest = {
                        sourceCdbId: this.ctx.id.toString(),
                        gatewayId,
                        invalidations: group.map(row => ({
                            subscription: {
                                gatewayId: row.gateway_id,
                                registrationId: row.registration_id,
                                connectionId: row.connection_id,
                                clientId: ClientId(row.client_id),
                                subId: SubId(row.sub_id),
                            },
                            changeSeq: row.change_seq,
                        })),
                    };
                    const rawResponse: GatewayInvalidationResponse = await gateway.invalidateSubscriptions(request);
                    const requested = new Map(group.map(row => [row.registration_id, row.change_seq] as const));
                    return {
                        ok: true as const,
                        gatewayId,
                        group,
                        acknowledgements: validateInvalidationResponse(rawResponse, gatewayId, requested),
                    };
                } catch (error) {
                    return { ok: false as const, group, error };
                }
            })
        );

        // Settle durable outcomes in the same stable Gateway order returned by
        // the outbox query. Only the network waits run concurrently.
        for (const outcome of outcomes) {
            if (outcome.ok) {
                this.acknowledgeInvalidations(outcome.gatewayId, outcome.group, outcome.acknowledgements, nowMs);
            } else {
                this.recordInvalidationFailure(outcome.group, nowMs, outcome.error);
            }
        }
    }

    private async maintainInvalidationDelivery(): Promise<void> {
        const nowMs = this.invalidationNowMs();
        try {
            await this.drainInvalidations(nowMs);
        } catch (deliveryError) {
            try {
                await this.scheduleAlarmNoLaterThan(nowMs + INVALIDATION_BASE_RETRY_MS);
                return;
            } catch (alarmError) {
                throw new CdbError({
                    code: "CDB_SHARD_UNAVAILABLE",
                    message: "invalidation delivery and alarm scheduling failed after mutation commit",
                    cause: { deliveryError, alarmError },
                });
            }
        }
        const nextAttemptAt = readNextInvalidationAlarmAt(adaptSqlStorage(this.ctx.storage.sql));
        if (nextAttemptAt === null) return;
        try {
            await this.scheduleAlarmNoLaterThan(Math.max(nowMs + 1, nextAttemptAt));
        } catch (error) {
            throw new CdbError({
                code: "CDB_SHARD_UNAVAILABLE",
                message: "invalidation alarm scheduling failed after mutation commit",
                cause: error,
            });
        }
    }

    private async scheduleAlarmNoLaterThan(deadline: number): Promise<void> {
        const storage = this.ctx.storage as DurableObjectStorage & { getAlarm?: () => Promise<number | null> };
        if (typeof storage.getAlarm !== "function") {
            await storage.setAlarm(deadline);
            return;
        }
        const current = await storage.getAlarm();
        if (current === null || deadline < current) await storage.setAlarm(deadline);
    }

    private async maintainAlarmWork(options: { readonly deliverVectors: boolean }): Promise<void> {
        let failure: unknown;
        try {
            if (this.stageNextVectorOrganizationDeletion(this.invalidationNowMs())) {
                await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
            }
        } catch (error) {
            failure ??= error;
        }
        try {
            await this.maintainInvalidationDelivery();
        } catch (error) {
            failure ??= error;
        }
        try {
            await this.files.maintain(this.invalidationNowMs(), deadline => this.scheduleAlarmNoLaterThan(deadline));
        } catch (error) {
            failure ??= error;
        }
        try {
            const retention = this.opLogRetention.maintain(this.invalidationNowMs());
            if (retention.nextAt !== null) await this.scheduleAlarmNoLaterThan(retention.nextAt);
        } catch (error) {
            failure ??= error;
        }
        if (options.deliverVectors) {
            try {
                if (this.vectorResources().length > 0) await this.vectors.maintain();
            } catch (error) {
                failure ??= error;
            }
        }
        if (failure) throw failure;
    }

    /** Private same-Worker RPC used by the organization file upload dispatcher. */
    async reserveFile(input: CdbFileReserveRequest & { readonly schemaEpoch: number }): Promise<StoredFile> {
        try {
            this.assertRecoveryGeneration(input.recoveryGeneration);
            this.resharding.assertRoutingAdmission(input.schemaEpoch, Number(vshardOf([input.organizationId])));
            const reserved = this.files.reserve(input);
            await this.scheduleAlarmNoLaterThan(input.nowMs + CDB_FILE_PENDING_TTL_MS);
            return reserved;
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Private same-Worker RPC that revalidates untrusted Vectorize candidates against authoritative SQLite. */
    async resolveOrganizationVectorSearch(
        input: OrganizationVectorSearchValidation
    ): Promise<readonly CdbValidatedVectorMatch[]> {
        try {
            const vshard = Number(vshardOf([input.organizationId]));
            this.assertRecoveryGeneration(input.route.recoveryGeneration);
            this.assertActiveSchemaEpoch(input.route.domainSchemaEpoch);
            this.resharding.assertRoutingAdmission(input.route.schemaEpoch, vshard);
            this.assertOrganizationActive(input.organizationId, adaptSqlStorage(this.ctx.storage.sql));
            const matches = await resolveCdbVectorSearchMatches({
                storage: this.ctx.storage,
                schema: this.mutationSchema(),
                auth: input.auth,
                organizationId: input.organizationId,
                resource: input.resource,
                matches: input.matches,
                limit: input.limit,
            });
            this.assertRecoveryGeneration(input.route.recoveryGeneration);
            this.assertActiveSchemaEpoch(input.route.domainSchemaEpoch);
            this.resharding.assertRoutingAdmission(input.route.schemaEpoch, vshard);
            this.assertOrganizationActive(input.organizationId, adaptSqlStorage(this.ctx.storage.sql));
            return matches.map(match => ({
                vectorId: match.vectorId,
                rowPk: match.rowPk,
                score: match.score,
                // Stored metadata intentionally has a null prototype. Workers RPC accepts only ordinary JSON objects.
                metadata: JSON.parse(JSON.stringify(match.metadata)) as Readonly<Record<string, unknown>>,
            }));
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Private same-Worker RPC after R2 has accepted and hashed the immutable object. */
    markFileReady(input: CdbFileReadyRequest & { readonly schemaEpoch: number }): StoredFile {
        try {
            this.assertRecoveryGeneration(input.recoveryGeneration);
            this.resharding.assertRoutingAdmission(input.schemaEpoch, Number(vshardOf([input.organizationId])));
            return this.files.markReady(input);
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Private same-Worker RPC that applies row and column policy before exposing object metadata. */
    async resolveFileDownload(
        input: CdbFileDownloadRequest & { readonly schemaEpoch: number }
    ): Promise<StoredFile | null> {
        try {
            this.assertRecoveryGeneration(input.recoveryGeneration);
            this.resharding.assertRoutingAdmission(input.schemaEpoch, Number(vshardOf([input.organizationId])));
            const file = await resolveCdbFileDownload({
                storage: this.ctx.storage,
                schema: this.mutationSchema(),
                files: this.files,
                request: input,
            });
            assertRecoveryAvailable(adaptSqlStorage(this.ctx.storage.sql));
            return file;
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Private Catalog outbox target. The permanent shard fence wins every late upload or attachment race. */
    async deleteOrganizationFiles(
        input: CdbOrganizationFileDeletionRequest
    ): Promise<CdbOrganizationFileDeletionResult> {
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(input.recoveryGeneration, sql);
            this.assertActiveSchemaEpoch(input.domainSchemaEpoch, sql);
            this.assertFileOwnership(input.organizationId, sql);
            withExternalReshardCapture(sql, Number(vshardOf([input.organizationId])), () => {
                new CdbFileStore(sql).fenceOrganizationDeletion(input.organizationId, input.nowMs);
                if (this.vectorResources().length === 0) return;
                const vectors = new CdbVectorOrganizationDeletionStore(sql, callback => callback());
                vectors.acceptOrganization({ organizationId: input.organizationId, nowMs: input.nowMs });
            });
        });
        await this.scheduleAlarmNoLaterThan(input.nowMs + 1);
        return Object.freeze({ organizationId: input.organizationId, accepted: true });
    }

    /** Private same-Worker operational status for external vector purge completion or manual intervention. */
    vectorOrganizationPurgeStatus(input: {
        readonly organizationId: string;
        readonly schemaEpoch: number;
        readonly recoveryGeneration: number;
        readonly domainSchemaEpoch: number;
    }) {
        try {
            const vshard = Number(vshardOf([input.organizationId]));
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(input.recoveryGeneration, sql);
            assertRecoveryAvailable(sql);
            this.assertActiveSchemaEpoch(input.domainSchemaEpoch, sql);
            this.resharding.assertRoutingAdmission(input.schemaEpoch, vshard, sql);
            this.assertFileOwnership(input.organizationId, sql);
            const status = new CdbVectorOrganizationDeletionStore(sql, callback => callback()).readPurgeStatus(
                input.organizationId
            );
            this.assertActiveSchemaEpoch(input.domainSchemaEpoch, sql);
            this.resharding.assertRoutingAdmission(input.schemaEpoch, vshard, sql);
            this.assertFileOwnership(input.organizationId, sql);
            return status;
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    private async executeRegisteredQueryPlan(input: {
        readonly routed: Extract<QueryRouteResponse, { readonly ok: true }>;
        readonly placement: CdbPlacement;
        readonly auth: CdbQueryRequest["auth"];
        readonly subject: string;
        readonly ref: ChardbRef;
    }): Promise<RawJson> {
        if (input.routed.vectorPlan) {
            if (input.routed.selectPlan || input.placement.authority !== "organization") {
                throw subscriptionInvariant("registered vector query has conflicting plan metadata");
            }
            return (await executeRegisteredVectorQueryPlan({
                index: this.resolveVectorSearchIndex(input.routed.vectorPlan.resource.binding),
                storage: this.ctx.storage,
                schema: this.mutationSchema(),
                auth: input.auth,
                plan: input.routed.vectorPlan,
            })) as unknown as RawJson;
        }
        if (!input.routed.selectPlan) {
            throw subscriptionInvariant("planned query omitted its canonical executable plan");
        }
        return executeCdbSelectPlan({
            storage: this.ctx.storage,
            schema: this.mutationSchema(),
            plan: input.routed.selectPlan,
            placement: input.placement,
            auth: input.auth,
            subject: input.subject,
            ref: input.ref,
            intent: input.routed.intent,
        });
    }

    /**
     * Register a live-query subscription on this shard. Caller is the Gateway DO.
     */
    async subscribe(input: SubscribeArgs): Promise<CdbSubscriptionResponse> {
        assertLiveSubscriptionIdentity(input.subscription);
        const args = { ...input, args: snapshotCdbQueryArgs(input.args) };
        this.assertActiveSchemaEpoch(args.domainSchemaEpoch);
        if (Number(vshardOf([args.organizationId])) !== args.vshard) {
            throw subscriptionInvariant("live subscription vshard does not match its partition");
        }
        let routed: Extract<QueryRouteResponse, { readonly ok: true }> | undefined;
        if (args.placement) {
            routed = routeValidatedQuery(this.mutationManifest(), { ref: args.ref, args: args.args }, tables =>
                cdbPolicyDigest(this.mutationSchema(), tables)
            );
            if (
                routed.authority !== args.placement.authority ||
                routed.partitionKey !== args.placement.partitionKey ||
                args.organizationId !== args.placement.partitionKey
            ) {
                throw subscriptionInvariant("live subscription placement does not match its server manifest route");
            }
        }
        this.validateIntervals(args);
        const policyDigest = cdbPolicyDigest(this.mutationSchema(), args.tables);
        if (routed && (routed.queryHash !== args.queryHash || routed.policyDigest !== policyDigest)) {
            return {
                ok: false,
                registrationState: "absent",
                subscription: args.subscription,
                error: subscriptionInvariant(
                    "live subscription plan or policy does not match its server manifest route"
                ).toJSON(),
            };
        }
        const vectorResourceId = routed?.vectorPlan ? cdbVectorResourceId(routed.vectorPlan.resource) : null;
        let response: CdbSubscriptionResponse | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(args.recoveryGeneration, sql);
            this.assertActiveSchemaEpoch(args.domainSchemaEpoch, sql);
            const existing = sql.one<{ present: number }>(
                `SELECT 1 AS present FROM _chardb_live_subscriptions
                 WHERE gateway_id = ? AND registration_id = ?`,
                args.subscription.gatewayId,
                args.subscription.registrationId
            );
            try {
                this.resharding.assertRoutingAdmission(args.schemaEpoch, args.vshard, sql);
                this.assertOrganizationActive(args.organizationId, sql);
            } catch (error) {
                if (isCdbError(error) && error.code === "CDB_STALE_EPOCH" && !existing) {
                    response = {
                        ok: false,
                        registrationState: "absent",
                        subscription: args.subscription,
                        error: error.toJSON(),
                    };
                    return;
                }
                throw error;
            }
            response = vectorResourceId
                ? persistLiveSubscriptionWithVectorDependency(sql, args, policyDigest, vectorResourceId)
                : persistLiveSubscription(sql, args, policyDigest);
        });
        if (!response) throw subscriptionInvariant("subscription completed without a durable outcome");
        return response;
    }

    async unsubscribe(request: {
        readonly subscription: LiveSubscriptionId;
        readonly recoveryGeneration: number;
    }): Promise<void> {
        const { subscription, recoveryGeneration } = request;
        assertLiveSubscriptionIdentity(subscription);
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(recoveryGeneration, sql);
            retireLiveSubscription(sql, subscription);
        });
    }

    /** Delete one exact tombstone after Gateway has finished every pending install and cleanup retry for it. */
    async finalizeUnsubscribe(request: {
        readonly subscription: LiveSubscriptionId;
        readonly recoveryGeneration: number;
    }): Promise<void> {
        const { subscription, recoveryGeneration } = request;
        assertLiveSubscriptionIdentity(subscription);
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(recoveryGeneration, sql);
            finalizeRetiredLiveSubscription(sql, subscription);
        });
    }

    /** Resolve and run a registered mutation entirely inside this shard isolate. */
    async mutate(input: CdbMutationRequest): Promise<CdbMutationResponse> {
        let response: CdbMutationResponse;
        try {
            let vectorContext: CdbVectorMutationContext | undefined;
            response = await executeCdbMutation({
                storage: this.ctx.storage,
                cdbId: () => this.ctx.id.toString(),
                schema: () => this.mutationSchema(),
                manifest: () => this.mutationManifest(),
                request: input,
                invalidationNowMs: () => this.invalidationNowMs(),
                assertActiveSchemaEpoch: (expectedEpoch, sql) => {
                    this.assertActiveSchemaEpoch(expectedEpoch, sql);
                    this.assertRecoveryGeneration(input.recoveryGeneration, sql);
                },
                assertRoutingFence: (schemaEpoch, placement, sql) => {
                    if (!placement) {
                        this.resharding.assertUnplacedRoutingAdmission(sql);
                        return;
                    }
                    this.resharding.assertRoutingAdmission(
                        schemaEpoch,
                        Number(vshardOf([placement.partitionKey])),
                        sql
                    );
                    if (placement.authority === "organization") {
                        this.assertOrganizationActive(placement.partitionKey, sql);
                    }
                },
                extendContext: (ctx, sql, isTransactionActive, placement) => {
                    vectorContext = new CdbVectorMutationContext({
                        sql,
                        schema: this.mutationSchema(),
                        auth: input.auth,
                        placement,
                        nowMs: this.invalidationNowMs(),
                        isTransactionActive,
                        assertOrganizationActive: organizationId => this.assertOrganizationActive(organizationId, sql),
                    });
                    return bindCdbVectorMutationContext(ctx, vectorContext);
                },
                captureSplitOutcome: ({ sql, principalId, mutId, placement }) => {
                    vectorContext?.assertDomainHeads();
                    if (!placement) return;
                    this.resharding.captureSplitOutcome({
                        sql,
                        principalId,
                        mutId,
                        vshard: Number(vshardOf([placement.partitionKey])),
                    });
                },
            });
        } catch (error) {
            return { ok: false, error: cdbRuntimeError(error).toJSON() };
        }
        try {
            // Vector outbox delivery can wait on an external Vectorize call. The
            // transaction already armed the durable alarm, so keep that network
            // boundary out of the committed mutation response path.
            await this.maintainAlarmWork({ deliverVectors: false });
        } catch {
            // The pre-armed alarm owns recovery. The mutation is committed and
            // its result must remain stable across an op-log replay.
        }
        return response;
    }

    override async alarm(): Promise<void> {
        if (abortForArmedRecoveryRestore(this.ctx, adaptSqlStorage(this.ctx.storage.sql))) return;
        if (new RecoveryAdmissionStore(adaptSqlStorage(this.ctx.storage.sql)).blocksBackgroundWork()) return;
        await this.maintainAlarmWork({ deliverVectors: true });
    }

    async adminRecoveryBookmark(args: { readonly atMs?: number }): Promise<{
        readonly bookmark: string;
        readonly atMs: number;
    }> {
        try {
            return await this.recovery.bookmark(args.atMs);
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminArmRecoveryRestore(args: {
        readonly bookmark: string;
        readonly armedAt: number;
        readonly operationId: string;
        readonly generation: number;
    }) {
        try {
            return await this.recovery.arm(args.bookmark, args.armedAt, {
                operationId: args.operationId,
                generation: args.generation,
            });
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminReleaseRecovery(args: { readonly operationId: string; readonly generation: number }) {
        try {
            const released = this.recovery.release(args.operationId, args.generation, sql => {
                enqueueRecoveryGenerationInvalidations(sql, args.generation);
            });
            const nextAttemptAt = readNextInvalidationAlarmAt(adaptSqlStorage(this.ctx.storage.sql));
            if (nextAttemptAt !== null) {
                await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextAttemptAt));
            }
            return released;
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminCancelRecoveryRestore(args: { readonly bookmark: string }) {
        try {
            return await this.recovery.cancel(args.bookmark);
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminCommitRecoveryRestore(args: { readonly bookmark: string }) {
        try {
            return await this.recovery.commit(args.bookmark);
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminRecoveryRestoreStatus(args: { readonly bookmark: string }) {
        try {
            return this.recovery.status(args.bookmark);
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminScrubRecoveryVectors(args: {
        readonly bookmark: string;
        readonly afterVectorId: string;
        readonly afterPhysicalVersion: number;
        readonly limit: number;
    }) {
        try {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const armed = readArmedRecoveryRestore(sql);
            if (!armed || armed.targetBookmark !== args.bookmark) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "vector recovery scrub does not match the armed restore",
                });
            }
            const schema = this.schemaMigrations.state(sql);
            if (schema.status !== "active") {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "vector recovery scrub requires an active schema",
                });
            }
            return await scrubCdbVectorRecoveryPage({
                sql,
                resources: this.vectorResources(),
                resolveIndex: binding => this.resolveVectorIndex(binding),
                cursor: {
                    afterVectorId: args.afterVectorId,
                    afterPhysicalVersion: args.afterPhysicalVersion,
                },
                limit: args.limit,
            });
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminRequeueRecoveryVectors(args: {
        readonly afterCreatedSeq: number;
        readonly limit: number;
        readonly nowMs: number;
        readonly bookmark?: string;
    }) {
        try {
            let result: ReturnType<CdbVectorOutboxStore["requeueRecoveryPage"]> | undefined;
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                assertRecoveryAvailableFor(sql, args.bookmark);
                const schema = this.schemaMigrations.state(sql);
                if (schema.status !== "active") {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "vector recovery requires an active schema",
                    });
                }
                if (this.vectorResources().length === 0) {
                    result = Object.freeze({
                        processed: 0,
                        afterCreatedSeq: args.afterCreatedSeq,
                        done: true,
                    });
                    return;
                }
                result = new CdbVectorOutboxStore(sql).requeueRecoveryPage(args);
            });
            if (!result)
                throw new CdbError({ code: "CDB_INVARIANT", message: "vector recovery page was not recorded" });
            if (result.processed > 0) await this.scheduleAlarmNoLaterThan(args.nowMs + 1);
            return result;
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminSettleRecoveryVectors(args: { readonly bookmark: string }) {
        try {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            assertRecoveryAvailableFor(sql, args.bookmark);
            if (this.vectorResources().length === 0) return { pending: 0, terminal: 0 };
            await this.vectors.maintain({ recoveryBookmark: args.bookmark });
            return this.recoveryVectorDeliveryState(sql, "settlement");
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Drain only the vector work that existed before this exact restore fence. */
    async adminQuiesceRecoveryVectors(args: { readonly bookmark: string }) {
        try {
            const before = adaptSqlStorage(this.ctx.storage.sql);
            assertRecoveryAvailableFor(before, args.bookmark);
            if (this.vectorResources().length === 0) return { pending: 0, terminal: 0 };
            await this.vectors.maintain({ recoveryBookmark: args.bookmark });
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            assertRecoveryAvailableFor(sql, args.bookmark);
            return this.recoveryVectorDeliveryState(sql, "quiescence");
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    private recoveryVectorDeliveryState(sql: SyncSql, phase: "quiescence" | "settlement") {
        const row = sql.one<{ readonly pending: number | bigint; readonly terminal: number | bigint }>(
            `SELECT COUNT(*) AS pending,
                    COALESCE(SUM(CASE WHEN terminal_failure = 1 THEN 1 ELSE 0 END), 0) AS terminal
             FROM _chardb_vector_outbox`
        );
        const pending = Number(row?.pending ?? 0);
        const terminal = Number(row?.terminal ?? 0);
        if (
            !Number.isSafeInteger(pending) ||
            pending < 0 ||
            pending > 65_536 ||
            !Number.isSafeInteger(terminal) ||
            terminal < 0 ||
            terminal > pending
        ) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `vector recovery ${phase} is invalid` });
        }
        return { pending, terminal };
    }

    async adminRetainRecoveryFiles(args: {
        readonly bookmark: string;
        readonly afterFileId: string;
        readonly limit: number;
    }) {
        try {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const armed = readArmedRecoveryRestore(sql);
            if (!armed || armed.targetBookmark !== args.bookmark) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "file recovery retention does not match the armed restore",
                });
            }
            const schema = this.schemaMigrations.state(sql);
            if (schema.status !== "active") {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "file recovery retention requires an active schema",
                });
            }
            if (!this.hasFileResources()) {
                return Object.freeze({ processed: 0, afterFileId: args.afterFileId, done: true });
            }
            if (!this.env.CDB_FILES) {
                throw new CdbError({
                    code: "CDB_SHARD_UNAVAILABLE",
                    message: "file recovery retention requires CDB_FILES",
                });
            }
            const bucket = this.env.CDB_FILES;
            const page = new CdbFileStore(sql).retentionPage(args.afterFileId, args.limit);
            for (let index = 0; index < page.files.length; index += 4) {
                await Promise.all(page.files.slice(index, index + 4).map(file => refreshRecoverableFile(bucket, file)));
            }
            return Object.freeze({
                processed: page.files.length,
                afterFileId: page.afterFileId,
                done: page.done,
            });
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    async adminRehydrateRecoveryFiles(args: {
        readonly afterFileId: string;
        readonly limit: number;
        readonly bookmark?: string;
    }) {
        try {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            assertRecoveryAvailableFor(sql, args.bookmark);
            const schema = this.schemaMigrations.state(sql);
            if (schema.status !== "active") {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "file recovery requires an active schema",
                });
            }
            if (!this.hasFileResources()) {
                return Object.freeze({ processed: 0, afterFileId: args.afterFileId, done: true });
            }
            if (!this.env.CDB_FILES) {
                throw new CdbError({ code: "CDB_SHARD_UNAVAILABLE", message: "file recovery requires CDB_FILES" });
            }
            const bucket = this.env.CDB_FILES;
            const page = new CdbFileStore(sql).recoveryPage(args.afterFileId, args.limit);
            for (let index = 0; index < page.files.length; index += 4) {
                await Promise.all(
                    page.files.slice(index, index + 4).map(file => rehydrateRecoverableFile(bucket, file))
                );
            }
            return Object.freeze({
                processed: page.files.length,
                afterFileId: page.afterFileId,
                done: page.done,
            });
        } catch (error) {
            throwCdbRpcError(error);
        }
    }

    /** Execute a registered shard-local query without exposing it through Gateway yet. */
    async query(input: CdbQueryRequest): Promise<CdbQueryResponse> {
        try {
            const request = { ...input, args: snapshotCdbQueryArgs(input.args) };
            this.assertRecoveryGeneration(request.recoveryGeneration);
            this.assertActiveSchemaEpoch(request.domainSchemaEpoch);
            if (request.placement) this.assertRoutingEpoch(request.schemaEpoch, request.placement);
            else this.resharding.assertUnplacedRoutingAdmission();
            if (request.placement?.authority === "organization") {
                this.assertOrganizationActive(request.placement.partitionKey, adaptSqlStorage(this.ctx.storage.sql));
            }
            const routed = routeValidatedQuery(
                this.mutationManifest(),
                { ref: request.ref, args: request.args },
                tables => cdbPolicyDigest(this.mutationSchema(), tables)
            );
            if (request.placement) {
                if (
                    routed.authority !== request.placement.authority ||
                    routed.partitionKey !== request.placement.partitionKey
                ) {
                    throw subscriptionInvariant("query placement does not match its server manifest route");
                }
            }
            if (!request.placement) {
                throw subscriptionInvariant("planned query omitted its placement");
            }
            const result = await this.executeRegisteredQueryPlan({
                routed,
                placement: request.placement,
                auth: request.auth,
                subject: "query result",
                ref: request.ref,
            });
            this.assertRecoveryGeneration(request.recoveryGeneration);
            this.assertRoutingEpoch(request.schemaEpoch, request.placement);
            if (request.placement.authority === "organization") {
                this.assertOrganizationActive(request.placement.partitionKey, adaptSqlStorage(this.ctx.storage.sql));
            }
            this.assertActiveSchemaEpoch(request.domainSchemaEpoch);
            return { ok: true, result };
        } catch (error) {
            return { ok: false, error: cdbRuntimeError(error).toJSON() };
        }
    }

    /** Execute one native binding plan after revalidation against this shard's packaged schema. */
    async executePlan(input: CdbBindingPlanRequest): Promise<CdbQueryResponse> {
        try {
            this.assertRecoveryGeneration(input.recoveryGeneration);
            this.assertActiveSchemaEpoch(input.domainSchemaEpoch);
            this.assertRoutingEpoch(input.schemaEpoch, input.placement);
            if (input.placement.authority === "organization") {
                this.assertOrganizationActive(input.placement.partitionKey, adaptSqlStorage(this.ctx.storage.sql));
            }
            const result = await executeCdbSelectPlan({
                storage: this.ctx.storage,
                schema: this.mutationSchema(),
                plan: input.plan,
                placement: input.placement,
                auth: input.auth,
                subject: "DB select plan result",
            });
            this.assertRecoveryGeneration(input.recoveryGeneration);
            this.assertRoutingEpoch(input.schemaEpoch, input.placement);
            if (input.placement.authority === "organization") {
                this.assertOrganizationActive(input.placement.partitionKey, adaptSqlStorage(this.ctx.storage.sql));
            }
            this.assertActiveSchemaEpoch(input.domainSchemaEpoch);
            return { ok: true, result };
        } catch (error) {
            return { ok: false, error: cdbRuntimeError(error).toJSON() };
        }
    }

    /** Execute the query persisted for one active live registration. */
    async queryRegistered(request: CdbRegisteredQueryRequest): Promise<CdbQueryResponse> {
        try {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertRecoveryGeneration(request.recoveryGeneration, sql);
            this.assertActiveSchemaEpoch(request.domainSchemaEpoch, sql);
            const row = sql.one<StoredSubscriptionRow>(
                `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                        principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                        ref, args_json, policy_digest, query_hash,
                        tables_json, intervals_json
                 FROM _chardb_live_subscriptions
                 WHERE gateway_id = ? AND registration_id = ?`,
                request.subscription.gatewayId,
                request.subscription.registrationId
            );
            if (!row) throw subscriptionInvariant("registered query subscription does not exist");
            if (!sameSubscriptionIdentity(row, request.subscription)) {
                throw subscriptionInvariant("registered query identity does not match its subscription");
            }
            if (row.state !== "active") throw subscriptionInvariant("registered query subscription is retired");
            if (row.principal_id !== request.auth.userId) {
                throw subscriptionInvariant("registered query principal does not match fresh authorization");
            }
            const subscription = parseStoredSubscription(row);
            const requestedRecoveryGeneration = request.recoveryGeneration;
            if (subscription.recoveryGeneration > requestedRecoveryGeneration) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "registered query belongs to a newer recovery generation",
                });
            }
            if (Number(vshardOf([subscription.organizationId])) !== request.vshard) {
                throw subscriptionInvariant("registered query vshard does not match its persisted partition");
            }
            if (subscription.domainSchemaEpoch !== request.domainSchemaEpoch) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "registered query belongs to an older domain schema epoch",
                });
            }
            if (subscription.schemaEpoch !== request.schemaEpoch || subscription.vshard !== request.vshard) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "registered query belongs to an older physical routing generation",
                    hint: "resolve the active vshard placement from Catalog and reinstall the subscription",
                });
            }
            this.resharding.assertRoutingAdmission(request.schemaEpoch, request.vshard, sql);
            assertSubscriptionTables(sql, subscription.subscription, [...new Set(subscription.tables)].sort());
            this.validateIntervals(subscription);

            const routed = routeValidatedQuery(
                this.mutationManifest(),
                { ref: subscription.ref, args: subscription.args },
                tables => cdbPolicyDigest(this.mutationSchema(), tables)
            );
            if (routed.authority !== "organization" && routed.authority !== "user" && routed.authority !== "global") {
                throw subscriptionInvariant("registered query no longer has declared authority");
            }
            const placement = request.placement;
            if (row.authority !== null && row.authority !== routed.authority) {
                throw subscriptionInvariant("registered query authority changed after registration");
            }
            if (row.authority === "global" && !placement) {
                throw subscriptionInvariant("registered global query omitted fresh placement");
            }
            if (placement && placement.authority !== routed.authority) {
                throw subscriptionInvariant("registered query authority changed after registration");
            }
            const reroutedPartition = placement?.partitionKey ?? subscription.organizationId;
            if (reroutedPartition !== row.organization_id) {
                throw subscriptionInvariant("registered query partition does not match fresh authorization");
            }
            if (routed.authority === "organization" && reroutedPartition !== request.auth.tenantId) {
                throw subscriptionInvariant("registered query organization does not match fresh authorization");
            }
            if (routed.authority === "organization") this.assertOrganizationActive(reroutedPartition, sql);
            if (routed.authority === "user" && reroutedPartition !== request.auth.userId) {
                throw subscriptionInvariant("registered query user does not match fresh authorization");
            }
            if (routed.partitionKey !== subscription.organizationId) {
                throw subscriptionInvariant("registered query partition changed after registration");
            }
            if (routed.policyDigest !== row.policy_digest) {
                throw subscriptionInvariant("registered query policy changed after registration");
            }
            if (routed.queryHash !== subscription.queryHash) {
                throw subscriptionInvariant("registered query intent changed after registration");
            }
            const vectorResourceId = routed.vectorPlan ? cdbVectorResourceId(routed.vectorPlan.resource) : null;
            assertLiveVectorSubscriptionDependency(sql, subscription.subscription, vectorResourceId);

            const result = await this.executeRegisteredQueryPlan({
                routed,
                placement: { authority: routed.authority, partitionKey: reroutedPartition },
                auth: request.auth,
                subject: "registered query result",
                ref: subscription.ref,
            });
            if (!Array.isArray(result)) {
                throw subscriptionInvariant("registered query result must be an array");
            }

            const current = sql.one<StoredSubscriptionRow>(
                `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                        principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                        ref, args_json, policy_digest, query_hash,
                        tables_json, intervals_json
                 FROM _chardb_live_subscriptions
                 WHERE gateway_id = ? AND registration_id = ?`,
                request.subscription.gatewayId,
                request.subscription.registrationId
            );
            if (
                !current ||
                current.state !== "active" ||
                !sameSubscriptionIdentity(current, request.subscription) ||
                current.principal_id !== row.principal_id ||
                current.organization_id !== row.organization_id ||
                current.authority !== row.authority ||
                current.payload_hash !== row.payload_hash ||
                current.policy_digest !== row.policy_digest ||
                current.query_hash !== row.query_hash ||
                current.schema_epoch !== row.schema_epoch ||
                current.recovery_generation !== row.recovery_generation ||
                current.vshard !== row.vshard ||
                current.domain_schema_epoch !== row.domain_schema_epoch
            ) {
                throw subscriptionInvariant("registered query changed while its handler was running");
            }
            parseStoredSubscription(current);
            assertSubscriptionTables(sql, request.subscription, [...new Set(subscription.tables)].sort());
            assertLiveVectorSubscriptionDependency(sql, request.subscription, vectorResourceId);
            this.assertRecoveryGeneration(request.recoveryGeneration, sql);
            this.assertActiveSchemaEpoch(request.domainSchemaEpoch, sql);
            this.resharding.assertRoutingAdmission(request.schemaEpoch, request.vshard, sql);
            if (routed.authority === "organization") this.assertOrganizationActive(reroutedPartition, sql);
            promoteLiveSubscriptionRecoveryGeneration(sql, current, requestedRecoveryGeneration);
            return { ok: true, result };
        } catch (error) {
            return { ok: false, error: cdbRuntimeError(error).toJSON() };
        }
    }

    /**
     * Source-side begin: records the migration in `_chardb_split_state` and
     * installs `AFTER INSERT/UPDATE/DELETE` triggers on each migrating table
     * that project changes into `_chardb_split_log`. The destination later
     * replays those rows in LSN order, filtering by `vshardOf(partition_key)`
     * so peer migrations on the same source don't cross-pollute. Exact
     * re-entry replaces every deterministic trigger inside this transaction,
     * upgrading active captures without a write gap.
     */
    prepareReshardFileSource(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly afterKind: "file" | "organization_tombstone";
            readonly afterId: string;
            readonly limit: number;
        }
    ): {
        enabled: boolean;
        backfill: { files: number; tombstones: number; done: boolean };
        cursor: { kind: "file" | "organization_tombstone"; afterId: string; done: boolean };
    } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (!this.hasOrganizationTombstoneResources()) {
            return {
                enabled: false,
                backfill: { files: 0, tombstones: 0, done: true },
                cursor: { kind: args.afterKind, afterId: args.afterId, done: true },
            };
        }
        if (args.limit !== CDB_FILE_RESHARD_PAGE_SIZE) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "file reshard preparation limit must be 500" });
        }
        let result:
            | {
                  enabled: true;
                  backfill: { files: number; tombstones: number; done: boolean };
                  cursor: { kind: "file" | "organization_tombstone"; afterId: string; done: boolean };
              }
            | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertFileReshardSchema(args, sql);
            const backfill = backfillFilePlacements(sql, args.limit);
            const cursor = backfill.done
                ? validateFilePlacementsPage(sql, {
                      afterKind: args.afterKind,
                      afterId: args.afterId,
                      limit: args.limit,
                  })
                : { kind: args.afterKind, afterId: args.afterId, done: false };
            result = { enabled: true, backfill, cursor };
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "file reshard preparation failed" });
        return result;
    }

    beginReshardFileSource(args: Omit<CdbReshardSplitIdentity, "role">): {
        enabled: boolean;
        triggersInstalled: number;
    } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (!this.hasOrganizationTombstoneResources()) return { enabled: false, triggersInstalled: 0 };
        let triggersInstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({
                migId: args.migId,
                role: "source",
                range: { lo: args.rangeLo, hi: args.rangeHi },
                tables: args.tables,
                sql,
            });
            const missing = sql.one<{ present: number }>(
                `SELECT 1 AS present FROM _chardb_files WHERE placement_vshard IS NULL
                 UNION ALL
                 SELECT 1 AS present FROM _chardb_deleted_organizations WHERE placement_vshard IS NULL LIMIT 1`
            );
            if (missing) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "file placement backfill must finish before source capture starts",
                });
            }
            new CdbFileReshardStore(sql).beginSource(this.fileReshardIdentity(args), Date.now());
            const triggers = renderFileReshardTriggers(args.migId);
            for (const statement of triggers.uninstall) sql.exec(statement);
            for (const statement of triggers.install) {
                sql.exec(statement);
                triggersInstalled++;
            }
        });
        return { enabled: true, triggersInstalled };
    }

    beginReshardFileDest(args: Omit<CdbReshardSplitIdentity, "role">): {
        enabled: boolean;
        triggersUninstalled: number;
    } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (!this.hasOrganizationTombstoneResources()) return { enabled: false, triggersUninstalled: 0 };
        let triggersUninstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.assertFileReshardSchema(args, sql);
            this.resharding.assertFreshDestinationProvisioningAllowed(args.migId);
            new CdbFileReshardStore(sql).beginDest(this.fileReshardIdentity(args), Date.now());
            triggersUninstalled = this.setFileAttachmentTriggers(sql, "uninstall");
        });
        return { enabled: true, triggersUninstalled };
    }

    readReshardFileSnapshot(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly afterPlacement: number;
            readonly afterFileId: string;
            readonly limit: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "source", sql });
            const page = new CdbFileReshardStore(sql).readSnapshot({
                ...this.fileReshardIdentity(args),
                afterPlacement: args.afterPlacement,
                afterFileId: args.afterFileId,
                limit: args.limit,
            });
            return { ...page, throughLsn: this.fileSnapshotThroughLsn(sql, args.migId) };
        });
    }

    applyReshardFileSnapshot(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly rows: readonly CdbReshardFileRecord[];
            readonly throughLsn: number;
        }
    ): { applied: number; inserted: number } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let result: { applied: number; inserted: number } | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "dest", sql });
            result = new CdbFileReshardStore(sql).applySnapshot(
                this.fileReshardIdentity(args),
                args.rows,
                args.throughLsn
            );
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "file snapshot apply failed" });
        return result;
    }

    readReshardFileTombstones(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly afterPlacement: number;
            readonly afterOrganizationId: string;
            readonly limit: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        const page = this.readReshardFileTombstonesV2(args);
        if (page.rows.some(row => row.vectorUnprovenTurns !== 0)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: "legacy file tombstone snapshot cannot carry vector purge progress",
            });
        }
        return {
            ...page,
            rows: page.rows.map(({ vectorUnprovenTurns: _omitted, ...row }) => row),
        };
    }

    readReshardFileTombstonesV2(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly afterPlacement: number;
            readonly afterOrganizationId: string;
            readonly limit: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "source", sql });
            const page = new CdbFileReshardStore(sql).readTombstones({
                ...this.fileReshardIdentity(args),
                afterPlacement: args.afterPlacement,
                afterOrganizationId: args.afterOrganizationId,
                limit: args.limit,
            });
            return { ...page, throughLsn: this.fileSnapshotThroughLsn(sql, args.migId) };
        });
    }

    applyReshardFileTombstones(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly rows: readonly CdbReshardOrganizationTombstone[];
            readonly throughLsn: number;
        }
    ): { applied: number; inserted: number } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        const rows = args.rows.map(row => {
            const vectorUnprovenTurns = (row as Partial<CdbReshardOrganizationTombstone>).vectorUnprovenTurns;
            if (vectorUnprovenTurns !== undefined && vectorUnprovenTurns !== 0) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "legacy file tombstone apply cannot carry vector purge progress",
                });
            }
            return { ...row, vectorUnprovenTurns: 0 };
        });
        return this.applyReshardFileTombstonesV2({ ...args, rows });
    }

    applyReshardFileTombstonesV2(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly rows: readonly CdbReshardOrganizationTombstone[];
            readonly throughLsn: number;
        }
    ): { applied: number; inserted: number } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let result: { applied: number; inserted: number } | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "dest", sql });
            result = new CdbFileReshardStore(sql).applyTombstones(
                this.fileReshardIdentity(args),
                args.rows,
                args.throughLsn
            );
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "file tombstone apply failed" });
        return result;
    }

    fenceReshardFileSource(args: Omit<CdbReshardSplitIdentity, "role">): { fenced: boolean } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "source", sql });
            new CdbFileReshardStore(sql).fenceSource(this.fileReshardIdentity(args), Date.now());
        });
        return { fenced: true };
    }

    validateReshardFiles(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly cursor: CdbFileReshardDrainCursor;
            readonly limit: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertReshardMovement({ migId: args.migId, role: "dest" });
        return new CdbFileReshardStore(adaptSqlStorage(this.ctx.storage.sql)).validate(
            this.fileReshardIdentity(args),
            args.cursor,
            args.limit
        );
    }

    reshardFileAppliedProvenance(args: Omit<CdbReshardSplitIdentity, "role">): { rows: number; legacyRows: number } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertReshardMovement({ migId: args.migId, role: "dest" });
        return new CdbFileReshardStore(adaptSqlStorage(this.ctx.storage.sql)).appliedProvenance(
            this.fileReshardIdentity(args)
        );
    }

    readReshardFileParityPage(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly role: "source" | "dest";
            readonly cursor: CdbFileReshardDrainCursor;
            readonly limit: number;
        }
    ): CdbFileReshardParityPage {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        this.resharding.assertReshardMovement({ migId: args.migId, role: args.role });
        return new CdbFileReshardStore(adaptSqlStorage(this.ctx.storage.sql)).readParityPage(
            this.fileReshardIdentity(args),
            args.role,
            args.cursor,
            args.limit
        );
    }

    prepareReshardFileDestAttachments(args: Omit<CdbReshardSplitIdentity, "role">): {
        prepared: boolean;
        triggersInstalled: number;
    } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let prepared = false;
        let triggersInstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "dest", sql });
            triggersInstalled = this.setFileAttachmentTriggers(sql, "install");
            prepared = new CdbFileReshardStore(sql).prepareDestAttachments(
                this.fileReshardIdentity(args),
                Date.now()
            ).prepared;
        });
        return { prepared, triggersInstalled };
    }

    activateReshardFileDest(args: Omit<CdbReshardSplitIdentity, "role">): { activated: boolean } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let activated = false;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "dest", sql });
            activated = new CdbFileReshardStore(sql).activateDest(this.fileReshardIdentity(args), Date.now()).activated;
        });
        return { activated };
    }

    stopReshardFileSource(args: Omit<CdbReshardSplitIdentity, "role">): {
        stopped: boolean;
        triggersUninstalled: number;
    } {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let stopped = false;
        let triggersUninstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "source", sql });
            const capture = renderFileReshardTriggers(args.migId);
            for (const statement of capture.uninstall) {
                sql.exec(statement);
                triggersUninstalled++;
            }
            // Attachment triggers are table-wide. Source routing rejects the moved range,
            // while unrelated ranges on this Cdb still need their normal file lifecycle.
            stopped = new CdbFileReshardStore(sql).stopSourceAttachments(
                this.fileReshardIdentity(args),
                Date.now()
            ).stopped;
        });
        return { stopped, triggersUninstalled };
    }

    drainReshardFiles(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly cursor: CdbFileReshardDrainCursor;
            readonly limit: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let result: ReturnType<CdbFileReshardStore["drain"]> | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            this.resharding.assertReshardMovement({ migId: args.migId, role: "source", sql });
            result = new CdbFileReshardStore(sql).drain(this.fileReshardIdentity(args), args.cursor, args.limit);
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "file metadata drain failed" });
        return result;
    }

    abortReshardFiles(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly role: "source" | "dest";
            readonly afterKind?: "" | "file" | "organization_tombstone";
            readonly afterId?: string;
            readonly limit?: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let result: {
            afterKind: "" | "file" | "organization_tombstone";
            afterId: string;
            deleted: number;
            done: boolean;
        } = {
            afterKind: args.afterKind ?? "",
            afterId: args.afterId ?? "",
            deleted: 0,
            done: true,
        };
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const store = new CdbFileReshardStore(sql);
            if (args.role === "source") {
                for (const statement of renderFileReshardTriggers(args.migId).uninstall) sql.exec(statement);
                this.setFileAttachmentTriggers(sql, "install");
                store.abortSource(this.fileReshardIdentity(args), Date.now());
                return;
            }
            this.setFileAttachmentTriggers(sql, "uninstall");
            result = store.abortDest(
                this.fileReshardIdentity(args),
                Date.now(),
                args.afterKind,
                args.afterId,
                args.limit
            );
        });
        return result;
    }

    finishReshardFiles(
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly role: "source" | "dest"; readonly limit?: number }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        let result: { cleaned: number; done: boolean } | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            result = new CdbFileReshardStore(sql).finish(
                this.fileReshardIdentity(args),
                args.role,
                Date.now(),
                args.limit
            );
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "file reshard finish failed" });
        return result;
    }

    beginReshardVectorSource(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) {
            return { enabled: false as const, triggersInstalled: 0, snapshot: null };
        }
        let result:
            | {
                  enabled: true;
                  triggersInstalled: number;
                  snapshot: ReturnType<CdbVectorReshardSnapshotSessionStore["begin"]>;
              }
            | undefined;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "capturing");
            assertVectorReshardCaptureForeignKeys(sql);
            const triggers = renderVectorReshardTriggers(args.migId);
            this.uninstallVectorReshardTriggers(sql, args.migId);
            for (const statement of triggers.install) sql.exec(statement);
            const snapshot = new CdbVectorReshardSnapshotSessionStore(sql).begin(identity);
            result = { enabled: true, triggersInstalled: triggers.install.length, snapshot };
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "vector source capture begin failed" });
        return result;
    }

    inspectReshardVectorSnapshot(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, snapshot: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "capturing");
            return {
                enabled: true as const,
                snapshot: new CdbVectorReshardSnapshotSessionStore(sql).inspect(identity),
            };
        });
    }

    readReshardVectorSnapshot(args: Omit<CdbReshardSplitIdentity, "role"> & CdbVectorReshardSnapshotRequest) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, page: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "capturing");
            const page = new CdbVectorReshardSnapshotSessionStore(sql).read(identity, {
                pageNumber: args.pageNumber,
                cursor: args.cursor,
            });
            return { enabled: true as const, page };
        });
    }

    stopReshardVectorSource(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) {
            return { enabled: false as const, stopped: false, triggersUninstalled: 0 };
        }
        let triggersUninstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "frozen");
            const snapshot = new CdbVectorReshardSnapshotSessionStore(sql).inspect(identity);
            if (!snapshot.terminal) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "vector snapshot must finish before source capture stops",
                });
            }
            const tail = sql.one<{ readonly acked_lsn: number; readonly high_lsn: number }>(
                `SELECT state.acked_lsn,
                        MAX(state.acked_lsn, COALESCE(
                            (SELECT MAX(tail.lsn) FROM _chardb_split_log AS tail WHERE tail.mig_id = state.mig_id),
                            0
                        )) AS high_lsn
                 FROM _chardb_split_state AS state
                 WHERE state.mig_id = ? AND state.role = 'source'`,
                args.migId
            );
            if (!tail || tail.acked_lsn !== tail.high_lsn) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "generic source tail must converge before vector capture stops",
                });
            }
            triggersUninstalled = this.uninstallVectorReshardTriggers(sql, args.migId);
        });
        return { enabled: true as const, stopped: triggersUninstalled > 0, triggersUninstalled };
    }

    beginReshardVectorDest(args: Omit<CdbReshardSplitIdentity, "role"> & { readonly throughHeadSeq: number }) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, cursor: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardDestIdentity(args, sql, "bulk");
            initializeCdbVectorReshardDestStore(sql);
            this.setVectorMutationTriggers(sql, this.vectorResources(), "uninstall");
            uninstallCdbVectorOrganizationDeletionGuards(sql);
            return {
                enabled: true as const,
                cursor: new CdbVectorReshardDestStore(sql).begin(identity, args.throughHeadSeq),
            };
        });
    }

    applyReshardVectorSnapshot(args: Omit<CdbReshardSplitIdentity, "role"> & CdbVectorReshardDestRequest) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, result: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardDestIdentity(args, sql, "bulk");
            return { enabled: true as const, result: new CdbVectorReshardDestStore(sql).apply(identity, args) };
        });
    }

    readReshardVectorParityPage(
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly cursor: CdbVectorReshardCursor }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, encodedPage: null, throughLsn: 0 };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "frozen");
            const page = new CdbVectorReshardSnapshotReader(sql).read(identity, args.cursor);
            return {
                enabled: true as const,
                encodedPage: encodeCdbVectorReshardPage(page),
                throughLsn: this.fileSnapshotThroughLsn(sql, args.migId),
            };
        });
    }

    verifyReshardVectorParity(args: Omit<CdbReshardSplitIdentity, "role"> & CdbVectorReshardParityRequest) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, result: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardDestIdentity(args, sql, "parity");
            const split = sql.one<{ applied_lsn: number }>(
                "SELECT applied_lsn FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
                args.migId
            );
            if (!split || split.applied_lsn !== args.throughLsn) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "vector parity watermark does not equal the destination tail cursor",
                });
            }
            return {
                enabled: true as const,
                result: new CdbVectorReshardDestStore(sql).verifyParityPage(identity, args),
            };
        });
    }

    finalizeReshardVectorDest(args: Omit<CdbReshardSplitIdentity, "role"> & { readonly throughLsn: number }) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) {
            return { enabled: false as const, finalized: false, triggersInstalled: 0 };
        }
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardDestIdentity(args, sql, "parity");
            const split = sql.one<{ applied_lsn: number }>(
                "SELECT applied_lsn FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
                args.migId
            );
            if (!split || split.applied_lsn !== args.throughLsn) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "vector finalization watermark does not equal the destination tail cursor",
                });
            }
            const changed = new CdbVectorReshardDestStore(sql).finalize(identity, args.throughLsn).finalized;
            const finalized =
                sql.one<{ outcome: string }>(
                    "SELECT outcome FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?",
                    args.migId
                )?.outcome === "finalized";
            this.assertVectorDerivedState(sql);
            const resources = this.vectorResources();
            this.setVectorMutationTriggers(sql, resources, "install");
            installCdbVectorOrganizationDeletionGuards(sql);
            const triggersInstalled = resources.reduce(
                (count, resource) => count + renderVectorMutationTriggerSet(resource).install.length,
                0
            );
            this.assertVectorMutationTriggersInstalled(sql);
            return { enabled: true as const, finalized, changed, triggersInstalled };
        });
    }

    prepareReshardVectorSourceDrain(
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly cursor: CdbVectorReshardSourcePrepareCursor }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, result: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "frozen");
            return {
                enabled: true as const,
                result: new CdbVectorReshardSourceDrainStore(sql).prepare(identity, args.cursor),
            };
        });
    }

    drainReshardVectorSource(
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly cursor: CdbVectorReshardSourceDeleteCursor }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, result: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "frozen");
            if (args.cursor.kind === "head") {
                assertReshardSourceDomainDrained(sql, args.migId, args.tables);
            }
            return {
                enabled: true as const,
                result: new CdbVectorReshardSourceDrainStore(sql).delete(identity, args.cursor),
            };
        });
    }

    abortReshardVectorDest(
        args: Omit<CdbReshardSplitIdentity, "role"> & {
            readonly destinationGeneration: number;
            readonly limit?: number;
        }
    ) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, result: null };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const session = sql.one("SELECT 1 FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?", args.migId);
            const bound = sql.one("SELECT 1 FROM _chardb_split_identity WHERE mig_id = ?", args.migId);
            if (!session) {
                if (bound) {
                    this.assertVectorReshardDestIdentity(args, sql, "abort");
                    this.setVectorMutationTriggers(sql, this.vectorResources(), "install");
                    installCdbVectorOrganizationDeletionGuards(sql);
                } else {
                    assertCdbReshardRangeIdentity(args);
                    const schema = this.schemaMigrations.state(sql);
                    const split = sql.one<{
                        role: string;
                        range_lo: number;
                        range_hi: number;
                        destination_generation: number | null;
                        destination_serving: number;
                        abort_started: number;
                        drained: number;
                    }>(
                        `SELECT role, range_lo, range_hi, destination_generation, destination_serving,
                                abort_started, drained
                         FROM _chardb_split_state WHERE mig_id = ?`,
                        args.migId
                    );
                    if (
                        schema.status !== "active" ||
                        schema.activeVersion !== args.schemaVersion ||
                        schema.activeEpoch !== args.schemaEpoch ||
                        schema.activeDigest !== args.schemaDigest ||
                        split?.role !== "dest" ||
                        split.range_lo !== args.rangeLo ||
                        split.range_hi !== args.rangeHi ||
                        split.destination_generation !== args.destinationGeneration ||
                        split.destination_serving !== 0 ||
                        split.abort_started !== 1 ||
                        split.drained !== 1
                    ) {
                        throw new CdbError({
                            code: "CDB_RESHARD_PHASE_MISMATCH",
                            message: `migration ${args.migId} has no exact vector destination abort tombstone`,
                        });
                    }
                }
                return { enabled: false as const, result: null };
            }
            const identity = this.assertVectorReshardDestIdentity(args, sql, "abort");
            this.setVectorMutationTriggers(sql, this.vectorResources(), "uninstall");
            uninstallCdbVectorOrganizationDeletionGuards(sql);
            const result = new CdbVectorReshardDestStore(sql).abort(identity, args.limit, { allowFinalized: true });
            if (result.done) {
                this.setVectorMutationTriggers(sql, this.vectorResources(), "install");
                installCdbVectorOrganizationDeletionGuards(sql);
            }
            return { enabled: true as const, result };
        });
    }

    private cleanupReshardVectorSource(
        args: Omit<CdbReshardSplitIdentity, "role">,
        options: { readonly allowAbsent?: boolean } = {}
    ) {
        if (this.vectorResources().length === 0) {
            return { enabled: false as const, cleaned: false, done: true, triggersUninstalled: 0 };
        }
        let cleaned = false;
        let done = false;
        let triggersUninstalled = 0;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const session = sql.one("SELECT 1 FROM _chardb_vector_snapshot_sessions WHERE mig_id = ?", args.migId);
            if (!session && options.allowAbsent) {
                const bound = sql.one("SELECT 1 FROM _chardb_split_identity WHERE mig_id = ?", args.migId);
                if (bound) {
                    this.assertVectorReshardSourceIdentity(args, sql, "drained");
                    triggersUninstalled = this.uninstallVectorReshardTriggers(sql, args.migId);
                } else {
                    assertCdbReshardRangeIdentity(args);
                    const schema = this.schemaMigrations.state(sql);
                    const split = sql.one<{
                        role: string;
                        range_lo: number;
                        range_hi: number;
                        capture: number;
                        abort_started: number;
                        drained: number;
                    }>(
                        `SELECT role, range_lo, range_hi, capture, abort_started, drained
                         FROM _chardb_split_state WHERE mig_id = ?`,
                        args.migId
                    );
                    if (
                        schema.status !== "active" ||
                        schema.activeVersion !== args.schemaVersion ||
                        schema.activeEpoch !== args.schemaEpoch ||
                        schema.activeDigest !== args.schemaDigest ||
                        split?.role !== "source" ||
                        split.range_lo !== args.rangeLo ||
                        split.range_hi !== args.rangeHi ||
                        split.capture !== 0 ||
                        split.abort_started !== 1 ||
                        split.drained !== 1
                    ) {
                        throw new CdbError({
                            code: "CDB_RESHARD_PHASE_MISMATCH",
                            message: `migration ${args.migId} has no exact vector source abort tombstone`,
                        });
                    }
                }
                done = true;
                return;
            }
            const identity = this.assertVectorReshardSourceIdentity(args, sql, "drained");
            triggersUninstalled = this.uninstallVectorReshardTriggers(sql, args.migId);
            cleaned = new CdbVectorReshardSnapshotSessionStore(sql).cleanup(identity).cleaned;
            done =
                sql.one<{ cleaned: number }>(
                    "SELECT cleaned FROM _chardb_vector_snapshot_sessions WHERE mig_id = ?",
                    args.migId
                )?.cleaned === 1;
        });
        return { enabled: true as const, cleaned, done, triggersUninstalled };
    }

    abortReshardVectors(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.cleanupReshardVectorSource(args, { allowAbsent: true });
    }

    finishReshardVectorDest(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (this.vectorResources().length === 0) return { enabled: false as const, cleaned: false, done: true };
        return this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const identity = this.assertVectorReshardDestIdentity(args, sql, "serving");
            const cleanup = new CdbVectorReshardDestStore(sql).cleanup(identity);
            const done =
                sql.one<{ cleaned: number }>(
                    "SELECT cleaned FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?",
                    args.migId
                )?.cleaned === 1;
            return { enabled: true as const, ...cleanup, done };
        });
    }

    finishReshardVectors(args: Omit<CdbReshardSplitIdentity, "role">) {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.cleanupReshardVectorSource(args);
    }

    async beginReshardSource(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
        recoveryGeneration: number;
    }): Promise<{ enabled: boolean; triggersInstalled: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        if (!this.hasOrganizationTombstoneResources()) {
            this.ctx.storage.transactionSync(() =>
                this.resharding.assertNoUnmovableFileState(adaptSqlStorage(this.ctx.storage.sql))
            );
        }
        return this.resharding.beginReshardSource(args);
    }

    /** Persist fail-closed destination ownership before any provisioning can expose the shard. */
    async prepareReshardDestOwnership(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<{ prepared: boolean; serving: boolean }> {
        await this.reconcileRecoveryAdmission();
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.prepareReshardDestOwnership(args);
    }

    /** Destination-shard counterpart; tracks the migration so duplicate applies are rejected. */
    async beginReshardDest(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<{ ready: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.beginReshardDest(args);
    }

    /** Start serving only after Catalog has committed this exact cutover generation. */
    async activateReshardDestServing(
        args: Omit<CdbReshardSplitIdentity, "role"> & { destinationGeneration: number }
    ): Promise<{ activated: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        const result = this.resharding.activateReshardDestServing(args);
        if (this.vectorResources().length > 0) {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const pendingOrganizationDeletion = new CdbVectorOrganizationDeletionStore(sql, callback =>
                callback()
            ).nextPendingPage();
            if (pendingOrganizationDeletion) {
                await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
            }
            const nextVectorAt = new CdbVectorOutboxStore(sql).nextDueAt();
            if (nextVectorAt !== null) {
                await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextVectorAt));
            }
        }
        return result;
    }

    /** Read bounded captured mutation outcomes in source LSN order. */
    async readSplitOpLogBatch(args: {
        migId: string;
        recoveryGeneration: number;
        afterLsn: number;
        limit: number;
    }): Promise<SplitOpLogBatch> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.readSplitOpLogBatch(args);
    }

    /** Prune source mutation outcomes only after the Resharder durably records destination acceptance. */
    async ackSplitOpLog(args: {
        migId: string;
        recoveryGeneration: number;
        throughLsn: number;
    }): Promise<SplitOpLogAckResult> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.ackSplitOpLog(args);
    }

    /** Reconstruct source mutation outcomes and its durable cursor atomically on the destination. */
    async applySplitOpLogBatch(args: {
        migId: string;
        recoveryGeneration: number;
        rangeLo: number;
        rangeHi: number;
        entries: SplitOpLogBatch["entries"];
    }): Promise<SplitOpLogApplyResult> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.applySplitOpLogBatch(args);
    }

    /** Returns the tail-capture watermark — the latest `_chardb_split_log.lsn` Source has produced. */
    async tailWatermark(args: { migId: string; recoveryGeneration: number }): Promise<{ lsn: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.tailWatermark(args.migId);
    }

    /** Resolve the deterministic parent-before-child order for bulk copy. */
    async reshardTableOrder(args: {
        migId: string;
        recoveryGeneration: number;
        role: "source" | "dest";
        range: RangeFilter;
        tables: readonly TableSpec[];
    }): Promise<{ tableNames: readonly string[] }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.reshardTableOrder(args);
    }

    /**
     * Read one paginated bulk-copy batch from this (source) shard. The batch
     * contains rows whose partition column hashes into `[lo, hi]`; the caller
     * pages by `afterRowid` until `done=true`. Rows are returned as plain
     * column maps for the destination's primary-key-aware apply path.
     */
    async bulkCopyBatch(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: RangeFilter;
        afterRowid: number;
        limit: number;
    }): Promise<{ rows: readonly Record<string, RawJson>[]; lastRowid: number; done: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.bulkCopyBatch(args);
    }

    /**
     * Apply a bulk-copy batch on this (destination) shard. Each row is filtered
     * by `vshardOf(partition_key) ∈ range` defensively so a misrouted batch
     * cannot corrupt non-migrating data, then upserted against the declared
     * primary key without SQLite's delete-and-insert `REPLACE` behavior.
     * The whole batch runs in a single `transactionSync` to keep destination
     * state consistent against retries.
     */
    async applyBulkBatch(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: RangeFilter;
        rows: readonly Record<string, RawJson>[];
    }): Promise<{ applied: number; skipped: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.applyBulkBatch(args);
    }

    /** Permanently reject delayed bulk batches once all source snapshots are copied. */
    async closeReshardBulkDest(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ closed: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.closeReshardBulkDest(args);
    }

    /**
     * Drain the source's `_chardb_split_log` for a migration. The returned rows
     * are ordered by `lsn` and bounded by `limit`; the destination applies them
     * via `applyTailBatch`. The split log is preserved on the source until
     * `finishReshardSource` so a crash mid-replay can resume.
     */
    async readTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        afterLsn: number;
        limit: number;
    }): Promise<{
        transactions: readonly TailTransaction[];
        lastLsn: number;
        done: boolean;
    }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.readTailBatch(args);
    }

    /** Prune only tail transactions the Resharder has durably acknowledged. */
    async ackTail(args: {
        migId: string;
        recoveryGeneration: number;
        throughLsn: number;
    }): Promise<{ pruned: number; ackedLsn: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.ackTail(args);
    }

    /** Durably stage complete source transactions while stale bulk pages may still arrive. */
    async stageTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        tables: readonly TableSpec[];
        range: RangeFilter;
        transactions: readonly TailTransaction[];
    }): Promise<{ staged: number; lastLsn: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.stageTailBatch(args);
    }

    async readStagedTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        limit: number;
    }): Promise<{ transactions: TailTransaction[] }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.readStagedTailBatch(args);
    }

    async ackStagedTail(args: {
        migId: string;
        recoveryGeneration: number;
        throughLsn: number;
    }): Promise<{ removed: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.ackStagedTail(args);
    }

    async closeTailStaging(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ closed: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.closeTailStaging(args);
    }

    /**
     * Apply one source-ordered, cross-table tail batch on the destination.
     * The data changes and durable LSN cursor commit in one transaction.
     * Inserts and updates use primary-key-aware upserts. Deletes use the exact
     * primary key recovered from the trigger pre-image.
     */
    async applyTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        tables: readonly TableSpec[];
        range: RangeFilter;
        transactions: readonly TailTransaction[];
    }): Promise<{ applied: number; lastLsn: number }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.applyTailBatch(args);
    }

    /** Freeze source capture after fenced convergence and before destructive drain. */
    async stopReshardCapture(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ stopped: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.stopReshardCapture(args);
    }

    /**
     * Post-cutover, delete the migrated rows from the source and tear down
     * the per-table triggers + the split-state record. Idempotent: a
     * re-entry deletes nothing because the destination already owns the
     * migrated keys via the new range table.
     */
    async dropMigratedRange(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: RangeFilter;
        batchSize: number;
    }): Promise<{ deleted: number; done: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.dropMigratedRange(args);
    }

    /**
     * Tear down the per-migration triggers and mark the split-state row as
     * drained. After this call the source is clean of all migration artifacts.
     */
    async finishReshardSource(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.finishReshardSource(args);
    }

    /** Stop source capture and remove all transient artifacts before a pre-fence abort. */
    async abortReshardSource(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.abortReshardSource(args);
    }

    /** Permanently fence destination movement RPCs before bounded abort cleanup starts. */
    async beginReshardDestAbort(
        args: Omit<CdbReshardSplitIdentity, "role"> & { destinationGeneration: number }
    ): Promise<{ started: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        const result = await this.resharding.beginReshardDestAbort(args);
        if (this.vectorResources().length > 0) {
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                const ownsVectorAbort =
                    sql.one("SELECT 1 FROM _chardb_split_identity WHERE mig_id = ?", args.migId) !== null ||
                    sql.one("SELECT 1 FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?", args.migId) !== null;
                if (ownsVectorAbort) {
                    this.setVectorMutationTriggers(sql, this.vectorResources(), "uninstall");
                    uninstallCdbVectorOrganizationDeletionGuards(sql);
                }
            });
        }
        return result;
    }

    /** Delete one bounded child-before-parent destination abort batch. */
    async abortReshardDestBatch(
        args: Omit<CdbReshardSplitIdentity, "role"> & { batchSize: number }
    ): Promise<{ deleted: number; done: boolean }> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.abortReshardDestBatch(args);
    }

    /** Mark a successful destination split drained and release transfer-only state. */
    async finishReshardDest(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.assertRecoveryGeneration(args.recoveryGeneration);
        return this.resharding.finishReshardDest(args);
    }
}

/** Bind one application schema and handler manifest into a shard-local DO class. */
export function configureCdbRuntime<TSchema extends Record<string, unknown>>(
    config: CdbRuntimeConfig<TSchema>
): typeof Cdb {
    return class ConfiguredCdb extends Cdb {
        protected override mutationSchema(): TSchema {
            return config.schema();
        }

        protected override mutationManifest(): ChardbManifest {
            return config.manifest();
        }

        protected override migrationJournal(): ChardbMigrationJournal {
            return config.migrations?.() ?? EMPTY_MIGRATION_JOURNAL;
        }
    };
}

function cdbRuntimeError(error: unknown): CdbError {
    if (isCdbError(error)) return error;
    if (error instanceof Error) {
        const match = /^(CDB_[A-Z_]+)(?::\s*)?/.exec(error.message);
        if (match?.[1] && isCdbErrorCode(match[1])) {
            return new CdbError({ code: match[1], message: error.message });
        }
        return new CdbError({ code: "CDB_INVARIANT", message: error.message });
    }
    return new CdbError({ code: "CDB_INVARIANT", message: "mutation failed with a non-Error value" });
}
