/**
 * `Resharder` DO. Orchestrates online vshard-range moves with idempotent
 * recovery from any phase via `migration_state`. The phase sequence:
 *
 *   0  record migration_state
 *   1  Source: enable tail capture into _chardb_split_log + _chardb_split_oplog
 *   2  Bulk copy Source → Dest, idempotent UPSERT keyed by per-row LSN
 *   3  Tail replay until lag is ms-level
 *   4  Dual-write window
 *   5  Atomic Catalog cutover (epoch advance + range table write in one tx)
 *   6  60s drain, then DROP migrated rows + their op-log entries from Source
 *
 * The phase machine here is the orchestrator only — actual data movement RPCs
 * happen against Source/Dest Cdb DOs via service bindings.
 */

import { DurableObject } from "cloudflare:workers";
import { CdbError } from "../../errors.ts";
import type { TableSpec } from "../../reshard/triggers.ts";
import type { RawJson } from "../../types.ts";
import { stableJson } from "../../util/canonical.ts";
import { VSHARD_COUNT } from "../../vshard.ts";
import { withChardbLoopbacks } from "../loopback.ts";
import {
    CDB_FILE_RESHARD_PAGE_SIZE,
    type CdbFileReshardDrainCursor,
    type CdbFileReshardParityPage,
    type CdbReshardFileRecord,
    type CdbReshardOrganizationTombstone,
} from "./cdb-file-reshard-store.ts";
import { canonicalRegisteredTableSpecs } from "./cdb-reshard-identity-store.ts";
import { isKnownReshardTailTable } from "./cdb-reshard-relational.ts";
import type { TailTransaction } from "./cdb-reshard-runtime.ts";
import type { SplitOpLogEntry } from "./cdb-split-oplog-store.ts";
import type { CdbVectorReshardAbortCursor } from "./cdb-vector-reshard-provenance.ts";
import type { CdbVectorReshardCursor } from "./cdb-vector-reshard-records.ts";
import type { CdbVectorReshardSnapshotResponse } from "./cdb-vector-reshard-snapshot-session.ts";
import type {
    CdbVectorReshardSourceDeleteCursor,
    CdbVectorReshardSourcePrepareCursor,
} from "./cdb-vector-reshard-source-drain.ts";
import {
    RecoveryCoordinatorStore,
    type RecoveryProviderCounts,
    type RecoveryReconcileCounts,
} from "./recovery-coordinator.ts";
import { RESHARDER_FILE_CURSOR_DDL, ResharderFileCursorStore } from "./resharder-file-cursor-store.ts";
import {
    RESHARDER_START_INTENT_DDL,
    type ResharderStartIdentity,
    type ResharderStartIntent,
    ResharderStartIntentStore,
} from "./resharder-start-intent-store.ts";
import { RESHARDER_VECTOR_CURSOR_DDL, ResharderVectorCursorStore } from "./resharder-vector-cursor-store.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

const RESHARDER_DDL = `
CREATE TABLE IF NOT EXISTS migration_state (
  mig_id TEXT PRIMARY KEY,
  src_shard TEXT NOT NULL,
  dst_shard TEXT NOT NULL,
  range_lo INTEGER NOT NULL,
  range_hi INTEGER NOT NULL,
  phase INTEGER NOT NULL,
  epoch_at_start INTEGER NOT NULL,
  recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0),
  tables_json TEXT NOT NULL DEFAULT '[]',
  bulk_cursor TEXT NOT NULL DEFAULT '{}',
  tail_cursor INTEGER NOT NULL DEFAULT 0,
  legacy_cutover_recovered INTEGER NOT NULL DEFAULT 0 CHECK (legacy_cutover_recovered IN (0, 1)),
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_oplog_cursor (
  mig_id TEXT PRIMARY KEY,
  source_lsn INTEGER NOT NULL DEFAULT 0 CHECK (source_lsn >= 0),
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_work_cursor (
  mig_id TEXT PRIMARY KEY,
  turn INTEGER NOT NULL DEFAULT 0 CHECK (turn BETWEEN 0 AND 3),
  bulk_table_index INTEGER NOT NULL DEFAULT 0 CHECK (bulk_table_index >= 0),
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_schema_identity (
  mig_id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL CHECK (schema_version >= 0),
  schema_epoch INTEGER NOT NULL CHECK (schema_epoch > 0),
  schema_digest TEXT NOT NULL
);
` as const;

export const RESHARDER_AUTO_TRIGGER_SIZE_BYTES = 5 * 1024 * 1024 * 1024;
export const RESHARDER_AUTO_TRIGGER_RPS = 800;

/**
 * Named phases. The numeric values are persisted in `migration_state.phase`
 * and must remain stable across versions; new phases append at the end.
 */
export const RESHARDER_PHASE = {
    INIT: 0,
    TAIL_CAPTURE_ENABLED: 1,
    BULK_COPY_DONE: 2,
    TAIL_CAUGHT_UP: 3,
    DUAL_WRITE_OPEN: 4,
    CATALOG_CUT_OVER: 5,
    SOURCE_DRAINED: 6,
    ABORTING: 7,
    ABORTED: -1,
} as const;
export type ResharderPhase = (typeof RESHARDER_PHASE)[keyof typeof RESHARDER_PHASE];

export interface ResharderEnv {
    readonly CDB_CATALOG?: DurableObjectNamespace;
    readonly CDB_SHARD?: DurableObjectNamespace;
}

/** Subset of `Catalog` RPC the Resharder needs. */
interface CatalogReshardRpc {
    topologyOperation(args: { migrationId: string; recoveryGeneration: number }): Promise<{
        migrationId: string;
        sourceShard: string;
        destinationShard: string;
        rangeLo: number;
        rangeHi: number;
        startEpoch: number;
        recoveryGeneration: number;
        status: "active" | "completed" | "aborted";
    } | null>;
    topologyRoutingStatus(args: {
        migId: string;
        sourceShard: string;
        destinationShard: string;
        rangeLo: number;
        rangeHi: number;
        startEpoch: number;
        recoveryGeneration: number;
    }): Promise<{
        owner: "source" | "destination";
        schemaEpoch: number;
        operationStatus: "active" | "completed" | "aborted";
    }>;
    beginTopologyOperation(args: {
        migId: string;
        sourceShard: string;
        destinationShard: string;
        rangeLo: number;
        rangeHi: number;
        startEpoch: number;
        recoveryGeneration: number;
    }): Promise<{
        status: "active" | "completed" | "aborted";
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
    }>;
    cutover(args: {
        migId: string;
        lo: number;
        hi: number;
        fromShard: string;
        toShard: string;
        startEpoch: number;
        recoveryGeneration: number;
    }): Promise<{ applied: boolean; newEpoch: number }>;
    beginOrganizationDeletionBarrier(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        recoveryGeneration: number;
    }): Promise<{
        migrationId: string;
        rangeLo: number;
        rangeHi: number;
        deletionWatermark: number;
        status: "active" | "released" | "aborted";
        createdAt: number;
        finishedAt: number | null;
    }>;
    organizationDeletionBarrierStatus(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        recoveryGeneration: number;
    }): Promise<{ barrier: unknown; olderDeletionsComplete: boolean }>;
    completeTopologyOperation(args: {
        migId: string;
        sourceShard: string;
        destinationShard: string;
        rangeLo: number;
        rangeHi: number;
        startEpoch: number;
        recoveryGeneration: number;
    }): Promise<{ status: "completed" }>;
    abortTopologyOperation(args: {
        migId: string;
        sourceShard: string;
        destinationShard: string;
        rangeLo: number;
        rangeHi: number;
        startEpoch: number;
        recoveryGeneration: number;
    }): Promise<{ status: "aborted" }>;
}

/** Subset of `Cdb` RPC the Resharder needs. */
interface CdbReshardRpc {
    reshardSideStateProtocolCapabilitiesV2?(): Promise<{
        readonly vectorSnapshot: "v2";
        readonly fileTombstones: "v2";
    }>;
    prepareReshardDestOwnership(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<{ prepared: boolean; serving: boolean }>;
    provisionFreshReshardDestination(args: {
        recoveryGeneration: number;
        migrationId: string;
        targetVersion: number;
        targetEpoch: number;
        targetDigest: string;
    }): Promise<unknown>;
    prepareRoutingFence(args: {
        migrationId: string;
        rangeLo: number;
        rangeHi: number;
        sourceGeneration: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<unknown>;
    activateRoutingFence(args: {
        migrationId: string;
        rangeLo: number;
        rangeHi: number;
        sourceGeneration: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<unknown>;
    completeRoutingFenceCleanup(args: {
        migrationId: string;
        rangeLo: number;
        rangeHi: number;
        sourceGeneration: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<unknown>;
    cancelRoutingFenceBeforeCutover?(args: {
        migrationId: string;
        rangeLo: number;
        rangeHi: number;
        sourceGeneration: number;
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<unknown>;
    beginReshardSource(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
        recoveryGeneration: number;
    }): Promise<{ enabled: boolean; triggersInstalled: number }>;
    beginReshardDest(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
        destinationGeneration: number;
        recoveryGeneration: number;
    }): Promise<{
        ready: boolean;
    }>;
    beginReshardVectorSource?(args: ReshardCleanupIdentity): Promise<{
        enabled: boolean;
        snapshot: null | {
            throughHeadSeq: number;
            next: { pageNumber: number; cursor: CdbVectorReshardCursor };
            terminal: boolean;
        };
    }>;
    beginReshardVectorDest?(
        args: ReshardCleanupIdentity & { throughHeadSeq: number }
    ): Promise<{ enabled: boolean; cursor: CdbVectorReshardCursor | null }>;
    readReshardVectorSnapshot?(
        args: ReshardCleanupIdentity & { pageNumber: number; cursor: CdbVectorReshardCursor }
    ): Promise<{ enabled: boolean; page: CdbVectorReshardSnapshotResponse | null }>;
    applyReshardVectorSnapshot?(
        args: ReshardCleanupIdentity & CdbVectorReshardSnapshotResponse & { cursor: CdbVectorReshardCursor }
    ): Promise<{
        enabled: boolean;
        result: null | { next: CdbVectorReshardCursor; done: boolean };
    }>;
    readReshardVectorParityPage?(
        args: ReshardCleanupIdentity & { cursor: CdbVectorReshardCursor }
    ): Promise<{ enabled: boolean; encodedPage: string | null; throughLsn: number }>;
    verifyReshardVectorParity?(
        args: ReshardCleanupIdentity & {
            pageNumber: number;
            cursor: CdbVectorReshardCursor;
            encodedSourcePage: string;
            throughLsn: number;
        }
    ): Promise<{ enabled: boolean; result: null | { next: CdbVectorReshardCursor; done: boolean } }>;
    finalizeReshardVectorDest?(
        args: ReshardCleanupIdentity & { throughLsn: number }
    ): Promise<{ enabled: boolean; finalized: boolean; triggersInstalled: number }>;
    stopReshardVectorSource?(
        args: ReshardCleanupIdentity
    ): Promise<{ enabled: boolean; stopped: boolean; triggersUninstalled: number }>;
    prepareReshardVectorSourceDrain?(
        args: ReshardCleanupIdentity & { cursor: CdbVectorReshardSourcePrepareCursor }
    ): Promise<{
        enabled: boolean;
        result: null | { prepared: number; cursor: CdbVectorReshardSourcePrepareCursor; done: boolean };
    }>;
    drainReshardVectorSource?(args: ReshardCleanupIdentity & { cursor: CdbVectorReshardSourceDeleteCursor }): Promise<{
        enabled: boolean;
        result: null | { deleted: number; cursor: CdbVectorReshardSourceDeleteCursor; done: boolean };
    }>;
    abortReshardVectorDest?(args: ReshardCleanupIdentity & { destinationGeneration: number; limit?: number }): Promise<{
        enabled: boolean;
        result: null | { deleted: number; next: CdbVectorReshardAbortCursor; done: boolean };
    }>;
    abortReshardVectors?(args: ReshardCleanupIdentity): Promise<{
        enabled: boolean;
        cleaned: boolean;
        done: boolean;
    }>;
    finishReshardVectors?(args: ReshardCleanupIdentity): Promise<{
        enabled: boolean;
        cleaned: boolean;
        done: boolean;
    }>;
    finishReshardVectorDest?(args: ReshardCleanupIdentity): Promise<{
        enabled: boolean;
        cleaned: boolean;
        done: boolean;
    }>;
    prepareReshardFileSource(
        args: ReshardCleanupIdentity & {
            afterKind: "file" | "organization_tombstone";
            afterId: string;
            limit: number;
        }
    ): Promise<{
        enabled: boolean;
        backfill: { files: number; tombstones: number; done: boolean };
        cursor: { kind: "file" | "organization_tombstone"; afterId: string; done: boolean };
    }>;
    beginReshardFileSource(args: ReshardCleanupIdentity): Promise<{ enabled: boolean; triggersInstalled: number }>;
    beginReshardFileDest(args: ReshardCleanupIdentity): Promise<{ enabled: boolean; triggersUninstalled: number }>;
    readReshardFileSnapshot(
        args: ReshardCleanupIdentity & { afterPlacement: number; afterFileId: string; limit: number }
    ): Promise<{
        rows: readonly CdbReshardFileRecord[];
        afterPlacement: number;
        afterId: string;
        done: boolean;
        throughLsn: number;
    }>;
    applyReshardFileSnapshot(
        args: ReshardCleanupIdentity & { rows: readonly CdbReshardFileRecord[]; throughLsn: number }
    ): Promise<{ applied: number; inserted: number }>;
    readReshardFileTombstones(
        args: ReshardCleanupIdentity & { afterPlacement: number; afterOrganizationId: string; limit: number }
    ): Promise<{
        rows: readonly CdbReshardOrganizationTombstone[];
        afterPlacement: number;
        afterId: string;
        done: boolean;
        throughLsn: number;
    }>;
    readReshardFileTombstonesV2(
        args: ReshardCleanupIdentity & { afterPlacement: number; afterOrganizationId: string; limit: number }
    ): Promise<{
        rows: readonly CdbReshardOrganizationTombstone[];
        afterPlacement: number;
        afterId: string;
        done: boolean;
        throughLsn: number;
    }>;
    applyReshardFileTombstones(
        args: ReshardCleanupIdentity & {
            rows: readonly CdbReshardOrganizationTombstone[];
            throughLsn: number;
        }
    ): Promise<{ applied: number; inserted: number }>;
    applyReshardFileTombstonesV2(
        args: ReshardCleanupIdentity & {
            rows: readonly CdbReshardOrganizationTombstone[];
            throughLsn: number;
        }
    ): Promise<{ applied: number; inserted: number }>;
    fenceReshardFileSource(args: ReshardCleanupIdentity): Promise<{ fenced: boolean }>;
    validateReshardFiles(
        args: ReshardCleanupIdentity & { cursor: CdbFileReshardDrainCursor; limit: number }
    ): Promise<{ cursor: CdbFileReshardDrainCursor; checked: number; done: boolean }>;
    readReshardFileParityPage(
        args: ReshardCleanupIdentity & {
            role: "source" | "dest";
            cursor: CdbFileReshardDrainCursor;
            limit: number;
        }
    ): Promise<CdbFileReshardParityPage>;
    prepareReshardFileDestAttachments(
        args: ReshardCleanupIdentity
    ): Promise<{ prepared: boolean; triggersInstalled: number }>;
    activateReshardFileDest(args: ReshardCleanupIdentity): Promise<{ activated: boolean }>;
    reshardFileAppliedProvenance(args: ReshardCleanupIdentity): Promise<{ rows: number; legacyRows: number }>;
    stopReshardFileSource(args: ReshardCleanupIdentity): Promise<{ stopped: boolean; triggersUninstalled: number }>;
    drainReshardFiles(
        args: ReshardCleanupIdentity & { cursor: CdbFileReshardDrainCursor; limit: number }
    ): Promise<{ cursor: CdbFileReshardDrainCursor; deleted: number; done: boolean }>;
    abortReshardFiles(
        args: ReshardCleanupIdentity & {
            role: "source" | "dest";
            afterKind?: "" | "file" | "organization_tombstone";
            afterId?: string;
            limit?: number;
        }
    ): Promise<{
        afterKind: "" | "file" | "organization_tombstone";
        afterId: string;
        deleted: number;
        done: boolean;
    }>;
    finishReshardFiles(
        args: ReshardCleanupIdentity & { role: "source" | "dest"; limit?: number }
    ): Promise<{ cleaned: number; done: boolean }>;
    activateReshardDestServing(
        args: ReshardCleanupIdentity & { destinationGeneration: number }
    ): Promise<{ activated: boolean }>;
    reshardTableOrder(args: {
        migId: string;
        recoveryGeneration: number;
        role: "source" | "dest";
        range: { lo: number; hi: number };
        tables: readonly TableSpec[];
    }): Promise<{ tableNames: readonly string[] }>;
    tailWatermark(args: { migId: string; recoveryGeneration: number }): Promise<{ lsn: number }>;
    bulkCopyBatch(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: { lo: number; hi: number };
        afterRowid: number;
        limit: number;
    }): Promise<{ rows: readonly Record<string, RawJson>[]; lastRowid: number; done: boolean }>;
    applyBulkBatch(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: { lo: number; hi: number };
        rows: readonly Record<string, RawJson>[];
    }): Promise<{ applied: number; skipped: number }>;
    closeReshardBulkDest(args: ReshardCleanupIdentity): Promise<{ closed: boolean }>;
    readTailBatch(args: { migId: string; recoveryGeneration: number; afterLsn: number; limit: number }): Promise<{
        transactions: readonly TailTransaction[];
        lastLsn: number;
        done: boolean;
    }>;
    ackTail(args: { migId: string; recoveryGeneration: number; throughLsn: number }): Promise<{
        pruned: number;
        ackedLsn: number;
    }>;
    applyTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        tables: readonly TableSpec[];
        range: { lo: number; hi: number };
        transactions: readonly TailTransaction[];
    }): Promise<{ applied: number; lastLsn: number }>;
    stageTailBatch(args: {
        migId: string;
        recoveryGeneration: number;
        tables: readonly TableSpec[];
        range: { lo: number; hi: number };
        transactions: readonly TailTransaction[];
    }): Promise<{ staged: number; lastLsn: number }>;
    readStagedTailBatch(args: { migId: string; recoveryGeneration: number; limit: number }): Promise<{
        transactions: readonly TailTransaction[];
    }>;
    ackStagedTail(args: { migId: string; recoveryGeneration: number; throughLsn: number }): Promise<{
        removed: number;
    }>;
    closeTailStaging(args: ReshardCleanupIdentity): Promise<{ closed: boolean }>;
    stopReshardCapture(args: ReshardCleanupIdentity): Promise<{ stopped: boolean }>;
    readSplitOpLogBatch(args: { migId: string; recoveryGeneration: number; afterLsn: number; limit: number }): Promise<{
        entries: readonly SplitOpLogEntry[];
        lastLsn: number;
        done: boolean;
    }>;
    ackSplitOpLog(args: {
        migId: string;
        recoveryGeneration: number;
        throughLsn: number;
    }): Promise<{ pruned: number; prunedBytes: number; ackedLsn: number }>;
    applySplitOpLogBatch(args: {
        migId: string;
        recoveryGeneration: number;
        rangeLo: number;
        rangeHi: number;
        entries: readonly SplitOpLogEntry[];
    }): Promise<{ applied: number; replayed: number; lastLsn: number }>;
    dropMigratedRange(args: {
        migId: string;
        recoveryGeneration: number;
        table: TableSpec;
        range: { lo: number; hi: number };
        batchSize: number;
    }): Promise<{ deleted: number; done: boolean }>;
    finishReshardSource(args: ReshardCleanupIdentity): Promise<void>;
    finishReshardDest(args: ReshardCleanupIdentity): Promise<void>;
    abortReshardSource(args: ReshardCleanupIdentity): Promise<void>;
    beginReshardDestAbort(
        args: ReshardCleanupIdentity & { destinationGeneration: number }
    ): Promise<{ started: boolean }>;
    abortReshardDestBatch(
        args: ReshardCleanupIdentity & { batchSize: number }
    ): Promise<{ deleted: number; done: boolean }>;
}

interface ReshardCleanupIdentity {
    readonly migId: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly schemaVersion: number;
    readonly schemaEpoch: number;
    readonly schemaDigest: string;
    readonly tables: readonly TableSpec[];
    readonly recoveryGeneration: number;
}

interface MigrationState {
    readonly phase: number;
    readonly src: string;
    readonly dst: string;
    readonly lo: number;
    readonly hi: number;
    readonly epochAtStart: number;
    readonly recoveryGeneration: number;
    readonly schemaVersion: number;
    readonly schemaEpoch: number;
    readonly schemaDigest: string;
    readonly tables: readonly TableSpec[];
    readonly bulkCursor: Record<string, number>;
    readonly tailCursor: number;
    readonly workTurn: number;
    readonly bulkTableIndex: number;
    readonly legacyCutoverRecovered: boolean;
}

interface StartSplitArgs {
    readonly migId: string;
    readonly srcShard: string;
    readonly dstShard: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly epochAtStart: number;
    readonly tables: readonly TableSpec[];
}

const BULK_BATCH = 500;
const TAIL_BATCH = 500;
const OPLOG_BATCH = 64;
const DROP_BATCH = 500;

function parseBulkCursor(value: string): Record<string, number> {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "reshard bulk cursor is malformed" });
    }
    const cursor: Record<string, number> = {};
    for (const [table, rowid] of Object.entries(parsed)) {
        if (!Number.isSafeInteger(rowid) || (rowid as number) < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "reshard bulk cursor is malformed" });
        }
        cursor[table] = rowid as number;
    }
    return cursor;
}
const RESHARDER_MAX_TABLES = 256;
const RESHARDER_MAX_COLUMNS_PER_TABLE = 256;
const RESHARDER_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const RESHARDER_MIG_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
const SQLITE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/;
const SCHEMA_DIGEST = /^[a-f0-9]{64}$/;

function invalidSplit(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message: `reshard split: ${message}` });
}

function canonicalSplitTables(tables: readonly TableSpec[]): string {
    if (!Array.isArray(tables) || tables.length < 1 || tables.length > RESHARDER_MAX_TABLES) {
        invalidSplit(`tables must contain from 1 through ${RESHARDER_MAX_TABLES} entries`);
    }
    const names = new Set<string>();
    const normalized = tables.map(table => {
        if (!table || typeof table !== "object" || Array.isArray(table)) invalidSplit("table spec is malformed");
        if (typeof table.name !== "string" || !SQLITE_IDENTIFIER.test(table.name))
            invalidSplit("table name is invalid");
        if (names.has(table.name)) invalidSplit("table names must be unique");
        names.add(table.name);
        if (typeof table.partitionColumn !== "string" || !SQLITE_IDENTIFIER.test(table.partitionColumn)) {
            invalidSplit("partition column is invalid");
        }
        if (
            !Array.isArray(table.columns) ||
            table.columns.length < 1 ||
            table.columns.length > RESHARDER_MAX_COLUMNS_PER_TABLE
        ) {
            invalidSplit(`table columns must contain from 1 through ${RESHARDER_MAX_COLUMNS_PER_TABLE} entries`);
        }
        const columns = [...table.columns];
        if (columns.some(column => typeof column !== "string" || !SQLITE_IDENTIFIER.test(column))) {
            invalidSplit("table column is invalid");
        }
        if (new Set(columns).size !== columns.length) invalidSplit("table columns must be unique");
        if (!columns.includes(table.partitionColumn)) invalidSplit("partition column must be present in table columns");
        return { name: table.name, partitionColumn: table.partitionColumn, columns };
    });
    normalized.sort((left, right) => left.name.localeCompare(right.name));
    return stableJson(normalized);
}

function canonicalStartSplit(args: {
    readonly migId: string;
    readonly srcShard: string;
    readonly dstShard: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly epochAtStart: number;
    readonly tables: readonly TableSpec[];
}): string {
    if (typeof args.migId !== "string" || !RESHARDER_MIG_ID.test(args.migId)) {
        invalidSplit("migration id is invalid");
    }
    if (
        typeof args.srcShard !== "string" ||
        typeof args.dstShard !== "string" ||
        !RESHARDER_ID.test(args.srcShard) ||
        !RESHARDER_ID.test(args.dstShard)
    ) {
        invalidSplit("shard id is invalid");
    }
    if (args.srcShard === args.dstShard) invalidSplit("source and destination shards must differ");
    if (
        !Number.isSafeInteger(args.rangeLo) ||
        !Number.isSafeInteger(args.rangeHi) ||
        args.rangeLo < 0 ||
        args.rangeHi < args.rangeLo ||
        args.rangeHi >= VSHARD_COUNT
    ) {
        invalidSplit("virtual-shard range is invalid");
    }
    if (!Number.isSafeInteger(args.epochAtStart) || args.epochAtStart < 1) {
        invalidSplit("starting routing epoch is invalid");
    }
    return canonicalSplitTables(args.tables);
}

export class Resharder extends DurableObject<ResharderEnv> {
    private bootstrapped = false;
    private readonly activeStarts = new Map<
        string,
        { readonly identity: ResharderStartIdentity; readonly promise: Promise<void> }
    >();
    private readonly activeRuns = new Map<string, Promise<{ phase: ResharderPhase }>>();
    private readonly activeAborts = new Map<string, Promise<void>>();
    private readonly activeRecoveries = new Map<
        string,
        Promise<{ action: "aborted" | "resumed"; phase: ResharderPhase }>
    >();
    private readonly ensuredSourcePhases = new Set<string>();

    constructor(state: DurableObjectState, env: ResharderEnv) {
        super(state, withChardbLoopbacks(env, state));
        state.blockConcurrencyWhile(async () => this.bootstrap());
    }

    /** Configured Workers override this so admission uses the packaged schema before Catalog takes a lease. */
    protected runtimeSchema(): Record<string, unknown> | null {
        return null;
    }

    async adminRecoveryAdmissionClock() {
        return new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).admissionClock();
    }

    async adminRecoveryCoordinatorState(args: { readonly operationId: string }) {
        return new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).read(args.operationId);
    }

    async adminActiveRecoveryForDigest(args: { readonly digest: string }) {
        return new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).activeForDigest(args.digest);
    }

    async adminClaimRecoveryPreparation(args: {
        readonly operationId: string;
        readonly digest: string;
        readonly continuationJson: string;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).claimPreparation(
                args.operationId,
                args.digest,
                args.continuationJson
            )
        );
    }

    async adminSaveRecoveryPreparation(args: { readonly operationId: string; readonly continuationJson: string }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).savePreparation(
                args.operationId,
                args.continuationJson
            )
        );
    }

    async adminCancelRecoveryPreparation(args: { readonly operationId: string }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).cancelPreparation(args.operationId)
        );
    }

    async adminBeginRecoveryCommits(args: { readonly operationId: string; readonly counts: RecoveryProviderCounts }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).beginCommits(
                args.operationId,
                args.counts
            )
        );
    }

    async adminFinishRecoveryShardCommits(args: {
        readonly operationId: string;
        readonly continuationJson: string;
        readonly shardCount: number;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).finishShards(
                args.operationId,
                args.continuationJson,
                args.shardCount
            )
        );
    }

    async adminAdvanceRecoveryShardCommit(args: {
        readonly operationId: string;
        readonly index: number;
        readonly objectId: string;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).advanceShard(
                args.operationId,
                args.index,
                args.objectId
            )
        );
    }

    async adminSaveRecoveryReconcile(args: { readonly operationId: string; readonly continuationJson: string }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).saveReconcile(
                args.operationId,
                args.continuationJson
            )
        );
    }

    async adminBeginRecoveryReleases(args: {
        readonly operationId: string;
        readonly counts: RecoveryReconcileCounts;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).beginReleases(
                args.operationId,
                args.counts
            )
        );
    }

    async adminAdvanceRecoveryRelease(args: { readonly operationId: string; readonly index: number }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).advanceRelease(
                args.operationId,
                args.index
            )
        );
    }

    async adminBeginRecoveryCatalogCommit(args: { readonly operationId: string; readonly shardCount: number }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).beginCatalog(
                args.operationId,
                args.shardCount
            )
        );
    }

    async adminCompleteRecovery(args: { readonly operationId: string }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).complete(args.operationId)
        );
    }

    async adminBeginRecoveryObjectCommit(args: {
        readonly operationId: string;
        readonly objectId: string;
        readonly bookmark: string;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).beginObject(
                args.operationId,
                args.objectId,
                args.bookmark
            )
        );
    }

    async adminFinishRecoveryObjectCommit(args: {
        readonly operationId: string;
        readonly objectId: string;
        readonly bookmark: string;
    }) {
        return this.ctx.storage.transactionSync(() =>
            new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).finishObject(
                args.operationId,
                args.objectId,
                args.bookmark
            )
        );
    }

    private bootstrap(): void {
        if (this.bootstrapped) return;
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        for (const stmt of `${RESHARDER_DDL}\n${RESHARDER_FILE_CURSOR_DDL}\n${RESHARDER_VECTOR_CURSOR_DDL}\n${RESHARDER_START_INTENT_DDL}`
            .split(";")
            .map(s => s.trim())
            .filter(Boolean))
            sql.exec(stmt);
        const workColumns = new Set(
            sql.all<{ name: string }>("PRAGMA table_info(migration_work_cursor)").map(column => column.name)
        );
        if (!workColumns.has("bulk_table_index")) {
            sql.exec(
                "ALTER TABLE migration_work_cursor ADD COLUMN bulk_table_index INTEGER NOT NULL DEFAULT 0 CHECK (bulk_table_index >= 0)"
            );
        }
        const migrationColumns = new Set(
            sql.all<{ name: string }>("PRAGMA table_info(migration_state)").map(column => column.name)
        );
        if (!migrationColumns.has("legacy_cutover_recovered")) {
            sql.exec(
                "ALTER TABLE migration_state ADD COLUMN legacy_cutover_recovered INTEGER NOT NULL DEFAULT 0 CHECK (legacy_cutover_recovered IN (0, 1))"
            );
        }
        if (!migrationColumns.has("recovery_generation")) {
            sql.exec(
                "ALTER TABLE migration_state ADD COLUMN recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0)"
            );
        }
        const startColumns = new Set(
            sql.all<{ name: string }>("PRAGMA table_info(migration_start_intent)").map(column => column.name)
        );
        if (!startColumns.has("recovery_generation")) {
            sql.exec(
                "ALTER TABLE migration_start_intent ADD COLUMN recovery_generation INTEGER CHECK (recovery_generation >= 0)"
            );
            sql.exec("UPDATE migration_start_intent SET recovery_generation = 0 WHERE src_shard IS NOT NULL");
        }
        sql.exec(
            `UPDATE migration_start_intent SET recovery_generation = NULL
             WHERE state = 'abort_requested' AND recovery_generation = 0
               AND src_shard IS NULL AND dst_shard IS NULL AND range_lo IS NULL AND range_hi IS NULL
               AND epoch_at_start IS NULL AND tables_json IS NULL`
        );
        sql.exec(
            `INSERT OR IGNORE INTO migration_work_cursor (mig_id, turn, updated_at)
             SELECT mig_id, 0, updated_at FROM migration_state`
        );
        new ResharderFileCursorStore(sql).ensureForMigrations();
        new ResharderVectorCursorStore(sql).ensureForMigrations();
        const identityColumns = sql.all<{ name: string }>("PRAGMA table_info(migration_schema_identity)");
        if (!identityColumns.some(column => column.name === "schema_epoch")) {
            sql.exec("ALTER TABLE migration_schema_identity ADD COLUMN schema_epoch INTEGER CHECK (schema_epoch > 0)");
        }
        this.bootstrapped = true;
    }

    /**
     * Open a new migration. `tables` enumerates every base table participating
     * in the split, with the column whose value drives the partition hash and
     * the full ordered column list (used by the bulk-copy + apply paths). The
     * Resharder persists the spec so `runSplit` can resume after a restart.
     */
    async startSplit(args: StartSplitArgs): Promise<void> {
        const schema = this.runtimeSchema();
        const admittedArgs: StartSplitArgs = schema
            ? { ...args, tables: canonicalRegisteredTableSpecs(schema, args.tables).tables }
            : args;
        const tablesJson = canonicalStartSplit(admittedArgs);
        const clock = new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).admissionClock();
        if (clock.activeOperationId !== null) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: "point-in-time recovery blocks resharding",
            });
        }
        const identity = this.startIdentity(admittedArgs, tablesJson, clock.generation);
        if (this.activeRecoveries.has(admittedArgs.migId) || this.activeAborts.has(admittedArgs.migId)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${admittedArgs.migId} cannot start while recovery or abort is active`,
            });
        }
        const active = this.activeStarts.get(admittedArgs.migId);
        if (active) {
            this.assertSameStartIdentity(active.identity, identity);
            return active.promise;
        }
        const operation = Promise.resolve().then(() => this.startSplitDriver(admittedArgs, tablesJson, identity));
        this.activeStarts.set(admittedArgs.migId, { identity, promise: operation });
        try {
            await operation;
        } finally {
            if (this.activeStarts.get(admittedArgs.migId)?.promise === operation) {
                this.activeStarts.delete(admittedArgs.migId);
            }
        }
    }

    private async startSplitDriver(
        args: StartSplitArgs,
        tablesJson: string,
        startIdentity: ResharderStartIdentity
    ): Promise<void> {
        const existing = this.readMigration(args.migId);
        if (
            existing &&
            (existing.src !== args.srcShard ||
                existing.dst !== args.dstShard ||
                existing.lo !== args.rangeLo ||
                existing.hi !== args.rangeHi ||
                existing.epochAtStart !== args.epochAtStart ||
                existing.recoveryGeneration !== startIdentity.recoveryGeneration ||
                canonicalSplitTables(existing.tables) !== tablesJson)
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${args.migId} is already bound to a different split`,
            });
        }
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const clock = new RecoveryCoordinatorStore(sql).admissionClock();
            if (clock.activeOperationId !== null || clock.generation !== startIdentity.recoveryGeneration) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "point-in-time recovery blocks resharding",
                });
            }
            new ResharderStartIntentStore(sql).begin(startIdentity, Date.now());
        });
        const catalog = this.catalog();
        const topology = await catalog.beginTopologyOperation(
            this.topologyRequest(args.migId, {
                src: args.srcShard,
                dst: args.dstShard,
                lo: args.rangeLo,
                hi: args.rangeHi,
                epochAtStart: args.epochAtStart,
                recoveryGeneration: startIdentity.recoveryGeneration,
            })
        );
        if (topology.status === "aborted") {
            this.ctx.storage.transactionSync(() => {
                new ResharderStartIntentStore(adaptSqlStorage(this.ctx.storage.sql)).requestAbort(
                    args.migId,
                    Date.now()
                );
            });
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "reshard topology operation is aborted" });
        }
        if (topology.status === "completed" && !existing) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${args.migId} completed in Catalog without local migration state`,
            });
        }
        if (
            !Number.isSafeInteger(topology.schemaVersion) ||
            topology.schemaVersion < 0 ||
            !Number.isSafeInteger(topology.schemaEpoch) ||
            topology.schemaEpoch < 1 ||
            typeof topology.schemaDigest !== "string" ||
            !SCHEMA_DIGEST.test(topology.schemaDigest)
        ) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: "Catalog returned an invalid topology schema identity",
            });
        }
        if (
            existing &&
            (existing.schemaVersion !== topology.schemaVersion ||
                existing.schemaEpoch !== topology.schemaEpoch ||
                existing.schemaDigest !== topology.schemaDigest)
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${args.migId} Catalog schema identity changed`,
            });
        }
        const now = Date.now();
        let canceled = false;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const intents = new ResharderStartIntentStore(sql);
            if (intents.requireExact(startIdentity).state === "abort_requested") {
                canceled = true;
                return;
            }
            const existing = sql.one<{
                src_shard: string;
                dst_shard: string;
                range_lo: number;
                range_hi: number;
                epoch_at_start: number;
                recovery_generation: number;
                tables_json: string;
            }>(
                `SELECT src_shard, dst_shard, range_lo, range_hi, epoch_at_start, recovery_generation, tables_json
                 FROM migration_state WHERE mig_id = ?`,
                args.migId
            );
            if (existing) {
                if (
                    existing.src_shard !== args.srcShard ||
                    existing.dst_shard !== args.dstShard ||
                    existing.range_lo !== args.rangeLo ||
                    existing.range_hi !== args.rangeHi ||
                    existing.epoch_at_start !== args.epochAtStart ||
                    existing.recovery_generation !== startIdentity.recoveryGeneration ||
                    canonicalSplitTables(JSON.parse(existing.tables_json) as TableSpec[]) !== tablesJson
                ) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `migId=${args.migId} is already bound to a different split`,
                    });
                }
                intents.clearStarted(startIdentity);
                return;
            }
            sql.exec(
                `INSERT INTO migration_state
                 (mig_id, src_shard, dst_shard, range_lo, range_hi, phase, epoch_at_start, recovery_generation,
                  tables_json, bulk_cursor, tail_cursor, started_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, '{}', 0, ?, ?)`,
                args.migId,
                args.srcShard,
                args.dstShard,
                args.rangeLo,
                args.rangeHi,
                args.epochAtStart,
                startIdentity.recoveryGeneration,
                tablesJson,
                now,
                now
            );
            sql.exec(
                "INSERT OR IGNORE INTO migration_oplog_cursor (mig_id, source_lsn, updated_at) VALUES (?, 0, ?)",
                args.migId,
                now
            );
            sql.exec(
                `INSERT OR IGNORE INTO migration_work_cursor
                 (mig_id, turn, bulk_table_index, updated_at) VALUES (?, 0, 0, ?)`,
                args.migId,
                now
            );
            new ResharderFileCursorStore(sql).create(args.migId, now);
            new ResharderVectorCursorStore(sql).create(args.migId, now);
            sql.exec(
                `INSERT INTO migration_schema_identity (mig_id, schema_version, schema_epoch, schema_digest)
                 VALUES (?, ?, ?, ?)`,
                args.migId,
                topology.schemaVersion,
                topology.schemaEpoch,
                topology.schemaDigest
            );
            intents.clearStarted(startIdentity);
        });
        if (canceled) {
            await catalog.abortTopologyOperation(this.topologyRequest(args.migId, this.startMigrationIdentity(args)));
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: `migId=${args.migId} was canceled before its split started`,
            });
        }
    }

    async advance(migId: string, expected?: ResharderPhase): Promise<{ phase: number }> {
        let outPhase = RESHARDER_PHASE.ABORTED as number;
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const row = sql.one<{ phase: number }>("SELECT phase FROM migration_state WHERE mig_id = ?", migId);
            if (!row) return;
            if (row.phase === RESHARDER_PHASE.ABORTING || row.phase === RESHARDER_PHASE.ABORTED) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migId=${migId} cannot advance while aborting or aborted`,
                });
            }
            if (expected !== undefined && row.phase !== expected) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migId=${migId} expected=${expected} actual=${row.phase}`,
                });
            }
            const next = row.phase + 1;
            sql.exec("UPDATE migration_state SET phase = ?, updated_at = ? WHERE mig_id = ?", next, Date.now(), migId);
            sql.exec(
                "UPDATE migration_work_cursor SET turn = 0, bulk_table_index = 0, updated_at = ? WHERE mig_id = ?",
                Date.now(),
                migId
            );
            outPhase = next;
        });
        return { phase: outPhase };
    }

    async abort(migId: string): Promise<void> {
        if (typeof migId !== "string" || !RESHARDER_MIG_ID.test(migId)) invalidSplit("migration id is invalid");
        if (this.activeRecoveries.has(migId)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} cannot abort while a movement driver is active`,
            });
        }
        const active = this.activeAborts.get(migId);
        if (active) return active;
        const operation = Promise.resolve().then(() => this.abortDriver(migId));
        this.activeAborts.set(migId, operation);
        try {
            await operation;
        } finally {
            if (this.activeAborts.get(migId) === operation) this.activeAborts.delete(migId);
        }
    }

    private async abortDriver(migId: string): Promise<void> {
        let migration = this.readMigration(migId);
        if (!migration) {
            let intent = this.ctx.storage.transactionSync(() =>
                new ResharderStartIntentStore(adaptSqlStorage(this.ctx.storage.sql)).requestAbort(migId, Date.now())
            );
            const activeStart = this.activeStarts.get(migId)?.promise;
            if (activeStart) await activeStart.catch(() => {});
            migration = this.readMigration(migId);
            if (!migration) {
                intent = new ResharderStartIntentStore(adaptSqlStorage(this.ctx.storage.sql)).read(migId) ?? intent;
                await this.abortPendingStart(intent);
                return;
            }
        }
        if (migration.phase === RESHARDER_PHASE.ABORTED) return;
        if (migration.phase !== RESHARDER_PHASE.ABORTING && migration.phase >= RESHARDER_PHASE.TAIL_CAUGHT_UP) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} can no longer abort after its source-fence boundary`,
                hint: "resume the same migration until source cleanup completes",
            });
        }
        if (migration.phase !== RESHARDER_PHASE.ABORTING) {
            const priorPhase = migration.phase;
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                sql.exec(
                    "UPDATE migration_state SET phase = ?, updated_at = ? WHERE mig_id = ? AND phase = ?",
                    RESHARDER_PHASE.ABORTING,
                    Date.now(),
                    migId,
                    priorPhase
                );
                if (sql.changes() !== 1) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `migId=${migId} changed while entering abort`,
                    });
                }
            });
            migration = this.readMigration(migId);
            if (!migration) throw new CdbError({ code: "CDB_INVARIANT", message: `unknown migId=${migId}` });
        }
        await this.runAbortCleanup(migId, migration);
    }

    async recoverLegacyFileMovement(migId: string): Promise<{
        action: "aborted" | "resumed";
        phase: ResharderPhase;
    }> {
        if (typeof migId !== "string" || !RESHARDER_MIG_ID.test(migId)) invalidSplit("migration id is invalid");
        const active = this.activeRecoveries.get(migId);
        if (active) return active;
        const operation = Promise.resolve().then(() => this.recoverLegacyFileMovementDriver(migId));
        this.activeRecoveries.set(migId, operation);
        try {
            return await operation;
        } finally {
            if (this.activeRecoveries.get(migId) === operation) this.activeRecoveries.delete(migId);
        }
    }

    private async recoverLegacyFileMovementDriver(migId: string): Promise<{
        action: "aborted" | "resumed";
        phase: ResharderPhase;
    }> {
        if (this.activeRuns.has(migId) || this.activeStarts.has(migId) || this.activeAborts.has(migId)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} cannot recover while another driver is active`,
            });
        }
        let migration = this.readMigration(migId);
        if (
            !migration ||
            (migration.phase !== RESHARDER_PHASE.TAIL_CAUGHT_UP &&
                migration.phase !== RESHARDER_PHASE.ABORTING &&
                !(migration.phase === RESHARDER_PHASE.DUAL_WRITE_OPEN && migration.legacyCutoverRecovered))
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} legacy recovery requires durable phase 3, its completed recovery marker, or an in-progress recovery abort`,
            });
        }
        const namespace = this.env.CDB_SHARD;
        if (!namespace) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "Resharder requires CDB_SHARD service binding" });
        }
        const source = namespace.get(namespace.idFromName(migration.src)) as unknown as CdbReshardRpc;
        const dest = namespace.get(namespace.idFromName(migration.dst)) as unknown as CdbReshardRpc;
        const fileCursor = this.fileCursors().read(migId);
        let vectorCursor = this.vectorCursors().read(migId);
        if (migration.phase === RESHARDER_PHASE.TAIL_CAUGHT_UP && fileCursor.enabled && vectorCursor.enabled === null) {
            // Phase three predates vector-aware movement only when the older
            // file protocol had already admitted this schema. That protocol
            // rejected vector resources, so persist the historical capability
            // result before any recovery side effect.
            this.vectorCursors().persistBegin(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP, false, null, null);
            vectorCursor = this.vectorCursors().read(migId);
        }
        if (!fileCursor.enabled && !vectorCursor.enabled) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} has no recoverable resource movement`,
            });
        }
        if (migration.phase === RESHARDER_PHASE.TAIL_CAUGHT_UP && fileCursor.enabled && !vectorCursor.enabled) {
            const provenance = await dest.reshardFileAppliedProvenance(this.splitIdentityRequest(migId, migration));
            if (provenance.rows < 1 || provenance.legacyRows < 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migId=${migId} has no legacy file provenance to recover`,
                });
            }
        }
        const routing = await this.catalog().topologyRoutingStatus(this.topologyRequest(migId, migration));
        if (routing.owner === "destination") {
            if (
                migration.phase !== RESHARDER_PHASE.TAIL_CAUGHT_UP &&
                !(migration.phase === RESHARDER_PHASE.DUAL_WRITE_OPEN && migration.legacyCutoverRecovered)
            ) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migId=${migId} cannot resume a cut-over route from abort cleanup`,
                });
            }
            if (routing.schemaEpoch !== migration.epochAtStart + 1) {
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "legacy recovery destination epoch changed" });
            }
            await dest.prepareReshardDestOwnership({
                migId,
                rangeLo: migration.lo,
                rangeHi: migration.hi,
                destinationGeneration: migration.epochAtStart + 1,
                recoveryGeneration: migration.recoveryGeneration,
            });
            if (fileCursor.enabled) {
                await dest.activateReshardFileDest(this.splitIdentityRequest(migId, migration));
            }
            await dest.activateReshardDestServing({
                ...this.splitIdentityRequest(migId, migration),
                destinationGeneration: migration.epochAtStart + 1,
            });
            if (migration.phase === RESHARDER_PHASE.TAIL_CAUGHT_UP) {
                this.ctx.storage.transactionSync(() => {
                    const sql = adaptSqlStorage(this.ctx.storage.sql);
                    sql.exec(
                        `UPDATE migration_state
                         SET phase = ?, legacy_cutover_recovered = 1, updated_at = ?
                         WHERE mig_id = ? AND phase = ?`,
                        RESHARDER_PHASE.DUAL_WRITE_OPEN,
                        Date.now(),
                        migId,
                        RESHARDER_PHASE.TAIL_CAUGHT_UP
                    );
                    if (sql.changes() !== 1) this.phaseChanged(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP);
                    sql.exec(
                        "UPDATE migration_work_cursor SET turn = 0, bulk_table_index = 0, updated_at = ? WHERE mig_id = ?",
                        Date.now(),
                        migId
                    );
                });
            }
            return { action: "resumed", phase: RESHARDER_PHASE.DUAL_WRITE_OPEN };
        }
        if (routing.schemaEpoch !== migration.epochAtStart || routing.operationStatus === "completed") {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "legacy recovery source epoch changed" });
        }
        if (typeof source.cancelRoutingFenceBeforeCutover !== "function") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "source Cdb cannot cancel a legacy recovery fence",
            });
        }
        if (migration.phase !== RESHARDER_PHASE.ABORTING) {
            this.ctx.storage.transactionSync(() => {
                const sql = adaptSqlStorage(this.ctx.storage.sql);
                sql.exec(
                    "UPDATE migration_state SET phase = ?, updated_at = ? WHERE mig_id = ? AND phase = ?",
                    RESHARDER_PHASE.ABORTING,
                    Date.now(),
                    migId,
                    RESHARDER_PHASE.TAIL_CAUGHT_UP
                );
                if (sql.changes() !== 1) this.phaseChanged(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP);
            });
            migration = this.readMigration(migId) as MigrationState;
        }
        await this.runAbortCleanup(migId, migration);
        return {
            action: "aborted",
            phase: (this.readMigration(migId)?.phase ?? RESHARDER_PHASE.ABORTED) as ResharderPhase,
        };
    }

    async getPhase(migId: string): Promise<ResharderPhase | null> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const row = sql.one<{ phase: number }>("SELECT phase FROM migration_state WHERE mig_id = ?", migId);
        return row ? (row.phase as ResharderPhase) : null;
    }

    migrationStatus(migId: string): {
        migId: string;
        phase: ResharderPhase;
        srcShard: string;
        dstShard: string;
        rangeLo: number;
        rangeHi: number;
    } | null {
        if (typeof migId !== "string" || !RESHARDER_MIG_ID.test(migId)) invalidSplit("migration id is invalid");
        const row = adaptSqlStorage(this.ctx.storage.sql).one<{
            mig_id: string;
            phase: number;
            src_shard: string;
            dst_shard: string;
            range_lo: number;
            range_hi: number;
        }>(
            `SELECT mig_id, phase, src_shard, dst_shard, range_lo, range_hi
             FROM migration_state WHERE mig_id = ?`,
            migId
        );
        return row
            ? {
                  migId: row.mig_id,
                  phase: row.phase as ResharderPhase,
                  srcShard: row.src_shard,
                  dstShard: row.dst_shard,
                  rangeLo: row.range_lo,
                  rangeHi: row.range_hi,
              }
            : null;
    }

    /**
     * Drive a migration from its current phase to terminal `SOURCE_DRAINED`.
     * Crash-safe: each phase advance is its own `transactionSync`, so a
     * re-entry after a crash resumes from the persisted `migration_state.phase`
     * and the per-table cursors. The driver orchestrates:
     *
     *   INIT → install per-table triggers + dest split-state, advance.
     *   TAIL_CAPTURE_ENABLED → paginated bulk copy of every range-matching row.
     *   BULK_COPY_DONE → drain `_chardb_split_log` until two consecutive reads
     *     return zero new rows or the convergence iteration cap is hit.
     *   TAIL_CAUGHT_UP → prepare + activate the source fence, converge again,
     *     then atomically cut over Catalog to the next routing generation.
     *   DUAL_WRITE_OPEN → verify the fenced tail remains converged, then drop
     *     every migrated source row.
     *   CATALOG_CUT_OVER → remove source triggers, mark fence cleanup, and
     *     complete the exact Catalog topology operation.
     *
     * See `spec/Resharder.tla` for the protocol invariants this driver is the
     * executable counterpart to.
     */
    async runSplit(migId: string): Promise<{ phase: ResharderPhase }> {
        if (this.activeRecoveries.has(migId) || this.activeAborts.has(migId)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} cannot run while recovery or abort is active`,
            });
        }
        const active = this.activeRuns.get(migId);
        if (active) return active;
        const operation = Promise.resolve().then(() => this.runSplitDriver(migId));
        this.activeRuns.set(migId, operation);
        try {
            return await operation;
        } finally {
            if (this.activeRuns.get(migId) === operation) this.activeRuns.delete(migId);
        }
    }

    private async runSplitDriver(migId: string): Promise<{ phase: ResharderPhase }> {
        const fetchState = (): MigrationState | null => this.readMigration(migId);
        const st = fetchState();
        if (!st) throw new CdbError({ code: "CDB_INVARIANT", message: `unknown migId=${migId}` });

        if (st.phase === RESHARDER_PHASE.ABORTING) {
            await this.runAbortCleanup(migId, st);
            return { phase: (this.readMigration(migId)?.phase ?? RESHARDER_PHASE.ABORTED) as ResharderPhase };
        }
        if (st.phase === RESHARDER_PHASE.ABORTED) return { phase: RESHARDER_PHASE.ABORTED };

        const ns = this.env.CDB_SHARD;
        const cat = this.env.CDB_CATALOG;
        if (!ns || !cat) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: "Resharder requires CDB_SHARD + CDB_CATALOG service bindings",
            });
        }
        const source = ns.get(ns.idFromName(st.src)) as unknown as CdbReshardRpc;
        const dest = ns.get(ns.idFromName(st.dst)) as unknown as CdbReshardRpc;
        const catalog = cat.get(cat.idFromName("global")) as unknown as CatalogReshardRpc;
        const range = { lo: st.lo, hi: st.hi };
        await this.assertSideStateProtocolCapabilities(source, dest);

        if (st.phase >= RESHARDER_PHASE.INIT && st.phase <= RESHARDER_PHASE.CATALOG_CUT_OVER) {
            const ownership = await dest.prepareReshardDestOwnership({
                migId,
                rangeLo: range.lo,
                rangeHi: range.hi,
                destinationGeneration: st.epochAtStart + 1,
                recoveryGeneration: st.recoveryGeneration,
            });
            this.assertCurrentPhase(migId, st.phase as ResharderPhase);
            if (st.phase >= RESHARDER_PHASE.DUAL_WRITE_OPEN && !ownership.serving) {
                const cutover = await catalog.cutover({
                    migId,
                    lo: range.lo,
                    hi: range.hi,
                    fromShard: st.src,
                    toShard: st.dst,
                    startEpoch: st.epochAtStart,
                    recoveryGeneration: st.recoveryGeneration,
                });
                if (cutover.newEpoch !== st.epochAtStart + 1) {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: `migId=${migId} Catalog cutover returned an unexpected routing generation`,
                    });
                }
                if (this.fileCursors().read(migId).enabled) {
                    await dest.activateReshardFileDest(this.splitIdentityRequest(migId, st));
                }
                await dest.activateReshardDestServing({
                    ...this.splitIdentityRequest(migId, st),
                    destinationGeneration: st.epochAtStart + 1,
                });
                this.assertCurrentPhase(migId, st.phase as ResharderPhase);
            }
        }

        const ensureKey = `${migId}:${st.phase}`;
        if (
            st.phase >= RESHARDER_PHASE.TAIL_CAPTURE_ENABLED &&
            st.phase <= RESHARDER_PHASE.BULK_COPY_DONE &&
            !this.ensuredSourcePhases.has(ensureKey)
        ) {
            await source.beginReshardSource({
                migId,
                rangeLo: range.lo,
                rangeHi: range.hi,
                schemaVersion: st.schemaVersion,
                schemaEpoch: st.schemaEpoch,
                schemaDigest: st.schemaDigest,
                tables: st.tables,
                recoveryGeneration: st.recoveryGeneration,
            });
            if (this.fileCursors().read(migId).enabled) {
                await source.beginReshardFileSource(this.splitIdentityRequest(migId, st));
            }
            if (this.vectorCursors().read(migId).enabled && typeof source.beginReshardVectorSource === "function") {
                await source.beginReshardVectorSource(this.splitIdentityRequest(migId, st));
            }
            this.ensuredSourcePhases.add(ensureKey);
        }

        while (st.phase >= 0 && st.phase < RESHARDER_PHASE.SOURCE_DRAINED) {
            switch (st.phase as ResharderPhase) {
                case RESHARDER_PHASE.INIT: {
                    const filePrepared = await this.runFilePreparation(migId, source, st);
                    if (!filePrepared) break;
                    await dest.provisionFreshReshardDestination({
                        recoveryGeneration: st.recoveryGeneration,
                        migrationId: `reshard-dest:${migId}`,
                        targetVersion: st.schemaVersion,
                        targetEpoch: st.schemaEpoch,
                        targetDigest: st.schemaDigest,
                    });
                    this.assertCurrentPhase(migId, RESHARDER_PHASE.INIT);
                    const fileCursor = this.fileCursors().read(migId);
                    if (fileCursor.enabled) {
                        const begun = await dest.beginReshardFileDest(this.splitIdentityRequest(migId, st));
                        if (!begun.enabled) {
                            throw new CdbError({
                                code: "CDB_INVARIANT",
                                message: `migId=${migId} destination lost its file-resource contract`,
                            });
                        }
                    }
                    this.assertCurrentPhase(migId, RESHARDER_PHASE.INIT);
                    await dest.beginReshardDest({
                        migId,
                        rangeLo: range.lo,
                        rangeHi: range.hi,
                        schemaVersion: st.schemaVersion,
                        schemaEpoch: st.schemaEpoch,
                        schemaDigest: st.schemaDigest,
                        tables: st.tables,
                        destinationGeneration: st.epochAtStart + 1,
                        recoveryGeneration: st.recoveryGeneration,
                    });
                    await source.beginReshardSource({
                        migId,
                        rangeLo: range.lo,
                        rangeHi: range.hi,
                        schemaVersion: st.schemaVersion,
                        schemaEpoch: st.schemaEpoch,
                        schemaDigest: st.schemaDigest,
                        tables: st.tables,
                        recoveryGeneration: st.recoveryGeneration,
                    });
                    this.assertCurrentPhase(migId, RESHARDER_PHASE.INIT);
                    if (fileCursor.enabled) {
                        const begun = await source.beginReshardFileSource(this.splitIdentityRequest(migId, st));
                        if (!begun.enabled) {
                            throw new CdbError({
                                code: "CDB_INVARIANT",
                                message: `migId=${migId} source lost its file-resource contract`,
                            });
                        }
                    }
                    await this.beginVectorMovement(migId, source, dest, st);
                    await this.advance(migId, RESHARDER_PHASE.INIT);
                    break;
                }
                case RESHARDER_PHASE.TAIL_CAPTURE_ENABLED: {
                    const vectorsCopied = await this.runVectorCopyStep(migId, source, dest, st);
                    try {
                        await this.runTailStage(migId, source, dest, range, st);
                    } catch (error) {
                        const code =
                            error instanceof CdbError
                                ? error.code
                                : typeof error === "object" && error !== null && "code" in error
                                  ? (error as { readonly code?: unknown }).code
                                  : undefined;
                        if (code !== "CDB_RATE_LIMITED") throw error;
                        if (!vectorsCopied) break;
                    }
                    if (!vectorsCopied) break;
                    const filesCopied = await this.runFileCopyStep(migId, source, dest, st);
                    if (!filesCopied) break;
                    const done = await this.runBulkCopyStep(migId, source, dest, range, st);
                    if (done) await this.advance(migId, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED);
                    break;
                }
                case RESHARDER_PHASE.BULK_COPY_DONE: {
                    const converged = await this.runTailReplay(migId, source, dest, range, st);
                    if (!converged) break;
                    const oplogConverged = await this.runOpLogReplay(
                        migId,
                        source,
                        dest,
                        range,
                        RESHARDER_PHASE.BULK_COPY_DONE,
                        st.recoveryGeneration
                    );
                    if (!oplogConverged) break;
                    await this.advance(migId, RESHARDER_PHASE.BULK_COPY_DONE);
                    break;
                }
                case RESHARDER_PHASE.TAIL_CAUGHT_UP: {
                    const fence = this.routingFenceRequest(migId, st);
                    await source.prepareRoutingFence(fence);
                    const fileCursor = this.fileCursors().read(migId);
                    if (fileCursor.enabled) {
                        await catalog.beginOrganizationDeletionBarrier({
                            migId,
                            rangeLo: range.lo,
                            rangeHi: range.hi,
                            recoveryGeneration: st.recoveryGeneration,
                        });
                        const barrier = await catalog.organizationDeletionBarrierStatus({
                            migId,
                            rangeLo: range.lo,
                            rangeHi: range.hi,
                            recoveryGeneration: st.recoveryGeneration,
                        });
                        if (!barrier.olderDeletionsComplete) break;
                    }
                    await source.activateRoutingFence(fence);
                    if (fileCursor.enabled) {
                        await source.fenceReshardFileSource(this.splitIdentityRequest(migId, st));
                    }
                    const converged = await this.runTailReplay(migId, source, dest, range, st);
                    if (!converged) break;
                    const oplogConverged = await this.runOpLogReplay(
                        migId,
                        source,
                        dest,
                        range,
                        RESHARDER_PHASE.TAIL_CAUGHT_UP,
                        st.recoveryGeneration
                    );
                    if (!oplogConverged) break;
                    const vectorBeforeFreeze = this.vectorCursors().read(migId);
                    if (vectorBeforeFreeze.enabled && !vectorBeforeFreeze.sourceFrozen) {
                        if (typeof source.stopReshardVectorSource !== "function") {
                            throw new CdbError({
                                code: "CDB_UNSUPPORTED_FEATURE",
                                message: "vector source freeze RPC is unavailable",
                            });
                        }
                        await source.stopReshardCapture(this.splitIdentityRequest(migId, st));
                        const stopped = await source.stopReshardVectorSource(this.splitIdentityRequest(migId, st));
                        if (!stopped.enabled) {
                            throw new CdbError({
                                code: "CDB_RESHARD_PHASE_MISMATCH",
                                message: `migId=${migId} vector source disappeared while freezing`,
                            });
                        }
                        this.vectorCursors().persistSourceFrozen(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP);
                    }
                    const vectorsValid = await this.runVectorParityStep(migId, source, dest, st);
                    if (!vectorsValid) break;
                    const vectorAfterParity = this.vectorCursors().read(migId);
                    if (vectorAfterParity.enabled) {
                        if (typeof dest.finalizeReshardVectorDest !== "function") {
                            throw new CdbError({
                                code: "CDB_UNSUPPORTED_FEATURE",
                                message: "vector destination finalization RPC is unavailable",
                            });
                        }
                        const finalized = await dest.finalizeReshardVectorDest({
                            ...this.splitIdentityRequest(migId, st),
                            throughLsn: st.tailCursor,
                        });
                        if (!finalized.enabled || !finalized.finalized) {
                            throw new CdbError({
                                code: "CDB_RESHARD_PHASE_MISMATCH",
                                message: `migId=${migId} vector destination did not finalize`,
                            });
                        }
                    }
                    const filesValid = await this.runFileValidationStep(migId, source, dest, st);
                    if (!filesValid) break;
                    if (fileCursor.enabled) {
                        await dest.prepareReshardFileDestAttachments(this.splitIdentityRequest(migId, st));
                    }
                    const cutover = await catalog.cutover({
                        migId,
                        lo: range.lo,
                        hi: range.hi,
                        fromShard: st.src,
                        toShard: st.dst,
                        startEpoch: st.epochAtStart,
                        recoveryGeneration: st.recoveryGeneration,
                    });
                    if (cutover.newEpoch !== fence.destinationGeneration) {
                        throw new CdbError({
                            code: "CDB_STALE_EPOCH",
                            message: `migId=${migId} Catalog cutover returned an unexpected routing generation`,
                            hint: "inspect the Catalog topology operation before retrying",
                        });
                    }
                    if (fileCursor.enabled) {
                        await dest.activateReshardFileDest(this.splitIdentityRequest(migId, st));
                    }
                    await dest.activateReshardDestServing({
                        ...this.splitIdentityRequest(migId, st),
                        destinationGeneration: fence.destinationGeneration,
                    });
                    this.assertCurrentPhase(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP);
                    await this.advance(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP);
                    break;
                }
                case RESHARDER_PHASE.DUAL_WRITE_OPEN: {
                    if (!st.legacyCutoverRecovered) {
                        const converged = await this.runTailReplay(migId, source, dest, range, st);
                        if (!converged) break;
                        const oplogConverged = await this.runOpLogReplay(
                            migId,
                            source,
                            dest,
                            range,
                            RESHARDER_PHASE.DUAL_WRITE_OPEN,
                            st.recoveryGeneration
                        );
                        if (!oplogConverged) break;
                    }
                    await source.stopReshardCapture(this.splitIdentityRequest(migId, st));
                    if (this.fileCursors().read(migId).enabled) {
                        await source.stopReshardFileSource(this.splitIdentityRequest(migId, st));
                    }
                    const vectorForDrain = this.vectorCursors().read(migId);
                    if (vectorForDrain.enabled && !vectorForDrain.sourcePrepareDone) {
                        if (typeof source.prepareReshardVectorSourceDrain !== "function") {
                            throw new CdbError({
                                code: "CDB_UNSUPPORTED_FEATURE",
                                message: "vector source drain preparation RPC is unavailable",
                            });
                        }
                        const prepared = await source.prepareReshardVectorSourceDrain({
                            ...this.splitIdentityRequest(migId, st),
                            cursor: vectorForDrain.sourcePrepareCursor,
                        });
                        if (!prepared.enabled || !prepared.result) {
                            throw new CdbError({
                                code: "CDB_RESHARD_PHASE_MISMATCH",
                                message: `migId=${migId} vector source disappeared during drain preparation`,
                            });
                        }
                        this.vectorCursors().persistSourcePrepare(
                            migId,
                            RESHARDER_PHASE.DUAL_WRITE_OPEN,
                            prepared.result.cursor,
                            prepared.result.done
                        );
                        if (!prepared.result.done) break;
                    }
                    const { tableNames } = await source.reshardTableOrder({
                        migId,
                        recoveryGeneration: st.recoveryGeneration,
                        role: "source",
                        range,
                        tables: st.tables,
                    });
                    const tablesByName = new Map(st.tables.map(table => [table.name, table]));
                    if (
                        tableNames.length !== st.tables.length ||
                        new Set(tableNames).size !== st.tables.length ||
                        tableNames.some(name => !tablesByName.has(name))
                    ) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: "source returned an invalid reshard table order",
                        });
                    }
                    const dropOrder = [...tableNames].reverse();
                    if (st.bulkTableIndex >= dropOrder.length) {
                        const vectorAfterDomainDrain = this.vectorCursors().read(migId);
                        if (vectorAfterDomainDrain.enabled && !vectorAfterDomainDrain.sourceDeleteDone) {
                            if (typeof source.drainReshardVectorSource !== "function") {
                                throw new CdbError({
                                    code: "CDB_UNSUPPORTED_FEATURE",
                                    message: "vector source drain RPC is unavailable",
                                });
                            }
                            const drained = await source.drainReshardVectorSource({
                                ...this.splitIdentityRequest(migId, st),
                                cursor: vectorAfterDomainDrain.sourceDeleteCursor,
                            });
                            if (!drained.enabled || !drained.result) {
                                throw new CdbError({
                                    code: "CDB_RESHARD_PHASE_MISMATCH",
                                    message: `migId=${migId} vector source disappeared during drain`,
                                });
                            }
                            this.vectorCursors().persistSourceDelete(
                                migId,
                                RESHARDER_PHASE.DUAL_WRITE_OPEN,
                                drained.result.cursor,
                                drained.result.done
                            );
                            if (!drained.result.done) break;
                        }
                        const filesDrained = await this.runFileDrainStep(migId, source, st);
                        if (!filesDrained) break;
                        await this.advance(migId, RESHARDER_PHASE.DUAL_WRITE_OPEN);
                        break;
                    }
                    const t = tablesByName.get(dropOrder[st.bulkTableIndex] as string) as TableSpec;
                    const dropped = await source.dropMigratedRange({
                        migId,
                        recoveryGeneration: st.recoveryGeneration,
                        table: t,
                        range,
                        batchSize: DROP_BATCH,
                    });
                    if (dropped.done) {
                        this.persistWorkCursor(
                            migId,
                            RESHARDER_PHASE.DUAL_WRITE_OPEN,
                            st.workTurn,
                            st.bulkTableIndex + 1
                        );
                    }
                    break;
                }
                case RESHARDER_PHASE.CATALOG_CUT_OVER: {
                    const identity = this.splitIdentityRequest(migId, st);
                    const fileCursor = this.fileCursors().read(migId);
                    if (fileCursor.enabled && !fileCursor.sourceFinishDone) {
                        const finished = await source.finishReshardFiles({
                            ...identity,
                            role: "source",
                            limit: CDB_FILE_RESHARD_PAGE_SIZE,
                        });
                        if (!finished.done) break;
                        this.fileCursors().persistFinish(migId, RESHARDER_PHASE.CATALOG_CUT_OVER, "source");
                    }
                    const afterSourceFinish = this.fileCursors().read(migId);
                    if (afterSourceFinish.enabled && !afterSourceFinish.destFinishDone) {
                        const finished = await dest.finishReshardFiles({
                            ...identity,
                            role: "dest",
                            limit: CDB_FILE_RESHARD_PAGE_SIZE,
                        });
                        if (!finished.done) break;
                        this.fileCursors().persistFinish(migId, RESHARDER_PHASE.CATALOG_CUT_OVER, "dest");
                    }
                    await source.finishReshardSource(identity);
                    const vectorBeforeFinish = this.vectorCursors().read(migId);
                    if (vectorBeforeFinish.enabled && !vectorBeforeFinish.sourceFinishDone) {
                        if (typeof source.finishReshardVectors !== "function") {
                            throw new CdbError({
                                code: "CDB_UNSUPPORTED_FEATURE",
                                message: "vector source finish RPC is unavailable",
                            });
                        }
                        const finished = await source.finishReshardVectors(identity);
                        if (!finished.enabled || !finished.done) break;
                        this.vectorCursors().persistFinish(migId, RESHARDER_PHASE.CATALOG_CUT_OVER, "source");
                    }
                    const vectorAfterSourceFinish = this.vectorCursors().read(migId);
                    if (vectorAfterSourceFinish.enabled && !vectorAfterSourceFinish.destFinishDone) {
                        if (typeof dest.finishReshardVectorDest !== "function") {
                            throw new CdbError({
                                code: "CDB_UNSUPPORTED_FEATURE",
                                message: "vector destination finish RPC is unavailable",
                            });
                        }
                        const finished = await dest.finishReshardVectorDest(identity);
                        if (!finished.enabled || !finished.done) break;
                        this.vectorCursors().persistFinish(migId, RESHARDER_PHASE.CATALOG_CUT_OVER, "dest");
                    }
                    await source.completeRoutingFenceCleanup(this.routingFenceRequest(migId, st));
                    await dest.finishReshardDest(identity);
                    await catalog.completeTopologyOperation(this.topologyRequest(migId, st));
                    await this.advance(migId, RESHARDER_PHASE.CATALOG_CUT_OVER);
                    break;
                }
            }
            const next = fetchState();
            return { phase: (next?.phase ?? RESHARDER_PHASE.ABORTED) as ResharderPhase };
        }
        return { phase: (st.phase as ResharderPhase) ?? RESHARDER_PHASE.ABORTED };
    }

    private async runAbortCleanup(migId: string, migration: MigrationState): Promise<void> {
        if (migration.phase !== RESHARDER_PHASE.ABORTING) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} is not in its durable abort phase`,
            });
        }
        const namespace = this.env.CDB_SHARD;
        if (!namespace) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "Resharder requires CDB_SHARD service binding" });
        }
        const source = namespace.get(namespace.idFromName(migration.src)) as unknown as CdbReshardRpc;
        const dest = namespace.get(namespace.idFromName(migration.dst)) as unknown as CdbReshardRpc;
        const identity = this.splitIdentityRequest(migId, migration);
        const fileCursor = this.fileCursors().read(migId);

        // Catalog must reject cutover before any source fence is reopened or capture is removed.
        await this.catalog().abortTopologyOperation(this.topologyRequest(migId, migration));
        if (typeof source.cancelRoutingFenceBeforeCutover === "function") {
            await source.cancelRoutingFenceBeforeCutover(this.routingFenceRequest(migId, migration));
        }

        // Source capture must stop before any copied destination rows are removed.
        if (fileCursor.enabled) await source.abortReshardFiles({ ...identity, role: "source" });
        await source.abortReshardSource(identity);
        const vectorAtAbort = this.vectorCursors().read(migId);
        if (vectorAtAbort.enabled !== false) {
            if (typeof source.abortReshardVectors !== "function") {
                throw new CdbError({
                    code: "CDB_UNSUPPORTED_FEATURE",
                    message: "vector source abort probe RPC is unavailable",
                });
            }
            const sourceVector = await source.abortReshardVectors(identity);
            if (sourceVector.enabled && !sourceVector.done) return;
        }
        // Fence delayed destination applies before snapshotting any cleanup watermark.
        await dest.beginReshardDestAbort({
            ...identity,
            destinationGeneration: migration.epochAtStart + 1,
        });
        if (fileCursor.enabled && !fileCursor.abortDone) {
            const files = await dest.abortReshardFiles({
                ...identity,
                role: "dest",
                afterKind: fileCursor.abortKind,
                afterId: fileCursor.abortAfterId,
                limit: CDB_FILE_RESHARD_PAGE_SIZE,
            });
            this.fileCursors().persistAbort(
                migId,
                RESHARDER_PHASE.ABORTING,
                files.afterKind,
                files.afterId,
                files.done
            );
            if (!files.done) return;
        }
        const batch = await dest.abortReshardDestBatch({ ...identity, batchSize: DROP_BATCH });
        if (!batch.done) return;
        const vectorAfterGenericAbort = this.vectorCursors().read(migId);
        if (vectorAfterGenericAbort.enabled !== false && !vectorAfterGenericAbort.abortDone) {
            if (typeof dest.abortReshardVectorDest !== "function") {
                throw new CdbError({
                    code: "CDB_UNSUPPORTED_FEATURE",
                    message: "vector destination abort probe RPC is unavailable",
                });
            }
            const vectorAbort = await dest.abortReshardVectorDest({
                ...identity,
                destinationGeneration: migration.epochAtStart + 1,
                limit: 500,
            });
            const next = vectorAbort.result?.next ?? vectorAfterGenericAbort.abortCursor;
            const done = !vectorAbort.enabled || vectorAbort.result?.done === true;
            this.vectorCursors().persistAbort(migId, RESHARDER_PHASE.ABORTING, next, done);
            if (!done) return;
        }
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            sql.exec(
                "UPDATE migration_state SET phase = ?, updated_at = ? WHERE mig_id = ? AND phase = ?",
                RESHARDER_PHASE.ABORTED,
                Date.now(),
                migId,
                RESHARDER_PHASE.ABORTING
            );
            if (sql.changes() !== 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `migId=${migId} changed while completing abort`,
                });
            }
        });
    }

    private async runFilePreparation(migId: string, source: CdbReshardRpc, st: MigrationState): Promise<boolean> {
        const file = this.fileCursors().read(migId);
        if (file.prepareDone) return true;
        if (typeof source.prepareReshardFileSource !== "function") {
            this.fileCursors().persistPreparation(migId, RESHARDER_PHASE.INIT, false, {
                kind: file.prepareKind,
                afterId: file.prepareAfterId,
                done: true,
            });
            return true;
        }
        const prepared = await source.prepareReshardFileSource({
            ...this.splitIdentityRequest(migId, st),
            afterKind: file.prepareKind,
            afterId: file.prepareAfterId,
            limit: CDB_FILE_RESHARD_PAGE_SIZE,
        });
        if (!prepared.enabled) {
            this.fileCursors().persistPreparation(migId, RESHARDER_PHASE.INIT, false, {
                kind: file.prepareKind,
                afterId: file.prepareAfterId,
                done: true,
            });
            return true;
        }
        this.fileCursors().persistPreparation(migId, RESHARDER_PHASE.INIT, true, prepared.cursor);
        return prepared.cursor.done;
    }

    private async beginVectorMovement(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        st: MigrationState
    ): Promise<void> {
        const current = this.vectorCursors().read(migId);
        if (current.enabled !== null) return;
        if (typeof source.beginReshardVectorSource !== "function") {
            this.vectorCursors().persistBegin(migId, RESHARDER_PHASE.INIT, false, null, null);
            return;
        }
        const identity = this.splitIdentityRequest(migId, st);
        const sourceBegin = await source.beginReshardVectorSource(identity);
        if (!sourceBegin.enabled) {
            if (sourceBegin.snapshot !== null) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: `migId=${migId} disabled vector source has state`,
                });
            }
            this.vectorCursors().persistBegin(migId, RESHARDER_PHASE.INIT, false, null, null);
            return;
        }
        const snapshot = sourceBegin.snapshot;
        if (!snapshot || snapshot.next.pageNumber !== 0 || snapshot.terminal) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `migId=${migId} vector source begin is not pristine`,
            });
        }
        if (typeof dest.beginReshardVectorDest !== "function") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `migId=${migId} destination cannot accept vector movement`,
            });
        }
        const destinationBegin = await dest.beginReshardVectorDest({
            ...identity,
            throughHeadSeq: snapshot.throughHeadSeq,
        });
        if (!destinationBegin.enabled || stableJson(destinationBegin.cursor) !== stableJson(snapshot.next.cursor)) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `migId=${migId} vector destination begin changed the source cursor`,
            });
        }
        this.vectorCursors().persistBegin(
            migId,
            RESHARDER_PHASE.INIT,
            true,
            snapshot.throughHeadSeq,
            snapshot.next.cursor
        );
    }

    private async assertSideStateProtocolCapabilities(source: CdbReshardRpc, dest: CdbReshardRpc): Promise<void> {
        if (
            typeof source.reshardSideStateProtocolCapabilitiesV2 !== "function" ||
            typeof dest.reshardSideStateProtocolCapabilitiesV2 !== "function"
        ) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message:
                    "reshard peers must both support vector snapshot v2 and file tombstone v2 before movement begins",
            });
        }
        const [sourceCapabilities, destinationCapabilities] = await Promise.all([
            source.reshardSideStateProtocolCapabilitiesV2(),
            dest.reshardSideStateProtocolCapabilitiesV2(),
        ]);
        if (
            sourceCapabilities.vectorSnapshot !== "v2" ||
            sourceCapabilities.fileTombstones !== "v2" ||
            destinationCapabilities.vectorSnapshot !== "v2" ||
            destinationCapabilities.fileTombstones !== "v2"
        ) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "reshard peers reported incompatible side-state protocols",
            });
        }
    }

    private async runVectorCopyStep(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        st: MigrationState
    ): Promise<boolean> {
        const vector = this.vectorCursors().read(migId);
        if (vector.enabled === null) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} vector begin is incomplete` });
        }
        if (!vector.enabled || vector.copyDone) return true;
        if (!vector.copyCursor) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} vector copy cursor is missing` });
        }
        if (
            typeof source.readReshardVectorSnapshot !== "function" ||
            typeof dest.applyReshardVectorSnapshot !== "function"
        ) {
            throw new CdbError({ code: "CDB_UNSUPPORTED_FEATURE", message: "vector snapshot RPC is unavailable" });
        }
        const identity = this.splitIdentityRequest(migId, st);
        const page = await source.readReshardVectorSnapshot({
            ...identity,
            pageNumber: vector.copyPageNumber,
            cursor: vector.copyCursor,
        });
        if (!page.enabled || !page.page) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} vector source disappeared` });
        }
        const applied = await dest.applyReshardVectorSnapshot({
            ...identity,
            ...page.page,
            cursor: vector.copyCursor,
        });
        if (!applied.enabled || !applied.result) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} vector destination disappeared` });
        }
        this.vectorCursors().persistCopy(
            migId,
            RESHARDER_PHASE.TAIL_CAPTURE_ENABLED,
            vector.copyPageNumber,
            vector.copyCursor,
            applied.result.next,
            applied.result.done
        );
        return applied.result.done;
    }

    private async runVectorParityStep(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        st: MigrationState
    ): Promise<boolean> {
        const vector = this.vectorCursors().read(migId);
        if (!vector.enabled || vector.parityDone) return true;
        if (
            typeof source.readReshardVectorParityPage !== "function" ||
            typeof dest.verifyReshardVectorParity !== "function"
        ) {
            throw new CdbError({ code: "CDB_UNSUPPORTED_FEATURE", message: "vector parity RPC is unavailable" });
        }
        const identity = this.splitIdentityRequest(migId, st);
        const sourcePage = await source.readReshardVectorParityPage({ ...identity, cursor: vector.parityCursor });
        if (!sourcePage.enabled || sourcePage.encodedPage === null || sourcePage.throughLsn !== st.tailCursor) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} vector parity source watermark is not converged`,
            });
        }
        const verified = await dest.verifyReshardVectorParity({
            ...identity,
            pageNumber: vector.parityPageNumber,
            cursor: vector.parityCursor,
            encodedSourcePage: sourcePage.encodedPage,
            throughLsn: sourcePage.throughLsn,
        });
        if (!verified.enabled || !verified.result) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `migId=${migId} vector parity destination disappeared`,
            });
        }
        this.vectorCursors().persistParity(
            migId,
            RESHARDER_PHASE.TAIL_CAUGHT_UP,
            vector.parityPageNumber,
            vector.parityCursor,
            verified.result.next,
            verified.result.done
        );
        return verified.result.done;
    }

    private async runFileCopyStep(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        st: MigrationState
    ): Promise<boolean> {
        const file = this.fileCursors().read(migId);
        if (file.enabled === null) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} file preparation is incomplete` });
        }
        if (!file.enabled || file.copyDone) return true;
        const identity = this.splitIdentityRequest(migId, st);
        if (file.copyKind === "organization_tombstone") {
            const page = await source.readReshardFileTombstonesV2({
                ...identity,
                afterPlacement: file.copyAfterPlacement,
                afterOrganizationId: file.copyAfterId,
                limit: CDB_FILE_RESHARD_PAGE_SIZE,
            });
            if (page.rows.length > 0) {
                await dest.applyReshardFileTombstonesV2({ ...identity, rows: page.rows, throughLsn: page.throughLsn });
            }
            if (page.done) {
                this.fileCursors().persistCopy(migId, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED, "file", -1, "", false);
            } else {
                this.fileCursors().persistCopy(
                    migId,
                    RESHARDER_PHASE.TAIL_CAPTURE_ENABLED,
                    "organization_tombstone",
                    page.afterPlacement,
                    page.afterId,
                    false
                );
            }
            return false;
        }
        const page = await source.readReshardFileSnapshot({
            ...identity,
            afterPlacement: file.copyAfterPlacement,
            afterFileId: file.copyAfterId,
            limit: CDB_FILE_RESHARD_PAGE_SIZE,
        });
        if (page.rows.length > 0) {
            await dest.applyReshardFileSnapshot({ ...identity, rows: page.rows, throughLsn: page.throughLsn });
        }
        this.fileCursors().persistCopy(
            migId,
            RESHARDER_PHASE.TAIL_CAPTURE_ENABLED,
            "file",
            page.afterPlacement,
            page.afterId,
            page.done
        );
        return page.done;
    }

    private async runFileValidationStep(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        st: MigrationState
    ): Promise<boolean> {
        const file = this.fileCursors().read(migId);
        if (!file.enabled || file.validateDone) return true;
        const input = {
            ...this.splitIdentityRequest(migId, st),
            cursor: file.validateCursor,
            limit: CDB_FILE_RESHARD_PAGE_SIZE,
        };
        const sourcePage = await source.readReshardFileParityPage({ ...input, role: "source" });
        const destinationPage = await dest.readReshardFileParityPage({ ...input, role: "dest" });
        if (stableJson(sourcePage) !== stableJson(destinationPage)) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} source and destination file metadata do not match`,
            });
        }
        this.fileCursors().persistValidation(migId, RESHARDER_PHASE.TAIL_CAUGHT_UP, sourcePage.cursor, sourcePage.done);
        return sourcePage.done;
    }

    private async runFileDrainStep(migId: string, source: CdbReshardRpc, st: MigrationState): Promise<boolean> {
        const file = this.fileCursors().read(migId);
        if (!file.enabled || file.drainDone) return true;
        const drained = await source.drainReshardFiles({
            ...this.splitIdentityRequest(migId, st),
            cursor: file.drainCursor,
            limit: CDB_FILE_RESHARD_PAGE_SIZE,
        });
        this.fileCursors().persistDrain(migId, RESHARDER_PHASE.DUAL_WRITE_OPEN, drained.cursor, drained.done);
        return drained.done;
    }

    /**
     * Paginated bulk copy. Per-table cursor (rowid) lives in
     * `migration_state.bulk_cursor` so a crash mid-copy resumes from the next
     * unscanned rowid. A table is "done" when the source returns fewer rows
     * than `BULK_BATCH`; the loop exits when every table is done.
     */
    private async runBulkCopyStep(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        range: { lo: number; hi: number },
        st: MigrationState
    ): Promise<boolean> {
        const cursor = { ...st.bulkCursor };
        if (st.tailCursor > 0) {
            await source.ackTail({ migId, recoveryGeneration: st.recoveryGeneration, throughLsn: st.tailCursor });
        }
        const oplogCursor = this.readOpLogCursor(migId);
        if (oplogCursor > 0) {
            await source.ackSplitOpLog({ migId, recoveryGeneration: st.recoveryGeneration, throughLsn: oplogCursor });
        }
        if (st.workTurn === 0) {
            try {
                await this.runTailStage(migId, source, dest, range, st);
            } catch (error) {
                const code =
                    error instanceof CdbError
                        ? error.code
                        : typeof error === "object" && error !== null && "code" in error
                          ? (error as { code?: unknown }).code
                          : undefined;
                if (code !== "CDB_RATE_LIMITED") throw error;
            }
            this.persistWorkCursor(migId, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED, 1);
            return false;
        }
        if (st.workTurn === 1) {
            await this.runOpLogReplay(
                migId,
                source,
                dest,
                range,
                RESHARDER_PHASE.TAIL_CAPTURE_ENABLED,
                st.recoveryGeneration
            );
            this.persistWorkCursor(migId, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED, 2);
            return false;
        }
        const tablesByName = new Map(st.tables.map(table => [table.name, table]));
        const { tableNames } = await dest.reshardTableOrder({
            migId,
            recoveryGeneration: st.recoveryGeneration,
            role: "dest",
            range,
            tables: st.tables,
        });
        if (
            tableNames.length !== st.tables.length ||
            new Set(tableNames).size !== st.tables.length ||
            tableNames.some(name => !tablesByName.has(name))
        ) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: "destination returned an invalid reshard table order",
            });
        }
        if (st.bulkTableIndex >= tableNames.length) {
            if (st.workTurn !== 3) {
                await dest.closeReshardBulkDest(this.splitIdentityRequest(migId, st));
                this.persistWorkCursor(migId, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED, 3);
                return false;
            }
            const batch = await dest.readStagedTailBatch({
                migId,
                recoveryGeneration: st.recoveryGeneration,
                limit: TAIL_BATCH,
            });
            if (batch.transactions.length > 0) {
                const applied = await dest.applyTailBatch({
                    migId,
                    recoveryGeneration: st.recoveryGeneration,
                    tables: st.tables,
                    range,
                    transactions: batch.transactions,
                });
                await dest.ackStagedTail({
                    migId,
                    recoveryGeneration: st.recoveryGeneration,
                    throughLsn: applied.lastLsn,
                });
                return false;
            }
            await dest.closeTailStaging(this.splitIdentityRequest(migId, st));
            return true;
        }
        const table = tablesByName.get(tableNames[st.bulkTableIndex] as string) as TableSpec;
        const after = cursor[table.name] ?? 0;
        const batch = await source.bulkCopyBatch({
            migId,
            recoveryGeneration: st.recoveryGeneration,
            table,
            range,
            afterRowid: after,
            limit: BULK_BATCH,
        });
        if (batch.rows.length > 0) {
            await dest.applyBulkBatch({
                migId,
                recoveryGeneration: st.recoveryGeneration,
                table,
                range,
                rows: batch.rows,
            });
        }
        cursor[table.name] = batch.lastRowid;
        this.persistBulkCursor(migId, cursor, RESHARDER_PHASE.TAIL_CAPTURE_ENABLED);
        this.persistWorkCursor(
            migId,
            RESHARDER_PHASE.TAIL_CAPTURE_ENABLED,
            0,
            st.bulkTableIndex + (batch.done ? 1 : 0)
        );
        return false;
    }

    private async runTailStage(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        range: { readonly lo: number; readonly hi: number },
        st: MigrationState
    ): Promise<void> {
        const boundTableNames = new Set(st.tables.map(table => table.name));
        let cursor = st.tailCursor;
        if (cursor > 0) {
            await source.ackTail({ migId, recoveryGeneration: st.recoveryGeneration, throughLsn: cursor });
        }
        const batch = await source.readTailBatch({
            migId,
            recoveryGeneration: st.recoveryGeneration,
            afterLsn: cursor,
            limit: TAIL_BATCH,
        });
        if (batch.transactions.length === 0) return;
        for (const transaction of batch.transactions) {
            for (const entry of transaction.entries) {
                if (!isKnownReshardTailTable(entry.table_name, boundTableNames)) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: `migId=${migId} source tail contains unbound table ${entry.table_name}`,
                    });
                }
            }
        }
        const staged = await dest.stageTailBatch({
            migId,
            recoveryGeneration: st.recoveryGeneration,
            tables: st.tables,
            range,
            transactions: batch.transactions,
        });
        if (staged.lastLsn !== batch.lastLsn) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} staged tail cursor mismatch` });
        }
        cursor = batch.lastLsn;
        this.persistTailCursor(migId, cursor, st.phase as ResharderPhase);
    }

    /**
     * Tail-replay loop. Pulls `_chardb_split_log` entries from the source in
     * LSN order, applies each cross-table batch in one destination transaction, and stops when the
     * watermark stops advancing across `TAIL_CONVERGENCE_ITERATIONS` reads.
     * The cursor is persisted after every round so a crash never re-applies
     * an entry that was already accepted on the destination.
     */
    private async runTailReplay(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        range: { lo: number; hi: number },
        st: MigrationState
    ): Promise<boolean> {
        const boundTableNames = new Set(st.tables.map(table => table.name));
        let cursor = st.tailCursor;
        if (cursor > 0) {
            await source.ackTail({ migId, recoveryGeneration: st.recoveryGeneration, throughLsn: cursor });
        }
        for (let i = 0; i < 2; i++) {
            const batch = await source.readTailBatch({
                migId,
                recoveryGeneration: st.recoveryGeneration,
                afterLsn: cursor,
                limit: TAIL_BATCH,
            });
            if (batch.transactions.length === 0) {
                continue;
            }
            for (const transaction of batch.transactions) {
                for (const entry of transaction.entries) {
                    if (!isKnownReshardTailTable(entry.table_name, boundTableNames)) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: `migId=${migId} source tail contains unbound table ${entry.table_name}`,
                        });
                    }
                }
            }
            const applied = await dest.applyTailBatch({
                migId,
                recoveryGeneration: st.recoveryGeneration,
                tables: st.tables,
                range,
                transactions: batch.transactions,
            });
            if (applied.lastLsn !== batch.lastLsn) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: `migId=${migId} destination tail cursor did not accept the full source batch`,
                });
            }
            cursor = batch.lastLsn;
            this.persistTailCursor(migId, cursor, st.phase as ResharderPhase);
            return false;
        }
        return true;
    }

    /**
     * Copy captured mutation replay envelopes before Catalog can publish the
     * destination. Destination apply and its cursor commit together. The
     * Resharder cursor advances only after that RPC returns, so response loss
     * repeats an already committed batch and exercises idempotent apply.
     */
    private async runOpLogReplay(
        migId: string,
        source: CdbReshardRpc,
        dest: CdbReshardRpc,
        range: { readonly lo: number; readonly hi: number },
        expectedPhase: ResharderPhase,
        recoveryGeneration: number
    ): Promise<boolean> {
        let cursor = this.readOpLogCursor(migId);
        if (cursor > 0) await source.ackSplitOpLog({ migId, recoveryGeneration, throughLsn: cursor });
        for (let iteration = 0; iteration < 2; iteration++) {
            const batch = await source.readSplitOpLogBatch({
                migId,
                recoveryGeneration,
                afterLsn: cursor,
                limit: OPLOG_BATCH,
            });
            if (batch.entries.length === 0) {
                continue;
            }
            const applied = await dest.applySplitOpLogBatch({
                migId,
                recoveryGeneration,
                rangeLo: range.lo,
                rangeHi: range.hi,
                entries: batch.entries,
            });
            if (applied.lastLsn !== batch.lastLsn) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: `migId=${migId} destination split-oplog cursor did not accept the full source batch`,
                });
            }
            cursor = batch.lastLsn;
            this.persistOpLogCursor(migId, cursor, expectedPhase);
            return false;
        }
        return true;
    }

    private fileCursors(): ResharderFileCursorStore {
        return new ResharderFileCursorStore(adaptSqlStorage(this.ctx.storage.sql));
    }

    private vectorCursors(): ResharderVectorCursorStore {
        return new ResharderVectorCursorStore(adaptSqlStorage(this.ctx.storage.sql));
    }

    private readOpLogCursor(migId: string): number {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const row = sql.one<{ source_lsn: number }>(
            "SELECT source_lsn FROM migration_oplog_cursor WHERE mig_id = ?",
            migId
        );
        if (!row || !Number.isSafeInteger(row.source_lsn) || row.source_lsn < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} split-oplog cursor is missing` });
        }
        return row.source_lsn;
    }

    private persistOpLogCursor(migId: string, sourceLsn: number, expectedPhase: ResharderPhase): void {
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            const current = this.readOpLogCursor(migId);
            if (sourceLsn < current) {
                throw new CdbError({ code: "CDB_INVARIANT", message: `migId=${migId} split-oplog cursor regressed` });
            }
            sql.exec(
                `UPDATE migration_oplog_cursor SET source_lsn = ?, updated_at = ?
                 WHERE mig_id = ? AND source_lsn = ?
                   AND EXISTS (SELECT 1 FROM migration_state WHERE mig_id = ? AND phase = ?)`,
                sourceLsn,
                Date.now(),
                migId,
                current,
                migId,
                expectedPhase
            );
            if (sql.changes() !== 1) {
                this.phaseChanged(migId, expectedPhase);
            }
        });
    }

    private readMigration(migId: string): MigrationState | null {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const row = sql.one<{
            phase: number;
            src: string;
            dst: string;
            lo: number;
            hi: number;
            epoch_at_start: number;
            schema_version: number;
            schema_epoch: number;
            schema_digest: string;
            tables_json: string;
            bulk_cursor: string;
            tail_cursor: number;
            work_turn: number;
            bulk_table_index: number;
            legacy_cutover_recovered: number;
            recovery_generation: number;
        }>(
            `SELECT m.phase, m.src_shard AS src, m.dst_shard AS dst, m.range_lo AS lo, m.range_hi AS hi,
                    m.epoch_at_start, i.schema_version, i.schema_epoch, i.schema_digest,
                    m.tables_json, m.bulk_cursor, m.tail_cursor, COALESCE(w.turn, 0) AS work_turn,
                    COALESCE(w.bulk_table_index, 0) AS bulk_table_index,
                    m.legacy_cutover_recovered, m.recovery_generation
             FROM migration_state AS m
             LEFT JOIN migration_schema_identity AS i ON i.mig_id = m.mig_id
             LEFT JOIN migration_work_cursor AS w ON w.mig_id = m.mig_id
             WHERE m.mig_id = ?`,
            migId
        );
        if (!row) return null;
        if (
            !Number.isSafeInteger(row.schema_version) ||
            row.schema_version < 0 ||
            !Number.isSafeInteger(row.schema_epoch) ||
            row.schema_epoch < 1 ||
            typeof row.schema_digest !== "string" ||
            !SCHEMA_DIGEST.test(row.schema_digest)
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${migId} predates durable topology schema identity`,
            });
        }
        return {
            phase: row.phase,
            src: row.src,
            dst: row.dst,
            lo: row.lo,
            hi: row.hi,
            epochAtStart: row.epoch_at_start,
            recoveryGeneration: row.recovery_generation,
            schemaVersion: row.schema_version,
            schemaEpoch: row.schema_epoch,
            schemaDigest: row.schema_digest,
            tables: JSON.parse(row.tables_json) as TableSpec[],
            bulkCursor: JSON.parse(row.bulk_cursor) as Record<string, number>,
            tailCursor: row.tail_cursor,
            workTurn: row.work_turn,
            bulkTableIndex: row.bulk_table_index,
            legacyCutoverRecovered: row.legacy_cutover_recovered === 1,
        };
    }

    private startIdentity(
        args: StartSplitArgs,
        tablesJson: string,
        recoveryGeneration: number
    ): ResharderStartIdentity {
        return {
            migId: args.migId,
            srcShard: args.srcShard,
            dstShard: args.dstShard,
            rangeLo: args.rangeLo,
            rangeHi: args.rangeHi,
            epochAtStart: args.epochAtStart,
            recoveryGeneration,
            tablesJson,
        };
    }

    private startMigrationIdentity(
        args: StartSplitArgs
    ): Pick<MigrationState, "src" | "dst" | "lo" | "hi" | "epochAtStart" | "recoveryGeneration"> {
        return {
            src: args.srcShard,
            dst: args.dstShard,
            lo: args.rangeLo,
            hi: args.rangeHi,
            epochAtStart: args.epochAtStart,
            recoveryGeneration: new RecoveryCoordinatorStore(adaptSqlStorage(this.ctx.storage.sql)).admissionClock()
                .generation,
        };
    }

    private assertSameStartIdentity(stored: ResharderStartIdentity, requested: ResharderStartIdentity): void {
        if (stableJson(stored) === stableJson(requested)) return;
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: `migId=${requested.migId} is already bound to a different split start`,
        });
    }

    private async abortPendingStart(intent: ResharderStartIntent): Promise<void> {
        if (!intent.identity) return;
        const topology = await this.catalog().topologyOperation({
            migrationId: intent.migId,
            recoveryGeneration: intent.identity.recoveryGeneration,
        });
        if (!topology) return;
        const identity = intent.identity;
        if (
            topology.migrationId !== identity.migId ||
            topology.sourceShard !== identity.srcShard ||
            topology.destinationShard !== identity.dstShard ||
            topology.rangeLo !== identity.rangeLo ||
            topology.rangeHi !== identity.rangeHi ||
            topology.startEpoch !== identity.epochAtStart
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${intent.migId} Catalog start identity changed before abort`,
            });
        }
        if (topology.status === "aborted") return;
        if (topology.status === "completed") {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migId=${intent.migId} completed without local migration state`,
            });
        }
        await this.catalog().abortTopologyOperation({
            migId: identity.migId,
            sourceShard: identity.srcShard,
            destinationShard: identity.dstShard,
            rangeLo: identity.rangeLo,
            rangeHi: identity.rangeHi,
            startEpoch: identity.epochAtStart,
            recoveryGeneration: identity.recoveryGeneration,
        });
    }

    private catalog(): CatalogReshardRpc {
        const namespace = this.env.CDB_CATALOG;
        if (!namespace) {
            throw new CdbError({ code: "CDB_CATALOG_UNAVAILABLE", message: "CDB_CATALOG binding is unavailable" });
        }
        return namespace.get(namespace.idFromName("global")) as unknown as CatalogReshardRpc;
    }

    private topologyRequest(
        migId: string,
        migration: Pick<MigrationState, "src" | "dst" | "lo" | "hi" | "epochAtStart" | "recoveryGeneration">
    ) {
        return {
            migId,
            sourceShard: migration.src,
            destinationShard: migration.dst,
            rangeLo: migration.lo,
            rangeHi: migration.hi,
            startEpoch: migration.epochAtStart,
            recoveryGeneration: migration.recoveryGeneration,
        };
    }

    private routingFenceRequest(
        migId: string,
        migration: Pick<MigrationState, "lo" | "hi" | "epochAtStart" | "recoveryGeneration">
    ) {
        return {
            migrationId: migId,
            rangeLo: migration.lo,
            rangeHi: migration.hi,
            sourceGeneration: migration.epochAtStart,
            destinationGeneration: migration.epochAtStart + 1,
            recoveryGeneration: migration.recoveryGeneration,
        };
    }

    private splitIdentityRequest(migId: string, migration: MigrationState): ReshardCleanupIdentity {
        return {
            migId,
            rangeLo: migration.lo,
            rangeHi: migration.hi,
            schemaVersion: migration.schemaVersion,
            schemaEpoch: migration.schemaEpoch,
            schemaDigest: migration.schemaDigest,
            recoveryGeneration: migration.recoveryGeneration,
            tables: migration.tables,
        };
    }

    private persistBulkCursor(migId: string, cursor: Record<string, number>, expectedPhase: ResharderPhase): void {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        const current = sql.one<{ bulk_cursor: string; phase: number }>(
            "SELECT bulk_cursor, phase FROM migration_state WHERE mig_id = ?",
            migId
        );
        if (!current || current.phase !== expectedPhase) this.phaseChanged(migId, expectedPhase, current?.phase);
        const persisted = parseBulkCursor(current.bulk_cursor);
        if (Object.entries(persisted).some(([table, value]) => (cursor[table] ?? -1) < value)) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "reshard bulk cursor cannot move backward" });
        }
        sql.exec(
            `UPDATE migration_state SET bulk_cursor = ?, updated_at = ?
             WHERE mig_id = ? AND phase = ? AND bulk_cursor = ?`,
            JSON.stringify(cursor),
            Date.now(),
            migId,
            expectedPhase,
            current.bulk_cursor
        );
        if (sql.changes() !== 1) this.phaseChanged(migId, expectedPhase);
    }

    private persistTailCursor(migId: string, cursor: number, expectedPhase: ResharderPhase): void {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        sql.exec(
            `UPDATE migration_state SET tail_cursor = ?, updated_at = ?
             WHERE mig_id = ? AND phase = ? AND tail_cursor <= ?`,
            cursor,
            Date.now(),
            migId,
            expectedPhase,
            cursor
        );
        if (sql.changes() !== 1) this.phaseChanged(migId, expectedPhase);
    }

    private persistWorkCursor(
        migId: string,
        expectedPhase: ResharderPhase,
        turn: number,
        bulkTableIndex?: number
    ): void {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        sql.exec(
            `UPDATE migration_work_cursor SET turn = ?, bulk_table_index = COALESCE(?, bulk_table_index), updated_at = ?
             WHERE mig_id = ? AND EXISTS (
               SELECT 1 FROM migration_state WHERE mig_id = ? AND phase = ?
             )`,
            turn,
            bulkTableIndex ?? null,
            Date.now(),
            migId,
            migId,
            expectedPhase
        );
        if (sql.changes() !== 1) this.phaseChanged(migId, expectedPhase);
    }

    private assertCurrentPhase(migId: string, expectedPhase: ResharderPhase): void {
        const actual = this.readMigration(migId)?.phase;
        if (actual !== expectedPhase) this.phaseChanged(migId, expectedPhase, actual);
    }

    private phaseChanged(migId: string, expectedPhase: ResharderPhase, actual?: number): never {
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: `migId=${migId} expected=${expectedPhase} actual=${actual ?? this.readMigration(migId)?.phase ?? "missing"}`,
        });
    }

    async listMigrations(): Promise<
        {
            migId: string;
            phase: number;
            srcShard: string;
            dstShard: string;
            rangeLo: number;
            rangeHi: number;
        }[]
    > {
        const out: {
            migId: string;
            phase: number;
            srcShard: string;
            dstShard: string;
            rangeLo: number;
            rangeHi: number;
        }[] = [];
        const cur = this.ctx.storage.sql.exec<{
            mig_id: string;
            phase: number;
            src_shard: string;
            dst_shard: string;
            range_lo: number;
            range_hi: number;
        }>("SELECT mig_id, phase, src_shard, dst_shard, range_lo, range_hi FROM migration_state ORDER BY started_at");
        for (const r of cur) {
            out.push({
                migId: r.mig_id,
                phase: r.phase,
                srcShard: r.src_shard,
                dstShard: r.dst_shard,
                rangeLo: r.range_lo,
                rangeHi: r.range_hi,
            });
        }
        return out;
    }
}

/** Bind the packaged application schema into Resharder admission. */
export function configureResharderRuntime(config: {
    readonly schema: () => Record<string, unknown>;
}): typeof Resharder {
    return class ConfiguredResharder extends Resharder {
        protected override runtimeSchema(): Record<string, unknown> {
            return config.schema();
        }
    };
}
