import { CdbError } from "../../errors.ts";
import { CDB_SPLIT_LOG_MAX_BYTES, CDB_SPLIT_LOG_MAX_ROWS, SPLIT_LOG_ACCOUNTED_BYTES_SQL } from "../../oplog/schema.ts";
import { type JsonText, type SyncSql, parseJsonColumn } from "../../oplog/wrapper.ts";
import { type RangeFilter, filterRowsInRange, inRange } from "../../reshard/range.ts";
import { type TableSpec, renderTableTriggers, uninstallOwnedLegacyTableTriggers } from "../../reshard/triggers.ts";
import type { RawJson } from "../../types.ts";
import {
    chardbResourceDescriptorsAt,
    collectSchemaResourceDescriptors,
    isChardbVectorResourceDescriptor,
} from "../resource-descriptors.ts";
import type { ChardbMigrationJournal } from "../schema-migrations.ts";
import {
    enqueueRoutingFenceInvalidations,
    nextInvalidationAlarmAt as readNextInvalidationAlarmAt,
} from "./cdb-live-store.ts";
import {
    CdbReshardIdentityStore,
    type CdbReshardSplitIdentity,
    assertCdbReshardRangeIdentity,
    assertCdbSplitHistoryCapacity,
} from "./cdb-reshard-identity-store.ts";
import {
    CDB_RESHARD_MAX_BATCH_BYTES,
    CDB_RESHARD_MAX_ROW_BYTES,
    applyReshardRow,
    applyReshardSystemTailEntry,
    applyReshardUpdate,
    assertNoUnexpectedReshardTriggers,
    assertReshardBatchBudget,
    assertReshardDestinationRangeEmpty,
    assertReshardEnvelopeBudget,
    assertReshardRowForeignKeysColocated,
    assertReshardSourceTransactionId,
    deleteReshardRow,
    isKnownReshardTailTable,
    orderReshardTables,
    readReshardForeignKeys,
    readReshardTableLayout,
    renderReshardForeignKeyGuards,
    reshardJsonBytes,
    uninstallOwnedLegacyReshardForeignKeyGuards,
} from "./cdb-reshard-relational.ts";
import { type CdbRoutingFence, type CdbRoutingFenceIdentity, CdbRoutingFenceStore } from "./cdb-routing-fence-store.ts";
import type { CdbSchemaMigrationStore } from "./cdb-schema-migration-store.ts";
import {
    type SplitOpLogAckResult,
    type SplitOpLogApplyResult,
    type SplitOpLogBatch,
    ackSplitOpLog,
    applySplitOpLogBatch,
    beginSplitOpLogDestination,
    captureSplitOpLogOutcome,
    finalizeSplitOpLogDestination,
    finalizeSplitOpLogSource,
    readSplitOpLogBatch,
    seedSplitOpLogRange,
} from "./cdb-split-oplog-store.ts";
import { CdbVectorReshardProvenanceStore } from "./cdb-vector-reshard-provenance.ts";
import {
    assertCdbVectorTailReplay,
    initializeCdbVectorReshardTailStore,
    isCdbVectorTailTable,
} from "./cdb-vector-reshard-tail.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

const UTF8 = new TextEncoder();

export interface CdbReshardRuntimeOptions {
    readonly storage: DurableObjectState["storage"];
    readonly schemaMigrations: CdbSchemaMigrationStore;
    readonly schema: () => Record<string, unknown>;
    readonly journal: () => ChardbMigrationJournal;
    readonly invalidationNowMs: () => number;
    readonly scheduleAlarmNoLaterThan: (deadline: number) => Promise<void>;
    /** Exact schema-derived application triggers that may coexist with source capture. */
    readonly allowedApplicationTriggerNames?: () => readonly string[];
    /** Destination-local side-state setup that must commit with generic movement begin. */
    readonly prepareDestination?: (
        sql: SyncSql,
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly destinationGeneration: number }
    ) => void;
    /** Side-state authorization that must hold in the destination serving transaction. */
    readonly assertDestinationActivation?: (
        sql: SyncSql,
        args: Omit<CdbReshardSplitIdentity, "role"> & { readonly destinationGeneration: number }
    ) => void;
}

export function hasActiveCdbVectorResources(input: {
    readonly schema: Readonly<Record<string, unknown>>;
    readonly journal: ChardbMigrationJournal;
    readonly activeVersion: number;
}): boolean {
    const resources =
        input.journal.version === 0
            ? collectSchemaResourceDescriptors(input.schema)
            : chardbResourceDescriptorsAt(input.journal.migrations, input.activeVersion);
    return resources.some(isChardbVectorResourceDescriptor);
}

export function readCdbSourceTailHighWatermark(sql: SyncSql, migId: string): number {
    const row = sql.one<{ high_lsn: number }>(
        `SELECT MAX(
            s.acked_lsn,
            COALESCE((SELECT MAX(l.lsn) FROM _chardb_split_log AS l WHERE l.mig_id = s.mig_id), 0)
         ) AS high_lsn
         FROM _chardb_split_state AS s WHERE s.mig_id = ? AND s.role = 'source'`,
        migId
    );
    if (!row || !Number.isSafeInteger(row.high_lsn) || row.high_lsn < 0) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "source tail high-watermark is invalid" });
    }
    return row.high_lsn;
}

export class CdbReshardRuntime {
    private readonly storage: DurableObjectState["storage"];
    private readonly schemaMigrations: CdbSchemaMigrationStore;
    private readonly routingFences: CdbRoutingFenceStore;
    private readonly reshardIdentities: CdbReshardIdentityStore;
    private readonly schema: () => Record<string, unknown>;
    private readonly journal: () => ChardbMigrationJournal;
    private readonly invalidationNowMs: () => number;
    private readonly scheduleAlarmNoLaterThan: (deadline: number) => Promise<void>;
    private readonly allowedApplicationTriggerNames: () => readonly string[];
    private readonly prepareDestination: NonNullable<CdbReshardRuntimeOptions["prepareDestination"]>;
    private readonly assertDestinationActivation: NonNullable<CdbReshardRuntimeOptions["assertDestinationActivation"]>;

    constructor(options: CdbReshardRuntimeOptions) {
        this.storage = options.storage;
        this.schemaMigrations = options.schemaMigrations;
        this.routingFences = new CdbRoutingFenceStore(options.storage);
        this.reshardIdentities = new CdbReshardIdentityStore(adaptSqlStorage(options.storage.sql));
        this.schema = options.schema;
        this.journal = options.journal;
        this.invalidationNowMs = options.invalidationNowMs;
        this.scheduleAlarmNoLaterThan = options.scheduleAlarmNoLaterThan;
        this.allowedApplicationTriggerNames = options.allowedApplicationTriggerNames ?? (() => []);
        this.prepareDestination = options.prepareDestination ?? (() => undefined);
        this.assertDestinationActivation = options.assertDestinationActivation ?? (() => undefined);
    }

    private hasActiveVectorResources(sql: SyncSql): boolean {
        return hasActiveCdbVectorResources({
            schema: this.schema(),
            journal: this.journal(),
            activeVersion: this.schemaMigrations.state(sql).activeVersion,
        });
    }

    captureSplitOutcome(args: {
        readonly sql: SyncSql;
        readonly principalId: string;
        readonly mutId: string;
        readonly vshard: number;
    }): void {
        captureSplitOpLogOutcome(args);
    }

    assertFreshDestinationProvisioningAllowed(migId: string): void {
        const prepared = adaptSqlStorage(this.storage.sql).one<{
            role: string;
            drained: number;
            abort_started: number;
            destination_generation: number | null;
            destination_serving: number;
        }>(
            `SELECT role, drained, abort_started, destination_generation, destination_serving
             FROM _chardb_split_state WHERE mig_id = ?`,
            migId
        );
        if (
            !prepared ||
            prepared.role !== "dest" ||
            prepared.drained === 1 ||
            prepared.abort_started === 1 ||
            prepared.destination_generation === null ||
            prepared.destination_serving !== 0
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: "destination split ownership is not prepared, is serving, or was canceled before provisioning",
            });
        }
    }

    assertRoutingAdmission(schemaEpoch: number, vshard: number, sql = adaptSqlStorage(this.storage.sql)): void {
        const legacyDestination = sql.one<{ present: number }>(
            `SELECT 1 AS present FROM _chardb_split_state
             WHERE role = 'dest' AND destination_generation IS NULL
               AND range_lo <= ? AND range_hi >= ? LIMIT 1`,
            vshard,
            vshard
        );
        if (legacyDestination) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "destination ownership must be rebound before this upgraded split can serve",
            });
        }
        const destinations = sql.all<{
            destination_generation: number | null;
            destination_serving: number;
        }>(
            `SELECT destination_generation, destination_serving
             FROM _chardb_split_state
             WHERE role = 'dest' AND range_lo <= ? AND range_hi >= ?
             ORDER BY destination_generation DESC, destination_serving DESC, mig_id
             LIMIT 3`,
            vshard,
            vshard
        );
        if (destinations.length > 0) {
            const generation = destinations[0]?.destination_generation;
            if (typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 1) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "destination ownership must be rebound before this upgraded split can serve",
                });
            }
            const latest = destinations.filter(row => row.destination_generation === generation);
            const serving = latest.filter(row => row.destination_serving === 1);
            if (
                serving.length > 1 ||
                latest.some(row => row.destination_serving !== 0 && row.destination_serving !== 1)
            ) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "destination ownership state is ambiguous" });
            }
            if (serving.length !== 1 || schemaEpoch < generation) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "destination does not own this vshard at the requested routing generation",
                    hint: "resolve the active vshard placement from Catalog and retry on its shard",
                });
            }
        }
        this.routingFences.assertMutationAdmission({ schemaEpoch, vshard }, sql);
    }

    /** Assert current physical ownership for alarm work that has no caller-supplied routing generation. */
    assertBackgroundDeliveryAdmission(vshard: number, sql = adaptSqlStorage(this.storage.sql)): void {
        if (!Number.isSafeInteger(vshard) || vshard < 0 || vshard >= 16_384) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "background delivery vshard is invalid" });
        }
        if (this.routingFences.activeSourceFence(vshard, sql)) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "background delivery is fenced on the moved source range",
            });
        }
        const destinations = sql.all<{
            destination_generation: number | null;
            destination_serving: number;
            drained: number;
        }>(
            `SELECT destination_generation, destination_serving, drained
             FROM _chardb_split_state
             WHERE role = 'dest' AND range_lo <= ? AND range_hi >= ?
             ORDER BY destination_generation DESC, destination_serving DESC, mig_id
             LIMIT 3`,
            vshard,
            vshard
        );
        if (destinations.length === 0) return;
        const generation = destinations[0]?.destination_generation;
        if (typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 1) {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "background destination ownership is unbound" });
        }
        const latest = destinations.filter(row => row.destination_generation === generation);
        if (latest.length !== 1 || latest[0]?.destination_serving !== 1 || latest[0]?.drained !== 1) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "background delivery requires one drained, serving destination owner",
            });
        }
    }

    assertUnplacedRoutingAdmission(sql = adaptSqlStorage(this.storage.sql)): void {
        const destination = sql.one<{ present: number }>(
            "SELECT 1 AS present FROM _chardb_split_state WHERE role = 'dest' LIMIT 1"
        );
        const sourceFence = sql.one<{ present: number }>(
            "SELECT 1 AS present FROM _chardb_routing_fences WHERE status != 'superseded' LIMIT 1"
        );
        if (destination || sourceFence) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: "unplaced application RPC cannot prove shard ownership after resharding starts",
                hint: "resolve an exact Catalog placement and retry with its routing generation",
            });
        }
    }

    assertNoActiveReshard(sql = adaptSqlStorage(this.storage.sql)): void {
        const active = sql.one<{ mig_id: string }>(
            "SELECT mig_id FROM _chardb_split_state WHERE drained = 0 ORDER BY mig_id LIMIT 1"
        );
        if (active) {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: `schema migration is blocked by active reshard ${active.mig_id}`,
                hint: "finish or abort the exact range move before changing this Cdb schema",
            });
        }
    }

    /** Undeclared internal file state cannot be left behind by a relational-only move. */
    assertNoUnmovableFileState(sql = adaptSqlStorage(this.storage.sql)): void {
        const tables = new Set(
            sql
                .all<{ name: string }>(
                    `SELECT name FROM sqlite_master
                     WHERE type = 'table' AND name IN ('_chardb_files', '_chardb_deleted_organizations')`
                )
                .map(row => row.name)
        );
        const kind =
            tables.has("_chardb_files") && sql.one("SELECT 1 FROM _chardb_files LIMIT 1")
                ? "file metadata"
                : tables.has("_chardb_deleted_organizations") &&
                    sql.one("SELECT 1 FROM _chardb_deleted_organizations LIMIT 1")
                  ? "organization deletion tombstone"
                  : null;
        if (kind) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `${kind} has no online reshard transfer protocol`,
                hint: "declare the file resource and use its file-aware movement protocol before moving this Cdb",
            });
        }
    }

    /** Prepare one immutable source-side routing range fence. */
    prepareRoutingFence(args: CdbRoutingFenceIdentity): CdbRoutingFence {
        return this.routingFences.prepare(args);
    }

    /** Stop this source and wake every live registration stranded in the moved range. */
    async activateRoutingFence(args: CdbRoutingFenceIdentity): Promise<CdbRoutingFence> {
        const fence = this.routingFences.activate(args, Date.now(), sql => {
            enqueueRoutingFenceInvalidations(sql, args.rangeLo, args.rangeHi);
        });
        const nextAttemptAt = readNextInvalidationAlarmAt(adaptSqlStorage(this.storage.sql));
        if (nextAttemptAt !== null) {
            await this.scheduleAlarmNoLaterThan(Math.max(this.invalidationNowMs() + 1, nextAttemptAt));
        }
        return fence;
    }

    /** Mark source data cleanup complete without reopening the fenced range. */
    completeRoutingFenceCleanup(args: CdbRoutingFenceIdentity): CdbRoutingFence {
        return this.routingFences.cleanup(args);
    }

    cancelRoutingFenceBeforeCutover(args: CdbRoutingFenceIdentity): CdbRoutingFence | null {
        return this.routingFences.cancelBeforeCutover(args);
    }

    /** Inspect the activated source fence that covers one vshard. */
    activeRoutingFence(vshard: number): CdbRoutingFence | null {
        return this.routingFences.activeSourceFence(vshard);
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
    async beginReshardSource(args: {
        migId: string;
        recoveryGeneration: number;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
    }): Promise<{ enabled: boolean; triggersInstalled: number }> {
        let triggers = 0;
        let enabled = true;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardBeginAllowed(sql, args, "source");
            this.reshardIdentities.bind(
                {
                    ...args,
                    role: "source",
                },
                this.schema(),
                this.schemaMigrations.state(sql),
                this.journal(),
                Date.now()
            );
            const captureTriggers = args.tables.map(table => renderTableTriggers(args.migId, table));
            uninstallOwnedLegacyReshardForeignKeyGuards(sql, args.migId, args.tables);
            const foreignKeyGuards = renderReshardForeignKeyGuards(sql, args.migId, args.tables);
            for (const table of args.tables) uninstallOwnedLegacyTableTriggers(sql, args.migId, table);
            assertNoUnexpectedReshardTriggers(sql, args.tables, [
                ...captureTriggers.flatMap(triggers => triggers.names),
                ...foreignKeyGuards.names,
                ...this.allowedApplicationTriggerNames(),
            ]);
            const legacy = sql.one<{ principal_id: string; mut_id: string }>(
                `SELECT principal_id, mut_id FROM _chardb_op_log
                 WHERE placement_vshard IS NULL LIMIT 1`
            );
            if (legacy) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "retained mutation replay history predates durable vshard placement",
                    hint: "wait for the configured mutation deduplication horizon before resharding this Cdb",
                });
            }
            sql.exec(
                `INSERT OR IGNORE INTO _chardb_split_state (mig_id, range_lo, range_hi, role, capture, updated_at)
                 VALUES (?, ?, ?, 'source', 1, ?)`,
                args.migId,
                args.rangeLo,
                args.rangeHi,
                Date.now()
            );
            const state = sql.one<{
                range_lo: number;
                range_hi: number;
                role: string;
                drained: number;
                drain_started: number;
            }>(
                "SELECT range_lo, range_hi, role, drained, drain_started FROM _chardb_split_state WHERE mig_id = ?",
                args.migId
            );
            if (
                !state ||
                state.range_lo !== args.rangeLo ||
                state.range_hi !== args.rangeHi ||
                state.role !== "source" ||
                state.drained !== 0
            ) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "source split id belongs to a different immutable identity",
                });
            }
            for (const table of args.tables) {
                if (sql.one(`SELECT 1 FROM ${quoteIdent(table.name)} WHERE rowid <= 0 LIMIT 1`)) {
                    throw new CdbError({
                        code: "CDB_INVALID_ARGS",
                        message: `reshard source table ${table.name} has nonpositive rowids`,
                    });
                }
                const watermark = sql.one<{ max_rowid: number }>(
                    `SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM ${quoteIdent(table.name)}`
                );
                if (!watermark || !Number.isSafeInteger(watermark.max_rowid) || watermark.max_rowid < 0) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: "reshard bulk watermark is invalid" });
                }
                sql.exec(
                    `INSERT OR IGNORE INTO _chardb_split_bulk_watermark (mig_id, table_name, max_rowid)
                     VALUES (?, ?, ?)`,
                    args.migId,
                    table.name,
                    watermark.max_rowid
                );
            }
            for (const statement of foreignKeyGuards.uninstall) sql.exec(statement);
            for (const statement of foreignKeyGuards.install) sql.exec(statement);
            if (state.drain_started === 1) {
                enabled = false;
                return;
            }
            sql.exec(
                "UPDATE _chardb_split_state SET capture = 1, updated_at = ? WHERE mig_id = ?",
                Date.now(),
                args.migId
            );
            seedSplitOpLogRange(sql, args);
            for (const ts of captureTriggers) {
                for (const stmt of ts.uninstall) sql.exec(stmt);
                for (const stmt of ts.install) {
                    sql.exec(stmt);
                    triggers++;
                }
            }
        });
        return { enabled, triggersInstalled: triggers };
    }

    /** Persist fail-closed destination ownership before any provisioning can expose the shard. */
    prepareReshardDestOwnership(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        destinationGeneration: number;
    }): { prepared: boolean; serving: boolean } {
        assertCdbReshardRangeIdentity(args);
        if (!Number.isSafeInteger(args.destinationGeneration) || args.destinationGeneration < 1) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "destination generation must be a positive integer",
            });
        }
        let prepared = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            const nowMs = Date.now();
            assertCdbSplitHistoryCapacity(sql, args.migId);
            sql.exec(
                `INSERT OR IGNORE INTO _chardb_split_state
                 (mig_id, range_lo, range_hi, role, capture, destination_generation, destination_serving, updated_at)
                 VALUES (?, ?, ?, 'dest', 0, ?, 0, ?)`,
                args.migId,
                args.rangeLo,
                args.rangeHi,
                args.destinationGeneration,
                nowMs
            );
            prepared = sql.changes() === 1;
            const state = sql.one<{
                range_lo: number;
                range_hi: number;
                role: string;
                destination_generation: number | null;
                destination_serving: number;
                abort_started: number;
            }>(
                `SELECT range_lo, range_hi, role, destination_generation, destination_serving, abort_started
                 FROM _chardb_split_state WHERE mig_id = ?`,
                args.migId
            );
            if (
                state?.role === "dest" &&
                state.range_lo === args.rangeLo &&
                state.range_hi === args.rangeHi &&
                state.destination_generation === null &&
                state.destination_serving === 0 &&
                state.abort_started === 0
            ) {
                sql.exec(
                    `UPDATE _chardb_split_state SET destination_generation = ?, updated_at = ?
                     WHERE mig_id = ? AND role = 'dest' AND destination_generation IS NULL
                       AND destination_serving = 0 AND abort_started = 0`,
                    args.destinationGeneration,
                    nowMs,
                    args.migId
                );
                if (sql.changes() !== 1) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "destination ownership upgrade changed concurrently",
                    });
                }
                prepared = true;
                state.destination_generation = args.destinationGeneration;
            }
            if (
                !state ||
                state.range_lo !== args.rangeLo ||
                state.range_hi !== args.rangeHi ||
                state.role !== "dest" ||
                state.destination_generation !== args.destinationGeneration ||
                state.abort_started !== 0
            ) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination ownership preparation conflicts with its immutable split identity",
                });
            }
        });
        const serving = adaptSqlStorage(this.storage.sql).one<{ destination_serving: number }>(
            "SELECT destination_serving FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
            args.migId
        )?.destination_serving;
        if (serving !== 0 && serving !== 1) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "destination ownership serving state is invalid" });
        }
        return { prepared, serving: serving === 1 };
    }

    /** Destination-shard counterpart; tracks the migration so duplicate applies are rejected. */
    async beginReshardDest(args: {
        migId: string;
        recoveryGeneration: number;
        rangeLo: number;
        rangeHi: number;
        schemaVersion: number;
        schemaEpoch: number;
        schemaDigest: string;
        tables: readonly TableSpec[];
        destinationGeneration: number;
    }): Promise<{ ready: boolean }> {
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            const nowMs = Date.now();
            this.assertReshardBeginAllowed(sql, args, "dest");
            this.reshardIdentities.bind(
                { ...args, role: "dest" },
                this.schema(),
                this.schemaMigrations.state(sql),
                this.journal(),
                nowMs
            );
            orderReshardTables(sql, args.tables);
            this.prepareDestination(sql, args);
            assertNoUnexpectedReshardTriggers(sql, args.tables);
            assertReshardDestinationRangeEmpty(sql, args.tables, { lo: args.rangeLo, hi: args.rangeHi });
            sql.exec(
                `INSERT OR IGNORE INTO _chardb_split_state
                 (mig_id, range_lo, range_hi, role, capture, destination_generation, destination_serving, updated_at)
                 VALUES (?, ?, ?, 'dest', 0, ?, 0, ?)`,
                args.migId,
                args.rangeLo,
                args.rangeHi,
                args.destinationGeneration,
                nowMs
            );
            const state = sql.one<{
                range_lo: number;
                range_hi: number;
                role: string;
                drained: number;
                destination_generation: number | null;
                destination_serving: number;
            }>(
                `SELECT range_lo, range_hi, role, drained, destination_generation, destination_serving
                 FROM _chardb_split_state WHERE mig_id = ?`,
                args.migId
            );
            if (
                !state ||
                state.range_lo !== args.rangeLo ||
                state.range_hi !== args.rangeHi ||
                state.role !== "dest" ||
                state.drained !== 0 ||
                state.destination_generation !== args.destinationGeneration ||
                state.destination_serving !== 0
            ) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination split id belongs to a different immutable identity",
                });
            }
            beginSplitOpLogDestination(sql, args, nowMs);
        });
        return { ready: true };
    }

    /** Start serving only after Catalog has committed this exact cutover generation. */
    activateReshardDestServing(args: Omit<CdbReshardSplitIdentity, "role"> & { destinationGeneration: number }): {
        activated: boolean;
    } {
        let activated = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardCleanupIdentity({ ...args, role: "dest" }, sql);
            const state = sql.one<{
                destination_generation: number | null;
                destination_serving: number;
                abort_started: number;
            }>(
                `SELECT destination_generation, destination_serving, abort_started
                 FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'`,
                args.migId
            );
            if (!state || state.destination_generation !== args.destinationGeneration) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination serving activation does not match its prepared routing generation",
                });
            }
            if (state.abort_started === 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "aborted destination cannot become serving",
                });
            }
            this.assertDestinationActivation(sql, args);
            if (state.destination_serving === 1) return;
            this.routingFences.assertDestinationActivationAllowed(
                {
                    rangeLo: args.rangeLo,
                    rangeHi: args.rangeHi,
                    destinationGeneration: args.destinationGeneration,
                },
                sql
            );
            sql.exec(
                `UPDATE _chardb_split_state SET destination_serving = 1, updated_at = ?
                 WHERE mig_id = ? AND role = 'dest' AND destination_serving = 0 AND abort_started = 0`,
                Date.now(),
                args.migId
            );
            if (sql.changes() !== 1) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: "destination serving activation changed concurrently",
                });
            }
            activated = true;
        });
        return { activated };
    }

    /** Read bounded captured mutation outcomes in source LSN order. */
    async readSplitOpLogBatch(args: {
        migId: string;
        afterLsn: number;
        limit: number;
    }): Promise<SplitOpLogBatch> {
        let result: SplitOpLogBatch | undefined;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({ migId: args.migId, role: "source", sql });
            result = readSplitOpLogBatch(sql, args);
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "split-oplog read failed" });
        return result;
    }

    /** Prune source mutation outcomes only after the Resharder durably records destination acceptance. */
    async ackSplitOpLog(args: { migId: string; throughLsn: number }): Promise<SplitOpLogAckResult> {
        let result: SplitOpLogAckResult | undefined;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({ migId: args.migId, role: "source", sql });
            result = ackSplitOpLog(sql, args.migId, args.throughLsn);
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "split-oplog acknowledgement failed" });
        return result;
    }

    /** Reconstruct source mutation outcomes and its durable cursor atomically on the destination. */
    async applySplitOpLogBatch(args: {
        migId: string;
        rangeLo: number;
        rangeHi: number;
        entries: SplitOpLogBatch["entries"];
    }): Promise<SplitOpLogApplyResult> {
        let result: SplitOpLogApplyResult | undefined;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: { lo: args.rangeLo, hi: args.rangeHi },
                sql,
            });
            initializeCdbVectorReshardTailStore(sql);
            result = applySplitOpLogBatch(sql, args);
        });
        if (!result) throw new CdbError({ code: "CDB_INVARIANT", message: "split-oplog apply returned no result" });
        return result;
    }

    /** Returns the tail-capture watermark — the latest `_chardb_split_log.lsn` Source has produced. */
    async tailWatermark(migId: string): Promise<{ lsn: number }> {
        const sql = adaptSqlStorage(this.storage.sql);
        this.assertReshardMovement({ migId, role: "source", sql });
        return { lsn: this.sourceTailHighWatermark(migId, sql) };
    }

    sourceTailHighWatermark(migId: string, sql = adaptSqlStorage(this.storage.sql)): number {
        return readCdbSourceTailHighWatermark(sql, migId);
    }

    /** Resolve the deterministic parent-before-child order for bulk copy. */
    async reshardTableOrder(args: {
        migId: string;
        role: "source" | "dest";
        range: RangeFilter;
        tables: readonly TableSpec[];
    }): Promise<{ tableNames: readonly string[] }> {
        const sql = adaptSqlStorage(this.storage.sql);
        this.assertReshardMovement({
            migId: args.migId,
            role: args.role,
            range: args.range,
            tables: args.tables,
            sql,
        });
        return { tableNames: orderReshardTables(sql, args.tables).map(table => table.name) };
    }

    /**
     * Read one paginated bulk-copy batch from this (source) shard. The batch
     * contains rows whose partition column hashes into `[lo, hi]`; the caller
     * pages by `afterRowid` until `done=true`. Rows are returned as plain
     * column maps for the destination's primary-key-aware apply path.
     */
    async bulkCopyBatch(args: {
        migId: string;
        table: TableSpec;
        range: RangeFilter;
        afterRowid: number;
        limit: number;
    }): Promise<{ rows: readonly Record<string, RawJson>[]; lastRowid: number; done: boolean }> {
        if (!Number.isSafeInteger(args.limit) || args.limit < 1 || args.limit > 500) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard bulk limit must be from 1 through 500" });
        }
        const identity = this.assertReshardMovement({
            migId: args.migId,
            role: "source",
            range: args.range,
            table: args.table,
        });
        const ident = quoteIdent(args.table.name);
        const cols = args.table.columns.map(quoteIdent).join(", ");
        const jsonObject = args.table.columns.map(column => `'${column}', ${quoteIdent(column)}`).join(", ");
        const sql = adaptSqlStorage(this.storage.sql);
        const watermark = sql.one<{ max_rowid: number }>(
            "SELECT max_rowid FROM _chardb_split_bulk_watermark WHERE mig_id = ? AND table_name = ?",
            args.migId,
            args.table.name
        );
        if (!watermark) {
            throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message: "reshard bulk watermark is missing" });
        }
        const foreignKeys = readReshardForeignKeys(sql, identity.tables);
        const candidates = sql.all<{ rowid: number; row_bytes: number }>(
            `SELECT rowid, length(CAST(json_object(${jsonObject}) AS BLOB)) AS row_bytes
             FROM ${ident} WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
            args.afterRowid,
            watermark.max_rowid,
            args.limit
        );
        const rows: Record<string, RawJson>[] = [];
        let bytes = 0;
        let lastRowid = args.afterRowid;
        let budgetStopped = false;
        for (const candidate of candidates) {
            assertReshardRowForeignKeysColocated(sql, args.table, candidate.rowid, foreignKeys);
            if (candidate.row_bytes > CDB_RESHARD_MAX_ROW_BYTES) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `reshard bulk row exceeds ${CDB_RESHARD_MAX_ROW_BYTES} UTF-8 bytes`,
                });
            }
            const row = sql.one<Record<string, RawJson>>(
                `SELECT ${cols} FROM ${ident} WHERE rowid = ?`,
                candidate.rowid
            );
            if (!row) throw new CdbError({ code: "CDB_INVARIANT", message: "reshard bulk row disappeared" });
            const rowBytes = reshardJsonBytes(row);
            if (rowBytes > CDB_RESHARD_MAX_ROW_BYTES) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `reshard bulk row exceeds ${CDB_RESHARD_MAX_ROW_BYTES} UTF-8 bytes`,
                });
            }
            if (bytes + rowBytes > CDB_RESHARD_MAX_BATCH_BYTES) {
                budgetStopped = true;
                break;
            }
            bytes += rowBytes;
            lastRowid = candidate.rowid;
            if (inRange(row[args.table.partitionColumn], args.range)) rows.push(row);
        }
        const done = !budgetStopped && candidates.length < args.limit;
        return { rows, lastRowid, done };
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
        table: TableSpec;
        range: RangeFilter;
        rows: readonly Record<string, RawJson>[];
    }): Promise<{ applied: number; skipped: number }> {
        if (!Array.isArray(args.rows) || args.rows.length > 500) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard bulk batch exceeds 500 rows" });
        }
        assertReshardBatchBudget(args.rows, "reshard bulk");
        const accepted = filterRowsInRange(args.rows, args.table.partitionColumn, args.range);
        const skipped = args.rows.length - accepted.length;
        let applied = 0;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: args.range,
                table: args.table,
                sql,
            });
            const split = sql.one<{ bulk_done: number }>(
                "SELECT bulk_done FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
                args.migId
            );
            if (!split || split.bulk_done !== 0) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination bulk copy is already closed",
                });
            }
            const layout = readReshardTableLayout(sql, args.table);
            for (const r of accepted) {
                applyReshardRow(sql, args.table, r, layout);
                applied++;
            }
        });
        return { applied, skipped };
    }

    /** Permanently reject delayed bulk batches once all source snapshots are copied. */
    async closeReshardBulkDest(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ closed: boolean }> {
        let closed = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: { lo: args.rangeLo, hi: args.rangeHi },
                tables: args.tables,
                sql,
            });
            sql.exec(
                `UPDATE _chardb_split_state SET bulk_done = 1, updated_at = ?
                 WHERE mig_id = ? AND role = 'dest' AND bulk_done = 0`,
                Date.now(),
                args.migId
            );
            closed = sql.changes() === 1;
        });
        return { closed };
    }

    /**
     * Drain the source's `_chardb_split_log` for a migration. The returned rows
     * are ordered by `lsn` and bounded by `limit`; the destination applies them
     * via `applyTailBatch`. The split log is preserved on the source until
     * `finishReshardSource` so a crash mid-replay can resume.
     */
    async readTailBatch(args: {
        migId: string;
        afterLsn: number;
        limit: number;
    }): Promise<{
        transactions: readonly TailTransaction[];
        lastLsn: number;
        done: boolean;
    }> {
        if (args.limit !== 500) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "reshard tail protocol limit must be exactly 500",
            });
        }
        this.assertReshardMovement({ migId: args.migId, role: "source" });
        const sql = adaptSqlStorage(this.storage.sql);
        const candidates = sql.all<{ lsn: number; source_tx_id: number }>(
            `SELECT lsn, source_tx_id
             FROM _chardb_split_log WHERE mig_id = ? AND lsn > ? ORDER BY lsn LIMIT ?`,
            args.migId,
            args.afterLsn,
            501
        );
        for (const candidate of candidates) assertReshardSourceTransactionId(candidate.source_tx_id);
        let complete = candidates.slice(0, 500);
        const probe = candidates.at(500);
        if (probe && complete.at(-1)?.source_tx_id === probe.source_tx_id) {
            complete = complete.filter(candidate => candidate.source_tx_id !== probe.source_tx_id);
        }
        if (candidates.length > 0 && complete.length === 0) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: "source split transaction exceeds the tail row cap",
            });
        }
        const transactionRows: { sourceTxId: number; lsns: number[] }[] = [];
        for (const candidate of complete) {
            const current = transactionRows.at(-1);
            if (current?.sourceTxId === candidate.source_tx_id) current.lsns.push(candidate.lsn);
            else transactionRows.push({ sourceTxId: candidate.source_tx_id, lsns: [candidate.lsn] });
        }
        const transactions: TailTransaction[] = [];
        let wireBytes = 2;
        let acceptedRows = 0;
        let lastLsn = args.afterLsn;
        for (const transaction of transactionRows) {
            const entries = sql.all<TailEntry>(
                `SELECT source_tx_id, lsn, op, table_name, pk, after, before
                 FROM _chardb_split_log WHERE mig_id = ? AND source_tx_id = ? AND lsn >= ? AND lsn <= ? ORDER BY lsn`,
                args.migId,
                transaction.sourceTxId,
                transaction.lsns[0] as number,
                transaction.lsns.at(-1) as number
            );
            assertReshardBatchBudget(entries, "reshard tail transaction");
            const firstLsn = entries[0]?.lsn;
            const transactionLastLsn = entries.at(-1)?.lsn;
            if (entries.length !== transaction.lsns.length || firstLsn == null || transactionLastLsn == null) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "source split transaction changed during read" });
            }
            const envelope: TailTransaction = {
                sourceTxId: transaction.sourceTxId,
                firstLsn,
                lastLsn: transactionLastLsn,
                entries,
            };
            const envelopeBytes = reshardJsonBytes(envelope) + (transactions.length > 0 ? 1 : 0);
            if (
                transactions.length > 0 &&
                (acceptedRows + entries.length > args.limit || wireBytes + envelopeBytes > CDB_RESHARD_MAX_BATCH_BYTES)
            ) {
                break;
            }
            if (wireBytes + envelopeBytes > CDB_RESHARD_MAX_BATCH_BYTES) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: "source split transaction exceeds tail byte cap",
                });
            }
            transactions.push(envelope);
            wireBytes += envelopeBytes;
            acceptedRows += entries.length;
            lastLsn = transactionLastLsn;
        }
        const done = !sql.one<{ found: number }>(
            "SELECT 1 AS found FROM _chardb_split_log WHERE mig_id = ? AND lsn > ? LIMIT 1",
            args.migId,
            lastLsn
        );
        return { transactions, lastLsn, done };
    }

    /** Prune only tail transactions the Resharder has durably acknowledged. */
    async ackTail(args: { migId: string; throughLsn: number }): Promise<{ pruned: number; ackedLsn: number }> {
        if (!Number.isSafeInteger(args.throughLsn) || args.throughLsn < 0) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard tail acknowledgement is invalid" });
        }
        let pruned = 0;
        let ackedLsn = 0;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({ migId: args.migId, role: "source", sql });
            const state = sql.one<{
                acked_lsn: number;
                split_log_rows: number;
                split_log_bytes: number;
            }>(
                `SELECT acked_lsn, split_log_rows, split_log_bytes
                 FROM _chardb_split_state WHERE mig_id = ? AND role = 'source'`,
                args.migId
            );
            if (!state) throw new CdbError({ code: "CDB_INVARIANT", message: "source split state is missing" });
            ackedLsn = state.acked_lsn;
            if (args.throughLsn <= state.acked_lsn) return;
            const endpoint = sql.one<{ source_tx_id: number }>(
                "SELECT source_tx_id FROM _chardb_split_log WHERE mig_id = ? AND lsn = ?",
                args.migId,
                args.throughLsn
            );
            if (!endpoint) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "tail acknowledgement endpoint is missing" });
            }
            assertReshardSourceTransactionId(endpoint.source_tx_id);
            if (
                sql.one<{ found: number }>(
                    `SELECT 1 AS found FROM _chardb_split_log
                     WHERE mig_id = ? AND source_tx_id = ? AND lsn > ? LIMIT 1`,
                    args.migId,
                    endpoint.source_tx_id,
                    args.throughLsn
                )
            ) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: "tail acknowledgement splits a source transaction",
                });
            }
            const stats = sql.one<{ rows: number; bytes: number }>(
                `SELECT COUNT(*) AS rows, COALESCE(SUM(${SPLIT_LOG_ACCOUNTED_BYTES_SQL}), 0) AS bytes
                 FROM _chardb_split_log WHERE mig_id = ? AND lsn <= ?`,
                args.migId,
                args.throughLsn
            );
            if (
                !stats ||
                !Number.isSafeInteger(stats.rows) ||
                !Number.isSafeInteger(stats.bytes) ||
                stats.rows < 1 ||
                stats.rows > state.split_log_rows ||
                stats.bytes < 0 ||
                stats.bytes > state.split_log_bytes
            ) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "tail acknowledgement accounting is invalid" });
            }
            sql.exec("DELETE FROM _chardb_split_log WHERE mig_id = ? AND lsn <= ?", args.migId, args.throughLsn);
            sql.exec(
                `UPDATE _chardb_split_state
                 SET acked_lsn = ?, split_log_rows = split_log_rows - ?,
                     split_log_bytes = split_log_bytes - ?, updated_at = ?
                 WHERE mig_id = ? AND role = 'source' AND acked_lsn = ?
                   AND split_log_rows >= ? AND split_log_bytes >= ?`,
                args.throughLsn,
                stats.rows,
                stats.bytes,
                Date.now(),
                args.migId,
                state.acked_lsn,
                stats.rows,
                stats.bytes
            );
            if (sql.changes() !== 1) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "tail acknowledgement state changed" });
            }
            pruned = stats.rows;
            ackedLsn = args.throughLsn;
        });
        return { pruned, ackedLsn };
    }

    /** Durably stage complete source transactions while stale bulk pages may still arrive. */
    async stageTailBatch(args: {
        migId: string;
        tables: readonly TableSpec[];
        range: RangeFilter;
        transactions: readonly TailTransaction[];
    }): Promise<{ staged: number; lastLsn: number }> {
        if (!Array.isArray(args.transactions)) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard staged tail must be an array" });
        }
        const entries = args.transactions.flatMap(transaction => transaction.entries);
        if (entries.length > 500)
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "staged tail exceeds 500 rows" });
        assertReshardBatchBudget(entries, "reshard staged tail");
        assertReshardEnvelopeBudget(args.transactions, "reshard staged tail");
        assertTailTransactions(args.transactions);
        const tableNames = new Set(args.tables.map(table => table.name));
        for (const entry of entries) {
            isKnownReshardTailTable(entry.table_name, tableNames);
        }
        const encodedTransactions = args.transactions.map(transaction => {
            const json = JSON.stringify(transaction);
            return Object.freeze({
                transaction,
                json,
                rows: transaction.entries.length,
                bytes: UTF8.encode(json).byteLength,
            });
        });
        const addedRows = encodedTransactions.reduce((total, item) => total + item.rows, 0);
        const addedBytes = encodedTransactions.reduce((total, item) => total + item.bytes, 0);
        const lastLsn = args.transactions.at(-1)?.lastLsn ?? 0;
        let staged = 0;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: args.range,
                tables: args.tables,
                sql,
            });
            const state = sql.one<{
                staged_lsn: number;
                inbox_rows: number;
                inbox_bytes: number;
                inbox_closed: number;
            }>(
                "SELECT staged_lsn, inbox_rows, inbox_bytes, inbox_closed FROM _chardb_split_state WHERE mig_id = ?",
                args.migId
            );
            if (!state || state.inbox_closed === 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination tail staging is closed",
                });
            }
            if (lastLsn <= state.staged_lsn) {
                for (const transaction of args.transactions) {
                    const stored = sql.one<{ transaction_json: string }>(
                        `SELECT transaction_json FROM _chardb_split_tail_inbox
                         WHERE mig_id = ? AND source_tx_id = ?`,
                        args.migId,
                        transaction.sourceTxId
                    );
                    if (stored) {
                        if (stored.transaction_json !== JSON.stringify(transaction)) {
                            throw new CdbError({
                                code: "CDB_INVARIANT",
                                message: "staged tail retry changed a transaction envelope",
                            });
                        }
                    } else {
                        const applied = sql.one<{ applied_lsn: number }>(
                            "SELECT applied_lsn FROM _chardb_split_state WHERE mig_id = ?",
                            args.migId
                        );
                        if (!applied || applied.applied_lsn < transaction.lastLsn) {
                            throw new CdbError({
                                code: "CDB_INVARIANT",
                                message: "staged tail retry is missing before its applied cursor",
                            });
                        }
                    }
                }
                return;
            }
            if ((args.transactions[0]?.firstLsn ?? lastLsn) <= state.staged_lsn) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "staged tail crosses its durable cursor" });
            }
            if (
                state.inbox_rows + addedRows > CDB_SPLIT_LOG_MAX_ROWS ||
                state.inbox_bytes + addedBytes > CDB_SPLIT_LOG_MAX_BYTES
            ) {
                throw new CdbError({
                    code: "CDB_RATE_LIMITED",
                    message: "destination staged-tail inbox reached its durable limit",
                    hint: "retry after bulk copy closes and the destination drains staged transactions",
                });
            }
            for (const item of encodedTransactions) {
                sql.exec(
                    `INSERT INTO _chardb_split_tail_inbox
                     (mig_id, source_tx_id, first_lsn, last_lsn, row_count, byte_size, transaction_json)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    args.migId,
                    item.transaction.sourceTxId,
                    item.transaction.firstLsn,
                    item.transaction.lastLsn,
                    item.rows,
                    item.bytes,
                    item.json
                );
            }
            sql.exec(
                `UPDATE _chardb_split_state SET staged_lsn = ?, inbox_rows = inbox_rows + ?,
                 inbox_bytes = inbox_bytes + ?, updated_at = ? WHERE mig_id = ? AND staged_lsn = ?`,
                lastLsn,
                addedRows,
                addedBytes,
                Date.now(),
                args.migId,
                state.staged_lsn
            );
            if (sql.changes() !== 1)
                throw new CdbError({ code: "CDB_INVARIANT", message: "staged tail cursor changed" });
            staged = addedRows;
        });
        return { staged, lastLsn };
    }

    async readStagedTailBatch(args: { migId: string; limit: number }): Promise<{ transactions: TailTransaction[] }> {
        if (args.limit !== 500)
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "staged tail limit must be 500" });
        this.assertReshardMovement({ migId: args.migId, role: "dest" });
        const sql = adaptSqlStorage(this.storage.sql);
        const rows = sql.all<{ transaction_json: string; row_count: number; byte_size: number }>(
            `SELECT transaction_json, row_count, byte_size FROM _chardb_split_tail_inbox
             WHERE mig_id = ? ORDER BY first_lsn LIMIT 500`,
            args.migId
        );
        const transactions: TailTransaction[] = [];
        let count = 0;
        let envelopeBytes = 2;
        for (const row of rows) {
            if (count + row.row_count > 500) break;
            const transaction = JSON.parse(row.transaction_json) as TailTransaction;
            if (transaction.entries.length !== row.row_count || reshardJsonBytes(transaction) !== row.byte_size) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "stored staged-tail envelope is invalid" });
            }
            const nextEnvelopeBytes = envelopeBytes + (transactions.length === 0 ? 0 : 1) + row.byte_size;
            if (nextEnvelopeBytes > CDB_RESHARD_MAX_BATCH_BYTES) {
                if (transactions.length === 0)
                    throw new CdbError({ code: "CDB_INVARIANT", message: "stored staged-tail envelope is oversized" });
                break;
            }
            transactions.push(transaction);
            count += row.row_count;
            envelopeBytes = nextEnvelopeBytes;
        }
        assertTailTransactions(transactions);
        assertReshardEnvelopeBudget(transactions, "staged tail read");
        return { transactions };
    }

    async ackStagedTail(args: { migId: string; throughLsn: number }): Promise<{ removed: number }> {
        let removed = 0;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({ migId: args.migId, role: "dest", sql });
            const state = sql.one<{ applied_lsn: number }>(
                "SELECT applied_lsn FROM _chardb_split_state WHERE mig_id = ?",
                args.migId
            );
            if (!state || args.throughLsn > state.applied_lsn) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "staged tail ack exceeds applied cursor" });
            }
            const stats = sql.one<{ rows: number; bytes: number }>(
                `SELECT COALESCE(SUM(row_count), 0) AS rows, COALESCE(SUM(byte_size), 0) AS bytes
                 FROM _chardb_split_tail_inbox WHERE mig_id = ? AND last_lsn <= ?`,
                args.migId,
                args.throughLsn
            );
            if (!stats || stats.rows === 0) return;
            sql.exec(
                "DELETE FROM _chardb_split_tail_inbox WHERE mig_id = ? AND last_lsn <= ?",
                args.migId,
                args.throughLsn
            );
            sql.exec(
                `UPDATE _chardb_split_state SET inbox_rows = inbox_rows - ?, inbox_bytes = inbox_bytes - ?, updated_at = ?
                 WHERE mig_id = ? AND inbox_rows >= ? AND inbox_bytes >= ?`,
                stats.rows,
                stats.bytes,
                Date.now(),
                args.migId,
                stats.rows,
                stats.bytes
            );
            if (sql.changes() !== 1)
                throw new CdbError({ code: "CDB_INVARIANT", message: "staged inbox accounting changed" });
            removed = stats.rows;
        });
        return { removed };
    }

    async closeTailStaging(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ closed: boolean }> {
        let closed = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: { lo: args.rangeLo, hi: args.rangeHi },
                tables: args.tables,
                sql,
            });
            const state = sql.one<{ inbox_rows: number }>(
                "SELECT inbox_rows FROM _chardb_split_state WHERE mig_id = ?",
                args.migId
            );
            if (!state || state.inbox_rows !== 0) {
                throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message: "staged tail inbox is not empty" });
            }
            sql.exec(
                "UPDATE _chardb_split_state SET inbox_closed = 1, updated_at = ? WHERE mig_id = ? AND inbox_closed = 0",
                Date.now(),
                args.migId
            );
            closed = sql.changes() === 1;
        });
        return { closed };
    }

    /**
     * Apply one source-ordered, cross-table tail batch on the destination.
     * The data changes and durable LSN cursor commit in one transaction.
     * Inserts and updates use primary-key-aware upserts. Deletes use the exact
     * primary key recovered from the trigger pre-image.
     */
    async applyTailBatch(args: {
        migId: string;
        tables: readonly TableSpec[];
        range: RangeFilter;
        transactions: readonly TailTransaction[];
    }): Promise<{ applied: number; lastLsn: number }> {
        if (!Array.isArray(args.transactions)) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard tail transactions must be an array" });
        }
        const entries = args.transactions.flatMap(transaction => transaction.entries);
        if (entries.length > 500) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard tail batch exceeds 500 rows" });
        }
        assertReshardBatchBudget(entries, "reshard tail");
        assertReshardEnvelopeBudget(args.transactions, "reshard tail transaction");
        assertTailTransactions(args.transactions);
        let applied = 0;
        const lastLsn = args.transactions.at(-1)?.lastLsn ?? 0;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            sql.exec("PRAGMA defer_foreign_keys = ON");
            this.assertReshardMovement({
                migId: args.migId,
                role: "dest",
                range: args.range,
                tables: args.tables,
                sql,
            });
            const tablesByName = new Map(args.tables.map(table => [table.name, table]));
            const domainTableNames = new Set(tablesByName.keys());
            if (entries.some(entry => isCdbVectorTailTable(entry.table_name))) {
                const vectorSnapshot = sql.one<{ terminal: number; outcome: string }>(
                    `SELECT terminal, outcome FROM _chardb_vector_reshard_dest_sessions
                     WHERE mig_id = ? AND range_lo = ? AND range_hi = ?`,
                    args.migId,
                    args.range.lo,
                    args.range.hi
                );
                if (!vectorSnapshot || vectorSnapshot.terminal !== 1 || vectorSnapshot.outcome !== "active") {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: "vector tail cannot apply before its destination snapshot is terminal",
                    });
                }
            }
            const layouts = new Map(args.tables.map(table => [table.name, readReshardTableLayout(sql, table)]));
            let previousLsn = 0;
            const state = sql.one<{ applied_lsn: number; role: string }>(
                "SELECT applied_lsn, role FROM _chardb_split_state WHERE mig_id = ?",
                args.migId
            );
            if (!state || state.role !== "dest") {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination split state is missing",
                });
            }
            if (state.applied_lsn > 0) {
                const overlapping = args.transactions.find(
                    transaction => transaction.firstLsn <= state.applied_lsn && transaction.lastLsn > state.applied_lsn
                );
                if (overlapping) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "destination tail cursor splits a source transaction",
                    });
                }
            }
            const firstLsn = args.transactions[0]?.firstLsn;
            if (
                firstLsn !== undefined &&
                firstLsn > state.applied_lsn &&
                this.hasActiveVectorResources(sql) &&
                sql.one<{ present: number }>(
                    `SELECT 1 AS present FROM _chardb_vector_reshard_provenance_identity
                     WHERE mig_id = ? AND outcome = 'active'`,
                    args.migId
                )
            ) {
                new CdbVectorReshardProvenanceStore(sql).pruneReceipts(
                    { migId: args.migId, rangeLo: args.range.lo, rangeHi: args.range.hi },
                    state.applied_lsn,
                    500
                );
                if (
                    sql.one<{ present: number }>(
                        `SELECT 1 AS present FROM _chardb_split_vector_tail_applied
                         WHERE mig_id = ? AND lsn <= ? LIMIT 1`,
                        args.migId,
                        state.applied_lsn
                    )
                ) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "vector tail receipt replay window exceeded its batch bound",
                    });
                }
            }
            for (const e of entries) {
                if (!Number.isSafeInteger(e.lsn) || e.lsn <= previousLsn) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "reshard tail entries are not in strict source LSN order",
                    });
                }
                previousLsn = e.lsn;
                isKnownReshardTailTable(e.table_name, domainTableNames);
                if (e.lsn <= state.applied_lsn) {
                    if (isCdbVectorTailTable(e.table_name)) {
                        assertCdbVectorTailReplay(sql, args.migId, e, args.range);
                    }
                    continue;
                }
                if (e.op !== "ins" && e.op !== "upd" && e.op !== "del") {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "reshard tail entry has an unknown operation",
                    });
                }
                if (applyReshardSystemTailEntry(sql, args.migId, e, args.range)) {
                    applied++;
                    continue;
                }
                const table = tablesByName.get(e.table_name);
                if (!table) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: "known domain tail table is missing" });
                }
                if (!inRange(e.pk, args.range)) continue;
                if (e.op === "del") {
                    const before = parseJsonColumn("before", e.before);
                    if (!before) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: "reshard delete entry omitted its pre-image",
                        });
                    }
                    deleteReshardRow(sql, table, before, layouts.get(table.name));
                } else if (e.op === "upd") {
                    const before = parseJsonColumn("before", e.before);
                    const after = parseJsonColumn("after", e.after);
                    if (!before || !after) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: "reshard update entry omitted its pre-image or post-image",
                        });
                    }
                    applyReshardUpdate(sql, table, before, after, layouts.get(table.name));
                } else {
                    const row = parseJsonColumn("after", e.after);
                    if (!row) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: "reshard insert entry omitted its post-image",
                        });
                    }
                    applyReshardRow(sql, table, row, layouts.get(table.name));
                }
                applied++;
            }
            sql.exec(
                "UPDATE _chardb_split_state SET applied_lsn = MAX(applied_lsn, ?), updated_at = ? WHERE mig_id = ?",
                lastLsn,
                Date.now(),
                args.migId
            );
        });
        return { applied, lastLsn };
    }

    /** Freeze source capture after fenced convergence and before destructive drain. */
    async stopReshardCapture(args: Omit<CdbReshardSplitIdentity, "role">): Promise<{ stopped: boolean }> {
        let stopped = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "source",
                range: { lo: args.rangeLo, hi: args.rangeHi },
                tables: args.tables,
                sql,
            });
            const state = sql.one<{ drained: number; drain_started: number }>(
                "SELECT drained, drain_started FROM _chardb_split_state WHERE mig_id = ? AND role = 'source'",
                args.migId
            );
            if (!state || state.drained !== 0) {
                throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message: "source split cannot begin drain" });
            }
            for (const table of args.tables) {
                uninstallOwnedLegacyTableTriggers(sql, args.migId, table);
                for (const statement of renderTableTriggers(args.migId, table).uninstall) sql.exec(statement);
            }
            if (state.drain_started === 0) {
                sql.exec(
                    `UPDATE _chardb_split_state
                     SET capture = 0, drain_started = 1, capture_tx_id = NULL,
                         capture_tx_rows = 0, capture_tx_bytes = 0, updated_at = ?
                     WHERE mig_id = ? AND role = 'source' AND drained = 0 AND drain_started = 0`,
                    Date.now(),
                    args.migId
                );
                if (sql.changes() !== 1) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: "source drain state changed concurrently" });
                }
                stopped = true;
            }
        });
        return { stopped };
    }

    /**
     * Post-cutover, delete the migrated rows from the source and tear down
     * the per-table triggers + the split-state record. Idempotent: a
     * re-entry deletes nothing because the destination already owns the
     * migrated keys via the new range table.
     */
    async dropMigratedRange(args: {
        migId: string;
        table: TableSpec;
        range: RangeFilter;
        batchSize: number;
    }): Promise<{ deleted: number; done: boolean }> {
        if (!Number.isSafeInteger(args.batchSize) || args.batchSize < 1 || args.batchSize > 1_000) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "reshard drop batch size must be from 1 through 1000",
            });
        }
        const ident = quoteIdent(args.table.name);
        const pkCol = quoteIdent(args.table.partitionColumn);
        let deleted = 0;
        let done = true;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardMovement({
                migId: args.migId,
                role: "source",
                range: args.range,
                table: args.table,
                sql,
            });
            const drain = sql.one<{ drain_started: number }>(
                "SELECT drain_started FROM _chardb_split_state WHERE mig_id = ? AND role = 'source'",
                args.migId
            );
            if (drain?.drain_started !== 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "source capture must stop before range drain",
                });
            }
            const nowMs = Date.now();
            const currentMax =
                sql.one<{ max_rowid: number }>(`SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM ${ident}`)
                    ?.max_rowid ?? 0;
            sql.exec(
                `INSERT OR IGNORE INTO _chardb_split_drop_cursor
                 (mig_id, table_name, after_rowid, max_rowid, done, updated_at)
                 VALUES (?, ?, 0, ?, 0, ?)`,
                args.migId,
                args.table.name,
                currentMax,
                nowMs
            );
            sql.exec(
                `UPDATE _chardb_split_drop_cursor SET max_rowid = ?
                 WHERE mig_id = ? AND table_name = ? AND max_rowid IS NULL`,
                currentMax,
                args.migId,
                args.table.name
            );
            const cursor = sql.one<{ after_rowid: number; max_rowid: number; done: number }>(
                `SELECT after_rowid, max_rowid, done FROM _chardb_split_drop_cursor
                 WHERE mig_id = ? AND table_name = ?`,
                args.migId,
                args.table.name
            );
            if (!cursor) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "reshard drop cursor disappeared" });
            }
            if (cursor.done === 1) {
                done = true;
                return;
            }
            const rows = sql.all<{ rowid: number; pk: RawJson }>(
                `SELECT rowid, ${pkCol} AS pk FROM ${ident}
                 WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
                cursor.after_rowid,
                cursor.max_rowid,
                args.batchSize
            );
            const candidates: number[] = [];
            for (const r of rows) {
                if (inRange(r.pk, args.range)) candidates.push(r.rowid);
            }
            for (const rid of candidates) {
                sql.exec(`DELETE FROM ${ident} WHERE rowid = ?`, rid);
                deleted++;
            }
            done = rows.length < args.batchSize || (rows.at(-1)?.rowid ?? cursor.after_rowid) >= cursor.max_rowid;
            const afterRowid = rows.at(-1)?.rowid ?? cursor.after_rowid;
            sql.exec(
                `UPDATE _chardb_split_drop_cursor
                 SET after_rowid = ?, done = ?, updated_at = ?
                 WHERE mig_id = ? AND table_name = ? AND after_rowid = ? AND done = 0`,
                afterRowid,
                done ? 1 : 0,
                nowMs,
                args.migId,
                args.table.name,
                cursor.after_rowid
            );
            if (sql.changes() !== 1) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "reshard drop cursor changed during scan" });
            }
        });
        return { deleted, done };
    }

    /**
     * Tear down the per-migration triggers and mark the split-state row as
     * drained. After this call the source is clean of all migration artifacts.
     */
    async finishReshardSource(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardCleanupIdentity({ ...args, role: "source" }, sql);
            const state = this.readExactSplitState(sql, args, "source");
            if (!state) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "source split state is missing at success" });
            }
            if (state.drained === 1) return;
            for (const t of args.tables) {
                uninstallOwnedLegacyTableTriggers(sql, args.migId, t);
                const ts = renderTableTriggers(args.migId, t);
                for (const stmt of ts.uninstall) sql.exec(stmt);
            }
            for (const statement of renderReshardForeignKeyGuards(sql, args.migId, args.tables).uninstall) {
                sql.exec(statement);
            }
            uninstallOwnedLegacyReshardForeignKeyGuards(sql, args.migId, args.tables);
            sql.exec(
                `UPDATE _chardb_split_state
                 SET capture = 0, drained = 1, split_log_rows = 0, split_log_bytes = 0, updated_at = ?
                 WHERE mig_id = ?`,
                Date.now(),
                args.migId
            );
            sql.exec("DELETE FROM _chardb_split_log WHERE mig_id = ?", args.migId);
            finalizeSplitOpLogSource(sql, args.migId);
            sql.exec("DELETE FROM _chardb_split_drop_cursor WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_bulk_watermark WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_tail_inbox WHERE mig_id = ?", args.migId);
        });
        await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
    }

    /** Stop source capture and remove all transient artifacts before a pre-fence abort. */
    async abortReshardSource(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            const bound = this.assertReshardCleanupIdentity({ ...args, role: "source" }, sql);
            const state = this.readExactSplitState(sql, args, "source", !bound);
            if (!state) {
                this.persistReshardAbortTombstone(sql, args, "source");
                return;
            }
            if (state.drained === 1) return;
            for (const table of args.tables) {
                uninstallOwnedLegacyTableTriggers(sql, args.migId, table);
                for (const statement of renderTableTriggers(args.migId, table).uninstall) sql.exec(statement);
            }
            for (const statement of renderReshardForeignKeyGuards(sql, args.migId, args.tables).uninstall) {
                sql.exec(statement);
            }
            uninstallOwnedLegacyReshardForeignKeyGuards(sql, args.migId, args.tables);
            sql.exec("DELETE FROM _chardb_split_log WHERE mig_id = ?", args.migId);
            finalizeSplitOpLogSource(sql, args.migId);
            sql.exec("DELETE FROM _chardb_split_drop_cursor WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_bulk_watermark WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_tail_inbox WHERE mig_id = ?", args.migId);
            sql.exec(
                `UPDATE _chardb_split_state
                 SET capture = 0, drained = 1, split_log_rows = 0, split_log_bytes = 0, updated_at = ?
                 WHERE mig_id = ?`,
                Date.now(),
                args.migId
            );
        });
        await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
    }

    /** Permanently fence destination movement RPCs before bounded abort cleanup starts. */
    async beginReshardDestAbort(
        args: Omit<CdbReshardSplitIdentity, "role"> & { destinationGeneration: number }
    ): Promise<{ started: boolean }> {
        assertCdbReshardRangeIdentity(args);
        if (!Number.isSafeInteger(args.destinationGeneration) || args.destinationGeneration < 1) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "destination generation must be a positive integer",
            });
        }
        let started = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            const bound = this.assertReshardCleanupIdentity({ ...args, role: "dest" }, sql);
            if (!bound) {
                const prepared = sql.one<{
                    role: string;
                    range_lo: number;
                    range_hi: number;
                    destination_generation: number | null;
                    destination_serving: number;
                    drained: number;
                    abort_started: number;
                }>(
                    `SELECT role, range_lo, range_hi, destination_generation, destination_serving, drained, abort_started
                     FROM _chardb_split_state WHERE mig_id = ?`,
                    args.migId
                );
                if (prepared) {
                    if (
                        prepared.role !== "dest" ||
                        prepared.range_lo !== args.rangeLo ||
                        prepared.range_hi !== args.rangeHi ||
                        prepared.destination_generation !== args.destinationGeneration ||
                        prepared.destination_serving !== 0
                    ) {
                        throw new CdbError({
                            code: "CDB_RESHARD_PHASE_MISMATCH",
                            message: "unbound destination abort does not match prepared ownership",
                        });
                    }
                    if (prepared.drained === 1 && prepared.abort_started === 1) return;
                    sql.exec(
                        `UPDATE _chardb_split_state
                         SET abort_started = 1, capture = 0, bulk_done = 1, drained = 1, updated_at = ?
                         WHERE mig_id = ? AND role = 'dest' AND destination_serving = 0
                           AND drained = 0 AND abort_started = 0`,
                        Date.now(),
                        args.migId
                    );
                    if (sql.changes() !== 1) {
                        throw new CdbError({
                            code: "CDB_INVARIANT",
                            message: "prepared destination abort changed concurrently",
                        });
                    }
                    started = true;
                    return;
                }
            }
            const state = this.readExactSplitState(sql, args, "dest", !bound);
            if (!state) {
                this.persistReshardAbortTombstone(sql, args, "dest", args.destinationGeneration);
                return;
            }
            const ownership = sql.one<{ destination_serving: number }>(
                "SELECT destination_serving FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
                args.migId
            );
            if (ownership?.destination_serving === 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "serving destination cannot enter pre-cutover abort cleanup",
                });
            }
            if (state.drained === 1 || state.abortStarted === 1) return;
            sql.exec(
                `UPDATE _chardb_split_state
                 SET abort_started = 1, capture = 0, bulk_done = 1, updated_at = ?
                 WHERE mig_id = ? AND role = 'dest' AND drained = 0 AND abort_started = 0`,
                Date.now(),
                args.migId
            );
            if (sql.changes() !== 1) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "destination abort fence changed concurrently" });
            }
            started = true;
        });
        return { started };
    }

    /** Delete one bounded child-before-parent destination abort batch. */
    async abortReshardDestBatch(
        args: Omit<CdbReshardSplitIdentity, "role"> & { batchSize: number }
    ): Promise<{ deleted: number; done: boolean }> {
        if (!Number.isSafeInteger(args.batchSize) || args.batchSize < 1 || args.batchSize > 1_000) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "reshard abort batch size must be from 1 through 1000",
            });
        }
        let deleted = 0;
        let done = false;
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            const bound = this.assertReshardCleanupIdentity({ ...args, role: "dest" }, sql);
            const state = this.readExactSplitState(sql, args, "dest", !bound);
            if (!state) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination abort cleanup cannot run before its abort fence starts",
                });
            }
            if (state.drained === 1) {
                done = true;
                return;
            }
            if (state.abortStarted !== 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination abort fence must start before cleanup",
                });
            }
            const ordered = [...orderReshardTables(sql, args.tables)].reverse();
            const table = ordered.find(candidate => {
                const cursor = sql.one<{ done: number }>(
                    "SELECT done FROM _chardb_split_drop_cursor WHERE mig_id = ? AND table_name = ?",
                    args.migId,
                    candidate.name
                );
                return cursor?.done !== 1;
            });
            if (table) {
                const nowMs = Date.now();
                const ident = quoteIdent(table.name);
                const currentMax =
                    sql.one<{ max_rowid: number }>(`SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM ${ident}`)
                        ?.max_rowid ?? 0;
                sql.exec(
                    `INSERT OR IGNORE INTO _chardb_split_drop_cursor
                     (mig_id, table_name, after_rowid, max_rowid, done, updated_at) VALUES (?, ?, 0, ?, 0, ?)`,
                    args.migId,
                    table.name,
                    currentMax,
                    nowMs
                );
                sql.exec(
                    `UPDATE _chardb_split_drop_cursor SET max_rowid = ?
                     WHERE mig_id = ? AND table_name = ? AND max_rowid IS NULL`,
                    currentMax,
                    args.migId,
                    table.name
                );
                const cursor = sql.one<{ after_rowid: number; max_rowid: number }>(
                    "SELECT after_rowid, max_rowid FROM _chardb_split_drop_cursor WHERE mig_id = ? AND table_name = ?",
                    args.migId,
                    table.name
                );
                if (!cursor) throw new CdbError({ code: "CDB_INVARIANT", message: "abort cursor disappeared" });
                const partition = quoteIdent(table.partitionColumn);
                const rows = sql.all<{ rowid: number; pk: RawJson }>(
                    `SELECT rowid, ${partition} AS pk FROM ${ident}
                     WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
                    cursor.after_rowid,
                    cursor.max_rowid,
                    args.batchSize
                );
                for (const row of rows) {
                    if (!inRange(row.pk, { lo: args.rangeLo, hi: args.rangeHi })) continue;
                    sql.exec(`DELETE FROM ${ident} WHERE rowid = ?`, row.rowid);
                    deleted++;
                }
                const tableDone =
                    rows.length < args.batchSize || (rows.at(-1)?.rowid ?? cursor.after_rowid) >= cursor.max_rowid;
                sql.exec(
                    `UPDATE _chardb_split_drop_cursor SET after_rowid = ?, done = ?, updated_at = ?
                     WHERE mig_id = ? AND table_name = ? AND after_rowid = ?`,
                    rows.at(-1)?.rowid ?? cursor.after_rowid,
                    tableDone ? 1 : 0,
                    nowMs,
                    args.migId,
                    table.name,
                    cursor.after_rowid
                );
                return;
            }
            finalizeSplitOpLogDestination(sql, args.migId, true);
            sql.exec("DELETE FROM _chardb_split_log WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_oplog_key WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_oplog WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_drop_cursor WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_bulk_watermark WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_tail_inbox WHERE mig_id = ?", args.migId);
            sql.exec(
                `UPDATE _chardb_split_state
                 SET capture = 0, abort_started = 1, drained = 1,
                     split_log_rows = 0, split_log_bytes = 0,
                     inbox_rows = 0, inbox_bytes = 0, inbox_closed = 1, updated_at = ?
                 WHERE mig_id = ?`,
                Date.now(),
                args.migId
            );
            done = true;
        });
        if (done) await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
        return { deleted, done };
    }

    /** Mark a successful destination split drained and release transfer-only state. */
    async finishReshardDest(args: Omit<CdbReshardSplitIdentity, "role">): Promise<void> {
        this.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.storage.sql);
            this.assertReshardCleanupIdentity({ ...args, role: "dest" }, sql);
            const state = this.readExactSplitState(sql, args, "dest");
            if (!state) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "destination split state is missing at success" });
            }
            if (state.abortStarted === 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination split cannot finish after abort started",
                });
            }
            const ownership = sql.one<{ destination_serving: number }>(
                "SELECT destination_serving FROM _chardb_split_state WHERE mig_id = ? AND role = 'dest'",
                args.migId
            );
            if (ownership?.destination_serving !== 1) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: "destination split cannot finish before Catalog cutover activation",
                });
            }
            finalizeSplitOpLogDestination(sql, args.migId, false);
            sql.exec("DELETE FROM _chardb_split_log WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_oplog_key WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_oplog WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_drop_cursor WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_bulk_watermark WHERE mig_id = ?", args.migId);
            sql.exec("DELETE FROM _chardb_split_tail_inbox WHERE mig_id = ?", args.migId);
            sql.exec(
                `UPDATE _chardb_split_state SET capture = 0, drained = 1,
                 inbox_rows = 0, inbox_bytes = 0, inbox_closed = 1, updated_at = ? WHERE mig_id = ?`,
                Date.now(),
                args.migId
            );
        });
        await this.scheduleAlarmNoLaterThan(this.invalidationNowMs() + 1);
    }

    assertReshardMovement(args: {
        readonly migId: string;
        readonly role: "source" | "dest";
        readonly range?: RangeFilter;
        readonly table?: TableSpec;
        readonly tables?: readonly TableSpec[];
        readonly sql?: SyncSql;
    }) {
        const sql = args.sql ?? adaptSqlStorage(this.storage.sql);
        const identity = this.reshardIdentities.assertMovement({
            ...args,
            schema: this.schema(),
            state: this.schemaMigrations.state(sql),
            journal: this.journal(),
        });
        const split = sql.one<{
            role: string;
            range_lo: number;
            range_hi: number;
            drained: number;
            abort_started: number;
        }>(
            "SELECT role, range_lo, range_hi, drained, abort_started FROM _chardb_split_state WHERE mig_id = ?",
            args.migId
        );
        if (
            !split ||
            split.role !== args.role ||
            split.range_lo !== identity.rangeLo ||
            split.range_hi !== identity.rangeHi ||
            split.drained !== 0 ||
            split.abort_started !== 0
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} is no longer active on its ${args.role} Cdb`,
            });
        }
        return identity;
    }

    assertReshardCleanupIdentity(identity: CdbReshardSplitIdentity, sql: SyncSql) {
        return this.reshardIdentities.assertCleanupIfBound(
            identity,
            this.schema(),
            this.schemaMigrations.state(sql),
            this.journal()
        );
    }

    assertReshardBeginAllowed(
        sql: SyncSql,
        args: { readonly migId: string; readonly rangeLo: number; readonly rangeHi: number },
        role: "source" | "dest"
    ): void {
        const state = sql.one<{
            role: string;
            range_lo: number;
            range_hi: number;
            drained: number;
            abort_started: number;
        }>(
            "SELECT role, range_lo, range_hi, drained, abort_started FROM _chardb_split_state WHERE mig_id = ?",
            args.migId
        );
        if (
            state &&
            (state.role !== role ||
                state.range_lo !== args.rangeLo ||
                state.range_hi !== args.rangeHi ||
                state.drained === 1 ||
                state.abort_started === 1)
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `${role} split id belongs to a different immutable identity or a canceled split`,
            });
        }
    }

    persistReshardAbortTombstone(
        sql: SyncSql,
        args: { readonly migId: string; readonly rangeLo: number; readonly rangeHi: number },
        role: "source" | "dest",
        destinationGeneration: number | null = null
    ): void {
        assertCdbReshardRangeIdentity(args);
        if (
            (role === "dest" && (!Number.isSafeInteger(destinationGeneration) || (destinationGeneration ?? 0) < 1)) ||
            (role === "source" && destinationGeneration !== null)
        ) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "abort tombstone destination generation is invalid",
            });
        }
        assertCdbSplitHistoryCapacity(sql, args.migId);
        sql.exec(
            `INSERT OR IGNORE INTO _chardb_split_state
             (mig_id, range_lo, range_hi, role, capture, abort_started, drained,
              destination_generation, destination_serving, updated_at)
             VALUES (?, ?, ?, ?, 0, 1, 1, ?, 0, ?)`,
            args.migId,
            args.rangeLo,
            args.rangeHi,
            role,
            destinationGeneration,
            Date.now()
        );
        const state = sql.one<{ role: string; range_lo: number; range_hi: number; drained: number }>(
            "SELECT role, range_lo, range_hi, drained FROM _chardb_split_state WHERE mig_id = ?",
            args.migId
        );
        if (
            !state ||
            state.role !== role ||
            state.range_lo !== args.rangeLo ||
            state.range_hi !== args.rangeHi ||
            state.drained !== 1
        ) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} abort tombstone conflicts with its ${role} split state`,
            });
        }
    }

    readExactSplitState(
        sql: SyncSql,
        args: { readonly migId: string; readonly rangeLo: number; readonly rangeHi: number },
        role: "source" | "dest",
        allowMissingIdentity = false
    ): { readonly drained: number; readonly abortStarted: number } | null {
        const state = sql.one<{
            role: string;
            range_lo: number;
            range_hi: number;
            drained: number;
            abort_started: number;
        }>(
            "SELECT role, range_lo, range_hi, drained, abort_started FROM _chardb_split_state WHERE mig_id = ?",
            args.migId
        );
        if (!state) return null;
        if (state.role !== role || state.range_lo !== args.rangeLo || state.range_hi !== args.rangeHi) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} cleanup does not match its ${role} split state`,
            });
        }
        if (allowMissingIdentity && state.drained !== 1) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `migration ${args.migId} has active ${role} split state without a bound identity`,
            });
        }
        return { drained: state.drained, abortStarted: state.abort_started };
    }
}

export interface TailEntry {
    source_tx_id: number;
    lsn: number;
    op: "ins" | "upd" | "del";
    table_name: string;
    pk: string;
    /** `json_object(...)` of the post-image; `null` for `del` ops. */
    after: JsonText | null;
    /** `json_object(...)` of the pre-image (currently only set for `del`). */
    before: JsonText | null;
}

export interface TailTransaction {
    sourceTxId: number;
    firstLsn: number;
    lastLsn: number;
    entries: readonly TailEntry[];
}

function assertTailTransactions(transactions: readonly TailTransaction[]): void {
    const seen = new Set<number>();
    let previousLsn: number | null = null;
    for (const transaction of transactions) {
        if (
            !Number.isSafeInteger(transaction.sourceTxId) ||
            transaction.sourceTxId === 0 ||
            !Number.isSafeInteger(transaction.firstLsn) ||
            !Number.isSafeInteger(transaction.lastLsn) ||
            transaction.entries.length === 0 ||
            transaction.entries[0]?.lsn !== transaction.firstLsn ||
            transaction.entries.at(-1)?.lsn !== transaction.lastLsn ||
            transaction.lastLsn - transaction.firstLsn + 1 !== transaction.entries.length ||
            (previousLsn !== null && transaction.firstLsn !== previousLsn + 1) ||
            seen.has(transaction.sourceTxId)
        ) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "reshard tail transaction envelope is invalid" });
        }
        assertReshardSourceTransactionId(transaction.sourceTxId);
        seen.add(transaction.sourceTxId);
        for (const entry of transaction.entries) {
            if (
                entry.source_tx_id !== transaction.sourceTxId ||
                (previousLsn !== null && entry.lsn !== previousLsn + 1)
            ) {
                throw new CdbError({ code: "CDB_INVARIANT", message: "reshard tail transaction entries are invalid" });
            }
            previousLsn = entry.lsn;
        }
    }
}

const ALLOWED_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

function quoteIdent(raw: string): string {
    if (!ALLOWED_IDENT.test(raw)) throw new Error(`reshard: refusing identifier: ${raw}`);
    return `"${raw}"`;
}
