import { CdbError } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { VSHARD_COUNT } from "../../vshard.ts";
import {
    type CdbReshardFileRecord,
    type CdbReshardOrganizationTombstone,
    type CdbReshardStoredFileRow as StoredFileRow,
    type CdbReshardStoredTombstoneRow as StoredTombstoneRow,
    exactCdbReshardFile as exactFile,
    exactCdbReshardTombstone as exactTombstone,
    projectCdbReshardFileRecord as projectFile,
    projectCdbReshardTombstone as projectTombstone,
} from "./cdb-file-reshard-record.ts";
import { assertCdbReshardRangeIdentity } from "./cdb-reshard-identity-store.ts";
import { assertReshardBatchBudget } from "./cdb-reshard-relational.ts";

export type { CdbReshardFileRecord, CdbReshardOrganizationTombstone } from "./cdb-file-reshard-record.ts";

export const CDB_FILE_RESHARD_PAGE_SIZE = 500;

export const CDB_FILE_RESHARD_STORE_DDL = `
CREATE TABLE IF NOT EXISTS _chardb_split_file_cursor (
  mig_id TEXT PRIMARY KEY,
  range_lo INTEGER NOT NULL CHECK (range_lo >= 0 AND range_lo < 16384),
  range_hi INTEGER NOT NULL CHECK (range_hi >= range_lo AND range_hi < 16384),
  role TEXT NOT NULL CHECK (role IN ('source', 'dest')),
  outcome TEXT NOT NULL DEFAULT 'active' CHECK (outcome IN ('active', 'aborted', 'finished')),
  maintenance_enabled INTEGER NOT NULL CHECK (maintenance_enabled IN (0, 1)),
  attachments_enabled INTEGER NOT NULL DEFAULT 0 CHECK (attachments_enabled IN (0, 1)),
  source_fenced INTEGER NOT NULL DEFAULT 0 CHECK (source_fenced IN (0, 1)),
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);
CREATE TABLE IF NOT EXISTS _chardb_split_file_applied (
  mig_id TEXT NOT NULL,
  record_kind TEXT NOT NULL CHECK (record_kind IN ('file', 'organization_tombstone')),
  record_id TEXT NOT NULL,
  inserted INTEGER NOT NULL CHECK (inserted IN (0, 1)),
  snapshot_through_lsn INTEGER CHECK (snapshot_through_lsn IS NULL OR snapshot_through_lsn >= 0),
  PRIMARY KEY (mig_id, record_kind, record_id)
);
` as const;

export interface CdbFileReshardIdentity {
    readonly migId: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
}

export interface CdbFileReshardPage<T> {
    readonly rows: readonly T[];
    readonly afterPlacement: number;
    readonly afterId: string;
    readonly done: boolean;
}

export interface CdbFileReshardDrainCursor {
    readonly kind: "file" | "organization_tombstone";
    readonly afterPlacement: number;
    readonly afterId: string;
}

export interface CdbFileReshardParityPage {
    readonly kind: CdbFileReshardDrainCursor["kind"];
    readonly rows: readonly (CdbReshardFileRecord | CdbReshardOrganizationTombstone)[];
    readonly cursor: CdbFileReshardDrainCursor;
    readonly done: boolean;
}

interface StoredCursor {
    readonly mig_id: string;
    readonly range_lo: number;
    readonly range_hi: number;
    readonly role: "source" | "dest";
    readonly outcome: "active" | "aborted" | "finished";
    readonly maintenance_enabled: number;
    readonly attachments_enabled: number;
    readonly source_fenced: number;
}

function mismatch(message: string): never {
    throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message });
}

function invalid(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message: `file reshard: ${message}` });
}

function safeInteger(value: number | bigint, subject: string, minimum = 0): number {
    const number = Number(value);
    if (!Number.isSafeInteger(number) || number < minimum) mismatch(`${subject} is invalid`);
    return number;
}

function assertPageInput(
    identity: CdbFileReshardIdentity,
    afterPlacement: number,
    afterId: string,
    limit: number
): void {
    const atStart = afterPlacement === -1 && afterId === "";
    if (
        !Number.isSafeInteger(afterPlacement) ||
        (!atStart && (afterPlacement < identity.rangeLo || afterPlacement > identity.rangeHi)) ||
        (afterPlacement === -1 && afterId !== "") ||
        typeof afterId !== "string" ||
        new TextEncoder().encode(afterId).byteLength > 256 ||
        limit !== CDB_FILE_RESHARD_PAGE_SIZE
    ) {
        invalid(`page cursor is invalid or limit is not exactly ${CDB_FILE_RESHARD_PAGE_SIZE}`);
    }
}

export function initializeCdbFileReshardStore(sql: SyncSql): void {
    for (const statement of CDB_FILE_RESHARD_STORE_DDL.split(";")
        .map(value => value.trim())
        .filter(Boolean)) {
        sql.exec(statement);
    }
    const cursorColumns = sql.all<{ name: string }>("PRAGMA table_info('_chardb_split_file_cursor')");
    if (!cursorColumns.some(column => column.name === "attachments_enabled")) {
        sql.exec(
            `ALTER TABLE _chardb_split_file_cursor ADD COLUMN attachments_enabled INTEGER NOT NULL DEFAULT 0
             CHECK (attachments_enabled IN (0, 1))`
        );
    }
    const appliedColumns = sql.all<{ name: string }>("PRAGMA table_info('_chardb_split_file_applied')");
    if (!appliedColumns.some(column => column.name === "snapshot_through_lsn")) {
        sql.exec(
            `ALTER TABLE _chardb_split_file_applied ADD COLUMN snapshot_through_lsn INTEGER
             CHECK (snapshot_through_lsn IS NULL OR snapshot_through_lsn >= 0)`
        );
    }
}

/**
 * Durable file ownership operations. Every mutating page method must run in
 * the caller's `transactionSync`; the store never exposes a partial page as a
 * recoverable state.
 */
export class CdbFileReshardStore {
    readonly sql: SyncSql;

    constructor(sql: SyncSql) {
        this.sql = sql;
    }

    beginSource(identity: CdbFileReshardIdentity, nowMs: number): void {
        this.begin(identity, "source", 1, nowMs);
    }

    beginDest(identity: CdbFileReshardIdentity, nowMs: number): void {
        this.begin(identity, "dest", 0, nowMs);
    }

    appliedProvenance(identity: CdbFileReshardIdentity): { readonly rows: number; readonly legacyRows: number } {
        this.assertBound(identity, "dest");
        const row = this.sql.one<{ rows: number | bigint; legacy_rows: number | bigint }>(
            `SELECT COUNT(*) AS rows,
                    COALESCE(SUM(CASE WHEN snapshot_through_lsn IS NULL THEN 1 ELSE 0 END), 0) AS legacy_rows
             FROM _chardb_split_file_applied WHERE mig_id = ?`,
            identity.migId
        );
        return Object.freeze({
            rows: safeInteger(row?.rows ?? 0, "file reshard provenance row count"),
            legacyRows: safeInteger(row?.legacy_rows ?? 0, "legacy file reshard provenance row count"),
        });
    }

    readSnapshot(
        input: CdbFileReshardIdentity & { afterPlacement: number; afterFileId: string; limit: number }
    ): CdbFileReshardPage<CdbReshardFileRecord> {
        this.assertActive(input, "source");
        assertPageInput(input, input.afterPlacement, input.afterFileId, input.limit);
        const rows = this.sql
            .all<StoredFileRow>(
                `SELECT * FROM _chardb_files
                 WHERE placement_vshard BETWEEN ? AND ?
                   AND (placement_vshard > ? OR (placement_vshard = ? AND file_id > ?))
                 ORDER BY placement_vshard, file_id LIMIT ?`,
                input.rangeLo,
                input.rangeHi,
                input.afterPlacement,
                input.afterPlacement,
                input.afterFileId,
                CDB_FILE_RESHARD_PAGE_SIZE + 1
            )
            .map(projectFile);
        const page = rows.slice(0, CDB_FILE_RESHARD_PAGE_SIZE);
        assertReshardBatchBudget(page, "file reshard snapshot");
        const last = page.at(-1);
        return Object.freeze({
            rows: page,
            afterPlacement: last?.placementVshard ?? input.afterPlacement,
            afterId: last?.fileId ?? input.afterFileId,
            done: rows.length <= CDB_FILE_RESHARD_PAGE_SIZE,
        });
    }

    applySnapshot(
        identity: CdbFileReshardIdentity,
        rows: readonly CdbReshardFileRecord[],
        throughLsn = 0
    ): { applied: number; inserted: number } {
        this.assertActive(identity, "dest");
        this.assertBatch(rows);
        const watermark = safeInteger(throughLsn, "file snapshot tail watermark");
        let inserted = 0;
        for (const raw of rows) {
            const row = projectFile({
                file_id: raw.fileId,
                organization_id: raw.organizationId,
                table_name: raw.table,
                column_name: raw.column,
                object_key: raw.objectKey,
                content_type: raw.contentType,
                size: raw.size,
                sha256: raw.sha256,
                status: raw.status,
                row_id: raw.rowId,
                created_at: raw.createdAt,
                updated_at: raw.updatedAt,
                placement_vshard: raw.placementVshard,
            });
            this.assertInRange(identity, row.placementVshard);
            const ledger = this.applied(identity.migId, "file", row.fileId);
            const existing = this.readFile(row.fileId);
            if (ledger) {
                if (ledger.snapshot_through_lsn === null) {
                    mismatch(`file ${row.fileId} was created by tail before snapshot apply`);
                }
                if (watermark < ledger.snapshot_through_lsn)
                    mismatch(`file ${row.fileId} snapshot watermark regressed`);
                if (!existing) mismatch(`file ${row.fileId} disappeared after snapshot apply`);
                if (!exactFile(existing, row)) {
                    if (watermark === ledger.snapshot_through_lsn) {
                        mismatch(`file ${row.fileId} changed at the same snapshot watermark`);
                    }
                    this.updateFile(row);
                }
                this.updateSnapshotWatermark(identity.migId, "file", row.fileId, watermark);
                continue;
            }
            if (existing) mismatch(`destination file ${row.fileId} predates this fresh-destination migration`);
            this.insertFile(row);
            inserted++;
            this.recordApplied(identity.migId, "file", row.fileId, 1, watermark);
        }
        return Object.freeze({ applied: rows.length, inserted });
    }

    readTombstones(
        input: CdbFileReshardIdentity & { afterPlacement: number; afterOrganizationId: string; limit: number }
    ): CdbFileReshardPage<CdbReshardOrganizationTombstone> {
        this.assertActive(input, "source");
        assertPageInput(input, input.afterPlacement, input.afterOrganizationId, input.limit);
        const rows = this.sql
            .all<StoredTombstoneRow>(
                `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
                 WHERE placement_vshard BETWEEN ? AND ?
                   AND (placement_vshard > ? OR (placement_vshard = ? AND organization_id > ?))
                 ORDER BY placement_vshard, organization_id LIMIT ?`,
                input.rangeLo,
                input.rangeHi,
                input.afterPlacement,
                input.afterPlacement,
                input.afterOrganizationId,
                CDB_FILE_RESHARD_PAGE_SIZE + 1
            )
            .map(projectTombstone);
        const page = rows.slice(0, CDB_FILE_RESHARD_PAGE_SIZE);
        assertReshardBatchBudget(page, "file reshard tombstones");
        const last = page.at(-1);
        return Object.freeze({
            rows: page,
            afterPlacement: last?.placementVshard ?? input.afterPlacement,
            afterId: last?.organizationId ?? input.afterOrganizationId,
            done: rows.length <= CDB_FILE_RESHARD_PAGE_SIZE,
        });
    }

    applyTombstones(
        identity: CdbFileReshardIdentity,
        rows: readonly CdbReshardOrganizationTombstone[],
        throughLsn = 0
    ): { applied: number; inserted: number } {
        this.assertActive(identity, "dest");
        this.assertBatch(rows);
        const watermark = safeInteger(throughLsn, "file tombstone tail watermark");
        let inserted = 0;
        for (const raw of rows) {
            const row = projectTombstone({
                organization_id: raw.organizationId,
                deleted_at: raw.deletedAt,
                placement_vshard: raw.placementVshard,
                vector_unproven_turns: raw.vectorUnprovenTurns,
            });
            this.assertInRange(identity, row.placementVshard);
            const ledger = this.applied(identity.migId, "organization_tombstone", row.organizationId);
            const existing = this.readTombstone(row.organizationId);
            if (ledger) {
                if (ledger.snapshot_through_lsn === null) {
                    mismatch(`organization tombstone ${row.organizationId} was created by tail before snapshot apply`);
                }
                if (watermark < ledger.snapshot_through_lsn) {
                    mismatch(`organization tombstone ${row.organizationId} snapshot watermark regressed`);
                }
                if (!existing) mismatch(`organization tombstone ${row.organizationId} disappeared after apply`);
                if (!exactTombstone(existing, row)) {
                    if (
                        watermark === ledger.snapshot_through_lsn ||
                        row.vectorUnprovenTurns < existing.vectorUnprovenTurns ||
                        !exactTombstone({ ...existing, vectorUnprovenTurns: row.vectorUnprovenTurns }, row)
                    ) {
                        mismatch(`organization tombstone ${row.organizationId} changed after apply`);
                    }
                    this.sql.exec(
                        "UPDATE _chardb_deleted_organizations SET vector_unproven_turns = ? WHERE organization_id = ?",
                        row.vectorUnprovenTurns,
                        row.organizationId
                    );
                    if (this.sql.changes() !== 1)
                        mismatch(`organization tombstone ${row.organizationId} disappeared after apply`);
                }
                this.updateSnapshotWatermark(identity.migId, "organization_tombstone", row.organizationId, watermark);
                continue;
            }
            if (existing) {
                mismatch(`destination organization tombstone ${row.organizationId} predates this migration`);
            }
            this.insertTombstone(row);
            inserted++;
            this.recordApplied(identity.migId, "organization_tombstone", row.organizationId, 1, watermark);
        }
        return Object.freeze({ applied: rows.length, inserted });
    }

    fenceSource(identity: CdbFileReshardIdentity, nowMs: number): void {
        const cursor = this.assertActive(identity, "source");
        if (cursor.source_fenced === 1) return;
        this.sql.exec(
            `UPDATE _chardb_split_file_cursor SET source_fenced = 1, maintenance_enabled = 0, updated_at = ?
             WHERE mig_id = ? AND outcome = 'active'`,
            safeInteger(nowMs, "file reshard fence time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("source file fence lost its ownership row");
    }

    activateDest(identity: CdbFileReshardIdentity, nowMs: number): { readonly activated: boolean } {
        const cursor = this.assertActive(identity, "dest");
        if (cursor.maintenance_enabled === 1) return Object.freeze({ activated: false });
        if (cursor.attachments_enabled !== 1) mismatch("destination file attachments are not prepared");
        this.sql.exec(
            `UPDATE _chardb_split_file_cursor SET maintenance_enabled = 1, updated_at = ?
             WHERE mig_id = ? AND role = 'dest' AND outcome = 'active'`,
            safeInteger(nowMs, "file reshard activation time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("destination file activation lost its ownership row");
        return Object.freeze({ activated: true });
    }

    prepareDestAttachments(identity: CdbFileReshardIdentity, nowMs: number): { readonly prepared: boolean } {
        const cursor = this.assertActive(identity, "dest");
        if (cursor.maintenance_enabled === 1 && cursor.attachments_enabled !== 1) {
            mismatch("active destination file ownership lost its attachment triggers");
        }
        if (cursor.attachments_enabled === 1) return Object.freeze({ prepared: false });
        this.sql.exec(
            `UPDATE _chardb_split_file_cursor SET attachments_enabled = 1, updated_at = ?
             WHERE mig_id = ? AND role = 'dest' AND outcome = 'active' AND maintenance_enabled = 0`,
            safeInteger(nowMs, "file reshard attachment preparation time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("destination attachment preparation lost its ownership row");
        return Object.freeze({ prepared: true });
    }

    stopSourceAttachments(identity: CdbFileReshardIdentity, nowMs: number): { readonly stopped: boolean } {
        const cursor = this.assertActive(identity, "source");
        if (cursor.source_fenced !== 1) mismatch("source file attachments cannot stop before its ownership fence");
        if (cursor.attachments_enabled === 0) return Object.freeze({ stopped: false });
        this.sql.exec(
            `UPDATE _chardb_split_file_cursor SET attachments_enabled = 0, updated_at = ?
             WHERE mig_id = ? AND role = 'source' AND outcome = 'active' AND source_fenced = 1`,
            safeInteger(nowMs, "file reshard attachment stop time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("source attachment stop lost its ownership row");
        return Object.freeze({ stopped: true });
    }

    validate(
        identity: CdbFileReshardIdentity,
        cursor: CdbFileReshardDrainCursor,
        limit: number
    ): { readonly cursor: CdbFileReshardDrainCursor; readonly checked: number; readonly done: boolean } {
        this.assertActive(identity, "dest");
        assertPageInput(identity, cursor.afterPlacement, cursor.afterId, limit);
        if (cursor.kind === "file") {
            const rows = this.sql.all<StoredFileRow>(
                `SELECT * FROM _chardb_files
                 WHERE placement_vshard BETWEEN ? AND ?
                   AND (placement_vshard > ? OR (placement_vshard = ? AND file_id > ?))
                 ORDER BY placement_vshard, file_id LIMIT ?`,
                identity.rangeLo,
                identity.rangeHi,
                cursor.afterPlacement,
                cursor.afterPlacement,
                cursor.afterId,
                limit
            );
            for (const row of rows) projectFile(row);
            if (rows.length === limit) {
                const last = projectFile(rows.at(-1) as StoredFileRow);
                return {
                    cursor: { kind: "file", afterPlacement: last.placementVshard, afterId: last.fileId },
                    checked: rows.length,
                    done: false,
                };
            }
            return {
                cursor: { kind: "organization_tombstone", afterPlacement: -1, afterId: "" },
                checked: rows.length,
                done: false,
            };
        }
        const rows = this.sql.all<StoredTombstoneRow>(
            `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
             WHERE placement_vshard BETWEEN ? AND ?
               AND (placement_vshard > ? OR (placement_vshard = ? AND organization_id > ?))
             ORDER BY placement_vshard, organization_id LIMIT ?`,
            identity.rangeLo,
            identity.rangeHi,
            cursor.afterPlacement,
            cursor.afterPlacement,
            cursor.afterId,
            limit
        );
        for (const row of rows) projectTombstone(row);
        const last = rows.length > 0 ? projectTombstone(rows.at(-1) as StoredTombstoneRow) : null;
        return {
            cursor: {
                kind: "organization_tombstone",
                afterPlacement: last?.placementVshard ?? cursor.afterPlacement,
                afterId: last?.organizationId ?? cursor.afterId,
            },
            checked: rows.length,
            done: rows.length < limit,
        };
    }

    readParityPage(
        identity: CdbFileReshardIdentity,
        role: "source" | "dest",
        cursor: CdbFileReshardDrainCursor,
        limit: number
    ): CdbFileReshardParityPage {
        this.assertActive(identity, role);
        assertPageInput(identity, cursor.afterPlacement, cursor.afterId, limit);
        if (cursor.kind === "file") {
            const rows = this.sql
                .all<StoredFileRow>(
                    `SELECT * FROM _chardb_files
                     WHERE placement_vshard BETWEEN ? AND ?
                       AND (placement_vshard > ? OR (placement_vshard = ? AND file_id > ?))
                     ORDER BY placement_vshard, file_id LIMIT ?`,
                    identity.rangeLo,
                    identity.rangeHi,
                    cursor.afterPlacement,
                    cursor.afterPlacement,
                    cursor.afterId,
                    limit
                )
                .map(projectFile);
            assertReshardBatchBudget(rows, "file reshard parity");
            if (rows.length === limit) {
                const last = rows.at(-1) as CdbReshardFileRecord;
                return {
                    kind: "file",
                    rows,
                    cursor: { kind: "file", afterPlacement: last.placementVshard, afterId: last.fileId },
                    done: false,
                };
            }
            return {
                kind: "file",
                rows,
                cursor: { kind: "organization_tombstone", afterPlacement: -1, afterId: "" },
                done: false,
            };
        }
        const rows = this.sql
            .all<StoredTombstoneRow>(
                `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
                 WHERE placement_vshard BETWEEN ? AND ?
                   AND (placement_vshard > ? OR (placement_vshard = ? AND organization_id > ?))
                 ORDER BY placement_vshard, organization_id LIMIT ?`,
                identity.rangeLo,
                identity.rangeHi,
                cursor.afterPlacement,
                cursor.afterPlacement,
                cursor.afterId,
                limit
            )
            .map(projectTombstone);
        assertReshardBatchBudget(rows, "file reshard tombstone parity");
        const last = rows.at(-1);
        return {
            kind: "organization_tombstone",
            rows,
            cursor: {
                kind: "organization_tombstone",
                afterPlacement: last?.placementVshard ?? cursor.afterPlacement,
                afterId: last?.organizationId ?? cursor.afterId,
            },
            done: rows.length < limit,
        };
    }

    assertOwnership(vshard: number): void {
        if (!Number.isSafeInteger(vshard) || vshard < 0 || vshard >= VSHARD_COUNT) invalid("vshard is invalid");
        const rows = this.sql.all<StoredCursor>(
            `SELECT * FROM _chardb_split_file_cursor
             WHERE ? BETWEEN range_lo AND range_hi
             ORDER BY CASE outcome WHEN 'active' THEN 0 ELSE 1 END, updated_at DESC, mig_id DESC LIMIT 2`,
            vshard
        );
        if (rows.length > 1 && rows[0]?.outcome === "active" && rows[1]?.outcome === "active") {
            mismatch("overlapping file ownership transfers exist");
        }
        const row = rows[0];
        if (!row) return;
        const allowed =
            (row.role === "source" &&
                ((row.outcome === "active" && row.source_fenced === 0 && row.maintenance_enabled === 1) ||
                    row.outcome === "aborted")) ||
            (row.role === "dest" &&
                (row.outcome === "active" || row.outcome === "finished") &&
                row.maintenance_enabled === 1);
        if (!allowed) {
            throw new CdbError({ code: "CDB_STALE_EPOCH", message: "file ownership moved to another shard" });
        }
    }

    drain(
        identity: CdbFileReshardIdentity,
        cursor: CdbFileReshardDrainCursor,
        limit: number
    ): { readonly cursor: CdbFileReshardDrainCursor; readonly deleted: number; readonly done: boolean } {
        const source = this.assertActive(identity, "source");
        if (source.source_fenced !== 1) mismatch("source file metadata cannot drain before its ownership fence");
        assertPageInput(identity, cursor.afterPlacement, cursor.afterId, limit);
        if (cursor.kind === "file") {
            const rows = this.sql.all<StoredFileRow>(
                `SELECT * FROM _chardb_files
                 WHERE placement_vshard BETWEEN ? AND ?
                   AND (placement_vshard > ? OR (placement_vshard = ? AND file_id > ?))
                 ORDER BY placement_vshard, file_id LIMIT ?`,
                identity.rangeLo,
                identity.rangeHi,
                cursor.afterPlacement,
                cursor.afterPlacement,
                cursor.afterId,
                limit
            );
            for (const raw of rows) {
                const row = projectFile(raw);
                this.sql.exec(
                    "DELETE FROM _chardb_files WHERE file_id = ? AND placement_vshard = ?",
                    row.fileId,
                    row.placementVshard
                );
                if (this.sql.changes() !== 1) mismatch(`source file ${row.fileId} changed during drain`);
            }
            if (rows.length === limit) {
                const last = projectFile(rows.at(-1) as StoredFileRow);
                return {
                    cursor: { kind: "file", afterPlacement: last.placementVshard, afterId: last.fileId },
                    deleted: rows.length,
                    done: false,
                };
            }
            return {
                cursor: { kind: "organization_tombstone", afterPlacement: -1, afterId: "" },
                deleted: rows.length,
                done: false,
            };
        }
        const rows = this.sql.all<StoredTombstoneRow>(
            `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
             WHERE placement_vshard BETWEEN ? AND ?
               AND (placement_vshard > ? OR (placement_vshard = ? AND organization_id > ?))
             ORDER BY placement_vshard, organization_id LIMIT ?`,
            identity.rangeLo,
            identity.rangeHi,
            cursor.afterPlacement,
            cursor.afterPlacement,
            cursor.afterId,
            limit
        );
        for (const raw of rows) {
            const row = projectTombstone(raw);
            this.sql.exec(
                `DELETE FROM _chardb_deleted_organizations
                 WHERE organization_id = ? AND placement_vshard = ?`,
                row.organizationId,
                row.placementVshard
            );
            if (this.sql.changes() !== 1) mismatch(`source organization ${row.organizationId} changed during drain`);
        }
        const last = rows.length > 0 ? projectTombstone(rows.at(-1) as StoredTombstoneRow) : null;
        return {
            cursor: {
                kind: "organization_tombstone",
                afterPlacement: last?.placementVshard ?? cursor.afterPlacement,
                afterId: last?.organizationId ?? cursor.afterId,
            },
            deleted: rows.length,
            done: rows.length < limit,
        };
    }

    abortDest(
        identity: CdbFileReshardIdentity,
        nowMs: number,
        afterKind: "" | "file" | "organization_tombstone" = "",
        afterId = "",
        limit = CDB_FILE_RESHARD_PAGE_SIZE
    ): {
        readonly afterKind: "" | "file" | "organization_tombstone";
        readonly afterId: string;
        readonly deleted: number;
        readonly done: boolean;
    } {
        if (
            !["", "file", "organization_tombstone"].includes(afterKind) ||
            typeof afterId !== "string" ||
            new TextEncoder().encode(afterId).byteLength > 256 ||
            limit !== CDB_FILE_RESHARD_PAGE_SIZE
        ) {
            invalid(`abort cursor is invalid or limit is not exactly ${CDB_FILE_RESHARD_PAGE_SIZE}`);
        }
        const cursor = this.assertBound(identity, "dest");
        if (cursor.outcome === "finished") mismatch("finished file ownership cannot abort");
        if (cursor.outcome === "active") {
            this.sql.exec(
                "UPDATE _chardb_split_file_cursor SET outcome = 'aborted', maintenance_enabled = 0, updated_at = ? WHERE mig_id = ?",
                safeInteger(nowMs, "file reshard abort time"),
                identity.migId
            );
            if (this.sql.changes() !== 1) mismatch("destination file abort lost its ownership row");
        }
        const applied = this.sql.all<{
            record_kind: "file" | "organization_tombstone";
            record_id: string;
            inserted: number;
        }>(
            `SELECT record_kind, record_id, inserted FROM _chardb_split_file_applied
             WHERE mig_id = ? AND (record_kind > ? OR (record_kind = ? AND record_id > ?))
             ORDER BY record_kind, record_id LIMIT ?`,
            identity.migId,
            afterKind,
            afterKind,
            afterId,
            limit
        );
        let deleted = 0;
        for (const row of applied) {
            if (row.inserted === 1) {
                if (row.record_kind === "file") {
                    this.sql.exec("DELETE FROM _chardb_files WHERE file_id = ?", row.record_id);
                } else {
                    this.sql.exec("DELETE FROM _chardb_deleted_organizations WHERE organization_id = ?", row.record_id);
                }
                const removed = this.sql.changes();
                if (removed !== 0 && removed !== 1) {
                    mismatch(`destination ${row.record_kind} ${row.record_id} changed during abort`);
                }
                deleted += removed;
            }
            this.sql.exec(
                `DELETE FROM _chardb_split_file_applied
                 WHERE mig_id = ? AND record_kind = ? AND record_id = ?`,
                identity.migId,
                row.record_kind,
                row.record_id
            );
            if (this.sql.changes() !== 1) mismatch("destination file abort ledger changed during cleanup");
        }
        const last = applied.at(-1);
        return Object.freeze({
            afterKind: last?.record_kind ?? afterKind,
            afterId: last?.record_id ?? afterId,
            deleted,
            done: applied.length < limit,
        });
    }

    abortSource(identity: CdbFileReshardIdentity, nowMs: number): void {
        const cursor = this.assertBound(identity, "source");
        if (cursor.outcome === "finished") mismatch("finished file ownership cannot abort");
        if (cursor.outcome === "aborted") return;
        if (cursor.source_fenced === 1) mismatch("fenced source file ownership cannot abort after cutover starts");
        this.sql.exec(
            `UPDATE _chardb_split_file_cursor
             SET outcome = 'aborted', maintenance_enabled = 0, attachments_enabled = 1, updated_at = ? WHERE mig_id = ?`,
            safeInteger(nowMs, "file reshard abort time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("source file abort lost its ownership row");
    }

    finish(
        identity: CdbFileReshardIdentity,
        role: "source" | "dest",
        nowMs: number,
        limit = CDB_FILE_RESHARD_PAGE_SIZE
    ): { readonly cleaned: number; readonly done: boolean } {
        if (limit !== CDB_FILE_RESHARD_PAGE_SIZE) {
            invalid(`finish limit is not exactly ${CDB_FILE_RESHARD_PAGE_SIZE}`);
        }
        const cursor = this.assertBound(identity, role);
        if (cursor.outcome === "finished") return Object.freeze({ cleaned: 0, done: true });
        if (cursor.outcome === "aborted") mismatch("aborted file ownership cannot finish");
        if (role === "source") {
            if (cursor.source_fenced !== 1) mismatch("source file ownership is not fenced");
            if (this.hasMetadata(identity)) mismatch("source file metadata has not drained");
        } else if (cursor.maintenance_enabled !== 1) {
            mismatch("destination file ownership is not active");
        }
        const applied = this.sql.all<{ record_kind: string; record_id: string }>(
            `SELECT record_kind, record_id FROM _chardb_split_file_applied
             WHERE mig_id = ? ORDER BY record_kind, record_id LIMIT ?`,
            identity.migId,
            limit
        );
        for (const row of applied) {
            this.sql.exec(
                `DELETE FROM _chardb_split_file_applied
                 WHERE mig_id = ? AND record_kind = ? AND record_id = ?`,
                identity.migId,
                row.record_kind,
                row.record_id
            );
            if (this.sql.changes() !== 1) mismatch("file reshard finish ledger changed during cleanup");
        }
        if (applied.length === limit) return Object.freeze({ cleaned: applied.length, done: false });
        this.sql.exec(
            "UPDATE _chardb_split_file_cursor SET outcome = 'finished', updated_at = ? WHERE mig_id = ? AND outcome = 'active'",
            safeInteger(nowMs, "file reshard finish time"),
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("file reshard finish lost its ownership row");
        return Object.freeze({ cleaned: applied.length, done: true });
    }

    private begin(
        identity: CdbFileReshardIdentity,
        role: "source" | "dest",
        maintenanceEnabled: 0 | 1,
        nowMs: number
    ): void {
        assertCdbReshardRangeIdentity(identity);
        safeInteger(nowMs, "file reshard start time");
        const existing = this.readCursor(identity.migId);
        if (existing) {
            this.assertCursorIdentity(existing, identity, role);
            if (existing.outcome !== "active") mismatch(`file reshard ${identity.migId} is ${existing.outcome}`);
            return;
        }
        const overlap = this.sql.one<{ mig_id: string }>(
            `SELECT mig_id FROM _chardb_split_file_cursor
             WHERE outcome = 'active' AND range_lo <= ? AND ? <= range_hi LIMIT 1`,
            identity.rangeHi,
            identity.rangeLo
        );
        if (overlap) mismatch(`file reshard range overlaps ${overlap.mig_id}`);
        this.sql.exec(
            `INSERT INTO _chardb_split_file_cursor
               (mig_id, range_lo, range_hi, role, outcome, maintenance_enabled, attachments_enabled,
                source_fenced, updated_at)
             VALUES (?, ?, ?, ?, 'active', ?, ?, 0, ?)`,
            identity.migId,
            identity.rangeLo,
            identity.rangeHi,
            role,
            maintenanceEnabled,
            role === "source" ? 1 : 0,
            nowMs
        );
    }

    private assertBatch(rows: readonly unknown[]): void {
        if (!Array.isArray(rows) || rows.length > CDB_FILE_RESHARD_PAGE_SIZE) invalid("batch exceeds 500 rows");
        assertReshardBatchBudget(rows, "file reshard apply");
    }

    private assertInRange(identity: CdbFileReshardIdentity, placement: number): void {
        if (placement < identity.rangeLo || placement > identity.rangeHi)
            mismatch("file metadata is outside the moving range");
    }

    private readCursor(migId: string): StoredCursor | null {
        return this.sql.one<StoredCursor>("SELECT * FROM _chardb_split_file_cursor WHERE mig_id = ?", migId);
    }

    private assertBound(identity: CdbFileReshardIdentity, role: "source" | "dest"): StoredCursor {
        assertCdbReshardRangeIdentity(identity);
        const cursor = this.readCursor(identity.migId);
        if (!cursor) mismatch("file reshard ownership is not bound");
        this.assertCursorIdentity(cursor, identity, role);
        return cursor;
    }

    private assertActive(identity: CdbFileReshardIdentity, role: "source" | "dest"): StoredCursor {
        const cursor = this.assertBound(identity, role);
        if (cursor.outcome !== "active") mismatch(`file reshard ${identity.migId} is ${cursor.outcome}`);
        return cursor;
    }

    private assertCursorIdentity(
        cursor: StoredCursor,
        identity: CdbFileReshardIdentity,
        role: "source" | "dest"
    ): void {
        if (cursor.range_lo !== identity.rangeLo || cursor.range_hi !== identity.rangeHi || cursor.role !== role) {
            mismatch("file reshard id belongs to a different immutable identity");
        }
    }

    private applied(
        migId: string,
        kind: "file" | "organization_tombstone",
        id: string
    ): { inserted: number; snapshot_through_lsn: number | null } | null {
        return this.sql.one<{ inserted: number; snapshot_through_lsn: number | null }>(
            `SELECT inserted, snapshot_through_lsn FROM _chardb_split_file_applied
             WHERE mig_id = ? AND record_kind = ? AND record_id = ?`,
            migId,
            kind,
            id
        );
    }

    private recordApplied(
        migId: string,
        kind: "file" | "organization_tombstone",
        id: string,
        inserted: 0 | 1,
        snapshotThroughLsn: number
    ): void {
        this.sql.exec(
            `INSERT INTO _chardb_split_file_applied
               (mig_id, record_kind, record_id, inserted, snapshot_through_lsn)
             VALUES (?, ?, ?, ?, ?)`,
            migId,
            kind,
            id,
            inserted,
            snapshotThroughLsn
        );
    }

    private updateSnapshotWatermark(
        migId: string,
        kind: "file" | "organization_tombstone",
        id: string,
        throughLsn: number
    ): void {
        this.sql.exec(
            `UPDATE _chardb_split_file_applied SET snapshot_through_lsn = ?
             WHERE mig_id = ? AND record_kind = ? AND record_id = ?
               AND snapshot_through_lsn IS NOT NULL AND snapshot_through_lsn <= ?`,
            throughLsn,
            migId,
            kind,
            id,
            throughLsn
        );
        if (this.sql.changes() !== 1) mismatch(`${kind} ${id} snapshot watermark changed concurrently`);
    }

    private readFile(fileId: string): CdbReshardFileRecord | null {
        const row = this.sql.one<StoredFileRow>("SELECT * FROM _chardb_files WHERE file_id = ?", fileId);
        return row ? projectFile(row) : null;
    }

    private readTombstone(organizationId: string): CdbReshardOrganizationTombstone | null {
        const row = this.sql.one<StoredTombstoneRow>(
            `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
             WHERE organization_id = ?`,
            organizationId
        );
        return row ? projectTombstone(row) : null;
    }

    private insertFile(row: CdbReshardFileRecord): void {
        this.sql.exec(
            `INSERT INTO _chardb_files
               (file_id, organization_id, table_name, column_name, object_key, content_type, size, sha256,
                status, row_id, created_at, updated_at, placement_vshard)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            row.fileId,
            row.organizationId,
            row.table,
            row.column,
            row.objectKey,
            row.contentType,
            row.size,
            row.sha256,
            row.status,
            row.rowId,
            row.createdAt,
            row.updatedAt,
            row.placementVshard
        );
    }

    private updateFile(row: CdbReshardFileRecord): void {
        this.sql.exec(
            `UPDATE _chardb_files SET
               organization_id = ?, table_name = ?, column_name = ?, object_key = ?, content_type = ?, size = ?,
               sha256 = ?, status = ?, row_id = ?, created_at = ?, updated_at = ?, placement_vshard = ?
             WHERE file_id = ?`,
            row.organizationId,
            row.table,
            row.column,
            row.objectKey,
            row.contentType,
            row.size,
            row.sha256,
            row.status,
            row.rowId,
            row.createdAt,
            row.updatedAt,
            row.placementVshard,
            row.fileId
        );
        if (this.sql.changes() !== 1) mismatch(`file ${row.fileId} changed during snapshot retry`);
    }

    private insertTombstone(row: CdbReshardOrganizationTombstone): void {
        this.sql.exec(
            `INSERT INTO _chardb_deleted_organizations
               (organization_id, deleted_at, placement_vshard, vector_unproven_turns)
             VALUES (?, ?, ?, ?)`,
            row.organizationId,
            row.deletedAt,
            row.placementVshard,
            row.vectorUnprovenTurns
        );
    }

    private hasMetadata(identity: CdbFileReshardIdentity): boolean {
        return (
            this.sql.one<{ present: number }>(
                `SELECT 1 AS present FROM _chardb_files WHERE placement_vshard BETWEEN ? AND ?
                 UNION ALL
                 SELECT 1 AS present FROM _chardb_deleted_organizations WHERE placement_vshard BETWEEN ? AND ?
                 LIMIT 1`,
                identity.rangeLo,
                identity.rangeHi,
                identity.rangeLo,
                identity.rangeHi
            ) !== null
        );
    }
}
