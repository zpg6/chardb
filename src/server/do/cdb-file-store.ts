import { CdbError } from "../../errors.ts";
import { CDB_FILE_MAX_BYTES, FileId, type FileId as FileIdType } from "../../files/index.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { VSHARD_COUNT, vshardOf } from "../../vshard.ts";
import { CDB_FILE_MAINTENANCE_OWNERSHIP_SQL } from "./cdb-file-ownership-sql.ts";

export const CDB_FILE_ORGANIZATION_QUOTA_BYTES = 10 * 1_024 * 1_024 * 1_024;
export const CDB_FILE_MAX_PENDING_PER_ORGANIZATION = 64;
export const CDB_FILE_PENDING_TTL_MS = 15 * 60 * 1_000;
export const CDB_FILE_DELETE_BATCH_SIZE = 32;

export const CDB_FILE_STORE_DDL = `
CREATE TABLE IF NOT EXISTS _chardb_files (
  file_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  object_key TEXT NOT NULL UNIQUE,
  content_type TEXT NOT NULL,
  size INTEGER NOT NULL CHECK (size > 0),
  sha256 TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'ready', 'attached', 'deleting')),
  row_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  placement_vshard INTEGER NOT NULL CHECK (placement_vshard >= 0 AND placement_vshard < 16384),
  CHECK (
    (status = 'pending' AND sha256 IS NULL AND row_id IS NULL)
    OR (status = 'ready' AND sha256 IS NOT NULL AND row_id IS NULL)
    OR (status = 'attached' AND sha256 IS NOT NULL AND row_id IS NOT NULL)
    OR status = 'deleting'
  )
);
CREATE INDEX IF NOT EXISTS _chardb_files_by_organization_status
  ON _chardb_files (organization_id, status, updated_at, file_id);
CREATE INDEX IF NOT EXISTS _chardb_files_delete_queue
  ON _chardb_files (status, updated_at, file_id);
CREATE INDEX IF NOT EXISTS _chardb_files_by_placement
  ON _chardb_files (placement_vshard, file_id);
CREATE TABLE IF NOT EXISTS _chardb_deleted_organizations (
  organization_id TEXT PRIMARY KEY,
  deleted_at INTEGER NOT NULL CHECK (deleted_at >= 0),
  placement_vshard INTEGER NOT NULL CHECK (placement_vshard >= 0 AND placement_vshard < 16384),
  vector_unproven_turns INTEGER NOT NULL DEFAULT 0 CHECK (vector_unproven_turns BETWEEN 0 AND 32)
);
CREATE INDEX IF NOT EXISTS _chardb_deleted_organizations_by_placement
  ON _chardb_deleted_organizations (placement_vshard, organization_id);
` as const;

export function initializeFileStore(sql: SyncSql): void {
    const fileColumns = sql.all<{ name: string }>("PRAGMA table_info(_chardb_files)");
    if (fileColumns.length > 0 && !fileColumns.some(column => column.name === "placement_vshard")) {
        sql.exec(
            `ALTER TABLE _chardb_files ADD COLUMN placement_vshard INTEGER
             CHECK (placement_vshard IS NULL OR (placement_vshard >= 0 AND placement_vshard < 16384))`
        );
    }
    const tombstoneColumns = sql.all<{ name: string }>("PRAGMA table_info(_chardb_deleted_organizations)");
    if (tombstoneColumns.length > 0 && !tombstoneColumns.some(column => column.name === "placement_vshard")) {
        sql.exec(
            `ALTER TABLE _chardb_deleted_organizations ADD COLUMN placement_vshard INTEGER
             CHECK (placement_vshard IS NULL OR (placement_vshard >= 0 AND placement_vshard < 16384))`
        );
    }
    if (tombstoneColumns.length > 0 && !tombstoneColumns.some(column => column.name === "vector_unproven_turns")) {
        sql.exec(
            `ALTER TABLE _chardb_deleted_organizations
             ADD COLUMN vector_unproven_turns INTEGER NOT NULL DEFAULT 0
             CHECK (vector_unproven_turns BETWEEN 0 AND 32)`
        );
    }
    for (const statement of CDB_FILE_STORE_DDL.split(";")
        .map(value => value.trim())
        .filter(Boolean)) {
        sql.exec(statement);
    }
}

export interface CdbFileStoreLimits {
    readonly organizationQuotaBytes: number;
    readonly maxPendingPerOrganization: number;
}

export interface CdbFileMaintenanceOptions {
    /** Filter before LIMIT/MIN so another vshard cannot monopolize this shard's alarm. */
    readonly ownedOnly?: boolean;
}

const DEFAULT_LIMITS: CdbFileStoreLimits = Object.freeze({
    organizationQuotaBytes: CDB_FILE_ORGANIZATION_QUOTA_BYTES,
    maxPendingPerOrganization: CDB_FILE_MAX_PENDING_PER_ORGANIZATION,
});

export interface StoredFile {
    readonly fileId: FileIdType;
    readonly organizationId: string;
    readonly table: string;
    readonly column: string;
    readonly objectKey: string;
    readonly contentType: string;
    readonly size: number;
    readonly sha256: string | null;
    readonly status: "pending" | "ready" | "attached" | "deleting";
    readonly rowId: string | null;
    readonly createdAt: number;
    readonly updatedAt: number;
}

interface StoredFileRow {
    readonly file_id: string;
    readonly organization_id: string;
    readonly table_name: string;
    readonly column_name: string;
    readonly object_key: string;
    readonly content_type: string;
    readonly size: number | bigint;
    readonly sha256: string | null;
    readonly status: StoredFile["status"];
    readonly row_id: string | null;
    readonly created_at: number | bigint;
    readonly updated_at: number | bigint;
    readonly placement_vshard: number | bigint | null;
}

interface StoredOrganizationTombstoneRow {
    readonly organization_id: string;
    readonly deleted_at: number | bigint;
    readonly placement_vshard: number | bigint | null;
}

export interface FilePlacementBackfillResult {
    readonly files: number;
    readonly tombstones: number;
    readonly done: boolean;
}

export interface CdbFileRecoveryPage {
    readonly files: readonly StoredFile[];
    readonly afterFileId: string;
    readonly done: boolean;
}

function invalid(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message: `file store: ${message}` });
}

function boundedText(value: string, subject: string): string {
    if (typeof value !== "string" || value.length === 0 || new TextEncoder().encode(value).byteLength > 256) {
        invalid(`${subject} is invalid`);
    }
    return value;
}

function safeTime(value: number): number {
    if (!Number.isSafeInteger(value) || value < 0) invalid("timestamp is invalid");
    return value;
}

function project(row: StoredFileRow): StoredFile {
    assertExactPlacement(row.organization_id, row.placement_vshard, `file ${row.file_id}`);
    if (row.object_key !== `v1/${row.organization_id}/${row.file_id}`) {
        throw new CdbError({ code: "CDB_INVARIANT", message: `file ${row.file_id} has an invalid object key` });
    }
    return {
        fileId: FileId(row.file_id),
        organizationId: row.organization_id,
        table: row.table_name,
        column: row.column_name,
        objectKey: row.object_key,
        contentType: row.content_type,
        size: Number(row.size),
        sha256: row.sha256,
        status: row.status,
        rowId: row.row_id,
        createdAt: Number(row.created_at),
        updatedAt: Number(row.updated_at),
    };
}

function placement(value: string): number {
    return Number(vshardOf([value]));
}

function assertExactPlacement(organizationId: string, stored: number | bigint | null, subject: string): number {
    const numeric = stored === null ? Number.NaN : Number(stored);
    const expected = placement(organizationId);
    if (!Number.isSafeInteger(numeric) || numeric < 0 || numeric >= VSHARD_COUNT || numeric !== expected) {
        throw new CdbError({ code: "CDB_INVARIANT", message: `${subject} has invalid virtual-shard placement` });
    }
    return numeric;
}

function positiveBatchLimit(value: number): number {
    if (!Number.isSafeInteger(value) || value < 1 || value > 500) invalid("placement backfill limit is invalid");
    return value;
}

/**
 * Upgrade at most `limit` legacy ownership rows. The cursor is implicit in the
 * remaining NULL rows, so response-loss retries repeat the exact unfinished
 * page without skipping metadata.
 */
export function backfillFilePlacements(sql: SyncSql, limit = 500): FilePlacementBackfillResult {
    const bounded = positiveBatchLimit(limit);
    const files = sql.all<{ file_id: string; organization_id: string }>(
        `SELECT file_id, organization_id FROM _chardb_files
         WHERE placement_vshard IS NULL ORDER BY file_id LIMIT ?`,
        bounded
    );
    for (const row of files) {
        sql.exec(
            "UPDATE _chardb_files SET placement_vshard = ? WHERE file_id = ? AND placement_vshard IS NULL",
            placement(row.organization_id),
            row.file_id
        );
        if (sql.changes() !== 1) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "file placement backfill lost its row" });
        }
    }
    const remaining = bounded - files.length;
    const tombstones = sql.all<{ organization_id: string }>(
        `SELECT organization_id FROM _chardb_deleted_organizations
         WHERE placement_vshard IS NULL ORDER BY organization_id LIMIT ?`,
        remaining
    );
    for (const row of tombstones) {
        sql.exec(
            `UPDATE _chardb_deleted_organizations SET placement_vshard = ?
             WHERE organization_id = ? AND placement_vshard IS NULL`,
            placement(row.organization_id),
            row.organization_id
        );
        if (sql.changes() !== 1) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: "organization tombstone placement backfill lost its row",
            });
        }
    }
    const done =
        sql.one<{ present: number }>(
            `SELECT 1 AS present FROM _chardb_files WHERE placement_vshard IS NULL
             UNION ALL
             SELECT 1 AS present FROM _chardb_deleted_organizations WHERE placement_vshard IS NULL
             LIMIT 1`
        ) === null;
    return Object.freeze({ files: files.length, tombstones: tombstones.length, done });
}

/** Validate one bounded page before a split. A caller advances only after this succeeds. */
export function validateFilePlacementsPage(
    sql: SyncSql,
    input: { readonly afterKind: "file" | "organization_tombstone"; readonly afterId: string; readonly limit: number }
): { readonly kind: "file" | "organization_tombstone"; readonly afterId: string; readonly done: boolean } {
    const limit = positiveBatchLimit(input.limit);
    if (input.afterKind === "file") {
        const rows = sql.all<StoredFileRow>(
            "SELECT * FROM _chardb_files WHERE file_id > ? ORDER BY file_id LIMIT ?",
            input.afterId,
            limit
        );
        for (const row of rows) project(row);
        if (rows.length === limit) {
            return { kind: "file", afterId: rows.at(-1)?.file_id ?? input.afterId, done: false };
        }
        return { kind: "organization_tombstone", afterId: "", done: false };
    }
    const rows = sql.all<StoredOrganizationTombstoneRow>(
        `SELECT organization_id, deleted_at, placement_vshard FROM _chardb_deleted_organizations
         WHERE organization_id > ? ORDER BY organization_id LIMIT ?`,
        input.afterId,
        limit
    );
    for (const row of rows) {
        safeTime(Number(row.deleted_at));
        assertExactPlacement(row.organization_id, row.placement_vshard, `organization ${row.organization_id}`);
    }
    return {
        kind: "organization_tombstone",
        afterId: rows.at(-1)?.organization_id ?? input.afterId,
        done: rows.length < limit,
    };
}

export class CdbFileStore {
    readonly sql: SyncSql;
    readonly limits: CdbFileStoreLimits;

    constructor(sql: SyncSql, limits: CdbFileStoreLimits = DEFAULT_LIMITS) {
        if (!Number.isSafeInteger(limits.organizationQuotaBytes) || limits.organizationQuotaBytes < 1) {
            invalid("organization quota is invalid");
        }
        if (!Number.isSafeInteger(limits.maxPendingPerOrganization) || limits.maxPendingPerOrganization < 1) {
            invalid("pending-file limit is invalid");
        }
        this.sql = sql;
        this.limits = Object.freeze({ ...limits });
    }

    recoveryPage(afterFileId: string, limit: number): CdbFileRecoveryPage {
        return this.fileRecoveryPage(afterFileId, limit, false);
    }

    retentionPage(afterFileId: string, limit: number): CdbFileRecoveryPage {
        return this.fileRecoveryPage(afterFileId, limit, true);
    }

    private fileRecoveryPage(afterFileId: string, limit: number, includeDeleting: boolean): CdbFileRecoveryPage {
        if (
            typeof afterFileId !== "string" ||
            new TextEncoder().encode(afterFileId).byteLength > 256 ||
            !Number.isSafeInteger(limit) ||
            limit < 1 ||
            limit > 500
        ) {
            invalid("recovery page is invalid");
        }
        const rows = this.sql.all<StoredFileRow>(
            `SELECT * FROM _chardb_files
             WHERE file_id > ?
               AND status IN ('ready', 'attached'${includeDeleting ? ", 'deleting'" : ""})
               AND sha256 IS NOT NULL
             ORDER BY file_id LIMIT ?`,
            afterFileId,
            limit + 1
        );
        const selected = rows.slice(0, limit);
        return Object.freeze({
            files: Object.freeze(selected.map(project)),
            afterFileId: selected.at(-1)?.file_id ?? afterFileId,
            done: rows.length <= limit,
        });
    }

    reserve(input: {
        readonly fileId: string;
        readonly organizationId: string;
        readonly table: string;
        readonly column: string;
        readonly contentType: string;
        readonly size: number;
        readonly nowMs: number;
    }): StoredFile {
        const fileId = FileId(input.fileId);
        const organizationId = boundedText(input.organizationId, "organization id");
        const table = boundedText(input.table, "table");
        const column = boundedText(input.column, "column");
        const contentType = boundedText(input.contentType, "content type");
        const nowMs = safeTime(input.nowMs);
        this.assertOrganizationActive(organizationId);
        if (!Number.isSafeInteger(input.size) || input.size < 1 || input.size > CDB_FILE_MAX_BYTES) {
            invalid(`size must be from 1 through ${CDB_FILE_MAX_BYTES}`);
        }
        const objectKey = `v1/${organizationId}/${fileId}`;
        const placementVshard = placement(organizationId);
        const existing = this.read(fileId);
        if (existing) {
            if (
                existing.organizationId === organizationId &&
                existing.table === table &&
                existing.column === column &&
                existing.contentType === contentType &&
                existing.size === input.size &&
                existing.objectKey === objectKey &&
                existing.status !== "deleting"
            ) {
                if (existing.status === "pending" && nowMs > existing.updatedAt) {
                    this.sql.exec(
                        "UPDATE _chardb_files SET updated_at = ? WHERE file_id = ? AND status = 'pending'",
                        nowMs,
                        fileId
                    );
                    return this.require(fileId);
                }
                return existing;
            }
            invalid("FileId is already reserved for a different upload");
        }

        const usage = this.sql.one<{ bytes: number | bigint; pending: number | bigint }>(
            `SELECT COALESCE(SUM(size), 0) AS bytes,
                    COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending
             FROM _chardb_files
             WHERE organization_id = ?`,
            organizationId
        );
        const bytes = Number(usage?.bytes ?? 0);
        const pending = Number(usage?.pending ?? 0);
        if (!Number.isSafeInteger(bytes) || !Number.isSafeInteger(pending) || bytes < 0 || pending < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "file store usage is invalid" });
        }
        if (bytes + input.size > this.limits.organizationQuotaBytes) {
            throw new CdbError({ code: "CDB_RATE_LIMITED", message: "organization file quota exceeded" });
        }
        if (pending >= this.limits.maxPendingPerOrganization) {
            throw new CdbError({ code: "CDB_RATE_LIMITED", message: "organization pending-file limit exceeded" });
        }
        this.sql.exec(
            `INSERT INTO _chardb_files
              (file_id, organization_id, table_name, column_name, object_key, content_type, size, sha256, status, row_id, created_at, updated_at, placement_vshard)
             VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'pending', NULL, ?, ?, ?)`,
            fileId,
            organizationId,
            table,
            column,
            objectKey,
            contentType,
            input.size,
            nowMs,
            nowMs,
            placementVshard
        );
        return this.require(fileId);
    }

    markReady(fileId: string, sha256: string, actualSize: number, nowMs: number): StoredFile {
        const existing = this.require(FileId(fileId));
        this.assertOrganizationActive(existing.organizationId);
        safeTime(nowMs);
        if (!/^[0-9a-f]{64}$/.test(sha256)) invalid("sha256 must be 64 lowercase hexadecimal characters");
        if (actualSize !== existing.size) invalid("uploaded size does not match the reservation");
        if (existing.status === "ready" || existing.status === "attached") {
            if (existing.sha256 !== sha256) invalid("ready retry does not match the stored hash");
            return existing;
        }
        if (existing.status !== "pending") invalid("only a pending file can become ready");
        this.sql.exec(
            `UPDATE _chardb_files
             SET status = 'ready', sha256 = ?, updated_at = MAX(updated_at, ?)
             WHERE file_id = ? AND status = 'pending'`,
            sha256,
            nowMs,
            existing.fileId
        );
        if (this.sql.changes() !== 1)
            throw new CdbError({ code: "CDB_INVARIANT", message: "file ready transition lost" });
        return this.require(existing.fileId);
    }

    attach(
        fileId: string,
        organizationId: string,
        table: string,
        column: string,
        rowId: string,
        nowMs: number
    ): StoredFile {
        const existing = this.require(FileId(fileId));
        this.assertOrganizationActive(existing.organizationId);
        boundedText(rowId, "row id");
        safeTime(nowMs);
        if (existing.organizationId !== organizationId || existing.table !== table || existing.column !== column) {
            invalid("file ownership locator does not match the target column");
        }
        if (existing.status === "attached") {
            if (existing.rowId !== rowId) invalid("FileId cannot be reused by another row");
            return existing;
        }
        if (existing.status !== "ready") invalid("only a ready file can be attached");
        this.sql.exec(
            `UPDATE _chardb_files
             SET status = 'attached', row_id = ?, updated_at = MAX(updated_at, ?)
             WHERE file_id = ? AND status = 'ready'`,
            rowId,
            nowMs,
            existing.fileId
        );
        if (this.sql.changes() !== 1)
            throw new CdbError({ code: "CDB_INVARIANT", message: "file attach transition lost" });
        return this.require(existing.fileId);
    }

    queueDelete(fileId: string, nowMs: number): StoredFile {
        const existing = this.require(FileId(fileId));
        safeTime(nowMs);
        if (existing.status === "deleting") return existing;
        this.sql.exec(
            "UPDATE _chardb_files SET status = 'deleting', updated_at = MAX(updated_at, ?) WHERE file_id = ?",
            nowMs,
            fileId
        );
        if (this.sql.changes() !== 1)
            throw new CdbError({ code: "CDB_INVARIANT", message: "file delete transition lost" });
        return this.require(existing.fileId);
    }

    nextUnattachedExpiryAt(ttlMs: number, options: CdbFileMaintenanceOptions = {}): number | null {
        if (!Number.isSafeInteger(ttlMs) || ttlMs < 1) invalid("unattached TTL is invalid");
        const ownership = options.ownedOnly ? `AND ${CDB_FILE_MAINTENANCE_OWNERSHIP_SQL}` : "";
        const row = this.sql.one<{ expires_at: number | bigint | null }>(
            `SELECT MIN(files.updated_at + ?) AS expires_at FROM _chardb_files AS files
             WHERE files.status IN ('pending', 'ready') ${ownership}`,
            ttlMs
        );
        if (row?.expires_at === null || row?.expires_at === undefined) return null;
        const expiresAt = Number(row.expires_at);
        if (!Number.isSafeInteger(expiresAt) || expiresAt < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "file expiry deadline is invalid" });
        }
        return expiresAt;
    }

    dueDeletes(
        limit: number = CDB_FILE_DELETE_BATCH_SIZE,
        options: CdbFileMaintenanceOptions = {}
    ): readonly StoredFile[] {
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > CDB_FILE_DELETE_BATCH_SIZE) {
            invalid(`delete batch must be from 1 through ${CDB_FILE_DELETE_BATCH_SIZE}`);
        }
        const ownership = options.ownedOnly ? `AND ${CDB_FILE_MAINTENANCE_OWNERSHIP_SQL}` : "";
        return this.sql
            .all<StoredFileRow>(
                `SELECT files.* FROM _chardb_files AS files
                 WHERE files.status = 'deleting' ${ownership}
                 ORDER BY files.updated_at, files.file_id LIMIT ?`,
                limit
            )
            .map(project);
    }

    completeDelete(fileId: string): void {
        const existing = this.require(FileId(fileId));
        if (existing.status !== "deleting") invalid("only a deleting file can be released");
        this.sql.exec("DELETE FROM _chardb_files WHERE file_id = ? AND status = 'deleting'", existing.fileId);
        if (this.sql.changes() !== 1)
            throw new CdbError({ code: "CDB_INVARIANT", message: "file delete completion lost" });
    }

    maintenanceCandidates(cutoffMs: number, options: CdbFileMaintenanceOptions = {}): readonly StoredFile[] {
        safeTime(cutoffMs);
        const ownership = options.ownedOnly ? `AND ${CDB_FILE_MAINTENANCE_OWNERSHIP_SQL}` : "";
        return this.sql
            .all<StoredFileRow>(
                `SELECT files.* FROM _chardb_files AS files
                 WHERE ((files.status IN ('pending', 'ready') AND files.updated_at <= ?)
                    OR (files.status IN ('ready', 'attached') AND EXISTS (
                      SELECT 1 FROM _chardb_deleted_organizations AS deleted
                      WHERE deleted.organization_id = files.organization_id
                    ))) ${ownership}
                 ORDER BY files.updated_at, files.file_id LIMIT ?`,
                cutoffMs,
                CDB_FILE_DELETE_BATCH_SIZE
            )
            .map(project);
    }

    /** Fence the identity first, then queue every materialized object. Pending uploads keep their bounded lease. */
    fenceOrganizationDeletion(organizationId: string, nowMs: number): number {
        boundedText(organizationId, "organization id");
        safeTime(nowMs);
        this.sql.exec(
            `INSERT OR IGNORE INTO _chardb_deleted_organizations
               (organization_id, deleted_at, placement_vshard) VALUES (?, ?, ?)`,
            organizationId,
            nowMs,
            placement(organizationId)
        );
        const tombstone = this.sql.one<StoredOrganizationTombstoneRow>(
            `SELECT organization_id, deleted_at, placement_vshard FROM _chardb_deleted_organizations
             WHERE organization_id = ?`,
            organizationId
        );
        if (!tombstone) throw new CdbError({ code: "CDB_INVARIANT", message: "organization tombstone is missing" });
        assertExactPlacement(organizationId, tombstone.placement_vshard, `organization ${organizationId}`);
        this.queueOrganizationFiles(organizationId, nowMs);
        return this.organizationFileCount(organizationId);
    }

    queueOrganizationFiles(organizationId: string, nowMs: number, limit = CDB_FILE_DELETE_BATCH_SIZE): number {
        boundedText(organizationId, "organization id");
        safeTime(nowMs);
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > CDB_FILE_DELETE_BATCH_SIZE) {
            invalid(`delete batch must be from 1 through ${CDB_FILE_DELETE_BATCH_SIZE}`);
        }
        this.sql.exec(
            `UPDATE _chardb_files SET status = 'deleting', updated_at = MAX(updated_at, ?)
             WHERE file_id IN (
               SELECT file_id FROM _chardb_files
               WHERE organization_id = ? AND status IN ('ready', 'attached')
               ORDER BY updated_at, file_id LIMIT ?
             )`,
            nowMs,
            organizationId,
            limit
        );
        return this.sql.changes();
    }

    isOrganizationDeleted(organizationId: string): boolean {
        boundedText(organizationId, "organization id");
        return (
            this.sql.one<{ present: number }>(
                "SELECT 1 AS present FROM _chardb_deleted_organizations WHERE organization_id = ?",
                organizationId
            ) !== null
        );
    }

    organizationFileCount(organizationId: string): number {
        boundedText(organizationId, "organization id");
        const row = this.sql.one<{ count: number | bigint }>(
            "SELECT COUNT(*) AS count FROM _chardb_files WHERE organization_id = ?",
            organizationId
        );
        const count = Number(row?.count ?? 0);
        if (!Number.isSafeInteger(count) || count < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "organization file count is invalid" });
        }
        return count;
    }

    hasTombstonedMaterializedFiles(options: CdbFileMaintenanceOptions = {}): boolean {
        const ownership = options.ownedOnly ? `AND ${CDB_FILE_MAINTENANCE_OWNERSHIP_SQL}` : "";
        return (
            this.sql.one<{ present: number }>(
                `SELECT 1 AS present FROM _chardb_files AS files
                 INNER JOIN _chardb_deleted_organizations AS deleted
                   ON deleted.organization_id = files.organization_id
                 WHERE files.status IN ('ready', 'attached')
                   ${ownership}
                 LIMIT 1`
            ) !== null
        );
    }

    read(fileId: string): StoredFile | null {
        const row = this.sql.one<StoredFileRow>("SELECT * FROM _chardb_files WHERE file_id = ?", fileId);
        return row ? project(row) : null;
    }

    private require(fileId: string): StoredFile {
        const file = this.read(fileId);
        if (!file) invalid("FileId does not exist");
        return file;
    }

    private assertOrganizationActive(organizationId: string): void {
        if (this.isOrganizationDeleted(organizationId)) {
            throw new CdbError({ code: "CDB_FORBIDDEN", message: "file organization was deleted" });
        }
    }
}
