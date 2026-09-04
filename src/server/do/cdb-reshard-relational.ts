import { CdbError } from "../../errors.ts";
import type { JsonText, SqlParam, SyncSql } from "../../oplog/wrapper.ts";
import type { RangeFilter } from "../../reshard/range.ts";
import {
    type TableSpec,
    type TriggerSet,
    legacyReshardTriggerMigrationId,
    reshardTriggerMigrationId,
} from "../../reshard/triggers.ts";
import {
    type CdbReshardFileRecord,
    type CdbReshardOrganizationTombstone,
    type CdbReshardStoredFileRow,
    type CdbReshardStoredTombstoneRow,
    exactCdbReshardFile,
    exactCdbReshardTombstone,
    projectCdbReshardFileRecord,
    projectCdbReshardTombstone,
} from "./cdb-file-reshard-record.ts";
import {
    CDB_VECTOR_ATTEMPT_TAIL_TABLE,
    CDB_VECTOR_HEAD_TAIL_TABLE,
    CDB_VECTOR_OUTBOX_TAIL_TABLE,
    applyCdbVectorTailEntry,
} from "./cdb-vector-reshard-tail.ts";

const SQLITE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const TEXT_ENCODER = new TextEncoder();

export const CDB_RESHARD_MAX_ROW_BYTES = 256 * 1_024;
export const CDB_RESHARD_MAX_BATCH_BYTES = 1 * 1_024 * 1_024;

export function reshardJsonBytes(value: unknown): number {
    return TEXT_ENCODER.encode(JSON.stringify(value)).byteLength;
}

export function assertReshardBatchBudget(values: readonly unknown[], subject: string): number {
    let bytes = 0;
    for (const value of values) {
        const rowBytes = reshardJsonBytes(value);
        if (rowBytes > CDB_RESHARD_MAX_ROW_BYTES) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `${subject} row exceeds ${CDB_RESHARD_MAX_ROW_BYTES} UTF-8 bytes`,
            });
        }
        bytes += rowBytes;
        if (bytes > CDB_RESHARD_MAX_BATCH_BYTES) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `${subject} batch exceeds ${CDB_RESHARD_MAX_BATCH_BYTES} UTF-8 bytes`,
            });
        }
    }
    return bytes;
}

export function assertReshardEnvelopeBudget(value: unknown, subject: string): number {
    const bytes = reshardJsonBytes(value);
    if (bytes > CDB_RESHARD_MAX_BATCH_BYTES) {
        throw new CdbError({
            code: "CDB_INVALID_ARGS",
            message: `${subject} envelope exceeds ${CDB_RESHARD_MAX_BATCH_BYTES} UTF-8 bytes`,
        });
    }
    return bytes;
}

/** Exact UTF-8 size of a JSON array assembled from already encoded JSON values. */
export function reshardJsonArrayBytes(itemBytes: readonly number[]): number {
    let bytes = 2;
    for (let index = 0; index < itemBytes.length; index++) {
        const item = itemBytes[index];
        if (item === undefined || !Number.isSafeInteger(item) || item < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "stored reshard envelope byte size is invalid" });
        }
        bytes += item + (index === 0 ? 0 : 1);
        if (!Number.isSafeInteger(bytes)) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "stored reshard envelope byte size overflowed" });
        }
    }
    return bytes;
}

/** Mutation transactions are positive. External side-state capture transactions are negative. Zero is never durable. */
export function assertReshardSourceTransactionId(value: number): void {
    if (!Number.isSafeInteger(value) || value === 0) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "reshard source transaction identity is invalid" });
    }
}

function quoteIdent(value: string): string {
    if (!SQLITE_IDENTIFIER.test(value)) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "reshard table metadata contains an invalid identifier" });
    }
    return `"${value}"`;
}

interface TableColumn {
    readonly name: string;
    readonly pk: number;
}

interface ForeignKeyRow {
    readonly id: number;
    readonly seq: number;
    readonly table: string;
    readonly from: string;
    readonly to: string | null;
}

export interface ReshardForeignKey {
    readonly id: number;
    readonly child: TableSpec;
    readonly parent: TableSpec;
    readonly columns: readonly {
        readonly child: string;
        readonly parent: string;
    }[];
}

export interface ReshardTableLayout {
    readonly primaryKey: readonly string[];
}

export function readReshardTableLayout(sql: SyncSql, table: TableSpec): ReshardTableLayout {
    const columns = sql.all<TableColumn>(`PRAGMA table_info(${quoteIdent(table.name)})`);
    if (columns.length === 0) {
        throw new CdbError({ code: "CDB_INVARIANT", message: `reshard destination table ${table.name} is missing` });
    }
    const primaryKey = columns
        .filter(column => column.pk > 0)
        .sort((left, right) => left.pk - right.pk)
        .map(column => column.name);
    if (primaryKey.length === 0) {
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: `reshard table ${table.name} has no declared primary key`,
        });
    }
    return { primaryKey };
}

/** Resolve every declared FK to its exact migrating child and parent columns. */
export function readReshardForeignKeys(sql: SyncSql, tables: readonly TableSpec[]): readonly ReshardForeignKey[] {
    const byName = new Map(tables.map(table => [table.name, table]));
    const result: ReshardForeignKey[] = [];
    for (const child of tables) {
        const rows = sql.all<ForeignKeyRow>(`PRAGMA foreign_key_list(${quoteIdent(child.name)})`);
        const byId = new Map<number, ForeignKeyRow[]>();
        for (const row of rows) {
            const group = byId.get(row.id) ?? [];
            group.push(row);
            byId.set(row.id, group);
        }
        for (const [id, group] of [...byId].sort(([left], [right]) => left - right)) {
            group.sort((left, right) => left.seq - right.seq);
            const parentName = group[0]?.table;
            if (!parentName || group.some(row => row.table !== parentName)) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: `reshard table ${child.name} has malformed FK ${id}`,
                });
            }
            const parent = byName.get(parentName);
            if (!parent) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `reshard table ${child.name} references non-migrating table ${parentName}`,
                    hint: "move the complete foreign-key component in one split",
                });
            }
            const parentPrimaryKey = readReshardTableLayout(sql, parent).primaryKey;
            const columns = group.map(row => {
                const parentColumn = row.to ?? parentPrimaryKey[row.seq];
                if (!row.from || !parentColumn) {
                    throw new CdbError({
                        code: "CDB_RESHARD_PHASE_MISMATCH",
                        message: `reshard table ${child.name} has an unsupported FK ${id}`,
                    });
                }
                return { child: row.from, parent: parentColumn };
            });
            result.push({ id, child, parent, columns });
        }
    }
    return result;
}

function foreignKeyJoin(edge: ReshardForeignKey, childAlias: string, parentAlias: string): string {
    return edge.columns
        .map(columns => `${parentAlias}.${quoteIdent(columns.parent)} = ${childAlias}.${quoteIdent(columns.child)}`)
        .join(" AND ");
}

/**
 * Validate one row encountered by the bounded bulk scan. Every physical child
 * row is checked, including rows outside the moving range. That makes the
 * later child-before-parent drain safe from RESTRICT and CASCADE crossing the
 * split boundary.
 */
export function assertReshardRowForeignKeysColocated(
    sql: SyncSql,
    table: TableSpec,
    rowid: number,
    foreignKeys: readonly ReshardForeignKey[]
): void {
    for (const edge of foreignKeys) {
        if (edge.child.name !== table.name) continue;
        const mismatch = sql.one<{ found: number }>(
            `SELECT 1 AS found FROM ${quoteIdent(edge.child.name)} AS child
             JOIN ${quoteIdent(edge.parent.name)} AS parent ON ${foreignKeyJoin(edge, "child", "parent")}
             WHERE child.rowid = ?
               AND child.${quoteIdent(edge.child.partitionColumn)} IS NOT parent.${quoteIdent(
                   edge.parent.partitionColumn
               )}
             LIMIT 1`,
            rowid
        );
        if (mismatch) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `reshard FK ${edge.child.name} -> ${edge.parent.name} crosses partitions`,
                hint: "foreign-key-related rows must use the same partition key before an online split",
            });
        }
    }
}

/**
 * Install source-side guards that reject new cross-partition FK edges while a
 * split is active. Parent partition-key changes are frozen during this short
 * window, so the guard remains O(1) even when child FK columns lack an index.
 */
function renderReshardForeignKeyGuardsForMigration(
    sql: SyncSql,
    migration: string,
    tables: readonly TableSpec[]
): TriggerSet {
    const foreignKeys = readReshardForeignKeys(sql, tables);
    const install: string[] = [];
    const uninstall: string[] = [];
    const names: string[] = [];
    for (const edge of foreignKeys) {
        const suffix = `${migration}_${edge.child.name}_${edge.id}`;
        const insertRawName = `_chardb_fkguard_${suffix}_ins`;
        const updateRawName = `_chardb_fkguard_${suffix}_upd`;
        const insertName = quoteIdent(insertRawName);
        const updateName = quoteIdent(updateRawName);
        names.push(insertRawName, updateRawName);
        const mismatch = [
            `EXISTS (SELECT 1 FROM ${quoteIdent(edge.parent.name)} AS parent WHERE `,
            edge.columns
                .map(columns => `parent.${quoteIdent(columns.parent)} = NEW.${quoteIdent(columns.child)}`)
                .join(" AND "),
            ` AND parent.${quoteIdent(edge.parent.partitionColumn)} IS NOT NEW.${quoteIdent(
                edge.child.partitionColumn
            )})`,
        ].join("");
        const error = `CDB_RESHARD_PHASE_MISMATCH: FK ${edge.child.name} -> ${edge.parent.name} crosses partitions`;
        install.push(
            `CREATE TRIGGER IF NOT EXISTS ${insertName} BEFORE INSERT ON ${quoteIdent(edge.child.name)} ` +
                `WHEN ${mismatch} BEGIN SELECT RAISE(ABORT, '${error}'); END`,
            `CREATE TRIGGER IF NOT EXISTS ${updateName} BEFORE UPDATE ON ${quoteIdent(edge.child.name)} ` +
                `WHEN ${mismatch} BEGIN SELECT RAISE(ABORT, '${error}'); END`
        );
        uninstall.push(`DROP TRIGGER IF EXISTS ${insertName}`, `DROP TRIGGER IF EXISTS ${updateName}`);
    }

    const inboundParents = new Map<string, TableSpec>();
    for (const edge of foreignKeys) inboundParents.set(edge.parent.name, edge.parent);
    for (const parent of [...inboundParents.values()].sort((left, right) => left.name.localeCompare(right.name))) {
        const rawName = `_chardb_fkguard_${migration}_${parent.name}_partition`;
        const name = quoteIdent(rawName);
        names.push(rawName);
        const partition = quoteIdent(parent.partitionColumn);
        install.push(
            `CREATE TRIGGER IF NOT EXISTS ${name} BEFORE UPDATE OF ${partition} ON ${quoteIdent(parent.name)} WHEN OLD.${partition} IS NOT NEW.${partition} BEGIN SELECT RAISE(ABORT, 'CDB_RESHARD_PHASE_MISMATCH: referenced row partition is frozen during split'); END`
        );
        uninstall.push(`DROP TRIGGER IF EXISTS ${name}`);
    }
    return { names, install, uninstall };
}

export function renderReshardForeignKeyGuards(sql: SyncSql, migId: string, tables: readonly TableSpec[]): TriggerSet {
    let migration: string;
    try {
        migration = reshardTriggerMigrationId(migId);
    } catch {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard migration id is invalid" });
    }
    return renderReshardForeignKeyGuardsForMigration(sql, migration, tables);
}

/**
 * Legacy FK guard bodies did not contain their migration identity. Remove
 * them only when durable split state proves this migration is the unique
 * active owner of the lossy legacy suffix.
 */
export function uninstallOwnedLegacyReshardForeignKeyGuards(
    sql: SyncSql,
    migId: string,
    tables: readonly TableSpec[]
): number {
    let legacy: string;
    try {
        legacy = legacyReshardTriggerMigrationId(migId);
    } catch {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "reshard migration id is invalid" });
    }
    const owners = sql.all<{ mig_id: string }>(
        `SELECT mig_id FROM _chardb_split_state
         WHERE replace(mig_id, '-', '_') = ? AND role = 'source' AND drained = 0
         ORDER BY mig_id LIMIT 2`,
        legacy
    );
    if (owners.length !== 1 || owners[0]?.mig_id !== migId) return 0;
    let removed = 0;
    for (const name of renderReshardForeignKeyGuardsForMigration(sql, legacy, tables).names) {
        const stored = sql.one<{ name: string; present: number }>(
            `SELECT name, 1 AS present FROM sqlite_master
             WHERE type = 'trigger' AND name = ? COLLATE NOCASE`,
            name
        );
        if (!stored) continue;
        sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(stored.name)}`);
        removed++;
    }
    return removed;
}

/** Reject application triggers whose side effects cannot be replayed exactly once across a split. */
export function assertNoUnexpectedReshardTriggers(
    sql: SyncSql,
    tables: readonly TableSpec[],
    allowedNames: readonly string[] = []
): void {
    const placeholders = allowedNames.map(() => "?").join(", ");
    for (const table of tables) {
        const unexpected = sql.one<{ name: string }>(
            `SELECT name FROM sqlite_master
             WHERE type = 'trigger' AND tbl_name = ?
             ${allowedNames.length > 0 ? `AND name NOT IN (${placeholders})` : ""}
             ORDER BY name LIMIT 1`,
            table.name,
            ...allowedNames
        );
        if (unexpected) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `online resharding cannot move table ${table.name} while trigger ${unexpected.name} is installed`,
                hint: "remove application triggers or wait for an explicit trigger side-effect transfer protocol",
            });
        }
    }
}

/**
 * Return a deterministic parent-before-child order for immediate FK-safe bulk copy.
 * Every referenced table must participate in the move. Cycles fail closed because
 * SQLite cannot satisfy them through independently committed table batches.
 */
export function orderReshardTables(sql: SyncSql, tables: readonly TableSpec[]): readonly TableSpec[] {
    const byName = new Map(tables.map(table => [table.name, table]));
    const children = new Map<string, Set<string>>();
    const incoming = new Map(tables.map(table => [table.name, 0]));

    for (const table of tables) {
        readReshardTableLayout(sql, table);
        const foreignKeys = sql.all<ForeignKeyRow>(`PRAGMA foreign_key_list(${quoteIdent(table.name)})`);
        const parents = new Set(foreignKeys.map(foreignKey => foreignKey.table));
        for (const parent of parents) {
            if (!byName.has(parent)) {
                throw new CdbError({
                    code: "CDB_RESHARD_PHASE_MISMATCH",
                    message: `reshard table ${table.name} references non-migrating table ${parent}`,
                    hint: "move the complete foreign-key component in one split",
                });
            }
            const next = children.get(parent) ?? new Set<string>();
            if (!next.has(table.name)) {
                next.add(table.name);
                children.set(parent, next);
                incoming.set(table.name, (incoming.get(table.name) ?? 0) + 1);
            }
        }
    }

    const ready = [...incoming]
        .filter(([, count]) => count === 0)
        .map(([name]) => name)
        .sort();
    const ordered: TableSpec[] = [];
    while (ready.length > 0) {
        const name = ready.shift() as string;
        ordered.push(byName.get(name) as TableSpec);
        for (const child of [...(children.get(name) ?? [])].sort()) {
            const count = (incoming.get(child) ?? 0) - 1;
            incoming.set(child, count);
            if (count === 0) {
                ready.push(child);
                ready.sort();
            }
        }
    }
    if (ordered.length !== tables.length) {
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: "reshard tables contain a foreign-key cycle",
            hint: "online resharding requires an acyclic parent-before-child bulk order",
        });
    }
    return ordered;
}

export function assertReshardDestinationRangeEmpty(
    sql: SyncSql,
    tables: readonly TableSpec[],
    _range: RangeFilter
): void {
    for (const table of tables) {
        const row = sql.one<{ found: number }>(`SELECT 1 AS found FROM ${quoteIdent(table.name)} LIMIT 1`);
        if (row) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `reshard destination table ${table.name} is not empty`,
                hint: "V1 resharding requires a fresh physical destination shard",
            });
        }
    }
}

/** Prove the source drained every declared domain table without scanning unrelated ranges. */
export function assertReshardSourceDomainDrained(sql: SyncSql, migId: string, tables: readonly TableSpec[]): void {
    const expected = new Set(tables.map(table => table.name));
    if (expected.size !== tables.length) {
        throw new CdbError({ code: "CDB_INVARIANT", message: "reshard source tables contain duplicate names" });
    }
    const rows = sql.all<{ table_name: string; done: number }>(
        `SELECT table_name, done FROM _chardb_split_drop_cursor
         WHERE mig_id = ? ORDER BY table_name LIMIT ?`,
        migId,
        tables.length + 1
    );
    if (
        rows.length !== tables.length ||
        rows.some(row => row.done !== 1 || !expected.delete(row.table_name)) ||
        expected.size !== 0
    ) {
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: "reshard source domain range is not fully drained",
        });
    }
}

function rowParams(table: TableSpec, row: Readonly<Record<string, unknown>>): readonly SqlParam[] {
    return table.columns.map(column => {
        if (!Object.hasOwn(row, column)) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `reshard row for ${table.name} omitted column ${column}`,
            });
        }
        const value = row[column];
        if (
            value !== null &&
            typeof value !== "string" &&
            typeof value !== "number" &&
            typeof value !== "bigint" &&
            !(value instanceof Uint8Array)
        ) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `reshard row for ${table.name} contains an unsupported value in column ${column}`,
            });
        }
        return value;
    });
}

function primaryKeyParams(
    table: TableSpec,
    row: Readonly<Record<string, unknown>>,
    primaryKey: readonly string[]
): readonly SqlParam[] {
    return primaryKey.map(column => {
        if (!Object.hasOwn(row, column)) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `reshard row for ${table.name} omitted primary-key column ${column}`,
            });
        }
        return row[column] as SqlParam;
    });
}

/** Apply one row without SQLite REPLACE semantics or cross-partition PK takeover. */
export function applyReshardRow(
    sql: SyncSql,
    table: TableSpec,
    row: Readonly<Record<string, unknown>>,
    layout: ReshardTableLayout = readReshardTableLayout(sql, table)
): void {
    const params = rowParams(table, row);
    const keyParams = primaryKeyParams(table, row, layout.primaryKey);
    const keyPredicate = layout.primaryKey.map(column => `${quoteIdent(column)} IS ?`).join(" AND ");
    const existing = sql.one<{ found: number }>(
        `SELECT 1 AS found FROM ${quoteIdent(table.name)} WHERE ${keyPredicate} LIMIT 1`,
        ...keyParams
    );
    if (existing) {
        const samePartition = sql.one<{ found: number }>(
            `SELECT 1 AS found FROM ${quoteIdent(table.name)} WHERE ${keyPredicate} AND ${quoteIdent(
                table.partitionColumn
            )} IS ? LIMIT 1`,
            ...keyParams,
            row[table.partitionColumn] as SqlParam
        );
        if (!samePartition) {
            throw new CdbError({
                code: "CDB_RESHARD_PHASE_MISMATCH",
                message: `reshard primary-key collision crosses partitions in table ${table.name}`,
            });
        }
    }

    const columns = table.columns.map(quoteIdent);
    const updateColumns = table.columns.filter(column => !layout.primaryKey.includes(column));
    const conflict = layout.primaryKey.map(quoteIdent).join(", ");
    const action =
        updateColumns.length === 0
            ? "DO NOTHING"
            : `DO UPDATE SET ${updateColumns
                  .map(column => `${quoteIdent(column)} = excluded.${quoteIdent(column)}`)
                  .join(", ")}`;
    sql.exec(
        `INSERT INTO ${quoteIdent(table.name)} (${columns.join(", ")}) VALUES (${columns
            .map(() => "?")
            .join(", ")}) ON CONFLICT (${conflict}) ${action}`,
        ...params
    );
}

export function deleteReshardRow(
    sql: SyncSql,
    table: TableSpec,
    row: Readonly<Record<string, unknown>>,
    layout: ReshardTableLayout = readReshardTableLayout(sql, table)
): void {
    const exactColumns = layout.primaryKey.includes(table.partitionColumn)
        ? layout.primaryKey
        : [...layout.primaryKey, table.partitionColumn];
    const params = primaryKeyParams(table, row, exactColumns);
    sql.exec(
        `DELETE FROM ${quoteIdent(table.name)} WHERE ${exactColumns
            .map(column => `${quoteIdent(column)} IS ?`)
            .join(" AND ")}`,
        ...params
    );
}

function reshardPrimaryKeyChanged(
    sql: SyncSql,
    table: TableSpec,
    before: Readonly<Record<string, unknown>>,
    after: Readonly<Record<string, unknown>>,
    layout: ReshardTableLayout
): boolean {
    const beforeKey = primaryKeyParams(table, before, layout.primaryKey);
    const afterKey = primaryKeyParams(table, after, layout.primaryKey);
    const params = layout.primaryKey.flatMap((_column, index) => [
        beforeKey[index] as SqlParam,
        afterKey[index] as SqlParam,
    ]);
    const comparison = sql.one<{ same: number }>(
        `SELECT ${layout.primaryKey.map(() => "? IS ?").join(" AND ")} AS same`,
        ...params
    );
    if (!comparison) throw new CdbError({ code: "CDB_INVARIANT", message: "failed to compare reshard primary key" });
    return comparison.same !== 1;
}

/**
 * Apply an update pre-image and post-image inside the caller's transaction.
 * A changed primary-key tuple updates the exact old row in place so SQLite
 * preserves ON UPDATE foreign-key behavior and bulk-copy state cannot survive
 * under the old key. A replay with no old tuple falls back to the normal safe
 * upsert of the post-image.
 */
export function applyReshardUpdate(
    sql: SyncSql,
    table: TableSpec,
    before: Readonly<Record<string, unknown>>,
    after: Readonly<Record<string, unknown>>,
    layout: ReshardTableLayout = readReshardTableLayout(sql, table)
): void {
    if (!reshardPrimaryKeyChanged(sql, table, before, after, layout)) {
        applyReshardRow(sql, table, after, layout);
        return;
    }
    const exactColumns = layout.primaryKey.includes(table.partitionColumn)
        ? layout.primaryKey
        : [...layout.primaryKey, table.partitionColumn];
    const beforeParams = primaryKeyParams(table, before, exactColumns);
    const afterParams = rowParams(table, after);
    sql.exec(
        `UPDATE ${quoteIdent(table.name)} SET ${table.columns
            .map(column => `${quoteIdent(column)} = ?`)
            .join(", ")} WHERE ${exactColumns.map(column => `${quoteIdent(column)} IS ?`).join(" AND ")}`,
        ...afterParams,
        ...beforeParams
    );
    if (sql.changes() !== 0) return;

    const afterKey = primaryKeyParams(table, after, layout.primaryKey);
    const keyPredicate = layout.primaryKey.map(column => `${quoteIdent(column)} IS ?`).join(" AND ");
    const existingAfter = sql.one<{ found: number }>(
        `SELECT 1 AS found FROM ${quoteIdent(table.name)} WHERE ${keyPredicate} LIMIT 1`,
        ...afterKey
    );
    if (!existingAfter) {
        applyReshardRow(sql, table, after, layout);
        return;
    }
    const exactAfter = sql.one<{ found: number }>(
        `SELECT 1 AS found FROM ${quoteIdent(table.name)} WHERE ${keyPredicate} AND ${table.columns
            .map(column => `${quoteIdent(column)} IS ?`)
            .join(" AND ")} LIMIT 1`,
        ...afterKey,
        ...rowParams(table, after)
    );
    if (!exactAfter) {
        throw new CdbError({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: `reshard changed primary-key replay collides with a different row in table ${table.name}`,
        });
    }
}

const FILE_SYSTEM_TABLE = "_chardb_files";
const TOMBSTONE_SYSTEM_TABLE = "_chardb_deleted_organizations";
const FILE_IMAGE_KEYS = [
    "column_name",
    "content_type",
    "created_at",
    "file_id",
    "object_key",
    "organization_id",
    "placement_vshard",
    "row_id",
    "sha256",
    "size",
    "status",
    "table_name",
    "updated_at",
] as const;
const TOMBSTONE_IMAGE_KEYS = ["deleted_at", "organization_id", "placement_vshard", "vector_unproven_turns"] as const;

export interface ReshardSystemTailEntry {
    readonly lsn: number;
    readonly op: "ins" | "upd" | "del";
    readonly table_name: string;
    readonly pk: string;
    readonly before: JsonText | null;
    readonly after: JsonText | null;
}

interface SystemMigrationLedger {
    readonly inserted: number;
    readonly snapshot_through_lsn: number | null;
}

function systemMismatch(message: string): never {
    throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message });
}

function parseExactSystemImage(
    value: JsonText | null,
    keys: readonly string[],
    subject: string
): Record<string, unknown> {
    if (value === null) systemMismatch(`${subject} image is missing`);
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        systemMismatch(`${subject} image is not valid JSON`);
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        systemMismatch(`${subject} image is not an object`);
    }
    const actual = Object.keys(parsed).sort();
    if (actual.length !== keys.length || actual.some((key, index) => key !== keys[index])) {
        systemMismatch(`${subject} image fields are not exact`);
    }
    return parsed as Record<string, unknown>;
}

function fileImage(value: JsonText | null): CdbReshardFileRecord {
    const row = parseExactSystemImage(value, FILE_IMAGE_KEYS, "file tail") as unknown as CdbReshardStoredFileRow;
    return projectCdbReshardFileRecord(row);
}

function tombstoneImage(value: JsonText | null): CdbReshardOrganizationTombstone {
    if (value === null) systemMismatch("organization tombstone tail image is missing");
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        systemMismatch("organization tombstone tail image is not valid JSON");
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        systemMismatch("organization tombstone tail image is not an object");
    }
    const fields = parsed as Record<string, unknown>;
    const actual = Object.keys(fields).sort();
    const legacyKeys = TOMBSTONE_IMAGE_KEYS.filter(key => key !== "vector_unproven_turns");
    if (
        !(
            (actual.length === TOMBSTONE_IMAGE_KEYS.length &&
                actual.every((key, index) => key === TOMBSTONE_IMAGE_KEYS[index])) ||
            (actual.length === legacyKeys.length && actual.every((key, index) => key === legacyKeys[index]))
        )
    ) {
        systemMismatch("organization tombstone tail image fields are not exact");
    }
    const row = {
        vector_unproven_turns: 0,
        ...fields,
    } as unknown as CdbReshardStoredTombstoneRow;
    return projectCdbReshardTombstone(row);
}

function readSystemFile(sql: SyncSql, fileId: string): CdbReshardFileRecord | null {
    const row = sql.one<CdbReshardStoredFileRow>("SELECT * FROM _chardb_files WHERE file_id = ?", fileId);
    return row ? projectCdbReshardFileRecord(row) : null;
}

function readSystemTombstone(sql: SyncSql, organizationId: string): CdbReshardOrganizationTombstone | null {
    const row = sql.one<CdbReshardStoredTombstoneRow>(
        `SELECT organization_id, deleted_at, placement_vshard, vector_unproven_turns FROM _chardb_deleted_organizations
         WHERE organization_id = ?`,
        organizationId
    );
    return row ? projectCdbReshardTombstone(row) : null;
}

function systemMigrationLedger(
    sql: SyncSql,
    migId: string,
    kind: "file" | "organization_tombstone",
    id: string
): SystemMigrationLedger | null {
    return sql.one<SystemMigrationLedger>(
        `SELECT inserted, snapshot_through_lsn FROM _chardb_split_file_applied
         WHERE mig_id = ? AND record_kind = ? AND record_id = ?`,
        migId,
        kind,
        id
    );
}

function recordTailSystemRow(sql: SyncSql, migId: string, kind: "file" | "organization_tombstone", id: string): void {
    sql.exec(
        `INSERT INTO _chardb_split_file_applied
           (mig_id, record_kind, record_id, inserted, snapshot_through_lsn)
         VALUES (?, ?, ?, 1, NULL)`,
        migId,
        kind,
        id
    );
    if (sql.changes() !== 1) systemMismatch(`${kind} ${id} lost its migration provenance`);
}

function snapshotCoversEntry(ledger: SystemMigrationLedger | null, lsn: number): boolean {
    return ledger?.snapshot_through_lsn !== null && lsn <= (ledger?.snapshot_through_lsn ?? -1);
}

function insertSystemFile(sql: SyncSql, row: CdbReshardFileRecord): void {
    sql.exec(
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

function updateSystemFile(sql: SyncSql, row: CdbReshardFileRecord): void {
    sql.exec(
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
    if (sql.changes() !== 1) systemMismatch(`file ${row.fileId} changed during tail apply`);
}

function assertFileTransition(before: CdbReshardFileRecord, after: CdbReshardFileRecord): void {
    if (
        before.fileId !== after.fileId ||
        before.organizationId !== after.organizationId ||
        before.table !== after.table ||
        before.column !== after.column ||
        before.objectKey !== after.objectKey ||
        before.contentType !== after.contentType ||
        before.size !== after.size ||
        before.createdAt !== after.createdAt ||
        before.placementVshard !== after.placementVshard
    ) {
        systemMismatch(`file ${before.fileId} changed immutable ownership during tail apply`);
    }
    const transition = `${before.status}->${after.status}`;
    if (
        ![
            "pending->pending",
            "pending->ready",
            "pending->deleting",
            "ready->attached",
            "ready->deleting",
            "attached->deleting",
        ].includes(transition)
    ) {
        systemMismatch(`file ${before.fileId} has invalid tail transition ${transition}`);
    }
}

function assertSystemPlacement(placement: number, range: RangeFilter): void {
    if (placement < range.lo || placement > range.hi) {
        systemMismatch("file system tail placement is outside the moving range");
    }
}

export function isKnownReshardTailTable(tableName: string, domainTables: ReadonlySet<string>): boolean {
    if (domainTables.has(tableName)) return true;
    if (
        tableName === FILE_SYSTEM_TABLE ||
        tableName === TOMBSTONE_SYSTEM_TABLE ||
        tableName === CDB_VECTOR_HEAD_TAIL_TABLE ||
        tableName === CDB_VECTOR_OUTBOX_TAIL_TABLE ||
        tableName === CDB_VECTOR_ATTEMPT_TAIL_TABLE
    )
        return true;
    if (tableName.startsWith("_") || tableName.startsWith("sqlite")) {
        throw new CdbError({ code: "CDB_INVARIANT", message: `reshard tail names unknown system table ${tableName}` });
    }
    throw new CdbError({ code: "CDB_INVARIANT", message: `reshard tail names unknown table ${tableName}` });
}

/**
 * Apply one exact file-system tail entry. Returns false for a registered
 * domain table so the caller can use the ordinary relational executor.
 */
export function applyReshardSystemTailEntry(
    sql: SyncSql,
    migId: string,
    entry: ReshardSystemTailEntry,
    range: RangeFilter
): boolean {
    if (applyCdbVectorTailEntry(sql, migId, entry, range)) return true;
    if (entry.table_name !== FILE_SYSTEM_TABLE && entry.table_name !== TOMBSTONE_SYSTEM_TABLE) return false;
    if (typeof migId !== "string" || migId.length === 0 || new TextEncoder().encode(migId).byteLength > 256) {
        systemMismatch("file system tail migration identity is invalid");
    }
    if (!Number.isSafeInteger(entry.lsn) || entry.lsn < 1) systemMismatch("file system tail LSN is invalid");
    if (entry.table_name === TOMBSTONE_SYSTEM_TABLE) {
        if (entry.op !== "ins" && entry.op !== "upd") {
            systemMismatch("organization tombstone tail only accepts insert and update entries");
        }
        const after = tombstoneImage(entry.after);
        const before = entry.op === "upd" ? tombstoneImage(entry.before) : null;
        if (entry.op === "ins" && entry.before !== null) {
            systemMismatch("organization tombstone insert tail includes a pre-image");
        }
        if (entry.pk !== after.organizationId) systemMismatch("organization tombstone tail primary key changed");
        if (
            before &&
            (before.organizationId !== after.organizationId ||
                before.deletedAt !== after.deletedAt ||
                before.placementVshard !== after.placementVshard ||
                after.vectorUnprovenTurns < before.vectorUnprovenTurns)
        ) {
            systemMismatch("organization tombstone tail changed immutable state or regressed vector purge turns");
        }
        assertSystemPlacement(after.placementVshard, range);
        const existing = readSystemTombstone(sql, after.organizationId);
        const ledger = systemMigrationLedger(sql, migId, "organization_tombstone", after.organizationId);
        if (snapshotCoversEntry(ledger, entry.lsn)) {
            if (!existing) systemMismatch(`organization tombstone ${after.organizationId} lost its snapshot row`);
            if (
                existing.vectorUnprovenTurns < after.vectorUnprovenTurns ||
                !exactCdbReshardTombstone({ ...after, vectorUnprovenTurns: existing.vectorUnprovenTurns }, existing)
            ) {
                systemMismatch(`organization tombstone ${after.organizationId} differs from its covered tail image`);
            }
            return true;
        }
        if (entry.op === "upd") {
            if (!ledger || !existing || !before || !exactCdbReshardTombstone(existing, before)) {
                systemMismatch(`organization tombstone ${after.organizationId} update pre-image differs`);
            }
            sql.exec(
                `UPDATE _chardb_deleted_organizations SET vector_unproven_turns = ?
                 WHERE organization_id = ? AND vector_unproven_turns = ?`,
                after.vectorUnprovenTurns,
                after.organizationId,
                before.vectorUnprovenTurns
            );
            if (sql.changes() !== 1) {
                systemMismatch(`organization tombstone ${after.organizationId} changed during tail apply`);
            }
            return true;
        }
        if (existing && (!ledger || !exactCdbReshardTombstone(existing, after))) {
            systemMismatch(`organization tombstone ${after.organizationId} collides during tail apply`);
        }
        if (!existing) {
            if (ledger) systemMismatch(`organization tombstone ${after.organizationId} was reused during tail apply`);
            sql.exec(
                `INSERT INTO _chardb_deleted_organizations
                   (organization_id, deleted_at, placement_vshard, vector_unproven_turns)
                 VALUES (?, ?, ?, ?)`,
                after.organizationId,
                after.deletedAt,
                after.placementVshard,
                after.vectorUnprovenTurns
            );
            recordTailSystemRow(sql, migId, "organization_tombstone", after.organizationId);
        }
        return true;
    }

    if (entry.op === "ins") {
        if (entry.before !== null) systemMismatch("file insert tail includes a pre-image");
        const after = fileImage(entry.after);
        if (after.status !== "pending") systemMismatch("file insert tail does not begin in pending state");
        if (entry.pk !== after.fileId) systemMismatch("file insert tail primary key changed");
        assertSystemPlacement(after.placementVshard, range);
        const existing = readSystemFile(sql, after.fileId);
        const ledger = systemMigrationLedger(sql, migId, "file", after.fileId);
        if (snapshotCoversEntry(ledger, entry.lsn)) {
            if (!existing) systemMismatch(`file ${after.fileId} lost its snapshot row`);
            return true;
        }
        if (existing && (!ledger || !exactCdbReshardFile(existing, after))) {
            systemMismatch(`file ${after.fileId} collides during insert tail apply`);
        }
        if (!existing) {
            if (ledger) systemMismatch(`file ${after.fileId} was reused during insert tail apply`);
            insertSystemFile(sql, after);
            recordTailSystemRow(sql, migId, "file", after.fileId);
        }
        return true;
    }
    if (entry.op === "upd") {
        const before = fileImage(entry.before);
        const after = fileImage(entry.after);
        if (entry.pk !== after.fileId || before.fileId !== after.fileId) {
            systemMismatch("file update tail primary key changed");
        }
        assertSystemPlacement(after.placementVshard, range);
        assertFileTransition(before, after);
        const existing = readSystemFile(sql, after.fileId);
        const ledger = systemMigrationLedger(sql, migId, "file", after.fileId);
        if (snapshotCoversEntry(ledger, entry.lsn)) {
            if (!existing) systemMismatch(`file ${after.fileId} lost its snapshot row`);
            return true;
        }
        if (existing && ledger && exactCdbReshardFile(existing, after)) return true;
        if (!existing && !ledger) {
            insertSystemFile(sql, before);
            recordTailSystemRow(sql, migId, "file", before.fileId);
            updateSystemFile(sql, after);
            return true;
        }
        if (!existing || !exactCdbReshardFile(existing, before)) {
            systemMismatch(`file ${after.fileId} pre-image changed during update tail apply`);
        }
        updateSystemFile(sql, after);
        return true;
    }
    const before = fileImage(entry.before);
    if (entry.after !== null) systemMismatch("file delete tail includes a post-image");
    if (before.status !== "deleting") systemMismatch("file delete tail does not own deletion state");
    if (entry.pk !== before.fileId) systemMismatch("file delete tail primary key changed");
    assertSystemPlacement(before.placementVshard, range);
    const existing = readSystemFile(sql, before.fileId);
    const ledger = systemMigrationLedger(sql, migId, "file", before.fileId);
    if (snapshotCoversEntry(ledger, entry.lsn)) {
        if (!existing) systemMismatch(`file ${before.fileId} lost its snapshot row`);
        return true;
    }
    if (!existing) return true;
    if (!ledger || !exactCdbReshardFile(existing, before)) {
        systemMismatch(`file ${before.fileId} pre-image changed during delete tail apply`);
    }
    sql.exec("DELETE FROM _chardb_files WHERE file_id = ?", before.fileId);
    if (sql.changes() !== 1) systemMismatch(`file ${before.fileId} changed during delete tail apply`);
    return true;
}
