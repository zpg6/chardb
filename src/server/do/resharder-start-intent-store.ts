import { CdbError } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { VSHARD_COUNT } from "../../vshard.ts";

export const RESHARDER_START_INTENT_DDL = `
CREATE TABLE IF NOT EXISTS migration_start_intent (
  mig_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('starting', 'abort_requested')),
  src_shard TEXT,
  dst_shard TEXT,
  range_lo INTEGER,
  range_hi INTEGER,
  epoch_at_start INTEGER,
  recovery_generation INTEGER CHECK (recovery_generation >= 0),
  tables_json TEXT,
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
  CHECK (
    (src_shard IS NULL AND dst_shard IS NULL AND range_lo IS NULL AND range_hi IS NULL
      AND epoch_at_start IS NULL AND recovery_generation IS NULL AND tables_json IS NULL)
    OR
    (src_shard IS NOT NULL AND dst_shard IS NOT NULL AND range_lo IS NOT NULL AND range_hi IS NOT NULL
      AND epoch_at_start IS NOT NULL AND recovery_generation IS NOT NULL AND tables_json IS NOT NULL)
  )
);
` as const;

export interface ResharderStartIdentity {
    readonly migId: string;
    readonly srcShard: string;
    readonly dstShard: string;
    readonly rangeLo: number;
    readonly rangeHi: number;
    readonly epochAtStart: number;
    readonly recoveryGeneration: number;
    readonly tablesJson: string;
}

export interface ResharderStartIntent {
    readonly migId: string;
    readonly state: "starting" | "abort_requested";
    readonly identity: ResharderStartIdentity | null;
}

interface StoredStartIntent {
    readonly mig_id: string;
    readonly state: ResharderStartIntent["state"];
    readonly src_shard: string | null;
    readonly dst_shard: string | null;
    readonly range_lo: number | null;
    readonly range_hi: number | null;
    readonly epoch_at_start: number | null;
    readonly recovery_generation: number | null;
    readonly tables_json: string | null;
}

const MIGRATION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

function invalid(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message: `reshard start intent: ${message}` });
}

function mismatch(message: string): never {
    throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message });
}

function assertMigrationId(migId: string): void {
    if (typeof migId !== "string" || !MIGRATION_ID.test(migId)) invalid("migration id is invalid");
}

function sameIdentity(left: ResharderStartIdentity, right: ResharderStartIdentity): boolean {
    return (
        left.migId === right.migId &&
        left.srcShard === right.srcShard &&
        left.dstShard === right.dstShard &&
        left.rangeLo === right.rangeLo &&
        left.rangeHi === right.rangeHi &&
        left.epochAtStart === right.epochAtStart &&
        left.recoveryGeneration === right.recoveryGeneration &&
        left.tablesJson === right.tablesJson
    );
}

function project(row: StoredStartIntent): ResharderStartIntent {
    const values = [
        row.src_shard,
        row.dst_shard,
        row.range_lo,
        row.range_hi,
        row.epoch_at_start,
        row.recovery_generation,
        row.tables_json,
    ];
    const allNull = values.every(value => value === null);
    const allPresent = values.every(value => value !== null);
    if (!allNull && !allPresent) mismatch(`migId=${row.mig_id} has a partial durable start identity`);
    return {
        migId: row.mig_id,
        state: row.state,
        identity: allNull
            ? null
            : {
                  migId: row.mig_id,
                  srcShard: row.src_shard as string,
                  dstShard: row.dst_shard as string,
                  rangeLo: row.range_lo as number,
                  rangeHi: row.range_hi as number,
                  epochAtStart: row.epoch_at_start as number,
                  recoveryGeneration: row.recovery_generation as number,
                  tablesJson: row.tables_json as string,
              },
    };
}

/** Durable start identity and cancellation tombstone for the Catalog await window. */
export class ResharderStartIntentStore {
    constructor(private readonly sql: SyncSql) {}

    read(migId: string): ResharderStartIntent | null {
        assertMigrationId(migId);
        const row = this.sql.one<StoredStartIntent>(
            `SELECT mig_id, state, src_shard, dst_shard, range_lo, range_hi, epoch_at_start,
                    recovery_generation, tables_json
             FROM migration_start_intent WHERE mig_id = ?`,
            migId
        );
        return row ? project(row) : null;
    }

    begin(identity: ResharderStartIdentity, nowMs: number): ResharderStartIntent {
        assertMigrationId(identity.migId);
        if (!Number.isSafeInteger(nowMs) || nowMs < 0) invalid("timestamp is invalid");
        const existing = this.read(identity.migId);
        if (existing) {
            if (existing.identity && !sameIdentity(existing.identity, identity)) {
                mismatch(`migId=${identity.migId} is already bound to a different split start`);
            }
            if (existing.state === "abort_requested") {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: `migId=${identity.migId} was canceled before its split started`,
                });
            }
            if (!existing.identity) mismatch(`migId=${identity.migId} has no durable split start identity`);
            return existing;
        }
        this.assertCapacity();
        this.sql.exec(
            `INSERT INTO migration_start_intent
             (mig_id, state, src_shard, dst_shard, range_lo, range_hi, epoch_at_start, recovery_generation, tables_json,
              created_at, updated_at)
             VALUES (?, 'starting', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            identity.migId,
            identity.srcShard,
            identity.dstShard,
            identity.rangeLo,
            identity.rangeHi,
            identity.epochAtStart,
            identity.recoveryGeneration,
            identity.tablesJson,
            nowMs,
            nowMs
        );
        return this.requireExact(identity);
    }

    requestAbort(migId: string, nowMs: number): ResharderStartIntent {
        assertMigrationId(migId);
        if (!Number.isSafeInteger(nowMs) || nowMs < 0) invalid("timestamp is invalid");
        const existing = this.read(migId);
        if (!existing) {
            this.assertCapacity();
            this.sql.exec(
                `INSERT INTO migration_start_intent
                 (mig_id, state, recovery_generation, created_at, updated_at)
                 VALUES (?, 'abort_requested', NULL, ?, ?)`,
                migId,
                nowMs,
                nowMs
            );
        } else if (existing.state === "starting") {
            this.sql.exec(
                `UPDATE migration_start_intent SET state = 'abort_requested', updated_at = ?
                 WHERE mig_id = ? AND state = 'starting'`,
                nowMs,
                migId
            );
        }
        return this.required(migId);
    }

    requireExact(identity: ResharderStartIdentity): ResharderStartIntent {
        const intent = this.required(identity.migId);
        if (!intent.identity || !sameIdentity(intent.identity, identity)) {
            mismatch(`migId=${identity.migId} start identity changed`);
        }
        return intent;
    }

    clearStarted(identity: ResharderStartIdentity): void {
        const intent = this.requireExact(identity);
        if (intent.state !== "starting") {
            throw new CdbError({
                code: "CDB_STALE_EPOCH",
                message: `migId=${identity.migId} was canceled before its split started`,
            });
        }
        this.sql.exec("DELETE FROM migration_start_intent WHERE mig_id = ? AND state = 'starting'", identity.migId);
        if (this.sql.changes() !== 1) mismatch(`migId=${identity.migId} start intent changed before completion`);
    }

    private required(migId: string): ResharderStartIntent {
        const intent = this.read(migId);
        if (!intent) mismatch(`migId=${migId} has no durable start intent`);
        return intent;
    }

    private assertCapacity(): void {
        const row = this.sql.one<{ count: number }>("SELECT COUNT(*) AS count FROM migration_start_intent");
        if (!row || !Number.isSafeInteger(row.count) || row.count < 0) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "reshard start intent count is invalid" });
        }
        if (row.count >= VSHARD_COUNT) {
            throw new CdbError({
                code: "CDB_RATE_LIMITED",
                message: `reshard start cancellation history reached its ${VSHARD_COUNT}-record limit`,
            });
        }
    }
}
