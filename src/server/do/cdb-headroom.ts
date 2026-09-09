/**
 * Automatic shard headroom. A Cdb reports its SQLite size once it passes the
 * split mark; the Resharder scans that shard's rows in bounded pages, hashes
 * partition values here because SQLite has no xxhash64, and moves the prefix
 * that holds about half of the rows into a fresh shard.
 */

import { rowVshard } from "../../reshard/range.ts";
import type { TableSpec } from "../../reshard/triggers.ts";
import type { VshardRange } from "../../vshard.ts";
import type { SqlStorageLike } from "./sql_adapter.ts";

/** Half of Cloudflare's SQLite-backed Durable Object cap, so a split finishes with room to spare. */
export const CDB_AUTO_SPLIT_BYTES = 5 * 1024 * 1024 * 1024;
/** A shard reports once per this fraction of the mark of growth. */
export const CDB_HEADROOM_REPORT_STEPS = 8;
export const CDB_HEADROOM_PAGE_ROWS = 4_096;
/** A move carrying less than this fraction of a range's rows relieves nothing and is refused. */
const MIN_SHARE = 8;

export interface HeadroomPage {
    /** Rowid to continue from, or `null` once the table is exhausted. */
    readonly nextRowid: number | null;
    readonly vshards: readonly (readonly [vshard: number, rows: number])[];
}

export interface SplitSuggestion {
    readonly lo: number;
    readonly hi: number;
    readonly rows: number;
}

export interface CdbHeadroomRpc {
    readHeadroomPage(args: { readonly table: string; readonly afterRowid: number }): Promise<HeadroomPage>;
}

export interface ResharderHeadroomRpc {
    reportShardSize(args: { readonly cdbId: string; readonly bytes: number }): Promise<void>;
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
const SHARD_SEQUENCE = /^ShardDO_(\d{1,9})$/;

function quoteIdent(raw: string): string {
    if (!IDENT.test(raw)) throw new TypeError(`headroom: refusing non-identifier name ${raw}`);
    return `"${raw}"`;
}

export function readHeadroomPage(sql: SqlStorageLike, spec: TableSpec, afterRowid: number): HeadroomPage {
    if (!Number.isSafeInteger(afterRowid) || afterRowid < 0) throw new TypeError("headroom rowid is invalid");
    const cursor = sql.exec(
        `SELECT rowid, ${quoteIdent(spec.partitionColumn)} FROM ${quoteIdent(spec.name)}
         WHERE rowid > ? ORDER BY rowid LIMIT ?`,
        afterRowid,
        CDB_HEADROOM_PAGE_ROWS
    );
    const keys = new Map<unknown, number>();
    let seen = 0;
    let lastRowid = afterRowid;
    for (const [rowid, key] of cursor.raw()) {
        keys.set(key, (keys.get(key) ?? 0) + 1);
        lastRowid = Number(rowid);
        seen++;
    }
    const counts = new Map<number, number>();
    for (const [key, rows] of keys) {
        const vshard = rowVshard(key);
        counts.set(vshard, (counts.get(vshard) ?? 0) + rows);
    }
    return { nextRowid: seen === CDB_HEADROOM_PAGE_ROWS ? lastRowid : null, vshards: [...counts] };
}

/**
 * Take the heaviest owned range and cut it where the prefix lands nearest
 * half of its rows; empty vshards after the cut go with the prefix so the new
 * shard receives future owners too. Rows that sit in one vshard, or a prefix
 * too small to matter, yield `null`.
 */
export function suggestSplit(ranges: readonly VshardRange[], vshards: ArrayLike<number>): SplitSuggestion | null {
    let best: { readonly range: VshardRange; readonly total: number } | null = null;
    for (const range of ranges) {
        let total = 0;
        for (let v = range.lo; v <= range.hi; v++) total += vshards[v] ?? 0;
        if (total > 0 && (!best || total > best.total)) best = { range, total };
    }
    if (!best) return null;
    const { range, total } = best;
    let prefix = 0;
    let cut = -1;
    let rows = 0;
    for (let v = range.lo; v < range.hi; v++) {
        prefix += vshards[v] ?? 0;
        if (prefix === 0 || prefix === rows) continue;
        if (prefix === total || (cut >= 0 && Math.abs(2 * prefix - total) >= Math.abs(2 * rows - total))) break;
        cut = v;
        rows = prefix;
    }
    if (cut < 0 || rows * MIN_SHARE < total) return null;
    let hi = cut;
    while (hi + 1 < range.hi && (vshards[hi + 1] ?? 0) === 0) hi++;
    return { lo: range.lo, hi, rows };
}

/** Next `ShardDO_<n>` name, or `null` when an operator chose a naming scheme this code must not guess. */
export function nextShardId(shardIds: readonly string[]): string | null {
    let max = -1;
    for (const shardId of shardIds) {
        const match = SHARD_SEQUENCE.exec(shardId);
        if (!match) return null;
        max = Math.max(max, Number(match[1]));
    }
    return max < 0 ? null : `ShardDO_${max + 1}`;
}
