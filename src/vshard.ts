/**
 * vshard router.
 *
 * The pinned routing surface is fixed for life of the major version:
 *
 *   - 16,384 vshards as the routing namespace
 *   - xxhash64 with seed 0 as the partition-key hash
 *   - the (vshard_lo, vshard_hi) → ShardDO map only ever splits, never merges
 *
 * The Vitess-style range table grows by carving sub-ranges out of an existing
 * shard's range and reassigning them to a new shard, never by re-hashing rows
 * (see https://vitess.io/docs/22.0/reference/features/vindexes/ for the
 * underlying vindex pattern).
 *
 * Composite partition keys hash via
 * `xxhash64(canonical_concat(cols, sep=0x1F))`; the composite encoding is part
 * of the partition contract digest emitted by `chardb doctor schema`.
 */

import { xxhash64 } from "./hash/xxhash64.ts";
import { ShardId, type Vshard } from "./types.ts";

export const VSHARD_COUNT = 16_384 as const;
export const XXHASH64_SEED = 0n;
const SEP = 0x1f;

const TEXT = new TextEncoder();

/** Canonically concatenate composite partition-key columns. */
export function canonicalConcat(cols: readonly (string | number | bigint | Uint8Array)[]): Uint8Array {
    const parts: Uint8Array[] = [];
    for (let i = 0; i < cols.length; i++) {
        if (i > 0) parts.push(new Uint8Array([SEP]));
        const c = cols[i];
        if (c instanceof Uint8Array) parts.push(c);
        else if (typeof c === "string") parts.push(TEXT.encode(c));
        else if (typeof c === "number" || typeof c === "bigint") parts.push(TEXT.encode(String(c)));
        else throw new TypeError(`unsupported partition key scalar: ${typeof c}`);
    }
    let len = 0;
    for (const p of parts) len += p.length;
    const out = new Uint8Array(len);
    let off = 0;
    for (const p of parts) {
        out.set(p, off);
        off += p.length;
    }
    return out;
}

/** xxhash64(seed=0) of canonicalConcat(cols), modulo VSHARD_COUNT. */
export function vshardOf(cols: readonly (string | number | bigint | Uint8Array)[]): Vshard {
    const bytes = canonicalConcat(cols);
    const h = xxhash64(bytes, XXHASH64_SEED);
    return Number(h % BigInt(VSHARD_COUNT)) as Vshard;
}

/** Inclusive range `[lo, hi]` over [0, VSHARD_COUNT). */
export interface VshardRange {
    readonly lo: number;
    readonly hi: number;
    readonly shardId: ShardId;
}

/**
 * The (vshard_lo, vshard_hi) → ShardDO map. Ranges only split, never merge —
 * a shard rebalance is always a `split(lo, hi, newShard)` carving a sub-range
 * out of an existing range.
 *
 * Stored as a sorted array (one per logical DB; never more than ~128 entries).
 */
export class VshardMap {
    private readonly ranges: VshardRange[] = [];

    constructor(initial: readonly VshardRange[] = [{ lo: 0, hi: VSHARD_COUNT - 1, shardId: ShardId("ShardDO_0") }]) {
        for (const r of initial) this.insertSorted(r);
        this.assertContiguous();
    }

    private insertSorted(r: VshardRange): void {
        if (!Number.isInteger(r.lo) || !Number.isInteger(r.hi) || r.lo < 0 || r.hi >= VSHARD_COUNT || r.lo > r.hi) {
            throw new RangeError(`bad vshard range [${r.lo}, ${r.hi}]`);
        }
        let i = 0;
        while (i < this.ranges.length && (this.ranges[i] as VshardRange).lo < r.lo) i++;
        this.ranges.splice(i, 0, r);
    }

    private assertContiguous(): void {
        if (this.ranges.length === 0) throw new RangeError("vshard map is empty");
        if ((this.ranges[0] as VshardRange).lo !== 0) {
            throw new RangeError("vshard map does not start at 0");
        }
        for (let i = 1; i < this.ranges.length; i++) {
            const prev = this.ranges[i - 1] as VshardRange;
            const cur = this.ranges[i] as VshardRange;
            if (cur.lo !== prev.hi + 1) {
                throw new RangeError(`vshard map gap between ${prev.hi} and ${cur.lo}`);
            }
        }
        const last = this.ranges[this.ranges.length - 1] as VshardRange;
        if (last.hi !== VSHARD_COUNT - 1) {
            throw new RangeError(`vshard map does not end at ${VSHARD_COUNT - 1}`);
        }
    }

    routeVshard(v: Vshard): ShardId {
        if (!Number.isInteger(v) || v < 0 || v >= VSHARD_COUNT) throw new RangeError(`vshard ${v} out of range`);
        let lo = 0;
        let hi = this.ranges.length - 1;
        while (lo <= hi) {
            const mid = (lo + hi) >>> 1;
            const r = this.ranges[mid] as VshardRange;
            if (v < r.lo) hi = mid - 1;
            else if (v > r.hi) lo = mid + 1;
            else return r.shardId;
        }
        throw new RangeError(`vshard ${v} not routable (corrupt map)`);
    }

    /**
     * Split a contiguous sub-range out to a new shard. Validates that the
     * sub-range falls entirely inside one existing range and that the resulting
     * map remains contiguous and monotonic.
     */
    split(lo: number, hi: number, toShard: ShardId): VshardMap {
        const idx = this.ranges.findIndex(r => r.lo <= lo && hi <= r.hi);
        if (idx < 0) {
            throw new RangeError(`split [${lo}, ${hi}] does not lie within a single existing range`);
        }
        const target = this.ranges[idx] as VshardRange;
        if (target.shardId === toShard) {
            throw new RangeError(`split target shard ${toShard} matches source`);
        }
        const next: VshardRange[] = [];
        for (let i = 0; i < idx; i++) next.push(this.ranges[i] as VshardRange);
        if (target.lo < lo) next.push({ lo: target.lo, hi: lo - 1, shardId: target.shardId });
        next.push({ lo, hi, shardId: toShard });
        if (hi < target.hi) next.push({ lo: hi + 1, hi: target.hi, shardId: target.shardId });
        for (let i = idx + 1; i < this.ranges.length; i++) next.push(this.ranges[i] as VshardRange);
        return new VshardMap(next);
    }

    ranges_(): readonly VshardRange[] {
        return this.ranges;
    }
}
