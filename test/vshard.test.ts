import { describe, expect, test } from "bun:test";
import { ShardId, type Vshard } from "../src/types.ts";
import { VSHARD_COUNT, VshardMap, canonicalConcat, vshardOf } from "../src/vshard.ts";

describe("vshard router", () => {
    test("routing matches published xxhash64 seed-zero vectors", () => {
        expect(Number(vshardOf([]))).toBe(Number(0xef46db3751d8e999n % 16384n));
        expect(Number(vshardOf(["a"]))).toBe(Number(0xd24ec4f1a98c6e5bn % 16384n));
        expect(Number(vshardOf(["asdf"]))).toBe(Number(0x415872f599cea71en % 16384n));
    });

    test("composite key encoding differs from concatenated single-string key", () => {
        const a = vshardOf(["abc", "def"]);
        const b = vshardOf(["abcdef"]);
        expect(a).not.toBe(b);
    });

    test("canonicalConcat inserts a sep between cols", () => {
        const a = canonicalConcat(["a", "b"]);
        const b = canonicalConcat(["ab"]);
        expect([...a]).toEqual([97, 31, 98]);
        expect([...b]).toEqual([97, 98]);
    });

    test("composite keys with unicode encode UTF-8 deterministically", () => {
        expect([...canonicalConcat(["café", "🚀"])]).toEqual([99, 97, 102, 195, 169, 31, 240, 159, 154, 128]);
        // ASCII-only spelling differs from unicode spelling.
        expect(vshardOf(["cafe"])).not.toBe(vshardOf(["café"]));
    });

    test("composite keys with bigint and number coerce via String(...) so 1n and 1 hash equal", () => {
        expect(vshardOf([1n])).toBe(vshardOf([1]));
        expect(vshardOf([1n])).toBe(vshardOf(["1"]));
        expect(vshardOf([2n ** 53n + 7n])).toBe(vshardOf([String(2n ** 53n + 7n)]));
    });

    test("composite keys with Uint8Array travel through canonicalConcat untouched", () => {
        const bytes = new Uint8Array([1, 2, 3, 4, 5]);
        expect(canonicalConcat([bytes])).toEqual(bytes);
        // Different bytes → different vshard (probabilistically; deterministic for these inputs).
        expect(vshardOf([new Uint8Array([1, 2, 3])])).not.toBe(vshardOf([new Uint8Array([3, 2, 1])]));
    });

    test("permuted composite keys produce different vshards (column order is part of the key)", () => {
        expect(vshardOf(["alpha", "beta"])).not.toBe(vshardOf(["beta", "alpha"]));
    });

    test("unsupported scalar types throw with a clear message", () => {
        // boolean / object / null are not in the supported scalar set.
        expect(() => vshardOf([true as unknown as string])).toThrow(/unsupported partition key scalar/);
        expect(() => vshardOf([null as unknown as string])).toThrow(/unsupported partition key scalar/);
        expect(() => vshardOf([{} as unknown as string])).toThrow(/unsupported partition key scalar/);
    });
});

describe("VshardMap", () => {
    test("rejects routing values outside the integer namespace", () => {
        const map = new VshardMap();
        for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, -1, 0.5, VSHARD_COUNT]) {
            expect(() => map.routeVshard(value as Vshard)).toThrow(RangeError);
        }
    });

    test("rejects empty and fractional range maps", () => {
        expect(() => new VshardMap([])).toThrow(RangeError);
        expect(
            () =>
                new VshardMap([
                    { lo: 0, hi: 0.5, shardId: ShardId("a") },
                    { lo: 1.5, hi: VSHARD_COUNT - 1, shardId: ShardId("b") },
                ])
        ).toThrow(RangeError);
    });

    test("rejects fractional splits without changing ownership", () => {
        const map = new VshardMap();
        expect(() => map.split(0.5, 10.5, ShardId("new"))).toThrow(RangeError);
        expect(map.ranges_()).toEqual([{ lo: 0, hi: VSHARD_COUNT - 1, shardId: ShardId("ShardDO_0") }]);
    });

    test("default map covers full namespace, single shard", () => {
        const m = new VshardMap();
        expect(m.routeVshard(0 as Vshard)).toBe(ShardId("ShardDO_0"));
        expect(m.routeVshard((VSHARD_COUNT - 1) as Vshard)).toBe(ShardId("ShardDO_0"));
    });

    test("split carves out a sub-range to a new shard", () => {
        const m = new VshardMap().split(0, 8191, ShardId("ShardDO_1"));
        expect(m.routeVshard(0 as Vshard)).toBe(ShardId("ShardDO_1"));
        expect(m.routeVshard(8192 as Vshard)).toBe(ShardId("ShardDO_0"));
        expect(m.ranges_().length).toBe(2);
    });

    test("split into the middle of an existing range produces three pieces", () => {
        const m = new VshardMap().split(4096, 8191, ShardId("ShardDO_1"));
        expect(m.ranges_().length).toBe(3);
        expect(m.routeVshard(0 as Vshard)).toBe(ShardId("ShardDO_0"));
        expect(m.routeVshard(4096 as Vshard)).toBe(ShardId("ShardDO_1"));
        expect(m.routeVshard(8192 as Vshard)).toBe(ShardId("ShardDO_0"));
    });

    test("nested splits are monotonic", () => {
        const m = new VshardMap().split(0, 8191, ShardId("ShardDO_1")).split(4096, 6143, ShardId("ShardDO_2"));
        expect(m.routeVshard(0 as Vshard)).toBe(ShardId("ShardDO_1"));
        expect(m.routeVshard(4096 as Vshard)).toBe(ShardId("ShardDO_2"));
        expect(m.routeVshard(6143 as Vshard)).toBe(ShardId("ShardDO_2"));
        expect(m.routeVshard(6144 as Vshard)).toBe(ShardId("ShardDO_1"));
        expect(m.routeVshard(8192 as Vshard)).toBe(ShardId("ShardDO_0"));
    });

    test("split that crosses range boundaries throws", () => {
        const m = new VshardMap().split(0, 8191, ShardId("ShardDO_1"));
        expect(() => m.split(4096, 12000, ShardId("ShardDO_2"))).toThrow();
    });
});
