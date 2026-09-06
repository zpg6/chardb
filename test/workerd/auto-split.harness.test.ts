import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { Miniflare } from "miniflare";
import { disposeMiniflareBounded } from "../../scripts/miniflare-lifecycle.mjs";
import { RESHARDER_PHASE } from "../../src/server/do/resharder.ts";
import { VSHARD_COUNT, vshardOf } from "../../src/vshard.ts";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ENTRY = path.join(HERE, "auto-split.entry.ts");
const ORGANIZATIONS = Array.from({ length: 48 }, (_, index) => `org-${index}`);

let mf: Miniflare | undefined;
let temporaryPath = "";

interface Headroom {
    readonly alarmAt: number | null;
    readonly shards: readonly { shard_id: string; bytes: number; judged_bytes: number; mig_id: string | null }[];
    readonly migrations: readonly {
        mig_id: string;
        src_shard: string;
        dst_shard: string;
        range_lo: number;
        range_hi: number;
        phase: number;
    }[];
}

interface Topology {
    readonly ranges: readonly { lo: number; hi: number; shardId: string }[];
    readonly activeOperation: { migrationId: string } | null;
}

beforeAll(async () => {
    temporaryPath = await mkdtemp(path.join(tmpdir(), "chardb-auto-split-"));
    const bundle = path.join(temporaryPath, "worker.mjs");
    const child = Bun.spawn(
        [
            "bun",
            "build",
            ENTRY,
            "--target=browser",
            "--format=esm",
            "--external=cloudflare:workers",
            "--outfile",
            bundle,
        ],
        { stdout: "pipe", stderr: "pipe" }
    );
    if ((await child.exited) !== 0) throw new Error(await new Response(child.stderr).text());
    const script = (await Bun.file(bundle).text())
        .replace(
            "await import(this.#props.path.join(this.#props.migrationFolder, fileName))",
            'await Promise.reject(new Error("file migrations are unavailable in workerd"))'
        )
        .replace(
            "await import(nodeSqlite)",
            'await Promise.reject(new Error("node:sqlite is unavailable in workerd"))'
        );
    if (script.includes("import(")) throw new Error("fixture bundle contains a dynamic import");
    mf = new Miniflare({
        name: "auto-split",
        modules: true,
        script,
        durableObjects: {
            CDB_CATALOG: { className: "Catalog", useSQLite: true },
            CDB_SHARD: { className: "Cdb", useSQLite: true },
            CDB_RESHARD: { className: "Resharder", useSQLite: true },
        },
        durableObjectsPersist: path.join(temporaryPath, "durable-objects"),
        compatibilityDate: "2026-08-06",
        compatibilityFlags: ["nodejs_compat"],
    });
    await mf.ready;
});

afterAll(async () => {
    await disposeMiniflareBounded(mf, { label: "auto-split teardown" });
    mf = undefined;
    if (temporaryPath) await rm(temporaryPath, { recursive: true, force: true });
});

async function call<T>(operation: string, body: Record<string, unknown> = {}): Promise<T> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch(`http://example.com/${operation}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
    });
    const result = (await response.json()) as T;
    if (!response.ok) throw new Error(`${operation} returned ${response.status}: ${JSON.stringify(result)}`);
    return result;
}

async function settled(timeoutMs: number): Promise<Headroom> {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        const headroom = await call<Headroom>("headroom");
        const busy = headroom.alarmAt !== null || headroom.shards.some(shard => shard.mig_id !== null);
        if (!busy && headroom.migrations.length > 0) return headroom;
        if (Date.now() > deadline) throw new Error(`governor did not settle: ${JSON.stringify(headroom)}`);
        await new Promise(resolve => setTimeout(resolve, 200));
    }
}

test("a shard past the mark splits itself into a fresh ShardDO once its own alarm fires", async () => {
    await call("setup");
    const { bytes, mark } = await call<{ bytes: number; mark: number }>("seed", {
        organizationIds: ORGANIZATIONS,
        rows: 30,
        bytes: 1_024,
    });
    expect(bytes).toBeGreaterThan(mark);
    expect(await call<Topology>("topology")).toMatchObject({
        ranges: [{ lo: 0, hi: VSHARD_COUNT - 1, shardId: "ShardDO_0" }],
    });

    await call("alarm", { shardId: "ShardDO_0" });
    const headroom = await settled(60_000);
    expect(headroom.migrations).toHaveLength(1);
    const move = headroom.migrations[0];
    if (!move) throw new Error("no migration");
    expect(move.mig_id).toMatch(/^auto-ShardDO_0-\d+$/);
    expect(move).toMatchObject({
        src_shard: "ShardDO_0",
        dst_shard: "ShardDO_1",
        range_lo: 0,
        phase: RESHARDER_PHASE.SOURCE_DRAINED,
    });
    expect(headroom.shards).toEqual([{ shard_id: "ShardDO_0", bytes, judged_bytes: bytes, mig_id: null }]);
    expect(headroom.alarmAt).toBeNull();

    const topology = await call<Topology>("topology");
    expect(topology.activeOperation).toBeNull();
    expect(topology.ranges).toEqual([
        { lo: 0, hi: move.range_hi, shardId: "ShardDO_1" },
        { lo: move.range_hi + 1, hi: VSHARD_COUNT - 1, shardId: "ShardDO_0" },
    ]);

    const moved = ORGANIZATIONS.filter(id => Number(vshardOf([id])) <= move.range_hi).sort();
    const kept = ORGANIZATIONS.filter(id => Number(vshardOf([id])) > move.range_hi).sort();
    expect(moved.length).toBeGreaterThan(0);
    expect(kept.length).toBeGreaterThan(0);
    const rows = (shardId: string) => call<{ organizationId: string; rows: number }[]>("rows", { shardId });
    expect(await rows("ShardDO_1")).toEqual(moved.map(organizationId => ({ organizationId, rows: 30 })));
    expect(await rows("ShardDO_0")).toEqual(kept.map(organizationId => ({ organizationId, rows: 30 })));

    // The source is still past the mark, but it was judged at this size: nothing arms until it grows.
    await call("alarm", { shardId: "ShardDO_0" });
    const after = await call<Headroom>("headroom");
    expect(after.alarmAt).toBeNull();
    expect(after.shards).toEqual(headroom.shards);
    expect(after.migrations).toHaveLength(1);
}, 60_000);
