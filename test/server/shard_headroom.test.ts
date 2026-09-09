import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, setSystemTime, spyOn, test } from "bun:test";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { CdbError } from "../../src/errors.ts";
import { rowVshard } from "../../src/reshard/range.ts";
import type { TableSpec } from "../../src/reshard/triggers.ts";
import {
    CDB_HEADROOM_PAGE_ROWS,
    nextShardId,
    readHeadroomPage,
    suggestSplit,
} from "../../src/server/do/cdb-headroom.ts";
import { configureCdbRuntime } from "../../src/server/do/cdb.ts";
import { RecoveryCoordinatorStore } from "../../src/server/do/recovery-coordinator.ts";
import { RESHARDER_PHASE, Resharder } from "../../src/server/do/resharder.ts";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";
import { emptyManifest } from "../../src/server/manifest.ts";
import { serializeRecoveryContinuationState } from "../../src/server/recovery-continuation.ts";
import { ShardId } from "../../src/types.ts";
import { VSHARD_COUNT, type VshardRange, vshardOf } from "../../src/vshard.ts";
import { forOrg, globalScope } from "../helpers/cdb-table.ts";

function sqlStorage(db: Database) {
    return {
        exec(query: string, ...bindings: unknown[]) {
            const statement = db.prepare(query);
            const rows = statement.all(...(bindings as never[])) as Record<string, unknown>[];
            const columnNames = [...statement.columnNames];
            const rawRows = rows.map(row => columnNames.map(column => row[column]));
            return {
                columnNames,
                raw: () => rawRows.values(),
                *[Symbol.iterator]() {
                    yield* rows;
                },
            };
        },
        get databaseSize(): number {
            const page = db.prepare("PRAGMA page_size").get() as { page_size: number };
            const count = db.prepare("PRAGMA page_count").get() as { page_count: number };
            return page.page_size * count.page_count;
        },
    };
}

function durableState(db: Database, id: string) {
    let alarmAt: number | null = null;
    let bootstrap: Promise<unknown> = Promise.resolve();
    const state = {
        id: { toString: () => id },
        storage: {
            sql: sqlStorage(db),
            transactionSync: <T>(callback: () => T): T => db.transaction(callback)(),
            getAlarm: async () => alarmAt,
            setAlarm: async (at: number) => {
                alarmAt = at;
            },
        },
        blockConcurrencyWhile: (callback: () => Promise<unknown>): void => {
            bootstrap = callback();
        },
    } as unknown as DurableObjectState;
    const clearAlarm = () => {
        alarmAt = null;
    };
    return { state, ready: () => bootstrap, alarm: () => alarmAt, clearAlarm };
}

/** Fake namespaces stringify ids to the name, so `idFromName(x).toString()` round-trips like workerd. */
function namespace(get: (name: string) => object): DurableObjectNamespace {
    return {
        idFromName: (name: string) => ({ name, toString: () => name }),
        get: (id: { name: string }) => get(id.name),
    } as unknown as DurableObjectNamespace;
}

const SPEC: TableSpec = { name: "messages", partitionColumn: "organization_id", columns: ["id", "organization_id"] };

function range(lo: number, hi: number, shardId = "ShardDO_0"): VshardRange {
    return { lo, hi, shardId: ShardId(shardId) };
}

function histogram(entries: Readonly<Record<number, number>>): Uint32Array {
    const vshards = new Uint32Array(VSHARD_COUNT);
    for (const [vshard, rows] of Object.entries(entries)) vshards[Number(vshard)] = rows;
    return vshards;
}

const FULL = [range(0, VSHARD_COUNT - 1)];

describe("readHeadroomPage", () => {
    test("pages by rowid, hashes each distinct partition value once, and reports when the table is exhausted", () => {
        const db = new Database(":memory:");
        db.exec("CREATE TABLE messages (id INTEGER PRIMARY KEY, organization_id TEXT)");
        const insert = db.prepare("INSERT INTO messages (organization_id) VALUES (?)");
        const owners = ["org-a", "org-b", null];
        for (let index = 0; index < CDB_HEADROOM_PAGE_ROWS + 3; index++) insert.run(owners[index % 3] ?? null);
        const sql = sqlStorage(db);
        const first = readHeadroomPage(sql, SPEC, 0);
        expect(first.nextRowid).toBe(CDB_HEADROOM_PAGE_ROWS);
        const second = readHeadroomPage(sql, SPEC, CDB_HEADROOM_PAGE_ROWS);
        expect(second.nextRowid).toBeNull();
        const totals = new Map<number, number>();
        for (const [vshard, rows] of [...first.vshards, ...second.vshards]) {
            totals.set(vshard, (totals.get(vshard) ?? 0) + rows);
        }
        expect(totals.get(rowVshard("org-a"))).toBe(1_367);
        expect(totals.get(rowVshard("org-b"))).toBe(1_366);
        expect(totals.get(rowVshard(null))).toBe(1_366);
        expect(readHeadroomPage(sql, SPEC, CDB_HEADROOM_PAGE_ROWS + 3)).toEqual({ nextRowid: null, vshards: [] });
        expect(() => readHeadroomPage(sql, SPEC, -1)).toThrow(TypeError);
        expect(() => readHeadroomPage(sql, { ...SPEC, name: "messages; DROP TABLE x" }, 0)).toThrow(TypeError);
    });
});

describe("suggestSplit", () => {
    test("cuts the heaviest range nearest half its rows and hands trailing empty vshards to the prefix", () => {
        expect(suggestSplit(FULL, histogram({ 10: 5, 20: 5, 30: 5, 40: 5 }))).toEqual({ lo: 0, hi: 29, rows: 10 });
        expect(suggestSplit(FULL, histogram({ 5: 4, 6: 4, 7: 2 }))).toEqual({ lo: 0, hi: 5, rows: 4 });
        expect(suggestSplit(FULL, histogram({ 5: 4, 6: 6 }))).toEqual({ lo: 0, hi: 5, rows: 4 });
        const ranges = [range(0, 8191), range(8192, 16383)];
        expect(suggestSplit(ranges, histogram({ 1: 50, 2: 50, 9000: 3, 9001: 7 }))).toEqual({ lo: 0, hi: 1, rows: 50 });
        expect(suggestSplit(FULL, histogram({ 0: 90, 100: 10 }))).toEqual({ lo: 0, hi: 99, rows: 90 });
    });

    test("refuses a move that carries under an eighth of the rows or cannot separate anything", () => {
        expect(suggestSplit(FULL, histogram({ 5: 10, 16383: 90 }))).toBeNull();
        expect(suggestSplit(FULL, histogram({ 0: 1, 5: 1_000 }))).toBeNull();
        expect(suggestSplit(FULL, histogram({ 7: 100 }))).toBeNull();
        expect(suggestSplit(FULL, histogram({}))).toBeNull();
        expect(suggestSplit([range(7, 7)], histogram({ 7: 100 }))).toBeNull();
    });
});

describe("nextShardId", () => {
    test("continues the ShardDO sequence and refuses foreign names", () => {
        expect(nextShardId(["ShardDO_0"])).toBe("ShardDO_1");
        expect(nextShardId(["ShardDO_0", "ShardDO_3", "ShardDO_0"])).toBe("ShardDO_4");
        expect(nextShardId(["ShardDO_0", "eu-1"])).toBeNull();
        expect(nextShardId([])).toBeNull();
    });
});

describe("Cdb headroom", () => {
    const organization = sqliteTable("organization", { id: text("id").primaryKey() });
    const { cdbTable } = forOrg();
    const messages = cdbTable(
        "messages",
        {
            id: text("id").primaryKey(),
            organizationId: text("organization_id")
                .notNull()
                .references(() => organization.id),
        },
        { roles: { member: { read: "*" } } }
    );
    const ConfiguredCdb = configureCdbRuntime({
        schema: () => ({ organization, messages }),
        manifest: () => emptyManifest(),
    });
    class LowMarkCdb extends ConfiguredCdb {
        protected override autoSplitBytes(): number {
            return 1_000;
        }
    }
    const reports: { cdbId: string; bytes: number }[] = [];
    let sinkFails = false;
    const resharder = namespace(() => ({
        adminRecoveryAdmissionClock: async () => ({ generation: 0, activeOperationId: null, activeDigest: null }),
        reportShardSize: async (args: { cdbId: string; bytes: number }) => {
            if (sinkFails) throw new Error("resharder unavailable");
            reports.push(args);
        },
    }));

    beforeEach(() => {
        reports.splice(0);
        sinkFails = false;
    });

    test("reports its durable id once per growth step past the mark and retries only on the next alarm", async () => {
        const db = new Database(":memory:");
        const { state, ready } = durableState(db, "cdb-0");
        const cdb = new LowMarkCdb(state, { CDB_RESHARD: resharder });
        await ready();
        sinkFails = true;
        await cdb.alarm();
        sinkFails = false;
        await cdb.alarm();
        await cdb.alarm();
        const first = sqlStorage(db).databaseSize;
        expect(reports).toEqual([{ cdbId: "cdb-0", bytes: first }]);
        db.exec("CREATE TABLE filler (body TEXT)");
        for (let index = 0; index < 64; index++) db.run("INSERT INTO filler (body) VALUES (?)", ["x".repeat(512)]);
        await cdb.alarm();
        expect(reports).toHaveLength(2);
        expect(reports[1]?.bytes).toBeGreaterThanOrEqual(first + 1_000 / 8);
    });

    test("stays silent below the default mark", async () => {
        const db = new Database(":memory:");
        const { state, ready } = durableState(db, "cdb-0");
        const cdb = new ConfiguredCdb(state, { CDB_RESHARD: resharder });
        await ready();
        await cdb.alarm();
        expect(reports).toEqual([]);
    });

    test("serves histogram pages only for packaged movable tables", async () => {
        const db = new Database(":memory:");
        const { state, ready } = durableState(db, "cdb-0");
        const cdb = new ConfiguredCdb(state, { CDB_RESHARD: resharder });
        await ready();
        db.exec("CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL)");
        for (let index = 0; index < 3; index++) {
            db.run("INSERT INTO messages (id, organization_id) VALUES (?, ?)", [`m-${index}`, "org-1"]);
        }
        expect(cdb.readHeadroomPage({ table: "messages", afterRowid: 0 })).toEqual({
            nextRowid: null,
            vshards: [[Number(vshardOf(["org-1"])), 3]],
        });
        expect(() => cdb.readHeadroomPage({ table: "organization", afterRowid: 0 })).toThrow("CDB_INVALID_ARGS");
    });
});

describe("Resharder headroom governor", () => {
    const MARK = 1_000;
    const QUARTER = MARK / 4;
    const { cdbTable } = globalScope();
    const messages = cdbTable(
        "messages",
        { id: text("id").primaryKey(), organizationId: text("organization_id").notNull() },
        { partitionBy: "organizationId" }
    );
    const schema = { messages };

    class FakeCatalog {
        ranges: VshardRange[] = [range(0, VSHARD_COUNT - 1)];
        activeOperation: Record<string, unknown> | null = null;
        schemaActive = true;
        readonly claims: Record<string, unknown>[] = [];
        readonly aborts: Record<string, unknown>[] = [];
        releaseError: Error | null = null;
        async topology() {
            return { ranges: this.ranges, activeOperation: this.activeOperation, schemaActive: this.schemaActive };
        }
        async beginDerivedTopologyOperation(args: {
            migId: string;
            destinationShard: string;
            rangeLo: number;
            rangeHi: number;
        }) {
            this.claims.push(args);
            return {
                status: "active" as const,
                migrationId: args.migId,
                sourceShard: "ShardDO_0",
                destinationShard: args.destinationShard,
                rangeLo: args.rangeLo,
                rangeHi: args.rangeHi,
                startEpoch: 7,
            };
        }
        async abortTopologyOperation(args: Record<string, unknown>) {
            if (this.releaseError) throw this.releaseError;
            this.aborts.push(args);
            this.activeOperation = null;
        }
    }

    class GovernedResharder extends Resharder {
        readonly starts: Record<string, unknown>[] = [];
        readonly runs: string[] = [];
        readonly aborted: string[] = [];
        nextPhase: number | null = RESHARDER_PHASE.SOURCE_DRAINED;
        runError: Error | null = null;
        protected override runtimeSchema() {
            return schema;
        }
        protected override autoSplitBytes() {
            return MARK;
        }
        override async startSplit(args: {
            migId: string;
            srcShard: string;
            dstShard: string;
            rangeLo: number;
            rangeHi: number;
            epochAtStart: number;
            tables: readonly TableSpec[];
        }): Promise<void> {
            this.starts.push(args);
            this.ctx.storage.sql.exec(
                `INSERT INTO migration_state (mig_id, src_shard, dst_shard, range_lo, range_hi, phase, epoch_at_start, started_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, 0, ?, 1, 1)`,
                args.migId,
                args.srcShard,
                args.dstShard,
                args.rangeLo,
                args.rangeHi,
                args.epochAtStart
            );
            this.ctx.storage.sql.exec(
                "INSERT INTO migration_schema_identity (mig_id, schema_version, schema_epoch, schema_digest) VALUES (?, 1, 1, ?)",
                args.migId,
                "a".repeat(64)
            );
        }
        override async runSplit(migId: string) {
            this.runs.push(migId);
            if (this.runError) throw this.runError;
            if (this.nextPhase === null) return { phase: RESHARDER_PHASE.INIT };
            this.ctx.storage.sql.exec("UPDATE migration_state SET phase = ? WHERE mig_id = ?", this.nextPhase, migId);
            return { phase: this.nextPhase as never };
        }
        override async abort(migId: string): Promise<void> {
            this.aborted.push(migId);
        }
        headroom() {
            return adaptSqlStorage(this.ctx.storage.sql).all(
                "SELECT shard_id, bytes, judged_bytes, mig_id FROM headroom_shards ORDER BY shard_id"
            );
        }
    }

    let db: Database;
    let resharder: GovernedResharder;
    let catalog: FakeCatalog;
    let alarm: () => number | null;
    let clearAlarm: () => void;
    let rows: [rowid: number, owner: string][];
    let pageReads: number;
    let warnings: ReturnType<typeof spyOn>;
    let clock: number;

    beforeEach(async () => {
        db = new Database(":memory:");
        const durable = durableState(db, "resharder");
        catalog = new FakeCatalog();
        rows = [];
        pageReads = 0;
        const shard = {
            async readHeadroomPage(args: { table: string; afterRowid: number }) {
                pageReads++;
                expect(args.table).toBe("messages");
                const page = rows.filter(([rowid]) => rowid > args.afterRowid).slice(0, 3);
                const counts = new Map<number, number>();
                for (const [, owner] of page) counts.set(rowVshard(owner), (counts.get(rowVshard(owner)) ?? 0) + 1);
                return { nextRowid: page.length === 3 ? page[2]?.[0] : null, vshards: [...counts] };
            },
        };
        resharder = new GovernedResharder(durable.state, {
            CDB_CATALOG: namespace(() => catalog),
            CDB_SHARD: namespace(() => shard),
        });
        await durable.ready();
        alarm = durable.alarm;
        clearAlarm = durable.clearAlarm;
        warnings = spyOn(console, "warn").mockImplementation(() => {});
        spyOn(console, "info").mockImplementation(() => {});
        clock = 1_800_000_000_000;
    });

    afterEach(() => {
        setSystemTime();
        warnings.mockRestore();
        db.close();
    });

    function seed(owners: Readonly<Record<string, number>>): void {
        let rowid = rows.at(-1)?.[0] ?? 0;
        for (const [owner, count] of Object.entries(owners)) {
            for (let index = 0; index < count; index++) rows.push([++rowid, owner]);
        }
    }

    function report(bytes: number): Promise<void> {
        return resharder.reportShardSize({ cdbId: "ShardDO_0", bytes });
    }

    /** Each tick runs one second later on a frozen clock, so delays compare exactly and ids never collide. */
    async function tick(): Promise<number | null> {
        clearAlarm();
        clock += 1_000;
        setSystemTime(clock);
        await resharder.alarm();
        const at = alarm();
        return at === null ? null : at - clock;
    }

    test("arms the governor only once a sample from a known shard reaches the mark", async () => {
        await report(MARK - 1);
        expect(alarm()).toBeNull();
        await resharder.reportShardSize({ cdbId: "unknown", bytes: MARK });
        expect(resharder.headroom()).toHaveLength(1);
        await report(MARK);
        expect(alarm()).not.toBeNull();
        await expect(resharder.reportShardSize({ cdbId: "", bytes: MARK })).rejects.toMatchObject({
            code: "CDB_INVALID_ARGS",
        });
        expect(resharder.headroom()).toEqual([{ shard_id: "ShardDO_0", bytes: MARK, judged_bytes: 0, mig_id: null }]);
    });

    test("scans across ticks, claims the cut for the next unused ShardDO, drives it to the end, and waits for growth", async () => {
        seed({ "org-a": 120, "org-b": 80 });
        const a = rowVshard("org-a");
        const b = rowVshard("org-b");
        const expected = suggestSplit(catalog.ranges, histogram({ [a]: 120, [b]: 80 }));
        if (!expected) throw new Error("fixture rows must be splittable");
        await report(MARK + 5);

        expect(await tick()).toBe(0);
        expect(pageReads).toBe(64);
        expect(catalog.claims).toEqual([]);
        expect(await tick()).toBe(0);
        expect(pageReads).toBe(67);
        expect(catalog.claims).toHaveLength(1);
        expect(catalog.claims[0]).toMatchObject({ destinationShard: "ShardDO_1", rangeLo: 0, rangeHi: expected.hi });
        const migId = String(catalog.claims[0]?.migId);
        expect(migId).toMatch(/^auto-ShardDO_0-\d+$/);
        expect(resharder.starts[0]).toMatchObject({
            migId,
            srcShard: "ShardDO_0",
            dstShard: "ShardDO_1",
            rangeLo: 0,
            rangeHi: expected.hi,
            epochAtStart: 7,
            tables: [{ name: "messages", partitionColumn: "organization_id", columns: ["id", "organization_id"] }],
        });
        expect(resharder.headroom()).toEqual([
            { shard_id: "ShardDO_0", bytes: MARK + 5, judged_bytes: MARK + 5, mig_id: migId },
        ]);

        expect(await tick()).toBe(0);
        expect(resharder.runs).toEqual([migId]);
        expect(resharder.headroom()[0]).toMatchObject({ judged_bytes: MARK + 5, mig_id: null });
        expect(await tick()).toBeNull();

        await report(MARK + 5 + QUARTER - 1);
        expect(await tick()).toBeNull();
        expect(catalog.claims).toHaveLength(1);

        catalog.ranges = [range(0, expected.hi, "ShardDO_1"), range(expected.hi + 1, VSHARD_COUNT - 1)];
        const kept = (prefix: string): string => {
            for (let index = 0; ; index++)
                if (rowVshard(`${prefix}-${index}`) > expected.hi) return `${prefix}-${index}`;
        };
        seed({ [kept("org-c")]: 30, [kept("org-d")]: 30, [kept("org-e")]: 30 });
        db.run(
            "INSERT INTO migration_state (mig_id, src_shard, dst_shard, range_lo, range_hi, phase, epoch_at_start, started_at, updated_at) VALUES ('op-1', 'ShardDO_1', 'ShardDO_5', 0, 1, -1, 3, 1, 1)"
        );
        await report(MARK + 5 + QUARTER);
        await tick();
        await tick();
        expect(catalog.claims).toHaveLength(2);
        expect(catalog.claims[1]).toMatchObject({ destinationShard: "ShardDO_6", rangeLo: expected.hi + 1 });
    });

    test("waits a minute behind a recovery, a schema migration, or another movement", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        await report(MARK);
        new RecoveryCoordinatorStore(adaptSqlStorage(sqlStorage(db))).claimPreparation(
            "00000000-0000-4000-8000-000000000001",
            "a".repeat(64),
            serializeRecoveryContinuationState({
                kind: "restore",
                phase: "arm",
                shardIndex: 0,
                afterRetainedFileId: "",
                afterVectorId: "",
                afterPhysicalVersion: 0,
                files: 0,
                filePages: 0,
                filesRetained: 0,
                retentionPages: 0,
                quiescenceTurns: 0,
                vectors: 0,
                vectorPages: 0,
                commitPolls: 0,
            })
        );
        expect(await tick()).toBe(60_000);
        expect(pageReads).toBe(0);
        db.run("UPDATE _chardb_recovery_clock SET active_operation_id = NULL");

        catalog.schemaActive = false;
        expect(await tick()).toBe(60_000);
        catalog.schemaActive = true;

        catalog.activeOperation = {
            migrationId: "split-1",
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_9",
            rangeLo: 0,
            rangeHi: 1,
            startEpoch: 3,
        };
        expect(await tick()).toBe(60_000);
        expect(pageReads).toBe(0);
        expect(catalog.claims).toEqual([]);
    });

    test("judges a shard it cannot split and leaves it until the file grows a quarter of the mark", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        catalog.ranges = [range(0, 8191, "eu-1"), range(8192, VSHARD_COUNT - 1)];
        await report(MARK);
        expect(await tick()).toBeNull();
        expect(catalog.claims).toEqual([]);
        expect(resharder.headroom()[0]).toMatchObject({ judged_bytes: MARK, mig_id: null });
        expect(warnings).toHaveBeenCalledTimes(1);
        expect(String(warnings.mock.calls[0]?.[0])).toContain("ShardDO_<n>");
        await report(MARK + QUARTER - 1);
        expect(alarm()).not.toBeNull();
        expect(await tick()).toBeNull();
        expect(pageReads).toBe(2);
    });

    test("releases a lease it claimed but never started, even when the release fails", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        await report(MARK);
        catalog.activeOperation = {
            migrationId: "auto-ShardDO_0-1",
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_1",
            rangeLo: 0,
            rangeHi: 1,
            startEpoch: 3,
        };
        expect(await tick()).toBe(0);
        expect(catalog.aborts[0]).toMatchObject({ migId: "auto-ShardDO_0-1", startEpoch: 3, recoveryGeneration: 0 });

        await tick();
        const migId = String(catalog.claims[0]?.migId);
        db.run("DELETE FROM migration_state WHERE mig_id = ?", [migId]);
        catalog.activeOperation = { ...catalog.activeOperation, migrationId: migId };
        catalog.releaseError = new Error("CDB_STALE_EPOCH: generation moved");
        expect(await tick()).toBe(0);
        expect(resharder.headroom()[0]).toMatchObject({ mig_id: null, judged_bytes: MARK });
        expect(resharder.aborted).toEqual([]);
        expect(warnings).toHaveBeenCalledTimes(2);
    });

    test("records an aborted move and tries again only after a quarter of the mark of growth", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        resharder.nextPhase = RESHARDER_PHASE.ABORTED;
        await report(MARK);
        await tick();
        await tick();
        expect(resharder.headroom()[0]).toMatchObject({ judged_bytes: MARK, mig_id: null });
        expect(warnings).toHaveBeenCalledTimes(1);
        await report(MARK + QUARTER - 1);
        expect(await tick()).toBeNull();
        expect(catalog.claims).toHaveLength(1);
        await report(MARK + QUARTER);
        await tick();
        expect(catalog.claims).toHaveLength(2);
    });

    test("waits on rate limits, stale epochs, and stalled steps without aborting", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        await report(MARK);
        await tick();
        resharder.runError = new CdbError({ code: "CDB_RATE_LIMITED", message: "split log full" });
        expect(await tick()).toBe(5_000);
        resharder.runError = new Error("CDB_STALE_EPOCH: schema migration blocks topology");
        for (let attempt = 0; attempt < 12; attempt++) expect(await tick()).toBe(60_000);
        resharder.runError = null;
        resharder.nextPhase = null;
        expect(await tick()).toBe(0);
        db.run("UPDATE migration_state SET phase = ?", [RESHARDER_PHASE.TAIL_CAUGHT_UP]);
        expect(await tick()).toBe(15_000);
        expect(resharder.aborted).toEqual([]);
        expect(resharder.headroom()[0]?.mig_id).not.toBeNull();
    });

    test("backs off other errors for as long as they last and aborts the move after thirty minutes", async () => {
        seed({ "org-a": 2, "org-b": 2 });
        await report(MARK);
        await tick();
        const migId = String(catalog.claims[0]?.migId);
        resharder.runError = new Error("destination unreachable");
        expect(await tick()).toBe(1_000);
        const start = clock;
        clock = start + 19_000;
        expect(await tick()).toBe(20_000);
        clock = start + 10 * 60_000 - 1_000;
        expect(await tick()).toBe(60_000);
        expect(resharder.aborted).toEqual([]);
        clock = start + 30 * 60_000 - 1_000;
        expect(await tick()).toBe(60_000);
        expect(resharder.aborted).toEqual([migId]);
        expect(warnings).toHaveBeenCalledTimes(1);
    });
});
