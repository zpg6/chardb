import { Database } from "bun:sqlite";
/**
 * End-to-end integration test for the resharding pipeline.
 *
 * Drives a workload through a "source" bun:sqlite database with the trigger
 * set installed, then replays the captured `_chardb_split_log` rows into a
 * "destination" bun:sqlite database via the production row applier, filtered by the
 * migration's vshard range. Asserts:
 *   - rows in-range survive INSERT → UPDATE → DELETE → INSERT cycles with
 *     destination state matching the source for every in-range key,
 *   - rows out-of-range never appear in the destination,
 *   - replaying the same log a second time is idempotent (no row drift),
 *   - multi-table workloads share `_chardb_split_log` correctly with no
 *     cross-table contamination.
 *
 */
import { beforeEach, describe, expect, test } from "bun:test";
import type { SyncSql } from "../../src/oplog/wrapper.ts";
import { inRange, rowVshard } from "../../src/reshard/range.ts";
import { type TableSpec, renderTableTriggers } from "../../src/reshard/triggers.ts";
import { applyReshardRow } from "../../src/server/do/cdb-reshard-relational.ts";

const SPLIT_LOG_DDL = `
CREATE TABLE _chardb_split_log (
  lsn INTEGER PRIMARY KEY AUTOINCREMENT,
  source_tx_id INTEGER NOT NULL,
  mig_id TEXT NOT NULL,
  op TEXT NOT NULL,
  table_name TEXT NOT NULL,
  pk TEXT NOT NULL,
  before TEXT,
  after TEXT,
  ts INTEGER NOT NULL
)`;

const SPLIT_STATE_DDL = `
CREATE TABLE _chardb_split_state (
  mig_id TEXT PRIMARY KEY, range_lo INTEGER NOT NULL, range_hi INTEGER NOT NULL,
  role TEXT NOT NULL, capture INTEGER NOT NULL,
  split_log_rows INTEGER NOT NULL DEFAULT 0, split_log_bytes INTEGER NOT NULL DEFAULT 0,
  capture_tx_id INTEGER, capture_tx_rows INTEGER NOT NULL DEFAULT 0,
  capture_tx_bytes INTEGER NOT NULL DEFAULT 0
)`;

const messagesSpec: TableSpec = {
    name: "messages",
    partitionColumn: "org_id",
    columns: ["id", "org_id", "channel_id", "body", "created_at"],
};

const channelsSpec: TableSpec = {
    name: "channels",
    partitionColumn: "org_id",
    columns: ["id", "org_id", "name"],
};

const MIG_ID = "mig_007";

interface TailRow {
    lsn: number;
    op: "ins" | "upd" | "del";
    table_name: string;
    pk: string;
    before: string | null;
    after: string | null;
}

function makeSource() {
    const db = new Database(":memory:");
    db.run(SPLIT_LOG_DDL);
    db.run(SPLIT_STATE_DDL);
    db.run(`CREATE TABLE _chardb_op_log (
      event_id INTEGER PRIMARY KEY AUTOINCREMENT, payload_enc BLOB NOT NULL,
      byte_size INTEGER NOT NULL, placement_vshard INTEGER
    )`);
    db.run("INSERT INTO _chardb_split_state VALUES (?, 0, 16383, 'source', 1, 0, 0, NULL, 0, 0)", [MIG_ID]);
    db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, org_id TEXT, channel_id TEXT, body TEXT, created_at INTEGER)");
    db.run("CREATE TABLE channels (id TEXT PRIMARY KEY, org_id TEXT, name TEXT)");
    for (const stmt of renderTableTriggers(MIG_ID, messagesSpec).install) db.run(stmt);
    for (const stmt of renderTableTriggers(MIG_ID, channelsSpec).install) db.run(stmt);
    return db;
}

function sourceWrite(db: Database, statement: string, params: readonly unknown[] = []): void {
    db.transaction(() => {
        db.run("INSERT INTO _chardb_op_log (payload_enc, byte_size, placement_vshard) VALUES (X'', 0, 0)");
        const tx = (db.query("SELECT last_insert_rowid() AS id").get() as { id: number }).id;
        db.run(statement, params as never[]);
        db.run("UPDATE _chardb_op_log SET payload_enc = X'01', byte_size = 1 WHERE event_id = ?", [tx]);
    })();
}

function makeDest() {
    const db = new Database(":memory:");
    db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, org_id TEXT, channel_id TEXT, body TEXT, created_at INTEGER)");
    db.run("CREATE TABLE channels (id TEXT PRIMARY KEY, org_id TEXT, name TEXT)");
    return db;
}

function spec(name: string): TableSpec {
    return name === "messages" ? messagesSpec : channelsSpec;
}

function syncSql(db: Database): SyncSql {
    return {
        exec(statement, ...params) {
            db.run(statement, params as never[]);
        },
        one<T>(statement: string, ...params: never[]): T | null {
            return (db.query(statement).get(...params) as T | null) ?? null;
        },
        all<T>(statement: string, ...params: never[]): T[] {
            return db.query(statement).all(...params) as T[];
        },
        changes() {
            return Number((db.query("SELECT changes() AS n").get() as { n: number }).n);
        },
    };
}

function applyTail(dest: Database, src: Database, range: { lo: number; hi: number }) {
    const rows = src
        .prepare("SELECT lsn, op, table_name, pk, before, after FROM _chardb_split_log ORDER BY lsn")
        .all() as TailRow[];
    for (const r of rows) {
        if (!inRange(r.pk, range)) continue;
        const t = spec(r.table_name);
        if (r.op === "del") {
            dest.run(`DELETE FROM "${r.table_name}" WHERE "${t.partitionColumn}" = ? AND id = ?`, [
                r.pk,
                (JSON.parse(r.before ?? "{}") as { id?: string }).id ?? "",
            ]);
            continue;
        }
        const after = JSON.parse(r.after ?? "{}") as Record<string, unknown>;
        applyReshardRow(syncSql(dest), t, after);
    }
}

function dump(db: Database, table: string): unknown[] {
    return db.prepare(`SELECT * FROM "${table}" ORDER BY id`).all();
}

describe("reshard pipeline — multi-table integration", () => {
    let src: Database;
    let dest: Database;
    // pick a range that contains org "org-A" but not "org-Z"
    const orgA = "org-A";
    const orgZ = "org-zzzz";

    beforeEach(() => {
        src = makeSource();
        dest = makeDest();
    });

    test("INSERT → UPDATE → DELETE → INSERT cycles converge on destination for in-range rows", () => {
        sourceWrite(src, "INSERT INTO channels VALUES (?, ?, ?)", ["ch-1", orgA, "general"]);
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["m-1", orgA, "ch-1", "first", 1]);
        sourceWrite(src, "UPDATE messages SET body = ? WHERE id = ?", ["edited", "m-1"]);
        sourceWrite(src, "DELETE FROM messages WHERE id = ?", ["m-1"]);
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["m-1", orgA, "ch-1", "reborn", 2]);

        const aV = rowVshard(orgA);
        applyTail(dest, src, { lo: aV, hi: aV });

        expect(dump(dest, "channels")).toEqual([{ id: "ch-1", org_id: orgA, name: "general" }]);
        expect(dump(dest, "messages")).toEqual([
            { id: "m-1", org_id: orgA, channel_id: "ch-1", body: "reborn", created_at: 2 },
        ]);
    });

    test("out-of-range rows are filtered before apply and never reach the destination", () => {
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["m-A", orgA, "ch-1", "in", 1]);
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["m-Z", orgZ, "ch-9", "out", 1]);
        sourceWrite(src, "INSERT INTO channels VALUES (?, ?, ?)", ["ch-1", orgA, "in"]);
        sourceWrite(src, "INSERT INTO channels VALUES (?, ?, ?)", ["ch-9", orgZ, "out"]);

        const aV = rowVshard(orgA);
        applyTail(dest, src, { lo: aV, hi: aV });

        const msgRows = dump(dest, "messages") as { id: string }[];
        expect(msgRows.map(r => r.id)).toEqual(["m-A"]);
        const chanRows = dump(dest, "channels") as { id: string }[];
        expect(chanRows.map(r => r.id)).toEqual(["ch-1"]);
    });

    test("partition-key moves remove stale destination rows and admit rows that move into range", () => {
        const aV = rowVshard(orgA);
        let outside = orgZ;
        while (rowVshard(outside) === aV) outside += "z";

        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["move-out", orgA, "ch-1", "before", 1]);
        applyTail(dest, src, { lo: aV, hi: aV });
        expect((dump(dest, "messages") as { id: string }[]).map(row => row.id)).toEqual(["move-out"]);

        src.run("DELETE FROM _chardb_split_log");
        sourceWrite(src, "UPDATE messages SET org_id = ?, body = ? WHERE id = ?", [outside, "outside", "move-out"]);
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["move-in", outside, "ch-2", "before", 2]);
        src.run("DELETE FROM _chardb_split_log WHERE op = 'ins'");
        sourceWrite(src, "UPDATE messages SET org_id = ?, body = ? WHERE id = ?", [orgA, "inside", "move-in"]);

        applyTail(dest, src, { lo: aV, hi: aV });
        expect(dump(dest, "messages")).toEqual([
            { id: "move-in", org_id: orgA, channel_id: "ch-2", body: "inside", created_at: 2 },
        ]);
    });

    test("re-running the tail replay is idempotent — no row drift", () => {
        sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", ["m-1", orgA, "ch-1", "v1", 1]);
        sourceWrite(src, "UPDATE messages SET body = ? WHERE id = ?", ["v2", "m-1"]);

        const aV = rowVshard(orgA);
        applyTail(dest, src, { lo: aV, hi: aV });
        const first = dump(dest, "messages");
        applyTail(dest, src, { lo: aV, hi: aV });
        const second = dump(dest, "messages");

        expect(second).toEqual(first);
    });

    test("multi-table workload — channels and messages share the log without contamination", () => {
        for (let i = 0; i < 20; i++) {
            sourceWrite(src, "INSERT INTO channels VALUES (?, ?, ?)", [`ch-${i}`, orgA, `c${i}`]);
            sourceWrite(src, "INSERT INTO messages VALUES (?, ?, ?, ?, ?)", [
                `m-${i}`,
                orgA,
                `ch-${i}`,
                `body-${i}`,
                i,
            ]);
        }

        const aV = rowVshard(orgA);
        applyTail(dest, src, { lo: aV, hi: aV });

        expect((dump(dest, "channels") as unknown[]).length).toBe(20);
        expect((dump(dest, "messages") as unknown[]).length).toBe(20);

        // Tail rows in the log are partitioned cleanly by table_name with no
        // cross-table mix-ups.
        const tail = src
            .prepare("SELECT table_name, COUNT(*) AS n FROM _chardb_split_log GROUP BY table_name")
            .all() as { table_name: string; n: number }[];
        expect(new Set(tail.map(t => t.table_name))).toEqual(new Set(["channels", "messages"]));
        for (const t of tail) expect(t.n).toBe(20);
    });
});
