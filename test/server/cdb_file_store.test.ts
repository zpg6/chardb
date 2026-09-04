import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type { SyncSql } from "../../src/oplog/wrapper.ts";
import {
    CdbFileStore,
    backfillFilePlacements,
    initializeFileStore,
    validateFilePlacementsPage,
} from "../../src/server/do/cdb-file-store.ts";
import { renderFileAttachmentTriggers } from "../../src/server/file-triggers.ts";
import { vshardOf } from "../../src/vshard.ts";

function syncSql(db: Database): SyncSql {
    return {
        exec(query, ...params) {
            db.run(query, params as never[]);
        },
        one<T>(query: string, ...params: never[]): T | null {
            return (db.query(query).get(...params) as T | null) ?? null;
        },
        all<T>(query: string, ...params: never[]): T[] {
            return db.query(query).all(...params) as T[];
        },
        changes() {
            return Number((db.query("SELECT changes() AS count").get() as { count: number }).count);
        },
    };
}

const HASH_A = "a".repeat(64);
const HASH_B = "b".repeat(64);

describe("Cdb file lifecycle store", () => {
    let db: Database;
    let store: CdbFileStore;

    beforeEach(() => {
        db = new Database(":memory:");
        const sql = syncSql(db);
        initializeFileStore(sql);
        store = new CdbFileStore(sql, { organizationQuotaBytes: 10, maxPendingPerOrganization: 2 });
    });

    afterEach(() => db.close());

    function reserve(fileId: string, size = 4, nowMs = 100) {
        return store.reserve({
            fileId,
            organizationId: "org-1",
            table: "messages",
            column: "attachment",
            contentType: "image/png",
            size,
            nowMs,
        });
    }

    test("reserves idempotently, finalizes exact bytes, and attaches once", () => {
        const pending = reserve("file_a");
        expect(pending).toMatchObject({
            status: "pending",
            objectKey: "v1/org-1/file_a",
            size: 4,
            sha256: null,
        });
        expect(reserve("file_a")).toEqual(pending);
        expect(() => reserve("file_a", 5)).toThrow(/different upload/);

        const ready = store.markReady("file_a", HASH_A, 4, 110);
        expect(ready).toMatchObject({ status: "ready", sha256: HASH_A });
        expect(store.markReady("file_a", HASH_A, 4, 120)).toEqual(ready);
        expect(() => store.markReady("file_a", HASH_B, 4, 120)).toThrow(/stored hash/);
        expect(() => store.markReady("file_a", HASH_A, 3, 120)).toThrow(/size/);

        const attached = store.attach("file_a", "org-1", "messages", "attachment", "row-1", 130);
        expect(attached).toMatchObject({ status: "attached", rowId: "row-1" });
        expect(store.attach("file_a", "org-1", "messages", "attachment", "row-1", 140)).toEqual(attached);
        expect(() => store.attach("file_a", "org-1", "messages", "attachment", "row-2", 140)).toThrow(
            /cannot be reused/
        );
    });

    test("never regresses lifecycle clocks when a caller or maintenance clock moves backward", () => {
        reserve("file_a", 4, 100);
        expect(store.markReady("file_a", HASH_A, 4, 90).updatedAt).toBe(100);
        expect(store.attach("file_a", "org-1", "messages", "attachment", "row-1", 80).updatedAt).toBe(100);
        expect(store.queueDelete("file_a", 70).updatedAt).toBe(100);

        reserve("file_pending", 1, 200);
        expect(store.maintenanceCandidates(200).map(file => String(file.fileId))).toEqual(["file_pending"]);
        store.queueDelete("file_pending", 150);
        expect(store.read("file_pending")).toMatchObject({ status: "deleting", updatedAt: 200 });

        reserve("file_ready", 1, 300);
        store.markReady("file_ready", HASH_A, 1, 310);
        expect(store.maintenanceCandidates(310).map(file => String(file.fileId))).toEqual(["file_ready"]);
        store.queueDelete("file_ready", 250);
        expect(store.read("file_ready")).toMatchObject({ status: "deleting", updatedAt: 310 });
    });

    test("pages only recoverable live files in stable file-id order", () => {
        reserve("file_b", 4);
        store.markReady("file_b", HASH_B, 4, 110);
        store.attach("file_b", "org-1", "messages", "attachment", "row-b", 120);
        reserve("file_a", 4);
        store.markReady("file_a", HASH_A, 4, 110);
        reserve("file_pending", 1);
        reserve("file_deleting", 1);
        store.markReady("file_deleting", HASH_A, 1, 110);
        store.attach("file_deleting", "org-1", "messages", "attachment", "row-deleting", 120);
        store.queueDelete("file_deleting", 130);

        const first = store.recoveryPage("", 1);
        expect(first.files.map(file => String(file.fileId))).toEqual(["file_a"]);
        expect(first).toMatchObject({ afterFileId: "file_a", done: false });
        const second = store.recoveryPage(first.afterFileId, 1);
        expect(second.files.map(file => String(file.fileId))).toEqual(["file_b"]);
        expect(second).toMatchObject({ afterFileId: "file_b", done: true });
        expect(store.retentionPage("", 10).files.map(file => String(file.fileId))).toEqual([
            "file_a",
            "file_b",
            "file_deleting",
        ]);
    });

    test("keeps quota charged through deletion and releases it only after R2 completion", () => {
        reserve("file_a", 6);
        reserve("file_b", 4);
        expect(() => reserve("file_c", 1)).toThrow(expect.objectContaining({ code: "CDB_RATE_LIMITED" }));
        store.queueDelete("file_a", 200);
        expect(() => reserve("file_c", 1)).toThrow(expect.objectContaining({ code: "CDB_RATE_LIMITED" }));
        store.completeDelete("file_a");
        expect(reserve("file_c", 1).status).toBe("pending");
    });

    test("bounds concurrent pending files and expires abandoned reservations", () => {
        reserve("file_a", 1, 100);
        reserve("file_b", 1, 200);
        expect(() => reserve("file_c", 1, 300)).toThrow(expect.objectContaining({ code: "CDB_RATE_LIMITED" }));
        expect(store.maintenanceCandidates(150).map(file => String(file.fileId))).toEqual(["file_a"]);
        store.queueDelete("file_a", 400);
        expect(store.dueDeletes()).toEqual([expect.objectContaining({ fileId: "file_a", status: "deleting" })]);
        expect(reserve("file_c", 1, 500).status).toBe("pending");
    });

    test("permanently fences a deleted organization while retaining the pending upload lease", () => {
        reserve("file_pending", 1, 100);
        expect(reserve("file_pending", 1, 150).updatedAt).toBe(150);
        reserve("file_ready", 1, 101);
        store.markReady("file_ready", HASH_A, 1, 102);

        expect(store.fenceOrganizationDeletion("org-1", 200)).toBe(2);
        expect(store.isOrganizationDeleted("org-1")).toBe(true);
        expect(store.read("file_pending")?.status).toBe("pending");
        expect(store.read("file_ready")?.status).toBe("deleting");
        expect(() => reserve("file_late", 1, 201)).toThrow(expect.objectContaining({ code: "CDB_FORBIDDEN" }));
        expect(() => store.markReady("file_pending", HASH_A, 1, 201)).toThrow(
            expect.objectContaining({ code: "CDB_FORBIDDEN" })
        );

        expect(store.maintenanceCandidates(149)).toEqual([]);
        expect(store.maintenanceCandidates(150).map(file => String(file.fileId))).toEqual(["file_pending"]);
        store.queueDelete("file_pending", 301);
        expect(store.read("file_pending")?.status).toBe("deleting");
    });

    test("batches tombstoned files without touching another organization", () => {
        store = new CdbFileStore(store.sql, { organizationQuotaBytes: 100, maxPendingPerOrganization: 64 });
        for (let index = 0; index < 40; index++) {
            reserve(`file_${String(index).padStart(2, "0")}`, 1, index + 1);
            store.markReady(`file_${String(index).padStart(2, "0")}`, HASH_A, 1, index + 101);
        }
        store.reserve({
            fileId: "other_file",
            organizationId: "org-2",
            table: "messages",
            column: "attachment",
            contentType: "image/png",
            size: 1,
            nowMs: 200,
        });
        store.markReady("other_file", HASH_B, 1, 201);

        expect(store.fenceOrganizationDeletion("org-1", 300)).toBe(40);
        expect(store.dueDeletes()).toHaveLength(32);
        expect(store.hasTombstonedMaterializedFiles()).toBe(true);
        expect(store.read("other_file")?.status).toBe("ready");
        const candidates = store.maintenanceCandidates(0);
        expect(candidates).toHaveLength(8);
        for (const file of candidates) store.queueDelete(file.fileId, 301);
        expect(store.hasTombstonedMaterializedFiles()).toBe(false);
        expect(store.read("other_file")?.status).toBe("ready");
    });

    test("generated triggers attach, replace, and delete in the owning row transaction", () => {
        db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, attachment TEXT)");
        for (const statement of renderFileAttachmentTriggers({
            kind: "file",
            version: 1,
            table: "messages",
            column: "attachment",
            primaryKey: "id",
            organizationColumn: "organization_id",
            maxSize: 10,
            contentTypes: ["image/png"],
        })) {
            db.run(statement);
        }

        reserve("file_old", 4);
        store.markReady("file_old", HASH_A, 4, 101);
        const beforeInsert = db.query("SELECT total_changes() AS count").get() as { count: number };
        db.run("INSERT INTO messages (id, organization_id, attachment) VALUES (?, ?, ?)", [
            "row-1",
            "org-1",
            "file_old",
        ]);
        const afterInsert = db.query("SELECT total_changes() AS count").get() as { count: number };
        expect(afterInsert.count - beforeInsert.count).toBe(2);
        expect(store.read("file_old")).toMatchObject({ status: "attached", rowId: "row-1" });
        expect(() =>
            db.run("INSERT INTO messages (id, organization_id, attachment) VALUES (?, ?, ?)", [
                "row-2",
                "org-1",
                "file_old",
            ])
        ).toThrow(/CDB_FILE_INVALID_ATTACHMENT/);

        reserve("file_pending", 1, 102);
        expect(() =>
            db.run("INSERT INTO messages (id, organization_id, attachment) VALUES (?, ?, ?)", [
                "row-pending",
                "org-1",
                "file_pending",
            ])
        ).toThrow(/CDB_FILE_INVALID_ATTACHMENT/);

        expect(() => db.run("UPDATE messages SET attachment = 'file_pending' WHERE id = 'row-1'")).toThrow(
            /CDB_FILE_INVALID_ATTACHMENT/
        );
        expect(db.query("SELECT attachment FROM messages WHERE id = 'row-1'").get()).toEqual({
            attachment: "file_old",
        });
        expect(store.read("file_old")?.status).toBe("attached");
        expect(store.read("file_pending")?.status).toBe("pending");

        reserve("file_new", 5, 103);
        store.markReady("file_new", HASH_B, 5, 104);
        const beforeReplace = db.query("SELECT total_changes() AS count").get() as { count: number };
        db.run("UPDATE messages SET attachment = ? WHERE id = ?", ["file_new", "row-1"]);
        const afterReplace = db.query("SELECT total_changes() AS count").get() as { count: number };
        expect(afterReplace.count - beforeReplace.count).toBe(3);
        expect(store.read("file_old")?.status).toBe("deleting");
        expect(store.read("file_new")).toMatchObject({ status: "attached", rowId: "row-1" });

        const beforeDelete = db.query("SELECT total_changes() AS count").get() as { count: number };
        db.run("DELETE FROM messages WHERE id = ?", ["row-1"]);
        const afterDelete = db.query("SELECT total_changes() AS count").get() as { count: number };
        expect(afterDelete.count - beforeDelete.count).toBe(2);
        expect(store.read("file_new")?.status).toBe("deleting");
    });

    test.each([
        ["id", "row-moved"],
        ["organization_id", "org-2"],
    ])("rejects changing %s while a file is attached", (column, value) => {
        db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, attachment TEXT)");
        for (const statement of renderFileAttachmentTriggers({
            kind: "file",
            version: 1,
            table: "messages",
            column: "attachment",
            primaryKey: "id",
            organizationColumn: "organization_id",
            maxSize: 10,
            contentTypes: "*",
        }))
            db.run(statement);
        reserve("file_a");
        store.markReady("file_a", HASH_A, 4, 101);
        db.run("INSERT INTO messages VALUES ('row-1', 'org-1', 'file_a')");

        for (const assignment of ["", ", attachment = NULL"]) {
            expect(() => db.run(`UPDATE messages SET ${column} = ?${assignment}`, [value])).toThrow(
                /CDB_FILE_INVALID_ATTACHMENT/
            );
            expect(db.query("SELECT * FROM messages").get()).toEqual({
                id: "row-1",
                organization_id: "org-1",
                attachment: "file_a",
            });
            expect(store.read("file_a")).toMatchObject({ status: "attached", rowId: "row-1" });
        }
        db.run("UPDATE messages SET id = id, organization_id = organization_id");
        db.run("UPDATE messages SET attachment = NULL");
        db.run(`UPDATE messages SET ${column} = ?`, [value]);
        expect(store.read("file_a")?.status).toBe("deleting");
    });

    test.each([
        ["empty", ""],
        ["ASCII overflow", "x".repeat(257)],
        ["UTF-8 overflow", "é".repeat(129)],
    ])("rejects an attachment whose row ID cannot be downloaded: %s", (_label, rowId) => {
        db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, attachment TEXT)");
        for (const statement of renderFileAttachmentTriggers({
            kind: "file",
            version: 1,
            table: "messages",
            column: "attachment",
            primaryKey: "id",
            organizationColumn: "organization_id",
            maxSize: 10,
            contentTypes: "*",
        }))
            db.run(statement);
        reserve("file_a");
        store.markReady("file_a", HASH_A, 4, 101);
        expect(() => db.run("INSERT INTO messages VALUES (?, 'org-1', 'file_a')", [rowId])).toThrow(
            /CDB_FILE_INVALID_ATTACHMENT/
        );
        expect(db.query("SELECT * FROM messages").all()).toEqual([]);
        db.run("INSERT INTO messages VALUES (?, 'org-1', NULL)", [rowId]);
        expect(() => db.run("UPDATE messages SET attachment = 'file_a'")).toThrow(/CDB_FILE_INVALID_ATTACHMENT/);
        expect(store.read("file_a")?.status).toBe("ready");
        db.run("DELETE FROM messages");
        db.run("INSERT INTO messages VALUES (?, 'org-1', 'file_a')", ["é".repeat(128)]);
        expect(store.read("file_a")).toMatchObject({ status: "attached", rowId: "é".repeat(128) });
    });

    test("generated triggers reject a synthetic ready file after the organization fence", () => {
        db.run("CREATE TABLE messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, attachment TEXT)");
        for (const statement of renderFileAttachmentTriggers({
            kind: "file",
            version: 1,
            table: "messages",
            column: "attachment",
            primaryKey: "id",
            organizationColumn: "organization_id",
            maxSize: 10,
            contentTypes: ["image/png"],
        })) {
            db.run(statement);
        }
        store.fenceOrganizationDeletion("org-1", 100);
        db.run(
            `INSERT INTO _chardb_files
              (file_id, organization_id, table_name, column_name, object_key, content_type, size, sha256,
               status, row_id, created_at, updated_at, placement_vshard)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready', NULL, ?, ?, ?)`,
            [
                "synthetic",
                "org-1",
                "messages",
                "attachment",
                "v1/org-1/synthetic",
                "image/png",
                1,
                HASH_A,
                101,
                101,
                Number(vshardOf(["org-1"])),
            ]
        );
        expect(() =>
            db.run("INSERT INTO messages (id, organization_id, attachment) VALUES (?, ?, ?)", [
                "row-late",
                "org-1",
                "synthetic",
            ])
        ).toThrow(/CDB_FILE_INVALID_ATTACHMENT/);
        expect(store.read("synthetic")?.status).toBe("ready");
    });

    test("backfills legacy placement in bounded idempotent pages and rejects drift", () => {
        db.close();
        db = new Database(":memory:");
        db.run(`
            CREATE TABLE _chardb_files (
              file_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, table_name TEXT NOT NULL,
              column_name TEXT NOT NULL, object_key TEXT NOT NULL UNIQUE, content_type TEXT NOT NULL,
              size INTEGER NOT NULL, sha256 TEXT, status TEXT NOT NULL, row_id TEXT,
              created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE TABLE _chardb_deleted_organizations (
              organization_id TEXT PRIMARY KEY, deleted_at INTEGER NOT NULL
            );
        `);
        db.run(
            `INSERT INTO _chardb_files VALUES
             ('legacy-a', 'org-a', 'messages', 'attachment', 'v1/org-a/legacy-a', 'image/png', 1, NULL, 'pending', NULL, 1, 1),
             ('legacy-b', 'org-b', 'messages', 'attachment', 'v1/org-b/legacy-b', 'image/png', 1, NULL, 'pending', NULL, 1, 1)`
        );
        db.run("INSERT INTO _chardb_deleted_organizations VALUES ('org-c', 1)");
        const sql = syncSql(db);
        initializeFileStore(sql);

        expect(backfillFilePlacements(sql, 2)).toEqual({ files: 2, tombstones: 0, done: false });
        expect(backfillFilePlacements(sql, 2)).toEqual({ files: 0, tombstones: 1, done: true });
        expect(backfillFilePlacements(sql, 2)).toEqual({ files: 0, tombstones: 0, done: true });
        expect(validateFilePlacementsPage(sql, { afterKind: "file", afterId: "", limit: 2 })).toEqual({
            kind: "file",
            afterId: "legacy-b",
            done: false,
        });
        expect(
            validateFilePlacementsPage(sql, {
                afterKind: "organization_tombstone",
                afterId: "",
                limit: 2,
            }).done
        ).toBe(true);

        db.run("UPDATE _chardb_files SET placement_vshard = (placement_vshard + 1) % 16384 WHERE file_id = 'legacy-a'");
        expect(() => new CdbFileStore(sql).read("legacy-a")).toThrow(/invalid virtual-shard placement/);
    });
});
