import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { OP_LOG_DDL, SPLIT_LOG_ACCOUNTED_BYTES_SQL, SPLIT_LOG_DDL } from "../../src/oplog/schema.ts";
import type { SyncSql } from "../../src/oplog/wrapper.ts";
import { CdbVectorOutboxStore, initializeCdbVectorOutboxStore } from "../../src/server/do/cdb-vector-outbox-store.ts";
import {
    type CdbVectorSystemTailEntry,
    applyCdbVectorTailEntry,
    initializeCdbVectorReshardTailStore,
} from "../../src/server/do/cdb-vector-reshard-tail.ts";
import {
    beginExternalReshardCapture,
    endExternalReshardCapture,
    initializeExternalReshardCapture,
} from "../../src/server/external-reshard-capture.ts";
import {
    assertVectorReshardCaptureForeignKeys,
    renderVectorReshardTriggers,
} from "../../src/server/vector-reshard-triggers.ts";
import { vshardOf } from "../../src/vshard.ts";

interface TailRow {
    readonly source_tx_id: number;
    readonly op: "ins" | "upd" | "del";
    readonly table_name: string;
    readonly pk: string;
    readonly before: string | null;
    readonly after: string | null;
}

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

function initialize(db: Database): { readonly sql: SyncSql; readonly store: CdbVectorOutboxStore } {
    db.exec(OP_LOG_DDL);
    db.exec(SPLIT_LOG_DDL);
    const sql = syncSql(db);
    initializeExternalReshardCapture(sql);
    initializeCdbVectorOutboxStore(sql);
    return { sql, store: new CdbVectorOutboxStore(sql) };
}

describe("vector reshard capture triggers", () => {
    let db: Database;
    let sql: SyncSql;
    let store: CdbVectorOutboxStore;
    const organizationId = "org-vector-moving";
    const placement = Number(vshardOf([organizationId]));

    beforeEach(() => {
        db = new Database(":memory:");
        ({ sql, store } = initialize(db));
        db.run(
            `INSERT INTO _chardb_split_state (mig_id, range_lo, range_hi, role, capture, updated_at)
             VALUES ('vector-move', ?, ?, 'source', 1, 1)`,
            [placement, placement]
        );
    });

    afterEach(() => db.close());

    function install(): void {
        for (const statement of renderVectorReshardTriggers("vector-move").install) db.run(statement);
    }

    function stage(vectorId = "vec-moving", nowMs = 10): void {
        store.stageUpsert({
            vectorId,
            organizationId,
            resourceId: "message_embedding",
            rowPk: `row-${vectorId}`,
            dimensions: 2,
            values: [1, 2],
            metadata: { scope: "moving" },
            nowMs,
        });
    }

    function pendingMutation(targetPlacement = placement, mutId = "mutation-1"): void {
        db.run(
            `INSERT INTO _chardb_op_log
               (principal_id, mut_id, payload_hash, payload_enc, committed_at, schema_epoch,
                touched_keys, byte_size, placement_vshard)
             VALUES ('principal', ?, X'00', X'', 1, 1, '[]', 0, ?)`,
            [mutId, targetPlacement]
        );
    }

    function finishMutation(mutId = "mutation-1"): void {
        db.run("UPDATE _chardb_op_log SET payload_enc = X'01', byte_size = 1 WHERE mut_id = ?", [mutId]);
    }

    function tail(): TailRow[] {
        return db
            .query("SELECT source_tx_id, op, table_name, pk, before, after FROM _chardb_split_log ORDER BY lsn")
            .all() as TailRow[];
    }

    test("replays staged upserts and deletes when the source clock moves backwards", () => {
        install();
        for (const nowMs of [30, 20, 10]) {
            db.transaction(() => {
                const mutId = `clock-${nowMs}`;
                pendingMutation(placement, mutId);
                if (nowMs === 10) store.stageDelete({ vectorId: "vec-moving", organizationId, nowMs });
                else stage("vec-moving", nowMs);
                finishMutation(mutId);
            })();
        }

        const entries = db
            .query("SELECT lsn, table_name, op, pk, before, after FROM _chardb_split_log ORDER BY lsn")
            .all() as CdbVectorSystemTailEntry[];
        expect(
            entries
                .filter(entry => entry.table_name === "_chardb_vectors")
                .map(entry => JSON.parse(entry.after as string).updated_at)
        ).toEqual([30, 20, 10]);
        const destination = new Database(":memory:");
        try {
            const destinationSql = syncSql(destination);
            initializeCdbVectorOutboxStore(destinationSql);
            initializeCdbVectorReshardTailStore(destinationSql);
            for (const entry of entries) {
                destination.transaction(() => {
                    expect(
                        applyCdbVectorTailEntry(destinationSql, "vector-move", entry, {
                            lo: placement,
                            hi: placement,
                        })
                    ).toBe(true);
                })();
            }
            for (const table of ["_chardb_vectors", "_chardb_vector_outbox"]) {
                expect(destination.query(`SELECT * FROM ${table}`).all()).toEqual(
                    db.query(`SELECT * FROM ${table}`).all()
                );
            }
        } finally {
            destination.close();
        }
    });

    test("captures registered vector writes with exact scalar images and lowercase BLOB hex", () => {
        install();
        db.transaction(() => {
            pendingMutation();
            stage();
            db.run(
                `INSERT INTO _chardb_vector_attempts
                   (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                    response_ambiguous, delete_confirmed, delete_claim_token)
                 VALUES ('vec-moving', 1, 10, 20, 0, 0, 0, NULL)`
            );
            finishMutation();
        })();

        const rows = tail();
        expect(rows.map(row => [row.source_tx_id, row.table_name, row.op])).toEqual([
            [1, "_chardb_vectors", "ins"],
            [1, "_chardb_vector_outbox", "ins"],
            [1, "_chardb_vector_attempts", "ins"],
        ]);
        const head = JSON.parse(rows[0]?.after ?? "null") as Record<string, unknown>;
        expect(head).toMatchObject({
            vector_id: "vec-moving",
            organization_id: organizationId,
            placement_vshard: placement,
            dimensions: 2,
            values_hex: "0000803f00000040",
            metadata_json: '{"scope":"moving"}',
            state: "pending",
        });
        expect(head).not.toHaveProperty("values_enc");
        expect(rows[0]?.before).toBeNull();
        expect(rows[1]?.pk).toBe("vec-moving");
        expect(JSON.parse(rows[2]?.pk ?? "null")).toEqual(["vec-moving", 1]);
        expect(JSON.parse(rows[2]?.after ?? "null")).toMatchObject({
            vector_id: "vec-moving",
            physical_version: 1,
            response_ambiguous: 0,
        });
        const accounting = db
            .query(
                `SELECT state.split_log_rows, state.split_log_bytes,
                        (SELECT COUNT(*) FROM _chardb_split_log WHERE mig_id = 'vector-move') AS rows,
                        (SELECT COALESCE(SUM(${SPLIT_LOG_ACCOUNTED_BYTES_SQL}), 0)
                         FROM _chardb_split_log WHERE mig_id = 'vector-move') AS bytes
                 FROM _chardb_split_state AS state WHERE state.mig_id = 'vector-move'`
            )
            .get();
        expect(accounting).toEqual({
            split_log_rows: 3,
            split_log_bytes: expect.any(Number),
            rows: 3,
            bytes: expect.any(Number),
        });
        expect((accounting as { split_log_bytes: number }).split_log_bytes).toBe(
            (accounting as { bytes: number }).bytes
        );
    });

    test("captures exact before and after images for head, outbox, and attempt updates", () => {
        stage();
        db.run(
            `INSERT INTO _chardb_vector_attempts
               (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                response_ambiguous, delete_confirmed, delete_claim_token)
             VALUES ('vec-moving', 1, 10, 20, 0, 0, 0, NULL)`
        );
        install();
        db.transaction(() => {
            pendingMutation();
            store.stageUpsert({
                vectorId: "vec-moving",
                organizationId,
                resourceId: "message_embedding",
                rowPk: "row-vec-moving",
                dimensions: 2,
                values: [3, 4],
                metadata: { scope: "updated" },
                nowMs: 30,
            });
            db.run("UPDATE _chardb_vector_attempts SET response_ambiguous = 1 WHERE vector_id = 'vec-moving'");
            finishMutation();
        })();

        const rows = tail();
        expect(rows.map(row => [row.table_name, row.op])).toEqual([
            ["_chardb_vectors", "upd"],
            ["_chardb_vector_outbox", "upd"],
            ["_chardb_vector_attempts", "upd"],
            ["_chardb_vector_attempts", "upd"],
        ]);
        const headBefore = JSON.parse(rows[0]?.before ?? "null") as Record<string, unknown>;
        const headAfter = JSON.parse(rows[0]?.after ?? "null") as Record<string, unknown>;
        expect(headBefore).toMatchObject({ version: 1, values_hex: "0000803f00000040" });
        expect(headAfter).toMatchObject({
            version: 2,
            values_hex: "0000404000008040",
            metadata_json: '{"scope":"updated"}',
        });
        expect(JSON.parse(rows[1]?.before ?? "null")).toMatchObject({ target_version: 1 });
        expect(JSON.parse(rows[1]?.after ?? "null")).toMatchObject({ target_version: 2 });
        expect(JSON.parse(rows[2]?.before ?? "null")).toMatchObject({ response_ambiguous: 0 });
        expect(JSON.parse(rows[2]?.after ?? "null")).toMatchObject({ response_ambiguous: 0 });
        expect(JSON.parse(rows[3]?.before ?? "null")).toMatchObject({ response_ambiguous: 0 });
        expect(JSON.parse(rows[3]?.after ?? "null")).toMatchObject({ response_ambiguous: 1 });
    });

    test("uses one negative identity for an external vector transaction", () => {
        install();
        db.transaction(() => {
            const transactionId = beginExternalReshardCapture(sql, placement);
            stage("vec-external");
            endExternalReshardCapture(sql, transactionId);
        })();
        expect(tail().map(row => row.source_tx_id)).toEqual([-1, -1]);
    });

    test("captures cascading child deletes before the head in one replayable transaction", () => {
        stage("vec-cascade");
        db.run(
            `INSERT INTO _chardb_vector_attempts
               (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                response_ambiguous, delete_confirmed, delete_claim_token)
             VALUES ('vec-cascade', 1, 10, 20, 0, 1, 0, NULL)`
        );
        db.exec(`
            CREATE TABLE cascade_parent_visibility (child_table TEXT NOT NULL, head_present INTEGER NOT NULL);
            CREATE TRIGGER audit_outbox_cascade AFTER DELETE ON _chardb_vector_outbox BEGIN
              INSERT INTO cascade_parent_visibility
              VALUES ('outbox', EXISTS (SELECT 1 FROM _chardb_vectors WHERE vector_id = OLD.vector_id));
            END;
            CREATE TRIGGER audit_attempt_cascade AFTER DELETE ON _chardb_vector_attempts BEGIN
              INSERT INTO cascade_parent_visibility
              VALUES ('attempt', EXISTS (SELECT 1 FROM _chardb_vectors WHERE vector_id = OLD.vector_id));
            END;
        `);
        install();

        db.transaction(() => {
            const transactionId = beginExternalReshardCapture(sql, placement);
            db.run("DELETE FROM _chardb_vectors WHERE vector_id = 'vec-cascade'");
            endExternalReshardCapture(sql, transactionId);
        })();

        const rows = tail();
        expect(rows).toHaveLength(3);
        expect(rows.map(row => row.source_tx_id)).toEqual([-1, -1, -1]);
        expect(rows.map(row => row.table_name).sort()).toEqual([
            "_chardb_vector_attempts",
            "_chardb_vector_outbox",
            "_chardb_vectors",
        ]);
        expect(
            db.query("SELECT child_table, head_present FROM cascade_parent_visibility ORDER BY child_table").all()
        ).toEqual([
            { child_table: "attempt", head_present: 0 },
            { child_table: "outbox", head_present: 0 },
        ]);
        expect(rows.at(-1)?.table_name).toBe("_chardb_vectors");
        expect(rows.every(row => row.op === "del" && row.before !== null && row.after === null)).toBe(true);

        const mirror = new Database(":memory:");
        try {
            const initialized = initialize(mirror);
            initialized.store.stageUpsert({
                vectorId: "vec-cascade",
                organizationId,
                resourceId: "message_embedding",
                rowPk: "row-vec-cascade",
                dimensions: 2,
                values: [1, 2],
                metadata: { scope: "moving" },
                nowMs: 10,
            });
            mirror.run(
                `INSERT INTO _chardb_vector_attempts
                   (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                    response_ambiguous, delete_confirmed, delete_claim_token)
                 VALUES ('vec-cascade', 1, 10, 20, 0, 1, 0, NULL)`
            );
            for (const row of rows) {
                if (row.table_name === "_chardb_vector_attempts") {
                    const [vectorId, physicalVersion] = JSON.parse(row.pk) as [string, number];
                    mirror.run("DELETE FROM _chardb_vector_attempts WHERE vector_id = ? AND physical_version = ?", [
                        vectorId,
                        physicalVersion,
                    ]);
                } else {
                    mirror.run(`DELETE FROM ${row.table_name} WHERE vector_id = ?`, [row.pk]);
                }
            }
            expect(mirror.query("SELECT * FROM _chardb_vectors").all()).toEqual([]);
            expect(mirror.query("PRAGMA foreign_key_check").all()).toEqual([]);
        } finally {
            mirror.close();
        }
    });

    test("fails closed for missing and dual transaction identity", () => {
        install();
        expect(() => db.transaction(() => stage("vec-missing"))()).toThrow(/exactly one transaction identity/);

        expect(() =>
            db.transaction(() => {
                pendingMutation();
                db.run(
                    "UPDATE _chardb_split_capture_tx SET next_id = 1, active_id = -1, active_vshard = ? WHERE singleton = 1",
                    [placement]
                );
                stage("vec-dual");
            })()
        ).toThrow(/exactly one transaction identity/);

        expect(() =>
            db.transaction(() => {
                pendingMutation(placement, "mutation-a");
                pendingMutation(placement, "mutation-b");
                stage("vec-two-pending");
            })()
        ).toThrow(/exactly one transaction identity/);
        expect(db.query("SELECT * FROM _chardb_vectors").all()).toEqual([]);
    });

    test("fails closed on wrong placement and overlapping source ranges", () => {
        install();
        const wrongPlacement = (placement + 1) % 16_384;
        expect(wrongPlacement).not.toBe(placement);
        expect(() =>
            db.transaction(() => {
                const transactionId = beginExternalReshardCapture(sql, wrongPlacement);
                stage("vec-wrong-placement");
                endExternalReshardCapture(sql, transactionId);
            })()
        ).toThrow(/placement differs/);

        db.run(
            `INSERT INTO _chardb_split_state (mig_id, range_lo, range_hi, role, capture, updated_at)
             VALUES ('vector-overlap', ?, ?, 'source', 1, 1)`,
            [placement, placement]
        );
        expect(() =>
            db.transaction(() => {
                const transactionId = beginExternalReshardCapture(sql, placement);
                stage("vec-overlap");
                endExternalReshardCapture(sql, transactionId);
            })()
        ).toThrow(/overlapping active vector splits/);
        expect(db.query("SELECT * FROM _chardb_vectors").all()).toEqual([]);
    });

    test("rolls vector and split accounting back when tail capacity is exhausted", () => {
        install();
        db.run("UPDATE _chardb_split_state SET split_log_rows = 65536 WHERE mig_id = 'vector-move'");
        const capacityBefore = db.query("SELECT * FROM _chardb_vector_capacity").get();
        expect(() =>
            db.transaction(() => {
                const transactionId = beginExternalReshardCapture(sql, placement);
                stage("vec-capacity");
                endExternalReshardCapture(sql, transactionId);
            })()
        ).toThrow(/source split log capacity reached/);
        expect(db.query("SELECT * FROM _chardb_vector_capacity").get()).toEqual(capacityBefore);
        expect(db.query("SELECT * FROM _chardb_vectors").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chardb_vector_outbox").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chardb_split_log").all()).toEqual([]);
        expect(db.query("SELECT next_id, active_id, active_vshard FROM _chardb_split_capture_tx").get()).toEqual({
            next_id: 0,
            active_id: null,
            active_vshard: null,
        });
    });

    test("rejects head deletion when SQLite foreign-key cascades are disabled", () => {
        stage("vec-fk");
        install();
        db.run("PRAGMA foreign_keys = OFF");
        expect(() =>
            db.transaction(() => {
                const transactionId = beginExternalReshardCapture(sql, placement);
                db.run("DELETE FROM _chardb_vectors WHERE vector_id = 'vec-fk'");
                endExternalReshardCapture(sql, transactionId);
            })()
        ).toThrow(/vector capture requires foreign keys/);
        expect(db.query("SELECT vector_id FROM _chardb_vectors").all()).toEqual([{ vector_id: "vec-fk" }]);
        expect(db.query("SELECT vector_id FROM _chardb_vector_outbox").all()).toEqual([{ vector_id: "vec-fk" }]);
        expect(db.query("SELECT * FROM _chardb_split_log").all()).toEqual([]);
    });

    test("fails closed for orphan child transitions without any placement source", () => {
        install();
        db.run("PRAGMA foreign_keys = OFF");
        expect(() =>
            db.run(
                `INSERT INTO _chardb_vector_outbox
                   (vector_id, target_version, operation, phase, mutation_id, accepted_at, verify_ids_json,
                    attempts, next_attempt_at, leased_until, lease_token, last_error)
                 VALUES ('orphan-outbox', 1, 'upsert', 'submit', NULL, NULL, NULL, 0, 0, NULL, NULL, NULL)`
            )
        ).toThrow(/vector child capture placement is unavailable/);
        expect(() =>
            db.run(
                `INSERT INTO _chardb_vector_attempts
                   (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                    response_ambiguous, delete_confirmed, delete_claim_token)
                 VALUES ('orphan-attempt', 1, 0, 0, 0, 0, 0, NULL)`
            )
        ).toThrow(/vector child capture placement is unavailable/);
        expect(db.query("SELECT * FROM _chardb_vector_outbox").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chardb_vector_attempts").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chardb_split_log").all()).toEqual([]);
    });

    test("uses case-insensitive injective trigger names and validates cascade prerequisites", () => {
        const hyphen = renderVectorReshardTriggers("move-a");
        const underscore = renderVectorReshardTriggers("move_a");
        const upper = renderVectorReshardTriggers("Move");
        const lower = renderVectorReshardTriggers("move");
        expect(new Set(hyphen.names.map(name => name.toLowerCase()))).not.toEqual(
            new Set(underscore.names.map(name => name.toLowerCase()))
        );
        expect(new Set(upper.names.map(name => name.toLowerCase()))).not.toEqual(
            new Set(lower.names.map(name => name.toLowerCase()))
        );
        expect(hyphen.names.every(name => name.includes("_m_"))).toBe(true);
        const collisionNames = [...hyphen.names, ...underscore.names, ...upper.names, ...lower.names];
        for (const triggers of [hyphen, underscore, upper, lower]) {
            for (const statement of triggers.install) db.run(statement);
        }
        const installed = new Set(
            (db.query("SELECT name FROM sqlite_master WHERE type = 'trigger'").all() as Array<{ name: string }>).map(
                row => row.name.toLowerCase()
            )
        );
        expect(collisionNames.every(name => installed.has(name.toLowerCase()))).toBe(true);
        expect(new Set(collisionNames.map(name => name.toLowerCase()))).toHaveLength(36);
        expect(() => assertVectorReshardCaptureForeignKeys(sql)).not.toThrow();
        const extraRelationSql: SyncSql = {
            ...sql,
            all<T>(query: string, ...params: never[]): T[] {
                const rows = sql.all<T>(query, ...params);
                return query.includes("pragma_foreign_key_list") ? [...rows, rows[0] as T] : rows;
            },
        };
        expect(() => assertVectorReshardCaptureForeignKeys(extraRelationSql)).toThrow(
            /vector outbox capture foreign key differs/
        );
        db.run("PRAGMA foreign_keys = OFF");
        expect(() => assertVectorReshardCaptureForeignKeys(sql)).toThrow(/requires foreign keys/);
    });
});
