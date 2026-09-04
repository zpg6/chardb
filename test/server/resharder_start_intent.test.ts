import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { RESHARDER_PHASE, Resharder } from "../../src/server/do/resharder.ts";
import { serializeRecoveryContinuationState } from "../../src/server/recovery-continuation.ts";
import { VSHARD_COUNT } from "../../src/vshard.ts";

interface Cursor<T> extends Iterable<T> {
    readonly columnNames: string[];
    raw(): IterableIterator<unknown[]>;
}

function sqlStorage(db: Database) {
    return {
        exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): Cursor<T> {
            const statement = db.prepare(query);
            const rows = statement.all(...(bindings as never[])) as Record<string, unknown>[];
            const columnNames = [...statement.columnNames];
            const rawRows = rows.map(row => columnNames.map(column => row[column]));
            return {
                columnNames,
                raw: () => rawRows.values(),
                *[Symbol.iterator]() {
                    yield* rows as T[];
                },
            };
        },
    };
}

const table = {
    name: "messages",
    partitionColumn: "organization_id",
    columns: ["id", "organization_id", "body"],
} as const;

const split = {
    migId: "start-intent-1",
    srcShard: "source-a",
    dstShard: "destination-b",
    rangeLo: 100,
    rangeHi: 199,
    epochAtStart: 7,
    tables: [table],
} as const;

const topologySchema = { schemaVersion: 3, schemaEpoch: 4, schemaDigest: "a".repeat(64) } as const;

describe("Resharder durable start intent", () => {
    let db: Database;
    let ready: Promise<unknown>;
    let durableState: DurableObjectState;

    beforeEach(() => {
        db = new Database(":memory:");
        ready = Promise.resolve();
        durableState = {
            id: { toString: () => "resharder-start-intent" },
            storage: {
                sql: sqlStorage(db),
                transactionSync: <T>(callback: () => T): T => db.transaction(callback)(),
            },
            blockConcurrencyWhile: (callback: () => Promise<unknown>): void => {
                ready = callback();
            },
        } as unknown as DurableObjectState;
    });

    afterEach(() => db.close());

    async function construct(catalog: object, shards?: object): Promise<Resharder> {
        const catalogNamespace = {
            idFromName: (name: string) => ({ name }),
            idFromString: (name: string) => ({ name }),
            get: () => catalog,
        } as unknown as DurableObjectNamespace;
        const shardNamespace = shards
            ? ({
                  idFromName: (name: string) => ({ name }),
                  idFromString: (name: string) => ({ name }),
                  get: (id: { name: string }) => (shards as Record<string, unknown>)[id.name],
              } as unknown as DurableObjectNamespace)
            : undefined;
        const resharder = new Resharder(durableState, {
            CDB_CATALOG: catalogNamespace,
            ...(shardNamespace ? { CDB_SHARD: shardNamespace } : {}),
        });
        await ready;
        return resharder;
    }

    test("a concurrent abort wins the Catalog begin await window", async () => {
        let releaseBegin!: () => void;
        let markBeginStarted!: () => void;
        const beginGate = new Promise<void>(resolve => {
            releaseBegin = resolve;
        });
        const beginStarted = new Promise<void>(resolve => {
            markBeginStarted = resolve;
        });
        let topologyStatus: "active" | "aborted" = "active";
        let abortCalls = 0;
        const catalog = {
            async beginTopologyOperation() {
                markBeginStarted();
                await beginGate;
                return { status: topologyStatus, ...topologySchema };
            },
            async topologyOperation() {
                return {
                    migrationId: split.migId,
                    sourceShard: split.srcShard,
                    destinationShard: split.dstShard,
                    rangeLo: split.rangeLo,
                    rangeHi: split.rangeHi,
                    startEpoch: split.epochAtStart,
                    status: topologyStatus,
                };
            },
            async abortTopologyOperation() {
                abortCalls++;
                topologyStatus = "aborted";
                return { status: "aborted" as const };
            },
        };
        const resharder = await construct(catalog);

        const start = resharder.startSplit(split);
        await beginStarted;
        const abort = resharder.abort(split.migId);
        releaseBegin();

        await expect(start).rejects.toMatchObject({ code: "CDB_STALE_EPOCH" });
        await expect(abort).resolves.toBeUndefined();
        expect(abortCalls).toBe(1);
        await expect(resharder.getPhase(split.migId)).resolves.toBeNull();
        expect(
            db.query("SELECT state, src_shard, dst_shard FROM migration_start_intent WHERE mig_id = ?").get(split.migId)
        ).toEqual({ state: "abort_requested", src_shard: split.srcShard, dst_shard: split.dstShard });
    });

    test("an active recovery claim rejects resharding before Catalog mutation", async () => {
        let topologyBegins = 0;
        const resharder = await construct({
            async beginTopologyOperation() {
                topologyBegins++;
                return { status: "active" as const, ...topologySchema };
            },
        });
        await resharder.adminClaimRecoveryPreparation({
            operationId: "00000000-0000-4000-8000-000000000001",
            digest: "d".repeat(64),
            continuationJson: serializeRecoveryContinuationState({
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
            }),
        });
        await expect(resharder.startSplit(split)).rejects.toMatchObject({ code: "CDB_RESHARD_PHASE_MISMATCH" });
        expect(topologyBegins).toBe(0);
    });

    test("an unknown abort permanently cancels the id before Catalog acquisition", async () => {
        let topologyBegins = 0;
        const resharder = await construct({
            async beginTopologyOperation() {
                topologyBegins++;
                return { status: "active" as const, ...topologySchema };
            },
        });

        await expect(resharder.abort(split.migId)).resolves.toBeUndefined();
        await expect(resharder.abort(split.migId)).resolves.toBeUndefined();
        await expect(resharder.startSplit(split)).rejects.toMatchObject({ code: "CDB_STALE_EPOCH" });
        expect(topologyBegins).toBe(0);
        expect(
            db.query("SELECT state, src_shard FROM migration_start_intent WHERE mig_id = ?").get(split.migId)
        ).toEqual({ state: "abort_requested", src_shard: null });
    });

    test("permanent cancellation history is bounded by the vshard count", async () => {
        const resharder = await construct({});
        const insert = db.prepare(
            `INSERT INTO migration_start_intent (mig_id, state, created_at, updated_at)
             VALUES (?, 'abort_requested', 0, 0)`
        );
        db.transaction(() => {
            for (let index = 0; index < VSHARD_COUNT; index++) insert.run(`bounded-${index}`);
        })();

        await expect(resharder.abort("bounded-overflow")).rejects.toMatchObject({ code: "CDB_RATE_LIMITED" });
        expect(db.query("SELECT COUNT(*) AS count FROM migration_start_intent").get()).toEqual({
            count: VSHARD_COUNT,
        });
    });

    test.each([false, true])(
        "preserves legacy starts and cancellations with prior generation upgrade=%p",
        async alreadyUpgraded => {
            db.exec(`CREATE TABLE migration_start_intent (
            mig_id TEXT PRIMARY KEY, state TEXT NOT NULL,
            src_shard TEXT, dst_shard TEXT, range_lo INTEGER, range_hi INTEGER,
            epoch_at_start INTEGER, tables_json TEXT,
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        )`);
            db.query(`INSERT INTO migration_start_intent
            (mig_id, state, created_at, updated_at) VALUES (?, 'abort_requested', 0, 0)`).run("legacy-canceled");
            db.query(`INSERT INTO migration_start_intent VALUES (?, 'starting', ?, ?, ?, ?, ?, ?, 0, 0)`).run(
                split.migId,
                split.srcShard,
                split.dstShard,
                split.rangeLo,
                split.rangeHi,
                split.epochAtStart,
                JSON.stringify([{ columns: table.columns, name: table.name, partitionColumn: table.partitionColumn }])
            );
            if (alreadyUpgraded) {
                db.exec("ALTER TABLE migration_start_intent ADD COLUMN recovery_generation INTEGER DEFAULT 0");
            }
            const generations: number[] = [];
            let resharder = await construct({
                async beginTopologyOperation(args: { recoveryGeneration: number }) {
                    generations.push(args.recoveryGeneration);
                    return { status: "active" as const, ...topologySchema };
                },
            });

            await expect(resharder.abort("legacy-canceled")).resolves.toBeUndefined();
            await expect(resharder.startSplit({ ...split, migId: "legacy-canceled" })).rejects.toMatchObject({
                code: "CDB_STALE_EPOCH",
            });
            await expect(resharder.startSplit(split)).resolves.toBeUndefined();
            expect(generations).toEqual([0]);
            await expect(resharder.abort("new-canceled")).resolves.toBeUndefined();
            resharder = await construct({});
            for (const migId of ["legacy-canceled", "new-canceled"]) {
                await expect(resharder.abort(migId)).resolves.toBeUndefined();
                await expect(resharder.startSplit({ ...split, migId })).rejects.toMatchObject({
                    code: "CDB_STALE_EPOCH",
                });
            }
        }
    );

    test("a Catalog begin response loss resumes from the exact durable identity after reconstruction", async () => {
        let beginCalls = 0;
        const catalog = {
            async beginTopologyOperation() {
                beginCalls++;
                if (beginCalls === 1) throw new Error("lost Catalog begin response after commit");
                return { status: "active" as const, ...topologySchema };
            },
        };
        let resharder = await construct(catalog);
        await expect(resharder.startSplit(split)).rejects.toThrow("lost Catalog begin response after commit");
        expect(
            db
                .query("SELECT state, src_shard, tables_json FROM migration_start_intent WHERE mig_id = ?")
                .get(split.migId)
        ).toMatchObject({ state: "starting", src_shard: split.srcShard });

        resharder = await construct(catalog);
        await expect(resharder.startSplit(split)).resolves.toBeUndefined();
        await expect(resharder.getPhase(split.migId)).resolves.toBe(RESHARDER_PHASE.INIT);
        expect(beginCalls).toBe(2);
        expect(db.query("SELECT COUNT(*) AS count FROM migration_start_intent").get()).toEqual({ count: 0 });
    });

    test("a completed Catalog operation cannot manufacture missing local migration state", async () => {
        const resharder = await construct({
            async beginTopologyOperation() {
                return { status: "completed" as const, ...topologySchema };
            },
        });

        await expect(resharder.startSplit(split)).rejects.toMatchObject({
            code: "CDB_RESHARD_PHASE_MISMATCH",
        });
        await expect(resharder.getPhase(split.migId)).resolves.toBeNull();
        expect(
            db.query("SELECT state, src_shard FROM migration_start_intent WHERE mig_id = ?").get(split.migId)
        ).toEqual({ state: "starting", src_shard: split.srcShard });
    });

    test("abort reconstructs and cancels an exact Catalog lease after begin response loss", async () => {
        let topologyStatus: "active" | "aborted" = "active";
        let abortCalls = 0;
        const catalog = {
            async beginTopologyOperation() {
                throw new Error("lost Catalog begin response after commit");
            },
            async topologyOperation() {
                return {
                    migrationId: split.migId,
                    sourceShard: split.srcShard,
                    destinationShard: split.dstShard,
                    rangeLo: split.rangeLo,
                    rangeHi: split.rangeHi,
                    startEpoch: split.epochAtStart,
                    status: topologyStatus,
                };
            },
            async abortTopologyOperation() {
                abortCalls++;
                topologyStatus = "aborted";
                return { status: "aborted" as const };
            },
        };
        let resharder = await construct(catalog);
        await expect(resharder.startSplit(split)).rejects.toThrow("lost Catalog begin response after commit");

        resharder = await construct(catalog);
        await expect(resharder.abort(split.migId)).resolves.toBeUndefined();
        expect(abortCalls).toBe(1);
        await expect(resharder.startSplit(split)).rejects.toMatchObject({ code: "CDB_STALE_EPOCH" });
    });

    test("concurrent exact starts share one Catalog call and a differing start fails before Catalog", async () => {
        let releaseBegin!: () => void;
        let markBeginStarted!: () => void;
        const gate = new Promise<void>(resolve => {
            releaseBegin = resolve;
        });
        const started = new Promise<void>(resolve => {
            markBeginStarted = resolve;
        });
        let beginCalls = 0;
        const resharder = await construct({
            async beginTopologyOperation() {
                beginCalls++;
                markBeginStarted();
                await gate;
                return { status: "active" as const, ...topologySchema };
            },
        });

        const first = resharder.startSplit(split);
        await started;
        const exact = resharder.startSplit({ ...split, tables: [{ ...table, columns: [...table.columns] }] });
        await expect(resharder.startSplit({ ...split, dstShard: "destination-other" })).rejects.toMatchObject({
            code: "CDB_RESHARD_PHASE_MISMATCH",
        });
        releaseBegin();
        await expect(Promise.all([first, exact])).resolves.toEqual([undefined, undefined]);
        expect(beginCalls).toBe(1);
    });

    test("concurrent runSplit calls share one phase driver", async () => {
        let releaseProvision!: () => void;
        let markProvisionStarted!: () => void;
        const provisionGate = new Promise<void>(resolve => {
            releaseProvision = resolve;
        });
        const provisionStarted = new Promise<void>(resolve => {
            markProvisionStarted = resolve;
        });
        let provisions = 0;
        let cutovers = 0;
        const sideStateCapabilities = async () => ({ vectorSnapshot: "v2" as const, fileTombstones: "v2" as const });
        const source = {
            reshardSideStateProtocolCapabilitiesV2: sideStateCapabilities,
            async beginReshardSource() {
                return { enabled: true, triggersInstalled: 3 };
            },
            async reshardTableOrder() {
                return { tableNames: [table.name] };
            },
            async bulkCopyBatch() {
                return { rows: [], lastRowid: 0, done: true };
            },
            async readTailBatch() {
                return { transactions: [], lastLsn: 0, done: true };
            },
            async ackTail() {
                return { pruned: 0, ackedLsn: 0 };
            },
            async readSplitOpLogBatch() {
                return { entries: [], lastLsn: 0, done: true };
            },
            async ackSplitOpLog() {
                return { pruned: 0, prunedBytes: 0, ackedLsn: 0 };
            },
            async prepareRoutingFence() {},
            async activateRoutingFence() {},
            async stopReshardCapture() {
                return { stopped: true };
            },
            async dropMigratedRange() {
                return { deleted: 0, done: true };
            },
            async finishReshardSource() {},
            async completeRoutingFenceCleanup() {},
        };
        const destination = {
            reshardSideStateProtocolCapabilitiesV2: sideStateCapabilities,
            async prepareReshardDestOwnership() {
                return { prepared: true, serving: true };
            },
            async activateReshardDestServing() {
                return { activated: true };
            },
            async provisionFreshReshardDestination() {
                provisions++;
                markProvisionStarted();
                await provisionGate;
            },
            async beginReshardDest() {
                return { ready: true };
            },
            async reshardTableOrder() {
                return { tableNames: [table.name] };
            },
            async applyBulkBatch() {
                return { applied: 0, skipped: 0 };
            },
            async closeReshardBulkDest() {
                return { closed: true };
            },
            async stageTailBatch() {
                return { staged: 0, lastLsn: 0 };
            },
            async readStagedTailBatch() {
                return { transactions: [] };
            },
            async ackStagedTail() {
                return { removed: 0 };
            },
            async closeTailStaging() {
                return { closed: true };
            },
            async applyTailBatch() {
                return { applied: 0, lastLsn: 0 };
            },
            async applySplitOpLogBatch() {
                return { applied: 0, replayed: 0, lastLsn: 0 };
            },
            async finishReshardDest() {},
        };
        const catalog = {
            async beginTopologyOperation() {
                return { status: "active" as const, ...topologySchema };
            },
            async cutover() {
                cutovers++;
                return { applied: true, newEpoch: split.epochAtStart + 1 };
            },
            async completeTopologyOperation() {
                return { status: "completed" as const };
            },
        };
        const resharder = await construct(catalog, {
            [split.srcShard]: source,
            [split.dstShard]: destination,
        });
        await resharder.startSplit(split);

        const first = resharder.runSplit(split.migId);
        await provisionStarted;
        const second = resharder.runSplit(split.migId);
        releaseProvision();

        await expect(Promise.all([first, second])).resolves.toEqual([
            { phase: RESHARDER_PHASE.TAIL_CAPTURE_ENABLED },
            { phase: RESHARDER_PHASE.TAIL_CAPTURE_ENABLED },
        ]);
        expect(provisions).toBe(1);
        for (let step = 0; step < 64; step++) {
            const result = await resharder.runSplit(split.migId);
            if (result.phase === RESHARDER_PHASE.SOURCE_DRAINED) break;
        }
        await expect(resharder.getPhase(split.migId)).resolves.toBe(RESHARDER_PHASE.SOURCE_DRAINED);
        expect(cutovers).toBe(1);
    });
});
