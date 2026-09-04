import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { CatalogAuthInvalidationStore } from "../../src/server/do/catalog-auth-invalidation-store.ts";
import { CatalogOrganizationDeletionStore } from "../../src/server/do/catalog-organization-deletion-store.ts";
import { Catalog, configureCatalogRuntime } from "../../src/server/do/catalog.ts";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";
import { defineMigrations } from "../../src/server/schema-migrations.ts";
import { PrincipalId, ShardId, TenantId } from "../../src/types.ts";
import { withRecoveryEnv } from "../helpers/recovery.ts";

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

describe("Catalog routing inventory", () => {
    let db: Database;
    let catalog: Catalog;
    let bootstrap: Promise<unknown>;
    let failNextTransactionCommit: boolean;
    let state: DurableObjectState;
    let alarmAt: number | null;

    beforeEach(async () => {
        db = new Database(":memory:");
        bootstrap = Promise.resolve();
        failNextTransactionCommit = false;
        alarmAt = null;
        const transactionSync = <T>(callback: () => T): T => {
            db.exec("BEGIN IMMEDIATE");
            try {
                const result = callback();
                if (failNextTransactionCommit) {
                    failNextTransactionCommit = false;
                    throw new Error("injected transaction commit failure");
                }
                db.exec("COMMIT");
                return result;
            } catch (error) {
                db.exec("ROLLBACK");
                throw error;
            }
        };
        state = {
            storage: {
                sql: sqlStorage(db),
                transactionSync,
                getAlarm: async () => alarmAt,
                setAlarm: async (deadline: number) => {
                    alarmAt = deadline;
                },
                transaction: async <T>(callback: (transaction: DurableObjectTransaction) => Promise<T>): Promise<T> => {
                    const previousAlarm = alarmAt;
                    db.exec("BEGIN IMMEDIATE");
                    try {
                        const result = await callback({
                            getAlarm: async () => alarmAt,
                            setAlarm: async (deadline: number) => {
                                alarmAt = deadline;
                            },
                        } as DurableObjectTransaction);
                        if (failNextTransactionCommit) {
                            failNextTransactionCommit = false;
                            throw new Error("injected transaction commit failure");
                        }
                        db.exec("COMMIT");
                        return result;
                    } catch (error) {
                        db.exec("ROLLBACK");
                        alarmAt = previousAlarm;
                        throw error;
                    }
                },
            },
            blockConcurrencyWhile: (callback: () => Promise<unknown>): void => {
                bootstrap = callback();
            },
        } as unknown as DurableObjectState;
        catalog = new Catalog(state, withRecoveryEnv({}));
        await bootstrap;
    });

    afterEach(() => db.close());

    test("lists narrow ranges, removes duplicate shard ids, and sorts the result", async () => {
        db.exec("DELETE FROM catalog_ranges");
        db.run("INSERT INTO catalog_ranges VALUES (0, 0, 'ShardDO_0')");
        db.run("INSERT INTO catalog_ranges VALUES (1, 1, 'ShardDO_z')");
        db.run("INSERT INTO catalog_ranges VALUES (2, 2, 'ShardDO_a')");
        db.run("INSERT INTO catalog_ranges VALUES (3, 3, 'ShardDO_z')");
        db.run("INSERT INTO catalog_ranges VALUES (4, 16383, 'ShardDO_0')");

        expect(await catalog.route(1)).toMatchObject({ shardId: "ShardDO_z" });
        expect(await catalog.listShardIds()).toEqual([
            ShardId("ShardDO_0"),
            ShardId("ShardDO_a"),
            ShardId("ShardDO_z"),
        ]);
    });

    test("sends only the scoped auth invalidation request across the Cdb RPC boundary", async () => {
        const calls: Record<string, unknown>[] = [];
        const shardNamespace = {
            idFromName: (name: string) => ({ name }),
            get: () => ({
                async invalidateAuthScope(input: Record<string, unknown>) {
                    calls.push(structuredClone(input));
                    return { ...input, accepted: true, registrations: 0, changeSeq: 0 };
                },
            }),
        } as unknown as DurableObjectNamespace;
        catalog = new Catalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
        await bootstrap;

        await catalog.bumpAuthEpoch("tenant", "org-rpc-contract");
        await catalog.alarm();

        expect(calls).toHaveLength(1);
        expect(Object.keys(calls[0] ?? {}).sort()).toEqual(["epoch", "recoveryGeneration", "scope", "scopeId"]);
        expect(calls[0]).toEqual({
            scope: "tenant",
            scopeId: "org-rpc-contract",
            epoch: 1,
            recoveryGeneration: 0,
        });
    });

    for (const scope of ["tenant", "principal", "global"] as const) {
        test.each(scope === "tenant" ? [null] : [null, undefined, false, 0, ""])(
            `retries ${scope} invalidation after rejecting with %p`,
            async failure => {
                const calls: string[] = [];
                let failing = true;
                const shardNamespace = {
                    idFromName: (name: string) => ({ name }),
                    get: (id: { name: string }) => ({
                        async invalidateAuthScope(input: Record<string, unknown>) {
                            calls.push(id.name);
                            if (failing && id.name === "ShardDO_0") throw failure;
                            return { ...input, accepted: true, registrations: 0, changeSeq: 0 };
                        },
                    }),
                } as unknown as DurableObjectNamespace;
                catalog = new Catalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
                await bootstrap;
                db.exec("DELETE FROM catalog_ranges");
                db.run("INSERT INTO catalog_ranges VALUES (0, 8191, 'ShardDO_0')");
                db.run("INSERT INTO catalog_ranges VALUES (8192, 16383, 'ShardDO_1')");
                const store = new CatalogAuthInvalidationStore(adaptSqlStorage(state.storage.sql));
                if (scope === "tenant") store.enqueueTargets(scope, "org", 1, ["ShardDO_0", "ShardDO_1"], 100);
                else if (scope === "principal") store.enqueuePrincipal("user", 1, 100);
                else store.enqueueGlobal(1, 100);
                const pending = () =>
                    scope === "tenant"
                        ? (store.targets(scope, "org")[0] ?? null)
                        : scope === "principal"
                          ? store.principal("user")
                          : store.global();
                const originalNow = Date.now;
                try {
                    Date.now = () => 100;
                    await catalog.alarm();
                    expect(calls).toEqual(["ShardDO_0", "ShardDO_1"]);
                    expect(pending()).toMatchObject({ attempts: 1, nextAttemptAt: 1_100, lastError: String(failure) });
                    if (scope !== "tenant") expect(pending()).toMatchObject({ cursorShardId: null });
                    else expect(store.targets(scope, "org")).toHaveLength(1);

                    failing = false;
                    Date.now = () => 1_100;
                    await catalog.alarm();
                    expect(calls.filter(id => id === "ShardDO_0")).toHaveLength(2);
                    Date.now = () => 1_101;
                    await catalog.alarm();
                    expect(pending()).toBeNull();
                } finally {
                    Date.now = originalNow;
                }
            }
        );
    }

    test("projects auth epochs to both topology participants through cutover, then only the destination", async () => {
        const calls: Array<{
            shardId: string;
            scope: string;
            scopeId: string;
            epoch: number;
            recoveryGeneration: number;
        }> = [];
        const shardNamespace = {
            idFromName: (name: string) => ({ name }),
            get: (id: { name: string }) => ({
                invalidateAuthScope(input: {
                    scope: "tenant";
                    scopeId: string;
                    epoch: number;
                    recoveryGeneration: number;
                }) {
                    calls.push({ shardId: id.name, ...input });
                    return { ...input, accepted: true, registrations: 0, changeSeq: 0 };
                },
            }),
        } as unknown as DurableObjectNamespace;
        let ready: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            ready = callback();
        };
        const projected = new Catalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
        await ready;
        const topology = {
            migId: "auth-cutover-projection",
            recoveryGeneration: 0,
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_1",
            rangeLo: 0,
            rangeHi: 16_383,
            startEpoch: 1,
        } as const;
        projected.beginTopologyOperation(topology);

        await projected.bumpAuthEpoch("tenant", "org-moving");
        await projected.alarm();
        expect(calls.splice(0)).toEqual([
            { shardId: "ShardDO_0", scope: "tenant", scopeId: "org-moving", epoch: 1, recoveryGeneration: 0 },
            { shardId: "ShardDO_1", scope: "tenant", scopeId: "org-moving", epoch: 1, recoveryGeneration: 0 },
        ]);

        await projected.cutover({
            recoveryGeneration: 0,
            migId: topology.migId,
            lo: topology.rangeLo,
            hi: topology.rangeHi,
            fromShard: topology.sourceShard,
            toShard: topology.destinationShard,
            startEpoch: topology.startEpoch,
        });
        await projected.bumpAuthEpoch("tenant", "org-moving");
        await projected.alarm();
        expect(calls.splice(0)).toEqual([
            { shardId: "ShardDO_0", scope: "tenant", scopeId: "org-moving", epoch: 2, recoveryGeneration: 0 },
            { shardId: "ShardDO_1", scope: "tenant", scopeId: "org-moving", epoch: 2, recoveryGeneration: 0 },
        ]);

        projected.completeTopologyOperation(topology);
        await projected.bumpAuthEpoch("tenant", "org-moving");
        await projected.alarm();
        expect(calls).toEqual([
            { shardId: "ShardDO_1", scope: "tenant", scopeId: "org-moving", epoch: 3, recoveryGeneration: 0 },
        ]);
    });

    test.each([new Error("injected shard outage"), null])(
        "bounds a legacy persisted multi-shard deletion inventory and retries its failed target for %p",
        async failure => {
            const journal = defineMigrations([
                {
                    version: 1,
                    name: "organization_files",
                    statements: ["SELECT 1"],
                    resources: [
                        {
                            kind: "file",
                            version: 1,
                            table: "messages",
                            column: "attachment",
                            primaryKey: "id",
                            organizationColumn: "organization_id",
                            maxSize: 8,
                            contentTypes: ["image/png"],
                        },
                    ],
                },
            ]);
            const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => journal });
            const calls = new Map<string, number>();
            const shardNamespace = {
                idFromName: (name: string) => ({ name }),
                get: (id: { name: string }) => ({
                    async deleteOrganizationFiles(input: { organizationId: string }) {
                        const count = (calls.get(id.name) ?? 0) + 1;
                        calls.set(id.name, count);
                        if (id.name === "ShardDO_00" && count === 1) throw failure;
                        return { organizationId: input.organizationId, accepted: true } as const;
                    },
                }),
            } as unknown as DurableObjectNamespace;
            let ready: Promise<unknown> = Promise.resolve();
            (
                state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
            ).blockConcurrencyWhile = callback => {
                ready = callback();
            };
            let configured = new ConfiguredCatalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
            await ready;

            const sql = adaptSqlStorage(state.storage.sql);
            state.storage.transactionSync(() => {
                sql.exec("DELETE FROM catalog_ranges");
                for (let index = 0; index < 35; index++) {
                    sql.exec(
                        "INSERT INTO catalog_ranges (lo, hi, shard_id) VALUES (?, ?, ?)",
                        index,
                        index === 34 ? 16_383 : index,
                        `ShardDO_${String(index).padStart(2, "0")}`
                    );
                }
                sql.exec(
                    `UPDATE catalog_schema_state
                 SET active_version = 1, active_epoch = 2, active_digest = ?
                 WHERE singleton = 1`,
                    journal.digest
                );
                const deletions = new CatalogOrganizationDeletionStore(sql);
                deletions.record("org-bounded", 0, 100);
                // Rows written by the former all-shard design must still drain after upgrade.
                deletions.recordShards(
                    "org-bounded",
                    Array.from({ length: 35 }, (_, index) => `ShardDO_${String(index).padStart(2, "0")}`),
                    100
                );
            });

            const originalNow = Date.now;
            try {
                Date.now = () => 100;
                await configured.alarm();
                expect([...calls.values()].reduce((sum, count) => sum + count, 0)).toBe(32);
                expect(new CatalogOrganizationDeletionStore(sql).shards("org-bounded")).toEqual(
                    expect.arrayContaining([
                        expect.objectContaining({ shardId: "ShardDO_00", status: "pending", attempts: 1 }),
                        expect.objectContaining({ shardId: "ShardDO_01", status: "complete", attempts: 0 }),
                        expect.objectContaining({ shardId: "ShardDO_34", status: "pending", attempts: 0 }),
                    ])
                );

                ready = Promise.resolve();
                configured = new ConfiguredCatalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
                await ready;
                Date.now = () => 101;
                await configured.alarm();
                expect(calls.get("ShardDO_01")).toBe(1);
                expect(calls.get("ShardDO_34")).toBe(1);

                Date.now = () => 1_100;
                await configured.alarm();
                expect(calls.get("ShardDO_00")).toBe(2);
                expect(
                    [...calls.entries()]
                        .filter(([shardId]) => shardId !== "ShardDO_00")
                        .every(([, count]) => count === 1)
                ).toBe(true);
                expect(new CatalogOrganizationDeletionStore(sql).read("org-bounded")).toMatchObject({
                    status: "complete",
                    completedAt: 1_100,
                });
            } finally {
                Date.now = originalNow;
            }
        }
    );

    test("recovers a vector-backed deletion through the legacy shard cleanup RPC", async () => {
        const journal = defineMigrations([
            {
                version: 1,
                name: "organization_vectors",
                statements: ["SELECT 1"],
                resources: [
                    {
                        kind: "vector",
                        version: 1,
                        table: "messages",
                        column: "embedding",
                        primaryKey: "id",
                        organizationColumn: "organization_id",
                        binding: "CDB_MESSAGES",
                        dimensions: 3,
                        metric: "cosine",
                    },
                ],
            },
        ]);
        const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => journal });
        const calls: Array<{ readonly shardId: string; readonly input: Record<string, unknown> }> = [];
        const shardNamespace = {
            idFromName: (name: string) => ({ name }),
            get: (id: { name: string }) => ({
                async deleteOrganizationFiles(input: Record<string, unknown>) {
                    calls.push({ shardId: id.name, input: structuredClone(input) });
                    return { organizationId: input.organizationId as string, accepted: true } as const;
                },
            }),
        } as unknown as DurableObjectNamespace;
        let ready: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            ready = callback();
        };
        const configured = new ConfiguredCatalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
        await ready;

        const sql = adaptSqlStorage(state.storage.sql);
        state.storage.transactionSync(() => {
            sql.exec(
                `UPDATE catalog_schema_state
                 SET active_version = 1, active_epoch = 2, active_digest = ?
                 WHERE singleton = 1`,
                journal.digest
            );
            new CatalogOrganizationDeletionStore(sql).record("org-vector-cleanup", 0, 100);
            sql.exec(
                `UPDATE _chardb_recovery_admission
                 SET generation = 1, operation_id = '00000000-0000-4000-8000-000000000001', state = 'released'
                 WHERE singleton = 1`
            );
        });

        const originalNow = Date.now;
        try {
            Date.now = () => 100;
            await configured.alarm();
        } finally {
            Date.now = originalNow;
        }

        expect(calls).toEqual([
            {
                shardId: "ShardDO_0",
                input: {
                    organizationId: "org-vector-cleanup",
                    nowMs: 100,
                    recoveryGeneration: 1,
                    domainSchemaEpoch: 2,
                },
            },
        ]);
        expect(new CatalogOrganizationDeletionStore(sql).read("org-vector-cleanup")).toMatchObject({
            status: "complete",
            completedAt: 100,
        });
    });

    test("returns file-only deletion status without opening vector tables on its Cdb", async () => {
        const journal = defineMigrations([
            {
                version: 1,
                name: "organization_files_status",
                statements: ["SELECT 1"],
                resources: [
                    {
                        kind: "file",
                        version: 1,
                        table: "messages",
                        column: "attachment",
                        primaryKey: "id",
                        organizationColumn: "organization_id",
                        maxSize: 8,
                        contentTypes: ["image/png"],
                    },
                ],
            },
        ]);
        const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => journal });
        let ready: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            ready = callback();
        };
        const configured = new ConfiguredCatalog(state, withRecoveryEnv({}));
        await ready;
        const sql = adaptSqlStorage(state.storage.sql);
        state.storage.transactionSync(() => {
            sql.exec(
                `UPDATE catalog_schema_state
                 SET active_version = 1, active_epoch = 2, active_digest = ?
                 WHERE singleton = 1`,
                journal.digest
            );
            const deletions = new CatalogOrganizationDeletionStore(sql);
            deletions.record("org-file-status", 0, 100);
            deletions.recordShards("org-file-status", ["ShardDO_0"], 100);
            deletions.completeShard("org-file-status", "ShardDO_0", 101);
        });

        expect(await configured.organizationDeletionPurgeStatus({ organizationId: "org-file-status" })).toEqual({
            organizationId: "org-file-status",
            authDeleted: true,
            handoffComplete: true,
            handoff: { state: "complete", attempts: 0, completedAt: 101, lastError: null },
            vectorPurge: null,
        });
    });

    test("fails closed when a completed vector handoff has no owner tombstone", async () => {
        const journal = defineMigrations([
            {
                version: 1,
                name: "organization_vectors_status",
                statements: ["SELECT 1"],
                resources: [
                    {
                        kind: "vector",
                        version: 1,
                        table: "messages",
                        column: "embedding",
                        primaryKey: "id",
                        organizationColumn: "organization_id",
                        binding: "CDB_MESSAGES",
                        dimensions: 3,
                        metric: "cosine",
                    },
                ],
            },
        ]);
        const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => journal });
        const shardNamespace = {
            idFromName: (name: string) => ({ name }),
            get: () => ({ vectorOrganizationPurgeStatus: async () => null }),
        } as unknown as DurableObjectNamespace;
        let ready: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            ready = callback();
        };
        const configured = new ConfiguredCatalog(state, withRecoveryEnv({ CDB_SHARD: shardNamespace }));
        await ready;
        const sql = adaptSqlStorage(state.storage.sql);
        state.storage.transactionSync(() => {
            sql.exec(
                `UPDATE catalog_schema_state
                 SET active_version = 1, active_epoch = 2, active_digest = ?
                 WHERE singleton = 1`,
                journal.digest
            );
            const deletions = new CatalogOrganizationDeletionStore(sql);
            deletions.record("org-vector-status", 0, 100);
            deletions.recordShards("org-vector-status", ["ShardDO_0"], 100);
            deletions.completeShard("org-vector-status", "ShardDO_0", 101);
        });

        await expect(
            configured.organizationDeletionPurgeStatus({ organizationId: "org-vector-status" })
        ).rejects.toThrow("CDB_INVARIANT: completed vector deletion handoff has no current-owner purge tombstone");
    });

    test("publishes a cutover map only after its durable transaction commits", async () => {
        const rangesBefore = db.query("SELECT lo, hi, shard_id FROM catalog_ranges ORDER BY lo").all();
        const epochBefore = db
            .query("SELECT epoch FROM catalog_epoch WHERE scope = 'schema' AND scope_id = 'global'")
            .get() as { epoch: number };
        expect(await catalog.route(0)).toEqual({
            shardId: ShardId("ShardDO_0"),
            schemaEpoch: epochBefore.epoch,
            recoveryGeneration: 0,
            domainSchemaEpoch: 1,
        });

        const request = {
            migId: "migration-commit-failure",
            recoveryGeneration: 0,
            lo: 0,
            hi: 0,
            fromShard: "ShardDO_0",
            toShard: "ShardDO_1",
        };
        await expect(catalog.cutover(request)).rejects.toMatchObject({
            code: "CDB_STALE_EPOCH",
            message: "topology operation lease is missing",
        });
        await expect(
            catalog.beginOrganizationDeletionBarrier({
                recoveryGeneration: 0,
                migId: request.migId,
                rangeLo: request.lo,
                rangeHi: request.hi,
            })
        ).rejects.toMatchObject({
            code: "CDB_STALE_EPOCH",
            message: "organization deletion barrier does not match an active topology operation",
        });
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: request.migId })).toBeNull();
        catalog.beginTopologyOperation({
            recoveryGeneration: 0,
            migId: request.migId,
            sourceShard: request.fromShard,
            destinationShard: request.toShard,
            rangeLo: request.lo,
            rangeHi: request.hi,
            startEpoch: epochBefore.epoch,
        });
        const deletions = new CatalogOrganizationDeletionStore(adaptSqlStorage(state.storage.sql));
        deletions.record("pre-barrier-delete", request.lo, 10);
        const deletionBarrierRequest = {
            migId: request.migId,
            recoveryGeneration: request.recoveryGeneration,
            rangeLo: request.lo,
            rangeHi: request.hi,
        };
        await expect(catalog.beginOrganizationDeletionBarrier(deletionBarrierRequest)).resolves.toMatchObject({
            status: "active",
        });
        await expect(catalog.cutover(request)).rejects.toMatchObject({
            code: "CDB_RESHARD_PHASE_MISMATCH",
            message: expect.stringContaining("older organization deletions are still pending"),
        });
        expect(await catalog.route(0)).toMatchObject({ shardId: "ShardDO_0", schemaEpoch: epochBefore.epoch });
        expect(db.query("SELECT v FROM catalog_meta WHERE k = ?").get(`cutover:${request.migId}`)).toBeNull();
        deletions.complete("pre-barrier-delete", 11);
        expect(catalog.organizationDeletionBarrierStatus(deletionBarrierRequest).olderDeletionsComplete).toBe(true);
        failNextTransactionCommit = true;
        await expect(catalog.cutover(request)).rejects.toThrow("injected transaction commit failure");

        expect(db.query("SELECT lo, hi, shard_id FROM catalog_ranges ORDER BY lo").all()).toEqual(rangesBefore);
        expect(
            db.query("SELECT epoch FROM catalog_epoch WHERE scope = 'schema' AND scope_id = 'global'").get()
        ).toEqual(epochBefore);
        expect(db.query("SELECT v FROM catalog_meta WHERE k = ?").get(`cutover:${request.migId}`)).toBeNull();
        expect(await catalog.route(0)).toEqual({
            shardId: ShardId("ShardDO_0"),
            schemaEpoch: epochBefore.epoch,
            recoveryGeneration: 0,
            domainSchemaEpoch: 1,
        });
        expect(catalog.organizationDeletionBarrierStatus(deletionBarrierRequest)).toMatchObject({
            barrier: { status: "active", finishedAt: null },
            olderDeletionsComplete: true,
        });

        await expect(catalog.cutover(request)).resolves.toEqual({ applied: true, newEpoch: epochBefore.epoch + 1 });
        expect(await catalog.route(0)).toEqual({
            shardId: ShardId("ShardDO_1"),
            schemaEpoch: epochBefore.epoch + 1,
            recoveryGeneration: 0,
            domainSchemaEpoch: 1,
        });
        expect(catalog.organizationDeletionBarrierStatus(deletionBarrierRequest)).toMatchObject({
            barrier: { status: "released", finishedAt: expect.any(Number) },
            olderDeletionsComplete: true,
        });
        await expect(catalog.cutover(request)).resolves.toEqual({ applied: false, newEpoch: epochBefore.epoch + 1 });
        expect(db.query("SELECT v FROM catalog_meta WHERE k = ?").get(`cutover:${request.migId}`)).toEqual({
            v: "ShardDO_0",
        });
    });

    test("reconstructs one exact topology lease and excludes schema migration until completion", async () => {
        const request = {
            migId: "durable-split-1",
            recoveryGeneration: 0,
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_1",
            rangeLo: 0,
            rangeHi: 31,
            startEpoch: 1,
        };
        expect(catalog.beginTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            migrationId: request.migId,
            status: "active",
            schemaVersion: 0,
            schemaEpoch: 1,
        });
        expect(catalog.beginTopologyOperation(request)).toMatchObject({ recoveryGeneration: 0, status: "active" });
        expect(catalog.topologyRoutingStatus(request)).toEqual({
            owner: "source",
            schemaEpoch: 1,
            operationStatus: "active",
        });
        expect(() => catalog.beginTopologyOperation({ ...request, rangeHi: 32 })).toThrow(
            expect.objectContaining({ code: "CDB_STALE_EPOCH", message: "topology operation identity changed" })
        );

        let reconstructedReady: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            reconstructedReady = callback();
        };
        const reconstructed = new Catalog(state, withRecoveryEnv({}));
        await reconstructedReady;
        expect(reconstructed.topologyOperation({ recoveryGeneration: 0, migrationId: request.migId })).toMatchObject({
            status: "active",
            rangeLo: 0,
            rangeHi: 31,
        });

        const future = defineMigrations([{ version: 1, name: "topology_exclusion", statements: ["SELECT 1"] }]);
        const FutureCatalog = configureCatalogRuntime({ migrations: () => future });
        let futureReady: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            futureReady = callback();
        };
        const futureCatalog = new FutureCatalog(state, withRecoveryEnv({}));
        await futureReady;
        expect(() => futureCatalog.beginSchemaMigration({ migrationId: "schema-v1", targetVersion: 1 })).toThrow(
            expect.objectContaining({
                code: "CDB_STALE_EPOCH",
                message: "topology operation durable-split-1 is in progress",
            })
        );

        await expect(
            futureCatalog.cutover({
                recoveryGeneration: 0,
                migId: request.migId,
                lo: request.rangeLo,
                hi: request.rangeHi,
                fromShard: request.sourceShard,
                toShard: "ShardDO_wrong",
                startEpoch: request.startEpoch,
            })
        ).rejects.toMatchObject({ code: "CDB_STALE_EPOCH", message: "topology operation identity changed" });
        expect(await reconstructed.route(0)).toMatchObject({ shardId: "ShardDO_0", schemaEpoch: 1 });

        await expect(
            reconstructed.cutover({
                recoveryGeneration: 0,
                migId: request.migId,
                lo: request.rangeLo,
                hi: request.rangeHi,
                fromShard: request.sourceShard,
                toShard: request.destinationShard,
                startEpoch: request.startEpoch,
            })
        ).resolves.toEqual({ applied: true, newEpoch: 2 });
        expect(reconstructed.topologyRoutingStatus(request)).toEqual({
            owner: "destination",
            schemaEpoch: 2,
            operationStatus: "active",
        });
        expect(reconstructed.topologyOperation({ recoveryGeneration: 0, migrationId: request.migId })).toMatchObject({
            status: "active",
        });
        expect(reconstructed.beginTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            status: "active",
        });

        expect(reconstructed.completeTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            status: "completed",
            completedEpoch: 2,
        });
        expect(reconstructed.completeTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            status: "completed",
        });
        expect(reconstructed.topologyRoutingStatus(request)).toEqual({
            owner: "destination",
            schemaEpoch: 2,
            operationStatus: "completed",
        });
        expect(reconstructed.beginTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            status: "completed",
        });
        await expect(
            reconstructed.cutover({
                recoveryGeneration: 0,
                migId: request.migId,
                lo: request.rangeLo,
                hi: request.rangeHi,
                fromShard: request.sourceShard,
                toShard: request.destinationShard,
                startEpoch: request.startEpoch,
            })
        ).resolves.toEqual({ applied: false, newEpoch: 2 });
        expect(futureCatalog.beginSchemaMigration({ migrationId: "schema-v1", targetVersion: 1 })).toMatchObject({
            status: "migrating",
        });
    });

    test("atomically derives and claims one exact current topology owner", () => {
        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "invalid-range",
                destinationShard: "ShardDO_1",
                rangeLo: 15,
                rangeHi: 8,
            })
        ).toThrow(expect.objectContaining({ code: "CDB_INVALID_ARGS", message: "topology vshard range is invalid" }));
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "invalid-range" })).toBeNull();
        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "invalid-destination",
                destinationShard: "bad shard",
                rangeLo: 8,
                rangeHi: 15,
            })
        ).toThrow(
            expect.objectContaining({ code: "CDB_INVALID_ARGS", message: "topology destination shard is invalid" })
        );
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "invalid-destination" })).toBeNull();

        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "same-owner",
                destinationShard: "ShardDO_0",
                rangeLo: 8,
                rangeHi: 15,
            })
        ).toThrow(
            expect.objectContaining({
                code: "CDB_INVALID_ARGS",
                message: "topology source and destination must differ",
            })
        );
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "same-owner" })).toBeNull();

        failNextTransactionCommit = true;
        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "derived-split",
                destinationShard: "ShardDO_1",
                rangeLo: 8,
                rangeHi: 15,
            })
        ).toThrow(/injected transaction commit failure/);
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "derived-split" })).toBeNull();

        const claimed = catalog.beginDerivedTopologyOperation({
            recoveryGeneration: 0,
            migId: "derived-split",
            destinationShard: "ShardDO_1",
            rangeLo: 8,
            rangeHi: 15,
        });
        expect(claimed).toMatchObject({
            migrationId: "derived-split",
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_1",
            rangeLo: 8,
            rangeHi: 15,
            startEpoch: 1,
            schemaVersion: 0,
            schemaEpoch: 1,
            status: "active",
        });
        expect(
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "derived-split",
                destinationShard: "ShardDO_1",
                rangeLo: 8,
                rangeHi: 15,
            })
        ).toEqual(claimed);
        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "derived-split",
                destinationShard: "ShardDO_2",
                rangeLo: 8,
                rangeHi: 15,
            })
        ).toThrow(expect.objectContaining({ code: "CDB_STALE_EPOCH", message: "topology operation identity changed" }));
        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "conflicting-split",
                destinationShard: "ShardDO_2",
                rangeLo: 16,
                rangeHi: 31,
            })
        ).toThrow(
            expect.objectContaining({
                code: "CDB_STALE_EPOCH",
                message: "topology operation derived-split is already active",
            })
        );
    });

    test("derived topology claim rejects a range crossing another owner", () => {
        db.exec("DELETE FROM catalog_ranges");
        db.run("INSERT INTO catalog_ranges VALUES (0, 7, 'ShardDO_0')");
        db.run("INSERT INTO catalog_ranges VALUES (8, 15, 'ShardDO_middle')");
        db.run("INSERT INTO catalog_ranges VALUES (16, 16383, 'ShardDO_0')");

        expect(() =>
            catalog.beginDerivedTopologyOperation({
                recoveryGeneration: 0,
                migId: "derived-crossing-owner",
                destinationShard: "ShardDO_1",
                rangeLo: 0,
                rangeHi: 31,
            })
        ).toThrow(
            expect.objectContaining({
                code: "CDB_STALE_EPOCH",
                message: "topology range does not have one exact current owner",
            })
        );
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "derived-crossing-owner" })).toBeNull();
    });

    test("aborts only a pre-cutover topology lease and releases the next lease", async () => {
        const request = {
            recoveryGeneration: 0,
            migId: "abort-split-1",
            sourceShard: "ShardDO_0",
            destinationShard: "ShardDO_1",
            rangeLo: 8,
            rangeHi: 15,
            startEpoch: 1,
        };
        catalog.beginTopologyOperation(request);
        const deletionBarrierRequest = {
            recoveryGeneration: 0,
            migId: request.migId,
            rangeLo: request.rangeLo,
            rangeHi: request.rangeHi,
        };
        await expect(catalog.beginOrganizationDeletionBarrier(deletionBarrierRequest)).resolves.toMatchObject({
            status: "active",
        });
        expect(catalog.abortTopologyOperation(request)).toMatchObject({
            recoveryGeneration: 0,
            status: "aborted",
            completedEpoch: null,
        });
        expect(catalog.organizationDeletionBarrierStatus(deletionBarrierRequest)).toMatchObject({
            barrier: { status: "aborted", finishedAt: expect.any(Number) },
        });
        expect(catalog.abortTopologyOperation(request)).toMatchObject({ recoveryGeneration: 0, status: "aborted" });
        expect(
            catalog.beginTopologyOperation({
                recoveryGeneration: 0,
                migId: "after-abort",
                sourceShard: "ShardDO_0",
                destinationShard: "ShardDO_2",
                rangeLo: 8,
                rangeHi: 15,
                startEpoch: 1,
            })
        ).toMatchObject({ status: "active" });
        expect(await catalog.route(8)).toMatchObject({ shardId: "ShardDO_0", schemaEpoch: 1 });
        expect(catalog.abortTopologyOperation(request)).toMatchObject({ recoveryGeneration: 0, status: "aborted" });
    });

    test("rejects a topology lease whose matching endpoints hide another range owner", async () => {
        db.exec("DELETE FROM catalog_ranges");
        db.run("INSERT INTO catalog_ranges VALUES (0, 7, 'ShardDO_0')");
        db.run("INSERT INTO catalog_ranges VALUES (8, 15, 'ShardDO_middle')");
        db.run("INSERT INTO catalog_ranges VALUES (16, 16383, 'ShardDO_0')");
        db.run("UPDATE catalog_epoch SET epoch = 2 WHERE scope = 'schema' AND scope_id = 'global'");
        expect(() =>
            catalog.beginTopologyOperation({
                recoveryGeneration: 0,
                migId: "crossing-owner",
                sourceShard: "ShardDO_0",
                destinationShard: "ShardDO_1",
                rangeLo: 0,
                rangeHi: 31,
                startEpoch: 2,
            })
        ).toThrow(
            expect.objectContaining({
                code: "CDB_STALE_EPOCH",
                message: "topology source identity does not match current routing",
            })
        );
        expect(catalog.topologyOperation({ recoveryGeneration: 0, migrationId: "crossing-owner" })).toBeNull();
    });

    test("persists migration ownership, fences routes, and activates one exact journal version", async () => {
        const journal = defineMigrations([]);
        const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => journal });
        db.close();
        db = new Database(":memory:");
        (state.storage as unknown as { sql: ReturnType<typeof sqlStorage> }).sql = sqlStorage(db);
        let configuredReady: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            configuredReady = callback();
        };
        const configured = new ConfiguredCatalog(state, withRecoveryEnv({}));
        await configuredReady;

        expect(configured.schemaState()).toMatchObject({
            activeVersion: 0,
            activeEpoch: 1,
            activeDigest: journal.digest,
            status: "active",
        });

        const future = defineMigrations([
            {
                version: 1,
                name: "add_slug",
                statements: ["ALTER TABLE example ADD COLUMN slug TEXT"],
                catalogStatements: ["SELECT 3"],
            },
        ]);
        const FutureCatalog = configureCatalogRuntime({ migrations: () => future });
        const migrationCalls: string[] = [];
        const migrationCdb = {
            async prepareSchemaMigration() {
                migrationCalls.push("prepare");
            },
            async applySchemaMigration(input: { version: number }) {
                migrationCalls.push(`apply:${input.version}`);
            },
            async activateSchemaMigration() {
                migrationCalls.push("activate");
            },
        };
        let futureReady: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            futureReady = callback();
        };
        const reconstructed = new FutureCatalog(
            state,
            withRecoveryEnv({
                CDB_SHARD: {
                    idFromName: (name: string) => ({ toString: () => name }),
                    get: () => migrationCdb,
                } as unknown as DurableObjectNamespace,
            })
        );
        await futureReady;
        failNextTransactionCommit = true;
        expect(() => reconstructed.beginSchemaMigration({ migrationId: "deploy-3", targetVersion: 1 })).toThrow(
            /injected transaction commit failure/
        );
        expect(reconstructed.schemaState()).toMatchObject({ activeVersion: 0, status: "active" });
        await expect(reconstructed.route(0)).rejects.toMatchObject({ code: "CDB_STALE_EPOCH", retryable: true });
        expect(reconstructed.beginSchemaMigration({ migrationId: "deploy-3", targetVersion: 1 })).toMatchObject({
            activeVersion: 0,
            activeEpoch: 1,
            status: "migrating",
            migrationId: "deploy-3",
            targetVersion: 1,
            targetEpoch: 2,
        });
        expect(() =>
            reconstructed.beginTopologyOperation({
                recoveryGeneration: 0,
                migId: "topology-during-schema",
                sourceShard: "ShardDO_0",
                destinationShard: "ShardDO_1",
                rangeLo: 0,
                rangeHi: 31,
                startEpoch: 1,
            })
        ).toThrow(
            expect.objectContaining({
                code: "CDB_STALE_EPOCH",
                message: "schema migration blocks topology operation",
            })
        );
        expect(
            reconstructed.topologyOperation({ recoveryGeneration: 0, migrationId: "topology-during-schema" })
        ).toBeNull();
        expect(reconstructed.beginSchemaMigration({ migrationId: "deploy-3", targetVersion: 1 })).toMatchObject({
            status: "migrating",
        });
        await expect(reconstructed.route(0)).rejects.toMatchObject({ code: "CDB_STALE_EPOCH", retryable: true });
        await expect(
            reconstructed.resolveOrganizationAuthorityRoute({
                principalId: PrincipalId("migration-user"),
                organizationId: TenantId("migration-org"),
                vshard: 0,
            })
        ).rejects.toMatchObject({ code: "CDB_STALE_EPOCH", retryable: true });
        expect(() => reconstructed.completeSchemaMigration({ migrationId: "deploy-3" })).toThrow(/incomplete/);
        failNextTransactionCommit = true;
        await expect(
            reconstructed.migrateSchemaShard({ migrationId: "deploy-3", shardId: "ShardDO_0" })
        ).rejects.toThrow(/injected transaction commit failure/);
        expect(reconstructed.schemaMigrationShards({ migrationId: "deploy-3" })).toEqual([
            expect.objectContaining({ shardId: "ShardDO_0", status: "pending" }),
        ]);
        expect(migrationCalls).toEqual(["prepare", "apply:1", "activate"]);
        await expect(
            reconstructed.migrateSchemaShard({ migrationId: "deploy-3", shardId: "ShardDO_0" })
        ).resolves.toMatchObject({ shardId: "ShardDO_0", status: "active" });
        expect(migrationCalls).toEqual(["prepare", "apply:1", "activate", "prepare", "apply:1", "activate"]);
        await expect(
            reconstructed.migrateSchemaShard({ migrationId: "deploy-3", shardId: "ShardDO_0" })
        ).resolves.toMatchObject({ shardId: "ShardDO_0", status: "active" });
        expect(migrationCalls).toEqual(["prepare", "apply:1", "activate", "prepare", "apply:1", "activate"]);
        expect(() => reconstructed.completeSchemaMigration({ migrationId: "deploy-3" })).toThrow(
            /steps are incomplete/
        );
        expect(reconstructed.applyCatalogSchemaMigration({ migrationId: "deploy-3", version: 1 })).toMatchObject({
            activeVersion: 0,
            status: "migrating",
        });
        expect(reconstructed.applyCatalogSchemaMigration({ migrationId: "deploy-3", version: 1 })).toMatchObject({
            status: "migrating",
        });
        failNextTransactionCommit = true;
        expect(() => reconstructed.completeSchemaMigration({ migrationId: "deploy-3" })).toThrow(
            /injected transaction commit failure/
        );
        expect(reconstructed.schemaState()).toMatchObject({ activeVersion: 0, status: "migrating" });
        await expect(reconstructed.route(0)).rejects.toMatchObject({ code: "CDB_STALE_EPOCH", retryable: true });
        expect(reconstructed.completeSchemaMigration({ migrationId: "deploy-3" })).toMatchObject({
            activeVersion: 1,
            activeEpoch: 2,
            activeDigest: future.digest,
            lastMigrationId: "deploy-3",
            status: "active",
        });
        expect(reconstructed.completeSchemaMigration({ migrationId: "deploy-3" })).toMatchObject({
            activeVersion: 1,
            activeEpoch: 2,
        });
        expect(() => reconstructed.completeSchemaMigration({ migrationId: "another-deploy" })).toThrow(/not active/);
        await expect(reconstructed.route(0)).resolves.toMatchObject({ shardId: "ShardDO_0" });
    });

    test("baselines every existing shard and skips packaged SQL after exact schema checks", async () => {
        const journal = defineMigrations([
            {
                version: 1,
                name: "adopt_existing_schema",
                statements: ["THIS CDB SQL MUST NOT EXECUTE"],
                catalogStatements: ["THIS CATALOG SQL MUST NOT EXECUTE"],
            },
        ]);
        const FutureCatalog = configureCatalogRuntime({ migrations: () => journal });
        const calls: unknown[] = [];
        const migrationCdb = {
            async baselineSchemaMigration(input: unknown) {
                calls.push(input);
            },
            async prepareSchemaMigration() {
                throw new Error("baseline must not prepare an applying migration");
            },
        };
        let ready: Promise<unknown> = Promise.resolve();
        (
            state as unknown as { blockConcurrencyWhile: (callback: () => Promise<unknown>) => void }
        ).blockConcurrencyWhile = callback => {
            ready = callback();
        };
        const adopting = new FutureCatalog(
            state,
            withRecoveryEnv({
                CDB_SHARD: {
                    idFromName: (name: string) => ({ toString: () => name }),
                    get: () => migrationCdb,
                } as unknown as DurableObjectNamespace,
            })
        );
        await ready;

        expect(adopting.beginSchemaBaseline({ migrationId: "baseline-v1", targetVersion: 1 })).toMatchObject({
            activeVersion: 0,
            status: "migrating",
            migrationId: "baseline-v1",
            targetVersion: 1,
            targetEpoch: 2,
        });
        await expect(
            adopting.migrateSchemaShard({ migrationId: "baseline-v1", shardId: "ShardDO_0" })
        ).resolves.toMatchObject({ status: "active" });
        expect(calls).toEqual([
            {
                migrationId: "baseline-v1",
                recoveryGeneration: 0,
                targetVersion: 1,
                targetEpoch: 2,
                targetDigest: journal.digest,
            },
        ]);
        expect(adopting.applyCatalogSchemaMigration({ migrationId: "baseline-v1", version: 1 })).toMatchObject({
            status: "migrating",
        });
        expect(adopting.completeSchemaMigration({ migrationId: "baseline-v1" })).toMatchObject({
            activeVersion: 1,
            activeEpoch: 2,
            activeDigest: journal.digest,
            lastMigrationId: "baseline-v1",
            status: "active",
        });
        expect(db.query("SELECT COUNT(*) AS count FROM catalog_schema_steps").get()).toEqual({ count: 1 });
        expect(db.query("SELECT COUNT(*) AS count FROM catalog_schema_baselines").get()).toEqual({ count: 0 });
        expect(db.query("SELECT COUNT(*) AS count FROM catalog_schema_shards").get()).toEqual({ count: 0 });
    });
});
