/**
 * Workerd-level integration test for the `Cdb` reshard surface.
 *
 * Boots `miniflare` with a bundled test worker that exposes `TestCdb`
 * (extends the production `Cdb`) and drives a single-table reshard
 * end-to-end against the real Durable Object `SqlStorage`:
 *
 *   1. seed source rows on `Cdb_src`,
 *   2. `beginReshardSource(migId, range, tables)` installs triggers,
 *   3. additional inserts after triggers populate `_chardb_split_log`,
 *   4. `bulkCopyBatch` paginates the source's pre-trigger rows,
 *   5. `applyBulkBatch` lands them on `Cdb_dst`,
 *   6. `readTailBatch` + `applyTailBatch` move the post-trigger writes,
 *   7. assert dest matches the source's in-range view.
 *
 * The harness deliberately keeps the bundle minimal — `Bun.build` resolves
 * the chardb sources directly so we don't drag the whole library into a
 * worker bundle. Skipped on environments without `Bun` (e.g. node-only
 * CI) by checking `typeof Bun !== "undefined"`.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { Miniflare } from "miniflare";
import { disposeMiniflareBounded, restartMiniflareBounded } from "../../scripts/miniflare-lifecycle.mjs";
import { rowVshard } from "../../src/reshard/range.ts";
import { type TableSpec, renderTableTriggers } from "../../src/reshard/triggers.ts";
import { CDB_RESHARD_MAX_ROW_BYTES } from "../../src/server/do/cdb-reshard-relational.ts";
import { VSHARD_COUNT } from "../../src/vshard.ts";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ENTRY = path.join(HERE, "worker.entry.ts");

const MOVABLE_TABLES = [
    { name: "dedup_records", partitionColumn: "org_id", columns: ["id", "org_id", "value"] },
    { name: "messages", partitionColumn: "org_id", columns: ["id", "org_id", "body"] },
    { name: "messages_iso", partitionColumn: "org_id", columns: ["id", "org_id", "body"] },
    { name: "partition_moves", partitionColumn: "org_id", columns: ["id", "org_id", "body"] },
    { name: "drain_progress", partitionColumn: "org_id", columns: ["id", "org_id", "body"] },
    { name: "reshard_parents", partitionColumn: "org_id", columns: ["id", "org_id"] },
    {
        name: "reshard_children",
        partitionColumn: "org_id",
        columns: ["id", "org_id", "parent_id"],
    },
    {
        name: "composite_moves",
        partitionColumn: "org_id",
        columns: ["id", "revision", "org_id", "parent_id", "body"],
    },
    {
        name: "org_user_documents",
        partitionColumn: "organization_id",
        columns: ["id", "organization_id", "owner_id", "reviewer_id", "body"],
    },
] as const satisfies readonly TableSpec[];

interface RpcCall {
    readonly op: string;
    readonly target: string;
    readonly body?: unknown;
}

const RECOVERY_FENCED_OPS = new Set([
    "prepareSchemaMigration",
    "provisionFreshReshardDestination",
    "prepareReshardDestOwnership",
    "beginReshardSource",
    "beginReshardDest",
    "activateReshardDestServing",
    "_activateReshardDestThenLoseResponse",
    "_ackThenLoseResponse",
    "_ackSplitOpLogThenLoseResponse",
    "_stageTailThenLoseResponse",
    "_finishReshardSourceThenLoseResponse",
    "prepareRoutingFence",
    "activateRoutingFence",
    "tailWatermark",
    "reshardTableOrder",
    "bulkCopyBatch",
    "applyBulkBatch",
    "closeReshardBulkDest",
    "readTailBatch",
    "ackTail",
    "applyTailBatch",
    "stageTailBatch",
    "readStagedTailBatch",
    "ackStagedTail",
    "closeTailStaging",
    "stopReshardCapture",
    "readSplitOpLogBatch",
    "ackSplitOpLog",
    "applySplitOpLogBatch",
    "dropMigratedRange",
    "finishReshardSource",
    "finishReshardDest",
    "abortReshardSource",
    "beginReshardDestAbort",
    "abortReshardDestBatch",
]);

const SCHEMA_IDENTITY_OPS = new Set([
    "beginReshardSource",
    "beginReshardDest",
    "activateReshardDestServing",
    "_activateReshardDestThenLoseResponse",
    "finishReshardSource",
    "finishReshardDest",
    "abortReshardSource",
    "beginReshardDestAbort",
    "abortReshardDestBatch",
]);

let mf: Miniflare | undefined;
let workerSource = "";
let temporaryPath = "";

async function buildWorker(): Promise<string> {
    // Bun's build API (Bun.build) and the CLI both reach the same bundler,
    // but inside `bun test` the API hits a stricter resolver that drops
    // relative `.ts` imports. Shell out to the CLI which behaves correctly.
    const out = path.join(HERE, ".test-worker.bundle.mjs");
    const proc = Bun.spawn(
        ["bun", "build", ENTRY, "--target=browser", "--format=esm", "--external=cloudflare:workers", "--outfile", out],
        { stdout: "pipe", stderr: "pipe" }
    );
    const exitCode = await proc.exited;
    if (exitCode !== 0) {
        const stderr = await new Response(proc.stderr).text();
        throw new Error(`bundle failed (exit ${exitCode}):\n${stderr}`);
    }
    return Bun.file(out).text();
}

function createRuntime(): Miniflare {
    return new Miniflare({
        modules: true,
        script: workerSource,
        durableObjects: {
            CDB: { className: "TestCdb", useSQLite: true },
            FRESH: { className: "FreshTestCdb", useSQLite: true },
            REPLICATED: { className: "ReplicatedTestCdb", useSQLite: true },
            CDB_RESHARD: { className: "Resharder", useSQLite: true },
        },
        durableObjectsPersist: path.join(temporaryPath, "durable-objects"),
        compatibilityDate: "2024-09-23",
        compatibilityFlags: ["nodejs_compat"],
    });
}

async function restartRuntime(): Promise<void> {
    const current = mf;
    mf = undefined;
    if (!current) throw new Error("reshard fixture restart has no active runtime");
    const restarted = await restartMiniflareBounded(current, createRuntime, {
        label: "reshard fixture cold reconstruction",
        settleDelayMs: 500,
    });
    if (restarted.disposal.status !== "disposed") {
        await disposeMiniflareBounded(restarted.instance, { label: "rejected reshard fixture reconstruction" });
        throw new Error(`reshard fixture restart failed: ${restarted.disposal.status}`);
    }
    mf = restarted.instance;
}

beforeAll(async () => {
    temporaryPath = await mkdtemp(path.join(tmpdir(), "chardb-org-user-reshard-"));
    workerSource = await buildWorker();
    mf = (
        await restartMiniflareBounded(undefined, createRuntime, {
            label: "reshard fixture startup",
            settleDelayMs: 100,
        })
    ).instance;
});

afterAll(async () => {
    await disposeMiniflareBounded(mf, { label: "reshard fixture final teardown" });
    mf = undefined;
    if (temporaryPath) await rm(temporaryPath, { recursive: true, force: true });
});

async function rpc({ op, target, body }: RpcCall): Promise<unknown> {
    if (!mf) throw new Error("miniflare not initialized");
    let requestBody = body;
    if (RECOVERY_FENCED_OPS.has(op) && body && typeof body === "object") {
        requestBody = { ...(body as Record<string, unknown>), recoveryGeneration: 0 };
    }
    if (SCHEMA_IDENTITY_OPS.has(op) && body && typeof body === "object") {
        const schema = (await rpc({ op: "schemaState", target })) as {
            activeVersion: number;
            activeEpoch: number;
            activeDigest: string;
        };
        requestBody = {
            ...(requestBody as Record<string, unknown>),
            schemaVersion: schema.activeVersion,
            schemaEpoch: schema.activeEpoch,
            schemaDigest: schema.activeDigest,
            ...(["beginReshardDest", "beginReshardDestAbort"].includes(op) &&
            !("destinationGeneration" in (body as Record<string, unknown>))
                ? { destinationGeneration: 2 }
                : {}),
        };
    }
    const url = `http://example.com/${op}?name=${target}`;
    const res = await mf.dispatchFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody ?? {}, (_key, item) =>
            item instanceof Uint8Array ? { __chardb_test_bytes: Array.from(item) } : item
        ),
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`rpc ${op}/${target} → HTTP ${res.status}: ${text}`);
    }
    return JSON.parse(await res.text(), (_key, item) => {
        if (
            item &&
            typeof item === "object" &&
            !Array.isArray(item) &&
            Object.keys(item).length === 1 &&
            Array.isArray((item as Record<string, unknown>).__chardb_test_bytes)
        ) {
            return Uint8Array.from((item as { __chardb_test_bytes: number[] }).__chardb_test_bytes);
        }
        return item;
    });
}

describe("workerd reshard harness", () => {
    test("moves an organization-user row without materializing Catalog foreign keys", async () => {
        const source = "org-user-source";
        const destination = "org-user-destination";
        const organizationId = "org-user-reshard";
        const vshard = rowVshard(organizationId);
        const migId = "mig_org_user";
        const table = {
            name: "org_user_documents",
            partitionColumn: "organization_id",
            columns: ["id", "organization_id", "owner_id", "reviewer_id", "body"],
        } as const;
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };

        expect(await rpc({ op: "_foreignKeys", target: source, body: { table: table.name } })).toEqual({ rows: [] });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: `INSERT INTO org_user_documents
                      (id, organization_id, owner_id, reviewer_id, body) VALUES (?, ?, ?, ?, ?)`,
                params: ["document-1", organizationId, "owner-1", "reviewer-1", "private"],
            },
        });
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });

        const bulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 10 },
        })) as { rows: Record<string, unknown>[] };
        expect(bulk.rows).toHaveLength(1);
        await rpc({
            op: "applyBulkBatch",
            target: destination,
            body: { migId, table, range: { lo: vshard, hi: vshard }, rows: bulk.rows },
        });

        await restartRuntime();

        expect(await rpc({ op: "_foreignKeys", target: destination, body: { table: table.name } })).toEqual({
            rows: [],
        });
        expect(await rpc({ op: "_dump", target: destination, body: { table: table.name, orderBy: "id" } })).toEqual({
            rows: [
                {
                    id: "document-1",
                    organization_id: organizationId,
                    owner_id: "owner-1",
                    reviewer_id: "reviewer-1",
                    body: "private",
                },
            ],
        });
    }, 15_000);

    test("provisions a pristine version-zero destination at the exact topology schema", async () => {
        const result = (await rpc({ op: "_freshProvisionProof", target: "fresh-native" })) as {
            before: { activeVersion: number; activeEpoch: number };
            after: { activeVersion: number; activeEpoch: number };
        };
        expect(result.before).toMatchObject({ activeVersion: 0, activeEpoch: 1 });
        expect(result.after).toMatchObject({ activeVersion: 1, activeEpoch: 7 });
        const retry = (await rpc({ op: "_freshProvisionProof", target: "fresh-native" })) as typeof result;
        expect(retry.before).toEqual(retry.after);
        expect(retry.after).toMatchObject({ activeVersion: 1, activeEpoch: 7 });
    }, 30_000);

    test("destination ownership stays closed through provisioning and survives a lost activation response", async () => {
        const destination = "destination-admission";
        const organizationId = "org-destination-admission";
        const vshard = rowVshard(organizationId);
        const identity = {
            migId: "mig_destination_admission",
            rangeLo: vshard,
            rangeHi: vshard,
            tables: MOVABLE_TABLES,
        };
        const request = {
            principalId: "user-destination-admission",
            mutId: "mutation-destination-admission",
            ref: "workerd-reshard.ts#putDedupRecord",
            args: { id: "admitted", orgId: organizationId, value: "after-cutover" },
            placement: { authority: "global", partitionKey: organizationId },
            auth: { userId: "user-destination-admission", role: "member", roles: ["member"], claims: {} },
            recoveryGeneration: 0,
            schemaEpoch: 2,
            domainSchemaEpoch: 1,
        } as const;

        await expect(
            rpc({
                op: "prepareReshardDestOwnership",
                target: destination,
                body: { ...identity, destinationGeneration: 2 },
            })
        ).resolves.toEqual({ prepared: true, serving: false });
        await expect(rpc({ op: "mutate", target: destination, body: request })).resolves.toMatchObject({
            ok: false,
            error: { code: "CDB_STALE_EPOCH" },
        });
        await expect(
            rpc({ op: "mutate", target: destination, body: { ...request, placement: undefined } })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_STALE_EPOCH" } });
        await expect(
            rpc({
                op: "query",
                target: destination,
                body: {
                    ref: "workerd-reshard.ts#missingQuery",
                    args: {},
                    auth: request.auth,
                    recoveryGeneration: 0,
                    schemaEpoch: 2,
                    domainSchemaEpoch: 1,
                },
            })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_STALE_EPOCH" } });
        await rpc({ op: "beginReshardDest", target: destination, body: { ...identity, destinationGeneration: 2 } });
        await expect(rpc({ op: "mutate", target: destination, body: request })).resolves.toMatchObject({
            ok: false,
            error: { code: "CDB_STALE_EPOCH" },
        });

        await expect(
            rpc({
                op: "_activateReshardDestThenLoseResponse",
                target: destination,
                body: { ...identity, destinationGeneration: 2 },
            })
        ).rejects.toThrow("simulated response loss after destination activation");
        if (!mf) throw new Error("miniflare not initialized");
        await mf.unsafeEvictDurableObject("", "TestCdb", { name: destination });
        await expect(
            rpc({
                op: "activateReshardDestServing",
                target: destination,
                body: { ...identity, destinationGeneration: 2 },
            })
        ).resolves.toEqual({ activated: false });
        await expect(rpc({ op: "mutate", target: destination, body: request })).resolves.toMatchObject({
            ok: true,
            result: { id: "admitted", value: "after-cutover" },
        });
        await rpc({ op: "finishReshardDest", target: destination, body: identity });
        await expect(
            rpc({
                op: "mutate",
                target: destination,
                body: {
                    ...request,
                    mutId: "mutation-later-global-epoch",
                    args: { ...request.args, id: "later-global-epoch" },
                    recoveryGeneration: 0,
                    schemaEpoch: 3,
                },
            })
        ).resolves.toMatchObject({ ok: true, result: { id: "later-global-epoch" } });
        await expect(
            rpc({
                op: "mutate",
                target: destination,
                body: { ...request, mutId: "mutation-stale-destination", recoveryGeneration: 0, schemaEpoch: 1 },
            })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_STALE_EPOCH" } });
    }, 30_000);

    test("phase-zero abort tombstones reject delayed destination provisioning and both delayed begins", async () => {
        const source = "cancel-before-source-begin";
        const destination = "cancel-before-destination-begin";
        const migId = "mig_cancel_before_begin";
        const organizationId = "org-cancel-before-begin";
        const vshard = rowVshard(organizationId);
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };

        await rpc({ op: "abortReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDestAbort", target: destination, body: identity });
        await expect(
            rpc({
                op: "beginReshardDestAbort",
                target: destination,
                body: { ...identity, destinationGeneration: 3 },
            })
        ).rejects.toThrow("does not match prepared ownership");
        await expect(
            rpc({
                op: "beginReshardDestAbort",
                target: destination,
                body: { ...identity, rangeHi: vshard + 1, destinationGeneration: 2 },
            })
        ).rejects.toThrow("does not match prepared ownership");
        await expect(
            rpc({
                op: "abortReshardDestBatch",
                target: destination,
                body: { ...identity, batchSize: 1 },
            })
        ).resolves.toMatchObject({ deleted: 0, done: true });
        await rpc({ op: "abortReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDestAbort", target: destination, body: identity });
        await expect(
            rpc({
                op: "abortReshardDestBatch",
                target: destination,
                body: { ...identity, batchSize: 1 },
            })
        ).resolves.toMatchObject({ deleted: 0, done: true });

        const destinationSchema = (await rpc({ op: "schemaState", target: destination })) as {
            activeVersion: number;
            activeEpoch: number;
            activeDigest: string;
        };
        await expect(
            rpc({
                op: "provisionFreshReshardDestination",
                target: destination,
                body: {
                    migrationId: `reshard-dest:${migId}`,
                    targetVersion: destinationSchema.activeVersion,
                    targetEpoch: destinationSchema.activeEpoch,
                    targetDigest: destinationSchema.activeDigest,
                },
            })
        ).rejects.toThrow("canceled");
        await expect(rpc({ op: "beginReshardSource", target: source, body: identity })).rejects.toThrow(
            "different immutable identity"
        );
        await expect(rpc({ op: "beginReshardDest", target: destination, body: identity })).rejects.toThrow(
            "different immutable identity"
        );

        for (const [target, role] of [
            [source, "source"],
            [destination, "dest"],
        ] as const) {
            const state = (await rpc({
                op: "_dump",
                target,
                body: { table: "_chardb_split_state", orderBy: "mig_id" },
            })) as {
                rows: {
                    role: string;
                    capture: number;
                    drained: number;
                    destination_generation: number | null;
                    destination_serving: number;
                }[];
            };
            expect(state.rows).toHaveLength(1);
            expect(state.rows[0]).toMatchObject({ role, capture: 0, drained: 1 });
            if (role === "dest") {
                expect(state.rows[0]).toMatchObject({ destination_generation: 2, destination_serving: 0 });
            }
        }
    }, 30_000);

    test("abort tombstones validate before writes, enforce exact history capacity, and require destination begin", async () => {
        const malformed = "abort-tombstone-malformed";
        const organizationId = "org-abort-tombstone-validation";
        const vshard = rowVshard(organizationId);
        const identity = {
            migId: "abort_tombstone_valid",
            rangeLo: vshard,
            rangeHi: vshard,
            tables: MOVABLE_TABLES,
        };

        await expect(
            rpc({ op: "abortReshardSource", target: malformed, body: { ...identity, migId: "bad id" } })
        ).rejects.toThrow("migration id is invalid");
        await expect(
            rpc({
                op: "beginReshardDestAbort",
                target: malformed,
                body: { ...identity, rangeHi: VSHARD_COUNT },
            })
        ).rejects.toThrow("virtual-shard range is invalid");
        await expect(
            rpc({ op: "abortReshardDestBatch", target: malformed, body: { ...identity, batchSize: 1 } })
        ).rejects.toThrow("cannot run before its abort fence starts");
        expect(await rpc({ op: "_countRows", target: malformed, body: { table: "_chardb_split_state" } })).toEqual({
            count: 0,
        });

        const capacity = "abort-tombstone-capacity";
        await rpc({
            op: "_exec",
            target: capacity,
            body: {
                sql: `WITH RECURSIVE seq(n) AS (
                        VALUES(0) UNION ALL SELECT n + 1 FROM seq WHERE n + 1 < 128
                      )
                      INSERT INTO _chardb_split_state
                        (mig_id, range_lo, range_hi, role, capture, abort_started, drained, updated_at)
                      SELECT printf('history-%05d', left_side.n * 128 + right_side.n),
                             0, 0, 'source', 0, 1, 1, 1
                      FROM seq AS left_side CROSS JOIN seq AS right_side
                      WHERE left_side.n * 128 + right_side.n < ${VSHARD_COUNT - 1}`,
            },
        });
        expect(await rpc({ op: "_countRows", target: capacity, body: { table: "_chardb_split_state" } })).toEqual({
            count: VSHARD_COUNT - 1,
        });
        await expect(rpc({ op: "abortReshardSource", target: capacity, body: identity })).resolves.toEqual({
            ok: true,
        });
        expect(await rpc({ op: "_countRows", target: capacity, body: { table: "_chardb_split_state" } })).toEqual({
            count: VSHARD_COUNT,
        });
        await expect(rpc({ op: "abortReshardSource", target: capacity, body: identity })).resolves.toEqual({
            ok: true,
        });
        await expect(
            rpc({
                op: "abortReshardSource",
                target: capacity,
                body: { ...identity, migId: "abort_tombstone_overflow" },
            })
        ).rejects.toThrow("durable row limit");
        expect(await rpc({ op: "_countRows", target: capacity, body: { table: "_chardb_split_state" } })).toEqual({
            count: VSHARD_COUNT,
        });
    }, 30_000);

    test("a populated replicated table blocks a split until replication has a transfer protocol", async () => {
        const target = "replicated:populated";
        const organizationId = "org-with-replicated-settings";
        const vshard = rowVshard(organizationId);
        await rpc({
            op: "_exec",
            target,
            body: { sql: "INSERT INTO replicated_settings VALUES ('theme', 'dark')" },
        });
        await expect(
            rpc({
                op: "beginReshardSource",
                target,
                body: {
                    migId: "mig_replicated_blocked",
                    rangeLo: vshard,
                    rangeHi: vshard,
                    tables: [
                        {
                            name: "messages",
                            partitionColumn: "org_id",
                            columns: ["id", "org_id", "body"],
                        },
                    ],
                },
            })
        ).rejects.toThrow("replicated table replicated_settings has no online reshard transfer protocol");
        expect(
            (await rpc({
                op: "_dump",
                target,
                body: { table: "replicated_settings", orderBy: "id" },
            })) as { rows: unknown[] }
        ).toEqual({ rows: [{ id: "theme", value: "dark" }] });
    }, 15_000);

    test("an active source or destination split blocks every schema-migration prepare", async () => {
        const source = "schema-fence-source";
        const destination = "schema-fence-destination";
        const organizationId = "org-schema-fence";
        const vshard = rowVshard(organizationId);
        const migId = "mig_schema_fence";
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        const invalidTarget = {
            migrationId: "schema-during-split",
            activeVersion: 0,
            targetVersion: 1,
            targetEpoch: 2,
            activeDigest: "a".repeat(64),
            targetDigest: "b".repeat(64),
        };
        for (const target of [source, destination]) {
            await expect(rpc({ op: "prepareSchemaMigration", target, body: invalidTarget })).rejects.toThrow(
                `schema migration is blocked by active reshard ${migId}`
            );
        }
        await rpc({ op: "abortReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDestAbort", target: destination, body: identity });
        await rpc({ op: "abortReshardDestBatch", target: destination, body: { ...identity, batchSize: 1 } });
        await expect(rpc({ op: "prepareSchemaMigration", target: source, body: invalidTarget })).rejects.not.toThrow(
            "blocked by active reshard"
        );
    }, 15_000);

    test("orphaned file metadata or organization tombstones block relational-only resharding", async () => {
        const organizationId = "org-orphaned-file-state";
        const vshard = rowVshard(organizationId);
        for (const [suffix, createSql, sql, params] of [
            [
                "metadata",
                "CREATE TABLE _chardb_files (file_id TEXT PRIMARY KEY)",
                "INSERT INTO _chardb_files (file_id) VALUES ('file-orphaned')",
                [],
            ],
            [
                "tombstone",
                "CREATE TABLE _chardb_deleted_organizations (organization_id TEXT PRIMARY KEY, deleted_at INTEGER)",
                "INSERT INTO _chardb_deleted_organizations (organization_id, deleted_at) VALUES (?, 1)",
                [organizationId],
            ],
        ] as const) {
            const source = `orphaned-file-${suffix}`;
            await rpc({ op: "_exec", target: source, body: { sql: createSql } });
            await rpc({ op: "_exec", target: source, body: { sql, params } });
            await expect(
                rpc({
                    op: "beginReshardSource",
                    target: source,
                    body: {
                        migId: `mig_orphaned_file_${suffix}`,
                        rangeLo: vshard,
                        rangeHi: vshard,
                        tables: MOVABLE_TABLES,
                    },
                })
            ).rejects.toThrow("has no online reshard transfer protocol");
        }
    }, 15_000);

    test("a retained pre-split mutation replays its exact response after cutover", async () => {
        const source = "dedup-source";
        const destination = "dedup-destination";
        const organizationId = "org-response-loss";
        const vshard = rowVshard(organizationId);
        const migId = "mig_response_loss";
        const table = {
            name: "dedup_records",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "value"],
        } as const;
        const request = {
            principalId: "user-response-loss",
            mutId: "mutation-response-loss",
            ref: "workerd-reshard.ts#putDedupRecord",
            args: { id: "record-1", orgId: organizationId, value: "committed-on-source" },
            placement: { authority: "global", partitionKey: organizationId },
            auth: { userId: "user-response-loss", role: "member", roles: ["member"], claims: {} },
            recoveryGeneration: 0,
            schemaEpoch: 1,
            domainSchemaEpoch: 1,
        } as const;

        await expect(rpc({ op: "_mutateThenLoseResponse", target: source, body: request })).rejects.toThrow(
            "simulated response loss after mutation commit"
        );
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        await rpc({
            op: "beginReshardDest",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });

        const bulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 10 },
        })) as { rows: Record<string, unknown>[]; lastRowid: number; done: boolean };
        expect(bulk.rows).toHaveLength(1);
        await rpc({
            op: "applyBulkBatch",
            target: destination,
            body: { migId, table, range: { lo: vshard, hi: vshard }, rows: bulk.rows },
        });

        const fence = {
            migrationId: migId,
            rangeLo: vshard,
            rangeHi: vshard,
            sourceGeneration: 1,
            destinationGeneration: 2,
        };
        await rpc({ op: "prepareRoutingFence", target: source, body: fence });
        await rpc({ op: "activateRoutingFence", target: source, body: fence });
        await expect(
            rpc({
                op: "mutate",
                target: source,
                body: { ...request, mutId: "unplaced-after-source-fence", placement: undefined },
            })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_STALE_EPOCH" } });
        const oplog = (await rpc({
            op: "readSplitOpLogBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 64 },
        })) as { entries: { lsn: number; oplogRow: Uint8Array }[]; lastLsn: number; done: boolean };
        expect(oplog.entries).toHaveLength(1);
        expect(oplog.done).toBe(true);
        await rpc({
            op: "applySplitOpLogBatch",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, entries: oplog.entries },
        });
        await expect(
            rpc({
                op: "_ackSplitOpLogThenLoseResponse",
                target: source,
                body: { migId, throughLsn: oplog.lastLsn },
            })
        ).rejects.toThrow("simulated response loss after split-oplog acknowledgement");
        await expect(
            rpc({ op: "ackSplitOpLog", target: source, body: { migId, throughLsn: oplog.lastLsn } })
        ).resolves.toEqual({ pruned: 0, prunedBytes: 0, ackedLsn: oplog.lastLsn });
        expect(await rpc({ op: "_splitOpLogState", target: source, body: { migId } })).toMatchObject({
            ackedLsn: oplog.lastLsn,
            retainedRows: 0,
            retainedBytes: 0,
        });
        const afterAck = (await rpc({
            op: "readSplitOpLogBatch",
            target: source,
            body: { migId, afterLsn: oplog.lastLsn, limit: 64 },
        })) as { entries: unknown[]; lastLsn: number; done: boolean };
        expect(afterAck).toEqual({ entries: [], lastLsn: oplog.lastLsn, done: true });
        expect(
            (await rpc({ op: "_dump", target: source, body: { table: "_chardb_op_log", orderBy: "event_id" } })) as {
                rows: unknown[];
            }
        ).toMatchObject({ rows: [expect.objectContaining({ mut_id: request.mutId })] });

        await rpc({
            op: "activateReshardDestServing",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES, destinationGeneration: 2 },
        });

        const replay = (await rpc({
            op: "mutate",
            target: destination,
            body: { ...request, recoveryGeneration: 0, schemaEpoch: 2 },
        })) as { ok: boolean; ran: boolean; result: unknown };
        expect(replay).toMatchObject({
            ok: true,
            ran: false,
            result: { id: "record-1", value: "committed-on-source" },
        });
        const rows = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: "dedup_records", orderBy: "id" },
        })) as { rows: unknown[] };
        expect(rows.rows).toHaveLength(1);

        const collision = (await rpc({
            op: "mutate",
            target: destination,
            body: {
                ...request,
                args: { ...request.args, value: "different-payload" },
                recoveryGeneration: 0,
                schemaEpoch: 2,
            },
        })) as { ok: boolean; error: { code: string } };
        expect(collision).toMatchObject({ ok: false, error: { code: "CDB_MUT_ID_COLLISION" } });

        const nextMigration = "mig_repeated_split";
        await rpc({
            op: "beginReshardSource",
            target: destination,
            body: { migId: nextMigration, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        const retainedAgain = (await rpc({
            op: "readSplitOpLogBatch",
            target: destination,
            body: { migId: nextMigration, afterLsn: 0, limit: 64 },
        })) as { entries: unknown[] };
        expect(retainedAgain.entries).toHaveLength(1);

        const legacySource = "dedup-legacy-source";
        await rpc({
            op: "_exec",
            target: legacySource,
            body: {
                sql: `INSERT INTO _chardb_op_log
                      (principal_id, mut_id, payload_hash, payload_enc, committed_at,
                       schema_epoch, touched_keys, byte_size)
                      VALUES ('legacy-user', 'legacy-mutation', zeroblob(32), x'01', 1, 1, '[]', 1)`,
            },
        });
        await expect(
            rpc({
                op: "beginReshardSource",
                target: legacySource,
                body: { migId: "mig_legacy_blocked", rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
            })
        ).rejects.toThrow("retained mutation replay history predates durable vshard placement");
    }, 30_000);

    test("pre-fence abort resumes bounded child-first cleanup and removes only copied replay state", async () => {
        const source = "abort-native-source";
        const destination = "abort-native-destination";
        const organizationId = "org-native-abort";
        const vshard = rowVshard(organizationId);
        const migId = "mig_native_abort";
        const mutation = {
            principalId: "abort-user",
            mutId: "abort-mutation",
            ref: "workerd-reshard.ts#putDedupRecord",
            args: { id: "abort-record", orgId: organizationId, value: "copied" },
            placement: { authority: "global", partitionKey: organizationId },
            auth: { userId: "abort-user", role: "member", roles: ["member"], claims: {} },
            recoveryGeneration: 0,
            schemaEpoch: 1,
            domainSchemaEpoch: 1,
        } as const;
        await rpc({ op: "mutate", target: source, body: mutation });
        for (const index of [1, 2]) {
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO reshard_parents (id, org_id) VALUES (?, ?)",
                    params: [`parent-${index}`, organizationId],
                },
            });
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO reshard_children (id, org_id, parent_id) VALUES (?, ?, ?)",
                    params: [`child-${index}`, organizationId, `parent-${index}`],
                },
            });
        }
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        let delayedBulk: { table: TableSpec; rows: Record<string, unknown>[] } | undefined;
        for (const table of [MOVABLE_TABLES[0], MOVABLE_TABLES[5], MOVABLE_TABLES[6]]) {
            const batch = (await rpc({
                op: "bulkCopyBatch",
                target: source,
                body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 10 },
            })) as { rows: Record<string, unknown>[] };
            if (!delayedBulk && batch.rows.length > 0) delayedBulk = { table, rows: batch.rows };
            await rpc({
                op: "applyBulkBatch",
                target: destination,
                body: { migId, table, range: { lo: vshard, hi: vshard }, rows: batch.rows },
            });
        }
        const oplog = (await rpc({
            op: "readSplitOpLogBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 64 },
        })) as { entries: unknown[] };
        await rpc({
            op: "applySplitOpLogBatch",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, entries: oplog.entries },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO messages VALUES ('abort-tail', ?, 'delayed')",
                params: [organizationId],
            },
        });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: unknown[] };
        await rpc({
            op: "stageTailBatch",
            target: destination,
            body: {
                migId,
                tables: MOVABLE_TABLES,
                range: { lo: vshard, hi: vshard },
                transactions: tail.transactions,
            },
        });

        await rpc({ op: "abortReshardSource", target: source, body: identity });
        await rpc({ op: "abortReshardSource", target: source, body: identity });
        expect(await rpc({ op: "_splitOpLogState", target: source, body: { migId } })).toBeNull();
        await expect(rpc({ op: "beginReshardDestAbort", target: destination, body: identity })).resolves.toEqual({
            started: true,
        });
        await expect(rpc({ op: "beginReshardDestAbort", target: destination, body: identity })).resolves.toEqual({
            started: false,
        });
        if (!delayedBulk) throw new Error("expected delayed bulk fixture rows");
        await expect(
            rpc({
                op: "applyBulkBatch",
                target: destination,
                body: {
                    migId,
                    table: delayedBulk.table,
                    range: { lo: vshard, hi: vshard },
                    rows: delayedBulk.rows,
                },
            })
        ).rejects.toThrow("no longer active");
        await expect(
            rpc({
                op: "applyTailBatch",
                target: destination,
                body: {
                    migId,
                    tables: MOVABLE_TABLES,
                    range: { lo: vshard, hi: vshard },
                    transactions: tail.transactions,
                },
            })
        ).rejects.toThrow("no longer active");
        await expect(
            rpc({
                op: "applySplitOpLogBatch",
                target: destination,
                body: { migId, rangeLo: vshard, rangeHi: vshard, entries: oplog.entries },
            })
        ).rejects.toThrow("no longer active");
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_parents (id, org_id) VALUES ('after-abort-parent', ?)",
                params: [organizationId],
            },
        });
        expect(
            (await rpc({ op: "_dump", target: source, body: { table: "_chardb_split_log" } })) as {
                rows: unknown[];
            }
        ).toEqual({ rows: [] });

        const first = (await rpc({
            op: "abortReshardDestBatch",
            target: destination,
            body: { ...identity, batchSize: 1 },
        })) as { deleted: number; done: boolean };
        expect(first.done).toBe(false);
        let calls = 1;
        let done = false;
        while (!done && calls < 32) {
            const batch = (await rpc({
                op: "abortReshardDestBatch",
                target: destination,
                body: { ...identity, batchSize: 1 },
            })) as { done: boolean };
            calls++;
            done = batch.done;
        }
        expect(done).toBe(true);
        expect(calls).toBeGreaterThan(1);
        await expect(
            rpc({
                op: "abortReshardDestBatch",
                target: destination,
                body: { ...identity, batchSize: 1 },
            })
        ).resolves.toMatchObject({ deleted: 0, done: true });
        for (const table of ["dedup_records", "reshard_children", "reshard_parents", "_chardb_op_log"]) {
            expect((await rpc({ op: "_dump", target: destination, body: { table } })) as { rows: unknown[] }).toEqual({
                rows: [],
            });
        }
        for (const table of [
            "_chardb_split_log",
            "_chardb_split_oplog",
            "_chardb_split_oplog_applied",
            "_chardb_split_oplog_cursor",
            "_chardb_split_drop_cursor",
            "_chardb_split_tail_inbox",
        ]) {
            expect((await rpc({ op: "_dump", target: destination, body: { table } })) as { rows: unknown[] }).toEqual({
                rows: [],
            });
        }
        const canceledState = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: "_chardb_split_state", orderBy: "mig_id" },
        })) as { rows: { role: string; capture: number; drained: number }[] };
        expect(canceledState.rows).toHaveLength(1);
        expect(canceledState.rows[0]).toMatchObject({ role: "dest", capture: 0, drained: 1 });
        const tombstone = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: "_chardb_split_identity", orderBy: "mig_id" },
        })) as { rows: unknown[] };
        expect(tombstone.rows).toHaveLength(1);
    }, 30_000);

    test("successful destination finalization drains split state and re-enables native op-log retention", async () => {
        const source = "finish-native-source";
        const destination = "finish-native-destination";
        const organizationId = "org-native-finish";
        const vshard = rowVshard(organizationId);
        const migId = "mig_native_finish";
        await rpc({
            op: "mutate",
            target: source,
            body: {
                principalId: "finish-user",
                mutId: "finish-mutation",
                ref: "workerd-reshard.ts#putDedupRecord",
                args: { id: "finish-record", orgId: organizationId, value: "retained" },
                placement: { authority: "global", partitionKey: organizationId },
                auth: { userId: "finish-user", role: "member", roles: ["member"], claims: {} },
                recoveryGeneration: 0,
                schemaEpoch: 1,
                domainSchemaEpoch: 1,
            },
        });
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        const captured = (await rpc({
            op: "readSplitOpLogBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 64 },
        })) as { entries: unknown[] };
        await rpc({
            op: "applySplitOpLogBatch",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, entries: captured.entries },
        });
        await rpc({
            op: "activateReshardDestServing",
            target: destination,
            body: { ...identity, destinationGeneration: 2 },
        });
        await rpc({ op: "finishReshardDest", target: destination, body: identity });
        await rpc({ op: "finishReshardDest", target: destination, body: identity });
        const state = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: "_chardb_split_state", orderBy: "mig_id" },
        })) as { rows: { drained: number }[] };
        expect(state.rows).toHaveLength(1);
        expect(state.rows[0]?.drained).toBe(1);
        expect(
            (await rpc({ op: "_dump", target: destination, body: { table: "_chardb_split_oplog_cursor" } })) as {
                rows: unknown[];
            }
        ).toEqual({ rows: [] });
        await rpc({
            op: "_exec",
            target: destination,
            body: { sql: "UPDATE _chardb_op_log SET committed_at = 0" },
        });
        const retained = (await rpc({
            op: "_maintainOpLog",
            target: destination,
            body: { nowMs: 24 * 60 * 60 * 1_000 + 1 },
        })) as { deleted: number; blockedBySplit: boolean };
        expect(retained).toMatchObject({ deleted: 1, blockedBySplit: false });
    }, 30_000);

    test("end-to-end reshard: bulk copy + tail replay against real DO SqlStorage", async () => {
        const messagesSpec = {
            name: "messages",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "body"],
        } as const;

        const orgInRange = "org-A";
        const orgOutOfRange = "org-Z";

        // Seed both shards' table schemas. We pick a vshard range that covers
        // org-A but not org-Z; rowVshard isn't pure-importable from the worker
        // bundle path so we use a wide [0, 16383] range and rely on the
        // partition-column filter being identity for in-range rows. The
        // out-of-range assertion uses a tighter range below.
        for (const target of ["src", "dst"] as const) {
            await rpc({
                op: "_exec",
                target,
                body: {
                    sql: "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, org_id TEXT, body TEXT)",
                },
            });
        }

        // Pre-trigger seed on source.
        for (let i = 0; i < 25; i++) {
            await rpc({
                op: "_exec",
                target: "src",
                body: {
                    sql: "INSERT INTO messages VALUES (?, ?, ?)",
                    params: [`m-${i}`, orgInRange, `body-${i}`],
                },
            });
        }
        await rpc({
            op: "_exec",
            target: "src",
            body: {
                sql: "INSERT INTO messages VALUES (?, ?, ?)",
                params: ["m-out", orgOutOfRange, "out-of-range"],
            },
        });

        const migId = "mig_workerd_1";
        // Wide range — every vshard maps inside. The reshard layer's per-row
        // `inRange` then becomes a tautology, which is exactly what we want
        // for the bulk-copy half of the assertion.
        const range = { rangeLo: 0, rangeHi: 16383 };

        await rpc({
            op: "beginReshardSource",
            target: "src",
            body: { migId, ...range, tables: MOVABLE_TABLES },
        });
        await rpc({
            op: "beginReshardDest",
            target: "dst",
            body: { migId, ...range, tables: MOVABLE_TABLES },
        });

        // Post-trigger writes go into the split log.
        for (let i = 25; i < 35; i++) {
            await rpc({
                op: "_exec",
                target: "src",
                body: {
                    sql: "INSERT INTO messages VALUES (?, ?, ?)",
                    params: [`m-${i}`, orgInRange, `body-${i}`],
                },
            });
        }
        // An update to a pre-trigger row also lands in the split log.
        await rpc({
            op: "_exec",
            target: "src",
            body: {
                sql: "UPDATE messages SET body = ? WHERE id = ?",
                params: ["body-0-edited", "m-0"],
            },
        });

        // Bulk copy in pages.
        let after = 0;
        let copied = 0;
        while (true) {
            const batch = (await rpc({
                op: "bulkCopyBatch",
                target: "src",
                body: {
                    migId,
                    table: messagesSpec,
                    range: { lo: range.rangeLo, hi: range.rangeHi },
                    afterRowid: after,
                    limit: 10,
                },
            })) as { rows: Record<string, unknown>[]; lastRowid: number; done: boolean };
            if (batch.rows.length > 0) {
                const apply = (await rpc({
                    op: "applyBulkBatch",
                    target: "dst",
                    body: {
                        migId,
                        table: messagesSpec,
                        range: { lo: range.rangeLo, hi: range.rangeHi },
                        rows: batch.rows,
                    },
                })) as { applied: number; skipped: number };
                copied += apply.applied;
            }
            after = batch.lastRowid;
            if (batch.done) break;
        }
        expect(copied).toBeGreaterThan(0);

        // Tail replay — drains the split log produced by the post-trigger
        // writes, including the UPDATE on m-0.
        let lsn = 0;
        while (true) {
            const tail = (await rpc({
                op: "readTailBatch",
                target: "src",
                body: { migId, afterLsn: lsn, limit: 500 },
            })) as { transactions: unknown[]; lastLsn: number; done: boolean };
            if (tail.transactions.length > 0) {
                await rpc({
                    op: "applyTailBatch",
                    target: "dst",
                    body: {
                        migId,
                        tables: MOVABLE_TABLES,
                        range: { lo: range.rangeLo, hi: range.rangeHi },
                        transactions: tail.transactions,
                    },
                });
            }
            lsn = tail.lastLsn;
            if (tail.done) break;
        }

        // Assert dest converged on the source's view (m-0 carries the edit).
        const srcDump = (await rpc({
            op: "_dump",
            target: "src",
            body: { table: "messages", orderBy: "id" },
        })) as {
            rows: { id: string; body: string }[];
        };
        const dstDump = (await rpc({
            op: "_dump",
            target: "dst",
            body: { table: "messages", orderBy: "id" },
        })) as {
            rows: { id: string; body: string }[];
        };
        expect(dstDump.rows.length).toBe(srcDump.rows.length);
        const m0Dst = dstDump.rows.find(r => r.id === "m-0");
        expect(m0Dst?.body).toBe("body-0-edited");
        // Tail-only inserts also landed.
        expect(dstDump.rows.find(r => r.id === "m-30")).toBeDefined();

        await expect(
            rpc({ op: "_ackThenLoseResponse", target: "src", body: { migId, throughLsn: lsn } })
        ).rejects.toThrow("simulated response loss after tail acknowledgement");
        await expect(rpc({ op: "ackTail", target: "src", body: { migId, throughLsn: lsn } })).resolves.toEqual({
            pruned: 0,
            ackedLsn: lsn,
        });
        expect(await rpc({ op: "_splitState", target: "src", body: { migId } })).toMatchObject({
            rows: 0,
            bytes: 0,
            ackedLsn: lsn,
        });
        const prunedTail = (await rpc({
            op: "_dump",
            target: "src",
            body: { table: "_chardb_split_log", orderBy: "lsn" },
        })) as { rows: unknown[] };
        expect(prunedTail.rows).toEqual([]);

        // A delete tail entry must remove only its exact primary-key row. The
        // partition column is shared by every row in this fixture, so deleting
        // by org_id would silently wipe the whole migrated organization.
        await rpc({
            op: "_exec",
            target: "src",
            body: { sql: "DELETE FROM messages WHERE id = ?", params: ["m-1"] },
        });
        const deletion = (await rpc({
            op: "readTailBatch",
            target: "src",
            body: { migId, afterLsn: lsn, limit: 500 },
        })) as { transactions: unknown[]; lastLsn: number; done: boolean };
        expect(deletion.transactions).toHaveLength(1);
        await rpc({
            op: "applyTailBatch",
            target: "dst",
            body: {
                migId,
                tables: MOVABLE_TABLES,
                range: { lo: range.rangeLo, hi: range.rangeHi },
                transactions: deletion.transactions,
            },
        });
        const afterDelete = (await rpc({
            op: "_dump",
            target: "dst",
            body: { table: "messages", orderBy: "id" },
        })) as { rows: { id: string }[] };
        expect(afterDelete.rows.find(row => row.id === "m-1")).toBeUndefined();
        expect(afterDelete.rows.find(row => row.id === "m-2")).toBeDefined();
        expect(afterDelete.rows).toHaveLength(dstDump.rows.length - 1);
    }, 30_000);

    test("applyBulkBatch defensively filters out-of-range rows so a misrouted batch can't pollute dest", async () => {
        const messagesSpec = {
            name: "messages_iso",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "body"],
        } as const;
        await rpc({
            op: "_exec",
            target: "iso-dst",
            body: { sql: "CREATE TABLE IF NOT EXISTS messages_iso (id TEXT PRIMARY KEY, org_id TEXT, body TEXT)" },
        });
        await rpc({
            op: "beginReshardDest",
            target: "iso-dst",
            body: { migId: "mig_iso", rangeLo: 0, rangeHi: 0, tables: MOVABLE_TABLES },
        });
        // Range [0,0] only — only org_ids whose vshard hashes to 0 land. We
        // pass a batch with mixed orgs and assert applied < total.
        const result = (await rpc({
            op: "applyBulkBatch",
            target: "iso-dst",
            body: {
                migId: "mig_iso",
                table: messagesSpec,
                range: { lo: 0, hi: 0 },
                rows: [
                    { id: "row-1", org_id: "needle", body: "x" },
                    { id: "row-2", org_id: "haystack", body: "y" },
                    { id: "row-3", org_id: "anvil", body: "z" },
                ],
            },
        })) as { applied: number; skipped: number };
        expect(result.applied + result.skipped).toBe(3);
        // At minimum, we expect not every row to land — the [0,0] vshard slot
        // is 1/16384 wide, so the prior probability of all 3 hashing into it
        // is essentially zero.
        expect(result.skipped).toBeGreaterThan(0);
    }, 15_000);

    test("rejects nonpositive source rowids before starting a split", async () => {
        for (const rowid of [-1, 0]) {
            const source = `nonpositive-rowid-${rowid}`;
            const organizationId = "org-nonpositive-rowid";
            const vshard = rowVshard(organizationId);
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO messages (rowid, id, org_id, body) VALUES (?, 'nonpositive', ?, 'preserved'), (1, 'positive', ?, 'preserved')",
                    params: [rowid, organizationId, organizationId],
                },
            });
            await expect(
                rpc({
                    op: "beginReshardSource",
                    target: source,
                    body: {
                        migId: `mig_nonpositive_${rowid + 1}`,
                        rangeLo: vshard,
                        rangeHi: vshard,
                        tables: MOVABLE_TABLES,
                    },
                })
            ).rejects.toThrow("reshard source table messages has nonpositive rowids");
            for (const table of ["_chardb_split_state", "_chardb_split_identity", "_chardb_split_bulk_watermark"]) {
                expect(await rpc({ op: "_countRows", target: source, body: { table } })).toEqual({ count: 0 });
            }
            expect(await rpc({ op: "_countRows", target: source, body: { table: "messages" } })).toEqual({ count: 2 });
        }
    }, 15_000);

    test("bulk copy keeps its begin-time rowid watermark despite later out-of-range inserts", async () => {
        const source = "bulk-watermark-source";
        const organizationId = "org-bulk-watermark";
        let outside = "org-bulk-watermark-outside";
        const vshard = rowVshard(organizationId);
        while (rowVshard(outside) === vshard) outside += "x";
        const table = MOVABLE_TABLES.find(item => item.name === "messages") as TableSpec;
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO messages VALUES (?, ?, ?)", params: ["snapshot", organizationId, "before"] },
        });
        const migId = "mig_bulk_watermark";
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: `WITH RECURSIVE n(i) AS (
                        VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 600
                      ) INSERT INTO messages SELECT 'outside-' || i, ?, 'later' FROM n`,
                params: [outside],
                placementVshard: rowVshard(outside),
            },
        });
        await rpc({ op: "beginReshardSource", target: source, body: identity });

        const bulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 100 },
        })) as { rows: Record<string, unknown>[]; done: boolean; lastRowid: number };
        expect(bulk).toMatchObject({ rows: [{ id: "snapshot", org_id: organizationId, body: "before" }], done: true });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: unknown[] };
        expect(tail.transactions).toEqual([]);
    }, 15_000);

    test("closing destination bulk permanently rejects a delayed stale batch after tail apply", async () => {
        const source = "bulk-close-source";
        const destination = "bulk-close-destination";
        const organizationId = "org-bulk-close";
        const vshard = rowVshard(organizationId);
        const migId = "mig_bulk_close";
        const table = MOVABLE_TABLES.find(item => item.name === "messages") as TableSpec;
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO messages VALUES (?, ?, ?)", params: ["row", organizationId, "bulk-v1"] },
        });
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        const range = { lo: vshard, hi: vshard };
        const bulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range, afterRowid: 0, limit: 500 },
        })) as { rows: Record<string, unknown>[] };
        const staleBulk = { migId, table, range, rows: bulk.rows };
        await rpc({ op: "applyBulkBatch", target: destination, body: staleBulk });
        await rpc({ op: "closeReshardBulkDest", target: destination, body: identity });
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "UPDATE messages SET body = ? WHERE id = ?", params: ["tail-v2", "row"] },
        });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: unknown[] };
        await rpc({
            op: "applyTailBatch",
            target: destination,
            body: { migId, tables: MOVABLE_TABLES, range, transactions: tail.transactions },
        });

        await expect(rpc({ op: "applyBulkBatch", target: destination, body: staleBulk })).rejects.toThrow(
            "destination bulk copy is already closed"
        );
        const rows = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: table.name, orderBy: "id" },
        })) as { rows: Record<string, unknown>[] };
        expect(rows.rows).toEqual([{ id: "row", org_id: organizationId, body: "tail-v2" }]);
    }, 15_000);

    test("stages tail durably through response loss and waits for a later parent bulk page", async () => {
        const source = "tail-inbox-source";
        const destination = "tail-inbox-destination";
        const organizationId = "org-tail-inbox";
        const vshard = rowVshard(organizationId);
        const migId = "mig_tail_inbox";
        const parentTable = MOVABLE_TABLES.find(item => item.name === "reshard_parents") as TableSpec;
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        const range = { lo: vshard, hi: vshard };
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_parents (id, org_id) VALUES ('inbox-parent', ?)",
                params: [organizationId],
            },
        });
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_children (id, org_id, parent_id) VALUES ('inbox-child', ?, 'inbox-parent')",
                params: [organizationId],
            },
        });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: unknown[]; lastLsn: number };
        const stageBody = { migId, tables: MOVABLE_TABLES, range, transactions: tail.transactions };
        await expect(rpc({ op: "_stageTailThenLoseResponse", target: destination, body: stageBody })).rejects.toThrow(
            "injected lost staged-tail response"
        );
        await expect(rpc({ op: "stageTailBatch", target: destination, body: stageBody })).resolves.toMatchObject({
            staged: 0,
            lastLsn: tail.lastLsn,
        });
        await rpc({ op: "ackTail", target: source, body: { migId, throughLsn: tail.lastLsn } });
        expect(await rpc({ op: "_splitState", target: source, body: { migId } })).toMatchObject({ rows: 0 });

        const staged = (await rpc({
            op: "readStagedTailBatch",
            target: destination,
            body: { migId, limit: 500 },
        })) as { transactions: unknown[] };
        await expect(
            rpc({
                op: "applyTailBatch",
                target: destination,
                body: { migId, tables: MOVABLE_TABLES, range, transactions: staged.transactions },
            })
        ).rejects.toThrow("FOREIGN KEY constraint failed");

        const parentBulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table: parentTable, range, afterRowid: 0, limit: 500 },
        })) as { rows: Record<string, unknown>[] };
        await rpc({
            op: "applyBulkBatch",
            target: destination,
            body: { migId, table: parentTable, range, rows: parentBulk.rows },
        });
        await rpc({ op: "closeReshardBulkDest", target: destination, body: identity });
        const applied = (await rpc({
            op: "applyTailBatch",
            target: destination,
            body: { migId, tables: MOVABLE_TABLES, range, transactions: staged.transactions },
        })) as { lastLsn: number };
        await rpc({ op: "ackStagedTail", target: destination, body: { migId, throughLsn: applied.lastLsn } });
        await expect(rpc({ op: "stageTailBatch", target: destination, body: stageBody })).resolves.toMatchObject({
            staged: 0,
            lastLsn: tail.lastLsn,
        });
        await expect(rpc({ op: "closeTailStaging", target: destination, body: identity })).resolves.toEqual({
            closed: true,
        });
        expect(
            (await rpc({ op: "_dump", target: destination, body: { table: "_chardb_split_tail_inbox" } })) as {
                rows: unknown[];
            }
        ).toEqual({ rows: [] });
        expect(
            (await rpc({ op: "_dump", target: destination, body: { table: "reshard_children", orderBy: "id" } })) as {
                rows: unknown[];
            }
        ).toEqual({ rows: [{ id: "inbox-child", org_id: organizationId, parent_id: "inbox-parent" }] });
    }, 20_000);

    test("partition-key updates cross an exact range boundary without leaving stale rows", async () => {
        const source = "moves-src";
        const destinationShard = "moves-dst";
        const table = {
            name: "partition_moves",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "body"],
        } as const;
        const inside = "org-move-inside";
        let outside = "org-move-outside";
        const insideVshard = rowVshard(inside);
        while (rowVshard(outside) === insideVshard) outside += "x";

        for (const target of [source, destinationShard]) {
            await rpc({
                op: "_exec",
                target,
                body: {
                    sql: "CREATE TABLE IF NOT EXISTS partition_moves (id TEXT PRIMARY KEY, org_id TEXT, body TEXT)",
                },
            });
        }
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO partition_moves VALUES (?, ?, ?), (?, ?, ?)",
                params: ["move-out", inside, "old-in", "move-in", outside, "old-out"],
            },
        });

        const migId = "mig_partition_moves";
        const range = { lo: insideVshard, hi: insideVshard };
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: range.lo, rangeHi: range.hi, tables: MOVABLE_TABLES },
        });
        await rpc({
            op: "beginReshardDest",
            target: destinationShard,
            body: { migId, rangeLo: range.lo, rangeHi: range.hi, tables: MOVABLE_TABLES },
        });
        const bulk = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range, afterRowid: 0, limit: 10 },
        })) as { rows: Record<string, unknown>[] };
        await rpc({ op: "applyBulkBatch", target: destinationShard, body: { migId, table, range, rows: bulk.rows } });

        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "UPDATE partition_moves SET org_id = ?, body = ? WHERE id = ?",
                params: [outside, "new-out", "move-out"],
            },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "UPDATE partition_moves SET org_id = ?, body = ? WHERE id = ?",
                params: [inside, "new-in", "move-in"],
            },
        });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: { entries: unknown[] }[] };
        expect(tail.transactions.flatMap(transaction => transaction.entries)).toHaveLength(4);
        await rpc({
            op: "applyTailBatch",
            target: destinationShard,
            body: { migId, tables: MOVABLE_TABLES, range, transactions: tail.transactions },
        });

        const destination = (await rpc({
            op: "_dump",
            target: destinationShard,
            body: { table: table.name, orderBy: "id" },
        })) as { rows: { id: string; org_id: string; body: string }[] };
        expect(destination.rows).toEqual([{ id: "move-in", org_id: inside, body: "new-in" }]);
    }, 15_000);

    test("a same-partition composite primary-key update removes the bulk key and replays idempotently", async () => {
        const source = "composite-pk-source";
        const destination = "composite-pk-destination";
        const organizationId = "org-composite-pk";
        const vshard = rowVshard(organizationId);
        const range = { lo: vshard, hi: vshard };
        const parent = MOVABLE_TABLES.find(table => table.name === "reshard_parents") as TableSpec;
        const child = MOVABLE_TABLES.find(table => table.name === "composite_moves") as TableSpec;
        const createComposite = `CREATE TABLE composite_moves (
            id TEXT NOT NULL,
            revision TEXT NOT NULL,
            org_id TEXT NOT NULL,
            parent_id TEXT NOT NULL REFERENCES reshard_parents(id) ON UPDATE CASCADE ON DELETE RESTRICT,
            body TEXT NOT NULL,
            PRIMARY KEY (id, revision)
        )`;

        for (const target of [source, destination]) {
            await rpc({ op: "_exec", target, body: { sql: "DROP TABLE composite_moves" } });
            await rpc({ op: "_exec", target, body: { sql: createComposite } });
        }
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO reshard_parents VALUES (?, ?)", params: ["parent-composite", organizationId] },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO composite_moves VALUES (?, ?, ?, ?, ?)",
                params: ["child", "v1", organizationId, "parent-composite", "before"],
            },
        });

        const migId = "mig_composite_pk";
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });
        await rpc({ op: "beginReshardDest", target: destination, body: identity });
        for (const table of [parent, child]) {
            const bulk = (await rpc({
                op: "bulkCopyBatch",
                target: source,
                body: { migId, table, range, afterRowid: 0, limit: 10 },
            })) as { rows: Record<string, unknown>[] };
            await rpc({ op: "applyBulkBatch", target: destination, body: { migId, table, range, rows: bulk.rows } });
        }

        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "UPDATE composite_moves SET revision = ?, body = ? WHERE id = ? AND revision = ?",
                params: ["v2", "after", "child", "v1"],
            },
        });
        const tail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: { entries: unknown[] }[]; lastLsn: number };
        expect(tail.transactions.flatMap(transaction => transaction.entries)).toHaveLength(1);
        const applyBody = { migId, tables: MOVABLE_TABLES, range, transactions: tail.transactions };
        await expect(rpc({ op: "applyTailBatch", target: destination, body: applyBody })).resolves.toMatchObject({
            applied: 1,
        });
        await expect(rpc({ op: "applyTailBatch", target: destination, body: applyBody })).resolves.toMatchObject({
            applied: 0,
        });

        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "UPDATE reshard_parents SET id = ? WHERE id = ?",
                params: ["parent-moved", "parent-composite"],
            },
        });
        const cascadeTail = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: tail.lastLsn, limit: 500 },
        })) as { transactions: { entries: { table_name: string }[] }[] };
        expect(
            cascadeTail.transactions.flatMap(transaction => transaction.entries).map(entry => entry.table_name)
        ).toEqual(["composite_moves", "reshard_parents"]);
        const firstCascadeLsn = cascadeTail.transactions[0]?.entries[0] as { lsn: number } | undefined;
        if (!firstCascadeLsn) throw new Error("cascade transaction is missing its first entry");
        await expect(
            rpc({ op: "ackTail", target: source, body: { migId, throughLsn: firstCascadeLsn.lsn } })
        ).rejects.toThrow("acknowledgement splits a source transaction");
        await expect(
            rpc({
                op: "applyTailBatch",
                target: destination,
                body: { migId, tables: MOVABLE_TABLES, range, transactions: cascadeTail.transactions },
            })
        ).resolves.toMatchObject({ applied: 2 });

        const rows = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: child.name, orderBy: "revision" },
        })) as { rows: Record<string, unknown>[] };
        expect(rows.rows).toEqual([
            {
                id: "child",
                revision: "v2",
                org_id: organizationId,
                parent_id: "parent-moved",
                body: "after",
            },
        ]);
        const parents = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: parent.name, orderBy: "id" },
        })) as { rows: Record<string, unknown>[] };
        expect(parents.rows).toEqual([{ id: "parent-moved", org_id: organizationId }]);
        await expect(rpc({ op: "_foreignKeyCheck", target: destination })).resolves.toEqual({ rows: [] });
    }, 15_000);

    test("bulk copy derives parent-before-child order when the requested tables are child-first", async () => {
        const source = "fk-source";
        const destination = "fk-destination";
        const organizationId = "org-fk-order";
        const vshard = rowVshard(organizationId);
        const parent = {
            name: "reshard_parents",
            partitionColumn: "org_id",
            columns: ["id", "org_id"],
        } as const;
        const child = {
            name: "reshard_children",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "parent_id"],
        } as const;
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO reshard_parents VALUES (?, ?)", params: ["parent-1", organizationId] },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_children VALUES (?, ?, ?)",
                params: ["child-1", organizationId, "parent-1"],
            },
        });
        const migId = "mig_fk_order";
        const childFirst = [
            child,
            ...MOVABLE_TABLES.filter(table => table.name !== child.name && table.name !== parent.name),
            parent,
        ];
        await rpc({
            op: "beginReshardDest",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: childFirst },
        });
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: childFirst },
        });
        const order = (await rpc({
            op: "reshardTableOrder",
            target: destination,
            body: { migId, role: "dest", range: { lo: vshard, hi: vshard }, tables: childFirst },
        })) as { tableNames: string[] };
        expect(order.tableNames.indexOf(parent.name)).toBeLessThan(order.tableNames.indexOf(child.name));

        const byName = new Map<string, TableSpec>(MOVABLE_TABLES.map(table => [table.name, table]));
        for (const tableName of order.tableNames) {
            const table = byName.get(tableName);
            if (!table) throw new Error(`unknown table order entry ${tableName}`);
            const batch = (await rpc({
                op: "bulkCopyBatch",
                target: source,
                body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 10 },
            })) as { rows: Record<string, unknown>[] };
            if (batch.rows.length > 0) {
                await rpc({
                    op: "applyBulkBatch",
                    target: destination,
                    body: { migId, table, range: { lo: vshard, hi: vshard }, rows: batch.rows },
                });
            }
        }
        const children = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: child.name, orderBy: "id" },
        })) as { rows: unknown[] };
        expect(children.rows).toEqual([{ id: "child-1", org_id: organizationId, parent_id: "parent-1" }]);
    }, 15_000);

    test("FK colocation is proven during bulk and guarded for writes after begin", async () => {
        const source = "fk-colocation-source";
        const parentOrganization = "org-fk-parent";
        const childOrganization = "org-fk-child";
        const vshard = rowVshard(parentOrganization);
        const migId = "mig_fk_colocation";
        const child = MOVABLE_TABLES.find(table => table.name === "reshard_children") as TableSpec;
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO reshard_parents VALUES (?, ?)", params: ["parent-cross", parentOrganization] },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_children VALUES (?, ?, ?)",
                params: ["child-before-begin", childOrganization, "parent-cross"],
            },
        });
        const identity = { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES };
        await rpc({ op: "beginReshardSource", target: source, body: identity });

        await expect(
            rpc({
                op: "bulkCopyBatch",
                target: source,
                body: { migId, table: child, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 10 },
            })
        ).rejects.toThrow("FK reshard_children -> reshard_parents crosses partitions");
        await expect(
            rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO reshard_children VALUES (?, ?, ?)",
                    params: ["child-after-begin", childOrganization, "parent-cross"],
                    placementVshard: rowVshard(childOrganization),
                },
            })
        ).rejects.toThrow("FK reshard_children -> reshard_parents crosses partitions");
        await expect(
            rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "UPDATE reshard_parents SET org_id = ? WHERE id = ?",
                    params: [childOrganization, "parent-cross"],
                },
            })
        ).rejects.toThrow("referenced row partition is frozen during split");
        const children = (await rpc({
            op: "_dump",
            target: source,
            body: { table: "reshard_children", orderBy: "id" },
        })) as { rows: { id: string }[] };
        expect(children.rows.map(row => row.id)).toEqual(["child-before-begin"]);
    }, 15_000);

    test("source begin rejects insert, update, and delete application-trigger side effects", async () => {
        const organizationId = "org-trigger-gate";
        const vshard = rowVshard(organizationId);
        for (const operation of ["INSERT", "UPDATE", "DELETE"] as const) {
            const target = `application-trigger-${operation.toLowerCase()}`;
            const migId = `mig_trigger_${operation.toLowerCase()}`;
            await rpc({
                op: "_exec",
                target,
                body: { sql: "CREATE TABLE trigger_effects (kind TEXT NOT NULL)" },
            });
            await rpc({
                op: "_exec",
                target,
                body: {
                    sql: `CREATE TRIGGER application_${operation.toLowerCase()} AFTER ${operation} ON messages
                          BEGIN INSERT INTO trigger_effects VALUES ('${operation.toLowerCase()}'); END`,
                },
            });
            await expect(
                rpc({
                    op: "beginReshardSource",
                    target,
                    body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
                })
            ).rejects.toThrow(`application_${operation.toLowerCase()}`);
        }
    }, 15_000);

    test("a cross-table tail batch rolls back every row when a later table is unknown", async () => {
        const destination = "tail-atomic-destination";
        const organizationId = "org-tail-atomic";
        const vshard = rowVshard(organizationId);
        const migId = "mig_tail_atomic";
        await rpc({
            op: "beginReshardDest",
            target: destination,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        await expect(
            rpc({
                op: "applyTailBatch",
                target: destination,
                body: {
                    migId,
                    tables: MOVABLE_TABLES,
                    range: { lo: vshard, hi: vshard },
                    transactions: [
                        {
                            sourceTxId: 1,
                            firstLsn: 1,
                            lastLsn: 2,
                            entries: [
                                {
                                    source_tx_id: 1,
                                    lsn: 1,
                                    op: "ins",
                                    table_name: "reshard_parents",
                                    pk: organizationId,
                                    before: null,
                                    after: JSON.stringify({ id: "rolled-back", org_id: organizationId }),
                                },
                                {
                                    source_tx_id: 1,
                                    lsn: 2,
                                    op: "ins",
                                    table_name: "unknown_table",
                                    pk: organizationId,
                                    before: null,
                                    after: JSON.stringify({ id: "bad", org_id: organizationId }),
                                },
                            ],
                        },
                    ],
                },
            })
        ).rejects.toThrow("unknown table unknown_table");
        const rows = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: "reshard_parents", orderBy: "id" },
        })) as { rows: unknown[] };
        expect(rows.rows).toEqual([]);
    }, 15_000);

    test("same primary key in another partition cannot take over a destination row", async () => {
        const destination = "pk-collision-destination";
        const migId = "mig_pk_collision";
        await rpc({
            op: "beginReshardDest",
            target: destination,
            body: { migId, rangeLo: 0, rangeHi: 16_383, tables: MOVABLE_TABLES },
        });
        await rpc({
            op: "_exec",
            target: destination,
            body: {
                sql: "INSERT INTO partition_moves VALUES (?, ?, ?)",
                params: ["same-id", "org-existing", "old"],
            },
        });
        const table = MOVABLE_TABLES.find(item => item.name === "partition_moves");
        if (!table) throw new Error("partition_moves spec missing");
        await expect(
            rpc({
                op: "applyBulkBatch",
                target: destination,
                body: {
                    migId,
                    table,
                    range: { lo: 0, hi: 16_383 },
                    rows: [{ id: "same-id", org_id: "org-incoming", body: "new" }],
                },
            })
        ).rejects.toThrow("primary-key collision crosses partitions");
        const rows = (await rpc({
            op: "_dump",
            target: destination,
            body: { table: table.name, orderBy: "id" },
        })) as { rows: unknown[] };
        expect(rows.rows).toEqual([{ id: "same-id", org_id: "org-existing", body: "old" }]);
    }, 15_000);

    test("source drain reverses the FK order and deletes children before parents", async () => {
        const source = "fk-drain-source";
        const organizationId = "org-fk-drain";
        const vshard = rowVshard(organizationId);
        const migId = "mig_fk_drain";
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO reshard_parents VALUES (?, ?)", params: ["parent-drain", organizationId] },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "INSERT INTO reshard_children VALUES (?, ?, ?)",
                params: ["child-drain", organizationId, "parent-drain"],
            },
        });
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        const order = (await rpc({
            op: "reshardTableOrder",
            target: source,
            body: { migId, role: "source", range: { lo: vshard, hi: vshard }, tables: MOVABLE_TABLES },
        })) as { tableNames: string[] };
        await rpc({
            op: "stopReshardCapture",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        await expect(
            rpc({
                op: "beginReshardSource",
                target: source,
                body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
            })
        ).resolves.toEqual({ enabled: false, triggersInstalled: 0 });
        await expect(
            rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO reshard_children VALUES (?, ?, ?)",
                    params: ["child-cross-during-drain", "org-outside-drain", "parent-drain"],
                },
            })
        ).rejects.toThrow("FK reshard_children -> reshard_parents crosses partitions");
        const byName = new Map<string, TableSpec>(MOVABLE_TABLES.map(table => [table.name, table]));
        for (const tableName of [...order.tableNames].reverse()) {
            const table = byName.get(tableName);
            if (!table) throw new Error(`unknown table order entry ${tableName}`);
            let done = false;
            while (!done) {
                const result = (await rpc({
                    op: "dropMigratedRange",
                    target: source,
                    body: { migId, table, range: { lo: vshard, hi: vshard }, batchSize: 10 },
                })) as { done: boolean };
                done = result.done;
            }
        }
        const parents = (await rpc({
            op: "_dump",
            target: source,
            body: { table: "reshard_parents", orderBy: "id" },
        })) as { rows: unknown[] };
        const children = (await rpc({
            op: "_dump",
            target: source,
            body: { table: "reshard_children", orderBy: "id" },
        })) as { rows: unknown[] };
        expect(parents.rows).toEqual([]);
        expect(children.rows).toEqual([]);
    }, 15_000);

    test("bulk and tail RPC byte budgets stop at a resumable cursor and reject one oversized row", async () => {
        const source = "byte-budget-source";
        const organizationId = "org-byte-budget";
        const vshard = rowVshard(organizationId);
        const migId = "mig_byte_budget";
        const table = MOVABLE_TABLES.find(item => item.name === "messages");
        if (!table) throw new Error("messages spec missing");
        const body = "x".repeat(120 * 1_024);
        for (let index = 0; index < 9; index++) {
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO messages VALUES (?, ?, ?)",
                    params: [`bulk-budget-${index}`, organizationId, body],
                },
            });
        }
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        for (let index = 0; index < 9; index++) {
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO messages VALUES (?, ?, ?)",
                    params: [`tail-budget-${index}`, organizationId, body],
                },
            });
        }

        const bulk1 = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: 0, limit: 500 },
        })) as { rows: unknown[]; lastRowid: number; done: boolean };
        expect(bulk1.rows).toHaveLength(8);
        expect(bulk1.done).toBe(false);
        const bulk2 = (await rpc({
            op: "bulkCopyBatch",
            target: source,
            body: { migId, table, range: { lo: vshard, hi: vshard }, afterRowid: bulk1.lastRowid, limit: 500 },
        })) as { rows: unknown[]; lastRowid: number; done: boolean };
        expect(bulk2.rows).toHaveLength(1);
        expect(bulk2.done).toBe(true);

        await expect(
            rpc({
                op: "readTailBatch",
                target: source,
                body: { migId, afterLsn: 0, limit: 10 },
            })
        ).rejects.toThrow("tail protocol limit must be exactly 500");

        const tail1 = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: 0, limit: 500 },
        })) as { transactions: { entries: unknown[] }[]; lastLsn: number; done: boolean };
        expect(tail1.transactions.flatMap(transaction => transaction.entries)).toHaveLength(8);
        expect(tail1.done).toBe(false);
        const tail2 = (await rpc({
            op: "readTailBatch",
            target: source,
            body: { migId, afterLsn: tail1.lastLsn, limit: 500 },
        })) as { transactions: { entries: unknown[] }[]; lastLsn: number; done: boolean };
        expect(tail2.transactions.flatMap(transaction => transaction.entries)).toHaveLength(1);
        expect(tail2.done).toBe(true);

        const oversizedSource = "oversized-row-source";
        const oversizedMigration = "mig_oversized_row";
        await rpc({
            op: "beginReshardSource",
            target: oversizedSource,
            body: { migId: oversizedMigration, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        await expect(
            rpc({
                op: "_exec",
                target: oversizedSource,
                body: {
                    sql: "INSERT INTO messages VALUES (?, ?, ?)",
                    params: ["oversized", organizationId, "x".repeat(CDB_RESHARD_MAX_ROW_BYTES + 1)],
                },
            })
        ).rejects.toThrow("source split log capacity reached");
        const oversizedRows = (await rpc({
            op: "_dump",
            target: oversizedSource,
            body: { table: table.name, orderBy: "id" },
        })) as { rows: unknown[] };
        expect(oversizedRows.rows).toEqual([]);
    }, 30_000);

    test("source log capacity rolls back partition moves and exact re-entry replaces stale triggers", async () => {
        const source = "capture-cap-source";
        const migId = "mig_capture_cap";
        const inside = "org-capture-a";
        let moved = "org-capture-b";
        while (rowVshard(moved) === rowVshard(inside)) moved += "x";
        const vshard = rowVshard(inside);
        const table = MOVABLE_TABLES.find(item => item.name === "messages");
        if (!table) throw new Error("messages spec missing");
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        await rpc({
            op: "_replaceSplitTriggers",
            target: source,
            body: { migId, tables: [table], capacity: { maxRows: 2, maxBytes: 1_000_000 } },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: { sql: "INSERT INTO messages VALUES (?, ?, ?)", params: ["bounded", inside, "before"] },
        });
        await expect(
            rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "UPDATE messages SET org_id = ?, body = ? WHERE id = ?",
                    params: [moved, "after", "bounded"],
                },
            })
        ).rejects.toThrow("source split log capacity reached");
        const unchanged = (await rpc({
            op: "_dump",
            target: source,
            body: { table: "messages", orderBy: "id" },
        })) as { rows: unknown[] };
        expect(unchanged.rows).toEqual([{ id: "bounded", org_id: inside, body: "before" }]);
        expect(await rpc({ op: "_splitState", target: source, body: { migId } })).toMatchObject({ rows: 1 });

        await rpc({
            op: "beginReshardSource",
            target: source,
            body: { migId, rangeLo: vshard, rangeHi: vshard, tables: MOVABLE_TABLES },
        });
        const trigger = (await rpc({
            op: "_triggerSql",
            target: source,
            body: { name: renderTableTriggers(migId, table).names[1] },
        })) as { sql: string };
        expect(trigger.sql).toContain("split_log_rows < 65536");
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "UPDATE messages SET org_id = ?, body = ? WHERE id = ?",
                params: [moved, "after", "bounded"],
            },
        });
        expect(await rpc({ op: "_splitState", target: source, body: { migId } })).toMatchObject({ rows: 3 });

        const schema = (await rpc({ op: "schemaState", target: source })) as {
            activeVersion: number;
            activeEpoch: number;
            activeDigest: string;
        };
        const finishIdentity = {
            migId,
            rangeLo: vshard,
            rangeHi: vshard,
            schemaVersion: schema.activeVersion,
            recoveryGeneration: 0,
            schemaEpoch: schema.activeEpoch,
            schemaDigest: schema.activeDigest,
            tables: MOVABLE_TABLES,
        };
        await expect(
            rpc({ op: "_finishReshardSourceThenLoseResponse", target: source, body: finishIdentity })
        ).rejects.toThrow("injected lost source-finalize response");
        await expect(rpc({ op: "finishReshardSource", target: source, body: finishIdentity })).resolves.toEqual({
            ok: true,
        });
        expect(await rpc({ op: "_splitState", target: source, body: { migId } })).toMatchObject({
            rows: 0,
            bytes: 0,
            drained: 1,
        });
        const log = (await rpc({
            op: "_dump",
            target: source,
            body: { table: "_chardb_split_log", orderBy: "lsn" },
        })) as { rows: unknown[] };
        expect(log.rows).toEqual([]);
        expect(await rpc({ op: "_splitOpLogState", target: source, body: { migId } })).toBeNull();
    }, 30_000);

    test("dropMigratedRange advances past an out-of-range physical prefix", async () => {
        const source = "drain-progress-source";
        const table = {
            name: "drain_progress",
            partitionColumn: "org_id",
            columns: ["id", "org_id", "body"],
        } as const;
        const inRangeOrganization = "org-drain-in";
        const outOfRangeOrganization = "org-drain-out";
        const inRangeVshard = rowVshard(inRangeOrganization);
        expect(rowVshard(outOfRangeOrganization)).not.toBe(inRangeVshard);

        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: "CREATE TABLE IF NOT EXISTS drain_progress (id TEXT PRIMARY KEY, org_id TEXT NOT NULL, body TEXT)",
            },
        });
        await rpc({
            op: "_exec",
            target: source,
            body: {
                sql: `WITH RECURSIVE seq(n) AS (
                        SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n < 549
                      )
                      INSERT INTO drain_progress (id, org_id, body)
                      SELECT 'out-' || printf('%03d', n), ?, 'outside' FROM seq`,
                params: [outOfRangeOrganization],
            },
        });
        for (let index = 0; index < 3; index++) {
            await rpc({
                op: "_exec",
                target: source,
                body: {
                    sql: "INSERT INTO drain_progress (id, org_id, body) VALUES (?, ?, ?)",
                    params: [`in-${index}`, inRangeOrganization, "inside"],
                },
            });
        }
        const migId = "mig_drain_progress";
        await rpc({
            op: "beginReshardSource",
            target: source,
            body: {
                migId,
                rangeLo: inRangeVshard,
                rangeHi: inRangeVshard,
                tables: MOVABLE_TABLES,
            },
        });

        await rpc({
            op: "stopReshardCapture",
            target: source,
            body: {
                migId,
                rangeLo: inRangeVshard,
                rangeHi: inRangeVshard,
                tables: MOVABLE_TABLES,
            },
        });

        let calls = 0;
        let deleted = 0;
        let done = false;
        while (!done && calls < 10) {
            const result = (await rpc({
                op: "dropMigratedRange",
                target: source,
                body: {
                    migId,
                    table,
                    range: { lo: inRangeVshard, hi: inRangeVshard },
                    batchSize: 100,
                },
            })) as { deleted: number; done: boolean };
            calls++;
            deleted += result.deleted;
            done = result.done;
            if (calls === 1) {
                await rpc({
                    op: "_exec",
                    target: source,
                    body: {
                        sql: `WITH RECURSIVE seq(n) AS (
                                SELECT 0 UNION ALL SELECT n + 1 FROM seq WHERE n < 199
                              ) INSERT INTO drain_progress (id, org_id, body)
                              SELECT 'later-' || printf('%03d', n), ?, 'outside' FROM seq`,
                        params: [outOfRangeOrganization],
                    },
                });
            }
        }

        expect({ calls, deleted, done }).toEqual({ calls: 6, deleted: 3, done: true });
        await expect(
            rpc({
                op: "dropMigratedRange",
                target: source,
                body: {
                    migId,
                    table,
                    range: { lo: inRangeVshard, hi: inRangeVshard },
                    batchSize: 100,
                },
            })
        ).resolves.toEqual({ deleted: 0, done: true });
        const remaining = (await rpc({
            op: "_dump",
            target: source,
            body: { table: table.name, orderBy: "id" },
        })) as { rows: { id: string; org_id: string }[] };
        expect(remaining.rows).toHaveLength(750);
        expect(new Set(remaining.rows.map(row => row.org_id))).toEqual(new Set([outOfRangeOrganization]));
    }, 15_000);
});
