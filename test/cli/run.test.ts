import { describe, expect, test } from "bun:test";
import type { CliContext, CliFetch } from "../../src/cli/context.ts";
import { runCli } from "../../src/cli/run.ts";

function fakeCtx(overrides: Partial<CliContext> = {}): { ctx: CliContext; out: string[]; err: string[] } {
    const out: string[] = [];
    const err: string[] = [];
    return {
        ctx: {
            cwd: "/tmp/proj",
            env: {},
            stdout: value => out.push(value),
            stderr: value => err.push(value),
            async read() {
                throw new Error("not used");
            },
            async write() {},
            async exists() {
                return false;
            },
            ...overrides,
        },
        out,
        err,
    };
}

describe("chardb command availability", () => {
    test("help lists only commands supported by this release", async () => {
        const { ctx, out, err } = fakeCtx();

        expect(await runCli(ctx, ["--help"])).toBe(0);
        expect(err).toEqual([]);
        for (const command of ["init", "doctor", "migrate", "migrations", "vectorize", "api", "backups"]) {
            expect(out.join("")).toContain(`chardb ${command}`);
        }
        expect(out.join("")).not.toContain("shards");
        for (const command of ["explain", "deploy", "snapshot", "restore", "export", "schedule"]) {
            expect(out.join("")).not.toContain(`chardb ${command}`);
        }
        expect(out.join("")).not.toContain("not implemented");
        expect(out.join("")).not.toContain("__migrations-inspect");
    });

    test("parses initial migration generation strictly", async () => {
        for (const argv of [
            ["migrations"],
            ["migrations", "generate"],
            ["migrations", "generate", "initial_schema"],
            ["migrations", "generate", "--name"],
            ["migrations", "generate", "--name", "initial_schema", "extra"],
            ["migrations", "generate", "--name", "initial_schema", "--name", "again"],
        ]) {
            const { ctx, err } = fakeCtx();
            expect(await runCli(ctx, argv)).toBe(2);
            expect(err).toEqual(["usage: chardb migrations generate --name <name>\n"]);
        }
    });

    test("reports an init preflight failure without an uncaught exception", async () => {
        let writes = 0;
        const { ctx, out, err } = fakeCtx({
            async prepareDirectory() {
                return "existing";
            },
            async readDirectory() {
                return ["package.json"];
            },
            async removeDirectory() {},
            async exists(path) {
                return path.endsWith("/package.json");
            },
            async writeFilesExclusive() {
                writes++;
            },
        });

        expect(await runCli(ctx, ["init", "existing-app"])).toBe(1);
        expect(writes).toBe(0);
        expect(out).toEqual([]);
        expect(err).toHaveLength(1);
        expect(err[0]).toContain("chardb init requires an empty directory");
        expect(err[0]).not.toContain("runInit");
    });

    test("keeps the hidden inspector strict and unadvertised", async () => {
        const missing = fakeCtx();
        expect(await runCli(missing.ctx, ["__migrations-inspect"])).toBe(2);
        expect(missing.out).toEqual([]);
        expect(missing.err).toEqual([]);

        const invalid = fakeCtx();
        expect(await runCli(invalid.ctx, ["__migrations-inspect", "Bad Name", "1", "-"])).toBe(1);
        expect(invalid.out).toEqual([]);
        expect(invalid.err.join("")).toContain("migration name is invalid");
    });

    test("internal research commands are unknown to the shipped binary", async () => {
        for (const command of ["explain", "deploy", "snapshot", "restore", "export", "schedule"]) {
            const { ctx, out, err } = fakeCtx();

            expect(await runCli(ctx, [command])).toBe(2);
            expect(out.join("")).toContain("chardb init");
            expect(err).toEqual([`unknown command: ${command}\n`]);
        }
    });

    test("keeps range movement behind an explicit experimental namespace", async () => {
        const help = fakeCtx();
        expect(await runCli(help.ctx, ["experimental", "--help"])).toBe(0);
        expect(help.err).toEqual([]);
        expect(help.out.join("")).toContain("chardb experimental shards split");
        expect(help.out.join("")).toContain("no compatibility promise");

        let fetchCalls = 0;
        const old = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async () => {
                fetchCalls++;
                return Response.json({});
            },
        });
        expect(await runCli(old.ctx, ["shards", "status", "--url", "https://worker.example", "--id", "split-1"])).toBe(
            2
        );
        expect(fetchCalls).toBe(0);
        expect(old.out).toEqual([]);
        expect(old.err).toEqual([
            "chardb shards moved to chardb experimental shards; the old command is disabled and did not run\n",
        ]);
    });
});

describe("chardb migrate CLI", () => {
    test("resumes pending shards with bounded workers and completes the exact migration", async () => {
        const calls: { readonly path: string; readonly body: Record<string, unknown> | null }[] = [];
        const fetch: CliFetch = async (input, init) => {
            const url = new URL(String(input));
            expect(new Headers(init?.headers).get("authorization")).toBe("Bearer migration-secret");
            const body = typeof init?.body === "string" ? (JSON.parse(init.body) as Record<string, unknown>) : null;
            calls.push({ path: `${url.pathname}${url.search}`, body });
            if (url.pathname.endsWith("/state")) {
                return Response.json({
                    ok: true,
                    state: {
                        activeVersion: 1,
                        activeEpoch: 4,
                        status: "active",
                        migrationId: null,
                        targetVersion: null,
                    },
                });
            }
            if (url.pathname.endsWith("/begin")) {
                return Response.json({
                    ok: true,
                    state: {
                        activeVersion: 1,
                        activeEpoch: 4,
                        status: "migrating",
                        migrationId: "deploy-2",
                        targetVersion: 2,
                    },
                });
            }
            if (url.pathname.endsWith("/shards")) {
                return Response.json({
                    ok: true,
                    shards: [
                        { shardId: "ShardDO_0", status: "active" },
                        { shardId: "ShardDO_1", status: "pending" },
                        { shardId: "ShardDO_2", status: "pending" },
                    ],
                });
            }
            if (url.pathname.endsWith("/shard")) {
                return Response.json({ ok: true, shard: { shardId: body?.shardId, status: "active" } });
            }
            if (url.pathname.endsWith("/catalog")) {
                return Response.json({
                    ok: true,
                    state: {
                        activeVersion: 1,
                        activeEpoch: 4,
                        status: "migrating",
                        migrationId: "deploy-2",
                        targetVersion: 2,
                    },
                });
            }
            if (url.pathname.endsWith("/complete")) {
                return Response.json({
                    ok: true,
                    state: {
                        activeVersion: 2,
                        activeEpoch: 5,
                        lastMigrationId: "deploy-2",
                        status: "active",
                        migrationId: null,
                        targetVersion: null,
                    },
                });
            }
            return new Response("missing", { status: 404 });
        };
        const { ctx, out, err } = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "migration-secret" },
            fetch,
        });

        expect(
            await runCli(ctx, [
                "migrate",
                "--url",
                "https://worker.example",
                "--id",
                "deploy-2",
                "--target",
                "2",
                "--concurrency",
                "2",
            ])
        ).toBe(0);
        expect(err).toEqual([]);
        expect(out.join("")).toContain("migrating 2 pending shard(s)");
        expect(out.join("")).toContain("applied Catalog schema version 2");
        expect(out.join("")).toContain("schema version 2 active at epoch 5");
        expect(
            calls
                .filter(call => call.path.endsWith("/shard"))
                .map(call => call.body?.shardId)
                .sort()
        ).toEqual(["ShardDO_1", "ShardDO_2"]);
        expect(calls.at(-1)?.path).toBe("/_chardb/migrations/complete");
        expect(calls.find(call => call.path.endsWith("/catalog"))?.body).toEqual({
            migrationId: "deploy-2",
            version: 2,
        });
    });

    test("requires the secret and refuses unsafe remote HTTP", async () => {
        const missing = fakeCtx({ fetch: globalThis.fetch });
        expect(
            await runCli(missing.ctx, ["migrate", "--url", "https://worker.example", "--id", "m1", "--target", "1"])
        ).toBe(2);

        const unsafe = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async () => Response.json({ ok: true }),
        });
        expect(
            await runCli(unsafe.ctx, ["migrate", "--url", "http://worker.example", "--id", "m1", "--target", "1"])
        ).toBe(1);
        expect(unsafe.err.join("")).toContain("must use HTTPS");
    });

    test("uses the explicit baseline endpoint for version-zero adoption", async () => {
        const paths: string[] = [];
        const fetch: CliFetch = async input => {
            const url = new URL(String(input));
            paths.push(url.pathname);
            if (url.pathname.endsWith("/state")) {
                return Response.json({
                    state: {
                        activeVersion: 0,
                        activeEpoch: 1,
                        status: "active",
                        migrationId: null,
                        targetVersion: null,
                    },
                });
            }
            if (url.pathname.endsWith("/baseline") || url.pathname.endsWith("/catalog")) {
                return Response.json({
                    state: {
                        activeVersion: 0,
                        activeEpoch: 1,
                        status: "migrating",
                        migrationId: "adopt-v1",
                        targetVersion: 1,
                    },
                });
            }
            if (url.pathname.endsWith("/shards")) return Response.json({ shards: [] });
            if (url.pathname.endsWith("/complete")) {
                return Response.json({
                    state: {
                        activeVersion: 1,
                        activeEpoch: 2,
                        lastMigrationId: "adopt-v1",
                        status: "active",
                        migrationId: null,
                        targetVersion: null,
                    },
                });
            }
            throw new Error(`unexpected request ${url.pathname}`);
        };
        const { ctx, err } = fakeCtx({ env: { CHARDB_ADMIN_TOKEN: "secret" }, fetch });
        expect(
            await runCli(ctx, [
                "migrate",
                "--url",
                "https://worker.example",
                "--id",
                "adopt-v1",
                "--target",
                "1",
                "--baseline",
            ])
        ).toBe(0);
        expect(err).toEqual([]);
        expect(paths).toContain("/_chardb/migrations/baseline");
        expect(paths).not.toContain("/_chardb/migrations/begin");
    });
});

describe("chardb experimental shards CLI", () => {
    const state = (phase: number, phaseName: string, terminal = false) => ({
        migrationId: "split-1",
        phase,
        phaseName,
        terminal,
        sourceShard: "ShardDO_0",
        destinationShard: "ShardDO_1",
        rangeLo: 8,
        rangeHi: 15,
    });

    test("starts or resumes and drives only a bounded number of steps to an explicit terminal state", async () => {
        const calls: { readonly path: string; readonly body: Record<string, unknown> | null }[] = [];
        let drives = 0;
        const fetch: CliFetch = async (input, init) => {
            const url = new URL(String(input));
            expect(new Headers(init?.headers).get("authorization")).toBe("Bearer reshard-secret");
            const body = typeof init?.body === "string" ? (JSON.parse(init.body) as Record<string, unknown>) : null;
            calls.push({ path: url.pathname, body });
            if (url.pathname.endsWith("/start"))
                return Response.json({ ok: true, started: true, state: state(0, "INIT") });
            if (url.pathname.endsWith("/drive")) {
                drives++;
                return Response.json({
                    ok: true,
                    state: drives === 1 ? state(1, "TAIL_CAPTURE_ENABLED") : state(6, "SOURCE_DRAINED", true),
                });
            }
            throw new Error(`unexpected ${url.pathname}`);
        };
        const { ctx, out, err } = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "reshard-secret" },
            fetch,
        });

        expect(
            await runCli(ctx, [
                "experimental",
                "shards",
                "split",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
                "--lo",
                "8",
                "--hi",
                "15",
                "--to",
                "ShardDO_1",
                "--max-steps",
                "2",
            ])
        ).toBe(0);
        expect(err).toEqual([]);
        expect(out.join("")).toContain("started reshard split-1: INIT");
        expect(out.join("")).toContain("completed reshard split-1: SOURCE_DRAINED");
        expect(calls).toHaveLength(3);
        expect(calls[0]?.body).toEqual({
            migrationId: "split-1",
            destinationShard: "ShardDO_1",
            rangeLo: 8,
            rangeHi: 15,
        });
        expect(calls[0]?.body).not.toHaveProperty("sourceShard");
        expect(calls[0]?.body).not.toHaveProperty("epochAtStart");
        expect(calls[0]?.body).not.toHaveProperty("tables");
    });

    test("stops at max steps and does not poll forever", async () => {
        let calls = 0;
        const fetch: CliFetch = async input => {
            calls++;
            const url = new URL(String(input));
            return Response.json({
                ok: true,
                started: url.pathname.endsWith("/start"),
                state: state(
                    url.pathname.endsWith("/start") ? 0 : 1,
                    url.pathname.endsWith("/start") ? "INIT" : "TAIL_CAPTURE_ENABLED"
                ),
            });
        };
        const { ctx, err } = fakeCtx({ env: { CHARDB_ADMIN_TOKEN: "secret" }, fetch });
        expect(
            await runCli(ctx, [
                "experimental",
                "shards",
                "split",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
                "--lo",
                "8",
                "--hi",
                "15",
                "--to",
                "ShardDO_1",
                "--max-steps",
                "2",
            ])
        ).toBe(1);
        expect(calls).toBe(3);
        expect(err.join("")).toContain("did not reach a terminal state after 2 bounded steps");
    });

    test("runs legacy recovery only when explicitly requested", async () => {
        const paths: string[] = [];
        const bodies: unknown[] = [];
        const { ctx, out, err } = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async (input, init) => {
                paths.push(new URL(String(input)).pathname);
                bodies.push(typeof init?.body === "string" ? JSON.parse(init.body) : null);
                return Response.json({ ok: true, action: "resumed", state: state(4, "DUAL_WRITE_OPEN") });
            },
        });
        expect(
            await runCli(ctx, [
                "experimental",
                "shards",
                "recover",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
            ])
        ).toBe(0);
        expect(err).toEqual([]);
        expect(paths).toEqual(["/_chardb/shards/recover"]);
        expect(bodies).toEqual([{ migrationId: "split-1" }]);
        expect(out.join("")).toContain("recovery resumed reshard split-1: DUAL_WRITE_OPEN");
    });

    test("does not retry 4xx or invalid JSON, but bounds retryable responses", async () => {
        for (const response of [
            () => Response.json({ error: "bad request" }, { status: 400 }),
            () => new Response("not-json", { status: 200 }),
        ]) {
            let calls = 0;
            const { ctx } = fakeCtx({
                env: { CHARDB_ADMIN_TOKEN: "secret" },
                fetch: async () => {
                    calls++;
                    return response();
                },
            });
            expect(
                await runCli(ctx, [
                    "experimental",
                    "shards",
                    "status",
                    "--url",
                    "https://worker.example",
                    "--id",
                    "split-1",
                ])
            ).toBe(1);
            expect(calls).toBe(1);
        }

        let retryableCalls = 0;
        const retryable = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async (_input, init) => {
                retryableCalls++;
                expect(init?.signal).toBeDefined();
                return Response.json({ error: "busy" }, { status: 503 });
            },
        });
        expect(
            await runCli(retryable.ctx, [
                "experimental",
                "shards",
                "status",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
            ])
        ).toBe(1);
        expect(retryableCalls).toBe(3);

        let timeoutCalls = 0;
        const timeout = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async (_input, init) => {
                timeoutCalls++;
                expect(init?.signal).toBeDefined();
                throw new DOMException("request timed out", "TimeoutError");
            },
        });
        expect(
            await runCli(timeout.ctx, [
                "experimental",
                "shards",
                "status",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
            ])
        ).toBe(1);
        expect(timeoutCalls).toBe(3);
    });

    test("preserves the exact start body across a retryable response", async () => {
        const bodies: string[] = [];
        const fetch: CliFetch = async (_input, init) => {
            bodies.push(String(init?.body));
            if (bodies.length === 1) return Response.json({ error: "busy" }, { status: 503 });
            return Response.json({ ok: true, started: true, state: state(6, "SOURCE_DRAINED", true) });
        };
        const { ctx, err } = fakeCtx({ env: { CHARDB_ADMIN_TOKEN: "secret" }, fetch });
        expect(
            await runCli(ctx, [
                "experimental",
                "shards",
                "split",
                "--url",
                "https://worker.example",
                "--id",
                "split-1",
                "--lo",
                "8",
                "--hi",
                "15",
                "--to",
                "ShardDO_1",
                "--max-steps",
                "1",
            ])
        ).toBe(0);
        expect(err).toEqual([]);
        expect(bodies).toHaveLength(2);
        expect(bodies[1]).toBe(bodies[0]);
    });

    test("rejects non-origin URLs before fetch", async () => {
        let calls = 0;
        const { ctx, err } = fakeCtx({
            env: { CHARDB_ADMIN_TOKEN: "secret" },
            fetch: async () => {
                calls++;
                return Response.json({ state: null });
            },
        });
        expect(
            await runCli(ctx, [
                "experimental",
                "shards",
                "status",
                "--url",
                "https://worker.example/hidden?query=yes",
                "--id",
                "split-1",
            ])
        ).toBe(1);
        expect(calls).toBe(0);
        expect(err.join("")).toContain("must contain only an origin");
    });
});

describe("chardb doctor CLI", () => {
    test("help only advertises the implemented Wrangler check", async () => {
        const { ctx, out } = fakeCtx();

        expect(await runCli(ctx, ["--help"])).toBe(0);
        expect(out.join("")).toContain("chardb doctor [wrangler]");
        expect(out.join("")).not.toContain("wrangler.jsonc / schema / auth");
    });

    test("unimplemented and unknown targets are usage errors", async () => {
        for (const target of ["schema", "auth", "spelling-error"]) {
            const { ctx, out, err } = fakeCtx();

            expect(await runCli(ctx, ["doctor", target])).toBe(2);
            expect(out).toEqual([]);
            expect(err).toEqual(["usage: chardb doctor [wrangler]\n"]);
        }
    });

    test("extra targets are usage errors", async () => {
        const { ctx, out, err } = fakeCtx();

        expect(await runCli(ctx, ["doctor", "wrangler", "extra"])).toBe(2);
        expect(out).toEqual([]);
        expect(err).toEqual(["usage: chardb doctor [wrangler]\n"]);
    });
});
