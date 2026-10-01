import { afterAll, beforeAll, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const HERE = import.meta.dir;
const histories = ["better-auth-1.6", "better-auth-1.7"] as const;
type History = (typeof histories)[number];
type Action = Record<string, unknown> & { name: string; type: string };
interface Snapshot {
    catalog: {
        state: { activeVersion: number; activeEpoch: number; status: string };
        rows: unknown;
        keys: unknown;
        columns: string[];
        steps: unknown[];
    };
    shard: { state: { activeVersion: number; activeEpoch: number; status: string }; rows: unknown; steps: unknown[] };
}
let scratch = "";
let ordinal = 0;
const bundles = new Map<string, string>();

async function bundle(history: History, version: number) {
    const outfile = join(scratch, `${history}-v${version}.mjs`);
    const child = Bun.spawn(
        [process.execPath, join(HERE, "chat-migration.phase.mjs"), "--build", history, String(version), outfile],
        { stdout: "pipe", stderr: "pipe" }
    );
    const [exit, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
    if (exit !== 0) throw new Error(`chat migration bundle failed: ${stderr}`);
    bundles.set(`${history}-${version}`, outfile);
}

async function phase(history: History, version: number, persistence: string, actions: Action[]) {
    const control = join(scratch, `control-${++ordinal}.json`);
    const resultPath = join(scratch, `result-${ordinal}.json`);
    await writeFile(
        control,
        JSON.stringify({
            schema: "chardb.migration-workerd-phase-control.v1",
            release: `${history}-${version}`,
            scriptPath: bundles.get(`${history}-${version}`),
            persistencePath: persistence,
            resultPath,
            actions,
        })
    );
    await chmod(control, 0o600);
    const child = Bun.spawn([process.execPath, join(HERE, "migration.phase.mjs"), "--control", control], {
        cwd: HERE,
        stdout: "pipe",
        stderr: "pipe",
        detached: process.platform !== "win32",
    });
    const out = new Response(child.stdout).text();
    const err = new Response(child.stderr).text();
    const timer = setTimeout(() => {
        if (process.platform === "win32") child.kill("SIGKILL");
        else
            try {
                process.kill(-child.pid, "SIGKILL");
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
            }
    }, 30_000);
    const exit = await child.exited;
    clearTimeout(timer);
    const [stdout, stderr] = await Promise.all([out, err]);
    if (exit !== 0) throw new Error(`chat migration phase exited ${exit}\n${stdout}${stderr}`);
    const result = JSON.parse(await readFile(resultPath, "utf8"));
    expect(result.schema).toBe("chardb.migration-workerd-phase-result.v1");
    expect(result.release).toBe(`${history}-${version}`);
    return result.values as Record<string, unknown>;
}

beforeAll(async () => {
    scratch = await mkdtemp(join(tmpdir(), "chardb-chat-history-"));
    for (const history of histories) for (const version of [1, 2]) await bundle(history, version);
}, 30_000);
afterAll(async () => {
    if (scratch) await rm(scratch, { recursive: true, force: true });
});

for (const history of histories) {
    test(`preserves persisted chat rows, sessions, and signing keys for ${history}`, async () => {
        const persistence = join(scratch, history);
        await mkdir(persistence);
        const seeded = await phase(history, 1, persistence, [
            { name: "install", type: "migrate", migrationId: "install-v1", targetVersion: 1 },
            { name: "seed", type: "call", pathname: "/fixture/seed", body: {} },
            { name: "before", type: "call", pathname: "/fixture/state" },
            {
                name: "wrongHistory",
                type: "call",
                pathname: "/fixture/check-history",
                body: { history: history === "better-auth-1.6" ? "better-auth-1.7" : "better-auth-1.6" },
            },
        ]);
        expect(seeded.wrongHistory).toEqual({ code: "CDB_PARTITION_CONTRACT_CHANGED" });
        const before = seeded.before as Snapshot;
        expect(before.catalog.state).toMatchObject({ activeVersion: 1, activeEpoch: 2, status: "active" });
        expect(before.shard.state).toMatchObject({ activeVersion: 1, activeEpoch: 2, status: "active" });
        const token = (seeded.seed as { token: string }).token;
        const authenticatedBefore = await phase(history, 1, persistence, [
            { name: "auth", type: "call", pathname: "/fixture/auth", body: { token } },
        ]);
        const expectedAuth = { userId: "user", sessionId: "session", oldSubject: "user", newSubject: "user" };
        expect(authenticatedBefore.auth).toEqual(expectedAuth);
        const interrupted = await phase(history, 2, persistence, [
            {
                name: "begin",
                type: "migration-call",
                pathname: "/begin",
                body: { migrationId: "upgrade-v2", targetVersion: 2 },
            },
            {
                name: "shard",
                type: "migration-call",
                pathname: "/shard",
                body: { migrationId: "upgrade-v2", shardId: "ShardDO_0" },
            },
            { name: "state", type: "call", pathname: "/fixture/state" },
        ]);
        const pending = interrupted.state as Snapshot;
        expect(pending.catalog.state).toMatchObject({ activeVersion: 1, activeEpoch: 2, status: "migrating" });
        expect(pending.shard.state).toMatchObject({ activeVersion: 2, activeEpoch: 3, status: "active" });
        const resumed = await phase(history, 2, persistence, [
            { name: "resume", type: "migrate", migrationId: "upgrade-v2", targetVersion: 2 },
            { name: "repeat", type: "migrate", migrationId: "upgrade-v2", targetVersion: 2 },
            { name: "auth", type: "call", pathname: "/fixture/auth", body: { token } },
            { name: "after", type: "call", pathname: "/fixture/state" },
        ]);
        expect(resumed.auth).toEqual(expectedAuth);
        const after = resumed.after as Snapshot;
        expect(after.catalog.state).toMatchObject({ activeVersion: 2, activeEpoch: 3, status: "active" });
        expect(after.shard.state).toMatchObject({ activeVersion: 2, activeEpoch: 3, status: "active" });
        expect(after.catalog.rows).toEqual(before.catalog.rows);
        expect(after.catalog.keys).toEqual(before.catalog.keys);
        expect(after.shard.rows).toEqual(before.shard.rows);
        expect(after.catalog.columns.filter(name => name === "alg")).toHaveLength(1);
        expect(after.catalog.columns.filter(name => name === "crv")).toHaveLength(1);
        expect(after.catalog.steps).toHaveLength(2);
        expect(after.shard.steps).toHaveLength(2);
        const unknown = await phase(history, 2, persistence, [
            { name: "unknown", type: "call", pathname: "/fixture/check-history", body: { history, corrupt: true } },
        ]);
        expect(unknown.unknown).toEqual({ code: "CDB_PARTITION_CONTRACT_CHANGED" });
    }, 120_000);

    test(`installs a fresh version-two chat database for ${history}`, async () => {
        const persistence = join(scratch, `fresh-${history}`);
        await mkdir(persistence);
        const seeded = await phase(history, 2, persistence, [
            { name: "install", type: "migrate", migrationId: "fresh-v2", targetVersion: 2 },
            { name: "seed", type: "call", pathname: "/fixture/seed", body: {} },
            { name: "state", type: "call", pathname: "/fixture/state" },
        ]);
        const state = seeded.state as Snapshot;
        for (const storage of [state.catalog, state.shard]) {
            expect(storage.state).toMatchObject({ activeVersion: 2, activeEpoch: 2, status: "active" });
            expect(storage.steps).toHaveLength(2);
        }
        expect(state.catalog.columns.filter(name => name === "alg")).toHaveLength(1);
        expect(state.catalog.columns.filter(name => name === "crv")).toHaveLength(1);
        const auth = await phase(history, 2, persistence, [
            { name: "auth", type: "call", pathname: "/fixture/auth", body: seeded.seed },
        ]);
        expect(auth.auth).toEqual({ userId: "user", sessionId: "session", oldSubject: "user", newSubject: "user" });
    }, 60_000);
}
