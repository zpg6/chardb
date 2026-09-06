import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import { type GenerateRun, clientGenerationHooks, createClientGenerator, runGenerate } from "../src/vite/generate.ts";
import { chardb } from "../src/vite/index.ts";

const ROOT = "/app";
const ok = (stdout = ""): GenerateRun => ({ exitCode: 0, stdout, stderr: "" });
const failed = (stderr: string): GenerateRun => ({ exitCode: 1, stdout: "", stderr });

/** Runs answer from `results` in order; after `hold()` each run waits for a `release()`. */
function fakeRun(results: readonly GenerateRun[] = []) {
    const queue = [...results];
    const releases: (() => void)[] = [];
    const roots: string[] = [];
    let held = false;
    return {
        run: (root: string) => {
            roots.push(root);
            const result = queue.shift() ?? ok();
            if (!held) return Promise.resolve(result);
            return new Promise<GenerateRun>(done => releases.push(() => done(result)));
        },
        runs: () => roots.length,
        roots,
        hold: () => {
            held = true;
        },
        release: () => releases.shift()?.(),
    };
}

function harness(results: readonly GenerateRun[] = []) {
    const fake = fakeRun(results);
    const log: string[] = [];
    const generator = createClientGenerator({
        root: ROOT,
        run: fake.run,
        log: {
            info: message => log.push(`info ${message}`),
            warn: message => log.push(`warn ${message}`),
        },
    });
    return { ...fake, generator, log };
}

describe("client generation from the Vite dev server", () => {
    test("runs once at start and collapses a burst of changes into one run", async () => {
        const h = harness();
        await h.generator.start();
        expect(h.runs()).toBe(1);
        expect(h.roots).toEqual([ROOT]);
        const changes = ["src/queries.ts", "src/schema.ts", "src/worker.ts"].map(file =>
            h.generator.changed(`${ROOT}/${file}`)
        );
        expect(h.runs()).toBe(1);
        await Promise.all(changes);
        expect(h.runs()).toBe(2);
    });

    test("a change during a run queues exactly one follow-up run", async () => {
        const h = harness();
        h.hold();
        const first = h.generator.start();
        const during = h.generator.changed(`${ROOT}/src/api.ts`);
        await Bun.sleep(200);
        expect(h.runs()).toBe(1);
        h.release();
        await Bun.sleep(0);
        expect(h.runs()).toBe(2);
        h.release();
        await Promise.all([first, during]);
        expect(h.runs()).toBe(2);
    });

    test("ignores files outside the root and files that are not source", async () => {
        const h = harness([ok("chardb: wrote src/chardb_api.ts\r\n")]);
        await h.generator.start();
        expect(h.log).toEqual(["info [chardb] wrote src/chardb_api.ts"]);
        for (const file of ["/elsewhere/src/worker.ts", `${ROOT}/README.md`, `${ROOT}/src/data.json`]) {
            await h.generator.changed(file);
        }
        expect(h.runs()).toBe(1);
        await h.generator.changed(`${ROOT}/src/worker.mts`);
        expect(h.runs()).toBe(2);
    });

    test("a project without clients only reruns when src/worker.ts changes", async () => {
        const h = harness([ok("chardb: no clients configured\n"), ok("chardb: no clients configured\n")]);
        await h.generator.start();
        await h.generator.changed(`${ROOT}/src/queries.ts`);
        expect(h.runs()).toBe(1);
        await h.generator.changed(`${ROOT}/src/worker.ts`);
        expect(h.runs()).toBe(2);
        expect(h.log).toEqual([
            "info [chardb] no clients declared on chardb(); generate waits for src/worker.ts to change",
        ]);
        await h.generator.changed(`${ROOT}/src/worker.ts`);
        await h.generator.changed(`${ROOT}/src/queries.ts`);
        expect(h.runs()).toBe(4);
    });

    test("a failing run warns once per distinct failure without the CLI prefix, and later runs still happen", async () => {
        const stderr = "chardb generate: src/worker.ts: unexpected token\n";
        const h = harness([failed(stderr), failed(stderr), failed("boom: chardb generate: other\n")]);
        await h.generator.start();
        await h.generator.changed(`${ROOT}/src/worker.ts`);
        await h.generator.changed(`${ROOT}/src/worker.ts`);
        await h.generator.changed(`${ROOT}/src/worker.ts`);
        expect(h.runs()).toBe(4);
        expect(h.log).toEqual([
            "warn [chardb] generate failed: src/worker.ts: unexpected token",
            "warn [chardb] generate failed: boom: chardb generate: other",
        ]);
    });

    test("a spawn failure turns generation off for the session", async () => {
        const log: string[] = [];
        let attempts = 0;
        const generator = createClientGenerator({
            root: ROOT,
            run: () => {
                attempts++;
                return Promise.reject(new Error("spawn bun ENOENT"));
            },
            log: { info: message => log.push(message), warn: message => log.push(message) },
        });
        await generator.start();
        await generator.changed(`${ROOT}/src/worker.ts`);
        await generator.start();
        expect(attempts).toBe(1);
        expect(log).toEqual(["[chardb] generate is off for this session: spawn bun ENOENT"]);
    });

    test("the hooks generate for the dev server only", async () => {
        const logger = { info: () => {}, warn: () => {} };
        for (const config of [
            { root: ROOT, command: "build" as const, mode: "production", logger },
            { root: ROOT, command: "serve" as const, mode: "development", isPreview: true, logger },
            { root: ROOT, command: "serve" as const, mode: "test", logger },
        ]) {
            const fake = fakeRun();
            const hooks = clientGenerationHooks(fake.run);
            hooks.configResolved(config);
            hooks.configureServer();
            hooks.watchChange(`${ROOT}/src/worker.ts`);
            await Bun.sleep(200);
            expect(fake.runs()).toBe(0);
        }
        const fake = fakeRun();
        const hooks = clientGenerationHooks(fake.run);
        hooks.configResolved({ root: ROOT, command: "serve", mode: "development", logger });
        hooks.configureServer();
        await Bun.sleep(0);
        expect(fake.runs()).toBe(1);
        hooks.watchChange(`${ROOT}/src/worker.ts`);
        hooks.watchChange(`${ROOT}/src/queries.ts`);
        await Bun.sleep(250);
        expect(fake.runs()).toBe(2);
        expect(fake.roots).toEqual([ROOT, ROOT]);
    });

    test("the plugin carries the hooks unless generation is turned off", () => {
        expect(chardb({ generate: false })).not.toHaveProperty("configureServer");
        expect(chardb({ generate: false })).not.toHaveProperty("watchChange");
        expect(chardb()).toHaveProperty("configureServer");
        expect(chardb()).toHaveProperty("watchChange");
    });

    test("the real command runs under Bun in the app root and finds the committed fixtures current", async () => {
        const result = await runGenerate(resolve(import.meta.dir, "fixtures/generate"), true);
        expect(result).toEqual({ exitCode: 0, stdout: "", stderr: "" });
    }, 20_000);
});
