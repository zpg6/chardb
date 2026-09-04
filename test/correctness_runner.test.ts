import { afterEach, describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
    ChildProcessFailure,
    compareWorkerdHarnesses,
    isIsolatedNativeTest,
    isTransientWorkerdStartupFailure,
    run,
    runWithRetries,
} from "../scripts/test-correctness.mjs";

const temporaryDirectories: string[] = [];
const SYNTHETIC_PROCESS_TIMEOUT_MS = 1_000;
const SYNTHETIC_PROCESS_STARTUP_DEADLINE_MS = 5_000;
const SYNTHETIC_TERMINATION_GRACE_MS = 1_000;

afterEach(async () => {
    await Promise.all(temporaryDirectories.splice(0).map(directory => rm(directory, { recursive: true, force: true })));
});

async function processTreeCommand(
    childCompletion = "await new Promise(() => {});"
): Promise<{ readonly args: string[]; readonly pidFile: string }> {
    const directory = await mkdtemp(path.join(tmpdir(), "chardb-correctness-runner-"));
    temporaryDirectories.push(directory);
    const pidFile = path.join(directory, "grandchild.pid");
    const grandchildSource = `
        process.on("SIGINT", () => {});
        process.on("SIGTERM", () => {});
        setInterval(() => {}, 1_000);
    `;
    const childSource = `
        process.on("SIGINT", () => {});
        process.on("SIGTERM", () => {});
        const grandchild = Bun.spawn([process.execPath, "-e", ${JSON.stringify(grandchildSource)}], {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
        });
        await Bun.write(${JSON.stringify(pidFile)}, String(grandchild.pid));
        ${childCompletion}
    `;
    return { args: [process.execPath, "-e", childSource], pidFile };
}

async function readPid(pidFile: string): Promise<number> {
    const deadline = Date.now() + SYNTHETIC_PROCESS_STARTUP_DEADLINE_MS;
    while (Date.now() < deadline) {
        try {
            const pid = Number(await readFile(pidFile, "utf8"));
            if (Number.isSafeInteger(pid) && pid > 0) return pid;
        } catch {
            // The child has not published its grandchild yet.
        }
        await Bun.sleep(10);
    }
    throw new Error("synthetic child did not publish its grandchild pid");
}

async function expectProcessGone(pid: number): Promise<void> {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
        try {
            process.kill(pid, 0);
        } catch (error) {
            if (error !== null && typeof error === "object" && "code" in error && error.code === "ESRCH") return;
            throw error;
        }
        await Bun.sleep(10);
    }
    throw new Error(`process ${pid} survived process-tree cleanup`);
}

const posixTest = process.platform === "win32" ? test.skip : test;

describe("correctness runner process control", () => {
    posixTest(
        "keeps replacement control pipes open after garbage collection",
        async () => {
            const childSource = `
            process.stdin.resume();
            process.stdin.on("end", () => require("node:fs").writeSync(3, "ready"));
        `;
            const source = `
            import { spawn } from "node:child_process";
            import { once } from "node:events";
            for (let index = 0; index < 32; index++) {
                const child = spawn(process.execPath, ["-e", ${JSON.stringify(childSource)}], {
                    stdio: ["pipe", "ignore", "inherit", "pipe"],
                });
                const exited = once(child, "exit");
                const output = new Response(child.stdio[3]).text();
                // Old Bun finalizers double-close a previous child's reused fd.
                Bun.gc(true);
                child.stdin.end();
                const [code] = await exited;
                if (code !== 0 || await output !== "ready") {
                    throw new Error("Control pipe failed at launch " + index + " on Bun " + Bun.version);
                }
            }
        `;
            await expect(
                run("Workerd control pipe ownership", [process.execPath, "-e", source], 10_000, {
                    stdin: "ignore",
                    captureOutput: true,
                })
            ).resolves.toBeUndefined();
        },
        15_000
    );

    test("isolates the real Wrangler reshard producer from Bun's parallel unit-test pool", () => {
        expect(isIsolatedNativeTest("test/native_reshard_benchmark_producer.test.ts")).toBe(true);
        expect(isIsolatedNativeTest("test/native_reshard_benchmark_report.test.ts")).toBe(false);
        expect(isIsolatedNativeTest("test/workerd/reshard.harness.test.ts")).toBe(false);
    });

    test("runs the restart-in-place file store before actor-only Workerd harnesses", () => {
        const files = [
            "/repo/test/workerd/gateway.harness.test.ts",
            "/repo/test/workerd/file-store.harness.test.ts",
            "/repo/test/workerd/catalog.harness.test.ts",
        ];
        expect(files.sort(compareWorkerdHarnesses)).toEqual([
            "/repo/test/workerd/file-store.harness.test.ts",
            "/repo/test/workerd/catalog.harness.test.ts",
            "/repo/test/workerd/gateway.harness.test.ts",
        ]);
    });

    test("an outer timeout terminates a signal-resistant grandchild", async () => {
        const tree = await processTreeCommand();
        const failure = run("synthetic timeout", tree.args, SYNTHETIC_PROCESS_TIMEOUT_MS, {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
        }).catch((error: unknown) => error);
        const grandchildPid = await readPid(tree.pidFile);

        await expect(failure).resolves.toMatchObject({ timedOut: true, exitCode: null, signalCode: null });
        await expectProcessGone(grandchildPid);
    });

    test("forwards a parent termination signal to the whole child tree", async () => {
        const tree = await processTreeCommand();
        const signals = new EventEmitter();
        const failure = run("synthetic signal", tree.args, undefined, {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
            signalSource: signals,
        }).catch((error: unknown) => error);
        const grandchildPid = await readPid(tree.pidFile);
        signals.emit("SIGTERM");

        await expect(failure).resolves.toMatchObject({ timedOut: false, signalCode: "SIGTERM" });
        await expectProcessGone(grandchildPid);
    });

    test("cleans a leader-exits-first tree and preserves its nonzero exit code", async () => {
        const tree = await processTreeCommand("process.exit(23);");
        const failure = run("synthetic exit", tree.args, undefined, {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
        }).catch((error: unknown) => error);
        const grandchildPid = await readPid(tree.pidFile);

        await expect(failure).resolves.toBeInstanceOf(ChildProcessFailure);
        await expect(failure).resolves.toMatchObject({ timedOut: false, exitCode: 23, signalCode: null });
        await expectProcessGone(grandchildPid);
    });

    posixTest("cleans a resistant grandchild and preserves an unprompted child signal", async () => {
        const tree = await processTreeCommand(
            'process.removeAllListeners("SIGTERM"); process.kill(process.pid, "SIGTERM");'
        );
        const failure = run("synthetic child signal", tree.args, undefined, {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
        }).catch((error: unknown) => error);
        const grandchildPid = await readPid(tree.pidFile);

        await expect(failure).resolves.toBeInstanceOf(ChildProcessFailure);
        await expect(failure).resolves.toMatchObject({ timedOut: false, signalCode: "SIGTERM" });
        await expectProcessGone(grandchildPid);
    });

    test("does not report success before a leader-exits-first tree is gone", async () => {
        const tree = await processTreeCommand("process.exit(0);");
        const completion = run("synthetic success", tree.args, undefined, {
            stdin: "ignore",
            stdout: "ignore",
            stderr: "ignore",
            terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
        });
        const grandchildPid = await readPid(tree.pidFile);

        await expect(completion).resolves.toBeUndefined();
        await expectProcessGone(grandchildPid);
    });

    test("preserves captured stdout and stderr from a failed child", async () => {
        const directory = await mkdtemp(path.join(tmpdir(), "chardb-correctness-output-"));
        temporaryDirectories.push(directory);
        const stdoutPath = path.join(directory, "stdout.log");
        const stderrPath = path.join(directory, "stderr.log");
        const failure = await run(
            "synthetic captured failure",
            [process.execPath, "-e", 'console.log("sample output"); console.error("sample error"); process.exit(23)'],
            undefined,
            {
                stdin: "ignore",
                stdout: Bun.file(stdoutPath),
                stderr: Bun.file(stderrPath),
            }
        ).catch((error: unknown) => error);

        expect(failure).toMatchObject({ exitCode: 23, signalCode: null });
        expect(await readFile(stdoutPath, "utf8")).toBe("sample output\n");
        expect(await readFile(stderrPath, "utf8")).toBe("sample error\n");
    });

    test("retries one recognized startup failure without retrying a successful replacement", async () => {
        const directory = await mkdtemp(path.join(tmpdir(), "chardb-correctness-retry-"));
        temporaryDirectories.push(directory);
        const countPath = path.join(directory, "attempts.txt");
        const source = `
            const path = ${JSON.stringify(countPath)};
            const count = Number(await Bun.file(path).text().catch(() => "0")) + 1;
            await Bun.write(path, String(count));
            if (count === 1) console.error("Workers runtime failed to start");
            process.exit(count === 1 ? 23 : 0);
        `;

        await expect(
            runWithRetries("synthetic retry", [process.execPath, "-e", source], undefined, {
                attempts: 2,
                retryDelayMs: 0,
                captureOutput: true,
                shouldRetry: isTransientWorkerdStartupFailure,
                stdin: "ignore",
            })
        ).resolves.toBeUndefined();
        expect(await readFile(countPath, "utf8")).toBe("2");
    });

    test("does not retry an ordinary test failure", async () => {
        const directory = await mkdtemp(path.join(tmpdir(), "chardb-correctness-no-retry-"));
        temporaryDirectories.push(directory);
        const countPath = path.join(directory, "attempts.txt");
        const source = `
            const path = ${JSON.stringify(countPath)};
            const count = Number(await Bun.file(path).text().catch(() => "0")) + 1;
            await Bun.write(path, String(count));
            console.error("AssertionError: expected 1 to equal 2");
            process.exit(1);
        `;

        const failure = runWithRetries("synthetic assertion failure", [process.execPath, "-e", source], undefined, {
            attempts: 2,
            retryDelayMs: 0,
            captureOutput: true,
            shouldRetry: isTransientWorkerdStartupFailure,
            stdin: "ignore",
        }).catch((error: unknown) => error);

        await expect(failure).resolves.toMatchObject({ exitCode: 1, signalCode: null });
        expect(await readFile(countPath, "utf8")).toBe("1");
    });

    test("does not retry a timed-out process", async () => {
        const directory = await mkdtemp(path.join(tmpdir(), "chardb-correctness-timeout-retry-"));
        temporaryDirectories.push(directory);
        const countPath = path.join(directory, "attempts.txt");
        const source = `
            const path = ${JSON.stringify(countPath)};
            const count = Number(await Bun.file(path).text().catch(() => "0")) + 1;
            await Bun.write(path, String(count));
            await new Promise(() => {});
        `;
        const failure = runWithRetries(
            "synthetic retry timeout",
            [process.execPath, "-e", source],
            SYNTHETIC_PROCESS_TIMEOUT_MS,
            {
                attempts: 2,
                retryDelayMs: 0,
                terminationGraceMs: SYNTHETIC_TERMINATION_GRACE_MS,
                stdin: "ignore",
                stdout: "ignore",
                stderr: "ignore",
            }
        ).catch((error: unknown) => error);

        await expect(failure).resolves.toMatchObject({ timedOut: true });
        expect(await readFile(countPath, "utf8")).toBe("1");
    });
});
