import { readdir } from "node:fs/promises";
import path from "node:path";
import { ManagedProcessError, runManagedCommand } from "./process-lifecycle.mjs";

const ROOT = path.resolve(import.meta.dirname, "..");
const TEST_ROOT = path.join(ROOT, "test");
const WORKERD_ROOT = path.join(TEST_ROOT, "workerd");
const WORKERD_SUFFIX = ".harness.test.ts";
const ISOLATED_NATIVE_TESTS = new Set(["native_reshard_benchmark_producer.test.ts"]);
const RESTART_IN_PLACE_HARNESS = "file-store.harness.test.ts";
const TERMINATION_GRACE_MS = 2_000;
const WORKERD_PROCESS_SETTLE_MS = 5_000;
const OUTPUT_TAIL_LIMIT = 64 * 1024;

export class ChildProcessFailure extends Error {
    constructor(
        message,
        { exitCode = null, signalCode = null, timedOut = false, stdoutTail = "", stderrTail = "" } = {}
    ) {
        super(message);
        this.name = "ChildProcessFailure";
        this.exitCode = exitCode;
        this.signalCode = signalCode;
        this.timedOut = timedOut;
        this.stdoutTail = stdoutTail;
        this.stderrTail = stderrTail;
    }
}

export function isTransientWorkerdStartupFailure(error) {
    if (!(error instanceof ChildProcessFailure)) return false;
    const output = `${error.stdoutTail}\n${error.stderrTail}`;
    return /(?:broken pipe|\bEPIPE\b|address already in use|failed to start (?:the )?server|workers runtime failed to start|workerd[^\n]*(?:failed to start|startup failure))/i.test(
        output
    );
}

// The outer timeout is a hang guard for one harness process. Individual cases
// get caseTimeoutMs below; the file budget must leave room for several slow
// cases because hosted runners have run ten times slower than a quiet machine.
function outerTimeoutMs() {
    const raw = process.env.CHARDB_WORKERD_TEST_TIMEOUT_MS ?? "600000";
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(
            `CHARDB_WORKERD_TEST_TIMEOUT_MS must be a positive safe integer, received ${JSON.stringify(raw)}`
        );
    }
    return value;
}

// Per-test timeout handed to `bun test --timeout` for workerd harnesses and the
// isolated native test. Bun's 5 s default fits unit tests, not cases that drive
// real workerd instances through hundreds of registrations.
export function caseTimeoutMs(outerMs = outerTimeoutMs()) {
    const raw = process.env.CHARDB_WORKERD_CASE_TIMEOUT_MS ?? "60000";
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(
            `CHARDB_WORKERD_CASE_TIMEOUT_MS must be a positive safe integer, received ${JSON.stringify(raw)}`
        );
    }
    if (value >= outerMs) {
        throw new Error(
            `CHARDB_WORKERD_CASE_TIMEOUT_MS (${value}) must be below CHARDB_WORKERD_TEST_TIMEOUT_MS (${outerMs})`
        );
    }
    return value;
}

function workerdAttempts() {
    const raw = process.env.CHARDB_WORKERD_TEST_ATTEMPTS ?? "3";
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < 1 || value > 3) {
        throw new Error(
            `CHARDB_WORKERD_TEST_ATTEMPTS must be an integer from 1 through 3, received ${JSON.stringify(raw)}`
        );
    }
    return value;
}

async function filesUnder(directory) {
    const entries = await readdir(directory, { withFileTypes: true });
    const files = [];
    for (const entry of entries) {
        const file = path.join(directory, entry.name);
        if (entry.isDirectory()) files.push(...(await filesUnder(file)));
        else files.push(file);
    }
    return files;
}

function relative(file) {
    return path.relative(ROOT, file).split(path.sep).join("/");
}

export function compareWorkerdHarnesses(left, right) {
    const leftRestarts = path.basename(left) === RESTART_IN_PLACE_HARNESS;
    const rightRestarts = path.basename(right) === RESTART_IN_PLACE_HARNESS;
    if (leftRestarts !== rightRestarts) return leftRestarts ? -1 : 1;
    return left.localeCompare(right);
}

export function isIsolatedNativeTest(file) {
    return ISOLATED_NATIVE_TESTS.has(path.basename(file));
}

export async function run(label, args, timeoutMs, options = {}) {
    console.log(`\n> ${label}`);
    if (options.captureOutput && (options.stdout !== undefined || options.stderr !== undefined)) {
        throw new Error("captureOutput cannot be combined with explicit stdout or stderr options");
    }
    try {
        await runManagedCommand(args[0], args.slice(1), {
            label,
            timeoutMs,
            cwd: options.cwd ?? ROOT,
            env: options.env ?? process.env,
            stdin: options.stdin ?? "inherit",
            ...(options.captureOutput
                ? {}
                : { stdout: options.stdout ?? "inherit", stderr: options.stderr ?? "inherit" }),
            captureOutput: options.captureOutput ?? false,
            mirrorOutput: options.captureOutput ?? false,
            outputLimit: OUTPUT_TAIL_LIMIT,
            graceMs: options.terminationGraceMs ?? TERMINATION_GRACE_MS,
            signalSource: options.signalSource ?? process,
        });
    } catch (error) {
        if (!(error instanceof ManagedProcessError)) throw error;
        const failure = new ChildProcessFailure(error.message, {
            exitCode: error.timedOut || error.signalCode !== null ? null : error.exitCode,
            signalCode: error.timedOut ? null : error.signalCode,
            timedOut: error.timedOut,
            stdoutTail: error.stdout,
            stderrTail: error.stderr,
        });
        if (error.cause !== undefined) failure.cause = error.cause;
        throw failure;
    }
}

export async function runWithRetries(label, args, timeoutMs, options = {}) {
    const { attempts = 1, retryDelayMs = 250, shouldRetry = () => true, ...runOptions } = options;
    if (!Number.isSafeInteger(attempts) || attempts < 1 || attempts > 3) {
        throw new Error(`attempts must be an integer from 1 through 3, received ${JSON.stringify(attempts)}`);
    }
    if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
        throw new Error(`retryDelayMs must be a non-negative finite number, received ${JSON.stringify(retryDelayMs)}`);
    }
    for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
            return await run(
                attempt === 1 ? label : `${label} (retry ${attempt}/${attempts})`,
                args,
                timeoutMs,
                runOptions
            );
        } catch (error) {
            const retryable =
                error instanceof ChildProcessFailure &&
                !error.timedOut &&
                error.signalCode === null &&
                error.exitCode !== null &&
                shouldRetry(error) &&
                attempt < attempts;
            if (!retryable) throw error;
            console.warn(`${label} hit a recognized startup failure; retrying from a clean process group`);
            if (retryDelayMs > 0) await Bun.sleep(retryDelayMs);
        }
    }
}

export async function main(argv = process.argv.slice(2)) {
    const watch = argv.includes("--watch");
    const allTests = (await filesUnder(TEST_ROOT))
        .filter(file => file.endsWith(".test.ts") || file.endsWith(".test.tsx"))
        .sort();
    const workerdTests = allTests
        .filter(file => file.startsWith(`${WORKERD_ROOT}${path.sep}`) && file.endsWith(WORKERD_SUFFIX))
        .sort(compareWorkerdHarnesses);
    const isolatedNativeTests = watch ? [] : allTests.filter(isIsolatedNativeTest);
    const nonWorkerdTests = allTests.filter(
        file => !workerdTests.includes(file) && !isolatedNativeTests.includes(file)
    );

    if (nonWorkerdTests.length === 0) throw new Error("No non-workerd correctness tests found");
    if (!watch && workerdTests.length === 0) throw new Error("No workerd correctness harnesses found");
    if (!watch && isolatedNativeTests.length !== ISOLATED_NATIVE_TESTS.size) {
        throw new Error("An isolated native correctness test is missing");
    }

    await run(watch ? "non-workerd correctness tests in watch mode" : "non-workerd correctness tests", [
        "bun",
        "test",
        ...(watch ? ["--watch"] : []),
        ...nonWorkerdTests.map(relative),
    ]);

    if (!watch) {
        const timeoutMs = outerTimeoutMs();
        const caseTimeout = ["--timeout", String(caseTimeoutMs(timeoutMs))];
        const attempts = workerdAttempts();
        for (const test of isolatedNativeTests) {
            const file = relative(test);
            await run(file, ["bun", "test", ...caseTimeout, file], timeoutMs);
            await Bun.sleep(WORKERD_PROCESS_SETTLE_MS);
        }
        for (const [index, harness] of workerdTests.entries()) {
            const file = relative(harness);
            await runWithRetries(file, ["bun", "test", ...caseTimeout, file], timeoutMs, {
                attempts,
                retryDelayMs: WORKERD_PROCESS_SETTLE_MS,
                captureOutput: true,
                shouldRetry: isTransientWorkerdStartupFailure,
            });
            if (index + 1 < workerdTests.length) await Bun.sleep(WORKERD_PROCESS_SETTLE_MS);
        }
    }
}

if (import.meta.main) {
    try {
        await main();
    } catch (error) {
        console.error(error instanceof Error ? error.message : error);
        if (error instanceof ChildProcessFailure) {
            if (error.signalCode !== null) {
                process.kill(process.pid, error.signalCode);
                await new Promise(() => {});
            }
            process.exit(error.exitCode ?? 1);
        }
        process.exit(1);
    }
}
