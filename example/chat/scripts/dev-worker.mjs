import { join } from "node:path";

const origin = new URL(process.env.CHARDB_URL ?? "http://127.0.0.1:8787");
const adminToken = process.env.CHARDB_ADMIN_TOKEN ?? "local-chardb-admin";
const authSecret = process.env.BETTER_AUTH_SECRET ?? "local-chardb-auth-secret-that-is-at-least-32-characters";
const wranglerBin = join(process.cwd(), "node_modules", ".bin", "wrangler");
const chardbBin = join(process.cwd(), "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs");

const worker = Bun.spawn(
    [
        wranglerBin,
        "dev",
        "--config",
        "wrangler.template.toml",
        ...(process.env.CHARDB_PERSIST_TO ? ["--persist-to", process.env.CHARDB_PERSIST_TO] : []),
        "--ip",
        origin.hostname,
        "--port",
        origin.port || "8787",
        "--var",
        `CDB_ADMIN_TOKEN:${adminToken}`,
        "--var",
        `BETTER_AUTH_SECRET:${authSecret}`,
    ],
    { stdin: "inherit", stdout: "inherit", stderr: "inherit", detached: process.platform !== "win32" }
);

function processGroupExists(pid) {
    if (process.platform === "win32") return false;
    try {
        process.kill(-pid, 0);
        return true;
    } catch (error) {
        if (error && typeof error === "object" && "code" in error && error.code === "ESRCH") return false;
        if (error && typeof error === "object" && "code" in error && error.code === "EPERM") return true;
        throw error;
    }
}

async function waitForProcessGroupExit(pid, waitMs) {
    const deadline = performance.now() + waitMs;
    while (processGroupExists(pid) && performance.now() < deadline) await Bun.sleep(10);
    return !processGroupExists(pid);
}

async function terminateProcessGroup(signal) {
    if (process.platform === "win32") {
        worker.kill(signal);
        await Promise.race([worker.exited, Bun.sleep(2_000)]);
        if (worker.exitCode === null) worker.kill("SIGKILL");
        return;
    }
    try {
        process.kill(-worker.pid, signal);
    } catch (error) {
        if (error && typeof error === "object" && "code" in error && error.code === "ESRCH") return;
        throw error;
    }
    if (await waitForProcessGroupExit(worker.pid, 2_000)) {
        await worker.exited;
        return;
    }
    try {
        process.kill(-worker.pid, "SIGKILL");
    } catch (error) {
        if (!(error && typeof error === "object" && "code" in error && error.code === "ESRCH")) throw error;
    }
    if (!(await waitForProcessGroupExit(worker.pid, 2_000))) {
        throw new Error(`process group ${worker.pid} survived SIGKILL`);
    }
    await worker.exited;
}

let termination;
const stop = signal => {
    termination ??= terminateProcessGroup(signal);
    void termination.catch(() => {});
    return termination;
};
process.on("SIGINT", () => void stop("SIGINT"));
process.on("SIGTERM", () => void stop("SIGTERM"));

async function waitForWorker() {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
        if (worker.exitCode !== null) throw new Error(`wrangler exited with status ${worker.exitCode}`);
        let response;
        try {
            response = await fetch(new URL("/health", origin));
        } catch {
            // Wrangler has not opened its local listener yet.
            await Bun.sleep(100);
            continue;
        }
        if (response.ok) {
            let body;
            try {
                body = await response.json();
            } catch {
                throw new Error("/health returned invalid JSON");
            }
            if (
                body &&
                typeof body === "object" &&
                body.ok === true &&
                Number.isSafeInteger(body.schemaVersion) &&
                body.schemaVersion >= 1
            ) {
                return body.schemaVersion;
            }
            throw new Error("/health returned an invalid schema version");
        }
        await Bun.sleep(100);
    }
    throw new Error(`timed out waiting for ${origin.origin}/health`);
}

async function applyMigrations(targetVersion) {
    const migration = Bun.spawn(
        [
            "bun",
            chardbBin,
            "migrate",
            "--url",
            origin.origin,
            "--id",
            `chat-local-schema-v${targetVersion}`,
            "--target",
            String(targetVersion),
            "--concurrency",
            "4",
        ],
        {
            env: { ...process.env, CHARDB_ADMIN_TOKEN: adminToken },
            stdout: "inherit",
            stderr: "inherit",
        }
    );
    const exitCode = await migration.exited;
    if (exitCode !== 0) throw new Error(`chardb migrate exited with status ${exitCode}`);
}

try {
    const targetVersion = await waitForWorker();
    await applyMigrations(targetVersion);
    console.log(`chat Worker ready at ${origin.origin} with schema version ${targetVersion}`);
    process.exitCode = await worker.exited;
    await stop("SIGTERM");
} catch (error) {
    await stop("SIGTERM");
    await worker.exited;
    throw error;
}
