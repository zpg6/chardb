import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";
import { isOccupiedPortFailure } from "./helpers/windows-port-collision.ts";

if (process.platform !== "win32") throw new Error("windows-dev-tree.mjs must run on Windows");

const CHILD_PROCESS_TIMEOUT_MS = 5 * 60_000;
const FILESYSTEM_CLEANUP_TIMEOUT_MS = 30_000;
const HTTP_REQUEST_TIMEOUT_MS = 10_000;
const PROCESS_OUTPUT_TIMEOUT_MS = 5_000;
// Win32_Process enumeration can briefly stall on hosted Windows runners while
// toolchains are starting or exiting. It remains bounded, but needs enough
// headroom to avoid treating that transient WMI contention as a dev-server failure.
const WINDOWS_UTILITY_TIMEOUT_MS = 30_000;

const tarballArgument = process.argv.indexOf("--tarball");
if (tarballArgument === -1 || !process.argv[tarballArgument + 1]) {
    throw new Error("usage: bun test/windows-dev-tree.mjs --tarball <package.tgz> --react-tarball <react.tgz>");
}
const reactTarballArgument = process.argv.indexOf("--react-tarball");
if (reactTarballArgument === -1 || !process.argv[reactTarballArgument + 1]) {
    throw new Error("--react-tarball <package.tgz> is required");
}
const tarball = resolve(process.argv[tarballArgument + 1]);
const reactTarball = resolve(process.argv[reactTarballArgument + 1]);
const root = await mkdtemp(join(tmpdir(), "chardb-windows-dev-tree-"));
const bootstrap = join(root, "bootstrap");
const project = join(bootstrap, "generated-app");
const corePackage = `file:${relative(project, tarball).replaceAll("\\", "/")}`;
const reactPackage = `file:${relative(project, reactTarball).replaceAll("\\", "/")}`;
const persistTo = join(project, ".wrangler", "state");
const messageId = `windows-persistence-${Date.now().toString(36)}`;
let identity;

try {
    await mkdir(bootstrap, { recursive: true });
    await writeFile(join(bootstrap, "package.json"), '{"name":"chardb-windows-bootstrap","private":true}\n');
    await runPhase("install packed core candidate", () =>
        mustRun(process.execPath, ["add", "--no-save", tarball], bootstrap)
    );
    const chardb = join(bootstrap, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs");
    const candidatePackage = JSON.parse(
        await readFile(join(bootstrap, "node_modules", "@chardb", "core", "package.json"), "utf8")
    );
    await runPhase("scaffold generated app", () =>
        mustRun(
            process.execPath,
            [chardb, "init", "generated-app", "--core-package", corePackage, "--react-package", reactPackage],
            bootstrap
        )
    );

    const packageJson = JSON.parse(await readFile(join(project, "package.json"), "utf8"));
    if (packageJson.dependencies?.["@chardb/core"] !== corePackage) {
        throw new Error(
            `generated @chardb/core specifier ${String(packageJson.dependencies?.["@chardb/core"])} does not match ${corePackage}`
        );
    }
    if (packageJson.dependencies?.["@chardb/react"] !== reactPackage) {
        throw new Error(
            `generated @chardb/react specifier ${String(packageJson.dependencies?.["@chardb/react"])} does not match ${reactPackage}`
        );
    }
    await runPhase("install generated app", () => mustRun(process.execPath, ["install"], project));
    const installedPackage = JSON.parse(
        await readFile(join(project, "node_modules", "@chardb", "core", "package.json"), "utf8")
    );
    const installedReactPackage = JSON.parse(
        await readFile(join(project, "node_modules", "@chardb", "react", "package.json"), "utf8")
    );
    if (installedPackage.name !== candidatePackage.name || installedPackage.version !== candidatePackage.version) {
        throw new Error(
            `generated app installed ${String(installedPackage.name)}@${String(installedPackage.version)} instead of the packed candidate`
        );
    }
    if (installedReactPackage.name !== "@chardb/react") {
        throw new Error("generated app did not install the packed @chardb/react candidate");
    }
    const installedChardb = join(project, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs");
    const [candidateCliBytes, installedCliBytes] = await Promise.all([readFile(chardb), readFile(installedChardb)]);
    if (!candidateCliBytes.equals(installedCliBytes)) {
        throw new Error("generated app did not install the CLI bytes used to create it");
    }
    await runPhase("typecheck generated app", () => mustRun(process.execPath, ["run", "typecheck"], project));
    await runPhase("test generated app", () => mustRun(process.execPath, ["run", "test"], project));
    await runPhase("build generated app", () => mustRun(process.execPath, ["run", "build"], project));
    await runPhase("run generated Wrangler doctor", () =>
        mustRun(process.execPath, [installedChardb, "doctor", "wrangler"], project)
    );

    const blockedWorker = await listen(0);
    const blockedAddress = blockedWorker.address();
    if (!blockedAddress || typeof blockedAddress === "string") throw new Error("blocked worker port is unavailable");
    const failedWebPort = await reservePort();
    await runPhase("prove occupied Worker port cleanup", () =>
        proveOccupiedPortFailure({
            blockedServer: blockedWorker,
            blockedPort: blockedAddress.port,
            workerPort: blockedAddress.port,
            webPort: failedWebPort,
            service: "Worker",
            timeoutMs: 10_000,
        })
    );

    const failedWorkerPort = await reservePort();
    const blockedWeb = await listen(0);
    const blockedWebAddress = blockedWeb.address();
    if (!blockedWebAddress || typeof blockedWebAddress === "string") throw new Error("blocked web port is unavailable");
    await runPhase("prove occupied Web port cleanup", () =>
        proveOccupiedPortFailure({
            blockedServer: blockedWeb,
            blockedPort: blockedWebAddress.port,
            workerPort: failedWorkerPort,
            webPort: blockedWebAddress.port,
            service: "Web",
            timeoutMs: 30_000,
        })
    );

    for (let iteration = 0; iteration < 3; iteration++) {
        const cycle = iteration + 1;
        const workerPort = await reservePort();
        const webPort = await reservePort();
        const workerOrigin = new URL(`http://127.0.0.1:${workerPort}`);
        const child = spawnGeneratedDev(workerPort, webPort);
        try {
            await runPhase(`restart cycle ${cycle}: wait for Worker and Web`, () =>
                Promise.all([
                    waitForUrl(new URL("/health", workerOrigin), child),
                    waitForUrl(`http://127.0.0.1:${webPort}`, child),
                ])
            );
            if (iteration === 0) {
                identity = await runPhase(`restart cycle ${cycle}: provision organization`, () =>
                    provisionOrganization(workerOrigin)
                );
                await runPhase(`restart cycle ${cycle}: write organization data`, () =>
                    writeMessage(workerOrigin, identity.token, identity.organizationId, messageId)
                );
            } else {
                if (!identity) throw new Error("persistence identity was not provisioned");
                identity.token = await runPhase(`restart cycle ${cycle}: refresh auth token`, () =>
                    issueToken(workerOrigin, identity.cookie)
                );
            }
            if (!identity) throw new Error("persistence identity is unavailable");
            await runPhase(`restart cycle ${cycle}: read persisted organization data`, () =>
                assertMessage(workerOrigin, identity.token, identity.organizationId, messageId)
            );

            const before = await runPhase(`restart cycle ${cycle}: snapshot descendants`, processSnapshot);
            const descendants = descendantProcesses(before, child.pid);
            if (descendants.length < 3) {
                throw new Error(`iteration ${iteration + 1} observed only ${descendants.length} dev descendants`);
            }

            const stopped = await runPhase(`restart cycle ${cycle}: force-stop dev parent`, () =>
                runUtility(["taskkill.exe", "/PID", String(child.pid), "/F"])
            );
            if (stopped.exitCode !== 0) {
                throw new Error(
                    `iteration ${iteration + 1} could not force-stop the dev parent: ${stopped.stderr.trim()}`
                );
            }
            const exited = await Promise.race([child.exited.then(() => true), Bun.sleep(5_000).then(() => false)]);
            if (!exited) throw new Error(`iteration ${iteration + 1} dev parent survived forced termination`);
            await runPhase(`restart cycle ${cycle}: verify descendant cleanup`, () =>
                waitForProcessesToExit(descendants, 5_000)
            );
            await runPhase(`restart cycle ${cycle}: verify project cleanup`, () =>
                waitForNoProjectProcesses(project, 5_000)
            );
            await runPhase(`restart cycle ${cycle}: verify port reuse`, () =>
                Promise.all([assertPortReusable(workerPort), assertPortReusable(webPort)])
            );
        } finally {
            if (child.exitCode === null) {
                await runUtility(["taskkill.exe", "/PID", String(child.pid), "/T", "/F"]).catch(() => undefined);
                await Promise.race([child.exited, Bun.sleep(2_000)]);
            }
        }
    }

    function spawnGeneratedDev(workerPort, webPort, output = "inherit") {
        return Bun.spawn([process.execPath, "run", "dev"], {
            cwd: project,
            env: {
                ...process.env,
                CHARDB_DEV_URL: `http://127.0.0.1:${workerPort}`,
                CHARDB_DEV_WEB_URL: `http://127.0.0.1:${webPort}`,
                CHARDB_DEV_PERSIST_TO: persistTo,
                WRANGLER_SEND_METRICS: "false",
            },
            stdin: "ignore",
            stdout: output,
            stderr: output,
            windowsHide: true,
        });
    }

    async function proveOccupiedPortFailure({ blockedServer, blockedPort, workerPort, webPort, service, timeoutMs }) {
        const failedStartup = spawnGeneratedDev(workerPort, webPort, "pipe");
        const failedStdout = drainStream(failedStartup.stdout);
        const failedStderr = drainStream(failedStartup.stderr);
        try {
            const exited = await Promise.race([
                failedStartup.exited.then(() => true),
                Bun.sleep(timeoutMs).then(() => false),
            ]);
            if (!exited) throw new Error(`generated dev did not exit after ${service} startup failure`);
            if (failedStartup.exitCode === 0) throw new Error(`generated dev accepted an occupied ${service} port`);
            const [stdout, stderr] = await withTimeout(
                Promise.all([failedStdout.promise, failedStderr.promise]),
                PROCESS_OUTPUT_TIMEOUT_MS,
                `${service} collision output`
            );
            const output = `${stdout}\n${stderr}`;
            if (!isOccupiedPortFailure(output, blockedPort)) {
                throw new Error(
                    `generated dev failed for an unrelated reason instead of occupied ${service} port ${blockedPort}: ${output}`
                );
            }
        } finally {
            await closeServer(blockedServer);
            if (failedStartup.exitCode === null) {
                await runUtility(["taskkill.exe", "/PID", String(failedStartup.pid), "/T", "/F"]).catch(
                    () => undefined
                );
                await Promise.race([failedStartup.exited, Bun.sleep(2_000)]);
            }
            await Promise.all([failedStdout.cancel(), failedStderr.cancel()]);
        }
        await waitForNoProjectProcesses(project, 5_000);
        await assertPortReusable(workerPort);
        await assertPortReusable(webPort);
    }
} finally {
    await runPhase("remove temporary generated app", () =>
        withTimeout(
            rm(root, { recursive: true, force: true }),
            FILESYSTEM_CLEANUP_TIMEOUT_MS,
            "temporary generated app cleanup"
        )
    );
}

async function mustRun(command, args, cwd) {
    const child = Bun.spawn([command, ...args], { cwd, stdin: "ignore", stdout: "inherit", stderr: "inherit" });
    let exitCode;
    try {
        exitCode = await withTimeout(child.exited, CHILD_PROCESS_TIMEOUT_MS, `${command} ${args.join(" ")}`);
    } catch (error) {
        await runUtility(["taskkill.exe", "/PID", String(child.pid), "/T", "/F"]).catch(() => undefined);
        if (child.exitCode === null) child.kill("SIGKILL");
        await withTimeout(child.exited, 2_000, `${command} termination`).catch(() => undefined);
        throw error;
    }
    if (exitCode !== 0) throw new Error(`${command} exited with status ${exitCode}`);
}

async function runUtility(command) {
    const child = Bun.spawn(command, { stdin: "ignore", stdout: "pipe", stderr: "pipe", windowsHide: true });
    const stdout = drainStream(child.stdout);
    const stderr = drainStream(child.stderr);
    const label = command.join(" ");
    try {
        const exitCode = await withTimeout(child.exited, WINDOWS_UTILITY_TIMEOUT_MS, label);
        const output = await withTimeout(
            Promise.all([stdout.promise, stderr.promise]),
            PROCESS_OUTPUT_TIMEOUT_MS,
            `${label} output`
        );
        return { exitCode, stdout: output[0], stderr: output[1] };
    } catch (error) {
        if (child.exitCode === null) child.kill("SIGKILL");
        await withTimeout(child.exited, 2_000, `${label} termination`).catch(() => undefined);
        throw error;
    } finally {
        await Promise.all([stdout.cancel(), stderr.cancel()]);
    }
}

function drainStream(stream) {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    const promise = (async () => {
        let output = "";
        while (true) {
            const chunk = await reader.read();
            if (chunk.done) return output + decoder.decode();
            output += decoder.decode(chunk.value, { stream: true });
        }
    })();
    void promise.catch(() => undefined);
    return {
        promise,
        cancel: async () => {
            await withTimeout(
                reader.cancel().catch(() => undefined),
                PROCESS_OUTPUT_TIMEOUT_MS,
                "process output cancellation"
            ).catch(() => undefined);
            try {
                reader.releaseLock();
            } catch {
                // A timed-out read can retain its lock without blocking harness cleanup.
            }
        },
    };
}

async function runPhase(label, operation) {
    const started = performance.now();
    console.log(`[windows-dev-tree] ${label}: start`);
    try {
        const result = await operation();
        console.log(`[windows-dev-tree] ${label}: passed (${Math.round(performance.now() - started)}ms)`);
        return result;
    } catch (error) {
        console.error(`[windows-dev-tree] ${label}: failed (${Math.round(performance.now() - started)}ms)`);
        throw error;
    }
}

async function withTimeout(promise, timeoutMs, label) {
    let timer;
    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function processSnapshot() {
    const script = [
        "$ErrorActionPreference = 'Stop'",
        "@(Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId, CreationDate, CommandLine) | ConvertTo-Json -Compress",
    ].join("; ");
    const result = await runUtility(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script]);
    if (result.exitCode !== 0) throw new Error(`PowerShell process enumeration failed: ${result.stderr.trim()}`);
    const parsed = JSON.parse(result.stdout || "[]");
    return (Array.isArray(parsed) ? parsed : [parsed])
        .map(row => ({
            pid: Number(row.ProcessId),
            parentPid: Number(row.ParentProcessId),
            createdAt: typeof row.CreationDate === "string" ? row.CreationDate : "",
            commandLine: typeof row.CommandLine === "string" ? row.CommandLine : "",
        }))
        .filter(
            row =>
                Number.isSafeInteger(row.pid) &&
                row.pid > 0 &&
                Number.isSafeInteger(row.parentPid) &&
                row.createdAt.length > 0
        );
}

function descendantProcesses(snapshot, rootPid) {
    const children = new Map();
    for (const row of snapshot) {
        const entries = children.get(row.parentPid) ?? [];
        entries.push(row);
        children.set(row.parentPid, entries);
    }
    const result = [];
    const pending = [...(children.get(rootPid) ?? [])];
    const seen = new Set([rootPid]);
    while (pending.length > 0) {
        const process = pending.pop();
        if (seen.has(process.pid)) continue;
        seen.add(process.pid);
        result.push(process);
        pending.push(...(children.get(process.pid) ?? []));
    }
    return result;
}

async function waitForProcessesToExit(processes, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let remaining = [...processes];
    while (remaining.length > 0 && Date.now() < deadline) {
        const live = new Map((await processSnapshot()).map(row => [row.pid, row.createdAt]));
        remaining = remaining.filter(process => live.get(process.pid) === process.createdAt);
        if (remaining.length > 0) await Bun.sleep(50);
    }
    if (remaining.length > 0) {
        throw new Error(`dev descendants survived shutdown: ${remaining.map(process => process.pid).join(", ")}`);
    }
}

async function waitForNoProjectProcesses(directory, timeoutMs) {
    const needle = directory.toLowerCase();
    const deadline = Date.now() + timeoutMs;
    let leftovers = [];
    do {
        leftovers = (await processSnapshot()).filter(
            row => row.pid !== process.pid && row.commandLine.toLowerCase().includes(needle)
        );
        if (leftovers.length === 0) return;
        await Bun.sleep(50);
    } while (Date.now() < deadline);
    throw new Error(`generated dev processes survived shutdown: ${leftovers.map(row => row.pid).join(", ")}`);
}

async function provisionOrganization(origin) {
    let cookie = await signIn(origin);
    const created = await postAuthJson(origin, "organization/create", cookie, {
        name: "Windows persistence proof",
        slug: `windows-proof-${Date.now().toString(36)}`,
        keepCurrentActiveOrganization: true,
    });
    cookie = created.cookie;
    const organizationId = created.body?.id;
    if (typeof organizationId !== "string" || organizationId.length === 0) {
        throw new Error(`organization/create returned no id: ${JSON.stringify(created.body)}`);
    }
    const activated = await postAuthJson(origin, "organization/set-active", cookie, { organizationId });
    cookie = activated.cookie;
    return { cookie, organizationId, token: await issueToken(origin, cookie) };
}

async function signIn(origin) {
    const response = await fetchWithTimeout(new URL("/api/auth/sign-in/anonymous", origin), {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    const text = await readResponseText(response);
    if (!response.ok) throw new Error(`anonymous sign-in failed (${response.status}): ${text}`);
    const cookie = sessionCookies(response.headers);
    if (!cookie) throw new Error("anonymous sign-in returned no session cookie");
    return cookie;
}

async function postAuthJson(origin, path, cookie, body) {
    const response = await fetchWithTimeout(new URL(`/api/auth/${path}`, origin), {
        method: "POST",
        headers: { "content-type": "application/json", cookie, origin: origin.origin },
        body: JSON.stringify(body),
    });
    const text = await readResponseText(response);
    let parsed;
    try {
        parsed = JSON.parse(text);
    } catch {
        throw new Error(`Better Auth ${path} returned invalid JSON (${response.status}): ${text}`);
    }
    if (!response.ok) throw new Error(`Better Auth ${path} failed (${response.status}): ${text}`);
    return { body: parsed, cookie: mergeCookies(cookie, response.headers) };
}

async function issueToken(origin, cookie) {
    const response = await fetchJson(new URL("/api/auth/token", origin), { headers: { cookie } });
    if (!response.response.ok || typeof response.body?.token !== "string") {
        throw new Error(`JWT issue failed: ${JSON.stringify(response.body)}`);
    }
    return response.body.token;
}

async function writeMessage(origin, token, organizationId, id) {
    const response = await fetchJson(new URL("/api/messages", origin), {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ id, organizationId, body: "survives Windows restart", clientCreatedAt: Date.now() }),
    });
    if (!response.response.ok || response.body?.id !== id) {
        throw new Error(`message write failed: ${JSON.stringify(response.body)}`);
    }
}

async function assertMessage(origin, token, organizationId, expectedId) {
    const url = new URL("/api/messages", origin);
    url.searchParams.set("organizationId", organizationId);
    const response = await fetchJson(url, { headers: { authorization: `Bearer ${token}` } });
    if (!response.response.ok || !Array.isArray(response.body)) {
        throw new Error(`message read failed: ${JSON.stringify(response.body)}`);
    }
    if (!response.body.some(row => row?.id === expectedId && row.organizationId === organizationId)) {
        throw new Error(`persisted message ${expectedId} was not found: ${JSON.stringify(response.body)}`);
    }
}

async function fetchJson(url, init) {
    const response = await fetchWithTimeout(url, init);
    const text = await readResponseText(response);
    let body;
    try {
        body = JSON.parse(text);
    } catch {
        throw new Error(`${url.pathname} returned invalid JSON (${response.status}): ${text}`);
    }
    return { response, body };
}

function fetchWithTimeout(url, init = {}) {
    return fetch(url, { ...init, signal: init.signal ?? AbortSignal.timeout(HTTP_REQUEST_TIMEOUT_MS) });
}

function readResponseText(response) {
    return withTimeout(response.text(), HTTP_REQUEST_TIMEOUT_MS, `${response.url} response body`);
}

function sessionCookies(headers) {
    const values = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [headers.get("set-cookie")];
    return values
        .filter(Boolean)
        .map(value => value.split(";", 1)[0])
        .join("; ");
}

function mergeCookies(current, headers) {
    const cookies = new Map();
    for (const source of [current, sessionCookies(headers)]) {
        for (const pair of source.split(";")) {
            const trimmed = pair.trim();
            if (!trimmed) continue;
            const separator = trimmed.indexOf("=");
            if (separator <= 0) continue;
            cookies.set(trimmed.slice(0, separator), trimmed);
        }
    }
    return [...cookies.values()].join("; ");
}

async function waitForUrl(url, child) {
    const deadline = Date.now() + 45_000;
    while (Date.now() < deadline) {
        if (child.exitCode !== null) throw new Error(`generated dev exited before ${url} became ready`);
        try {
            const response = await fetchWithTimeout(url);
            if (response.ok) return;
        } catch {
            // The generated server has not opened its listener yet.
        }
        await Bun.sleep(100);
    }
    throw new Error(`timed out waiting for ${url}`);
}

async function reservePort() {
    const server = await listen(0);
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("port reservation did not expose an address");
    await closeServer(server);
    return address.port;
}

async function assertPortReusable(port) {
    const server = await listen(port);
    await closeServer(server);
}

async function listen(port) {
    const server = createServer();
    await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(port, "127.0.0.1", resolve);
    });
    return server;
}

async function closeServer(server) {
    await new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
}
