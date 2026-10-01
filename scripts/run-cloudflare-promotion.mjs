import { createHash, randomBytes, randomUUID } from "node:crypto";
import { chmod, cp, lstat, mkdir, readFile, readdir, rename, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import path from "node:path";
import { assertChatBenchmarkReport, compareChatBenchmarkReports } from "./chat-benchmark-report.mjs";
import { PREVIEW_SCHEMA_VERSION, PREVIEW_UPGRADE_SCHEMA_VERSION } from "./prepare-preview-upgrade.mjs";

const ROOT = path.resolve(import.meta.dirname, "..");
const REPORT_SCHEMA = "chardb.cloudflare-promotion.report.v1";
const PRIVATE_SCHEMA = "chardb.cloudflare-promotion.private.v1";
const STAGES = Object.freeze([
    "prepare",
    "validate",
    "deploy-v1",
    "activate-v1",
    "seed-v1",
    "deploy-v2",
    "fence-before-v2",
    "interrupt-v2",
    "redeploy-v2",
    "fence-after-redeploy",
    "resume-v2",
    "verify-v2",
    "deploy-obsolete-v1",
    "fence-obsolete-v1",
    "restore-v2",
    "verify-final",
    "benchmark",
    "benchmark-local",
    "finalize",
]);
const COMMAND_TIMEOUT_MS = 15 * 60_000;
const HTTP_TIMEOUT_MS = 30_000;

function value(argv, flag) {
    const index = argv.indexOf(flag);
    return index < 0 ? undefined : argv[index + 1];
}

export function parseCloudflarePromotionArgs(argv) {
    const allowed = new Set([
        "--tarball",
        "--react-tarball",
        "--worker",
        "--url",
        "--output",
        "--private-dir",
        "--migration-prefix",
        "--benchmark-samples",
        "--secrets-file",
        "--admin-token-file",
        "--help",
        "-h",
    ]);
    for (let index = 0; index < argv.length; index++) {
        const argument = argv[index];
        if (!allowed.has(argument)) throw new Error(`unknown promotion argument ${JSON.stringify(argument)}`);
        if (argument !== "--help" && argument !== "-h") index++;
    }
    const help = argv.includes("--help") || argv.includes("-h");
    const tarball = value(argv, "--tarball");
    const reactTarball = value(argv, "--react-tarball");
    const worker = value(argv, "--worker");
    const url = value(argv, "--url");
    const output = value(argv, "--output");
    const privateDir = value(argv, "--private-dir");
    const migrationPrefix = value(argv, "--migration-prefix");
    const secretsFile = value(argv, "--secrets-file");
    const adminTokenFile = value(argv, "--admin-token-file");
    const rawSamples = value(argv, "--benchmark-samples") ?? "3";
    const benchmarkSamples = Number(rawSamples);
    if (!help) {
        for (const [flag, item] of [
            ["--tarball", tarball],
            ["--react-tarball", reactTarball],
            ["--worker", worker],
            ["--url", url],
            ["--output", output],
            ["--private-dir", privateDir],
            ["--migration-prefix", migrationPrefix],
        ]) {
            if (!item) throw new Error(`${flag} is required`);
        }
        if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(worker)) {
            throw new Error("--worker must be a lowercase Cloudflare Worker name with at most 63 characters");
        }
        const origin = new URL(url);
        if (origin.protocol !== "https:" || origin.pathname !== "/" || origin.search || origin.hash) {
            throw new Error("--url must be an HTTPS origin with no path, query, or fragment");
        }
        if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$/.test(migrationPrefix)) {
            throw new Error("--migration-prefix is invalid");
        }
        if (!Number.isSafeInteger(benchmarkSamples) || benchmarkSamples < 0 || benchmarkSamples > 10) {
            throw new Error("--benchmark-samples must be an integer from 0 through 10");
        }
        if (Boolean(secretsFile) !== Boolean(adminTokenFile)) {
            throw new Error("--secrets-file and --admin-token-file must be provided together");
        }
        const outputPath = path.resolve(output);
        const privatePath = path.resolve(privateDir);
        if (outputPath === privatePath || privatePath.startsWith(`${outputPath}${path.sep}`)) {
            throw new Error("--private-dir must be outside the evidence output directory");
        }
    }
    return {
        help,
        tarball,
        reactTarball,
        worker,
        url,
        output,
        privateDir,
        migrationPrefix,
        benchmarkSamples,
        secretsFile,
        adminTokenFile,
    };
}

export function parsePromotionSecrets(contents, adminTokenContents) {
    const values = new Map();
    for (const rawLine of contents.split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line || line.startsWith("#")) continue;
        const separator = line.indexOf("=");
        if (separator <= 0) throw new Error("the supplied secrets file is not KEY=VALUE data");
        values.set(line.slice(0, separator).trim(), line.slice(separator + 1).trim());
    }
    const authSecret = values.get("BETTER_AUTH_SECRET");
    const adminToken = values.get("CDB_ADMIN_TOKEN");
    const tokenFile = adminTokenContents.trim();
    if (!authSecret || authSecret.length < 32) throw new Error("BETTER_AUTH_SECRET is missing or too short");
    if (!adminToken || adminToken.length < 32) throw new Error("CDB_ADMIN_TOKEN is missing or too short");
    if (tokenFile !== adminToken) throw new Error("the admin token file does not match CDB_ADMIN_TOKEN");
    return { authSecret, adminToken };
}

export function newestWranglerVersion(input) {
    if (!Array.isArray(input) || input.length === 0) throw new Error("Wrangler returned no Worker versions");
    const versions = input.map(item => {
        if (
            !item ||
            typeof item !== "object" ||
            !/^[a-f0-9-]{36}$/.test(item.id ?? "") ||
            !Number.isSafeInteger(item.number) ||
            item.number < 1
        ) {
            throw new Error("Wrangler returned an invalid Worker version");
        }
        return { id: item.id, number: item.number };
    });
    return versions.sort((left, right) => right.number - left.number)[0];
}

export function assertFullTrafficDeployment(input, expectedVersion) {
    if (!input || typeof input !== "object" || !/^[a-f0-9-]{36}$/.test(input.id ?? "")) {
        throw new Error("Wrangler returned an invalid deployment");
    }
    if (
        !Array.isArray(input.versions) ||
        input.versions.length !== 1 ||
        input.versions[0]?.version_id !== expectedVersion ||
        input.versions[0]?.percentage !== 100
    ) {
        throw new Error(`Worker version ${expectedVersion} does not have 100% traffic`);
    }
    return { deploymentId: input.id, versionId: expectedVersion, percentage: 100 };
}

export function wranglerVersionUploadArgs(worker, secretsFile, tag, message) {
    return ["versions", "upload", "--name", worker, "--secrets-file", secretsFile, "--tag", tag, "--message", message];
}

export function wranglerVersionDeployArgs(worker, versionId, message) {
    return ["versions", "deploy", `${versionId}@100`, "--name", worker, "--message", message, "--yes"];
}

export function wranglerInitialDeployArgs(worker, secretsFile, tag, message) {
    return ["deploy", "--name", worker, "--strict", "--secrets-file", secretsFile, "--tag", tag, "--message", message];
}

export async function retryUntil(check, options = {}) {
    const timeoutMs = options.timeoutMs ?? 45_000;
    const intervalMs = options.intervalMs ?? 500;
    const deadline = Date.now() + timeoutMs;
    let lastError;
    do {
        try {
            return await check();
        } catch (error) {
            lastError = error;
        }
        await new Promise(resolve => setTimeout(resolve, intervalMs));
    } while (Date.now() < deadline);
    throw lastError ?? new Error("readiness check timed out");
}

function usage() {
    return [
        "Usage: bun scripts/run-cloudflare-promotion.mjs [options]",
        "",
        "  --tarball <core.tgz>         exact @chardb/core candidate",
        "  --react-tarball <react.tgz>  exact @chardb/react candidate",
        "  --worker <name>             exact Cloudflare Worker name",
        "  --url <https-origin>        deployed Worker origin",
        "  --output <directory>        absent or resumable evidence directory",
        "  --private-dir <directory>   secret and build state outside evidence",
        "  --migration-prefix <id>     stable prefix for v1 and v2 migration IDs",
        "  --benchmark-samples <0-10>  samples per profile, default 3",
        "  --secrets-file <file>        reuse existing deployment secrets",
        "  --admin-token-file <file>    matching existing admin token",
    ].join("\n");
}

function sha256(bytes) {
    return createHash("sha256").update(bytes).digest("hex");
}

async function atomicJson(file, value, mode) {
    const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, mode === undefined ? undefined : { mode });
    if (mode !== undefined) await chmod(temporary, mode);
    await rename(temporary, file);
}

async function pathExists(file) {
    try {
        await lstat(file);
        return true;
    } catch (error) {
        if (error?.code === "ENOENT") return false;
        throw error;
    }
}

async function regularFiles(root, directory = root) {
    const files = [];
    for (const entry of (await readdir(directory, { withFileTypes: true })).sort((a, b) =>
        a.name.localeCompare(b.name)
    )) {
        const absolute = path.join(directory, entry.name);
        if (entry.isSymbolicLink()) continue;
        if (entry.isDirectory()) files.push(...(await regularFiles(root, absolute)));
        else if (entry.isFile()) files.push(path.relative(root, absolute).split(path.sep).join("/"));
    }
    return files;
}

async function fingerprintTree(root, included) {
    const records = [];
    for (const item of included) {
        const absolute = path.join(root, item);
        const stat = await lstat(absolute);
        if (stat.isDirectory()) {
            for (const relative of await regularFiles(root, absolute)) {
                const bytes = await readFile(path.join(root, ...relative.split("/")));
                records.push({ path: relative, bytes: bytes.byteLength, sha256: sha256(bytes) });
            }
        } else if (stat.isFile()) {
            const bytes = await readFile(absolute);
            records.push({ path: item, bytes: bytes.byteLength, sha256: sha256(bytes) });
        } else throw new Error(`deployment input ${item} is not a file or directory`);
    }
    records.sort((left, right) => left.path.localeCompare(right.path));
    return {
        algorithm: "sha256",
        digest: sha256(records.map(record => `${record.sha256}  ${record.path}\n`).join("")),
        files: records,
    };
}

async function fingerprintDeployment(app, tarballs) {
    const fingerprint = await fingerprintTree(app, [
        "package.json",
        "package-lock.json",
        "preview-manifest.json",
        "wrangler.toml",
        "src",
        "dist",
    ]);
    for (const tarball of tarballs) {
        const bytes = await readFile(tarball);
        fingerprint.files.push({
            path: `../${path.basename(tarball)}`,
            bytes: bytes.byteLength,
            sha256: sha256(bytes),
        });
    }
    fingerprint.files.sort((left, right) => left.path.localeCompare(right.path));
    fingerprint.digest = sha256(fingerprint.files.map(record => `${record.sha256}  ${record.path}\n`).join(""));
    return fingerprint;
}

function scrub(text, secrets) {
    let result = String(text);
    for (const secret of secrets) {
        if (secret) result = result.replaceAll(secret, "[redacted]");
    }
    return result.slice(-16_384);
}

async function runCommand(command, args, options) {
    const child = Bun.spawn([command, ...args], {
        cwd: options.cwd,
        env: options.env ?? process.env,
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
        detached: process.platform !== "win32",
    });
    const timeout = setTimeout(() => {
        try {
            if (process.platform === "win32") child.kill("SIGTERM");
            else process.kill(-child.pid, "SIGTERM");
        } catch {}
    }, options.timeoutMs ?? COMMAND_TIMEOUT_MS);
    const [stdout, stderr, exitCode] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
    ]);
    clearTimeout(timeout);
    if (exitCode !== 0) {
        throw new Error(
            `${options.label} exited with ${exitCode}\n${scrub(`${stdout}\n${stderr}`, options.secrets ?? [])}`
        );
    }
    return { stdout, stderr };
}

async function availablePort() {
    const server = createServer();
    await new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : null;
    await new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    if (!Number.isSafeInteger(port) || port < 1) throw new Error("could not reserve a local benchmark port");
    return port;
}

async function terminateProcess(child) {
    if (child.exitCode === null) {
        try {
            if (process.platform === "win32") child.kill("SIGTERM");
            else process.kill(-child.pid, "SIGTERM");
        } catch {}
    }
    const exited = await Promise.race([
        child.exited.then(() => true),
        new Promise(resolve => setTimeout(() => resolve(false), 5_000)),
    ]);
    if (!exited && child.exitCode === null) {
        try {
            if (process.platform === "win32") child.kill("SIGKILL");
            else process.kill(-child.pid, "SIGKILL");
        } catch {}
        await child.exited;
    }
}

async function requestJson(origin, pathname, init = {}) {
    const signal = AbortSignal.timeout(HTTP_TIMEOUT_MS);
    const response = await fetch(new URL(pathname, origin), { ...init, signal });
    const text = await response.text();
    let body;
    try {
        body = text ? JSON.parse(text) : null;
    } catch {
        body = text;
    }
    return { response, body };
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}

function sessionCookies(headers) {
    const values = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [headers.get("set-cookie")];
    return values
        .filter(Boolean)
        .map(item => item.split(";", 1)[0])
        .join("; ");
}

function mergeCookies(current, headers) {
    const cookies = new Map();
    for (const value of [current, sessionCookies(headers)]) {
        for (const cookie of value.split(/;\s*/)) {
            const separator = cookie.indexOf("=");
            if (separator > 0) cookies.set(cookie.slice(0, separator), cookie.slice(separator + 1));
        }
    }
    return [...cookies].map(([name, value]) => `${name}=${value}`).join("; ");
}

async function refreshPrincipal(origin, principal) {
    const session = await requestJson(origin, "/api/auth/get-session", { headers: { cookie: principal.cookie } });
    assert(session.response.ok && session.body?.user?.id, "Better Auth session refresh failed");
    principal.cookie = mergeCookies(principal.cookie, session.response.headers);
    principal.session = session.body;
    const token = await requestJson(origin, "/api/auth/token", { headers: { cookie: principal.cookie } });
    assert(token.response.ok && typeof token.body?.token === "string", "Better Auth token issue failed");
    principal.cookie = mergeCookies(principal.cookie, token.response.headers);
    principal.token = token.body.token;
}

async function authPost(origin, principal, pathname, body) {
    const result = await requestJson(origin, `/api/auth${pathname}`, {
        method: "POST",
        headers: { "content-type": "application/json", cookie: principal.cookie, origin: origin.origin },
        body: JSON.stringify(body),
    });
    assert(result.response.ok, `Better Auth ${pathname} failed with ${result.response.status}`);
    principal.cookie = mergeCookies(principal.cookie, result.response.headers);
    return result.body;
}

async function seedOrganization(origin) {
    const signIn = await requestJson(origin, "/api/auth/sign-in/anonymous", {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    assert(signIn.response.ok, `anonymous sign-in failed with ${signIn.response.status}`);
    const principal = { cookie: sessionCookies(signIn.response.headers), session: null, token: null };
    await refreshPrincipal(origin, principal);
    const suffix = `${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
    const organization = await authPost(origin, principal, "/organization/create", {
        name: "Cloudflare upgrade proof",
        slug: `upgrade-proof-${suffix}`,
        keepCurrentActiveOrganization: true,
    });
    assert(typeof organization?.id === "string", "organization creation returned no id");
    await authPost(origin, principal, "/organization/set-active", { organizationId: organization.id });
    await refreshPrincipal(origin, principal);
    const message = {
        id: `upgrade-before-${suffix}`,
        organizationId: organization.id,
        authorId: principal.session.user.id,
        body: `before deployed upgrade ${suffix}`,
        createdAt: Date.now(),
    };
    await writeMessage(origin, principal.token, message, `upgrade-before-${suffix}`);
    await assertMessages(origin, principal.token, organization.id, [message]);
    return { principal, organizationId: organization.id, messages: [message], blockedIds: [] };
}

function normalizeMessage(row) {
    return {
        id: row.id,
        organizationId: row.organizationId,
        authorId: row.authorId,
        body: row.body,
        createdAt: row.createdAt,
    };
}

async function assertMessages(origin, token, organizationId, expected) {
    const url = new URL("/api/messages", origin);
    url.searchParams.set("organizationId", organizationId);
    url.searchParams.set("limit", "100");
    const result = await requestJson(origin, `${url.pathname}${url.search}`, {
        headers: { authorization: `Bearer ${token}` },
    });
    assert(result.response.ok, `message read failed with ${result.response.status}`);
    const actual = Array.isArray(result.body) ? result.body.map(normalizeMessage) : result.body;
    const ordered = [...expected].sort(
        (left, right) => right.createdAt - left.createdAt || right.id.localeCompare(left.id)
    );
    assert(JSON.stringify(actual) === JSON.stringify(ordered), "message rows diverged during promotion");
}

async function writeMessage(origin, token, message, mutId) {
    const result = await requestJson(origin, "/api/messages", {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({
            id: message.id,
            organizationId: message.organizationId,
            body: message.body,
            clientCreatedAt: message.createdAt,
            mutId,
        }),
    });
    assert(
        result.response.ok && result.body?.id === message.id,
        `message mutation failed with ${result.response.status}`
    );
}

async function assertTrafficClosed(origin, token, organizationId, marker) {
    const readUrl = new URL("/api/messages", origin);
    readUrl.searchParams.set("organizationId", organizationId);
    const read = await requestJson(origin, `${readUrl.pathname}${readUrl.search}`, {
        headers: { authorization: `Bearer ${token}` },
    });
    assert(!read.response.ok, `${marker} authenticated read unexpectedly succeeded`);
    const blockedId = `blocked-${marker}-${randomUUID().slice(0, 8)}`;
    const write = await requestJson(origin, "/api/messages", {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({
            id: blockedId,
            organizationId,
            body: "must not commit",
            clientCreatedAt: Date.now(),
            mutId: blockedId,
        }),
    });
    assert(!write.response.ok, `${marker} authenticated mutation unexpectedly succeeded`);
    return { blockedId, readStatus: read.response.status, writeStatus: write.response.status };
}

async function adminRequest(origin, token, route, body) {
    const result = await requestJson(origin, `/_chardb/migrations/${route}`, {
        method: body === undefined ? "GET" : "POST",
        headers: {
            authorization: `Bearer ${token}`,
            ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    assert(
        result.response.ok,
        `migration ${route} failed with ${result.response.status}: ${JSON.stringify(result.body)}`
    );
    return result.body;
}

function assertSchemaState(body, expected) {
    const state = body?.state;
    for (const [key, value] of Object.entries(expected)) {
        assert(state?.[key] === value, `schema state ${key} drifted, expected ${JSON.stringify(value)}`);
    }
    return state;
}

export function classifyObsoleteControlPlane(status, body) {
    if (status >= 200 && status < 300) {
        assertSchemaState(body, { status: "active", activeVersion: PREVIEW_UPGRADE_SCHEMA_VERSION, activeEpoch: 3 });
        return "warm-v2-catalog";
    }
    assert(status === 500, "obsolete v1 migration state failed unexpectedly");
    assert(
        typeof body?.error === "string" && body.error.includes(`newer than packaged version ${PREVIEW_SCHEMA_VERSION}`),
        "obsolete v1 migration state did not report its packaged-journal fence"
    );
    return "cold-v1-journal-fence";
}

async function assertHealth(origin, digest, schemaVersion) {
    const result = await requestJson(origin, "/health");
    assert(result.response.ok && result.body?.ok === true, "Worker health failed");
    assert(result.body.releaseSha256 === digest, "Worker release digest drifted");
    assert(result.body.schemaVersion === schemaVersion, `Worker package schema version is not ${schemaVersion}`);
    assert(result.response.headers.get("cf-chardb-server-version") === "0.1.0", "server version header is missing");
    assert(result.response.headers.has("server-timing"), "server timing header is missing");
    assert(result.response.headers.has("cf-chardb-correlation-id"), "correlation header is missing");
    return { schemaVersion: result.body.schemaVersion, serverVersion: "0.1.0" };
}

async function waitForHealth(origin, digest, schemaVersion, options) {
    return retryUntil(() => assertHealth(origin, digest, schemaVersion), options);
}

function median(values) {
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.floor((sorted.length - 1) / 2)];
}

const BENCHMARK_PROFILES = Object.freeze(["ci-smoke", "throughput"]);
const BENCHMARK_METRICS = Object.freeze([
    "directRead",
    "liveMutation",
    "liveMutationAck",
    "liveOwnerSnapshot",
    "liveObserverSnapshot",
]);

function summarizeBenchmarks(reports) {
    const summary = {};
    for (const profile of BENCHMARK_PROFILES) {
        const selected = reports.filter(item => item.profile.name === profile);
        summary[profile] = {};
        for (const metric of BENCHMARK_METRICS) {
            summary[profile][metric] = {
                throughputMedian: median(selected.map(item => item.metrics[metric].operationsPerSecond)),
                latencyP50Median: median(selected.map(item => item.metrics[metric].latencyMs.p50)),
                latencyP95Median: median(selected.map(item => item.metrics[metric].latencyMs.p95)),
            };
        }
    }
    return summary;
}

function summarizeComparisons(comparisons) {
    const summary = {};
    for (const profile of BENCHMARK_PROFILES) {
        const selected = comparisons.filter(item => item.profile.name === profile);
        summary[profile] = {};
        for (const metric of BENCHMARK_METRICS) {
            summary[profile][metric] = {
                throughputMedianRatio: median(selected.map(item => item.ratios[metric].throughput)),
                latencyP50MedianRatio: median(selected.map(item => item.ratios[metric].latencyP50)),
                latencyP95MedianRatio: median(selected.map(item => item.ratios[metric].latencyP95)),
            };
        }
    }
    return summary;
}

async function main() {
    const options = parseCloudflarePromotionArgs(process.argv.slice(2));
    if (options.help) {
        console.log(usage());
        return;
    }
    const sourceTarball = path.resolve(options.tarball);
    const sourceReactTarball = path.resolve(options.reactTarball);
    const output = path.resolve(options.output);
    const privateDir = path.resolve(options.privateDir);
    const origin = new URL(options.url);
    const reportPath = path.join(output, "promotion-report.json");
    const privateStatePath = path.join(privateDir, "private-state.json");
    const sessionPath = path.join(privateDir, "session.json");
    const secretsPath = path.join(privateDir, "staging-secrets.env");
    const adminTokenPath = path.join(privateDir, "admin-token");
    const work = path.join(privateDir, "work");
    const v1 = path.join(work, "v1");
    const v2 = path.join(work, "v2");
    const tarball = path.join(work, "core.tgz");
    const reactTarball = path.join(work, "react.tgz");
    const candidateBytes = await readFile(sourceTarball);
    const reactCandidateBytes = await readFile(sourceReactTarball);
    const candidate = { algorithm: "sha256", digest: sha256(candidateBytes), bytes: candidateBytes.byteLength };
    const reactCandidate = {
        algorithm: "sha256",
        digest: sha256(reactCandidateBytes),
        bytes: reactCandidateBytes.byteLength,
    };
    const v1MigrationId = `${options.migrationPrefix}-v1`;
    const v2MigrationId = `${options.migrationPrefix}-v2`;

    await mkdir(output, { recursive: true });
    await mkdir(privateDir, { recursive: true, mode: 0o700 });
    await chmod(privateDir, 0o700);
    let report;
    if (await pathExists(reportPath)) {
        report = JSON.parse(await readFile(reportPath, "utf8"));
        assert(report.schema === REPORT_SCHEMA, "existing promotion report has a different schema");
        assert(
            report.target?.worker === options.worker && report.target?.origin === origin.origin,
            "promotion target drifted"
        );
        assert(
            report.migrations?.v1 === v1MigrationId && report.migrations?.v2 === v2MigrationId,
            "migration identity drifted"
        );
        assert(
            report.candidate?.digest === candidate.digest && report.candidate?.bytes === candidate.bytes,
            "core candidate drifted"
        );
        assert(
            report.reactCandidate?.digest === reactCandidate.digest &&
                report.reactCandidate?.bytes === reactCandidate.bytes,
            "React candidate drifted"
        );
    } else {
        if ((await readdir(output)).length > 0) throw new Error("promotion output must be absent, empty, or resumable");
        report = {
            schema: REPORT_SCHEMA,
            ok: false,
            startedAt: new Date().toISOString(),
            completedAt: null,
            target: { worker: options.worker, origin: origin.origin },
            migrations: { v1: v1MigrationId, v2: v2MigrationId },
            candidate: null,
            reactCandidate: null,
            stages: [],
            results: {},
        };
        await atomicJson(reportPath, report);
    }

    let privateState;
    if (await pathExists(privateStatePath)) {
        privateState = JSON.parse(await readFile(privateStatePath, "utf8"));
        assert(privateState.schema === PRIVATE_SCHEMA, "private promotion state has a different schema");
    } else {
        const adopted = options.secretsFile
            ? parsePromotionSecrets(
                  await readFile(path.resolve(options.secretsFile), "utf8"),
                  await readFile(path.resolve(options.adminTokenFile), "utf8")
              )
            : null;
        const authSecret = adopted?.authSecret ?? randomBytes(32).toString("base64url");
        const adminToken = adopted?.adminToken ?? randomBytes(32).toString("base64url");
        privateState = { schema: PRIVATE_SCHEMA, authSecret, adminToken };
        await atomicJson(privateStatePath, privateState, 0o600);
    }
    await writeFile(
        secretsPath,
        `BETTER_AUTH_SECRET=${privateState.authSecret}\nCDB_ADMIN_TOKEN=${privateState.adminToken}\n`,
        { mode: 0o600 }
    );
    await writeFile(adminTokenPath, `${privateState.adminToken}\n`, { mode: 0o600 });
    await chmod(secretsPath, 0o600);
    await chmod(adminTokenPath, 0o600);
    const secrets = [privateState.authSecret, privateState.adminToken];
    const completed = new Set(report.stages.filter(stage => stage.status === "passed").map(stage => stage.name));
    let changed = false;

    const save = async () => atomicJson(reportPath, report);
    const stage = async (name, action) => {
        if (completed.has(name)) {
            console.log(`skip ${name}`);
            return;
        }
        const startedAt = new Date().toISOString();
        console.log(`\n> ${name}`);
        try {
            const result = await action();
            report.results[name] = result ?? { ok: true };
            report.stages.push({ name, startedAt, completedAt: new Date().toISOString(), status: "passed" });
            completed.add(name);
            changed = true;
            await save();
        } catch (error) {
            report.stages.push({
                name,
                startedAt,
                completedAt: new Date().toISOString(),
                status: "failed",
                error: scrub(error instanceof Error ? error.message : error, secrets),
            });
            await save();
            throw error;
        }
    };

    const cli = path.join(v1, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs");

    const versions = async app => {
        const command = path.join(app, "node_modules", ".bin", "wrangler");
        const result = await runCommand(command, ["versions", "list", "--name", options.worker, "--json"], {
            cwd: app,
            label: "Wrangler versions list",
            secrets,
        });
        return JSON.parse(result.stdout);
    };
    const deployment = async app => {
        const command = path.join(app, "node_modules", ".bin", "wrangler");
        const result = await runCommand(command, ["deployments", "status", "--name", options.worker, "--json"], {
            cwd: app,
            label: "Wrangler deployment status",
            secrets,
        });
        return JSON.parse(result.stdout);
    };
    const deploy = async (app, label, expectedHash) => {
        const actualHash = await fingerprintDeployment(app, [tarball, reactTarball]);
        if (expectedHash && actualHash.digest !== expectedHash.digest)
            throw new Error(`${label} deployment input drifted`);
        let before = null;
        try {
            before = newestWranglerVersion(await versions(app));
        } catch {
            console.log(`no prior Worker version found for ${label}; deploy will prove the target exists`);
        }
        const command = path.join(app, "node_modules", ".bin", "wrangler");
        const message = `CharDB automated promotion ${label}`;
        const tag = `${options.migrationPrefix}-${label}`;
        if (!before) {
            await runCommand(command, wranglerInitialDeployArgs(options.worker, secretsPath, tag, message), {
                cwd: app,
                label: `Wrangler initial deploy ${label}`,
                secrets,
            });
            const initial = newestWranglerVersion(await versions(app));
            return {
                ...assertFullTrafficDeployment(await deployment(app), initial.id),
                number: initial.number,
                input: actualHash,
                bootstrap: true,
            };
        }
        await runCommand(command, wranglerVersionUploadArgs(options.worker, secretsPath, tag, message), {
            cwd: app,
            label: `Wrangler version upload ${label}`,
            secrets,
        });
        const after = newestWranglerVersion(await versions(app));
        if (before) {
            assert(
                after.number > before.number && after.id !== before.id,
                `${label} did not upload a new Worker version`
            );
        }
        await runCommand(command, wranglerVersionDeployArgs(options.worker, after.id, message), {
            cwd: app,
            label: `Wrangler version deploy ${label}`,
            secrets,
        });
        return {
            ...assertFullTrafficDeployment(await deployment(app), after.id),
            number: after.number,
            input: actualHash,
        };
    };
    const migrate = async (migrationId, targetVersion) => {
        await runCommand(
            "bun",
            [
                cli,
                "migrate",
                "--url",
                origin.origin,
                "--id",
                migrationId,
                "--target",
                String(targetVersion),
                "--concurrency",
                "2",
            ],
            {
                cwd: v1,
                env: { ...process.env, CHARDB_ADMIN_TOKEN: privateState.adminToken },
                label: `schema migration ${targetVersion}`,
                secrets,
            }
        );
    };
    const loadSession = async () => JSON.parse(await readFile(sessionPath, "utf8"));
    const saveSession = async value => atomicJson(sessionPath, value, 0o600);

    await stage("prepare", async () => {
        const completeWork =
            (await pathExists(tarball)) &&
            (await pathExists(reactTarball)) &&
            (await pathExists(v1)) &&
            (await pathExists(v2));
        const reusableWork =
            completeWork &&
            sha256(await readFile(tarball)) === candidate.digest &&
            sha256(await readFile(reactTarball)) === reactCandidate.digest;
        if (!reusableWork) {
            if (await pathExists(work)) {
                await rename(work, `${work}.incomplete-${Date.now()}-${randomUUID().slice(0, 8)}`);
            }
            await mkdir(work, { recursive: true });
            await cp(sourceTarball, tarball);
            await cp(sourceReactTarball, reactTarball);
            await runCommand(
                "bun",
                [
                    path.join(ROOT, "scripts", "prepare-preview-chat.mjs"),
                    "--tarball",
                    tarball,
                    "--react-tarball",
                    reactTarball,
                    "--output",
                    v1,
                    "--name",
                    options.worker,
                ],
                { cwd: ROOT, label: "prepare version-one app", secrets }
            );
            await runCommand(
                "bun",
                [path.join(ROOT, "scripts", "prepare-preview-upgrade.mjs"), "--input", v1, "--output", v2],
                {
                    cwd: ROOT,
                    label: "prepare version-two app",
                    secrets,
                }
            );
        }
        report.candidate = { algorithm: "sha256", digest: candidate.digest, bytes: candidate.bytes };
        report.reactCandidate = {
            algorithm: "sha256",
            digest: reactCandidate.digest,
            bytes: reactCandidate.bytes,
        };
        return {
            candidate: report.candidate,
            reactCandidate: report.reactCandidate,
            v1: "prepared",
            v2: "prepared",
        };
    });

    await stage("validate", async () => {
        const npmCache = path.join(privateDir, "npm-cache");
        await runCommand("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund"], {
            cwd: v1,
            env: { ...process.env, npm_config_cache: npmCache },
            label: "install promotion app",
            secrets,
        });
        await cp(path.join(v1, "package-lock.json"), path.join(v2, "package-lock.json"));
        if (!(await pathExists(path.join(v2, "node_modules")))) {
            await symlink(path.join(v1, "node_modules"), path.join(v2, "node_modules"), "dir");
        }
        for (const [label, app] of [
            ["v1", v1],
            ["v2", v2],
        ]) {
            for (const [command, args] of [
                ["npm", ["run", "typecheck"]],
                ["npm", ["run", "build"]],
                [path.join(app, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs"), ["doctor", "wrangler"]],
                [
                    path.join(app, "node_modules", ".bin", "wrangler"),
                    ["deploy", "--name", options.worker, "--dry-run", "--outdir", "worker-dist"],
                ],
            ]) {
                await runCommand(command, args, { cwd: app, label: `${label} ${args.join(" ")}`, secrets });
            }
        }
        return {
            v1: await fingerprintDeployment(v1, [tarball, reactTarball]),
            v2: await fingerprintDeployment(v2, [tarball, reactTarball]),
        };
    });

    await stage("deploy-v1", async () => deploy(v1, "v1", report.results.validate.v1));

    await stage("activate-v1", async () => {
        await waitForHealth(origin, report.candidate.digest, PREVIEW_SCHEMA_VERSION);
        const current = await adminRequest(origin, privateState.adminToken, "state");
        const state = current.state;
        if (state.status === "active" && state.activeVersion === 0)
            await migrate(v1MigrationId, PREVIEW_SCHEMA_VERSION);
        else if (state.status === "migrating" && state.migrationId === v1MigrationId)
            await migrate(v1MigrationId, PREVIEW_SCHEMA_VERSION);
        else
            assert(
                state.status === "active" && state.activeVersion === PREVIEW_SCHEMA_VERSION,
                "target is not at a promotable version-one state"
            );
        const active = await adminRequest(origin, privateState.adminToken, "state");
        assertSchemaState(active, { status: "active", activeVersion: PREVIEW_SCHEMA_VERSION, activeEpoch: 2 });
        await migrate(v1MigrationId, PREVIEW_SCHEMA_VERSION);
        return { activeVersion: PREVIEW_SCHEMA_VERSION, activeEpoch: 2, idempotentRetry: true };
    });

    await stage("seed-v1", async () => {
        const session = await seedOrganization(origin);
        await saveSession(session);
        return {
            userId: session.principal.session.user.id,
            organizationId: session.organizationId,
            messageIds: session.messages.map(message => message.id),
        };
    });

    await stage("deploy-v2", async () => deploy(v2, "v2-initial", report.results.validate.v2));

    await stage("fence-before-v2", async () => {
        await waitForHealth(origin, report.candidate.digest, PREVIEW_UPGRADE_SCHEMA_VERSION);
        const session = await loadSession();
        const fence = await assertTrafficClosed(
            origin,
            session.principal.token,
            session.organizationId,
            "before-v2-migration"
        );
        session.blockedIds.push(fence.blockedId);
        await saveSession(session);
        const state = await adminRequest(origin, privateState.adminToken, "state");
        assertSchemaState(state, { status: "active", activeVersion: PREVIEW_SCHEMA_VERSION, activeEpoch: 2 });
        return fence;
    });

    await stage("interrupt-v2", async () => {
        const begun = await adminRequest(origin, privateState.adminToken, "begin", {
            migrationId: v2MigrationId,
            targetVersion: PREVIEW_UPGRADE_SCHEMA_VERSION,
        });
        assertSchemaState(begun, {
            status: "migrating",
            activeVersion: PREVIEW_SCHEMA_VERSION,
            migrationId: v2MigrationId,
            targetVersion: PREVIEW_UPGRADE_SCHEMA_VERSION,
        });
        const shards = await adminRequest(
            origin,
            privateState.adminToken,
            `shards?migrationId=${encodeURIComponent(v2MigrationId)}`
        );
        const pending = shards.shards?.filter(shard => shard.status === "pending") ?? [];
        assert(pending.length > 0, "version-two migration has no pending shard to interrupt after");
        const selected = pending[0];
        const activated = await adminRequest(origin, privateState.adminToken, "shard", {
            migrationId: v2MigrationId,
            shardId: selected.shardId,
        });
        assert(
            activated.shard?.shardId === selected.shardId && activated.shard?.status === "active",
            "selected shard did not activate"
        );
        const session = await loadSession();
        const fence = await assertTrafficClosed(
            origin,
            session.principal.token,
            session.organizationId,
            "during-v2-migration"
        );
        session.blockedIds.push(fence.blockedId);
        await saveSession(session);
        return { activatedShard: selected.shardId, trafficFence: fence };
    });

    await stage("redeploy-v2", async () => deploy(v2, "v2-interrupted-redeploy", report.results.validate.v2));

    await stage("fence-after-redeploy", async () => {
        await waitForHealth(origin, report.candidate.digest, PREVIEW_UPGRADE_SCHEMA_VERSION);
        const state = await adminRequest(origin, privateState.adminToken, "state");
        assertSchemaState(state, {
            status: "migrating",
            activeVersion: PREVIEW_SCHEMA_VERSION,
            migrationId: v2MigrationId,
            targetVersion: PREVIEW_UPGRADE_SCHEMA_VERSION,
        });
        const session = await loadSession();
        const fence = await assertTrafficClosed(
            origin,
            session.principal.token,
            session.organizationId,
            "after-v2-redeploy"
        );
        session.blockedIds.push(fence.blockedId);
        await saveSession(session);
        return fence;
    });

    await stage("resume-v2", async () => {
        await migrate(v2MigrationId, PREVIEW_UPGRADE_SCHEMA_VERSION);
        const state = await adminRequest(origin, privateState.adminToken, "state");
        assertSchemaState(state, { status: "active", activeVersion: PREVIEW_UPGRADE_SCHEMA_VERSION, activeEpoch: 3 });
        await migrate(v2MigrationId, PREVIEW_UPGRADE_SCHEMA_VERSION);
        return { activeVersion: PREVIEW_UPGRADE_SCHEMA_VERSION, activeEpoch: 3, idempotentRetry: true };
    });

    await stage("verify-v2", async () => {
        const session = await loadSession();
        await refreshPrincipal(origin, session.principal);
        await assertMessages(origin, session.principal.token, session.organizationId, session.messages);
        const suffix = `${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
        const message = {
            id: `upgrade-after-${suffix}`,
            organizationId: session.organizationId,
            authorId: session.principal.session.user.id,
            body: `after deployed upgrade ${suffix}`,
            createdAt: Date.now(),
        };
        await writeMessage(origin, session.principal.token, message, `upgrade-after-${suffix}`);
        session.messages.push(message);
        await assertMessages(origin, session.principal.token, session.organizationId, session.messages);
        await saveSession(session);
        return { messageIds: session.messages.map(item => item.id), blockedIds: session.blockedIds };
    });

    await stage("deploy-obsolete-v1", async () => deploy(v1, "obsolete-v1", report.results.validate.v1));

    await stage("fence-obsolete-v1", async () => {
        await waitForHealth(origin, report.candidate.digest, PREVIEW_SCHEMA_VERSION);
        const controlPlane = await requestJson(origin, "/_chardb/migrations/state", {
            headers: { authorization: `Bearer ${privateState.adminToken}` },
        });
        const controlPlaneMode = classifyObsoleteControlPlane(controlPlane.response.status, controlPlane.body);
        const session = await loadSession();
        const fence = await assertTrafficClosed(origin, session.principal.token, session.organizationId, "obsolete-v1");
        session.blockedIds.push(fence.blockedId);
        await saveSession(session);
        return { ...fence, controlPlaneStatus: controlPlane.response.status, controlPlaneMode };
    });

    await stage("restore-v2", async () => deploy(v2, "v2-final", report.results.validate.v2));

    await stage("verify-final", async () => {
        await waitForHealth(origin, report.candidate.digest, PREVIEW_UPGRADE_SCHEMA_VERSION);
        await retryUntil(
            async () => {
                const state = await adminRequest(origin, privateState.adminToken, "state");
                return assertSchemaState(state, {
                    status: "active",
                    activeVersion: PREVIEW_UPGRADE_SCHEMA_VERSION,
                    activeEpoch: 3,
                });
            },
            { timeoutMs: 60_000, intervalMs: 1_000 }
        );
        const session = await loadSession();
        await refreshPrincipal(origin, session.principal);
        await assertMessages(origin, session.principal.token, session.organizationId, session.messages);
        return {
            betterAuthSessionPersisted: true,
            organizationPersisted: true,
            exactRowsPersisted: true,
            blockedWritesAbsent: true,
            messageIds: session.messages.map(item => item.id),
            blockedIds: session.blockedIds,
        };
    });

    await stage("benchmark", async () => {
        if (options.benchmarkSamples === 0) return { skipped: true, samplesPerProfile: 0 };
        const benchmarkDir = path.join(output, "benchmarks");
        await mkdir(benchmarkDir, { recursive: true });
        const finalVersion = report.results["restore-v2"].versionId;
        const reports = [];
        for (const profile of BENCHMARK_PROFILES) {
            for (let sample = 1; sample <= options.benchmarkSamples; sample++) {
                const target = path.join(benchmarkDir, `${profile}-${sample}.json`);
                await runCommand(
                    "bun",
                    [
                        path.join(ROOT, "scripts", "run-chat-benchmark.mjs"),
                        "--url",
                        origin.origin,
                        "--output",
                        target,
                        "--kind",
                        "cloudflare",
                        "--label",
                        `${options.worker}-${profile}-${sample}`,
                        "--profile",
                        profile,
                        "--sha256",
                        report.candidate.digest,
                        "--deployment-version",
                        finalVersion,
                    ],
                    { cwd: ROOT, label: `${profile} benchmark ${sample}`, secrets, timeoutMs: COMMAND_TIMEOUT_MS }
                );
                reports.push(assertChatBenchmarkReport(JSON.parse(await readFile(target, "utf8"))));
            }
        }
        const summary = summarizeBenchmarks(reports);
        await atomicJson(path.join(benchmarkDir, "summary.json"), {
            schema: "chardb.cloudflare-promotion.benchmark-summary.v1",
            samplesPerProfile: options.benchmarkSamples,
            profiles: summary,
        });
        return { samplesPerProfile: options.benchmarkSamples, profiles: summary };
    });

    await stage("benchmark-local", async () => {
        if (options.benchmarkSamples === 0) return { skipped: true, samplesPerProfile: 0 };
        const benchmarkDir = path.join(output, "benchmarks");
        const localDir = path.join(benchmarkDir, "local");
        const comparisonDir = path.join(benchmarkDir, "comparisons");
        const localState = path.join(privateDir, `local-miniflare-state-${randomUUID().slice(0, 8)}`);
        await mkdir(localDir, { recursive: true });
        await mkdir(comparisonDir, { recursive: true });
        await mkdir(localState, { recursive: true });
        const port = await availablePort();
        const localOrigin = new URL(`http://127.0.0.1:${port}`);
        const localWorker = Bun.spawn(
            [
                path.join(v2, "node_modules", ".bin", "wrangler"),
                "dev",
                "--local",
                "--ip",
                "127.0.0.1",
                "--port",
                String(port),
                "--persist-to",
                localState,
                "--env-file",
                secretsPath,
            ],
            {
                cwd: v2,
                env: process.env,
                stdin: "ignore",
                stdout: "pipe",
                stderr: "pipe",
                detached: process.platform !== "win32",
            }
        );
        const stdout = new Response(localWorker.stdout).text();
        const stderr = new Response(localWorker.stderr).text();
        let result;
        let failure;
        try {
            let ready = false;
            const exitedBeforeReady = localWorker.exited.then(code => {
                if (!ready) throw new Error(`local Wrangler exited with ${code} before readiness`);
            });
            await Promise.race([
                waitForHealth(localOrigin, report.candidate.digest, PREVIEW_UPGRADE_SCHEMA_VERSION, {
                    timeoutMs: 45_000,
                }).then(value => {
                    ready = true;
                    return value;
                }),
                exitedBeforeReady,
            ]);
            await runCommand(
                "bun",
                [
                    cli,
                    "migrate",
                    "--url",
                    localOrigin.origin,
                    "--id",
                    `${options.migrationPrefix}-local-v2`,
                    "--target",
                    String(PREVIEW_UPGRADE_SCHEMA_VERSION),
                    "--concurrency",
                    "2",
                ],
                {
                    cwd: v2,
                    env: { ...process.env, CHARDB_ADMIN_TOKEN: privateState.adminToken },
                    label: "local Miniflare schema migration",
                    secrets,
                }
            );
            const localReports = [];
            const comparisons = [];
            for (const profile of BENCHMARK_PROFILES) {
                for (let sample = 1; sample <= options.benchmarkSamples; sample++) {
                    const localPath = path.join(localDir, `${profile}-${sample}.json`);
                    await runCommand(
                        "bun",
                        [
                            path.join(ROOT, "scripts", "run-chat-benchmark.mjs"),
                            "--url",
                            localOrigin.origin,
                            "--output",
                            localPath,
                            "--kind",
                            "local",
                            "--label",
                            `miniflare-v2-${profile}-${sample}`,
                            "--profile",
                            profile,
                            "--sha256",
                            report.candidate.digest,
                        ],
                        {
                            cwd: ROOT,
                            label: `local ${profile} benchmark ${sample}`,
                            secrets,
                            timeoutMs: COMMAND_TIMEOUT_MS,
                        }
                    );
                    const localReport = assertChatBenchmarkReport(JSON.parse(await readFile(localPath, "utf8")));
                    const deployedPath = path.join(benchmarkDir, `${profile}-${sample}.json`);
                    const deployedReport = assertChatBenchmarkReport(JSON.parse(await readFile(deployedPath, "utf8")));
                    const comparison = compareChatBenchmarkReports(localReport, deployedReport);
                    await atomicJson(path.join(comparisonDir, `${profile}-${sample}.json`), comparison);
                    localReports.push(localReport);
                    comparisons.push(comparison);
                }
            }
            const localSummary = summarizeBenchmarks(localReports);
            const comparisonSummary = summarizeComparisons(comparisons);
            await atomicJson(path.join(localDir, "summary.json"), {
                schema: "chardb.cloudflare-promotion.local-benchmark-summary.v1",
                samplesPerProfile: options.benchmarkSamples,
                profiles: localSummary,
            });
            await atomicJson(path.join(comparisonDir, "summary.json"), {
                schema: "chardb.cloudflare-promotion.comparison-summary.v1",
                samplesPerProfile: options.benchmarkSamples,
                ratioDefinition: "cloudflare/local",
                profiles: comparisonSummary,
            });
            result = {
                samplesPerProfile: options.benchmarkSamples,
                runtime: "Wrangler and Miniflare",
                local: localSummary,
                cloudflareToLocal: comparisonSummary,
            };
        } catch (error) {
            failure = error;
        }
        await terminateProcess(localWorker);
        const logs = scrub(`${await stdout}\n${await stderr}`, secrets);
        if (failure) {
            throw new Error(`${failure instanceof Error ? failure.message : failure}\n${logs}`);
        }
        return result;
    });

    await stage("finalize", async () => {
        for (const relative of (await regularFiles(output)).filter(item => item !== "evidence.sha256")) {
            const bytes = await readFile(path.join(output, ...relative.split("/")));
            for (const secret of secrets) {
                if (secret && bytes.includes(Buffer.from(secret)))
                    throw new Error(`secret leaked into evidence file ${relative}`);
            }
        }
        return { secretsAbsent: true };
    });

    const evidenceFiles = (await regularFiles(output)).filter(item => item !== "evidence.sha256");
    for (const relative of evidenceFiles) {
        const bytes = await readFile(path.join(output, ...relative.split("/")));
        for (const secret of secrets) {
            if (secret && bytes.includes(Buffer.from(secret)))
                throw new Error(`secret leaked into evidence file ${relative}`);
        }
    }
    report.ok = true;
    if (changed || !report.completedAt) report.completedAt = new Date().toISOString();
    await save();
    const lines = [];
    for (const relative of evidenceFiles) {
        const bytes = await readFile(path.join(output, ...relative.split("/")));
        lines.push(`${sha256(bytes)}  ${relative}`);
    }
    await writeFile(path.join(output, "evidence.sha256"), `${lines.join("\n")}\n`);

    console.log(JSON.stringify({ report: reportPath, ok: report.ok, target: report.target, stages: STAGES.length }));
}

if (import.meta.main) await main();
