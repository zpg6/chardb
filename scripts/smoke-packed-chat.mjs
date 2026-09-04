import { spawnSync } from "node:child_process";
import { chmod, cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { release, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";
import { fingerprintFile, writeJsonAtomically } from "./browser-proof-report.mjs";
import { disposeMiniflareBounded } from "./miniflare-lifecycle.mjs";
import {
    assertPackedChatRestartHandoff,
    assertPackedChatRestartResult,
    buildPackedChatReport,
    buildPackedChatRestartHandoff,
    buildPackedChatRestartResult,
    parsePackedChatArgs,
} from "./packed-chat-report.mjs";
import { runManagedCommand } from "./process-lifecycle.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const CHAT = join(ROOT, "example", "chat");
const scriptPath = fileURLToPath(import.meta.url);
const internalPhaseIndex = process.argv.indexOf("--internal-phase");
const internalPhase = internalPhaseIndex < 0 ? undefined : process.argv[internalPhaseIndex + 1];
const internalControlIndex = process.argv.indexOf("--control");
const internalControl = internalControlIndex < 0 ? undefined : process.argv[internalControlIndex + 1];
const options = internalPhase === undefined ? parsePackedChatArgs(process.argv.slice(2)) : undefined;
const tarball = options === undefined ? undefined : resolve(options.tarball);
const reactTarball = options === undefined ? undefined : resolve(options.reactTarball);
const reportPath =
    options === undefined
        ? undefined
        : resolve(process.env.CDB_PACKED_CHAT_REPORT ?? options.reportPath ?? `${tarball}.packed-chat.json`);
const runStartedAt = new Date().toISOString();
const runStartedAtMs = performance.now();
const ADMIN_TOKEN = "packed-chat-migration-secret";
const BENCHMARK_PROFILES = {
    "ci-smoke": { queries: 32, concurrency: 8, liveUpdates: 4 },
    throughput: { queries: 256, concurrency: 32, liveUpdates: 16 },
};
const profileName = process.env.CDB_BINDING_BENCH_PROFILE ?? "ci-smoke";
const profile = BENCHMARK_PROFILES[profileName];
const DURABLE_OBJECTS = {
    Catalog: { className: "Catalog", useSQLite: true },
    Gateway: { className: "Gateway", useSQLite: true },
    Cdb: { className: "Cdb", useSQLite: true },
    Resharder: { className: "Resharder", useSQLite: true },
};
const betterAuthRoutes = [];
let forceExitAfterMain = false;

if (!profile) throw new Error(`unknown CDB_BINDING_BENCH_PROFILE ${JSON.stringify(profileName)}`);

function run(command, args, cwd, env = {}, stdio = ["ignore", "pipe", "pipe"]) {
    const result = spawnSync(command, args, {
        cwd,
        env: { ...process.env, ...env },
        encoding: "utf8",
        stdio,
    });
    if (result.status !== 0) {
        throw new Error(
            `${command} ${args.join(" ")} failed (${result.status})\n${result.stdout ?? ""}${result.stderr ?? ""}`
        );
    }
    return result.stdout ?? "";
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}

function percentile(sorted, fraction) {
    return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function benchmarkRecord(type, latenciesMs, elapsedMs, concurrency, invariants) {
    const sorted = [...latenciesMs].sort((left, right) => left - right);
    return {
        type,
        version: 1,
        profile: profileName,
        queries: latenciesMs.length,
        concurrency,
        elapsedMs,
        queriesPerSecond: (latenciesMs.length * 1_000) / elapsedMs,
        latencyMs: {
            min: sorted[0],
            p50: percentile(sorted, 0.5),
            p95: percentile(sorted, 0.95),
            max: sorted.at(-1),
        },
        invariants,
    };
}

function sessionCookies(headers) {
    const values = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [headers.get("set-cookie")];
    return values
        .filter(Boolean)
        .map(value => value.split(";", 1)[0])
        .join("; ");
}

function mergeSessionCookies(current, headers) {
    const cookies = new Map();
    for (const value of [current, sessionCookies(headers)]) {
        for (const cookie of value.split(/;\s*/)) {
            const separator = cookie.indexOf("=");
            if (separator > 0) cookies.set(cookie.slice(0, separator), cookie.slice(separator + 1));
        }
    }
    return [...cookies].map(([name, value]) => `${name}=${value}`).join("; ");
}

function socketInbox(socket) {
    const queued = [];
    const waiters = [];
    socket.addEventListener("message", event => {
        const message = JSON.parse(String(event.data));
        const waiter = waiters.shift();
        if (waiter) waiter.resolve(message);
        else queued.push(message);
    });
    socket.addEventListener("close", event => {
        const error = new Error(`Gateway closed (${event.code}: ${event.reason})`);
        for (const waiter of waiters.splice(0)) waiter.reject(error);
    });
    return async function next(predicate, timeoutMs = 8_000) {
        const found = queued.findIndex(predicate);
        if (found >= 0) return queued.splice(found, 1)[0];
        const message = await new Promise((resolvePromise, reject) => {
            const waiter = { resolve: resolvePromise, reject };
            waiters.push(waiter);
            const timeout = setTimeout(() => {
                const index = waiters.indexOf(waiter);
                if (index >= 0) waiters.splice(index, 1);
                reject(new Error(`timed out waiting for Gateway message; queued=${JSON.stringify(queued)}`));
            }, timeoutMs);
            waiter.resolve = value => {
                clearTimeout(timeout);
                resolvePromise(value);
            };
        });
        if (predicate(message)) return message;
        queued.push(message);
        return next(predicate, timeoutMs);
    };
}

async function connectGateway(origin, clientId, jwt) {
    const url = new URL(`/ws?clientId=${encodeURIComponent(clientId)}`, origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    await new Promise((resolvePromise, reject) => {
        const timeout = setTimeout(() => reject(new Error("timed out opening Gateway WebSocket")), 5_000);
        socket.addEventListener(
            "open",
            () => {
                clearTimeout(timeout);
                resolvePromise();
            },
            { once: true }
        );
        socket.addEventListener(
            "error",
            () => {
                clearTimeout(timeout);
                reject(new Error("Gateway WebSocket failed to open"));
            },
            { once: true }
        );
    });
    const next = socketInbox(socket);
    socket.send(JSON.stringify({ t: "hello", protocolV: 3, clientId, jwt }));
    const welcome = await next(message => message.t === "welcome" || message.t === "error");
    assert(welcome.t === "welcome", `Gateway rejected Better Auth JWT: ${JSON.stringify(welcome)}`);
    return { socket, next };
}

async function closeSocket(socket) {
    if (socket.readyState === WebSocket.CLOSED) return;
    await new Promise(resolvePromise => {
        const timeout = setTimeout(resolvePromise, 5_000);
        socket.addEventListener(
            "close",
            () => {
                clearTimeout(timeout);
                resolvePromise();
            },
            { once: true }
        );
        socket.close();
    });
}

async function bundleWorker(consumer, bundlePath) {
    run(
        "bun",
        [
            "build",
            join(consumer, "src", "server", "worker.ts"),
            "--target=browser",
            "--format=esm",
            "--external=cloudflare:workers",
            "--outfile",
            bundlePath,
        ],
        consumer
    );
    let source = await readFile(bundlePath, "utf8");
    source = source.replace(
        "await import(this.#props.path.join(this.#props.migrationFolder, fileName))",
        'await Promise.reject(new Error("Node file migrations are unavailable in workerd"))'
    );
    source = source.replace(
        "await import(nodeSqlite)",
        'await Promise.reject(new Error("Node sqlite is unavailable in workerd"))'
    );
    if (/\bimport\s*\([^"'`]/.test(source)) {
        throw new Error("chat Worker bundle contains an unsupported dynamic module specifier");
    }
    return source;
}

async function migrate(consumer, origin) {
    const result = await runManagedCommand(
        "bun",
        [
            join(consumer, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs"),
            "migrate",
            "--url",
            origin.origin,
            "--id",
            "packed-chat-initial-schema",
            "--target",
            "1",
            "--concurrency",
            "2",
        ],
        {
            label: "packed migration",
            cwd: consumer,
            env: { ...process.env, CHARDB_ADMIN_TOKEN: ADMIN_TOKEN },
            captureOutput: true,
            reject: false,
        }
    );
    assert(
        result.status === 0 && result.signal === null && !result.timedOut,
        `packed migration failed (status ${result.status}, signal ${result.signal}, timed out ${result.timedOut})\n${result.stdout}${result.stderr}`
    );
    assert(
        result.stdout.includes("schema version 1 active at epoch 2"),
        `packed migration output drifted: ${result.stdout}`
    );
}

async function authPost(mf, origin, principal, path, body, label) {
    const response = await mf.dispatchFetch(new URL(`/api/auth${path}`, origin), {
        method: "POST",
        headers: { "content-type": "application/json", cookie: principal.cookie, origin: origin.origin },
        body: JSON.stringify(body),
    });
    const text = await response.text();
    const result = text ? JSON.parse(text) : null;
    betterAuthRoutes.push({ method: "POST", path: `/api/auth${path}`, status: response.status });
    assert(response.ok, `${label} failed (${response.status}): ${text}`);
    principal.cookie = mergeSessionCookies(principal.cookie, response.headers);
    return result;
}

async function refreshPrincipal(mf, origin, principal) {
    const sessionResponse = await mf.dispatchFetch(new URL("/api/auth/get-session", origin), {
        headers: { cookie: principal.cookie },
    });
    const session = await sessionResponse.json();
    assert(sessionResponse.ok && session?.user?.id, `session refresh failed: ${JSON.stringify(session)}`);
    const tokenResponse = await mf.dispatchFetch(new URL("/api/auth/token", origin), {
        headers: { cookie: principal.cookie },
    });
    const token = await tokenResponse.json();
    assert(tokenResponse.ok && typeof token?.token === "string", `JWT issue failed: ${JSON.stringify(token)}`);
    principal.session = session;
    principal.token = token.token;
    return principal;
}

async function signIn(mf, origin) {
    const response = await mf.dispatchFetch(new URL("/api/auth/sign-in/anonymous", origin), {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    betterAuthRoutes.push({ method: "POST", path: "/api/auth/sign-in/anonymous", status: response.status });
    const text = await response.text();
    assert(response.ok, `anonymous sign-in failed (${response.status}): ${text}`);
    const cookie = sessionCookies(response.headers);
    const sessionResponse = await mf.dispatchFetch(new URL("/api/auth/get-session", origin), {
        headers: { cookie },
    });
    const session = await sessionResponse.json();
    assert(
        sessionResponse.ok && session?.user?.id && typeof session?.user?.email === "string",
        `anonymous session bootstrap failed: ${JSON.stringify(session)}`
    );
    return { cookie, session, token: undefined };
}

async function createAndActivateOrganization(mf, origin, principal, name, slug) {
    const organization = await authPost(
        mf,
        origin,
        principal,
        "/organization/create",
        { name, slug, keepCurrentActiveOrganization: true },
        "organization creation"
    );
    assert(organization?.id, `organization creation returned no id: ${JSON.stringify(organization)}`);
    const active = await authPost(
        mf,
        origin,
        principal,
        "/organization/set-active",
        { organizationId: organization.id },
        "organization activation"
    );
    assert(active?.id === organization.id, `organization activation drifted: ${JSON.stringify(active)}`);
    await refreshPrincipal(mf, origin, principal);
    assert(
        principal.session.session.activeOrganizationId === organization.id,
        `active organization did not reach the session: ${JSON.stringify(principal.session)}`
    );
    return organization;
}

async function joinAndActivateOrganization(mf, origin, owner, member, organizationId) {
    const invitation = await authPost(
        mf,
        origin,
        owner,
        "/organization/invite-member",
        { email: member.session.user.email, role: "member", organizationId },
        "organization invitation"
    );
    assert(invitation?.id, `organization invitation returned no id: ${JSON.stringify(invitation)}`);
    await authPost(
        mf,
        origin,
        member,
        "/organization/accept-invitation",
        { invitationId: invitation.id },
        "organization invitation acceptance"
    );
    const active = await authPost(
        mf,
        origin,
        member,
        "/organization/set-active",
        { organizationId },
        "member organization activation"
    );
    assert(active?.id === organizationId, `member organization activation drifted: ${JSON.stringify(active)}`);
    await refreshPrincipal(mf, origin, member);
    assert(
        member.session.session.activeOrganizationId === organizationId,
        `member active organization did not reach the session: ${JSON.stringify(member.session)}`
    );
}

const PHASE_CONTROL_SCHEMA = "chardb.packed-chat-phase-control.v1";
const PHASE_TIMEOUT_MS = 180_000;

function sameFingerprint(left, right) {
    return JSON.stringify(left) === JSON.stringify(right);
}

async function writePrivateJson(path, value) {
    await writeJsonAtomically(path, value);
    await chmod(path, 0o600);
}

async function readPhaseControl() {
    assert(internalControl, "packed-chat internal phase requires --control");
    const control = JSON.parse(await readFile(resolve(internalControl), "utf8"));
    assert(control?.schema === PHASE_CONTROL_SCHEMA, `packed-chat phase control schema drifted: ${control?.schema}`);
    for (const name of ["consumer", "workerPath", "persistencePath", "handoffPath", "resultPath", "tarballPath"]) {
        assert(
            typeof control[name] === "string" && control[name].length > 0,
            `packed-chat phase control lacks ${name}`
        );
    }
    const actualFingerprint = await fingerprintFile(control.tarballPath);
    assert(sameFingerprint(actualFingerprint, control.tarball), "packed-chat phase tarball identity changed");
    return control;
}

async function startPhaseMiniflare(control) {
    const instance = new Miniflare({
        modules: true,
        script: await readFile(control.workerPath, "utf8"),
        bindings: {
            BETTER_AUTH_SECRET: "packed-chat-secret-that-is-at-least-32-characters",
            CDB_ADMIN_TOKEN: ADMIN_TOKEN,
        },
        durableObjects: DURABLE_OBJECTS,
        durableObjectsPersist: control.persistencePath,
        compatibilityDate: "2026-05-10",
        compatibilityFlags: ["nodejs_compat", "nodejs_compat_populate_process_env"],
    });
    return { instance, origin: await instance.ready };
}

async function runWithPhaseMiniflare(control, label, execute) {
    let mf;
    const sockets = [];
    try {
        const started = await startPhaseMiniflare(control);
        mf = started.instance;
        return await execute(mf, started.origin, sockets);
    } finally {
        await Promise.allSettled(sockets.map(closeSocket));
        const current = mf;
        mf = undefined;
        await disposeMiniflareBounded(current, {
            label,
            onTimeout: () => {
                forceExitAfterMain = true;
            },
        });
    }
}

async function runBeforeRestartPhase(control) {
    await runWithPhaseMiniflare(control, "packed chat phase A cleanup", async (mf, origin, sockets) => {
        await migrate(control.consumer, origin);
        const first = await signIn(mf, origin);
        const second = await signIn(mf, origin);
        assert(first.session.user.id !== second.session.user.id, "anonymous sign-ins reused one principal");
        const sharedOrganization = await createAndActivateOrganization(mf, origin, first, "Packed Chat", "packed-chat");
        const organizationId = sharedOrganization.id;
        await joinAndActivateOrganization(mf, origin, first, second, organizationId);

        const primary = await connectGateway(origin, "packed-chat-primary", first.token);
        const observer = await connectGateway(origin, "packed-chat-observer", second.token);
        sockets.push(primary.socket, observer.socket);
        const queryArgs = { organizationId, limit: 50 };
        for (const connection of [primary, observer]) {
            connection.socket.send(JSON.stringify({ t: "sub", subId: 1, ref: "query#listMessages", args: queryArgs }));
        }
        const [initial, observerInitial] = await Promise.all([
            primary.next(message => (message.t === "snapshot" && message.subId === 1) || message.t === "error"),
            observer.next(message => (message.t === "snapshot" && message.subId === 1) || message.t === "error"),
        ]);
        assert(
            initial.t === "snapshot" && initial.rows.length === 0,
            `initial snapshot failed: ${JSON.stringify(initial)}`
        );
        assert(
            observerInitial.t === "snapshot" && observerInitial.rows.length === 0,
            `observer snapshot failed: ${JSON.stringify(observerInitial)}`
        );
        primary.socket.send(JSON.stringify({ t: "ack", cookie: initial.cookie }));
        observer.socket.send(JSON.stringify({ t: "ack", cookie: observerInitial.cookie }));

        const firstMutation = {
            t: "mut",
            mutId: "packed-chat-mut-1",
            ref: "mutation#postMessage",
            args: {
                id: "packed-message-1",
                organizationId,
                body: "packed hello",
                clientCreatedAt: 1,
            },
        };
        primary.socket.send(JSON.stringify(firstMutation));
        const mutationResult = await primary.next(
            message => message.t === "error" || message.mutResults?.some(entry => entry.mutId === firstMutation.mutId)
        );
        assert(
            mutationResult.mutResults?.find(entry => entry.mutId === firstMutation.mutId)?.ok === true,
            `packed mutation failed: ${JSON.stringify(mutationResult)}`
        );
        const [firstReplacement, observerReplacement] = await Promise.all([
            primary.next(message => (message.t === "snapshot" && message.rows?.length === 1) || message.t === "error"),
            observer.next(message => (message.t === "snapshot" && message.rows?.length === 1) || message.t === "error"),
        ]);
        assert(
            firstReplacement.t === "snapshot" && observerReplacement.t === "snapshot",
            "same-organization clients did not receive the first replacement"
        );
        primary.socket.send(JSON.stringify({ t: "ack", cookie: firstReplacement.cookie }));
        observer.socket.send(JSON.stringify({ t: "ack", cookie: observerReplacement.cookie }));

        const secondMutationBody = {
            id: "packed-message-2",
            organizationId,
            body: "env.DB hello",
            clientCreatedAt: 2,
            mutId: "packed-chat-binding-mut-2",
        };
        const secondMutationResponse = await mf.dispatchFetch(new URL("/api/messages", origin), {
            method: "POST",
            headers: { authorization: `Bearer ${first.token}`, "content-type": "application/json" },
            body: JSON.stringify(secondMutationBody),
        });
        const secondMutation = await secondMutationResponse.json();
        assert(secondMutationResponse.ok && secondMutation?.id === "packed-message-2", "env.DB mutation failed");
        const [secondReplacement, secondObserverReplacement] = await Promise.all([
            primary.next(message => (message.t === "snapshot" && message.rows?.length === 2) || message.t === "error"),
            observer.next(message => (message.t === "snapshot" && message.rows?.length === 2) || message.t === "error"),
        ]);
        primary.socket.send(JSON.stringify({ t: "ack", cookie: secondReplacement.cookie }));
        observer.socket.send(JSON.stringify({ t: "ack", cookie: secondObserverReplacement.cookie }));

        const replayResponse = await mf.dispatchFetch(new URL("/api/messages", origin), {
            method: "POST",
            headers: { authorization: `Bearer ${first.token}`, "content-type": "application/json" },
            body: JSON.stringify(secondMutationBody),
        });
        const replay = await replayResponse.json();
        assert(
            replayResponse.ok && JSON.stringify(replay) === JSON.stringify(secondMutation),
            `mutation replay changed its result: ${JSON.stringify(replay)}`
        );

        const listUrl = new URL("/api/messages", origin);
        listUrl.searchParams.set("organizationId", organizationId);
        listUrl.searchParams.set("limit", "50");
        const listResponse = await mf.dispatchFetch(listUrl, {
            headers: { authorization: `Bearer ${first.token}` },
        });
        const rows = await listResponse.json();
        assert(
            listResponse.ok &&
                rows.length === 2 &&
                rows[0]?.id === "packed-message-2" &&
                rows[1]?.id === "packed-message-1",
            `direct select diverged: ${JSON.stringify(rows)}`
        );
        const overLimitUrl = new URL(listUrl);
        overLimitUrl.searchParams.set("limit", "101");
        const overLimit = await mf.dispatchFetch(overLimitUrl, {
            headers: { authorization: `Bearer ${first.token}` },
        });
        assert(!overLimit.ok, "direct select accepted a limit above 100");

        const directLatencies = [];
        const directStarted = performance.now();
        for (let offset = 0; offset < profile.queries; offset += profile.concurrency) {
            const count = Math.min(profile.concurrency, profile.queries - offset);
            await Promise.all(
                Array.from({ length: count }, async () => {
                    const started = performance.now();
                    const response = await mf.dispatchFetch(listUrl, {
                        headers: { authorization: `Bearer ${first.token}` },
                    });
                    const result = await response.json();
                    directLatencies.push(performance.now() - started);
                    assert(response.ok && result?.length === 2, `direct benchmark diverged: ${JSON.stringify(result)}`);
                })
            );
        }
        const directElapsed = performance.now() - directStarted;
        const directBenchmark = benchmarkRecord(
            "chardb-direct-select-benchmark",
            directLatencies,
            directElapsed,
            profile.concurrency,
            { exactRows: 2, overLimitDenied: true }
        );
        console.log(JSON.stringify(directBenchmark));

        const liveLatencies = [];
        let expectedRows = 2;
        for (let index = 0; index < profile.liveUpdates; index++) {
            const mutId = `packed-live-${index}`;
            const started = performance.now();
            primary.socket.send(
                JSON.stringify({
                    t: "mut",
                    mutId,
                    ref: "mutation#postMessage",
                    args: {
                        id: `packed-live-message-${index}`,
                        organizationId,
                        body: `live ${index}`,
                        clientCreatedAt: index + 3,
                    },
                })
            );
            const resultMessage = await primary.next(
                message => message.t === "error" || message.mutResults?.some(entry => entry.mutId === mutId)
            );
            assert(resultMessage.mutResults?.find(entry => entry.mutId === mutId)?.ok === true, "live mutation failed");
            expectedRows++;
            const [primaryReplacement, observerUpdate] = await Promise.all([
                primary.next(
                    message =>
                        (message.t === "snapshot" && message.rows?.length === expectedRows) || message.t === "error"
                ),
                observer.next(
                    message =>
                        (message.t === "snapshot" && message.rows?.length === expectedRows) || message.t === "error"
                ),
            ]);
            liveLatencies.push(performance.now() - started);
            assert(primaryReplacement.t === "snapshot" && observerUpdate.t === "snapshot", "live clients diverged");
            primary.socket.send(JSON.stringify({ t: "ack", cookie: primaryReplacement.cookie }));
            observer.socket.send(JSON.stringify({ t: "ack", cookie: observerUpdate.cookie }));
        }
        const liveElapsed = liveLatencies.reduce((sum, value) => sum + value, 0);
        const liveBenchmark = benchmarkRecord("chardb-binding-benchmark", liveLatencies, liveElapsed, 1, {
            organizationOnly: true,
            replacementClients: 2,
            finalRows: expectedRows,
            mutationReplayStable: true,
        });
        console.log(JSON.stringify(liveBenchmark));
        await Promise.all([closeSocket(primary.socket), closeSocket(observer.socket)]);

        const expectedRowIds = [
            ...Array.from({ length: profile.liveUpdates }, (_, index) => `packed-live-message-${index}`).reverse(),
            "packed-message-2",
            "packed-message-1",
        ];
        const handoff = buildPackedChatRestartHandoff({
            tarball: control.tarball,
            producerPid: process.pid,
            owner: { userId: first.session.user.id, cookie: first.cookie },
            member: { userId: second.session.user.id, cookie: second.cookie },
            sharedOrganization: { id: organizationId, slug: sharedOrganization.slug },
            expectedRows,
            expectedRowIds,
            betterAuthRoutes,
            benchmark: { profile: profileName, direct: directBenchmark, live: liveBenchmark },
        });
        await writePrivateJson(control.handoffPath, handoff);
    });
}

async function runAfterRestartPhase(control) {
    const handoff = assertPackedChatRestartHandoff(
        JSON.parse(await readFile(control.handoffPath, "utf8")),
        control.tarball
    );
    assert(handoff.producerPid !== process.pid, "packed-chat restart reused the phase A process");
    await runWithPhaseMiniflare(control, "packed chat phase B cleanup", async (mf, origin, sockets) => {
        const organizationId = handoff.sharedOrganization.id;
        const first = { cookie: handoff.owner.cookie, session: { user: { id: handoff.owner.userId } } };
        const second = { cookie: handoff.member.cookie, session: { user: { id: handoff.member.userId } } };
        for (const principal of [first, second]) {
            const sessionResponse = await mf.dispatchFetch(new URL("/api/auth/get-session", origin), {
                headers: { cookie: principal.cookie },
            });
            const session = await sessionResponse.json();
            assert(
                sessionResponse.ok &&
                    session?.user?.id === principal.session.user.id &&
                    session?.session?.activeOrganizationId === organizationId,
                `session did not survive process restart: ${JSON.stringify(session)}`
            );
            principal.session = session;
        }
        const restartedTokenResponse = await mf.dispatchFetch(new URL("/api/auth/token", origin), {
            headers: { cookie: first.cookie },
        });
        const restartedToken = await restartedTokenResponse.json();
        assert(
            restartedTokenResponse.ok && typeof restartedToken?.token === "string",
            "owner JWT issue failed after restart"
        );
        const restartedRowsUrl = new URL("/api/messages", origin);
        restartedRowsUrl.searchParams.set("organizationId", organizationId);
        restartedRowsUrl.searchParams.set("limit", "50");
        const restartedRowsResponse = await mf.dispatchFetch(restartedRowsUrl, {
            headers: { authorization: `Bearer ${restartedToken.token}` },
        });
        const restartedRows = await restartedRowsResponse.json();
        assert(
            restartedRowsResponse.ok &&
                JSON.stringify(restartedRows.map(row => row.id)) === JSON.stringify(handoff.expectedRowIds),
            `exact direct rows did not survive process restart: ${JSON.stringify(restartedRows)}`
        );

        const primary = await connectGateway(origin, "packed-chat-primary-restarted", restartedToken.token);
        sockets.push(primary.socket);
        primary.socket.send(
            JSON.stringify({
                t: "sub",
                subId: 2,
                ref: "query#listMessages",
                args: { organizationId, limit: 50 },
            })
        );
        const readback = await primary.next(
            message => (message.t === "snapshot" && message.subId === 2) || message.t === "error"
        );
        assert(
            readback.t === "snapshot" &&
                JSON.stringify(readback.rows.map(row => row.id)) === JSON.stringify(handoff.expectedRowIds),
            `exact live rows did not survive process restart: ${JSON.stringify(readback)}`
        );
        primary.socket.send(JSON.stringify({ t: "ack", cookie: readback.cookie }));

        const leave = await mf.dispatchFetch(new URL("/api/auth/organization/leave", origin), {
            method: "POST",
            headers: { "content-type": "application/json", cookie: second.cookie, origin: origin.origin },
            body: JSON.stringify({ organizationId }),
        });
        betterAuthRoutes.push({ method: "POST", path: "/api/auth/organization/leave", status: leave.status });
        assert(leave.ok, `second principal could not leave the shared organization: ${await leave.text()}`);
        second.cookie = mergeSessionCookies(second.cookie, leave.headers);
        const isolatedOrganization = await createAndActivateOrganization(
            mf,
            origin,
            second,
            "Packed Isolation",
            "packed-isolation"
        );
        const forbiddenUrl = new URL("/api/messages", origin);
        forbiddenUrl.searchParams.set("organizationId", organizationId);
        const isolatedUrl = new URL("/api/messages", origin);
        isolatedUrl.searchParams.set("organizationId", isolatedOrganization.id);
        const [forbiddenRead, isolatedRead, ownerRetainedRead] = await Promise.all([
            mf.dispatchFetch(forbiddenUrl, { headers: { authorization: `Bearer ${second.token}` } }),
            mf.dispatchFetch(isolatedUrl, { headers: { authorization: `Bearer ${second.token}` } }),
            mf.dispatchFetch(forbiddenUrl, { headers: { authorization: `Bearer ${restartedToken.token}` } }),
        ]);
        assert(
            forbiddenRead.status === 403,
            `revoked organization read returned ${forbiddenRead.status}, expected 403`
        );
        const isolatedRows = await isolatedRead.json();
        assert(
            isolatedRead.ok && isolatedRows.length === 0,
            `new organization was not empty: ${JSON.stringify(isolatedRows)}`
        );
        const ownerRows = await ownerRetainedRead.json();
        assert(
            ownerRetainedRead.ok &&
                JSON.stringify(ownerRows.map(row => row.id)) === JSON.stringify(handoff.expectedRowIds),
            `owner lost exact shared rows after member leave: ${JSON.stringify(ownerRows)}`
        );

        const result = buildPackedChatRestartResult({
            tarball: control.tarball,
            identity: { ownerUserId: handoff.owner.userId, memberUserId: handoff.member.userId },
            organizations: {
                shared: { id: organizationId, slug: handoff.sharedOrganization.slug },
                isolated: { id: isolatedOrganization.id, slug: isolatedOrganization.slug },
            },
            betterAuthRoutes,
            invariants: {
                distinctProcessReconstruction: true,
                sessionPersistenceObserved: true,
                exactDirectRowsObserved: true,
                exactLiveRowsObserved: true,
                membershipLeaveRevokedAccess: true,
                organizationIsolation: true,
                ownerRetainedAccess: true,
            },
        });
        await writePrivateJson(control.resultPath, result);
    });
}

async function runIsolatedPhase(phase, controlPath) {
    await runManagedCommand(process.execPath, [scriptPath, "--internal-phase", phase, "--control", controlPath], {
        label: `packed-chat ${phase} phase`,
        timeoutMs: PHASE_TIMEOUT_MS,
        cwd: ROOT,
        env: process.env,
        stdin: "ignore",
        stdout: "inherit",
        stderr: "inherit",
    });
}

async function main() {
    assert(
        tarball && reactTarball && reportPath,
        "packed-chat orchestrator requires core and React tarballs and a report path"
    );
    assert(
        Object.keys(DURABLE_OBJECTS).every(name => !name.startsWith("CDB_")),
        "packed chat must use native exported class names"
    );
    const scratch = await mkdtemp(join(tmpdir(), "chardb-packed-chat-"));
    const consumer = join(scratch, "consumer");
    try {
        await mkdir(consumer, { recursive: true });
        await cp(join(CHAT, "src"), join(consumer, "src"), { recursive: true });
        await cp(join(CHAT, "test"), join(consumer, "test"), { recursive: true });
        for (const name of ["index.html", "tsconfig.json", "vite.config.ts"]) {
            await cp(join(CHAT, name), join(consumer, name));
        }
        await cp(join(CHAT, "wrangler.template.toml"), join(consumer, "wrangler.toml"));
        const files = new Set(await readdir(consumer));
        assert(files.has("wrangler.toml") && !files.has("wrangler.jsonc"), "packed consumer must use wrangler.toml");

        const packageJson = JSON.parse(await readFile(join(CHAT, "package.json"), "utf8"));
        packageJson.dependencies["@chardb/core"] = `file:${tarball}`;
        packageJson.dependencies["@chardb/react"] = `file:${reactTarball}`;
        await writeFile(join(consumer, "package.json"), `${JSON.stringify(packageJson, null, 4)}\n`);
        const npmCache = process.env.CHARDDB_PACKED_CHAT_NPM_CACHE ?? join(scratch, "npm-cache");
        run(
            "npm",
            ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--cache", npmCache],
            consumer,
            {},
            "inherit"
        );
        const installed = JSON.parse(
            await readFile(join(consumer, "node_modules", "@chardb", "core", "package.json"), "utf8")
        );
        const installedReact = JSON.parse(
            await readFile(join(consumer, "node_modules", "@chardb", "react", "package.json"), "utf8")
        );
        assert(installed.name === "@chardb/core", "packed consumer did not install @chardb/core");
        assert(installedReact.name === "@chardb/react", "packed consumer did not install @chardb/react");
        run("npm", ["run", "typecheck"], consumer);
        run("npm", ["run", "build"], consumer);

        const assets = await readdir(join(consumer, "dist", "assets"));
        const browserSource = (
            await Promise.all(
                assets
                    .filter(name => name.endsWith(".js"))
                    .map(name => readFile(join(consumer, "dist", "assets", name), "utf8"))
            )
        ).join("\n");
        assert(browserSource.includes("mutation#postMessage"), "Vite output lost the stable mutation ref");
        assert(browserSource.includes("query#listMessages"), "Vite output lost the stable query ref");
        assert(!browserSource.includes("createUserPreference"), "tutorial bundle contains retired user-tenancy API");
        assert(!browserSource.includes("createGlobalNotice"), "tutorial bundle contains retired global API");

        const workerPath = join(scratch, "chat-worker.mjs");
        await writeFile(workerPath, await bundleWorker(consumer, workerPath));
        const tarballFingerprint = await fingerprintFile(tarball);
        const controlPath = join(scratch, "phase-control.json");
        const handoffPath = join(scratch, "restart-handoff.json");
        const resultPath = join(scratch, "restart-result.json");
        await writePrivateJson(controlPath, {
            schema: PHASE_CONTROL_SCHEMA,
            consumer,
            workerPath,
            persistencePath: join(scratch, "durable-objects"),
            handoffPath,
            resultPath,
            tarballPath: tarball,
            tarball: tarballFingerprint,
        });

        await runIsolatedPhase("before-restart", controlPath);
        const handoff = assertPackedChatRestartHandoff(
            JSON.parse(await readFile(handoffPath, "utf8")),
            tarballFingerprint
        );
        await runIsolatedPhase("after-restart", controlPath);
        const restartResult = assertPackedChatRestartResult(
            JSON.parse(await readFile(resultPath, "utf8")),
            tarballFingerprint
        );

        const [wranglerPackage, miniflarePackage, betterAuthPackage] = await Promise.all(
            ["wrangler", "miniflare", "better-auth"].map(name =>
                readFile(join(consumer, "node_modules", name, "package.json"), "utf8").then(JSON.parse)
            )
        );
        const report = buildPackedChatReport({
            run: {
                id: `${Date.now().toString(36)}-${process.pid}`,
                startedAt: runStartedAt,
                durationMs: performance.now() - runStartedAtMs,
            },
            packageEvidence: { name: installed.name, version: installed.version, tarball: tarballFingerprint },
            reactPackageEvidence: {
                name: installedReact.name,
                version: installedReact.version,
                tarball: await fingerprintFile(reactTarball),
            },
            platform: { operatingSystem: process.platform, release: release(), architecture: process.arch },
            runtime: {
                name: "packed-chat-miniflare-process-restart",
                bun: Bun.version,
                nodeCompatibility: process.versions.node,
                wrangler: wranglerPackage.version,
                miniflare: miniflarePackage.version,
                betterAuth: betterAuthPackage.version,
            },
            identity: restartResult.identity,
            organizations: restartResult.organizations,
            betterAuthRoutes: [...handoff.betterAuthRoutes, ...restartResult.betterAuthRoutes],
            benchmark: handoff.benchmark,
            invariants: {
                packedPackageInstalled: true,
                packedReactPackageInstalled: true,
                wranglerTomlConsumer: true,
                tutorialTypecheckPassed: true,
                tutorialBuildPassed: true,
                stableRefsInBrowserBundle: true,
                nativeAnonymousSignIn: true,
                nativeOrganizationCreateAndActivate: true,
                nativeInvitationAccepted: true,
                twoPrincipalSharedMembership: true,
                sameOrganizationLiveReplacement: true,
                mutationReplayStable: true,
                directReadParity: true,
                directLimitRejected: true,
                benchmarkProfileCompleted: true,
                workerdRestartObserved: restartResult.invariants.distinctProcessReconstruction,
                sessionPersistenceObserved: restartResult.invariants.sessionPersistenceObserved,
                dataPersistenceObserved:
                    restartResult.invariants.exactDirectRowsObserved && restartResult.invariants.exactLiveRowsObserved,
                membershipLeaveRevokedAccess: restartResult.invariants.membershipLeaveRevokedAccess,
                organizationIsolation: restartResult.invariants.organizationIsolation,
                ownerRetainedAccess: restartResult.invariants.ownerRetainedAccess,
            },
        });
        const serializedReport = JSON.stringify(report);
        assert(!/"(?:cookie|token|jwt)"\s*:/i.test(serializedReport), "packed-chat final report contains auth secrets");
        await writeJsonAtomically(reportPath, report);
        console.log(
            JSON.stringify({
                schema: report.schema,
                ok: true,
                artifact: { path: reportPath },
                version: installed.version,
            })
        );
    } finally {
        await rm(scratch, { recursive: true, force: true });
    }
}

async function internalMain() {
    assert(internalPhase === "before-restart" || internalPhase === "after-restart", "unknown packed-chat phase");
    const control = await readPhaseControl();
    if (internalPhase === "before-restart") await runBeforeRestartPhase(control);
    else await runAfterRestartPhase(control);
}

let failure;
try {
    if (internalPhase === undefined) await main();
    else await internalMain();
} catch (error) {
    failure = error;
}
if (internalPhase !== undefined || forceExitAfterMain) {
    if (failure) console.error(failure);
    process.exit(failure ? 1 : 0);
}
if (failure) throw failure;
