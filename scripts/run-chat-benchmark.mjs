import { writeFile } from "node:fs/promises";
import { arch, platform, release } from "node:os";
import path from "node:path";
import {
    CHAT_BENCHMARK_DRIVER_VERSION,
    CHAT_BENCHMARK_SCHEMA,
    CHAT_BENCHMARK_WORKLOAD_ID,
    assertChatBenchmarkReport,
} from "./chat-benchmark-report.mjs";

const PROFILES = {
    "ci-smoke": { directQueries: 32, directConcurrency: 8, liveUpdates: 4 },
    throughput: { directQueries: 256, directConcurrency: 32, liveUpdates: 16 },
};

function parseArgs(argv) {
    const values = {};
    for (let index = 0; index < argv.length; index += 1) {
        const key = argv[index];
        if (
            !["--url", "--output", "--kind", "--label", "--profile", "--sha256", "--deployment-version"].includes(key)
        ) {
            throw new Error(`unknown benchmark argument ${JSON.stringify(key)}`);
        }
        const value = argv[++index];
        if (!value) throw new Error(`${key} requires a value`);
        values[key.slice(2)] = value;
    }
    const kind = values.kind;
    if (kind !== "local" && kind !== "cloudflare") throw new Error("--kind must be local or cloudflare");
    const origin = new URL(values.url);
    if (kind === "cloudflare" && origin.protocol !== "https:") throw new Error("Cloudflare benchmarks require HTTPS");
    if (kind === "local" && (origin.protocol !== "http:" || !["127.0.0.1", "localhost"].includes(origin.hostname))) {
        throw new Error("local benchmarks require an HTTP loopback URL");
    }
    if (!values.output) throw new Error("--output is required");
    if (!/^[a-f0-9]{64}$/.test(values.sha256 ?? "")) throw new Error("--sha256 must be 64 lowercase hex characters");
    const profileName = values.profile ?? "ci-smoke";
    const profile = PROFILES[profileName];
    if (!profile) throw new Error(`unknown benchmark profile ${JSON.stringify(profileName)}`);
    return {
        origin,
        output: path.resolve(values.output),
        kind,
        label: values.label ?? kind,
        sha256: values.sha256,
        deploymentVersion: values["deployment-version"],
        profileName,
        profile,
    };
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}

function assertExactRows(rows, expected, label) {
    const actual = Array.isArray(rows)
        ? rows.map(row => ({
              id: row.id,
              organizationId: row.organizationId,
              authorId: row.authorId,
              body: row.body,
              createdAt: row.createdAt,
          }))
        : rows;
    assert(JSON.stringify(actual) === JSON.stringify(expected), `${label} diverged: ${JSON.stringify(actual)}`);
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

async function requestJson(origin, pathname, init = {}) {
    const response = await fetch(new URL(pathname, origin), init);
    const text = await response.text();
    let body = null;
    try {
        body = text ? JSON.parse(text) : null;
    } catch {
        body = text;
    }
    return { response, body };
}

async function signIn(origin) {
    const { response, body } = await requestJson(origin, "/api/auth/sign-in/anonymous", {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    assert(response.ok, `anonymous sign-in failed (${response.status}): ${JSON.stringify(body)}`);
    const principal = { cookie: sessionCookies(response.headers), session: null, token: null };
    await refreshPrincipal(origin, principal);
    assert(principal.session?.user?.id, "anonymous session has no user id");
    return principal;
}

async function refreshPrincipal(origin, principal) {
    const session = await requestJson(origin, "/api/auth/get-session", { headers: { cookie: principal.cookie } });
    assert(session.response.ok && session.body?.user?.id, `session refresh failed: ${JSON.stringify(session.body)}`);
    principal.cookie = mergeSessionCookies(principal.cookie, session.response.headers);
    const token = await requestJson(origin, "/api/auth/token", { headers: { cookie: principal.cookie } });
    assert(
        token.response.ok && typeof token.body?.token === "string",
        `JWT issue failed: ${JSON.stringify(token.body)}`
    );
    principal.cookie = mergeSessionCookies(principal.cookie, token.response.headers);
    principal.session = session.body;
    principal.token = token.body.token;
}

async function authPost(origin, principal, pathname, body) {
    const result = await requestJson(origin, `/api/auth${pathname}`, {
        method: "POST",
        headers: { "content-type": "application/json", cookie: principal.cookie, origin: origin.origin },
        body: JSON.stringify(body),
    });
    assert(result.response.ok, `${pathname} failed (${result.response.status}): ${JSON.stringify(result.body)}`);
    principal.cookie = mergeSessionCookies(principal.cookie, result.response.headers);
    return result.body;
}

async function createOrganization(origin, principal, name, slug) {
    const organization = await authPost(origin, principal, "/organization/create", {
        name,
        slug,
        keepCurrentActiveOrganization: true,
    });
    assert(organization?.id, "organization creation returned no id");
    await authPost(origin, principal, "/organization/set-active", { organizationId: organization.id });
    await refreshPrincipal(origin, principal);
    return organization;
}

async function joinOrganization(origin, owner, member, organizationId) {
    const invitation = await authPost(origin, owner, "/organization/invite-member", {
        email: member.session.user.email,
        role: "member",
        organizationId,
    });
    assert(invitation?.id, "organization invitation returned no id");
    await authPost(origin, member, "/organization/accept-invitation", { invitationId: invitation.id });
    await authPost(origin, member, "/organization/set-active", { organizationId });
    await refreshPrincipal(origin, member);
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
    return async function next(predicate, timeoutMs = 20_000) {
        const found = queued.findIndex(predicate);
        if (found >= 0) return queued.splice(found, 1)[0];
        const message = await new Promise((resolve, reject) => {
            const waiter = { resolve, reject };
            waiters.push(waiter);
            const timeout = setTimeout(() => {
                const index = waiters.indexOf(waiter);
                if (index >= 0) waiters.splice(index, 1);
                reject(new Error(`timed out waiting for Gateway message; queued=${JSON.stringify(queued)}`));
            }, timeoutMs);
            waiter.resolve = value => {
                clearTimeout(timeout);
                resolve(value);
            };
        });
        if (predicate(message)) return message;
        queued.push(message);
        return next(predicate, timeoutMs);
    };
}

async function connectGateway(origin, jwt) {
    const clientId = crypto.randomUUID();
    const url = new URL(`/ws?clientId=${encodeURIComponent(clientId)}`, origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("timed out opening Gateway WebSocket")), 20_000);
        socket.addEventListener("open", () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.addEventListener("error", () => {
            clearTimeout(timeout);
            reject(new Error("Gateway WebSocket failed to open"));
        });
    });
    const next = socketInbox(socket);
    socket.send(JSON.stringify({ t: "hello", protocolV: 3, clientId, jwt }));
    const welcome = await next(message => message.t === "welcome" || message.t === "error");
    assert(welcome.t === "welcome", `Gateway rejected JWT: ${JSON.stringify(welcome)}`);
    return { socket, next };
}

async function closeSocket(socket) {
    if (socket.readyState === WebSocket.CLOSED) return;
    await new Promise(resolve => {
        const timeout = setTimeout(resolve, 5_000);
        socket.addEventListener("close", () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.close();
    });
}

function percentile(sorted, fraction) {
    return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function metric(latencies, elapsedMs, concurrency) {
    const sorted = [...latencies].sort((left, right) => left - right);
    return {
        operations: latencies.length,
        concurrency,
        elapsedMs,
        operationsPerSecond: (latencies.length * 1_000) / elapsedMs,
        rawLatencyMs: [...latencies],
        latencyMs: { min: sorted[0], p50: percentile(sorted, 0.5), p95: percentile(sorted, 0.95), max: sorted.at(-1) },
    };
}

async function main() {
    const options = parseArgs(process.argv.slice(2));
    const startedAt = new Date().toISOString();
    const health = await requestJson(options.origin, "/health");
    assert(health.response.ok && health.body?.ok === true, `health check failed: ${JSON.stringify(health.body)}`);
    assert(
        health.body.releaseSha256 === options.sha256,
        `target release digest mismatch: expected ${options.sha256}, received ${JSON.stringify(health.body.releaseSha256)}`
    );

    const suffix = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
    const owner = await signIn(options.origin);
    const member = await signIn(options.origin);
    const primaryOrg = await createOrganization(options.origin, owner, "Benchmark primary", `bench-primary-${suffix}`);
    const isolatedOrg = await createOrganization(
        options.origin,
        owner,
        "Benchmark isolated",
        `bench-isolated-${suffix}`
    );
    await authPost(options.origin, owner, "/organization/set-active", { organizationId: primaryOrg.id });
    await refreshPrincipal(options.origin, owner);
    await joinOrganization(options.origin, owner, member, primaryOrg.id);

    const primary = await connectGateway(options.origin, owner.token);
    const observer = await connectGateway(options.origin, member.token);
    const sockets = [primary.socket, observer.socket];
    try {
        const args = { organizationId: primaryOrg.id, limit: 50 };
        const expectedRows = [];
        for (const connection of [primary, observer]) {
            connection.socket.send(JSON.stringify({ t: "sub", subId: 1, ref: "query#listMessages", args }));
        }
        const initial = await Promise.all(
            [primary, observer].map(connection =>
                connection.next(message => (message.t === "snapshot" && message.subId === 1) || message.t === "error")
            )
        );
        assert(
            initial.every(message => message.t === "snapshot" && message.rows.length === 0),
            "initial snapshots diverged"
        );
        primary.socket.send(JSON.stringify({ t: "ack", cookie: initial[0].cookie }));
        observer.socket.send(JSON.stringify({ t: "ack", cookie: initial[1].cookie }));

        for (let index = 0; index < 2; index += 1) {
            const mutId = `seed-${suffix}-${index}`;
            const message = {
                id: `seed-message-${suffix}-${index}`,
                organizationId: primaryOrg.id,
                authorId: owner.session.user.id,
                body: `seed ${index}`,
                createdAt: index + 1,
            };
            primary.socket.send(
                JSON.stringify({
                    t: "mut",
                    mutId,
                    ref: "mutation#postMessage",
                    args: {
                        id: message.id,
                        organizationId: primaryOrg.id,
                        body: message.body,
                        clientCreatedAt: message.createdAt,
                    },
                })
            );
            const result = await primary.next(
                message => message.t === "error" || message.mutResults?.some(entry => entry.mutId === mutId)
            );
            const mutationResult = result.mutResults?.find(entry => entry.mutId === mutId);
            assert(
                mutationResult?.ok === true && mutationResult.result?.id === message.id,
                `seed mutation failed: ${JSON.stringify(result)}`
            );
            expectedRows.unshift(message);
            const updates = await Promise.all(
                [primary, observer].map(connection =>
                    connection.next(
                        update =>
                            (update.t === "snapshot" && update.rows?.length === expectedRows.length) ||
                            update.t === "error"
                    )
                )
            );
            assert(
                updates.every(message => message.t === "snapshot"),
                "seed live delivery diverged"
            );
            for (const [updateIndex, update] of updates.entries()) {
                assertExactRows(update.rows, expectedRows, `seed snapshot ${updateIndex}`);
            }
            primary.socket.send(JSON.stringify({ t: "ack", cookie: updates[0].cookie }));
            observer.socket.send(JSON.stringify({ t: "ack", cookie: updates[1].cookie }));
        }

        const listUrl = new URL("/api/messages", options.origin);
        listUrl.searchParams.set("organizationId", primaryOrg.id);
        listUrl.searchParams.set("limit", "50");
        const directLatencies = [];
        const directStarted = performance.now();
        for (let offset = 0; offset < options.profile.directQueries; offset += options.profile.directConcurrency) {
            const count = Math.min(options.profile.directConcurrency, options.profile.directQueries - offset);
            await Promise.all(
                Array.from({ length: count }, async () => {
                    const started = performance.now();
                    const result = await requestJson(options.origin, listUrl.pathname + listUrl.search, {
                        headers: { authorization: `Bearer ${owner.token}` },
                    });
                    directLatencies.push(performance.now() - started);
                    assert(result.response.ok, `direct read failed (${result.response.status})`);
                    assertExactRows(result.body, expectedRows, "direct read");
                })
            );
        }
        const directElapsed = performance.now() - directStarted;

        const overLimit = new URL(listUrl);
        overLimit.searchParams.set("limit", "101");
        const overLimitResult = await requestJson(options.origin, overLimit.pathname + overLimit.search, {
            headers: { authorization: `Bearer ${owner.token}` },
        });
        assert(overLimitResult.response.status === 400, `over-limit read returned ${overLimitResult.response.status}`);

        const isolatedUrl = new URL(listUrl);
        isolatedUrl.searchParams.set("organizationId", isolatedOrg.id);
        const isolated = await requestJson(options.origin, isolatedUrl.pathname + isolatedUrl.search, {
            headers: { authorization: `Bearer ${owner.token}` },
        });
        assert(
            isolated.response.ok && isolated.body?.length === 0,
            `organization isolation failed: ${JSON.stringify(isolated.body)}`
        );

        const liveLatencies = [];
        const liveMutationAckLatencies = [];
        const liveOwnerSnapshotLatencies = [];
        const liveObserverSnapshotLatencies = [];
        let replayBody;
        let firstMutationResult;
        for (let index = 0; index < options.profile.liveUpdates; index += 1) {
            const mutId = `live-${suffix}-${index}`;
            const message = {
                id: `live-message-${suffix}-${index}`,
                organizationId: primaryOrg.id,
                authorId: owner.session.user.id,
                body: `live ${index}`,
                createdAt: index + 3,
            };
            const mutation = {
                t: "mut",
                mutId,
                ref: "mutation#postMessage",
                args: {
                    id: message.id,
                    organizationId: primaryOrg.id,
                    body: message.body,
                    clientCreatedAt: message.createdAt,
                },
            };
            const started = performance.now();
            primary.socket.send(JSON.stringify(mutation));
            const result = await primary.next(
                message => message.t === "error" || message.mutResults?.some(entry => entry.mutId === mutId)
            );
            const mutationResult = result.mutResults?.find(entry => entry.mutId === mutId);
            liveMutationAckLatencies.push(performance.now() - started);
            assert(
                mutationResult?.ok === true && mutationResult.result?.id === message.id,
                `live mutation failed: ${JSON.stringify(result)}`
            );
            if (index === 0) {
                replayBody = structuredClone(mutation);
                firstMutationResult = structuredClone(mutationResult.result);
            }
            expectedRows.unshift(message);
            const updates = await Promise.all(
                [primary, observer].map(async connection => {
                    const update = await connection.next(
                        update =>
                            (update.t === "snapshot" && update.rows?.length === expectedRows.length) ||
                            update.t === "error"
                    );
                    return { update, latencyMs: performance.now() - started };
                })
            );
            liveLatencies.push(performance.now() - started);
            liveOwnerSnapshotLatencies.push(updates[0].latencyMs);
            liveObserverSnapshotLatencies.push(updates[1].latencyMs);
            assert(
                updates.every(({ update }) => update.t === "snapshot"),
                "two-client live delivery diverged"
            );
            for (const [updateIndex, { update }] of updates.entries()) {
                assertExactRows(update.rows, expectedRows, `live snapshot ${updateIndex}`);
            }
            primary.socket.send(JSON.stringify({ t: "ack", cookie: updates[0].update.cookie }));
            observer.socket.send(JSON.stringify({ t: "ack", cookie: updates[1].update.cookie }));
        }
        const liveElapsed = liveLatencies.reduce((sum, value) => sum + value, 0);

        primary.socket.send(JSON.stringify(replayBody));
        const replay = await primary.next(
            message => message.t === "error" || message.mutResults?.some(entry => entry.mutId === replayBody.mutId)
        );
        const replayResult = replay.mutResults?.find(entry => entry.mutId === replayBody.mutId);
        assert(replayResult?.ok === true, `mutation replay failed: ${JSON.stringify(replay)}`);
        assert(
            JSON.stringify(replayResult.result) === JSON.stringify(firstMutationResult),
            `mutation replay result changed: ${JSON.stringify(replayResult.result)}`
        );
        const afterReplay = await requestJson(options.origin, listUrl.pathname + listUrl.search, {
            headers: { authorization: `Bearer ${owner.token}` },
        });
        assert(afterReplay.response.ok, `post-replay read failed (${afterReplay.response.status})`);
        assertExactRows(afterReplay.body, expectedRows, "post-replay read");

        const report = assertChatBenchmarkReport({
            schema: CHAT_BENCHMARK_SCHEMA,
            ok: true,
            workload: {
                id: CHAT_BENCHMARK_WORKLOAD_ID,
                driverVersion: CHAT_BENCHMARK_DRIVER_VERSION,
            },
            target: {
                kind: options.kind,
                origin: options.origin.origin,
                label: options.label,
                runtime: {
                    serverVersion: health.response.headers.get("cf-chardb-server-version"),
                    cfRay: health.response.headers.get("cf-ray"),
                    schemaVersion: health.body.schemaVersion ?? null,
                },
            },
            candidate: {
                sha256: options.sha256,
                verifiedByTarget: true,
                ...(options.deploymentVersion ? { deploymentVersion: options.deploymentVersion } : {}),
            },
            profile: {
                name: options.profileName,
                ...options.profile,
                liveConcurrency: 1,
                seedRows: 2,
                replacementClients: 2,
            },
            run: {
                startedAt,
                completedAt: new Date().toISOString(),
                processSamples: 1,
            },
            runner: {
                measuredAt: new Date().toISOString(),
                bun: Bun.version,
                operatingSystem: platform(),
                release: release(),
                architecture: arch(),
            },
            metrics: {
                directRead: metric(directLatencies, directElapsed, options.profile.directConcurrency),
                liveMutation: metric(liveLatencies, liveElapsed, 1),
                liveMutationAck: metric(
                    liveMutationAckLatencies,
                    liveMutationAckLatencies.reduce((sum, value) => sum + value, 0),
                    1
                ),
                liveOwnerSnapshot: metric(
                    liveOwnerSnapshotLatencies,
                    liveOwnerSnapshotLatencies.reduce((sum, value) => sum + value, 0),
                    1
                ),
                liveObserverSnapshot: metric(
                    liveObserverSnapshotLatencies,
                    liveObserverSnapshotLatencies.reduce((sum, value) => sum + value, 0),
                    1
                ),
            },
            invariants: {
                nativeBetterAuth: true,
                organizationIsolation: true,
                exactDirectRows: true,
                overLimitDenied: true,
                twoClientLiveDelivery: true,
                mutationReplayStable: true,
            },
        });
        await writeFile(options.output, `${JSON.stringify(report, null, 2)}\n`);
        console.log(JSON.stringify({ report: options.output, metrics: report.metrics }));
    } finally {
        await Promise.all(sockets.map(closeSocket));
    }
}

if (import.meta.main) await main();
