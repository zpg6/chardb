import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { rm } from "node:fs/promises";
import * as path from "node:path";
import { SignJWT, exportJWK, generateKeyPair } from "jose";
import { Miniflare } from "miniflare";
import { disposeMiniflareBounded } from "../../scripts/miniflare-lifecycle.mjs";
import { type ChardbClient, createChardbClient } from "../../src/client/index.ts";
import { GATEWAY_BUCKET_COUNT, gatewayBucketName } from "../../src/server/gateway-bucket.ts";
import { type ChardbRef, ClientId, MutId, type RawJson, SubId } from "../../src/types.ts";
import { type Down, PROTOCOL_V, type Up, decodeWire, encodeWire } from "../../src/wire.ts";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ENTRY = path.join(HERE, "gateway-live.entry.ts");
const BUNDLE = path.join(process.env.TMPDIR ?? "/tmp", `chardb-gateway-live-${process.pid}.bundle.mjs`);
const KID = "gateway-live-workerd-key";
const WORKER_NAME = "gateway-live-restart-worker";
const ISSUER = "https://issuer.example";
const AUDIENCE = "chardb-workerd";
const ORGANIZATION_A = "workerd-org";
const ORGANIZATION_B = "workerd-org-b";
const SCALE_CLIENTS_PER_TENANT = boundedIntegerEnv("CHARDB_WORKERD_CLIENTS_PER_TENANT", 1, 1, 8);
const SCALE_MUTATIONS_PER_TENANT = boundedIntegerEnv("CHARDB_WORKERD_MUTATIONS_PER_TENANT", 4, 1, 1_024);
const SCALE_MUTATION_BATCH = boundedIntegerEnv("CHARDB_WORKERD_MUTATION_BATCH", 16, 1, 32);
const SCALE_SUBSCRIPTIONS = boundedIntegerEnv("CHARDB_WORKERD_SUBSCRIPTIONS", 4, 1, 64);
const SCALE_REFRESH_ROUNDS = boundedIntegerEnv("CHARDB_WORKERD_REFRESH_ROUNDS", 2, 1, 64);
const SCALE_WAIT_MS = boundedIntegerEnv("CHARDB_WORKERD_WAIT_MS", 5_000, 1_000, 60_000);
const SCALE_TEST_TIMEOUT_MS = boundedIntegerEnv("CHARDB_WORKERD_TEST_TIMEOUT_MS", 30_000, 5_000, 300_000);
const USER_AXIS_BENCH_USERS = boundedIntegerEnv("CHARDB_WORKERD_USER_AXIS_USERS", 8, 2, 32);
const GLOBAL_AXIS_BENCH_PARTITIONS = boundedIntegerEnv("CHARDB_WORKERD_GLOBAL_AXIS_PARTITIONS", 8, 2, 32);

setDefaultTimeout(SCALE_TEST_TIMEOUT_MS);

function boundedIntegerEnv(name: string, fallback: number, minimum: number, maximum: number): number {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    if (!/^[0-9]+$/.test(raw)) throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
        throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
    }
    return value;
}

interface GatewayLiveState {
    readonly instanceId: string;
    readonly registrations: readonly {
        readonly registrationId: string;
        readonly connectionId: string;
        readonly clientId: string;
        readonly subId: number;
        readonly organizationId: string;
        readonly lifecycle: string;
        readonly cdbState: string;
        readonly dirtyVersion: number;
        readonly deliveredVersion: number;
        readonly initialSnapshotPending: boolean;
        readonly lastCookie: string | null;
        readonly lastSnapshotCookie: string | null;
        readonly currentHead: boolean;
        readonly outboxCookie: string | null;
        readonly outboxTargetVersion: number | null;
        readonly retryError: string | null;
    }[];
}

interface CdbLiveState {
    readonly instanceId: string;
    readonly domainRows: number;
    readonly opLogRows: number;
    readonly changeSeq: number;
    readonly subscriptions: readonly {
        readonly gatewayId: string;
        readonly registrationId: string;
        readonly clientId: string;
        readonly subId: number;
        readonly state: string;
        readonly organizationId: string | null;
    }[];
    readonly invalidations: readonly {
        readonly gatewayId: string;
        readonly registrationId: string;
        readonly changeSeq: number;
    }[];
}

interface OpenedSocket {
    readonly socket: WebSocket;
    readonly welcome: Down;
    readonly closed: Promise<CloseEvent>;
}

let mf: Miniflare | undefined;
let workerdUrl: URL | undefined;
let mutationRef: ChardbRef | undefined;
let queryRef: ChardbRef | undefined;
let publicQueryRef: ChardbRef | undefined;
let userMutationRef: ChardbRef | undefined;
let userQueryRef: ChardbRef | undefined;
let globalMutationRef: ChardbRef | undefined;
let globalQueryRef: ChardbRef | undefined;
let shardId = "";
let signToken: ((subject: string) => Promise<string>) | undefined;

async function buildWorker(): Promise<string> {
    try {
        const proc = Bun.spawn(
            [
                "bun",
                "build",
                ENTRY,
                "--target=browser",
                "--format=esm",
                "--external=cloudflare:workers",
                "--outfile",
                BUNDLE,
            ],
            { stdout: "pipe", stderr: "pipe" }
        );
        const exitCode = await proc.exited;
        if (exitCode !== 0) {
            throw new Error(`bundle failed (exit ${exitCode}):\n${await new Response(proc.stderr).text()}`);
        }
        let source = await Bun.file(BUNDLE).text();
        source = source.replace(
            "await import(this.#props.path.join(this.#props.migrationFolder, fileName))",
            'await Promise.reject(new Error("Node file migrations are unavailable in workerd"))'
        );
        source = source.replace(
            "await import(nodeSqlite)",
            'await Promise.reject(new Error("Node sqlite is unavailable in workerd"))'
        );
        if (/\bimport\s*\([^"'`]/.test(source)) {
            throw new Error("Worker bundle still contains an unsupported dynamic module specifier");
        }
        return source;
    } finally {
        await rm(BUNDLE, { force: true });
    }
}

async function openSocket(clientId: string, jwt: string, immediate?: Up): Promise<OpenedSocket> {
    if (!workerdUrl) throw new Error("Miniflare is not initialized");
    const url = new URL("/ws", workerdUrl);
    url.searchParams.set("clientId", clientId);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("timed out opening Gateway WebSocket")), 2_000);
        socket.addEventListener(
            "open",
            () => {
                clearTimeout(timeout);
                resolve();
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
    const welcome = nextDown(socket);
    const closed = new Promise<CloseEvent>(resolve => socket.addEventListener("close", resolve, { once: true }));
    socket.send(
        encodeWire({
            t: "hello",
            protocolV: PROTOCOL_V,
            clientId: ClientId(clientId),
            jwt,
        })
    );
    if (immediate) socket.send(encodeWire(immediate));
    return { socket, welcome: await welcome, closed };
}

interface DownWaiter {
    readonly resolve: (message: Down) => void;
    readonly reject: (error: Error) => void;
    readonly timeout: ReturnType<typeof setTimeout>;
}

const downInboxes = new WeakMap<WebSocket, { readonly queued: Down[]; readonly waiters: DownWaiter[] }>();

function downInbox(socket: WebSocket): { readonly queued: Down[]; readonly waiters: DownWaiter[] } {
    const existing = downInboxes.get(socket);
    if (existing) return existing;
    const inbox = { queued: [] as Down[], waiters: [] as DownWaiter[] };
    downInboxes.set(socket, inbox);
    socket.addEventListener("message", event => {
        const message = decodeWire(String(event.data)) as Down;
        const waiter = inbox.waiters.shift();
        if (!waiter) {
            inbox.queued.push(message);
            return;
        }
        clearTimeout(waiter.timeout);
        waiter.resolve(message);
    });
    socket.addEventListener(
        "close",
        event => {
            for (const waiter of inbox.waiters.splice(0)) {
                clearTimeout(waiter.timeout);
                waiter.reject(new Error(`Gateway closed before replying (${event.code}: ${event.reason})`));
            }
        },
        { once: true }
    );
    return inbox;
}

function nextDown(socket: WebSocket, timeoutMs = 3_000): Promise<Down> {
    const inbox = downInbox(socket);
    const queued = inbox.queued.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve, reject) => {
        const waiter = {
            resolve,
            reject,
            timeout: setTimeout(() => {
                const index = inbox.waiters.indexOf(waiter);
                if (index >= 0) inbox.waiters.splice(index, 1);
                reject(new Error("timed out waiting for Gateway message"));
            }, timeoutMs),
        };
        inbox.waiters.push(waiter);
    });
}

async function nextDownMatching<T extends Down>(
    socket: WebSocket,
    predicate: (message: Down) => message is T,
    label: string,
    timeoutMs = 3_000
): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    const skipped: Down[] = [];
    try {
        while (Date.now() < deadline) {
            const message = await nextDown(socket, Math.max(1, deadline - Date.now()));
            if (predicate(message)) return message;
            skipped.push(message);
        }
        throw new Error(`timed out waiting for ${label}`);
    } finally {
        if (skipped.length > 0) downInbox(socket).queued.unshift(...skipped);
    }
}

function nextMutationResult(
    socket: WebSocket,
    mutId: string,
    timeoutMs = 3_000
): Promise<Extract<Down, { t: "poke" }>> {
    return nextDownMatching(
        socket,
        (message): message is Extract<Down, { t: "poke" }> =>
            message.t === "poke" && message.mutResults?.some(result => result.mutId === mutId) === true,
        `mutation result ${mutId}`,
        timeoutMs
    );
}

async function nextAfterAcknowledgedSnapshot(
    socket: WebSocket,
    acknowledged: Extract<Down, { t: "snapshot" }>,
    timeoutMs = SCALE_WAIT_MS
): Promise<Down> {
    const deadline = Date.now() + timeoutMs;
    const acknowledgedRows = JSON.stringify(acknowledged.rows);
    while (Date.now() < deadline) {
        const message = await nextDown(socket, Math.max(1, deadline - Date.now()));
        // A fixture drain and a WebSocket acknowledgement use separate event
        // sources. A second pre-mutation materialization can already be in
        // flight with a new cookie when durable delivery state becomes idle.
        if (
            message.t !== "snapshot" ||
            message.subId !== acknowledged.subId ||
            JSON.stringify(message.rows) !== acknowledgedRows
        ) {
            return message;
        }
        expect(message).toMatchObject({ t: "snapshot", subId: acknowledged.subId, rows: acknowledged.rows });
        acknowledge(socket, message);
    }
    throw new Error(`timed out after acknowledged rows for subscription ${acknowledged.subId}`);
}

async function fixtureFetch<T>(pathname: string, search: Record<string, string>): Promise<T> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const url = new URL(pathname, "http://example.com");
    for (const [key, value] of Object.entries(search)) url.searchParams.set(key, value);
    const response = await mf.dispatchFetch(url);
    if (!response.ok) throw new Error(`${pathname} failed: ${response.status} ${await response.text()}`);
    return (await response.json()) as T;
}

async function mutateMembership(action: "delete" | "upsert", role?: string): Promise<{ readonly affected?: number }> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-membership", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
            action,
            organizationId: ORGANIZATION_A,
            userId: "workerd-user",
            ...(role === undefined ? {} : { role }),
        }),
    });
    if (!response.ok) throw new Error(`membership mutation failed: ${response.status} ${await response.text()}`);
    return (await response.json()) as { readonly affected?: number };
}

async function drainAuthInvalidations(additionalShardIds: readonly string[] = []): Promise<void> {
    await fixtureFetch("/live-catalog-drain", {});
    await fixtureFetch("/live-cdb-drain", { shardId });
    for (const additionalShardId of additionalShardIds) {
        await fixtureFetch("/live-cdb-drain", { shardId: additionalShardId });
    }
}

async function settleAuthInvalidations(additionalShardIds: readonly string[] = []): Promise<void> {
    let lastState:
        | {
              readonly targets: number;
              readonly principals: number;
              readonly global: number;
              readonly nextTargetAt: number | null;
              readonly lastTargetError: string | null;
          }
        | undefined;
    for (let turn = 0; turn < 256; turn++) {
        const state = await fixtureFetch<{
            readonly targets: number;
            readonly principals: number;
            readonly global: number;
            readonly nextTargetAt: number | null;
            readonly lastTargetError: string | null;
        }>("/live-auth-invalidation-state", {});
        lastState = state;
        if (state.targets === 0 && state.principals === 0 && state.global === 0) {
            await fixtureFetch("/live-cdb-drain", { shardId });
            for (const additionalShardId of additionalShardIds) {
                await fixtureFetch("/live-cdb-drain", { shardId: additionalShardId });
            }
            return;
        }
        await fixtureFetch("/live-catalog-drain", {});
        await Bun.sleep(1);
    }
    throw new Error(`Catalog auth invalidation work exceeded 256 bounded fixture turns: ${JSON.stringify(lastState)}`);
}

async function mutateFixtureMembership(
    organizationId: string,
    userId: string,
    action: "delete" | "upsert" = "upsert"
): Promise<void> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-membership", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action, organizationId, userId, role: "member" }),
    });
    if (!response.ok) throw new Error(`membership mutation failed: ${response.status} ${await response.text()}`);
}

async function mutateFixtureOrganization(id: string, action: "create" | "delete"): Promise<void> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-organization", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id, action }),
    });
    if (!response.ok) throw new Error(`organization mutation failed: ${response.status} ${await response.text()}`);
}

async function routeFixtureOrganization(organizationId: string, shardId: string): Promise<void> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-route-organization", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ organizationId, shardId }),
    });
    if (!response.ok) throw new Error(`organization route failed: ${response.status} ${await response.text()}`);
}

async function createFixtureUser(userId: string): Promise<void> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-user", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: userId }),
    });
    if (!response.ok) throw new Error(`user creation failed: ${response.status} ${await response.text()}`);
}

async function deleteFixtureUser(userId: string): Promise<void> {
    if (!mf) throw new Error("Miniflare is not initialized");
    const response = await mf.dispatchFetch("http://example.com/live-user", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: userId, action: "delete" }),
    });
    if (!response.ok) throw new Error(`user deletion failed: ${response.status} ${await response.text()}`);
}

async function gatewayState(clientId: string): Promise<GatewayLiveState> {
    return fixtureFetch("/live-gateway-state", { clientId });
}

async function drainGateway(clientId: string): Promise<void> {
    await fixtureFetch("/live-gateway-drain", { clientId });
}

function collocatedGatewayClientIds(prefix: string, count: number): string[] {
    const clientIds: string[] = [];
    let bucketName: string | undefined;
    const searchLimit = GATEWAY_BUCKET_COUNT * count * 8;
    for (let index = 0; index < searchLimit && clientIds.length < count; index++) {
        const clientId = `${prefix}-${index.toString().padStart(6, "0")}`;
        const candidateBucket = gatewayBucketName(clientId);
        bucketName ??= candidateBucket;
        if (candidateBucket === bucketName) clientIds.push(clientId);
    }
    if (clientIds.length !== count) {
        throw new Error(`could not find ${count} deterministic client ids for one Gateway bucket`);
    }
    return clientIds;
}

async function stageGateway(clientId: string): Promise<void> {
    await fixtureFetch("/live-gateway-stage", { clientId });
}

async function currentRegistration(
    clientId: string,
    subId: number
): Promise<GatewayLiveState["registrations"][number]> {
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
        const state = await gatewayState(clientId);
        const registration = state.registrations.find(row => row.subId === subId && row.currentHead);
        if (registration?.lifecycle === "active" && registration.cdbState === "active") return registration;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for active registration ${clientId}:${subId}`);
}

async function waitForNoRegistration(clientId: string, subId: number): Promise<void> {
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
        const state = await gatewayState(clientId);
        if (!state.registrations.some(row => row.subId === subId && row.currentHead)) return;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for registration retirement ${clientId}:${subId}`);
}

async function subscribe(
    opened: OpenedSocket,
    clientId: string,
    subId: number,
    organizationId: string,
    body = "live-proof"
): Promise<Extract<Down, { t: "snapshot" }>> {
    if (!queryRef) throw new Error("query ref was not seeded");
    const snapshot = nextDown(opened.socket);
    opened.socket.send(
        encodeWire({
            t: "sub",
            subId: SubId(subId),
            ref: queryRef,
            args: { organizationId, body },
        })
    );
    await currentRegistration(clientId, subId);
    await drainGateway(clientId);
    const message = await snapshot;
    if (message.t !== "snapshot") throw new Error(`expected snapshot, received ${message.t}`);
    return message;
}

async function subscribeGlobal(
    opened: OpenedSocket,
    clientId: string,
    subId: number,
    namespace: string
): Promise<Extract<Down, { t: "snapshot" }>> {
    if (!globalQueryRef) throw new Error("global query ref was not seeded");
    const snapshot = nextDown(opened.socket);
    opened.socket.send(
        encodeWire({
            t: "sub",
            subId: SubId(subId),
            ref: globalQueryRef,
            args: { namespace },
        })
    );
    await currentRegistration(clientId, subId);
    await drainGateway(clientId);
    const message = await snapshot;
    if (message.t !== "snapshot") throw new Error(`expected global snapshot, received ${message.t}`);
    return message;
}

async function mutateGlobal(
    opened: OpenedSocket,
    mutId: string,
    input: { readonly id: string; readonly namespace: string; readonly storedNamespace: string; readonly value: string }
): Promise<Down> {
    if (!globalMutationRef) throw new Error("global mutation ref was not seeded");
    const result = nextMutationResult(opened.socket, mutId);
    opened.socket.send(
        encodeWire({
            t: "mut",
            mutId: MutId(mutId),
            ref: globalMutationRef,
            args: input,
        })
    );
    return result;
}

function acknowledge(socket: WebSocket, snapshot: Extract<Down, { t: "snapshot" }>): void {
    socket.send(encodeWire({ t: "ack", cookie: snapshot.cookie }));
}

async function expectNoDown(socket: WebSocket, waitMs = 100): Promise<void> {
    await new Promise<void>((resolve, reject) => {
        const cleanup = () => {
            clearTimeout(timeout);
            socket.removeEventListener("message", onMessage);
        };
        const onMessage = (event: MessageEvent) => {
            cleanup();
            reject(new Error(`received unexpected Gateway message: ${String(event.data)}`));
        };
        const timeout = setTimeout(() => {
            cleanup();
            resolve();
        }, waitMs);
        socket.addEventListener("message", onMessage);
    });
}

async function signed(subject: string): Promise<string> {
    if (!signToken) throw new Error("JWT signer is not initialized");
    return signToken(subject);
}

interface ScaleRow {
    readonly id: string;
    readonly organizationId: string;
    readonly authorId: string;
    readonly body: string;
    readonly createdAt: number;
}

interface QueryObservation {
    readonly rows: readonly ScaleRow[];
    readonly state: string;
}

interface QueryObserver {
    readonly listener: (rows: ScaleRow[], state?: string) => void;
    readonly latest: () => QueryObservation | null;
    readonly waitFor: (
        predicate: (observation: QueryObservation) => boolean,
        label: string
    ) => Promise<QueryObservation>;
}

function createQueryObserver(): QueryObserver {
    let current: QueryObservation | null = null;
    const waiters = new Set<() => void>();
    return {
        listener(rows, state) {
            current = { rows: rows.map(row => ({ ...row })), state: state ?? "missing" };
            for (const wake of [...waiters]) wake();
        },
        latest: () => current,
        async waitFor(predicate, label) {
            const deadline = Date.now() + SCALE_WAIT_MS;
            while (current === null || !predicate(current)) {
                const remaining = deadline - Date.now();
                if (remaining <= 0) {
                    throw new Error(`timed out waiting for ${label}; latest=${JSON.stringify(current)}`);
                }
                await new Promise<void>((resolve, reject) => {
                    const wake = () => {
                        clearTimeout(timeout);
                        waiters.delete(wake);
                        resolve();
                    };
                    const timeout = setTimeout(() => {
                        waiters.delete(wake);
                        reject(new Error(`timed out waiting for ${label}; latest=${JSON.stringify(current)}`));
                    }, remaining);
                    waiters.add(wake);
                });
            }
            return current;
        },
    };
}

function sdkEndpoint(): string {
    if (!workerdUrl) throw new Error("Miniflare is not initialized");
    const url = new URL("/ws", workerdUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return url.toString();
}

async function createSdkClient(clientId: string, subject: string): Promise<ChardbClient> {
    const jwt = await signed(subject);
    return createChardbClient({
        endpoint: sdkEndpoint(),
        clientId,
        getJwt: async () => jwt,
        mutationTimeoutMs: SCALE_WAIT_MS,
    });
}

async function createSdkClientWithTrackedClose(
    clientId: string,
    subject: string
): Promise<{ readonly client: ChardbClient; readonly socketClosed: Promise<void> }> {
    const NativeWebSocket = globalThis.WebSocket;
    const jwt = await signed(subject);
    let settleCreated: (() => void) | undefined;
    let settleClosed: (() => void) | undefined;
    const socketCreated = new Promise<void>(resolve => {
        settleCreated = resolve;
    });
    const socketClosed = new Promise<void>(resolve => {
        settleClosed = resolve;
    });

    class CloseTrackingWebSocket {
        static readonly CONNECTING = NativeWebSocket.CONNECTING;
        static readonly OPEN = NativeWebSocket.OPEN;
        static readonly CLOSING = NativeWebSocket.CLOSING;
        static readonly CLOSED = NativeWebSocket.CLOSED;
        private readonly inner: WebSocket;

        constructor(url: string | URL) {
            this.inner = new NativeWebSocket(url);
            this.inner.addEventListener("close", () => settleClosed?.(), { once: true });
            settleCreated?.();
        }

        get readyState(): number {
            return this.inner.readyState;
        }

        get onopen(): WebSocket["onopen"] {
            return this.inner.onopen;
        }

        set onopen(listener: WebSocket["onopen"]) {
            this.inner.onopen = listener;
        }

        get onmessage(): WebSocket["onmessage"] {
            return this.inner.onmessage;
        }

        set onmessage(listener: WebSocket["onmessage"]) {
            this.inner.onmessage = listener;
        }

        get onclose(): WebSocket["onclose"] {
            return this.inner.onclose;
        }

        set onclose(listener: WebSocket["onclose"]) {
            this.inner.onclose = listener;
        }

        get onerror(): WebSocket["onerror"] {
            return this.inner.onerror;
        }

        set onerror(listener: WebSocket["onerror"]) {
            this.inner.onerror = listener;
        }

        send(data: Parameters<WebSocket["send"]>[0]): void {
            this.inner.send(data);
        }

        close(code?: number, reason?: string): void {
            this.inner.close(code, reason);
        }
    }

    let client: ChardbClient | undefined;
    let creationTimer: ReturnType<typeof setTimeout> | undefined;
    let created = false;
    try {
        globalThis.WebSocket = CloseTrackingWebSocket as unknown as typeof WebSocket;
        client = createChardbClient({
            endpoint: sdkEndpoint(),
            clientId,
            getJwt: async () => jwt,
            mutationTimeoutMs: SCALE_WAIT_MS,
        });
        await Promise.race([
            socketCreated,
            new Promise<never>((_, reject) => {
                creationTimer = setTimeout(
                    () => reject(new Error(`timed out constructing SDK WebSocket for ${clientId}`)),
                    SCALE_WAIT_MS
                );
            }),
        ]);
        created = true;
        return { client, socketClosed };
    } finally {
        if (creationTimer) clearTimeout(creationTimer);
        globalThis.WebSocket = NativeWebSocket;
        if (!created) {
            client?.close();
            settleClosed?.();
        }
    }
}

interface DroppedMutationResult {
    readonly mutId: MutId;
    readonly result: RawJson;
    readonly raw: string;
}

interface ArmedMutationResponseLoss {
    readonly dropped: Promise<DroppedMutationResult>;
    readonly replacementHeld: Promise<void>;
    releaseReplacement(): void;
}

interface MutationResponseLossClient {
    readonly client: ChardbClient;
    armNextSuccessfulResult(): ArmedMutationResponseLoss;
    successfulResultCount(mutId: MutId): number;
    restoreWebSocket(): void;
}

interface ActiveMutationResponseLoss {
    dropPending: boolean;
    readonly settleDropped: (result: DroppedMutationResult) => void;
    readonly settleReplacementHeld: () => void;
    releaseReplacement?: () => void;
}

async function createSdkClientWithMutationResponseLoss(
    clientId: string,
    subject: string
): Promise<MutationResponseLossClient> {
    const NativeWebSocket = globalThis.WebSocket;
    const jwt = await signed(subject);
    const successfulResults = new Map<MutId, number>();
    let armed: ActiveMutationResponseLoss | undefined;
    let holdReplacementOpen = false;

    class MutationResponseDroppingWebSocket {
        static readonly CONNECTING = NativeWebSocket.CONNECTING;
        static readonly OPEN = NativeWebSocket.OPEN;
        static readonly CLOSING = NativeWebSocket.CLOSING;
        static readonly CLOSED = NativeWebSocket.CLOSED;
        private readonly inner: WebSocket;
        private readonly owned: boolean;

        constructor(url: string | URL) {
            this.owned = new URL(url).searchParams.get("clientId") === clientId;
            this.inner = new NativeWebSocket(url);
        }

        get readyState(): number {
            return this.inner.readyState;
        }

        get onopen(): WebSocket["onopen"] {
            return this.inner.onopen;
        }

        set onopen(listener: WebSocket["onopen"]) {
            if (listener === null) {
                this.inner.onopen = null;
                return;
            }
            this.inner.onopen = event => {
                if (!this.owned || !holdReplacementOpen) {
                    listener.call(this.inner, event);
                    return;
                }
                holdReplacementOpen = false;
                const current = armed;
                if (!current) throw new Error("replacement socket opened without an armed response loss");
                current.releaseReplacement = () => listener.call(this.inner, event);
                current.settleReplacementHeld();
            };
        }

        get onmessage(): WebSocket["onmessage"] {
            return this.inner.onmessage;
        }

        set onmessage(listener: WebSocket["onmessage"]) {
            if (listener === null) {
                this.inner.onmessage = null;
                return;
            }
            this.inner.onmessage = event => {
                if (this.owned && typeof event.data === "string") {
                    const message = decodeWire(event.data) as Down;
                    if (message.t === "poke") {
                        const result = message.mutResults?.find(candidate => candidate.ok);
                        if (result?.ok) {
                            successfulResults.set(result.mutId, (successfulResults.get(result.mutId) ?? 0) + 1);
                            const current = armed;
                            if (current?.dropPending) {
                                current.dropPending = false;
                                holdReplacementOpen = true;
                                current.settleDropped({
                                    mutId: result.mutId,
                                    result: result.result,
                                    raw: event.data,
                                });
                                this.inner.close();
                                return;
                            }
                        }
                    }
                }
                listener.call(this.inner, event);
            };
        }

        get onclose(): WebSocket["onclose"] {
            return this.inner.onclose;
        }

        set onclose(listener: WebSocket["onclose"]) {
            this.inner.onclose = listener;
        }

        get onerror(): WebSocket["onerror"] {
            return this.inner.onerror;
        }

        set onerror(listener: WebSocket["onerror"]) {
            this.inner.onerror = listener;
        }

        send(data: Parameters<WebSocket["send"]>[0]): void {
            this.inner.send(data);
        }

        close(code?: number, reason?: string): void {
            this.inner.close(code, reason);
        }
    }

    let restored = false;
    const restoreWebSocket = () => {
        if (restored) return;
        restored = true;
        if (globalThis.WebSocket === (MutationResponseDroppingWebSocket as unknown as typeof WebSocket)) {
            globalThis.WebSocket = NativeWebSocket;
        }
    };
    let client: ChardbClient;
    try {
        globalThis.WebSocket = MutationResponseDroppingWebSocket as unknown as typeof WebSocket;
        client = createChardbClient({
            endpoint: sdkEndpoint(),
            clientId,
            getJwt: async () => jwt,
            mutationTimeoutMs: SCALE_WAIT_MS,
        });
    } catch (error) {
        restoreWebSocket();
        throw error;
    }
    return {
        client,
        armNextSuccessfulResult() {
            if (armed || holdReplacementOpen) throw new Error("a mutation response loss is already armed");
            let settleDropped: ((result: DroppedMutationResult) => void) | undefined;
            let settleReplacementHeld: (() => void) | undefined;
            const dropped = new Promise<DroppedMutationResult>(resolve => {
                settleDropped = resolve;
            });
            const replacementHeld = new Promise<void>(resolve => {
                settleReplacementHeld = resolve;
            });
            const session: ActiveMutationResponseLoss = {
                dropPending: true,
                settleDropped: result => settleDropped?.(result),
                settleReplacementHeld: () => settleReplacementHeld?.(),
            };
            armed = session;
            return {
                dropped,
                replacementHeld,
                releaseReplacement() {
                    if (armed !== session || !session.releaseReplacement) {
                        throw new Error("replacement socket has not reached its open event");
                    }
                    const release = session.releaseReplacement;
                    armed = undefined;
                    release();
                },
            };
        },
        successfulResultCount(mutId) {
            return successfulResults.get(mutId) ?? 0;
        },
        restoreWebSocket,
    };
}

async function drainUntilSettled(clientId: string, subIds: readonly number[]): Promise<GatewayLiveState> {
    const deadline = Date.now() + SCALE_WAIT_MS;
    let latest: GatewayLiveState | null = null;
    while (Date.now() < deadline) {
        await drainGateway(clientId);
        const state = await gatewayState(clientId);
        latest = state;
        const settled = subIds.every(subId => {
            const row = state.registrations.find(
                candidate => candidate.clientId === clientId && candidate.subId === subId && candidate.currentHead
            );
            return (
                row?.lifecycle === "active" &&
                row.cdbState === "active" &&
                !row.initialSnapshotPending &&
                row.dirtyVersion === row.deliveredVersion &&
                row.outboxCookie === null &&
                row.outboxTargetVersion === null
            );
        });
        if (settled) return state;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    const unresolved = subIds.filter(subId => {
        const row = latest?.registrations.find(
            candidate => candidate.clientId === clientId && candidate.subId === subId && candidate.currentHead
        );
        return (
            row?.lifecycle !== "active" ||
            row.cdbState !== "active" ||
            row.initialSnapshotPending ||
            row.dirtyVersion !== row.deliveredVersion ||
            row.outboxCookie !== null ||
            row.outboxTargetVersion !== null
        );
    });
    throw new Error(`Gateway did not settle ${clientId} subscriptions ${unresolved.join(",")}`);
}

async function cleanupSdkClient(
    clientId: string,
    client: ChardbClient,
    subscriptions: readonly { unsubscribe: () => void }[]
): Promise<void> {
    for (const subscription of subscriptions) {
        try {
            subscription.unsubscribe();
        } catch {
            // close() below remains the terminal cleanup path.
        }
    }
    client.close();
    const deadline = Date.now() + SCALE_WAIT_MS;
    while (Date.now() < deadline) {
        await drainGateway(clientId);
        const state = await gatewayState(clientId);
        if (!state.registrations.some(row => row.clientId === clientId && row.currentHead)) return;
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error(`timed out cleaning SDK client ${clientId}`);
}

async function inBatches<T>(items: readonly T[], batchSize: number, run: (item: T) => Promise<void>): Promise<void> {
    for (let offset = 0; offset < items.length; offset += batchSize) {
        await Promise.all(items.slice(offset, offset + batchSize).map(run));
    }
}

function rate(count: number, durationMs: number): number {
    return durationMs === 0 ? 0 : Number(((count * 1_000) / durationMs).toFixed(2));
}

function expectCdbMutationDelta(before: CdbLiveState, after: CdbLiveState, count: number): void {
    expect(after.domainRows - before.domainRows).toBe(count);
    expect(after.opLogRows - before.opLogRows).toBe(count);
    expect(after.changeSeq - before.changeSeq).toBe(count);
}

beforeAll(async () => {
    const { privateKey, publicKey } = await generateKeyPair("ES256");
    const publicJwk = { ...(await exportJWK(publicKey)), kid: KID, alg: "ES256", use: "sig" };
    signToken = async subject => {
        const now = Math.floor(Date.now() / 1_000);
        return new SignJWT({ probe: "gateway-live-workerd" })
            .setProtectedHeader({ alg: "ES256", kid: KID })
            .setSubject(subject)
            .setIssuer(ISSUER)
            .setAudience(AUDIENCE)
            .setIssuedAt(now)
            .setExpirationTime(now + 300)
            .sign(privateKey);
    };

    mf = new Miniflare({
        name: WORKER_NAME,
        modules: true,
        script: await buildWorker(),
        durableObjects: {
            CDB_CATALOG: { className: "Catalog", useSQLite: true },
            CDB_GATEWAY: { className: "Gateway", useSQLite: true },
            CDB_SHARD: { className: "Cdb", useSQLite: true },
            CDB_RESHARD: { className: "Resharder", useSQLite: true },
        },
        compatibilityDate: "2025-09-01",
        compatibilityFlags: ["nodejs_compat"],
    });
    workerdUrl = await mf.ready;
    const seeded = await mf.dispatchFetch("http://example.com/seed", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kid: KID, jwk: publicJwk }),
    });
    if (!seeded.ok) throw new Error(`failed to seed live fixture: ${seeded.status} ${await seeded.text()}`);
    const seed = (await seeded.json()) as {
        readonly mutationRef: ChardbRef;
        readonly queryRef: ChardbRef;
        readonly publicQueryRef: ChardbRef;
        readonly userMutationRef: ChardbRef;
        readonly userQueryRef: ChardbRef;
        readonly globalMutationRef: ChardbRef;
        readonly globalQueryRef: ChardbRef;
        readonly shardA: string;
        readonly shardB: string;
    };
    mutationRef = seed.mutationRef;
    queryRef = seed.queryRef;
    publicQueryRef = seed.publicQueryRef;
    userMutationRef = seed.userMutationRef;
    userQueryRef = seed.userQueryRef;
    globalMutationRef = seed.globalMutationRef;
    globalQueryRef = seed.globalQueryRef;
    shardId = seed.shardA;
    expect(seed.shardA).toBe(seed.shardB);
    await fixtureFetch("/live-public-seed", { shardId });
});

afterAll(async () => {
    await disposeMiniflareBounded(mf, { label: "Gateway live fixture final teardown" });
    mf = undefined;
});

describe("public durable live queries in real workerd", () => {
    test("publicRead remains JWT-authenticated, membership-bound, and tenant-scoped", async () => {
        if (!publicQueryRef) throw new Error("public query ref was not seeded");
        const clientId = "public-reader-08";
        const opened = await openSocket(clientId, await signed("workerd-user"));
        expect(opened.welcome.t).toBe("welcome");

        const snapshotMessage = nextDown(opened.socket);
        opened.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(20),
                ref: publicQueryRef,
                args: { organizationId: ORGANIZATION_A },
            })
        );
        await currentRegistration(clientId, 20);
        await drainGateway(clientId);
        const snapshot = await snapshotMessage;
        expect(snapshot).toEqual({
            t: "snapshot",
            subId: SubId(20),
            cookie: expect.any(String),
            rows: [{ id: "public-org-a", organizationId: ORGANIZATION_A, label: "Organization A" }],
        });
        if (snapshot.t !== "snapshot") throw new Error("expected publicRead snapshot");
        acknowledge(opened.socket, snapshot);

        const forbidden = nextDown(opened.socket);
        opened.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(21),
                ref: publicQueryRef,
                args: { organizationId: ORGANIZATION_B },
            })
        );
        await expect(forbidden).resolves.toMatchObject({
            t: "error",
            subId: 21,
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        expect((await gatewayState(clientId)).registrations.some(row => row.subId === 21 && row.currentHead)).toBe(
            false
        );

        opened.socket.close();
        await opened.closed;
        await drainGateway(clientId);

        for (const [rejectedClientId, jwt] of [
            ["missing-jwt-09", ""],
            ["invalid-jwt-10", "not-a-jwt"],
        ] as const) {
            const rejected = await openSocket(rejectedClientId, jwt, {
                t: "sub",
                subId: SubId(1),
                ref: publicQueryRef,
                args: { organizationId: ORGANIZATION_A },
            });
            expect(rejected.welcome).toMatchObject({ t: "error", code: "CDB_FORBIDDEN", retryable: false });
            expect(rejected.welcome.t).not.toBe("welcome");
            await rejected.closed;
            expect((await gatewayState(rejectedClientId)).registrations.some(row => row.currentHead)).toBe(false);
        }
    });

    test("user authority isolates live queries and mutations by verified subject", async () => {
        if (!mf || !userMutationRef || !userQueryRef) throw new Error("user authority refs were not seeded");
        const clientA = "user-axis-a";
        const clientB = "user-axis-b";
        const openedA = await openSocket(clientA, await signed("workerd-user"));
        const openedB = await openSocket(clientB, await signed("workerd-user-2"));
        expect(openedA.welcome.t).toBe("welcome");
        expect(openedB.welcome.t).toBe("welcome");

        const initialA = nextDown(openedA.socket);
        openedA.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(30),
                ref: userQueryRef,
                args: { userId: "workerd-user" },
            })
        );
        await currentRegistration(clientA, 30);
        await drainGateway(clientA);
        const initialMessageA = await initialA;
        expect(initialMessageA).toMatchObject({ t: "snapshot", subId: 30, rows: [] });
        if (initialMessageA.t !== "snapshot") throw new Error("expected initial user snapshot");
        acknowledge(openedA.socket, initialMessageA);

        const forged = nextDown(openedA.socket);
        openedA.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(31),
                ref: userQueryRef,
                args: { userId: "workerd-user-2" },
            })
        );
        await expect(forged).resolves.toMatchObject({
            t: "error",
            subId: 31,
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        expect((await gatewayState(clientA)).registrations.some(row => row.subId === 31 && row.currentHead)).toBe(
            false
        );

        const mutationA = nextMutationResult(openedA.socket, "user-axis-write-a");
        openedA.socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("user-axis-write-a"),
                ref: userMutationRef,
                args: { id: "user-row-a", userId: "workerd-user", value: "alpha" },
            })
        );
        await expect(mutationA).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "user-axis-write-a", ok: true }],
        });
        const dirtyA = await currentRegistration(clientA, 30);
        if (dirtyA.dirtyVersion <= dirtyA.deliveredVersion) {
            throw new Error(`user authority subscription was not invalidated: ${JSON.stringify(dirtyA)}`);
        }
        const replacementA = nextDown(openedA.socket);
        await drainGateway(clientA);
        const afterDrainA = await currentRegistration(clientA, 30);
        if (afterDrainA.retryError) throw new Error(`user authority rerun failed: ${afterDrainA.retryError}`);
        const replacementMessageA = await replacementA;
        expect(replacementMessageA).toMatchObject({
            t: "snapshot",
            subId: 30,
            rows: [{ id: "user-row-a", userId: "workerd-user", value: "alpha" }],
        });
        if (replacementMessageA.t !== "snapshot") throw new Error("expected replacement user snapshot");
        acknowledge(openedA.socket, replacementMessageA);

        const gatewayBeforeEviction = await gatewayState(clientA);
        const cdbBeforeEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Gateway", {
            name: gatewayBucketName(clientA),
            webSockets: "hibernate",
        });
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Cdb", { name: shardId });
        const gatewayAfterEviction = await gatewayState(clientA);
        const cdbAfterEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        expect(gatewayAfterEviction.instanceId).not.toBe(gatewayBeforeEviction.instanceId);
        expect(cdbAfterEviction.instanceId).not.toBe(cdbBeforeEviction.instanceId);
        expect(gatewayAfterEviction.registrations).toEqual(gatewayBeforeEviction.registrations);

        const mutationAfterEviction = nextMutationResult(openedA.socket, "user-axis-write-after-eviction");
        openedA.socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("user-axis-write-after-eviction"),
                ref: userMutationRef,
                args: { id: "user-row-a-2", userId: "workerd-user", value: "after-eviction" },
            })
        );
        await expect(mutationAfterEviction).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "user-axis-write-after-eviction", ok: true }],
        });
        const dirtyAfterEviction = await currentRegistration(clientA, 30);
        expect(dirtyAfterEviction.dirtyVersion).toBeGreaterThan(dirtyAfterEviction.deliveredVersion);
        const replacementAfterEviction = nextDown(openedA.socket);
        await drainGateway(clientA);
        const replacementAfterEvictionMessage = await replacementAfterEviction;
        expect(replacementAfterEvictionMessage).toMatchObject({
            t: "snapshot",
            subId: 30,
            rows: [
                { id: "user-row-a", userId: "workerd-user", value: "alpha" },
                { id: "user-row-a-2", userId: "workerd-user", value: "after-eviction" },
            ],
        });
        if (replacementAfterEvictionMessage.t !== "snapshot") {
            throw new Error("expected user snapshot after Durable Object reconstruction");
        }
        acknowledge(openedA.socket, replacementAfterEvictionMessage);

        const initialB = nextDown(openedB.socket);
        openedB.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(30),
                ref: userQueryRef,
                args: { userId: "workerd-user-2" },
            })
        );
        await currentRegistration(clientB, 30);
        await drainGateway(clientB);
        const initialMessageB = await initialB;
        expect(initialMessageB).toMatchObject({ t: "snapshot", subId: 30, rows: [] });
        if (initialMessageB.t !== "snapshot") throw new Error("expected isolated user snapshot");
        acknowledge(openedB.socket, initialMessageB);

        const forgedMutation = nextMutationResult(openedA.socket, "user-axis-forged-write");
        openedA.socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("user-axis-forged-write"),
                ref: userMutationRef,
                args: { id: "forged-user-row", userId: "workerd-user-2", value: "forged" },
            })
        );
        await expect(forgedMutation).resolves.toMatchObject({
            t: "poke",
            mutResults: [
                {
                    mutId: "user-axis-forged-write",
                    ok: false,
                    error: { code: "CDB_FORBIDDEN", retryable: false },
                },
            ],
        });

        openedA.socket.close();
        openedB.socket.close();
        await Promise.all([openedA.closed, openedB.closed]);
        await Promise.all([drainGateway(clientA), drainGateway(clientB)]);
    });

    test("global authority shares one exact application partition and fences neighboring rows", async () => {
        if (!mf || !globalMutationRef || !globalQueryRef) throw new Error("global authority refs were not seeded");
        const clientA = "global-axis-a";
        const clientB = "global-axis-b";
        const namespace = "global-shared";
        const openedA = await openSocket(clientA, await signed("workerd-user"));
        const openedB = await openSocket(clientB, await signed("workerd-user-2"));
        expect(openedA.welcome.t).toBe("welcome");
        expect(openedB.welcome.t).toBe("welcome");

        const [initialA, initialB] = await Promise.all([
            subscribeGlobal(openedA, clientA, 50, namespace),
            subscribeGlobal(openedB, clientB, 50, namespace),
        ]);
        expect(initialA.rows).toEqual([]);
        expect(initialB.rows).toEqual([]);
        acknowledge(openedA.socket, initialA);
        acknowledge(openedB.socket, initialB);

        await expect(
            mutateGlobal(openedA, "global-axis-write-a", {
                id: "global-row-a",
                namespace,
                storedNamespace: namespace,
                value: "alpha",
            })
        ).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "global-axis-write-a", ok: true }],
        });
        const replacementA = nextDown(openedA.socket);
        const replacementB = nextDown(openedB.socket);
        await Promise.all([drainGateway(clientA), drainGateway(clientB)]);
        const [messageA, messageB] = await Promise.all([replacementA, replacementB]);
        const sharedRows = [{ id: "global-row-a", namespace, value: "alpha" }];
        expect(messageA).toMatchObject({ t: "snapshot", subId: 50, rows: sharedRows });
        expect(messageB).toMatchObject({ t: "snapshot", subId: 50, rows: sharedRows });
        if (messageA.t !== "snapshot" || messageB.t !== "snapshot") {
            throw new Error("expected shared global replacement snapshots");
        }
        acknowledge(openedA.socket, messageA);
        acknowledge(openedB.socket, messageB);

        await expect(
            mutateGlobal(openedA, "global-axis-misplaced-write", {
                id: "global-row-misplaced",
                namespace,
                storedNamespace: "global-neighbor",
                value: "misplaced",
            })
        ).resolves.toMatchObject({
            t: "poke",
            mutResults: [
                {
                    mutId: "global-axis-misplaced-write",
                    ok: false,
                    error: { code: "CDB_FORBIDDEN", retryable: false },
                },
            ],
        });

        const neighborInitial = await subscribeGlobal(openedA, clientA, 51, "global-neighbor");
        expect(neighborInitial.rows).toEqual([]);
        acknowledge(openedA.socket, neighborInitial);
        openedA.socket.send(encodeWire({ t: "unsub", subId: SubId(51) }));
        await waitForNoRegistration(clientA, 51);

        const gatewayBeforeEviction = await gatewayState(clientA);
        const cdbBeforeEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Gateway", {
            name: gatewayBucketName(clientA),
            webSockets: "hibernate",
        });
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Cdb", { name: shardId });
        const gatewayAfterEviction = await gatewayState(clientA);
        const cdbAfterEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        expect(gatewayAfterEviction.instanceId).not.toBe(gatewayBeforeEviction.instanceId);
        expect(cdbAfterEviction.instanceId).not.toBe(cdbBeforeEviction.instanceId);
        expect(gatewayAfterEviction.registrations).toEqual(gatewayBeforeEviction.registrations);

        await expect(
            mutateGlobal(openedB, "global-axis-write-after-eviction", {
                id: "global-row-b",
                namespace,
                storedNamespace: namespace,
                value: "after-eviction",
            })
        ).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "global-axis-write-after-eviction", ok: true }],
        });
        const afterEvictionA = nextDown(openedA.socket);
        const afterEvictionB = nextDown(openedB.socket);
        await Promise.all([drainGateway(clientA), drainGateway(clientB)]);
        const [afterEvictionMessageA, afterEvictionMessageB] = await Promise.all([afterEvictionA, afterEvictionB]);
        const reconstructedRows = [
            { id: "global-row-a", namespace, value: "alpha" },
            { id: "global-row-b", namespace, value: "after-eviction" },
        ];
        expect(afterEvictionMessageA).toMatchObject({ t: "snapshot", subId: 50, rows: reconstructedRows });
        expect(afterEvictionMessageB).toMatchObject({ t: "snapshot", subId: 50, rows: reconstructedRows });
        if (afterEvictionMessageA.t === "snapshot") acknowledge(openedA.socket, afterEvictionMessageA);
        if (afterEvictionMessageB.t === "snapshot") acknowledge(openedB.socket, afterEvictionMessageB);

        const revokedUser = "global-revoked-user";
        await createFixtureUser(revokedUser);
        const revokedClient = "global-revoked";
        const revoked = await openSocket(revokedClient, await signed(revokedUser));
        const revokedInitial = await subscribeGlobal(revoked, revokedClient, 52, "global-revoked-partition");
        expect(revokedInitial.rows).toEqual([]);
        acknowledge(revoked.socket, revokedInitial);
        await deleteFixtureUser(revokedUser);

        await expect(
            mutateGlobal(openedB, "global-axis-revocation-trigger", {
                id: "global-revocation-row",
                namespace: "global-revoked-partition",
                storedNamespace: "global-revoked-partition",
                value: "trigger",
            })
        ).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "global-axis-revocation-trigger", ok: true }],
        });
        const revokedFailure = nextDown(revoked.socket);
        await drainGateway(revokedClient);
        await expect(revokedFailure).resolves.toMatchObject({
            t: "error",
            subId: 52,
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        expect((await gatewayState(revokedClient)).registrations.some(row => row.subId === 52 && row.currentHead)).toBe(
            false
        );

        openedA.socket.close();
        openedB.socket.close();
        revoked.socket.close();
        await Promise.all([openedA.closed, openedB.closed, revoked.closed]);
        await Promise.all([drainGateway(clientA), drainGateway(clientB), drainGateway(revokedClient)]);
    });

    test("global-axis fanout stays exact across concurrent application partitions", async () => {
        if (!globalMutationRef || !globalQueryRef) throw new Error("global authority refs were not seeded");
        const partitions = Array.from(
            { length: GLOBAL_AXIS_BENCH_PARTITIONS },
            (_, index) => `global-bench-${index.toString().padStart(2, "0")}`
        );
        const clientIds = partitions.map((_, index) => `global-bench-client-${index.toString().padStart(2, "0")}`);

        const registrationStartedAt = performance.now();
        const opened = await Promise.all(
            clientIds.map(async clientId => openSocket(clientId, await signed("workerd-user")))
        );
        const initialSnapshots = await Promise.all(
            opened.map((connection, index) =>
                subscribeGlobal(connection, clientIds[index] as string, 53, partitions[index] as string)
            )
        );
        initialSnapshots.forEach((message, index) => {
            expect(message.rows).toEqual([]);
            acknowledge((opened[index] as OpenedSocket).socket, message);
        });
        await Promise.all(clientIds.map(clientId => drainUntilSettled(clientId, [53])));
        const registrationMs = performance.now() - registrationStartedAt;

        const writeStartedAt = performance.now();
        const mutationMessages = await Promise.all(
            opened.map((connection, index) =>
                mutateGlobal(connection, `global-bench-mut-${index}`, {
                    id: `global-bench-row-${index}`,
                    namespace: partitions[index] as string,
                    storedNamespace: partitions[index] as string,
                    value: `value-${index}`,
                })
            )
        );
        mutationMessages.forEach((message, index) => {
            expect(message).toMatchObject({
                t: "poke",
                mutResults: [{ mutId: `global-bench-mut-${index}`, ok: true }],
            });
        });
        const replacements = opened.map((connection, index) =>
            nextAfterAcknowledgedSnapshot(
                connection.socket,
                initialSnapshots[index] as Extract<Down, { t: "snapshot" }>
            )
        );
        await Promise.all(clientIds.map(drainGateway));
        const replacementMessages = await Promise.all(replacements);
        replacementMessages.forEach((message, index) => {
            expect(message).toMatchObject({
                t: "snapshot",
                subId: 53,
                rows: [
                    {
                        id: `global-bench-row-${index}`,
                        namespace: partitions[index],
                        value: `value-${index}`,
                    },
                ],
            });
            if (message.t === "snapshot") acknowledge((opened[index] as OpenedSocket).socket, message);
        });
        const writeAndConvergenceMs = performance.now() - writeStartedAt;

        console.log(
            JSON.stringify({
                type: "chardb-workerd-benchmark",
                scenario: "global-axis-concurrent-application-partitions",
                principals: 1,
                partitions: partitions.length,
                registrations: partitions.length,
                registrationMs: Number(registrationMs.toFixed(2)),
                registrationsPerSecond: rate(partitions.length, registrationMs),
                writes: partitions.length,
                writeAndConvergenceMs: Number(writeAndConvergenceMs.toFixed(2)),
                writesPerSecond: rate(partitions.length, writeAndConvergenceMs),
                exactIsolatedSnapshots: replacementMessages.length,
            })
        );

        for (const connection of opened) connection.socket.close();
        await Promise.all(opened.map(connection => connection.closed));
        await Promise.all(clientIds.map(drainGateway));
    });

    test("user-axis fanout stays exact across concurrent principals", async () => {
        if (!userMutationRef || !userQueryRef) throw new Error("user authority refs were not seeded");
        const writeRef = userMutationRef;
        const readRef = userQueryRef;
        const subjects = Array.from(
            { length: USER_AXIS_BENCH_USERS },
            (_, index) => `user-bench-${index.toString().padStart(2, "0")}`
        );
        await Promise.all(subjects.map(createFixtureUser));
        await settleAuthInvalidations();
        const clientIds = subjects.map((_, index) => `user-bench-client-${index.toString().padStart(2, "0")}`);

        const registrationStartedAt = performance.now();
        const opened = await Promise.all(
            subjects.map(async (subject, index) => openSocket(clientIds[index] as string, await signed(subject)))
        );
        try {
            const initialSnapshots = opened.map((connection, index) => {
                const snapshot = nextDown(connection.socket);
                connection.socket.send(
                    encodeWire({
                        t: "sub",
                        subId: SubId(40),
                        ref: readRef,
                        args: { userId: subjects[index] as string },
                    })
                );
                return snapshot;
            });
            await Promise.all(clientIds.map(clientId => currentRegistration(clientId, 40)));
            await Promise.all(clientIds.map(drainGateway));
            const initialMessages = await Promise.all(initialSnapshots);
            initialMessages.forEach((message, index) => {
                expect(message).toMatchObject({ t: "snapshot", subId: 40, rows: [] });
                if (message.t === "snapshot") acknowledge((opened[index] as OpenedSocket).socket, message);
            });
            await Promise.all(clientIds.map(clientId => drainUntilSettled(clientId, [40])));
            const registrationMs = performance.now() - registrationStartedAt;

            const writeStartedAt = performance.now();
            const mutationResults = opened.map((connection, index) => {
                const result = nextMutationResult(connection.socket, `user-bench-mut-${index}`);
                connection.socket.send(
                    encodeWire({
                        t: "mut",
                        mutId: MutId(`user-bench-mut-${index}`),
                        ref: writeRef,
                        args: {
                            id: `user-bench-row-${index}`,
                            userId: subjects[index] as string,
                            value: `value-${index}`,
                        },
                    })
                );
                return result;
            });
            const mutationMessages = await Promise.all(mutationResults);
            mutationMessages.forEach((message, index) => {
                expect(message).toMatchObject({
                    t: "poke",
                    mutResults: [{ mutId: `user-bench-mut-${index}`, ok: true }],
                });
            });
            const replacements = opened.map((connection, index) =>
                nextAfterAcknowledgedSnapshot(
                    connection.socket,
                    initialMessages[index] as Extract<Down, { t: "snapshot" }>
                )
            );
            await Promise.all(clientIds.map(drainGateway));
            const replacementMessages = await Promise.all(replacements);
            replacementMessages.forEach((message, index) => {
                expect(message).toMatchObject({
                    t: "snapshot",
                    subId: 40,
                    rows: [
                        {
                            id: `user-bench-row-${index}`,
                            userId: subjects[index],
                            value: `value-${index}`,
                        },
                    ],
                });
                if (message.t === "snapshot") acknowledge((opened[index] as OpenedSocket).socket, message);
            });
            const writeAndConvergenceMs = performance.now() - writeStartedAt;

            console.log(
                JSON.stringify({
                    type: "chardb-workerd-benchmark",
                    scenario: "user-axis-concurrent-principal-fanout",
                    principals: subjects.length,
                    registrations: subjects.length,
                    registrationMs: Number(registrationMs.toFixed(2)),
                    registrationsPerSecond: rate(subjects.length, registrationMs),
                    writes: subjects.length,
                    writeAndConvergenceMs: Number(writeAndConvergenceMs.toFixed(2)),
                    writesPerSecond: rate(subjects.length, writeAndConvergenceMs),
                    exactIsolatedSnapshots: replacementMessages.length,
                })
            );
        } finally {
            for (const connection of opened) connection.socket.close();
            await Promise.all(opened.map(connection => connection.closed));
            await Promise.all(clientIds.map(drainGateway));
        }
    });

    test("two clients receive a committed replacement while another organization stays isolated", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const clientA1 = "live-a-one-001";
        const clientA2 = "live-a-two-002";
        const clientB = "live-b-one-003";
        const mutatorId = "live-mutator-04";
        const [a1, a2, b, mutator] = await Promise.all([
            openSocket(clientA1, await signed("workerd-user")),
            openSocket(clientA2, await signed("workerd-user")),
            openSocket(clientB, await signed("workerd-user-b")),
            openSocket(mutatorId, await signed("workerd-user")),
        ]);
        expect([a1.welcome, a2.welcome, b.welcome, mutator.welcome].every(message => message.t === "welcome")).toBe(
            true
        );

        const [initialA1, initialA2, initialB] = await Promise.all([
            subscribe(a1, clientA1, 1, ORGANIZATION_A),
            subscribe(a2, clientA2, 1, ORGANIZATION_A),
            subscribe(b, clientB, 1, ORGANIZATION_B),
        ]);
        expect(initialA1.rows).toEqual([]);
        expect(initialA2.rows).toEqual([]);
        expect(initialB.rows).toEqual([]);
        acknowledge(a1.socket, initialA1);
        acknowledge(a2.socket, initialA2);
        acknowledge(b.socket, initialB);

        const beforeA1 = await currentRegistration(clientA1, 1);
        const beforeA2 = await currentRegistration(clientA2, 1);
        const beforeB = await currentRegistration(clientB, 1);
        expect([beforeA1, beforeA2, beforeB].every(row => !row.initialSnapshotPending)).toBe(true);
        expect(beforeA1.deliveredVersion).toBe(beforeA1.dirtyVersion);
        expect(beforeA2.deliveredVersion).toBe(beforeA2.dirtyVersion);
        expect(beforeB.deliveredVersion).toBe(beforeB.dirtyVersion);

        const mutationResult = nextMutationResult(mutator.socket, "live-proof-write");
        mutator.socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("live-proof-write"),
                ref: mutationRef,
                args: {
                    id: "live-proof-row",
                    organizationId: ORGANIZATION_A,
                    body: "live-proof",
                    createdAt: 42,
                },
            })
        );
        await expect(mutationResult).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "live-proof-write", ok: true }],
        });

        const dirtyA1 = await currentRegistration(clientA1, 1);
        const dirtyA2 = await currentRegistration(clientA2, 1);
        const dirtyB = await currentRegistration(clientB, 1);
        expect(dirtyA1.dirtyVersion).toBeGreaterThan(dirtyA1.deliveredVersion);
        expect(dirtyA2.dirtyVersion).toBeGreaterThan(dirtyA2.deliveredVersion);
        expect(dirtyB.dirtyVersion).toBeGreaterThan(dirtyB.deliveredVersion);

        const replacementA1 = nextDown(a1.socket);
        const replacementA2 = nextDown(a2.socket);
        const replacementB = nextDown(b.socket);
        await Promise.all([drainGateway(clientA1), drainGateway(clientA2), drainGateway(clientB)]);
        const [snapshotA1, snapshotA2, snapshotB] = await Promise.all([replacementA1, replacementA2, replacementB]);
        expect(snapshotA1).toMatchObject({
            t: "snapshot",
            subId: 1,
            rows: [
                {
                    id: "live-proof-row",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-user",
                    body: "live-proof",
                    createdAt: 42,
                },
            ],
        });
        expect(snapshotA2).toMatchObject({
            t: "snapshot",
            subId: 1,
            rows: [
                {
                    id: "live-proof-row",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-user",
                    body: "live-proof",
                    createdAt: 42,
                },
            ],
        });
        expect(snapshotB).toMatchObject({ t: "snapshot", subId: 1, rows: [] });
        if (snapshotA1.t !== "snapshot" || snapshotA2.t !== "snapshot" || snapshotB.t !== "snapshot") {
            throw new Error("expected replacement snapshots");
        }
        expect(snapshotA1.cookie).not.toBe(initialA1.cookie);
        expect(snapshotA2.cookie).not.toBe(initialA2.cookie);
        acknowledge(a1.socket, snapshotA1);
        acknowledge(a2.socket, snapshotA2);
        acknowledge(b.socket, snapshotB);

        const deliveredA1 = await currentRegistration(clientA1, 1);
        const deliveredA2 = await currentRegistration(clientA2, 1);
        const isolatedB = await currentRegistration(clientB, 1);
        expect(deliveredA1).toMatchObject({
            deliveredVersion: dirtyA1.dirtyVersion,
            lastCookie: snapshotA1.cookie,
            lastSnapshotCookie: snapshotA1.cookie,
            outboxCookie: null,
            outboxTargetVersion: null,
        });
        expect(deliveredA2).toMatchObject({
            deliveredVersion: dirtyA2.dirtyVersion,
            lastCookie: snapshotA2.cookie,
            lastSnapshotCookie: snapshotA2.cookie,
            outboxCookie: null,
            outboxTargetVersion: null,
        });
        expect(isolatedB).toMatchObject({
            dirtyVersion: dirtyB.dirtyVersion,
            deliveredVersion: dirtyB.dirtyVersion,
            lastSnapshotCookie: snapshotB.cookie,
        });

        const cdb = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        expect(cdb.invalidations).toEqual([]);
        expect(
            cdb.subscriptions
                .filter(subscription => subscription.state === "active")
                .map(subscription => ({
                    clientId: subscription.clientId,
                    organizationId: subscription.organizationId,
                }))
        ).toEqual(
            expect.arrayContaining([
                { clientId: clientA1, organizationId: ORGANIZATION_A },
                { clientId: clientA2, organizationId: ORGANIZATION_A },
                { clientId: clientB, organizationId: ORGANIZATION_B },
            ])
        );

        a1.socket.close();
        await a1.closed;
        const reconnected = await openSocket(clientA1, await signed("workerd-user"));
        const reconnectSnapshot = await subscribe(reconnected, clientA1, 1, ORGANIZATION_A);
        expect(reconnectSnapshot.rows).toEqual([
            expect.objectContaining({ id: "live-proof-row", organizationId: ORGANIZATION_A }),
        ]);
        acknowledge(reconnected.socket, reconnectSnapshot);

        reconnected.socket.close();
        a2.socket.close();
        b.socket.close();
        mutator.socket.close();
        await Promise.all([reconnected.closed, a2.closed, b.closed, mutator.closed]);
        await Promise.all([
            drainGateway(clientA1),
            drainGateway(clientA2),
            drainGateway(clientB),
            drainGateway(mutatorId),
        ]);
        expect((await gatewayState(clientA1)).registrations.every(row => !row.currentHead)).toBe(true);
        expect((await gatewayState(clientA2)).registrations.every(row => !row.currentHead)).toBe(true);
        expect((await gatewayState(clientB)).registrations.every(row => !row.currentHead)).toBe(true);
    });

    test("Gateway and Cdb reconstruction preserves a staged replacement", async () => {
        if (!mf || !mutationRef) throw new Error("live fixture was not initialized");
        const clientId = "live-restart-05";
        const opened = await openSocket(clientId, await signed("workerd-user"));
        expect(opened.welcome.t).toBe("welcome");

        const initial = await subscribe(opened, clientId, 9, ORGANIZATION_A, "restart-proof");
        expect(initial.rows).toEqual([]);
        acknowledge(opened.socket, initial);

        const beforeMutation = await currentRegistration(clientId, 9);
        const mutationResult = nextMutationResult(opened.socket, "live-restart-write");
        opened.socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("live-restart-write"),
                ref: mutationRef,
                args: {
                    id: "live-restart-row",
                    organizationId: ORGANIZATION_A,
                    body: "restart-proof",
                    createdAt: 43,
                },
            })
        );
        await expect(mutationResult).resolves.toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "live-restart-write", ok: true }],
        });

        const dirty = await currentRegistration(clientId, 9);
        expect(dirty.dirtyVersion).toBeGreaterThan(beforeMutation.deliveredVersion);
        await stageGateway(clientId);

        const beforeEviction = await gatewayState(clientId);
        const staged = beforeEviction.registrations.find(row => row.subId === 9 && row.currentHead);
        expect(staged).toMatchObject({
            registrationId: dirty.registrationId,
            lifecycle: "active",
            cdbState: "active",
            dirtyVersion: dirty.dirtyVersion,
            deliveredVersion: beforeMutation.deliveredVersion,
            outboxTargetVersion: dirty.dirtyVersion,
        });
        expect(staged?.outboxCookie).toBeString();
        if (!staged?.outboxCookie) throw new Error("Gateway did not stage the replacement snapshot");
        const stagedCookie = staged.outboxCookie;

        const cdbBeforeEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        const cdbRegistration = cdbBeforeEviction.subscriptions.find(
            subscription => subscription.registrationId === dirty.registrationId
        );
        expect(cdbRegistration).toMatchObject({
            clientId,
            subId: 9,
            state: "active",
            organizationId: ORGANIZATION_A,
        });
        if (!cdbRegistration) throw new Error("Cdb did not retain the live subscription");

        await mf.unsafeEvictDurableObject(WORKER_NAME, "Gateway", {
            name: gatewayBucketName(clientId),
            webSockets: "hibernate",
        });
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Cdb", { name: shardId });

        const afterEviction = await gatewayState(clientId);
        const cdbAfterEviction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
        expect(afterEviction.instanceId).not.toBe(beforeEviction.instanceId);
        expect(afterEviction.registrations).toEqual(beforeEviction.registrations);
        expect(cdbAfterEviction.instanceId).not.toBe(cdbBeforeEviction.instanceId);
        expect(cdbAfterEviction.subscriptions).toEqual(cdbBeforeEviction.subscriptions);
        expect(cdbAfterEviction.invalidations).toEqual(expect.arrayContaining(cdbBeforeEviction.invalidations));
        expect(
            cdbAfterEviction.subscriptions.find(
                subscription => subscription.registrationId === cdbRegistration.registrationId
            )
        ).toEqual(cdbRegistration);

        const replacementMessage = nextDown(opened.socket);
        await drainGateway(clientId);
        const replacement = await replacementMessage;
        expect(replacement).toMatchObject({
            t: "snapshot",
            subId: 9,
            rows: [
                {
                    id: "live-restart-row",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-user",
                    body: "restart-proof",
                    createdAt: 43,
                },
            ],
        });
        if (replacement.t !== "snapshot") throw new Error("expected reconstructed snapshot delivery");
        expect(String(replacement.cookie)).toBe(stagedCookie);
        acknowledge(opened.socket, replacement);

        const delivered = await currentRegistration(clientId, 9);
        expect(delivered).toMatchObject({
            dirtyVersion: dirty.dirtyVersion,
            deliveredVersion: dirty.dirtyVersion,
            lastCookie: replacement.cookie,
            lastSnapshotCookie: replacement.cookie,
            outboxCookie: null,
            outboxTargetVersion: null,
        });
        opened.socket.close();
        await opened.closed;
        await drainGateway(clientId);
        expect((await gatewayState(clientId)).registrations.every(row => !row.currentHead)).toBe(true);
    });

    test("an idle long-lived socket re-reads membership authority after durable auth invalidation", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const writeRef = mutationRef;
        const clientId = "live-revoke-06";
        const writerId = "authority-writer-07";
        const isolatedClientId = "live-revoke-isolated-08";
        const crossShardOrganizationId = "workerd-principal-cross-shard-org";
        const crossShardId = "ShardDO_auth_cross";
        const crossShardClientId = "live-revoke-cross-shard-09";
        await routeFixtureOrganization(crossShardOrganizationId, crossShardId);
        await mutateFixtureOrganization(crossShardOrganizationId, "create");
        await mutateFixtureMembership(crossShardOrganizationId, "workerd-user");
        await drainAuthInvalidations([crossShardId]);
        const opened = await openSocket(clientId, await signed("workerd-user"));
        const writer = await openSocket(writerId, await signed("workerd-writer"));
        const isolated = await openSocket(isolatedClientId, await signed("workerd-user-b"));
        const crossShard = await openSocket(crossShardClientId, await signed("workerd-user"));
        expect(opened.welcome.t).toBe("welcome");
        expect(writer.welcome.t).toBe("welcome");
        expect(isolated.welcome.t).toBe("welcome");
        expect(crossShard.welcome.t).toBe("welcome");

        const isolatedInitial = await subscribe(isolated, isolatedClientId, 13, ORGANIZATION_B, "authority-isolation");
        acknowledge(isolated.socket, isolatedInitial);
        const crossShardInitial = await subscribe(
            crossShard,
            crossShardClientId,
            14,
            crossShardOrganizationId,
            "authority-cross-shard"
        );
        acknowledge(crossShard.socket, crossShardInitial);

        const write = async (mutId: string, id: string): Promise<void> => {
            const result = nextMutationResult(writer.socket, mutId);
            writer.socket.send(
                encodeWire({
                    t: "mut",
                    mutId: MutId(mutId),
                    ref: writeRef,
                    args: {
                        id,
                        organizationId: ORGANIZATION_A,
                        body: "authority-proof",
                        createdAt: 44,
                    },
                })
            );
            expect(await result).toMatchObject({
                t: "poke",
                mutResults: [{ mutId, ok: true }],
            });
        };

        await write("live-authority-seed", "live-authority-row-1");
        const initial = await subscribe(opened, clientId, 12, ORGANIZATION_A, "authority-proof");
        expect(initial.rows).toEqual([
            {
                id: "live-authority-row-1",
                organizationId: ORGANIZATION_A,
                authorId: "workerd-writer",
                body: "authority-proof",
                createdAt: 44,
            },
        ]);
        acknowledge(opened.socket, initial);

        expect((await mutateMembership("upsert", "viewer")).affected).toBe(1);
        const beforeDowngrade = await currentRegistration(clientId, 12);
        await write("live-authority-downgrade", "live-authority-row-2");
        const dirtyAfterDowngrade = await currentRegistration(clientId, 12);
        expect(dirtyAfterDowngrade.dirtyVersion).toBeGreaterThan(beforeDowngrade.deliveredVersion);
        const downgradedMessage = nextDown(opened.socket);
        await drainGateway(clientId);
        const downgraded = await downgradedMessage;
        expect(downgraded).toMatchObject({ t: "snapshot", subId: 12, rows: [] });
        if (downgraded.t !== "snapshot") throw new Error("expected role downgrade replacement snapshot");
        acknowledge(opened.socket, downgraded);

        expect((await mutateMembership("upsert", "member")).affected).toBe(1);
        await write("live-authority-restore-role", "live-authority-row-3");
        const restoredMessage = nextDown(opened.socket);
        await drainGateway(clientId);
        const restored = await restoredMessage;
        expect(restored).toMatchObject({
            t: "snapshot",
            subId: 12,
            rows: [
                {
                    id: "live-authority-row-1",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-writer",
                    body: "authority-proof",
                    createdAt: 44,
                },
                {
                    id: "live-authority-row-2",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-writer",
                    body: "authority-proof",
                    createdAt: 44,
                },
                {
                    id: "live-authority-row-3",
                    organizationId: ORGANIZATION_A,
                    authorId: "workerd-writer",
                    body: "authority-proof",
                    createdAt: 44,
                },
            ],
        });
        if (restored.t !== "snapshot") throw new Error("expected restored-role replacement snapshot");
        acknowledge(opened.socket, restored);

        // Force and fully settle one fresh auth epoch so no prior test's
        // Catalog alarm can hold an object reference during the eviction proof.
        expect((await mutateMembership("upsert", "member")).affected).toBe(1);
        const settledPrimaryPromise = nextDown(opened.socket);
        const settledCrossShardPromise = nextDown(crossShard.socket);
        await settleAuthInvalidations([crossShardId]);
        await Promise.all([drainGateway(clientId), drainGateway(crossShardClientId)]);
        const settledPrimary = await settledPrimaryPromise;
        const settledCrossShard = await settledCrossShardPromise;
        expect(settledPrimary).toMatchObject({ t: "snapshot", subId: 12 });
        expect(settledCrossShard).toMatchObject({ t: "snapshot", subId: 14 });
        if (settledPrimary.t !== "snapshot" || settledCrossShard.t !== "snapshot") {
            throw new Error("expected settled auth replacement snapshots before eviction");
        }
        acknowledge(opened.socket, settledPrimary);
        acknowledge(crossShard.socket, settledCrossShard);

        let authResponsesBlocked = false;
        let membershipDeleted = false;
        try {
            await fixtureFetch("/live-cdb-auth-response-failure", { shardId: crossShardId, enabled: "true" });
            authResponsesBlocked = true;
            expect((await mutateMembership("delete")).affected).toBe(1);
            membershipDeleted = true;
            const revokedMessage = nextDown(opened.socket);
            const isolatedNoChange = expectNoDown(isolated.socket);
            const crossShardRefresh = nextDown(crossShard.socket);
            await drainAuthInvalidations([crossShardId]);
            const failedInvalidation = await fixtureFetch<{
                readonly state: { readonly attempts?: number; readonly [key: string]: unknown } | null;
            }>("/live-principal-invalidation-state", { principalId: "workerd-user" });
            expect(failedInvalidation).toMatchObject({
                state: { scopeId: "workerd-user", cursorShardId: null },
            });
            expect(failedInvalidation.state?.attempts).toBeGreaterThanOrEqual(1);
            await drainGateway(clientId);
            await expect(revokedMessage).resolves.toMatchObject({
                t: "error",
                subId: 12,
                code: "CDB_FORBIDDEN",
                retryable: false,
            });
            expect((await gatewayState(clientId)).registrations.some(row => row.subId === 12 && row.currentHead)).toBe(
                false
            );
            await drainGateway(isolatedClientId);
            await isolatedNoChange;
            expect(
                (await gatewayState(isolatedClientId)).registrations.some(row => row.subId === 13 && row.currentHead)
            ).toBe(true);
            await drainGateway(crossShardClientId);
            await expect(crossShardRefresh).resolves.toMatchObject({ t: "snapshot", subId: 14, rows: [] });
            expect(
                (await gatewayState(crossShardClientId)).registrations.some(row => row.subId === 14 && row.currentHead)
            ).toBe(true);
            await fixtureFetch("/live-cdb-auth-response-failure", { shardId: crossShardId, enabled: "false" });
            authResponsesBlocked = false;
            await fixtureFetch("/live-principal-invalidation-due", { principalId: "workerd-user" });
            await fixtureFetch("/live-catalog-drain", {});
            await fixtureFetch("/live-catalog-drain", {});
            expect(
                await fixtureFetch<{ readonly state: Record<string, unknown> | null }>(
                    "/live-principal-invalidation-state",
                    { principalId: "workerd-user" }
                )
            ).toEqual({ state: null });

            const noReplacement = expectNoDown(opened.socket);
            await write("live-authority-after-revoke", "live-authority-row-4");
            await drainGateway(clientId);
            await noReplacement;

            expect((await mutateMembership("upsert", "member")).affected).toBe(1);
            membershipDeleted = false;
            const fresh = await subscribe(opened, clientId, 12, ORGANIZATION_A, "authority-proof");
            expect(fresh.rows).toHaveLength(4);
            expect(fresh.rows).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({
                        id: "live-authority-row-1",
                        organizationId: ORGANIZATION_A,
                        authorId: "workerd-writer",
                        body: "authority-proof",
                        createdAt: 44,
                    }),
                    expect.objectContaining({
                        id: "live-authority-row-4",
                        organizationId: ORGANIZATION_A,
                        authorId: "workerd-writer",
                        body: "authority-proof",
                        createdAt: 44,
                    }),
                ])
            );
            acknowledge(opened.socket, fresh);
        } finally {
            if (authResponsesBlocked) {
                await fixtureFetch("/live-cdb-auth-response-failure", { shardId: crossShardId, enabled: "false" });
            }
            if (membershipDeleted) {
                await mutateMembership("upsert", "member");
                await settleAuthInvalidations([crossShardId]);
            }
            opened.socket.close();
            writer.socket.close();
            isolated.socket.close();
            crossShard.socket.close();
            await Promise.all([opened.closed, writer.closed, isolated.closed, crossShard.closed]);
            await Promise.all([
                drainGateway(clientId),
                drainGateway(writerId),
                drainGateway(isolatedClientId),
                drainGateway(crossShardClientId),
            ]);
        }
        expect((await gatewayState(clientId)).registrations.every(row => !row.currentHead)).toBe(true);
    });

    test("organization deletion clears its idle client without touching another organization", async () => {
        const deletedOrganizationId = "workerd-idle-delete-org";
        await mutateFixtureOrganization(deletedOrganizationId, "create");
        await mutateFixtureMembership(deletedOrganizationId, "workerd-user");

        const deletedClientId = "idle-org-delete-client";
        const isolatedClientId = "idle-org-delete-isolated";
        const deleted = await openSocket(deletedClientId, await signed("workerd-user"));
        const isolated = await openSocket(isolatedClientId, await signed("workerd-user-b"));
        const deletedInitial = await subscribe(deleted, deletedClientId, 61, deletedOrganizationId, "idle-delete");
        const isolatedInitial = await subscribe(isolated, isolatedClientId, 62, ORGANIZATION_B, "idle-delete-isolated");
        acknowledge(deleted.socket, deletedInitial);
        acknowledge(isolated.socket, isolatedInitial);

        const deletedFailure = nextDown(deleted.socket);
        const isolatedNoChange = expectNoDown(isolated.socket);
        await mutateFixtureMembership(deletedOrganizationId, "workerd-user", "delete");
        await mutateFixtureOrganization(deletedOrganizationId, "delete");
        await drainAuthInvalidations();
        await drainGateway(deletedClientId);
        await expect(deletedFailure).resolves.toMatchObject({
            t: "error",
            subId: 61,
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        expect(
            (await gatewayState(deletedClientId)).registrations.some(row => row.subId === 61 && row.currentHead)
        ).toBe(false);
        await drainGateway(isolatedClientId);
        await isolatedNoChange;
        expect(
            (await gatewayState(isolatedClientId)).registrations.some(row => row.subId === 62 && row.currentHead)
        ).toBe(true);

        deleted.socket.close();
        isolated.socket.close();
        await Promise.all([deleted.closed, isolated.closed]);
        await Promise.all([drainGateway(deletedClientId), drainGateway(isolatedClientId)]);
    });

    test("one configured Gateway enforces the exact 256-registration boundary and readmits after release", async () => {
        if (!mf || !queryRef) throw new Error("live fixture was not initialized");
        const readRef = queryRef;
        const gatewayPrefix = "quota-shared";
        const collocatedClientIds = collocatedGatewayClientIds(gatewayPrefix, 6);
        const expectedClientIds = collocatedClientIds.slice(0, 4);
        const rejectedClientId = collocatedClientIds[4];
        const replacementClientId = collocatedClientIds[5];
        const gatewayClientId = expectedClientIds[0];
        if (!gatewayClientId || !rejectedClientId || !replacementClientId) {
            throw new Error("quota fixture did not produce all collocated client ids");
        }
        expect(new Set(collocatedClientIds.map(gatewayBucketName))).toEqual(
            new Set([gatewayBucketName(gatewayClientId)])
        );
        const clients: Array<{
            readonly clientId: string;
            readonly client: ChardbClient;
            readonly subscriptions: Array<{ unsubscribe: () => void }>;
        }> = [];
        let rejectedSocket: OpenedSocket | undefined;
        let primaryFailure: unknown;
        const cleanupFailures: unknown[] = [];

        const quotaRows = (state: GatewayLiveState) =>
            state.registrations.filter(row => row.currentHead && row.clientId.startsWith(gatewayPrefix));
        const waitForCounts = async (gatewayCount: number, cdbCount: number): Promise<GatewayLiveState> => {
            const deadline = Date.now() + Math.max(SCALE_WAIT_MS, 15_000);
            let lastGatewayCount = -1;
            let lastCdbCount = -1;
            while (Date.now() < deadline) {
                await drainGateway(gatewayClientId);
                const state = await gatewayState(gatewayClientId);
                const gatewayRows = quotaRows(state);
                const cdb = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                const activeCdbRows = cdb.subscriptions.filter(
                    row => row.state === "active" && row.clientId.startsWith(gatewayPrefix)
                );
                lastGatewayCount = gatewayRows.length;
                lastCdbCount = activeCdbRows.length;
                const admitted = gatewayRows.every(row => row.lifecycle === "active" && row.cdbState === "active");
                if (gatewayRows.length === gatewayCount && activeCdbRows.length === cdbCount && admitted) {
                    return state;
                }
                await Bun.sleep(10);
            }
            throw new Error(
                `timed out waiting for Gateway/Cdb quota counts ${gatewayCount}/${cdbCount}; last=${lastGatewayCount}/${lastCdbCount}`
            );
        };

        try {
            for (const clientId of expectedClientIds) {
                const client = await createSdkClient(clientId, "workerd-user");
                const subscriptions: Array<{ unsubscribe: () => void }> = [];
                clients.push({ clientId, client, subscriptions });
                for (let index = 0; index < 64; index++) {
                    subscriptions.push(
                        client.subscribe<ScaleRow>(
                            readRef,
                            {
                                organizationId: ORGANIZATION_A,
                                body: `quota-${clientId}-${index.toString().padStart(2, "0")}`,
                            },
                            () => {}
                        )
                    );
                }
            }

            const full = await waitForCounts(256, 256);
            const fullRows = quotaRows(full);
            expect(fullRows).toHaveLength(256);
            expect(new Set(fullRows.map(row => `${row.clientId}:${row.subId}`)).size).toBe(256);
            for (const clientId of expectedClientIds) {
                expect(fullRows.filter(row => row.clientId === clientId)).toHaveLength(64);
            }

            rejectedSocket = await openSocket(rejectedClientId, await signed("workerd-user"));
            expect(rejectedSocket.welcome).toMatchObject({ t: "welcome" });
            const rejection = nextDown(rejectedSocket.socket, Math.max(SCALE_WAIT_MS, 5_000));
            rejectedSocket.socket.send(
                encodeWire({
                    t: "sub",
                    subId: SubId(1),
                    ref: readRef,
                    args: { organizationId: ORGANIZATION_A, body: "quota-rejected" },
                })
            );
            const rejectionMessage = await rejection;
            expect(rejectionMessage).toMatchObject({
                t: "error",
                subId: 1,
                code: "CDB_RATE_LIMITED",
                retryable: true,
            });
            const afterRejection = await gatewayState(rejectedClientId);
            expect(quotaRows(afterRejection)).toHaveLength(256);
            expect(afterRejection.registrations.some(row => row.clientId === rejectedClientId)).toBe(false);
            expect(
                (await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId })).subscriptions.some(
                    row => row.clientId === rejectedClientId
                )
            ).toBe(false);
            rejectedSocket.socket.close();
            await rejectedSocket.closed;
            rejectedSocket = undefined;

            const releasing = clients[0];
            if (!releasing) throw new Error("missing quota client to release");
            const released = releasing.subscriptions.shift();
            if (!released) throw new Error("missing quota subscription to release");
            released.unsubscribe();
            await waitForCounts(255, 255);

            const replacementClient = await createSdkClient(replacementClientId, "workerd-user");
            const replacementSubscriptions: Array<{ unsubscribe: () => void }> = [];
            clients.push({
                clientId: replacementClientId,
                client: replacementClient,
                subscriptions: replacementSubscriptions,
            });
            replacementSubscriptions.push(
                replacementClient.subscribe<ScaleRow>(
                    readRef,
                    { organizationId: ORGANIZATION_A, body: "quota-replacement" },
                    () => {}
                )
            );
            const refilled = await waitForCounts(256, 256);
            expect(
                quotaRows(refilled).find(row => row.clientId === replacementClientId && row.subId === 1)
            ).toMatchObject({ lifecycle: "active", cdbState: "active", currentHead: true });
        } catch (error) {
            primaryFailure = error;
        } finally {
            if (rejectedSocket) {
                try {
                    rejectedSocket.socket.close();
                    await rejectedSocket.closed;
                } catch (error) {
                    cleanupFailures.push(error);
                }
            }
            for (const entry of clients) {
                try {
                    await cleanupSdkClient(entry.clientId, entry.client, entry.subscriptions);
                } catch (error) {
                    cleanupFailures.push(error);
                }
            }
            try {
                await waitForCounts(0, 0);
            } catch (error) {
                cleanupFailures.push(error);
            }
        }

        if (primaryFailure !== undefined) {
            if (primaryFailure instanceof Error && cleanupFailures.length > 0) {
                Object.defineProperty(primaryFailure, "cause", {
                    configurable: true,
                    value: new AggregateError(cleanupFailures, "quota proof cleanup failed"),
                });
            }
            throw primaryFailure;
        }
        if (cleanupFailures.length > 0) throw new AggregateError(cleanupFailures, "quota proof cleanup failed");
        expect(quotaRows(await waitForCounts(0, 0))).toHaveLength(0);
    }, 30_000);

    test(
        "scaled SDK mutation fanout stays tenant-isolated",
        async () => {
            if (!mutationRef || !queryRef) throw new Error("live fixture refs were not seeded");
            const writeRef = mutationRef;
            const readRef = queryRef;
            const body = "sdk-scale-fanout-v1";
            const tenants = [
                { label: "a", organizationId: ORGANIZATION_A, subject: "workerd-user" },
                { label: "b", organizationId: ORGANIZATION_B, subject: "workerd-user-b" },
            ] as const;
            const clients: Array<{
                readonly clientId: string;
                readonly organizationId: string;
                readonly subject: string;
                readonly slot: number;
                client: ChardbClient;
                observer: QueryObserver;
                readonly subscriptions: Array<{ unsubscribe: () => void }>;
            }> = [];
            const responseLossClientId = "bench-response-loss-0001";
            const responseLossBody = "sdk-scale-response-loss-v1";
            let responseLossClient: MutationResponseLossClient | undefined;
            const responseLossSubscriptions: Array<{ unsubscribe: () => void }> = [];
            const startedAt = performance.now();
            try {
                for (const tenant of tenants) {
                    for (let index = 0; index < SCALE_CLIENTS_PER_TENANT; index++) {
                        const clientId = `bench-${tenant.label}-${index.toString().padStart(4, "0")}`;
                        const client = await createSdkClient(clientId, tenant.subject);
                        const observer = createQueryObserver();
                        const subscription = client.subscribe<ScaleRow>(
                            readRef,
                            { organizationId: tenant.organizationId, body },
                            observer.listener
                        );
                        clients.push({
                            clientId,
                            organizationId: tenant.organizationId,
                            subject: tenant.subject,
                            slot: index,
                            client,
                            observer,
                            subscriptions: [subscription],
                        });
                    }
                }

                await Promise.all(clients.map(entry => drainUntilSettled(entry.clientId, [1])));
                await Promise.all(
                    clients.map(entry =>
                        entry.observer.waitFor(
                            observation => observation.state === "live" && observation.rows.length === 0,
                            `${entry.clientId} initial empty snapshot`
                        )
                    )
                );
                const initialMs = performance.now() - startedAt;
                const cdbBeforeMutations = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });

                const expectedIds = new Map<string, string[]>();
                const jobsByTenant = tenants.map(tenant => {
                    const jobs = Array.from({ length: SCALE_MUTATIONS_PER_TENANT }, (_, index) => ({
                        index,
                        id: `sdk-fanout-${tenant.label}-${index.toString().padStart(5, "0")}`,
                    }));
                    expectedIds.set(
                        tenant.organizationId,
                        jobs.map(job => job.id)
                    );
                    return { tenant, jobs };
                });
                const runMutationSlice = async (start: number, end: number): Promise<number> => {
                    const phaseStartedAt = performance.now();
                    await Promise.all(
                        jobsByTenant.map(async ({ tenant, jobs }) => {
                            const mutator = clients.find(entry => entry.organizationId === tenant.organizationId);
                            if (!mutator) throw new Error(`missing mutator for ${tenant.organizationId}`);
                            await inBatches(jobs.slice(start, end), SCALE_MUTATION_BATCH, async job => {
                                const result = await mutator.client.mutate<{
                                    readonly id: string;
                                    readonly userId: string;
                                    readonly tenantId: string | null;
                                }>(writeRef, {
                                    id: job.id,
                                    organizationId: tenant.organizationId,
                                    body,
                                    createdAt: 10_000 + job.index,
                                });
                                expect(result).toMatchObject({
                                    id: job.id,
                                    userId: tenant.subject,
                                    tenantId: tenant.organizationId,
                                });
                            });
                        })
                    );
                    return performance.now() - phaseStartedAt;
                };

                const firstMutationCount = Math.ceil(SCALE_MUTATIONS_PER_TENANT / 2);
                let mutationMs = await runMutationSlice(0, firstMutationCount);
                const midConvergenceStartedAt = performance.now();
                await Promise.all(clients.map(entry => drainUntilSettled(entry.clientId, [1])));
                await Promise.all(
                    clients.map(entry =>
                        entry.observer.waitFor(
                            observation =>
                                observation.state === "live" && observation.rows.length === firstMutationCount,
                            `${entry.clientId} pre-churn fanout snapshot`
                        )
                    )
                );
                const midConvergenceMs = performance.now() - midConvergenceStartedAt;
                const cdbBeforeChurn = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                expectCdbMutationDelta(cdbBeforeMutations, cdbBeforeChurn, tenants.length * firstMutationCount);
                const fanoutClientIds = new Set(clients.map(entry => entry.clientId));
                const activeBeforeChurn = cdbBeforeChurn.subscriptions.filter(
                    row => row.state === "active" && fanoutClientIds.has(row.clientId)
                );
                expect(activeBeforeChurn).toHaveLength(clients.length);
                const registrationBeforeChurn = new Map(
                    activeBeforeChurn.map(row => [row.clientId, row.registrationId] as const)
                );

                const churnTargets = clients.filter(entry => entry.slot % 2 === 0);
                const churnTargetIds = new Set(churnTargets.map(entry => entry.clientId));
                const churnStartedAt = performance.now();
                await Promise.all(
                    churnTargets.map(async entry => {
                        await cleanupSdkClient(entry.clientId, entry.client, entry.subscriptions);
                        const replacement = await createSdkClient(entry.clientId, entry.subject);
                        const observer = createQueryObserver();
                        const subscription = replacement.subscribe<ScaleRow>(
                            readRef,
                            { organizationId: entry.organizationId, body },
                            observer.listener
                        );
                        entry.client = replacement;
                        entry.observer = observer;
                        entry.subscriptions.splice(0, entry.subscriptions.length, subscription);
                    })
                );
                await Promise.all(churnTargets.map(entry => drainUntilSettled(entry.clientId, [1])));
                await Promise.all(
                    churnTargets.map(entry =>
                        entry.observer.waitFor(
                            observation =>
                                observation.state === "live" && observation.rows.length === firstMutationCount,
                            `${entry.clientId} post-churn fanout snapshot`
                        )
                    )
                );
                const churnMs = performance.now() - churnStartedAt;
                const cdbAfterChurn = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                expect(cdbAfterChurn.domainRows).toBe(cdbBeforeChurn.domainRows);
                expect(cdbAfterChurn.opLogRows).toBe(cdbBeforeChurn.opLogRows);
                expect(cdbAfterChurn.changeSeq).toBe(cdbBeforeChurn.changeSeq);
                const activeAfterChurn = cdbAfterChurn.subscriptions.filter(
                    row => row.state === "active" && fanoutClientIds.has(row.clientId)
                );
                expect(activeAfterChurn).toHaveLength(clients.length);
                for (const row of activeAfterChurn) {
                    const priorRegistrationId = registrationBeforeChurn.get(row.clientId);
                    if (!priorRegistrationId) throw new Error(`missing pre-churn registration for ${row.clientId}`);
                    if (churnTargetIds.has(row.clientId)) {
                        expect(row.registrationId).not.toBe(priorRegistrationId);
                    } else {
                        expect(row.registrationId).toBe(priorRegistrationId);
                    }
                }

                mutationMs += await runMutationSlice(firstMutationCount, SCALE_MUTATIONS_PER_TENANT);

                const convergenceStartedAt = performance.now();
                const settledStates = await Promise.all(clients.map(entry => drainUntilSettled(entry.clientId, [1])));
                const finalObservations = await Promise.all(
                    clients.map(entry =>
                        entry.observer.waitFor(
                            observation =>
                                observation.state === "live" && observation.rows.length === SCALE_MUTATIONS_PER_TENANT,
                            `${entry.clientId} final fanout snapshot`
                        )
                    )
                );
                const convergenceMs = performance.now() - convergenceStartedAt;

                for (let index = 0; index < clients.length; index++) {
                    const entry = clients[index] as (typeof clients)[number];
                    const observation = finalObservations[index] as QueryObservation;
                    const ids = observation.rows.map(row => row.id);
                    const tenantIds = expectedIds.get(entry.organizationId);
                    if (!tenantIds) throw new Error(`missing expected ids for ${entry.organizationId}`);
                    expect(ids).toEqual(tenantIds);
                    expect(new Set(ids).size).toBe(SCALE_MUTATIONS_PER_TENANT);
                    expect(observation.rows.map(row => row.createdAt)).toEqual(
                        Array.from({ length: SCALE_MUTATIONS_PER_TENANT }, (_, rowIndex) => 10_000 + rowIndex)
                    );
                    expect(
                        observation.rows.every(
                            row =>
                                row.organizationId === entry.organizationId &&
                                row.body === body &&
                                row.authorId === entry.subject
                        )
                    ).toBe(true);
                    const registration = settledStates[index]?.registrations.find(
                        row => row.clientId === entry.clientId && row.subId === 1 && row.currentHead
                    );
                    expect(registration).toMatchObject({
                        lifecycle: "active",
                        cdbState: "active",
                        initialSnapshotPending: false,
                        outboxCookie: null,
                        outboxTargetVersion: null,
                    });
                    expect(registration?.dirtyVersion).toBe(registration?.deliveredVersion);
                }
                const mutationCount = tenants.length * SCALE_MUTATIONS_PER_TENANT;
                const cdbAfterFanout = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                expectCdbMutationDelta(cdbBeforeMutations, cdbAfterFanout, mutationCount);
                expect(cdbAfterFanout.invalidations).toEqual([]);

                responseLossClient = await createSdkClientWithMutationResponseLoss(
                    responseLossClientId,
                    "workerd-user"
                );
                const responseLossObserver = createQueryObserver();
                responseLossSubscriptions.push(
                    responseLossClient.client.subscribe<ScaleRow>(
                        readRef,
                        { organizationId: ORGANIZATION_A, body: responseLossBody },
                        responseLossObserver.listener
                    )
                );
                await drainUntilSettled(responseLossClientId, [1]);
                await responseLossObserver.waitFor(
                    observation => observation.state === "live" && observation.rows.length === 0,
                    "response-loss initial snapshot"
                );

                const responseLossCount = Math.min(SCALE_MUTATIONS_PER_TENANT, SCALE_MUTATION_BATCH);
                const responseLossIds: string[] = [];
                const responseLossStartedAt = performance.now();
                let responseLossBefore = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                for (let index = 0; index < responseLossCount; index++) {
                    const id = `sdk-response-loss-${index.toString().padStart(5, "0")}`;
                    responseLossIds.push(id);
                    const loss = responseLossClient.armNextSuccessfulResult();
                    const replayedMutation = responseLossClient.client.mutate<{
                        readonly id: string;
                        readonly userId: string;
                        readonly tenantId: string | null;
                    }>(writeRef, {
                        id,
                        organizationId: ORGANIZATION_A,
                        body: responseLossBody,
                        createdAt: 30_000 + index,
                    });
                    const dropped = await loss.dropped;
                    await loss.replacementHeld;
                    const firstCommit = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                    expectCdbMutationDelta(responseLossBefore, firstCommit, 1);
                    expect(dropped.result).toMatchObject({
                        id,
                        userId: "workerd-user",
                        tenantId: ORGANIZATION_A,
                    });
                    const droppedWire = decodeWire(dropped.raw) as Down;
                    expect(droppedWire).toMatchObject({
                        t: "poke",
                        mutResults: [{ mutId: dropped.mutId, ok: true, result: dropped.result }],
                    });

                    loss.releaseReplacement();
                    const replayedResult = await replayedMutation;
                    expect(JSON.stringify(replayedResult)).toBe(JSON.stringify(dropped.result));
                    expect(responseLossClient.successfulResultCount(dropped.mutId)).toBe(2);
                    const afterReplay = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                    expect(afterReplay.domainRows).toBe(firstCommit.domainRows);
                    expect(afterReplay.opLogRows).toBe(firstCommit.opLogRows);
                    expect(afterReplay.changeSeq).toBe(firstCommit.changeSeq);
                    responseLossBefore = afterReplay;
                }
                const responseLossReplayMs = performance.now() - responseLossStartedAt;
                await drainUntilSettled(responseLossClientId, [1]);
                const responseLossObservation = await responseLossObserver.waitFor(
                    observation => observation.state === "live" && observation.rows.length === responseLossCount,
                    "response-loss exact rows"
                );
                expect(responseLossObservation.rows.map(row => row.id)).toEqual(responseLossIds);
                expect(new Set(responseLossObservation.rows.map(row => row.id)).size).toBe(responseLossCount);
                expect(responseLossObservation.rows.map(row => row.createdAt)).toEqual(
                    Array.from({ length: responseLossCount }, (_, index) => 30_000 + index)
                );
                expect(
                    responseLossObservation.rows.every(
                        row =>
                            row.organizationId === ORGANIZATION_A &&
                            row.body === responseLossBody &&
                            row.authorId === "workerd-user"
                    )
                ).toBe(true);

                const finalCdb = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                const committedMutationCount = mutationCount + responseLossCount;
                expectCdbMutationDelta(cdbBeforeMutations, finalCdb, committedMutationCount);
                expect(finalCdb.invalidations).toEqual([]);
                const finalDeliveryRows =
                    SCALE_MUTATIONS_PER_TENANT > firstMutationCount ? clients.length * SCALE_MUTATIONS_PER_TENANT : 0;
                const logicalRowDeliveries =
                    clients.length * firstMutationCount + churnTargets.length * firstMutationCount + finalDeliveryRows;
                const deliveryMs = midConvergenceMs + churnMs + convergenceMs;
                console.info(
                    JSON.stringify({
                        type: "chardb-workerd-benchmark",
                        scenario: "sdk-two-tenant-mutation-fanout",
                        clients: clients.length,
                        mutations: mutationCount,
                        initialMs: Number(initialMs.toFixed(2)),
                        mutationMs: Number(mutationMs.toFixed(2)),
                        mutationsPerSecond: rate(mutationCount, mutationMs),
                        midConvergenceMs: Number(midConvergenceMs.toFixed(2)),
                        churnMs: Number(churnMs.toFixed(2)),
                        reconnectedClients: churnTargets.length,
                        reconnectedClientsPerSecond: rate(churnTargets.length, churnMs),
                        responseLossMutations: responseLossCount,
                        responseLossReplayMs: Number(responseLossReplayMs.toFixed(2)),
                        responseLossReplaysPerSecond: rate(responseLossCount, responseLossReplayMs),
                        exactReplayResults: responseLossCount,
                        replayDuplicateRows: 0,
                        committedRows: committedMutationCount,
                        opLogEntries: committedMutationCount,
                        changeSeqAdvance: committedMutationCount,
                        convergenceMs: Number(convergenceMs.toFixed(2)),
                        deliveryMs: Number(deliveryMs.toFixed(2)),
                        logicalRowDeliveries,
                        logicalRowDeliveriesPerSecond: rate(logicalRowDeliveries, deliveryMs),
                    })
                );
            } finally {
                const cleanup = clients.map(entry =>
                    cleanupSdkClient(entry.clientId, entry.client, entry.subscriptions)
                );
                if (responseLossClient) {
                    cleanup.push(
                        (async () => {
                            try {
                                await cleanupSdkClient(
                                    responseLossClientId,
                                    responseLossClient.client,
                                    responseLossSubscriptions
                                );
                            } finally {
                                responseLossClient.restoreWebSocket();
                            }
                        })()
                    );
                }
                await Promise.all(cleanup);
            }
        },
        SCALE_TEST_TIMEOUT_MS
    );

    test(
        "scaled SDK selective subscription refresh stays exact",
        async () => {
            if (!mf || !mutationRef || !queryRef) throw new Error("live fixture was not initialized");
            const writeRef = mutationRef;
            const readRef = queryRef;
            const clientId = "bench-select-0001";
            const initialClient = await createSdkClientWithTrackedClose(clientId, "workerd-user");
            let client = initialClient.client;
            let subscriptions: Array<{ unsubscribe: () => void }> = [];
            let observers = Array.from({ length: SCALE_SUBSCRIPTIONS }, () => createQueryObserver());
            const bodies = observers.map((_, index) => `sdk-scale-filter-${index.toString().padStart(3, "0")}`);
            let writeMs = 0;
            let refreshMs = 0;
            let recoveryMs = 0;
            const registrationStartedAt = performance.now();
            try {
                for (let index = 0; index < observers.length; index++) {
                    subscriptions.push(
                        client.subscribe<ScaleRow>(
                            readRef,
                            { organizationId: ORGANIZATION_A, body: bodies[index] as string },
                            (observers[index] as QueryObserver).listener
                        )
                    );
                }
                const subIds = observers.map((_, index) => index + 1);
                await drainUntilSettled(clientId, subIds);
                await Promise.all(
                    observers.map((observer, index) =>
                        observer.waitFor(
                            observation => observation.state === "live" && observation.rows.length === 0,
                            `selective subscription ${index} initial snapshot`
                        )
                    )
                );
                const registrationMs = performance.now() - registrationStartedAt;

                const gatewayBeforeReconstruction = await gatewayState(clientId);
                const cdbBeforeReconstruction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                const activeBeforeReconstruction = cdbBeforeReconstruction.subscriptions.filter(
                    row => row.clientId === clientId && row.state === "active"
                );
                expect(activeBeforeReconstruction).toHaveLength(SCALE_SUBSCRIPTIONS);
                expect(new Set(activeBeforeReconstruction.map(row => `${row.registrationId}:${row.subId}`)).size).toBe(
                    SCALE_SUBSCRIPTIONS
                );
                const priorRegistrationIds = new Set(activeBeforeReconstruction.map(row => row.registrationId));

                const reconstructionStartedAt = performance.now();
                await mf.unsafeEvictDurableObject(WORKER_NAME, "Cdb", { name: shardId });
                const cdbAfterReconstruction = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                expect(cdbAfterReconstruction.instanceId).not.toBe(cdbBeforeReconstruction.instanceId);
                expect(
                    cdbAfterReconstruction.subscriptions.filter(
                        row => row.clientId === clientId && row.state === "active"
                    )
                ).toEqual(activeBeforeReconstruction);
                expect(cdbAfterReconstruction.invalidations).toEqual(cdbBeforeReconstruction.invalidations);
                expect(await gatewayState(clientId)).toEqual(gatewayBeforeReconstruction);

                await cleanupSdkClient(clientId, client, subscriptions);
                await initialClient.socketClosed;
                subscriptions = [];
                client = await createSdkClient(clientId, "workerd-user");
                observers = Array.from({ length: SCALE_SUBSCRIPTIONS }, () => createQueryObserver());
                for (let index = 0; index < observers.length; index++) {
                    subscriptions.push(
                        client.subscribe<ScaleRow>(
                            readRef,
                            { organizationId: ORGANIZATION_A, body: bodies[index] as string },
                            (observers[index] as QueryObserver).listener
                        )
                    );
                }
                await drainUntilSettled(clientId, subIds);
                await Promise.all(
                    observers.map((observer, index) =>
                        observer.waitFor(
                            observation => observation.state === "live" && observation.rows.length === 0,
                            `selective subscription ${index} recovery snapshot`
                        )
                    )
                );
                const gatewayAfterRecovery = await gatewayState(clientId);
                const recoveredHeads = gatewayAfterRecovery.registrations.filter(row => row.currentHead);
                const cdbAfterRecovery = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                const recoveredCdbRegistrations = cdbAfterRecovery.subscriptions.filter(
                    row => row.clientId === clientId && row.state === "active"
                );
                recoveryMs = performance.now() - reconstructionStartedAt;
                expect(recoveredHeads).toHaveLength(SCALE_SUBSCRIPTIONS);
                expect(recoveredHeads.every(row => row.lifecycle === "active" && row.cdbState === "active")).toBe(true);
                expect(recoveredCdbRegistrations).toHaveLength(SCALE_SUBSCRIPTIONS);
                expect(recoveredCdbRegistrations.every(row => !priorRegistrationIds.has(row.registrationId))).toBe(
                    true
                );
                expect(new Set(recoveredCdbRegistrations.map(row => `${row.registrationId}:${row.subId}`)).size).toBe(
                    SCALE_SUBSCRIPTIONS
                );
                expect(recoveredCdbRegistrations.map(row => `${row.registrationId}:${row.subId}`).sort()).toEqual(
                    recoveredHeads.map(row => `${row.registrationId}:${row.subId}`).sort()
                );
                expect(cdbAfterRecovery.invalidations).toEqual([]);
                expect(cdbAfterRecovery.domainRows).toBe(cdbBeforeReconstruction.domainRows);
                expect(cdbAfterRecovery.opLogRows).toBe(cdbBeforeReconstruction.opLogRows);
                expect(cdbAfterRecovery.changeSeq).toBe(cdbBeforeReconstruction.changeSeq);

                for (let round = 0; round < SCALE_REFRESH_ROUNDS; round++) {
                    const jobs = bodies.map((body, index) => ({
                        body,
                        index,
                        id: `sdk-select-${index.toString().padStart(3, "0")}-${round.toString().padStart(3, "0")}`,
                    }));
                    const writesStartedAt = performance.now();
                    await inBatches(jobs, SCALE_MUTATION_BATCH, async job => {
                        const result = await client.mutate<{
                            readonly id: string;
                            readonly userId: string;
                            readonly tenantId: string | null;
                        }>(writeRef, {
                            id: job.id,
                            organizationId: ORGANIZATION_A,
                            body: job.body,
                            createdAt: 20_000 + round * SCALE_SUBSCRIPTIONS + job.index,
                        });
                        expect(result).toMatchObject({
                            id: job.id,
                            userId: "workerd-user",
                            tenantId: ORGANIZATION_A,
                        });
                    });
                    writeMs += performance.now() - writesStartedAt;

                    const refreshStartedAt = performance.now();
                    await drainUntilSettled(clientId, subIds);
                    await Promise.all(
                        observers.map((observer, index) =>
                            observer.waitFor(
                                observation => observation.state === "live" && observation.rows.length === round + 1,
                                `selective subscription ${index} round ${round}`
                            )
                        )
                    );
                    refreshMs += performance.now() - refreshStartedAt;

                    for (let index = 0; index < observers.length; index++) {
                        const observation = (observers[index] as QueryObserver).latest();
                        if (!observation) throw new Error(`missing selective observation ${index}`);
                        expect(observation.rows.map(row => row.id)).toEqual(
                            Array.from(
                                { length: round + 1 },
                                (_, priorRound) =>
                                    `sdk-select-${index.toString().padStart(3, "0")}-${priorRound
                                        .toString()
                                        .padStart(3, "0")}`
                            )
                        );
                        expect(observation.rows.map(row => row.createdAt)).toEqual(
                            Array.from(
                                { length: round + 1 },
                                (_, priorRound) => 20_000 + priorRound * SCALE_SUBSCRIPTIONS + index
                            )
                        );
                        expect(
                            observation.rows.every(
                                row =>
                                    row.organizationId === ORGANIZATION_A &&
                                    row.body === bodies[index] &&
                                    row.authorId === "workerd-user"
                            )
                        ).toBe(true);
                    }
                }
                const writes = SCALE_SUBSCRIPTIONS * SCALE_REFRESH_ROUNDS;
                const materializations = SCALE_SUBSCRIPTIONS * SCALE_REFRESH_ROUNDS;
                const finalCdb = await fixtureFetch<CdbLiveState>("/live-cdb-state", { shardId });
                expectCdbMutationDelta(cdbBeforeReconstruction, finalCdb, writes);
                expect(finalCdb.invalidations).toEqual([]);
                console.info(
                    JSON.stringify({
                        type: "chardb-workerd-benchmark",
                        scenario: "sdk-selective-subscription-refresh",
                        subscriptions: SCALE_SUBSCRIPTIONS,
                        rounds: SCALE_REFRESH_ROUNDS,
                        writes,
                        registrationMs: Number(registrationMs.toFixed(2)),
                        registrationsPerSecond: rate(SCALE_SUBSCRIPTIONS, registrationMs),
                        recoveryMs: Number(recoveryMs.toFixed(2)),
                        recoveredRegistrations: SCALE_SUBSCRIPTIONS,
                        recoveredRegistrationsPerSecond: rate(SCALE_SUBSCRIPTIONS, recoveryMs),
                        committedRows: writes,
                        opLogEntries: writes,
                        changeSeqAdvance: writes,
                        writeMs: Number(writeMs.toFixed(2)),
                        writesPerSecond: rate(writes, writeMs),
                        refreshMs: Number(refreshMs.toFixed(2)),
                        materializations,
                        materializationsPerSecond: rate(materializations, refreshMs),
                    })
                );
            } finally {
                await cleanupSdkClient(clientId, client, subscriptions);
            }
        },
        SCALE_TEST_TIMEOUT_MS
    );
});
