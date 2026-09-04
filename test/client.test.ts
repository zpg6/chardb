/**
 * Behaviour tests for `createChardbClient` against an in-process fake
 * WebSocket. Covers the wire round-trip the React hooks rely on:
 *   - hello → welcome captures the baseCookie,
 *   - subscribe → server poke fires the listener with the new rows,
 *   - mutate → server poke.mutResults resolves the pending promise,
 *   - mutResults with ok=false rejects with a typed CdbError,
 *   - mustRefetch resets a sub's state and re-emits `sub`.
 *
 * The fake WebSocket replays a programmable script of inbound messages on
 * the same tick the client `send`s its outbound, so the test stays in
 * synchronous control of ordering.
 */
import { afterEach, beforeEach, describe, expect, setSystemTime, test } from "bun:test";
import {
    type ChardbClientOptions,
    createChardbClient,
    createDeferredChardbClientController,
} from "../src/client/index.ts";
import { CdbError } from "../src/errors.ts";
import { ChardbRef, ClientId, Cookie, MutId, type RawJson, SubId } from "../src/types.ts";
import { type Down, PROTOCOL_V, type Up, decodeWire, encodeWire } from "../src/wire.ts";

class FakeWS {
    static OPEN = 1 as const;
    static CONNECTING = 0 as const;
    static CLOSING = 2 as const;
    static CLOSED = 3 as const;
    readonly sent: string[] = [];
    onopen: (() => void) | null = null;
    onclose: (() => void) | null = null;
    onmessage: ((ev: { data: unknown }) => void) | null = null;
    onerror: (() => void) | null = null;
    readyState: number = FakeWS.OPEN;
    failNextSend = false;
    closeCalls = 0;
    static instances: FakeWS[] = [];
    static autoOpen = true;
    constructor(public readonly url: string) {
        FakeWS.instances.push(this);
        if (FakeWS.autoOpen) queueMicrotask(() => this.onopen?.());
    }
    send(raw: string): void {
        if (this.failNextSend) {
            this.failNextSend = false;
            throw new Error("forced send failure");
        }
        this.sent.push(raw);
    }
    close(): void {
        this.closeCalls += 1;
        this.readyState = FakeWS.CLOSED;
        queueMicrotask(() => this.onclose?.());
    }
    emit(msg: Down): void {
        this.onmessage?.({ data: encodeWire(msg) });
    }
    emitRaw(data: unknown): void {
        this.onmessage?.({ data });
    }
}

const realWS = globalThis.WebSocket;
beforeEach(() => {
    FakeWS.instances.length = 0;
    FakeWS.autoOpen = true;
    (globalThis as { WebSocket: unknown }).WebSocket = FakeWS;
});

afterEach(() => {
    (globalThis as { WebSocket: unknown }).WebSocket = realWS;
});

function client(overrides: Partial<ChardbClientOptions> = {}) {
    return createChardbClient({
        endpoint: "wss://example.com/ws",
        getJwt: async () => "jwt-stub",
        clientId: "c-test",
        ...overrides,
    });
}

async function flush() {
    await new Promise<void>(r => queueMicrotask(r));
    await new Promise<void>(r => queueMicrotask(r));
}

function fakeWebSocket(index = 0): FakeWS {
    const ws = FakeWS.instances[index];
    if (!ws) throw new Error(`expected fake WebSocket instance ${index}`);
    return ws;
}

async function welcome(ws: FakeWS, cookie = "c-test:0"): Promise<void> {
    ws.emit({ t: "welcome", protocolV: PROTOCOL_V, baseCookie: Cookie(cookie), region: "test" });
    await flush();
}

function captureCdbError(run: () => unknown): CdbError {
    let caught: unknown;
    try {
        run();
    } catch (error) {
        caught = error;
    }
    expect(caught).toBeInstanceOf(CdbError);
    return caught as CdbError;
}

function sentMutations(ws: FakeWS): Extract<Up, { t: "mut" }>[] {
    return ws.sent
        .map(raw => JSON.parse(raw) as Up)
        .filter((message): message is Extract<Up, { t: "mut" }> => message.t === "mut");
}

function sentSubscriptions(ws: FakeWS): Extract<Up, { t: "sub" }>[] {
    return ws.sent
        .map(raw => JSON.parse(raw) as Up)
        .filter((message): message is Extract<Up, { t: "sub" }> => message.t === "sub");
}

const RETRYABLE_SUBSCRIPTION_CODES = [
    "CDB_STALE_EPOCH",
    "CDB_TXN_ABORTED_EVICTION",
    "CDB_RATE_LIMITED",
    "CDB_SHARD_UNAVAILABLE",
    "CDB_CATALOG_UNAVAILABLE",
    "CDB_STREAM_ABORTED",
] as const;

function subscriptionError(
    code: (typeof RETRYABLE_SUBSCRIPTION_CODES)[number],
    subId = SubId(1)
): Extract<Down, { t: "error" }> {
    return {
        t: "error",
        code,
        subId,
        retryable: true,
        correlationId: `corr-${code.toLowerCase()}` as never,
        docs: `https://chardb.dev/errors/${code.toLowerCase()}`,
    };
}

function jwtWithClaims(subject: string, expiresAtSeconds: number): string {
    const payload = btoa(JSON.stringify({ sub: subject, exp: expiresAtSeconds }))
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/, "");
    return `e30.${payload}.signature`;
}

function nestedJson(depth: number): RawJson {
    let value: RawJson = null;
    for (let level = 0; level < depth; level++) value = { value };
    return value;
}

function nestedEmptyJson(depth: number): RawJson {
    let value: RawJson = {};
    for (let level = 1; level < depth; level++) value = { value };
    return value;
}

function stringRowsAtBytes(bytes: number): RawJson[] {
    if (bytes < 4) throw new RangeError("serialized row array must include brackets and string quotes");
    return ["x".repeat(bytes - 4)];
}

function keyedRowsAtBytes(bytes: number, rowKey: string): RawJson[] {
    const empty = [{ __key: rowKey, value: "" }];
    const overhead = JSON.stringify(empty).length;
    if (bytes < overhead) throw new RangeError("serialized keyed row array is too small");
    return [{ __key: rowKey, value: "x".repeat(bytes - overhead) }];
}

function pokeRawAtBytes(bytes: number, multibyte = false): string {
    const envelope: { t: "poke"; cookie: string; patches: [] } = {
        t: "poke",
        cookie: "",
        patches: [],
    };
    const overhead = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    if (bytes < overhead) throw new RangeError("poke envelope is larger than the requested byte length");
    const remaining = bytes - overhead;
    envelope.cookie = multibyte
        ? `${"é".repeat(Math.floor(remaining / 2))}${remaining % 2 === 0 ? "" : "x"}`
        : "x".repeat(remaining);
    const raw = JSON.stringify(envelope);
    if (new TextEncoder().encode(raw).byteLength !== bytes) {
        throw new Error("poke helper produced the wrong UTF-8 byte length");
    }
    return raw;
}

function spyOnClearTimeout(): { readonly calls: unknown[]; restore: () => void } {
    const original = globalThis.clearTimeout;
    const calls: unknown[] = [];
    globalThis.clearTimeout = ((handle: Parameters<typeof clearTimeout>[0]) => {
        calls.push(handle);
        original(handle);
    }) as typeof clearTimeout;
    return {
        calls,
        restore() {
            globalThis.clearTimeout = original;
        },
    };
}

function installManualTimers(): {
    runDelay: (delayMs: number) => void;
    scheduledDelays: () => number[];
    restore: () => void;
} {
    const originalSetTimeout = globalThis.setTimeout;
    const originalClearTimeout = globalThis.clearTimeout;
    let nextId = 1;
    const scheduled = new Map<number, { readonly delayMs: number; readonly run: () => void }>();
    globalThis.setTimeout = ((callback: (...args: unknown[]) => void, delayMs = 0, ...args: unknown[]) => {
        const id = nextId++;
        scheduled.set(id, { delayMs, run: () => callback(...args) });
        return id as unknown as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout;
    globalThis.clearTimeout = ((handle: Parameters<typeof clearTimeout>[0]) => {
        scheduled.delete(handle as unknown as number);
    }) as typeof clearTimeout;
    return {
        runDelay(delayMs) {
            const entry = [...scheduled].find(([, timer]) => timer.delayMs === delayMs);
            if (!entry) throw new Error(`expected a scheduled ${delayMs}ms timer`);
            scheduled.delete(entry[0]);
            entry[1].run();
        },
        scheduledDelays() {
            return [...scheduled.values()].map(timer => timer.delayMs);
        },
        restore() {
            globalThis.setTimeout = originalSetTimeout;
            globalThis.clearTimeout = originalClearTimeout;
        },
    };
}

describe("createChardbClient — wire round-trip", () => {
    test("deferred clients start once after valid subscription or mutation admission", async () => {
        let subscriptionJwtCalls = 0;
        const subscriptionController = createDeferredChardbClientController({
            endpoint: "wss://example.com/ws",
            getJwt: async () => {
                subscriptionJwtCalls += 1;
                return "jwt-stub";
            },
            clientId: "c-deferred-subscription",
        });

        expect(subscriptionJwtCalls).toBe(0);
        expect(FakeWS.instances).toHaveLength(0);
        expect(() => subscriptionController.client.subscribe("invalid-ref", {}, () => {})).toThrow("invalid ChardbRef");
        await expect(subscriptionController.client.mutate("invalid-ref", {})).rejects.toThrow("invalid ChardbRef");
        expect(subscriptionJwtCalls).toBe(0);
        expect(FakeWS.instances).toHaveLength(0);

        subscriptionController.client.subscribe("queries.ts#deferred", {}, () => {});
        subscriptionController.start();
        subscriptionController.start();
        expect(subscriptionJwtCalls).toBe(1);
        await flush();
        expect(FakeWS.instances).toHaveLength(1);
        subscriptionController.client.close();

        let mutationJwtCalls = 0;
        const mutationController = createDeferredChardbClientController({
            endpoint: "wss://example.com/ws",
            getJwt: async () => {
                mutationJwtCalls += 1;
                return "jwt-stub";
            },
            clientId: "c-deferred-mutation",
        });
        const mutationError = mutationController.client.mutate("mutations.ts#deferred", {}).catch(error => error);
        mutationController.start();
        expect(mutationJwtCalls).toBe(1);
        await flush();
        expect(FakeWS.instances).toHaveLength(2);
        mutationController.client.close();
        await expect(mutationError).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
    });

    test("closing a deferred client before start prevents all connection work", async () => {
        let getJwtCalls = 0;
        const controller = createDeferredChardbClientController({
            endpoint: "wss://example.com/ws",
            getJwt: async () => {
                getJwtCalls += 1;
                return "jwt-stub";
            },
            clientId: "c-deferred-closed",
        });

        controller.client.close();
        controller.start();
        expect(() => controller.client.subscribe("queries.ts#closed", {}, () => {})).toThrow(
            "cannot open a subscription after the CharDB client has closed"
        );
        await expect(controller.client.mutate("mutations.ts#closed", {})).rejects.toMatchObject({
            code: "CDB_STREAM_ABORTED",
        });
        await flush();
        expect(getJwtCalls).toBe(0);
        expect(FakeWS.instances).toHaveLength(0);
    });

    test("lets an auth-aware owner hold queued work until it explicitly starts the deferred client", async () => {
        const timers = installManualTimers();
        let getJwtCalls = 0;
        const controller = createDeferredChardbClientController(
            {
                endpoint: "wss://example.com/ws",
                getJwt: async () => {
                    getJwtCalls += 1;
                    return "jwt-stub";
                },
                clientId: "c-deferred-auth",
                mutationTimeoutMs: 1_000,
            },
            { autoStartOnOperation: false }
        );
        const mutation = controller.client.mutate("mutations.ts#held-auth", {}).catch(error => error);
        try {
            controller.client.subscribe("queries.ts#held-auth", {}, () => {});
            await flush();
            expect(getJwtCalls).toBe(0);
            expect(FakeWS.instances).toHaveLength(0);

            controller.start();
            await flush();
            const ws = fakeWebSocket();
            expect(getJwtCalls).toBe(1);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            await welcome(ws);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello", "sub", "mut"]);
        } finally {
            controller.client.close();
            await mutation;
            timers.restore();
        }
    });

    test("rejects mutation timeout values that cannot produce a bounded timer", () => {
        for (const mutationTimeoutMs of [0, -1, 1.5, Number.POSITIVE_INFINITY, 2_147_483_648]) {
            expect(() => client({ mutationTimeoutMs })).toThrow(
                "mutationTimeoutMs must be an integer between 1 and 2147483647"
            );
        }
        expect(FakeWS.instances).toHaveLength(0);
    });

    test("hello is sent on open with clientId and jwt", async () => {
        client();
        await flush();
        const ws = fakeWebSocket();
        expect(ws.sent.length).toBe(1);
        const rawSent = ws.sent[0];
        if (!rawSent) throw new Error("expected the client to send a hello envelope");
        const sent = JSON.parse(rawSent) as Up;
        expect(sent.t).toBe("hello");
        if (sent.t !== "hello") throw new Error("unreachable");
        expect(sent.clientId).toBe(ClientId("c-test"));
        expect(sent.jwt).toBe("jwt-stub");
        expect(sent.protocolV).toBe(PROTOCOL_V);
    });

    test("refreshes an expiring JWT on the existing socket without disturbing a live subscription", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        const initial = jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 120);
        const refreshed = jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 240);
        let getJwtCalls = 0;
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                return getJwtCalls === 1 ? initial : refreshed;
            },
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const states: string[] = [];
            c.subscribe("queries.ts#refresh", {}, (_rows, state) => states.push(state ?? "missing"));
            ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-refresh:1"), rows: [] });
            await flush();

            expect(states).toEqual(["live"]);
            expect(timers.scheduledDelays()).toEqual([60_000]);
            timers.runDelay(60_000);
            await flush();

            expect(getJwtCalls).toBe(2);
            expect(FakeWS.instances).toHaveLength(1);
            expect(ws.sent.map(raw => JSON.parse(raw) as Up)).toContainEqual({ t: "updateAuth", jwt: refreshed });
            expect(states).toEqual(["live"]);
            expect(c.state).toBe("open");
            expect(timers.scheduledDelays()).toEqual([120_000]);

            ws.emit({ t: "mustRefetch", subIds: [], reason: "authChanged" });
            await flush();
            expect(states).toEqual(["live"]);
            expect(timers.scheduledDelays()).toEqual([180_000]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("retries a transient proactive JWT read failure while the current token remains valid", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        const diagnostics: unknown[] = [];
        const refreshed = jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 180);
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                if (getJwtCalls === 2) throw new Error("transient token-store failure");
                return getJwtCalls === 1 ? jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 61) : refreshed;
            },
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const states: string[] = [];
            c.subscribe("queries.ts#refresh-failure", {}, (_rows, state) => states.push(state ?? "missing"));
            ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-refresh-failure:1"), rows: [] });
            await flush();

            timers.runDelay(1_000);
            await flush();

            expect(getJwtCalls).toBe(2);
            expect(c.state).toBe("open");
            expect(ws.readyState).toBe(FakeWS.OPEN);
            expect(states).toEqual(["live"]);
            expect(diagnostics).toEqual([]);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).not.toContain("updateAuth");
            expect(timers.scheduledDelays().sort((left, right) => left - right)).toEqual([1_000, 61_000]);

            timers.runDelay(1_000);
            await flush();

            expect(getJwtCalls).toBe(3);
            expect(ws.sent.map(raw => JSON.parse(raw) as Up)).toContainEqual({ t: "updateAuth", jwt: refreshed });
            expect(c.state).toBe("open");
            expect(states).toEqual(["live"]);
            expect(diagnostics).toEqual([]);
            expect(timers.scheduledDelays()).toEqual([61_000]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("closes at the current JWT expiry when a proactive token read never settles", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        let releaseJwt: (jwt: string) => void = () => {};
        const heldJwt = new Promise<string>(resolve => {
            releaseJwt = resolve;
        });
        const diagnostics: unknown[] = [];
        const c = client({
            getJwt: () => {
                getJwtCalls += 1;
                return getJwtCalls === 1
                    ? Promise.resolve(jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 61))
                    : heldJwt;
            },
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);

            timers.runDelay(1_000);
            await flush();
            expect(getJwtCalls).toBe(2);
            expect(timers.scheduledDelays()).toEqual([61_000]);

            setSystemTime(nowMs + 61_000);
            timers.runDelay(61_000);
            await flush();
            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(diagnostics).toEqual([{ code: "CDB_FORBIDDEN", reason: "auth-refresh-read" }]);
            expect(timers.scheduledDelays()).toEqual([]);

            releaseJwt(jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 180));
            await flush();
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            expect(diagnostics).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("closes at the current JWT expiry when updateAuth is never acknowledged", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        const diagnostics: unknown[] = [];
        const refreshed = jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 180);
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                return getJwtCalls === 1 ? jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 61) : refreshed;
            },
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);

            timers.runDelay(1_000);
            await flush();
            expect(ws.sent.map(raw => JSON.parse(raw) as Up)).toContainEqual({ t: "updateAuth", jwt: refreshed });
            expect(timers.scheduledDelays()).toEqual([61_000]);

            setSystemTime(nowMs + 61_000);
            timers.runDelay(61_000);
            await flush();
            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(diagnostics).toEqual([{ code: "CDB_FORBIDDEN", reason: "auth-refresh-close" }]);
            expect(timers.scheduledDelays()).toEqual([]);

            ws.emit({ t: "mustRefetch", subIds: [], reason: "authChanged" });
            await flush();
            expect(diagnostics).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("closes after proactive JWT reads keep failing through the current token expiry", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        const diagnostics: unknown[] = [];
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                if (getJwtCalls > 1) throw new Error("token store unavailable");
                return jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 61);
            },
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);

            timers.runDelay(1_000);
            await flush();
            expect(timers.scheduledDelays().sort((left, right) => left - right)).toEqual([1_000, 61_000]);

            setSystemTime(nowMs + 61_000);
            timers.runDelay(1_000);
            await flush();

            expect(getJwtCalls).toBe(3);
            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(diagnostics).toEqual([{ code: "CDB_FORBIDDEN", reason: "auth-refresh-read" }]);
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("closes before sending updateAuth when a refreshed JWT changes principal", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        const diagnostics: unknown[] = [];
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                return jwtWithClaims(
                    getJwtCalls === 1 ? "user-1" : "user-2",
                    Math.floor(nowMs / 1_000) + (getJwtCalls === 1 ? 61 : 180)
                );
            },
            onSessionError: diagnostic => {
                diagnostics.push(diagnostic);
                throw new Error("diagnostic listener failure");
            },
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            timers.runDelay(1_000);
            await flush();

            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            expect(FakeWS.instances).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(diagnostics).toEqual([{ code: "CDB_FORBIDDEN", reason: "auth-refresh-principal-changed" }]);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("closes when the Gateway rejects a structurally valid refreshed JWT", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let getJwtCalls = 0;
        const c = client({
            getJwt: async () => {
                getJwtCalls += 1;
                return jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + (getJwtCalls === 1 ? 61 : 180));
            },
        });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            timers.runDelay(1_000);
            await flush();
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello", "updateAuth"]);

            ws.emit({
                t: "error",
                code: "CDB_FORBIDDEN",
                retryable: false,
                correlationId: "corr-refresh" as never,
                docs: "https://chardb.dev/errors/cdb_forbidden",
            });
            await flush();

            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(FakeWS.instances).toHaveLength(1);
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("reconnects when the socket closes while an auth refresh is awaiting verification", async () => {
        const timers = installManualTimers();
        const nowMs = 2_000_000_000_000;
        setSystemTime(nowMs);
        let jwtCalls = 0;
        const c = client({
            getJwt: async () => {
                jwtCalls += 1;
                return jwtWithClaims("user-1", Math.floor(nowMs / 1_000) + 60 + jwtCalls * 120);
            },
        });
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            timers.runDelay(120_000);
            await flush();
            expect(first.sent.map(raw => (JSON.parse(raw) as Up).t)).toContain("updateAuth");

            first.close();
            await flush();
            expect(c.state).toBe("reconnecting");
            expect(timers.scheduledDelays()).toEqual([250]);

            timers.runDelay(250);
            await flush();
            expect(FakeWS.instances).toHaveLength(2);
            expect(c.state).toBe("connecting");
        } finally {
            c.close();
            timers.restore();
            setSystemTime();
        }
    });

    test("reconnects queued work when the hello send throws", async () => {
        const timers = installManualTimers();
        FakeWS.autoOpen = false;
        let c: ReturnType<typeof client> | undefined;
        let mutation: Promise<{ saved: boolean }> | undefined;
        try {
            c = client({ mutationTimeoutMs: 1_000 });
            await flush();
            const first = fakeWebSocket();
            const queuedError = first.onerror;
            const queuedClose = first.onclose;
            c.subscribe("queries.ts#hello-retry", { organizationId: "org-1" }, () => {});
            mutation = c.mutate("mutations.ts#hello-retry", { organizationId: "org-1" });

            first.failNextSend = true;
            expect(() => first.onopen?.()).not.toThrow();
            queuedError?.();
            queuedClose?.();
            await flush();

            expect(c.state).toBe("reconnecting");
            expect(first.sent).toEqual([]);
            expect(first.closeCalls).toBe(1);
            expect(timers.scheduledDelays().sort((left, right) => left - right)).toEqual([250, 1_000]);
            expect(FakeWS.instances).toHaveLength(1);

            timers.runDelay(250);
            await flush();
            const replacement = fakeWebSocket(1);
            replacement.onopen?.();
            expect(replacement.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            await welcome(replacement, "c-hello-retry:1");
            const replacementMessages = replacement.sent.map(raw => JSON.parse(raw) as Up);
            expect(replacementMessages.map(message => message.t)).toEqual(["hello", "sub", "mut"]);
            expect(replacementMessages.filter(message => message.t === "sub")).toHaveLength(1);
            const sentMutation = replacementMessages.find(message => message.t === "mut");
            if (!sentMutation || sentMutation.t !== "mut") throw new Error("expected queued mutation on replacement");
            expect(timers.scheduledDelays()).toEqual([1_000]);

            replacement.emit({
                t: "poke",
                cookie: Cookie("c-hello-retry:2"),
                patches: [],
                mutResults: [
                    {
                        mutId: sentMutation.mutId,
                        ok: true,
                        result: { saved: true },
                        cookie: Cookie("c-hello-retry:2"),
                    },
                ],
            });
            await expect(mutation).resolves.toEqual({ saved: true });
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c?.close();
            await mutation?.catch(() => {});
            FakeWS.autoOpen = true;
            timers.restore();
        }
    });

    test("a mismatched welcome protocol terminates every queued operation once", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        let subscriptionNotifications = 0;
        c.subscribe("queries.ts#listMessages", {}, () => subscriptionNotifications++);
        let rejectionCount = 0;
        const mutationErrors = [c.mutate("src/api.ts#one", {}), c.mutate("src/api.ts#two", {})].map(promise =>
            promise.catch(error => {
                rejectionCount++;
                return error;
            })
        );
        ws.onmessage?.({
            data: JSON.stringify({ t: "welcome", protocolV: 2, baseCookie: "c-1:42", region: "test" }),
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        expect(subscriptionNotifications).toBe(1);
        expect(rejectionCount).toBe(2);
        for (const error of await Promise.all(mutationErrors)) {
            expect(error).toMatchObject({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "server selected an unsupported CharDB protocol version",
            });
        }
        c.close();
        await flush();
        expect(subscriptionNotifications).toBe(1);
        expect(rejectionCount).toBe(2);
    });

    test("queues protected operations until the verified welcome arrives", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        c.subscribe("queries.ts#listMessages", { organizationId: "org-1" }, () => {});
        const mutation = c.mutate("src/api.ts#post", { body: "hi" });
        await flush();
        expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

        await welcome(ws);
        expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello", "sub", "mut"]);
        const rejection = mutation.catch(error => error);
        c.close();
        await expect(rejection).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
    });

    test("getJwt rejection terminates queued work without opening a socket", async () => {
        const diagnostics: unknown[] = [];
        let rejectJwt: ((reason: unknown) => void) | undefined;
        const jwt = new Promise<string>((_resolve, reject) => {
            rejectJwt = reject;
        });
        const c = createChardbClient({
            endpoint: "wss://example.com/ws",
            getJwt: () => jwt,
            clientId: "c-jwt-failure",
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        let subscriptionNotifications = 0;
        c.subscribe("queries.ts#listMessages", {}, () => subscriptionNotifications++);
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => error);
        if (!rejectJwt) throw new Error("expected getJwt to start during client construction");
        rejectJwt(new Error("token endpoint unavailable"));
        await flush();

        expect(c.state).toBe("closed");
        expect(FakeWS.instances).toHaveLength(0);
        expect(subscriptionNotifications).toBe(1);
        expect(diagnostics).toEqual([{ code: "CDB_INVARIANT", reason: "connect" }]);
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_INVARIANT",
            message: "failed to establish CharDB client session",
        });
    });

    test("retries a transient getJwt failure while reconnecting an established session", async () => {
        const timers = installManualTimers();
        let jwtCalls = 0;
        const c = client({
            getJwt: async () => {
                jwtCalls += 1;
                if (jwtCalls === 2) throw new Error("token endpoint unavailable");
                return "jwt-stub";
            },
        });
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);

            first.close();
            await flush();
            timers.runDelay(250);
            await flush();

            expect(c.state).toBe("reconnecting");
            expect(jwtCalls).toBe(2);
            expect(FakeWS.instances).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([500]);

            timers.runDelay(500);
            await flush();
            expect(jwtCalls).toBe(3);
            expect(FakeWS.instances).toHaveLength(2);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("invalid connection setup terminates queued work without retrying", async () => {
        const diagnostics: unknown[] = [];
        const c = createChardbClient({
            endpoint: "not a websocket URL",
            getJwt: async () => "jwt-stub",
            clientId: "c-setup-failure",
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => error);
        await flush();

        expect(c.state).toBe("closed");
        expect(FakeWS.instances).toHaveLength(0);
        expect(diagnostics).toEqual([{ code: "CDB_INVARIANT", reason: "connect" }]);
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_INVARIANT",
            message: "failed to establish CharDB client session",
        });
        await new Promise(resolve => setTimeout(resolve, 300));
        expect(FakeWS.instances).toHaveLength(0);
    });

    test("a malformed pre-welcome message terminates queued work", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        let subscriptionNotifications = 0;
        c.subscribe("queries.ts#listMessages", {}, () => subscriptionNotifications++);
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => error);
        ws.onmessage?.({ data: "{" });
        await flush();

        expect(c.state).toBe("closed");
        expect(subscriptionNotifications).toBe(1);
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_INVARIANT",
            message: "server sent an invalid CharDB handshake message",
        });
    });

    test("rejects data frames before the authenticated welcome", async () => {
        const diagnostics: unknown[] = [];
        const c = client({
            clientId: "c-pre-welcome-data",
            onSessionError: diagnostic => diagnostics.push(diagnostic),
        });
        await flush();
        const ws = fakeWebSocket();
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push(rows));
        const mutation = c.mutate("src/api.ts#post", {});

        ws.emit({
            t: "poke",
            cookie: Cookie("c-pre-welcome-data:1"),
            patches: [{ op: "put", subId: SubId(1), rowKey: "forged", row: { secret: true } }],
        });
        await flush();

        expect(c.state).toBe("closed");
        expect(seen).toEqual([[]]);
        expect(diagnostics).toEqual([{ code: "CDB_INVARIANT", reason: "invalid-handshake-frame" }]);
        await expect(mutation).rejects.toMatchObject({
            code: "CDB_INVARIANT",
            message: "server sent an invalid CharDB handshake message",
        });
    });

    test("a valid client-to-server message received from the server fails the session", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => error);

        ws.emitRaw(encodeWire({ t: "ping" } satisfies Up));
        await flush();

        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_INVARIANT",
            message: "server sent an invalid CharDB handshake message",
        });
    });

    test("a malformed established-session message settles in-flight work instead of escaping the callback", async () => {
        const timeoutSpy = spyOnClearTimeout();
        const c = client({ clientId: "c-malformed-session" });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const seen: RawJson[][] = [];
            c.subscribe("queries.ts#listMessages", {}, rows => seen.push(rows));
            ws.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-malformed:1"),
                rows: [{ secret: "authoritative" }],
            });
            const mutationError = c.mutate("src/api.ts#post", {}).catch(error => error);
            expect(seen.at(-1)).toEqual([{ secret: "authoritative" }]);

            ws.onmessage?.({ data: "{" });
            await flush();

            expect(c.state).toBe("closed");
            expect(seen.at(-1)).toEqual([]);
            expect(seen).toHaveLength(2);
            await expect(mutationError).resolves.toMatchObject({
                code: "CDB_INVARIANT",
                message: "server sent an invalid CharDB session message",
            });
            expect(timeoutSpy.calls).toHaveLength(1);
            expect(ws.readyState).toBe(FakeWS.CLOSED);

            ws.onmessage?.({ data: "{" });
            ws.onclose?.();
            await flush();
            expect(seen).toHaveLength(2);
        } finally {
            c.close();
            timeoutSpy.restore();
        }
    });

    test("rejects a second welcome after the session is open", async () => {
        const c = client({ clientId: "c-duplicate-welcome" });
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws, "c-duplicate-welcome:0");
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push(rows));
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-duplicate-welcome:1"),
            rows: [{ id: "authoritative" }],
        });
        expect(seen).toEqual([[{ id: "authoritative" }]]);

        ws.emit({
            t: "welcome",
            protocolV: PROTOCOL_V,
            baseCookie: Cookie("c-duplicate-welcome:0"),
            region: "test",
        });
        await flush();

        expect(c.state).toBe("closed");
        expect(seen).toEqual([[{ id: "authoritative" }], []]);
        expect(ws.readyState).toBe(FakeWS.CLOSED);
    });

    test("accepts an inbound text envelope at the exact 1 MiB transport limit", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);

        ws.emitRaw(pokeRawAtBytes(1_024 * 1_024));
        await flush();

        expect(c.state).toBe("open");
        expect(ws.readyState).toBe(FakeWS.OPEN);
        c.close();
    });

    test("rejects ASCII, multibyte, and binary inbound messages outside the text transport limit", async () => {
        const cases: readonly unknown[] = [
            pokeRawAtBytes(1_024 * 1_024 + 1),
            pokeRawAtBytes(1_024 * 1_024 + 1, true),
            new ArrayBuffer(8),
        ];

        for (const data of cases) {
            const c = client();
            await flush();
            const ws = FakeWS.instances.at(-1);
            if (!ws) throw new Error("expected a fake WebSocket instance");
            await welcome(ws);
            const mutationError = c.mutate("mutations.ts#pending", {}).catch(error => error);

            ws.emitRaw(data);
            await flush();

            expect(c.state).toBe("closed");
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            await expect(mutationError).resolves.toMatchObject({
                code: "CDB_INVARIANT",
                message: "server sent an invalid CharDB session message",
            });
        }
    });

    test("an oversized inbound frame performs terminal cleanup once and never reconnects", async () => {
        const timers = installManualTimers();
        try {
            const c = client({ clientId: "c-inbound-cleanup" });
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            let subscriptionNotifications = 0;
            c.subscribe("queries.ts#pending", {}, () => subscriptionNotifications++);
            const mutationError = c.mutate("mutations.ts#pending", {}).catch(error => error);
            expect(timers.scheduledDelays()).toEqual([60_000]);

            ws.emitRaw(pokeRawAtBytes(1_024 * 1_024 + 1));
            await flush();

            expect(c.state).toBe("closed");
            expect(subscriptionNotifications).toBe(1);
            await expect(mutationError).resolves.toMatchObject({
                code: "CDB_INVARIANT",
                message: "server sent an invalid CharDB session message",
            });
            expect(timers.scheduledDelays()).toEqual([]);
            expect(ws.readyState).toBe(FakeWS.CLOSED);
            expect(FakeWS.instances).toHaveLength(1);

            ws.emitRaw(new ArrayBuffer(1));
            ws.onclose?.();
            await flush();

            expect(subscriptionNotifications).toBe(1);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(FakeWS.instances).toHaveLength(1);
        } finally {
            timers.restore();
        }
    });

    test("a terminal auth error before welcome closes without entering a reconnect loop", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        let subscriptionNotifications = 0;
        c.subscribe("queries.ts#listMessages", {}, () => subscriptionNotifications++);
        const mutation = c.mutate("src/api.ts#post", {});
        ws.emit({
            t: "error",
            code: "CDB_FORBIDDEN",
            retryable: false,
            correlationId: "corr-auth" as never,
            docs: "https://chardb.dev/errors/cdb_forbidden",
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        expect(subscriptionNotifications).toBe(1);
        await expect(mutation).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        await new Promise(resolve => setTimeout(resolve, 300));
        expect(FakeWS.instances).toHaveLength(1);
    });

    test("a protocol mismatch before welcome terminates queued work", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        const mutation = c.mutate("src/api.ts#post", {});
        ws.emit({ t: "mustRefetch", subIds: [], reason: "protocolMismatch" });
        await flush();
        expect(c.state).toBe("closed");
        await expect(mutation).rejects.toMatchObject({ code: "CDB_UNSUPPORTED_FEATURE" });
    });

    test("backs off repeated pre-welcome closes through the 10 second cap", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            let socket = fakeWebSocket();
            expect(socket.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            for (const delay of [250, 500, 1_000, 2_000, 4_000, 8_000, 10_000, 10_000]) {
                socket.close();
                await flush();
                expect(timers.scheduledDelays()).toEqual([delay]);

                timers.runDelay(delay);
                await flush();
                socket = fakeWebSocket(FakeWS.instances.length - 1);
                expect(socket.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
                expect(c.state).toBe("connecting");
            }
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("a valid welcome resets a backed-off connection to the initial reconnect delay", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            let socket = fakeWebSocket();
            socket.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);

            timers.runDelay(250);
            await flush();
            socket = fakeWebSocket(1);
            socket.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([500]);

            timers.runDelay(500);
            await flush();
            socket = fakeWebSocket(2);
            expect(socket.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            await welcome(socket, "c-test:backoff-reset");
            socket.close();
            await flush();

            expect(timers.scheduledDelays()).toEqual([250]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("subscribe → server poke delivers rows to the listener", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        c.subscribe<{ id: string }>("queries.ts#listMessages", { organizationId: "org-1" }, rows =>
            seen.push([...rows])
        );
        await flush();
        // The client should have sent an Up.sub envelope.
        const subSent = ws.sent.map(r => JSON.parse(r) as Up).find(m => m.t === "sub");
        expect(subSent).toBeDefined();
        if (!subSent || subSent.t !== "sub") throw new Error("unreachable");
        expect(subSent.ref).toBe(ChardbRef("queries.ts#listMessages"));
        expect(subSent.args).toEqual({ organizationId: "org-1" });
        // Server pokes with one row patch.
        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:1"),
            patches: [{ op: "put", subId: subSent.subId, rowKey: "row-1", row: { id: "r-1" } }],
        });
        await flush();
        expect(seen.length).toBe(1);
        expect(seen[0]).toEqual([{ id: "r-1", __key: "row-1" }]);
    });

    test("caps active subscriptions at 64 without sending or consuming an id for rejected work", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        expect(() => c.subscribe("invalid-ref", {}, () => {})).toThrow("invalid ChardbRef");
        const subscriptions = Array.from({ length: 64 }, (_, index) =>
            c.subscribe("queries.ts#bounded", { index }, () => {})
        );
        const admitted = ws.sent.map(raw => JSON.parse(raw) as Up).filter(message => message.t === "sub");
        expect(admitted).toHaveLength(64);
        expect(admitted.at(0)).toMatchObject({ t: "sub", subId: 1 });
        expect(admitted.at(-1)).toMatchObject({ t: "sub", subId: 64 });

        ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-test:1"), rows: [] });
        await flush();
        const sentBeforeRejection = ws.sent.length;
        expect(() => c.subscribe("still-invalid-at-cap", {}, () => {})).toThrow("invalid ChardbRef");
        expect(ws.sent).toHaveLength(sentBeforeRejection);
        expect(
            captureCdbError(() => c.subscribe("queries.ts#invalid-at-cap", nestedJson(100), () => {}))
        ).toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        expect(ws.sent).toHaveLength(sentBeforeRejection);
        const limited = captureCdbError(() => c.subscribe("queries.ts#over-limit", {}, () => {}));
        expect(limited).toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        expect(ws.sent).toHaveLength(sentBeforeRejection);

        subscriptions[0]?.unsubscribe();
        const replacement = c.subscribe("queries.ts#replacement", {}, () => {});
        expect(
            ws.sent
                .map(raw => JSON.parse(raw) as Up)
                .filter(message => message.t === "sub")
                .at(-1)
        ).toMatchObject({
            t: "sub",
            subId: 65,
        });
        replacement.unsubscribe();
        c.close();
        expect(captureCdbError(() => c.subscribe("queries.ts#after-close", {}, () => {}))).toMatchObject({
            code: "CDB_STREAM_ABORTED",
        });
    });

    test("caps subscription argument members, depth, and bytes before id allocation or send", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        let getterRuns = 0;
        const accessorArgs: Record<string, RawJson> = {};
        Object.defineProperty(accessorArgs, "value", {
            enumerable: true,
            get() {
                getterRuns++;
                return null;
            },
        });
        const cyclicArgs: Record<string, RawJson> = {};
        cyclicArgs.self = cyclicArgs;

        expect(() => c.subscribe("invalid-ref", accessorArgs, () => {})).toThrow(TypeError);
        expect(getterRuns).toBe(0);
        for (const args of [
            Array.from({ length: 2_048 }, (_, index) => (index === 0 ? [null, null] : [null])),
            nestedJson(100),
            nestedEmptyJson(100),
            { value: "é".repeat(262_139) },
            accessorArgs,
            cyclicArgs,
        ] as RawJson[]) {
            expect(captureCdbError(() => c.subscribe("queries.ts#invalid-arguments", args, () => {}))).toMatchObject({
                code: "CDB_INVALID_ARGS",
                retryable: false,
            });
        }
        expect(getterRuns).toBe(0);
        expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

        c.subscribe(
            "queries.ts#exact-argument-count",
            Array.from({ length: 2_048 }, () => [null]),
            () => {}
        );
        c.subscribe("queries.ts#exact-argument-depth", nestedJson(99), () => {});
        c.subscribe("queries.ts#exact-empty-argument-depth", nestedEmptyJson(99), () => {});
        c.subscribe("queries.ts#exact-argument-bytes", { value: "é".repeat(262_138) }, () => {});
        expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

        await welcome(ws);
        expect(sentSubscriptions(ws).map(message => message.subId)).toEqual([SubId(1), SubId(2), SubId(3), SubId(4)]);
        c.close();
    });

    test("owns queued subscription and mutation arguments before welcome", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            const subscriptionArgs = Object.create(null) as Record<string, RawJson>;
            subscriptionArgs.value = "subscription-original";
            Object.defineProperty(subscriptionArgs, "__proto__", {
                value: { marker: "subscription-proto" },
                enumerable: true,
                writable: true,
                configurable: true,
            });
            const mutationArgs = Object.create(null) as Record<string, RawJson>;
            mutationArgs.value = "mutation-original";
            Object.defineProperty(mutationArgs, "__proto__", {
                value: { marker: "mutation-proto" },
                enumerable: true,
                writable: true,
                configurable: true,
            });

            c.subscribe("queries.ts#owned-arguments", subscriptionArgs, () => {});
            const mutation = c.mutate("mutations.ts#owned-arguments", mutationArgs).catch(error => error);
            subscriptionArgs.value = "subscription-mutated";
            subscriptionArgs.self = subscriptionArgs;
            mutationArgs.value = "mutation-mutated";
            mutationArgs.self = mutationArgs;

            await welcome(ws);
            const sentSubArgs = sentSubscriptions(ws)[0]?.args as Record<string, RawJson>;
            const sentMutationArgs = sentMutations(ws)[0]?.args as Record<string, RawJson>;
            expect(sentSubArgs.value).toBe("subscription-original");
            expect(Object.getOwnPropertyDescriptor(sentSubArgs, "__proto__")?.value).toEqual({
                marker: "subscription-proto",
            });
            expect(sentMutationArgs.value).toBe("mutation-original");
            expect(Object.getOwnPropertyDescriptor(sentMutationArgs, "__proto__")?.value).toEqual({
                marker: "mutation-proto",
            });

            c.close();
            await expect(mutation).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("clones subscription and mutation arrays without reading a poisoned prototype", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            let getterRuns = 0;
            const poisonedArray = (value: string): RawJson[] => {
                const array: RawJson[] = [value];
                const prototype = Object.create(Array.prototype) as Record<string, unknown>;
                Object.defineProperty(prototype, "map", {
                    get() {
                        getterRuns++;
                        throw new Error("poisoned map getter must not run");
                    },
                });
                Object.setPrototypeOf(array, prototype);
                return array;
            };
            const subscriptionArgs = poisonedArray("subscription-original");
            const mutationArgs = poisonedArray("mutation-original");

            c.subscribe("queries.ts#poisoned-array-prototype", subscriptionArgs, () => {});
            const mutation = c.mutate("mutations.ts#poisoned-array-prototype", mutationArgs).catch(error => error);
            expect(getterRuns).toBe(0);
            await welcome(ws);
            expect(getterRuns).toBe(0);
            expect(sentSubscriptions(ws)[0]?.args).toEqual(["subscription-original"]);
            expect(sentMutations(ws)[0]?.args).toEqual(["mutation-original"]);

            c.close();
            await expect(mutation).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("snapshots proxy arguments without a second enumeration or property read", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            const adversarialArgs = (safe: string) => {
                let ownKeysRuns = 0;
                let getRuns = 0;
                const target: Record<string, RawJson> = { safe, hiddenOnSecondTraversal: "hostile" };
                const args = new Proxy(target, {
                    ownKeys() {
                        ownKeysRuns++;
                        return ownKeysRuns === 1 ? ["safe"] : ["safe", "hiddenOnSecondTraversal"];
                    },
                    getOwnPropertyDescriptor(targetObject, key) {
                        return Reflect.getOwnPropertyDescriptor(targetObject, key);
                    },
                    get() {
                        getRuns++;
                        return "hostile-get-value";
                    },
                });
                return {
                    args: args as RawJson,
                    ownKeysRuns: () => ownKeysRuns,
                    getRuns: () => getRuns,
                };
            };
            const subscription = adversarialArgs("subscription-descriptor-value");
            const mutationArgs = adversarialArgs("mutation-descriptor-value");

            c.subscribe("queries.ts#single-pass-proxy", subscription.args, () => {});
            const mutation = c.mutate("mutations.ts#single-pass-proxy", mutationArgs.args).catch(error => error);
            expect(subscription.ownKeysRuns()).toBe(1);
            expect(mutationArgs.ownKeysRuns()).toBe(1);
            expect(subscription.getRuns()).toBe(0);
            expect(mutationArgs.getRuns()).toBe(0);

            await welcome(ws);
            expect(sentSubscriptions(ws)[0]?.args).toEqual({ safe: "subscription-descriptor-value" });
            expect(sentMutations(ws)[0]?.args).toEqual({ safe: "mutation-descriptor-value" });
            expect(subscription.ownKeysRuns()).toBe(1);
            expect(mutationArgs.ownKeysRuns()).toBe(1);
            expect(subscription.getRuns()).toBe(0);
            expect(mutationArgs.getRuns()).toBe(0);

            c.close();
            await expect(mutation).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("rolls back a subscription whose synchronous send fails and never reconnects it", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            first.failNextSend = true;
            const failedArgs: Record<string, RawJson> = { value: "original" };
            expect(captureCdbError(() => c.subscribe("queries.ts#send-failure", failedArgs, () => {}))).toMatchObject({
                code: "CDB_STREAM_ABORTED",
            });
            failedArgs.self = failedArgs;

            first.close();
            await flush();
            timers.runDelay(250);
            await flush();
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected);
            expect(reconnected.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            c.subscribe("queries.ts#replacement-after-send-failure", {}, () => {});
            expect(
                reconnected.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "sub")
            ).toMatchObject({ t: "sub", subId: 2 });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("closes the session when unsubscribe cannot reach the server", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const first = c.subscribe("queries.ts#first", {}, () => {});
        let remainingNotifications = 0;
        c.subscribe("queries.ts#remaining", {}, () => remainingNotifications++);
        ws.failNextSend = true;

        expect(captureCdbError(() => first.unsubscribe())).toMatchObject({ code: "CDB_STREAM_ABORTED" });
        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        expect(remainingNotifications).toBe(1);
        expect(captureCdbError(() => c.subscribe("queries.ts#after-unsub-failure", {}, () => {}))).toMatchObject({
            code: "CDB_STREAM_ABORTED",
        });
    });

    test("finishes terminal cleanup when a subscription listener throws", async () => {
        const timeoutSpy = spyOnClearTimeout();
        try {
            const c = client({ clientId: "c-listener-cleanup" });
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            c.subscribe("queries.ts#throwing", {}, rows => {
                if (rows.length === 0) throw new Error("listener failed");
            });
            const remainingSeen: RawJson[][] = [];
            c.subscribe("queries.ts#remaining", {}, rows => remainingSeen.push(rows));
            ws.emit({
                t: "snapshot",
                subId: SubId(2),
                cookie: Cookie("c-listener-cleanup:1"),
                rows: [{ secret: "authoritative" }],
            });
            expect(remainingSeen.at(-1)).toEqual([{ secret: "authoritative" }]);
            const mutationError = c.mutate("mutations.ts#pending", {}).catch(error => error);

            expect(() => c.close()).not.toThrow();
            await expect(mutationError).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
            expect(remainingSeen.at(-1)).toEqual([]);
            expect(remainingSeen).toHaveLength(2);
            expect(timeoutSpy.calls).toHaveLength(1);
            expect(ws.readyState).toBe(FakeWS.CLOSED);

            c.close();
            ws.onclose?.();
            expect(remainingSeen).toHaveLength(2);
        } finally {
            timeoutSpy.restore();
        }
    });

    test("reconnect resends the same 64 subscriptions without consuming more capacity", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const subscriptions = Array.from({ length: 64 }, (_, index) =>
                c.subscribe("queries.ts#bounded-reconnect", { index }, () => {})
            );

            first.close();
            await flush();
            timers.runDelay(250);
            await flush();
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected);
            expect(
                reconnected.sent.map(raw => JSON.parse(raw) as Up).filter(message => message.t === "sub")
            ).toHaveLength(64);

            const sentBeforeRejection = reconnected.sent.length;
            expect(captureCdbError(() => c.subscribe("queries.ts#reconnect-over-limit", {}, () => {}))).toMatchObject({
                code: "CDB_RATE_LIMITED",
            });
            expect(reconnected.sent).toHaveLength(sentBeforeRejection);

            subscriptions[0]?.unsubscribe();
            c.subscribe("queries.ts#reconnect-replacement", {}, () => {});
            expect(
                reconnected.sent
                    .map(raw => JSON.parse(raw) as Up)
                    .filter(message => message.t === "sub")
                    .at(-1)
            ).toMatchObject({ t: "sub", subId: 65 });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("snapshot replaces existing rows exactly", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push([...rows]));
        await flush();

        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:1"),
            patches: [{ op: "put", subId: SubId(1), rowKey: "stale", row: { id: "stale" } }],
        });
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:2"),
            rows: [{ id: "fresh-1" }, { id: "fresh-2", nested: { value: true } }],
        });
        await flush();

        expect(seen.at(-1)).toEqual([{ id: "fresh-1" }, { id: "fresh-2", nested: { value: true } }]);
        c.close();
    });

    test("empty snapshot replaces rows and notifies the subscription", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push([...rows]));
        await flush();

        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [],
        });
        await flush();

        expect(seen).toEqual([[]]);
        expect(ws.sent.map(raw => JSON.parse(raw) as Up)).toContainEqual({ t: "ack", cookie: Cookie("c-1:1") });
        c.close();
    });

    test("listener mutation and cycles cannot alter private retained query state", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#isolated-listener-state", {}, rows => {
            seen.push(rows);
            if (seen.length !== 1) return;
            const first = rows[0] as Record<string, RawJson>;
            first.value = "listener-mutated";
            first.cycle = first as RawJson;
            rows.push({ value: "listener-added" });
        });

        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-test:isolated-listener-snapshot"),
            rows: [{ value: "canonical" }],
        });
        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:isolated-listener-poke"),
            patches: [{ op: "del", subId: SubId(1), rowKey: "missing" }],
        });
        await flush();

        expect(c.state).toBe("open");
        expect(seen).toHaveLength(2);
        expect(seen[1]).toEqual([{ value: "canonical" }]);
        c.close();
    });

    test("preserves an own __proto__ data property without prototype mutation", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#proto-data", {}, rows => seen.push(rows));
        const row = JSON.parse('{"__proto__":{"source":"canonical"},"value":"safe"}') as RawJson;
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-test:proto-data"),
            rows: [row],
        });
        await flush();

        const delivered = seen[0]?.[0] as Record<string, RawJson>;
        expect(Object.getPrototypeOf(delivered)).toBe(Object.prototype);
        expect(Object.hasOwn(delivered, "__proto__")).toBeTrue();
        expect(delivered.__proto__).toEqual({ source: "canonical" });
        expect((delivered as { polluted?: unknown }).polluted).toBeUndefined();
        delivered.__proto__ = { listener: "mutated" };
        expect(Object.getPrototypeOf(delivered)).toBe(Object.prototype);

        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:proto-data-poke"),
            patches: [{ op: "del", subId: SubId(1), rowKey: "missing" }],
        });
        await flush();
        const redelivered = seen.at(-1)?.[0] as Record<string, RawJson>;
        expect(Object.hasOwn(redelivered, "__proto__")).toBeTrue();
        expect(redelivered.__proto__).toEqual({ source: "canonical" });
        expect(Object.getPrototypeOf(redelivered)).toBe(Object.prototype);
        c.close();
    });

    test("caps aggregate retained query rows at exactly 8 MiB and releases on unsubscribe", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const subscriptions = Array.from({ length: 16 }, (_, index) =>
            c.subscribe(`queries.ts#aggregate-${index}`, {}, () => {})
        );
        const fullRows = stringRowsAtBytes(512 * 1_024);
        for (let subId = 1; subId <= 16; subId++) {
            ws.emit({
                t: "snapshot",
                subId: SubId(subId),
                cookie: Cookie(`c-test:aggregate-full-${subId}`),
                rows: fullRows,
            });
        }
        await flush();
        expect(c.state).toBe("open");

        expect(captureCdbError(() => c.subscribe("queries.ts#aggregate-over", {}, () => {}))).toMatchObject({
            code: "CDB_RATE_LIMITED",
        });
        expect(c.state).toBe("open");

        subscriptions[0]?.unsubscribe();
        const released = c.subscribe("queries.ts#aggregate-released", {}, () => {});
        ws.emit({
            t: "snapshot",
            subId: SubId(17),
            cookie: Cookie("c-test:aggregate-refilled"),
            rows: fullRows,
        });
        await flush();
        expect(c.state).toBe("open");
        released.unsubscribe();
        c.close();
    });

    test("rejects an aggregate-overflowing multi-sub patch before either subscription commits", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen = new Map<number, RawJson[][]>();
        for (let index = 0; index < 18; index++) {
            const subId = index + 1;
            seen.set(subId, []);
            c.subscribe(`queries.ts#aggregate-atomic-${index}`, {}, rows => seen.get(subId)?.push(rows));
        }
        const fullRows = stringRowsAtBytes(512 * 1_024);
        for (let subId = 1; subId <= 15; subId++) {
            ws.emit({
                t: "snapshot",
                subId: SubId(subId),
                cookie: Cookie(`c-test:aggregate-atomic-full-${subId}`),
                rows: fullRows,
            });
        }
        const targetRowsBytes = 180_000;
        for (let subId = 16; subId <= 17; subId++) {
            ws.emit({
                t: "snapshot",
                subId: SubId(subId),
                cookie: Cookie(`c-test:aggregate-atomic-target-${subId}`),
                rows: keyedRowsAtBytes(targetRowsBytes, `target-${subId}`),
            });
        }
        ws.emit({
            t: "snapshot",
            subId: SubId(18),
            cookie: Cookie("c-test:aggregate-atomic-filler"),
            rows: stringRowsAtBytes(164_288),
        });
        await flush();
        expect(c.state).toBe("open");

        const before16 = seen.get(16)?.at(-1);
        const before17 = seen.get(17)?.at(-1);
        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:aggregate-atomic-over"),
            patches: [
                {
                    op: "put",
                    subId: SubId(16),
                    rowKey: "target-16",
                    row: { value: `${(before16?.[0] as { value: string }).value}x` },
                },
                {
                    op: "put",
                    subId: SubId(17),
                    rowKey: "target-17",
                    row: { value: `${(before17?.[0] as { value: string }).value}x` },
                },
            ],
        });
        await flush();

        expect(c.state).toBe("closed");
        expect(seen.get(16)?.at(-2)).toEqual(before16);
        expect(seen.get(17)?.at(-2)).toEqual(before17);
        expect(seen.get(16)?.at(-1)).toEqual([]);
        expect(seen.get(17)?.at(-1)).toEqual([]);
    });

    test("accepts 4096 snapshot rows and terminates before storing one over", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#bounded-snapshot", {}, rows => seen.push([...rows]));
        const boundaryRows = Array.from({ length: 4_096 }, (_, index) => ({ id: index }));
        ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-test:rows-boundary"), rows: boundaryRows });
        await flush();
        expect(c.state).toBe("open");
        expect(seen.at(-1)).toHaveLength(4_096);

        const pendingMutation = c.mutate("mutations.ts#pending-at-row-overflow", {}).catch(error => error);
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-test:rows-over"),
            rows: [...boundaryRows, { id: 4_096 }],
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        expect(seen.at(-2)).toHaveLength(4_096);
        expect(seen.at(-1)).toEqual([]);
        expect(seen).toHaveLength(2);
        expect(ws.sent.map(raw => JSON.parse(raw) as Up)).not.toContainEqual({
            t: "ack",
            cookie: Cookie("c-test:rows-over"),
        });
        await expect(pendingMutation).resolves.toMatchObject({ code: "CDB_INVARIANT" });
    });

    test("accepts an exact 512 KiB snapshot and terminates on one serialized byte over", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#bounded-snapshot-bytes", {}, rows => seen.push([...rows]));
        const exact = "x".repeat(512 * 1_024 - 4);
        ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-test:bytes-boundary"), rows: [exact] });
        await flush();
        expect(c.state).toBe("open");
        expect(seen.at(-1)).toEqual([exact]);

        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-test:bytes-over"),
            rows: [`${exact}x`],
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(seen.at(-2)).toEqual([exact]);
        expect(seen.at(-1)).toEqual([]);
        expect(seen).toHaveLength(2);
    });

    test("applies patch batches atomically and terminates before a 4097th cached row", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#bounded-patches", {}, rows => seen.push([...rows]));
        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:patch-boundary"),
            patches: Array.from({ length: 4_096 }, (_, index) => ({
                op: "put" as const,
                subId: SubId(1),
                rowKey: `row-${index}`,
                row: { value: index },
            })),
        });
        await flush();
        expect(c.state).toBe("open");
        expect(seen).toHaveLength(1);
        expect(seen[0]).toHaveLength(4_096);

        const pendingMutation = c.mutate("mutations.ts#pending-at-patch-overflow", {}).catch(error => error);
        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:patch-over"),
            patches: [
                { op: "put", subId: SubId(1), rowKey: "row-0", row: { value: "must-not-apply" } },
                { op: "put", subId: SubId(1), rowKey: "row-over", row: { value: "must-not-apply" } },
            ],
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(seen.at(-2)).toHaveLength(4_096);
        expect((seen.at(-2)?.[0] as { value?: unknown }).value).toBe(0);
        expect(seen.at(-2)?.some(row => (row as { __key?: string }).__key === "row-over")).toBe(false);
        expect(seen.at(-1)).toEqual([]);
        expect(seen).toHaveLength(2);
        await expect(pendingMutation).resolves.toMatchObject({ code: "CDB_INVARIANT" });
    });

    test("rejects a whole canonical batch of 4097 repeated same-row updates", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: RawJson[][] = [];
        c.subscribe("queries.ts#bounded-batch", {}, rows => seen.push([...rows]));
        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:batch-count-over"),
            patches: Array.from({ length: 4_097 }, (_, index) => ({
                op: "put" as const,
                subId: SubId(1),
                rowKey: "same-row",
                row: { value: index },
            })),
        });
        await flush();
        expect(c.state).toBe("closed");
        expect(seen.at(-1)).toEqual([]);
    });

    test("validates oversized and malformed patches even when their subscription is unknown", async () => {
        const oversized = client();
        await flush();
        const oversizedSocket = fakeWebSocket();
        await welcome(oversizedSocket);
        oversizedSocket.emit({
            t: "poke",
            cookie: Cookie("c-test:unknown-byte-over"),
            patches: [
                {
                    op: "put",
                    subId: SubId(999),
                    rowKey: "x".repeat(512 * 1_024),
                    row: { value: true },
                },
            ],
        });
        await flush();
        expect(oversized.state).toBe("closed");

        const malformed = client();
        await flush();
        const malformedSocket = fakeWebSocket(1);
        await welcome(malformedSocket);
        malformedSocket.emit({
            t: "poke",
            cookie: Cookie("c-test:unknown-malformed"),
            patches: [{ op: "put", subId: SubId(999), rowKey: "unknown", row: "not-an-object" }],
        });
        await flush();
        expect(malformed.state).toBe("closed");
    });

    test("a throwing listener does not terminate the session or block other subscription notifications", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        c.subscribe("queries.ts#first-patched", {}, rows => {
            if (rows.length > 0) throw new Error("first listener failed");
        });
        const secondSeen: RawJson[][] = [];
        c.subscribe("queries.ts#second-patched", {}, rows => secondSeen.push([...rows]));

        ws.emit({
            t: "poke",
            cookie: Cookie("c-test:multi-sub"),
            patches: [
                { op: "put", subId: SubId(1), rowKey: "first", row: { value: 1 } },
                { op: "put", subId: SubId(2), rowKey: "second", row: { value: 2 } },
            ],
        });
        await flush();
        expect(c.state).toBe("open");
        expect(secondSeen).toEqual([[{ value: 2, __key: "second" }]]);
        c.close();
    });

    test("a duplicate snapshot is re-acknowledged before sizing and does not regress the connection cookie", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push([...rows]));
        await flush();

        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [{ id: "first" }],
        });
        ws.emit({ t: "poke", cookie: Cookie("c-1:2"), patches: [] });
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [{ id: "duplicate-must-not-apply" }],
        });
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: Array.from({ length: 4_097 }, (_, index) => ({ id: `oversized-duplicate-${index}` })),
        });
        await flush();

        expect(seen).toEqual([[{ id: "first" }]]);
        expect(
            ws.sent
                .map(raw => JSON.parse(raw) as Up)
                .filter(message => message.t === "ack" && message.cookie === Cookie("c-1:1"))
        ).toHaveLength(3);
        ws.close();
        await new Promise(resolve => setTimeout(resolve, 350));
        const reconnected = fakeWebSocket(1);
        await flush();
        const hello = reconnected.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
        if (!hello || hello.t !== "hello") throw new Error("expected hello on reconnect");
        expect(hello.resumeFromCookie).toBe(Cookie("c-1:2"));
        c.close();
    });

    test("snapshots for unknown or unsubscribed subscriptions are not acknowledged", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        const subscription = c.subscribe("queries.ts#listMessages", {}, rows => seen.push([...rows]));
        await flush();

        subscription.unsubscribe();
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:98"),
            rows: [{ id: "for-unsubscribed-sub" }],
        });

        ws.emit({
            t: "snapshot",
            subId: SubId(999),
            cookie: Cookie("c-1:99"),
            rows: [{ id: "not-for-this-client" }],
        });
        await flush();

        expect(seen).toEqual([]);
        expect(ws.sent.some(raw => (JSON.parse(raw) as Up).t === "ack")).toBe(false);
        expect(c.state).toBe("open");
        ws.close();
        await new Promise(resolve => setTimeout(resolve, 350));
        const reconnected = fakeWebSocket(1);
        await flush();
        const hello = reconnected.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
        if (!hello || hello.t !== "hello") throw new Error("expected hello on reconnect");
        expect(hello.resumeFromCookie).toBe(Cookie("c-test:0"));
        c.close();
    });

    test("a failed snapshot acknowledgement is retried on duplicate delivery after reconnect", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: unknown[][] = [];
        c.subscribe("queries.ts#listMessages", {}, rows => seen.push([...rows]));
        await flush();

        ws.failNextSend = true;
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [{ id: "applied-before-ack-failure" }],
        });
        await flush();
        expect(c.state).toBe("open");
        expect(seen).toEqual([[{ id: "applied-before-ack-failure" }]]);
        expect(ws.sent.some(raw => (JSON.parse(raw) as Up).t === "ack")).toBe(false);

        ws.close();
        await new Promise(resolve => setTimeout(resolve, 350));
        const reconnected = fakeWebSocket(1);
        await flush();
        await welcome(reconnected, "c-1:1");
        reconnected.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [{ id: "duplicate-must-not-reapply" }],
        });
        await flush();

        expect(c.state).toBe("open");
        expect(seen).toEqual([[{ id: "applied-before-ack-failure" }]]);
        expect(reconnected.sent.map(raw => JSON.parse(raw) as Up)).toContainEqual({
            t: "ack",
            cookie: Cookie("c-1:1"),
        });
        c.close();
    });

    test("malformed snapshot terminates the established session", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        let subscriptionNotifications = 0;
        c.subscribe("queries.ts#listMessages", {}, () => subscriptionNotifications++);
        await flush();

        ws.onmessage?.({ data: JSON.stringify({ t: "snapshot", subId: 1, cookie: "c-1:1" }) });
        await flush();

        expect(c.state).toBe("closed");
        expect(subscriptionNotifications).toBe(1);
    });

    test("rejects an invalid mutation ref before allocating pending capacity", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            await expect(c.mutate("invalid-ref", {})).rejects.toBeInstanceOf(TypeError);
            expect(timers.scheduledDelays()).toHaveLength(0);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            const admitted = c.mutate("mutations.ts#after-invalid-ref", {}).catch(error => error);
            expect(timers.scheduledDelays()).toHaveLength(1);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            c.close();
            await expect(admitted).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("rejects non-JSON mutation arguments without invoking accessors or allocating work", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);

            const sparse = Array<RawJson>(1);
            const extraArray: RawJson[] = [null];
            Object.defineProperty(extraArray, "extra", { value: true, enumerable: true });
            const symbolArray: RawJson[] = [null];
            Object.defineProperty(symbolArray, Symbol("hidden"), { value: true, enumerable: true });
            const symbolObject: Record<string, RawJson> = {};
            Object.defineProperty(symbolObject, Symbol("hidden"), { value: true, enumerable: true });
            const nonEnumerableObject: Record<string, RawJson> = {};
            Object.defineProperty(nonEnumerableObject, "hidden", { value: true, enumerable: false });
            let getterRuns = 0;
            const accessorObject: Record<string, RawJson> = {};
            Object.defineProperty(accessorObject, "value", {
                enumerable: true,
                get() {
                    getterRuns++;
                    return null;
                },
            });
            const accessorArray: RawJson[] = [null];
            Object.defineProperty(accessorArray, "0", {
                enumerable: true,
                get() {
                    getterRuns++;
                    return null;
                },
            });

            await expect(c.mutate("invalid-ref", accessorObject)).rejects.toBeInstanceOf(TypeError);
            for (const args of [
                -0,
                Number.POSITIVE_INFINITY,
                sparse,
                extraArray,
                symbolArray,
                symbolObject,
                nonEnumerableObject,
                accessorObject,
                accessorArray,
            ] as unknown as RawJson[]) {
                await expect(c.mutate("mutations.ts#invalid-json", args)).rejects.toMatchObject({
                    code: "CDB_INVALID_ARGS",
                    retryable: false,
                });
            }
            expect(getterRuns).toBe(0);
            expect(timers.scheduledDelays()).toHaveLength(0);
            expect(sentMutations(ws)).toHaveLength(0);

            const nullPrototypeArgs = Object.create(null) as Record<string, RawJson>;
            nullPrototypeArgs.value = "accepted";
            const admitted = c.mutate("mutations.ts#after-invalid-json", nullPrototypeArgs).catch(error => error);
            expect(timers.scheduledDelays()).toHaveLength(1);
            expect(sentMutations(ws)).toEqual([
                expect.objectContaining({
                    ref: ChardbRef("mutations.ts#after-invalid-json"),
                    args: { value: "accepted" },
                }),
            ]);
            c.close();
            await expect(admitted).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("caps mutation argument members, depth, and bytes before allocation or send", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            const exactCount = c
                .mutate(
                    "mutations.ts#exact-argument-count",
                    Array.from({ length: 2_048 }, () => [null])
                )
                .catch(error => error);
            const exactDepth = c.mutate("mutations.ts#exact-argument-depth", nestedJson(99)).catch(error => error);
            const exactEmptyDepth = c
                .mutate("mutations.ts#exact-empty-argument-depth", nestedEmptyJson(99))
                .catch(error => error);
            const exactBytes = c
                .mutate("mutations.ts#exact-argument-bytes", { value: "é".repeat(262_138) })
                .catch(error => error);
            expect(timers.scheduledDelays()).toHaveLength(4);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            await expect(
                c.mutate(
                    "mutations.ts#too-many-arguments",
                    Array.from({ length: 2_048 }, (_, index) => (index === 0 ? [null, null] : [null]))
                )
            ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
            await expect(c.mutate("mutations.ts#too-deep-arguments", nestedJson(100))).rejects.toMatchObject({
                code: "CDB_INVALID_ARGS",
                retryable: false,
            });
            await expect(c.mutate("mutations.ts#too-deep-empty-arguments", nestedEmptyJson(100))).rejects.toMatchObject(
                { code: "CDB_INVALID_ARGS", retryable: false }
            );
            await expect(
                c.mutate("mutations.ts#too-many-argument-bytes", { value: "é".repeat(262_139) })
            ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
            expect(timers.scheduledDelays()).toHaveLength(4);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            await welcome(ws);
            expect(sentMutations(ws).map(message => message.ref)).toEqual([
                ChardbRef("mutations.ts#exact-argument-count"),
                ChardbRef("mutations.ts#exact-argument-depth"),
                ChardbRef("mutations.ts#exact-empty-argument-depth"),
                ChardbRef("mutations.ts#exact-argument-bytes"),
            ]);
            const exactDepthRaw = ws.sent.find(raw => raw.includes('"ref":"mutations.ts#exact-argument-depth"'));
            if (!exactDepthRaw) throw new Error("expected exact-depth mutation send");
            expect(decodeWire(exactDepthRaw)).toMatchObject({
                t: "mut",
                ref: ChardbRef("mutations.ts#exact-argument-depth"),
            });

            c.close();
            await Promise.all([exactCount, exactDepth, exactEmptyDepth, exactBytes]);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("keeps invalid ref and argument-limit polarity ahead of a full pending queue", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            const pending = Array.from({ length: 32 }, (_, index) =>
                c.mutate("mutations.ts#held-for-argument-order", { index }).catch(error => error)
            );
            expect(timers.scheduledDelays()).toHaveLength(32);

            await expect(c.mutate("invalid-ref", { value: "é".repeat(262_139) })).rejects.toBeInstanceOf(TypeError);
            await expect(
                c.mutate("mutations.ts#oversized-at-cap", { value: "é".repeat(262_139) })
            ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
            await expect(c.mutate("mutations.ts#too-deep-at-cap", nestedJson(100))).rejects.toMatchObject({
                code: "CDB_INVALID_ARGS",
                retryable: false,
            });
            await expect(c.mutate("mutations.ts#valid-at-cap", {})).rejects.toMatchObject({
                code: "CDB_RATE_LIMITED",
                retryable: true,
            });
            expect(timers.scheduledDelays()).toHaveLength(32);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);

            c.close();
            await Promise.all(pending);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("caps 32 mutations queued before welcome and refills after success and typed failure", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            const queued = Array.from({ length: 32 }, (_, index) =>
                c.mutate("mutations.ts#queued", { index }).catch(error => error)
            );
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            expect(timers.scheduledDelays()).toHaveLength(32);

            await expect(c.mutate("invalid-ref-at-cap", {})).rejects.toBeInstanceOf(TypeError);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            expect(timers.scheduledDelays()).toHaveLength(32);
            await expect(c.mutate("mutations.ts#limited", {})).rejects.toMatchObject({
                code: "CDB_RATE_LIMITED",
                retryable: true,
            });
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
            expect(timers.scheduledDelays()).toHaveLength(32);

            await welcome(ws);
            expect(sentMutations(ws)).toHaveLength(32);
            const first = sentMutations(ws)[0];
            if (!first) throw new Error("expected first queued mutation");
            ws.emit({
                t: "poke",
                cookie: Cookie("c-test:mutation-success"),
                patches: [],
                mutResults: [
                    {
                        mutId: first.mutId,
                        ok: true,
                        result: { settled: true },
                        cookie: Cookie("c-test:mutation-success"),
                    },
                ],
            });
            await expect(queued[0]).resolves.toEqual({ settled: true });
            const afterSuccess = c.mutate("mutations.ts#after-success", {}).catch(error => error);
            expect(sentMutations(ws)).toHaveLength(33);
            expect(timers.scheduledDelays()).toHaveLength(32);

            const second = sentMutations(ws)[1];
            if (!second) throw new Error("expected second queued mutation");
            ws.emit({
                t: "poke",
                cookie: Cookie("c-test:mutation-failure"),
                patches: [],
                mutResults: [
                    {
                        mutId: second.mutId,
                        ok: false,
                        error: {
                            code: "CDB_CROSS_PARTITION",
                            retryable: false,
                            docs: "https://chardb.dev/errors/cdb_cross_partition",
                        },
                    },
                ],
            });
            await expect(queued[1]).resolves.toMatchObject({ code: "CDB_CROSS_PARTITION" });
            const afterFailure = c.mutate("mutations.ts#after-failure", {}).catch(error => error);
            expect(sentMutations(ws)).toHaveLength(34);
            expect(timers.scheduledDelays()).toHaveLength(32);

            c.close();
            await Promise.all([...queued, afterSuccess, afterFailure]);
            await expect(c.mutate("mutations.ts#after-close-cap", {})).rejects.toMatchObject({
                code: "CDB_STREAM_ABORTED",
            });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("refills capacity after synchronous send failure and timeout", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 1_000 });
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const pending = Array.from({ length: 31 }, (_, index) =>
                c.mutate("mutations.ts#held", { index }).catch(error => error)
            );
            expect(timers.scheduledDelays()).toHaveLength(31);

            ws.failNextSend = true;
            await expect(c.mutate("mutations.ts#send-failure-cap", {})).rejects.toMatchObject({
                code: "CDB_STREAM_ABORTED",
            });
            expect(timers.scheduledDelays()).toHaveLength(31);
            const admitted = c.mutate("mutations.ts#after-send-failure-cap", {}).catch(error => error);
            expect(timers.scheduledDelays()).toHaveLength(32);
            expect(sentMutations(ws)).toHaveLength(32);
            await expect(c.mutate("mutations.ts#limited-before-timeout", {})).rejects.toMatchObject({
                code: "CDB_RATE_LIMITED",
            });

            timers.runDelay(1_000);
            await expect(pending[0]).resolves.toMatchObject({ code: "CDB_MUTATION_OUTCOME_UNKNOWN" });
            expect(timers.scheduledDelays()).toHaveLength(31);
            const afterTimeout = c.mutate("mutations.ts#after-timeout-cap", {}).catch(error => error);
            expect(timers.scheduledDelays()).toHaveLength(32);
            expect(sentMutations(ws)).toHaveLength(33);

            c.close();
            await Promise.all([...pending, admitted, afterTimeout]);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("reconnect resends the same 32 pending mutations without consuming extra capacity", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 5_000 });
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const pending = Array.from({ length: 32 }, (_, index) =>
                c.mutate("mutations.ts#reconnect-cap", { index }).catch(error => error)
            );
            const original = sentMutations(first);
            expect(original).toHaveLength(32);

            first.close();
            await flush();
            timers.runDelay(250);
            await flush();
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected);
            const resent = sentMutations(reconnected);
            expect(resent).toEqual(original);
            expect(timers.scheduledDelays()).toHaveLength(32);
            await expect(c.mutate("mutations.ts#reconnect-limited", {})).rejects.toMatchObject({
                code: "CDB_RATE_LIMITED",
            });
            expect(sentMutations(reconnected)).toHaveLength(32);

            const settled = resent[0];
            if (!settled) throw new Error("expected resent mutation");
            reconnected.emit({
                t: "poke",
                cookie: Cookie("c-test:reconnect-cap-settle"),
                patches: [],
                mutResults: [
                    {
                        mutId: settled.mutId,
                        ok: true,
                        result: null,
                        cookie: Cookie("c-test:reconnect-cap-settle"),
                    },
                ],
            });
            await expect(pending[0]).resolves.toBeNull();
            const replacement = c.mutate("mutations.ts#reconnect-replacement", {}).catch(error => error);
            expect(sentMutations(reconnected)).toHaveLength(33);
            expect(sentMutations(reconnected).at(-1)?.mutId).not.toBe(settled.mutId);
            expect(timers.scheduledDelays()).toHaveLength(32);

            c.close();
            await Promise.all([...pending, replacement]);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("reconnect resends owned subscription and mutation arguments byte-for-byte", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 5_000 });
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const subscriptionArgs: Record<string, RawJson> = { value: "subscription-original" };
            const mutationArgs: Record<string, RawJson> = { value: "mutation-original" };
            c.subscribe("queries.ts#owned-reconnect", subscriptionArgs, () => {});
            const mutation = c.mutate("mutations.ts#owned-reconnect", mutationArgs).catch(error => error);
            const originalSub = first.sent.find(raw => (JSON.parse(raw) as Up).t === "sub");
            const originalMutation = first.sent.find(raw => (JSON.parse(raw) as Up).t === "mut");
            if (!originalSub || !originalMutation) throw new Error("expected initial owned requests");

            subscriptionArgs.value = "subscription-mutated";
            subscriptionArgs.self = subscriptionArgs;
            mutationArgs.value = "mutation-mutated";
            mutationArgs.self = mutationArgs;
            first.close();
            await flush();
            timers.runDelay(250);
            await flush();
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected);
            expect(reconnected.sent.find(raw => (JSON.parse(raw) as Up).t === "sub")).toBe(originalSub);
            expect(reconnected.sent.find(raw => (JSON.parse(raw) as Up).t === "mut")).toBe(originalMutation);

            c.close();
            await expect(mutation).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
            expect(timers.scheduledDelays()).toEqual([]);
            await flush();
            expect(FakeWS.instances).toHaveLength(2);
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("mutate → server poke.mutResults ok=true resolves the promise with the result", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const promise = c.mutate<{ id: string }>("src/api.ts#post", { body: "hi" });
        await flush();
        const mutSent = ws.sent.map(r => JSON.parse(r) as Up).find(m => m.t === "mut");
        if (!mutSent || mutSent.t !== "mut") throw new Error("expected Up.mut");
        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:2"),
            patches: [],
            mutResults: [{ mutId: mutSent.mutId, ok: true, result: { id: "row-1" }, cookie: Cookie("c-1:2") }],
        });
        const result = await promise;
        expect(result).toEqual({ id: "row-1" });
    });

    test("a successful mutation clears its deadline timer", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const timeoutSpy = spyOnClearTimeout();
        try {
            const mutation = c.mutate("src/api.ts#post", {});
            await flush();
            const sent = ws.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "mut");
            if (!sent || sent.t !== "mut") throw new Error("expected Up.mut");
            ws.emit({
                t: "poke",
                cookie: Cookie("c-1:2"),
                patches: [],
                mutResults: [{ mutId: sent.mutId, ok: true, result: null, cookie: Cookie("c-1:2") }],
            });
            await mutation;
            expect(timeoutSpy.calls).toHaveLength(1);
        } finally {
            timeoutSpy.restore();
            c.close();
        }
    });

    test("mutate → mutResults ok=false rejects with a CdbError carrying the wire code", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const promise = c.mutate("src/api.ts#post", {});
        await flush();
        const mutSent = ws.sent.map(r => JSON.parse(r) as Up).find(m => m.t === "mut");
        if (!mutSent || mutSent.t !== "mut") throw new Error("expected Up.mut");
        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:3"),
            patches: [],
            mutResults: [
                {
                    mutId: mutSent.mutId,
                    ok: false,
                    error: {
                        code: "CDB_CROSS_PARTITION",
                        retryable: false,
                        docs: "https://chardb.dev/errors/cdb_cross_partition",
                    },
                },
            ],
        });
        let captured: CdbError | undefined;
        try {
            await promise;
        } catch (e) {
            if (e instanceof CdbError) captured = e;
        }
        expect(captured).toBeInstanceOf(CdbError);
        expect(captured?.code).toBe("CDB_CROSS_PARTITION");
    });

    test("a terminal server mutation failure clears its deadline timer", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const timeoutSpy = spyOnClearTimeout();
        try {
            const mutation = c.mutate("src/api.ts#post", {});
            await flush();
            const sent = ws.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "mut");
            if (!sent || sent.t !== "mut") throw new Error("expected Up.mut");
            ws.emit({
                t: "poke",
                cookie: Cookie("c-1:3"),
                patches: [],
                mutResults: [
                    {
                        mutId: sent.mutId,
                        ok: false,
                        error: {
                            code: "CDB_CROSS_PARTITION",
                            retryable: false,
                            docs: "https://chardb.dev/errors/cdb_cross_partition",
                        },
                    },
                ],
            });
            await expect(mutation).rejects.toMatchObject({ code: "CDB_CROSS_PARTITION", retryable: false });
            expect(timeoutSpy.calls).toHaveLength(1);
        } finally {
            timeoutSpy.restore();
            c.close();
        }
    });

    test("a synchronous mutation send failure settles once and cannot resend after reconnect", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        const first = fakeWebSocket();
        await welcome(first);
        const timeoutSpy = spyOnClearTimeout();
        try {
            first.failNextSend = true;
            const mutation = c.mutate("src/api.ts#post", {});
            await expect(mutation).rejects.toMatchObject({ code: "CDB_STREAM_ABORTED", retryable: true });
            expect(timeoutSpy.calls).toHaveLength(1);

            first.close();
            await new Promise(resolve => setTimeout(resolve, 350));
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected);
            expect(reconnected.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
        } finally {
            timeoutSpy.restore();
            c.close();
        }
    });

    test("a mutation timeout rejects once as outcome-unknown and ignores a late result", async () => {
        const c = client({ mutationTimeoutMs: 20 });
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        let rejectionCount = 0;
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => {
            rejectionCount++;
            return error;
        });
        await flush();
        const sent = ws.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "mut");
        if (!sent || sent.t !== "mut") throw new Error("expected Up.mut");

        await new Promise(resolve => setTimeout(resolve, 40));
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_MUTATION_OUTCOME_UNKNOWN",
            retryable: false,
            message: `mutation ${sent.mutId} timed out after 20ms`,
        });

        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:4"),
            patches: [],
            mutResults: [{ mutId: sent.mutId, ok: true, result: { late: true }, cookie: Cookie("c-1:4") }],
        });
        await flush();
        expect(rejectionCount).toBe(1);
        expect(c.state).toBe("open");
        c.close();
    });

    test("reconnect resends the same mutId without resetting the original deadline", async () => {
        const timers = installManualTimers();
        const c = client({ mutationTimeoutMs: 500 });
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const mutation = c.mutate("src/api.ts#post", { body: "once" });
            await flush();
            const firstSend = first.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "mut");
            if (!firstSend || firstSend.t !== "mut") throw new Error("expected first Up.mut");

            first.close();
            await flush();
            expect(timers.scheduledDelays().sort((a, b) => a - b)).toEqual([250, 500]);
            timers.runDelay(250);
            await flush();
            const reconnected = fakeWebSocket(1);
            await welcome(reconnected, "c-1:4");
            const retry = reconnected.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "mut");
            if (!retry || retry.t !== "mut") throw new Error("expected retried Up.mut");
            expect(retry).toEqual(firstSend);
            expect(timers.scheduledDelays()).toEqual([500]);

            timers.runDelay(500);
            await expect(mutation).rejects.toMatchObject({
                code: "CDB_MUTATION_OUTCOME_UNKNOWN",
                retryable: false,
                message: `mutation ${retry.mutId} timed out after 500ms`,
            });
        } finally {
            timers.restore();
            c.close();
        }
    });

    test("mustRefetch resets sub state and re-sends an Up.sub envelope", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: Array<{ readonly rows: unknown[]; readonly state: string }> = [];
        c.subscribe<{ id: string }>("queries.ts#listMessages", { organizationId: "org-1" }, (rows, state) =>
            seen.push({ rows: [...rows], state: state ?? "missing" })
        );
        await flush();
        // A patch cannot promote a pending subscription before its first
        // authoritative snapshot.
        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:1"),
            patches: [{ op: "put", subId: SubId(1), rowKey: "r1", row: { id: "r1" } }],
        });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [{ id: "r1", __key: "r1" }], state: "pending" });
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:2"),
            rows: [{ id: "authoritative" }],
        });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [{ id: "authoritative" }], state: "live" });

        // Refetch clears the authoritative rows and re-sends `sub`.
        const sentBefore = ws.sent.length;
        ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "schemaChanged" });
        await flush();
        // Listener saw cleared rows.
        expect(seen.at(-1)).toEqual({ rows: [], state: "refetching" });
        // Client re-sent the `sub` envelope after refetch.
        const newSubs = ws.sent
            .slice(sentBefore)
            .map(r => JSON.parse(r) as Up)
            .filter(m => m.t === "sub");
        expect(newSubs.length).toBe(1);
        const resent = newSubs[0];
        if (!resent || resent.t !== "sub") throw new Error("expected a re-sent query subscription");
        expect(resent.ref).toBe(ChardbRef("queries.ts#listMessages"));
        expect(resent.args).toEqual({ organizationId: "org-1" });

        // A rematerialization may reuse the prior data cookie. The first
        // snapshot after mustRefetch is authoritative despite that equality;
        // once accepted, another copy is a duplicate again.
        const seenBeforeReplacement = seen.length;
        const acknowledgementsBeforeReplacement = ws.sent.filter(raw => (JSON.parse(raw) as Up).t === "ack").length;
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:2"),
            rows: [{ id: "rematerialized" }],
        });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [{ id: "rematerialized" }], state: "live" });
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:2"),
            rows: [{ id: "duplicate-must-not-apply" }],
        });
        await flush();
        expect(seen).toHaveLength(seenBeforeReplacement + 1);
        expect(seen.at(-1)).toEqual({ rows: [{ id: "rematerialized" }], state: "live" });
        expect(ws.sent.filter(raw => (JSON.parse(raw) as Up).t === "ack")).toHaveLength(
            acknowledgementsBeforeReplacement + 2
        );

        // A patch during the refetch window is not a complete materialization
        // and therefore cannot restore live state.
        ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "schemaChanged" });
        await flush();
        ws.emit({
            t: "poke",
            cookie: Cookie("c-1:3"),
            patches: [{ op: "put", subId: SubId(1), rowKey: "partial", row: { id: "partial" } }],
        });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [{ id: "partial", __key: "partial" }], state: "refetching" });

        ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:4"), rows: [] });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [], state: "live" });
    });

    test("shardsChanged coalesces per subscription with capped backoff and snapshot reset", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const states: string[] = [];
            c.subscribe("queries.ts#shardBackoff", { organizationId: "org-1" }, (_rows, state) => {
                states.push(state ?? "missing");
            });
            await flush();
            ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-shards:1"), rows: [{ id: "one" }] });
            await flush();

            const initialSubscriptions = sentSubscriptions(ws).length;
            ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "shardsChanged" });
            ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "shardsChanged" });
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);
            expect(sentSubscriptions(ws)).toHaveLength(initialSubscriptions);
            expect(states.filter(value => value === "refetching")).toHaveLength(1);

            let refetches = 0;
            for (const delay of [100, 200, 400, 800, 1_600, 2_000]) {
                timers.runDelay(delay);
                refetches++;
                expect(sentSubscriptions(ws)).toHaveLength(initialSubscriptions + refetches);
                ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "shardsChanged" });
                await flush();
                expect(timers.scheduledDelays()).toEqual([Math.min(delay * 2, 2_000)]);
            }

            ws.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-shards:2"), rows: [{ id: "two" }] });
            await flush();
            expect(timers.scheduledDelays()).toEqual([]);
            ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "shardsChanged" });
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);

            const beforeImmediate = sentSubscriptions(ws).length;
            ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "schemaChanged" });
            await flush();
            expect(timers.scheduledDelays()).toEqual([]);
            expect(sentSubscriptions(ws)).toHaveLength(beforeImmediate + 1);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("shardsChanged timers cannot resurrect an unsubscribed sub and are cleared on reconnect", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const subscription = c.subscribe("queries.ts#shardCleanup", { organizationId: "org-1" }, () => {});
            await flush();
            first.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "shardsChanged" });
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);
            subscription.unsubscribe();
            expect(timers.scheduledDelays()).toEqual([]);
            expect(sentSubscriptions(first)).toHaveLength(1);

            c.subscribe("queries.ts#shardReconnect", { organizationId: "org-2" }, () => {});
            first.emit({ t: "mustRefetch", subIds: [SubId(2)], reason: "shardsChanged" });
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);
            first.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);
            timers.runDelay(250);
            await flush();
            const second = fakeWebSocket(1);
            await welcome(second, "c-shards:reconnected");
            expect(sentSubscriptions(second)).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("mustRefetch does not resurrect a subscription removed by its refetch listener", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const subscription = c.subscribe("queries.ts#listMessages", { organizationId: "org-1" }, (_rows, state) => {
            if (state === "refetching") subscription.unsubscribe();
        });
        await flush();

        const sentBefore = ws.sent.length;
        ws.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "lagged" });
        await flush();

        expect(
            ws.sent
                .slice(sentBefore)
                .map(raw => JSON.parse(raw) as Up)
                .map(message => message.t)
        ).toEqual(["unsub"]);
        subscription.unsubscribe();
        expect(sentSubscriptions(ws)).toHaveLength(1);
    });

    test("a subscription error clears rows instead of leaving stale live data", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const seen: Array<{ readonly rows: unknown[]; readonly state: string }> = [];
        c.subscribe<{ id: string }>("queries.ts#listMessages", { organizationId: "org-1" }, (rows, state) =>
            seen.push({ rows: [...rows], state: state ?? "missing" })
        );
        await flush();
        ws.emit({
            t: "snapshot",
            subId: SubId(1),
            cookie: Cookie("c-1:1"),
            rows: [{ id: "r1" }],
        });
        await flush();
        expect(seen.at(-1)).toEqual({ rows: [{ id: "r1" }], state: "live" });

        ws.emit({
            t: "error",
            code: "CDB_FORBIDDEN",
            retryable: false,
            correlationId: "corr-revoked" as never,
            docs: "https://chardb.dev/errors/cdb_forbidden",
            subId: SubId(1),
        });
        await flush();

        expect(seen.at(-1)).toEqual({ rows: [], state: "error" });
    });

    for (const code of RETRYABLE_SUBSCRIPTION_CODES) {
        test(`${code} retries the same subscription and accepts a same-cookie replacement snapshot`, async () => {
            const timers = installManualTimers();
            const c = client();
            try {
                await flush();
                const ws = fakeWebSocket();
                await welcome(ws);
                const seen: Array<{ readonly rows: RawJson[]; readonly state: string }> = [];
                c.subscribe("queries.ts#retryable", { organizationId: "org-1" }, (rows, state) => {
                    seen.push({ rows, state: state ?? "missing" });
                });
                const cookie = Cookie(`c-retryable:${code}`);
                ws.emit({ t: "snapshot", subId: SubId(1), cookie, rows: [{ id: "before" }] });
                await flush();
                const initialSubscription = sentSubscriptions(ws)[0];
                if (!initialSubscription) throw new Error("expected initial subscription");

                ws.emit(subscriptionError(code));
                await flush();
                expect(seen.at(-1)).toEqual({ rows: [], state: "refetching" });
                expect(timers.scheduledDelays()).toEqual([100]);
                expect(sentSubscriptions(ws)).toHaveLength(1);

                timers.runDelay(100);
                expect(sentSubscriptions(ws)).toEqual([initialSubscription, initialSubscription]);
                ws.emit({ t: "snapshot", subId: SubId(1), cookie, rows: [{ id: "after" }] });
                await flush();
                expect(seen.at(-1)).toEqual({ rows: [{ id: "after" }], state: "live" });
                expect(timers.scheduledDelays()).toEqual([]);

                ws.emit(subscriptionError(code));
                await flush();
                expect(timers.scheduledDelays()).toEqual([100]);
            } finally {
                c.close();
                timers.restore();
            }
        });
    }

    test("retryable subscription errors coalesce and back off through the two second cap", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const states: string[] = [];
            c.subscribe("queries.ts#retry-backoff", {}, (_rows, state) => states.push(state ?? "missing"));
            ws.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-retry-backoff:1"),
                rows: [{ id: "before" }],
            });
            await flush();

            let sends = sentSubscriptions(ws).length;
            for (const delay of [100, 200, 400, 800, 1_600, 2_000, 2_000]) {
                ws.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
                ws.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
                await flush();
                expect(timers.scheduledDelays()).toEqual([delay]);
                expect(states.filter(value => value === "refetching")).toHaveLength(1);
                timers.runDelay(delay);
                sends += 1;
                expect(sentSubscriptions(ws)).toHaveLength(sends);
            }

            ws.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-retry-backoff:2"),
                rows: [{ id: "after" }],
            });
            await flush();
            ws.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);
            expect(states.filter(value => value === "refetching")).toHaveLength(2);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("a refetch listener can unsubscribe before a retry timer is created", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const subscription = c.subscribe("queries.ts#unsubscribe-in-retry", {}, (_rows, state) => {
                if (state === "refetching") subscription.unsubscribe();
            });
            ws.emit(subscriptionError("CDB_CATALOG_UNAVAILABLE"));
            await flush();

            expect(timers.scheduledDelays()).toEqual([]);
            expect(ws.sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello", "sub", "unsub"]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("unsubscribe clears an already scheduled subscription retry", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const subscription = c.subscribe("queries.ts#unsubscribe-scheduled-retry", {}, () => {});
            ws.emit(subscriptionError("CDB_RATE_LIMITED"));
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);

            subscription.unsubscribe();
            expect(timers.scheduledDelays()).toEqual([]);
            expect(sentSubscriptions(ws)).toHaveLength(1);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("closing from a retry listener cannot schedule or send a subscription retry", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const states: string[] = [];
            c.subscribe("queries.ts#close-in-retry", {}, (_rows, state) => {
                states.push(state ?? "missing");
                if (state === "refetching") c.close();
            });
            ws.emit(subscriptionError("CDB_TXN_ABORTED_EVICTION"));
            await flush();

            expect(states).toEqual(["refetching", "closed"]);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(sentSubscriptions(ws)).toHaveLength(1);
            expect(c.state).toBe("closed");
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("socket reconnect replaces a pending subscription retry with one welcome resend", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            c.subscribe("queries.ts#retry-reconnect", {}, () => {});
            first.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
            await flush();
            expect(timers.scheduledDelays()).toEqual([100]);
            const lateMessage = first.onmessage;

            first.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);
            timers.runDelay(250);
            await flush();
            const second = fakeWebSocket(1);
            await welcome(second, "c-retry-reconnect:1");
            expect(sentSubscriptions(second)).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);

            lateMessage?.({ data: encodeWire(subscriptionError("CDB_SHARD_UNAVAILABLE")) });
            await flush();
            expect(sentSubscriptions(second)).toHaveLength(1);
            expect(timers.scheduledDelays()).toEqual([]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("a terminal subscription error is absorbing across later frames and reconnect", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const seen: Array<{ readonly rows: RawJson[]; readonly state: string }> = [];
            c.subscribe("queries.ts#terminal-error", {}, (rows, state) => {
                seen.push({ rows, state: state ?? "missing" });
            });
            first.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-terminal:1"),
                rows: [{ id: "before" }],
            });
            first.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
            await flush();
            expect(seen.at(-1)).toEqual({ rows: [], state: "refetching" });
            expect(timers.scheduledDelays()).toEqual([100]);
            first.emit({
                t: "error",
                code: "CDB_FORBIDDEN",
                retryable: false,
                correlationId: "corr-terminal" as never,
                docs: "https://chardb.dev/errors/cdb_forbidden",
                subId: SubId(1),
            });
            await flush();
            expect(seen.at(-1)).toEqual({ rows: [], state: "error" });
            expect(timers.scheduledDelays()).toEqual([]);
            const observations = seen.length;
            const sentBefore = first.sent.length;

            first.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-terminal:2"),
                rows: [{ id: "must-not-return" }],
            });
            first.emit({
                t: "poke",
                cookie: Cookie("c-terminal:3"),
                patches: [{ op: "put", subId: SubId(1), rowKey: "late", row: { id: "must-not-return" } }],
            });
            first.emit({ t: "mustRefetch", subIds: [SubId(1)], reason: "schemaChanged" });
            first.emit(subscriptionError("CDB_SHARD_UNAVAILABLE"));
            await flush();
            expect(seen).toHaveLength(observations);
            expect(first.sent).toHaveLength(sentBefore);
            expect(timers.scheduledDelays()).toEqual([]);

            first.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);
            timers.runDelay(250);
            await flush();
            const second = fakeWebSocket(1);
            await welcome(second, "c-terminal:reconnect");
            expect(sentSubscriptions(second)).toEqual([]);
            expect(seen).toHaveLength(observations);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("a retryable session error reconnects before welcome instead of stalling", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            first.emit({
                t: "error",
                code: "CDB_RATE_LIMITED",
                retryable: true,
                correlationId: "corr-session-retry" as never,
                docs: "https://chardb.dev/errors/cdb_rate_limited",
            });
            await flush();
            expect(c.state).toBe("reconnecting");
            expect(first.readyState).toBe(FakeWS.CLOSED);
            expect(timers.scheduledDelays()).toEqual([250]);

            timers.runDelay(250);
            await flush();
            expect(FakeWS.instances).toHaveLength(2);
            expect(fakeWebSocket(1).sent.map(raw => (JSON.parse(raw) as Up).t)).toEqual(["hello"]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("an open retryable session error uses reconnect and retained-state resume", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const first = fakeWebSocket();
            await welcome(first, "c-session-retry:1");
            const seen: Array<{ readonly rows: RawJson[]; readonly state: string }> = [];
            c.subscribe("queries.ts#session-retry", {}, (rows, state) => {
                seen.push({ rows, state: state ?? "missing" });
            });
            first.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-session-retry:2"),
                rows: [{ id: "retained" }],
            });
            await flush();
            first.emit({
                t: "error",
                code: "CDB_SHARD_UNAVAILABLE",
                retryable: true,
                correlationId: "corr-session-open" as never,
                docs: "https://chardb.dev/errors/cdb_shard_unavailable",
            });
            await flush();
            expect(c.state).toBe("reconnecting");
            expect(seen.at(-1)).toEqual({ rows: [{ id: "retained" }], state: "live" });
            expect(timers.scheduledDelays()).toEqual([30_000, 250]);

            timers.runDelay(250);
            await flush();
            const second = fakeWebSocket(1);
            const hello = second.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
            expect(hello).toMatchObject({ resumeFromCookie: Cookie("c-session-retry:2") });
            await welcome(second, "c-session-retry:new");
            expect(sentSubscriptions(second)).toHaveLength(1);
            expect(seen).toHaveLength(1);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("an open nonretryable session error terminates subscriptions and mutations", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        const ws = fakeWebSocket();
        await welcome(ws);
        const states: string[] = [];
        c.subscribe("queries.ts#session-terminal", {}, (_rows, state) => states.push(state ?? "missing"));
        const mutation = c.mutate("mutations.ts#session-terminal", {});
        ws.emit({
            t: "error",
            code: "CDB_FORBIDDEN",
            retryable: false,
            correlationId: "corr-session-terminal" as never,
            docs: "https://chardb.dev/errors/cdb_forbidden",
        });
        await flush();

        expect(c.state).toBe("closed");
        expect(states).toEqual(["error"]);
        await expect(mutation).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        expect(ws.readyState).toBe(FakeWS.CLOSED);
    });

    test("reconnect within RYW window resumes from lastCookie via Up.hello.resumeFromCookie", async () => {
        const c = client();
        await flush();
        const ws1 = fakeWebSocket();
        // Server welcome stamps the resume cookie.
        ws1.emit({ t: "welcome", protocolV: PROTOCOL_V, baseCookie: Cookie("c-1:42"), region: "test" });
        await flush();
        // Drop the connection. The reconnect timer fires after RECONNECT_INITIAL_BACKOFF_MS (250ms)
        // and we want to land inside the 30s RYW window so lastCookie is preserved.
        ws1.close();
        await new Promise(r => setTimeout(r, 350));
        const ws2 = FakeWS.instances[1];
        expect(ws2).toBeDefined();
        if (!ws2) throw new Error("expected reconnect to spawn a new WS");
        await flush();
        const helloAfterReconnect = ws2.sent.map(r => JSON.parse(r) as Up).find(m => m.t === "hello");
        if (!helloAfterReconnect || helloAfterReconnect.t !== "hello") throw new Error("expected hello on reconnect");
        expect(helloAfterReconnect.resumeFromCookie).toBe(Cookie("c-1:42"));
        c.close();
    });

    test("reconnect after RYW expiry drops the cookie and retained query state before resubscribing", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws1 = fakeWebSocket();
            ws1.emit({ t: "welcome", protocolV: PROTOCOL_V, baseCookie: Cookie("c-1:42"), region: "test" });
            const seen: Array<{ readonly rows: unknown[]; readonly state: string }> = [];
            c.subscribe<{ id: string }>("queries.ts#listMessages", { organizationId: "org-1" }, (rows, state) =>
                seen.push({ rows: [...rows], state: state ?? "missing" })
            );
            ws1.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "stale" }] });
            await flush();
            expect(seen.at(-1)).toEqual({ rows: [{ id: "stale" }], state: "live" });

            ws1.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([30_000, 250]);
            timers.runDelay(30_000);
            expect(seen.at(-1)).toEqual({ rows: [], state: "refetching" });
            timers.runDelay(250);
            await flush();

            const ws2 = FakeWS.instances[1];
            if (!ws2) throw new Error("expected reconnect to spawn a new WS");
            await flush();
            const helloAfter = ws2.sent.map(r => JSON.parse(r) as Up).find(m => m.t === "hello");
            if (!helloAfter || helloAfter.t !== "hello") throw new Error("expected hello on reconnect");
            expect(helloAfter.resumeFromCookie).toBeUndefined();
            await welcome(ws2);
            expect(sentSubscriptions(ws2)).toHaveLength(1);

            ws2.emit({
                t: "snapshot",
                subId: SubId(1),
                cookie: Cookie("c-1:43"),
                rows: [{ id: "rematerialized" }],
            });
            await flush();
            expect(seen.at(-1)).toEqual({ rows: [{ id: "rematerialized" }], state: "live" });
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("pre-welcome reconnect failures share one RYW deadline and clear retained state once", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            let socket = fakeWebSocket();
            await welcome(socket, "c-1:42");
            const seen: Array<{ readonly rows: unknown[]; readonly state: string }> = [];
            c.subscribe<{ id: string }>("queries.ts#listMessages", { organizationId: "org-1" }, (rows, state) =>
                seen.push({ rows: [...rows], state: state ?? "missing" })
            );
            socket.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "stale" }] });
            await flush();

            for (const delayMs of [250, 500, 1_000, 2_000, 4_000, 8_000]) {
                socket.close();
                await flush();
                expect(timers.scheduledDelays()).toEqual([30_000, delayMs]);
                timers.runDelay(delayMs);
                await flush();
                socket = fakeWebSocket(FakeWS.instances.length - 1);
                const hello = socket.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
                if (!hello || hello.t !== "hello") throw new Error("expected hello on reconnect");
                expect(hello.resumeFromCookie).toBe(Cookie("c-1:43"));
            }

            socket.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([30_000, 10_000]);
            timers.runDelay(30_000);
            expect(seen.filter(observation => observation.state === "refetching")).toEqual([
                { rows: [], state: "refetching" },
            ]);
            expect(timers.scheduledDelays()).toEqual([10_000]);
            timers.runDelay(10_000);
            await flush();
            socket = fakeWebSocket(FakeWS.instances.length - 1);
            const freshHello = socket.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
            if (!freshHello || freshHello.t !== "hello") throw new Error("expected fresh retry hello");
            expect(freshHello.resumeFromCookie).toBeUndefined();

            socket.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([10_000]);
            timers.runDelay(10_000);
            await flush();
            expect(seen.filter(observation => observation.state === "refetching")).toHaveLength(1);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("RYW expiry clears only pre-disconnect state while reconnect JWT is held", async () => {
        const timers = installManualTimers();
        let jwtCalls = 0;
        let resolveHeldJwt: ((jwt: string) => void) | undefined;
        const heldJwt = new Promise<string>(resolve => {
            resolveHeldJwt = resolve;
        });
        const c = client({
            getJwt: async () => {
                jwtCalls += 1;
                return jwtCalls === 1 ? "jwt-initial" : heldJwt;
            },
        });
        try {
            await flush();
            const ws1 = fakeWebSocket();
            await welcome(ws1, "c-1:42");
            const retainedStates: string[] = [];
            c.subscribe("queries.ts#retained", { organizationId: "org-1" }, (_rows, state) =>
                retainedStates.push(state ?? "missing")
            );
            ws1.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "stale" }] });
            await flush();

            ws1.close();
            await flush();
            let offlineNotifications = 0;
            c.subscribe("queries.ts#offline", { organizationId: "org-1" }, () => {
                offlineNotifications += 1;
            });
            timers.runDelay(250);
            await flush();
            expect(jwtCalls).toBe(2);
            expect(FakeWS.instances).toHaveLength(1);

            timers.runDelay(30_000);
            expect(retainedStates.at(-1)).toBe("refetching");
            expect(retainedStates.filter(state => state === "refetching")).toHaveLength(1);
            expect(offlineNotifications).toBe(0);

            resolveHeldJwt?.("jwt-replacement");
            await flush();
            const ws2 = FakeWS.instances[1];
            if (!ws2) throw new Error("expected held JWT reconnect socket");
            const hello = ws2.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
            if (!hello || hello.t !== "hello") throw new Error("expected hello after held JWT");
            expect(hello.resumeFromCookie).toBeUndefined();
            await welcome(ws2);
            expect(sentSubscriptions(ws2).map(message => message.subId)).toEqual([SubId(1), SubId(2)]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("one duplicate-recovered subscription preserves the cookie while another expires", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws1 = fakeWebSocket();
            await welcome(ws1, "c-1:42");
            const statesA: string[] = [];
            const statesB: string[] = [];
            c.subscribe("queries.ts#a", { organizationId: "org-1" }, (_rows, state) =>
                statesA.push(state ?? "missing")
            );
            c.subscribe("queries.ts#b", { organizationId: "org-1" }, (_rows, state) =>
                statesB.push(state ?? "missing")
            );
            ws1.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "a" }] });
            ws1.emit({ t: "snapshot", subId: SubId(2), cookie: Cookie("c-1:44"), rows: [{ id: "b" }] });
            await flush();

            ws1.close();
            await flush();
            timers.runDelay(250);
            await flush();
            const ws2 = FakeWS.instances[1];
            if (!ws2) throw new Error("expected first replacement socket");
            ws2.emit({
                t: "welcome",
                protocolV: PROTOCOL_V,
                baseCookie: Cookie("c-test:0"),
                resumedFromCookie: Cookie("c-1:44"),
                region: "test",
            });
            await flush();
            expect(timers.scheduledDelays()).toEqual([30_000]);

            ws2.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "ignored" }] });
            await flush();
            expect(statesA.filter(state => state === "live")).toHaveLength(1);
            timers.runDelay(30_000);
            expect(statesA.at(-1)).toBe("live");
            expect(statesB.at(-1)).toBe("refetching");

            ws2.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([30_000, 250]);
            timers.runDelay(250);
            await flush();
            const ws3 = FakeWS.instances[2];
            if (!ws3) throw new Error("expected second replacement socket");
            const hello = ws3.sent.map(raw => JSON.parse(raw) as Up).find(message => message.t === "hello");
            if (!hello || hello.t !== "hello") throw new Error("expected second replacement hello");
            expect(hello.resumeFromCookie).toBe(Cookie("c-1:44"));

            ws3.close();
            await flush();
            timers.runDelay(30_000);
            expect(statesA.at(-1)).toBe("refetching");
            expect(statesA.filter(state => state === "refetching")).toHaveLength(1);
            expect(statesB.filter(state => state === "refetching")).toHaveLength(1);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("RYW expiry does not resend a subscription removed by its refetch listener", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws1 = fakeWebSocket();
            await welcome(ws1, "c-1:42");
            const subscription = c.subscribe("queries.ts#listMessages", { organizationId: "org-1" }, (_rows, state) => {
                if (state === "refetching") subscription.unsubscribe();
            });
            ws1.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "stale" }] });
            await flush();

            ws1.close();
            await flush();
            timers.runDelay(30_000);
            timers.runDelay(250);
            await flush();
            const ws2 = FakeWS.instances[1];
            if (!ws2) throw new Error("expected reconnect to spawn a new WS");
            await welcome(ws2);
            expect(sentSubscriptions(ws2)).toEqual([]);
            subscription.unsubscribe();
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("RYW expiry stops reconnect when a refetch listener closes the client", async () => {
        const timers = installManualTimers();
        const c = client();
        try {
            await flush();
            const ws1 = fakeWebSocket();
            await welcome(ws1, "c-1:42");
            const states: string[] = [];
            c.subscribe("queries.ts#listMessages", { organizationId: "org-1" }, (_rows, state) => {
                states.push(state ?? "missing");
                if (state === "refetching") c.close();
            });
            ws1.emit({ t: "snapshot", subId: SubId(1), cookie: Cookie("c-1:43"), rows: [{ id: "stale" }] });
            await flush();

            ws1.close();
            await flush();
            timers.runDelay(30_000);
            await flush();
            expect(c.state).toBe("closed");
            expect(FakeWS.instances).toHaveLength(1);
            expect(states.slice(-2)).toEqual(["refetching", "closed"]);
        } finally {
            c.close();
            timers.restore();
        }
    });

    test("close() settles queued work once and halts reconnect attempts", async () => {
        const c = client();
        await flush();
        const ws = fakeWebSocket();
        let subscriptionNotifications = 0;
        const subscriptionStates: string[] = [];
        c.subscribe("queries.ts#listMessages", {}, (_rows, state) => {
            subscriptionNotifications++;
            subscriptionStates.push(state ?? "missing");
        });
        let rejectionCount = 0;
        const mutationError = c.mutate("src/api.ts#post", {}).catch(error => {
            rejectionCount++;
            return error;
        });
        c.close();
        c.close();
        await flush();
        expect(c.state).toBe("closed");
        expect(ws.readyState).toBe(FakeWS.CLOSED);
        expect(subscriptionNotifications).toBe(1);
        expect(subscriptionStates).toEqual(["closed"]);
        expect(rejectionCount).toBe(1);
        await expect(mutationError).resolves.toMatchObject({
            code: "CDB_STREAM_ABORTED",
            message: "CharDB client closed before pending work settled",
        });
        await new Promise(resolve => setTimeout(resolve, 300));
        expect(FakeWS.instances).toHaveLength(1);
    });

    test("fences every queued socket callback after terminal close", async () => {
        const timers = installManualTimers();
        FakeWS.autoOpen = false;
        try {
            const c = client({ mutationTimeoutMs: 1_000 });
            await flush();
            const ws = fakeWebSocket();
            const lateOpen = ws.onopen;
            const lateMessage = ws.onmessage;
            const lateError = ws.onerror;
            const lateClose = ws.onclose;
            const seen: RawJson[][] = [];
            c.subscribe("queries.ts#late-terminal", {}, rows => seen.push(rows));
            let rejectionCount = 0;
            const mutationError = c.mutate("mutations.ts#late-terminal", {}).catch(error => {
                rejectionCount += 1;
                return error;
            });

            c.close();
            lateOpen?.();
            lateMessage?.({
                data: encodeWire({
                    t: "welcome",
                    protocolV: PROTOCOL_V,
                    baseCookie: Cookie("c-late:1"),
                    region: "test",
                }),
            });
            lateMessage?.({
                data: encodeWire({
                    t: "poke",
                    cookie: Cookie("c-late:2"),
                    patches: [{ op: "put", subId: SubId(1), rowKey: "late", row: { value: "must-not-apply" } }],
                    mutResults: [
                        {
                            mutId: MutId("00000000-0000-4000-8000-000000000000"),
                            ok: true,
                            result: "late",
                            cookie: Cookie("c-late:2"),
                        },
                    ],
                }),
            });
            lateError?.();
            lateClose?.();
            await flush();

            expect(c.state).toBe("closed");
            expect(seen).toEqual([[]]);
            expect(rejectionCount).toBe(1);
            await expect(mutationError).resolves.toMatchObject({ code: "CDB_STREAM_ABORTED" });
            expect(timers.scheduledDelays()).toEqual([]);
            expect(ws.sent).toEqual([]);
            expect(ws.closeCalls).toBe(1);
            expect(FakeWS.instances).toHaveLength(1);
        } finally {
            FakeWS.autoOpen = true;
            timers.restore();
        }
    });

    test("revokes a socket before error-driven backoff begins", async () => {
        const timers = installManualTimers();
        let c: ReturnType<typeof client> | undefined;
        try {
            c = client();
            await flush();
            const ws = fakeWebSocket();
            await welcome(ws);
            const seen: RawJson[][] = [];
            c.subscribe("queries.ts#error-backoff", {}, rows => seen.push(rows));
            const staleOpen = ws.onopen;
            const staleMessage = ws.onmessage;
            const staleError = ws.onerror;
            const staleClose = ws.onclose;
            const sentBeforeError = ws.sent.length;

            staleError?.();
            staleOpen?.();
            staleMessage?.({
                data: encodeWire({
                    t: "welcome",
                    protocolV: PROTOCOL_V,
                    baseCookie: Cookie("c-stale-error:1"),
                    region: "test",
                }),
            });
            staleMessage?.({
                data: encodeWire({
                    t: "poke",
                    cookie: Cookie("c-stale-error:2"),
                    patches: [{ op: "put", subId: SubId(1), rowKey: "stale", row: { value: true } }],
                }),
            });
            staleClose?.();
            staleClose?.();
            staleError?.();
            await flush();

            expect(c.state).toBe("reconnecting");
            expect(seen).toEqual([]);
            expect(ws.sent).toHaveLength(sentBeforeError);
            expect(ws.closeCalls).toBe(1);
            expect(timers.scheduledDelays()).toEqual([250]);
            expect(FakeWS.instances).toHaveLength(1);
        } finally {
            c?.close();
            timers.restore();
        }
    });

    test("keeps a closed socket fenced while reconnect JWT is pending", async () => {
        const timers = installManualTimers();
        let resolveReconnectJwt: ((jwt: string) => void) | undefined;
        const reconnectJwt = new Promise<string>(resolve => {
            resolveReconnectJwt = resolve;
        });
        let jwtCalls = 0;
        let c: ReturnType<typeof client> | undefined;
        try {
            c = client({
                getJwt: () => {
                    jwtCalls += 1;
                    return jwtCalls === 1 ? Promise.resolve("jwt-initial") : reconnectJwt;
                },
            });
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const seen: RawJson[][] = [];
            c.subscribe("queries.ts#held-reconnect", {}, rows => seen.push(rows));
            const staleOpen = first.onopen;
            const staleMessage = first.onmessage;
            const staleError = first.onerror;
            const staleClose = first.onclose;
            const sentBeforeClose = first.sent.length;

            first.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);
            timers.runDelay(250);
            await flush();
            expect(jwtCalls).toBe(2);
            expect(c.state).toBe("connecting");
            expect(FakeWS.instances).toHaveLength(1);

            staleOpen?.();
            staleMessage?.({
                data: encodeWire({
                    t: "welcome",
                    protocolV: PROTOCOL_V,
                    baseCookie: Cookie("c-held-stale:1"),
                    region: "test",
                }),
            });
            staleMessage?.({
                data: encodeWire({
                    t: "poke",
                    cookie: Cookie("c-held-stale:2"),
                    patches: [{ op: "put", subId: SubId(1), rowKey: "stale", row: { value: true } }],
                }),
            });
            staleError?.();
            staleClose?.();
            staleClose?.();
            await flush();

            expect(c.state).toBe("connecting");
            expect(seen).toEqual([]);
            expect(first.sent).toHaveLength(sentBeforeClose);
            expect(first.closeCalls).toBe(1);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(FakeWS.instances).toHaveLength(1);

            resolveReconnectJwt?.("jwt-reconnect");
            await flush();
            expect(FakeWS.instances).toHaveLength(2);
        } finally {
            c?.close();
            timers.restore();
        }
    });

    test("ignores callbacks from a superseded socket after reconnect", async () => {
        const timers = installManualTimers();
        let c: ReturnType<typeof client> | undefined;
        try {
            c = client();
            await flush();
            const first = fakeWebSocket();
            await welcome(first);
            const seen: RawJson[][] = [];
            c.subscribe("queries.ts#stale-socket", {}, rows => seen.push(rows));
            const staleOpen = first.onopen;
            const staleMessage = first.onmessage;
            const staleError = first.onerror;
            const staleClose = first.onclose;

            first.close();
            await flush();
            expect(timers.scheduledDelays()).toEqual([250]);
            timers.runDelay(250);
            await flush();
            const second = fakeWebSocket(1);
            await welcome(second, "c-current:1");
            const currentSentCount = second.sent.length;

            staleOpen?.();
            staleMessage?.({
                data: encodeWire({
                    t: "poke",
                    cookie: Cookie("c-stale:2"),
                    patches: [{ op: "put", subId: SubId(1), rowKey: "stale", row: { value: true } }],
                }),
            });
            staleError?.();
            staleClose?.();
            await flush();

            expect(c.state).toBe("open");
            expect(seen).toEqual([]);
            expect(second.sent).toHaveLength(currentSentCount);
            expect(first.closeCalls).toBe(1);
            expect(second.closeCalls).toBe(0);
            expect(timers.scheduledDelays()).toEqual([]);
            expect(FakeWS.instances).toHaveLength(2);
        } finally {
            c?.close();
            timers.restore();
        }
    });

    test("close clears every pending mutation deadline", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        const timeoutSpy = spyOnClearTimeout();
        try {
            const mutations = [c.mutate("src/api.ts#one", {}), c.mutate("src/api.ts#two", {})];
            c.close();
            await Promise.all(
                mutations.map(mutation => expect(mutation).rejects.toMatchObject({ code: "CDB_STREAM_ABORTED" }))
            );
            expect(timeoutSpy.calls).toHaveLength(2);
        } finally {
            timeoutSpy.restore();
            c.close();
        }
    });

    test("mutate after close rejects immediately without creating a deadline", async () => {
        const c = client({ mutationTimeoutMs: 1_000 });
        await flush();
        c.close();
        await flush();
        const timeoutSpy = spyOnClearTimeout();
        try {
            await expect(c.mutate("src/api.ts#after-close", {})).rejects.toMatchObject({
                code: "CDB_STREAM_ABORTED",
                retryable: true,
            });
            expect(timeoutSpy.calls).toHaveLength(0);
        } finally {
            timeoutSpy.restore();
        }
    });
});
