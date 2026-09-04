/**
 * `chardb` client SDK.
 *
 * Owns:
 *   - WS reconnect with cookie carryover (G24, RECONNECT_RYW_WINDOW_MS = 30s)
 *   - per-sub state machine (pending → live → live → refetching → live)
 *   - mutId allocation (UUIDv7)
 */

import { uuidv7 } from "uuidv7";
import { CdbError, type CdbErrorCode, isCdbError } from "../errors.ts";
import { ChardbRef, ClientId, type Cookie, MutId, type RawJson, SubId } from "../types.ts";
import { PROTOCOL_V, type RowPatch, type Up, checkProtocolV, decodeDown, encodeWire } from "../wire.ts";
import { assertSerializedSize, snapshotMutationArguments, snapshotSubscriptionArguments } from "./serialized-json.ts";

export const RECONNECT_RYW_WINDOW_MS = 30_000;
const DEFAULT_MUTATION_TIMEOUT_MS = 60_000;
const RECONNECT_INITIAL_BACKOFF_MS = 250;
const RECONNECT_MAX_BACKOFF_MS = 10_000;
const SUBSCRIPTION_RETRY_INITIAL_BACKOFF_MS = 100;
const SUBSCRIPTION_RETRY_MAX_BACKOFF_MS = 2_000;
const JWT_REFRESH_LEAD_MS = 60_000;
const JWT_REFRESH_READ_INITIAL_BACKOFF_MS = 1_000;
const JWT_REFRESH_READ_MAX_BACKOFF_MS = 10_000;
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const MAX_INBOUND_WEBSOCKET_BYTES = 1_024 * 1_024;
const MAX_ACTIVE_SUBSCRIPTIONS = 64;
const MAX_SUBSCRIPTION_ROWS = 4_096;
const MAX_SUBSCRIPTION_BYTES = 512 * 1_024;
const MAX_PATCHES_PER_BATCH = 4_096;
const MAX_PATCH_BATCH_BYTES = 512 * 1_024;
const MAX_PENDING_MUTATIONS = 32;
const MAX_RETAINED_QUERY_STATE_BYTES = 8 * 1_024 * 1_024;
const INBOUND_TEXT_ENCODER = new TextEncoder();

export interface ChardbClientOptions {
    readonly endpoint: string;
    readonly getJwt: () => Promise<string>;
    readonly clientId?: string;
    /** Maximum time to wait for a mutation result, including reconnects. Defaults to 60 seconds. */
    readonly mutationTimeoutMs?: number;
    /** Receives bounded terminal-session diagnostics. Listener failures are ignored. */
    readonly onSessionError?: (diagnostic: ChardbClientSessionErrorDiagnostic) => void;
}

export type ChardbClientSessionFailureReason =
    | "auth-refresh-read"
    | "auth-refresh-invalid-token"
    | "auth-refresh-principal-changed"
    | "auth-refresh-expiry-not-extended"
    | "auth-refresh-send"
    | "connect"
    | "subscription-retry-send"
    | "reconnect-refetch-listener"
    | "auth-refresh-close"
    | "invalid-handshake-frame"
    | "invalid-session-frame"
    | "protocol-selection"
    | "protocol-rejection"
    | "auth-refresh-rejection"
    | "session-rejection"
    | "unsubscribe-send"
    | "client-close";

export interface ChardbClientSessionErrorDiagnostic {
    readonly code: CdbErrorCode;
    readonly reason: ChardbClientSessionFailureReason;
}

type SubState = "pending" | "live" | "refetching" | "error" | "closed";
type TerminalSubState = Extract<SubState, "error" | "closed">;

/**
 * A subscription record. Rows always travel as `RawJson` over the wire;
 * the public `subscribe<TRow>` wraps the user's typed listener to widen on
 * the way in. Keeping the storage type uniform avoids dual-typing the `Map`
 * and the listener `Set` and lets us delete the cast chain that used to
 * land at `subs.set` and the listener constructor.
 */
interface SubRecord {
    readonly subId: SubId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    state: SubState;
    rows: RawJson[];
    listeners: Set<(rows: RawJson[], state: SubState) => void>;
    lastSnapshotCookie: Cookie | undefined;
    retryTimer: ReturnType<typeof setTimeout> | null;
    retryBackoffMs: number;
}

interface PlannedSubState {
    rows: RawJson[];
}

interface PendingMutation {
    readonly mutId: MutId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    resolve: (result: RawJson) => void;
    reject: (err: CdbError) => void;
    /** Set after first send so reconnect doesn't double-resolve. */
    inFlight: boolean;
    timeout: ReturnType<typeof setTimeout> | null;
}

interface JwtRefreshClaims {
    readonly subject: string;
    readonly expiresAtMs: number;
}

interface PendingAuthRefresh {
    readonly attempt: number;
    readonly socket: WebSocket;
    readonly claims: JwtRefreshClaims;
    readonly generation: number;
}

function decodeJwtRefreshClaims(jwt: string): JwtRefreshClaims | null {
    const parts = jwt.split(".");
    const payload = parts[1];
    if (parts.length !== 3 || payload === undefined || payload.length === 0) return null;
    try {
        const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
        const binary = atob(padded);
        const bytes = Uint8Array.from(binary, character => character.charCodeAt(0));
        const value = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
        if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
        const claims = value as { readonly sub?: unknown; readonly exp?: unknown };
        if (
            typeof claims.sub !== "string" ||
            claims.sub.length === 0 ||
            typeof claims.exp !== "number" ||
            !Number.isSafeInteger(claims.exp) ||
            claims.exp <= 0
        ) {
            return null;
        }
        const expiresAtMs = claims.exp * 1_000;
        if (!Number.isSafeInteger(expiresAtMs)) return null;
        return { subject: claims.sub, expiresAtMs };
    } catch {
        return null;
    }
}

export interface ChardbClient {
    /** Open a live subscription; returns a disposer. */
    subscribe<TRow = RawJson>(
        ref: string,
        args: RawJson,
        onChange: (rows: TRow[], state?: SubState) => void
    ): { unsubscribe: () => void };
    /** Issue a mutation; resolves with server result after canonical state arrives. */
    mutate<TResult = RawJson>(ref: string, args: RawJson): Promise<TResult>;
    close(): void;
    /** Current connection liveness (for diagnostics). */
    readonly state: "connecting" | "open" | "reconnecting" | "closed";
}

export function createChardbClient(opts: ChardbClientOptions): ChardbClient {
    const controller = createDeferredChardbClientController(opts);
    controller.start();
    return controller.client;
}

interface DeferredChardbClientControllerOptions {
    /** @internal Let an auth-aware owner decide when queued work may connect. */
    readonly autoStartOnOperation?: boolean;
    /** @internal Bounded retries for retryable JWT bootstrap failures. */
    readonly initialJwtFailureRetries?: number;
}

/** @internal Used by the React provider to keep connection startup out of render. */
export function createDeferredChardbClientController(
    opts: ChardbClientOptions,
    controllerOpts: DeferredChardbClientControllerOptions = {}
): {
    readonly client: ChardbClient;
    readonly start: () => void;
} {
    const autoStartOnOperation = controllerOpts.autoStartOnOperation ?? true;
    const initialJwtFailureRetries = controllerOpts.initialJwtFailureRetries ?? 0;
    if (!Number.isSafeInteger(initialJwtFailureRetries) || initialJwtFailureRetries < 0) {
        throw new RangeError("initialJwtFailureRetries must be a non-negative integer");
    }
    const mutationTimeoutMs = opts.mutationTimeoutMs ?? DEFAULT_MUTATION_TIMEOUT_MS;
    if (!Number.isSafeInteger(mutationTimeoutMs) || mutationTimeoutMs <= 0 || mutationTimeoutMs > MAX_TIMER_DELAY_MS) {
        throw new RangeError(`mutationTimeoutMs must be an integer between 1 and ${MAX_TIMER_DELAY_MS}`);
    }
    const clientId = ClientId(opts.clientId ?? crypto.randomUUID());
    const subs = new Map<number, SubRecord>();
    const pending = new Map<string, PendingMutation>();
    let nextSubId = 1;
    let lastCookie: Cookie | undefined;
    let ws: WebSocket | null = null;
    let state: ChardbClient["state"] = "connecting";
    let terminated = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let reconnectBackoff = RECONNECT_INITIAL_BACKOFF_MS;
    const resumeRetainedSubs = new Map<number, SubRecord>();
    let resumeExpiryTimer: ReturnType<typeof setTimeout> | null = null;
    let resumeExpiryCookie: Cookie | undefined;
    let started = false;
    let connectionAttempt = 0;
    let authRefreshTimer: ReturnType<typeof setTimeout> | null = null;
    let authRefreshDeadlineTimer: ReturnType<typeof setTimeout> | null = null;
    let authRefreshGeneration = 0;
    let authRefreshReadBackoffMs = JWT_REFRESH_READ_INITIAL_BACKOFF_MS;
    let pendingAuthRefresh: PendingAuthRefresh | null = null;
    let openedSession = false;
    let initialJwtFailures = 0;

    function clearAuthRefreshTimer(): void {
        if (authRefreshTimer === null) return;
        clearTimeout(authRefreshTimer);
        authRefreshTimer = null;
    }

    function clearAuthRefreshDeadline(): void {
        authRefreshGeneration += 1;
        if (authRefreshDeadlineTimer === null) return;
        clearTimeout(authRefreshDeadlineTimer);
        authRefreshDeadlineTimer = null;
    }

    function beginAuthRefresh(attempt: number, socket: WebSocket, claims: JwtRefreshClaims): void {
        clearAuthRefreshDeadline();
        if (terminated || attempt !== connectionAttempt || ws !== socket || state !== "open") return;
        const generation = authRefreshGeneration;
        const delayMs = Math.max(0, claims.expiresAtMs - Date.now());
        const deadlineTimer = setTimeout(() => {
            if (
                authRefreshDeadlineTimer !== deadlineTimer ||
                terminated ||
                generation !== authRefreshGeneration ||
                attempt !== connectionAttempt ||
                ws !== socket ||
                state !== "open"
            ) {
                return;
            }
            authRefreshDeadlineTimer = null;
            const awaitingGateway = pendingAuthRefresh?.generation === generation;
            pendingAuthRefresh = null;
            failSession(
                "CDB_FORBIDDEN",
                "authentication expired before refresh completed",
                awaitingGateway ? "auth-refresh-close" : "auth-refresh-read"
            );
        }, delayMs);
        authRefreshDeadlineTimer = deadlineTimer;
        void refreshAuth(attempt, socket, claims, generation);
    }

    function scheduleAuthRefresh(attempt: number, socket: WebSocket, claims: JwtRefreshClaims): void {
        clearAuthRefreshTimer();
        if (terminated || attempt !== connectionAttempt || ws !== socket || state !== "open") return;
        if (authRefreshDeadlineTimer !== null) return;
        const remainingMs = claims.expiresAtMs - JWT_REFRESH_LEAD_MS - Date.now();
        const delayMs = Math.max(0, Math.min(remainingMs, MAX_TIMER_DELAY_MS));
        const refreshTimer = setTimeout(() => {
            if (authRefreshTimer !== refreshTimer) return;
            authRefreshTimer = null;
            if (remainingMs > MAX_TIMER_DELAY_MS) {
                scheduleAuthRefresh(attempt, socket, claims);
                return;
            }
            beginAuthRefresh(attempt, socket, claims);
        }, delayMs);
        authRefreshTimer = refreshTimer;
    }

    async function refreshAuth(
        attempt: number,
        socket: WebSocket,
        current: JwtRefreshClaims,
        generation: number
    ): Promise<void> {
        let jwt: string;
        try {
            jwt = await opts.getJwt();
        } catch {
            if (
                terminated ||
                generation !== authRefreshGeneration ||
                attempt !== connectionAttempt ||
                ws !== socket ||
                state !== "open"
            ) {
                return;
            }
            const validForMs = current.expiresAtMs - Date.now();
            if (validForMs <= 0) {
                failSession("CDB_FORBIDDEN", "failed to refresh CharDB authentication", "auth-refresh-read");
                return;
            }
            const delayMs = Math.min(authRefreshReadBackoffMs, validForMs);
            authRefreshReadBackoffMs = Math.min(authRefreshReadBackoffMs * 2, JWT_REFRESH_READ_MAX_BACKOFF_MS);
            const retryTimer = setTimeout(() => {
                if (authRefreshTimer !== retryTimer) return;
                authRefreshTimer = null;
                void refreshAuth(attempt, socket, current, generation);
            }, delayMs);
            authRefreshTimer = retryTimer;
            return;
        }
        if (
            terminated ||
            generation !== authRefreshGeneration ||
            attempt !== connectionAttempt ||
            ws !== socket ||
            state !== "open"
        ) {
            return;
        }
        const refreshed = decodeJwtRefreshClaims(jwt);
        if (!refreshed) {
            failSession(
                "CDB_FORBIDDEN",
                "authentication refresh returned an invalid JWT",
                "auth-refresh-invalid-token"
            );
            return;
        }
        // Decoded claims only decide whether this client can retain its
        // socket. The Gateway still verifies the token before it admits work.
        if (refreshed.subject !== current.subject) {
            failSession("CDB_FORBIDDEN", "authentication refresh changed principal", "auth-refresh-principal-changed");
            return;
        }
        if (refreshed.expiresAtMs <= current.expiresAtMs || refreshed.expiresAtMs <= Date.now()) {
            failSession(
                "CDB_FORBIDDEN",
                "authentication refresh did not extend the JWT expiry",
                "auth-refresh-expiry-not-extended"
            );
            return;
        }
        authRefreshReadBackoffMs = JWT_REFRESH_READ_INITIAL_BACKOFF_MS;
        try {
            const update: Up = { t: "updateAuth", jwt };
            pendingAuthRefresh = { attempt, socket, claims: refreshed, generation };
            socket.send(encodeWire(update));
        } catch {
            pendingAuthRefresh = null;
            failSession("CDB_STREAM_ABORTED", "failed to send refreshed CharDB authentication", "auth-refresh-send");
        }
    }

    async function connect(attempt: number): Promise<void> {
        if (terminated || attempt !== connectionAttempt) return;
        state = "connecting";
        let jwt: string;
        try {
            jwt = await opts.getJwt();
        } catch (error) {
            if (terminated || attempt !== connectionAttempt) return;
            if (!openedSession) {
                if (!isCdbError(error) || !error.retryable || initialJwtFailures >= initialJwtFailureRetries) {
                    throw error;
                }
                initialJwtFailures += 1;
            }
            scheduleReconnect();
            return;
        }
        const jwtRefreshClaims = decodeJwtRefreshClaims(jwt);
        if (terminated || attempt !== connectionAttempt) return;
        authRefreshReadBackoffMs = JWT_REFRESH_READ_INITIAL_BACKOFF_MS;
        const url = new URL(opts.endpoint);
        url.searchParams.set("clientId", clientId);
        const socket = new WebSocket(url.toString());
        ws = socket;
        socket.onopen = () => {
            if (terminated || attempt !== connectionAttempt || ws !== socket) return;
            try {
                const hello: Up = {
                    t: "hello",
                    protocolV: PROTOCOL_V,
                    clientId,
                    ...(lastCookie ? { resumeFromCookie: lastCookie } : {}),
                    jwt,
                };
                socket.send(encodeWire(hello));
            } catch {
                disconnectSocket(attempt, socket);
            }
        };
        socket.onmessage = ev => {
            if (terminated || attempt !== connectionAttempt || ws !== socket) return;
            receiveWire(ev.data, attempt, socket, jwtRefreshClaims);
        };
        socket.onclose = () => {
            if (!revokeSocket(attempt, socket)) return;
            onClose();
        };
        socket.onerror = () => {
            disconnectSocket(attempt, socket);
        };
    }

    function disconnectSocket(attempt: number, socket: WebSocket): void {
        if (!revokeSocket(attempt, socket)) return;
        try {
            socket.close();
        } finally {
            onClose();
        }
    }

    function revokeSocket(attempt: number, socket: WebSocket): boolean {
        if (terminated || attempt !== connectionAttempt || ws !== socket) return false;
        connectionAttempt += 1;
        clearAuthRefreshTimer();
        clearAuthRefreshDeadline();
        if (pendingAuthRefresh?.socket === socket) pendingAuthRefresh = null;
        ws = null;
        return true;
    }

    function scheduleReconnect(): void {
        if (terminated) return;
        state = "reconnecting";
        if (reconnectTimer !== null) return;
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            reconnectBackoff = Math.min(reconnectBackoff * 2, RECONNECT_MAX_BACKOFF_MS);
            startConnect();
        }, reconnectBackoff);
    }

    function startConnect(): void {
        const attempt = ++connectionAttempt;
        void connect(attempt).catch(error => {
            if (terminated || attempt !== connectionAttempt) return;
            if (isCdbError(error)) {
                failSession(error.code, error.message, "connect");
                return;
            }
            failSession("CDB_INVARIANT", "failed to establish CharDB client session", "connect");
        });
    }

    function start(): void {
        if (started || terminated) return;
        started = true;
        startConnect();
    }

    function sendSessionState(): void {
        // The Gateway verifies hello asynchronously. Do not send protected
        // operations until its welcome proves the auth boundary opened.
        for (const sub of subs.values()) {
            if (sub.state === "error" || sub.state === "closed") continue;
            clearSubscriptionRetryTimer(sub);
            const upSub: Up = {
                t: "sub",
                subId: sub.subId,
                ref: sub.ref,
                args: sub.args,
            };
            ws?.send(encodeWire(upSub));
        }
        for (const m of pending.values()) {
            if (m.inFlight) continue;
            const upMut: Up = { t: "mut", mutId: m.mutId, ref: m.ref, args: m.args };
            ws?.send(encodeWire(upMut));
            m.inFlight = true;
        }
    }

    function acknowledgeSnapshot(cookie: Cookie): void {
        const socket = ws;
        if (!socket || state !== "open" || socket.readyState !== WebSocket.OPEN) return;
        try {
            const acknowledgement: Up = { t: "ack", cookie };
            socket.send(encodeWire(acknowledgement));
        } catch {
            // Snapshot acknowledgements are retryable by duplicate delivery.
        }
    }

    function clearSubscriptionRetryTimer(sub: SubRecord, resetBackoff = false): void {
        if (sub.retryTimer !== null) {
            clearTimeout(sub.retryTimer);
            sub.retryTimer = null;
        }
        if (resetBackoff) sub.retryBackoffMs = SUBSCRIPTION_RETRY_INITIAL_BACKOFF_MS;
    }

    function clearAllSubscriptionRetryTimers(): void {
        for (const sub of subs.values()) clearSubscriptionRetryTimer(sub);
    }

    function sendSubscription(sub: SubRecord, socket: WebSocket): void {
        const up: Up = {
            t: "sub",
            subId: sub.subId,
            ref: sub.ref,
            args: sub.args,
        };
        socket.send(encodeWire(up));
    }

    function scheduleSubscriptionRetry(sub: SubRecord): void {
        if (sub.retryTimer !== null) return;
        const delayMs = sub.retryBackoffMs;
        sub.retryTimer = setTimeout(() => {
            sub.retryTimer = null;
            if (terminated || subs.get(sub.subId) !== sub || sub.state === "error" || sub.state === "closed") {
                return;
            }
            const socket = ws;
            if (!socket || state !== "open" || socket.readyState !== WebSocket.OPEN) return;
            try {
                sendSubscription(sub, socket);
                sub.retryBackoffMs = Math.min(delayMs * 2, SUBSCRIPTION_RETRY_MAX_BACKOFF_MS);
            } catch {
                failSession(
                    "CDB_STREAM_ABORTED",
                    `failed to retry subscription ${sub.subId}`,
                    "subscription-retry-send"
                );
            }
        }, delayMs);
    }

    function releaseResumeRetainedSub(subId: SubId, sub: SubRecord): void {
        if (resumeRetainedSubs.get(subId) !== sub) return;
        resumeRetainedSubs.delete(subId);
        if (resumeRetainedSubs.size === 0 && resumeExpiryTimer !== null) {
            clearTimeout(resumeExpiryTimer);
            resumeExpiryTimer = null;
            resumeExpiryCookie = undefined;
        }
    }

    function beginResumeExpiry(): void {
        if (lastCookie === undefined || resumeExpiryTimer !== null) return;
        for (const [subId, sub] of subs) {
            if (sub.rows.length > 0 || sub.lastSnapshotCookie !== undefined || sub.state === "live") {
                resumeRetainedSubs.set(subId, sub);
            }
        }
        if (resumeRetainedSubs.size === 0) return;
        resumeExpiryCookie = lastCookie;
        resumeExpiryTimer = setTimeout(expireRetainedResumeState, RECONNECT_RYW_WINDOW_MS);
    }

    function expireRetainedResumeState(): void {
        resumeExpiryTimer = null;
        const retained = [...resumeRetainedSubs];
        const retainedRecords = new Set(retained.map(([, sub]) => sub));
        const hasAuthoritativeStateOutsideExpiry = [...subs.values()].some(
            sub => !retainedRecords.has(sub) && (sub.state === "live" || sub.lastSnapshotCookie !== undefined)
        );
        const cookieAdvanced = lastCookie !== undefined && lastCookie !== resumeExpiryCookie;
        if (!cookieAdvanced && !hasAuthoritativeStateOutsideExpiry) lastCookie = undefined;
        else if (!cookieAdvanced) lastCookie = resumeExpiryCookie;
        resumeExpiryCookie = undefined;
        resumeRetainedSubs.clear();
        for (const [subId, sub] of retained) {
            if (terminated) return;
            if (subs.get(subId) !== sub) continue;
            sub.state = "refetching";
            sub.rows = [];
            sub.lastSnapshotCookie = undefined;
            notify(sub);
        }
    }

    function onClose(): void {
        if (state === "closed") return;
        clearAllSubscriptionRetryTimers();
        beginResumeExpiry();
        for (const m of pending.values()) m.inFlight = false;
        scheduleReconnect();
    }

    function receiveWire(
        raw: unknown,
        attempt: number,
        socket: WebSocket,
        jwtRefreshClaims: JwtRefreshClaims | null
    ): void {
        if (terminated) return;
        try {
            if (typeof raw !== "string") throw new TypeError("server sent a non-text WebSocket message");
            // UTF-8 is never shorter than the JavaScript code-unit count. Reject
            // obvious excess before allocating the encoded copy, then measure
            // multibyte text exactly before JSON parsing.
            if (
                raw.length > MAX_INBOUND_WEBSOCKET_BYTES ||
                INBOUND_TEXT_ENCODER.encode(raw).byteLength > MAX_INBOUND_WEBSOCKET_BYTES
            ) {
                throw new TypeError("server WebSocket message exceeds the client transport limit");
            }
            onWire(raw, attempt, socket, jwtRefreshClaims);
        } catch {
            const message =
                state === "connecting"
                    ? "server sent an invalid CharDB handshake message"
                    : "server sent an invalid CharDB session message";
            failSession(
                "CDB_INVARIANT",
                message,
                state === "connecting" ? "invalid-handshake-frame" : "invalid-session-frame"
            );
        }
    }

    function onWire(raw: string, attempt: number, socket: WebSocket, jwtRefreshClaims: JwtRefreshClaims | null): void {
        const msg = decodeDown(raw);
        if (state === "connecting") {
            const validHandshakeMessage =
                msg.t === "welcome" ||
                (msg.t === "error" && msg.subId === undefined) ||
                (msg.t === "mustRefetch" && msg.reason === "protocolMismatch");
            if (!validHandshakeMessage) throw new TypeError("server sent data before welcome");
        } else if (msg.t === "welcome" || (msg.t === "mustRefetch" && msg.reason === "protocolMismatch")) {
            throw new TypeError("server sent a handshake frame after welcome");
        }
        switch (msg.t) {
            case "welcome":
                if (checkProtocolV(msg.protocolV)) {
                    failSession(
                        "CDB_UNSUPPORTED_FEATURE",
                        "server selected an unsupported CharDB protocol version",
                        "protocol-selection"
                    );
                    return;
                }
                lastCookie = msg.resumedFromCookie ?? msg.baseCookie;
                state = "open";
                openedSession = true;
                reconnectBackoff = RECONNECT_INITIAL_BACKOFF_MS;
                if (jwtRefreshClaims) scheduleAuthRefresh(attempt, socket, jwtRefreshClaims);
                sendSessionState();
                return;
            case "poke":
                applyPatches(msg.patches);
                lastCookie = msg.cookie;
                if (msg.mutResults) {
                    for (const r of msg.mutResults) {
                        const m = takePendingMutation(r.mutId);
                        if (!m) continue;
                        if (r.ok) {
                            m.resolve(r.result);
                        } else {
                            m.reject(
                                new CdbError({
                                    code: r.error.code,
                                    message: `mutation ${r.mutId} failed: ${r.error.code}`,
                                })
                            );
                        }
                    }
                }
                return;
            case "snapshot": {
                const sub = subs.get(msg.subId);
                if (!sub || sub.state === "error" || sub.state === "closed") return;
                if (sub.lastSnapshotCookie === msg.cookie) {
                    releaseResumeRetainedSub(msg.subId, sub);
                    acknowledgeSnapshot(msg.cookie);
                    return;
                }
                assertSubscriptionRows(msg.rows, "snapshot");
                const next: PlannedSubState = {
                    rows: cloneRawJson(msg.rows) as RawJson[],
                };
                assertAggregateQueryState(new Map([[sub, next]]));
                clearSubscriptionRetryTimer(sub, true);
                lastCookie = msg.cookie;
                sub.rows = next.rows;
                sub.lastSnapshotCookie = msg.cookie;
                sub.state = "live";
                releaseResumeRetainedSub(msg.subId, sub);
                notify(sub);
                acknowledgeSnapshot(msg.cookie);
                return;
            }
            case "mustRefetch":
                if (state === "connecting" && msg.reason === "protocolMismatch") {
                    failSession(
                        "CDB_UNSUPPORTED_FEATURE",
                        "server rejected the CharDB protocol version",
                        "protocol-rejection"
                    );
                    return;
                }
                if (msg.reason === "authChanged" && pendingAuthRefresh !== null) {
                    const completed = pendingAuthRefresh;
                    pendingAuthRefresh = null;
                    clearAuthRefreshDeadline();
                    scheduleAuthRefresh(completed.attempt, completed.socket, completed.claims);
                }
                for (const subId of msg.subIds) {
                    const sub = subs.get(subId);
                    if (!sub || sub.state === "error" || sub.state === "closed") continue;
                    if (msg.reason === "shardsChanged" && sub.retryTimer !== null) continue;
                    if (msg.reason !== "shardsChanged") clearSubscriptionRetryTimer(sub);
                    const notifyRefetch =
                        sub.state !== "refetching" || sub.rows.length > 0 || sub.lastSnapshotCookie !== undefined;
                    sub.state = "refetching";
                    sub.rows = [];
                    sub.lastSnapshotCookie = undefined;
                    releaseResumeRetainedSub(subId, sub);
                    if (notifyRefetch || msg.reason !== "shardsChanged") notify(sub);
                    if (terminated || subs.get(subId) !== sub) continue;
                    const socket = ws;
                    if (!socket || state !== "open" || socket.readyState !== WebSocket.OPEN) continue;
                    if (msg.reason === "shardsChanged") scheduleSubscriptionRetry(sub);
                    else sendSubscription(sub, socket);
                }
                return;
            case "error":
                if (pendingAuthRefresh !== null && msg.subId === undefined) {
                    pendingAuthRefresh = null;
                    clearAuthRefreshDeadline();
                    failSession(msg.code, `authentication refresh failed: ${msg.code}`, "auth-refresh-rejection");
                    return;
                }
                if (msg.subId !== undefined) {
                    applySubscriptionError(msg.retryable, msg.subId);
                    return;
                }
                if (msg.retryable) {
                    disconnectSocket(attempt, socket);
                    return;
                }
                failSession(msg.code, `CharDB session failed: ${msg.code}`, "session-rejection");
                return;
        }
        msg satisfies never;
    }

    function applyPatches(patches: readonly RowPatch[]): void {
        assertPatchBatchLimits(patches);
        for (const patch of patches) assertPatchRowShape(patch);
        const planned = new Map<SubRecord, PlannedSubState>();
        for (const p of patches) {
            const sub = subs.get(p.subId);
            if (!sub || sub.state === "error" || sub.state === "closed") continue;
            let next = planned.get(sub);
            if (!next) {
                next = { rows: [...sub.rows] };
                planned.set(sub, next);
            }
            const idx = next.rows.findIndex(r => (r as { __key?: string }).__key === p.rowKey);
            if (p.op === "del") {
                if (idx >= 0) next.rows.splice(idx, 1);
            } else {
                const row = {
                    ...(cloneRawJson(p.row as RawJson) as { readonly [key: string]: RawJson }),
                    __key: p.rowKey,
                } as RawJson;
                if (idx >= 0) next.rows[idx] = row;
                else next.rows.push(row);
            }
        }
        for (const next of planned.values()) {
            assertSubscriptionRows(next.rows, "patch result");
        }
        assertAggregateQueryState(planned);
        for (const [sub, next] of planned) {
            sub.rows = next.rows;
        }
        for (const sub of planned.keys()) {
            notify(sub);
        }
    }

    function assertPatchRowShape(patch: RowPatch): void {
        if (patch.op === "del" && patch.row === undefined) return;
        if (patch.row === null || typeof patch.row !== "object" || Array.isArray(patch.row)) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "row patch must contain an object row" });
        }
    }

    function assertPatchBatchLimits(value: unknown): asserts value is readonly unknown[] {
        if (!Array.isArray(value))
            throw new CdbError({ code: "CDB_INVARIANT", message: "patch batch must be an array" });
        if (value.length > MAX_PATCHES_PER_BATCH) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "patch batch exceeds the client count limit" });
        }
        assertSerializedSize(value, MAX_PATCH_BATCH_BYTES, "patch batch");
    }

    function cloneRawJson(value: RawJson | readonly RawJson[]): RawJson | RawJson[] {
        if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
            return value;
        }
        if (Array.isArray(value)) {
            const clone: RawJson[] = [];
            for (let index = 0; index < value.length; index++) {
                const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
                if (!descriptor || !("value" in descriptor)) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: "cannot clone an invalid JSON array",
                    });
                }
                clone.push(cloneRawJson(descriptor.value as RawJson) as RawJson);
            }
            return clone;
        }
        const clone: Record<string, RawJson> = Object.getPrototypeOf(value) === null ? Object.create(null) : {};
        for (const [key, child] of Object.entries(value)) {
            Object.defineProperty(clone, key, {
                value: cloneRawJson(child) as RawJson,
                enumerable: true,
                writable: true,
                configurable: true,
            });
        }
        return clone;
    }

    function assertAggregateQueryState(
        planned: ReadonlyMap<SubRecord, PlannedSubState>,
        additional: PlannedSubState | undefined = undefined,
        errorCode: CdbErrorCode = "CDB_INVARIANT"
    ): void {
        let bytes = 0;
        const add = (state: PlannedSubState): void => {
            bytes += assertSerializedSize(state.rows, MAX_RETAINED_QUERY_STATE_BYTES, "retained query state", {
                errorCode,
            });
            if (bytes > MAX_RETAINED_QUERY_STATE_BYTES) {
                throw new CdbError({
                    code: errorCode,
                    message: `retained query state exceeds the ${MAX_RETAINED_QUERY_STATE_BYTES}-byte client limit`,
                });
            }
        };
        for (const sub of subs.values()) {
            add(planned.get(sub) ?? sub);
        }
        if (additional) add(additional);
    }

    function assertSubscriptionRows(rows: readonly RawJson[], subject: string): void {
        if (rows.length > MAX_SUBSCRIPTION_ROWS) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `${subject} exceeds the ${MAX_SUBSCRIPTION_ROWS}-row client limit`,
            });
        }
        assertSerializedSize(rows, MAX_SUBSCRIPTION_BYTES, subject);
    }

    function applySubscriptionError(retryable: boolean, subId: SubId): void {
        const sub = subs.get(subId);
        if (!sub || sub.state === "error" || sub.state === "closed") return;
        const notifyRefetch = sub.state !== "refetching" || sub.rows.length > 0 || sub.lastSnapshotCookie !== undefined;
        if (!retryable) clearSubscriptionRetryTimer(sub);
        sub.state = retryable ? "refetching" : "error";
        sub.rows = [];
        sub.lastSnapshotCookie = undefined;
        releaseResumeRetainedSub(subId, sub);
        if (!retryable || notifyRefetch) notify(sub);
        if (retryable && !terminated && subs.get(subId) === sub) scheduleSubscriptionRetry(sub);
    }

    function clearMutationTimeout(mutation: PendingMutation): void {
        if (mutation.timeout === null) return;
        clearTimeout(mutation.timeout);
        mutation.timeout = null;
    }

    function takePendingMutation(mutId: MutId): PendingMutation | undefined {
        const mutation = pending.get(mutId);
        if (!mutation) return undefined;
        pending.delete(mutId);
        clearMutationTimeout(mutation);
        return mutation;
    }

    function failSession(
        code: CdbErrorCode,
        message: string,
        reason: ChardbClientSessionFailureReason,
        subState: TerminalSubState = "error"
    ): void {
        if (terminated) return;
        terminated = true;
        state = "closed";
        if (reconnectTimer !== null) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
        if (resumeExpiryTimer !== null) {
            clearTimeout(resumeExpiryTimer);
            resumeExpiryTimer = null;
        }
        clearAuthRefreshTimer();
        clearAuthRefreshDeadline();
        pendingAuthRefresh = null;
        resumeExpiryCookie = undefined;
        resumeRetainedSubs.clear();
        if (subState === "error") {
            try {
                opts.onSessionError?.(Object.freeze({ code, reason }));
            } catch {
                // Diagnostic listeners cannot interrupt terminal resource cleanup.
            }
        }
        const subscriptions = [...subs.values()];
        subs.clear();
        for (const sub of subscriptions) {
            clearSubscriptionRetryTimer(sub);
            sub.state = subState;
            sub.rows = [];
            for (const listener of sub.listeners) {
                try {
                    listener(cloneRawJson(sub.rows) as RawJson[], sub.state);
                } catch {
                    // User listeners cannot interrupt terminal resource cleanup.
                }
            }
        }
        const mutations = [...pending.values()];
        pending.clear();
        const error = new CdbError({ code, message });
        for (const mutation of mutations) {
            clearMutationTimeout(mutation);
            mutation.reject(error);
        }
        ws?.close();
    }

    function notify(sub: SubRecord): void {
        for (const fn of sub.listeners) {
            try {
                fn(cloneRawJson(sub.rows) as RawJson[], sub.state);
            } catch {
                // Subscription listeners do not own session liveness.
            }
        }
    }

    function subscribe<TRow = RawJson>(
        ref: string,
        args: RawJson,
        onChange: (rows: TRow[], state?: SubState) => void
    ): { unsubscribe: () => void } {
        if (terminated) {
            throw new CdbError({
                code: "CDB_STREAM_ABORTED",
                message: "cannot open a subscription after the CharDB client has closed",
            });
        }
        const queryRef = ChardbRef(ref);
        const ownedArgs = snapshotSubscriptionArguments(args);
        if (subs.size >= MAX_ACTIVE_SUBSCRIPTIONS) {
            throw new CdbError({
                code: "CDB_RATE_LIMITED",
                message: `cannot open more than ${MAX_ACTIVE_SUBSCRIPTIONS} active subscriptions`,
            });
        }
        assertAggregateQueryState(new Map(), { rows: [] }, "CDB_RATE_LIMITED");
        const subId = SubId(nextSubId++);
        const widenedListener: (rows: RawJson[], state: SubState) => void = (rows, state) =>
            onChange(rows as readonly RawJson[] as TRow[], state);
        const rec: SubRecord = {
            subId,
            ref: queryRef,
            args: ownedArgs,
            state: "pending",
            rows: [],
            listeners: new Set([widenedListener]),
            lastSnapshotCookie: undefined,
            retryTimer: null,
            retryBackoffMs: SUBSCRIPTION_RETRY_INITIAL_BACKOFF_MS,
        };
        subs.set(subId, rec);
        if (autoStartOnOperation) start();
        if (ws && state === "open") {
            const up: Up = { t: "sub", subId, ref: queryRef, args: rec.args };
            try {
                ws.send(encodeWire(up));
            } catch (cause) {
                subs.delete(subId);
                throw new CdbError({
                    code: "CDB_STREAM_ABORTED",
                    message: `failed to send subscription ${subId}`,
                    cause,
                });
            }
        }
        return {
            unsubscribe() {
                if (!subs.delete(subId)) return;
                clearSubscriptionRetryTimer(rec);
                releaseResumeRetainedSub(subId, rec);
                if (ws && state === "open") {
                    const up: Up = { t: "unsub", subId };
                    try {
                        ws.send(encodeWire(up));
                    } catch (cause) {
                        failSession(
                            "CDB_STREAM_ABORTED",
                            `failed to send unsubscription ${subId}; client session closed`,
                            "unsubscribe-send"
                        );
                        throw new CdbError({
                            code: "CDB_STREAM_ABORTED",
                            message: `failed to send unsubscription ${subId}`,
                            cause,
                        });
                    }
                }
            },
        };
    }

    function mutate<TResult = RawJson>(ref: string, args: RawJson): Promise<TResult> {
        if (terminated) {
            return Promise.reject(
                new CdbError({
                    code: "CDB_STREAM_ABORTED",
                    message: "cannot issue a mutation after the CharDB client has closed",
                })
            );
        }
        let mutationRef: ChardbRef;
        let ownedArgs: RawJson;
        try {
            mutationRef = ChardbRef(ref);
            ownedArgs = snapshotMutationArguments(args);
        } catch (error) {
            return Promise.reject(error);
        }
        if (pending.size >= MAX_PENDING_MUTATIONS) {
            return Promise.reject(
                new CdbError({
                    code: "CDB_RATE_LIMITED",
                    message: `cannot queue more than ${MAX_PENDING_MUTATIONS} unsettled mutations`,
                })
            );
        }
        const mutId = MutId(uuidv7());
        return new Promise<TResult>((resolve, reject) => {
            const rec: PendingMutation = {
                mutId,
                ref: mutationRef,
                args: ownedArgs,
                resolve: resolve as (r: RawJson) => void,
                reject,
                inFlight: false,
                timeout: null,
            };
            pending.set(mutId, rec);
            rec.timeout = setTimeout(() => {
                if (pending.get(mutId) !== rec) return;
                pending.delete(mutId);
                rec.timeout = null;
                rec.reject(
                    new CdbError({
                        code: "CDB_MUTATION_OUTCOME_UNKNOWN",
                        message: `mutation ${mutId} timed out after ${mutationTimeoutMs}ms`,
                    })
                );
            }, mutationTimeoutMs);
            if (autoStartOnOperation) start();
            if (ws && state === "open") {
                const up: Up = { t: "mut", mutId, ref: rec.ref, args: rec.args };
                try {
                    ws.send(encodeWire(up));
                    rec.inFlight = true;
                } catch (cause) {
                    const failed = takePendingMutation(mutId);
                    failed?.reject(
                        new CdbError({
                            code: "CDB_STREAM_ABORTED",
                            message: `failed to send mutation ${mutId}`,
                            cause,
                        })
                    );
                }
            }
        });
    }

    function close(): void {
        failSession("CDB_STREAM_ABORTED", "CharDB client closed before pending work settled", "client-close", "closed");
    }

    const client: ChardbClient = {
        subscribe,
        mutate,
        close,
        get state() {
            return state;
        },
    };
    return { client, start };
}
