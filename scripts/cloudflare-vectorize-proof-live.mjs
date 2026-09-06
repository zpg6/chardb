import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

const SHA256 = /^[a-f0-9]{64}$/;
const MAX_TIMEOUT_MS = 10 * 60_000;
const QUERY_REF = "cloudflare-vectorize-proof/api.ts#searchVectorDocuments";
const QUERY = Object.assign(() => {}, { __chardbKind: "query", __chardbRef: QUERY_REF });
const CLIENT_SESSION_FAILURE_REASONS = new Set([
    "auth-refresh-read",
    "auth-refresh-invalid-token",
    "auth-refresh-principal-changed",
    "auth-refresh-expiry-not-extended",
    "auth-refresh-send",
    "connect",
    "subscription-retry-send",
    "reconnect-refetch-listener",
    "auth-refresh-close",
    "invalid-handshake-frame",
    "invalid-session-frame",
    "protocol-selection",
    "protocol-rejection",
    "auth-refresh-rejection",
    "session-rejection",
    "unsubscribe-send",
]);
const MUST_REFETCH_REASONS = new Set(["authChanged", "schemaChanged", "protocolMismatch", "lagged", "shardsChanged"]);

function check(condition, message, ErrorType = Error) {
    if (!condition) throw new ErrorType(message);
}

function text(value, label, maximum = 256) {
    check(
        typeof value === "string" && value.length > 0 && new TextEncoder().encode(value).byteLength <= maximum,
        `${label} is invalid`,
        TypeError
    );
    return value;
}

function positiveInteger(value, label, maximum = Number.MAX_SAFE_INTEGER) {
    check(Number.isSafeInteger(value) && value > 0 && value <= maximum, `${label} is invalid`, TypeError);
    return value;
}

function sha256(value) {
    return createHash("sha256").update(value).digest("hex");
}

function exactRows(value, expectedRowPk, expectedScore, label) {
    check(Array.isArray(value), `${label} must be an array`);
    if (expectedRowPk === null) {
        check(value.length === 0, `${label} must be empty`);
        return Object.freeze([]);
    }
    check(value.length === 1, `${label} must contain one row`);
    const row = value[0];
    check(row !== null && typeof row === "object" && !Array.isArray(row), `${label} row is invalid`);
    check(
        JSON.stringify(Object.keys(row).sort()) === JSON.stringify(["rowPk", "score"]),
        `${label} row fields are invalid`
    );
    check(row.rowPk === expectedRowPk, `${label} row identity drifted`);
    const score = Object.is(row.score, -0) ? 0 : row.score;
    check(
        typeof score === "number" && Number.isFinite(score) && (expectedScore === undefined || score === expectedScore),
        `${label} score drifted`
    );
    return Object.freeze([Object.freeze({ rowPk: expectedRowPk, score })]);
}

function frame(value, label) {
    check(typeof value === "string", `${label} must be text`);
    let parsed;
    try {
        parsed = JSON.parse(value);
    } catch {
        throw new TypeError(`${label} is not JSON`);
    }
    check(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed), `${label} is invalid`);
    return parsed;
}

function candidateUrl(value) {
    if (value instanceof URL) {
        check(value.protocol === "file:", "candidate client entry must be a file URL", TypeError);
        return value.href;
    }
    const path = text(value, "candidate client entry", 4_096);
    return path.startsWith("file:") ? new URL(path).href : pathToFileURL(path).href;
}

function websocketEndpoint(origin) {
    const url = new URL("/ws", origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return url.href;
}

export async function openCloudflareVectorizeProofLiveSubscription(input, dependencies = {}) {
    const timeoutMs = positiveInteger(input.timeoutMs, "live proof timeout", MAX_TIMEOUT_MS);
    const reconnectStabilityMs = positiveInteger(
        input.reconnectStabilityMs ?? 500,
        "live reconnect stability duration",
        10_000
    );
    const organizationId = text(input.organizationId, "live organization id", 128);
    const expectedRowPk = text(input.expectedRowPk, "live expected row id", 128);
    const expectedPendingFallbackRowPk = text(
        input.expectedPendingFallbackRowPk,
        "live expected pending fallback row id",
        128
    );
    check(expectedPendingFallbackRowPk !== expectedRowPk, "live pending fallback must differ from the live row");
    const clientId = text(input.clientId, "live client id", 128);
    const jwt = text(input.jwt, "live Better Auth JWT", 16 * 1024);
    const readJwt = input.getJwt ?? (async () => jwt);
    check(typeof readJwt === "function", "live Better Auth JWT reader is invalid", TypeError);
    check(
        Array.isArray(input.values) && input.values.length === 32 && input.values.every(Number.isFinite),
        "live query values must contain 32 finite numbers",
        TypeError
    );
    const entry = candidateUrl(input.candidateEntry);
    const origin = new URL(input.origin);
    check(
        origin.username === "" &&
            origin.password === "" &&
            origin.pathname === "/" &&
            origin.search === "" &&
            origin.hash === "" &&
            (origin.protocol === "https:" ||
                (origin.protocol === "http:" && ["localhost", "127.0.0.1"].includes(origin.hostname))),
        "live proof origin is invalid",
        TypeError
    );
    const endpoint = websocketEndpoint(origin);
    const loadCandidate = dependencies.loadCandidate ?? (specifier => import(specifier));
    const NativeWebSocket = dependencies.WebSocket ?? globalThis.WebSocket;
    const now = dependencies.now ?? Date.now;
    const sleep = dependencies.sleep ?? (milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)));
    check(typeof NativeWebSocket === "function", "live proof requires a native WebSocket", TypeError);

    const sockets = [];
    const helloCookieHashes = [];
    const welcomeCookieHashes = [];
    const snapshotCookieHashes = [];
    const acknowledgedCookieHashes = [];
    const issuedJwtHashes = new Set();
    const outboundSubscriptionIds = new Set();
    let helloCount = 0;
    let welcomeCount = 0;
    let resumedHelloCount = 0;
    let resumedWelcomeCount = 0;
    let authReadCount = 0;
    let callbackCount = 0;
    let baselineUpdateCount = 0;
    let pendingFallbackUpdateCount = 0;
    let replacementUpdateCount = 0;
    let prematureCurrentUpdateCount = 0;
    let duplicateContentUpdateCount = 0;
    let reconnectRefetchStateCount = 0;
    let reconnectBaselineRestoreCount = 0;
    let reconnectBaselineRestoreAckCount = 0;
    let reconnectBaselineRestoreCookieSha256 = null;
    let baselineRows = null;
    let pendingRows = null;
    let replacementRows = null;
    let phase = "baseline";
    let failure = null;
    let closing = false;
    let restored = false;
    let client;
    let subscription;
    let subscriptionErrorDiagnostic = null;
    let knownErrorCodes = null;
    let candidateIsRetryable = null;
    let activeTransportDiagnostic = null;
    let lastInboundDiagnostic = null;
    let clientSessionErrorDiagnostic = null;
    let lastSocketEventDiagnostic = null;
    let lastMustRefetchDiagnostic = null;
    let reconnecting = false;

    const fail = error => {
        if (failure || closing) return;
        failure = error instanceof Error ? error : new Error(String(error));
    };

    const inspectOutbound = (raw, socketIndex) => {
        const message = frame(raw, "live outbound WebSocket frame");
        if (message.t === "hello") {
            helloCount++;
            const sentJwt = text(message.jwt, "live outbound Better Auth JWT", 16 * 1024);
            check(
                issuedJwtHashes.has(sha256(sentJwt)),
                "live WebSocket hello used credentials not issued by the proof"
            );
            const resume = message.resumeFromCookie;
            if (helloCount === 1) {
                check(resume === undefined, "initial live WebSocket hello unexpectedly resumed");
                helloCookieHashes.push(null);
            } else {
                check(typeof resume === "string" && resume.length > 0, "reconnect hello omitted its resume cookie");
                resumedHelloCount++;
                helloCookieHashes.push(sha256(resume));
            }
        } else if (message.t === "ack") {
            check(
                typeof message.cookie === "string" && message.cookie.length > 0,
                "live snapshot ack cookie is invalid"
            );
            const cookieSha256 = sha256(message.cookie);
            acknowledgedCookieHashes.push(cookieSha256);
            if (socketIndex === 2 && cookieSha256 === reconnectBaselineRestoreCookieSha256) {
                reconnectBaselineRestoreAckCount++;
            }
        } else if (message.t === "sub") {
            outboundSubscriptionIds.add(positiveInteger(message.subId, "live outbound subscription id"));
        }
    };

    const inspectInbound = (raw, socketIndex) => {
        const message = frame(raw, "live inbound WebSocket frame");
        const frameType =
            typeof message.t === "string" && message.t.length > 0 && message.t.length <= 64 ? message.t : "invalid";
        lastInboundDiagnostic = Object.freeze({ frameType, frameSha256: sha256(raw) });
        if (message.t === "welcome") {
            welcomeCount++;
            const cookie = message.resumedFromCookie ?? message.baseCookie;
            check(typeof cookie === "string" && cookie.length > 0, "live welcome cookie is invalid");
            welcomeCookieHashes.push(sha256(cookie));
            if (message.resumedFromCookie !== undefined) resumedWelcomeCount++;
        } else if (message.t === "snapshot") {
            check(typeof message.cookie === "string" && message.cookie.length > 0, "live snapshot cookie is invalid");
            snapshotCookieHashes.push(sha256(message.cookie));
        } else if (message.t === "error") {
            const subId = positiveInteger(message.subId, "live inbound subscription error id");
            if (!outboundSubscriptionIds.has(subId)) return;
            const code = text(message.code, "live inbound subscription error code", 64);
            check(knownErrorCodes?.has(code) === true, "live inbound subscription error code is invalid", TypeError);
            check(
                typeof message.retryable === "boolean",
                "live inbound subscription error retryable is invalid",
                TypeError
            );
            check(
                candidateIsRetryable?.(code) === message.retryable,
                "live inbound subscription error retryable polarity is invalid",
                TypeError
            );
            const correlationId = text(message.correlationId, "live inbound subscription error correlation id", 512);
            subscriptionErrorDiagnostic = Object.freeze({
                code,
                retryable: message.retryable,
                subId,
                correlationIdSha256: sha256(correlationId),
            });
        } else if (message.t === "mustRefetch") {
            const reason = text(message.reason, "live inbound refetch reason", 64);
            check(MUST_REFETCH_REASONS.has(reason), "live inbound refetch reason is invalid", TypeError);
            check(
                Array.isArray(message.subIds) &&
                    message.subIds.length <= 256 &&
                    message.subIds.every(subId => Number.isSafeInteger(subId) && subId > 0),
                "live inbound refetch subscription ids are invalid",
                TypeError
            );
            lastMustRefetchDiagnostic = Object.freeze({
                socketIndex,
                reason,
                subIds: Object.freeze([...message.subIds]),
            });
        }
    };

    class TrackingWebSocket {
        static CONNECTING = NativeWebSocket.CONNECTING;
        static OPEN = NativeWebSocket.OPEN;
        static CLOSING = NativeWebSocket.CLOSING;
        static CLOSED = NativeWebSocket.CLOSED;

        constructor(url) {
            const actual = new URL(url);
            const expected = new URL(endpoint);
            const queryEntries = [...actual.searchParams.entries()];
            check(
                queryEntries.length === 1 && queryEntries[0]?.[0] === "clientId" && queryEntries[0]?.[1] === clientId,
                "live SDK opened a WebSocket without its exact client identity"
            );
            actual.search = "";
            check(actual.href === expected.href, "live SDK opened an unexpected WebSocket endpoint");
            this.inner = new NativeWebSocket(url);
            sockets.push(this);
            this.socketIndex = sockets.length;
            lastSocketEventDiagnostic = Object.freeze({ event: "created", socketIndex: this.socketIndex });
        }

        get readyState() {
            return this.inner.readyState;
        }

        get onopen() {
            return this.inner.onopen;
        }

        set onopen(listener) {
            if (listener === null) {
                this.inner.onopen = null;
                return;
            }
            this.inner.onopen = event => {
                lastSocketEventDiagnostic = Object.freeze({ event: "open", socketIndex: this.socketIndex });
                listener.call(this.inner, event);
            };
        }

        get onmessage() {
            return this.inner.onmessage;
        }

        set onmessage(listener) {
            if (listener === null) {
                this.inner.onmessage = null;
                return;
            }
            this.inner.onmessage = event => {
                lastSocketEventDiagnostic = Object.freeze({ event: "inbound-frame", socketIndex: this.socketIndex });
                activeTransportDiagnostic = Object.freeze({
                    source: "inbound-frame",
                    socketIndex: this.socketIndex,
                });
                try {
                    inspectInbound(event.data, this.socketIndex);
                } catch (error) {
                    fail(error);
                }
                try {
                    listener.call(this.inner, event);
                } finally {
                    activeTransportDiagnostic = null;
                }
            };
        }

        get onclose() {
            return this.inner.onclose;
        }

        set onclose(listener) {
            if (listener === null) {
                this.inner.onclose = null;
                return;
            }
            this.inner.onclose = event => {
                lastSocketEventDiagnostic = Object.freeze({ event: "close", socketIndex: this.socketIndex });
                activeTransportDiagnostic = Object.freeze({
                    source: "websocket-close",
                    socketIndex: this.socketIndex,
                    code: Number.isSafeInteger(event.code) ? event.code : 0,
                    wasClean: event.wasClean === true,
                    ...(typeof event.reason === "string" && event.reason.length > 0
                        ? { reasonSha256: sha256(event.reason) }
                        : {}),
                });
                try {
                    listener.call(this.inner, event);
                } finally {
                    activeTransportDiagnostic = null;
                }
            };
        }

        get onerror() {
            return this.inner.onerror;
        }

        set onerror(listener) {
            if (listener === null) {
                this.inner.onerror = null;
                return;
            }
            this.inner.onerror = event => {
                lastSocketEventDiagnostic = Object.freeze({ event: "error", socketIndex: this.socketIndex });
                activeTransportDiagnostic = Object.freeze({
                    source: "websocket-error",
                    socketIndex: this.socketIndex,
                });
                try {
                    listener.call(this.inner, event);
                } finally {
                    activeTransportDiagnostic = null;
                }
            };
        }

        send(data) {
            try {
                inspectOutbound(data, this.socketIndex);
            } catch (error) {
                fail(error);
                throw error;
            }
            this.inner.send(data);
        }

        close(code, reason) {
            this.inner.close(code, reason);
        }

        forceReconnect() {
            this.inner.close(1012, "proof reconnect");
        }
    }

    const restoreWebSocket = () => {
        if (restored) return;
        restored = true;
        if (globalThis.WebSocket === TrackingWebSocket) globalThis.WebSocket = NativeWebSocket;
    };

    const healthy = () => {
        if (failure) throw failure;
        check(client?.state !== "closed", "live SDK closed before proof completion");
    };

    const waitFor = async (predicate, label) => {
        const started = now();
        while (now() - started <= timeoutMs) {
            healthy();
            if (predicate()) return now() - started;
            await sleep(25);
        }
        throw new Error(`${label} timed out after ${timeoutMs}ms`);
    };

    const onChange = (rows, state) => {
        if (closing) return;
        try {
            if (state === "error") {
                if (subscriptionErrorDiagnostic === null) {
                    if (clientSessionErrorDiagnostic !== null) {
                        const trigger = activeTransportDiagnostic ?? Object.freeze({ source: "session-local" });
                        const diagnostic = {
                            source: "client-session",
                            ...clientSessionErrorDiagnostic,
                            trigger: trigger.source,
                            ...Object.fromEntries(Object.entries(trigger).filter(([key]) => key !== "source")),
                            ...(trigger.source === "session-local" && lastSocketEventDiagnostic !== null
                                ? {
                                      lastSocketEvent: lastSocketEventDiagnostic.event,
                                      lastSocketIndex: lastSocketEventDiagnostic.socketIndex,
                                  }
                                : {}),
                            ...(trigger.source === "inbound-frame" && lastInboundDiagnostic !== null
                                ? lastInboundDiagnostic
                                : {}),
                        };
                        throw new Error(`live SDK emitted error subscription state ${JSON.stringify(diagnostic)}`);
                    }
                    const transport = activeTransportDiagnostic ?? Object.freeze({ source: "session-local" });
                    const diagnostic = {
                        ...transport,
                        clientState: client?.state ?? "unknown",
                        ...(transport.source === "inbound-frame" && lastInboundDiagnostic !== null
                            ? lastInboundDiagnostic
                            : {}),
                    };
                    throw new Error(`live SDK emitted error subscription state ${JSON.stringify(diagnostic)}`);
                }
                throw new Error(
                    `live SDK emitted error subscription state ${JSON.stringify(subscriptionErrorDiagnostic)}`
                );
            }
            if (state === "refetching") {
                const expectedSubId = outboundSubscriptionIds.size === 1 ? [...outboundSubscriptionIds][0] : null;
                const diagnostic = {
                    source: activeTransportDiagnostic?.source ?? "session-local",
                    frameType: lastInboundDiagnostic?.frameType ?? "none",
                    socketIndex: lastMustRefetchDiagnostic?.socketIndex ?? null,
                    reason: lastMustRefetchDiagnostic?.reason ?? "none",
                    targeted:
                        expectedSubId !== null &&
                        lastMustRefetchDiagnostic?.subIds.length === 1 &&
                        lastMustRefetchDiagnostic.subIds[0] === expectedSubId,
                    reconnecting,
                    priorRefetchStateCount: reconnectRefetchStateCount,
                };
                check(
                    reconnecting &&
                        phase === "baseline" &&
                        reconnectRefetchStateCount === 0 &&
                        diagnostic.source === "inbound-frame" &&
                        diagnostic.frameType === "mustRefetch" &&
                        diagnostic.socketIndex === 2 &&
                        diagnostic.reason === "lagged" &&
                        diagnostic.targeted,
                    `live SDK emitted unexpected refetching subscription state ${JSON.stringify(diagnostic)}`
                );
                exactRows(rows, null, null, "live reconnect refetch rows");
                reconnectRefetchStateCount = 1;
                return;
            }
            check(state === "live", `live SDK emitted unexpected ${String(state)} subscription state`);
            callbackCount++;
            if (phase === "baseline") {
                const exact = exactRows(rows, expectedRowPk, undefined, "live baseline rows");
                if (reconnecting) {
                    check(
                        reconnectRefetchStateCount === 1 && reconnectBaselineRestoreCount === 0,
                        "live SDK restored reconnect content without one lagged refetch"
                    );
                    check(
                        baselineRows !== null && JSON.stringify(exact) === JSON.stringify(baselineRows),
                        "live SDK reconnect refetch changed baseline content"
                    );
                    check(
                        activeTransportDiagnostic?.source === "inbound-frame" &&
                            activeTransportDiagnostic.socketIndex === 2 &&
                            lastInboundDiagnostic?.frameType === "snapshot",
                        "live SDK reconnect restoration was not delivered by the resumed socket"
                    );
                    reconnectBaselineRestoreCookieSha256 = snapshotCookieHashes.at(-1) ?? null;
                    check(
                        SHA256.test(reconnectBaselineRestoreCookieSha256 ?? ""),
                        "live SDK reconnect restoration cookie is missing"
                    );
                    reconnectBaselineRestoreCount = 1;
                    return;
                }
                if (baselineUpdateCount !== 0) {
                    duplicateContentUpdateCount++;
                    throw new Error("live SDK repeated baseline content");
                }
                baselineUpdateCount = 1;
                baselineRows = exact;
                return;
            }
            if (phase === "pending") {
                if (Array.isArray(rows) && rows.some(row => row?.rowPk === expectedRowPk)) {
                    prematureCurrentUpdateCount++;
                    throw new Error("live SDK emitted the replacement before provider readiness");
                }
                const exact = exactRows(rows, expectedPendingFallbackRowPk, undefined, "live pending fallback rows");
                if (pendingFallbackUpdateCount !== 0) {
                    duplicateContentUpdateCount++;
                    throw new Error("live SDK repeated pending fallback content");
                }
                pendingFallbackUpdateCount = 1;
                pendingRows = exact;
                return;
            }
            if (phase === "current") {
                const exact = exactRows(rows, expectedRowPk, undefined, "live replacement rows");
                check(
                    baselineRows !== null && JSON.stringify(exact) !== JSON.stringify(baselineRows),
                    "live replacement content did not change"
                );
                if (replacementUpdateCount !== 0) {
                    duplicateContentUpdateCount++;
                    throw new Error("live SDK repeated replacement content");
                }
                replacementUpdateCount = 1;
                replacementRows = exact;
                return;
            }
            throw new Error("live SDK emitted content outside an active proof phase");
        } catch (error) {
            fail(error);
        }
    };

    const assertPendingState = () => {
        healthy();
        check(
            phase === "pending" &&
                pendingFallbackUpdateCount === 1 &&
                prematureCurrentUpdateCount === 0 &&
                replacementUpdateCount === 0 &&
                duplicateContentUpdateCount === 0,
            "live replacement became visible while provider delivery was pending"
        );
    };

    try {
        globalThis.WebSocket = TrackingWebSocket;
        const candidate = await loadCandidate(entry);
        check(
            candidate !== null && typeof candidate === "object" && typeof candidate.createChardbClient === "function",
            "installed candidate does not export createChardbClient"
        );
        check(
            Array.isArray(candidate.CDB_ERROR_CODES) &&
                candidate.CDB_ERROR_CODES.every(code => typeof code === "string"),
            "installed candidate does not export its error-code contract"
        );
        check(
            typeof candidate.isRetryable === "function",
            "installed candidate does not export retryable error metadata"
        );
        knownErrorCodes = new Set(candidate.CDB_ERROR_CODES);
        candidateIsRetryable = candidate.isRetryable;
        client = candidate.createChardbClient({
            endpoint,
            clientId,
            mutationTimeoutMs: timeoutMs,
            getJwt: async () => {
                authReadCount++;
                const nextJwt = text(await readJwt(), "live Better Auth JWT refresh", 16 * 1024);
                issuedJwtHashes.add(sha256(nextJwt));
                return nextJwt;
            },
            onSessionError: diagnostic => {
                try {
                    check(
                        diagnostic !== null && typeof diagnostic === "object" && !Array.isArray(diagnostic),
                        "installed client session diagnostic is invalid",
                        TypeError
                    );
                    check(
                        JSON.stringify(Object.keys(diagnostic).sort()) === JSON.stringify(["code", "reason"]),
                        "installed client session diagnostic fields are invalid",
                        TypeError
                    );
                    const code = text(diagnostic.code, "installed client session diagnostic code", 64);
                    check(knownErrorCodes?.has(code) === true, "installed client session diagnostic code is invalid");
                    const reason = text(diagnostic.reason, "installed client session diagnostic reason", 64);
                    check(
                        CLIENT_SESSION_FAILURE_REASONS.has(reason),
                        "installed client session diagnostic reason is invalid"
                    );
                    clientSessionErrorDiagnostic = Object.freeze({ code, reason });
                } catch (error) {
                    fail(error);
                }
            },
        });
        subscription = client.subscribe(QUERY, { organizationId, values: [...input.values], limit: 1 }, onChange);
        await waitFor(
            () => baselineUpdateCount === 1 && acknowledgedCookieHashes.length >= 1 && client.state === "open",
            "initial live SDK snapshot"
        );
    } catch (error) {
        closing = true;
        try {
            subscription?.unsubscribe();
        } catch {
            // The primary setup failure is more useful.
        }
        try {
            client?.close();
        } finally {
            restoreWebSocket();
        }
        throw error;
    }

    return Object.freeze({
        async reconnect() {
            healthy();
            check(phase === "baseline" && sockets.length === 1, "live SDK reconnect is out of order");
            const callbacksBefore = callbackCount;
            reconnecting = true;
            sockets[0].forceReconnect();
            await waitFor(
                () =>
                    sockets.length === 2 &&
                    helloCount === 2 &&
                    welcomeCount === 2 &&
                    resumedHelloCount === 1 &&
                    resumedWelcomeCount === 1 &&
                    client.state === "open",
                "live SDK cookie resume"
            );
            await waitFor(
                () =>
                    reconnectRefetchStateCount === 1 &&
                    reconnectBaselineRestoreCount === 1 &&
                    reconnectBaselineRestoreAckCount === 1,
                "live SDK lagged reconnect refetch"
            );
            await sleep(reconnectStabilityMs);
            healthy();
            reconnecting = false;
            check(callbackCount === callbacksBefore + 1, "live SDK reconnect callback count drifted");
            check(reconnectBaselineRestoreAckCount === 1, "live SDK reconnect restoration acknowledgement drifted");
            const initialAck = acknowledgedCookieHashes[0];
            check(SHA256.test(initialAck ?? ""), "initial live snapshot acknowledgement is missing");
            check(helloCookieHashes[1] === initialAck, "live reconnect hello used a different cookie");
            check(welcomeCookieHashes[1] === initialAck, "live reconnect welcome resumed a different cookie");
            return Object.freeze({ recovery: "lagged-refetch" });
        },
        beginReplacement() {
            healthy();
            check(phase === "baseline" && baselineUpdateCount === 1, "live replacement began out of order");
            phase = "pending";
        },
        async waitForPending() {
            const elapsedMs = await waitFor(() => pendingFallbackUpdateCount === 1, "pending live vector snapshot");
            return Object.freeze({ elapsedMs });
        },
        assertPending() {
            assertPendingState();
        },
        allowCurrent() {
            assertPendingState();
            phase = "current";
        },
        async waitForCurrent() {
            const elapsedMs = await waitFor(() => replacementUpdateCount === 1, "ready live vector snapshot");
            await sleep(reconnectStabilityMs);
            healthy();
            check(
                replacementUpdateCount === 1 && duplicateContentUpdateCount === 0,
                "live SDK repeated replacement content"
            );
            return Object.freeze({ elapsedMs: elapsedMs + reconnectStabilityMs });
        },
        finish() {
            healthy();
            check(phase === "current" && replacementUpdateCount === 1, "live SDK proof is incomplete");
            check(sockets.length === 2, "live SDK did not use exactly two WebSocket connections");
            check(helloCount === 2 && welcomeCount === 2, "live SDK handshake counts drifted");
            check(resumedHelloCount === 1 && resumedWelcomeCount === 1, "live SDK resume evidence is incomplete");
            check(authReadCount >= 2, "live SDK did not reacquire Better Auth credentials on reconnect");
            check(
                snapshotCookieHashes.length >= 3 && snapshotCookieHashes.length <= 4,
                "live snapshot count is invalid"
            );
            check(
                acknowledgedCookieHashes.length === snapshotCookieHashes.length,
                "live SDK did not acknowledge every snapshot"
            );
            for (const [index, cookieHash] of snapshotCookieHashes.entries()) {
                check(acknowledgedCookieHashes[index] === cookieHash, `live snapshot ${index} acknowledgement drifted`);
            }
            check(callbackCount === 4, "live SDK content callback count drifted");
            check(
                baselineUpdateCount === 1 && pendingFallbackUpdateCount === 1 && replacementUpdateCount === 1,
                "live SDK content transition counts drifted"
            );
            check(
                baselineRows !== null && pendingRows !== null && replacementRows !== null,
                "live SDK row evidence is missing"
            );
            check(
                prematureCurrentUpdateCount === 0 && duplicateContentUpdateCount === 0,
                "live SDK emitted premature or duplicate content"
            );
            const initialCookieSha256 = acknowledgedCookieHashes[0];
            const finalCookieSha256 = acknowledgedCookieHashes.at(-1);
            check(SHA256.test(initialCookieSha256 ?? ""), "initial live cookie digest is invalid");
            check(SHA256.test(finalCookieSha256 ?? ""), "final live cookie digest is invalid");
            check(initialCookieSha256 !== finalCookieSha256, "live cookie did not advance after replacement");
            closing = true;
            try {
                subscription.unsubscribe();
            } finally {
                try {
                    client.close();
                } finally {
                    restoreWebSocket();
                }
            }
            return Object.freeze({
                sdk: "installed-candidate-createChardbClient",
                transport: "worker-websocket",
                auth: "better-auth-jwt",
                queryRefSha256: sha256(QUERY_REF),
                clientIdSha256: sha256(clientId),
                connectionCount: sockets.length,
                helloCount,
                welcomeCount,
                reconnectCount: sockets.length - 1,
                authReadCount,
                snapshotCount: snapshotCookieHashes.length,
                acknowledgementCount: acknowledgedCookieHashes.length,
                acknowledgementEverySnapshot: true,
                resume: Object.freeze({
                    attempted: true,
                    helloResumeMatchedInitialAck: true,
                    welcomeResumeMatchedInitialAck: true,
                    recovery: "lagged-refetch",
                    refetchReason: "lagged",
                    refetchStateCount: reconnectRefetchStateCount,
                    baselineRestoreCount: reconnectBaselineRestoreCount,
                    baselineRestoredExactly: true,
                    baselineRestoreAcknowledged: reconnectBaselineRestoreAckCount === 1,
                    initialCookieSha256,
                    finalCookieSha256,
                }),
                content: Object.freeze({
                    callbackCount,
                    baselineUpdateCount,
                    pendingFallbackUpdateCount,
                    prematureCurrentUpdateCount,
                    replacementUpdateCount,
                    duplicateContentUpdateCount,
                    baselineRowsSha256: sha256(JSON.stringify(baselineRows)),
                    pendingFallbackRowPkSha256: sha256(expectedPendingFallbackRowPk),
                    pendingRowsSha256: sha256(JSON.stringify(pendingRows)),
                    replacementRowsSha256: sha256(JSON.stringify(replacementRows)),
                }),
            });
        },
        abort() {
            if (closing) return;
            closing = true;
            try {
                subscription.unsubscribe();
            } finally {
                try {
                    client.close();
                } finally {
                    restoreWebSocket();
                }
            }
        },
    });
}
