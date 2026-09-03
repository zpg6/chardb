import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { type JWK, SignJWT, exportJWK, generateKeyPair } from "jose";
import { Miniflare } from "miniflare";
import { build as viteBuild } from "vite";
import { disposeMiniflareBounded } from "../../scripts/miniflare-lifecycle.mjs";
import { ChardbRef, ClientId, Cookie, MutId, SubId } from "../../src/types.ts";
import { chardb as chardbVite } from "../../src/vite/index.ts";
import { type Down, PROTOCOL_V, type Up, decodeWire, encodeWire } from "../../src/wire.ts";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ENTRY = path.join(HERE, "gateway-jwt.entry.ts");
const BUNDLE = path.join(process.env.TMPDIR ?? "/tmp", `chardb-gateway-jwt-${process.pid}.bundle.mjs`);
const KID = "gateway-workerd-key";
const ROTATED_KID = "gateway-workerd-key-rotated";
const UNKNOWN_KID = "gateway-workerd-key-unknown";
const ISSUER = "https://issuer.example";
const AUDIENCE = "chardb-workerd";
const JWKS_URL = "https://unreachable.invalid/jwks";
const WRITE_REF = "test/workerd/gateway-jwt.entry.ts#writeOrganizationRow";
const CLOSED_REF = "test/workerd/gateway-jwt.entry.ts#closedOrganizationWrite";
const LIST_REF = "test/workerd/gateway-jwt.entry.ts#listOrganizationRows";

let mf: Miniflare | undefined;
let workerdUrl: URL | undefined;
let signToken: ((claims?: TokenOverrides) => Promise<string>) | undefined;
let signRotatedToken: ((claims?: TokenOverrides) => Promise<string>) | undefined;
let signUnknownToken: ((claims?: TokenOverrides) => Promise<string>) | undefined;
let rotatedPublicJwk: JWK | undefined;
let mutationRef: ChardbRef | undefined;
let closedMutationRef: ChardbRef | undefined;
let queryRef: ChardbRef | undefined;
let unconstrainedQueryRef: ChardbRef | undefined;
let invalidQueryRef: ChardbRef | undefined;
let shardId: string | undefined;

interface TokenOverrides {
    readonly subject?: string;
    readonly issuer?: string;
    readonly audience?: string;
    readonly expirationTime?: number;
}

const JWKS_WORKER = `
let keys;
let fetchCount = 0;
let lastUrl = null;
let mode = "ok";

export default {
    async fetch(request, env) {
        keys ??= JSON.parse(env.INITIAL_JWKS).keys;
        const url = new URL(request.url);
        if (url.pathname === "/jwks") {
            fetchCount += 1;
            lastUrl = request.url;
            if (mode === "throw") throw new Error("forced JWKS network failure");
            if (mode === "status") return new Response("unavailable", { status: 503 });
            if (mode === "invalid-json") return new Response("{");
            if (mode === "malformed") return Response.json({ keys: [null] });
            if (mode === "empty") return Response.json({ keys: [] });
            return Response.json({ keys });
        }
        if (url.pathname === "/__control/rotate" && request.method === "POST") {
            const document = await request.json();
            keys = document.keys;
            return Response.json({ ok: true });
        }
        if (url.pathname === "/__control/mode" && request.method === "POST") {
            const body = await request.json();
            mode = body.mode;
            return Response.json({ ok: true });
        }
        if (url.pathname === "/__control/stats") {
            return Response.json({ fetchCount, lastUrl, kids: keys.map(key => key.kid) });
        }
        return new Response("not found", { status: 404 });
    }
};
`;

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
        // Bun retains optional Node-only Kysely/sqlite loaders even though this
        // Worker never reaches them. workerd rejects arbitrary dynamic module
        // specifiers while parsing, so make those dead branches fail explicitly.
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

async function buildBrowserRefs(): Promise<readonly [ChardbRef, ChardbRef, ChardbRef]> {
    const fixture = await mkdtemp(path.join(tmpdir(), "chardb-vite-browser-"));
    const mutationEntry = path.join(fixture, "mutations.ts");
    const queryEntry = path.join(fixture, "queries.ts");
    const serverModule = path.join(HERE, "../../src/server/define.ts");
    try {
        await writeFile(
            mutationEntry,
            `
import { api } from "@chardb/core/server";
export const writeOrganizationRow = api.mutation({ ref: "${WRITE_REF}", handler: () => null });
export const closedOrganizationWrite = api.mutation({ ref: "${CLOSED_REF}", handler: () => null });
`
        );
        await writeFile(
            queryEntry,
            `
import { api } from "@chardb/core/server";
export const listOrganizationRows = api.query({
  ref: "${LIST_REF}",
  query: db => db.select().from(organizationRows),
});
`
        );
        const buildEntry = async (entry: string, emittedName: string): Promise<Record<string, unknown>> => {
            const built = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardbVite()],
                resolve: { alias: { "@chardb/core/server": serverModule } },
                build: { write: false, lib: { entry, formats: ["es"] } },
            });
            const results = (Array.isArray(built) ? built : [built]) as unknown as readonly {
                readonly output: readonly { readonly type: string; readonly code?: string }[];
            }[];
            const chunk = results.flatMap(result => result.output).find(output => output.type === "chunk");
            if (!chunk?.code) throw new Error("Vite did not emit the browser client chunk");
            const emittedPath = path.join(fixture, emittedName);
            await writeFile(emittedPath, chunk.code);
            return (await import(pathToFileURL(emittedPath).href)) as Record<string, unknown>;
        };
        const mutations = (await buildEntry(mutationEntry, "browser-mutations.mjs")) as {
            readonly writeOrganizationRow: { readonly __chardbRef?: unknown };
            readonly closedOrganizationWrite: { readonly __chardbRef?: unknown };
        };
        const queries = (await buildEntry(queryEntry, "browser-queries.mjs")) as {
            readonly listOrganizationRows: { readonly __chardbRef?: unknown };
        };
        return [
            ChardbRef(String(mutations.writeOrganizationRow.__chardbRef)),
            ChardbRef(String(mutations.closedOrganizationWrite.__chardbRef)),
            ChardbRef(String(queries.listOrganizationRows.__chardbRef)),
        ];
    } finally {
        await rm(fixture, { recursive: true, force: true });
    }
}

beforeAll(async () => {
    [mutationRef, closedMutationRef, queryRef] = await buildBrowserRefs();
    expect([mutationRef, closedMutationRef, queryRef]).toEqual([
        ChardbRef(WRITE_REF),
        ChardbRef(CLOSED_REF),
        ChardbRef(LIST_REF),
    ]);

    const initialKeyPair = await generateKeyPair("ES256");
    const rotatedKeyPair = await generateKeyPair("ES256");
    const unknownKeyPair = await generateKeyPair("ES256");
    const publicJwk = { ...(await exportJWK(initialKeyPair.publicKey)), kid: KID, alg: "ES256", use: "sig" };
    rotatedPublicJwk = {
        ...(await exportJWK(rotatedKeyPair.publicKey)),
        kid: ROTATED_KID,
        alg: "ES256",
        use: "sig",
    };
    const createSigner =
        (privateKey: CryptoKey, kid: string) =>
        async (overrides: TokenOverrides = {}) => {
            const now = Math.floor(Date.now() / 1000);
            return new SignJWT({ probe: "workerd", tenantId: "workerd-org-b", role: "owner", roles: ["owner"] })
                .setProtectedHeader({ alg: "ES256", kid })
                .setSubject(overrides.subject ?? "workerd-user")
                .setIssuer(overrides.issuer ?? ISSUER)
                .setAudience(overrides.audience ?? AUDIENCE)
                .setIssuedAt(now)
                .setExpirationTime(overrides.expirationTime ?? now + 300)
                .sign(privateKey);
        };
    signToken = createSigner(initialKeyPair.privateKey, KID);
    signRotatedToken = createSigner(rotatedKeyPair.privateKey, ROTATED_KID);
    signUnknownToken = createSigner(unknownKeyPair.privateKey, UNKNOWN_KID);

    mf = new Miniflare({
        workers: [
            {
                name: "gateway-jwt",
                modules: true,
                script: await buildWorker(),
                outboundService: "jwks",
                durableObjects: {
                    CDB_CATALOG: { className: "Catalog", useSQLite: true },
                    CDB_GATEWAY: { className: "Gateway", useSQLite: true },
                    CDB_SHARD: { className: "Cdb", useSQLite: true },
                    CDB_RESHARD: { className: "Resharder", useSQLite: true },
                },
                compatibilityDate: "2025-09-01",
                compatibilityFlags: ["nodejs_compat"],
            },
            {
                name: "jwks",
                modules: true,
                script: JWKS_WORKER,
                bindings: { INITIAL_JWKS: JSON.stringify({ keys: [publicJwk] }) },
                compatibilityDate: "2025-09-01",
            },
        ],
    });
    workerdUrl = await mf.ready;
    const seeded = await mf.dispatchFetch("http://example.com/seed", {
        method: "POST",
    });
    if (!seeded.ok) throw new Error(`failed to seed Catalog state: ${seeded.status} ${await seeded.text()}`);
    const seedResult = (await seeded.json()) as {
        mutationRef: ChardbRef;
        closedMutationRef: ChardbRef;
        queryRef: ChardbRef;
        unconstrainedQueryRef: ChardbRef;
        invalidQueryRef: ChardbRef;
        shardA: string;
        shardB: string;
    };
    ({ unconstrainedQueryRef, invalidQueryRef, shardA: shardId } = seedResult);
    expect(seedResult).toMatchObject({ mutationRef, closedMutationRef, queryRef });
    expect(seedResult.shardA).toBe(seedResult.shardB);
});

afterAll(async () => {
    await disposeMiniflareBounded(mf, { label: "Gateway JWT fixture final teardown" });
    mf = undefined;
});

interface OpenedSocket {
    readonly socket: WebSocket;
    readonly first: Promise<Down>;
    readonly closed: Promise<CloseEvent>;
}

async function openSocket(
    jwt: string,
    options: {
        readonly clientId?: string;
        readonly routedClientId?: string | null;
        readonly resumeFromCookie?: string;
        readonly protocolV?: number;
    } = {}
): Promise<OpenedSocket> {
    if (!workerdUrl) throw new Error("miniflare not initialized");
    const clientId = options.clientId ?? crypto.randomUUID();
    const url = new URL("/ws", workerdUrl);
    const routedClientId = options.routedClientId === undefined ? clientId : options.routedClientId;
    if (routedClientId !== null) url.searchParams.set("clientId", routedClientId);
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
    const first = nextDown(socket);
    const closed = new Promise<CloseEvent>(resolve => socket.addEventListener("close", resolve, { once: true }));
    const hello: Up = {
        t: "hello",
        protocolV: PROTOCOL_V,
        clientId: ClientId(clientId),
        ...(options.resumeFromCookie ? { resumeFromCookie: Cookie(options.resumeFromCookie) } : {}),
        jwt,
    };
    socket.send(
        options.protocolV === undefined ? encodeWire(hello) : JSON.stringify({ ...hello, protocolV: options.protocolV })
    );
    return { socket, first, closed };
}

function nextDown(socket: WebSocket): Promise<Down> {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("timed out waiting for Gateway message")), 2_000);
        const onClose = (event: CloseEvent) => {
            clearTimeout(timeout);
            reject(new Error(`Gateway closed before replying (${event.code}: ${event.reason})`));
        };
        socket.addEventListener("close", onClose, { once: true });
        socket.addEventListener(
            "message",
            event => {
                clearTimeout(timeout);
                socket.removeEventListener("close", onClose);
                resolve(decodeWire(String(event.data)) as Down);
            },
            { once: true }
        );
    });
}

async function signed(overrides?: TokenOverrides): Promise<string> {
    if (!signToken) throw new Error("signer not initialized");
    return signToken(overrides);
}

async function signedRotated(overrides?: TokenOverrides): Promise<string> {
    if (!signRotatedToken) throw new Error("rotated signer not initialized");
    return signRotatedToken(overrides);
}

async function signedUnknown(overrides?: TokenOverrides): Promise<string> {
    if (!signUnknownToken) throw new Error("unknown signer not initialized");
    return signUnknownToken(overrides);
}

function tamperJwtSignature(jwt: string): string {
    const parts = jwt.split(".");
    if (parts.length !== 3 || !parts[2]) throw new Error("expected a signed JWT");
    const signature = parts[2];
    // Mutate the first encoded signature character, which always carries data
    // bits. A tail rewrite can accidentally preserve the penultimate data
    // character and change only ignored padding bits in the final character.
    const first = signature[0] === "A" ? "B" : "A";
    return `${parts[0]}.${parts[1]}.${first}${signature.slice(1)}`;
}

async function jwksStats(): Promise<{
    readonly fetchCount: number;
    readonly lastUrl: string | null;
    readonly kids: string[];
}> {
    if (!mf) throw new Error("miniflare not initialized");
    const worker = await mf.getWorker("jwks");
    const response = await worker.fetch("http://jwks.test/__control/stats");
    if (!response.ok) throw new Error(`failed to read JWKS stats: ${response.status}`);
    return (await response.json()) as {
        readonly fetchCount: number;
        readonly lastUrl: string | null;
        readonly kids: string[];
    };
}

async function rotateJwks(): Promise<void> {
    if (!mf || !rotatedPublicJwk) throw new Error("rotated JWKS not initialized");
    const worker = await mf.getWorker("jwks");
    const response = await worker.fetch("http://jwks.test/__control/rotate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ keys: [rotatedPublicJwk] }),
    });
    if (!response.ok) throw new Error(`failed to rotate JWKS: ${response.status}`);
}

type JwksMode = "ok" | "throw" | "status" | "invalid-json" | "malformed" | "empty";

async function setJwksMode(mode: JwksMode): Promise<void> {
    if (!mf) throw new Error("miniflare not initialized");
    const worker = await mf.getWorker("jwks");
    const response = await worker.fetch("http://jwks.test/__control/mode", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ mode }),
    });
    if (!response.ok) throw new Error(`failed to set JWKS mode: ${response.status}`);
}

async function releaseJwksCooldown(): Promise<void> {
    if (!mf) throw new Error("miniflare not initialized");
    const response = await mf.dispatchFetch("http://example.com/release-jwks-cooldown", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jwksUrl: JWKS_URL }),
    });
    if (!response.ok) throw new Error(`failed to release JWKS cooldown: ${response.status}`);
}

async function expireCatalogJwk(kid: string): Promise<void> {
    if (!mf) throw new Error("miniflare not initialized");
    const response = await mf.dispatchFetch("http://example.com/expire-jwk", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kid, jwksUrl: JWKS_URL }),
    });
    if (!response.ok) throw new Error(`failed to expire Catalog JWK: ${response.status} ${await response.text()}`);
}

function sendAndReceive(socket: WebSocket, message: Up): Promise<Down> {
    const response = nextDown(socket);
    socket.send(encodeWire(message));
    return response;
}

function nextDowns(socket: WebSocket, count: number): Promise<Down[]> {
    return new Promise((resolve, reject) => {
        const messages: Down[] = [];
        const timeout = setTimeout(
            () =>
                reject(
                    new Error(
                        `timed out waiting for ${count} Gateway messages after receiving ${JSON.stringify(messages)}`
                    )
                ),
            3_000
        );
        const onMessage = (event: MessageEvent) => {
            messages.push(decodeWire(String(event.data)) as Down);
            if (messages.length !== count) return;
            clearTimeout(timeout);
            socket.removeEventListener("message", onMessage);
            resolve(messages);
        };
        socket.addEventListener("message", onMessage);
    });
}

async function expectNoDown(socket: WebSocket): Promise<void> {
    let received = false;
    const onMessage = () => {
        received = true;
    };
    socket.addEventListener("message", onMessage);
    await new Promise(resolve => setTimeout(resolve, 100));
    socket.removeEventListener("message", onMessage);
    expect(received).toBe(false);
}

async function setAuthorityFault(
    fault: "none" | "throw" | "malformed" | "hold" | "hold-throw" | "route-throw" | "route-malformed" | "legacy-throw"
): Promise<void> {
    if (!mf) throw new Error("miniflare not initialized");
    const response = await mf.dispatchFetch("http://example.com/authority-fault", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ fault }),
    });
    if (!response.ok) throw new Error(`failed to set authority fault: ${response.status}`);
}

async function authorityControl(pathname: "/authority-waiting" | "/authority-release"): Promise<void> {
    if (!mf) throw new Error("miniflare not initialized");
    const response = await mf.dispatchFetch(`http://example.com${pathname}`, { method: "POST" });
    if (!response.ok) throw new Error(`authority control failed: ${pathname} ${response.status}`);
}

async function mutationResponseControl(
    pathname: "/mutation-response-hold" | "/mutation-response-waiting" | "/mutation-response-release",
    mutId?: string
): Promise<void> {
    if (!mf || !shardId) throw new Error("mutation response control is not initialized");
    const response = await mf.dispatchFetch(`http://example.com${pathname}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ shardId, ...(mutId ? { mutId } : {}) }),
    });
    if (!response.ok) throw new Error(`mutation response control failed: ${pathname} ${response.status}`);
}

describe("configured Gateway JWT handshake in real workerd", () => {
    test.skipIf(!process.env.CHARDB_RUST_CONFORMANCE_BIN)(
        "runs the Rust client through the real Gateway, Catalog, shard, and JWT verifier",
        async () => {
            if (!workerdUrl) throw new Error("miniflare not initialized");
            const executable = process.env.CHARDB_RUST_CONFORMANCE_BIN;
            if (!executable) throw new Error("Rust conformance binary is not configured");
            const endpoint = new URL("/ws", workerdUrl);
            endpoint.protocol = endpoint.protocol === "https:" ? "wss:" : "ws:";
            const rowId = `rust-conformance-${crypto.randomUUID()}`;
            const processResult = Bun.spawn([executable, endpoint.toString(), await signed(), rowId], {
                stdout: "pipe",
                stderr: "pipe",
            });
            const [exitCode, stdout, stderr] = await Promise.all([
                processResult.exited,
                new Response(processResult.stdout).text(),
                new Response(processResult.stderr).text(),
            ]);
            expect(exitCode, stderr).toBe(0);
            expect(JSON.parse(stdout)).toEqual({ ok: true, rowId });
        },
        15_000
    );

    test("fetches the configured JWKS URL once and reuses the Catalog cache", async () => {
        expect(await jwksStats()).toEqual({ fetchCount: 0, lastUrl: null, kids: [KID] });

        const first = await openSocket(await signed(), { clientId: "jwks-cold-fetch" });
        await expect(first.first).resolves.toMatchObject({ t: "welcome" });
        first.socket.close();
        await first.closed;

        const cached = await openSocket(await signed(), { clientId: "jwks-cached-fetch" });
        await expect(cached.first).resolves.toMatchObject({ t: "welcome" });
        cached.socket.close();
        await cached.closed;

        expect(await jwksStats()).toEqual({
            fetchCount: 1,
            lastUrl: JWKS_URL,
            kids: [KID],
        });
    });

    test("rejects a hello client id that differs from the Worker-routed client id", async () => {
        const { first, closed } = await openSocket(await signed(), {
            clientId: "hello-client",
            routedClientId: "routed-client",
        });
        await expect(first).resolves.toMatchObject({ t: "error", code: "CDB_FORBIDDEN" });
        expect(await closed).toMatchObject({ code: 1008, reason: "CDB_FORBIDDEN" });
    });

    test("rejects an unsupported protocol before JWT verification", async () => {
        const before = await jwksStats();
        const { first, closed } = await openSocket("not-a-jwt", {
            clientId: "protocol-mismatch",
            protocolV: PROTOCOL_V + 1,
        });

        await expect(first).resolves.toEqual({ t: "mustRefetch", subIds: [], reason: "protocolMismatch" });
        expect(await closed).toMatchObject({
            code: 1002,
            reason: `unsupported chardb protocol ${PROTOCOL_V + 1}`,
        });
        expect(await jwksStats()).toEqual(before);
    });

    test("a seeded Catalog membership permits the declared organization mutation", async () => {
        const clientId = "workerd-authorized-client";
        const { socket, first, closed } = await openSocket(await signed(), { clientId });
        await expect(first).resolves.toMatchObject({ t: "welcome", protocolV: PROTOCOL_V });
        if (!mutationRef) throw new Error("mutation ref was not seeded");

        const mutation = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("workerd-mut"),
            ref: mutationRef,
            args: { id: "workerd-row", organizationId: "workerd-org", body: "written", createdAt: 1 },
        });
        const exactMutation = structuredClone(mutation);
        expect(mutation).toMatchObject({
            t: "poke",
            mutResults: [
                {
                    mutId: "workerd-mut",
                    ok: true,
                    result: {
                        id: "workerd-row",
                        userId: "workerd-user",
                        tenantId: "workerd-org",
                        role: "member",
                        roles: ["member"],
                        authEpochs: {
                            global: expect.any(Number),
                            tenant: expect.any(Number),
                            principal: expect.any(Number),
                        },
                        claims: { userRole: "admin" },
                    },
                },
            ],
        });
        if (mutation.t !== "poke") throw new Error("expected mutation poke");
        if (exactMutation.t !== "poke") throw new Error("expected exact mutation poke");
        const mutationResult = exactMutation.mutResults?.[0];
        if (!mutationResult?.ok || typeof mutationResult.result !== "object" || mutationResult.result === null) {
            throw new Error("expected successful organization mutation result");
        }
        const authEpochs = (mutationResult.result as { readonly authEpochs?: Record<string, unknown> }).authEpochs;
        expect(authEpochs).toBeDefined();
        expect(Number(authEpochs?.global)).toBeGreaterThanOrEqual(0);
        expect(Number(authEpochs?.tenant)).toBeGreaterThan(0);
        expect(Number(authEpochs?.principal)).toBeGreaterThan(0);

        const orgAdmin = await openSocket(await signed({ subject: "workerd-user-2" }), {
            clientId: "workerd-org-admin-without-user-admin",
        });
        await expect(orgAdmin.first).resolves.toMatchObject({ t: "welcome" });
        const deniedUserAdminGrant = await sendAndReceive(orgAdmin.socket, {
            t: "mut",
            mutId: MutId("workerd-org-admin-user-admin-denied"),
            ref: mutationRef,
            args: {
                id: "workerd-org-admin-user-admin-denied",
                organizationId: "workerd-org",
                body: "must-not-write",
                createdAt: 2,
            },
        });
        expect(deniedUserAdminGrant).toMatchObject({
            t: "poke",
            mutResults: [{ ok: false, error: { code: "CDB_FORBIDDEN" } }],
        });
        orgAdmin.socket.close();
        await orgAdmin.closed;

        if (!closedMutationRef) throw new Error("closed mutation ref was not seeded");
        const undeclared = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("workerd-closed"),
            ref: closedMutationRef,
            args: { organizationId: "workerd-org" },
        });
        expect(undeclared).toMatchObject({
            t: "poke",
            cookie: mutation.cookie,
            mutResults: [{ mutId: "workerd-closed", ok: false, error: { code: "CDB_AUTH_NOT_BOUND" } }],
        });

        if (!unconstrainedQueryRef) throw new Error("unconstrained query ref was not seeded");
        const subscription = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(7),
            ref: unconstrainedQueryRef,
            args: { organizationId: "workerd-org" },
        });
        expect(subscription).toMatchObject({
            t: "error",
            code: "CDB_CROSS_PARTITION",
            subId: 7,
        });

        socket.close();
        await closed;

        const resumed = await openSocket(await signed(), { clientId, resumeFromCookie: mutation.cookie });
        await expect(resumed.first).resolves.toMatchObject({
            t: "welcome",
            resumedFromCookie: mutation.cookie,
        });
        resumed.socket.close();
    });

    test("organization dispatch does not call the legacy authority and route RPCs", async () => {
        if (!mutationRef || !queryRef) throw new Error("public refs were not seeded");
        const { socket, first } = await openSocket(await signed(), { clientId: "combined-catalog-rpc" });
        await first;
        await setAuthorityFault("legacy-throw");

        const mutation = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("combined-catalog-rpc-mutation"),
            ref: mutationRef,
            args: {
                id: "combined-catalog-rpc-row",
                organizationId: "workerd-org",
                body: "combined-catalog-rpc",
                createdAt: 8,
            },
        });
        expect(mutation).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });

        const snapshot = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(8),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "combined-catalog-rpc" },
        });
        expect(snapshot).toMatchObject({
            t: "snapshot",
            subId: 8,
            rows: [{ id: "combined-catalog-rpc-row", organizationId: "workerd-org" }],
        });
        if (snapshot.t !== "snapshot") throw new Error("expected combined Catalog RPC snapshot");
        socket.send(encodeWire({ t: "ack", cookie: snapshot.cookie }));

        const replacement = nextDowns(socket, 2);
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("combined-catalog-rpc-live"),
                ref: mutationRef,
                args: {
                    id: "combined-catalog-rpc-live-row",
                    organizationId: "workerd-org",
                    body: "combined-catalog-rpc",
                    createdAt: 9,
                },
            })
        );
        const replacementMessages = await replacement;
        expect(replacementMessages.find(message => message.t === "poke")).toMatchObject({
            t: "poke",
            mutResults: [{ ok: true }],
        });
        expect(replacementMessages.find(message => message.t === "snapshot")).toMatchObject({
            t: "snapshot",
            subId: 8,
            rows: expect.arrayContaining([
                expect.objectContaining({ id: "combined-catalog-rpc-row", organizationId: "workerd-org" }),
                expect.objectContaining({ id: "combined-catalog-rpc-live-row", organizationId: "workerd-org" }),
            ]),
        });

        await setAuthorityFault("none");
        socket.close();
    });

    test("organization queries return initial and empty snapshots while other tenants stay closed", async () => {
        if (!mutationRef || !queryRef) throw new Error("public refs were not seeded");
        const otherTenant = await openSocket(await signed({ subject: "workerd-user-b" }));
        await otherTenant.first;
        const otherSeeded = await sendAndReceive(otherTenant.socket, {
            t: "mut",
            mutId: MutId("query-seed-b"),
            ref: mutationRef,
            args: {
                id: "query-seed-row-b",
                organizationId: "workerd-org-b",
                body: "query-visible",
                createdAt: 9,
            },
        });
        expect(otherSeeded).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });
        const otherSnapshot = await sendAndReceive(otherTenant.socket, {
            t: "sub",
            subId: SubId(9),
            ref: queryRef,
            args: { organizationId: "workerd-org-b", body: "query-visible" },
        });
        expect(otherSnapshot).toMatchObject({
            t: "snapshot",
            subId: 9,
            rows: [
                {
                    id: "query-seed-row-b",
                    organizationId: "workerd-org-b",
                    authorId: "workerd-user-b",
                    body: "query-visible",
                    createdAt: 9,
                },
            ],
        });
        if (otherSnapshot.t !== "snapshot") throw new Error("expected org B snapshot");
        expect(otherSnapshot.rows).toHaveLength(1);
        otherTenant.socket.close();

        const { socket, first } = await openSocket(await signed());
        await first;

        const seeded = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("query-seed"),
            ref: mutationRef,
            args: {
                id: "query-seed-row",
                organizationId: "workerd-org",
                body: "query-visible",
                createdAt: 10,
            },
        });
        expect(seeded).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });

        const snapshot = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(10),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "query-visible" },
        });
        expect(snapshot).toMatchObject({
            t: "snapshot",
            subId: 10,
            rows: [
                {
                    id: "query-seed-row",
                    organizationId: "workerd-org",
                    authorId: "workerd-user",
                    body: "query-visible",
                    createdAt: 10,
                },
            ],
        });
        if (snapshot.t !== "snapshot") throw new Error("expected org A snapshot");
        expect(snapshot.rows).toHaveLength(1);

        const empty = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(11),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "does-not-exist" },
        });
        expect(empty).toMatchObject({ t: "snapshot", subId: 11, rows: [] });

        const forbidden = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(12),
            ref: queryRef,
            args: { organizationId: "workerd-org-b" },
        });
        expect(forbidden).toMatchObject({ t: "error", subId: 12, code: "CDB_FORBIDDEN" });
        socket.close();
    });

    test("organization queries reject unconstrained, cross-partition, and foreign-tenant plans", async () => {
        if (!unconstrainedQueryRef || !invalidQueryRef || !queryRef) throw new Error("query refs were not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;

        const unconstrained = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(20),
            ref: unconstrainedQueryRef,
            args: { organizationId: "workerd-org" },
        });
        expect(unconstrained).toMatchObject({ t: "error", subId: 20, code: "CDB_CROSS_PARTITION" });

        for (const [subId, mode, code] of [
            [21, "scatter", "CDB_CROSS_PARTITION"],
            [22, "cross", "CDB_CROSS_PARTITION"],
            [23, "foreign", "CDB_FORBIDDEN"],
        ] as const) {
            const rejected = await sendAndReceive(socket, {
                t: "sub",
                subId: SubId(subId),
                ref: invalidQueryRef,
                args: { organizationId: "workerd-org", mode },
            });
            expect(rejected).toMatchObject({ t: "error", subId, code });
        }
        for (const [subId, fault] of [
            [24, "route-malformed"],
            [25, "route-throw"],
        ] as const) {
            await setAuthorityFault(fault);
            const rejected = await sendAndReceive(socket, {
                t: "sub",
                subId: SubId(subId),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "does-not-exist" },
            });
            expect(rejected).toMatchObject({ t: "error", subId, code: "CDB_CATALOG_UNAVAILABLE" });
        }
        await setAuthorityFault("none");
        socket.close();
    });

    test("query failures and pending unsubscribe do not produce a snapshot", async () => {
        if (!queryRef) throw new Error("query ref was not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;

        const queryFailure = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(30),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "__throw" },
        });
        expect(queryFailure).toMatchObject({ t: "error", subId: 30 });

        await setAuthorityFault("hold-throw");
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(32),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "does-not-exist" },
            })
        );
        await authorityControl("/authority-waiting");
        socket.send(encodeWire({ t: "unsub", subId: SubId(32) }));
        await authorityControl("/authority-release");
        await expectNoDown(socket);
        socket.close();
    });

    test("a concurrent duplicate subId is latest-wins without a stale snapshot", async () => {
        if (!mutationRef || !queryRef) throw new Error("public refs were not seeded");
        const clientId = "workerd-query-duplicate";
        const { socket, first } = await openSocket(await signed(), { clientId });
        await first;
        for (const body of ["duplicate-first", "duplicate-second"] as const) {
            const result = await sendAndReceive(socket, {
                t: "mut",
                mutId: MutId(body),
                ref: mutationRef,
                args: {
                    id: `${body}-row`,
                    organizationId: "workerd-org",
                    body,
                    createdAt: body === "duplicate-first" ? 12 : 13,
                },
            });
            expect(result).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });
        }

        await setAuthorityFault("hold-throw");
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(50),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "duplicate-first" },
            })
        );
        await authorityControl("/authority-waiting");
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(50),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "duplicate-second" },
            })
        );
        // The Catalog release is a separate event source. A later frame proves
        // the Gateway cancelled the first subscription before that release.
        const replacementBarrier = nextDown(socket);
        socket.send("{");
        await expect(replacementBarrier).resolves.toMatchObject({
            t: "error",
            code: "CDB_UNSUPPORTED_FEATURE",
        });
        const snapshot = nextDown(socket);
        await authorityControl("/authority-release");

        await expect(snapshot).resolves.toMatchObject({
            t: "snapshot",
            subId: 50,
            rows: [{ id: "duplicate-second-row", body: "duplicate-second" }],
        });
        await expectNoDown(socket);
        const refresh = await sendAndReceive(socket, {
            t: "updateAuth",
            jwt: await signed(),
        });
        expect(refresh).toMatchObject({ t: "mustRefetch", subIds: [], reason: "authChanged" });
        socket.close();
    });

    test("a failed auth refresh rejects an admitted query before shard execution", async () => {
        if (!queryRef) throw new Error("query ref was not seeded");
        const { socket, first, closed } = await openSocket(await signed());
        await first;
        await setAuthorityFault("hold");
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(60),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "does-not-exist" },
            })
        );
        await authorityControl("/authority-waiting");

        const rejection = nextDown(socket);
        socket.send(encodeWire({ t: "updateAuth", jwt: "invalid.refresh.token" }));
        await expect(rejection).resolves.toMatchObject({ t: "error", code: "CDB_FORBIDDEN" });
        await authorityControl("/authority-release");
        await closed;
    });

    test("closing a socket cancels an admitted initial query", async () => {
        if (!queryRef) throw new Error("query ref was not seeded");
        const clientId = "workerd-query-close";
        const firstSocket = await openSocket(await signed(), { clientId });
        await firstSocket.first;
        await setAuthorityFault("hold");
        firstSocket.socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(70),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "query-visible" },
            })
        );
        await authorityControl("/authority-waiting");
        firstSocket.socket.close();
        await firstSocket.closed;
        await authorityControl("/authority-release");

        const replacement = await openSocket(await signed(), { clientId });
        await replacement.first;
        const snapshot = await sendAndReceive(replacement.socket, {
            t: "sub",
            subId: SubId(70),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "query-visible" },
        });
        expect(snapshot).toMatchObject({ t: "snapshot", subId: 70 });
        replacement.socket.close();
    });

    test("unsubscribe removes a delivered snapshot from the auth-refresh refetch list", async () => {
        if (!queryRef) throw new Error("query ref was not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        const snapshot = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(80),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "does-not-exist" },
        });
        expect(snapshot).toMatchObject({ t: "snapshot", subId: 80, rows: [] });

        socket.send(encodeWire({ t: "unsub", subId: SubId(80) }));
        const refresh = await sendAndReceive(socket, {
            t: "updateAuth",
            jwt: await signed(),
        });
        expect(refresh).toMatchObject({ t: "mustRefetch", subIds: [], reason: "authChanged" });
        socket.close();
    });

    test("a connection caps delivered initial snapshots before the 65th query executes", async () => {
        if (!queryRef) throw new Error("query ref was not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        for (let index = 0; index < 64; index++) {
            const snapshot = await sendAndReceive(socket, {
                t: "sub",
                subId: SubId(1000 + index),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "does-not-exist" },
            });
            expect(snapshot).toMatchObject({ t: "snapshot", subId: 1000 + index, rows: [] });
        }

        const limited = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(1064),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "__throw" },
        });
        expect(limited).toMatchObject({ t: "error", subId: 1064, code: "CDB_RATE_LIMITED" });

        socket.send(encodeWire({ t: "unsub", subId: SubId(1000) }));
        const replacement = await sendAndReceive(socket, {
            t: "sub",
            subId: SubId(1064),
            ref: queryRef,
            args: { organizationId: "workerd-org", body: "does-not-exist" },
        });
        expect(replacement).toMatchObject({ t: "snapshot", subId: 1064, rows: [] });
        socket.close();
    });

    test("same-principal updateAuth drains an admitted query, preserves it, and gates the next query", async () => {
        if (!mutationRef || !queryRef) throw new Error("public refs were not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        const seeded = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("query-refresh-seed"),
            ref: mutationRef,
            args: {
                id: "query-refresh-row",
                organizationId: "workerd-org",
                body: "query-refresh",
                createdAt: 11,
            },
        });
        expect(seeded).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });

        await setAuthorityFault("hold");
        const ordered = nextDowns(socket, 3);
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(40),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "query-refresh" },
            })
        );
        await authorityControl("/authority-waiting");
        socket.send(encodeWire({ t: "updateAuth", jwt: await signed() }));
        socket.send(
            encodeWire({
                t: "sub",
                subId: SubId(41),
                ref: queryRef,
                args: { organizationId: "workerd-org", body: "query-refresh" },
            })
        );
        await expectNoDown(socket);
        await authorityControl("/authority-release");

        const [before, refresh, after] = await ordered;
        expect(before).toMatchObject({
            t: "snapshot",
            subId: 40,
            rows: [
                {
                    id: "query-refresh-row",
                    organizationId: "workerd-org",
                    authorId: "workerd-user",
                    body: "query-refresh",
                    createdAt: 11,
                },
            ],
        });
        expect(refresh).toMatchObject({ t: "mustRefetch", subIds: [], reason: "authChanged" });
        expect(after).toMatchObject({
            t: "snapshot",
            subId: 41,
            rows: [
                {
                    id: "query-refresh-row",
                    organizationId: "workerd-org",
                    authorId: "workerd-user",
                    body: "query-refresh",
                    createdAt: 11,
                },
            ],
        });
        await expectNoDown(socket);
        socket.close();
    });

    test("an older mutation response cannot regress the delivered cookie watermark", async () => {
        if (!mutationRef || !closedMutationRef) throw new Error("mutation refs were not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        const heldMutId = "cookie-held-older";
        await mutationResponseControl("/mutation-response-hold", heldMutId);

        const newerResponse = nextDown(socket);
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId(heldMutId),
                ref: mutationRef,
                args: { id: heldMutId, organizationId: "workerd-org", body: "older", createdAt: 2 },
            })
        );
        await mutationResponseControl("/mutation-response-waiting");
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("cookie-newer"),
                ref: mutationRef,
                args: { id: "cookie-newer", organizationId: "workerd-org", body: "newer", createdAt: 3 },
            })
        );
        const newer = await newerResponse;
        expect(newer).toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "cookie-newer", ok: true }],
        });
        if (newer.t !== "poke") throw new Error("expected newer mutation poke");

        const olderResponse = nextDown(socket);
        await mutationResponseControl("/mutation-response-release");
        const older = await olderResponse;
        expect(older).toMatchObject({
            t: "poke",
            cookie: newer.cookie,
            mutResults: [{ mutId: heldMutId, ok: true }],
        });
        if (older.t !== "poke") throw new Error("expected held mutation poke");
        const olderResult = older.mutResults?.[0];
        if (!olderResult?.ok) throw new Error("expected held mutation success");
        expect(olderResult.cookie).not.toBe(newer.cookie);

        const failure = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("after-out-of-order"),
            ref: closedMutationRef,
            args: { organizationId: "workerd-org" },
        });
        expect(failure).toMatchObject({
            t: "poke",
            cookie: newer.cookie,
            mutResults: [{ ok: false, error: { code: "CDB_AUTH_NOT_BOUND" } }],
        });
        socket.close();
    });

    test("malformed and throwing Catalog authority settle once as typed failures", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        for (const fault of ["malformed", "throw"] as const) {
            await setAuthorityFault(fault);
            const response = await sendAndReceive(socket, {
                t: "mut",
                mutId: MutId(`fault-${fault}`),
                ref: mutationRef,
                args: { id: `fault-${fault}`, organizationId: "workerd-org", body: fault, createdAt: 4 },
            });
            expect(response).toMatchObject({
                t: "poke",
                mutResults: [{ ok: false, error: { code: "CDB_CATALOG_UNAVAILABLE" } }],
            });
            await expectNoDown(socket);
        }
        await setAuthorityFault("none");
        socket.close();
    });

    test("updateAuth drains admitted mutations, preserves cookies, and gates later mutations", async () => {
        if (!mutationRef || !closedMutationRef) throw new Error("mutation refs were not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        await setAuthorityFault("hold");

        const ordered = nextDowns(socket, 3);
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("refresh-before"),
                ref: mutationRef,
                args: { id: "refresh-before", organizationId: "workerd-org", body: "before", createdAt: 5 },
            })
        );
        await authorityControl("/authority-waiting");
        socket.send(encodeWire({ t: "updateAuth", jwt: await signed() }));
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("refresh-after"),
                ref: mutationRef,
                args: { id: "refresh-after", organizationId: "workerd-org", body: "after", createdAt: 6 },
            })
        );
        await expectNoDown(socket);
        await authorityControl("/authority-release");

        const [before, refresh, after] = await ordered;
        expect(before).toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "refresh-before", ok: true, result: { userId: "workerd-user" } }],
        });
        expect(refresh).toMatchObject({ t: "mustRefetch", reason: "authChanged" });
        expect(after).toMatchObject({
            t: "poke",
            mutResults: [{ mutId: "refresh-after", ok: true, result: { userId: "workerd-user" } }],
        });
        if (!before || before.t !== "poke" || !after || after.t !== "poke") {
            throw new Error("expected ordered mutation pokes around refresh");
        }

        const failure = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("refresh-cookie-check"),
            ref: closedMutationRef,
            args: { organizationId: "workerd-org" },
        });
        expect(failure).toMatchObject({
            t: "poke",
            cookie: after.cookie,
            mutResults: [{ ok: false, error: { code: "CDB_AUTH_NOT_BOUND" } }],
        });
        socket.close();
    });

    test("malformed, tampered, expired, wrong-issuer, and wrong-audience tokens receive a terminal error and no welcome", async () => {
        const now = Math.floor(Date.now() / 1000);
        const valid = await signed();
        const cases = [
            { name: "malformed", token: "not-a-jwt" },
            { name: "tampered", token: tamperJwtSignature(valid) },
            { name: "expired", token: await signed({ expirationTime: now - 60 }) },
            { name: "wrong-issuer", token: await signed({ issuer: "https://attacker.example" }) },
            { name: "wrong-audience", token: await signed({ audience: "other-app" }) },
        ];
        for (const invalid of cases) {
            const { first, closed } = await openSocket(invalid.token, { clientId: `invalid-${invalid.name}` });
            const message = await first;
            expect({ case: invalid.name, message }).toMatchObject({
                case: invalid.name,
                message: { t: "error", code: "CDB_FORBIDDEN", retryable: false },
            });
            expect({ case: invalid.name, type: message.t }).not.toMatchObject({ type: "welcome" });
            expect({ case: invalid.name, close: await closed }).toMatchObject({
                case: invalid.name,
                close: { code: 1008, reason: "CDB_FORBIDDEN" },
            });
        }
    });

    test("updateAuth rejects a valid token that changes subject", async () => {
        const { socket, first, closed } = await openSocket(await signed());
        await expect(first).resolves.toMatchObject({ t: "welcome" });

        await expect(
            sendAndReceive(socket, { t: "updateAuth", jwt: await signed({ subject: "workerd-user-2" }) })
        ).resolves.toMatchObject({
            t: "error",
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        await closed;
    });

    test("sequential updateAuth messages settle before a later mutation", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const { socket, first } = await openSocket(await signed());
        await first;
        const firstRefresh = await sendAndReceive(socket, {
            t: "updateAuth",
            jwt: await signed(),
        });
        expect(firstRefresh).toMatchObject({ t: "mustRefetch", reason: "authChanged" });
        const secondRefresh = await sendAndReceive(socket, {
            t: "updateAuth",
            jwt: await signed({ subject: "workerd-user" }),
        });
        expect(secondRefresh).toMatchObject({ t: "mustRefetch", reason: "authChanged" });
        const mutation = await sendAndReceive(socket, {
            t: "mut",
            mutId: MutId("after-two-refreshes"),
            ref: mutationRef,
            args: { id: "after-two-refreshes", organizationId: "workerd-org", body: "final", createdAt: 8 },
        });
        expect(mutation).toMatchObject({
            t: "poke",
            mutResults: [{ ok: true, result: { userId: "workerd-user" } }],
        });
        socket.close();
    });

    test("a mutation queued behind a failed refresh never dispatches", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const { socket, first, closed } = await openSocket(await signed());
        await first;
        socket.send(encodeWire({ t: "updateAuth", jwt: "invalid.refresh.token" }));
        socket.send(
            encodeWire({
                t: "mut",
                mutId: MutId("after-failed-refresh"),
                ref: mutationRef,
                args: { id: "after-failed-refresh", organizationId: "workerd-org", body: "no", createdAt: 7 },
            })
        );
        await closed;
        await new Promise(resolve => setTimeout(resolve, 25));

        const retry = await openSocket(await signed());
        const retryWelcome = await retry.first;
        expect(retryWelcome).toMatchObject({ t: "welcome" });
        const retryResponse = await sendAndReceive(retry.socket, {
            t: "mut",
            mutId: MutId("after-failed-refresh-retry"),
            ref: mutationRef,
            args: { id: "after-failed-refresh", organizationId: "workerd-org", body: "yes", createdAt: 7 },
        });
        expect(retryResponse).toMatchObject({ t: "poke", mutResults: [{ ok: true }] });
        retry.socket.close();
    });

    test("fails closed on outbound JWKS errors and suppresses retries during durable cooldown", async () => {
        for (const mode of ["throw", "status", "invalid-json", "malformed", "empty"] as const) {
            await setJwksMode(mode);
            await releaseJwksCooldown();
            const before = await jwksStats();

            const failed = await openSocket(await signedUnknown(), { clientId: `a-${mode}-jwks-failure` });
            await expect(failed.first).resolves.toMatchObject({
                t: "error",
                code: "CDB_CATALOG_UNAVAILABLE",
                retryable: true,
            });
            await failed.closed;
            expect((await jwksStats()).fetchCount).toBe(before.fetchCount + 1);

            const cooledDown = await openSocket(await signedUnknown(), { clientId: `b-${mode}-jwks-failure` });
            await expect(cooledDown.first).resolves.toMatchObject({
                t: "error",
                code: "CDB_CATALOG_UNAVAILABLE",
                retryable: true,
            });
            await cooledDown.closed;
            expect((await jwksStats()).fetchCount).toBe(before.fetchCount + 1);
        }

        await setJwksMode("ok");
        await releaseJwksCooldown();
        const beforeRecovery = await jwksStats();
        const recovered = await openSocket(await signedUnknown(), { clientId: "jwks-failure-recovered" });
        await expect(recovered.first).resolves.toMatchObject({
            t: "error",
            code: "CDB_FORBIDDEN",
            retryable: false,
        });
        await recovered.closed;
        expect((await jwksStats()).fetchCount).toBe(beforeRecovery.fetchCount + 1);
    });

    test("refreshes an expired key, accepts the rotated kid, and rejects retired or unknown keys", async () => {
        if (!mutationRef) throw new Error("mutation ref was not seeded");
        const beforePrime = await jwksStats();
        expect(beforePrime).toMatchObject({ kids: [KID] });
        const primed = await openSocket(await signed(), { clientId: "jwks-rotation-prime" });
        await expect(primed.first).resolves.toMatchObject({ t: "welcome" });
        primed.socket.close();
        await primed.closed;
        const afterPrime = await jwksStats();
        expect(afterPrime.fetchCount - beforePrime.fetchCount).toBe(beforePrime.fetchCount === 0 ? 1 : 0);
        const fetchBaseline = afterPrime.fetchCount;

        await rotateJwks();
        await expireCatalogJwk(KID);

        const retired = await openSocket(await signed(), { clientId: "jwks-retired-key" });
        await expect(retired.first).resolves.toMatchObject({ t: "error", code: "CDB_FORBIDDEN", retryable: false });
        await retired.closed;
        expect(await jwksStats()).toEqual({
            fetchCount: fetchBaseline + 1,
            lastUrl: JWKS_URL,
            kids: [ROTATED_KID],
        });

        const rotated = await openSocket(await signedRotated(), { clientId: "jwks-rotated-key" });
        await expect(rotated.first).resolves.toMatchObject({ t: "welcome" });
        const authorized = await sendAndReceive(rotated.socket, {
            t: "mut",
            mutId: MutId("jwks-rotated-authority"),
            ref: mutationRef,
            args: {
                id: "jwks-rotated-authority",
                organizationId: "workerd-org",
                body: "rotated",
                createdAt: 14,
            },
        });
        expect(authorized).toMatchObject({
            t: "poke",
            mutResults: [
                {
                    ok: true,
                    result: {
                        userId: "workerd-user",
                        tenantId: "workerd-org",
                        role: "member",
                        roles: ["member"],
                        claims: { userRole: "admin" },
                    },
                },
            ],
        });
        rotated.socket.close();
        await rotated.closed;
        expect(await jwksStats()).toMatchObject({ fetchCount: fetchBaseline + 1 });

        const unknown = await openSocket(await signedUnknown(), { clientId: "jwks-unknown-key" });
        await expect(unknown.first).resolves.toMatchObject({ t: "error", code: "CDB_FORBIDDEN", retryable: false });
        await unknown.closed;
        expect(await jwksStats()).toMatchObject({ fetchCount: fetchBaseline + 1, kids: [ROTATED_KID] });
    });
});
