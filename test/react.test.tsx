/**
 * Lifecycle tests for `@chardb/react` hooks. We render through
 * `react-test-renderer` with a stub `ChardbClient` so we can observe the
 * subscribe → patch → unsubscribe contract without booting a real
 * WebSocket. Covers what the audit flagged: hooks were entirely unverified.
 */
import { describe, expect, test } from "bun:test";
import { createAuthClient } from "better-auth/react";
import * as React from "react";
import * as TestRenderer from "react-test-renderer";
import type { ChardbClient } from "../src/client/index.ts";
import { fileRef } from "../src/files/index.ts";
import type { MutationHandle, QueryHandle, QueryRow, WireResult } from "../src/handles.ts";
import * as ChardbReact from "../src/react/index.ts";
import {
    type ChardbProviderProps,
    ChardbProvider as CoreChardbProvider,
    useChardb,
    useMutation,
    useQuery,
} from "../src/react/index.ts";
import { PROTOCOL_V, type RawJson } from "../src/wire.ts";
import { mutationHandle } from "./helpers/handles.ts";

type TestSubState = "pending" | "live" | "refetching" | "error" | "closed";

function ChardbProvider(props: React.PropsWithChildren<Omit<ChardbProviderProps, "ownership">>) {
    return React.createElement(CoreChardbProvider, { ...props, ownership: "organization" });
}

interface SubInstance {
    readonly handle: QueryHandle<never, unknown>;
    readonly ref: string;
    readonly args: RawJson;
    readonly listener: (rows: RawJson[], state?: TestSubState) => void;
    unsubscribed: boolean;
}

class ProviderWebSocket {
    static readonly OPEN = 1;
    static readonly CLOSED = 3;
    static readonly instances: ProviderWebSocket[] = [];
    onopen: (() => void) | null = null;
    onclose: (() => void) | null = null;
    onmessage: ((event: { data: unknown }) => void) | null = null;
    onerror: (() => void) | null = null;
    readyState = ProviderWebSocket.OPEN;
    closeCalls = 0;
    readonly sent: string[] = [];

    constructor(readonly url: string) {
        ProviderWebSocket.instances.push(this);
        queueMicrotask(() => this.onopen?.());
    }

    send(raw: string): void {
        this.sent.push(raw);
    }
    close(): void {
        this.closeCalls += 1;
        this.readyState = ProviderWebSocket.CLOSED;
        queueMicrotask(() => this.onclose?.());
    }
}

function stubClient() {
    const subs: SubInstance[] = [];
    const mutateCalls: { handle: MutationHandle<never, unknown>; args: RawJson }[] = [];
    const lifecycle = { closeCalls: 0 };
    const client: ChardbClient = {
        subscribe<TArgs extends RawJson, TResult>(
            handle: QueryHandle<TArgs, TResult>,
            args: TArgs,
            onChange: (rows: QueryRow<TResult>[], state: TestSubState) => void
        ) {
            const inst: SubInstance = {
                handle,
                ref: handle.__chardbRef.toString(),
                args,
                listener: (rows, state = "live") => onChange(rows as QueryRow<TResult>[], state),
                unsubscribed: false,
            };
            subs.push(inst);
            return {
                unsubscribe() {
                    inst.unsubscribed = true;
                },
            };
        },
        async mutate<TArgs extends RawJson, TResult>(
            handle: MutationHandle<TArgs, TResult>,
            args: TArgs
        ): Promise<WireResult<TResult>> {
            mutateCalls.push({ handle, args });
            return { ok: true } as WireResult<TResult>;
        },
        close() {
            lifecycle.closeCalls += 1;
        },
        state: "open" as const,
    };
    return { client, subs, mutateCalls, lifecycle };
}

function sessionAuth(userId: string, token: string) {
    const activity = { fetchCalls: 0 };
    const snapshot = {
        data: { user: { id: userId } },
        isPending: false,
    };
    const auth: ChardbReact.AuthClientLike = {
        async $fetch<_T>() {
            activity.fetchCalls += 1;
            return { data: { token } as _T, error: null };
        },
        useSession: {
            get: () => snapshot,
            subscribe: () => () => {},
        },
    };
    return { auth, activity };
}

function mutableSessionAuth() {
    const activity = { fetchCalls: 0 };
    const listeners = new Set<() => void>();
    let userId: string | null = null;
    let sessionId: string | null = null;
    let activeOrganizationId: string | null = null;
    const sessionAtom: ChardbReact.AuthSessionAtom = {
        get: () => ({
            data:
                userId === null
                    ? null
                    : {
                          user: { id: userId },
                          session: { id: sessionId ?? "missing", activeOrganizationId },
                      },
            isPending: false,
        }),
        subscribe(listener) {
            listeners.add(listener);
            return () => listeners.delete(listener);
        },
    };
    const dynamicClient = {
        async $fetch<_T>(path: string) {
            if (path !== "/token") throw new Error(`unexpected Better Auth path ${path}`);
            activity.fetchCalls += 1;
            return { data: { token: `jwt-${userId ?? "missing"}` } as _T, error: null };
        },
        async getToken() {
            throw new Error("dynamic Better Auth actions must not be feature-detected by property access");
        },
        useSession: () => {
            throw new Error("the provider must not call Better Auth's React hook");
        },
        $store: { atoms: { session: sessionAtom } },
    };
    const auth: ChardbReact.AuthClientLike = dynamicClient;
    return {
        auth,
        activity,
        setUser(
            nextUserId: string | null,
            nextSessionId: string | null = nextUserId && `session-${nextUserId}`,
            nextOrganizationId: string | null = activeOrganizationId
        ) {
            userId = nextUserId;
            sessionId = nextSessionId;
            activeOrganizationId = nextUserId === null ? null : nextOrganizationId;
            for (const listener of listeners) listener();
        },
        setOrganization(nextOrganizationId: string | null) {
            activeOrganizationId = nextOrganizationId;
            for (const listener of listeners) listener();
        },
    };
}

async function flushMicrotasks(): Promise<void> {
    await Promise.resolve();
    await Promise.resolve();
}

function nestedEmptyJson(depth: number): RawJson {
    let value: RawJson = {};
    for (let level = 1; level < depth; level++) value = { value };
    return value;
}

describe("@chardb/react — hook lifecycle", () => {
    test("exports only supported hooks", () => {
        for (const name of [
            "ChardbProvider",
            "createChardbReactClient",
            "useChardb",
            "useChardbIdentity",
            "useQuery",
            "useMutation",
        ]) {
            expect(name in ChardbReact).toBe(true);
        }
        for (const name of ["useFile", "useSession", "usePresence", "useUpload", "useStream", "useVectorSearch"]) {
            expect(name in ChardbReact).toBe(false);
        }
    });

    test("narrows Better Auth identity through organization selection", () => {
        const session = mutableSessionAuth();
        const { client } = stubClient();
        let identity: ChardbReact.ChardbIdentity<ChardbReact.ChardbOwnership> | undefined;
        function Probe() {
            identity = ChardbReact.useChardbIdentity();
            return null;
        }
        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(
                React.createElement(
                    CoreChardbProvider,
                    { ownership: "organization", client, auth: session.auth },
                    React.createElement(Probe)
                )
            );
        });
        expect(identity).toMatchObject({ ownership: "organization", status: "signed-out", user: null });

        TestRenderer.act(() => session.setUser("user-a", "session-a", null));
        expect(identity).toMatchObject({
            ownership: "organization",
            status: "select-organization",
            user: { id: "user-a" },
            organizationId: null,
        });

        TestRenderer.act(() => session.setOrganization("org-a"));
        expect(identity).toMatchObject({
            ownership: "organization",
            status: "ready",
            user: { id: "user-a" },
            organizationId: "org-a",
        });
        TestRenderer.act(() => tree.unmount());
    });

    test("configured React client injects the authenticated organization into a live query", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        session.setUser("user-sdk", "session-sdk", "org-sdk");
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com",
            ownership: "organization",
            auth: () => session.auth,
            clientId: "configured-sdk",
        });
        expect(sdk.auth).toBe(session.auth);
        const query = Object.assign(
            async (_ctx: never, _args: { organizationId: string; limit: number }) => [{ id: "unused" }],
            { __chardbKind: "query" as const, __chardbRef: { toString: () => "src/queries.ts#listMessages" } }
        );
        let result: ChardbReact.UseQueryResult<{ id: string }> | undefined;
        function Probe() {
            result = sdk.useQuery(query, { limit: 25 });
            return null;
        }
        let tree: TestRenderer.ReactTestRenderer | undefined;
        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(React.createElement(sdk.Provider, null, React.createElement(Probe)));
                await flushMicrotasks();
            });
            expect(result?.state).toBe("pending");
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected configured SDK socket");
            const socketUrl = new URL(socket.url);
            expect(socketUrl.origin).toBe("wss://db.example.com");
            expect(socketUrl.pathname).toBe("/ws");
            expect(socketUrl.searchParams.get("clientId")).toBe("configured-sdk");
            socket.onmessage?.({
                data: JSON.stringify({
                    t: "welcome",
                    protocolV: PROTOCOL_V,
                    baseCookie: "c-sdk:1",
                    region: "test",
                }),
            });
            const subscription = socket.sent
                .map(raw => JSON.parse(raw) as { readonly t: string; readonly args?: RawJson })
                .find(message => message.t === "sub");
            expect(subscription?.args).toEqual({ organizationId: "org-sdk", limit: 25 });
        } finally {
            TestRenderer.act(() => tree?.unmount());
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("uses one canonical Worker origin to configure Better Auth, sockets, and files", () => {
        const session = mutableSessionAuth();
        let authBaseURL: string | undefined;
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com/",
            ownership: "organization",
            auth: ({ baseURL }) => {
                authBaseURL = baseURL;
                return Object.assign(session.auth, { pluginAction: () => "typed-plugin" as const });
            },
        });

        expect(authBaseURL).toBe("https://db.example.com");
        expect(sdk.url).toBe("https://db.example.com");
        expect(sdk.auth.pluginAction()).toBe("typed-plugin");
    });

    test("rejects missing and unknown ownership modes before client setup", () => {
        const createFromJavaScript = ChardbReact.createChardbReactClient as unknown as (
            options: Record<string, unknown>
        ) => unknown;
        let authFactoryCalls = 0;
        for (const ownership of [undefined, "global", null]) {
            expect(() =>
                createFromJavaScript({
                    url: "https://db.example.com",
                    ownership,
                    auth: () => {
                        authFactoryCalls++;
                        return mutableSessionAuth().auth;
                    },
                })
            ).toThrow('chardb: ownership must be exactly "organization" or "user"');
        }
        expect(authFactoryCalls).toBe(0);
    });

    test("configures a real Better Auth React client without losing its inferred actions", () => {
        let authBaseURL: string | undefined;
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com",
            ownership: "user",
            auth: ({ baseURL }) => {
                authBaseURL = baseURL;
                return createAuthClient({ baseURL });
            },
        });

        expect(authBaseURL).toBe("https://db.example.com");
        expect(typeof sdk.auth.signIn.email).toBe("function");
        expect(typeof sdk.auth.$fetch).toBe("function");
        const sessionAtom = sdk.auth.$store.atoms.session;
        expect(sessionAtom).toBeDefined();
        if (!sessionAtom) throw new Error("Better Auth client did not expose its session atom");
        expect(typeof sessionAtom.get).toBe("function");
        expect(typeof sessionAtom.subscribe).toBe("function");

        const directAuthDoesNotCompile = () => {
            // @ts-expect-error A preconfigured client can point auth at a different origin.
            ChardbReact.createChardbReactClient({ url: "https://db.example.com", ownership: "user", auth: sdk.auth });
        };
        expect(typeof directAuthDoesNotCompile).toBe("function");
    });

    test("rejects public Worker URLs whose path or metadata would be discarded", () => {
        for (const url of [
            "/relative",
            "wss://db.example.com",
            "https://user:secret@db.example.com",
            "https://db.example.com/app",
            "https://db.example.com?stage=one",
            "https://db.example.com#client",
        ]) {
            expect(() =>
                ChardbReact.createChardbReactClient({
                    url,
                    ownership: "organization",
                    auth: () => mutableSessionAuth().auth,
                })
            ).toThrow("public Worker URL");
        }
    });

    test("organization file hooks inject the ready organization and hide it from callers", async () => {
        const originalFetch = globalThis.fetch;
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        session.setUser("user-files", "session-files", "org-files");
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com",
            ownership: "organization",
            auth: () => session.auth,
        });
        let files: ChardbReact.ChardbOrganizationFileClient | undefined;
        function Probe() {
            files = sdk.useFile(fileRef("messages", "attachment"));
            return null;
        }
        let requested: string | undefined;
        globalThis.fetch = (async (input: string | URL | Request) => {
            requested = String(input);
            return new Response("file");
        }) as typeof globalThis.fetch;
        let tree: TestRenderer.ReactTestRenderer | undefined;
        try {
            TestRenderer.act(() => {
                tree = TestRenderer.create(React.createElement(sdk.Provider, null, React.createElement(Probe)));
            });
            if (!files) throw new Error("expected the scoped file client");
            await files.download({ rowId: "row-1" });
            expect(requested).toBe(
                "https://db.example.com/_chardb/files/download?organizationId=org-files&table=messages&column=attachment&rowId=row-1"
            );
            await TestRenderer.act(async () => {
                session.setOrganization("org-files-next");
                await flushMicrotasks();
            });
            await files.download({ rowId: "row-2" });
            expect(requested).toBe(
                "https://db.example.com/_chardb/files/download?organizationId=org-files-next&table=messages&column=attachment&rowId=row-2"
            );
            const callerCannotSupplyOrganization = () => {
                // @ts-expect-error Organization scope comes from Better Auth, not caller input.
                files.download({ organizationId: "org-other", rowId: "row-1" });
            };
            expect(typeof callerCannotSupplyOrganization).toBe("function");
        } finally {
            tree?.unmount();
            globalThis.fetch = originalFetch;
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("organization file hooks reject signed-out and cross-origin browser operations", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com",
            ownership: "organization",
            auth: () => session.auth,
        });
        let files: ChardbReact.ChardbOrganizationFileClient | undefined;
        function Probe() {
            files = sdk.useFile(fileRef("messages", "attachment"));
            return null;
        }
        const tree = TestRenderer.create(React.createElement(sdk.Provider, null, React.createElement(Probe)));
        try {
            if (!files) throw new Error("expected the scoped file client");
            await expect(files.download({ rowId: "row-1" })).rejects.toThrow("organization identity is signed-out");

            await TestRenderer.act(async () => {
                session.setUser("user-files", "session-files", "org-files");
                await flushMicrotasks();
            });
            const originalWindow = globalThis.window;
            Object.defineProperty(globalThis, "window", {
                configurable: true,
                value: { location: { origin: "https://app.example.com" } },
            });
            try {
                await expect(files.download({ rowId: "row-1" })).rejects.toThrow("must share the app origin");
                expect(() => files?.downloadUrl({ rowId: "row-1" })).toThrow("must share the app origin");
            } finally {
                if (originalWindow === undefined) Reflect.deleteProperty(globalThis, "window");
                else Object.defineProperty(globalThis, "window", { configurable: true, value: originalWindow });
            }
        } finally {
            tree.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("user-owned clients do not expose organization-only file hooks", () => {
        const sdk = ChardbReact.createChardbReactClient({
            url: "https://db.example.com",
            ownership: "user",
            auth: () => mutableSessionAuth().auth,
        });
        expect("useFile" in sdk).toBe(false);
        const userFileHookDoesNotCompile = () => {
            // @ts-expect-error User-owned CharDB apps do not have organization file routes.
            sdk.useFile(fileRef("messages", "attachment"));
        };
        expect(typeof userFileHookDoesNotCompile).toBe("function");
    });

    test("ChardbProvider forwards mutationTimeoutMs to its client", () => {
        expect(() =>
            TestRenderer.create(
                React.createElement(ChardbProvider, {
                    endpoint: "wss://example.com/ws",
                    getJwt: async () => "jwt-stub",
                    mutationTimeoutMs: 0,
                })
            )
        ).toThrow("mutationTimeoutMs must be an integer between 1 and 2147483647");
    });

    test("useQuery subscribes on mount, receives patches, unsubscribes on unmount", () => {
        const { client, subs, lifecycle } = stubClient();
        const query = Object.assign(async (_ctx: never, _args: { organizationId: string }) => [{ id: "unused" }], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#listMessages" },
        });
        const args = { organizationId: "org-1" };

        let captured: RawJson[] | undefined;
        function Probe() {
            const r = useQuery(query, args);
            captured = r.data as RawJson[] | undefined;
            return null;
        }

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(React.createElement(ChardbProvider, { client }, React.createElement(Probe)));
        });

        expect(subs.length).toBe(1);
        const sub = subs[0];
        if (!sub) throw new Error("expected useQuery to create a subscription");
        expect(sub.ref).toBe("queries.ts#listMessages");
        expect(sub.handle).toBe(query);
        expect(sub.args).toEqual(args);
        expect(captured).toBeUndefined();

        TestRenderer.act(() => {
            sub.listener([{ id: "r1" }]);
        });
        expect(captured).toEqual([{ id: "r1" }]);

        expect(sub.unsubscribed).toBe(false);
        TestRenderer.act(() => {
            tree.unmount();
        });
        expect(sub.unsubscribed).toBe(true);
        expect(lifecycle.closeCalls).toBe(0);
    });

    test("useQuery distinguishes live empty rows from refetching, error, and closed state", () => {
        const { client, subs } = stubClient();
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#stateful" },
        });
        let captured: ReturnType<typeof useQuery<typeof query>> | undefined;
        function Probe() {
            captured = useQuery(query, {});
            return null;
        }

        TestRenderer.act(() => {
            TestRenderer.create(React.createElement(ChardbProvider, { client }, React.createElement(Probe)));
        });
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        const sub = subs[0];
        if (!sub) throw new Error("expected useQuery to create a subscription");

        TestRenderer.act(() => sub.listener([{ id: "first" }], "live"));
        expect(captured).toMatchObject({ data: [{ id: "first" }], state: "live" });
        TestRenderer.act(() => sub.listener([], "refetching"));
        expect(captured).toMatchObject({ data: [], state: "refetching" });
        TestRenderer.act(() => sub.listener([], "live"));
        expect(captured).toMatchObject({ data: [], state: "live" });
        TestRenderer.act(() => sub.listener([], "error"));
        expect(captured).toMatchObject({ data: [], state: "error" });
        TestRenderer.act(() => sub.listener([], "closed"));
        expect(captured).toMatchObject({ data: [], state: "closed" });
    });

    test("useQuery accepts a structural legacy client and treats an omitted listener state as live", () => {
        let listener: ((rows: RawJson[]) => void) | undefined;
        let unsubscribed = false;
        const client: ChardbClient = {
            subscribe<TArgs extends RawJson, TResult>(
                _handle: QueryHandle<TArgs, TResult>,
                _args: TArgs,
                onChange: (rows: QueryRow<TResult>[]) => void
            ) {
                listener = rows => onChange(rows as QueryRow<TResult>[]);
                return {
                    unsubscribe() {
                        unsubscribed = true;
                    },
                };
            },
            async mutate<TResult>(): Promise<WireResult<TResult>> {
                return null as WireResult<TResult>;
            },
            close() {},
            state: "open",
        };
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#legacy" },
        });
        let captured: ReturnType<typeof useQuery<typeof query>> | undefined;
        function Probe() {
            captured = useQuery(query, {});
            return null;
        }

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(React.createElement(ChardbProvider, { client }, React.createElement(Probe)));
        });
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => listener?.([{ id: "legacy" }]));
        expect(captured).toMatchObject({ data: [{ id: "legacy" }], state: "live" });

        TestRenderer.act(() => tree.unmount());
        expect(unsubscribed).toBe(true);
    });

    test("replacing borrowed clients unsubscribes local queries without closing either client", () => {
        const first = stubClient();
        const second = stubClient();
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#replacement" },
        });

        function Probe() {
            useQuery(query, {});
            return null;
        }

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(
                React.createElement(ChardbProvider, { client: first.client }, React.createElement(Probe))
            );
        });
        expect(first.subs).toHaveLength(1);

        TestRenderer.act(() => {
            tree.update(React.createElement(ChardbProvider, { client: second.client }, React.createElement(Probe)));
        });
        expect(first.subs[0]?.unsubscribed).toBe(true);
        expect(first.lifecycle.closeCalls).toBe(0);
        expect(second.subs).toHaveLength(1);
        expect(second.subs[0]?.unsubscribed).toBe(false);

        TestRenderer.act(() => tree.unmount());
        expect(second.subs[0]?.unsubscribed).toBe(true);
        expect(first.lifecycle.closeCalls).toBe(0);
        expect(second.lifecycle.closeCalls).toBe(0);
    });

    test("closes each provider-created client exactly once across replacement and unmount", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        let tree: TestRenderer.ReactTestRenderer | undefined;
        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(ChardbProvider, {
                        endpoint: "wss://example.com/first",
                        getJwt: async () => "jwt-stub",
                        clientId: "provider-owned",
                    })
                );
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(1);
            const first = ProviderWebSocket.instances[0];
            if (!first) throw new Error("expected the first provider-owned socket");

            await TestRenderer.act(async () => {
                tree?.update(
                    React.createElement(ChardbProvider, {
                        endpoint: "wss://example.com/second",
                        getJwt: async () => "jwt-stub",
                        clientId: "provider-owned",
                    })
                );
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(2);
            const second = ProviderWebSocket.instances[1];
            if (!second) throw new Error("expected the replacement provider-owned socket");
            expect(first.closeCalls).toBe(1);
            expect(second.closeCalls).toBe(0);

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(first.closeCalls).toBe(1);
            expect(second.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("an auth-only update preserves an owned client when getJwt is explicit", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const firstAuth = sessionAuth("user-a", "unused-a");
        const secondAuth = sessionAuth("user-b", "unused-b");
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#auth-context" },
        });
        let getJwtCalls = 0;
        const getJwt = async () => {
            getJwtCalls += 1;
            return "explicit-jwt";
        };
        let capturedClient: ChardbClient | undefined;
        let tree: TestRenderer.ReactTestRenderer | undefined;
        let mutationOutcome:
            | Promise<{ readonly ok: true; readonly value: RawJson } | { readonly ok: false }>
            | undefined;

        function Probe() {
            capturedClient = useChardb();
            useQuery(query, {});
            return null;
        }

        const render = (auth: ChardbReact.AuthClientLike) =>
            React.createElement(
                ChardbProvider,
                {
                    endpoint: "wss://example.com/auth-context",
                    getJwt,
                    auth,
                    clientId: "provider-auth-context",
                },
                React.createElement(Probe)
            );

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(render(firstAuth.auth));
                await flushMicrotasks();
            });
            const originalClient = capturedClient;
            if (!originalClient) throw new Error("expected the provider-owned client");
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected the provider-owned socket");
            let mutationSettled = false;
            mutationOutcome = originalClient.mutate(mutationHandle("mutations.ts#auth-context"), {}).then(
                value => {
                    mutationSettled = true;
                    return { ok: true as const, value };
                },
                () => {
                    mutationSettled = true;
                    return { ok: false as const };
                }
            );

            await TestRenderer.act(async () => {
                tree?.update(render(secondAuth.auth));
                await flushMicrotasks();
            });
            expect(capturedClient).toBe(originalClient);
            expect(getJwtCalls).toBe(1);
            expect(firstAuth.activity.fetchCalls).toBe(0);
            expect(secondAuth.activity.fetchCalls).toBe(0);
            expect(ProviderWebSocket.instances).toHaveLength(1);
            expect(socket.closeCalls).toBe(0);
            expect(mutationSettled).toBe(false);

            socket.onmessage?.({
                data: JSON.stringify({
                    t: "welcome",
                    protocolV: PROTOCOL_V,
                    baseCookie: "c-auth-context:1",
                    region: "test",
                }),
            });
            const sent = socket.sent.map(raw => JSON.parse(raw) as { readonly t: string; readonly mutId?: string });
            expect(sent.map(message => message.t)).toEqual(["hello", "sub", "mut"]);
            expect(sent.filter(message => message.t === "sub")).toHaveLength(1);
            const mutation = sent.find(message => message.t === "mut");
            if (!mutation?.mutId) throw new Error("expected a queued mutation");
            socket.onmessage?.({
                data: JSON.stringify({
                    t: "poke",
                    cookie: "c-auth-context:2",
                    patches: [],
                    mutResults: [
                        {
                            mutId: mutation.mutId,
                            ok: true,
                            result: { saved: true },
                            cookie: "c-auth-context:2",
                        },
                    ],
                }),
            });
            await expect(mutationOutcome).resolves.toEqual({ ok: true, value: { saved: true } });

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(socket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            await mutationOutcome?.catch(() => {});
            capturedClient?.close();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("an auth-derived JWT update replaces and closes the owned client once", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const firstAuth = sessionAuth("user-a", "jwt-a");
        const secondAuth = sessionAuth("user-b", "jwt-b");
        let capturedClient: ChardbClient | undefined;
        let tree: TestRenderer.ReactTestRenderer | undefined;

        function CaptureClient() {
            capturedClient = useChardb();
            return null;
        }

        const render = (auth: ChardbReact.AuthClientLike) =>
            React.createElement(
                ChardbProvider,
                {
                    endpoint: "wss://example.com/auth-derived",
                    auth,
                    clientId: "provider-auth-derived",
                },
                React.createElement(CaptureClient)
            );

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(render(firstAuth.auth));
                await flushMicrotasks();
            });
            const originalClient = capturedClient;
            if (!originalClient) throw new Error("expected the first provider-owned client");
            const firstSocket = ProviderWebSocket.instances[0];
            if (!firstSocket) throw new Error("expected the first provider-owned socket");

            await TestRenderer.act(async () => {
                tree?.update(render(secondAuth.auth));
                await flushMicrotasks();
            });
            expect(capturedClient).not.toBe(originalClient);
            expect(firstAuth.activity.fetchCalls).toBe(1);
            expect(secondAuth.activity.fetchCalls).toBe(1);
            expect(ProviderWebSocket.instances).toHaveLength(2);
            const secondSocket = ProviderWebSocket.instances[1];
            if (!secondSocket) throw new Error("expected the replacement provider-owned socket");
            expect(firstSocket.closeCalls).toBe(1);
            expect(secondSocket.closeCalls).toBe(0);

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(firstSocket.closeCalls).toBe(1);
            expect(secondSocket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            capturedClient?.close();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("re-authenticates when one Better Auth client changes principal", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#signed-out" },
        });
        let tree: TestRenderer.ReactTestRenderer | undefined;

        function Probe() {
            useQuery(query, {});
            return null;
        }

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(
                        ChardbProvider,
                        {
                            endpoint: "wss://example.com/auth-session",
                            auth: session.auth,
                            clientId: "provider-auth-session",
                        },
                        React.createElement(Probe)
                    )
                );
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(0);
            expect(session.activity.fetchCalls).toBe(0);

            await TestRenderer.act(async () => {
                session.setUser("user-a");
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(1);
            expect(session.activity.fetchCalls).toBe(1);
            const firstSocket = ProviderWebSocket.instances[0];
            if (!firstSocket) throw new Error("expected the first authenticated socket");

            await TestRenderer.act(async () => {
                session.setUser("user-b");
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(2);
            expect(session.activity.fetchCalls).toBe(2);
            expect(firstSocket.closeCalls).toBe(1);
            const secondSocket = ProviderWebSocket.instances[1];
            if (!secondSocket) throw new Error("expected the replacement authenticated socket");

            await TestRenderer.act(async () => {
                session.setUser("user-b", "session-user-b-replacement");
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(3);
            expect(session.activity.fetchCalls).toBe(3);
            expect(secondSocket.closeCalls).toBe(1);
            const thirdSocket = ProviderWebSocket.instances[2];
            if (!thirdSocket) throw new Error("expected the same-user replacement session socket");

            await TestRenderer.act(async () => {
                session.setUser(null);
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(3);
            expect(session.activity.fetchCalls).toBe(3);
            expect(thirdSocket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("re-authenticates when the active Better Auth organization changes", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        session.setUser("user-a", "session-a", "org-a");
        let tree: TestRenderer.ReactTestRenderer | undefined;

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(ChardbProvider, {
                        endpoint: "wss://example.com/auth-organization",
                        auth: session.auth,
                        clientId: "provider-auth-organization",
                    })
                );
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(1);
            expect(session.activity.fetchCalls).toBe(1);
            const firstSocket = ProviderWebSocket.instances[0];
            if (!firstSocket) throw new Error("expected the first organization socket");

            await TestRenderer.act(async () => {
                session.setOrganization("org-b");
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(2);
            expect(session.activity.fetchCalls).toBe(2);
            expect(firstSocket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("keeps a user-owned connection when unrelated active organization state changes", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        session.setUser("user-a", "session-a", "org-a");
        let tree: TestRenderer.ReactTestRenderer | undefined;

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(CoreChardbProvider, {
                        ownership: "user",
                        endpoint: "wss://example.com/auth-user",
                        auth: session.auth,
                        clientId: "provider-auth-user",
                    })
                );
                await flushMicrotasks();
            });
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected the user-owned socket");

            await TestRenderer.act(async () => {
                session.setOrganization("org-b");
                await flushMicrotasks();
            });
            expect(ProviderWebSocket.instances).toHaveLength(1);
            expect(session.activity.fetchCalls).toBe(1);
            expect(socket.closeCalls).toBe(0);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("does not start a provider client for a render that never commits", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        let getJwtCalls = 0;
        function AbortRender(): React.ReactElement {
            throw new Error("abort render");
        }

        try {
            expect(() =>
                TestRenderer.create(
                    React.createElement(
                        ChardbProvider,
                        {
                            endpoint: "wss://example.com/aborted",
                            getJwt: async () => {
                                getJwtCalls += 1;
                                return "jwt-stub";
                            },
                            clientId: "provider-aborted",
                        },
                        React.createElement(AbortRender)
                    )
                )
            ).toThrow("abort render");
            await flushMicrotasks();
            expect(getJwtCalls).toBe(0);
            expect(ProviderWebSocket.instances).toHaveLength(0);
        } finally {
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("keeps the committed owned client alive through StrictMode rehearsal and closes it on unmount", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        let getJwtCalls = 0;
        let tree: TestRenderer.ReactTestRenderer | undefined;
        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(
                        React.StrictMode,
                        null,
                        React.createElement(ChardbProvider, {
                            endpoint: "wss://example.com/strict",
                            getJwt: async () => {
                                getJwtCalls += 1;
                                return "jwt-stub";
                            },
                            clientId: "provider-strict",
                        })
                    )
                );
                await flushMicrotasks();
            });
            expect(getJwtCalls).toBe(1);
            expect(ProviderWebSocket.instances).toHaveLength(1);
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected the StrictMode provider socket");
            expect(socket.closeCalls).toBe(0);

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(socket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("keeps a StrictMode provider open when its first query mounts after authentication", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const session = mutableSessionAuth();
        session.setUser("user-a", "session-a");
        const query = Object.assign(async (_ctx: never, _args: { organizationId: string }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#delayed-organization" },
        });
        let tree: TestRenderer.ReactTestRenderer | undefined;

        function QueryProbe() {
            useQuery(query, { organizationId: "org-a" });
            return null;
        }
        const render = (showQuery: boolean) =>
            React.createElement(
                React.StrictMode,
                null,
                React.createElement(
                    ChardbProvider,
                    {
                        endpoint: "wss://example.com/strict-delayed-query",
                        auth: session.auth,
                        clientId: "provider-strict-delayed-query",
                    },
                    showQuery ? React.createElement(QueryProbe) : null
                )
            );

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(render(false), { unstable_strictMode: true } as never);
                await flushMicrotasks();
            });
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected the authenticated StrictMode socket");
            expect(socket.closeCalls).toBe(0);

            await TestRenderer.act(async () => {
                tree?.update(render(true));
                await flushMicrotasks();
            });
            expect(socket.closeCalls).toBe(0);

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(socket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("hands an owned client to the caller without closing the same object", async () => {
        const realWebSocket = globalThis.WebSocket;
        ProviderWebSocket.instances.length = 0;
        (globalThis as { WebSocket: unknown }).WebSocket = ProviderWebSocket;
        const getJwt = async () => "jwt-stub";
        let captured: ChardbClient | undefined;
        let tree: TestRenderer.ReactTestRenderer | undefined;
        function CaptureClient() {
            captured = useChardb();
            return null;
        }

        try {
            await TestRenderer.act(async () => {
                tree = TestRenderer.create(
                    React.createElement(
                        ChardbProvider,
                        {
                            endpoint: "wss://example.com/handoff",
                            getJwt,
                            clientId: "provider-handoff",
                        },
                        React.createElement(CaptureClient)
                    )
                );
                await flushMicrotasks();
            });
            const ownedClient = captured;
            if (!ownedClient) throw new Error("expected the provider-owned client");
            const socket = ProviderWebSocket.instances[0];
            if (!socket) throw new Error("expected the provider-owned socket");

            await TestRenderer.act(async () => {
                tree?.update(
                    React.createElement(ChardbProvider, { client: ownedClient }, React.createElement(CaptureClient))
                );
                await flushMicrotasks();
            });
            expect(captured).toBe(ownedClient);
            expect(socket.closeCalls).toBe(0);

            TestRenderer.act(() => tree?.unmount());
            await flushMicrotasks();
            expect(socket.closeCalls).toBe(0);

            ownedClient.close();
            expect(socket.closeCalls).toBe(1);
        } finally {
            tree?.unmount();
            captured?.close();
            (globalThis as { WebSocket: unknown }).WebSocket = realWebSocket;
        }
    });

    test("returns pending immediately and ignores old listeners when query identity changes", () => {
        const first = stubClient();
        const second = stubClient();
        type Query = ((ctx: never, args: { organizationId: string }) => Promise<never[]>) & {
            readonly __chardbKind: "query";
            readonly __chardbRef: { toString(): string };
        };
        const firstQuery: Query = Object.assign(async (_ctx: never, _args: { organizationId: string }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#first" },
        });
        const secondQuery: Query = Object.assign(async (_ctx: never, _args: { organizationId: string }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#second" },
        });
        let captured: { readonly data: RawJson[] | undefined; readonly state: string } | undefined;
        function Probe(props: { readonly query: Query; readonly organizationId: string }) {
            captured = useQuery(props.query, { organizationId: props.organizationId }) as {
                readonly data: RawJson[] | undefined;
                readonly state: string;
            };
            return null;
        }
        const render = (client: ChardbClient, query: Query, organizationId: string) =>
            React.createElement(ChardbProvider, { client }, React.createElement(Probe, { query, organizationId }));

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(render(first.client, firstQuery, "org-1"));
        });
        const original = first.subs[0];
        if (!original) throw new Error("expected the original query subscription");
        TestRenderer.act(() => original.listener([{ id: "old-args" }]));
        expect(captured).toMatchObject({ data: [{ id: "old-args" }], state: "live" });

        TestRenderer.act(() => tree.update(render(first.client, firstQuery, "org-2")));
        const newArgs = first.subs[1];
        if (!newArgs) throw new Error("expected the replacement-arguments subscription");
        expect(original.unsubscribed).toBe(true);
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => original.listener([{ id: "late-old-args" }]));
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => newArgs.listener([{ id: "new-args" }]));
        expect(captured).toMatchObject({ data: [{ id: "new-args" }], state: "live" });

        TestRenderer.act(() => tree.update(render(first.client, secondQuery, "org-2")));
        const newRef = first.subs[2];
        if (!newRef) throw new Error("expected the replacement-reference subscription");
        expect(newArgs.unsubscribed).toBe(true);
        expect(newRef.ref).toBe("queries.ts#second");
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => newArgs.listener([{ id: "late-old-ref" }]));
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => newRef.listener([{ id: "new-ref" }]));

        TestRenderer.act(() => tree.update(render(second.client, secondQuery, "org-2")));
        const newClient = second.subs[0];
        if (!newClient) throw new Error("expected the replacement-client subscription");
        expect(newRef.unsubscribed).toBe(true);
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => newRef.listener([{ id: "late-old-client" }]));
        expect(captured).toMatchObject({ data: undefined, state: "pending" });
        TestRenderer.act(() => newClient.listener([{ id: "new-client" }]));
        expect(captured).toMatchObject({ data: [{ id: "new-client" }], state: "live" });

        TestRenderer.act(() => tree.unmount());
    });

    test("useQuery keeps one subscription when inline args are recreated by a result render", () => {
        const { client, subs } = stubClient();
        const query = Object.assign(async (_ctx: never, _args: { a: number; b: number }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#listMessages" },
        });

        function Probe(props: { readonly reverse: boolean }) {
            useQuery(query, props.reverse ? { b: 2, a: 1 } : { a: 1, b: 2 });
            return null;
        }

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(
                React.createElement(ChardbProvider, { client }, React.createElement(Probe, { reverse: false }))
            );
        });
        const sub = subs[0];
        if (!sub) throw new Error("expected useQuery to create a subscription");

        TestRenderer.act(() => {
            sub.listener([{ id: "r1" }]);
            tree.update(React.createElement(ChardbProvider, { client }, React.createElement(Probe, { reverse: true })));
        });

        expect(subs).toHaveLength(1);
        expect(sub.unsubscribed).toBe(false);
    });

    test("useQuery rejects non-JSON argument shapes without invoking getters", () => {
        const query = Object.assign(async (_ctx: never, _args: RawJson) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#strictArguments" },
        });
        let getterRuns = 0;
        const accessor: Record<string, unknown> = {};
        Object.defineProperty(accessor, "value", {
            enumerable: true,
            get() {
                getterRuns++;
                return "unsafe";
            },
        });
        const nonEnumerable = { visible: true };
        Object.defineProperty(nonEnumerable, "hidden", { value: true });
        const objectSymbol = { visible: true };
        Object.defineProperty(objectSymbol, Symbol("hidden"), { value: true, enumerable: true });
        const sparse = Array(1);
        const extraArray: unknown[] = [];
        Object.defineProperty(extraArray, "extra", { value: true, enumerable: true });
        const symbolArray: unknown[] = [];
        Object.defineProperty(symbolArray, Symbol("extra"), { value: true, enumerable: true });
        const cyclic: Record<string, unknown> = {};
        cyclic.self = cyclic;

        for (const args of [
            { value: -0 },
            { value: Number.NaN },
            sparse,
            extraArray,
            symbolArray,
            objectSymbol,
            nonEnumerable,
            accessor,
            new Date(0),
            cyclic,
        ]) {
            const { client, subs } = stubClient();
            let thrown: unknown;
            try {
                TestRenderer.create(
                    React.createElement(
                        ChardbProvider,
                        { client },
                        React.createElement(() => {
                            useQuery(query, args as RawJson);
                            return null;
                        })
                    )
                );
            } catch (error) {
                thrown = error;
            }
            expect(thrown).toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
            expect(subs).toHaveLength(0);
        }
        expect(getterRuns).toBe(0);
    });

    test("useQuery rejects a live argument change from zero to negative zero", () => {
        const { client, subs } = stubClient();
        const query = Object.assign(async (_ctx: never, _args: { value: number }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#signedZero" },
        });
        let captured: RawJson[] | undefined;
        function Probe(props: { readonly value: number }) {
            captured = useQuery(query, { value: props.value }).data as RawJson[] | undefined;
            return null;
        }
        const render = (value: number) =>
            React.createElement(ChardbProvider, { client }, React.createElement(Probe, { value }));

        let tree!: TestRenderer.ReactTestRenderer;
        TestRenderer.act(() => {
            tree = TestRenderer.create(render(0));
        });
        const sub = subs[0];
        if (!sub) throw new Error("expected the initial query subscription");
        TestRenderer.act(() => sub.listener([{ id: "zero" }]));
        expect(captured).toEqual([{ id: "zero" }]);

        let thrown: unknown;
        try {
            TestRenderer.act(() => tree.update(render(-0)));
        } catch (error) {
            thrown = error;
        }
        expect(thrown).toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        expect(subs).toHaveLength(1);
    });

    test("useQuery accepts the exact argument byte limit and rejects one value over it", () => {
        const query = Object.assign(async (_ctx: never, _args: { value: string }) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#argumentBytes" },
        });
        const exact = stubClient();
        TestRenderer.act(() => {
            TestRenderer.create(
                React.createElement(
                    ChardbProvider,
                    { client: exact.client },
                    React.createElement(() => {
                        useQuery(query, { value: "é".repeat(262_138) });
                        return null;
                    })
                )
            );
        });
        expect(exact.subs).toHaveLength(1);

        const over = stubClient();
        let thrown: unknown;
        try {
            TestRenderer.create(
                React.createElement(
                    ChardbProvider,
                    { client: over.client },
                    React.createElement(() => {
                        useQuery(query, { value: "é".repeat(262_139) });
                        return null;
                    })
                )
            );
        } catch (error) {
            thrown = error;
        }
        expect(thrown).toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        expect(over.subs).toHaveLength(0);
    });

    test("useQuery accepts 99 empty container levels and rejects the 100th before subscribing", () => {
        const query = Object.assign(async (_ctx: never, _args: RawJson) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#argumentDepth" },
        });
        const exact = stubClient();
        TestRenderer.act(() => {
            TestRenderer.create(
                React.createElement(
                    ChardbProvider,
                    { client: exact.client },
                    React.createElement(() => {
                        useQuery(query, nestedEmptyJson(99));
                        return null;
                    })
                )
            );
        });
        expect(exact.subs).toHaveLength(1);

        const over = stubClient();
        let thrown: unknown;
        try {
            TestRenderer.create(
                React.createElement(
                    ChardbProvider,
                    { client: over.client },
                    React.createElement(() => {
                        useQuery(query, nestedEmptyJson(100));
                        return null;
                    })
                )
            );
        } catch (error) {
            thrown = error;
        }
        expect(thrown).toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        expect(over.subs).toHaveLength(0);
    });

    test("useMutation hands the client the same mutation handle it was given", async () => {
        const { client, mutateCalls } = stubClient();
        const fn = mutationHandle("mutation#postMessage");

        let invoke: ((args: RawJson) => Promise<RawJson>) | undefined;
        function Probe() {
            const m = useMutation(fn);
            invoke = m as (args: RawJson) => Promise<RawJson>;
            return null;
        }

        TestRenderer.create(React.createElement(ChardbProvider, { client }, React.createElement(Probe)));

        expect(typeof invoke).toBe("function");
        if (!invoke) throw new Error("expected useMutation to expose an invoke function");
        await invoke({ body: "hi" });
        expect(mutateCalls).toHaveLength(1);
        expect(mutateCalls[0]?.handle).toBe(fn);
        expect(mutateCalls[0]?.args).toEqual({ body: "hi" });
    });

    test("useQuery without a Provider throws a clear error", () => {
        const query = Object.assign(async (_ctx: never, _args: Record<string, never>) => [], {
            __chardbKind: "query" as const,
            __chardbRef: { toString: () => "queries.ts#list" },
        });
        function Bad() {
            useQuery(query, {});
            return null;
        }
        expect(() => TestRenderer.create(React.createElement(Bad))).toThrow(
            /useChardb must be used inside <ChardbProvider>/
        );
    });
});
