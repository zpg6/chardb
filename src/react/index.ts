/**
 * Internal implementation for `@chardb/react`. All hooks accept function refs
 * from `@chardb/core/server`; users never type a wire identifier as a string.
 */

import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { Column } from "drizzle-orm";
import {
    type PropsWithChildren,
    type ReactElement,
    createContext,
    createElement,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useRef,
    useState,
    useSyncExternalStore,
} from "react";
import type { ChardbClient, ChardbClientOptions } from "../client/index.ts";
import { createDeferredChardbClientController } from "../client/index.ts";
import { normalizePublicWorkerUrl, publicWorkerWebSocketUrl } from "../client/public-url.ts";
import { snapshotSubscriptionArguments } from "../client/serialized-json.ts";
import { CdbError } from "../errors.ts";
import {
    type FileDownloadInput,
    type FileRef,
    type FileUploadInput,
    type FileUploadResult,
    createFileClient,
} from "../files/index.ts";
import { stableJson } from "../util/canonical.ts";
import type { RawJson } from "../wire.ts";

/**
 * Infer the wire-shape arguments of a `defineMutation` / `defineQuery`
 * handler. Equivalent to `Parameters<typeof handler>[1]` but
 * self-documenting at the call site:
 *
 * ```ts
 * type Args = InferArgs<typeof postMessage>; // { id, body, ... }
 * ```
 */
export type InferArgs<F> = F extends (ctx: never, args: infer A) => unknown ? A : never;

/**
 * Infer the row shape returned by a `defineQuery` handler. For
 * collection queries (`Promise<RowType[]>`) this resolves to the
 * element type; for scalar queries it resolves to the awaited result.
 *
 * ```ts
 * type MessageRow = InferRow<typeof listMessages>;
 * ```
 */
export type InferRow<F> = F extends (...args: never[]) => Promise<infer R>
    ? R extends readonly (infer Row)[]
        ? Row
        : R
    : never;

/**
 * Subset of the Better Auth client API the provider relies on. Both the
 * framework-neutral client and the React client expose the same session atom,
 * although the React client keeps it under `$store.atoms.session` and exposes
 * `useSession` as a hook.
 */
export interface AuthClientLike {
    readonly $fetch: <T = unknown>(
        path: string,
        init?: { method?: string; body?: unknown }
    ) => Promise<{
        data: T | null;
        error: { message?: string; status?: number; statusText?: string } | null;
    }>;
    readonly useSession?: AuthSessionAtom | (() => unknown);
    readonly $store?: {
        readonly atoms: { readonly session?: AuthSessionAtom; readonly [key: string]: unknown };
    };
}

/** Minimal nanostores atom surface — just enough to drive `useSyncExternalStore`. */
export interface AuthSessionAtom {
    get(): { readonly data: SessionData | null; readonly isPending: boolean };
    subscribe(listener: () => void): () => void;
}

export interface SessionData {
    readonly user?: { readonly id: string; readonly [k: string]: unknown };
    readonly session?: {
        readonly id?: string;
        readonly userId?: string;
        readonly activeOrganizationId?: string | null;
        readonly [k: string]: unknown;
    };
    readonly [k: string]: unknown;
}

/** The one ownership axis selected by a CharDB application. */
export type ChardbOwnership = "organization" | "user";

type SessionUser = NonNullable<SessionData["user"]>;
type SessionRecord = NonNullable<SessionData["session"]>;

interface PendingIdentity<M extends ChardbOwnership> {
    readonly ownership: M;
    readonly status: "loading" | "signed-out";
    readonly user: null;
    readonly session: null;
    readonly organizationId: null;
}

interface UserIdentity {
    readonly ownership: "user";
    readonly status: "ready";
    readonly user: SessionUser;
    readonly session: SessionRecord;
    readonly userId: string;
    readonly organizationId: null;
}

interface OrganizationSelectionIdentity {
    readonly ownership: "organization";
    readonly status: "select-organization";
    readonly user: SessionUser;
    readonly session: SessionRecord;
    readonly userId: string;
    readonly organizationId: null;
}

interface OrganizationIdentity {
    readonly ownership: "organization";
    readonly status: "ready";
    readonly user: SessionUser;
    readonly session: SessionRecord;
    readonly userId: string;
    readonly organizationId: string;
}

/** Better Auth session state, narrowed by the application's fixed ownership mode. */
export type ChardbIdentity<M extends ChardbOwnership> = M extends "organization"
    ? PendingIdentity<"organization"> | OrganizationSelectionIdentity | OrganizationIdentity
    : PendingIdentity<"user"> | UserIdentity;

interface ChardbContextValue {
    readonly client: ChardbClient;
    readonly ownership: ChardbOwnership;
    readonly sessionAtom: AuthSessionAtom | null;
}

type ProviderClientResource =
    | { readonly client: ChardbClient; readonly owned: false }
    | { readonly client: ChardbClient; readonly owned: true; readonly start: () => void };

interface PendingClientClose {
    cancelled: boolean;
}

const ChardbCtx = createContext<ChardbContextValue | null>(null);

function authSessionAtom(auth: AuthClientLike | null): AuthSessionAtom | null {
    const direct = auth?.useSession;
    if (
        typeof direct === "object" &&
        direct !== null &&
        typeof direct.get === "function" &&
        typeof direct.subscribe === "function"
    ) {
        return direct;
    }
    const stored = auth?.$store?.atoms.session;
    return stored && typeof stored.get === "function" && typeof stored.subscribe === "function" ? stored : null;
}

function sessionIdentity(atom: AuthSessionAtom | null, ownership: ChardbOwnership): string | null {
    const data = atom?.get().data;
    const userId = data?.user?.id ?? data?.session?.userId;
    if (!userId) return null;
    return JSON.stringify([
        userId,
        data?.session?.id ?? null,
        ownership === "organization" ? (data?.session?.activeOrganizationId ?? null) : null,
    ]);
}

function useAuthSessionIdentity(atom: AuthSessionAtom | null, ownership: ChardbOwnership): string | null {
    const subscribe = useCallback((listener: () => void) => (atom ? atom.subscribe(listener) : () => {}), [atom]);
    const getSnapshot = useCallback(() => sessionIdentity(atom, ownership), [atom, ownership]);
    return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

function isRetryableTokenStatus(status: number | undefined): boolean {
    return (
        status === 0 || status === 408 || status === 425 || status === 429 || (status !== undefined && status >= 500)
    );
}

function authTokenFailure(message: string, retryable: boolean, cause?: unknown): CdbError {
    return new CdbError({
        code: retryable ? "CDB_STREAM_ABORTED" : "CDB_FORBIDDEN",
        message,
        ...(cause === undefined ? {} : { cause }),
    });
}

export interface ChardbProviderProps extends Partial<ChardbClientOptions> {
    /** Must match the application's server-side `chardb({ ownership })` setting. */
    readonly ownership: ChardbOwnership;
    readonly client?: ChardbClient;
    /**
     * better-auth `createAuthClient(...)` instance. When provided,
     * `ChardbProvider` gets a token from the standard Better Auth `jwt()`
     * plugin through `$fetch("/token")`. Better Auth clients use a dynamic
     * action proxy, so property-presence checks cannot discover optional
     * actions safely. Accepts clients created by `better-auth/react` and
     * `better-auth/client`.
     */
    readonly auth?: AuthClientLike;
}

export function ChardbProvider(props: PropsWithChildren<ChardbProviderProps>): ReactElement {
    const jwtAuth = props.getJwt === undefined ? props.auth : undefined;
    const sessionAtom = authSessionAtom(props.auth ?? null);
    const authSessionIdentity = useAuthSessionIdentity(sessionAtom, props.ownership);
    const jwtSessionIdentity = jwtAuth === undefined ? null : authSessionIdentity;
    const resource = useMemo<ProviderClientResource>(() => {
        if (props.client !== undefined) return { client: props.client, owned: false };
        const getJwt =
            props.getJwt ??
            (jwtAuth
                ? async () => {
                      if (jwtSessionIdentity === null) {
                          throw authTokenFailure(
                              "chardb: cannot fetch a JWT without an authenticated Better Auth session",
                              false
                          );
                      }
                      let r: {
                          data: { token: string } | null;
                          error: { message?: string; status?: number; statusText?: string } | null;
                      };
                      try {
                          r = await jwtAuth.$fetch<{ token: string }>("/token");
                      } catch (cause) {
                          throw authTokenFailure("chardb: Better Auth token request failed", true, cause);
                      }
                      if (r.error || !r.data?.token) {
                          throw authTokenFailure(
                              `chardb: failed to fetch JWT (${r.error?.message ?? "no token"})`,
                              r.error !== null && isRetryableTokenStatus(r.error.status)
                          );
                      }
                      return r.data.token;
                  }
                : undefined);
        if (!props.endpoint || !getJwt) {
            throw new Error("ChardbProvider requires {endpoint} plus either {getJwt} or {auth: createAuthClient(...)}");
        }
        const controller = createDeferredChardbClientController(
            {
                endpoint: props.endpoint,
                getJwt,
                ...(props.clientId !== undefined ? { clientId: props.clientId } : {}),
                ...(props.mutationTimeoutMs !== undefined ? { mutationTimeoutMs: props.mutationTimeoutMs } : {}),
                ...(props.onSessionError !== undefined ? { onSessionError: props.onSessionError } : {}),
            },
            { autoStartOnOperation: false, initialJwtFailureRetries: 3 }
        );
        return { client: controller.client, owned: true, start: controller.start };
    }, [
        props.client,
        props.endpoint,
        props.getJwt,
        jwtAuth,
        jwtSessionIdentity,
        props.clientId,
        props.mutationTimeoutMs,
        props.onSessionError,
    ]);
    const pendingCloses = useRef(new WeakMap<ChardbClient, Set<PendingClientClose>>());

    useEffect(() => {
        const pendingForClient = pendingCloses.current.get(resource.client);
        if (pendingForClient) {
            for (const pending of pendingForClient) pending.cancelled = true;
            pendingCloses.current.delete(resource.client);
        }
        if (resource.owned && (jwtAuth === undefined || jwtSessionIdentity !== null)) resource.start();
        return () => {
            if (!resource.owned) return;
            const pending: PendingClientClose = { cancelled: false };
            let clientClosures = pendingCloses.current.get(resource.client);
            if (!clientClosures) {
                clientClosures = new Set();
                pendingCloses.current.set(resource.client, clientClosures);
            }
            clientClosures.add(pending);
            queueMicrotask(() => {
                clientClosures?.delete(pending);
                if (clientClosures?.size === 0) pendingCloses.current.delete(resource.client);
                if (!pending.cancelled) resource.client.close();
            });
        };
    }, [resource, jwtAuth, jwtSessionIdentity]);

    const value = useMemo<ChardbContextValue>(
        () => ({ client: resource.client, ownership: props.ownership, sessionAtom }),
        [resource.client, props.ownership, sessionAtom]
    );

    return createElement(ChardbCtx.Provider, { value }, props.children);
}

export function useChardb(): ChardbClient {
    const c = useContext(ChardbCtx);
    if (!c) throw new Error("useChardb must be used inside <ChardbProvider>");
    return c.client;
}

function sessionSnapshotKey(atom: AuthSessionAtom | null): string {
    if (!atom) return "missing";
    const snapshot = atom.get();
    return JSON.stringify([snapshot.isPending, snapshot.data ?? null]);
}

/** Read the Better Auth identity that owns the current CharDB scope. */
export function useChardbIdentity(): ChardbIdentity<ChardbOwnership> {
    const context = useContext(ChardbCtx);
    if (!context) throw new Error("useChardbIdentity must be used inside <ChardbProvider>");
    const subscribe = useCallback(
        (listener: () => void) => (context.sessionAtom ? context.sessionAtom.subscribe(listener) : () => {}),
        [context.sessionAtom]
    );
    const getSnapshot = useCallback(() => sessionSnapshotKey(context.sessionAtom), [context.sessionAtom]);
    useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

    if (!context.sessionAtom) {
        throw new Error("useChardbIdentity requires <ChardbProvider auth={createAuthClient(...)}>");
    }
    const snapshot = context.sessionAtom.get();
    const data = snapshot.data;
    const userId = data?.user?.id ?? data?.session?.userId;
    if (snapshot.isPending) {
        return {
            ownership: context.ownership,
            status: "loading",
            user: null,
            session: null,
            organizationId: null,
        } as ChardbIdentity<ChardbOwnership>;
    }
    if (!data?.user || !userId) {
        return {
            ownership: context.ownership,
            status: "signed-out",
            user: null,
            session: null,
            organizationId: null,
        } as ChardbIdentity<ChardbOwnership>;
    }
    const session: SessionRecord = data.session ?? { userId };
    if (context.ownership === "user") {
        return {
            ownership: "user",
            status: "ready",
            user: data.user,
            session,
            userId,
            organizationId: null,
        };
    }
    const organizationId = data.session?.activeOrganizationId ?? null;
    return organizationId
        ? { ownership: "organization", status: "ready", user: data.user, session, userId, organizationId }
        : {
              ownership: "organization",
              status: "select-organization",
              user: data.user,
              session,
              userId,
              organizationId: null,
          };
}

export interface UseQueryResult<T> {
    readonly data: T[] | undefined;
    readonly state: "idle" | "pending" | "live" | "error" | "refetching" | "closed";
}

export interface UseQueryOptions {
    /** Do not open a subscription until the caller's scope is ready. */
    readonly enabled?: boolean;
}

/**
 * Wire-shape stamp every `defineQuery` value carries. Pulled out of
 * the server `QueryFn` type so the React side can refer to it without
 * dragging in `@chardb/core/server`.
 */
export interface QueryHandleStamp<TArgs> {
    readonly __chardbRef: { toString(): string };
    /** Server argument validator; omitted by browser handle transforms. */
    readonly __chardbArgs?: StandardSchemaV1<unknown, TArgs>;
}

/**
 * Row inferred from a `defineQuery` handler's return type. Collection
 * queries (`Promise<readonly Row[]>` / `Promise<Row[]>`) resolve to
 * `Row`; scalar queries resolve to the awaited value.
 */
type RowOf<F> = F extends (...args: never[]) => Promise<infer R> ? (R extends readonly (infer Row)[] ? Row : R) : never;

type ArgsOf<F> = F extends (ctx: never, args: infer A) => unknown ? A : never;

export function useQuery<F extends (...args: never[]) => Promise<unknown>>(
    handle: F & QueryHandleStamp<ArgsOf<F>>,
    args: ArgsOf<F>,
    options: UseQueryOptions = {}
): UseQueryResult<RowOf<F>> {
    const client = useChardb();
    if (!isHandle(handle)) throw new TypeError("useQuery requires a defineQuery handle and raw JSON args");
    const enabled = options.enabled ?? true;
    const ref = handle.__chardbRef.toString();
    const ownedArgs = enabled ? snapshotSubscriptionArguments(args as RawJson) : ({} as RawJson);
    const argsIdentity = enabled ? stableJson(ownedArgs) : "disabled";
    const argsCache = useRef<{ readonly identity: string; readonly args: RawJson }>();
    if (argsCache.current?.identity !== argsIdentity) {
        argsCache.current = { identity: argsIdentity, args: ownedArgs };
    }
    const stableArgs = argsCache.current.args;
    const identity = useMemo(() => ({ client, ref, argsIdentity, enabled }), [client, ref, argsIdentity, enabled]);
    const [snapshot, setSnapshot] = useState<{
        readonly identity: typeof identity;
        readonly data: RowOf<F>[];
        readonly state: UseQueryResult<RowOf<F>>["state"];
    }>();
    useEffect(() => {
        if (!enabled) {
            setSnapshot(undefined);
            return;
        }
        let active = true;
        setSnapshot(current => (current?.identity === identity ? current : undefined));
        const sub = client.subscribe<RowOf<F>>(ref, stableArgs, (rows, state) => {
            if (active) setSnapshot({ identity, data: rows, state: state ?? "live" });
        });
        return () => {
            active = false;
            sub.unsubscribe();
        };
    }, [client, enabled, identity, ref, stableArgs]);
    const data = snapshot?.identity === identity ? snapshot.data : undefined;
    const state = enabled ? (snapshot?.identity === identity ? snapshot.state : "pending") : "idle";
    return { data, state };
}

function isHandle(v: unknown): v is ((...args: never[]) => unknown) & QueryHandleStamp<unknown> {
    return typeof v === "function" && typeof (v as { __chardbRef?: unknown }).__chardbRef !== "undefined";
}

export interface MutationFnLike {
    readonly __chardbRef: { toString(): string };
}

export function useMutation<TArgs extends RawJson, TResult>(
    fn: ((ctx: never, args: TArgs) => TResult) & MutationFnLike
): (args: TArgs) => Promise<Awaited<TResult>>;
export function useMutation<TArgs extends RawJson = RawJson, TResult = RawJson>(
    fn: MutationFnLike
): (args: TArgs) => Promise<TResult>;
export function useMutation(fn: MutationFnLike): (...args: never[]) => Promise<unknown> {
    const client = useChardb();
    return useCallback((args: RawJson) => client.mutate(fn.__chardbRef.toString(), args), [client, fn]) as (
        ...args: never[]
    ) => Promise<unknown>;
}

type OwnershipArgs<M extends ChardbOwnership> = M extends "organization"
    ? { readonly organizationId: string }
    : { readonly userId: string };

type PublicArgs<M extends ChardbOwnership, TArgs> = Omit<TArgs, keyof OwnershipArgs<M>>;

type QueryRow<TResult> = TResult extends readonly (infer Row)[] ? Row : TResult;

export interface CreateChardbReactClientOptions<M extends ChardbOwnership, A extends AuthClientLike>
    extends Pick<ChardbClientOptions, "clientId" | "mutationTimeoutMs" | "onSessionError"> {
    readonly ownership: M;
    /**
     * Creates the Better Auth client from the same public Worker URL CharDB
     * uses for sockets and files.
     */
    readonly auth: ChardbAuthFactory<A>;
    /**
     * Public HTTP origin of the CharDB Worker. Browser apps that use files
     * must expose the Worker routes at the app's origin.
     */
    readonly url: string;
}

export interface ChardbAuthFactoryOptions {
    /** Pass this value to Better Auth's `createAuthClient({ baseURL })`. */
    readonly baseURL: string;
}

export type ChardbAuthFactory<A extends AuthClientLike> = (options: ChardbAuthFactoryOptions) => A;

export interface ChardbOrganizationFileClient {
    upload(input: Omit<FileUploadInput, "organizationId">): Promise<FileUploadResult>;
    download(input: Omit<FileDownloadInput, "organizationId">): Promise<Response>;
    downloadUrl(input: Omit<FileDownloadInput, "organizationId">): string;
}

interface ChardbReactClientBase<M extends ChardbOwnership, A extends AuthClientLike> {
    /** Canonical public Worker origin used for auth, sockets, and file requests. */
    readonly url: string;
    /** The Better Auth client used by CharDB, kept intact with its inferred plugins. */
    readonly auth: A;
    readonly Provider: (props: PropsWithChildren) => ReactElement;
    readonly useIdentity: () => ChardbIdentity<M>;
    readonly useQuery: <TArgs extends Record<string, unknown> & OwnershipArgs<M>, TResult>(
        handle: ((ctx: never, args: TArgs) => Promise<TResult>) & QueryHandleStamp<TArgs>,
        args: PublicArgs<M, TArgs>
    ) => UseQueryResult<QueryRow<TResult>>;
    readonly useMutation: <TArgs extends RawJson & OwnershipArgs<M>, TResult>(
        fn: ((ctx: never, args: TArgs) => TResult) & MutationFnLike
    ) => (args: PublicArgs<M, TArgs>) => Promise<Awaited<TResult>>;
}

export type ChardbReactClient<M extends ChardbOwnership, A extends AuthClientLike> = ChardbReactClientBase<M, A> &
    (M extends "organization"
        ? { readonly useFile: (column: Column | FileRef) => ChardbOrganizationFileClient }
        : Record<never, never>);

/**
 * Configure the React SDK once from the Better Auth client and ownership mode.
 * Scoped hooks then inject the authenticated organization or user key, so app
 * code supplies only business arguments.
 */
export function createChardbReactClient<const M extends ChardbOwnership, const A extends AuthClientLike>(
    options: CreateChardbReactClientOptions<M, A>
): ChardbReactClient<M, A> {
    if (options.ownership !== "organization" && options.ownership !== "user") {
        throw new Error('chardb: ownership must be exactly "organization" or "user"');
    }
    const publicUrl = normalizePublicWorkerUrl(options.url);
    const endpoint = publicWorkerWebSocketUrl(publicUrl);
    const auth = options.auth({ baseURL: publicUrl });

    const Provider = ({ children }: PropsWithChildren): ReactElement => {
        return createElement(
            ChardbProvider,
            {
                ownership: options.ownership,
                auth,
                endpoint,
                ...(options.clientId !== undefined ? { clientId: options.clientId } : {}),
                ...(options.mutationTimeoutMs !== undefined ? { mutationTimeoutMs: options.mutationTimeoutMs } : {}),
                ...(options.onSessionError !== undefined ? { onSessionError: options.onSessionError } : {}),
            },
            children
        );
    };
    Provider.displayName = "ChardbClientProvider";

    const useIdentity = (): ChardbIdentity<M> => useChardbIdentity() as ChardbIdentity<M>;

    const useOwnedQuery = <TArgs extends Record<string, unknown> & OwnershipArgs<M>, TResult>(
        handle: ((ctx: never, args: TArgs) => Promise<TResult>) & QueryHandleStamp<TArgs>,
        args: PublicArgs<M, TArgs>
    ): UseQueryResult<QueryRow<TResult>> => {
        const identity = useIdentity();
        const scopeId =
            identity.status === "ready"
                ? options.ownership === "organization"
                    ? identity.organizationId
                    : identity.user.id
                : null;
        const scopeKey = options.ownership === "organization" ? "organizationId" : "userId";
        const scopedArgs = scopeId === null ? {} : { ...args, [scopeKey]: scopeId };
        return useQuery(handle, scopedArgs as TArgs, { enabled: scopeId !== null });
    };

    const useOwnedMutation = <TArgs extends RawJson & OwnershipArgs<M>, TResult>(
        fn: ((ctx: never, args: TArgs) => TResult) & MutationFnLike
    ): ((args: PublicArgs<M, TArgs>) => Promise<Awaited<TResult>>) => {
        const identity = useIdentity();
        const mutate = useMutation(fn);
        const ownership = options.ownership;
        return useCallback(
            (args: PublicArgs<M, TArgs>) => {
                if (identity.status !== "ready") {
                    return Promise.reject(new Error(`chardb: identity is ${identity.status}`));
                }
                const scopeKey = ownership === "organization" ? "organizationId" : "userId";
                const scopeId = ownership === "organization" ? identity.organizationId : identity.user.id;
                if (!scopeId) return Promise.reject(new Error("chardb: authenticated ownership scope is missing"));
                return mutate({ ...args, [scopeKey]: scopeId } as unknown as TArgs);
            },
            [identity, mutate, ownership]
        );
    };

    const useOwnedFile = (column: Column | FileRef): ChardbOrganizationFileClient => {
        const identity = useIdentity();
        const client = useMemo(() => createFileClient(column, { baseUrl: publicUrl }), [column]);
        const fileOrigin = publicUrl;
        const scope = useCallback((): string => {
            if (identity.status !== "ready" || identity.ownership !== "organization") {
                throw new Error(`chardb: organization identity is ${identity.status}`);
            }
            if (typeof window !== "undefined" && window.location.origin !== fileOrigin) {
                throw new Error(
                    "chardb: browser file routes must share the app origin; proxy the Worker routes through this origin"
                );
            }
            return identity.organizationId;
        }, [fileOrigin, identity]);
        return useMemo(
            () =>
                Object.freeze({
                    upload: async (input: Omit<FileUploadInput, "organizationId">) =>
                        await client.upload({ ...input, organizationId: scope() }),
                    download: async (input: Omit<FileDownloadInput, "organizationId">) =>
                        await client.download({ ...input, organizationId: scope() }),
                    downloadUrl: (input: Omit<FileDownloadInput, "organizationId">) =>
                        client.downloadUrl({ ...input, organizationId: scope() }),
                }),
            [client, scope]
        );
    };

    const base = {
        url: publicUrl,
        auth,
        Provider,
        useIdentity,
        useQuery: useOwnedQuery,
        useMutation: useOwnedMutation,
    };
    return Object.freeze(
        options.ownership === "organization" ? { ...base, useFile: useOwnedFile } : base
    ) as ChardbReactClient<M, A>;
}
