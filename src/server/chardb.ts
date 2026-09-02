/**
 * Build the Worker entrypoint from one ownership mode, Better Auth setup,
 * domain schema, API handles, and migration journal.
 *
 * The returned Hono app owns the mounted auth and database routes. It also
 * exposes the native DB entrypoint and Durable Object classes required by
 * Wrangler, so the Worker module can export them with one destructuring line.
 */

import { type Auth, type BetterAuthOptions, type BetterAuthPlugin, betterAuth } from "better-auth";
import { Hono } from "hono";
import { type ChardbAuthAdapterEnv, chardbAuthAdapter } from "../auth/chardb_adapter.ts";
import { type AuthOptionsInput, type ChardbAuth, type SynthesizedAuthSchema, defineAuth } from "../auth/synthesize.ts";
import type { ChardbBinding } from "../binding.ts";
import { CdbError } from "../errors.ts";
import { attachChardbAuthRuntimeEnv } from "./auth-runtime-context.ts";
import { type DB, configureDbBindingRuntime } from "./binding.ts";
import { collectCdbTables } from "./cdb-table-registry.ts";
import { type Catalog, configureCatalogRuntime } from "./do/catalog.ts";
import { type Cdb, configureCdbRuntime } from "./do/cdb.ts";
import type { GatewayJwtConfig } from "./do/gateway-auth-dispatch.ts";
import { type Gateway, configureGatewayRuntime } from "./do/gateway.ts";
import { type Resharder, configureResharderRuntime } from "./do/resharder.ts";
import {
    type ChardbEnv,
    type DefineChardbInput,
    type MountChardbOptions,
    defineChardb,
    mountChardb,
} from "./entrypoint.ts";
import { chardbHttpErrorHandler } from "./http-errors.ts";
import { sourceChardbEnv } from "./loopback.ts";
import { type OrganizationFileHttpAuth, handleOrganizationFileRequest } from "./organization-file-http.ts";
import { assertSchemaResourceJournal, collectSchemaFileResourceDescriptors } from "./resource-descriptors.ts";
import { type ChardbMigrationJournal, defineMigrations } from "./schema-migrations.ts";

/** Configure the Worker runtime and the one ownership mode its schema accepts. */
export interface ChardbFactoryInput<
    TPlugins extends readonly BetterAuthPlugin[],
    TSchema extends Record<string, unknown>,
> extends Omit<DefineChardbInput<TSchema>, "auth" | "refs" | "policy" | "manifest"> {
    /** The single ownership model accepted by this Worker's domain schema. */
    readonly ownership: "organization" | "user";
    /** A `defineAuth()` bundle or inline Better Auth options. */
    readonly auth?: ChardbAuth<TPlugins> | AuthOptionsInput<TPlugins>;
    /** Handler module namespace, usually `{ ...queries, ...mutations }`. */
    readonly api?: DefineChardbInput<TSchema>["refs"];
    /** Inline-route hook so the whole config can read top-to-bottom. */
    readonly routes?: (app: Hono<ChardbHonoEnv<TPlugins>>) => void;
    readonly authBasePath?: MountChardbOptions["authBasePath"];
    /** Immutable migration journal packaged with every Worker and Durable Object class. */
    readonly migrations?: ChardbMigrationJournal;
}

/** Hono app plus the bindings and classes required by the Worker module. */
export type ChardbAppEnv = ChardbEnv & { readonly DB: ChardbBinding };

type MutablePluginTuple<TPlugins extends readonly BetterAuthPlugin[]> = [...TPlugins];

/** Better Auth server instance available to Hono routes as `c.var.auth`. */
export type ChardbAuthRuntime<TPlugins extends readonly BetterAuthPlugin[]> = Auth<
    Omit<BetterAuthOptions, "plugins"> & { plugins: MutablePluginTuple<TPlugins> }
>;

type ChardbHonoEnv<TPlugins extends readonly BetterAuthPlugin[]> = {
    Bindings: ChardbAppEnv;
    Variables: { auth: ChardbAuthRuntime<TPlugins> };
};

export type ChardbApp<TPlugins extends readonly BetterAuthPlugin[], TSchema extends Record<string, unknown>> = Hono<
    ChardbHonoEnv<TPlugins>
> & {
    readonly fetch: (request: Request, env: ChardbEnv, ctx: ExecutionContext) => Promise<Response>;
    readonly auth: ChardbAuth<TPlugins>;
    readonly ownership: "organization" | "user";
    readonly schema: TSchema & SynthesizedAuthSchema<TPlugins>;
    readonly DB: typeof DB;
    readonly Cdb: typeof Cdb;
    readonly Catalog: typeof Catalog;
    readonly Gateway: typeof Gateway;
    readonly Resharder: typeof Resharder;
};

/** Distinguish a `defineAuth()` bundle from inline Better Auth options. */
function isChardbAuth<TPlugins extends readonly BetterAuthPlugin[]>(
    value: ChardbAuth<TPlugins> | AuthOptionsInput<TPlugins>
): value is ChardbAuth<TPlugins> {
    return (
        typeof value === "object" &&
        value !== null &&
        "options" in value &&
        "user" in value &&
        typeof (value as { user: unknown }).user === "object"
    );
}

export function chardb<
    const TPlugins extends readonly BetterAuthPlugin[] = [],
    const TSchema extends Record<string, unknown> = Record<string, unknown>,
>(input: ChardbFactoryInput<TPlugins, TSchema>): ChardbApp<TPlugins, TSchema> {
    if (input.ownership !== "organization" && input.ownership !== "user") {
        throw new CdbError({
            code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
            message: 'chardb: ownership must be exactly "organization" or "user"',
            hint: 'Pass ownership: "organization" or ownership: "user" to chardb().',
        });
    }
    const auth: ChardbAuth<TPlugins> =
        input.auth && isChardbAuth(input.auth)
            ? input.auth
            : defineAuth((input.auth ?? {}) as unknown as AuthOptionsInput<TPlugins>);

    const refsValue = input.api;
    const authBasePath = input.authBasePath ?? auth.options.basePath ?? "/api/auth";
    const jwtConfig = gatewayJwtConfigFromAuthOptions(auth.options, authBasePath);
    if (refsValue !== undefined && jwtConfig === null) {
        throw new CdbError({
            code: "CDB_AUTH_NOT_BOUND",
            message: "chardb: authenticated DB transport requires Better Auth's jwt() plugin",
            hint: "Add jwt() to defineAuth({ plugins: [...] }) before passing api to chardb().",
        });
    }

    const Chardb = defineChardb({
        schema: input.schema,
        auth,
        ...(refsValue ? { refs: refsValue } : {}),
    });
    const runtimeEntrypoint = Chardb as typeof Chardb & {
        readonly schema: Record<string, unknown>;
        readonly chardbManifest: import("./manifest.ts").ChardbManifest;
    };
    const migrationJournal = input.migrations ?? defineMigrations([]);
    let validatedSchema: Record<string, unknown> | undefined;
    const getValidatedSchema = (): Record<string, unknown> => {
        if (validatedSchema) return validatedSchema;
        const schema = runtimeEntrypoint.schema;
        assertOwnershipMode(schema, input.ownership);
        assertConfiguredAuthTargets(schema, auth.options);
        assertSchemaResourceJournal(schema, migrationJournal.migrations);
        validatedSchema = schema;
        return schema;
    };
    const ConfiguredCdb = configureCdbRuntime({
        schema: getValidatedSchema,
        manifest: () => runtimeEntrypoint.chardbManifest,
        migrations: () => migrationJournal,
    });
    const ConfiguredCatalog = configureCatalogRuntime({ migrations: () => migrationJournal });
    const ConfiguredGateway = configureGatewayRuntime({
        schema: getValidatedSchema,
        manifest: () => runtimeEntrypoint.chardbManifest,
        auth: jwtConfig,
    });
    const ConfiguredResharder = configureResharderRuntime({ schema: getValidatedSchema });
    const ConfiguredDB = configureDbBindingRuntime({
        schema: getValidatedSchema,
        manifest: () => runtimeEntrypoint.chardbManifest,
        auth: jwtConfig,
    });

    const authRuntime = buildDefaultAuthRuntime<TPlugins>(auth.options as BetterAuthOptions, migrationJournal.version);
    const hono = new Hono<ChardbHonoEnv<TPlugins>>();
    hono.onError(chardbHttpErrorHandler);
    hono.use("*", async (c, next) => {
        c.set("auth", authRuntime.get(c.env, c.req.raw));
        await next();
    });
    if (input.routes) input.routes(hono);

    // Snapshot Hono's own `.fetch` BEFORE handing the instance to
    // `mountChardb`. If we let `mountChardb` close over `hono.fetch` and
    // then overwrite `hono.fetch` with the wrapped handler below, the
    // wrapped handler would recurse into itself on the
    // non-reserved-prefix fall-through.
    const honoFetch = hono.fetch.bind(hono);

    // Auto-mount Better Auth at /api/auth/*. The adapter has to be
    // constructed per inbound env (the DO bindings live there), so we memoize a Better Auth
    // instance per env-identity to avoid re-running the adapter factory
    // on every request.
    //
    // The caller's plugin tuple and plugin options pass through unchanged.
    // Better Auth owns organization and user-management permissions;
    // CharDB enforces domain table policy independently.
    const mounted = mountChardb(
        Chardb,
        { fetch: honoFetch as Parameters<typeof mountChardb>[1]["fetch"] },
        {
            authHandler: async (request, env) => {
                try {
                    await catalogAuthAdmission(env, migrationJournal.version);
                    return authRuntime.get(env, request).handler(request);
                } catch (error) {
                    return chardbHttpErrorHandler(error instanceof Error ? error : new Error(String(error)));
                }
            },
            authBasePath,
            fileHandler: (request, env) =>
                handleOrganizationFileRequest({
                    request,
                    env,
                    auth: authRuntime.get(env, request) as unknown as OrganizationFileHttpAuth,
                    resources: collectSchemaFileResourceDescriptors(getValidatedSchema()),
                }),
        }
    );

    // Hono is an open object — augment it with the chardb-specific fields
    // and override `.fetch` with the prefix-aware mounted handler. The
    // user can still call `.get/.post/.put/.all/.route(...)` after the
    // factory returns; Hono's chaining methods mutate the same instance.
    // `.schema` is exposed as a getter so the merge with the user's
    // domain namespace happens lazily — `chardb({...})` can be called
    // mid-ESM-cycle (worker.ts ↔ schema.ts) before schema.ts's body has
    // finished evaluating, and the merge defers until first access.
    Object.defineProperty(hono, "schema", {
        enumerable: true,
        configurable: false,
        get: () => getValidatedSchema() as TSchema & SynthesizedAuthSchema<TPlugins>,
    });
    const merged = Object.assign(hono, {
        fetch: mounted.fetch,
        auth,
        ownership: input.ownership,
        DB: ConfiguredDB,
        Cdb: ConfiguredCdb,
        Catalog: ConfiguredCatalog,
        Gateway: ConfiguredGateway,
        Resharder: ConfiguredResharder,
    });
    return merged as ChardbApp<TPlugins, TSchema>;
}

function assertOwnershipMode(schema: Record<string, unknown>, ownership: "organization" | "user"): void {
    for (const { meta } of collectCdbTables(schema)) {
        const declared = meta.tenantKind === "user" ? "user" : meta.tenantKind === "org" ? "organization" : "none";
        if (declared === ownership) continue;
        const factory =
            meta.tenantKind === "user" ? "forUser()" : meta.selfTarget === "user" ? "forOrgUser()" : "forOrg()";
        throw new CdbError({
            code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
            message: `chardb: ownership "${ownership}" cannot include cdbTable "${meta.name}" from ${factory}`,
            hint:
                ownership === "organization"
                    ? "Use forOrg() or forOrgUser() for every domain table in this Worker."
                    : "Use forUser() for every domain table in this Worker.",
        });
    }
}

function assertConfiguredAuthTargets(schema: Record<string, unknown>, authOptions: BetterAuthOptions): void {
    const hasOrganizationPlugin = (authOptions.plugins ?? []).some(plugin => plugin.id === "organization");
    if (hasOrganizationPlugin) return;
    const orgTable = collectCdbTables(schema).find(({ meta }) => meta.authTarget === "organization");
    if (!orgTable) return;
    throw new CdbError({
        code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
        message: `chardb: cdbTable "${orgTable.meta.name}" uses forOrg(), but defineAuth() did not configure Better Auth's organization() plugin`,
        hint: "Add organization() to defineAuth({ plugins: [...] }).",
    });
}

interface BetterAuthJwtPluginOptions {
    readonly jwt?: {
        readonly issuer?: string;
        readonly audience?: string | readonly string[];
    };
    readonly jwks?: {
        readonly remoteUrl?: string;
        readonly jwksPath?: string;
        readonly keyPairConfig?: { readonly alg?: string };
    };
}

/** Derive the verifier contract from the exact Better Auth JWT plugin instance. */
export function gatewayJwtConfigFromAuthOptions(
    authOptions: BetterAuthOptions,
    authBasePath: string = authOptions.basePath ?? "/api/auth"
): GatewayJwtConfig | null {
    const plugin = (authOptions.plugins ?? []).find(candidate => candidate.id === "jwt");
    if (!plugin) return null;
    const options = (plugin as BetterAuthPlugin & { readonly options?: BetterAuthJwtPluginOptions }).options;
    const configuredOrigin = typeof authOptions.baseURL === "string" ? new URL(authOptions.baseURL).origin : undefined;
    const issuer = options?.jwt?.issuer ?? configuredOrigin;
    const audience = options?.jwt?.audience ?? configuredOrigin;
    return {
        ...(issuer !== undefined ? { issuer } : {}),
        ...(audience !== undefined ? { audience } : {}),
        algorithms: [options?.jwks?.keyPairConfig?.alg ?? "EdDSA"],
        ...(options?.jwks?.remoteUrl ? { jwksUrl: options.jwks.remoteUrl } : {}),
        authBasePath,
        jwksPath: options?.jwks?.jwksPath ?? "/jwks",
    };
}

/**
 * Build the per-env auth runtime. Better Auth constructs its own router
 * eagerly when called, so we defer the call until the first request
 * lands and capture the inbound `env` to wire the chardb adapter. The
 * resulting `betterAuth` instance is memoized per env-identity (one
 * worker isolate typically sees a single env reference, so this caches
 * one instance for the lifetime of the isolate; multi-isolate sharing
 * is by-design avoided so each isolate's adapter holds its own DO
 * stubs).
 */
function buildDefaultAuthRuntime<TPlugins extends readonly BetterAuthPlugin[]>(
    authOptions: BetterAuthOptions,
    expectedVersion: number
): {
    get: (env: ChardbEnv, request: Request) => ChardbAuthRuntime<TPlugins>;
} {
    const cache = new WeakMap<object, { key: string; instance: ChardbAuthRuntime<TPlugins> }>();
    const get = (env: ChardbEnv, request: Request): ChardbAuthRuntime<TPlugins> => {
        const e = sourceChardbEnv(env as unknown as object);
        const requestOrigin = new URL(request.url).origin;
        const cacheKey = authOptions.baseURL === undefined ? requestOrigin : "configured";
        const cached = cache.get(e);
        let instance = cached?.key === cacheKey ? cached.instance : undefined;
        if (!instance) {
            instance = betterAuth(
                attachChardbAuthRuntimeEnv(
                    {
                        ...authOptions,
                        // Workers can serve workers.dev, custom, preview, and local hosts.
                        // Pin an otherwise-unconfigured Better Auth instance to the
                        // canonical request origin instead of trusting an arbitrary
                        // Host header or forcing a wildcard allow-list.
                        ...(authOptions.baseURL === undefined ? { baseURL: requestOrigin } : {}),
                        database: chardbAuthAdapter({
                            env: env as unknown as ChardbAuthAdapterEnv,
                            recoveryGeneration: () => catalogAuthAdmission(env, expectedVersion),
                        }),
                        // Better Auth preserves its server-side options on endpoint contexts.
                        // The helper attaches this env so callbacks can use Worker bindings
                        // without process-global mutable state.
                    },
                    env
                )
            ) as unknown as ChardbAuthRuntime<TPlugins>;
            // Keep the cache bounded when one Worker serves many custom hosts.
            // A host switch replaces the previous unconfigured instance.
            cache.set(e, { key: cacheKey, instance });
        }
        return instance;
    };
    return { get };
}

async function catalogAuthAdmission(env: ChardbEnv, expectedVersion: number): Promise<number> {
    const catalog = env.CDB_CATALOG.get(env.CDB_CATALOG.idFromName("global")) as unknown as {
        schemaState(): Promise<{
            readonly activeVersion: number;
            readonly status: "active" | "migrating";
            readonly recoveryGeneration: number;
        }>;
    };
    const state = await catalog.schemaState();
    if (state.status !== "active" || state.activeVersion !== expectedVersion) {
        throw new CdbError({
            code: "CDB_STALE_EPOCH",
            message: "Catalog auth schema migration is not active",
            hint: "retry after the schema migration activates",
        });
    }
    if (!Number.isSafeInteger(state.recoveryGeneration) || state.recoveryGeneration < 0) {
        throw new CdbError({ code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog recovery generation is invalid" });
    }
    return state.recoveryGeneration;
}
