/**
 * Coverage for the `chardb({…})` mega-factory.
 *
 * The factory is the wrangler-ready worker entry: one call composes a
 * pre-built `defineAuth(...)` value (or inline `plugins`/`options`),
 * the user's Drizzle schema, the API refs, a Hono router, and the
 * `mountChardb` reserved-prefix handler. The shape it returns is a
 * Hono instance augmented with the DB entrypoint and four chardb Durable Object classes,
 * a lazy merged `.schema`, the `auth` value, and a chardb-mounted
 * `.fetch`.
 *
 * These tests pin the wire contract: which routes go where, that
 * Hono chaining keeps working after construction, that the DO classes
 * are present as direct fields, and that `.schema` is lazy enough to
 * survive an ESM-cycle-style namespace.
 */

import { describe, expect, test } from "bun:test";
import { admin } from "better-auth/plugins/admin";
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { eq } from "drizzle-orm";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { z } from "zod";
import { defineAuth } from "../../src/auth/synthesize.ts";
import type { ChardbBinding } from "../../src/binding.ts";
import { CdbError } from "../../src/errors.ts";
import { cdbPolicyDigest } from "../../src/server/cdb-policy.ts";
import { chardb } from "../../src/server/chardb.ts";
import { createApi, defineMutation } from "../../src/server/define.ts";
import { Cdb } from "../../src/server/do/cdb.ts";
import { Gateway } from "../../src/server/do/gateway.ts";
import { api as publicApi } from "../../src/server/index.ts";
import { defineMigrations } from "../../src/server/schema-migrations.ts";
import type { RawJson } from "../../src/types.ts";
import { vshardOf } from "../../src/vshard.ts";
import { forOrg, forOrgUser, forUser } from "../helpers/cdb-table.ts";

const organizationTable = sqliteTable("organization", { id: text("id").primaryKey() });
const userTable = sqliteTable("user", { id: text("id").primaryKey() });
const { cdbTable } = forOrg();
const items = cdbTable(
    "items",
    {
        id: text("id").primaryKey(),
        organizationId: text("organization_id")
            .notNull()
            .references(() => organizationTable.id),
        name: text("name").notNull(),
    },
    { partitionBy: "organizationId", roles: { member: { read: "*" } } }
);
const { cdbTable: orgUserTable } = forOrgUser();
const drafts = orgUserTable(
    "drafts",
    {
        id: text("id").primaryKey(),
        organizationId: text("organization_id")
            .notNull()
            .references(() => organizationTable.id),
        userId: text("user_id")
            .notNull()
            .references(() => userTable.id),
    },
    { roles: { self: { read: "*" } } }
);
const { cdbTable: userOwnedTable } = forUser();
const userNotes = userOwnedTable(
    "user_notes",
    {
        id: text("id").primaryKey(),
        userId: text("user_id")
            .notNull()
            .references(() => userTable.id),
    },
    { roles: { self: { read: "*" } } }
);

const auth = defineAuth({
    appName: "chardb-factory-test",
    baseURL: "https://example.com",
    plugins: [organization(), jwt()],
});

type RoutedArgs = { readonly organizationId: string } & { readonly [key: string]: RawJson };
type RoutedQueryArgs = { readonly organizationId: string; readonly limit: number };

function requestEnv(): unknown {
    const catalog = {
        async schemaState() {
            return { activeVersion: 0, status: "active" as const, recoveryGeneration: 0 };
        },
    };
    return { CDB_CATALOG: { idFromName: () => "global", get: () => catalog } };
}
const routedMutation = defineMutation<unknown, RoutedArgs, null>(() => null, {
    ref: "api/items#route",
    authority: "organization",
    singlePartition: true,
    partitionKey: args => args.organizationId,
});
const itemApi = createApi({ items });
const routedQuery = itemApi.query({
    ref: "api/items#list",
    args: z.object({ organizationId: z.string(), limit: z.number().int().default(25) }),
    query: (db, args: RoutedQueryArgs) =>
        db
            .select()
            .from(items)
            .where(eq(items.organizationId, args.organizationId))
            .orderBy(items.id)
            .limit(args.limit),
});
const queryWithNonJsonTransform = itemApi.query({
    ref: "api/items#nonJsonTransform",
    args: z.object({ organizationId: z.string(), at: z.string().transform(() => new Date(0)) }),
    query: (db, args) => db.select().from(items).where(eq(items.organizationId, args.organizationId)),
});

describe("chardb({…})", () => {
    test("binds automatic refs before direct Worker routes use the handles", () => {
        const save = publicApi.mutation({
            authority: "organization",
            partitionKey: (args: RoutedArgs) => args.organizationId,
            handler: () => null,
        });
        chardb({ ownership: "organization", auth, schema: { items }, api: { save } });
        expect(String(save.__chardbRef)).toBe("mutation#save");
    });

    test("rejects missing and unknown ownership modes at construction", () => {
        const callFromJavaScript = chardb as unknown as (input: Record<string, unknown>) => unknown;
        for (const ownership of [undefined, "global", null]) {
            expect(() => callFromJavaScript({ ownership, schema: {} })).toThrow(CdbError);
            try {
                callFromJavaScript({ ownership, schema: {} });
            } catch (error) {
                expect(error).toMatchObject({
                    code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                    message: 'chardb: ownership must be exactly "organization" or "user"',
                });
            }
        }
    });

    test("fails at construction when api transport is configured without jwt()", () => {
        const authWithoutJwt = defineAuth({ plugins: [organization()] });
        let caught: unknown;
        try {
            chardb({ ownership: "organization", auth: authWithoutJwt, schema: { items }, api: { routedMutation } });
        } catch (error) {
            caught = error;
        }
        expect(caught).toBeInstanceOf(CdbError);
        expect(caught).toMatchObject({
            code: "CDB_AUTH_NOT_BOUND",
            message: "chardb: authenticated DB transport requires Better Auth's jwt() plugin",
        });
    });

    test("allows a core-only route setup when authenticated DB transport is not configured", () => {
        const coreAuth = defineAuth({});
        const app = chardb({ ownership: "user", auth: coreAuth, schema: {} });
        expect(app.auth).toBe(coreAuth);
        expect(app.schema.user).toBeDefined();
    });

    test("fails clearly when forOrg() is used without organization()", () => {
        const authWithoutOrganization = defineAuth({ plugins: [jwt()] });
        const app = chardb({ ownership: "organization", auth: authWithoutOrganization, schema: { items } });
        let caught: unknown;
        try {
            void app.schema;
        } catch (error) {
            caught = error;
        }
        expect(caught).toBeInstanceOf(CdbError);
        expect(caught).toMatchObject({
            code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
            message:
                'chardb: cdbTable "items" uses forOrg(), but defineAuth() did not configure Better Auth\'s organization() plugin',
        });
    });

    test("rejects forOrgUser() when Better Auth omits organization()", () => {
        const authWithoutOrganization = defineAuth({ plugins: [jwt()] });
        const app = chardb({ ownership: "organization", auth: authWithoutOrganization, schema: { drafts } });

        expect(() => void app.schema).toThrow(CdbError);
        try {
            void app.schema;
        } catch (error) {
            expect(error).toMatchObject({
                code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                hint: "Add organization() to defineAuth({ plugins: [...] }).",
            });
        }
    });

    test("rejects a user-owned table in organization mode", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items, userNotes } });

        expect(() => void app.schema).toThrow(CdbError);
        try {
            void app.schema;
        } catch (error) {
            expect(error).toMatchObject({
                code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                message: 'chardb: ownership "organization" cannot include cdbTable "user_notes" from forUser()',
            });
        }
    });

    test("rejects organization-owned tables in user mode", () => {
        const app = chardb({ ownership: "user", auth, schema: { userNotes, items } });

        expect(() => void app.schema).toThrow(CdbError);
        try {
            void app.schema;
        } catch (error) {
            expect(error).toMatchObject({
                code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                message: 'chardb: ownership "user" cannot include cdbTable "items" from forOrg()',
            });
        }
    });

    test("accepts forOrg() and forOrgUser() together in organization mode", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items, drafts } });

        expect(app.ownership).toBe("organization");
        expect(app.schema.items).toBe(items);
        expect(app.schema.drafts).toBe(drafts);
    });

    test("accepts only forUser() tables in user mode", () => {
        const app = chardb({ ownership: "user", auth: defineAuth({}), schema: { userNotes } });

        expect(app.ownership).toBe("user");
        expect(app.schema.userNotes).toBe(userNotes);
    });

    test("returns a Hono instance the user can chain routes on", async () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        app.get("/hello", c => c.text("world"));
        const res = await app.fetch(
            new Request("https://example.com/hello"),
            requestEnv() as Parameters<typeof app.fetch>[1],
            { waitUntil() {}, passThroughOnException() {}, props: undefined } as Parameters<typeof app.fetch>[2]
        );
        expect(res.status).toBe(200);
        expect(res.headers.get("Cf-Chardb-Server-Version")).toBe("0.1.0");
        expect(res.headers.get("Server-Timing")).toMatch(/^cdb;dur=\d+;desc="chardb total"$/);
        expect(res.headers.get("cf-chardb-correlation-id")).toBeTruthy();
        expect(await res.text()).toBe("world");
    });

    test("maps typed route failures through the shared HTTP boundary", async () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        app.get("/forbidden", () => {
            throw new CdbError({ code: "CDB_FORBIDDEN", message: "membership revoked" });
        });
        const response = await app.fetch(
            new Request("https://example.com/forbidden"),
            requestEnv() as Parameters<typeof app.fetch>[1],
            { waitUntil() {}, passThroughOnException() {}, props: undefined } as Parameters<typeof app.fetch>[2]
        );
        expect(response.status).toBe(403);
        expect((await response.json()) as unknown).toMatchObject({
            error: { code: "CDB_FORBIDDEN", message: "membership revoked", retryable: false },
        });
    });

    test("rejects Better Auth at the HTTP boundary while its Catalog schema is fenced", async () => {
        const app = chardb({
            ownership: "organization",
            auth: defineAuth({}),
            schema: {},
            migrations: defineMigrations([{ version: 1, name: "initial", statements: ["SELECT 1"] }]),
        });
        const catalog = {
            async schemaState() {
                return { activeVersion: 0, status: "migrating" as const };
            },
            async authAdapterRpc() {
                throw new Error("fenced auth must not reach the adapter RPC");
            },
        };
        const env = {
            CDB_CATALOG: { idFromName: () => "global", get: () => catalog },
        } as unknown as Parameters<typeof app.fetch>[1];
        const response = await app.fetch(new Request("https://example.com/api/auth/get-session"), env, {
            waitUntil() {},
            passThroughOnException() {},
            props: undefined,
        } as Parameters<typeof app.fetch>[2]);

        expect(response.status).toBe(409);
        expect((await response.json()) as unknown).toMatchObject({
            error: {
                code: "CDB_STALE_EPOCH",
                message: "Catalog auth schema migration is not active",
                retryable: true,
            },
        });
    });

    test("pins unconfigured Better Auth instances to each canonical request origin", async () => {
        const app = chardb({ ownership: "user", auth: defineAuth({}), schema: {} });
        app.get("/auth-origin", c => c.json({ baseURL: c.var.auth.options.baseURL }));
        const env = requestEnv() as Parameters<typeof app.fetch>[1];
        const ctx = {
            waitUntil() {},
            passThroughOnException() {},
            props: undefined,
        } as Parameters<typeof app.fetch>[2];

        const local = await app.fetch(new Request("http://127.0.0.1:8787/auth-origin"), env, ctx);
        const deployed = await app.fetch(new Request("https://app.example/auth-origin"), env, ctx);
        expect((await local.json()) as unknown).toEqual({ baseURL: "http://127.0.0.1:8787" });
        expect((await deployed.json()) as unknown).toEqual({ baseURL: "https://app.example" });
    });

    test("binds the typed Better Auth runtime to Hono routes and caches it per env", async () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        let firstRuntime: unknown;
        app.get("/auth-runtime", c => {
            const getSession: typeof c.var.auth.api.getSession = c.var.auth.api.getSession;
            const createOrganization: typeof c.var.auth.api.createOrganization = c.var.auth.api.createOrganization;
            const reused = firstRuntime === undefined ? null : firstRuntime === c.var.auth;
            firstRuntime ??= c.var.auth;
            return c.json({
                hasGetSession: typeof getSession === "function",
                hasCreateOrganization: typeof createOrganization === "function",
                reused,
            });
        });

        const env = requestEnv() as Parameters<typeof app.fetch>[1];
        const db = {
            async executeQuery() {
                return { ok: true as const, result: null };
            },
            async executeMutation() {
                return { ok: true as const, cookie: "cookie", ran: true, result: null, rowsAffected: 0 };
            },
        } satisfies ChardbBinding;
        const ctx = {
            exports: { DB: db },
            waitUntil() {},
            passThroughOnException() {},
            props: undefined,
        } as unknown as Parameters<typeof app.fetch>[2];
        const request = (requestEnv: Parameters<typeof app.fetch>[1] = env): Promise<Response> =>
            app.fetch(new Request("https://example.com/auth-runtime"), requestEnv, ctx);

        expect((await (await request()).json()) as unknown).toEqual({
            hasGetSession: true,
            hasCreateOrganization: true,
            reused: null,
        });
        expect((await (await request()).json()) as unknown).toEqual({
            hasGetSession: true,
            hasCreateOrganization: true,
            reused: true,
        });
        expect((await (await request(requestEnv() as Parameters<typeof app.fetch>[1])).json()) as unknown).toEqual({
            hasGetSession: true,
            hasCreateOrganization: true,
            reused: false,
        });
    });

    test("binds Better Auth before inline routes run", async () => {
        const app = chardb({
            ownership: "organization",
            auth,
            schema: { items },
            routes: router => {
                router.get("/inline-auth", c => c.json({ available: typeof c.var.auth.api.getSession === "function" }));
            },
        });
        const unavailableCatalogEnv = {
            CDB_CATALOG: {
                idFromName: () => "global",
                get: () => ({
                    schemaState: async () => {
                        throw new Error("Catalog should stay lazy for this route");
                    },
                }),
            },
        };
        const response = await app.fetch(
            new Request("https://example.com/inline-auth"),
            unavailableCatalogEnv as unknown as Parameters<typeof app.fetch>[1],
            { waitUntil() {}, passThroughOnException() {}, props: undefined } as Parameters<typeof app.fetch>[2]
        );
        expect((await response.json()) as unknown).toEqual({ available: true });
    });

    test("passes the caller's exact Better Auth plugin profile to the runtime", async () => {
        const organizationPlugin = organization();
        const adminPlugin = admin();
        const plugins = [organizationPlugin, adminPlugin] as const;
        const exactAuth = defineAuth({ baseURL: "https://example.com", plugins });
        const app = chardb({ ownership: "organization", auth: exactAuth, schema: { items } });
        app.get("/auth-profile", c => {
            const runtimePlugins = c.var.auth.options.plugins;
            return c.json({
                sameTuple: runtimePlugins === plugins,
                sameOrganizationPlugin: runtimePlugins?.[0] === organizationPlugin,
                sameAdminPlugin: runtimePlugins?.[1] === adminPlugin,
            });
        });

        const response = await app.fetch(
            new Request("https://example.com/auth-profile"),
            requestEnv() as Parameters<typeof app.fetch>[1],
            { waitUntil() {}, passThroughOnException() {}, props: undefined } as Parameters<typeof app.fetch>[2]
        );
        expect((await response.json()) as unknown).toEqual({
            sameTuple: true,
            sameOrganizationPlugin: true,
            sameAdminPlugin: true,
        });
    });

    test("exposes the native DB loopback as typed Hono environment state", async () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        const db = {
            async executeQuery() {
                return { ok: true as const, result: null };
            },
            async executeMutation() {
                return { ok: true as const, cookie: "cookie", ran: true, result: null, rowsAffected: 0 };
            },
        } satisfies ChardbBinding;
        app.get("/binding", c => c.json({ available: c.env.DB === db }));
        const res = await app.fetch(
            new Request("https://example.com/binding"),
            requestEnv() as Parameters<typeof app.fetch>[1],
            {
                exports: { DB: db },
                waitUntil() {},
                passThroughOnException() {},
                props: undefined,
            } as unknown as Parameters<typeof app.fetch>[2]
        );
        expect((await res.json()) as unknown).toEqual({ available: true });
    });

    test("the DB entrypoint and four Durable Object classes are direct configured fields", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        // Existence + identity — these are the named exports wrangler binds.
        expect(typeof app.DB).toBe("function");
        expect(typeof app.Cdb).toBe("function");
        expect(app.Cdb).not.toBe(Cdb);
        expect(typeof app.Catalog).toBe("function");
        expect(typeof app.Gateway).toBe("function");
        expect(app.Gateway).not.toBe(Gateway);
        expect(typeof app.Resharder).toBe("function");
        expect("BlobMeta" in app).toBe(false);
        expect("GsiShard" in app).toBe(false);
    });

    test("the configured Gateway resolves mutation refs from the factory api manifest", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items }, api: { routedMutation } });
        const gateway = Object.create(app.Gateway.prototype) as InstanceType<typeof app.Gateway>;
        expect(
            gateway.routeMutation({
                ref: routedMutation.__chardbRef,
                args: { organizationId: "org-7" },
            })
        ).toEqual({
            ok: true,
            vshard: Number(vshardOf(["org-7"])),
            authority: "organization",
            partitionKey: "org-7",
            args: { organizationId: "org-7" },
        });
        const missing = gateway.routeMutation({ ref: "api.ts#missing", args: {} });
        expect(missing.ok).toBe(false);
        if (!missing.ok) expect(missing.error.code).toBe("CDB_REF_NOT_FOUND");
    });

    test("the configured Gateway validates args and derives a planned query route", async () => {
        const app = chardb({
            ownership: "organization",
            auth,
            schema: { items },
            api: { routedQuery },
        });
        const gateway = Object.create(app.Gateway.prototype) as InstanceType<typeof app.Gateway>;
        const routed = await gateway.routeQuery({ ref: routedQuery.__chardbRef, args: { organizationId: "org-7" } });
        expect(routed.ok).toBe(true);
        if (!routed.ok) throw new Error("expected query routing to succeed");
        expect(routed.intent).toEqual({
            kind: "select",
            tables: ["items"],
            partitionKey: { table: "items", column: "organization_id", values: ["org-7"] },
            joinShape: "colocated",
            intervals: expect.any(Array),
        });
        expect(routed.args).toEqual({ organizationId: "org-7", limit: 25 });
        expect(routed.authority).toBe("organization");
        expect(routed.partitionKey).toBe("org-7");
        const policyDigest = cdbPolicyDigest({ items }, routed.intent.tables);
        expect(routed.policyDigest).toBe(policyDigest);
        expect(routed.queryHash).toContain("planHash");

        const invalid = await gateway.routeQuery({ ref: routedQuery.__chardbRef, args: { organizationId: 7 } });
        expect(invalid.ok).toBe(false);
        if (!invalid.ok) expect(invalid.error.code).toBe("CDB_INVALID_ARGS");
    });

    test("query routing rejects non-JSON validator transforms", async () => {
        const app = chardb({
            ownership: "organization",
            auth,
            schema: { items },
            api: { queryWithNonJsonTransform },
        });
        const gateway = Object.create(app.Gateway.prototype) as InstanceType<typeof app.Gateway>;
        const result = await gateway.routeQuery({
            ref: queryWithNonJsonTransform.__chardbRef,
            args: { organizationId: "org-7", at: "now" },
        });
        expect(result).toMatchObject({ ok: false, error: { code: "CDB_INVALID_ARGS" } });
    });

    test("query routing caps raw and transformed arguments before compiling plans", async () => {
        let rawValidatorRuns = 0;
        let rawCompilerRuns = 0;
        const rawGuarded = itemApi.query({
            ref: "api/items#rawArgumentLimit",
            args: z.unknown().transform(value => {
                rawValidatorRuns++;
                return value as { organizationId: string };
            }),
            query: (db, args) => {
                rawCompilerRuns++;
                return db.select().from(items).where(eq(items.organizationId, args.organizationId));
            },
        });
        let transformedCompilerRuns = 0;
        const transformedGuarded = itemApi.query({
            ref: "api/items#transformedArgumentLimit",
            args: z.object({ organizationId: z.string() }).transform(args => ({
                ...args,
                padding: "é".repeat(262_139),
            })),
            query: (db, args) => {
                transformedCompilerRuns++;
                return db.select().from(items).where(eq(items.organizationId, args.organizationId));
            },
        });
        const app = chardb({
            ownership: "organization",
            auth,
            schema: { items },
            api: { rawGuarded, transformedGuarded },
        });
        const gateway = Object.create(app.Gateway.prototype) as InstanceType<typeof app.Gateway>;

        await expect(
            gateway.routeQuery({
                ref: rawGuarded.__chardbRef,
                args: { value: "é".repeat(262_139) },
            })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_INVALID_ARGS", retryable: false } });
        expect(rawValidatorRuns).toBe(0);
        expect(rawCompilerRuns).toBe(0);

        await expect(
            gateway.routeQuery({
                ref: transformedGuarded.__chardbRef,
                args: { organizationId: "org-7" },
            })
        ).resolves.toMatchObject({ ok: false, error: { code: "CDB_INVALID_ARGS", retryable: false } });
        expect(transformedCompilerRuns).toBe(0);
    });

    test("`auth` is the pre-built bundle when supplied", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        expect(app.auth).toBe(auth);
        expect(app.auth.user).toBeDefined();
        expect(app.auth.organization).toBeDefined();
    });

    test("inline `auth: { plugins, appName }` builds the bundle without a separate defineAuth call", () => {
        const app = chardb({
            ownership: "organization",
            auth: { appName: "inline-app", plugins: [organization()] },
            schema: { items },
        });
        expect(app.auth.options.appName).toBe("inline-app");
        expect(app.auth.user).toBeDefined();
        expect(app.auth.organization).toBeDefined();
    });

    test("`.schema` is a lazy getter (no eager spread of the schema namespace)", () => {
        let accessCount = 0;
        // Mimic an ESM-cycle namespace where touching the proxy counts.
        const schema = new Proxy(
            { items },
            {
                get(target, key) {
                    accessCount++;
                    return Reflect.get(target, key);
                },
                ownKeys(target) {
                    accessCount++;
                    return Reflect.ownKeys(target);
                },
                getOwnPropertyDescriptor(target, key) {
                    accessCount++;
                    return Reflect.getOwnPropertyDescriptor(target, key);
                },
            }
        );
        const app = chardb({ ownership: "organization", auth, schema });
        // Constructing the factory must NOT iterate the schema namespace —
        // the merge with auth tables is deferred to first `.schema` read.
        expect(accessCount).toBe(0);
        void app.schema;
        expect(accessCount).toBeGreaterThan(0);
    });

    test(".schema merges synthesized auth tables with the domain namespace", () => {
        const app = chardb({ ownership: "organization", auth, schema: { items } });
        const organizationIdColumn: typeof app.auth.organization.id = app.schema.organization.id;
        expect(app.schema.items).toBe(items);
        expect(app.schema.user).toBeDefined();
        expect(organizationIdColumn).toBeDefined();
    });
});
