import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { CdbError } from "@chardb/core";
import { defineMigrations, defineSchemaBaseline } from "@chardb/core/server";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { deleteMessage, editMessage, postMessage } from "../../src/server/api.ts";
import { auth } from "../../src/server/auth.ts";
import { selectMigrationInputs } from "../../src/server/migrations/history.ts";
import { initialSchema17 } from "../../src/server/migrations/v1-better-auth-17.ts";
import { initialSchema } from "../../src/server/migrations/v1.ts";
import { listMessages } from "../../src/server/queries.ts";
import * as schema from "../../src/server/schema.ts";
import { messages } from "../../src/server/schema.ts";

describe("tutorial Better Auth integration", () => {
    test("uses Better Auth's organization and JWT plugins", () => {
        const pluginIds = (auth.options.plugins ?? []).map(plugin => plugin.id);
        expect(pluginIds).toContain("anonymous");
        expect(pluginIds).toContain("organization");
        expect(pluginIds).toContain("jwt");
    });

    test("trusts only HTTP loopback origins during local development", async () => {
        const trustedOrigins = auth.options.trustedOrigins;
        expect(typeof trustedOrigins).toBe("function");
        if (typeof trustedOrigins !== "function") throw new Error("expected dynamic trusted origins");

        expect(
            await trustedOrigins(
                new Request("http://127.0.0.1:8787/api/auth/organization/create", {
                    headers: { origin: "http://127.0.0.1:5173" },
                })
            )
        ).toEqual(["http://127.0.0.1:5173"]);
        expect(
            await trustedOrigins(
                new Request("https://chat.example.com/api/auth/organization/create", {
                    headers: { origin: "http://127.0.0.1:5173" },
                })
            )
        ).toEqual([]);
        expect(
            await trustedOrigins(
                new Request("http://127.0.0.1:8787/api/auth/organization/create", {
                    headers: { origin: "https://attacker.example" },
                })
            )
        ).toEqual([]);
    });

    test("uses the React client and native organization workflow", async () => {
        const root = resolve(import.meta.dir, "../..");
        const [app, authSource, schemaSource, worker, wrangler, vite] = await Promise.all([
            readFile(resolve(root, "src/web/App.tsx"), "utf8"),
            readFile(resolve(root, "src/server/auth.ts"), "utf8"),
            readFile(resolve(root, "src/server/schema.ts"), "utf8"),
            readFile(resolve(root, "src/server/worker.ts"), "utf8"),
            readFile(resolve(root, "wrangler.template.toml"), "utf8"),
            readFile(resolve(root, "vite.config.ts"), "utf8"),
        ]);

        expect(app).toContain('from "better-auth/react"');
        expect(app).toContain('from "@chardb/react"');
        expect(app).toContain("const db = createChardbReactClient({");
        expect(app).toContain('ownership: "organization"');
        expect(app).toContain("auth: ({ baseURL }) =>");
        expect(app).toContain("plugins: [anonymousClient(), organizationClient(), jwtClient()]");
        expect(app).toContain("const session = db.auth.useSession()");
        expect(app).toContain("anonymousSignInRequest ??=");
        expect(app).toContain("Sign-in failed:");
        expect(app).toContain("const identity = db.useIdentity()");
        expect(app).toContain("const organizations = db.auth.useListOrganizations()");
        expect(app).toContain("db.auth.organization.create({");
        expect(app).toContain("db.auth.organization.setActive({ organizationId");
        expect(app).not.toContain("session.refetch");
        expect(app).toContain("db.useQuery(listMessages, { limit: 50 })");
        expect(app).toContain("db.useMutation(postMessage)");
        expect(app).toContain("<Messages key={activeOrganizationId}");
        expect(app).not.toContain("<ChardbProvider");
        expect(app).not.toContain("organizationId,\n                body");
        expect(app).not.toContain("DEMO_ORG_ID");
        expect(app).not.toContain("useSession.get()");
        expect(app).not.toContain("useSession.subscribe(");
        expect(authSource).not.toContain("DBAdapter");
        expect(authSource).not.toContain("databaseHooks");
        expect(schemaSource).toContain('owner: "*"');
        expect(worker).toContain('authBasePath: "/api/auth"');
        expect(worker).toContain("{ DB, Catalog, Cdb, Gateway, Resharder }");
        expect(wrangler).toContain('new_sqlite_classes = ["Cdb", "Catalog", "Gateway", "Resharder"]');
        expect(Bun.TOML.parse(wrangler)).toHaveProperty("vars.CHAT_SCHEMA_HISTORY", "better-auth-1.7");
        expect(Bun.TOML.parse(wrangler)).toHaveProperty("durable_objects.bindings", [
            { name: "CDB_CATALOG", class_name: "Catalog" },
            { name: "CDB_SHARD", class_name: "Cdb" },
            { name: "CDB_GATEWAY", class_name: "Gateway" },
            { name: "CDB_RESHARD", class_name: "Resharder" },
        ]);
        expect(wrangler).toContain('run_worker_first = ["/ws", "/_chardb/*", "/api/*", "/health"]');
        expect(vite).toContain('const workerOrigin = process.env.CHARDB_URL ?? "http://127.0.0.1:8787"');
        expect(vite).not.toContain("localhost:8787");
    });
});

describe("tutorial migrations", () => {
    const tablesAfter = (
        steps: readonly { statements: readonly string[]; catalogStatements?: readonly string[] }[]
    ) => {
        const sqlite = new Database(":memory:");
        try {
            for (const step of steps)
                for (const sql of [...step.statements, ...(step.catalogStatements ?? [])]) sqlite.run(sql);
            const tables = sqlite
                .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
                .all();
            const rows = (sql: string, table: string) => sqlite.query(sql).all(table);
            return Object.fromEntries(
                tables.map(({ name }) => [
                    name,
                    {
                        columns: rows(
                            `SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?) ORDER BY name`,
                            name
                        ),
                        indexes: rows(
                            `SELECT l.name, l."unique", l.origin, group_concat(i.name) AS columns
                             FROM pragma_index_list(?) AS l, pragma_index_info(l.name) AS i
                             GROUP BY l.name ORDER BY l.name`,
                            name
                        ),
                        foreignKeys: rows(
                            `SELECT "table", "from", "to", on_delete FROM pragma_foreign_key_list(?) ORDER BY "from"`,
                            name
                        ),
                    },
                ])
            );
        } finally {
            sqlite.close();
        }
    };

    test("keeps both deployed version-one digests", () => {
        const original = defineMigrations([initialSchema]);
        const upgraded = defineMigrations([initialSchema17]);
        expect(original.migrations[0]?.digest).toBe("2e6bcbd11c4cee1f410d1e350223b8ae5e92b4c44696f9727eedc2c6253629b8");
        expect(original.digest).toBe("ed228fceeca5b32cc1efe50e980d7e40f535ebcace2e455da676d91a3be0b2c6");
        expect(upgraded.migrations[0]?.digest).toBe("7f020773c07c8a535dc5a04f377bb6ae36f258bbda3cbec37b6c48561655eb22");
        expect(upgraded.digest).toBe("01d40904ac78655203372ea726f4e4052f16f2540ec14fec2096b90ebf232566");
    });

    test("selects the original history when unset and rejects unknown settings", () => {
        expect(selectMigrationInputs(undefined)).toEqual(selectMigrationInputs("better-auth-1.6"));
        expect(selectMigrationInputs("better-auth-1.7")[0]).toBe(initialSchema17);
        for (const setting of ["", "better-auth-1.8", null, 1]) {
            expect(() => selectMigrationInputs(setting)).toThrow("CHAT_SCHEMA_HISTORY");
        }
    });

    test("both histories end at the tables the current auth and domain schema declare", () => {
        const current = defineSchemaBaseline({
            version: 1,
            name: "current",
            domainSchema: schema,
            authOptions: auth.options,
        });
        for (const history of ["better-auth-1.6", "better-auth-1.7"]) {
            const migrations = defineMigrations(selectMigrationInputs(history));
            expect(migrations.version).toBe(2);
            expect(tablesAfter(migrations.migrations)).toEqual(tablesAfter([current]));
        }
    });
});

describe("tutorial organization flow", () => {
    test("defaults the live query to the same bounded direct-read shape", async () => {
        const internals = listMessages as typeof listMessages & {
            readonly __chardbValidateArgs: (args: unknown) => Promise<{
                readonly organizationId: string;
                readonly limit: number;
            }>;
            readonly __chardbCompilePlan: (args: { readonly organizationId: string; readonly limit: number }) => {
                readonly authority: string;
                readonly partitionKey: string;
                readonly limit: number;
                readonly orderBy: readonly { readonly column: string; readonly direction: string }[];
            };
        };

        const args = await internals.__chardbValidateArgs({ organizationId: "org-1" });
        const plan = internals.__chardbCompilePlan(args);

        expect(args).toEqual({ organizationId: "org-1", limit: 50 });
        expect(plan.authority).toBe("organization");
        expect(plan.partitionKey).toBe("org-1");
        expect(plan.limit).toBe(50);
        expect(plan.orderBy).toEqual([
            { column: "created_at", direction: "desc" },
            { column: "id", direction: "desc" },
        ]);
    });

    test("rejects a mutation when its active organization and route disagree", () => {
        let error: unknown;
        try {
            postMessage(
                {
                    db: {} as never,
                    auth: { userId: "user-1", tenantId: "other-org", claims: {} },
                },
                {
                    id: "message-1",
                    organizationId: "org-1",
                    body: "hello",
                    clientCreatedAt: 1,
                }
            );
        } catch (cause) {
            error = cause;
        }
        expect(error).toBeInstanceOf(CdbError);
        expect(error).toMatchObject({ code: "CDB_FORBIDDEN", retryable: false });
    });
});

describe("tutorial message lifecycle", () => {
    test("members can edit and delete their own rows, with organization isolation", () => {
        const sqlite = new Database(":memory:");
        try {
            for (const statement of initialSchema.statements) sqlite.run(statement);
            const raw = drizzle(sqlite, { schema: { messages } });
            const context = (userId: string, tenantId = "org-1", role = "member") => {
                const auth = { userId, tenantId, role, claims: {} };
                return { db: raw, auth };
            };
            const args = { id: "m1", organizationId: "org-1" };
            const denied = (attempt: () => unknown) =>
                expect(attempt).toThrow(/missing or not yours|does not match the routed partition/);
            postMessage(context("alice"), { ...args, body: "hello", clientCreatedAt: 1 });
            expect(raw.select().from(messages).get()).toMatchObject({
                authorId: "alice",
                organizationId: "org-1",
                body: "hello",
            });
            denied(() => editMessage(context("bob"), { ...args, body: "hijacked" }));
            denied(() => deleteMessage(context("bob"), args));
            denied(() => editMessage(context("alice", "org-2"), { ...args, body: "wrong org" }));
            denied(() => deleteMessage(context("alice", "org-2"), args));
            expect(raw.select().from(messages).get()?.body).toBe("hello");
            expect(editMessage(context("alice"), { ...args, body: "edited" })).toEqual({ id: "m1" });
            expect(raw.select().from(messages).get()?.body).toBe("edited");
            expect(editMessage(context("bob", "org-1", "admin"), { ...args, body: "moderated" })).toEqual({ id: "m1" });
            expect(raw.select().from(messages).get()?.body).toBe("moderated");
            expect(deleteMessage(context("alice"), args)).toEqual({ id: "m1" });
            denied(() => deleteMessage(context("alice"), args));
            expect(raw.select().from(messages).all()).toEqual([]);
        } finally {
            sqlite.close();
        }
    });

    test("validates edits before executing them", () => {
        const validate = (editMessage as typeof editMessage & { __chardbValidateArgs(args: unknown): unknown })
            .__chardbValidateArgs;
        const key = { organizationId: "org-1", id: "m1" };
        expect(validate({ ...key, body: "  edited  " })).toEqual({ ...key, body: "edited" });
        for (const body of ["   ", "x".repeat(2_001)]) expect(() => validate({ ...key, body })).toThrow();
    });
});
