import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { CdbError } from "@chardb/core";
import { drizzle } from "drizzle-orm/bun-sqlite";
import { deleteMessage, editMessage, postMessage } from "../../src/server/api.ts";
import { auth } from "../../src/server/auth.ts";
import { listMessages } from "../../src/server/queries.ts";
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
        const [app, authSource, schema, versionOne, worker, wrangler, vite] = await Promise.all([
            readFile(resolve(root, "src/web/App.tsx"), "utf8"),
            readFile(resolve(root, "src/server/auth.ts"), "utf8"),
            readFile(resolve(root, "src/server/schema.ts"), "utf8"),
            readFile(resolve(root, "src/server/migrations/v1.ts"), "utf8"),
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
        expect(app).toContain("const mutate = db.useMutation(postMessage)");
        expect(app).not.toContain("<ChardbProvider");
        expect(app).not.toContain("organizationId,\n                body");
        expect(app).not.toContain("DEMO_ORG_ID");
        expect(app).not.toContain("useSession.get()");
        expect(app).not.toContain("useSession.subscribe(");
        expect(authSource).not.toContain("DBAdapter");
        expect(authSource).not.toContain("databaseHooks");
        expect(schema).toContain('owner: "*"');
        expect(versionOne).toContain("plugins: [anonymous(), organization(), jwt()]");
        expect(versionOne).toContain('owner: "*"');
        expect(worker).toContain('authBasePath: "/api/auth"');
        expect(worker).toContain("{ DB, Catalog, Cdb, Gateway, Resharder }");
        expect(wrangler).toContain('new_sqlite_classes = ["Cdb", "Catalog", "Gateway", "Resharder"]');
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
            sqlite.run(`CREATE TABLE messages (
                id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
                author_id TEXT NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL
            )`);
            const raw = drizzle(sqlite, { schema: { messages } });
            const context = (userId: string, tenantId = "org-1", role = "member") => {
                const auth = { userId, tenantId, role, claims: {} };
                return { db: raw, auth };
            };
            const args = { id: "m1", organizationId: "org-1" };
            postMessage(context("alice"), { ...args, body: "hello", clientCreatedAt: 1 });
            expect(raw.select().from(messages).get()).toMatchObject({
                authorId: "alice",
                organizationId: "org-1",
                body: "hello",
            });
            expect(editMessage(context("bob"), { ...args, body: "hijacked" })).toEqual({ id: "m1" });
            expect(deleteMessage(context("bob"), args)).toEqual({ id: "m1" });
            expect(editMessage(context("alice", "org-2"), { ...args, body: "wrong org" })).toEqual({ id: "m1" });
            expect(deleteMessage(context("alice", "org-2"), args)).toEqual({ id: "m1" });
            expect(raw.select().from(messages).get()?.body).toBe("hello");
            expect(editMessage(context("alice"), { ...args, body: "edited" })).toEqual({ id: "m1" });
            expect(raw.select().from(messages).get()?.body).toBe("edited");
            expect(editMessage(context("bob", "org-1", "admin"), { ...args, body: "moderated" })).toEqual({ id: "m1" });
            expect(raw.select().from(messages).get()?.body).toBe("moderated");
            expect(deleteMessage(context("alice"), args)).toEqual({ id: "m1" });
            expect(deleteMessage(context("alice"), args)).toEqual({ id: "m1" });
            expect(raw.select().from(messages).all()).toEqual([]);
        } finally {
            sqlite.close();
        }
    });

    test("validates edits before executing them", async () => {
        const validate = (
            editMessage as typeof editMessage & {
                __chardbValidateArgs(args: unknown): Promise<unknown>;
            }
        ).__chardbValidateArgs;
        const key = { organizationId: "org-1", id: "m1" };
        expect(await validate({ ...key, body: "  edited  " })).toEqual({ ...key, body: "edited" });
        for (const body of ["   ", "x".repeat(2_001)]) {
            await expect(Promise.resolve().then(() => validate({ ...key, body }))).rejects.toThrow();
        }
    });
});
