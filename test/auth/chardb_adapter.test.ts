import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { admin } from "better-auth/plugins/admin";
import { organization } from "better-auth/plugins/organization";
import { chardbAuthAdapter } from "../../src/auth/chardb_adapter.ts";
import { renderSqliteTableDdl } from "../../src/auth/ddl.ts";
import { bindAuthRuntime, resetAuthRuntime } from "../../src/auth/runtime.ts";
import {
    AUTH_BULK_PRELOAD_MAX_ROWS,
    AUTH_BULK_PRELOAD_MAX_SCOPE_BYTES,
    AUTH_BULK_REPLACEMENT_MAX_BYTES,
    AUTH_READ_IN_MAX_VALUES,
} from "../../src/auth/sql.ts";
import { defineAuth, synthesizeAuthSchema } from "../../src/auth/synthesize.ts";
import { chardb } from "../../src/server/chardb.ts";
import { Catalog, configureCatalogRuntime } from "../../src/server/do/catalog.ts";
import { defineMigrations } from "../../src/server/schema-migrations.ts";
import { PrincipalId, ShardId, TenantId } from "../../src/types.ts";

interface Cursor<T> extends Iterable<T> {
    readonly columnNames: string[];
    raw(): IterableIterator<unknown[]>;
}

function sqlStorage(db: Database, statements: string[] = []) {
    return {
        exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): Cursor<T> {
            statements.push(query);
            const statement = db.prepare(query);
            const rows = statement.all(...(bindings as never[])) as Record<string, unknown>[];
            const columnNames = [...statement.columnNames];
            const rawRows = rows.map(row => columnNames.map(column => row[column]));
            return {
                columnNames,
                raw: () => rawRows.values(),
                *[Symbol.iterator]() {
                    yield* rows as T[];
                },
            };
        },
    };
}

const recoveryNamespace = {
    idFromName: () => "global",
    get: () => ({
        adminRecoveryAdmissionClock: async () => ({ generation: 0, activeOperationId: null, activeDigest: null }),
    }),
} as unknown as DurableObjectNamespace;

function withRecoveryEnv(env: Record<string, unknown>): Record<string, unknown> {
    return { CDB_RESHARD: recoveryNamespace, ...env };
}

class CatalogHarness {
    readonly db: Database;
    readonly sqlStatements: string[] = [];
    private bootstrap: Promise<unknown> = Promise.resolve();
    private readonly state: DurableObjectState;
    private CatalogClass: typeof Catalog = Catalog;
    private env: Record<string, unknown> = {};
    private alarm: number | null = null;
    catalog: Catalog;

    constructor(
        prepare?: (db: Database) => void,
        CatalogClass: typeof Catalog = Catalog,
        env: Record<string, unknown> = {}
    ) {
        this.db = new Database(":memory:");
        prepare?.(this.db);
        this.CatalogClass = CatalogClass;
        this.env = withRecoveryEnv(env);
        this.state = {
            storage: {
                sql: sqlStorage(this.db, this.sqlStatements),
                transactionSync: <T>(callback: () => T): T => this.db.transaction(callback)(),
                getAlarm: async () => this.alarm,
                setAlarm: async (scheduledTime: number | Date) => {
                    this.alarm = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
                },
                transaction: async <T>(callback: (transaction: DurableObjectTransaction) => Promise<T>): Promise<T> => {
                    this.db.run("BEGIN IMMEDIATE");
                    try {
                        const result = await callback({
                            getAlarm: async () => this.alarm,
                            setAlarm: async (scheduledTime: number | Date) => {
                                this.alarm = scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime;
                            },
                        } as unknown as DurableObjectTransaction);
                        this.db.run("COMMIT");
                        return result;
                    } catch (error) {
                        this.db.run("ROLLBACK");
                        throw error;
                    }
                },
            },
            blockConcurrencyWhile: (callback: () => Promise<unknown>): void => {
                this.bootstrap = callback();
            },
        } as unknown as DurableObjectState;
        this.catalog = new CatalogClass(this.state, this.env);
    }

    async ready(): Promise<void> {
        await this.bootstrap;
    }

    async restart(): Promise<void> {
        this.catalog = new this.CatalogClass(this.state as never, this.env);
        await this.ready();
    }

    async reconfigure(CatalogClass: typeof Catalog, env: Record<string, unknown> = {}): Promise<void> {
        this.CatalogClass = CatalogClass;
        this.env = withRecoveryEnv(env);
        this.catalog = new CatalogClass(this.state as never, this.env);
        await this.ready();
    }

    close(): void {
        this.db.close();
    }
}

function namespaceFor(harness: CatalogHarness): DurableObjectNamespace {
    return {
        idFromName(name: string) {
            if (name !== "global") throw new Error(`unexpected Catalog id: ${name}`);
            return name as never;
        },
        get() {
            return harness.catalog as never;
        },
    } as unknown as DurableObjectNamespace;
}

const auth = defineAuth({ plugins: [organization(), admin()] });
const authWithNickname = defineAuth({
    plugins: [organization(), admin()],
    user: { additionalFields: { nickname: { type: "string", required: false } } },
});
const rateLimitAuth = defineAuth({ rateLimit: { storage: "database" } });
const renamedRateLimitAuth = defineAuth({
    rateLimit: {
        storage: "database",
        modelName: "auth_rate_limits",
        fields: { key: "rate_key", count: "rate_count", lastRequest: "last_request_at" },
    },
});
const swappedRateLimitAuth = defineAuth({
    rateLimit: {
        storage: "database",
        fields: { key: "rate_key", count: "lastRequest", lastRequest: "count" },
    },
});

function bindRuntime(): void {
    resetAuthRuntime();
    bindAuthRuntime({
        schema: synthesizeAuthSchema(auth.options as never) as never,
        options: auth.options as { readonly [key: string]: unknown },
    });
}

function eq(field: string, value: string | number | boolean | string[] | number[] | Date | null) {
    return [{ field, value, operator: "eq" as const }];
}

describe("chardbAuthAdapter — Catalog-owned auth storage", () => {
    let harness: CatalogHarness;

    beforeEach(async () => {
        bindRuntime();
        harness = new CatalogHarness();
        await harness.ready();
    });

    afterEach(() => {
        harness.close();
        resetAuthRuntime();
    });

    test("creates and looks up core and membership rows by non-owner fields", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");

        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "user-1",
                name: "Ada",
                email: "ada@example.com",
                emailVerified: true,
                createdAt: now,
                updatedAt: now,
            },
        });
        await adapter.create({
            model: "session",
            forceAllowId: true,
            data: {
                id: "session-1",
                token: "session-token",
                userId: "user-1",
                expiresAt: now,
                createdAt: now,
                updatedAt: now,
            },
        });
        await adapter.create({
            model: "account",
            forceAllowId: true,
            data: {
                id: "account-1",
                accountId: "provider-account",
                providerId: "github",
                userId: "user-1",
                createdAt: now,
                updatedAt: now,
            },
        });
        await adapter.create({
            model: "organization",
            forceAllowId: true,
            data: {
                id: "org-1",
                name: "Example",
                slug: "example",
                createdAt: now,
            },
        });
        await adapter.create({
            model: "member",
            forceAllowId: true,
            data: {
                id: "member-1",
                organizationId: "org-1",
                userId: "user-1",
                role: "member",
                createdAt: now,
            },
        });

        const userByEmail = (await adapter.findOne({
            model: "user",
            where: eq("email", "ada@example.com"),
        })) as { readonly createdAt?: unknown } | null;
        expect(userByEmail).toMatchObject({ id: "user-1", emailVerified: true });
        expect(userByEmail?.createdAt).toBeInstanceOf(Date);
        if (!(userByEmail?.createdAt instanceof Date)) throw new Error("expected a Date from the auth adapter");
        expect(userByEmail.createdAt.getTime()).toBe(now.getTime());
        expect(await adapter.findOne({ model: "session", where: eq("token", "session-token") })).toMatchObject({
            id: "session-1",
        });
        expect(await adapter.findOne({ model: "session", where: eq("expiresAt", now) })).toMatchObject({
            id: "session-1",
            expiresAt: now,
        });
        expect(
            await adapter.findOne({
                model: "account",
                where: [
                    { field: "providerId", value: "github", operator: "eq" },
                    { field: "accountId", value: "provider-account", operator: "eq" },
                ],
            })
        ).toMatchObject({ id: "account-1" });
        expect(await adapter.findMany({ model: "member", where: eq("userId", "user-1") })).toEqual([
            expect.objectContaining({ id: "member-1", organizationId: "org-1" }),
        ]);
        expect(harness.catalog.authEpoch({ tenantId: "org-1" as never, principalId: "user-1" as never })).toEqual({
            global: 1,
            tenant: 2,
            principal: 4,
        });
    });

    test("counts filtered rows with scalar SQL and no row materialization", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const [id, name] of [
            ["count-user-1", "Counted"],
            ["count-user-2", "Counted"],
            ["count-user-3", "Excluded"],
        ] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name,
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }
        harness.sqlStatements.length = 0;

        await expect(adapter.count({ model: "user", where: eq("name", "Counted") })).resolves.toBe(2);
        expect(harness.sqlStatements).toContain('SELECT COUNT(*) AS c FROM "user" WHERE "name" = ?');
        expect(harness.sqlStatements.some(statement => statement.includes('SELECT * FROM "user"'))).toBe(false);
    });

    test("honors findMany sort and offset across stable pages", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const [id, name] of [
            ["sorted-user-d", "Delta"],
            ["sorted-user-a", "Alpha"],
            ["sorted-user-c", "Charlie"],
            ["sorted-user-b", "Bravo"],
        ] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name,
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }
        harness.sqlStatements.length = 0;

        const firstPage = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            limit: 2,
            offset: 0,
            sortBy: { field: "name", direction: "asc" },
        });
        const secondPage = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            limit: 2,
            offset: 2,
            sortBy: { field: "name", direction: "asc" },
        });
        const descendingMiddle = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            limit: 2,
            offset: 1,
            sortBy: { field: "name", direction: "desc" },
        });
        const offsetWithoutSort = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            limit: 2,
            offset: 1,
        });

        expect(firstPage.map(row => row.name)).toEqual(["Alpha", "Bravo"]);
        expect(secondPage.map(row => row.name)).toEqual(["Charlie", "Delta"]);
        expect(descendingMiddle.map(row => row.name)).toEqual(["Charlie", "Bravo"]);
        expect(offsetWithoutSort.map(row => row.id)).toEqual(["sorted-user-b", "sorted-user-c"]);
        expect(harness.sqlStatements).toContain(
            'SELECT * FROM "user" WHERE 1=1 ORDER BY "name" ASC, "id" ASC LIMIT ? OFFSET ?'
        );
        expect(harness.sqlStatements).toContain(
            'SELECT * FROM "user" WHERE 1=1 ORDER BY "name" DESC, "id" ASC LIMIT ? OFFSET ?'
        );
        expect(harness.sqlStatements).toContain('SELECT * FROM "user" WHERE 1=1 ORDER BY "id" ASC LIMIT ? OFFSET ?');
    });

    test("uses id as a stable tie-breaker across sorted pages", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["tied-user-d", "tied-user-a", "tied-user-c", "tied-user-b"] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: "Tied",
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }

        const firstPage = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            where: eq("name", "Tied"),
            limit: 2,
            offset: 0,
            sortBy: { field: "name", direction: "asc" },
        });
        const secondPage = await adapter.findMany<Record<string, unknown>>({
            model: "user",
            where: eq("name", "Tied"),
            limit: 2,
            offset: 2,
            sortBy: { field: "name", direction: "asc" },
        });

        expect(firstPage.map(row => row.id)).toEqual(["tied-user-a", "tied-user-b"]);
        expect(secondPage.map(row => row.id)).toEqual(["tied-user-c", "tied-user-d"]);
    });

    test("routes adapter counts through the structured Catalog boundary without materializing rows", async () => {
        const requests: unknown[] = [];
        const catalog = {
            async authAdapterRpc(request: unknown) {
                requests.push(structuredClone(request));
                return { ok: true, value: 7 };
            },
        };
        const namespace = {
            idFromName: () => "global",
            get: () => catalog,
        } as unknown as DurableObjectNamespace;
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespace } })(auth.options);

        await expect(adapter.count({ model: "user", where: eq("name", "Counted") })).resolves.toBe(7);
        expect(requests).toEqual([
            {
                operation: "count",
                recoveryGeneration: 0,
                args: { model: "user", where: [{ field: "name", operator: "eq", value: "Counted" }] },
            },
        ]);
    });

    test("resolves the current recovery generation only when an adapter operation reaches Catalog", async () => {
        const requests: unknown[] = [];
        const namespace = {
            idFromName: () => "global",
            get: () => ({
                authAdapterRpc: async (request: unknown) => {
                    requests.push(structuredClone(request));
                    return { ok: true, value: 1 };
                },
            }),
        } as unknown as DurableObjectNamespace;
        let generation = 3;
        const adapter = chardbAuthAdapter({
            env: { CDB_CATALOG: namespace },
            recoveryGeneration: async () => generation++,
        })(auth.options);

        expect(requests).toEqual([]);
        await adapter.count({ model: "user", where: [] });
        await adapter.count({ model: "user", where: [] });
        expect(requests).toEqual([
            { operation: "count", recoveryGeneration: 3, args: { model: "user", where: [] } },
            { operation: "count", recoveryGeneration: 4, args: { model: "user", where: [] } },
        ]);
    });

    test("routes bounded in filters through Catalog reads", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-24T00:00:00Z");
        for (const id of ["in-user-a", "in-user-b", "in-user-c"]) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: id,
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }

        await expect(
            adapter.findMany<Record<string, unknown>>({
                model: "user",
                where: [{ field: "id", operator: "in", value: ["in-user-c", "in-user-a"] }],
                sortBy: { field: "id", direction: "asc" },
            })
        ).resolves.toEqual([
            expect.objectContaining({ id: "in-user-a" }),
            expect.objectContaining({ id: "in-user-c" }),
        ]);
        await expect(
            adapter.count({ model: "user", where: [{ field: "id", operator: "in", value: [] }] })
        ).resolves.toBe(0);
        await expect(
            adapter.findMany({
                model: "user",
                where: [
                    {
                        field: "id",
                        operator: "in",
                        value: Array.from({ length: AUTH_READ_IN_MAX_VALUES + 1 }, (_, index) => `u-${index}`),
                    },
                ],
            })
        ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS" });
    });

    test("honors Better Auth organization list filter operators", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-24T00:00:00Z");
        for (const [id, name] of [
            ["filter-a", "Ada Lovelace"],
            ["filter-b", "Grace Hopper"],
            ["filter-c", "Linus Torvalds"],
            ["filter-d", "Margaret Hamilton"],
        ] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name,
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }

        const ids = async (operator: "contains" | "starts_with" | "ends_with", value: string) =>
            (
                await adapter.findMany<Record<string, unknown>>({
                    model: "user",
                    where: [{ field: "name", operator, value, mode: "insensitive" }],
                    sortBy: { field: "id", direction: "asc" },
                })
            ).map(row => row.id);

        await expect(ids("contains", "HOP")).resolves.toEqual(["filter-b"]);
        await expect(ids("starts_with", "lin")).resolves.toEqual(["filter-c"]);
        await expect(ids("ends_with", "TON")).resolves.toEqual(["filter-d"]);
        await expect(
            adapter.count({ model: "user", where: [{ field: "id", operator: "ne", value: "filter-a" }] })
        ).resolves.toBe(3);
        await expect(
            adapter.count({
                model: "user",
                where: [{ field: "id", operator: "not_in", value: ["filter-a", "filter-c"] }],
            })
        ).resolves.toBe(2);
        await expect(
            adapter.findMany<Record<string, unknown>>({
                model: "user",
                where: [
                    { field: "id", operator: "gte", value: "filter-b" },
                    { field: "id", operator: "lt", value: "filter-d" },
                ],
                sortBy: { field: "id", direction: "asc" },
            })
        ).resolves.toEqual([expect.objectContaining({ id: "filter-b" }), expect.objectContaining({ id: "filter-c" })]);
    });

    test("rejects hostile Better Auth filter operators and modes before Catalog SQL", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const find = (where: Record<string, unknown>) => adapter.findMany({ model: "user", where: [where as never] });

        await expect(find({ field: "name", operator: "between", value: "Ada" })).rejects.toMatchObject({
            code: "CDB_UNSUPPORTED_FEATURE",
        });
        await expect(find({ field: "name", operator: "eq", value: "Ada", mode: "locale-aware" })).rejects.toMatchObject(
            { code: "CDB_INVALID_ARGS" }
        );
        await expect(
            find({ field: "emailVerified", operator: "eq", value: true, mode: "insensitive" })
        ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS" });
        await expect(
            find({ field: "id", operator: "in", value: ["filter-a"], mode: "insensitive" })
        ).rejects.toMatchObject({ code: "CDB_UNSUPPORTED_FEATURE" });
    });

    test("routes incrementOne through the native Catalog RPC without fallback reads or writes", async () => {
        const requests: unknown[] = [];
        const catalog = {
            async authAdapterRpc(request: unknown) {
                requests.push(structuredClone(request));
                return {
                    ok: true,
                    value: {
                        ok: true,
                        affected: 1,
                        row: { id: "rate-native", key: "native", count: 2, lastRequest: 100 },
                    },
                };
            },
        };
        const namespace = {
            idFromName: () => "global",
            get: () => catalog,
        } as unknown as DurableObjectNamespace;
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespace } })(
            renamedRateLimitAuth.options
        );

        await expect(
            adapter.incrementOne<Record<string, unknown>>({
                model: "rateLimit",
                where: [
                    { field: "key", operator: "eq", value: "native" },
                    { field: "lastRequest", operator: "gt", value: 50 },
                    { field: "count", operator: "lt", value: 3 },
                ],
                increment: { count: 1 },
                set: { lastRequest: 200 },
            })
        ).resolves.toMatchObject({ id: "rate-native", count: 2 });
        expect(requests).toEqual([
            {
                operation: "increment",
                recoveryGeneration: 0,
                args: {
                    model: "rateLimit",
                    where: [
                        { field: "key", operator: "eq", value: "native" },
                        { field: "lastRequest", operator: "gt", value: 50 },
                        { field: "count", operator: "lt", value: 3 },
                    ],
                    increment: { count: 1 },
                    set: { lastRequest: 200 },
                },
            },
        ]);
        const request = (requests[0] as { args: { increment: object; set: object } }).args;
        expect(Object.getPrototypeOf(request.increment)).toBe(Object.prototype);
        expect(Object.getPrototypeOf(request.set)).toBe(Object.prototype);
    });

    test("maps swapped physical incrementOne fields back to their canonical columns", async () => {
        harness.close();
        resetAuthRuntime();
        bindAuthRuntime({
            schema: synthesizeAuthSchema(swappedRateLimitAuth.options as never) as never,
            options: swappedRateLimitAuth.options as { readonly [key: string]: unknown },
        });
        harness = new CatalogHarness();
        await harness.ready();
        await harness.catalog.mutateAuth(
            {
                model: "rateLimit",
                op: "create",
                payload: { id: "rate-swapped", key: "swapped", count: 1, lastRequest: 100 },
            },
            0
        );
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            swappedRateLimitAuth.options
        );

        await expect(
            adapter.incrementOne<Record<string, unknown>>({
                model: "rateLimit",
                where: [
                    { field: "key", operator: "eq", value: "swapped" },
                    { field: "count", operator: "lt", value: 2 },
                    { field: "lastRequest", operator: "gte", value: 100 },
                ],
                increment: { count: 1 },
                set: { lastRequest: 200 },
            })
        ).resolves.toMatchObject({ id: "rate-swapped", key: "swapped", count: 2, lastRequest: 200 });
        await expect(
            harness.catalog.queryAuth(
                {
                    model: "rateLimit",
                    where: [{ field: "id", operator: "eq", value: "rate-swapped" }],
                    limit: 1,
                },
                0
            )
        ).resolves.toEqual([
            expect.objectContaining({ id: "rate-swapped", key: "swapped", count: 2, lastRequest: 200 }),
        ]);
    });

    test("rejects duplicate physical incrementOne mappings at adapter construction", () => {
        expect(() =>
            chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })({
                rateLimit: {
                    storage: "database",
                    fields: { key: "duplicate", count: "duplicate" },
                },
            } as never)
        ).toThrow(/both map to "duplicate"/);
    });

    test("atomically enforces rate-limit guards, resets, and concurrent maxima", async () => {
        harness.close();
        resetAuthRuntime();
        bindAuthRuntime({
            schema: synthesizeAuthSchema(rateLimitAuth.options as never) as never,
            options: rateLimitAuth.options as { readonly [key: string]: unknown },
        });
        harness = new CatalogHarness();
        await harness.ready();
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            rateLimitAuth.options
        );

        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "rate-window", key: "window", count: 0, lastRequest: 100 },
        });
        const beforeIncrement = harness.catalog.authEpoch({}).global;
        await expect(
            adapter.incrementOne<Record<string, unknown>>({
                model: "rateLimit",
                where: [
                    { field: "key", operator: "eq", value: "window" },
                    { field: "lastRequest", operator: "gt", value: 50 },
                    { field: "count", operator: "lt", value: 2 },
                ],
                increment: { count: 1 },
            })
        ).resolves.toMatchObject({ id: "rate-window", count: 1, lastRequest: 100 });
        expect(harness.catalog.authEpoch({}).global).toBe(beforeIncrement + 1);

        harness.sqlStatements.length = 0;
        const beforeGuardMiss = harness.catalog.authEpoch({}).global;
        await expect(
            adapter.incrementOne({
                model: "rateLimit",
                where: [
                    { field: "key", operator: "eq", value: "window" },
                    { field: "count", operator: "lt", value: 1 },
                ],
                increment: { count: 1 },
            })
        ).resolves.toBeNull();
        expect(harness.sqlStatements.some(statement => statement.startsWith('UPDATE "rateLimit"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
        expect(harness.catalog.authEpoch({}).global).toBe(beforeGuardMiss);

        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "rate-concurrent", key: "concurrent", count: 0, lastRequest: 100 },
        });
        const beforeConcurrent = harness.catalog.authEpoch({}).global;
        const concurrent = await Promise.all(
            Array.from({ length: 3 }, () =>
                adapter.incrementOne<Record<string, unknown>>({
                    model: "rateLimit",
                    where: [
                        { field: "key", operator: "eq", value: "concurrent" },
                        { field: "count", operator: "lt", value: 2 },
                    ],
                    increment: { count: 1 },
                })
            )
        );
        expect(concurrent.filter(result => result !== null).map(result => result?.count)).toEqual([1, 2]);
        expect(concurrent.filter(result => result === null)).toHaveLength(1);
        expect(await adapter.findOne({ model: "rateLimit", where: eq("key", "concurrent") })).toMatchObject({
            count: 2,
        });
        expect(harness.catalog.authEpoch({}).global).toBe(beforeConcurrent + 2);

        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "rate-reset", key: "reset", count: 9, lastRequest: 10 },
        });
        await expect(
            adapter.incrementOne<Record<string, unknown>>({
                model: "rateLimit",
                where: [
                    { field: "key", operator: "eq", value: "reset" },
                    { field: "lastRequest", operator: "lte", value: 10 },
                ],
                increment: {},
                set: { count: 1, lastRequest: 200 },
            })
        ).resolves.toMatchObject({ id: "rate-reset", count: 1, lastRequest: 200 });

        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "a-empty-guard", key: "empty-a", count: 4, lastRequest: 10 },
        });
        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "b-empty-guard", key: "empty-b", count: 8, lastRequest: 20 },
        });
        const beforeEmptyGuard = harness.catalog.authEpoch({}).global;
        await expect(
            adapter.incrementOne<Record<string, unknown>>({
                model: "rateLimit",
                where: [],
                increment: { count: 1 },
            })
        ).resolves.toMatchObject({ id: "a-empty-guard", count: 5 });
        expect(await adapter.findOne({ model: "rateLimit", where: eq("id", "a-empty-guard") })).toMatchObject({
            count: 5,
        });
        expect(await adapter.findOne({ model: "rateLimit", where: eq("id", "b-empty-guard") })).toMatchObject({
            count: 8,
        });
        expect(harness.catalog.authEpoch({}).global).toBe(beforeEmptyGuard + 1);
    });

    test("fails incrementOne closed on invalid operators, fields, and deltas before writes", async () => {
        harness.close();
        resetAuthRuntime();
        bindAuthRuntime({
            schema: synthesizeAuthSchema(rateLimitAuth.options as never) as never,
            options: rateLimitAuth.options as { readonly [key: string]: unknown },
        });
        harness = new CatalogHarness();
        await harness.ready();
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            rateLimitAuth.options
        );
        await adapter.create({
            model: "rateLimit",
            forceAllowId: true,
            data: { id: "rate-invalid", key: "invalid", count: 0, lastRequest: 100 },
        });
        harness.sqlStatements.length = 0;

        await expect(
            adapter.incrementOne({
                model: "rateLimit",
                where: [{ field: "key", operator: "contains", value: "invalid" }],
                increment: { count: 1 },
            })
        ).rejects.toMatchObject({ code: "CDB_UNSUPPORTED_FEATURE", retryable: false });
        await expect(
            adapter.incrementOne({
                model: "rateLimit",
                where: eq("key", "invalid"),
                increment: { count: Number.POSITIVE_INFINITY },
            })
        ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        await expect(
            harness.catalog.incrementAuth(
                {
                    model: "rateLimit",
                    where: [{ field: "missing", operator: "eq", value: "invalid" }],
                    increment: { count: 1 },
                },
                0
            )
        ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        await expect(
            harness.catalog.incrementAuth(
                {
                    model: "rateLimit",
                    where: [{ field: "key", operator: "eq", value: "invalid" }],
                    increment: { key: 1 },
                },
                0
            )
        ).rejects.toMatchObject({ code: "CDB_INVALID_ARGS", retryable: false });
        await expect(
            harness.catalog.incrementAuth(
                {
                    model: "rateLimit",
                    where: [{ field: "key", operator: "eq", value: "invalid" }],
                    increment: {},
                    set: { key: "x".repeat(AUTH_BULK_REPLACEMENT_MAX_BYTES + 1) },
                },
                0
            )
        ).rejects.toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        expect(harness.sqlStatements.some(statement => statement.startsWith('UPDATE "rateLimit"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
    });

    test("updates and deletes through non-owner lookups while preserving epoch bumps", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");

        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "user-2",
                name: "Before",
                email: "update@example.com",
                emailVerified: false,
                createdAt: now,
                updatedAt: now,
            },
        });
        const updated = await adapter.update({
            model: "user",
            where: eq("email", "update@example.com"),
            update: { name: "After" },
        });
        expect(updated).toMatchObject({ id: "user-2", name: "After" });

        await adapter.create({
            model: "session",
            forceAllowId: true,
            data: {
                id: "session-2",
                token: "delete-token",
                userId: "user-2",
                expiresAt: now,
                createdAt: now,
                updatedAt: now,
            },
        });
        await adapter.delete({ model: "session", where: eq("token", "delete-token") });
        expect(await adapter.findOne({ model: "session", where: eq("token", "delete-token") })).toBeNull();
        expect(harness.catalog.authEpoch({ principalId: "user-2" as never }).principal).toBe(4);
    });

    test("consumes a single verification row atomically and returns its stored value", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        await adapter.create({
            model: "verification",
            forceAllowId: true,
            data: {
                id: "magic-link-token",
                identifier: "magic@example.com",
                value: "one-time-secret",
                expiresAt: now,
                createdAt: now,
                updatedAt: now,
            },
        });

        await expect(
            adapter.consumeOne({ model: "verification", where: eq("identifier", "magic@example.com") })
        ).resolves.toMatchObject({ id: "magic-link-token", value: "one-time-secret" });
        await expect(
            adapter.consumeOne({ model: "verification", where: eq("identifier", "magic@example.com") })
        ).resolves.toBeNull();
    });

    test("keeps empty single-row mutations as no-ops while bulk mutations remain explicit", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["empty-single-a", "empty-single-b"] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: "Before",
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }
        harness.sqlStatements.length = 0;

        await expect(adapter.update({ model: "user", where: [], update: { name: "Single" } })).resolves.toBeNull();
        await expect(adapter.delete({ model: "user", where: [] })).resolves.toBeUndefined();
        const singleStatements = [...harness.sqlStatements];
        expect(singleStatements.some(statement => statement.startsWith('UPDATE "user"'))).toBe(false);
        expect(singleStatements.some(statement => statement.startsWith('DELETE FROM "user"'))).toBe(false);
        expect(singleStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
        expect(await adapter.findMany({ model: "user", where: eq("name", "Before") })).toHaveLength(2);

        expect(await adapter.updateMany({ model: "user", where: [], update: { name: "Bulk" } })).toBe(2);
        expect(await adapter.deleteMany({ model: "user", where: [] })).toBe(2);
        expect(await adapter.findMany({ model: "user", where: [] })).toEqual([]);
    });

    test("keeps legacy direct Catalog mutations bulk by default", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["legacy-bulk-a", "legacy-bulk-b"] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: "Legacy",
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }

        await expect(
            harness.catalog.mutateAuth(
                {
                    model: "user",
                    op: "update",
                    where: { name: "Legacy" },
                    payload: { name: "Legacy changed" },
                    returnRow: false,
                },
                0
            )
        ).resolves.toMatchObject({ affected: 2 });
        await expect(
            harness.catalog.mutateAuth({ model: "user", op: "delete", where: { name: "Legacy changed" } }, 0)
        ).resolves.toMatchObject({ affected: 2 });
    });

    test("single-row update and delete choose the lowest matching id without touching siblings", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["single-c", "single-a", "single-b"] as const) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: "Shared",
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }
        const epochBeforeA = harness.catalog.authEpoch({ principalId: "single-a" as never }).principal;
        const epochBeforeB = harness.catalog.authEpoch({ principalId: "single-b" as never }).principal;
        const epochBeforeC = harness.catalog.authEpoch({ principalId: "single-c" as never }).principal;

        await expect(
            adapter.update({ model: "user", where: eq("name", "Shared"), update: { image: "selected" } })
        ).resolves.toMatchObject({ id: "single-a", image: "selected" });
        const shared = await adapter.findMany<Record<string, unknown>>({ model: "user", where: eq("name", "Shared") });
        expect(Object.fromEntries(shared.map(row => [row.id, row.image ?? null]))).toEqual({
            "single-a": "selected",
            "single-b": null,
            "single-c": null,
        });
        expect(harness.catalog.authEpoch({ principalId: "single-a" as never }).principal).toBe(epochBeforeA + 1);
        expect(harness.catalog.authEpoch({ principalId: "single-b" as never }).principal).toBe(epochBeforeB);
        expect(harness.catalog.authEpoch({ principalId: "single-c" as never }).principal).toBe(epochBeforeC);

        await adapter.delete({ model: "user", where: eq("name", "Shared") });
        expect(await adapter.findOne({ model: "user", where: eq("id", "single-a") })).toBeNull();
        expect(await adapter.findOne({ model: "user", where: eq("id", "single-b") })).not.toBeNull();
        expect(await adapter.findOne({ model: "user", where: eq("id", "single-c") })).not.toBeNull();
        expect(await adapter.deleteMany({ model: "user", where: eq("name", "Shared") })).toBe(2);
    });

    test("rolls back an auth row when its atomic epoch bump fails", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        harness.db.run(`CREATE TRIGGER fail_auth_epoch
            BEFORE UPDATE ON catalog_epoch
            WHEN NEW.scope = 'auth_principal'
            BEGIN SELECT RAISE(ABORT, 'forced epoch failure'); END`);
        const now = new Date("2026-08-23T00:00:00Z");

        await expect(
            adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id: "rollback-user",
                    name: "Rollback",
                    email: "rollback@example.com",
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            })
        ).rejects.toThrow("forced epoch failure");
        expect(await adapter.findOne({ model: "user", where: eq("email", "rollback@example.com") })).toBeNull();
        expect(harness.catalog.authEpoch({ principalId: "rollback-user" as never }).principal).toBe(0);
    });

    test("updateMany bumps every affected principal before returning", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["batch-user-1", "batch-user-2"]) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: "Before",
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                    role: "batch",
                },
            });
        }

        expect(await adapter.updateMany({ model: "user", where: eq("role", "batch"), update: { name: "After" } })).toBe(
            2
        );
        expect(await adapter.findMany({ model: "user", where: eq("name", "After") })).toHaveLength(2);
        expect(harness.catalog.authEpoch({ principalId: "batch-user-1" as never }).principal).toBe(2);
        expect(harness.catalog.authEpoch({ principalId: "batch-user-2" as never }).principal).toBe(2);
    });

    test("rejects a 4097-row auth update before the row or epoch write", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const insert = harness.db.prepare(
            'INSERT INTO "user" ("id", "name", "email", "emailVerified", "createdAt", "updatedAt") VALUES (?, ?, ?, ?, ?, ?)'
        );
        harness.db.transaction(() => {
            for (let index = 0; index <= AUTH_BULK_PRELOAD_MAX_ROWS; index++) {
                insert.run(
                    `bulk-over-${index}`,
                    "Bulk row cap",
                    `bulk-over-${index}@example.com`,
                    1,
                    1_777_000_000_000,
                    1_777_000_000_000
                );
            }
        })();
        harness.sqlStatements.length = 0;

        await expect(
            adapter.updateMany({ model: "user", where: eq("name", "Bulk row cap"), update: { name: "Changed" } })
        ).rejects.toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        expect(harness.db.query('SELECT COUNT(*) AS count FROM "user" WHERE "name" = ?').get("Changed")).toEqual({
            count: 0,
        });
        expect(harness.sqlStatements.some(statement => statement.startsWith('UPDATE "user"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
        expect(harness.catalog.authEpoch({ principalId: "bulk-over-0" as never }).principal).toBe(0);

        await expect(
            adapter.update({
                model: "user",
                where: eq("name", "Bulk row cap"),
                update: { name: "Single changed" },
            })
        ).resolves.toMatchObject({ id: "bulk-over-0", name: "Single changed" });
        expect(harness.db.query('SELECT COUNT(*) AS count FROM "user" WHERE "name" = ?').get("Single changed")).toEqual(
            {
                count: 1,
            }
        );

        await adapter.delete({ model: "user", where: eq("name", "Bulk row cap") });
        expect(harness.db.query('SELECT COUNT(*) AS count FROM "user" WHERE "name" = ?').get("Bulk row cap")).toEqual({
            count: AUTH_BULK_PRELOAD_MAX_ROWS - 1,
        });
        expect(harness.db.query('SELECT 1 FROM "user" WHERE "id" = ?').get("bulk-over-1")).toBeNull();
    });

    test("rejects excess stored and replacement scope bytes before auth writes", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const oversizedId = "s".repeat(AUTH_BULK_PRELOAD_MAX_SCOPE_BYTES + 1);
        harness.db
            .prepare(
                'INSERT INTO "user" ("id", "name", "email", "emailVerified", "createdAt", "updatedAt") VALUES (?, ?, ?, ?, ?, ?)'
            )
            .run(oversizedId, "Bulk byte cap", "bulk-byte-cap@example.com", 1, 1_777_000_000_000, 1_777_000_000_000);
        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "replacement-byte-user",
                name: "Before",
                email: "replacement-byte-user@example.com",
                emailVerified: true,
                createdAt: new Date("2026-08-23T00:00:00Z"),
                updatedAt: new Date("2026-08-23T00:00:00Z"),
            },
        });
        harness.sqlStatements.length = 0;

        await expect(
            adapter.updateMany({ model: "user", where: eq("name", "Bulk byte cap"), update: { name: "Changed" } })
        ).rejects.toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        await expect(adapter.deleteMany({ model: "user", where: eq("name", "Bulk byte cap") })).rejects.toMatchObject({
            code: "CDB_RATE_LIMITED",
            retryable: true,
        });
        await expect(
            harness.catalog.mutateAuth(
                {
                    model: "user",
                    op: "update",
                    where: { id: "replacement-byte-user" },
                    payload: { id: "n".repeat(AUTH_BULK_PRELOAD_MAX_SCOPE_BYTES) },
                    returnRow: false,
                },
                0
            )
        ).rejects.toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        expect(harness.db.query('SELECT "name" FROM "user" WHERE "id" = ?').get(oversizedId)).toEqual({
            name: "Bulk byte cap",
        });
        expect(harness.db.query('SELECT "name" FROM "user" WHERE "id" = ?').get("replacement-byte-user")).toEqual({
            name: "Before",
        });
        expect(harness.sqlStatements.some(statement => statement.startsWith('UPDATE "user"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith('DELETE FROM "user"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
        expect(harness.catalog.authEpoch({ principalId: oversizedId as never }).principal).toBe(0);
    });

    test("updateMany preloads only narrow scope columns and skips a full-row reread", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "wide-bulk-user",
                name: "Before",
                email: "wide-bulk-user@example.com",
                emailVerified: true,
                image: "w".repeat(1_024 * 1_024),
                createdAt: new Date("2026-08-23T00:00:00Z"),
                updatedAt: new Date("2026-08-23T00:00:00Z"),
            },
        });
        harness.sqlStatements.length = 0;

        await expect(
            adapter.updateMany({
                model: "user",
                where: eq("email", "wide-bulk-user@example.com"),
                update: { name: "After" },
            })
        ).resolves.toBe(1);
        const userSelects = harness.sqlStatements.filter(
            statement => statement.startsWith("SELECT") && statement.includes('FROM "user"')
        );
        expect(userSelects).toHaveLength(2);
        expect(userSelects.every(statement => !statement.includes('"image"'))).toBe(true);
        expect(userSelects.every(statement => !statement.includes("SELECT *"))).toBe(true);
        expect(harness.catalog.authEpoch({ principalId: "wide-bulk-user" as never }).principal).toBe(2);
    });

    test("rejects expanded non-scope replacements before base or epoch writes", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "replacement-expansion-user",
                name: "Before",
                email: "replacement-expansion-user@example.com",
                emailVerified: true,
                image: "before",
                createdAt: new Date("2026-08-23T00:00:00Z"),
                updatedAt: new Date("2026-08-23T00:00:00Z"),
            },
        });
        const epochBefore = harness.catalog.authEpoch({ principalId: "replacement-expansion-user" as never });
        harness.sqlStatements.length = 0;

        await expect(
            adapter.updateMany({
                model: "user",
                where: eq("id", "replacement-expansion-user"),
                update: { image: "x".repeat(AUTH_BULK_REPLACEMENT_MAX_BYTES + 1) },
            })
        ).rejects.toMatchObject({ code: "CDB_RATE_LIMITED", retryable: true });
        expect(harness.db.query('SELECT "image" FROM "user" WHERE "id" = ?').get("replacement-expansion-user")).toEqual(
            { image: "before" }
        );
        expect(harness.sqlStatements.some(statement => statement.startsWith('UPDATE "user"'))).toBe(false);
        expect(harness.sqlStatements.some(statement => statement.startsWith("UPDATE catalog_epoch"))).toBe(false);
        expect(harness.catalog.authEpoch({ principalId: "replacement-expansion-user" as never })).toEqual(epochBefore);
    });

    test("updateMany bumps every tenant and principal touched by membership rows", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const suffix of ["1", "2"]) {
            const userId = `batch-member-user-${suffix}`;
            const organizationId = `batch-member-org-${suffix}`;
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id: userId,
                    name: userId,
                    email: `${userId}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
            await adapter.create({
                model: "organization",
                forceAllowId: true,
                data: { id: organizationId, name: organizationId, slug: organizationId, createdAt: now },
            });
            await adapter.create({
                model: "member",
                forceAllowId: true,
                data: {
                    id: `batch-member-${suffix}`,
                    organizationId,
                    userId,
                    role: "pending-batch",
                    createdAt: now,
                },
            });
        }
        const before = ["1", "2"].map(suffix =>
            harness.catalog.authEpoch({
                tenantId: `batch-member-org-${suffix}` as never,
                principalId: `batch-member-user-${suffix}` as never,
            })
        );

        expect(
            await adapter.updateMany({
                model: "member",
                where: eq("role", "pending-batch"),
                update: { role: "member" },
            })
        ).toBe(2);
        for (const [index, suffix] of ["1", "2"].entries()) {
            const after = harness.catalog.authEpoch({
                tenantId: `batch-member-org-${suffix}` as never,
                principalId: `batch-member-user-${suffix}` as never,
            });
            expect(after.tenant).toBe((before[index]?.tenant ?? 0) + 1);
            expect(after.principal).toBe((before[index]?.principal ?? 0) + 1);
        }
    });

    test("moving a membership bumps every old and new tenant and principal scope", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        for (const id of ["move-user-1", "move-user-2"]) {
            await adapter.create({
                model: "user",
                forceAllowId: true,
                data: {
                    id,
                    name: id,
                    email: `${id}@example.com`,
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            });
        }
        for (const id of ["move-org-1", "move-org-2"]) {
            await adapter.create({
                model: "organization",
                forceAllowId: true,
                data: { id, name: id, slug: id, createdAt: now },
            });
        }
        await adapter.create({
            model: "member",
            forceAllowId: true,
            data: {
                id: "moving-member",
                organizationId: "move-org-1",
                userId: "move-user-1",
                role: "member",
                createdAt: now,
            },
        });

        const beforeOld = harness.catalog.authEpoch({
            tenantId: "move-org-1" as never,
            principalId: "move-user-1" as never,
        });
        const beforeNew = harness.catalog.authEpoch({
            tenantId: "move-org-2" as never,
            principalId: "move-user-2" as never,
        });
        expect(
            await adapter.update({
                model: "member",
                where: eq("id", "moving-member"),
                update: { organizationId: "move-org-2", userId: "move-user-2" },
            })
        ).toMatchObject({ organizationId: "move-org-2", userId: "move-user-2" });

        const afterOld = harness.catalog.authEpoch({
            tenantId: "move-org-1" as never,
            principalId: "move-user-1" as never,
        });
        const afterNew = harness.catalog.authEpoch({
            tenantId: "move-org-2" as never,
            principalId: "move-user-2" as never,
        });
        expect(afterOld.tenant).toBe(beforeOld.tenant + 1);
        expect(afterOld.principal).toBe(beforeOld.principal + 1);
        expect(afterNew.tenant).toBe(beforeNew.tenant + 1);
        expect(afterNew.principal).toBe(beforeNew.principal + 1);
    });

    test("a restarted Catalog instance reads rows from the same durable storage", async () => {
        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "restart-user",
                name: "Restart",
                email: "restart@example.com",
                emailVerified: true,
                createdAt: now,
                updatedAt: now,
            },
        });

        await harness.restart();
        const restartedAdapter = chardbAuthAdapter({
            recoveryGeneration: 0,
            env: { CDB_CATALOG: namespaceFor(harness) },
        })(auth.options);

        expect(
            await restartedAdapter.findOne({ model: "user", where: eq("email", "restart@example.com") })
        ).toMatchObject({ id: "restart-user" });
    });

    test("module initialization binds auth before the first Catalog bootstrap", async () => {
        harness.close();
        chardb({ ownership: "user", schema: {}, auth });
        resetAuthRuntime();

        // A fresh Worker or DO isolate evaluates the application module
        // again. This second factory call models that evaluation: no fetch,
        // schema getter, or adapter request runs before Catalog construction.
        chardb({ ownership: "user", schema: {}, auth });
        harness = new CatalogHarness();
        await harness.ready();

        const adapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            auth.options
        );
        const now = new Date("2026-08-23T00:00:00Z");
        await adapter.create({
            model: "user",
            forceAllowId: true,
            data: {
                id: "first-user",
                name: "First",
                email: "first@example.com",
                emailVerified: true,
                createdAt: now,
                updatedAt: now,
            },
        });
        await adapter.create({
            model: "session",
            forceAllowId: true,
            data: {
                id: "first-session",
                token: "first-request-token",
                userId: "first-user",
                expiresAt: now,
                createdAt: now,
                updatedAt: now,
            },
        });

        await harness.restart();
        const restartedAdapter = chardbAuthAdapter({
            recoveryGeneration: 0,
            env: { CDB_CATALOG: namespaceFor(harness) },
        })(auth.options);
        expect(
            await restartedAdapter.findOne({ model: "session", where: eq("token", "first-request-token") })
        ).toMatchObject({ id: "first-session", userId: "first-user" });
    });

    test("Catalog rejects a legacy auth table instead of pretending CREATE IF NOT EXISTS upgraded it", async () => {
        harness.close();
        harness = new CatalogHarness(db => {
            db.run('CREATE TABLE "user" ("id" TEXT PRIMARY KEY, "email" TEXT)');
        });

        await expect(harness.ready()).rejects.toMatchObject({
            code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
            message: expect.stringContaining("predates auth DDL v1"),
        });
    });

    test("applies a versioned auth migration, preserves rows, and fences auth traffic until activation", async () => {
        const now = Date.parse("2026-08-23T00:00:00Z");
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "migrated-user",
                    name: "Before migration",
                    email: "migration@example.com",
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            },
            0
        );
        const journal = defineMigrations([
            {
                version: 1,
                name: "add_user_nickname",
                statements: ["SELECT 1"],
                catalogStatements: ['ALTER TABLE "user" ADD COLUMN "nickname" text'],
            },
        ]);
        resetAuthRuntime();
        bindAuthRuntime({
            schema: synthesizeAuthSchema(authWithNickname.options as never) as never,
            options: authWithNickname.options as { readonly [key: string]: unknown },
        });
        const FutureCatalog = configureCatalogRuntime({ migrations: () => journal });
        const shardCalls: string[] = [];
        await harness.reconfigure(FutureCatalog, {
            CDB_SHARD: {
                idFromName: (name: string) => name,
                get: () => ({
                    async prepareSchemaMigration() {
                        shardCalls.push("prepare");
                    },
                    async applySchemaMigration(input: { version: number }) {
                        shardCalls.push(`apply:${input.version}`);
                    },
                    async activateSchemaMigration() {
                        shardCalls.push("activate");
                    },
                }),
            } as unknown as DurableObjectNamespace,
        });
        await expect(
            harness.catalog.queryAuth(
                {
                    model: "user",
                    where: [{ field: "id", operator: "eq", value: "migrated-user" }],
                },
                0
            )
        ).rejects.toMatchObject({ code: "CDB_STALE_EPOCH" });
        await expect(
            harness.catalog.authAdapterRpc({
                operation: "query",
                recoveryGeneration: 0,
                args: { model: "user", where: [{ field: "id", operator: "eq", value: "migrated-user" }] },
            })
        ).resolves.toEqual({
            ok: false,
            error: {
                code: "CDB_STALE_EPOCH",
                message: "Catalog auth schema migration is not active",
                hint: "retry after the schema migration activates",
            },
        });
        const fencedAdapter = chardbAuthAdapter({ recoveryGeneration: 0, env: { CDB_CATALOG: namespaceFor(harness) } })(
            authWithNickname.options
        );
        await expect(fencedAdapter.findOne({ model: "user", where: eq("id", "migrated-user") })).rejects.toMatchObject({
            code: "CDB_STALE_EPOCH",
            retryable: true,
        });

        expect(harness.catalog.beginSchemaMigration({ migrationId: "auth-v1", targetVersion: 1 })).toMatchObject({
            status: "migrating",
            targetVersion: 1,
        });
        await expect(
            harness.catalog.migrateSchemaShard({ migrationId: "auth-v1", shardId: "ShardDO_0" })
        ).resolves.toMatchObject({ status: "active" });
        expect(shardCalls).toEqual(["prepare", "apply:1", "activate"]);
        expect(harness.catalog.applyCatalogSchemaMigration({ migrationId: "auth-v1", version: 1 })).toMatchObject({
            status: "migrating",
        });
        expect(harness.db.query('SELECT nickname FROM "user" WHERE id = ?').get("migrated-user")).toEqual({
            nickname: null,
        });
        expect(harness.catalog.completeSchemaMigration({ migrationId: "auth-v1" })).toMatchObject({
            activeVersion: 1,
            activeEpoch: 2,
            status: "active",
        });

        await harness.restart();
        await expect(
            harness.catalog.mutateAuth(
                {
                    model: "user",
                    op: "update",
                    where: { id: "migrated-user" },
                    payload: { nickname: "after" },
                },
                0
            )
        ).resolves.toMatchObject({ row: expect.objectContaining({ id: "migrated-user", nickname: "after" }) });
        expect(harness.db.query('SELECT id, nickname FROM "user"').all()).toEqual([
            { id: "migrated-user", nickname: "after" },
        ]);
    });

    test("creates a fresh auth schema by applying the complete Catalog journal", async () => {
        harness.close();
        resetAuthRuntime();
        const schema = synthesizeAuthSchema(authWithNickname.options as never) as Record<string, unknown>;
        bindAuthRuntime({
            schema: schema as never,
            options: authWithNickname.options as { readonly [key: string]: unknown },
        });
        const catalogStatements = Object.values(schema).flatMap(table => {
            const ddl = renderSqliteTableDdl(table as never);
            return [ddl.createTable, ...ddl.indexes];
        });
        const journal = defineMigrations([
            { version: 1, name: "create_auth", statements: ["SELECT 1"], catalogStatements },
        ]);
        const FreshCatalog = configureCatalogRuntime({ migrations: () => journal });
        const shard = {
            async prepareSchemaMigration() {},
            async applySchemaMigration() {},
            async activateSchemaMigration() {},
        };
        harness = new CatalogHarness(undefined, FreshCatalog, {
            CDB_SHARD: {
                idFromName: (name: string) => name,
                get: () => shard,
            } as unknown as DurableObjectNamespace,
        });
        await harness.ready();
        expect(harness.catalog.schemaState()).toMatchObject({ activeVersion: 0, activeEpoch: 1, status: "active" });
        expect(harness.db.query("SELECT name FROM sqlite_master WHERE name = 'user'").get()).toBeNull();
        await expect(harness.catalog.queryAuth({ model: "user", where: [] }, 0)).rejects.toMatchObject({
            code: "CDB_STALE_EPOCH",
        });

        harness.catalog.beginSchemaMigration({ migrationId: "fresh-auth-v1", targetVersion: 1 });
        await harness.catalog.migrateSchemaShard({ migrationId: "fresh-auth-v1", shardId: "ShardDO_0" });
        harness.catalog.applyCatalogSchemaMigration({ migrationId: "fresh-auth-v1", version: 1 });
        expect(harness.catalog.completeSchemaMigration({ migrationId: "fresh-auth-v1" })).toMatchObject({
            activeVersion: 1,
            activeEpoch: 2,
            status: "active",
        });
        await expect(
            harness.catalog.mutateAuth(
                {
                    model: "user",
                    op: "create",
                    payload: {
                        id: "fresh-user",
                        name: "Fresh",
                        email: "fresh@example.com",
                        emailVerified: true,
                        createdAt: 1,
                        updatedAt: 1,
                        nickname: "new",
                    },
                },
                0
            )
        ).resolves.toMatchObject({ row: expect.objectContaining({ id: "fresh-user", nickname: "new" }) });
        expect(harness.db.query("SELECT COUNT(*) AS count FROM catalog_schema_steps").get()).toEqual({ count: 1 });
    });

    test("Catalog keeps tenant and principal epochs independent across restart", async () => {
        expect(await harness.catalog.bumpAuthEpoch("tenant", "tenant-a")).toBe(1);
        expect(await harness.catalog.bumpAuthEpoch("tenant", "tenant-b")).toBe(1);
        expect(await harness.catalog.bumpAuthEpoch("principal", "user-a")).toBe(1);
        expect(await harness.catalog.bumpAuthEpoch("principal", "user-b")).toBe(1);
        expect(await harness.catalog.bumpAuthEpoch("tenant", "tenant-a")).toBe(2);
        expect(await harness.catalog.bumpAuthEpoch("principal", "user-b")).toBe(2);

        await harness.restart();
        expect(harness.catalog.authEpoch({ tenantId: "tenant-a" as never, principalId: "user-a" as never })).toEqual({
            global: 1,
            tenant: 2,
            principal: 1,
        });
        expect(harness.catalog.authEpoch({ tenantId: "tenant-b" as never, principalId: "user-b" as never })).toEqual({
            global: 1,
            tenant: 1,
            principal: 2,
        });
    });

    test("derives canonical organization roles and current epochs from Catalog rows", async () => {
        const nowMs = Date.parse("2026-08-23T00:00:00Z");
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "authority-user",
                    name: "Authority User",
                    email: "authority@example.com",
                    emailVerified: true,
                    createdAt: nowMs,
                    updatedAt: nowMs,
                },
            },
            0
        );
        await harness.catalog.mutateAuth(
            {
                model: "organization",
                op: "create",
                payload: { id: "authority-org", name: "Authority Org", slug: "authority", createdAt: nowMs },
            },
            0
        );
        await harness.catalog.mutateAuth(
            {
                model: "member",
                op: "create",
                payload: {
                    id: "authority-member",
                    organizationId: "authority-org",
                    userId: "authority-user",
                    role: " member,admin, member ,owner ",
                    createdAt: nowMs,
                },
            },
            0
        );

        const expectedAuthority = {
            recoveryGeneration: 0,
            principalId: PrincipalId("authority-user"),
            organizationId: TenantId("authority-org"),
            role: "admin,member,owner",
            roles: ["admin", "member", "owner"],
            userRole: "user",
            authEpochs: { global: 1, tenant: 2, principal: 2 },
        } as const;
        const authorityRequest = {
            principalId: PrincipalId("authority-user"),
            organizationId: TenantId("authority-org"),
        };
        expect(await harness.catalog.resolveOrganizationAuthority(authorityRequest)).toEqual(expectedAuthority);
        expect(await harness.catalog.resolveOrganizationAuthorityRoute({ ...authorityRequest, vshard: 73 })).toEqual({
            authority: expectedAuthority,
            route: { shardId: ShardId("ShardDO_0"), schemaEpoch: 1, recoveryGeneration: 0, domainSchemaEpoch: 1 },
        });
    });

    test("derives user authority from the current Catalog user row", async () => {
        const nowMs = Date.parse("2026-08-23T00:00:00Z");
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "user-authority-subject",
                    name: "User Authority",
                    email: "user-authority@example.com",
                    emailVerified: true,
                    role: " user,admin,user ",
                    createdAt: nowMs,
                    updatedAt: nowMs,
                },
            },
            0
        );

        expect(
            await harness.catalog.resolveUserAuthority({ principalId: PrincipalId("user-authority-subject") })
        ).toEqual({
            recoveryGeneration: 0,
            principalId: PrincipalId("user-authority-subject"),
            role: "admin,user",
            roles: ["admin", "user"],
            authEpochs: { global: 1, tenant: 0, principal: 1 },
        });
        expect(
            await harness.catalog.resolveUserAuthority({ principalId: PrincipalId("missing-user-authority") })
        ).toBeNull();
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "default-role-user",
                    name: "Default Role User",
                    email: "default-role@example.com",
                    emailVerified: true,
                    createdAt: nowMs,
                    updatedAt: nowMs,
                },
            },
            0
        );
        expect(
            await harness.catalog.resolveUserAuthority({ principalId: PrincipalId("default-role-user") })
        ).toMatchObject({
            role: "user",
            roles: ["user"],
            authEpochs: { tenant: 0 },
        });

        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "delete",
                where: { id: "user-authority-subject" },
            },
            0
        );
        expect(
            await harness.catalog.resolveUserAuthority({ principalId: PrincipalId("user-authority-subject") })
        ).toBeNull();
    });

    test("isolates organizations and returns null for missing or revoked membership", async () => {
        const nowMs = Date.parse("2026-08-23T00:00:00Z");
        for (const organizationId of ["isolation-org-a", "isolation-org-b"]) {
            await harness.catalog.mutateAuth(
                {
                    model: "organization",
                    op: "create",
                    payload: { id: organizationId, name: organizationId, slug: organizationId, createdAt: nowMs },
                },
                0
            );
        }
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "isolation-user",
                    name: "Isolation User",
                    email: "isolation@example.com",
                    emailVerified: true,
                    createdAt: nowMs,
                    updatedAt: nowMs,
                },
            },
            0
        );

        const request = {
            principalId: PrincipalId("isolation-user"),
            organizationId: TenantId("isolation-org-a"),
        };
        expect(await harness.catalog.resolveOrganizationAuthority(request)).toBeNull();
        await harness.catalog.mutateAuth(
            {
                model: "member",
                op: "create",
                payload: {
                    id: "isolation-member",
                    organizationId: "isolation-org-a",
                    userId: "isolation-user",
                    role: "member",
                    createdAt: nowMs,
                },
            },
            0
        );
        expect(
            await harness.catalog.resolveOrganizationAuthority({
                ...request,
                organizationId: TenantId("isolation-org-b"),
            })
        ).toBeNull();
        await harness.catalog.mutateAuth(
            {
                model: "member",
                op: "delete",
                where: { id: "isolation-member" },
            },
            0
        );
        expect(await harness.catalog.resolveOrganizationAuthority(request)).toBeNull();
    });

    test("reflects membership role changes and their tenant/principal epoch bumps", async () => {
        const nowMs = Date.parse("2026-08-23T00:00:00Z");
        await harness.catalog.mutateAuth(
            {
                model: "user",
                op: "create",
                payload: {
                    id: "role-user",
                    name: "Role User",
                    email: "role@example.com",
                    emailVerified: true,
                    createdAt: nowMs,
                    updatedAt: nowMs,
                },
            },
            0
        );
        await harness.catalog.mutateAuth(
            {
                model: "organization",
                op: "create",
                payload: { id: "role-org", name: "Role Org", slug: "role-org", createdAt: nowMs },
            },
            0
        );
        await harness.catalog.mutateAuth(
            {
                model: "member",
                op: "create",
                payload: {
                    id: "role-member",
                    organizationId: "role-org",
                    userId: "role-user",
                    role: "member",
                    createdAt: nowMs,
                },
            },
            0
        );
        await harness.catalog.mutateAuth(
            {
                model: "member",
                op: "update",
                where: { id: "role-member" },
                payload: { role: "owner, admin" },
            },
            0
        );

        expect(
            await harness.catalog.resolveOrganizationAuthority({
                principalId: PrincipalId("role-user"),
                organizationId: TenantId("role-org"),
            })
        ).toMatchObject({
            role: "admin,owner",
            roles: ["admin", "owner"],
            authEpochs: { global: 1, tenant: 3, principal: 3 },
        });
    });

    test("fails closed when the organization authority models are unavailable", async () => {
        const fullSchema = synthesizeAuthSchema(auth.options as never) as Record<string, unknown>;
        for (const missingModel of ["organization", "member"]) {
            harness.close();
            const incompleteSchema = Object.fromEntries(
                Object.entries(fullSchema).filter(([model]) => model !== missingModel)
            );
            resetAuthRuntime();
            bindAuthRuntime({
                schema: incompleteSchema as never,
                options: auth.options as { readonly [key: string]: unknown },
            });
            harness = new CatalogHarness();
            await harness.ready();

            expect(
                await harness.catalog.resolveOrganizationAuthority({
                    principalId: PrincipalId("any-user"),
                    organizationId: TenantId("any-org"),
                })
            ).toBeNull();
        }
    });
});
