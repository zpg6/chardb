import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { admin } from "better-auth/plugins/admin";
import { organization } from "better-auth/plugins/organization";
import { bindAuthRuntime, resetAuthRuntime } from "../../src/auth/runtime.ts";
import { defineAuth, synthesizeAuthSchema } from "../../src/auth/synthesize.ts";
import {
    bumpCatalogAuthEpoch,
    initializeCatalogAuthorityStorage,
    mutateCatalogAuthWithEffects,
    readCatalogAuthEpoch,
    resolveOrganizationAuthorityFromCatalog,
    resolveUserAuthorityFromCatalog,
} from "../../src/server/do/catalog-authority-store.ts";
import { initializeCatalogStorage } from "../../src/server/do/catalog-schema-store.ts";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";
import { defineMigrations } from "../../src/server/schema-migrations.ts";
import { PrincipalId, TenantId } from "../../src/types.ts";

interface Cursor<T> extends Iterable<T> {
    readonly columnNames: string[];
    raw(): IterableIterator<unknown[]>;
}

function sqlStorage(db: Database) {
    return {
        exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): Cursor<T> {
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

describe("Catalog authority store", () => {
    let db: Database;
    let sql: ReturnType<typeof adaptSqlStorage>;

    beforeEach(() => {
        const auth = defineAuth({ plugins: [organization(), admin()] });
        resetAuthRuntime();
        bindAuthRuntime({
            schema: synthesizeAuthSchema(auth.options as never) as never,
            options: auth.options as { readonly [key: string]: unknown },
        });
        db = new Database(":memory:");
        sql = adaptSqlStorage(sqlStorage(db));
        initializeCatalogStorage(sql, defineMigrations([]));
        db.transaction(() => initializeCatalogAuthorityStorage(sql))();
    });

    afterEach(() => {
        db.close();
        resetAuthRuntime();
    });

    test("owns persisted organization and user authority projections", () => {
        const now = Date.parse("2026-08-25T00:00:00Z");
        db.transaction(() => {
            mutateCatalogAuthWithEffects(sql, {
                model: "user",
                op: "create",
                payload: {
                    id: "authority-user",
                    name: "Authority User",
                    email: "authority@example.com",
                    emailVerified: true,
                    role: " user,admin,user ",
                    createdAt: now,
                    updatedAt: now,
                },
            });
            mutateCatalogAuthWithEffects(sql, {
                model: "organization",
                op: "create",
                payload: { id: "authority-org", name: "Authority Org", slug: "authority", createdAt: now },
            });
            mutateCatalogAuthWithEffects(sql, {
                model: "member",
                op: "create",
                payload: {
                    id: "authority-member",
                    organizationId: "authority-org",
                    userId: "authority-user",
                    role: " member,admin, member ,owner ",
                    createdAt: now,
                },
            });
        })();

        const principalId = PrincipalId("authority-user");
        const organizationId = TenantId("authority-org");
        expect(resolveOrganizationAuthorityFromCatalog(sql, { principalId, organizationId })).toEqual({
            principalId,
            organizationId,
            role: "admin,member,owner",
            roles: ["admin", "member", "owner"],
            userRole: "admin,user",
            authEpochs: { global: 1, tenant: 2, principal: 2 },
        });
        expect(resolveUserAuthorityFromCatalog(sql, { principalId })).toEqual({
            principalId,
            role: "admin,user",
            roles: ["admin", "user"],
            authEpochs: { global: 1, tenant: 0, principal: 2 },
        });

        db.transaction(() =>
            mutateCatalogAuthWithEffects(sql, {
                model: "user",
                op: "update",
                where: { id: principalId },
                payload: { role: "user" },
            })
        )();
        expect(resolveOrganizationAuthorityFromCatalog(sql, { principalId, organizationId })).toMatchObject({
            role: "admin,member,owner",
            roles: ["admin", "member", "owner"],
            userRole: "user",
            authEpochs: { global: 1, tenant: 2, principal: 3 },
        });
    });

    test("keeps authority writes and epoch bumps in one SQLite transaction", () => {
        const principalId = PrincipalId("rollback-user");
        expect(() =>
            db.transaction(() => {
                mutateCatalogAuthWithEffects(sql, {
                    model: "user",
                    op: "create",
                    payload: {
                        id: principalId,
                        name: "Rollback User",
                        email: "rollback@example.com",
                        emailVerified: true,
                        createdAt: 1,
                        updatedAt: 1,
                    },
                });
                throw new Error("rollback probe");
            })()
        ).toThrow("rollback probe");

        expect(resolveUserAuthorityFromCatalog(sql, { principalId })).toBeNull();
        expect(readCatalogAuthEpoch(sql, { principalId })).toEqual({ global: 1, tenant: 0, principal: 0 });
    });

    test("increments tenant and principal epochs without cross-scope leakage", () => {
        db.transaction(() => {
            expect(bumpCatalogAuthEpoch(sql, "tenant", "org-a")).toBe(1);
            expect(bumpCatalogAuthEpoch(sql, "tenant", "org-a")).toBe(2);
            expect(bumpCatalogAuthEpoch(sql, "tenant", "org-b")).toBe(1);
            expect(bumpCatalogAuthEpoch(sql, "principal", "user-a")).toBe(1);
        })();

        expect(
            readCatalogAuthEpoch(sql, {
                tenantId: TenantId("org-a"),
                principalId: PrincipalId("user-a"),
            })
        ).toEqual({ global: 1, tenant: 2, principal: 1 });
        expect(
            readCatalogAuthEpoch(sql, {
                tenantId: TenantId("org-b"),
                principalId: PrincipalId("user-b"),
            })
        ).toEqual({ global: 1, tenant: 1, principal: 0 });
    });

    test("reports the exact organizations removed by a non-id predicate", () => {
        db.transaction(() => {
            for (const [id, name] of [
                ["org-a", "Retire"],
                ["org-b", "Retire"],
                ["org-c", "Keep"],
            ] as const) {
                mutateCatalogAuthWithEffects(sql, {
                    model: "organization",
                    op: "create",
                    payload: { id, name, slug: id, createdAt: 1 },
                });
            }
        })();

        const outcome = db.transaction(() =>
            mutateCatalogAuthWithEffects(sql, {
                model: "organization",
                op: "delete",
                where: { name: "Retire" },
            })
        )();
        expect(outcome.result).toMatchObject({ ok: true, affected: 2 });
        expect(outcome.deletedOrganizationIds).toEqual(["org-a", "org-b"]);
        expect(
            mutateCatalogAuthWithEffects(sql, {
                model: "member",
                op: "delete",
                where: { organizationId: "org-c" },
            }).deletedOrganizationIds
        ).toEqual([]);
    });
});
