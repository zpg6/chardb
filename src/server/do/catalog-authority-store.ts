import { renderSqliteTableDdl } from "../../auth/ddl.ts";
import { getAuthRuntime, placementFor, tableFor } from "../../auth/runtime.ts";
import {
    type AuthIncrementWhere,
    type AuthReadWhere,
    assertAuthIncrementInput,
    authCount,
    authCreate,
    authDelete,
    authFindFirstId,
    authFindFirstIncrementId,
    authFindMany,
    authFindOne,
    authIncrementOne,
    authPreloadScopeRows,
    authTableColumns,
    authUpdate,
} from "../../auth/sql.ts";
import { CdbError, type CdbErrorCode } from "../../errors.ts";
import type { PrincipalId, RawJson, TenantId } from "../../types.ts";
import type { CatalogSql } from "./catalog-schema-store.ts";

export type AuthEpochScope = "global" | "tenant" | "principal";

export interface CatalogAuthEpochChange {
    readonly scope: AuthEpochScope;
    readonly scopeId: string;
    readonly epoch: number;
}

export interface CatalogAuthMutationRequest {
    readonly model: string;
    readonly op: "create" | "update" | "delete";
    readonly where?: { readonly [k: string]: RawJson };
    readonly payload?: { readonly [k: string]: RawJson };
    readonly returnRow?: boolean;
    readonly limitOne?: boolean;
}

export interface CatalogAuthMutationResult {
    readonly ok: true;
    readonly row?: Record<string, RawJson> | null;
    readonly affected?: number;
}

export interface CatalogAuthMutationOutcome {
    readonly result: CatalogAuthMutationResult;
    /** Internal durable side effects discovered from the exact rows deleted in this transaction. */
    readonly deletedOrganizationIds: readonly string[];
    /** Exact epochs advanced by this transaction, for the durable invalidation handoff. */
    readonly authEpochChanges: readonly CatalogAuthEpochChange[];
}

export interface CatalogAuthIncrementOutcome {
    readonly result: CatalogAuthIncrementResult;
    readonly authEpochChanges: readonly CatalogAuthEpochChange[];
}

export interface CatalogAuthIncrementRequest {
    readonly model: string;
    readonly where: readonly AuthIncrementWhere[];
    readonly increment: { readonly [k: string]: number };
    readonly set?: { readonly [k: string]: RawJson };
}

export interface CatalogAuthIncrementResult {
    readonly ok: true;
    readonly row: Record<string, RawJson> | null;
    readonly affected: number;
}

export interface CatalogAuthQueryRequest {
    readonly model: string;
    readonly where: readonly AuthReadWhere[];
    readonly limit?: number;
    readonly offset?: number;
    readonly sortBy?: { readonly field: string; readonly direction: "asc" | "desc" };
}

export type CatalogAuthAdapterRpcRequest =
    | { readonly operation: "mutate"; readonly args: CatalogAuthMutationRequest; readonly recoveryGeneration: number }
    | {
          readonly operation: "increment";
          readonly args: CatalogAuthIncrementRequest;
          readonly recoveryGeneration: number;
      }
    | { readonly operation: "query"; readonly args: CatalogAuthQueryRequest; readonly recoveryGeneration: number }
    | {
          readonly operation: "count";
          readonly args: Pick<CatalogAuthQueryRequest, "model" | "where">;
          readonly recoveryGeneration: number;
      };

export type CatalogAuthAdapterRpcValue =
    | CatalogAuthMutationResult
    | CatalogAuthIncrementResult
    | readonly Record<string, RawJson>[]
    | number;

export type CatalogAuthAdapterRpcResult =
    | { readonly ok: true; readonly value: CatalogAuthAdapterRpcValue }
    | {
          readonly ok: false;
          readonly error: {
              readonly code: CdbErrorCode;
              readonly message: string;
              readonly hint?: string;
          };
      };

export interface OrganizationAuthorityRequest {
    /** Subject from a successfully signature-verified JWT. */
    readonly principalId: PrincipalId;
    /** Organization selected by the operation, not by JWT role claims. */
    readonly organizationId: TenantId;
}

export interface OrganizationAuthority {
    readonly recoveryGeneration: number;
    readonly principalId: PrincipalId;
    readonly organizationId: TenantId;
    /** Canonical comma-separated Better Auth membership role. */
    readonly role: string;
    /** Sorted, deduplicated membership roles. */
    readonly roles: readonly string[];
    /** Canonical Better Auth user role, independently sourced from the user row. */
    readonly userRole?: string;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
}

export interface UserAuthorityRequest {
    /** Subject from a successfully signature-verified JWT. */
    readonly principalId: PrincipalId;
}

export interface UserAuthority {
    readonly recoveryGeneration: number;
    readonly principalId: PrincipalId;
    /** Canonical comma-separated Better Auth user role. */
    readonly role: string;
    /** Sorted, deduplicated user roles. */
    readonly roles: readonly string[];
    readonly authEpochs: {
        readonly global: number;
        /** User scope has no organization epoch. */
        readonly tenant: 0;
        readonly principal: number;
    };
}

function addEpochScope(
    scopes: Map<string, { scope: AuthEpochScope; scopeId: string }>,
    scope: AuthEpochScope,
    scopeId: string
): void {
    scopes.set(`${scope}\0${scopeId}`, { scope, scopeId });
}

function addRowEpochScopes(
    scopes: Map<string, { scope: AuthEpochScope; scopeId: string }>,
    model: string,
    row: Record<string, RawJson>
): void {
    const rule = placementFor(model);
    if (rule.kind === "replicated") {
        addEpochScope(scopes, "global", "global");
    } else {
        const scopeId = row[rule.column];
        if (typeof scopeId === "string") addEpochScope(scopes, rule.kind, scopeId);
    }
    if (typeof row.organizationId === "string") addEpochScope(scopes, "tenant", row.organizationId);
    if (typeof row.userId === "string") addEpochScope(scopes, "principal", row.userId);
}

function authEpochScopeColumns(model: string, table: Parameters<typeof authTableColumns>[0]): readonly string[] {
    const schemaColumns = authTableColumns(table);
    const placement = placementFor(model);
    const candidates = [...(placement.kind === "replicated" ? [] : [placement.column]), "organizationId", "userId"];
    return [...new Set(candidates.filter(column => schemaColumns.has(column)))];
}

export function bumpCatalogAuthEpoch(sql: CatalogSql, scope: AuthEpochScope, scopeId: string): number {
    const dbScope = scope === "global" ? "auth_global" : scope === "tenant" ? "auth_tenant" : "auth_principal";
    sql.exec("INSERT OR IGNORE INTO catalog_epoch (scope, scope_id, epoch) VALUES (?, ?, 0)", dbScope, scopeId);
    sql.exec("UPDATE catalog_epoch SET epoch = epoch + 1 WHERE scope = ? AND scope_id = ?", dbScope, scopeId);
    const row = sql.one<{ epoch: number }>(
        "SELECT epoch FROM catalog_epoch WHERE scope = ? AND scope_id = ?",
        dbScope,
        scopeId
    );
    return row?.epoch ?? 1;
}

export function readCatalogEpoch(sql: CatalogSql, scope: string, scopeId: string): number {
    const row = sql.one<{ epoch: number }>(
        "SELECT epoch FROM catalog_epoch WHERE scope = ? AND scope_id = ?",
        scope,
        scopeId
    );
    return row?.epoch ?? 0;
}

export function readCatalogAuthEpoch(
    sql: CatalogSql,
    args: { readonly tenantId?: TenantId; readonly principalId?: PrincipalId }
): { readonly global: number; readonly tenant: number; readonly principal: number } {
    return {
        global: readCatalogEpoch(sql, "auth_global", "global"),
        tenant: args.tenantId ? readCatalogEpoch(sql, "auth_tenant", args.tenantId) : 0,
        principal: args.principalId ? readCatalogEpoch(sql, "auth_principal", args.principalId) : 0,
    };
}

export function initializeCatalogAuthorityStorage(sql: CatalogSql): void {
    let runtime: ReturnType<typeof getAuthRuntime>;
    try {
        runtime = getAuthRuntime();
    } catch {
        return;
    }
    for (const table of Object.values(runtime.schema)) {
        const ddl = renderSqliteTableDdl(table);
        const metadataKey = `auth_ddl_v1:${ddl.tableName}`;
        const existing = sql.one<{ sql: string }>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            ddl.tableName
        );
        const recorded = sql.one<{ v: string }>("SELECT v FROM catalog_meta WHERE k = ?", metadataKey);
        if (existing) {
            if (recorded?.v !== ddl.signature) {
                throw new CdbError({
                    code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                    message: `Catalog auth table "${ddl.tableName}" predates auth DDL v1 or has an incompatible schema`,
                    hint: "recreate pre-release Catalog storage or add an explicit auth schema migration",
                });
            }
            for (let index = 0; index < ddl.indexNames.length; index++) {
                const indexName = ddl.indexNames[index];
                const expectedSql = ddl.indexes[index];
                if (!indexName || !expectedSql) {
                    throw new CdbError({
                        code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                        message: `Catalog auth table "${ddl.tableName}" has invalid index metadata`,
                    });
                }
                const present = sql.one<{ sql: string }>(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ? AND tbl_name = ?",
                    indexName,
                    ddl.tableName
                );
                if (present?.sql !== expectedSql) {
                    throw new CdbError({
                        code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                        message: `Catalog auth table "${ddl.tableName}" has an incompatible declared index`,
                        hint: "recreate pre-release Catalog storage or add an explicit auth schema migration",
                    });
                }
            }
            continue;
        }
        sql.exec(ddl.createTable);
        for (const statement of ddl.indexes) sql.exec(statement);
        sql.exec("INSERT OR REPLACE INTO catalog_meta (k, v) VALUES (?, ?)", metadataKey, ddl.signature);
    }
}

export function recordMigratedCatalogAuthoritySchema(sql: CatalogSql): void {
    let runtime: ReturnType<typeof getAuthRuntime>;
    try {
        runtime = getAuthRuntime();
    } catch (error) {
        if (error instanceof CdbError && error.code === "CDB_AUTH_NOT_BOUND") return;
        throw error;
    }
    const rendered = Object.values(runtime.schema)
        .map(table => renderSqliteTableDdl(table))
        .sort((left, right) => left.tableName.localeCompare(right.tableName));
    const expectedNames = new Set(rendered.map(ddl => ddl.tableName));
    for (const ddl of rendered) {
        const table = sql.one<{ sql: string }>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            ddl.tableName
        );
        if (table?.sql !== ddl.createTable) {
            throw new CdbError({
                code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                message: `Catalog auth migration did not produce table "${ddl.tableName}"`,
            });
        }
        for (let index = 0; index < ddl.indexNames.length; index++) {
            const name = ddl.indexNames[index];
            const expectedSql = ddl.indexes[index];
            if (!name || !expectedSql) {
                throw new CdbError({
                    code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                    message: `Catalog auth migration has invalid index metadata for "${ddl.tableName}"`,
                });
            }
            const present = sql.one<{ sql: string }>(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ? AND tbl_name = ?",
                name,
                ddl.tableName
            );
            if (present?.sql !== expectedSql) {
                throw new CdbError({
                    code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                    message: `Catalog auth migration did not produce index "${String(name)}"`,
                });
            }
        }
        sql.exec(
            "INSERT OR REPLACE INTO catalog_meta (k, v) VALUES (?, ?)",
            `auth_ddl_v1:${ddl.tableName}`,
            ddl.signature
        );
    }
    const recorded = sql.all<{ k: string }>(
        "SELECT k FROM catalog_meta WHERE substr(k, 1, 12) = 'auth_ddl_v1:' ORDER BY k"
    );
    for (const row of recorded) {
        const tableName = row.k.slice(12);
        if (expectedNames.has(tableName)) continue;
        const existing = sql.one<{ present: number }>(
            "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ?",
            tableName
        );
        if (existing) {
            throw new CdbError({
                code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
                message: `Catalog auth migration left removed table "${tableName}" in storage`,
            });
        }
        sql.exec("DELETE FROM catalog_meta WHERE k = ?", row.k);
    }
}

export function mutateCatalogAuthWithEffects(
    sql: CatalogSql,
    args: CatalogAuthMutationRequest
): CatalogAuthMutationOutcome {
    const table = tableFor(args.model);
    const scopeColumns = authEpochScopeColumns(args.model, table);
    const placement = placementFor(args.model);
    const scopes = new Map<string, { scope: AuthEpochScope; scopeId: string }>();
    let row: Record<string, RawJson> | null | undefined;
    let affected = 0;
    const deletedOrganizationIds = new Set<string>();
    switch (args.op) {
        case "create": {
            if (!args.payload) throw new Error("auth: create requires payload");
            row = authCreate(sql, table, args.payload);
            affected = 1;
            addRowEpochScopes(scopes, args.model, row);
            break;
        }
        case "update": {
            if (!args.where || !args.payload) throw new Error("auth: update requires where and payload");
            if (args.limitOne && Object.keys(args.where).length === 0) break;
            const mutationWhere = args.limitOne
                ? (() => {
                      const id = authFindFirstId(sql, table, args.where);
                      return id === null ? null : { id };
                  })()
                : args.where;
            if (!mutationWhere) break;
            const before = authPreloadScopeRows(
                sql,
                table,
                mutationWhere,
                scopeColumns,
                scopeColumns.map(column => args.payload?.[column]),
                args.payload
            );
            const returnRow = args.returnRow !== false;
            const fullBefore = returnRow && before.matchedRows > 0 ? authFindOne(sql, table, mutationWhere) : null;
            const result = authUpdate(sql, table, mutationWhere, args.payload, returnRow);
            affected = result.affected;
            if (affected > 0) {
                if (placement.kind === "replicated") addEpochScope(scopes, "global", "global");
                row = returnRow ? (result.row ?? (fullBefore ? { ...fullBefore, ...args.payload } : null)) : null;
                for (const previous of before.rows) {
                    addRowEpochScopes(scopes, args.model, previous);
                    const next: Record<string, RawJson> = { ...previous };
                    for (const column of scopeColumns) {
                        if (Object.hasOwn(args.payload, column)) next[column] = args.payload[column] as RawJson;
                    }
                    addRowEpochScopes(scopes, args.model, next);
                }
            } else {
                row = result.row;
            }
            break;
        }
        case "delete": {
            if (!args.where) throw new Error("auth: delete requires where");
            if (args.limitOne && Object.keys(args.where).length === 0) break;
            const mutationWhere = args.limitOne
                ? (() => {
                      const id = authFindFirstId(sql, table, args.where);
                      return id === null ? null : { id };
                  })()
                : args.where;
            if (!mutationWhere) break;
            // This path is used by Better Auth's native consumeOne adapter
            // operation. Catalog serializes the read and delete inside the
            // same Durable Object transaction, preserving single-use tokens.
            const fullBefore = args.returnRow ? authFindOne(sql, table, mutationWhere) : null;
            const preloadColumns = args.model === "organization" ? [...new Set([...scopeColumns, "id"])] : scopeColumns;
            const before = authPreloadScopeRows(sql, table, mutationWhere, preloadColumns);
            affected = authDelete(sql, table, mutationWhere).affected;
            if (affected > 0) {
                row = fullBefore;
                if (placement.kind === "replicated") addEpochScope(scopes, "global", "global");
                for (const previous of before.rows) {
                    addRowEpochScopes(scopes, args.model, previous);
                    if (args.model === "organization" && typeof previous.id === "string") {
                        deletedOrganizationIds.add(previous.id);
                    }
                }
            }
            break;
        }
    }
    const authEpochChanges = [...scopes.values()].map(scope => ({
        ...scope,
        epoch: bumpCatalogAuthEpoch(sql, scope.scope, scope.scopeId),
    }));
    return {
        result: { ok: true, row: row ?? null, affected },
        deletedOrganizationIds: Object.freeze([...deletedOrganizationIds].sort()),
        authEpochChanges: Object.freeze(authEpochChanges),
    };
}

export function mutateCatalogAuth(sql: CatalogSql, args: CatalogAuthMutationRequest): CatalogAuthMutationResult {
    return mutateCatalogAuthWithEffects(sql, args).result;
}

export function incrementCatalogAuth(sql: CatalogSql, args: CatalogAuthIncrementRequest): CatalogAuthIncrementResult {
    return incrementCatalogAuthWithEffects(sql, args).result;
}

export function incrementCatalogAuthWithEffects(
    sql: CatalogSql,
    args: CatalogAuthIncrementRequest
): CatalogAuthIncrementOutcome {
    const table = tableFor(args.model);
    const scopeColumns = authEpochScopeColumns(args.model, table);
    const placement = placementFor(args.model);
    const set = args.set ?? {};
    assertAuthIncrementInput(table, args.where, args.increment, set);
    const targetId = authFindFirstIncrementId(sql, table, args.where);
    if (targetId === null) return { result: { ok: true, row: null, affected: 0 }, authEpochChanges: [] };

    const replacementAccounting = Object.create(null) as Record<string, RawJson>;
    for (const key of Object.keys(set)) replacementAccounting[key] = set[key] as RawJson;
    for (const [key, delta] of Object.entries(args.increment)) replacementAccounting[key] = delta;
    const before = authPreloadScopeRows(
        sql,
        table,
        { id: targetId },
        scopeColumns,
        scopeColumns.map(column => set[column]),
        replacementAccounting
    );
    if (before.matchedRows !== 1) {
        throw new CdbError({
            code: "CDB_INVARIANT",
            message: "Catalog incrementAuth selected a row that disappeared inside one transaction",
        });
    }
    const result = authIncrementOne(sql, table, targetId, args.where, args.increment, set);
    if (result.affected === 0) {
        return { result: { ok: true, row: result.row, affected: 0 }, authEpochChanges: [] };
    }
    if (!result.row) {
        throw new CdbError({
            code: "CDB_INVARIANT",
            message: "Catalog incrementAuth updated a row but could not reload it",
        });
    }
    const scopes = new Map<string, { scope: AuthEpochScope; scopeId: string }>();
    if (placement.kind === "replicated") addEpochScope(scopes, "global", "global");
    for (const previous of before.rows) addRowEpochScopes(scopes, args.model, previous);
    addRowEpochScopes(scopes, args.model, result.row);
    const authEpochChanges = [...scopes.values()].map(scope => ({
        ...scope,
        epoch: bumpCatalogAuthEpoch(sql, scope.scope, scope.scopeId),
    }));
    return {
        result: { ok: true, row: result.row, affected: result.affected },
        authEpochChanges: Object.freeze(authEpochChanges),
    };
}

export function queryCatalogAuth(sql: CatalogSql, args: CatalogAuthQueryRequest): readonly Record<string, RawJson>[] {
    return authFindMany(sql, tableFor(args.model), args.where, args.limit, args.offset, args.sortBy);
}

export function countCatalogAuth(
    sql: CatalogSql,
    args: { readonly model: string; readonly where: readonly AuthReadWhere[] }
): number {
    return authCount(sql, tableFor(args.model), args.where);
}

export function catalogOrganizationAuthorityAvailable(): boolean {
    try {
        const schema = getAuthRuntime().schema as unknown as Record<string, unknown>;
        return Boolean(schema.organization && schema.member);
    } catch (error) {
        if (error instanceof CdbError && error.code === "CDB_AUTH_NOT_BOUND") return false;
        throw error;
    }
}

export function catalogUserAuthorityAvailable(): boolean {
    try {
        return Boolean((getAuthRuntime().schema as unknown as Record<string, unknown>).user);
    } catch (error) {
        if (error instanceof CdbError && error.code === "CDB_AUTH_NOT_BOUND") return false;
        throw error;
    }
}

export function resolveOrganizationAuthorityFromCatalog(
    sql: CatalogSql,
    args: OrganizationAuthorityRequest
): Omit<OrganizationAuthority, "recoveryGeneration"> | null {
    const organization = authFindOne(sql, tableFor("organization"), { id: args.organizationId });
    if (!organization) return null;
    const membership = authFindOne(sql, tableFor("member"), {
        organizationId: args.organizationId,
        userId: args.principalId,
    });
    const roles = canonicalMembershipRoles(membership?.role);
    if (roles.length === 0) return null;
    const user = authFindOne(sql, tableFor("user"), { id: args.principalId });
    if (!user) return null;
    const storedUserRoles = canonicalMembershipRoles(user.role);
    const userRoles = storedUserRoles.length === 0 ? ["user"] : storedUserRoles;
    return {
        principalId: args.principalId,
        organizationId: args.organizationId,
        role: roles.join(","),
        roles,
        userRole: userRoles.join(","),
        authEpochs: readCatalogAuthEpoch(sql, {
            tenantId: args.organizationId,
            principalId: args.principalId,
        }),
    };
}

export function resolveUserAuthorityFromCatalog(
    sql: CatalogSql,
    args: UserAuthorityRequest
): Omit<UserAuthority, "recoveryGeneration"> | null {
    const user = authFindOne(sql, tableFor("user"), { id: args.principalId });
    if (!user) return null;
    const storedRoles = canonicalMembershipRoles(user.role);
    const roles = storedRoles.length === 0 ? ["user"] : storedRoles;
    const epochs = readCatalogAuthEpoch(sql, { principalId: args.principalId });
    return {
        principalId: args.principalId,
        role: roles.join(","),
        roles,
        authEpochs: { global: epochs.global, tenant: 0, principal: epochs.principal },
    };
}

export function canonicalMembershipRoles(value: RawJson | undefined): readonly string[] {
    if (typeof value !== "string") return [];
    return [
        ...new Set(
            value
                .split(",")
                .map(role => role.trim())
                .filter(Boolean)
        ),
    ].sort();
}
