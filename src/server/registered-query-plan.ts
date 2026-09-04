import { type Column, type SQL, getTableColumns, getTableName, is } from "drizzle-orm";
import { QueryBuilder, type SQLiteSelectConfig, SQLiteTable, getTableConfig } from "drizzle-orm/sqlite-core";
import {
    CHARDB_SELECT_PLAN_MAX_LIMIT,
    type ChardbSelectPlanV1,
    compileChardbSelectPlanOrder,
    compileChardbSelectPlanPredicate,
    validateChardbSelectPlanV1,
} from "../binding-plan.ts";
import { StaticIntentExtractor, intervalsForColumnPredicate } from "../drizzle/walker.ts";
import { CdbError } from "../errors.ts";
import { stableHashHex } from "../util/canonical.ts";
import { isChardbVectorSearchBuilder, normalizeChardbVectorSearchBuilder } from "../vector.ts";
import type { CdbIntent } from "../wire.ts";
import { resolveCdbMeta } from "./cdb-table.ts";
import type { MutationAuthority } from "./define.ts";
import { type VectorResourceV1, resolveOrganizationVectorResourceDescriptor } from "./resource-descriptors.ts";

interface RegisteredQueryPlanBase {
    readonly version: 1;
    readonly authority: MutationAuthority;
    readonly partitionKey: string;
    readonly intent: CdbIntent;
    readonly limit: number;
    readonly planHash: string;
}

export interface RegisteredSelectQueryPlan extends RegisteredQueryPlanBase {
    readonly kind: "select";
    /** Canonical executable plan. Legacy routing metadata remains alongside it during migration. */
    readonly plan: ChardbSelectPlanV1;
    readonly projection: readonly { readonly key: string; readonly column: string }[];
    readonly orderBy: readonly { readonly column: string; readonly direction: "asc" | "desc" }[];
}

export interface RegisteredVectorQueryPlan extends RegisteredQueryPlanBase {
    readonly kind: "searchVector";
    readonly authority: "organization";
    readonly resource: VectorResourceV1;
    readonly values: readonly number[];
    readonly plan?: never;
    readonly projection?: never;
    readonly orderBy?: never;
}

export type RegisteredQueryPlan = RegisteredSelectQueryPlan | RegisteredVectorQueryPlan;

export function registeredSelectQueryPlanHash(plan: Omit<RegisteredSelectQueryPlan, "planHash">): string {
    return stableHashHex(plan);
}

interface PlannedSelectBuilder {
    readonly config: SQLiteSelectConfig;
    toSQL(): { readonly sql: string; readonly params: readonly unknown[] };
}

function unsupported(message: string): never {
    throw new CdbError({ code: "CDB_UNSUPPORTED_FEATURE", message: `planned query: ${message}` });
}

function isPlannedSelectBuilder(value: unknown): value is PlannedSelectBuilder {
    if (!value || typeof value !== "object") return false;
    const candidate = value as { readonly config?: unknown; readonly toSQL?: unknown };
    return typeof candidate.config === "object" && candidate.config !== null && typeof candidate.toSQL === "function";
}

function compileRegisteredVectorQueryPlan(value: unknown): RegisteredVectorQueryPlan {
    const search = normalizeChardbVectorSearchBuilder(value);
    const resource = resolveOrganizationVectorResourceDescriptor(search.column);
    const values = Object.freeze(search.values.map(item => (Object.is(item, -0) ? 0 : item)));
    const intent: CdbIntent = Object.freeze({
        kind: "select",
        tables: Object.freeze([resource.table]),
        partitionKey: Object.freeze({
            table: resource.table,
            column: resource.organizationColumn,
            values: Object.freeze([search.organizationId]),
        }),
    });
    const hashInput = {
        version: 1,
        kind: "searchVector",
        authority: "organization",
        partitionKey: search.organizationId,
        intent,
        resource,
        values,
        limit: search.limit,
    } as const;
    return Object.freeze({
        ...hashInput,
        planHash: stableHashHex(hashInput),
    });
}

function projectionFor(config: SQLiteSelectConfig, table: SQLiteTable) {
    const tableColumns = getTableColumns(table) as Record<string, Column>;
    const fields = Object.entries(config.fields);
    const expected = Object.entries(tableColumns);
    if (fields.length !== expected.length || fields.some(([key, field]) => tableColumns[key] !== field)) {
        unsupported("explicit projections are unavailable in the first planned-query version");
    }
    return fields.map(([key, field]) => ({ key, column: (field as Column).name }));
}

function primaryKeyColumns(table: SQLiteTable): readonly string[] {
    const config = getTableConfig(table);
    const inline = config.columns.filter(column => column.primary).map(column => column.name);
    const composite = config.primaryKeys.flatMap(key => key.columns.map(column => column.name));
    return [...new Set([...inline, ...composite])];
}

function authorityAndPartitionColumn(table: SQLiteTable): {
    readonly authority: MutationAuthority;
    readonly column: string;
} {
    const meta = resolveCdbMeta(table);
    if (meta.tenantKind === "org") {
        if (!meta.tenantBy)
            throw new CdbError({ code: "CDB_INVARIANT", message: `${meta.name}: missing organization column` });
        return { authority: "organization", column: meta.tenantBy };
    }
    if (meta.tenantKind === "user") {
        if (!meta.tenantBy) throw new CdbError({ code: "CDB_INVARIANT", message: `${meta.name}: missing user column` });
        return { authority: "user", column: meta.tenantBy };
    }
    if (meta.partitionBy.kind !== "colocate" || meta.partitionBy.via.length !== 1 || !meta.partitionBy.via[0]) {
        unsupported(`${meta.name} requires one global partition column`);
    }
    return { authority: "global", column: meta.partitionBy.via[0] };
}

/** Compile one synchronous, sessionless Drizzle select before Catalog authority lookup. */
export function compileRegisteredQueryPlan<TDb, TArgs>(
    query: (db: TDb, args: TArgs) => unknown,
    args: TArgs
): RegisteredQueryPlan {
    const planningDb = new QueryBuilder();
    const built = query(planningDb as TDb, args);
    if (isChardbVectorSearchBuilder(built)) return compileRegisteredVectorQueryPlan(built);
    if (!isPlannedSelectBuilder(built)) {
        if (built && typeof built === "object" && typeof (built as { then?: unknown }).then === "function") {
            unsupported("query callback must return a builder synchronously");
        }
        unsupported("query callback must return a Drizzle select builder");
    }

    const config = built.config;
    if (!is(config.table, SQLiteTable)) unsupported("FROM must reference one concrete SQLite cdbTable");
    const table = config.table as SQLiteTable;
    if (config.withList?.length) unsupported("CTEs are unavailable");
    if (config.joins?.length) unsupported("joins are unavailable");
    if (config.distinct) unsupported("DISTINCT is unavailable");
    if (config.groupBy?.length || config.having) unsupported("grouped queries are unavailable");
    if (config.setOperators.length) unsupported("set operators are unavailable");
    if (config.offset !== undefined) unsupported("offset pagination is unavailable");
    if (
        !Number.isSafeInteger(config.limit) ||
        (config.limit as number) < 1 ||
        (config.limit as number) > CHARDB_SELECT_PLAN_MAX_LIMIT
    ) {
        unsupported(`limit must be an integer from 1 through ${CHARDB_SELECT_PLAN_MAX_LIMIT}`);
    }
    if (!config.where) {
        throw new CdbError({
            code: "CDB_CROSS_PARTITION",
            message: "planned query requires an exact placement predicate",
        });
    }
    const where = compileChardbSelectPlanPredicate(config.where, table);

    const { authority, column: partitionColumn } = authorityAndPartitionColumn(table);
    const tableName = getTableName(table);
    const baseIntent = new StaticIntentExtractor({ [tableName]: partitionColumn }).forSelect({
        tables: [tableName],
        where: config.where,
    });
    const partitionValues = baseIntent.partitionKey?.values;
    if (
        baseIntent.joinShape !== "colocated" ||
        partitionValues?.length !== 1 ||
        typeof partitionValues[0] !== "string" ||
        partitionValues[0].length === 0
    ) {
        throw new CdbError({
            code: "CDB_CROSS_PARTITION",
            message: "planned query must constrain its placement column to one nonempty string",
        });
    }

    const intervals = Object.values(getTableColumns(table) as Record<string, Column>).flatMap(column => {
        const observed = intervalsForColumnPredicate(config.where as SQL, tableName, column.name);
        return observed === "full" || observed.length === 0
            ? []
            : [{ table: tableName, indexName: column.name, intervals: observed }];
    });
    const intent: CdbIntent = {
        ...baseIntent,
        ...(intervals.length > 0 ? { intervals } : {}),
    };
    const projection = projectionFor(config, table);
    const compiledOrderBy = (config.orderBy ?? []).map(item => compileChardbSelectPlanOrder(item, table));
    const plan = validateChardbSelectPlanV1({
        version: 1,
        kind: "select",
        table: tableName,
        selection: { kind: "all" },
        where,
        orderBy: compiledOrderBy,
        limit: config.limit,
        cardinality: "many",
    });
    const orderBy = plan.orderBy ?? [];
    const limit = plan.limit;
    if (limit === undefined) throw new CdbError({ code: "CDB_INVARIANT", message: "planned query lost its limit" });
    const primaryKeys = primaryKeyColumns(table);
    if (primaryKeys.length === 0) unsupported("table must declare a primary key");
    const suffix = orderBy.slice(-primaryKeys.length).map(item => item.column);
    if (suffix.length !== primaryKeys.length || suffix.some((column, index) => column !== primaryKeys[index])) {
        unsupported(
            `ORDER BY must end with primary key column${primaryKeys.length === 1 ? "" : "s"} ${primaryKeys.join(", ")}`
        );
    }
    const hashInput = {
        version: 1,
        kind: "select",
        plan,
        authority,
        partitionKey: partitionValues[0],
        intent,
        projection,
        orderBy,
        limit,
    } as const;
    return {
        ...hashInput,
        planHash: registeredSelectQueryPlanHash(hashInput),
    };
}
