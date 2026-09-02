/**
 * `chardbAuthAdapter` — chardb's native better-auth `database` adapter.
 *
 * Implements the `CustomAdapter` contract from
 * `@better-auth/core/db/adapter` so it composes with
 * `createAdapterFactory({adapter, config})` and slots straight into
 * `betterAuth({database: chardbAuthAdapter(env)})`. Every auth model is
 * stored in the singleton Catalog DO. Better Auth routinely looks up rows
 * by session token, email, provider/account key, and membership fields;
 * central storage keeps those reads deterministic without shard fan-out or
 * a second set of auth-specific indexes.
 *
 * Catalog applies each auth write and every affected auth-epoch bump in
 * one SQLite transaction. The adapter does not issue a second invalidation
 * RPC after the row has committed.
 *
 * Reads support Better Auth's full filter operator set joined by AND.
 * Writes use equality predicates.
 */

import type { AdapterFactory, CleanedWhere } from "@better-auth/core/db/adapter";
import type { BetterAuthOptions } from "better-auth";
import { createAdapterFactory } from "better-auth/adapters";
import { CdbError } from "../errors.ts";
import type {
    CatalogAuthAdapterRpcRequest,
    CatalogAuthAdapterRpcResult,
    CatalogAuthIncrementResult,
    CatalogAuthMutationResult,
} from "../server/do/catalog-authority-store.ts";
import type { RawJson } from "../types.ts";
import { AUTH_READ_IN_MAX_VALUES, type AuthIncrementWhere, type AuthReadWhere } from "./sql.ts";

/**
 * Bindings the adapter needs at runtime. Provided by `mountChardb` /
 * `chardb({auth})` once the inbound `env` is known.
 */
export interface ChardbAuthAdapterEnv {
    readonly CDB_CATALOG: DurableObjectNamespace;
}

interface CatalogRpc {
    authAdapterRpc(args: CatalogAuthAdapterRpcRequest): Promise<CatalogAuthAdapterRpcResult>;
}

type UnstampedCatalogAuthRequest<T> = T extends unknown ? Omit<T, "recoveryGeneration"> : never;
type CatalogAuthRequest = UnstampedCatalogAuthRequest<CatalogAuthAdapterRpcRequest>;

interface CatalogClient {
    authAdapterRpc(args: CatalogAuthRequest): Promise<CatalogAuthAdapterRpcResult>;
}

export interface ChardbAuthAdapterOptions {
    readonly env: ChardbAuthAdapterEnv;
    /** Fixed generation or a lazy resolver used before each Catalog operation. */
    readonly recoveryGeneration: number | (() => Promise<number>);
}

async function callCatalog<T>(catalog: CatalogClient, request: CatalogAuthRequest): Promise<T> {
    const response = await catalog.authAdapterRpc(request);
    if (!response.ok) throw new CdbError(response.error);
    return response.value as T;
}

export function chardbAuthAdapter(opts: ChardbAuthAdapterOptions): AdapterFactory<BetterAuthOptions> {
    const { env } = opts;
    const recoveryGeneration =
        typeof opts.recoveryGeneration === "function"
            ? opts.recoveryGeneration
            : async (): Promise<number> => opts.recoveryGeneration as number;
    const catalog = (): CatalogClient => {
        const rpc = env.CDB_CATALOG.get(env.CDB_CATALOG.idFromName("global")) as unknown as CatalogRpc;
        return {
            authAdapterRpc: async request => {
                const generation = await recoveryGeneration();
                if (!Number.isSafeInteger(generation) || generation < 0) {
                    throw new CdbError({
                        code: "CDB_CATALOG_UNAVAILABLE",
                        message: "Catalog recovery generation is invalid",
                    });
                }
                return rpc.authAdapterRpc({
                    ...request,
                    recoveryGeneration: generation,
                } as CatalogAuthAdapterRpcRequest);
            },
        };
    };

    return createAdapterFactory({
        config: {
            adapterId: "chardb",
            adapterName: "chardb",
            supportsBooleans: true,
            supportsDates: true,
            supportsJSON: false,
            supportsNumericIds: false,
            transaction: false,
        },
        adapter: ({ getFieldName, schema }) => {
            const canonicalModels = new Map<string, string>();
            const canonicalFields = new Map<string, ReadonlyMap<string, string>>();
            for (const [canonicalModel, modelSchema] of Object.entries(schema)) {
                const physicalModel = modelSchema.modelName;
                const existingModel = canonicalModels.get(physicalModel);
                if (existingModel !== undefined) {
                    throw incompatibleAuthMapping(
                        `models "${existingModel}" and "${canonicalModel}" both map to "${physicalModel}"`
                    );
                }
                canonicalModels.set(physicalModel, canonicalModel);

                const fields = new Map<string, string>([["id", "id"]]);
                for (const [canonicalField, fieldSchema] of Object.entries(modelSchema.fields)) {
                    const physicalField = fieldSchema.fieldName ?? canonicalField;
                    const existingField = fields.get(physicalField);
                    if (existingField !== undefined) {
                        throw incompatibleAuthMapping(
                            `fields "${existingField}" and "${canonicalField}" on model "${canonicalModel}" both map to "${physicalField}"`
                        );
                    }
                    fields.set(physicalField, canonicalField);
                }
                canonicalFields.set(canonicalModel, fields);
            }

            const canonicalModelFor = (physicalModel: string): string => {
                const canonicalModel = canonicalModels.get(physicalModel);
                if (canonicalModel === undefined) {
                    throw new CdbError({
                        code: "CDB_INVALID_ARGS",
                        message: `chardb auth adapter: unknown physical model "${physicalModel}"`,
                    });
                }
                return canonicalModel;
            };
            const canonicalFieldFor = (canonicalModel: string, physicalField: string): string => {
                const canonicalField = canonicalFields.get(canonicalModel)?.get(physicalField);
                if (canonicalField === undefined) {
                    throw new CdbError({
                        code: "CDB_INVALID_ARGS",
                        message: `chardb auth adapter: unknown physical field "${physicalField}" on model "${canonicalModel}"`,
                    });
                }
                return canonicalField;
            };

            return {
                async create({ model, data }) {
                    const payload = data as { [k: string]: RawJson };
                    const r = await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: { model, op: "create", payload },
                    });
                    return (r.row ?? payload) as never;
                },

                async findOne({ model, where }) {
                    const filters = whereToReadFilters(where);
                    const rows = await callCatalog<readonly Record<string, RawJson>[]>(catalog(), {
                        operation: "query",
                        args: { model, where: filters, limit: 1 },
                    });
                    return (rows[0] ?? null) as never;
                },

                async findMany({ model, where, limit, offset, sortBy }) {
                    const filters = where ? whereToReadFilters(where) : [];
                    const rows = await callCatalog<readonly Record<string, RawJson>[]>(catalog(), {
                        operation: "query",
                        args: {
                            model,
                            where: filters,
                            limit: limit ?? 100,
                            ...(offset === undefined ? {} : { offset }),
                            ...(sortBy === undefined ? {} : { sortBy }),
                        },
                    });
                    return rows as never;
                },

                async count({ model, where }) {
                    const filters = where ? whereToReadFilters(where) : [];
                    return callCatalog<number>(catalog(), {
                        operation: "count",
                        args: { model, where: filters },
                    });
                },

                async update({ model, where, update }) {
                    const flat = whereToFlat(where);
                    const r = await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: {
                            model,
                            op: "update",
                            where: flat,
                            payload: update as { [k: string]: RawJson },
                            returnRow: true,
                            limitOne: true,
                        },
                    });
                    return (r.row ?? null) as never;
                },

                async updateMany({ model, where, update }) {
                    const flat = whereToFlat(where);
                    const r = await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: {
                            model,
                            op: "update",
                            where: flat,
                            payload: update as { [k: string]: RawJson },
                            returnRow: false,
                            limitOne: false,
                        },
                    });
                    return r.affected ?? 0;
                },

                async delete({ model, where }) {
                    const flat = whereToFlat(where);
                    await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: { model, op: "delete", where: flat, limitOne: true },
                    });
                },

                async deleteMany({ model, where }) {
                    const flat = whereToFlat(where);
                    const r = await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: { model, op: "delete", where: flat, limitOne: false },
                    });
                    return r.affected ?? 0;
                },

                async consumeOne({ model, where }) {
                    const flat = whereToFlat(where);
                    const r = await callCatalog<CatalogAuthMutationResult>(catalog(), {
                        operation: "mutate",
                        args: { model, op: "delete", where: flat, returnRow: true, limitOne: true },
                    });
                    return (r.row ?? null) as never;
                },

                async incrementOne({ model, where, increment, set }) {
                    const defaultModel = canonicalModelFor(model);
                    const defaultField = (field: string): string => canonicalFieldFor(defaultModel, field);
                    const guardedWhere = whereToIncrementGuards(where, defaultField);
                    const incrementEntries: [string, number][] = [];
                    for (const [field, delta] of Object.entries(increment)) {
                        if (typeof delta !== "number" || !Number.isFinite(delta) || Object.is(delta, -0)) {
                            throw new CdbError({
                                code: "CDB_INVALID_ARGS",
                                message: `chardb auth adapter: increment delta for "${field}" must be finite and not negative zero`,
                            });
                        }
                        incrementEntries.push([defaultField(field), delta]);
                    }
                    const ownedIncrement = Object.fromEntries(incrementEntries);
                    let ownedSet: Record<string, RawJson> | undefined;
                    if (set !== undefined) {
                        const setEntries: [string, RawJson][] = [];
                        for (const [field, value] of Object.entries(set ?? {})) {
                            setEntries.push([defaultField(field), value as RawJson]);
                        }
                        ownedSet = Object.fromEntries(setEntries);
                    }
                    const r = await callCatalog<CatalogAuthIncrementResult>(catalog(), {
                        operation: "increment",
                        args: {
                            model: defaultModel,
                            where: guardedWhere,
                            increment: ownedIncrement,
                            ...(ownedSet === undefined ? {} : { set: ownedSet }),
                        },
                    });
                    if (!r.row) return null;
                    const storageRow: Record<string, RawJson> = Object.create(null);
                    for (const [field, value] of Object.entries(r.row)) {
                        storageRow[getFieldName({ model: defaultModel, field })] = value;
                    }
                    return storageRow as never;
                },
            };
        },
    }) as AdapterFactory<BetterAuthOptions>;
}

function incompatibleAuthMapping(message: string): CdbError {
    return new CdbError({
        code: "CDB_AUTH_PROFILE_INCOMPATIBLE",
        message: `chardb auth adapter: ${message}`,
    });
}

/**
 * Translate a better-auth `CleanedWhere[]` into a flat
 * `{[col]: value}` map. Only `eq` joined by AND is supported today —
 * all four core models and every shipping plugin model use that
 * shape for their internal model-store operations.
 */
function whereToFlat(where: CleanedWhere[]): { [k: string]: RawJson } {
    const out: { [k: string]: RawJson } = {};
    for (const w of where) {
        if (w.operator !== "eq") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `chardb auth adapter: where operator "${w.operator}" not supported (only "eq")`,
            });
        }
        if (w.connector === "OR") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "chardb auth adapter: OR connectors are not supported in where clauses",
            });
        }
        out[w.field] = normalize(w.value);
    }
    return out;
}

const AUTH_READ_OPERATORS = new Set<string>([
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "in",
    "not_in",
    "contains",
    "starts_with",
    "ends_with",
]);

function isAuthReadOperator(value: unknown): value is AuthReadWhere["operator"] {
    return typeof value === "string" && AUTH_READ_OPERATORS.has(value);
}

function whereToReadFilters(where: CleanedWhere[]): AuthReadWhere[] {
    const out: AuthReadWhere[] = [];
    for (const condition of where) {
        if (condition.connector === "OR") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "chardb auth adapter: OR connectors are not supported in where clauses",
            });
        }
        const operator: unknown = condition.operator ?? "eq";
        if (!isAuthReadOperator(operator)) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `chardb auth adapter: where operator "${String(operator)}" is not supported`,
            });
        }
        const requestedMode: unknown = condition.mode ?? "sensitive";
        if (requestedMode !== "sensitive" && requestedMode !== "insensitive") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `chardb auth adapter: where mode "${String(requestedMode)}" is invalid`,
            });
        }
        const mode = requestedMode;
        if (operator === "in" || operator === "not_in") {
            if (!Array.isArray(condition.value)) {
                throw new CdbError({
                    code: "CDB_INVALID_ARGS",
                    message: `chardb auth adapter: ${operator} filter value must be an array`,
                });
            }
            if (condition.value.length > AUTH_READ_IN_MAX_VALUES) {
                throw new CdbError({
                    code: "CDB_INVALID_ARGS",
                    message: `chardb auth adapter: ${operator} filter exceeds ${AUTH_READ_IN_MAX_VALUES} values`,
                });
            }
            if (mode === "insensitive") {
                throw new CdbError({
                    code: "CDB_UNSUPPORTED_FEATURE",
                    message: `chardb auth adapter: case-insensitive ${operator} filters are not supported`,
                });
            }
            out.push({
                field: condition.field,
                operator,
                value: condition.value.map(value => normalize(value)),
            });
            continue;
        }
        const value = normalize(condition.value);
        if (value !== null && typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `chardb auth adapter: ${operator} filter value must be scalar`,
            });
        }
        if (mode === "insensitive" && typeof value !== "string") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `chardb auth adapter: case-insensitive ${operator} requires a string value`,
            });
        }
        if (
            (operator === "contains" || operator === "starts_with" || operator === "ends_with") &&
            typeof value !== "string"
        ) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `chardb auth adapter: ${operator} filter value must be a string`,
            });
        }
        if (value === null && operator !== "eq" && operator !== "ne") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `chardb auth adapter: ${operator} does not accept null`,
            });
        }
        out.push({
            field: condition.field,
            operator,
            value,
            ...(mode === "insensitive" ? { mode } : {}),
        });
    }
    return out;
}

function whereToIncrementGuards(where: CleanedWhere[], defaultField: (field: string) => string): AuthIncrementWhere[] {
    const out: AuthIncrementWhere[] = [];
    for (const condition of where) {
        if (condition.connector === "OR") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "chardb auth adapter: OR connectors are not supported in incrementOne guards",
            });
        }
        if (condition.mode !== "sensitive") {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: "chardb auth adapter: case-insensitive incrementOne guards are not supported",
            });
        }
        if (
            condition.operator !== "eq" &&
            condition.operator !== "lt" &&
            condition.operator !== "lte" &&
            condition.operator !== "gt" &&
            condition.operator !== "gte"
        ) {
            throw new CdbError({
                code: "CDB_UNSUPPORTED_FEATURE",
                message: `chardb auth adapter: incrementOne where operator "${condition.operator}" is not supported`,
            });
        }
        const value = normalize(condition.value);
        if (value !== null && typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "chardb auth adapter: incrementOne comparison values must be scalar",
            });
        }
        if (value === null && condition.operator !== "eq") {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "chardb auth adapter: incrementOne only supports null with the eq operator",
            });
        }
        if (typeof value === "number" && (!Number.isFinite(value) || Object.is(value, -0))) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "chardb auth adapter: incrementOne comparison numbers must be finite and not negative zero",
            });
        }
        out.push({ field: defaultField(condition.field), operator: condition.operator, value });
    }
    return out;
}

function normalize(v: CleanedWhere["value"]): RawJson {
    if (v instanceof Date) return v.getTime();
    if (Array.isArray(v)) {
        return v as unknown as RawJson;
    }
    return v as RawJson;
}
