/**
 * Public Worker-side API for organization and user tenancy.
 *
 * Runtime internals stay behind `chardb()`: Durable Object classes, RPC
 * contracts, policy compilers, resharding, and runtime configuration are not
 * package exports.
 */

import type { StandardSchemaV1 } from "@standard-schema/spec";
import {
    type ChardbDb,
    type MutationCtx,
    type MutationFn,
    type PlannedQueryBuilder,
    type PlannedQueryConfig,
    type QueryFn,
    api as internalApi,
} from "./define.ts";

type PartitionKeyOf<TArgs> = {
    readonly [K in keyof TArgs]: TArgs[K] extends string | number | bigint ? K : never;
}[keyof TArgs] &
    string;

export interface OrganizationMutationConfig<TDb, TArgs extends Record<string, unknown>, TResult> {
    readonly authority: "organization";
    readonly ref: string;
    readonly args?: StandardSchemaV1<unknown, TArgs>;
    readonly handler: (ctx: MutationCtx<TDb>, args: TArgs) => TResult;
    readonly partitionKey?: PartitionKeyOf<TArgs> | ((args: TArgs) => string | number | bigint | undefined);
    readonly singlePartition?: boolean;
    readonly idempotencyTtl?: "24h";
    readonly returnUserErrors?: boolean;
}

export interface UserMutationConfig<TDb, TArgs extends Record<string, unknown>, TResult> {
    readonly authority: "user";
    readonly ref: string;
    readonly args?: StandardSchemaV1<unknown, TArgs>;
    readonly handler: (ctx: MutationCtx<TDb>, args: TArgs) => TResult;
    readonly partitionKey: PartitionKeyOf<TArgs> | ((args: TArgs) => string | number | bigint | undefined);
    readonly singlePartition?: boolean;
    readonly idempotencyTtl?: "24h";
    readonly returnUserErrors?: boolean;
}

export interface PublicApi<TSchema extends Record<string, unknown>> {
    mutation<TArgs extends Record<string, unknown>, TResult>(
        config:
            | OrganizationMutationConfig<ChardbDb<TSchema>, TArgs, TResult>
            | UserMutationConfig<ChardbDb<TSchema>, TArgs, TResult>
    ): MutationFn<ChardbDb<TSchema>, TArgs, TResult>;
    query<TArgs extends Record<string, unknown>, TBuilder extends PlannedQueryBuilder>(
        config: PlannedQueryConfig<ChardbDb<TSchema>, TArgs, TBuilder>
    ): QueryFn<ChardbDb<TSchema>, TArgs, TBuilder["_"]["result"]>;
}

/** Organization or user mutations and single-partition planned live queries. */
export const api: PublicApi<Record<string, unknown>> = {
    mutation: config => internalApi.mutation(config),
    query: config => internalApi.query(config),
};
export { forOrg, forOrgUser, forUser } from "./schema-ownership.ts";
export { chardb } from "./chardb.ts";
export { getChardbAuthEnv } from "./auth-runtime-context.ts";
export { defineMigrations, defineSchemaBaseline } from "./schema-migrations.ts";
export {
    defineSchemaSnapshot,
    type ChardbSchemaSnapshot,
    type ChardbSchemaSnapshotInput,
} from "./schema-snapshot.ts";
export { defineAuth } from "../auth/synthesize.ts";
export {
    searchVector,
    vector,
    type VectorColumn,
    type VectorColumnHandle,
    type VectorConfig,
    type VectorMetric,
    type VectorMutationApi,
    type VectorRowPk,
    type VectorSearchInput,
    type VectorSearchResult,
} from "../vector.ts";
