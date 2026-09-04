/**
 * Runtime registry of `@chardb/core/server` exports.
 *
 * Every helper attaches `__chardbRef` and `__chardbKind` markers (see
 * `src/server/refs.ts`). The application passes its API module namespaces to
 * `chardb({ api })`, which builds this manifest in each Worker and Durable
 * Object isolate. The configured Cdb class retains this registry and resolves
 * mutation refs locally. The
 * configured Gateway retains the same manifest for mutation and query
 * routing.
 *
 * The manifest contains functions and Maps and must never cross RPC. Only
 * serializable mutation and subscription requests cross into Cdb.
 *
 * ### Wire boundary: TArgs is `RawJson`
 *
 * `defineMutation<TArgs>` and `defineQuery<TArgs>` accept a typed argument
 * shape so the user-side handler stays statically checked. The descriptor
 * stored in the manifest, however, deliberately erases that generic to
 * `RawJson`. This is the wire contract: an `Up.mut` envelope carries
 * `args: RawJson` after `decodeWire`; `Up.sub` carries the same pair for
 * queries. The Gateway's local route resolvers pass those values into manifest
 * descriptors with the same erased argument shape. Mutation results
 * remain `unknown` until the op-log wrapper verifies they are JSON.
 *
 * A phantom-map alternative (parallel `WeakMap<ChardbRef, TArgs>`) was
 * rejected because TypeScript lacks generic existentials: the map's value
 * type would still need to be `unknown` at the consumption site, and
 * threading `TArgs` through would force every dispatch through a generic
 * lookup that can't be resolved at the wire boundary anyway. Validation
 * (Zod / TypeBox / Valibot / ArkType) lives one level up in the user's
 * handler. The user can refine `args` with a standard-schema validator.
 * before calling into business logic. See `src/server/define.ts` for the
 * forward direction (typed → erased) and the routing functions in this file
 * for the consumers that need the erased shape.
 */

import { type ChardbSelectPlanV1, validateChardbSelectPlanV1 } from "../binding-plan.ts";
import { CdbError } from "../errors.ts";
import type { ChardbRef, RawJson } from "../types.ts";
import { stableHashHex, stableJson } from "../util/canonical.ts";
import { CDB_VECTOR_SEARCH_MAX_RESULTS } from "../vector.ts";
import type { CdbIntent } from "../wire.ts";
import type { MutationAuthority } from "./define.ts";
import {
    type RegisteredQueryPlan,
    type RegisteredVectorQueryPlan,
    registeredSelectQueryPlanHash,
} from "./registered-query-plan.ts";
import { isChardbVectorResourceDescriptor, normalizeChardbResourceDescriptor } from "./resource-descriptors.ts";
import {
    CDB_JSON_MAX_AGGREGATE_MEMBERS,
    CDB_QUERY_ARGS_MAX_BYTES,
    CDB_QUERY_ARGS_MAX_DEPTH,
    snapshotCdbJsonByteLimit,
    snapshotCdbMutationArgs,
    snapshotCdbQueryArgs,
} from "./result_limits.ts";

export interface MutationDescriptor {
    readonly ref: ChardbRef;
    readonly invoke: (ctx: unknown, args: RawJson) => unknown;
    readonly invokeValidated: (ctx: unknown, args: RawJson) => unknown;
    readonly validateArgs?: (args: unknown) => RawJson;
    readonly extractPartitionKey?: (args: RawJson) => string | number | bigint | undefined;
    readonly singlePartition: boolean;
    readonly authority?: MutationAuthority;
}

export interface QueryDescriptor {
    readonly ref: ChardbRef;
    readonly validateArgs?: (args: unknown) => Promise<RawJson>;
    readonly compilePlan: (args: RawJson) => RegisteredQueryPlan;
}

export type QueryRouteResponse =
    | {
          readonly ok: true;
          readonly args: RawJson;
          readonly intent: CdbIntent;
          readonly policyDigest: string;
          readonly queryHash: string;
          readonly authority: MutationAuthority | null;
          readonly partitionKey: string | null;
          /** Present only for registered planned selects compiled from this manifest's packaged callback. */
          readonly selectPlan?: ChardbSelectPlanV1 | undefined;
          /** Present only for registered organization vector searches. */
          readonly vectorPlan?: RegisteredVectorQueryPlan | undefined;
      }
    | { readonly ok: false; readonly error: ReturnType<CdbError["toJSON"]> };

export interface ChardbManifest {
    readonly mutations: ReadonlyMap<ChardbRef, MutationDescriptor>;
    readonly queries: ReadonlyMap<ChardbRef, QueryDescriptor>;
}

const EMPTY: ChardbManifest = {
    mutations: new Map(),
    queries: new Map(),
};

export function emptyManifest(): ChardbManifest {
    return EMPTY;
}

export { manifestFromExports } from "../vite/manifest.ts";

/**
 * Resolve a mutation by ref, raising `CDB_REF_NOT_FOUND` if the manifest
 * doesn't know about it. Used for entrypoint routing and shard-local execution.
 */
export function resolveMutation(manifest: ChardbManifest, ref: ChardbRef): MutationDescriptor {
    const desc = manifest.mutations.get(ref);
    if (!desc) {
        throw new CdbError({ code: "CDB_REF_NOT_FOUND", message: `unknown mutation ref: ${ref}` });
    }
    return desc;
}

export function resolveQuery(manifest: ChardbManifest, ref: ChardbRef): QueryDescriptor {
    const descriptor = manifest.queries.get(ref);
    if (!descriptor) {
        throw new CdbError({ code: "CDB_REF_NOT_FOUND", message: `unknown query ref: ${ref}` });
    }
    return descriptor;
}

function requireAuthorityPartition(
    authority: MutationAuthority | undefined,
    kind: "mutation" | "query",
    ref: string,
    key: string | number | bigint | undefined
): asserts key is string {
    if (authority === undefined) return;
    if (typeof key === "string" && key.length > 0) return;
    throw new CdbError({
        code: "CDB_INVALID_ARGS",
        message: `${authority} ${kind} ${ref} requires a nonempty string partition key`,
    });
}

function registeredSelectPlan(compiled: RegisteredQueryPlan | undefined, ref: string): ChardbSelectPlanV1 | undefined {
    if (!compiled || compiled.kind !== "select") return undefined;
    const plan = validateChardbSelectPlanV1(compiled.plan);
    const orderBy = plan.orderBy ?? [];
    const metadataMatches =
        plan.cardinality === "many" &&
        plan.limit === compiled.limit &&
        compiled.intent.tables.length === 1 &&
        compiled.intent.tables[0] === plan.table &&
        orderBy.length === compiled.orderBy.length &&
        orderBy.every(
            (item, index) =>
                item.column === compiled.orderBy[index]?.column && item.direction === compiled.orderBy[index]?.direction
        ) &&
        compiled.planHash ===
            registeredSelectQueryPlanHash({
                version: compiled.version,
                kind: compiled.kind,
                plan,
                authority: compiled.authority,
                partitionKey: compiled.partitionKey,
                intent: compiled.intent,
                projection: compiled.projection,
                orderBy: compiled.orderBy,
                limit: compiled.limit,
            });
    if (!metadataMatches) {
        throw new CdbError({
            code: "CDB_INVARIANT",
            message: `query ${ref} canonical select plan disagrees with its compiler metadata`,
        });
    }
    return plan;
}

function registeredVectorPlan(
    compiled: RegisteredQueryPlan | undefined,
    ref: string
): RegisteredVectorQueryPlan | undefined {
    if (!compiled || compiled.kind !== "searchVector") return undefined;
    const resource = normalizeChardbResourceDescriptor(compiled.resource);
    const expectedIntent: CdbIntent = {
        kind: "select",
        tables: [compiled.resource.table],
        partitionKey: {
            table: compiled.resource.table,
            column: compiled.resource.organizationColumn,
            values: [compiled.partitionKey],
        },
    };
    const hashInput = {
        version: 1,
        kind: "searchVector",
        authority: "organization",
        partitionKey: compiled.partitionKey,
        intent: compiled.intent,
        resource: compiled.resource,
        values: compiled.values,
        limit: compiled.limit,
    } as const;
    const metadataMatches =
        isChardbVectorResourceDescriptor(resource) &&
        stableJson(resource) === stableJson(compiled.resource) &&
        compiled.authority === "organization" &&
        typeof compiled.partitionKey === "string" &&
        compiled.partitionKey.length > 0 &&
        new TextEncoder().encode(compiled.partitionKey).byteLength <= 256 &&
        stableJson(compiled.intent as unknown as RawJson) === stableJson(expectedIntent as unknown as RawJson) &&
        Array.isArray(compiled.values) &&
        compiled.values.length === compiled.resource.dimensions &&
        compiled.values.every(
            value =>
                typeof value === "number" &&
                Number.isFinite(value) &&
                Math.fround(value) === value &&
                !Object.is(value, -0)
        ) &&
        Number.isSafeInteger(compiled.limit) &&
        compiled.limit >= 1 &&
        compiled.limit <= CDB_VECTOR_SEARCH_MAX_RESULTS &&
        compiled.planHash === stableHashHex(hashInput);
    if (!metadataMatches) {
        throw new CdbError({
            code: "CDB_INVARIANT",
            message: `query ${ref} vector plan disagrees with its organization resource metadata`,
        });
    }
    return compiled;
}

/** Re-derive query placement from arguments that Gateway already validated. */
export function routeValidatedQuery(
    manifest: ChardbManifest,
    input: { readonly ref: string; readonly args: RawJson },
    policyDigestForTables: (tableNames: readonly string[]) => string
): Extract<QueryRouteResponse, { readonly ok: true }> {
    const callbackArgs = snapshotCdbQueryArgs(input.args);
    const descriptor = resolveQuery(manifest, input.ref as ChardbRef);
    const plan = descriptor.compilePlan(callbackArgs);
    const selectPlan = registeredSelectPlan(plan, input.ref);
    const vectorPlan = registeredVectorPlan(plan, input.ref);
    const intentCandidate = plan.intent;
    const authority = plan.authority;
    const key = plan.partitionKey;
    const args = snapshotCdbQueryArgs(callbackArgs);
    const intent = snapshotCdbJsonByteLimit(
        intentCandidate as unknown as RawJson,
        CDB_QUERY_ARGS_MAX_BYTES,
        {
            code: "CDB_INVARIANT",
            subject: "query intent",
            hint: "reduce query intent metadata",
        },
        { maxAggregateMembers: CDB_JSON_MAX_AGGREGATE_MEMBERS, maxDepth: CDB_QUERY_ARGS_MAX_DEPTH }
    ) as unknown as CdbIntent;
    const policyDigest = policyDigestForTables(intent.tables);
    requireAuthorityPartition(authority, "query", input.ref, key);
    return {
        ok: true,
        args,
        intent,
        policyDigest,
        queryHash: stableJson({
            ref: input.ref,
            args,
            intent,
            policyDigest,
            planHash: plan.planHash,
        }),
        authority: authority ?? null,
        partitionKey: key === undefined ? null : String(key),
        ...(selectPlan ? { selectPlan } : {}),
        ...(vectorPlan ? { vectorPlan } : {}),
    };
}

/** Resolve server-owned query routing metadata without executing the query. */
export async function routeQuery(
    manifest: ChardbManifest,
    input: { readonly ref: string; readonly args: RawJson },
    policyDigestForTables: (tableNames: readonly string[]) => string
): Promise<QueryRouteResponse> {
    try {
        const rawArgs = snapshotCdbQueryArgs(input.args);
        const descriptor = resolveQuery(manifest, input.ref as ChardbRef);
        const validatedArgs = snapshotCdbQueryArgs(
            (descriptor.validateArgs ? await descriptor.validateArgs(rawArgs) : rawArgs) as RawJson
        );
        return routeValidatedQuery(manifest, { ref: input.ref, args: validatedArgs }, policyDigestForTables);
    } catch (error) {
        const cdb =
            error instanceof CdbError
                ? error
                : new CdbError({ code: "CDB_INVARIANT", message: "query intent extraction failed", cause: error });
        return { ok: false, error: cdb.toJSON() };
    }
}

/**
 * Pure routing decision: extract the partition key for a mutation and compute
 * the target vshard. Kept pure so configured Gateway isolates and tests share
 * the same decision without booting workerd. Returns the JSON-serialisable
 * shape consumed by the mutation dispatcher.
 */
export function routeMutation(
    manifest: ChardbManifest,
    input: { readonly ref: string; readonly args: RawJson },
    vshardOf: (parts: readonly (string | number | bigint | Uint8Array)[]) => number
):
    | {
          readonly ok: true;
          readonly vshard: number;
          readonly authority: MutationAuthority | null;
          readonly partitionKey: string | null;
          readonly args: RawJson;
      }
    | { readonly ok: false; readonly error: ReturnType<CdbError["toJSON"]> } {
    try {
        const rawArgs = snapshotCdbMutationArgs(input.args);
        const desc = resolveMutation(manifest, input.ref as ChardbRef);
        const validatedArgs = snapshotCdbMutationArgs(
            (desc.validateArgs ? desc.validateArgs(rawArgs) : rawArgs) as RawJson
        );
        let key: string | number | bigint | undefined;
        if (desc.extractPartitionKey) key = desc.extractPartitionKey(snapshotCdbMutationArgs(validatedArgs));
        const args = snapshotCdbMutationArgs(validatedArgs);
        requireAuthorityPartition(desc.authority, "mutation", input.ref, key);
        if (key === undefined && desc.singlePartition) {
            throw new CdbError({
                code: "CDB_CROSS_PARTITION",
                message: `mutation ${input.ref} declared singlePartition without resolvable partitionKey`,
            });
        }
        const scalar = key === undefined ? stableJson(args) : String(key);
        return {
            ok: true,
            vshard: Number(vshardOf([scalar])),
            authority: desc.authority ?? null,
            partitionKey: key === undefined ? null : String(key),
            args,
        };
    } catch (err) {
        if (err instanceof CdbError) return { ok: false, error: err.toJSON() };
        const cdb = new CdbError({
            code: "CDB_INVARIANT",
            message: err instanceof Error ? err.message : "internal",
        });
        return { ok: false, error: cdb.toJSON() };
    }
}
