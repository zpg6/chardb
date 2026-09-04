import { CdbError } from "../errors.ts";
import type { MutationAuthority } from "../server/define.ts";
import type { ChardbManifest, MutationDescriptor, QueryDescriptor } from "../server/manifest.ts";
import { type ChardbFunctionKind, bindExportRefs } from "../server/refs.ts";
import type { RegisteredQueryPlan } from "../server/registered-query-plan.ts";
import type { Brand, ChardbRef, RawJson } from "../types.ts";

interface RefMarked {
    readonly __chardbRef: Brand<string, "ChardbRef">;
    readonly __chardbKind: ChardbFunctionKind;
}

interface MutationMarked extends RefMarked {
    readonly __chardbKind: "mutation";
    readonly __chardbPartitionKey?: (args: RawJson) => string | number | bigint | undefined;
    readonly __chardbSinglePartition?: boolean;
    readonly __chardbAuthority?: MutationAuthority;
    readonly __chardbInvokeValidated?: (ctx: unknown, args: RawJson) => unknown;
    readonly __chardbValidateArgs?: (args: unknown) => RawJson;
}

interface QueryMarked extends RefMarked {
    readonly __chardbKind: "query";
    readonly __chardbValidateArgs?: (args: unknown) => Promise<RawJson>;
    readonly __chardbCompilePlan?: (args: RawJson) => RegisteredQueryPlan;
}

function isRefMarked(value: unknown): value is RefMarked {
    if (value === null) return false;
    if (typeof value !== "function" && typeof value !== "object") return false;
    const marker = value as { __chardbRef?: unknown; __chardbKind?: unknown };
    return typeof marker.__chardbRef === "string" && typeof marker.__chardbKind === "string";
}

/** Build the runtime manifest from application API module namespaces. */
export function manifestFromExports(exports: Record<string, unknown>): ChardbManifest {
    bindExportRefs(exports);
    const mutations = new Map<ChardbRef, MutationDescriptor>();
    const queries = new Map<ChardbRef, QueryDescriptor>();
    const seenRefs = new Map<ChardbRef, { readonly kind: string; readonly value: unknown }>();

    for (const value of Object.values(exports)) {
        if (!isRefMarked(value)) continue;
        const ref = value.__chardbRef as ChardbRef;
        const seen = seenRefs.get(ref);
        if (seen && seen.value !== value) {
            throw new CdbError({
                code: "CDB_INVARIANT",
                message: `duplicate ref across ${seen.kind} and ${value.__chardbKind}: ${ref}`,
            });
        }
        if (seen) continue;
        seenRefs.set(ref, { kind: value.__chardbKind, value });
        switch (value.__chardbKind) {
            case "mutation": {
                const mutation = value as MutationMarked & ((ctx: unknown, args: RawJson) => unknown);
                const duplicate = mutations.get(ref);
                if (duplicate && duplicate.invoke !== mutation) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: `duplicate mutation ref: ${ref}` });
                }
                mutations.set(ref, {
                    ref,
                    invoke: mutation,
                    invokeValidated: mutation.__chardbInvokeValidated ?? mutation,
                    ...(mutation.__chardbValidateArgs ? { validateArgs: mutation.__chardbValidateArgs } : {}),
                    ...(mutation.__chardbPartitionKey ? { extractPartitionKey: mutation.__chardbPartitionKey } : {}),
                    singlePartition: mutation.__chardbSinglePartition === true,
                    ...(mutation.__chardbAuthority ? { authority: mutation.__chardbAuthority } : {}),
                });
                break;
            }
            case "query": {
                const query = value as QueryMarked;
                if (!query.__chardbCompilePlan) {
                    throw new CdbError({
                        code: "CDB_INVARIANT",
                        message: `query ref has no compiled plan: ${ref}`,
                    });
                }
                const duplicate = queries.get(ref);
                if (duplicate) {
                    throw new CdbError({ code: "CDB_INVARIANT", message: `duplicate query ref: ${ref}` });
                }
                queries.set(ref, {
                    ref,
                    ...(query.__chardbValidateArgs ? { validateArgs: query.__chardbValidateArgs } : {}),
                    compilePlan: query.__chardbCompilePlan,
                });
                break;
            }
            default:
                break;
        }
    }
    return { mutations, queries };
}
