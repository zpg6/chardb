import type { MutationHandle, QueryHandle } from "../../src/handles.ts";
import type { RawJson } from "../../src/types.ts";

/** A wire-only handle: stamped like a `define*` export, never callable. */
function stamp(kind: "query" | "mutation", ref: string): unknown {
    return Object.defineProperties(
        () => {
            throw new Error(`chardb: ${ref} is a client handle, not a server function`);
        },
        { __chardbKind: { value: kind }, __chardbRef: { value: ref } }
    );
}

export const queryHandle = <TArgs extends RawJson = RawJson, TResult = RawJson[]>(ref: string) =>
    stamp("query", ref) as QueryHandle<TArgs, TResult>;

export const mutationHandle = <TArgs extends RawJson = RawJson, TResult = RawJson>(ref: string) =>
    stamp("mutation", ref) as MutationHandle<TArgs, TResult>;
