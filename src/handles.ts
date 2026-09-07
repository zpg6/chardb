/**
 * Registered API handles as every client sees them: a callable whose
 * signature carries the argument and result types, stamped with its kind
 * and stable wire ref. Server definitions, Vite browser stubs, and
 * `chardb generate` modules all satisfy these shapes, so clients never
 * take a wire ref as a string.
 */

import { ChardbRef, type RawJson } from "./types.ts";

export interface QueryHandle<TArgs, TResult> {
    readonly __chardbKind: "query";
    readonly __chardbRef: { toString(): string };
    (ctx: never, args: TArgs): Promise<TResult>;
}

export interface MutationHandle<TArgs, TResult> {
    readonly __chardbKind: "mutation";
    readonly __chardbRef: { toString(): string };
    (ctx: never, args: TArgs): TResult;
}

/** What a mutation resolves with on the client: the handler's result when it is JSON, otherwise untyped JSON. */
export type WireResult<TResult> = Awaited<TResult> extends RawJson ? Awaited<TResult> : RawJson;

/** Row type of a query result: collections yield their element, scalars pass through. */
export type QueryRow<TResult> = TResult extends readonly (infer Row)[] ? Row : TResult;

/** Read the wire ref of a handle, rejecting anything that is not a registered handle of that kind. */
export function handleRef(handle: unknown, kind: "query" | "mutation"): ChardbRef {
    if (
        typeof handle !== "function" ||
        (handle as { readonly __chardbKind?: unknown }).__chardbKind !== kind ||
        typeof (handle as { readonly __chardbRef?: { toString?: unknown } }).__chardbRef?.toString !== "function"
    ) {
        throw new TypeError(`chardb: ${kind} requires a define${kind === "query" ? "Query" : "Mutation"} handle`);
    }
    const ref = (handle as unknown as { readonly __chardbRef: { toString(): string } }).__chardbRef.toString();
    if (ref.length === 0 || ref.length > 1_024 || !ref.includes("#")) {
        throw new TypeError(`chardb: ${kind} handle has an invalid stable ref`);
    }
    return ChardbRef(ref);
}
