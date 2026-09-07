import { type ChardbBindingSelect, type ChardbSelectPlanV1, createBindingSelect } from "./binding-plan.ts";
import { snapshotMutationArguments, snapshotSubscriptionArguments } from "./client/serialized-json.ts";
import { CdbError, isCdbErrorCode } from "./errors.ts";
import { type MutationHandle, type QueryHandle, handleRef } from "./handles.ts";
import type { RawJson } from "./types.ts";
import { rawJsonResult } from "./util/raw_json.ts";

export interface ChardbBindingAuth {
    /** Better Auth JWT. The binding verifies its signature and timing before dispatch. */
    readonly jwt: string;
    /** Public origin that issued the JWT, such as `https://app.example.com`. */
    readonly authOrigin: string;
}

export interface ChardbBindingQueryRequest extends ChardbBindingAuth {
    readonly ref: string;
    readonly args: RawJson;
}

export interface ChardbBindingMutationRequest extends ChardbBindingQueryRequest {
    readonly mutId: string;
}

/** Bounded structured select request. It never carries SQL text or caller-owned authority. */
export interface ChardbBindingPlanRequest extends ChardbBindingAuth {
    readonly plan: ChardbSelectPlanV1;
}

export type ChardbBindingFailure = {
    readonly ok: false;
    readonly error: {
        readonly code: string;
        readonly retryable: boolean;
        readonly message: string;
        readonly correlationId?: string | undefined;
        readonly docs: string;
        readonly retryAfterMs?: number | undefined;
        readonly hint?: string | undefined;
    };
};

export type ChardbBindingQueryResponse = { readonly ok: true; readonly result: RawJson } | ChardbBindingFailure;

export type ChardbBindingMutationResponse =
    | {
          readonly ok: true;
          readonly cookie: string;
          readonly ran: boolean;
          readonly result: RawJson;
          readonly rowsAffected: number;
      }
    | ChardbBindingFailure;

/** Structural type of the native `env.DB` WorkerEntrypoint binding. */
export interface ChardbBinding {
    executeQuery(request: ChardbBindingQueryRequest): Promise<ChardbBindingQueryResponse>;
    executeMutation(request: ChardbBindingMutationRequest): Promise<ChardbBindingMutationResponse>;
    /** Optional for compatibility with bindings created before structured selects were added. */
    executePlan?(request: ChardbBindingPlanRequest): Promise<ChardbBindingQueryResponse>;
}

export interface ChardbBindingMutationOptions {
    /** Stable retry identity. Defaults to a fresh UUID for this call. */
    readonly mutId?: string;
}

export interface ChardbBindingClient {
    /** Start one bounded, full-row, single-table Drizzle select. */
    readonly select: ChardbBindingSelect;
    query<TArgs extends RawJson, TResult extends RawJson>(
        handle: QueryHandle<TArgs, TResult>,
        args: TArgs
    ): Promise<TResult>;
    mutate<TArgs extends RawJson, TResult extends RawJson>(
        handle: MutationHandle<TArgs, TResult>,
        args: TArgs,
        options?: ChardbBindingMutationOptions
    ): Promise<TResult>;
}

export const CHARDB_BINDING_MAX_IN_FLIGHT = 32;

function bindingInvariant(message: string): CdbError {
    return new CdbError({ code: "CDB_INVARIANT", message });
}

function bindingError(response: unknown): CdbError {
    if (typeof response !== "object" || response === null || Array.isArray(response)) {
        return bindingInvariant("DB binding returned a malformed failure");
    }
    const error = (response as { readonly error?: unknown }).error;
    if (typeof error !== "object" || error === null || Array.isArray(error)) {
        return bindingInvariant("DB binding returned a malformed failure");
    }
    const wire = error as Record<string, unknown>;
    if (
        !isCdbErrorCode(wire.code) ||
        typeof wire.retryable !== "boolean" ||
        typeof wire.message !== "string" ||
        typeof wire.docs !== "string" ||
        (wire.correlationId !== undefined && typeof wire.correlationId !== "string") ||
        (wire.retryAfterMs !== undefined &&
            (typeof wire.retryAfterMs !== "number" || !Number.isFinite(wire.retryAfterMs))) ||
        (wire.hint !== undefined && typeof wire.hint !== "string")
    ) {
        return bindingInvariant("DB binding returned a malformed failure");
    }
    return new CdbError({
        code: wire.code,
        message: wire.message as string,
        ...(wire.correlationId === undefined ? {} : { correlationId: wire.correlationId as string }),
        ...(wire.retryAfterMs === undefined ? {} : { retryAfterMs: wire.retryAfterMs as number }),
        ...(wire.hint === undefined ? {} : { hint: wire.hint as string }),
    });
}

function queryResult(response: unknown): RawJson {
    if (typeof response !== "object" || response === null || Array.isArray(response)) {
        throw bindingInvariant("DB binding returned a malformed query response");
    }
    const envelope = response as Record<string, unknown>;
    if (envelope.ok === false) throw bindingError(response);
    if (envelope.ok !== true || !("result" in envelope)) {
        throw bindingInvariant("DB binding returned a malformed query response");
    }
    return rawJsonResult(envelope.result, "DB binding query result");
}

function mutationResult(response: unknown): RawJson {
    if (typeof response !== "object" || response === null || Array.isArray(response)) {
        throw bindingInvariant("DB binding returned a malformed mutation response");
    }
    const envelope = response as Record<string, unknown>;
    if (envelope.ok === false) throw bindingError(response);
    if (
        envelope.ok !== true ||
        typeof envelope.cookie !== "string" ||
        envelope.cookie.length === 0 ||
        typeof envelope.ran !== "boolean" ||
        typeof envelope.rowsAffected !== "number" ||
        !Number.isSafeInteger(envelope.rowsAffected) ||
        envelope.rowsAffected < 0 ||
        !("result" in envelope)
    ) {
        throw bindingInvariant("DB binding returned a malformed mutation response");
    }
    return rawJsonResult(envelope.result, "DB binding mutation result");
}

/**
 * Create the Worker-side typed client for the native `env.DB` binding.
 * Handles remain local; only their stable refs and JSON arguments cross RPC.
 */
export function client(binding: ChardbBinding, auth: ChardbBindingAuth): ChardbBindingClient {
    if (!binding || typeof binding.executeQuery !== "function" || typeof binding.executeMutation !== "function") {
        throw new TypeError("chardb: env.DB is not a chardb binding");
    }
    let inFlight = 0;
    const admitted = async <T>(operation: () => Promise<T>): Promise<T> => {
        if (inFlight >= CHARDB_BINDING_MAX_IN_FLIGHT) {
            throw new CdbError({
                code: "CDB_RATE_LIMITED",
                message: `DB client permits at most ${CHARDB_BINDING_MAX_IN_FLIGHT} in-flight operations`,
            });
        }
        inFlight++;
        try {
            return await operation();
        } finally {
            inFlight--;
        }
    };
    const select = createBindingSelect(plan =>
        admitted(async () => {
            if (typeof binding.executePlan !== "function") {
                throw new CdbError({
                    code: "CDB_UNSUPPORTED_FEATURE",
                    message: "DB binding does not support structured select plans",
                });
            }
            const response = await binding.executePlan({ ...auth, plan });
            return queryResult(response);
        })
    );
    return {
        select,
        async query(handle, args) {
            const ref = handleRef(handle, "query");
            const ownedArgs = snapshotSubscriptionArguments(args);
            return admitted(async () => {
                const response = await binding.executeQuery({ ...auth, ref, args: ownedArgs });
                return queryResult(response) as Awaited<ReturnType<typeof handle>>;
            });
        },
        async mutate(handle, args, options = {}) {
            const ref = handleRef(handle, "mutation");
            const ownedArgs = snapshotMutationArguments(args);
            return admitted(async () => {
                const response = await binding.executeMutation({
                    ...auth,
                    ref,
                    args: ownedArgs,
                    mutId: options.mutId ?? crypto.randomUUID(),
                });
                return mutationResult(response) as ReturnType<typeof handle>;
            });
        },
    };
}
