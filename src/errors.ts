/**
 * Locked error-code surface.
 *
 * The `code` identifier and its `retryable` polarity are part of the public
 * contract: programmatic monitoring MUST key on `code`, never on message text.
 * Adding new codes is additive across minor releases; removing or repurposing
 * an existing code requires a major version bump. Message text and `docs` URL
 * are explicitly open — they may be reworded for clarity in any release.
 */

export const CDB_ERROR_CODES = [
    "CDB_STALE_EPOCH",
    "CDB_CROSS_PARTITION",
    "CDB_CROSS_PARTITION_BATCH",
    "CDB_INTERACTIVE_TXN_UNSUPPORTED",
    "CDB_NONLOCAL_UNIQUE",
    "CDB_NONLOCAL_FK",
    "CDB_AMBIGUOUS_COLOCATION",
    "CDB_POLICY_UNKNOWN_ROOT",
    "CDB_PARTITION_CONTRACT_CHANGED",
    "CDB_SCATTER_NOT_INDEX",
    "CDB_MUT_ID_COLLISION",
    "CDB_UNIQUE_VIOLATION",
    "CDB_MUTATION_OUTCOME_UNKNOWN",
    "CDB_OPLOG_PRESSURE",
    "CDB_AUTH_PROFILE_INCOMPATIBLE",
    "CDB_RESERVED_TABLE_NAME",
    "CDB_NO_INTENT_FOR_RAW_SQL",
    "CDB_TXN_ABORTED_EVICTION",
    "CDB_SHARDS_CHANGED",
    "CDB_RATE_LIMITED",
    "CDB_CALLER_DENIED",
    "CDB_UNSUPPORTED_FEATURE",
    "CDB_SHARD_UNAVAILABLE",
    "CDB_CATALOG_UNAVAILABLE",
    "CDB_FORBIDDEN",
    "CDB_GSI_STRICT_REQUIRES_2PC",
    "CDB_VECTORIZE_INDEX_MISSING",
    "CDB_VECTORIZE_DIM_MISMATCH",
    "CDB_STREAM_ABORTED",
    "CDB_DISTINCT_CAP_EXCEEDED",
    "CDB_INVARIANT",
    "CDB_RESHARD_PHASE_MISMATCH",
    "CDB_REF_NOT_FOUND",
    "CDB_DT_NOT_IMPLEMENTED",
    "CDB_DT_ABORTED",
    "CDB_AUTH_NOT_BOUND",
    "CDB_AUTH_CROSS_PARTITION_TX",
    "CDB_AUTH_GSI_MISS",
    "CDB_NOT_CDB_TABLE",
    "CDB_INVALID_ARGS",
    "CDB_INVALID_TENANT",
    "CDB_INVALID_SELF",
    "CDB_INVALID_COLUMN",
    "CDB_INVALID_PARTITION",
    "CDB_AMBIGUOUS_TENANT",
    "CDB_MISSING_TENANT_FK",
    "CDB_POLICY_CONFLICT",
    "CDB_FORBIDDEN_COLUMN",
    "CDB_TENANT_OVERRIDE",
    "CDB_SELF_OVERRIDE",
] as const;

export type CdbErrorCode = (typeof CDB_ERROR_CODES)[number];

const ERROR_CODE_SET: ReadonlySet<string> = new Set(CDB_ERROR_CODES);

/**
 * Narrow an arbitrary string to a `CdbErrorCode`. Use at wire boundaries
 * (decoded RPC envelopes, parsed `Error.message` prefixes) where the input
 * type-system claim could be a stale or hostile string.
 */
export function isCdbErrorCode(s: unknown): s is CdbErrorCode {
    return typeof s === "string" && ERROR_CODE_SET.has(s);
}

const RETRYABLE_CODES: ReadonlySet<CdbErrorCode> = new Set<CdbErrorCode>([
    "CDB_STALE_EPOCH",
    "CDB_TXN_ABORTED_EVICTION",
    "CDB_RATE_LIMITED",
    "CDB_SHARD_UNAVAILABLE",
    "CDB_CATALOG_UNAVAILABLE",
    "CDB_STREAM_ABORTED",
]);

export function isRetryable(code: CdbErrorCode): boolean {
    return RETRYABLE_CODES.has(code);
}

const DOCS_BASE = "https://chardb.dev/errors";

export function docsUrlFor(code: CdbErrorCode): string {
    return `${DOCS_BASE}/${code.toLowerCase()}`;
}

export interface CdbErrorInit {
    code: CdbErrorCode;
    message?: string;
    /** Forwarded into the wire envelope so server logs and client traces line up. */
    correlationId?: string;
    /** Free-form structured cause information; never put PII here. */
    cause?: unknown;
    /** For `CDB_RATE_LIMITED`. */
    retryAfterMs?: number;
    /** Free-form actionable hint shown in CLI/dashboard. */
    hint?: string;
}

/**
 * The single error class chardb throws across DO/SDK/CLI surfaces.
 * Always carries the locked `code`/`retryable`/`docs` fields.
 */
export class CdbError extends Error {
    readonly code: CdbErrorCode;
    readonly retryable: boolean;
    readonly correlationId: string | undefined;
    readonly docs: string;
    readonly retryAfterMs: number | undefined;
    readonly hint: string | undefined;

    constructor(init: CdbErrorInit) {
        super(init.message ?? init.code, init.cause !== undefined ? { cause: init.cause } : undefined);
        this.name = "CdbError";
        this.code = init.code;
        this.retryable = isRetryable(init.code);
        this.correlationId = init.correlationId;
        this.docs = docsUrlFor(init.code);
        this.retryAfterMs = init.retryAfterMs;
        this.hint = init.hint;
    }

    toJSON(): {
        code: CdbErrorCode;
        retryable: boolean;
        message: string;
        correlationId: string | undefined;
        docs: string;
        retryAfterMs: number | undefined;
        hint: string | undefined;
    } {
        return {
            code: this.code,
            retryable: this.retryable,
            message: this.message,
            correlationId: this.correlationId,
            docs: this.docs,
            retryAfterMs: this.retryAfterMs,
            hint: this.hint,
        };
    }
}

/** Type-narrowing helper for catch sites. */
export function isCdbError(e: unknown): e is CdbError {
    return e instanceof CdbError;
}

/** Encode an expected CharDB failure into the message that Workers RPC preserves. */
export function throwCdbRpcError(error: unknown): never {
    if (!isCdbError(error)) throw error;
    throw new Error(`${error.code}: ${error.message}`);
}

/** Rehydrate a CharDB failure after Workers RPC strips custom Error properties. */
export function rehydrateCdbRpcError(error: unknown): unknown {
    if (isCdbError(error) || !(error instanceof Error)) return error;
    const match = /^(CDB_[A-Z_]+):\s*([\s\S]*)$/.exec(error.message);
    if (!match?.[1] || !isCdbErrorCode(match[1])) return error;
    return new CdbError({ code: match[1], message: match[2] || match[1] });
}
