import type { CdbErrorCode } from "../errors.ts";
import { type CdbError, isCdbError } from "../errors.ts";

const BAD_REQUEST_CODES: ReadonlySet<CdbErrorCode> = new Set([
    "CDB_AMBIGUOUS_COLOCATION",
    "CDB_AMBIGUOUS_TENANT",
    "CDB_CROSS_PARTITION",
    "CDB_CROSS_PARTITION_BATCH",
    "CDB_INVALID_ARGS",
    "CDB_INVALID_COLUMN",
    "CDB_INVALID_PARTITION",
    "CDB_INVALID_SELF",
    "CDB_INVALID_TENANT",
    "CDB_MISSING_TENANT_FK",
    "CDB_NONLOCAL_FK",
    "CDB_NONLOCAL_UNIQUE",
    "CDB_NOT_CDB_TABLE",
    "CDB_POLICY_CONFLICT",
    "CDB_POLICY_UNKNOWN_ROOT",
    "CDB_RESERVED_TABLE_NAME",
    "CDB_SCATTER_NOT_INDEX",
]);

const FORBIDDEN_CODES: ReadonlySet<CdbErrorCode> = new Set([
    "CDB_CALLER_DENIED",
    "CDB_FORBIDDEN",
    "CDB_FORBIDDEN_COLUMN",
    "CDB_SELF_OVERRIDE",
    "CDB_TENANT_OVERRIDE",
]);

const CONFLICT_CODES: ReadonlySet<CdbErrorCode> = new Set([
    "CDB_MUT_ID_COLLISION",
    "CDB_UNIQUE_VIOLATION",
    "CDB_PARTITION_CONTRACT_CHANGED",
    "CDB_RESHARD_PHASE_MISMATCH",
    "CDB_SHARDS_CHANGED",
    "CDB_STALE_EPOCH",
]);

const NOT_IMPLEMENTED_CODES: ReadonlySet<CdbErrorCode> = new Set([
    "CDB_DT_NOT_IMPLEMENTED",
    "CDB_INTERACTIVE_TXN_UNSUPPORTED",
    "CDB_UNSUPPORTED_FEATURE",
]);

const UNAVAILABLE_CODES: ReadonlySet<CdbErrorCode> = new Set([
    "CDB_CATALOG_UNAVAILABLE",
    "CDB_MUTATION_OUTCOME_UNKNOWN",
    "CDB_SHARD_UNAVAILABLE",
    "CDB_STREAM_ABORTED",
    "CDB_TXN_ABORTED_EVICTION",
]);

/** Stable HTTP projection for errors thrown by application routes. */
export function httpStatusForCdbError(code: CdbErrorCode): number {
    if (BAD_REQUEST_CODES.has(code)) return 400;
    if (FORBIDDEN_CODES.has(code)) return 403;
    if (code === "CDB_REF_NOT_FOUND") return 404;
    if (CONFLICT_CODES.has(code)) return 409;
    if (code === "CDB_RATE_LIMITED") return 429;
    if (NOT_IMPLEMENTED_CODES.has(code)) return 501;
    if (UNAVAILABLE_CODES.has(code)) return 503;
    return 500;
}

export function cdbHttpErrorResponse(error: CdbError): Response {
    const headers = new Headers({
        "cache-control": "no-store",
        "content-type": "application/json; charset=UTF-8",
    });
    if (error.retryAfterMs !== undefined) {
        headers.set("retry-after", String(Math.max(1, Math.ceil(error.retryAfterMs / 1_000))));
    }
    return new Response(JSON.stringify({ error: error.toJSON() }), {
        status: httpStatusForCdbError(error.code),
        headers,
    });
}

/** Default Hono error boundary installed by `chardb()`. */
export function chardbHttpErrorHandler(error: Error): Response {
    if (isCdbError(error)) return cdbHttpErrorResponse(error);
    console.error(error);
    return new Response("Internal Server Error", {
        status: 500,
        headers: { "cache-control": "no-store", "content-type": "text/plain; charset=UTF-8" },
    });
}
