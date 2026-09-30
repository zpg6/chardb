import { describe, expect, test } from "bun:test";
import { CDB_ERROR_CODES, CdbError } from "../../src/errors.ts";
import { cdbHttpErrorResponse, httpStatusForCdbError } from "../../src/server/http-errors.ts";

describe("CharDB HTTP errors", () => {
    test("maps the stable error-code surface to valid failure statuses", () => {
        for (const code of CDB_ERROR_CODES) {
            const status = httpStatusForCdbError(code);
            expect(status).toBeGreaterThanOrEqual(400);
            expect(status).toBeLessThan(600);
        }
        expect(httpStatusForCdbError("CDB_INVALID_ARGS")).toBe(400);
        expect(httpStatusForCdbError("CDB_FORBIDDEN")).toBe(403);
        expect(httpStatusForCdbError("CDB_REF_NOT_FOUND")).toBe(404);
        expect(httpStatusForCdbError("CDB_STALE_EPOCH")).toBe(409);
        expect(httpStatusForCdbError("CDB_UNIQUE_VIOLATION")).toBe(409);
        expect(httpStatusForCdbError("CDB_RATE_LIMITED")).toBe(429);
        expect(httpStatusForCdbError("CDB_UNSUPPORTED_FEATURE")).toBe(501);
        expect(httpStatusForCdbError("CDB_SHARD_UNAVAILABLE")).toBe(503);
        expect(httpStatusForCdbError("CDB_INVARIANT")).toBe(500);
    });

    test("preserves the typed error envelope and emits an HTTP Retry-After", async () => {
        const response = cdbHttpErrorResponse(
            new CdbError({
                code: "CDB_RATE_LIMITED",
                message: "slow down",
                retryAfterMs: 1_001,
                correlationId: "trace-7",
            })
        );
        expect(response.status).toBe(429);
        expect(response.headers.get("cache-control")).toBe("no-store");
        expect(response.headers.get("retry-after")).toBe("2");
        expect((await response.json()) as unknown).toMatchObject({
            error: {
                code: "CDB_RATE_LIMITED",
                message: "slow down",
                retryable: true,
                correlationId: "trace-7",
            },
        });
    });
});
