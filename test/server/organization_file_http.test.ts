import { describe, expect, test } from "bun:test";
import { CdbError } from "../../src/errors.ts";
import type { ChardbEnv } from "../../src/server/entrypoint.ts";
import {
    ORGANIZATION_FILE_DOWNLOAD_PATH,
    ORGANIZATION_FILE_UPLOAD_PATH,
    handleOrganizationFileDownloadRequest,
    handleOrganizationFileUploadRequest,
    organizationFileId,
} from "../../src/server/organization-file-http.ts";
import type { OrganizationFileUploadCdb } from "../../src/server/organization-file-upload.ts";
import type { ChardbFileResourceDescriptor } from "../../src/server/resource-descriptors.ts";

const resource: ChardbFileResourceDescriptor = {
    kind: "file",
    version: 1,
    table: "messages",
    column: "attachment",
    primaryKey: "id",
    organizationColumn: "organization_id",
    maxSize: 64,
    contentTypes: ["image/png"],
};

const session = {
    user: { id: "user-1" },
    session: { activeOrganizationId: "org-1" },
};

const authority = {
    recoveryGeneration: 0,
    principalId: "user-1",
    organizationId: "org-1",
    role: "member",
    roles: ["member"],
    authEpochs: { global: 1, tenant: 2, principal: 3 },
};

type DownloadProviderFailure = "retained-get";

function fixture(
    options: {
        readonly session?: unknown;
        readonly moveOnRefresh?: boolean;
        readonly moveOnEveryRefresh?: boolean;
        readonly downloadStaleOnce?: boolean;
        readonly reserveRpcError?: boolean;
        readonly downloadRpcStaleOnce?: boolean;
        readonly downloadMissing?: boolean;
        readonly downloadObjectMissing?: boolean;
        readonly changeRoleAfterProviderRead?: boolean;
        readonly uploadProviderFailure?: "retain";
    } = {}
) {
    const calls: string[] = [];
    const reservedFileIds: string[] = [];
    let routeCalls = 0;
    let object:
        | {
              readonly key: string;
              readonly size: number;
              readonly bytes: Uint8Array;
              readonly customMetadata: Record<string, string>;
          }
        | undefined;
    let retained:
        | {
              readonly key: string;
              readonly size: number;
              readonly bytes: Uint8Array;
              readonly customMetadata: Record<string, string>;
          }
        | undefined;
    let status: "pending" | "ready" = "pending";
    let readyFile: { readonly fileId: string; readonly size: number; readonly sha256: string } | undefined;
    let downloadCalls = 0;
    let providerRead = false;
    let downloadProviderFailure: DownloadProviderFailure | undefined;
    const cdb: OrganizationFileUploadCdb & {
        resolveFileDownload(request: {
            readonly organizationId: string;
            readonly table: string;
            readonly column: string;
            readonly rowId: string;
            readonly auth: { readonly role: string };
        }): Promise<unknown>;
    } = {
        async reserveFile(request) {
            calls.push("cdb.reserve");
            if (options.reserveRpcError) {
                throw new Error("CDB_INVALID_ARGS: file content type is not accepted");
            }
            reservedFileIds.push(request.fileId);
            return {
                fileId: request.fileId as never,
                organizationId: request.organizationId,
                table: request.table,
                column: request.column,
                objectKey: `v1/${request.organizationId}/${request.fileId}`,
                contentType: request.contentType,
                size: request.size,
                sha256: status === "ready" ? (object?.customMetadata.chardbSha256 ?? null) : null,
                status,
                rowId: null,
                createdAt: request.nowMs,
                updatedAt: request.nowMs,
            };
        },
        markFileReady(request) {
            calls.push("cdb.ready");
            status = "ready";
            readyFile = { fileId: request.fileId, size: request.size, sha256: request.sha256 };
            return {
                fileId: request.fileId as never,
                organizationId: request.organizationId,
                table: "messages",
                column: "attachment",
                objectKey: `v1/${request.organizationId}/${request.fileId}`,
                contentType: "image/png",
                size: request.size,
                sha256: request.sha256,
                status,
                rowId: null,
                createdAt: request.nowMs,
                updatedAt: request.nowMs,
            };
        },
        async resolveFileDownload(request) {
            calls.push("cdb.download");
            downloadCalls++;
            if (request.auth.role === "viewer") {
                throw new CdbError({ code: "CDB_FORBIDDEN", message: "file row policy denied access" });
            }
            if ((options.downloadStaleOnce || options.downloadRpcStaleOnce) && downloadCalls === 1) {
                if (options.downloadRpcStaleOnce) {
                    throw new Error("CDB_STALE_EPOCH: download route moved");
                }
                throw new CdbError({ code: "CDB_STALE_EPOCH", message: "download route moved" });
            }
            if (options.downloadMissing || !readyFile) return null;
            return {
                fileId: readyFile.fileId,
                organizationId: request.organizationId,
                table: request.table,
                column: request.column,
                objectKey: `v1/${request.organizationId}/${readyFile.fileId}`,
                contentType: "image/png",
                size: readyFile.size,
                sha256: readyFile.sha256,
                status: "attached",
                rowId: request.rowId,
                createdAt: 100,
                updatedAt: 102,
            };
        },
    };
    const catalog = {
        async resolveOrganizationAuthority() {
            throw new Error("the routed RPC must be used");
        },
        async resolveOrganizationAuthorityRoute() {
            calls.push("catalog.route");
            routeCalls++;
            const movingEveryTime = options.moveOnEveryRefresh === true;
            return {
                authority:
                    options.changeRoleAfterProviderRead && providerRead
                        ? { ...authority, role: "viewer", roles: ["viewer"] }
                        : authority,
                route: {
                    shardId: (movingEveryTime
                        ? `shard-${routeCalls}`
                        : options.moveOnRefresh && routeCalls > 1
                          ? "shard-b"
                          : "shard-a") as never,
                    schemaEpoch: movingEveryTime ? routeCalls : options.moveOnRefresh && routeCalls > 1 ? 2 : 1,
                    recoveryGeneration: 0,
                    domainSchemaEpoch: 2,
                },
            };
        },
    };
    const bucket = {
        async put(key: string, value: Uint8Array, putOptions: R2PutOptions) {
            calls.push("r2.put");
            const retainedKey = key.startsWith("_chardb/retained/");
            if (options.uploadProviderFailure && retainedKey) {
                throw new Error("transient R2 put failure");
            }
            const target = retainedKey ? retained : object;
            if (target) return null;
            const stored = {
                key,
                size: value.byteLength,
                bytes: Uint8Array.from(value),
                customMetadata: putOptions.customMetadata ?? {},
            };
            if (key.startsWith("_chardb/retained/")) retained = stored;
            else object = stored;
            return stored;
        },
        async head(key: string) {
            calls.push("r2.head");
            if (object?.key === key) return object;
            return retained?.key === key ? retained : null;
        },
        async get(key: string) {
            calls.push("r2.get");
            if (options.downloadObjectMissing) return null;
            const retainedKey = key.startsWith("_chardb/retained/");
            if (retainedKey && downloadProviderFailure === "retained-get") {
                throw new Error("transient retained R2 get failure");
            }
            const stored = object?.key === key ? object : retained?.key === key ? retained : undefined;
            if (!stored) return null;
            providerRead = true;
            return {
                ...stored,
                body: new Response(Uint8Array.from(stored.bytes)).body,
            };
        },
    } as unknown as R2Bucket;
    const namespace = (value: unknown) =>
        ({
            idFromName: (name: string) => name,
            get: () => value,
        }) as unknown as DurableObjectNamespace;
    const env = {
        CDB_CATALOG: namespace(catalog),
        CDB_SHARD: namespace(cdb),
        CDB_GATEWAY: namespace({}),
        CDB_FILES: bucket,
    } as ChardbEnv;
    const auth = {
        api: {
            async getSession() {
                calls.push("auth.session");
                return options.session === undefined ? session : options.session;
            },
        },
    };
    const request = (
        overrides: {
            readonly key?: string | null;
            readonly origin?: string;
            readonly body?: BodyInit;
            readonly headers?: Record<string, string>;
        } = {}
    ) => {
        const key = overrides.key === undefined ? "avatar-v1" : overrides.key;
        return new Request(
            `https://app.example${ORGANIZATION_FILE_UPLOAD_PATH}?organizationId=org-1&table=messages&column=attachment`,
            {
                method: "PUT",
                headers: {
                    "content-type": "image/png",
                    ...(key === null ? {} : { "idempotency-key": key }),
                    ...(overrides.origin ? { origin: overrides.origin } : {}),
                    ...overrides.headers,
                },
                body: overrides.body ?? "exact bytes",
            }
        );
    };
    const downloadRequest = (overrides: { readonly origin?: string; readonly organizationId?: string } = {}) =>
        new Request(
            `https://app.example${ORGANIZATION_FILE_DOWNLOAD_PATH}?organizationId=${overrides.organizationId ?? "org-1"}&table=messages&column=attachment&rowId=row-1`,
            { headers: overrides.origin ? { origin: overrides.origin } : {} }
        );
    return {
        auth,
        calls,
        downloadRequest,
        env,
        failDownloadProviderAt(value: DownloadProviderFailure) {
            downloadProviderFailure = value;
        },
        object: () => object,
        retained: () => retained,
        request,
        reservedFileIds,
    };
}

describe("private organization file HTTP upload", () => {
    test("mints a deterministic opaque FileId scoped to principal, locator, and retry key", async () => {
        const locator = { organizationId: "org-1", table: "messages", column: "attachment" };
        const first = await organizationFileId({ principalId: "user-1", locator, idempotencyKey: "retry-1" });
        expect(first).toMatch(/^fil_[0-9a-f]{64}$/);
        expect(await organizationFileId({ principalId: "user-1", locator, idempotencyKey: "retry-1" })).toBe(first);
        expect(await organizationFileId({ principalId: "user-2", locator, idempotencyKey: "retry-1" })).not.toBe(first);
    });

    test("uploads through routed Catalog authority and retries the same immutable object", async () => {
        const f = fixture();
        const first = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
            nowMs: 100,
        });
        expect(first.status).toBe(200);
        const firstBody = (await first.json()) as {
            readonly file: { readonly fileId: string; readonly sha256: string };
        };
        expect(firstBody.file.fileId).toMatch(/^fil_[0-9a-f]{64}$/);
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "cdb.reserve",
            "r2.put",
            "catalog.route",
            "cdb.ready",
        ]);
        expect(f.retained()).toMatchObject({
            key: `_chardb/retained/sha256/${firstBody.file.sha256}`,
            size: 11,
            customMetadata: { chardbRetainedSha256: firstBody.file.sha256, chardbRetainedSize: "11" },
        });
        expect(f.object()).toBeUndefined();

        f.calls.splice(0);
        const retry = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
            nowMs: 101,
        });
        const retryBody = (await retry.json()) as unknown;
        expect(retryBody).toEqual(firstBody);
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "cdb.reserve",
            "r2.put",
            "catalog.route",
            "cdb.ready",
        ]);
    });

    test("rejects cross-origin and unauthenticated requests before Catalog, Cdb, or R2", async () => {
        const crossOrigin = fixture();
        const forbidden = await handleOrganizationFileUploadRequest({
            request: crossOrigin.request({ origin: "https://evil.example" }),
            env: crossOrigin.env,
            auth: crossOrigin.auth,
            resources: [resource],
        });
        expect(forbidden.status).toBe(403);
        expect(crossOrigin.calls).toEqual([]);

        const anonymous = fixture({ session: null });
        const unauthorized = await handleOrganizationFileUploadRequest({
            request: anonymous.request(),
            env: anonymous.env,
            auth: anonymous.auth,
            resources: [resource],
        });
        expect(unauthorized.status).toBe(401);
        expect(anonymous.calls).toEqual(["auth.session"]);
    });

    test("retries once on the new owner when Catalog placement changes after the R2 write", async () => {
        const f = fixture({ moveOnRefresh: true });
        const response = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(200);
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "cdb.reserve",
            "r2.put",
            "catalog.route",
            "catalog.route",
            "cdb.reserve",
            "r2.put",
            "catalog.route",
            "cdb.ready",
        ]);
        expect(new Set(f.reservedFileIds).size).toBe(1);
        expect(f.retained()).toBeDefined();
        expect(f.object()).toBeUndefined();
    });

    test("fails closed instead of looping when placement changes during the retry", async () => {
        const f = fixture({ moveOnEveryRefresh: true });
        const response = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(409);
        expect(f.calls.filter(call => call === "cdb.reserve")).toHaveLength(2);
        expect(f.calls.filter(call => call === "r2.put")).toHaveLength(2);
        expect(f.calls.filter(call => call === "cdb.ready")).toHaveLength(0);
        expect(f.calls.filter(call => call === "catalog.route")).toHaveLength(4);
        expect(new Set(f.reservedFileIds).size).toBe(1);
        expect(f.retained()).toBeDefined();
        expect(f.object()).toBeUndefined();
    });

    test("rejects a missing retry identity and an oversized body before Cdb or R2", async () => {
        const noKey = fixture();
        const missingKey = await handleOrganizationFileUploadRequest({
            request: noKey.request({ key: null }),
            env: noKey.env,
            auth: noKey.auth,
            resources: [resource],
        });
        expect(missingKey.status).toBe(400);
        expect(noKey.calls).toEqual([]);

        const tooLarge = fixture();
        const oversized = await handleOrganizationFileUploadRequest({
            request: tooLarge.request({ body: "x".repeat(resource.maxSize + 1) }),
            env: tooLarge.env,
            auth: tooLarge.auth,
            resources: [resource],
        });
        expect(oversized.status).toBe(400);
        expect(tooLarge.calls).toEqual(["auth.session", "catalog.route"]);
    });

    test("cancels a chunked upload as soon as it crosses the configured limit", async () => {
        const f = fixture();
        let pulls = 0;
        let cancelled = false;
        const body = new ReadableStream<Uint8Array>(
            {
                pull(controller) {
                    pulls++;
                    if (pulls <= 2) {
                        controller.enqueue(new Uint8Array(40));
                        return;
                    }
                    controller.error(new Error("upload reader pulled beyond the rejecting chunk"));
                },
                cancel() {
                    cancelled = true;
                },
            },
            { highWaterMark: 0 }
        );

        const response = await handleOrganizationFileUploadRequest({
            request: f.request({ body }),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(400);
        expect(cancelled).toBe(true);
        expect(pulls).toBe(2);
        expect(f.calls).toEqual(["auth.session", "catalog.route"]);
    });

    test("fills a declared-length upload exactly and rejects a body that outgrows it", async () => {
        const chunked = (chunks: readonly Uint8Array[], onCancel: () => void) => {
            let pulls = 0;
            return new ReadableStream<Uint8Array>(
                {
                    pull(controller) {
                        const chunk = chunks[pulls++];
                        if (chunk) controller.enqueue(chunk);
                        else if (pulls > chunks.length + 1) controller.error(new Error("pulled past the end"));
                        else controller.close();
                    },
                    cancel: onCancel,
                },
                { highWaterMark: 0 }
            );
        };
        const encoder = new TextEncoder();
        const exact = fixture();
        const stored = await handleOrganizationFileUploadRequest({
            request: exact.request({
                headers: { "content-length": "11" },
                body: chunked([encoder.encode("exact "), encoder.encode("bytes")], () => undefined),
            }),
            env: exact.env,
            auth: exact.auth,
            resources: [resource],
        });
        expect(stored.status).toBe(200);
        expect(exact.retained()).toBeDefined();

        const lying = fixture();
        let cancelled = false;
        const response = await handleOrganizationFileUploadRequest({
            request: lying.request({
                headers: { "content-length": "40" },
                body: chunked([new Uint8Array(40), new Uint8Array(1)], () => {
                    cancelled = true;
                }),
            }),
            env: lying.env,
            auth: lying.auth,
            resources: [resource],
        });
        expect(response.status).toBe(400);
        expect(cancelled).toBe(true);
        expect(lying.calls).toEqual(["auth.session", "catalog.route"]);
    });

    test("rehydrates a message-only Cdb error returned by Workers RPC", async () => {
        const f = fixture({ reserveRpcError: true });
        const response = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(400);
        expect(await response.json()).toMatchObject({ error: { code: "CDB_INVALID_ARGS" } });
        expect(f.calls).toEqual(["auth.session", "catalog.route", "cdb.reserve"]);
    });

    test("returns a typed retryable 503 when R2 fails during upload", async () => {
        const f = fixture({ uploadProviderFailure: "retain" });
        const response = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(503);
        expect(await response.json()).toMatchObject({
            error: {
                code: "CDB_SHARD_UNAVAILABLE",
                retryable: true,
                docs: "https://chardb.dev/errors/cdb_shard_unavailable",
            },
        });
        expect(f.calls.filter(call => call === "r2.put")).toHaveLength(1);
        expect(f.calls).not.toContain("cdb.ready");
    });

    test("downloads exact bytes only after a second authority refresh and the Cdb policy read", async () => {
        const f = fixture();
        const uploaded = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
            nowMs: 100,
        });
        expect(uploaded.status).toBe(200);
        f.calls.splice(0);

        const downloaded = await handleOrganizationFileDownloadRequest({
            request: f.downloadRequest(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(downloaded.status).toBe(200);
        expect(await downloaded.text()).toBe("exact bytes");
        expect(downloaded.headers.get("content-type")).toBe("image/png");
        expect(downloaded.headers.get("content-disposition")).toBe("attachment");
        expect(downloaded.headers.get("x-content-type-options")).toBe("nosniff");
        expect(downloaded.headers.get("cross-origin-resource-policy")).toBe("same-origin");
        expect(downloaded.headers.get("content-security-policy")).toBe("sandbox");
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "catalog.route",
            "cdb.download",
            "r2.get",
            "catalog.route",
            "cdb.download",
        ]);
    });

    test("reruns Cdb row policy after R2 and denies a same-route role change", async () => {
        const f = fixture({ changeRoleAfterProviderRead: true });
        expect(
            (
                await handleOrganizationFileUploadRequest({
                    request: f.request(),
                    env: f.env,
                    auth: f.auth,
                    resources: [resource],
                    nowMs: 100,
                })
            ).status
        ).toBe(200);
        f.calls.splice(0);

        const denied = await handleOrganizationFileDownloadRequest({
            request: f.downloadRequest(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(denied.status).toBe(403);
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "catalog.route",
            "cdb.download",
            "r2.get",
            "catalog.route",
            "cdb.download",
        ]);
    });

    test("retries a stale download once against fresh Catalog placement", async () => {
        const f = fixture({ downloadStaleOnce: true, moveOnRefresh: true });
        const uploaded = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
            nowMs: 100,
        });
        expect(uploaded.status).toBe(200);
        f.calls.splice(0);

        const downloaded = await handleOrganizationFileDownloadRequest({
            request: f.downloadRequest(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(downloaded.status).toBe(200);
        expect(await downloaded.text()).toBe("exact bytes");
        expect(f.calls).toEqual([
            "auth.session",
            "catalog.route",
            "catalog.route",
            "cdb.download",
            "catalog.route",
            "cdb.download",
            "r2.get",
            "catalog.route",
            "cdb.download",
        ]);
    });

    test("rehydrates a message-only stale download error before retrying", async () => {
        const f = fixture({ downloadRpcStaleOnce: true, moveOnRefresh: true });
        const uploaded = await handleOrganizationFileUploadRequest({
            request: f.request(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(uploaded.status).toBe(200);
        f.calls.splice(0);
        const response = await handleOrganizationFileDownloadRequest({
            request: f.downloadRequest(),
            env: f.env,
            auth: f.auth,
            resources: [resource],
        });
        expect(response.status).toBe(200);
        expect(f.calls.filter(call => call === "cdb.download")).toHaveLength(3);
    });

    test("makes missing, denied, and cross-organization downloads indistinguishable before R2", async () => {
        const missing = fixture({ downloadMissing: true });
        const missingResponse = await handleOrganizationFileDownloadRequest({
            request: missing.downloadRequest(),
            env: missing.env,
            auth: missing.auth,
            resources: [resource],
        });
        expect(missingResponse.status).toBe(404);
        expect((await missingResponse.json()) as unknown).toEqual({ error: { code: "NOT_FOUND" } });
        expect(missing.calls).not.toContain("r2.get");

        const otherOrganization = fixture();
        const deniedResponse = await handleOrganizationFileDownloadRequest({
            request: otherOrganization.downloadRequest({ organizationId: "org-2" }),
            env: otherOrganization.env,
            auth: otherOrganization.auth,
            resources: [resource],
        });
        expect(deniedResponse.status).toBe(404);
        expect((await deniedResponse.json()) as unknown).toEqual({ error: { code: "NOT_FOUND" } });
        expect(otherOrganization.calls).toEqual(["auth.session"]);

        const crossOrigin = fixture();
        const crossOriginResponse = await handleOrganizationFileDownloadRequest({
            request: crossOrigin.downloadRequest({ origin: "https://evil.example" }),
            env: crossOrigin.env,
            auth: crossOrigin.auth,
            resources: [resource],
        });
        expect(crossOriginResponse.status).toBe(403);
        expect(crossOrigin.calls).toEqual([]);
    });

    test("returns a typed retryable 503 when the retained download is unavailable", async () => {
        for (const failure of ["retained-get"] as const) {
            const f = fixture();
            const uploaded = await handleOrganizationFileUploadRequest({
                request: f.request(),
                env: f.env,
                auth: f.auth,
                resources: [resource],
            });
            expect(uploaded.status).toBe(200);
            f.calls.splice(0);
            f.failDownloadProviderAt(failure);

            const response = await handleOrganizationFileDownloadRequest({
                request: f.downloadRequest(),
                env: f.env,
                auth: f.auth,
                resources: [resource],
            });
            expect(response.status).toBe(503);
            expect(await response.json()).toMatchObject({
                error: {
                    code: "CDB_SHARD_UNAVAILABLE",
                    retryable: true,
                    docs: "https://chardb.dev/errors/cdb_shard_unavailable",
                },
            });
        }
    });

    test("reports attached-object corruption and rejects range reads", async () => {
        const corrupt = fixture({ downloadObjectMissing: true });
        await handleOrganizationFileUploadRequest({
            request: corrupt.request(),
            env: corrupt.env,
            auth: corrupt.auth,
            resources: [resource],
        });
        corrupt.calls.splice(0);
        const corruptResponse = await handleOrganizationFileDownloadRequest({
            request: corrupt.downloadRequest(),
            env: corrupt.env,
            auth: corrupt.auth,
            resources: [resource],
        });
        expect(corruptResponse.status).toBe(500);
        expect(await corruptResponse.json()).toMatchObject({
            error: { code: "CDB_INVARIANT", retryable: false },
        });
        expect(corrupt.calls).toEqual(["auth.session", "catalog.route", "catalog.route", "cdb.download", "r2.get"]);

        const ranged = fixture();
        const rangeRequest = ranged.downloadRequest();
        rangeRequest.headers.set("range", "bytes=0-3");
        const rangeResponse = await handleOrganizationFileDownloadRequest({
            request: rangeRequest,
            env: ranged.env,
            auth: ranged.auth,
            resources: [resource],
        });
        expect(rangeResponse.status).toBe(416);
        expect(ranged.calls).toEqual([]);
    });
});
