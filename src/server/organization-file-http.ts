import { CdbError, isCdbError, rehydrateCdbRpcError } from "../errors.ts";
import { FileId, type FileId as FileIdType } from "../files/index.ts";
import { stableJson } from "../util/canonical.ts";
import type { RouteResult } from "./do/catalog.ts";
import type { CdbFileDownloadRequest } from "./do/cdb-file-runtime.ts";
import type { StoredFile } from "./do/cdb-file-store.ts";
import type { ChardbEnv } from "./entrypoint.ts";
import {
    type OrganizationFileDispatchFailure,
    type OrganizationFileLocator,
    dispatchOrganizationFileOperation,
} from "./file-auth-dispatch.ts";
import { fileProviderCall, readRecoverableFile } from "./file-retention.ts";
import { cdbHttpErrorResponse } from "./http-errors.ts";
import { type OrganizationFileUploadCdb, uploadOrganizationFile } from "./organization-file-upload.ts";
import type { ChardbFileResourceDescriptor } from "./resource-descriptors.ts";
import type { CatalogOrganizationAuthorityRouteRpc, CatalogOrganizationAuthorityRpc } from "./rpc.ts";

export const ORGANIZATION_FILE_UPLOAD_PATH = "/_chardb/files/upload";
export const ORGANIZATION_FILE_DOWNLOAD_PATH = "/_chardb/files/download";
export const ORGANIZATION_FILE_IDEMPOTENCY_KEY_MAX_BYTES = 128;

type FileCatalogRpc = CatalogOrganizationAuthorityRpc & CatalogOrganizationAuthorityRouteRpc;
type OrganizationFileCdb = OrganizationFileUploadCdb & {
    resolveFileDownload(request: CdbFileDownloadRequest & { readonly schemaEpoch: number }): Promise<StoredFile | null>;
};

export interface OrganizationFileHttpAuth {
    readonly api: {
        getSession(input: { readonly headers: Headers }): Promise<unknown> | unknown;
    };
}

function json(status: number, value: unknown): Response {
    return new Response(JSON.stringify(value), {
        status,
        headers: {
            "cache-control": "no-store",
            "content-type": "application/json; charset=UTF-8",
        },
    });
}

function invalid(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message });
}

function oneQueryValue(url: URL, name: string): string {
    const values = url.searchParams.getAll(name);
    if (values.length !== 1 || !values[0]) invalid(`${name} is required exactly once`);
    return values[0];
}

function parseLocator(url: URL, extra: readonly string[] = []): OrganizationFileLocator {
    const allowed = new Set(["organizationId", "table", "column", ...extra]);
    for (const key of url.searchParams.keys()) if (!allowed.has(key)) invalid(`unexpected query parameter ${key}`);
    return {
        organizationId: oneQueryValue(url, "organizationId"),
        table: oneQueryValue(url, "table"),
        column: oneQueryValue(url, "column"),
    };
}

function idempotencyKey(request: Request): string {
    const value = request.headers.get("idempotency-key");
    const bytes = value ? new TextEncoder().encode(value).byteLength : 0;
    const hasControl = value ? Array.from(value).some(character => character.charCodeAt(0) < 32) : true;
    if (!value || bytes > ORGANIZATION_FILE_IDEMPOTENCY_KEY_MAX_BYTES || hasControl) {
        invalid(`Idempotency-Key must contain 1 through ${ORGANIZATION_FILE_IDEMPOTENCY_KEY_MAX_BYTES} bytes`);
    }
    return value;
}

/** A declared length, already bounded by the column, fills one exact buffer; otherwise chunks are joined once. */
async function readBoundedUploadBody(request: Request, maxBytes: number, declared: number | null): Promise<Uint8Array> {
    if (!request.body) invalid("file body is required");
    const exact = declared === null ? undefined : new Uint8Array(declared);
    const limit = declared ?? maxBytes;
    const reader = request.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    while (true) {
        const next = await reader.read();
        if (next.done) break;
        if (size + next.value.byteLength > limit) {
            await reader.cancel().catch(() => undefined);
            invalid("file size is outside the configured column bound");
        }
        if (exact) exact.set(next.value, size);
        else chunks.push(next.value);
        size += next.value.byteLength;
    }
    if (size < 1 || size !== (declared ?? size)) invalid("file size is outside the configured column bound");
    if (exact) return exact;
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return bytes;
}

function assertSameOrigin(request: Request): void {
    const requestOrigin = new URL(request.url).origin;
    const origin = request.headers.get("origin");
    if ((origin !== null && origin !== requestOrigin) || request.headers.get("sec-fetch-site") === "cross-site") {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "cross-origin file access is not allowed" });
    }
}

function fileCatalog(env: ChardbEnv): FileCatalogRpc {
    const id = env.CDB_CATALOG.idFromName("global");
    return env.CDB_CATALOG.get(id) as unknown as FileCatalogRpc;
}

function fileCdb(env: ChardbEnv, route: RouteResult): OrganizationFileCdb {
    const id = env.CDB_SHARD.idFromName(route.shardId);
    return env.CDB_SHARD.get(id) as unknown as OrganizationFileCdb;
}

function sameFileRoute(left: RouteResult, right: RouteResult | undefined): boolean {
    return (
        right !== undefined &&
        left.shardId === right.shardId &&
        left.schemaEpoch === right.schemaEpoch &&
        left.recoveryGeneration === right.recoveryGeneration &&
        left.domainSchemaEpoch === right.domainSchemaEpoch
    );
}

function sameStoredFile(left: StoredFile, right: StoredFile): boolean {
    return (
        left.fileId === right.fileId &&
        left.organizationId === right.organizationId &&
        left.table === right.table &&
        left.column === right.column &&
        left.objectKey === right.objectKey &&
        left.contentType === right.contentType &&
        left.size === right.size &&
        left.sha256 === right.sha256 &&
        left.status === right.status &&
        left.rowId === right.rowId &&
        left.createdAt === right.createdAt &&
        left.updatedAt === right.updatedAt
    );
}

function dispatchFailure(failure: OrganizationFileDispatchFailure): Response {
    return json(failure.status, { error: { code: failure.code } });
}

async function hexDigest(value: string): Promise<string> {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
    return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

/** Mint the opaque retry identity without accepting a caller-selected FileId. */
export async function organizationFileId(input: {
    readonly principalId: string;
    readonly locator: OrganizationFileLocator;
    readonly idempotencyKey: string;
}): Promise<FileIdType> {
    return FileId(`fil_${await hexDigest(stableJson([input.principalId, input.locator, input.idempotencyKey]))}`);
}

/** Private reserved HTTP upload boundary. The package does not export this route yet. */
export async function handleOrganizationFileUploadRequest(input: {
    readonly request: Request;
    readonly env: ChardbEnv;
    readonly auth: OrganizationFileHttpAuth;
    readonly resources: readonly ChardbFileResourceDescriptor[];
    readonly nowMs?: number;
}): Promise<Response> {
    try {
        const url = new URL(input.request.url);
        if (url.pathname !== ORGANIZATION_FILE_UPLOAD_PATH) return json(404, { error: { code: "NOT_FOUND" } });
        if (input.request.method !== "PUT") {
            return new Response(null, { status: 405, headers: { allow: "PUT", "cache-control": "no-store" } });
        }
        assertSameOrigin(input.request);
        const locator = parseLocator(url);
        const retryKey = idempotencyKey(input.request);
        const contentType = input.request.headers.get("content-type")?.trim().toLowerCase();
        if (!contentType) invalid("Content-Type is required");
        const declaredLength = input.request.headers.get("content-length");
        if (
            declaredLength !== null &&
            (!/^[0-9]+$/.test(declaredLength) || !Number.isSafeInteger(Number(declaredLength)))
        ) {
            invalid("Content-Length is invalid");
        }

        const session = await input.auth.api.getSession({ headers: input.request.headers });
        const dispatched = await dispatchOrganizationFileOperation({
            session,
            locator,
            resources: input.resources,
            catalog: fileCatalog(input.env),
            bucket: input.env.CDB_FILES,
            operation: async context => {
                if (!context.route) {
                    throw new CdbError({ code: "CDB_CATALOG_UNAVAILABLE", message: "file placement is unavailable" });
                }
                if (declaredLength !== null && Number(declaredLength) > context.resource.maxSize) {
                    invalid("file exceeds the configured column size");
                }
                const bytes = await readBoundedUploadBody(
                    input.request,
                    context.resource.maxSize,
                    declaredLength === null ? null : Number(declaredLength)
                );
                const fileId = await organizationFileId({
                    principalId: context.session.principalId,
                    locator,
                    idempotencyKey: retryKey,
                });
                const nowMs = input.nowMs ?? Date.now();
                const attempt = (route: RouteResult, auth: typeof context.auth) =>
                    uploadOrganizationFile({
                        cdb: fileCdb(input.env, route),
                        bucket: context.bucket,
                        fileId,
                        organizationId: locator.organizationId,
                        table: locator.table,
                        column: locator.column,
                        contentType,
                        bytes,
                        nowMs,
                        domainSchemaEpoch: route.domainSchemaEpoch,
                        schemaEpoch: route.schemaEpoch,
                        recoveryGeneration: route.recoveryGeneration,
                        auth,
                        refreshAuthority: async () => {
                            const refreshed = await context.refreshAuthorityRoute();
                            if (!sameFileRoute(route, refreshed.route)) {
                                throw new CdbError({
                                    code: "CDB_STALE_EPOCH",
                                    message: "file placement changed during upload",
                                });
                            }
                            return refreshed.auth;
                        },
                    });
                try {
                    return await attempt(context.route, context.auth);
                } catch (error) {
                    if (!isCdbError(error) || error.code !== "CDB_STALE_EPOCH") throw error;
                    const refreshed = await context.refreshAuthorityRoute();
                    if (!refreshed.route) {
                        throw new CdbError({
                            code: "CDB_CATALOG_UNAVAILABLE",
                            message: "file placement is unavailable after a stale route",
                        });
                    }
                    return attempt(refreshed.route, refreshed.auth);
                }
            },
        });
        if (!dispatched.ok) return dispatchFailure(dispatched);
        return json(200, { file: dispatched.value });
    } catch (error) {
        if (isCdbError(error)) return cdbHttpErrorResponse(error);
        console.error(error);
        return new Response("Internal Server Error", {
            status: 500,
            headers: { "cache-control": "no-store", "content-type": "text/plain; charset=UTF-8" },
        });
    }
}

/** Private download boundary. Row and column policy run inside Cdb before R2 is opened. */
export async function handleOrganizationFileDownloadRequest(input: {
    readonly request: Request;
    readonly env: ChardbEnv;
    readonly auth: OrganizationFileHttpAuth;
    readonly resources: readonly ChardbFileResourceDescriptor[];
}): Promise<Response> {
    try {
        const url = new URL(input.request.url);
        if (url.pathname !== ORGANIZATION_FILE_DOWNLOAD_PATH) return json(404, { error: { code: "NOT_FOUND" } });
        if (input.request.method !== "GET") {
            return new Response(null, { status: 405, headers: { allow: "GET", "cache-control": "no-store" } });
        }
        assertSameOrigin(input.request);
        if (input.request.headers.has("range")) {
            return new Response(null, { status: 416, headers: { "cache-control": "no-store" } });
        }
        const locator = parseLocator(url, ["rowId"]);
        const rowId = oneQueryValue(url, "rowId");
        const session = await input.auth.api.getSession({ headers: input.request.headers });
        const dispatched = await dispatchOrganizationFileOperation({
            session,
            locator,
            resources: input.resources,
            catalog: fileCatalog(input.env),
            bucket: input.env.CDB_FILES,
            operation: async context => {
                if (!context.route) {
                    throw new CdbError({ code: "CDB_CATALOG_UNAVAILABLE", message: "file placement is unavailable" });
                }
                const resolve = async (route: RouteResult, auth: typeof context.auth) => {
                    try {
                        return await fileCdb(input.env, route).resolveFileDownload({
                            ...locator,
                            rowId,
                            domainSchemaEpoch: route.domainSchemaEpoch,
                            schemaEpoch: route.schemaEpoch,
                            recoveryGeneration: route.recoveryGeneration,
                            auth,
                        });
                    } catch (error) {
                        throw rehydrateCdbRpcError(error);
                    }
                };
                const current = await context.refreshAuthorityRoute();
                if (!current.route) {
                    throw new CdbError({ code: "CDB_CATALOG_UNAVAILABLE", message: "file placement is unavailable" });
                }
                let stored: StoredFile | null;
                let resolvedRoute = current.route;
                try {
                    stored = await resolve(resolvedRoute, current.auth);
                } catch (error) {
                    if (!isCdbError(error) || error.code !== "CDB_STALE_EPOCH") throw error;
                    const refreshed = await context.refreshAuthorityRoute();
                    if (!refreshed.route) {
                        throw new CdbError({
                            code: "CDB_CATALOG_UNAVAILABLE",
                            message: "file placement is unavailable after a stale route",
                        });
                    }
                    resolvedRoute = refreshed.route;
                    stored = await resolve(resolvedRoute, refreshed.auth);
                }
                if (!stored) return null;
                const object = await fileProviderCall(() => readRecoverableFile(context.bucket, stored));
                const finalAuthority = await context.refreshAuthorityRoute();
                if (!sameFileRoute(resolvedRoute, finalAuthority.route)) {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "file placement changed while reading retained content",
                    });
                }
                const finalStored = await resolve(resolvedRoute, finalAuthority.auth);
                if (!finalStored) return null;
                if (!sameStoredFile(stored, finalStored)) {
                    throw new CdbError({
                        code: "CDB_STALE_EPOCH",
                        message: "file attachment changed while reading retained content",
                    });
                }
                return { object, stored: finalStored };
            },
        });
        if (!dispatched.ok) return dispatchFailure(dispatched);
        if (!dispatched.value) return json(404, { error: { code: "NOT_FOUND" } });
        const headers = new Headers({
            "cache-control": "private, no-store",
            "content-disposition": "attachment",
            "content-length": String(dispatched.value.stored.size),
            "content-security-policy": "sandbox",
            "content-type": dispatched.value.stored.contentType,
            "cross-origin-resource-policy": "same-origin",
            "x-content-type-options": "nosniff",
        });
        return new Response(dispatched.value.object.body, { status: 200, headers });
    } catch (error) {
        if (isCdbError(error)) return cdbHttpErrorResponse(error);
        console.error(error);
        return new Response("Internal Server Error", {
            status: 500,
            headers: { "cache-control": "no-store", "content-type": "text/plain; charset=UTF-8" },
        });
    }
}

/** Route the private reserved file subtree without exposing it as a package subpath. */
export function handleOrganizationFileRequest(input: {
    readonly request: Request;
    readonly env: ChardbEnv;
    readonly auth: OrganizationFileHttpAuth;
    readonly resources: readonly ChardbFileResourceDescriptor[];
    readonly nowMs?: number;
}): Promise<Response> {
    const pathname = new URL(input.request.url).pathname;
    if (pathname === ORGANIZATION_FILE_UPLOAD_PATH) return handleOrganizationFileUploadRequest(input);
    if (pathname === ORGANIZATION_FILE_DOWNLOAD_PATH) return handleOrganizationFileDownloadRequest(input);
    return Promise.resolve(json(404, { error: { code: "NOT_FOUND" } }));
}
