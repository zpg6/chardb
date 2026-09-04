import {
    type CatalogJwkResolution,
    type CatalogJwkResolutionRequest,
    createCatalogJwksResolver,
    createCatalogOwnedJwksResolver,
} from "../../auth/jwks_cache.ts";
import { DEFAULT_JWT_CLOCK_TOLERANCE_SECONDS, verifyJwt } from "../../auth/jwt.ts";
import { CdbError, isCdbError, isCdbErrorCode, isRetryable } from "../../errors.ts";
import {
    type ChardbRef,
    type ClientId,
    type Cookie,
    PrincipalId,
    type RawJson,
    type SubId,
    TenantId,
} from "../../types.ts";
import { rawJsonResult } from "../../util/raw_json.ts";
import { VSHARD_COUNT, vshardOf } from "../../vshard.ts";
import type { AuthCtx, MutationAuthority } from "../define.ts";
import type { QueryRouteResponse } from "../manifest.ts";
import { snapshotCdbMutationArgs, snapshotCdbQueryArgs } from "../result_limits.ts";
import type {
    CatalogMutationRpc,
    CatalogOrganizationAuthorityRouteRpc,
    CatalogOrganizationAuthorityRpc,
    CatalogUserAuthorityRpc,
    CdbErrorWire,
    CdbMutationResponse,
    CdbMutationRpc,
    CdbQueryResponse,
    CdbQueryRpc,
    TrustedMutationAuth,
    TrustedMutationDispatchRequest,
    TrustedQueryDispatchRequest,
} from "../rpc.ts";

export interface VerifiedGwAttachment {
    readonly kind: "verified";
    readonly connectionId: string;
    readonly authOrigin: string;
    readonly clientId: ClientId;
    readonly lastCookie?: Cookie;
    readonly snapshotSubIds?: readonly SubId[];
    /** Resumed subscriptions told to discard retained state and awaiting their replacement frame. */
    readonly resumeRefetchPendingSubIds?: readonly SubId[];
    /** Subject from a signature-verified token. */
    readonly principalId: PrincipalId;
    /** Exclusive expiry boundary in epoch seconds, including the verified clock tolerance. */
    readonly jwtExp: number;
    /** Inclusive not-before boundary in epoch seconds, including the verified clock tolerance. */
    readonly jwtNbf?: number;
}

export interface GatewayJwtConfig {
    readonly issuer?: string;
    readonly audience?: string | readonly string[];
    readonly algorithms: readonly string[];
    readonly jwksUrl?: string;
    readonly authBasePath: string;
    readonly jwksPath: string;
    readonly clockToleranceSeconds?: number;
}

interface CatalogJwksRpc extends CatalogMutationRpc {
    getJwk(kid: string): Promise<{ jwkJson: string; expiresAt: number } | null>;
    putJwk(kid: string, jwkJson: string, ttlMs: number): Promise<void>;
    resolveJwk?(request: CatalogJwkResolutionRequest): Promise<CatalogJwkResolution>;
}

export interface GatewayJwtVerificationRequest {
    readonly config: GatewayJwtConfig;
    readonly authOrigin: string;
    readonly catalog: CatalogJwksRpc;
    readonly jwt: string;
    readonly connectionId: string;
    readonly clientId: ClientId;
    readonly lastCookie?: Cookie;
}

export interface TrustedMutationDispatchDeps {
    readonly routeMutation: import("../rpc.ts").MutationRouteResolver;
    readonly catalog: CatalogMutationRpc &
        CatalogOrganizationAuthorityRpc &
        Partial<CatalogOrganizationAuthorityRouteRpc & CatalogUserAuthorityRpc>;
    readonly cdb: (shardId: string) => CdbMutationRpc;
}

export interface TrustedQueryDispatchDeps {
    readonly routeQuery: (request: { readonly ref: string; readonly args: RawJson }) => Promise<QueryRouteResponse>;
    readonly catalog: CatalogMutationRpc &
        CatalogOrganizationAuthorityRpc &
        Partial<CatalogOrganizationAuthorityRouteRpc & CatalogUserAuthorityRpc>;
    readonly cdb: (shardId: string) => CdbQueryRpc;
}

export function mutationFailure(
    code: import("../../errors.ts").CdbErrorCode,
    message: string
): Extract<CdbMutationResponse, { readonly ok: false }> {
    return { ok: false, error: new CdbError({ code, message }).toJSON() };
}

/** Reject stale or malformed shard RPC envelopes before WebSocket settlement. */
export function projectCdbMutationResponse(value: unknown): CdbMutationResponse {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed mutation response");
    }
    const response = value as Record<string, unknown>;
    if (response.ok === true) {
        if (
            typeof response.cookie !== "string" ||
            response.cookie.length === 0 ||
            typeof response.ran !== "boolean" ||
            typeof response.rowsAffected !== "number" ||
            !Number.isSafeInteger(response.rowsAffected) ||
            response.rowsAffected < 0
        ) {
            return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed mutation success");
        }
        try {
            const result = rawJsonResult(response.result, "Cdb mutation result");
            return {
                ok: true,
                cookie: response.cookie,
                ran: response.ran,
                result,
                rowsAffected: response.rowsAffected,
            };
        } catch {
            return mutationFailure("CDB_INVARIANT", "Cdb returned a non-JSON mutation result");
        }
    }
    if (response.ok === false) {
        const error = response.error;
        if (typeof error !== "object" || error === null || Array.isArray(error)) {
            return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed mutation failure");
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
            return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed mutation failure");
        }
        return { ok: false, error: wire as unknown as CdbErrorWire };
    }
    return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed mutation response");
}

export function projectCdbQueryResponse(value: unknown): CdbQueryResponse {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed query response");
    }
    const response = value as Record<string, unknown>;
    if (response.ok === true) {
        try {
            const result = rawJsonResult(response.result, "Cdb query result");
            return { ok: true, result };
        } catch {
            return mutationFailure("CDB_INVARIANT", "Cdb returned a non-JSON query result");
        }
    }
    if (response.ok === false) {
        const projected = projectCdbMutationResponse(response);
        return projected.ok ? mutationFailure("CDB_INVARIANT", "Cdb returned a malformed query failure") : projected;
    }
    return mutationFailure("CDB_INVARIANT", "Cdb returned a malformed query response");
}

type CdbQueryRowsResponse =
    | { readonly ok: true; readonly result: readonly RawJson[] }
    | Extract<CdbQueryResponse, { readonly ok: false }>;

export function projectCdbQueryRows(value: unknown): CdbQueryRowsResponse {
    const response = projectCdbQueryResponse(value);
    if (!response.ok) return response;
    if (!Array.isArray(response.result)) {
        return mutationFailure("CDB_INVARIANT", "organization query result must be an array");
    }
    return { ok: true, result: response.result };
}

export function isTerminalRegisteredQueryFailure(code: string): boolean {
    return isCdbErrorCode(code) && !isRetryable(code);
}

type OrganizationAuthFailure = {
    readonly ok: false;
    readonly code: "CDB_FORBIDDEN" | "CDB_CATALOG_UNAVAILABLE" | "CDB_STALE_EPOCH";
    readonly message: string;
};

type OrganizationAuthProjection =
    | { readonly ok: true; readonly auth: AuthCtx; readonly recoveryGeneration: number }
    | OrganizationAuthFailure;

type UserAuthProjection = OrganizationAuthProjection;

function catalogBoundaryFailure(error: unknown, message: string): OrganizationAuthFailure {
    if (isCdbError(error) && error.code === "CDB_STALE_EPOCH") {
        return { ok: false, code: error.code, message: error.message };
    }
    return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message };
}

function ownEnumerableData(value: object, key: string): unknown {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor?.enumerable === true && "value" in descriptor ? descriptor.value : undefined;
}

/** Validate the Catalog user envelope before it becomes runtime auth. */
export function projectUserMutationAuth(
    value: unknown,
    expected: { readonly principalId: PrincipalId }
): UserAuthProjection {
    if (value === null) {
        return { ok: false, code: "CDB_FORBIDDEN", message: "user is missing or revoked" };
    }
    if (typeof value !== "object" || Array.isArray(value)) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed user authority" };
    }
    const authority = value as Record<string, unknown>;
    const roles = authority.roles;
    const epochs = authority.authEpochs;
    const recoveryGeneration = ownEnumerableData(authority, "recoveryGeneration");
    if (
        typeof authority.principalId !== "string" ||
        typeof authority.role !== "string" ||
        !Number.isSafeInteger(recoveryGeneration) ||
        (recoveryGeneration as number) < 0 ||
        !Array.isArray(roles) ||
        !roles.every(role => typeof role === "string") ||
        typeof epochs !== "object" ||
        epochs === null ||
        Array.isArray(epochs)
    ) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed user authority" };
    }
    const epochRecord = epochs as Record<string, unknown>;
    if (
        ![epochRecord.global, epochRecord.tenant, epochRecord.principal].every(
            epoch => typeof epoch === "number" && Number.isSafeInteger(epoch) && epoch >= 0
        ) ||
        epochRecord.tenant !== 0
    ) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed auth epochs" };
    }
    if (authority.principalId !== expected.principalId || authority.role.length === 0 || roles.length === 0) {
        return { ok: false, code: "CDB_FORBIDDEN", message: "user is missing or revoked" };
    }
    if (roles.some(role => role.length === 0) || authority.role !== roles.join(",")) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned inconsistent user roles" };
    }
    return {
        ok: true,
        recoveryGeneration: recoveryGeneration as number,
        auth: {
            userId: authority.principalId,
            role: authority.role,
            roles: [...roles],
            authEpochs: {
                global: epochRecord.global as number,
                tenant: 0,
                principal: epochRecord.principal as number,
            },
            claims: {},
        },
    };
}

/** Validate the Catalog authority envelope before it becomes mutation auth. */
export function projectOrganizationMutationAuth(
    value: unknown,
    expected: { readonly principalId: PrincipalId; readonly organizationId: TenantId }
): OrganizationAuthProjection {
    if (value === null) {
        return { ok: false, code: "CDB_FORBIDDEN", message: "organization membership is missing or revoked" };
    }
    if (typeof value !== "object" || Array.isArray(value)) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed authority" };
    }
    const authority = value as Record<string, unknown>;
    const roles = authority.roles;
    const userRole = authority.userRole;
    const epochs = authority.authEpochs;
    const recoveryGeneration = ownEnumerableData(authority, "recoveryGeneration");
    if (
        typeof authority.principalId !== "string" ||
        typeof authority.organizationId !== "string" ||
        typeof authority.role !== "string" ||
        !Number.isSafeInteger(recoveryGeneration) ||
        (recoveryGeneration as number) < 0 ||
        !Array.isArray(roles) ||
        !roles.every(role => typeof role === "string") ||
        (userRole !== undefined && (typeof userRole !== "string" || userRole.length === 0)) ||
        typeof epochs !== "object" ||
        epochs === null ||
        Array.isArray(epochs)
    ) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed authority" };
    }
    const epochRecord = epochs as Record<string, unknown>;
    if (
        ![epochRecord.global, epochRecord.tenant, epochRecord.principal].every(
            epoch => typeof epoch === "number" && Number.isSafeInteger(epoch) && epoch >= 0
        )
    ) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed auth epochs" };
    }
    if (
        authority.principalId !== expected.principalId ||
        authority.organizationId !== expected.organizationId ||
        authority.role.length === 0 ||
        roles.length === 0
    ) {
        return { ok: false, code: "CDB_FORBIDDEN", message: "organization membership is missing or revoked" };
    }
    if (roles.some(role => role.length === 0) || authority.role !== roles.join(",")) {
        return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned inconsistent roles" };
    }
    return {
        ok: true,
        recoveryGeneration: recoveryGeneration as number,
        auth: {
            userId: authority.principalId,
            tenantId: authority.organizationId,
            role: authority.role,
            roles: [...roles],
            authEpochs: {
                global: epochRecord.global as number,
                tenant: epochRecord.tenant as number,
                principal: epochRecord.principal as number,
            },
            // Older Catalog envelopes omit userRole and retain membership-only
            // authorization. Current Catalogs source it from the Better Auth
            // user row; the principal epoch invalidates changes to that row.
            claims: userRole === undefined ? {} : { userRole },
        },
    };
}

export async function resolvePartitionAuth(
    catalog: CatalogOrganizationAuthorityRpc & Partial<CatalogUserAuthorityRpc>,
    authority: MutationAuthority,
    principalId: PrincipalId,
    partitionKey: string
): Promise<OrganizationAuthProjection> {
    if (authority === "user" || authority === "global") {
        if (authority === "user" && partitionKey !== principalId) {
            return { ok: false, code: "CDB_FORBIDDEN", message: "user partition does not match the verified subject" };
        }
        if (!catalog.resolveUserAuthority) {
            return {
                ok: false,
                code: "CDB_CATALOG_UNAVAILABLE",
                message: "Catalog user authority RPC is unavailable",
            };
        }
        try {
            return projectUserMutationAuth(await catalog.resolveUserAuthority({ principalId }), { principalId });
        } catch (error) {
            return catalogBoundaryFailure(error, "Catalog user authority RPC failed");
        }
    }
    try {
        const organizationId = TenantId(partitionKey);
        return projectOrganizationMutationAuth(
            await catalog.resolveOrganizationAuthority({ principalId, organizationId }),
            { principalId, organizationId }
        );
    } catch (error) {
        return catalogBoundaryFailure(error, "Catalog organization authority RPC failed");
    }
}

type PartitionAuthRouteProjection =
    | {
          readonly ok: true;
          readonly auth: AuthCtx;
          readonly route: Awaited<ReturnType<CatalogMutationRpc["route"]>>;
      }
    | Extract<OrganizationAuthProjection, { readonly ok: false }>;

export function isCatalogRouteResult(value: unknown): value is Awaited<ReturnType<CatalogMutationRpc["route"]>> {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
    const route = value as Record<string, unknown>;
    const recoveryGeneration = ownEnumerableData(route, "recoveryGeneration");
    return (
        typeof route.shardId === "string" &&
        route.shardId.length > 0 &&
        Number.isSafeInteger(route.schemaEpoch) &&
        (route.schemaEpoch as number) >= 0 &&
        Number.isSafeInteger(route.domainSchemaEpoch) &&
        (route.domainSchemaEpoch as number) >= 1 &&
        Number.isSafeInteger(recoveryGeneration) &&
        (recoveryGeneration as number) >= 0
    );
}

/** Resolve fresh authority and placement, using one Catalog RPC for organizations. */
export async function resolvePartitionAuthRoute(
    catalog: CatalogMutationRpc &
        CatalogOrganizationAuthorityRpc &
        Partial<CatalogOrganizationAuthorityRouteRpc & CatalogUserAuthorityRpc>,
    authority: MutationAuthority,
    principalId: PrincipalId,
    partitionKey: string,
    vshard: number
): Promise<PartitionAuthRouteProjection> {
    if (authority === "organization" && catalog.resolveOrganizationAuthorityRoute) {
        const organizationId = TenantId(partitionKey);
        let resolved: unknown;
        try {
            resolved = await catalog.resolveOrganizationAuthorityRoute({ principalId, organizationId, vshard });
        } catch (error) {
            return catalogBoundaryFailure(error, "Catalog organization authority and routing RPC failed");
        }
        if (typeof resolved !== "object" || resolved === null || Array.isArray(resolved)) {
            return { ok: false, code: "CDB_CATALOG_UNAVAILABLE", message: "Catalog returned malformed authority" };
        }
        const record = resolved as Record<string, unknown>;
        const projected = projectOrganizationMutationAuth(ownEnumerableData(record, "authority"), {
            principalId,
            organizationId,
        });
        if (!projected.ok) return projected;
        const route = ownEnumerableData(record, "route");
        if (!isCatalogRouteResult(route)) {
            return {
                ok: false,
                code: "CDB_CATALOG_UNAVAILABLE",
                message: "Catalog returned a malformed shard route",
            };
        }
        if (projected.recoveryGeneration !== route.recoveryGeneration) {
            return { ok: false, code: "CDB_STALE_EPOCH", message: "Catalog authority and route generations differ" };
        }
        return { ok: true, auth: projected.auth, route };
    }

    const projected = await resolvePartitionAuth(catalog, authority, principalId, partitionKey);
    if (!projected.ok) return projected;
    let route: unknown;
    try {
        route = await catalog.route(vshard);
    } catch (error) {
        return catalogBoundaryFailure(error, "Catalog routing RPC failed");
    }
    if (!isCatalogRouteResult(route)) {
        return {
            ok: false,
            code: "CDB_CATALOG_UNAVAILABLE",
            message: "Catalog returned a malformed shard route",
        };
    }
    if (projected.recoveryGeneration !== route.recoveryGeneration) {
        return { ok: false, code: "CDB_STALE_EPOCH", message: "Catalog authority and route generations differ" };
    }
    return { ok: true, auth: projected.auth, route };
}

/** Verify and project a JWT into the only attachment shape trusted by Gateway handlers. */
export async function verifyGatewayJwt(request: GatewayJwtVerificationRequest): Promise<VerifiedGwAttachment> {
    const issuer = request.config.issuer ?? request.authOrigin;
    const audience = request.config.audience ?? request.authOrigin;
    const jwksUrl =
        request.config.jwksUrl ??
        new URL(
            `${request.config.authBasePath.replace(/\/$/, "")}${request.config.jwksPath}`,
            `${request.authOrigin}/`
        ).toString();
    const resolver = request.catalog.resolveJwk
        ? createCatalogOwnedJwksResolver(
              { resolveJwk: value => (request.catalog.resolveJwk as NonNullable<CatalogJwksRpc["resolveJwk"]>)(value) },
              jwksUrl
          )
        : createCatalogJwksResolver({ catalog: request.catalog, jwksUrl });
    const tolerance = request.config.clockToleranceSeconds ?? DEFAULT_JWT_CLOCK_TOLERANCE_SECONDS;
    const claims = await verifyJwt(request.jwt, {
        resolver,
        issuer,
        audience,
        algorithms: request.config.algorithms,
        clockToleranceSeconds: tolerance,
    });
    if (typeof claims.sub !== "string" || typeof claims.exp !== "number") {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verified JWT is missing subject or expiry" });
    }
    return {
        kind: "verified",
        connectionId: request.connectionId,
        authOrigin: request.authOrigin,
        clientId: request.clientId,
        ...(request.lastCookie !== undefined ? { lastCookie: request.lastCookie } : {}),
        principalId: PrincipalId(claims.sub),
        jwtExp: claims.exp + tolerance,
        ...(typeof claims.nbf === "number" ? { jwtNbf: claims.nbf - tolerance } : {}),
    };
}

export function isVerifiedAttachment(attachment: { readonly kind: string } | null): attachment is VerifiedGwAttachment {
    return attachment?.kind === "verified";
}

/** Recheck time validity before every protected operation. */
export function isCurrentVerifiedAttachment(
    attachment: VerifiedGwAttachment,
    nowSeconds: number = Math.floor(Date.now() / 1000)
): boolean {
    if (attachment.jwtExp <= nowSeconds) return false;
    if (attachment.jwtNbf !== undefined && attachment.jwtNbf > nowSeconds) return false;
    return true;
}

/** Project the verified subject into the mutation dispatcher input. */
export function trustedMutationAuthFromAttachment(attachment: VerifiedGwAttachment): TrustedMutationAuth {
    return { principalId: attachment.principalId };
}

export async function dispatchTrustedMutation(
    deps: TrustedMutationDispatchDeps,
    request: TrustedMutationDispatchRequest
): Promise<CdbMutationResponse> {
    const principalId = request.principalId;
    const mutId = request.mutId;
    const ref = request.ref;
    let rawArgs: RawJson;
    try {
        rawArgs = snapshotCdbMutationArgs(request.args);
    } catch (error) {
        return mutationFailure(
            error instanceof CdbError ? error.code : "CDB_INVARIANT",
            error instanceof Error ? error.message : "mutation argument sizing failed"
        );
    }
    let routed: import("../rpc.ts").MutationRouteResponse;
    try {
        routed = deps.routeMutation({ ref, args: rawArgs });
    } catch {
        return mutationFailure("CDB_INVARIANT", "local mutation routing failed");
    }
    if (!routed.ok) return routed;
    const routeAuthority = routed.authority;
    const partitionKey = routed.partitionKey;
    const vshard = routed.vshard;
    let routedArgs: RawJson;
    try {
        routedArgs = snapshotCdbMutationArgs(routed.args);
    } catch (error) {
        return mutationFailure(
            error instanceof CdbError ? error.code : "CDB_INVARIANT",
            error instanceof Error ? error.message : "routed mutation argument sizing failed"
        );
    }
    if (!Number.isSafeInteger(vshard) || vshard < 0 || vshard >= VSHARD_COUNT) {
        return mutationFailure("CDB_INVARIANT", "local mutation routing returned an invalid vshard");
    }
    if (routeAuthority !== "organization" && routeAuthority !== "user" && routeAuthority !== "global") {
        return mutationFailure("CDB_AUTH_NOT_BOUND", "mutation has no declared authority");
    }
    if (typeof partitionKey !== "string" || partitionKey.length === 0) {
        return mutationFailure("CDB_INVALID_ARGS", `${routeAuthority} mutation has no partition key`);
    }

    if (routeAuthority === "user" && partitionKey !== principalId) {
        return mutationFailure("CDB_FORBIDDEN", "user mutation partition does not match the verified subject");
    }

    for (let attempt = 0; attempt < 2; attempt += 1) {
        const projected = await resolvePartitionAuthRoute(
            deps.catalog,
            routeAuthority,
            principalId,
            partitionKey,
            vshard
        );
        if (!projected.ok) return mutationFailure(projected.code, projected.message);
        const location = projected.route;

        let response: CdbMutationResponse;
        try {
            response = projectCdbMutationResponse(
                await deps.cdb(location.shardId).mutate({
                    principalId,
                    mutId,
                    ref,
                    args: snapshotCdbMutationArgs(routedArgs),
                    placement: { authority: routeAuthority, partitionKey },
                    auth: projected.auth,
                    schemaEpoch: location.schemaEpoch,
                    recoveryGeneration: location.recoveryGeneration,
                    domainSchemaEpoch: location.domainSchemaEpoch,
                })
            );
        } catch {
            return mutationFailure("CDB_SHARD_UNAVAILABLE", "Cdb mutation RPC failed");
        }
        if (response.ok || response.error.code !== "CDB_STALE_EPOCH" || attempt === 1) return response;
    }
    return mutationFailure("CDB_INVARIANT", "mutation stale-route retry completed without a result");
}

export async function dispatchTrustedQuery(
    deps: TrustedQueryDispatchDeps,
    request: TrustedQueryDispatchRequest
): Promise<CdbQueryResponse> {
    const principalId = request.principalId;
    const ref = request.ref;
    let rawArgs: RawJson;
    try {
        rawArgs = snapshotCdbQueryArgs(request.args);
    } catch (error) {
        return mutationFailure(
            error instanceof CdbError ? error.code : "CDB_INVARIANT",
            error instanceof Error ? error.message : "query argument sizing failed"
        );
    }

    let routedResult: QueryRouteResponse;
    try {
        routedResult = await deps.routeQuery({ ref, args: rawArgs });
    } catch {
        return mutationFailure("CDB_INVARIANT", "local query routing failed");
    }
    if (!routedResult.ok) return routedResult;

    let routed: Extract<QueryRouteResponse, { readonly ok: true }>;
    try {
        routed = { ...routedResult, args: snapshotCdbQueryArgs(routedResult.args) };
    } catch (error) {
        return mutationFailure(
            error instanceof CdbError ? error.code : "CDB_INVARIANT",
            error instanceof Error ? error.message : "routed query argument sizing failed"
        );
    }
    if (routed.authority !== "organization" && routed.authority !== "user" && routed.authority !== "global") {
        return mutationFailure("CDB_AUTH_NOT_BOUND", "query has no declared authority");
    }
    const partitionKey = routed.partitionKey;
    const partition = routed.intent.partitionKey;
    if (
        !partitionKey ||
        !partition ||
        partition.values.length === 0 ||
        routed.intent.joinShape === "cross-partition" ||
        !partition.values.every(value => typeof value === "string" && value === partitionKey)
    ) {
        return mutationFailure("CDB_CROSS_PARTITION", "query intent is not bound to one declared partition");
    }
    if (routed.authority === "user" && partitionKey !== principalId) {
        return mutationFailure("CDB_FORBIDDEN", "user query partition does not match the verified subject");
    }
    const vshard = Number(vshardOf([partitionKey]));

    for (let attempt = 0; attempt < 2; attempt += 1) {
        const projected = await resolvePartitionAuthRoute(
            deps.catalog,
            routed.authority,
            principalId,
            partitionKey,
            vshard
        );
        if (!projected.ok) return mutationFailure(projected.code, projected.message);
        const location = projected.route;

        let response: CdbQueryResponse;
        try {
            response = projectCdbQueryResponse(
                await deps.cdb(location.shardId).query({
                    ref: ref as ChardbRef,
                    args: snapshotCdbQueryArgs(routed.args),
                    placement: { authority: routed.authority, partitionKey },
                    auth: projected.auth,
                    schemaEpoch: location.schemaEpoch,
                    recoveryGeneration: location.recoveryGeneration,
                    domainSchemaEpoch: location.domainSchemaEpoch,
                })
            );
        } catch {
            return mutationFailure("CDB_SHARD_UNAVAILABLE", "Cdb query RPC failed");
        }
        if (response.ok || response.error.code !== "CDB_STALE_EPOCH" || attempt === 1) return response;
    }
    return mutationFailure("CDB_INVARIANT", "query stale-route retry completed without a result");
}
