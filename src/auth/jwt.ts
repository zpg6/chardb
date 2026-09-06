import { type JWK, decodeJwt, decodeProtectedHeader, importJWK, errors as joseErrors, jwtVerify } from "jose";
import { CdbError } from "../errors.ts";

export const DEFAULT_JWT_CLOCK_TOLERANCE_SECONDS = 30;

export interface JwtClaims {
    /** Subject — the principal the token represents. */
    readonly sub?: string;
    /** Issuer. */
    readonly iss?: string;
    /** Audience(s). */
    readonly aud?: string | readonly string[];
    /** Expiry, seconds since the epoch. */
    readonly exp?: number;
    /** Issued-at, seconds since the epoch. */
    readonly iat?: number;
    /** JWT ID (anti-replay). */
    readonly jti?: string;
    /** Custom claims kept opaque — typed `unknown` so callers must narrow. */
    readonly [k: string]: unknown;
}

/**
 * Resolver function the Gateway hands `verifyJwt`. Receives the JWT
 * `kid` and returns a JWK to verify against — typically backed by the
 * Catalog DO's `catalog_jwks` SWR cache (see `jwks_cache.ts`). Returning
 * `null` causes verification to fail with `CDB_FORBIDDEN`.
 */
export type JwksResolver = (kid: string) => Promise<JWK | null> | JWK | null;

export interface VerifyJwtOptions {
    readonly resolver: JwksResolver;
    /**
     * Expected `iss` claim. If unset, the issuer field is not checked
     * (useful for tests; the production Gateway always pins it to the
     * better-auth `baseURL` of the deployment).
     */
    readonly issuer?: string;
    /** Expected `aud` claim. Same opt-out semantics as `issuer`. */
    readonly audience?: string | readonly string[];
    /** Explicit algorithm allow-list derived from the Better Auth JWT plugin. */
    readonly algorithms: readonly string[];
    /** Allowed clock skew in seconds. Defaults to 30. */
    readonly clockToleranceSeconds?: number;
}

/**
 * Verify a JWT against a JWKS resolver and return its claims.
 *
 * Wraps `jose.jwtVerify` with chardb-shaped error mapping: any
 * verification failure surfaces as `CDB_FORBIDDEN` so callers (the
 * Gateway, the entrypoint) get a single locked error code to react
 * to. Unknown `kid`s yield the same code — the typical recovery is a
 * JWKS refresh + retry.
 */
export async function verifyJwt(jwt: string, opts: VerifyJwtOptions): Promise<JwtClaims> {
    const tolerance = opts.clockToleranceSeconds ?? DEFAULT_JWT_CLOCK_TOLERANCE_SECONDS;
    if (!Number.isFinite(tolerance) || tolerance < 0) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: invalid clock tolerance" });
    }
    let header: ReturnType<typeof decodeProtectedHeader>;
    let claims: ReturnType<typeof decodeJwt>;
    try {
        header = decodeProtectedHeader(jwt);
        claims = decodeJwt(jwt);
    } catch (cause) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: malformed JWT", cause });
    }
    if (typeof header.alg !== "string" || !opts.algorithms.includes(header.alg)) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: disallowed JWT algorithm" });
    }
    if (
        typeof claims.sub !== "string" ||
        claims.sub.length === 0 ||
        typeof claims.exp !== "number" ||
        !Number.isFinite(claims.exp) ||
        claims.exp <= Math.floor(Date.now() / 1000) - tolerance ||
        (claims.nbf !== undefined && !Number.isFinite(claims.nbf)) ||
        (claims.iat !== undefined && !Number.isFinite(claims.iat))
    ) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: invalid or expired JWT claims" });
    }
    const jwk = await opts.resolver(typeof header.kid === "string" ? header.kid : "");
    if (!jwk) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: no matching JWK" });
    }
    if (
        (jwk.alg !== undefined && jwk.alg !== header.alg) ||
        (jwk.use !== undefined && jwk.use !== "sig") ||
        (jwk.key_ops !== undefined && !jwk.key_ops.includes("verify"))
    ) {
        throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: JWK does not allow this signature" });
    }
    try {
        const key = await importJWK(jwk, header.alg);
        const audience: string | string[] | undefined =
            typeof opts.audience === "string"
                ? opts.audience
                : opts.audience !== undefined
                  ? [...opts.audience]
                  : undefined;
        const { payload } = await jwtVerify(jwt, key, {
            ...(opts.issuer ? { issuer: opts.issuer } : {}),
            ...(audience ? { audience } : {}),
            algorithms: [...opts.algorithms],
            requiredClaims: ["sub", "exp"],
            clockTolerance: tolerance,
        });
        if (typeof payload.sub !== "string" || payload.sub.length === 0) {
            throw new CdbError({ code: "CDB_FORBIDDEN", message: "verifyJwt: missing subject" });
        }
        return payload as JwtClaims;
    } catch (cause) {
        const message =
            cause instanceof joseErrors.JOSEError ? cause.message : "verifyJwt: signature verification failed";
        throw new CdbError({ code: "CDB_FORBIDDEN", message, cause });
    }
}
