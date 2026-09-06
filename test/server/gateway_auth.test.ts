import { describe, expect, test } from "bun:test";
import type { BetterAuthOptions } from "better-auth";
import { jwt } from "better-auth/plugins/jwt";
import { CompactSign, SignJWT, exportJWK, generateKeyPair } from "jose";
import { gatewayJwtConfigFromAuthOptions } from "../../src/server/chardb.ts";
import {
    type GatewayJwtConfig,
    isCurrentVerifiedAttachment,
    trustedMutationAuthFromAttachment,
    verifyGatewayJwt,
} from "../../src/server/do/gateway-auth-dispatch.ts";
import { routedClientIdFromUrl } from "../../src/server/do/gateway.ts";
import { ClientId, PrincipalId } from "../../src/types.ts";

const ORIGIN = "https://app.example";
const ISSUER = "https://issuer.example";
const AUDIENCE = "chardb-app";
const KID = "key-1";
const CONNECTION_ID = "connection-1";

async function signingFixture(
    keyOverrides: {
        readonly alg?: string | undefined;
        readonly use?: string | undefined;
        readonly key_ops?: readonly string[];
    } = {}
) {
    const { privateKey, publicKey } = await generateKeyPair("ES256");
    const jwk = await exportJWK(publicKey);
    const catalog = {
        async getJwk(kid: string) {
            return kid === KID
                ? {
                      jwkJson: JSON.stringify({ ...jwk, kid: KID, alg: "ES256", use: "sig", ...keyOverrides }),
                      expiresAt: Date.now() + 60_000,
                  }
                : null;
        },
        async putJwk() {},
        async route() {
            throw new Error("not used");
        },
    };
    const sign = async (
        overrides: {
            subject?: string | null;
            issuer?: string;
            audience?: string | string[];
            expirationTime?: number | null;
            notBefore?: number;
        } = {}
    ) => {
        const now = Math.floor(Date.now() / 1000);
        let builder = new SignJWT({ plan: "pro" })
            .setProtectedHeader({ alg: "ES256", kid: KID })
            .setIssuer(overrides.issuer ?? ISSUER)
            .setAudience(overrides.audience ?? AUDIENCE)
            .setIssuedAt(now);
        if (overrides.subject !== null) builder = builder.setSubject(overrides.subject ?? "user-1");
        if (overrides.expirationTime !== null) {
            builder = builder.setExpirationTime(overrides.expirationTime ?? now + 300);
        }
        if (overrides.notBefore !== undefined) builder = builder.setNotBefore(overrides.notBefore);
        return builder.sign(privateKey);
    };
    return { catalog, sign, privateKey };
}

function config(overrides: Partial<GatewayJwtConfig> = {}): GatewayJwtConfig {
    return {
        issuer: ISSUER,
        audience: AUDIENCE,
        algorithms: ["ES256"],
        jwksUrl: "https://issuer.example/jwks",
        authBasePath: "/api/auth",
        jwksPath: "/jwks",
        clockToleranceSeconds: 0,
        ...overrides,
    };
}

describe("Gateway verified JWT boundary", () => {
    test("accepts one bounded routed client id and rejects missing or malformed routes", () => {
        expect(routedClientIdFromUrl("https://app.example/ws?clientId=client-1")).toBe(ClientId("client-1"));
        for (const url of [
            "https://app.example/ws",
            "https://app.example/ws?clientId=",
            "https://app.example/ws?clientId=%20client-1",
            "https://app.example/ws?clientId=client%00one",
            "https://app.example/ws?clientId=client-1&clientId=client-2",
            `https://app.example/ws?clientId=${"x".repeat(257)}`,
        ]) {
            expect(routedClientIdFromUrl(url)).toBeNull();
        }
    });

    test("attaches only signature-verified identity and expiry", async () => {
        const { catalog, sign } = await signingFixture();
        const attachment = await verifyGatewayJwt({
            config: config(),
            authOrigin: ORIGIN,
            connectionId: CONNECTION_ID,
            catalog,
            jwt: await sign(),
            clientId: ClientId("client-1"),
        });

        expect(attachment).toMatchObject({
            kind: "verified",
            connectionId: CONNECTION_ID,
            authOrigin: ORIGIN,
            clientId: "client-1",
            principalId: "user-1",
        });
        expect(attachment.jwtExp).toBeGreaterThan(Math.floor(Date.now() / 1000));
        expect("tenantId" in attachment).toBe(false);
        expect("role" in attachment).toBe(false);
    });

    test("rejects malformed, tampered, expired, premature, wrong-issuer, wrong-audience, and disallowed-alg tokens", async () => {
        const { catalog, sign } = await signingFixture();
        const now = Math.floor(Date.now() / 1000);
        const valid = await sign();
        const cases = [
            "not-a-jwt",
            `${valid.slice(0, -2)}xx`,
            await sign({ expirationTime: now - 1 }),
            await sign({ notBefore: now + 60 }),
            await sign({ issuer: "https://attacker.example" }),
            await sign({ audience: "other-app" }),
        ];
        for (const token of cases) {
            await expect(
                verifyGatewayJwt({
                    config: config(),
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt: token,
                    clientId: ClientId("client-1"),
                })
            ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        }
        await expect(
            verifyGatewayJwt({
                config: config({ algorithms: ["RS256"] }),
                authOrigin: ORIGIN,
                connectionId: CONNECTION_ID,
                catalog,
                jwt: valid,
                clientId: ClientId("client-1"),
            })
        ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
    });

    test("rejects signed tokens that omit subject or expiry", async () => {
        const { catalog, sign } = await signingFixture();
        for (const jwt of [await sign({ subject: null }), await sign({ expirationTime: null })]) {
            await expect(
                verifyGatewayJwt({
                    config: config(),
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt,
                    clientId: ClientId("client-1"),
                })
            ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        }
    });

    test.each([{ use: "enc" }, { alg: "ES384" }, { key_ops: ["sign"] }])(
        "rejects a signing key with incompatible metadata %j",
        async metadata => {
            const { catalog, sign } = await signingFixture(metadata);
            await expect(
                verifyGatewayJwt({
                    config: config(),
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt: await sign(),
                    clientId: ClientId("client-1"),
                })
            ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        }
    );

    test.each([{ alg: undefined, use: undefined }, { key_ops: ["verify"] }])(
        "accepts a signing key with compatible optional metadata %j",
        async metadata => {
            const { catalog, sign } = await signingFixture(metadata);
            await expect(
                verifyGatewayJwt({
                    config: config(),
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt: await sign(),
                    clientId: ClientId("client-1"),
                })
            ).resolves.toMatchObject({ principalId: "user-1" });
        }
    );

    test("rejects disallowed algorithms before Catalog lookup", async () => {
        const { catalog, sign } = await signingFixture();
        let lookups = 0;
        catalog.getJwk = async () => {
            lookups += 1;
            throw new Error("Catalog unavailable");
        };
        await expect(
            verifyGatewayJwt({
                config: config({ algorithms: ["RS256"] }),
                authOrigin: ORIGIN,
                connectionId: CONNECTION_ID,
                catalog,
                jwt: await sign(),
                clientId: ClientId("client-1"),
            })
        ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        expect(lookups).toBe(0);
    });

    test.each(['"exp":1e400', '"exp":9999999999,"nbf":-1e400', '"exp":9999999999,"iat":1e400'])(
        "rejects signed nonfinite time claims %s",
        async timeClaims => {
            const { catalog, privateKey } = await signingFixture();
            const payload = `{"sub":"user-1","iss":"${ISSUER}","aud":"${AUDIENCE}",${timeClaims}}`;
            const token = await new CompactSign(new TextEncoder().encode(payload))
                .setProtectedHeader({ alg: "ES256", kid: KID })
                .sign(privateKey);
            await expect(
                verifyGatewayJwt({
                    config: config(),
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt: token,
                    clientId: ClientId("client-1"),
                })
            ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        }
    );

    test.each([undefined, 30, 10, 0])(
        "uses clock tolerance %s for verification and operation boundaries",
        async tolerance => {
            const { catalog, sign } = await signingFixture();
            const now = Math.floor(Date.now() / 1000);
            const jwtConfig = config();
            const { clockToleranceSeconds: _, ...withoutTolerance } = jwtConfig;
            const configured =
                tolerance === undefined ? withoutTolerance : config({ clockToleranceSeconds: tolerance });
            const verify = (token: string) =>
                verifyGatewayJwt({
                    config: configured,
                    authOrigin: ORIGIN,
                    connectionId: CONNECTION_ID,
                    catalog,
                    jwt: token,
                    clientId: ClientId("client-1"),
                });
            const expired = await sign({ expirationTime: now - 5 });
            const premature = await sign({ notBefore: now + 5 });
            if (tolerance === 0) {
                await expect(verify(expired)).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
                await expect(verify(premature)).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
            } else {
                const effectiveTolerance = tolerance ?? 30;
                const attachment = await verify(expired);
                expect(isCurrentVerifiedAttachment(attachment, now)).toBe(true);
                expect(isCurrentVerifiedAttachment(attachment, now - 6 + effectiveTolerance)).toBe(true);
                expect(isCurrentVerifiedAttachment(attachment, now - 5 + effectiveTolerance)).toBe(false);
                const early = await verify(premature);
                expect(isCurrentVerifiedAttachment(early, now + 4 - effectiveTolerance)).toBe(false);
                expect(isCurrentVerifiedAttachment(early, now + 5 - effectiveTolerance)).toBe(true);
            }
            await expect(verify(await sign({ expirationTime: now - 30 }))).rejects.toMatchObject({
                code: "CDB_FORBIDDEN",
            });
            await expect(verify(await sign({ notBefore: now + 60 }))).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
        }
    );

    test.each([-1, Number.NaN, Number.POSITIVE_INFINITY])("rejects invalid clock tolerance %s", async tolerance => {
        const { catalog, sign } = await signingFixture();
        await expect(
            verifyGatewayJwt({
                config: config({ clockToleranceSeconds: tolerance }),
                authOrigin: ORIGIN,
                connectionId: CONNECTION_ID,
                catalog,
                jwt: await sign(),
                clientId: ClientId("client-1"),
            })
        ).rejects.toMatchObject({ code: "CDB_FORBIDDEN" });
    });

    test("accepts a token audience array when one value matches", async () => {
        const { catalog, sign } = await signingFixture();
        await expect(
            verifyGatewayJwt({
                config: config(),
                authOrigin: ORIGIN,
                connectionId: CONNECTION_ID,
                catalog,
                jwt: await sign({ audience: ["another-service", AUDIENCE] }),
                clientId: ClientId("client-1"),
            })
        ).resolves.toMatchObject({ principalId: "user-1" });
    });

    test("accepts configured audience arrays when one expected value matches", async () => {
        const { catalog, sign } = await signingFixture();
        await expect(
            verifyGatewayJwt({
                config: config({ audience: ["chardb-admin", AUDIENCE] }),
                authOrigin: ORIGIN,
                connectionId: CONNECTION_ID,
                catalog,
                jwt: await sign(),
                clientId: ClientId("client-1"),
            })
        ).resolves.toMatchObject({ principalId: "user-1" });
    });

    test("rechecks expiry and projects only the verified subject for mutation dispatch", async () => {
        const { catalog, sign } = await signingFixture();
        const attachment = await verifyGatewayJwt({
            config: config(),
            authOrigin: ORIGIN,
            connectionId: CONNECTION_ID,
            catalog,
            jwt: await sign(),
            clientId: ClientId("client-1"),
        });
        expect(isCurrentVerifiedAttachment(attachment, attachment.jwtExp - 1)).toBe(true);
        expect(isCurrentVerifiedAttachment(attachment, attachment.jwtExp)).toBe(false);
        expect(trustedMutationAuthFromAttachment(attachment)).toEqual({ principalId: PrincipalId("user-1") });
    });
});

describe("Better Auth JWT configuration", () => {
    test("pins issuer, audience, algorithm, and remote JWKS from the plugin", () => {
        const options: BetterAuthOptions = {
            baseURL: "https://app.example/path",
            plugins: [
                jwt({
                    jwt: { issuer: ISSUER, audience: [AUDIENCE, "chardb-admin"] },
                    jwks: {
                        remoteUrl: "https://issuer.example/.well-known/jwks.json",
                        keyPairConfig: { alg: "ES256" },
                        jwksPath: "/keys",
                    },
                }),
            ],
        };
        expect(gatewayJwtConfigFromAuthOptions(options, "/custom-auth")).toEqual({
            issuer: ISSUER,
            audience: [AUDIENCE, "chardb-admin"],
            algorithms: ["ES256"],
            jwksUrl: "https://issuer.example/.well-known/jwks.json",
            authBasePath: "/custom-auth",
            jwksPath: "/keys",
        });
    });

    test("uses the Better Auth origin and EdDSA defaults, and fails closed without the JWT plugin", () => {
        expect(gatewayJwtConfigFromAuthOptions({ baseURL: "https://app.example/some/path", plugins: [jwt()] })).toEqual(
            {
                issuer: ORIGIN,
                audience: ORIGIN,
                algorithms: ["EdDSA"],
                authBasePath: "/api/auth",
                jwksPath: "/jwks",
            }
        );
        expect(gatewayJwtConfigFromAuthOptions({ plugins: [] })).toBeNull();
    });
});
