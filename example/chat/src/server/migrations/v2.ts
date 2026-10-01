export const betterAuth17 = Object.freeze({
    version: 2,
    name: "better_auth_1_7",
    statements: [],
    catalogStatements: ['ALTER TABLE "jwks" ADD COLUMN "alg" text', 'ALTER TABLE "jwks" ADD COLUMN "crv" text'],
});

export const existingAuth17 = Object.freeze({
    version: 2,
    name: "better_auth_1_7",
    statements: [],
    catalogStatements: ["SELECT 1"],
});
