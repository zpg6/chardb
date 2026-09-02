import { describe, expect, test } from "bun:test";
import { attachChardbAuthRuntimeEnv, getChardbAuthEnv } from "../../src/server/auth-runtime-context.ts";
import type { ChardbEnv } from "../../src/server/entrypoint.ts";

describe("Better Auth runtime environment", () => {
    test("keeps the Worker env on the per-instance options consumed by callbacks", () => {
        type AppEnv = ChardbEnv & { readonly EMAIL: { readonly send: () => Promise<void> } };
        const env = {
            CDB_CATALOG: {},
            CDB_SHARD: {},
            CDB_GATEWAY: {},
            EMAIL: { send: async () => undefined },
        } as unknown as AppEnv;
        const options = attachChardbAuthRuntimeEnv({ appName: "test" }, env);

        expect(getChardbAuthEnv<AppEnv>({ context: { options } })).toBe(env);
        expect(getChardbAuthEnv<AppEnv>({ context: { options } }).EMAIL).toBe(env.EMAIL);
    });

    test("rejects contexts not created by chardb", () => {
        expect(() => getChardbAuthEnv(undefined)).toThrow("Chardb auth runtime environment is unavailable");
    });
});
