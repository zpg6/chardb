import { describe, expect, test } from "bun:test";
import * as clientApi from "../src/index.ts";
import * as serverApi from "../src/server/index.ts";
import * as viteApi from "../src/vite/index.ts";

describe("published API boundary", () => {
    test("root exports only clients and the shared error contract", () => {
        expect(Object.keys(clientApi).sort()).toEqual([
            "CDB_ERROR_CODES",
            "CdbError",
            "client",
            "createChardbClient",
            "docsUrlFor",
            "isCdbError",
            "isRetryable",
        ]);
    });

    test("server exports only the supported application API", () => {
        expect(Object.keys(serverApi).sort()).toEqual([
            "api",
            "chardb",
            "defineAuth",
            "defineMigrations",
            "defineSchemaBaseline",
            "defineSchemaSnapshot",
            "forOrg",
            "forOrgUser",
            "forUser",
            "getChardbAuthEnv",
            "searchVector",
            "vector",
        ]);
    });

    test("vite exports only the browser-safety transform", () => {
        expect(Object.keys(viteApi).sort()).toEqual(["chardb", "default"]);
    });

    test("core publishes four supported entries and the React package bridge", async () => {
        const pkg = (await Bun.file(new URL("../package.json", import.meta.url)).json()) as {
            readonly exports: Record<string, unknown>;
        };
        expect(Object.keys(pkg.exports)).toEqual([".", "./server", "./internal/react", "./files", "./vite"]);
    });
});
