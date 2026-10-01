import { describe, expect, test } from "bun:test";
import {
    assertFullTrafficDeployment,
    classifyObsoleteControlPlane,
    newestWranglerVersion,
    parseCloudflarePromotionArgs,
    parsePromotionSecrets,
    retryUntil,
    wranglerInitialDeployArgs,
    wranglerVersionDeployArgs,
    wranglerVersionUploadArgs,
} from "../scripts/run-cloudflare-promotion.mjs";

describe("Cloudflare promotion runner", () => {
    test("parses a bounded secret-safe promotion target", () => {
        expect(
            parseCloudflarePromotionArgs([
                "--tarball",
                "/candidate/core.tgz",
                "--react-tarball",
                "/candidate/react.tgz",
                "--worker",
                "chardb-preview-k",
                "--url",
                "https://chardb-preview-k.example.workers.dev",
                "--output",
                "/evidence",
                "--private-dir",
                "/private/run",
                "--migration-prefix",
                "preview-k",
            ])
        ).toEqual({
            help: false,
            tarball: "/candidate/core.tgz",
            reactTarball: "/candidate/react.tgz",
            worker: "chardb-preview-k",
            url: "https://chardb-preview-k.example.workers.dev",
            output: "/evidence",
            privateDir: "/private/run",
            migrationPrefix: "preview-k",
            benchmarkSamples: 3,
            secretsFile: undefined,
            adminTokenFile: undefined,
        });
        expect(() =>
            parseCloudflarePromotionArgs([
                "--tarball",
                "/candidate/core.tgz",
                "--react-tarball",
                "/candidate/react.tgz",
                "--worker",
                "Bad Worker",
                "--url",
                "http://example.com",
                "--output",
                "/same",
                "--private-dir",
                "/same/private",
                "--migration-prefix",
                "x",
            ])
        ).toThrow("Cloudflare Worker name");
        expect(() =>
            parseCloudflarePromotionArgs([
                "--tarball",
                "/candidate/core.tgz",
                "--react-tarball",
                "/candidate/react.tgz",
                "--worker",
                "valid-worker",
                "--url",
                "https://example.com/path",
                "--output",
                "/evidence",
                "--private-dir",
                "/private",
                "--migration-prefix",
                "x",
            ])
        ).toThrow("HTTPS origin");
    });

    test("adopts existing secrets only as a matching pair", () => {
        const authSecret = "a".repeat(32);
        const adminToken = "b".repeat(32);
        expect(
            parsePromotionSecrets(
                `# deployed values\nBETTER_AUTH_SECRET=${authSecret}\nCDB_ADMIN_TOKEN=${adminToken}\n`,
                `${adminToken}\n`
            )
        ).toEqual({ authSecret, adminToken });
        expect(() =>
            parsePromotionSecrets(`BETTER_AUTH_SECRET=${authSecret}\nCDB_ADMIN_TOKEN=${adminToken}`, "wrong")
        ).toThrow("does not match");
        expect(() =>
            parseCloudflarePromotionArgs([
                "--tarball",
                "/candidate/core.tgz",
                "--react-tarball",
                "/candidate/react.tgz",
                "--worker",
                "valid-worker",
                "--url",
                "https://example.com",
                "--output",
                "/evidence",
                "--private-dir",
                "/private",
                "--migration-prefix",
                "x",
                "--secrets-file",
                "/secrets",
            ])
        ).toThrow("must be provided together");
    });

    test("selects only valid monotonically numbered Wrangler versions", () => {
        expect(
            newestWranglerVersion([
                { id: "11111111-1111-1111-1111-111111111111", number: 2 },
                { id: "22222222-2222-2222-2222-222222222222", number: 7 },
            ])
        ).toEqual({ id: "22222222-2222-2222-2222-222222222222", number: 7 });
        expect(() => newestWranglerVersion([])).toThrow("no Worker versions");
        expect(() => newestWranglerVersion([{ id: "nope", number: 1 }])).toThrow("invalid Worker version");
    });

    test("requires one exact version at full traffic", () => {
        expect(
            assertFullTrafficDeployment(
                {
                    id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    versions: [{ version_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", percentage: 100 }],
                },
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            )
        ).toEqual({
            deploymentId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            versionId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            percentage: 100,
        });
        expect(() =>
            assertFullTrafficDeployment(
                {
                    id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    versions: [{ version_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", percentage: 50 }],
                },
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            )
        ).toThrow("does not have 100% traffic");
    });

    test("pins every Wrangler version mutation to the explicit Worker", () => {
        expect(wranglerInitialDeployArgs("worker-k", "/private/secrets", "candidate-v1", "bootstrap v1")).toEqual([
            "deploy",
            "--name",
            "worker-k",
            "--strict",
            "--secrets-file",
            "/private/secrets",
            "--tag",
            "candidate-v1",
            "--message",
            "bootstrap v1",
        ]);
        expect(wranglerVersionUploadArgs("worker-k", "/private/secrets", "candidate-v2", "upload v2")).toEqual([
            "versions",
            "upload",
            "--name",
            "worker-k",
            "--secrets-file",
            "/private/secrets",
            "--tag",
            "candidate-v2",
            "--message",
            "upload v2",
        ]);
        expect(wranglerVersionDeployArgs("worker-k", "version-id", "deploy v2")).toEqual([
            "versions",
            "deploy",
            "version-id@100",
            "--name",
            "worker-k",
            "--message",
            "deploy v2",
            "--yes",
        ]);
    });

    test("waits through transient data-plane propagation", async () => {
        let attempts = 0;
        await expect(
            retryUntil(
                () => {
                    attempts++;
                    if (attempts < 3) throw new Error("old version");
                    return "ready";
                },
                { timeoutMs: 100, intervalMs: 1 }
            )
        ).resolves.toBe("ready");
        expect(attempts).toBe(3);
    });

    test("accepts warm and cold obsolete Durable Object control-plane fences", () => {
        expect(
            classifyObsoleteControlPlane(200, {
                state: { status: "active", activeVersion: 3, activeEpoch: 3 },
            })
        ).toBe("warm-v2-catalog");
        expect(
            classifyObsoleteControlPlane(500, {
                error: "Catalog schema version 3 is newer than packaged version 2",
            })
        ).toBe("cold-v1-journal-fence");
        expect(() => classifyObsoleteControlPlane(200, { state: { activeVersion: 1 } })).toThrow(
            "schema state status drifted"
        );
        expect(() =>
            classifyObsoleteControlPlane(200, { state: { status: "active", activeVersion: 2, activeEpoch: 3 } })
        ).toThrow("activeVersion drifted");
        expect(() =>
            classifyObsoleteControlPlane(500, { error: "Catalog schema version 2 is newer than packaged version 1" })
        ).toThrow("packaged-journal fence");
    });
});
