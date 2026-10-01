import { describe, expect, test } from "bun:test";
import { parsePreviewPrepareArgs, renderPreviewWrangler } from "../scripts/prepare-preview-chat.mjs";

describe("preview deployment preparation", () => {
    test("requires an exact tarball and output while bounding the Worker name", () => {
        expect(
            parsePreviewPrepareArgs([
                "--tarball",
                "artifacts/preview/chardb.tgz",
                "--react-tarball",
                "artifacts/preview/chardb-react.tgz",
                "--output",
                "artifacts/preview/staging-app",
                "--name",
                "chardb-preview-pr-42",
            ])
        ).toEqual({
            help: false,
            tarball: "artifacts/preview/chardb.tgz",
            reactTarball: "artifacts/preview/chardb-react.tgz",
            output: "artifacts/preview/staging-app",
            name: "chardb-preview-pr-42",
        });
        expect(() => parsePreviewPrepareArgs(["--output", "staging-app"])).toThrow("--tarball is required");
        expect(() => parsePreviewPrepareArgs(["--tarball", "chardb.tgz", "--output", "staging-app"])).toThrow(
            "--react-tarball is required"
        );
        expect(() =>
            parsePreviewPrepareArgs([
                "--tarball",
                "chardb.tgz",
                "--react-tarball",
                "react.tgz",
                "--output",
                "staging-app",
                "--name",
                "Bad Name",
            ])
        ).toThrow("Cloudflare Worker name");
    });

    test("changes only the Worker identity in the Wrangler template", () => {
        const source = 'name = "chardb-chat-example"\nmain = "src/server/worker.ts"\n';
        expect(renderPreviewWrangler(source, "chardb-preview", "a".repeat(64))).toBe(
            `name = "chardb-preview"\nmain = "src/server/worker.ts"\n\n[vars]\nCDB_RELEASE_SHA256 = "${"a".repeat(64)}"\n`
        );
        expect(renderPreviewWrangler(source, "chardb-chat-example", "b".repeat(64))).toBe(
            `name = "chardb-chat-example"\nmain = "src/server/worker.ts"\n\n[vars]\nCDB_RELEASE_SHA256 = "${"b".repeat(64)}"\n`
        );
        expect(() => renderPreviewWrangler('main = "worker.ts"\n', "chardb-preview", "a".repeat(64))).toThrow(
            "no Worker name"
        );
        expect(() => renderPreviewWrangler(source, "chardb-preview", "bad")).toThrow("SHA-256");
    });

    test("adds release provenance to existing Worker variables", () => {
        const source =
            'name = "chat"\n[vars]\nCHAT_SCHEMA_HISTORY = "better-auth-1.7"\n\n[assets]\ndirectory = "dist"\n';
        const rendered = renderPreviewWrangler(source, "preview", "c".repeat(64));
        expect(rendered.match(/^\[vars\]$/gm)).toHaveLength(1);
        expect(rendered).toContain(`[vars]\nCDB_RELEASE_SHA256 = "${"c".repeat(64)}"\nCHAT_SCHEMA_HISTORY`);
        expect(rendered).toContain('[assets]\ndirectory = "dist"');
    });
});
