import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
    parsePreviewUpgradeArgs,
    renderVersionTwoMigrations,
    renderVersionTwoSchema,
} from "../scripts/prepare-preview-upgrade.mjs";

describe("preview upgrade preparation", () => {
    test("parses exact input and output directories", () => {
        expect(parsePreviewUpgradeArgs(["--input", "v1", "--output", "v2"])).toEqual({
            help: false,
            input: "v1",
            output: "v2",
        });
        expect(() => parsePreviewUpgradeArgs(["--input", "v1"])).toThrow("--output is required");
        expect(() => parsePreviewUpgradeArgs(["--wat"])).toThrow("Unknown preview upgrade argument");
    });

    test("adds one nullable column without changing the existing schema text", () => {
        const source = `const table = {\n        createdAt: integer("created_at").notNull(),\n};\n`;
        expect(renderVersionTwoSchema(source)).toBe(
            `const table = {\n        createdAt: integer("created_at").notNull(),\n        editedAt: integer("edited_at"),\n};\n`
        );
        expect(() => renderVersionTwoSchema("const table = {};\n")).toThrow("marker is missing");
        expect(() => renderVersionTwoSchema(renderVersionTwoSchema(source))).toThrow("already contains");
    });

    test("appends one immutable migration after the frozen baseline", () => {
        const source = `import { defineMigrations } from "@chardb/core/server";\nexport const migrations = defineMigrations(migrationInputs);\n`;
        const rendered = renderVersionTwoMigrations(source);
        expect(rendered).toContain("version: 3");
        expect(rendered).toContain('ALTER TABLE "messages" ADD COLUMN "edited_at" integer');
        expect(rendered).toContain("...migrationInputs,");
        expect(() => renderVersionTwoMigrations("export const migrations = [];\n")).toThrow("marker is missing");
    });

    test("prepares the actual chat and preserves both migration histories", async () => {
        const root = path.resolve(import.meta.dir, "..");
        const scratch = await mkdtemp(path.join(tmpdir(), "chardb-preview-upgrade-"));
        try {
            const tarball = path.join(scratch, "core.tgz");
            const reactTarball = path.join(scratch, "react.tgz");
            const input = path.join(scratch, "base");
            const output = path.join(scratch, "upgrade");
            await writeFile(tarball, "test core");
            await writeFile(reactTarball, "test react");
            const run = async (script: string, args: string[]) => {
                const child = Bun.spawn([process.execPath, path.join(root, "scripts", script), ...args], {
                    stdout: "pipe",
                    stderr: "pipe",
                });
                const [code, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()]);
                if (code !== 0) throw new Error(`${script} failed: ${stderr}`);
            };
            await run("prepare-preview-chat.mjs", [
                "--tarball",
                tarball,
                "--react-tarball",
                reactTarball,
                "--output",
                input,
            ]);
            await run("prepare-preview-upgrade.mjs", ["--input", input, "--output", output]);
            const journalPath = path.join("src", "server", "migrations.ts");
            expect(await readFile(path.join(output, journalPath), "utf8")).toBe(
                renderVersionTwoMigrations(await readFile(path.join(input, journalPath), "utf8"))
            );
            const history = path.join("src", "server", "migrations");
            const files = (await readdir(path.join(input, history))).filter(file => file.endsWith(".ts"));
            expect(files).toContain("v1.ts");
            expect(files).toContain("v1-better-auth-17.ts");
            for (const file of files) {
                expect(await readFile(path.join(output, history, file), "utf8")).toBe(
                    await readFile(path.join(input, history, file), "utf8")
                );
            }
            const manifest = JSON.parse(await readFile(path.join(output, "preview-manifest.json"), "utf8"));
            expect(manifest.upgrade.fromVersion).toBe(2);
            expect(manifest.upgrade.toVersion).toBe(3);
            expect(Object.keys(manifest.upgrade.frozenHistory).sort()).toEqual(files.sort());
            const wrangler = await readFile(path.join(input, "wrangler.toml"), "utf8");
            expect(await readFile(path.join(output, "wrangler.toml"), "utf8")).toBe(wrangler);
            expect(wrangler.match(/^\[vars\]$/gm)).toHaveLength(1);
            expect(wrangler).toContain('CHAT_SCHEMA_HISTORY = "better-auth-1.7"');
            expect(wrangler).toContain("CDB_RELEASE_SHA256 = ");
        } finally {
            await rm(scratch, { recursive: true, force: true });
        }
    });
});
