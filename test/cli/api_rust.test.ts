import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { z } from "zod";
import { renderRustModule, runApiRust } from "../../src/cli/commands/api-rust.ts";
import { type CliContext, REAL_CONTEXT } from "../../src/cli/context.ts";
import { runCli } from "../../src/cli/run.ts";
import { api } from "../../src/server/index.ts";

const ROOT = resolve(import.meta.dir, "../..");
/** The app under `test/fixtures/api-rust` rendered by the real subprocess. */
const FIXTURE_APP = `${ROOT}/test/fixtures/api-rust`;
/**
 * Compiled and exercised by `rust/chardb/tests/generated_api.rs`. Regenerate from the fixture app:
 * `cd test/fixtures/api-rust && bun ../../../src/cli/bin.ts api rust --out ../../../rust/chardb/tests/fixtures/generated_api.rs`
 */
const FIXTURE_MODULE = `${ROOT}/rust/chardb/tests/fixtures/generated_api.rs`;

function fakeCtx(
    inspection: string,
    stderr = ""
): { readonly ctx: CliContext; readonly files: Map<string, string>; readonly writes: string[] } {
    const files = new Map<string, string>([["/project/src/worker.ts", "worker"]]);
    const writes: string[] = [];
    const ctx: CliContext = {
        cwd: "/project",
        env: {},
        stdout: () => {},
        stderr: () => {},
        async read(path) {
            const value = files.get(path);
            if (value === undefined) throw new Error(`ENOENT: ${path}`);
            return value;
        },
        async write(path, contents) {
            writes.push(path);
            files.set(path, contents);
        },
        async exists(path) {
            return files.has(path);
        },
        selfCommand: { executable: "/bun", args: ["/chardb"] },
        async runCommand(invocation) {
            expect(invocation.args).toEqual(["/chardb", "__api-inspect"]);
            return { exitCode: 0, stdout: inspection, stderr };
        },
    };
    return { ctx, files, writes };
}

describe("chardb api rust", () => {
    test("writes the module that rust/chardb compiles against its fixture", async () => {
        const out = await mkdtemp(`${tmpdir()}/chardb-api-rust-`);
        try {
            const ctx: CliContext = {
                ...REAL_CONTEXT,
                cwd: FIXTURE_APP,
                stdout: () => {},
                stderr: () => {},
                selfCommand: { executable: process.execPath, args: [`${ROOT}/src/cli/bin.ts`] },
            };
            await runApiRust(ctx, { out: `${out}/chardb_api.rs` });
            expect(await readFile(`${out}/chardb_api.rs`, "utf8")).toBe(await readFile(FIXTURE_MODULE, "utf8"));
        } finally {
            await rm(out, { recursive: true, force: true });
        }
    });

    test("keeps distinct wire names distinct and never aborts on a field zod cannot describe", () => {
        const mutation = (ref: string, args?: z.ZodObject) =>
            api.mutation({
                ref,
                authority: "organization",
                partitionKey: () => "org",
                ...(args ? { args } : {}),
                handler: () => null,
            });
        const rendered = renderRustModule(
            {
                listMessages: mutation(
                    "m#a",
                    z.object({
                        organizationId: z.string(),
                        organization_id: z.string(),
                        status: z.enum(["in_progress", "In Progress", "self"]),
                        when: z.date().optional(),
                        "form\ffeed": z.string(),
                        "lone\ud800surrogate": z.string(),
                    })
                ),
                list_messages: mutation("m#b\nfn main() {}"),
            },
            {}
        );
        expect(rendered).toContain('#[serde(rename = "organizationId")]\n    pub organization_id: String,');
        expect(rendered).toContain('#[serde(rename = "organization_id")]\n    pub organization_id_2: String,');
        expect(rendered).toContain("    InProgress,\n");
        expect(rendered).toContain("    InProgress_2,\n");
        expect(rendered).toContain('#[serde(rename = "self")]\n    Self_,');
        expect(rendered).toContain("pub when: Option<::serde_json::Value>,");
        expect(rendered).toContain('#[serde(rename = "form\\u{c}feed")]\n    pub form_feed: String,');
        expect(rendered).toContain('#[serde(rename = "lone\\u{fffd}surrogate")]\n    pub lone_surrogate: String,');
        expect(rendered).toContain("pub const LIST_MESSAGES:");
        expect(rendered).toContain("/// `m#bfn main() {}`\npub const LIST_MESSAGES_2:");
        expect(rendered).toContain('Mutation::new("m#b\\u{a}fn main() {}")');
        expect(() => renderRustModule({ a: mutation("m#same"), b: mutation("m#same") }, {})).toThrow(
            /m#same: registered by two different handles/
        );
    });

    test("--check reports a stale or missing module without touching it", async () => {
        const module = await readFile(FIXTURE_MODULE, "utf8");
        const { ctx, files, writes } = fakeCtx(module, "warning: something printed while loading\n");
        await expect(runApiRust(ctx, { out: "src/api.rs", check: true })).rejects.toThrow(/src\/api\.rs is stale/);
        files.set("/project/src/api.rs", "// old");
        await expect(runApiRust(ctx, { out: "src/api.rs", check: true })).rejects.toThrow(/src\/api\.rs is stale/);
        expect(files.get("/project/src/api.rs")).toBe("// old");
        await runApiRust(ctx, { out: "src/api.rs" });
        expect(files.get("/project/src/api.rs")).toBe(module);
        await expect(runApiRust(ctx, { out: "src/api.rs", check: true })).resolves.toBeUndefined();
        await runApiRust(ctx, { out: "src/api.rs" });
        expect(writes).toEqual(["/project/src/api.rs"]);
    });

    test("rejects inspector output that is not a generated module", async () => {
        await expect(runApiRust(fakeCtx("pub const X: u8 = 1;\n").ctx, { out: "x.rs" })).rejects.toThrow(
            /malformed output; the worker must not print to stdout/
        );
        await expect(runApiRust(fakeCtx("", "boom").ctx, { out: "x.rs" })).rejects.toThrow(/malformed output: boom/);
    });

    test("refuses a project without src/worker.ts and bad usage", async () => {
        const { ctx, files } = fakeCtx("");
        files.clear();
        await expect(runApiRust(ctx, { out: "x.rs" })).rejects.toThrow(/src\/worker\.ts is missing/);
        expect(await runCli(ctx, ["api", "rust", "--out", "x.rs"])).toBe(1);
        expect(await runCli(ctx, ["api", "rust", "--check", "--out", "x.rs"])).toBe(1);
        expect(await runCli(ctx, ["api", "rust"])).toBe(2);
        expect(await runCli(ctx, ["api", "rust", "--check"])).toBe(2);
        expect(await runCli(ctx, ["api", "export"])).toBe(2);
    });
});
