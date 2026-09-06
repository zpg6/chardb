import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import { z } from "zod";
import { collectApiHandles } from "../../src/cli/api-handles.ts";
import { type GeneratedApp, generateClients, runGenerate } from "../../src/cli/commands/generate.ts";
import { type CliContext, REAL_CONTEXT } from "../../src/cli/context.ts";
import { renderRustModule } from "../../src/cli/rust-module.ts";
import { renderTsModule } from "../../src/cli/ts-module.ts";
import { api } from "../../src/server/index.ts";

const ROOT = resolve(import.meta.dir, "../..");
/**
 * Declares `rust/chardb/tests/fixtures/generated_api.rs` and `test/cli/fixtures/generated_api.ts` as its clients.
 * Regenerate both: `cd test/fixtures/generate && bun ../../../src/cli/bin.ts generate`
 */
const FIXTURE_APP = `${ROOT}/test/fixtures/generate`;

interface FakeProject {
    readonly ctx: CliContext;
    readonly files: Map<string, string>;
    readonly output: string[];
    readonly invocations: (readonly string[])[];
}

function fakeProject(child: {
    readonly exitCode: number;
    readonly stdout: string;
    readonly stderr: string;
}): FakeProject {
    const files = new Map<string, string>([["/project/src/worker.ts", "worker"]]);
    const output: string[] = [];
    const invocations: (readonly string[])[] = [];
    const ctx: CliContext = {
        cwd: "/project",
        env: {},
        stdout: line => output.push(line),
        stderr: () => {},
        async read(path) {
            const value = files.get(path);
            if (value === undefined) throw new Error(`ENOENT: ${path}`);
            return value;
        },
        async write(path, contents) {
            files.set(path, contents);
        },
        async exists(path) {
            return files.has(path);
        },
        selfCommand: { executable: "/bun", args: ["/chardb"] },
        async runCommand(invocation) {
            invocations.push(invocation.args);
            return child;
        },
        async writeFilesAtomic(changes) {
            for (const change of changes) {
                if ((files.get(change.path) ?? null) !== change.expectedContents) {
                    throw new Error(`artifact target changed: ${change.path}`);
                }
            }
            for (const change of changes) files.set(change.path, change.contents);
        },
    };
    return { ctx, files, output, invocations };
}

const mutation = (ref: string, args?: z.ZodObject) =>
    api.mutation({
        ref,
        authority: "organization",
        partitionKey: () => "org",
        ...(args ? { args } : {}),
        handler: () => null,
    });

const app = (clients?: GeneratedApp["clients"]): GeneratedApp => ({
    api: { ping: mutation("m#ping", z.object({ organizationId: z.string() })) },
    schema: {},
    ...(clients ? { clients } : {}),
});

async function biomeFormat(source: string): Promise<string> {
    const child = Bun.spawn([`${ROOT}/node_modules/.bin/biome`, "format", "--stdin-file-path=generated.ts"], {
        cwd: ROOT,
        stdin: new Blob([source]),
        stdout: "pipe",
        stderr: "pipe",
    });
    const [formatted, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
    expect(exitCode).toBe(0);
    return formatted;
}

describe("chardb generate", () => {
    test("the committed fixtures are what the fresh process generates from the fixture app", async () => {
        const output: string[] = [];
        const ctx: CliContext = {
            ...REAL_CONTEXT,
            cwd: FIXTURE_APP,
            stdout: line => output.push(line),
            stderr: () => {},
            selfCommand: { executable: process.execPath, args: [`${ROOT}/src/cli/bin.ts`] },
        };
        await runGenerate(ctx, { check: true });
        expect(output).toEqual([]);
    }, 20_000);

    test("writes every stale target, then only the ones whose bytes changed", async () => {
        const clients = { rust: "src/api.rs", ts: "src/api.ts" };
        const project = fakeProject({ exitCode: 0, stdout: "", stderr: "" });
        await expect(generateClients(app(clients), project.ctx, true)).rejects.toThrow(
            "src/api.rs is missing, src/api.ts is missing; run chardb generate"
        );
        expect(project.files.size).toBe(1);

        await generateClients(app(clients), project.ctx, false);
        expect(project.output).toEqual(["chardb: wrote src/api.rs\n", "chardb: wrote src/api.ts\n"]);
        const rust = project.files.get("/project/src/api.rs") ?? "";
        expect(rust).toContain("pub const PING: Mutation<PingArgs, ::serde_json::Value>");
        expect(project.files.get("/project/src/api.ts")).toContain(
            'export const ping = handle("mutation", "m#ping") as MutationHandle<PingArgs, RawJson>;'
        );

        project.output.length = 0;
        await generateClients(app(clients), project.ctx, true);
        await generateClients(app(clients), project.ctx, false);
        expect(project.output).toEqual([]);

        const old = rust.replace("PingArgs", "OldArgs");
        project.files.set("/project/src/api.rs", old);
        await expect(generateClients(app(clients), project.ctx, true)).rejects.toThrow(
            "src/api.rs is stale; run chardb generate"
        );
        expect(project.files.get("/project/src/api.rs")).toBe(old);
        expect(project.output).toEqual([]);
        await generateClients(app(clients), project.ctx, false);
        expect(project.output).toEqual(["chardb: wrote src/api.rs\n"]);
        expect(project.files.get("/project/src/api.rs")).toBe(rust);

        project.files.set("/project/src/api.ts", "// hand written");
        for (const check of [true, false]) {
            await expect(generateClients(app(clients), project.ctx, check)).rejects.toThrow(
                "src/api.ts is not a generated module; move it or point clients elsewhere"
            );
        }
        expect(project.files.get("/project/src/api.ts")).toBe("// hand written");
    });

    test("rejects absolute paths and foreign extensions, and reports a project without clients", async () => {
        const project = fakeProject({ exitCode: 0, stdout: "", stderr: "" });
        await expect(generateClients(app({ rust: "/tmp/api.rs" }), project.ctx, false)).rejects.toThrow(
            "clients.rust must be relative to the project root: /tmp/api.rs"
        );
        await expect(generateClients(app({ ts: "src/api.js" }), project.ctx, false)).rejects.toThrow(
            "clients.ts must end in .ts: src/api.js"
        );
        await expect(generateClients(app({ rust: "src/api.ts" }), project.ctx, false)).rejects.toThrow(
            "clients.rust must end in .rs: src/api.ts"
        );
        expect(project.files.size).toBe(1);
        expect(project.output).toEqual([]);

        await generateClients(app(), project.ctx, true);
        await generateClients(app({}), project.ctx, false);
        expect(project.output).toEqual(["chardb: no clients configured\n", "chardb: no clients configured\n"]);
        expect(project.files.size).toBe(1);
    });

    test("relays the fresh process's output and verdict", async () => {
        const wrote = fakeProject({ exitCode: 0, stdout: "chardb: wrote src/api.ts\n", stderr: "" });
        await runGenerate(wrote.ctx, { check: true });
        expect(wrote.invocations).toEqual([["/chardb", "__generate-inspect", "--check"]]);
        expect(wrote.output).toEqual(["chardb: wrote src/api.ts\n"]);

        const stale = fakeProject({ exitCode: 1, stdout: "", stderr: "src/api.ts is stale; run chardb generate\n" });
        await expect(runGenerate(stale.ctx, { check: false })).rejects.toThrow(
            "src/api.ts is stale; run chardb generate"
        );
        expect(stale.invocations).toEqual([["/chardb", "__generate-inspect"]]);
        expect(stale.output).toEqual([]);

        const missing = fakeProject({ exitCode: 0, stdout: "", stderr: "" });
        missing.files.clear();
        await expect(runGenerate(missing.ctx, { check: false })).rejects.toThrow("src/worker.ts is missing");
        expect(missing.invocations).toEqual([]);
    });

    test("the Rust module keeps distinct wire names distinct and never aborts on a field zod cannot describe", () => {
        const rendered = renderRustModule(
            collectApiHandles(
                {
                    listMessages: mutation(
                        "m#a",
                        z.object({
                            organizationId: z.string(),
                            organization_id: z.string(),
                            status: z.enum(["in_progress", "In Progress", "self"]),
                            when: z.date().optional(),
                        })
                    ),
                    list_messages: mutation("m#b"),
                },
                {}
            )
        );
        expect(rendered).toContain('#[serde(rename = "organizationId")]\n    pub organization_id: String,');
        expect(rendered).toContain('#[serde(rename = "organization_id")]\n    pub organization_id_2: String,');
        expect(rendered).toContain("    InProgress,\n");
        expect(rendered).toContain("    InProgress_2,\n");
        expect(rendered).toContain('#[serde(rename = "self")]\n    Self_,');
        expect(rendered).toContain("pub when: Option<::serde_json::Value>,");
        expect(rendered).toContain("pub const LIST_MESSAGES:");
        expect(rendered).toContain("pub const LIST_MESSAGES_2:");
        expect(() => collectApiHandles({ a: mutation("m#same"), b: mutation("m#same") }, {})).toThrow(
            /m#same: registered by two different handles/
        );
    });

    test("the TypeScript module guards reserved names, types every schema shape, and is already biome-formatted", async () => {
        const rendered = renderTsModule(
            collectApiHandles(
                {
                    listMessages: mutation(
                        "m#a",
                        z.object({
                            "in-progress": z.enum(["in_progress", "self"]),
                            when: z.date().optional(),
                            note: z.string().nullable().optional(),
                            tags: z.array(z.string().nullable()),
                            filters: z.record(z.string(), z.number()),
                            nested: z.object({ deep: z.literal("x"), count: z.number().int() }),
                            "form\ffeed": z.string(),
                        })
                    ),
                    closer: mutation("m#*/ export {}\n"),
                    delete: mutation("m#b"),
                    handle: mutation("m#c"),
                    "list-messages": mutation("m#e"),
                    "1st": mutation("m#f"),
                    RawJson: mutation("m#g"),
                    eval: mutation("m#h"),
                    aVeryLongExportNameThatPushesTheDeclarationPastTheLineWidth: mutation(
                        `src/${"deeply/".repeat(8)}module.ts#aVeryLongExportNameThatPushesTheDeclarationPastTheLineWidth`
                    ),
                },
                {}
            )
        );
        expect(rendered).toContain('import type { MutationHandle, RawJson } from "@chardb/core";');
        expect(rendered).toContain('    "in-progress": "in_progress" | "self";\n');
        expect(rendered).toContain("    when?: RawJson;\n");
        expect(rendered).toContain("    note?: string | null;\n");
        expect(rendered).toContain("    tags: (string | null)[];\n");
        expect(rendered).toContain("    filters: { [key: string]: number };\n");
        expect(rendered).toContain('    nested: {\n        deep: "x";\n        count: number;\n    };\n');
        expect(rendered).toContain('    "form\\ffeed": string;\n');
        expect(rendered).toContain(
            '/** `m#*\\/ export {}` */\nexport const closer = handle("mutation", "m#*/ export {}\\n")'
        );
        expect(rendered).toContain(
            'export const delete_ = handle("mutation", "m#b") as MutationHandle<{ [key: string]: RawJson }, RawJson>;'
        );
        expect(rendered).toContain('export const handle_2 = handle("mutation", "m#c")');
        expect(rendered).toContain('export const listMessages = handle("mutation", "m#e")');
        expect(rendered).toContain('export const listMessages_2 = handle("mutation", "m#a")');
        expect(rendered).toContain('export const _1st = handle("mutation", "m#f")');
        expect(rendered).toContain('export const RawJson_2 = handle("mutation", "m#g")');
        expect(rendered).toContain('export const eval_ = handle("mutation", "m#h")');
        expect(await biomeFormat(rendered)).toBe(rendered);
        expect(renderTsModule([])).toBe(
            "// Typed CharDB handles generated by `chardb generate`. Do not edit.\nexport {};\n"
        );
    });
});
