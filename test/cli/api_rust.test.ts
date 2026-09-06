import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { z } from "zod";
import { renderRustModule, runApiRust } from "../../src/cli/commands/api-rust.ts";
import { type CliContext, REAL_CONTEXT } from "../../src/cli/context.ts";
import { runCli } from "../../src/cli/run.ts";
import { api } from "../../src/server/index.ts";

const ROOT = resolve(import.meta.dir, "../..");
/** Compiled and exercised by `rust/chardb/tests/generated_api.rs`. */
const FIXTURE_MODULE = `${ROOT}/rust/chardb/tests/fixtures/generated_api.rs`;
const server = JSON.stringify(`${ROOT}/src/server/index.ts`);

const FIXTURE_APP: Readonly<Record<string, string>> = {
    "src/auth.ts": `import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { defineAuth } from ${server};
export const auth = defineAuth({ plugins: [organization(), jwt()] });
`,
    "src/schema.ts": `import { integer, real, text } from "drizzle-orm/sqlite-core";
import { forOrg } from ${server};
import { auth } from "./auth.ts";
const { cdbTable } = forOrg(auth);
export const messages = cdbTable(
    "messages",
    {
        id: text("id").primaryKey(),
        body: text("body").notNull(),
        createdAt: integer("created_at").notNull(),
        pinned: integer("pinned", { mode: "boolean" }).notNull(),
        score: real("score"),
        meta: text("meta", { mode: "json" }),
    },
    { roles: { owner: "*", admin: "*", member: { read: "*" } } }
);
`,
    "src/queries.ts": `import { desc, eq } from "drizzle-orm";
import { z } from "zod";
import { api } from ${server};
import { messages } from "./schema.ts";
export const listMessages = api.query({
    ref: "src/queries.ts#listMessages",
    args: z.object({
        organizationId: z.string(),
        limit: z.number().int().min(0).max(100).default(50),
        kind: z.enum(["all", "pinned"]).optional(),
        scope: z.enum(["self", "team"]).optional(),
        filters: z.record(z.string(), z.string()).optional(),
    }),
    query: (db, args) =>
        db
            .select()
            .from(messages)
            .where(eq(messages.organizationId, args.organizationId))
            .orderBy(desc(messages.createdAt), desc(messages.id))
            .limit(args.limit),
});
export const allMessages = api.query({
    ref: "src/queries.ts#allMessages",
    query: db =>
        db.select().from(messages).where(eq(messages.organizationId, "org-fixture")).orderBy(desc(messages.id)).limit(10),
});
`,
    "src/api.ts": `import { z } from "zod";
import { api } from ${server};
import { messages } from "./schema.ts";
export const postMessage = api.mutation({
    ref: "src/api.ts#postMessage",
    authority: "organization",
    args: z.object({
        id: z.string(),
        organizationId: z.string(),
        body: z.string().min(1),
        type: z.string().nullable(),
        tags: z.array(z.string()).optional(),
        dueAt: z.string().nullable().optional(),
    }),
    partitionKey: "organizationId",
    handler: (ctx, args) => {
        ctx.db.insert(messages).values({ id: args.id, body: args.body, createdAt: 0, pinned: false }).run();
        return { id: args.id };
    },
});
export const clearMessages = api.mutation({
    ref: "src/api.ts#clearMessages",
    authority: "organization",
    partitionKey: (args: { organizationId: string }) => args.organizationId,
    handler: () => ({ cleared: true }),
});
`,
    "src/worker.ts": `import { chardb } from ${server};
import * as api from "./api.ts";
import { auth } from "./auth.ts";
import * as queries from "./queries.ts";
import * as schema from "./schema.ts";
export const app = chardb({ ownership: "organization", auth, schema, api: { ...api, ...queries } });
export default app;
`,
};

async function withFixtureApp<T>(run: (ctx: CliContext, project: string) => Promise<T>): Promise<T> {
    const project = await mkdtemp(`${ROOT}/.api-rust-`);
    try {
        await mkdir(`${project}/src`);
        for (const [path, contents] of Object.entries(FIXTURE_APP)) await Bun.write(`${project}/${path}`, contents);
        return await run(
            {
                ...REAL_CONTEXT,
                cwd: project,
                stdout: () => {},
                stderr: () => {},
                selfCommand: { executable: process.execPath, args: [`${ROOT}/src/cli/bin.ts`] },
            },
            project
        );
    } finally {
        await rm(project, { recursive: true, force: true });
    }
}

function fakeCtx(inspection: string): { readonly ctx: CliContext; readonly files: Map<string, string> } {
    const files = new Map<string, string>([["/project/src/worker.ts", "worker"]]);
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
            files.set(path, contents);
        },
        async exists(path) {
            return files.has(path);
        },
        selfCommand: { executable: "/bun", args: ["/chardb"] },
        async runCommand(invocation) {
            expect(invocation.args).toEqual(["/chardb", "__api-inspect"]);
            return { exitCode: 0, stdout: inspection, stderr: "" };
        },
    };
    return { ctx, files };
}

describe("chardb api rust", () => {
    test("writes the module that rust/chardb compiles against its fixture", async () => {
        const rendered = await withFixtureApp(async (ctx, project) => {
            await runApiRust(ctx, { out: "src/chardb_api.rs" });
            return readFile(`${project}/src/chardb_api.rs`, "utf8");
        });
        if (process.env.CHARDB_API_RUST_FIXTURE === "update") await Bun.write(FIXTURE_MODULE, rendered);
        expect(rendered).toBe(await readFile(FIXTURE_MODULE, "utf8"));
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
                    })
                ),
                list_messages: mutation("m#b"),
            },
            {}
        );
        expect(rendered).toContain('#[serde(rename = "organizationId")]\n    pub organization_id: String,');
        expect(rendered).toContain('#[serde(rename = "organization_id")]\n    pub organization_id_2: String,');
        expect(rendered).toContain("    InProgress,\n");
        expect(rendered).toContain("    InProgress_2,\n");
        expect(rendered).toContain('#[serde(rename = "self")]\n    Self_,');
        expect(rendered).toContain("pub when: Option<::serde_json::Value>,");
        expect(rendered).toContain("pub const LIST_MESSAGES:");
        expect(rendered).toContain("pub const LIST_MESSAGES_2:");
        expect(() => renderRustModule({ a: mutation("m#same"), b: mutation("m#same") }, {})).toThrow(
            /m#same: registered by two different handles/
        );
    });

    test("--check reports a stale module without touching it", async () => {
        const module = `${await readFile(FIXTURE_MODULE, "utf8")}`;
        const { ctx, files } = fakeCtx(module);
        files.set("/project/src/api.rs", "// old");
        await expect(runApiRust(ctx, { out: "src/api.rs", check: true })).rejects.toThrow(/src\/api\.rs is stale/);
        expect(files.get("/project/src/api.rs")).toBe("// old");
        await runApiRust(ctx, { out: "src/api.rs" });
        expect(files.get("/project/src/api.rs")).toBe(module);
        await expect(runApiRust(ctx, { out: "src/api.rs", check: true })).resolves.toBeUndefined();
    });

    test("rejects inspector output that is not a generated module", async () => {
        await expect(runApiRust(fakeCtx("pub const X: u8 = 1;\n").ctx, { out: "x.rs" })).rejects.toThrow(/malformed/);
    });

    test("refuses a project without src/worker.ts and bad usage", async () => {
        const { ctx, files } = fakeCtx("");
        files.clear();
        await expect(runApiRust(ctx, { out: "x.rs" })).rejects.toThrow(/src\/worker\.ts is missing/);
        expect(await runCli(ctx, ["api", "rust", "--out", "x.rs"])).toBe(1);
        expect(await runCli(ctx, ["api", "rust"])).toBe(2);
        expect(await runCli(ctx, ["api", "export"])).toBe(2);
    });
});
