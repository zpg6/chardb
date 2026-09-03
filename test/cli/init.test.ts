import { describe, expect, test } from "bun:test";
import { anonymous } from "better-auth/plugins/anonymous";
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { integer, text } from "drizzle-orm/sqlite-core";
import { defineAuth } from "../../src/auth/synthesize.ts";
import { runInit } from "../../src/cli/commands/init.ts";
import type { CliContext } from "../../src/cli/context.ts";
import { SCAFFOLD_INITIAL_SNAPSHOT } from "../../src/cli/scaffold-initial-snapshot.ts";
import { file } from "../../src/files/index.ts";
import { forOrg } from "../../src/server/schema-ownership.ts";
import { inspectInitialSchemaSnapshot } from "../../src/server/schema-snapshot.ts";

function generatedProject(): {
    readonly ctx: CliContext;
    readonly files: Map<string, string>;
    readonly writes: string[];
} {
    const files = new Map<string, string>();
    const writes: string[] = [];
    const ctx: CliContext = {
        cwd: "/tmp/generated",
        env: {},
        stdout: () => {},
        stderr: () => {},
        async read(path) {
            const contents = files.get(path);
            if (contents === undefined) throw new Error(`ENOENT: ${path}`);
            return contents;
        },
        async write(path, contents) {
            files.set(path, contents);
        },
        async exists(path) {
            return files.has(path);
        },
        async readDirectory(path) {
            const prefix = path.endsWith("/") ? path : `${path}/`;
            return [
                ...new Set(
                    [...files.keys()].flatMap(file => {
                        if (!file.startsWith(prefix)) return [];
                        const child = file.slice(prefix.length).split("/")[0];
                        return child ? [child] : [];
                    })
                ),
            ].sort();
        },
        async writeFilesExclusive(artifacts) {
            const conflict = artifacts.find(artifact => files.has(artifact.path));
            if (conflict) throw new Error(`artifact target already exists: ${conflict.path}`);
            for (const artifact of artifacts) {
                writes.push(artifact.path);
                files.set(artifact.path, artifact.contents);
            }
        },
    };
    return { ctx, files, writes };
}

describe("generated tutorial flow", () => {
    test("refuses an existing package.json before writing", async () => {
        const { ctx, files, writes } = generatedProject();
        files.set("/tmp/generated/package.json", '{"name":"keep-me"}\n');

        await expect(runInit(ctx, { name: "must-not-overwrite" })).rejects.toThrow("generated targets: package.json");

        expect(writes).toEqual([]);
        expect(files).toEqual(new Map([["/tmp/generated/package.json", '{"name":"keep-me"}\n']]));
    });

    test("refuses an existing nested schema before writing", async () => {
        const { ctx, files, writes } = generatedProject();
        files.set("/tmp/generated/src/schema.ts", "export const existing = true;\n");

        await expect(runInit(ctx, { name: "must-not-overwrite" })).rejects.toThrow("generated targets: src/schema.ts");

        expect(writes).toEqual([]);
        expect(files.get("/tmp/generated/src/schema.ts")).toBe("export const existing = true;\n");
        expect(files.size).toBe(1);
    });

    test("refuses a partial scaffold and leaves every existing file unchanged", async () => {
        const { ctx, files, writes } = generatedProject();
        const existing = new Map([
            ["/tmp/generated/README.md", "existing readme\n"],
            ["/tmp/generated/wrangler.toml", 'name = "existing"\n'],
            ["/tmp/generated/src/auth.ts", "export const existing = true;\n"],
        ]);
        for (const [path, contents] of existing) files.set(path, contents);

        await expect(runInit(ctx, { name: "must-not-overwrite" })).rejects.toThrow(
            "chardb init requires an empty directory"
        );

        expect(writes).toEqual([]);
        expect(files).toEqual(existing);
    });

    test("refuses unrelated directory contents but allows Git metadata and Finder metadata", async () => {
        const blocked = generatedProject();
        blocked.files.set("/tmp/generated/notes.txt", "keep me\n");
        await expect(runInit(blocked.ctx, { name: "blocked" })).rejects.toThrow("top-level entries: notes.txt");
        expect(blocked.writes).toEqual([]);

        const allowed = generatedProject();
        allowed.files.set("/tmp/generated/.git/config", "[core]\n");
        allowed.files.set("/tmp/generated/.DS_Store", "finder metadata");
        await runInit(allowed.ctx, { name: "allowed" });
        expect(allowed.files.get("/tmp/generated/.git/config")).toBe("[core]\n");
        expect(allowed.files.get("/tmp/generated/.DS_Store")).toBe("finder metadata");
        expect(allowed.files.has("/tmp/generated/package.json")).toBe(true);
    });

    test("keeps the embedded scaffold snapshot identical to a fresh inspection", () => {
        const auth = defineAuth({ plugins: [anonymous(), organization(), jwt()] });
        const { cdbTable } = forOrg(auth);
        const messages = cdbTable(
            "messages",
            {
                id: text("id").primaryKey(),
                authorId: text("author_id")
                    .notNull()
                    .references(() => auth.user.id, { onDelete: "cascade" }),
                body: text("body").notNull(),
                attachment: file("attachment", {
                    maxSize: 5 * 1_024 * 1_024,
                    contentTypes: ["image/jpeg", "image/png"],
                }),
                createdAt: integer("created_at").notNull(),
            },
            {
                selfBy: "authorId",
                roles: {
                    owner: "*",
                    admin: "*",
                    member: { read: "*", create: ["id", "body", "attachment", "createdAt"] },
                    self: { read: "*", update: ["body", "attachment"], delete: true },
                },
            }
        );

        expect(
            inspectInitialSchemaSnapshot({
                name: "initial_schema",
                domainSchema: { messages },
                authOptions: auth.options,
            })
        ).toEqual(SCAFFOLD_INITIAL_SNAPSHOT);
    });

    test("keeps version one independent from mutable application schema", async () => {
        const { ctx, files } = generatedProject();
        await runInit(ctx, { name: "migration-history-check" });

        const migrations = files.get("/tmp/generated/src/migrations.ts") ?? "";
        const versionOne = files.get("/tmp/generated/src/migrations/v1.ts") ?? "";
        const snapshotOne = files.get("/tmp/generated/src/migrations/v1.json") ?? "";
        const readme = files.get("/tmp/generated/README.md") ?? "";

        expect(migrations).toContain('import { initialSchema } from "./migrations/v1.ts"');
        expect(migrations).toContain("defineMigrations([\n  initialSchema,\n])");
        expect(migrations).not.toContain('from "./schema.ts"');
        expect(migrations).not.toContain('from "./auth.ts"');
        expect(migrations).not.toContain("defineSchemaBaseline");

        expect(versionOne).toContain("immutable version-one schema snapshot");
        expect(versionOne).toContain("Do not edit this file after");
        expect(versionOne).not.toContain('from "../schema.ts"');
        expect(versionOne).not.toContain('from "../auth.ts"');
        expect(versionOne).not.toContain("defineAuth");
        expect(versionOne).not.toContain("cdbTable(");
        expect(versionOne).not.toContain("defineSchemaBaseline");
        expect(versionOne).not.toContain("better-auth");
        expect(versionOne).not.toContain("drizzle-orm");
        expect(versionOne).toContain("defineSchemaSnapshot({");
        expect(versionOne).toContain('"format": "chardb.schema-snapshot.v1"');
        expect(versionOne).toContain('"digest": "0fd0fcf9a9449e01fdeeb9834234b794a8d6b20b8031319aa0734b2ea03481f7"');
        expect(versionOne).toContain(".initialMigration");
        expect(JSON.parse(snapshotOne)).toEqual(SCAFFOLD_INITIAL_SNAPSHOT);
        expect(readme).toContain("bunx @chardb/core migrations generate --name add_messages");
        expect(readme).not.toContain("shards split");
        expect(readme).not.toContain("virtual-shard range");
    });

    test("keeps local development isolated from deployed origins and secrets", async () => {
        const { ctx, files } = generatedProject();
        await runInit(ctx, { name: "local-boundary" });

        const dev = files.get("/tmp/generated/scripts/dev.mjs") ?? "";
        const vite = files.get("/tmp/generated/vite.config.ts") ?? "";
        const worker = files.get("/tmp/generated/src/worker.ts") ?? "";
        const workerIdentity = worker.match(/deploymentId: "(chardb\.app\.v1\/[^"]+)"/)?.[1];
        expect(workerIdentity).toMatch(
            /^chardb\.app\.v1\/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
        );

        expect(dev).toContain('process.env.CHARDB_DEV_URL, "http://127.0.0.1:8787"');
        expect(dev).toContain('process.env.CHARDB_DEV_WEB_URL, "http://127.0.0.1:5173"');
        expect(dev).toContain("process.env.CHARDB_DEV_PERSIST_TO");
        expect(dev).toContain("// readiness miss and keep polling within the existing deadline.");
        expect(vite).toContain('process.env.CHARDB_DEV_URL, "http://127.0.0.1:8787"');
        expect(vite).toContain('throw new Error(name + " must be a loopback HTTP origin")');
        expect(dev).toContain('throw new Error(name + " must be a loopback HTTP origin")');

        for (const productionVariable of ["CHARDB_URL", "CHARDB_WEB_URL", "CHARDB_ADMIN_TOKEN", "BETTER_AUTH_SECRET"]) {
            expect(dev).not.toContain(`process.env.${productionVariable}`);
            expect(dev).toContain(`delete env.${productionVariable}`);
        }
        expect(vite).not.toContain("process.env.CHARDB_URL");
        expect(dev).not.toContain("production-admin-token");
        expect(dev).not.toContain("production-better-auth-secret");
        expect(dev).toContain(`const deploymentId = "${workerIdentity}"`);
        expect(worker).toContain(`deploymentId: "${workerIdentity}"`);
        expect(files.get("/tmp/generated/test/worker.test.ts")).toContain(`deploymentId: "${workerIdentity}"`);
        expect(files.get("/tmp/generated/scripts/setup-cloudflare.mjs")).toContain(
            `const deploymentId = "${workerIdentity}"`
        );
        expect(files.get("/tmp/generated/scripts/deploy.mjs")).toContain(`const deploymentId = "${workerIdentity}"`);

        const packageJson = JSON.parse(files.get("/tmp/generated/package.json") ?? "null");
        expect(packageJson.engines).toEqual({ node: ">=22", bun: ">=1.2.22" });
        expect(packageJson.devDependencies.wrangler).toBe("4.125.0");
        expect(files.get("/tmp/generated/scripts/deploy.mjs")).toContain('const pinnedWranglerVersion = "4.125.0"');
    });
});
