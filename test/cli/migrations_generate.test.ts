import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { runMigrationsGenerate } from "../../src/cli/commands/migrations-generate.ts";
import { type CliContext, REAL_CONTEXT } from "../../src/cli/context.ts";
import { renderInitialMigrationArtifacts } from "../../src/cli/migration-artifacts.ts";
import { SCAFFOLD_INITIAL_SNAPSHOT } from "../../src/cli/scaffold-initial-snapshot.ts";
import {
    type ChardbSchemaSnapshotContent,
    type ChardbSchemaSnapshotInput,
    schemaSnapshotDigest,
} from "../../src/server/schema-snapshot.ts";
import { stableJson } from "../../src/util/canonical.ts";

interface FakeProject {
    readonly ctx: CliContext;
    readonly files: Map<string, string>;
    readonly output: string[];
    readonly invocations: {
        readonly args: readonly string[];
        readonly timeoutMs: number;
        readonly maxOutputBytes?: number;
    }[];
}

const NO_CLIENTS = "chardb: no clients configured\n";

/** The schema inspections a run made, without the client refresh that follows each write. */
const schemaInspections = (project: FakeProject) =>
    project.invocations.filter(invocation => invocation.args[1] === "__migrations-inspect");

function fakeProject(
    results: readonly string[] = [`${stableJson(SCAFFOLD_INITIAL_SNAPSHOT)}\n`],
    refresh = { exitCode: 0, stdout: NO_CLIENTS, stderr: "" }
): FakeProject {
    const files = new Map<string, string>([
        ["/project/src/schema.ts", "schema"],
        ["/project/src/auth.ts", "auth"],
        ["/project/src/worker.ts", "worker"],
    ]);
    const output: string[] = [];
    const invocations: FakeProject["invocations"] = [];
    let call = 0;
    const ctx: CliContext = {
        cwd: "/project",
        env: {},
        stdout: value => output.push(value),
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
        async readDirectory(path) {
            const prefix = `${path}/`;
            return [...files.keys()]
                .filter(candidate => candidate.startsWith(prefix))
                .map(candidate => candidate.slice(prefix.length))
                .filter(candidate => !candidate.includes("/"))
                .sort();
        },
        selfCommand: { executable: "/bun", args: ["/chardb"] },
        async runCommand(invocation) {
            invocations.push(invocation);
            if (invocation.args[1] === "__generate-inspect") {
                // Client modules are refreshed only once the migration artifacts are on disk.
                expect(files.has("/project/src/migrations.ts")).toBe(true);
                return refresh;
            }
            const stdout = results[call] ?? results[0] ?? "";
            call++;
            return { exitCode: 0, stdout, stderr: "" };
        },
        async writeFilesExclusive(artifacts) {
            if (artifacts.some(artifact => files.has(artifact.path))) throw new Error("EEXIST");
            for (const artifact of artifacts) files.set(artifact.path, artifact.contents);
        },
        async writeFilesAtomic(changes) {
            for (const change of changes) {
                const current = files.get(change.path);
                if (change.expectedContents === null ? current !== undefined : current !== change.expectedContents) {
                    throw new Error(`artifact target changed: ${change.path}`);
                }
            }
            for (const change of changes) files.set(change.path, change.contents);
        },
    };
    return { ctx, files, output, invocations };
}

function addMessageColumn(
    previous: ChardbSchemaSnapshotInput,
    version: number,
    name: string,
    column: string
): ChardbSchemaSnapshotInput {
    const content: ChardbSchemaSnapshotContent = {
        format: previous.format,
        version,
        name,
        previousDigest: previous.digest,
        cdbTables: previous.cdbTables.map(table =>
            table.tableName === "messages"
                ? { ...table, columns: [...table.columns, { name: column, sql: `"${column}" text` }] }
                : table
        ),
        catalogTables: previous.catalogTables,
        resources: previous.resources,
    };
    return Object.freeze({ ...content, digest: schemaSnapshotDigest(content) });
}

describe("initial migration generation", () => {
    test("observes two fresh bounded processes and writes only static version-one artifacts", async () => {
        const project = fakeProject();
        await runMigrationsGenerate(project.ctx, { name: "initial_schema" });

        expect(project.invocations.map(invocation => invocation.args[1])).toEqual([
            "__migrations-inspect",
            "__migrations-inspect",
            "__generate-inspect",
        ]);
        for (const invocation of schemaInspections(project)) {
            expect(invocation.args).toEqual(["/chardb", "__migrations-inspect", "initial_schema", "1", "-"]);
            expect(invocation.timeoutMs).toBe(15_000);
            expect(invocation.maxOutputBytes).toBe(16 * 1_024 * 1_024 + 1_024);
        }
        expect(project.invocations[2]?.args).toEqual(["/chardb", "__generate-inspect"]);
        const versionOne = project.files.get("/project/src/migrations/v1.ts") ?? "";
        const journal = project.files.get("/project/src/migrations.ts") ?? "";
        const snapshot = project.files.get("/project/src/migrations/v1.json") ?? "";
        expect(versionOne).toContain("defineSchemaSnapshot({");
        expect(versionOne).toContain(".initialMigration");
        expect(versionOne).not.toContain('from "../schema.ts"');
        expect(versionOne).not.toContain('from "../auth.ts"');
        expect(versionOne).not.toContain("better-auth");
        expect(versionOne).not.toContain("drizzle-orm");
        expect(journal).toContain("defineMigrations([\n  initialSchema,\n])");
        expect(snapshot).toBe(`${stableJson(SCAFFOLD_INITIAL_SNAPSHOT)}\n`);
        expect(project.output).toEqual(["chardb: generated immutable migration v1 (initial_schema)\n", NO_CLIENTS]);
    });

    test("keeps the written migration and says so when the client refresh fails", async () => {
        const project = fakeProject(undefined, { exitCode: 1, stdout: "", stderr: "src/worker.ts: boom\n" });
        await expect(runMigrationsGenerate(project.ctx, { name: "initial_schema" })).rejects.toThrow(
            "the migration was written, but its client modules were not: src/worker.ts: boom"
        );
        expect(project.files.has("/project/src/migrations.ts")).toBe(true);
        expect(project.output).toEqual(["chardb: generated immutable migration v1 (initial_schema)\n"]);
    });

    test("rejects nondeterministic observations without writing either artifact", async () => {
        const changedContent: ChardbSchemaSnapshotContent = {
            format: SCAFFOLD_INITIAL_SNAPSHOT.format,
            version: 1,
            name: SCAFFOLD_INITIAL_SNAPSHOT.name,
            previousDigest: null,
            cdbTables: SCAFFOLD_INITIAL_SNAPSHOT.cdbTables.map(table =>
                table.tableName === "messages"
                    ? {
                          ...table,
                          columns: table.columns.map(column =>
                              column.name === "body" ? { ...column, sql: '"body" text' } : column
                          ),
                      }
                    : table
            ),
            catalogTables: SCAFFOLD_INITIAL_SNAPSHOT.catalogTables,
            resources: SCAFFOLD_INITIAL_SNAPSHOT.resources,
        };
        const changed = { ...changedContent, digest: schemaSnapshotDigest(changedContent) };
        const project = fakeProject([`${stableJson(SCAFFOLD_INITIAL_SNAPSHOT)}\n`, `${stableJson(changed)}\n`]);

        await expect(runMigrationsGenerate(project.ctx, { name: "initial_schema" })).rejects.toThrow(
            /nondeterministic/
        );
        expect(project.files.has("/project/src/migrations/v1.ts")).toBe(false);
        expect(project.files.has("/project/src/migrations.ts")).toBe(false);
    });

    test("serializes fresh inspections and rejects bounded stderr", async () => {
        const project = fakeProject();
        const canonical = `${stableJson(SCAFFOLD_INITIAL_SNAPSHOT)}\n`;
        let active = 0;
        let maximumActive = 0;
        const sequentialContext: CliContext = {
            ...project.ctx,
            async runCommand() {
                active++;
                maximumActive = Math.max(maximumActive, active);
                await new Promise(resolve => setTimeout(resolve, 5));
                active--;
                return { exitCode: 0, stdout: canonical, stderr: "" };
            },
        };
        await runMigrationsGenerate(sequentialContext, { name: "initial_schema" });
        expect(maximumActive).toBe(1);

        const warning = fakeProject();
        const warningContext: CliContext = {
            ...warning.ctx,
            async runCommand() {
                return { exitCode: 0, stdout: canonical, stderr: "schema warning\n" };
            },
        };
        await expect(runMigrationsGenerate(warningContext, { name: "initial_schema" })).rejects.toThrow(
            /wrote to stderr/
        );
        expect(warning.files.has("/project/src/migrations/v1.ts")).toBe(false);
        expect(warning.files.has("/project/src/migrations.ts")).toBe(false);
    });

    test("refuses incomplete history, malformed child output, invalid names, and writer failure without partial files", async () => {
        const existing = fakeProject();
        existing.files.set("/project/src/migrations.ts", "existing journal");
        await expect(runMigrationsGenerate(existing.ctx, { name: "initial_schema" })).rejects.toThrow(
            /lacks its immutable JSON/
        );
        expect(existing.invocations).toEqual([]);

        const malformed = fakeProject(["not-json\n"]);
        await expect(runMigrationsGenerate(malformed.ctx, { name: "initial_schema" })).rejects.toThrow(
            /malformed JSON/
        );
        expect(malformed.files.has("/project/src/migrations/v1.ts")).toBe(false);

        const childFailed = fakeProject();
        const childFailedContext: CliContext = {
            ...childFailed.ctx,
            async runCommand() {
                return {
                    exitCode: 1,
                    stdout: "",
                    stderr: "schema inspection failed: missing cloudflare worker module\n",
                };
            },
        };
        await expect(runMigrationsGenerate(childFailedContext, { name: "initial_schema" })).rejects.toThrow(
            /missing cloudflare worker module/
        );
        expect(childFailed.files.has("/project/src/migrations/v1.ts")).toBe(false);

        const invalid = fakeProject();
        await expect(runMigrationsGenerate(invalid.ctx, { name: "Bad Name" })).rejects.toThrow(/must match/);
        expect(invalid.invocations).toEqual([]);

        const failedWrite = fakeProject();
        const failedContext: CliContext = {
            ...failedWrite.ctx,
            async writeFilesExclusive() {
                throw new Error("simulated atomic writer failure");
            },
        };
        await expect(runMigrationsGenerate(failedContext, { name: "initial_schema" })).rejects.toThrow(
            /simulated atomic writer failure/
        );
        expect(failedWrite.files.has("/project/src/migrations/v1.ts")).toBe(false);
        expect(failedWrite.files.has("/project/src/migrations.ts")).toBe(false);
    });

    test("generates an additive version two from immutable JSON without changing version one", async () => {
        const nextContent: ChardbSchemaSnapshotContent = {
            format: SCAFFOLD_INITIAL_SNAPSHOT.format,
            version: 2,
            name: "add_message_note",
            previousDigest: SCAFFOLD_INITIAL_SNAPSHOT.digest,
            cdbTables: SCAFFOLD_INITIAL_SNAPSHOT.cdbTables.map(table =>
                table.tableName === "messages"
                    ? { ...table, columns: [...table.columns, { name: "note", sql: '"note" text' }] }
                    : table
            ),
            catalogTables: SCAFFOLD_INITIAL_SNAPSHOT.catalogTables,
            resources: SCAFFOLD_INITIAL_SNAPSHOT.resources,
        };
        const next = { ...nextContent, digest: schemaSnapshotDigest(nextContent) };
        const project = fakeProject([`${stableJson(next)}\n`]);
        const initial = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);
        project.files.set("/project/src/migrations/v1.ts", initial.versionOne);
        project.files.set("/project/src/migrations/v1.json", initial.snapshotOne);
        project.files.set("/project/src/migrations.ts", initial.journal);

        await runMigrationsGenerate(project.ctx, { name: "add_message_note" });

        expect(schemaInspections(project)).toHaveLength(2);
        expect(project.invocations.at(-1)?.args).toEqual(["/chardb", "__generate-inspect"]);
        expect(project.invocations[0]?.args).toEqual([
            "/chardb",
            "__migrations-inspect",
            "add_message_note",
            "2",
            SCAFFOLD_INITIAL_SNAPSHOT.digest,
        ]);
        expect(project.files.get("/project/src/migrations/v1.ts")).toBe(initial.versionOne);
        expect(project.files.get("/project/src/migrations/v1.json")).toBe(initial.snapshotOne);
        expect(project.files.get("/project/src/migrations/v2.json")).toBe(`${stableJson(next)}\n`);
        const versionTwo = project.files.get("/project/src/migrations/v2.ts") ?? "";
        expect(versionTwo).toContain('ALTER TABLE \\"messages\\" ADD COLUMN \\"note\\" text');
        expect(versionTwo).not.toContain('from "../schema.ts"');
        expect(versionTwo).not.toContain('from "../auth.ts"');
        expect(project.files.get("/project/src/migrations.ts")).toContain("migrationV2,");
        expect(project.output).toEqual([
            "chardb: generated immutable additive migration v2 (add_message_note)\n",
            NO_CLIENTS,
        ]);
    });

    test("appends arbitrary sequential additive versions through v4", async () => {
        const v2 = addMessageColumn(SCAFFOLD_INITIAL_SNAPSHOT, 2, "add_note", "note");
        const v3 = addMessageColumn(v2, 3, "add_summary", "summary");
        const v4 = addMessageColumn(v3, 4, "add_caption", "caption");
        const project = fakeProject([
            `${stableJson(v2)}\n`,
            `${stableJson(v2)}\n`,
            `${stableJson(v3)}\n`,
            `${stableJson(v3)}\n`,
            `${stableJson(v4)}\n`,
            `${stableJson(v4)}\n`,
        ]);
        const initial = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);
        project.files.set("/project/src/migrations/v1.ts", initial.versionOne);
        project.files.set("/project/src/migrations/v1.json", initial.snapshotOne);
        project.files.set("/project/src/migrations.ts", initial.journal);

        await runMigrationsGenerate(project.ctx, { name: "add_note" });
        await runMigrationsGenerate(project.ctx, { name: "add_summary" });
        await runMigrationsGenerate(project.ctx, { name: "add_caption" });

        expect(schemaInspections(project).map(invocation => invocation.args.slice(-3))).toEqual([
            ["add_note", "2", SCAFFOLD_INITIAL_SNAPSHOT.digest],
            ["add_note", "2", SCAFFOLD_INITIAL_SNAPSHOT.digest],
            ["add_summary", "3", v2.digest],
            ["add_summary", "3", v2.digest],
            ["add_caption", "4", v3.digest],
            ["add_caption", "4", v3.digest],
        ]);
        expect(project.files.get("/project/src/migrations/v3.json")).toBe(`${stableJson(v3)}\n`);
        expect(project.files.get("/project/src/migrations/v4.json")).toBe(`${stableJson(v4)}\n`);
        expect(project.files.get("/project/src/migrations/v3.ts")).toContain(
            'ALTER TABLE \\"messages\\" ADD COLUMN \\"summary\\" text'
        );
        expect(project.files.get("/project/src/migrations/v4.ts")).toContain(
            'ALTER TABLE \\"messages\\" ADD COLUMN \\"caption\\" text'
        );
        expect(project.files.get("/project/src/migrations.ts")).toContain(
            'import { migration as migrationV4 } from "./migrations/v4.ts";'
        );
        expect(project.files.get("/project/src/migrations.ts")).toContain(
            "defineMigrations([\n  initialSchema,\n  migrationV2,\n  migrationV3,\n  migrationV4,\n])"
        );
        expect(project.output).toEqual([
            "chardb: generated immutable additive migration v2 (add_note)\n",
            NO_CLIENTS,
            "chardb: generated immutable additive migration v3 (add_summary)\n",
            NO_CLIENTS,
            "chardb: generated immutable additive migration v4 (add_caption)\n",
            NO_CLIENTS,
        ]);
    });

    test("rejects gaps and edited prior artifacts before inspecting the next schema", async () => {
        const v2 = addMessageColumn(SCAFFOLD_INITIAL_SNAPSHOT, 2, "add_note", "note");
        const v3 = addMessageColumn(v2, 3, "add_summary", "summary");
        const source = fakeProject([`${stableJson(v2)}\n`]);
        const initial = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);
        source.files.set("/project/src/migrations/v1.ts", initial.versionOne);
        source.files.set("/project/src/migrations/v1.json", initial.snapshotOne);
        source.files.set("/project/src/migrations.ts", initial.journal);
        await runMigrationsGenerate(source.ctx, { name: "add_note" });

        const gap = fakeProject([`${stableJson(v3)}\n`]);
        for (const [path, contents] of source.files) gap.files.set(path, contents);
        gap.files.delete("/project/src/migrations/v2.json");
        gap.files.set("/project/src/migrations/v3.json", `${stableJson(v3)}\n`);
        gap.files.set("/project/src/migrations/v3.ts", "orphaned version three");
        await expect(runMigrationsGenerate(gap.ctx, { name: "add_caption" })).rejects.toThrow(
            /version 2 migration history is incomplete/
        );
        expect(gap.invocations).toEqual([]);

        const edited = fakeProject([`${stableJson(v3)}\n`]);
        for (const [path, contents] of source.files) edited.files.set(path, contents);
        edited.files.set("/project/src/migrations/v2.ts", "edited after deployment\n");
        await expect(runMigrationsGenerate(edited.ctx, { name: "add_summary" })).rejects.toThrow(
            /version 2 migration source differs/
        );
        expect(edited.invocations).toEqual([]);
        expect(edited.files.has("/project/src/migrations/v3.ts")).toBe(false);
        expect(edited.files.has("/project/src/migrations/v3.json")).toBe(false);

        const interrupted = fakeProject([`${stableJson(v3)}\n`]);
        for (const [path, contents] of source.files) interrupted.files.set(path, contents);
        interrupted.files.set("/project/src/migrations/v3.ts", "interrupted append\n");
        await expect(runMigrationsGenerate(interrupted.ctx, { name: "add_summary" })).rejects.toThrow(
            /version 3 migration history is incomplete/
        );
        expect(interrupted.invocations).toEqual([]);
        expect(interrupted.files.has("/project/src/migrations/v3.json")).toBe(false);

        const brokenContent: ChardbSchemaSnapshotContent = {
            format: v3.format,
            version: 3,
            name: v3.name,
            previousDigest: "0".repeat(64),
            cdbTables: v3.cdbTables,
            catalogTables: v3.catalogTables,
            resources: v3.resources,
        };
        const brokenChain = { ...brokenContent, digest: schemaSnapshotDigest(brokenContent) };
        const chained = fakeProject();
        for (const [path, contents] of source.files) chained.files.set(path, contents);
        chained.files.set("/project/src/migrations/v3.ts", "invalid chain\n");
        chained.files.set("/project/src/migrations/v3.json", `${stableJson(brokenChain)}\n`);
        await expect(runMigrationsGenerate(chained.ctx, { name: "add_caption" })).rejects.toThrow(
            /next snapshot does not name the previous digest/
        );
        expect(chained.invocations).toEqual([]);
    });

    test("a journal collision while appending v3 leaves no partial v3 history", async () => {
        const v2 = addMessageColumn(SCAFFOLD_INITIAL_SNAPSHOT, 2, "add_note", "note");
        const v3 = addMessageColumn(v2, 3, "add_summary", "summary");
        const project = fakeProject([
            `${stableJson(v2)}\n`,
            `${stableJson(v2)}\n`,
            `${stableJson(v3)}\n`,
            `${stableJson(v3)}\n`,
        ]);
        const initial = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);
        project.files.set("/project/src/migrations/v1.ts", initial.versionOne);
        project.files.set("/project/src/migrations/v1.json", initial.snapshotOne);
        project.files.set("/project/src/migrations.ts", initial.journal);
        await runMigrationsGenerate(project.ctx, { name: "add_note" });
        const v2Journal = project.files.get("/project/src/migrations.ts") ?? "";
        const collidingContext: CliContext = {
            ...project.ctx,
            async writeFilesAtomic(changes) {
                project.files.set("/project/src/migrations.ts", `${v2Journal}// concurrent edit\n`);
                await project.ctx.writeFilesAtomic?.(changes);
            },
        };

        await expect(runMigrationsGenerate(collidingContext, { name: "add_summary" })).rejects.toThrow(
            /artifact target changed/
        );
        expect(project.files.has("/project/src/migrations/v3.ts")).toBe(false);
        expect(project.files.has("/project/src/migrations/v3.json")).toBe(false);
        expect(project.files.get("/project/src/migrations.ts")).toBe(`${v2Journal}// concurrent edit\n`);
    });

    test("does not write version two when an existing column changed", async () => {
        const nextContent: ChardbSchemaSnapshotContent = {
            format: SCAFFOLD_INITIAL_SNAPSHOT.format,
            version: 2,
            name: "unsafe_change",
            previousDigest: SCAFFOLD_INITIAL_SNAPSHOT.digest,
            cdbTables: SCAFFOLD_INITIAL_SNAPSHOT.cdbTables.map(table =>
                table.tableName === "messages"
                    ? {
                          ...table,
                          columns: table.columns.map(column =>
                              column.name === "body" ? { ...column, sql: '"body" blob NOT NULL' } : column
                          ),
                      }
                    : table
            ),
            catalogTables: SCAFFOLD_INITIAL_SNAPSHOT.catalogTables,
            resources: SCAFFOLD_INITIAL_SNAPSHOT.resources,
        };
        const next = { ...nextContent, digest: schemaSnapshotDigest(nextContent) };
        const project = fakeProject([`${stableJson(next)}\n`]);
        const initial = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);
        project.files.set("/project/src/migrations/v1.ts", initial.versionOne);
        project.files.set("/project/src/migrations/v1.json", initial.snapshotOne);
        project.files.set("/project/src/migrations.ts", initial.journal);

        await expect(runMigrationsGenerate(project.ctx, { name: "unsafe_change" })).rejects.toThrow(
            /changed or reordered existing columns/
        );
        expect(project.files.has("/project/src/migrations/v2.ts")).toBe(false);
        expect(project.files.has("/project/src/migrations/v2.json")).toBe(false);
        expect(project.files.get("/project/src/migrations.ts")).toBe(initial.journal);
    });

    test("the real exclusive writer rolls back its own first link when a later target collides", async () => {
        const directory = await mkdtemp(`${tmpdir()}/chardb-migration-writer-`);
        const first = `${directory}/migrations/v1.ts`;
        const second = `${directory}/migrations.ts`;
        try {
            await writeFile(second, "existing", "utf8");
            if (!REAL_CONTEXT.writeFilesExclusive) throw new Error("real exclusive writer is unavailable");
            await expect(
                REAL_CONTEXT.writeFilesExclusive([
                    { path: first, contents: "first" },
                    { path: second, contents: "replacement" },
                ])
            ).rejects.toThrow();
            expect(await Bun.file(first).exists()).toBe(false);
            expect(await readFile(second, "utf8")).toBe("existing");
        } finally {
            await rm(directory, { recursive: true, force: true });
        }
    });

    test("the real atomic writer compares the journal before publishing new history", async () => {
        const directory = await mkdtemp(`${tmpdir()}/chardb-migration-atomic-`);
        const versionTwo = `${directory}/v2.ts`;
        const journal = `${directory}/migrations.ts`;
        try {
            await writeFile(journal, "old journal", "utf8");
            if (!REAL_CONTEXT.writeFilesAtomic) throw new Error("real atomic writer is unavailable");
            await expect(
                REAL_CONTEXT.writeFilesAtomic([
                    { path: versionTwo, contents: "version two", expectedContents: null },
                    { path: journal, contents: "new journal", expectedContents: "stale journal" },
                ])
            ).rejects.toThrow(/changed/);
            expect(await Bun.file(versionTwo).exists()).toBe(false);
            expect(await readFile(journal, "utf8")).toBe("old journal");
            expect(await readdir(directory)).toEqual(["migrations.ts"]);

            await REAL_CONTEXT.writeFilesAtomic([
                { path: versionTwo, contents: "version two", expectedContents: null },
                { path: journal, contents: "new journal", expectedContents: "old journal" },
            ]);
            expect(await readFile(versionTwo, "utf8")).toBe("version two");
            expect(await readFile(journal, "utf8")).toBe("new journal");
            expect((await readdir(directory)).sort()).toEqual(["migrations.ts", "v2.ts"]);
        } finally {
            await rm(directory, { recursive: true, force: true });
        }
    });

    test("the real child boundary rejects output beyond its byte cap", async () => {
        if (!REAL_CONTEXT.runCommand) throw new Error("real command runner is unavailable");
        await expect(
            REAL_CONTEXT.runCommand({
                executable: process.execPath,
                args: ["--eval", 'process.stdout.write("x".repeat(64))'],
                cwd: process.cwd(),
                timeoutMs: 5_000,
                maxOutputBytes: 32,
            })
        ).rejects.toThrow(/stdout exceeded 32 UTF-8 bytes/);
    });

    test("the inspector preload supplies the Worker base classes in a fresh Bun process", async () => {
        if (!REAL_CONTEXT.runCommand) throw new Error("real command runner is unavailable");
        const root = resolve(import.meta.dir, "../..");
        const result = await REAL_CONTEXT.runCommand({
            executable: process.execPath,
            args: [
                "--preload",
                `${root}/src/cli/schema-inspector-preload.ts`,
                "--eval",
                'const worker = await import("cloudflare:workers"); process.stdout.write(Object.keys(worker).sort().join(","));',
            ],
            cwd: root,
            timeoutMs: 5_000,
            maxOutputBytes: 1_024,
        });
        expect(result).toEqual({
            exitCode: 0,
            stdout: "DurableObject,WorkerEntrypoint",
            stderr: "",
        });
    });

    test("runs initial and additive inspection against real conventional TypeScript modules", async () => {
        const root = resolve(import.meta.dir, "../..");
        const project = await mkdtemp(`${root}/.migration-generate-`);
        const source = `${project}/src`;
        try {
            await mkdir(source, { recursive: true });
            await Bun.write(
                `${source}/auth.ts`,
                `import { defineAuth } from ${JSON.stringify(`${root}/src/auth/synthesize.ts`)};\nexport const auth = defineAuth({});\n`
            );
            await Bun.write(
                `${source}/schema.ts`,
                `import { text } from "drizzle-orm/sqlite-core";\nimport { globalScope } from ${JSON.stringify(`${root}/test/helpers/cdb-table.ts`)};\nconst { cdbTable } = globalScope();\nexport const notes = cdbTable("notes", { id: text("id").primaryKey() });\n`
            );
            const ctx: CliContext = {
                ...REAL_CONTEXT,
                cwd: project,
                stdout: () => {},
                stderr: () => {},
                selfCommand: { executable: process.execPath, args: [`${root}/src/cli/bin.ts`] },
            };
            await runMigrationsGenerate(ctx, { name: "initial_schema" });
            const versionOne = await readFile(`${source}/migrations/v1.ts`, "utf8");
            expect(versionOne).toContain('"tableName": "notes"');
            expect(versionOne).not.toContain("schema.ts");
            expect(versionOne).not.toContain("auth.ts");

            await Bun.write(
                `${source}/schema.ts`,
                `import { text } from "drizzle-orm/sqlite-core";\nimport { globalScope } from ${JSON.stringify(`${root}/test/helpers/cdb-table.ts`)};\nconst { cdbTable } = globalScope();\nexport const notes = cdbTable("notes", { id: text("id").primaryKey(), note: text("note") });\n`
            );
            await runMigrationsGenerate(ctx, { name: "add_note" });
            expect(await readFile(`${source}/migrations/v1.ts`, "utf8")).toBe(versionOne);
            expect(await readFile(`${source}/migrations/v2.ts`, "utf8")).toContain(
                'ALTER TABLE \\"notes\\" ADD COLUMN \\"note\\" text'
            );
            expect(await readFile(`${source}/migrations.ts`, "utf8")).toContain("migrationV2,");
        } finally {
            await rm(project, { recursive: true, force: true });
        }
    }, 20_000);
});
