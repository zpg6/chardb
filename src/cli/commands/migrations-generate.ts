import { type ChardbSchemaSnapshotInput, defineSchemaSnapshot } from "../../server/schema-snapshot.ts";
import { stableJson } from "../../util/canonical.ts";
import type { CliCommandResult, CliContext } from "../context.ts";
import { runInspection } from "../inspector.ts";
import { renderAdditiveMigrationArtifacts, renderInitialMigrationArtifacts } from "../migration-artifacts.ts";
import { diffAdditiveSchemaSnapshots } from "../schema-snapshot-diff.ts";

const MIGRATION_NAME = /^[a-z0-9][a-z0-9_-]{0,127}$/;

export interface MigrationsGenerateOptions {
    readonly name: string;
}

interface StoredMigrationHistory {
    readonly latest: ChardbSchemaSnapshotInput;
    readonly journal: string;
}

function parseInspection(result: CliCommandResult): {
    readonly bytes: string;
    readonly snapshot: ChardbSchemaSnapshotInput;
} {
    if (result.exitCode !== 0) {
        const detail = result.stderr.trim().replaceAll(/\s+/g, " ").slice(0, 2_048);
        throw new Error(
            detail.length > 0
                ? `schema inspection failed in a fresh Bun process: ${detail}`
                : "schema inspection failed in a fresh Bun process"
        );
    }
    if (result.stderr.length > 0) throw new Error("schema inspector wrote to stderr");
    if (!result.stdout.endsWith("\n") || result.stdout.slice(0, -1).includes("\n")) {
        throw new Error("schema inspector returned malformed output");
    }
    const bytes = result.stdout.slice(0, -1);
    let parsed: unknown;
    try {
        parsed = JSON.parse(bytes);
    } catch {
        throw new Error("schema inspector returned malformed JSON");
    }
    const snapshot = defineSchemaSnapshot(parsed as ChardbSchemaSnapshotInput);
    if (stableJson(parsed) !== bytes) throw new Error("schema inspector output was not canonical");
    const input: ChardbSchemaSnapshotInput = {
        format: snapshot.format,
        version: snapshot.version,
        name: snapshot.name,
        previousDigest: snapshot.previousDigest,
        cdbTables: snapshot.cdbTables,
        catalogTables: snapshot.catalogTables,
        resources: snapshot.resources,
        digest: snapshot.digest,
    };
    return Object.freeze({ bytes, snapshot: Object.freeze(input) });
}

function parseStoredSnapshot(bytes: string, version: number): ChardbSchemaSnapshotInput {
    if (!bytes.endsWith("\n") || bytes.slice(0, -1).includes("\n")) {
        throw new Error(`immutable v${version} snapshot is not canonical JSON`);
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(bytes.slice(0, -1));
    } catch {
        throw new Error(`immutable v${version} snapshot is malformed JSON`);
    }
    const snapshot = defineSchemaSnapshot(parsed as ChardbSchemaSnapshotInput);
    if (snapshot.version !== version || stableJson(parsed) !== bytes.slice(0, -1)) {
        throw new Error(`immutable v${version} snapshot is not canonical JSON`);
    }
    return Object.freeze({
        format: snapshot.format,
        version: snapshot.version,
        name: snapshot.name,
        previousDigest: snapshot.previousDigest,
        cdbTables: snapshot.cdbTables,
        catalogTables: snapshot.catalogTables,
        resources: snapshot.resources,
        digest: snapshot.digest,
    });
}

async function inspectTwice(
    ctx: CliContext,
    name: string,
    version: number,
    previousDigest: string | null
): Promise<ChardbSchemaSnapshotInput> {
    const subcommand = ["__migrations-inspect", name, String(version), previousDigest ?? "-"];
    const first = parseInspection(await runInspection(ctx, subcommand));
    const second = parseInspection(await runInspection(ctx, subcommand));
    if (first.snapshot.name !== name || second.snapshot.name !== name || first.snapshot.version !== version) {
        throw new Error("schema inspector returned the wrong migration identity");
    }
    if (first.bytes !== second.bytes) {
        throw new Error("schema inspection is nondeterministic across fresh Bun processes");
    }
    return first.snapshot;
}

async function readStoredHistory(
    ctx: CliContext,
    journalPath: string,
    migrationDirectory: string
): Promise<StoredMigrationHistory> {
    if (!ctx.readDirectory) throw new Error("migration directory inspection is unavailable");
    const names = await ctx.readDirectory(migrationDirectory);
    const versions = new Map<number, Set<"json" | "ts">>();
    for (const name of names) {
        const match = /^v([1-9][0-9]*)\.(json|ts)$/.exec(name);
        if (!match) {
            if (/^v[0-9]+\.(?:json|ts)$/.test(name)) {
                throw new Error(`migration history contains noncanonical artifact ${name}`);
            }
            continue;
        }
        const version = Number(match[1]);
        if (!Number.isSafeInteger(version) || version > 1_024) {
            throw new Error(`migration history contains invalid version ${match[1]}`);
        }
        const kind = match[2] as "json" | "ts";
        const artifacts = versions.get(version) ?? new Set<"json" | "ts">();
        artifacts.add(kind);
        versions.set(version, artifacts);
    }
    const latestVersion = Math.max(0, ...versions.keys());
    if (latestVersion < 1) {
        throw new Error("version 1 history lacks its immutable JSON snapshot; regenerate before deployment");
    }
    for (let version = 1; version <= latestVersion; version++) {
        const artifacts = versions.get(version);
        if (!artifacts?.has("json") || !artifacts.has("ts")) {
            throw new Error(`version ${version} migration history is incomplete`);
        }
    }

    const storedJournal = await ctx.read(journalPath);
    let previous = parseStoredSnapshot(await ctx.read(`${migrationDirectory}/v1.json`), 1);
    if (!defineSchemaSnapshot(previous).initialMigration) {
        throw new Error("immutable v1 snapshot is not an initial snapshot");
    }
    const initialArtifacts = renderInitialMigrationArtifacts(previous);
    if ((await ctx.read(`${migrationDirectory}/v1.ts`)) !== initialArtifacts.versionOne) {
        throw new Error("version 1 migration source differs from its immutable JSON snapshot");
    }
    let expectedJournal = initialArtifacts.journal;
    for (let version = 2; version <= latestVersion; version++) {
        const next = parseStoredSnapshot(await ctx.read(`${migrationDirectory}/v${version}.json`), version);
        const migration = diffAdditiveSchemaSnapshots(previous, next);
        const artifacts = renderAdditiveMigrationArtifacts(next, migration);
        if ((await ctx.read(`${migrationDirectory}/v${version}.ts`)) !== artifacts.versionFile) {
            throw new Error(`version ${version} migration source differs from its immutable JSON snapshot`);
        }
        expectedJournal = artifacts.journal;
        previous = next;
    }
    if (storedJournal !== expectedJournal) {
        throw new Error("migration journal differs from its immutable JSON history");
    }
    return Object.freeze({ latest: previous, journal: storedJournal });
}

export async function runMigrationsGenerate(ctx: CliContext, options: MigrationsGenerateOptions): Promise<void> {
    if (!MIGRATION_NAME.test(options.name)) {
        throw new Error("migration name must match [a-z0-9][a-z0-9_-]{0,127}");
    }
    const journalPath = `${ctx.cwd}/src/migrations.ts`;
    const migrationDirectory = `${ctx.cwd}/src/migrations`;
    const versionOnePath = `${migrationDirectory}/v1.ts`;
    const snapshotOnePath = `${migrationDirectory}/v1.json`;
    const journalExists = await ctx.exists(journalPath);
    if (!(await ctx.exists(`${ctx.cwd}/src/schema.ts`))) throw new Error("src/schema.ts does not exist");
    if (!(await ctx.exists(`${ctx.cwd}/src/auth.ts`))) throw new Error("src/auth.ts does not exist");
    if (!journalExists) {
        if (
            (await ctx.exists(migrationDirectory)) ||
            (await ctx.exists(versionOnePath)) ||
            (await ctx.exists(snapshotOnePath))
        ) {
            throw new Error("partial migration history already exists");
        }
        if (!ctx.writeFilesExclusive) throw new Error("exclusive migration artifact writes are unavailable");
        const snapshot = await inspectTwice(ctx, options.name, 1, null);
        if (!defineSchemaSnapshot(snapshot).initialMigration) {
            throw new Error("schema inspector did not return an initial snapshot");
        }
        const artifacts = renderInitialMigrationArtifacts(snapshot);
        await ctx.writeFilesExclusive([
            { path: versionOnePath, contents: artifacts.versionOne },
            { path: snapshotOnePath, contents: artifacts.snapshotOne },
            { path: journalPath, contents: artifacts.journal },
        ]);
        ctx.stdout(`chardb: generated immutable migration v1 (${options.name})\n`);
        return;
    }

    if (!(await ctx.exists(versionOnePath)) || !(await ctx.exists(snapshotOnePath))) {
        throw new Error("version 1 history lacks its immutable JSON snapshot; regenerate before deployment");
    }
    if (!ctx.writeFilesAtomic) throw new Error("atomic migration history writes are unavailable");
    const history = await readStoredHistory(ctx, journalPath, migrationDirectory);
    const nextVersion = history.latest.version + 1;
    if (nextVersion > 1_024) throw new Error("migration history has reached the maximum version 1024");
    const versionPath = `${migrationDirectory}/v${nextVersion}.ts`;
    const snapshotPath = `${migrationDirectory}/v${nextVersion}.json`;
    const next = await inspectTwice(ctx, options.name, nextVersion, history.latest.digest);
    const migration = diffAdditiveSchemaSnapshots(history.latest, next);
    const artifacts = renderAdditiveMigrationArtifacts(next, migration);
    await ctx.writeFilesAtomic([
        { path: versionPath, contents: artifacts.versionFile, expectedContents: null },
        { path: snapshotPath, contents: artifacts.snapshotFile, expectedContents: null },
        { path: journalPath, contents: artifacts.journal, expectedContents: history.journal },
    ]);
    ctx.stdout(`chardb: generated immutable additive migration v${nextVersion} (${options.name})\n`);
}
