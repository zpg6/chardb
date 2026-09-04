import { runBackupCreate, runBackupRestore } from "./commands/backups.ts";
import { runDoctor } from "./commands/doctor.ts";
import { runInit, validateInitDirectoryName } from "./commands/init.ts";
import { runMigrate } from "./commands/migrate.ts";
import { runShards } from "./commands/shards.ts";
import { runVectorizePrepare } from "./commands/vectorize.ts";
import type { CliContext } from "./context.ts";

const HELP = `chardb — organization database for Cloudflare Workers

Commands:
  chardb init <name> [--core-package <specifier>] [--react-package <specifier>]
                                scaffold a new chardb app directory
  chardb doctor [wrangler]      validate wrangler.toml or wrangler.jsonc
  chardb migrations generate --name <name>
                                append the next immutable additive migration
  chardb vectorize prepare      create or verify required Vectorize metadata indexes
  chardb api rust --out <file> [--check]
                                write typed chardb-client handles for the registered queries and mutations
  chardb migrate --url <worker> --id <id> --target <version> [--concurrency <1-32>] [--baseline]
  chardb backups create --url <worker> --out <file> [--at <ISO-8601>]
                                save a native Durable Object recovery point
  chardb backups restore --url <worker> --from <file>
                                restore Catalog and Cdb objects from a recovery point
`;

const EXPERIMENTAL_HELP = `chardb experimental — unstable operator commands with no compatibility promise

Commands:
  chardb experimental shards split --url <worker> --id <id> --lo <0-16383> --hi <0-16383> --to <shard> [--max-steps <1-10000>]
  chardb experimental shards status|recover|abort --url <worker> --id <id>
`;

export async function runCli(ctx: CliContext, argv: readonly string[]): Promise<number> {
    const [cmd, ...rest] = argv;
    switch (cmd) {
        case undefined:
        case "--help":
        case "-h":
            ctx.stdout(HELP);
            return 0;
        case "init": {
            const directory = rest[0];
            if (!directory) {
                ctx.stderr("usage: chardb init <name> [--core-package <specifier>] [--react-package <specifier>]\n");
                return 2;
            }
            let corePackage: string | undefined;
            let reactPackage: string | undefined;
            let valid = true;
            for (let index = 1; index < rest.length; index += 2) {
                const flag = rest[index];
                const value = rest[index + 1];
                if (!value || (flag !== "--core-package" && flag !== "--react-package")) {
                    valid = false;
                    break;
                }
                if (flag === "--core-package" && corePackage === undefined) corePackage = value;
                else if (flag === "--react-package" && reactPackage === undefined) reactPackage = value;
                else valid = false;
            }
            if (!valid || rest.length % 2 === 0) {
                ctx.stderr("usage: chardb init <name> [--core-package <specifier>] [--react-package <specifier>]\n");
                return 2;
            }
            try {
                validateInitDirectoryName(directory);
                const name = directory
                    .toLowerCase()
                    .replace(/[^a-z0-9-]+/g, "-")
                    .replace(/^-+|-+$/g, "");
                if (!name) throw new Error("project directory must have a name");
                await runInit(ctx, {
                    name,
                    directory,
                    ...(corePackage === undefined ? {} : { corePackage }),
                    ...(reactPackage === undefined ? {} : { reactPackage }),
                });
                return 0;
            } catch (error) {
                ctx.stderr(`chardb init: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "doctor": {
            const which = rest[0] ?? "wrangler";
            if (which !== "wrangler" || rest.length > 1) {
                ctx.stderr("usage: chardb doctor [wrangler]\n");
                return 2;
            }
            const r = await runDoctor(ctx);
            return r.ok ? 0 : 1;
        }
        case "migrations": {
            if (rest.length !== 3 || rest[0] !== "generate" || rest[1] !== "--name" || !rest[2]) {
                ctx.stderr("usage: chardb migrations generate --name <name>\n");
                return 2;
            }
            try {
                const { runMigrationsGenerate } = await import("./commands/migrations-generate.ts");
                await runMigrationsGenerate(ctx, { name: rest[2] });
                return 0;
            } catch (error) {
                ctx.stderr(`chardb migrations generate: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "api": {
            const out = rest[1] === "--out" ? rest[2] : undefined;
            const check = rest[3] === "--check";
            if (rest[0] !== "rust" || !out || rest.length !== (check ? 4 : 3)) {
                ctx.stderr("usage: chardb api rust --out <file> [--check]\n");
                return 2;
            }
            try {
                const { runApiRust } = await import("./commands/api-rust.ts");
                await runApiRust(ctx, { out, check });
                return 0;
            } catch (error) {
                ctx.stderr(`chardb api rust: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "__api-inspect": {
            if (rest.length !== 0) return 2;
            try {
                const { runApiInspect } = await import("./commands/api-rust.ts");
                await runApiInspect(ctx);
                return 0;
            } catch (error) {
                ctx.stderr(`api inspection failed: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "__migrations-inspect": {
            if (rest.length !== 3) return 2;
            try {
                const { runMigrationsInspect } = await import("./commands/migrations-inspect.ts");
                const version = Number(rest[1]);
                await runMigrationsInspect(
                    ctx,
                    rest[0] as string,
                    version,
                    rest[2] === "-" ? null : (rest[2] as string)
                );
                return 0;
            } catch (error) {
                ctx.stderr(`schema inspection failed: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "vectorize": {
            if (rest.length !== 1 || rest[0] !== "prepare") {
                ctx.stderr("usage: chardb vectorize prepare\n");
                return 2;
            }
            try {
                await runVectorizePrepare(ctx);
                return 0;
            } catch (error) {
                ctx.stderr(`chardb vectorize prepare: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "migrate": {
            const baseUrl = valueAfterFlag(rest, "--url") ?? ctx.env.CHARDB_URL;
            const migrationId = valueAfterFlag(rest, "--id");
            const rawTarget = valueAfterFlag(rest, "--target");
            const rawConcurrency = valueAfterFlag(rest, "--concurrency") ?? "4";
            const token = ctx.env.CHARDB_ADMIN_TOKEN;
            if (!baseUrl || !migrationId || !rawTarget || !token || !ctx.fetch) {
                ctx.stderr(
                    "usage: CHARDB_ADMIN_TOKEN=<secret> chardb migrate --url <worker> --id <id> --target <version> [--concurrency <1-32>] [--baseline]\n"
                );
                return 2;
            }
            const targetVersion = Number(rawTarget);
            const concurrency = Number(rawConcurrency);
            try {
                await runMigrate(ctx, {
                    baseUrl,
                    token,
                    migrationId,
                    targetVersion,
                    concurrency,
                    baseline: rest.includes("--baseline"),
                    fetch: ctx.fetch,
                });
                return 0;
            } catch (error) {
                ctx.stderr(`chardb migrate: ${error instanceof Error ? error.message : String(error)}\n`);
                return 1;
            }
        }
        case "backups": {
            const action = rest[0];
            const args = rest.slice(1);
            const validArguments =
                (action === "create" && exactFlagPairs(args, ["--url", "--out", "--at"])) ||
                (action === "restore" && exactFlagPairs(args, ["--url", "--from"]));
            if (!validArguments) {
                ctx.stderr(backupUsage());
                return 2;
            }
            const baseUrl = valueAfterFlag(args, "--url") ?? ctx.env.CHARDB_URL;
            const token = ctx.env.CHARDB_ADMIN_TOKEN;
            if (!baseUrl || !token || !ctx.fetch) {
                ctx.stderr(backupUsage());
                return 2;
            }
            try {
                if (action === "create") {
                    const out = valueAfterFlag(args, "--out");
                    const rawAt = valueAfterFlag(args, "--at");
                    if (!out) {
                        ctx.stderr(backupUsage());
                        return 2;
                    }
                    const atMs = rawAt === undefined ? undefined : parseIsoTimestamp(rawAt);
                    if (rawAt !== undefined && !Number.isSafeInteger(atMs)) {
                        throw new Error("--at must be an ISO-8601 timestamp");
                    }
                    await runBackupCreate(ctx, {
                        baseUrl,
                        token,
                        fetch: ctx.fetch,
                        out,
                        ...(atMs === undefined ? {} : { atMs }),
                    });
                } else if (action === "restore") {
                    const from = valueAfterFlag(args, "--from");
                    if (!from) {
                        ctx.stderr(backupUsage());
                        return 2;
                    }
                    await runBackupRestore(ctx, { baseUrl, token, fetch: ctx.fetch, from });
                } else {
                    ctx.stderr(backupUsage());
                    return 2;
                }
                return 0;
            } catch (error) {
                ctx.stderr(
                    `chardb backups ${action ?? ""}: ${error instanceof Error ? error.message : String(error)}\n`
                );
                return 1;
            }
        }
        case "shards": {
            ctx.stderr(
                "chardb shards moved to chardb experimental shards; the old command is disabled and did not run\n"
            );
            return 2;
        }
        case "experimental": {
            if (rest.length === 0 || rest[0] === "--help" || rest[0] === "-h") {
                ctx.stdout(EXPERIMENTAL_HELP);
                return 0;
            }
            if (rest[0] !== "shards") {
                ctx.stderr(`unknown experimental command: ${rest[0]}\n`);
                ctx.stdout(EXPERIMENTAL_HELP);
                return 2;
            }
            const action = rest[1];
            const args = rest.slice(2);
            const baseUrl = valueAfterFlag(args, "--url") ?? ctx.env.CHARDB_URL;
            const migrationId = valueAfterFlag(args, "--id");
            const token = ctx.env.CHARDB_ADMIN_TOKEN;
            if (
                (action !== "split" && action !== "status" && action !== "recover" && action !== "abort") ||
                !baseUrl ||
                !migrationId ||
                !token ||
                !ctx.fetch
            ) {
                ctx.stderr(
                    "usage: CHARDB_ADMIN_TOKEN=<secret> chardb experimental shards split --url <worker> --id <id> --lo <0-16383> --hi <0-16383> --to <shard> [--max-steps <1-10000>]\n" +
                        "   or: CHARDB_ADMIN_TOKEN=<secret> chardb experimental shards status|recover|abort --url <worker> --id <id>\n"
                );
                return 2;
            }
            try {
                if (action === "split") {
                    const lo = valueAfterFlag(args, "--lo");
                    const hi = valueAfterFlag(args, "--hi");
                    const toShard = valueAfterFlag(args, "--to");
                    if (lo === undefined || hi === undefined || !toShard) {
                        ctx.stderr(
                            "usage: CHARDB_ADMIN_TOKEN=<secret> chardb experimental shards split --url <worker> --id <id> --lo <0-16383> --hi <0-16383> --to <shard> [--max-steps <1-10000>]\n"
                        );
                        return 2;
                    }
                    await runShards(ctx, {
                        cmd: "split",
                        baseUrl,
                        token,
                        migrationId,
                        vshardLo: Number(lo),
                        vshardHi: Number(hi),
                        toShard,
                        maxSteps: Number(valueAfterFlag(args, "--max-steps") ?? "512"),
                        fetch: ctx.fetch,
                    });
                } else {
                    await runShards(ctx, { cmd: action, baseUrl, token, migrationId, fetch: ctx.fetch });
                }
                return 0;
            } catch (error) {
                ctx.stderr(
                    `chardb experimental shards ${action}: ${error instanceof Error ? error.message : String(error)}\n`
                );
                return 1;
            }
        }
        default:
            ctx.stderr(`unknown command: ${cmd}\n`);
            ctx.stdout(HELP);
            return 2;
    }
}

function backupUsage(): string {
    return (
        "usage: CHARDB_ADMIN_TOKEN=<secret> chardb backups create --url <worker> --out <file> [--at <ISO-8601>]\n" +
        "   or: CHARDB_ADMIN_TOKEN=<secret> chardb backups restore --url <worker> --from <file>\n"
    );
}

function exactFlagPairs(argv: readonly string[], allowed: readonly string[]): boolean {
    if (argv.length % 2 !== 0) return false;
    const seen = new Set<string>();
    for (let index = 0; index < argv.length; index += 2) {
        const flag = argv[index];
        const value = argv[index + 1];
        if (!flag || !allowed.includes(flag) || seen.has(flag) || !value || value.startsWith("--")) return false;
        seen.add(flag);
    }
    return true;
}

function parseIsoTimestamp(value: string): number {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) return Number.NaN;
    return Date.parse(value);
}

function valueAfterFlag(argv: readonly string[], flag: string): string | undefined {
    const index = argv.indexOf(flag);
    return index >= 0 ? argv[index + 1] : undefined;
}
