import { describe, expect, test } from "bun:test";
import { runDoctor } from "../../src/cli/commands/doctor.ts";
import { runInit } from "../../src/cli/commands/init.ts";
import type { CliContext } from "../../src/cli/context.ts";
import {
    checkWrangler,
    configuredVectorizeIndexNames,
    renderWrangler,
    renderWranglerJsonc,
} from "../../src/cli/wrangler_template.ts";

function fakeCtx(): { ctx: CliContext; files: Map<string, string>; out: string[]; err: string[] } {
    const files = new Map<string, string>();
    const out: string[] = [];
    const err: string[] = [];
    const ctx: CliContext = {
        cwd: "/tmp/proj",
        env: {},
        stdout: s => out.push(s),
        stderr: s => err.push(s),
        async read(p) {
            const v = files.get(p);
            if (v === undefined) throw new Error(`ENOENT: ${p}`);
            return v;
        },
        async write(p, contents) {
            files.set(p, contents);
        },
        async exists(p) {
            return files.has(p);
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
            for (const artifact of artifacts) files.set(artifact.path, artifact.contents);
        },
    };
    return { ctx, files, out, err };
}

describe("renderWrangler / checkWrangler", () => {
    test("renderWrangler emits a complete config that doctor accepts", () => {
        const text = renderWrangler({
            name: "myapp",
            compatibilityDate: "2026-05-10",
            assetsDir: ".chardb/dashboard",
        });
        const r = checkWrangler(text);
        expect(r.ok).toBe(true);
        expect(r.errors).toEqual([]);
        expect(r.warnings).toEqual([]);
        const config = Bun.TOML.parse(text) as {
            services?: unknown;
            durable_objects?: { bindings?: { name?: string; class_name?: string }[] };
            migrations?: { new_sqlite_classes?: string[] }[];
            r2_buckets?: unknown;
            vectorize?: unknown;
            queues?: unknown;
            triggers?: unknown;
            tail_consumers?: unknown;
            compatibility_flags?: unknown;
            assets: { run_worker_first?: unknown };
        };
        expect(config.services).toBeUndefined();
        expect(config.durable_objects?.bindings).toEqual([
            { name: "CDB_CATALOG", class_name: "Catalog" },
            { name: "CDB_SHARD", class_name: "Cdb" },
            { name: "CDB_GATEWAY", class_name: "Gateway" },
            { name: "CDB_RESHARD", class_name: "Resharder" },
        ]);
        expect(config.migrations?.[0]?.new_sqlite_classes).toEqual(["Cdb", "Catalog", "Gateway", "Resharder"]);
        expect(config.r2_buckets).toBeUndefined();
        expect(config.vectorize).toBeUndefined();
        expect(config.queues).toBeUndefined();
        expect(config.triggers).toBeUndefined();
        expect(config.tail_consumers).toBeUndefined();
        expect(config.compatibility_flags).toEqual(["nodejs_compat"]);
        expect(config.assets.run_worker_first).toEqual(["/_chardb/*", "/api/*", "/health", "/ws"]);
    });

    test("checkWrangler reports each missing Durable Object migration", () => {
        const r = checkWrangler('{"name":"x","main":"y","compatibility_date":"2026-05-10"}');
        expect(r.ok).toBe(false);
        expect(r.errors.some(e => e.includes('"Catalog"'))).toBe(true);
        expect(r.errors.some(e => e.includes('"Cdb"'))).toBe(true);
        expect(r.errors.some(e => e.includes('"Resharder"'))).toBe(true);
    });

    test("requires native loopback compatibility by date or explicit flag", () => {
        const oldConfig = JSON.parse(
            renderWranglerJsonc({ name: "old", compatibilityDate: "2025-11-16", assetsDir: "public" })
        );
        expect(checkWrangler(JSON.stringify(oldConfig)).errors).toContain(
            'native loopback exports require compatibility_date >= "2025-11-17" or compatibility_flags to include "enable_ctx_exports"'
        );

        oldConfig.compatibility_flags.push("enable_ctx_exports");
        expect(checkWrangler(JSON.stringify(oldConfig)).ok).toBe(true);

        oldConfig.compatibility_flags.push("disable_ctx_exports");
        expect(checkWrangler(JSON.stringify(oldConfig)).ok).toBe(false);
    });

    test("supports inline and block JSONC comments without changing string contents", () => {
        const config = JSON.parse(
            renderWranglerJsonc({
                name: "x",
                compatibilityDate: "2026-05-10",
                assetsDir: ".chardb/dashboard",
            })
        );
        const literalIndex = "https://vectors.example/*literal*/?query=//still-a-string";
        config.vectorize = [{ binding: "CDB_MESSAGES", index_name: literalIndex, remote: true }];
        let text = JSON.stringify(config, null, 2)
            .replace('"main": "src/worker.ts",', '"main": "src/worker.ts", // Worker entry')
            .replace('"compatibility_date": "2026-05-10",', '"compatibility_date": /* pinned */ "2026-05-10",')
            .replace('"nodejs_compat"\n  ]', '"nodejs_compat",\n  ]')
            .replace(/\n}$/, ",\n}");
        text = `/* header */\n${text}\n// trailing comment`;

        const r = checkWrangler(text, { requiredVectorBindings: ["CDB_MESSAGES"] });
        expect(r.ok).toBe(true);
        expect(configuredVectorizeIndexNames(text)).toEqual([literalIndex]);
    });

    test("reports malformed JSONC containers and entries instead of throwing", () => {
        const config = JSON.parse(
            renderWranglerJsonc({
                name: "x",
                compatibilityDate: "2026-05-10",
                assetsDir: ".chardb/dashboard",
            })
        );
        config.compatibility_flags = "nodejs_compat";
        config.migrations = [null, { new_sqlite_classes: "Cdb" }];
        config.durable_objects.bindings = [null];
        config.r2_buckets = {};
        config.assets.run_worker_first = "/_chardb/*";

        expect(() => checkWrangler(JSON.stringify(config))).not.toThrow();
        expect(checkWrangler(JSON.stringify(config)).errors).toEqual(
            expect.arrayContaining([
                "compatibility_flags must be an array of strings",
                "migrations[0] must be an object",
                "migrations[1].new_sqlite_classes must be an array of strings",
                "durable_objects.bindings[0] must be an object",
                "r2_buckets must be an array of binding entries",
                "assets.run_worker_first must be an array of strings",
            ])
        );
        expect(checkWrangler("[]").errors).toEqual(["Wrangler config must be an object"]);
    });

    test("reports malformed TOML containers instead of throwing", () => {
        const base = renderWrangler({
            name: "x",
            compatibilityDate: "2026-05-10",
            assetsDir: ".chardb/dashboard",
            filesBucket: "x-files",
        });
        const scalarArrays = base
            .replace('compatibility_flags = ["nodejs_compat"]', 'compatibility_flags = "nodejs_compat"')
            .replace(
                'run_worker_first = ["/_chardb/*", "/api/*", "/health", "/ws"]',
                'run_worker_first = "/_chardb/*"'
            );
        const objectMigrations = base.replace("[[migrations]]", "[migrations]");
        const objectR2 = base.replace("[[r2_buckets]]", "[r2_buckets]");

        expect(() => checkWrangler(scalarArrays)).not.toThrow();
        expect(checkWrangler(scalarArrays).errors).toEqual(
            expect.arrayContaining([
                "compatibility_flags must be an array of strings",
                "assets.run_worker_first must be an array of strings",
            ])
        );
        expect(checkWrangler(objectMigrations).errors).toContain("migrations must be an array");
        expect(checkWrangler(objectR2).errors).toContain("r2_buckets must be an array of binding entries");
    });

    test("renderWranglerJsonc provisions the Resharder class", () => {
        const config = JSON.parse(
            renderWranglerJsonc({ name: "x", compatibilityDate: "2026-05-10", assetsDir: "public" })
        );

        expect(config.migrations[0].new_sqlite_classes).toEqual(["Cdb", "Catalog", "Gateway", "Resharder"]);
        expect(config.durable_objects.bindings).toEqual([
            { name: "CDB_CATALOG", class_name: "Catalog" },
            { name: "CDB_SHARD", class_name: "Cdb" },
            { name: "CDB_GATEWAY", class_name: "Gateway" },
            { name: "CDB_RESHARD", class_name: "Resharder" },
        ]);
    });

    test("requires each internal Durable Object binding exactly once with its matching class", () => {
        const config = JSON.parse(
            renderWranglerJsonc({ name: "x", compatibilityDate: "2026-05-10", assetsDir: "public" })
        );
        const missing = structuredClone(config);
        missing.durable_objects.bindings = missing.durable_objects.bindings.filter(
            (binding: { name: string }) => binding.name !== "CDB_SHARD"
        );
        expect(checkWrangler(JSON.stringify(missing)).errors).toContain(
            'durable_objects.bindings must contain exactly one "CDB_SHARD" binding'
        );

        const duplicate = structuredClone(config);
        duplicate.durable_objects.bindings.push({ name: "CDB_CATALOG", class_name: "Catalog" });
        expect(checkWrangler(JSON.stringify(duplicate)).errors).toContain(
            'durable_objects.bindings must contain only one "CDB_CATALOG" binding'
        );

        const miswired = structuredClone(config);
        miswired.durable_objects.bindings.find(
            (binding: { name: string }) => binding.name === "CDB_GATEWAY"
        ).class_name = "Cdb";
        expect(checkWrangler(JSON.stringify(miswired)).errors).toContain(
            'the "CDB_GATEWAY" Durable Object binding must use class_name "Gateway"'
        );

        const invalid = structuredClone(config);
        invalid.durable_objects.bindings = {};
        expect(checkWrangler(JSON.stringify(invalid)).errors).toContain("durable_objects.bindings must be an array");
    });

    test("rejects cross-Worker routing fields on internal bindings in TOML and JSONC", () => {
        const jsonc = JSON.parse(
            renderWranglerJsonc({ name: "x", compatibilityDate: "2026-05-10", assetsDir: "public" })
        );
        jsonc.durable_objects.bindings.find((binding: { name: string }) => binding.name === "CDB_SHARD").script_name =
            "other-worker";
        expect(checkWrangler(JSON.stringify(jsonc)).errors).toContain(
            'the "CDB_SHARD" Durable Object binding must be same-Worker and contain only name and class_name'
        );

        const toml = renderWrangler({
            name: "x",
            compatibilityDate: "2026-05-10",
            assetsDir: "public",
        }).replace(
            'name = "CDB_CATALOG"\nclass_name = "Catalog"',
            'name = "CDB_CATALOG"\nclass_name = "Catalog"\nenvironment = "staging"'
        );
        expect(checkWrangler(toml).errors).toContain(
            'the "CDB_CATALOG" Durable Object binding must be same-Worker and contain only name and class_name'
        );
    });

    test("accepts Resharder in an appended migration for deployed projects", () => {
        const config = JSON.parse(
            renderWranglerJsonc({ name: "x", compatibilityDate: "2026-05-10", assetsDir: "public" })
        );
        config.migrations = [
            { tag: "init", new_sqlite_classes: ["Cdb", "Catalog", "Gateway"] },
            { tag: "resharder", new_sqlite_classes: ["Resharder"] },
        ];

        expect(checkWrangler(JSON.stringify(config)).ok).toBe(true);
    });

    test("checkWrangler warns only about missing live reserved routes", () => {
        const cfg = JSON.parse(
            renderWranglerJsonc({
                name: "x",
                compatibilityDate: "2026-05-10",
                assetsDir: ".chardb/dashboard",
            })
        );
        cfg.assets.run_worker_first = [];

        const result = checkWrangler(JSON.stringify(cfg));

        expect(result.warnings).toEqual([
            "assets.run_worker_first should include reserved chardb routes (/_chardb/*,/api/*,/health,/ws)",
        ]);
        expect(result.warnings.join("")).not.toMatch(/\/q|\/f|\/p|\/s,/);
    });

    test("supports one fixed native file bucket in TOML and JSONC", () => {
        const input = {
            name: "files",
            compatibilityDate: "2026-05-10",
            assetsDir: "public",
            filesBucket: "files-preview",
        };
        const toml = renderWrangler(input);
        expect(Bun.TOML.parse(toml)).toMatchObject({
            r2_buckets: [{ binding: "CDB_FILES", bucket_name: "files-preview" }],
        });
        expect(checkWrangler(toml, { requireFilesBinding: true }).ok).toBe(true);
        expect(checkWrangler(renderWranglerJsonc(input), { requireFilesBinding: true }).ok).toBe(true);

        const withoutFiles = renderWrangler({
            name: input.name,
            compatibilityDate: input.compatibilityDate,
            assetsDir: input.assetsDir,
        });
        expect(checkWrangler(withoutFiles).ok).toBe(true);
        expect(checkWrangler(withoutFiles, { requireFilesBinding: true }).errors).toContain(
            'file columns require an r2_buckets binding named "CDB_FILES"'
        );
    });

    test("validates required vector bindings in TOML and JSONC without enabling vector runtime", () => {
        const base = { name: "vectors", compatibilityDate: "2026-05-10", assetsDir: "public" };
        const toml = `${renderWrangler(base)}

[[vectorize]]
binding = "CDB_MESSAGES"
index_name = "messages-index"
remote = true`;
        const jsonc = JSON.parse(renderWranglerJsonc(base));
        jsonc.vectorize = [{ binding: "CDB_MESSAGES", index_name: "messages-index", remote: true }];

        expect(checkWrangler(toml, { requiredVectorBindings: ["CDB_MESSAGES"] }).ok).toBe(true);
        expect(checkWrangler(JSON.stringify(jsonc), { requiredVectorBindings: ["CDB_MESSAGES"] }).ok).toBe(true);
        expect(checkWrangler(toml).warnings).toContain(
            'Vectorize index "messages-index" requires the string metadata index "cdb_resource"; run `chardb vectorize prepare` to create or verify it'
        );
        expect(checkWrangler(toml).warnings).toContain(
            'Vectorize binding "CDB_MESSAGES" uses the real remote Cloudflare index "messages-index" during wrangler dev; Miniflare does not emulate Vectorize and provider usage may be billed'
        );
        expect(checkWrangler(renderWrangler(base), { requiredVectorBindings: ["CDB_MESSAGES"] }).errors).toContain(
            'vector resources require a vectorize binding named "CDB_MESSAGES"'
        );

        jsonc.vectorize.push({ binding: "CDB_MESSAGES", index_name: "duplicate-index", remote: true });
        expect(checkWrangler(JSON.stringify(jsonc)).errors).toContain(
            'vectorize must contain only one "CDB_MESSAGES" binding'
        );
        jsonc.vectorize = [{ binding: "CDB_MESSAGES", index_name: "", remote: true }];
        expect(checkWrangler(JSON.stringify(jsonc)).errors).toContain(
            'the "CDB_MESSAGES" vectorize binding requires a nonempty index_name'
        );
        jsonc.vectorize = { binding: "CDB_MESSAGES", index_name: "messages-index", remote: true };
        expect(checkWrangler(JSON.stringify(jsonc)).errors).toContain("vectorize must be an array of binding entries");
        jsonc.vectorize = [null];
        expect(checkWrangler(JSON.stringify(jsonc)).errors).toContain("vectorize entries must be objects");
        expect(checkWrangler(renderWrangler(base), { requiredVectorBindings: ["bad-binding"] }).errors).toContain(
            "required vector bindings must be unique valid Worker binding names"
        );
        expect(
            checkWrangler(renderWrangler(base), { requiredVectorBindings: ["CDB_MESSAGES", "CDB_MESSAGES"] }).errors
        ).toContain("required vector bindings must be unique valid Worker binding names");
    });
});

describe("chardb init + doctor end-to-end", () => {
    test("init writes wrangler.toml + scaffolding; doctor passes", async () => {
        const { ctx, files } = fakeCtx();
        await runInit(ctx, { name: "myapp" });
        expect(files.has("/tmp/proj/package.json")).toBe(true);
        expect(files.has("/tmp/proj/tsconfig.json")).toBe(true);
        expect(files.has("/tmp/proj/.gitignore")).toBe(true);
        expect(files.has("/tmp/proj/README.md")).toBe(true);
        expect(files.has("/tmp/proj/wrangler.toml")).toBe(true);
        expect(files.has("/tmp/proj/wrangler.jsonc")).toBe(false);
        expect(files.has("/tmp/proj/src/auth.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/schema.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/api.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/queries.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/migrations.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/migrations/v1.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/worker.ts")).toBe(true);
        expect(files.has("/tmp/proj/src/web/App.tsx")).toBe(true);
        expect(files.has("/tmp/proj/src/web/main.tsx")).toBe(true);
        expect(files.has("/tmp/proj/src/web/styles.css")).toBe(true);
        expect(files.has("/tmp/proj/vite.config.ts")).toBe(true);
        expect(files.has("/tmp/proj/vitest.config.ts")).toBe(true);
        expect(files.has("/tmp/proj/test/tsconfig.json")).toBe(true);
        expect(files.has("/tmp/proj/test/env.d.ts")).toBe(true);
        expect(files.has("/tmp/proj/test/worker.test.ts")).toBe(true);
        expect(files.has("/tmp/proj/scripts/build.mjs")).toBe(true);
        expect(files.has("/tmp/proj/scripts/test.mjs")).toBe(true);
        expect(files.has("/tmp/proj/scripts/dev.mjs")).toBe(true);
        expect(files.has("/tmp/proj/index.html")).toBe(true);
        expect(files.has("/tmp/proj/public/.gitkeep")).toBe(true);
        expect(JSON.parse(files.get("/tmp/proj/package.json") ?? "")).toMatchObject({
            packageManager: "bun@1.2.22",
            dependencies: { "@chardb/core": "0.1.0" },
            devDependencies: {
                "@cloudflare/vitest-plugin": "1.1.2",
                "@cloudflare/workers-types": "5.20260820.1",
                "@msw/cloudflare": "0.0.1",
                msw: "2.15.0",
                vitest: "4.1.11",
                wrangler: "4.125.0",
            },
            scripts: {
                typecheck: "tsc --noEmit && tsc --noEmit -p test/tsconfig.json",
                test: "bun scripts/test.mjs",
                build: "bun scripts/build.mjs",
                "build:web": "vite build",
                "build:worker": "wrangler deploy --dry-run --outdir dist/worker",
                dev: "bun scripts/dev.mjs",
                "dev:web": "vite",
            },
        });
        expect(JSON.parse(files.get("/tmp/proj/tsconfig.json") ?? "").compilerOptions).not.toHaveProperty("paths");
        // Worker template must be specialised to the app name.
        expect(files.get("/tmp/proj/src/auth.ts")).toContain('appName: "myapp"');
        expect(files.get("/tmp/proj/src/worker.ts")).toContain("{ DB, Catalog, Cdb, Gateway, Resharder }");
        expect(files.get("/tmp/proj/src/worker.ts")).not.toContain("ChardbWorker");
        expect(files.get("/tmp/proj/wrangler.toml")).not.toContain("CDB_WORKER");
        expect(files.get("/tmp/proj/src/schema.ts")).toContain("const { cdbTable } = forOrg(auth)");
        expect(files.get("/tmp/proj/src/schema.ts")).toContain('from "./auth.ts"');
        expect(files.get("/tmp/proj/src/schema.ts")).not.toContain('from "./worker.ts"');
        expect(files.get("/tmp/proj/src/schema.ts")).toContain('selfBy: "authorId"');
        expect(files.get("/tmp/proj/src/schema.ts")).toContain('attachment: file("attachment"');
        expect(files.get("/tmp/proj/src/api.ts")).toContain('partitionKey: "organizationId"');
        expect(files.get("/tmp/proj/src/api.ts")).toContain('authority: "organization"');
        expect(files.get("/tmp/proj/src/api.ts")).not.toContain("ref:");
        expect(files.get("/tmp/proj/src/api.ts")).toContain("export const replaceMessageAttachment");
        expect(files.get("/tmp/proj/src/api.ts")).toContain("handler: (ctx, args) =>");
        expect(files.get("/tmp/proj/src/api.ts")).toContain("}).run()");
        expect(files.get("/tmp/proj/src/api.ts")).not.toContain("handler: async");
        expect(files.get("/tmp/proj/src/api.ts")).not.toContain("tenantScope");
        expect(files.get("/tmp/proj/src/queries.ts")).not.toContain("ref:");
        expect(files.get("/tmp/proj/src/migrations.ts")).toContain('from "./migrations/v1.ts"');
        expect(files.get("/tmp/proj/src/migrations/v1.ts")).toContain("defineSchemaSnapshot");
        expect(files.get("/tmp/proj/src/worker.ts")).toContain('app.get("/api/messages"');
        expect(files.get("/tmp/proj/src/worker.ts")).toContain('app.post("/api/messages"');
        expect(files.get("/tmp/proj/src/web/App.tsx")).toContain("db.auth.signIn.anonymous()");
        expect(files.get("/tmp/proj/src/web/App.tsx")).toContain("db.useQuery(listMessages");
        expect(files.get("/tmp/proj/src/web/App.tsx")).toContain("db.useMutation(postMessage)");
        expect(files.get("/tmp/proj/src/web/App.tsx")).toContain('fileRef("messages", "attachment")');
        expect(files.get("/tmp/proj/vite.config.ts")).toContain('import { chardb } from "@chardb/core/vite"');
        expect(files.get("/tmp/proj/vitest.config.ts")).toContain(
            'import { cloudflareTest } from "@cloudflare/vitest-plugin"'
        );
        expect(files.get("/tmp/proj/vitest.config.ts")).toContain('configPath: "./wrangler.toml"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('from "cloudflare:workers"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('migration("begin"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('auth("sign-in/anonymous"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('auth("organization/create"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('auth("organization/list"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('from "@msw/cloudflare"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('http.get(origin + "/api/auth/jwks"');
        expect(files.get("/tmp/proj/test/worker.test.ts")).toContain('body: "written inside workerd"');
        expect(files.get("/tmp/proj/vite.config.ts")).toContain("publicDir: false");
        expect(files.get("/tmp/proj/vite.config.ts")).toContain('outDir: "public"');
        expect(files.get("/tmp/proj/vite.config.ts")).toContain('process.env.CHARDB_DEV_URL, "http://127.0.0.1:8787"');
        expect(files.get("/tmp/proj/vite.config.ts")).toContain(
            '"/ws": { target: workerSocket, ws: true, changeOrigin: true }'
        );
        expect(files.get("/tmp/proj/README.md")).toContain("bun run dev");
        expect(files.get("/tmp/proj/README.md")).toContain("bun run deploy:bootstrap");
        expect(files.get("/tmp/proj/README.md")).not.toContain("shards split");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain(
            'throw new Error("/health did not identify the expected local Worker " + deploymentId)'
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("String(targetVersion)");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("await waitForWeb()");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain('"--persist-to"');
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain('import.meta.resolve("wrangler/package.json")');
        expect(files.get("/tmp/proj/scripts/test.mjs")).toContain('import.meta.resolve("vitest/package.json")');
        expect(files.get("/tmp/proj/scripts/test.mjs")).toContain(
            'realpathSync.native(join(dirname(vitestPackage), "vitest.mjs"))'
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain('Bun.which("node")');
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain('import.meta.resolve("vite/package.json")');
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain('["taskkill.exe", "/PID", String(pid), "/T", "/F"]');
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain(
            "Select-Object ProcessId, ParentProcessId, CreationDate"
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("const rootCreatedAt = root.createdAt");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("currentRoot.createdAt !== rootCreatedAt");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("const WINDOWS_STDIN_CANCEL_TIMEOUT_MS = 1_000");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("stdinReader.cancel().catch(() => undefined)");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("Bun.sleep(WINDOWS_STDIN_CANCEL_TIMEOUT_MS)");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain(
            "function descendantsOfProcessIdentity(snapshot, rootPid, rootCreatedAt)"
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain(
            "if (root && root.createdAt !== rootCreatedAt) return []"
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("live.get(pid) === createdAt");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain(
            "signal: AbortSignal.timeout(READINESS_PROBE_TIMEOUT_MS)"
        );
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("WINDOWS_WATCHDOG_ARGUMENT");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).toContain("Promise.allSettled([");
        expect(files.get("/tmp/proj/scripts/dev.mjs")).not.toContain('"--target",\n      "1"');
        expect(files.get("/tmp/proj/wrangler.toml")).toContain("durable_objects.bindings");
        expect(files.get("/tmp/proj/wrangler.toml")).toContain('name = "CDB_SHARD"');
        expect(files.get("/tmp/proj/wrangler.toml")).toContain('class_name = "Cdb"');
        expect(files.get("/tmp/proj/wrangler.toml")).toContain("new_sqlite_classes");
        expect(files.get("/tmp/proj/wrangler.toml")).toContain('binding = "CDB_FILES"');
        expect(files.get("/tmp/proj/wrangler.toml")).toContain('bucket_name = "myapp-files"');

        const r = await runDoctor(ctx);
        expect(r.ok).toBe(true);
    });

    test("doctor accepts an existing wrangler.jsonc project", async () => {
        const { ctx, files, out } = fakeCtx();
        files.set(
            "/tmp/proj/wrangler.jsonc",
            renderWranglerJsonc({ name: "legacy-jsonc", compatibilityDate: "2026-05-10", assetsDir: "public" })
        );

        const result = await runDoctor(ctx);

        expect(result.ok).toBe(true);
        expect(out).toEqual(["chardb doctor: wrangler.jsonc passes\n"]);
    });

    test("doctor accepts JSON and keeps TOML, JSON, JSONC precedence", async () => {
        const source = renderWranglerJsonc({
            name: "json-project",
            compatibilityDate: "2026-05-10",
            assetsDir: "public",
        });
        const jsonOnly = fakeCtx();
        jsonOnly.files.set("/tmp/proj/wrangler.json", source);
        expect((await runDoctor(jsonOnly.ctx)).ok).toBe(true);
        expect(jsonOnly.out).toEqual(["chardb doctor: wrangler.json passes\n"]);

        const precedence = fakeCtx();
        precedence.files.set(
            "/tmp/proj/wrangler.toml",
            renderWrangler({
                name: "toml-project",
                compatibilityDate: "2026-05-10",
                assetsDir: "public",
            })
        );
        precedence.files.set("/tmp/proj/wrangler.json", "{}");
        precedence.files.set("/tmp/proj/wrangler.jsonc", "{}");
        expect((await runDoctor(precedence.ctx)).ok).toBe(true);
        expect(precedence.out).toEqual(["chardb doctor: wrangler.toml passes\n"]);
    });

    test("escapes the application name in generated TypeScript", async () => {
        const { ctx, files } = fakeCtx();

        await runInit(ctx, { name: 'quoted"app' });

        expect(files.get("/tmp/proj/src/auth.ts")).toContain('appName: "quoted\\"app"');
    });
});
