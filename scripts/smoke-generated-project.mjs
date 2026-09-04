import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { arch, platform, release, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { fingerprintFile, writeJsonAtomically } from "./browser-benchmark-report.mjs";
import { injectGeneratedDevInspectorPort, injectGeneratedLoopbackProbe } from "./generated-loopback-probe.mjs";
import { buildGeneratedProjectReport, parseGeneratedProjectArgs } from "./generated-project-report.mjs";
import { preserveFailure, settleBounded, spawnManagedProcess } from "./process-lifecycle.mjs";

const ADMIN_TOKEN = "generated-project-migration-secret";
const AUTH_SECRET = "generated-project-auth-secret-at-least-32-characters";
const INITIAL_MIGRATION_ID = "generated-initial-schema";
const UPGRADE_MIGRATION_ID = "generated-upgrade-v2";
const INITIAL_MIGRATION_NAME = "generated_initial_schema";
const UPGRADE_MIGRATION_NAME = "add_message_edited_at";
const UPGRADE_STATEMENT = 'ALTER TABLE "messages" ADD COLUMN "edited_at" integer';
const VERSION_THREE_MIGRATION_NAME = "add_message_reviewed_at";
const VERSION_THREE_STATEMENT = 'ALTER TABLE "messages" ADD COLUMN "reviewed_at" integer';
const VERSION_FOUR_MIGRATION_NAME = "add_message_archived_at";
const VERSION_FOUR_STATEMENT = 'ALTER TABLE "messages" ADD COLUMN "archived_at" integer';

const options = parseGeneratedProjectArgs(process.argv.slice(2));
const tarballPath = resolve(options.tarball);
const reactTarballPath = resolve(options.reactTarball);
const reportPath = options.reportPath === undefined ? undefined : resolve(options.reportPath);
const smokeDirectory = await mkdtemp(join(tmpdir(), "chardb-generated-project-"));
const bootstrapDirectory = join(smokeDirectory, "bootstrap");
const projectDirectory = join(bootstrapDirectory, "generated app");
const npmCache = process.env.CHARDB_GENERATED_NPM_CACHE ?? join(smokeDirectory, "npm-cache");
const environment = {
    npm_config_cache: npmCache,
    WRANGLER_LOG_PATH: join(smokeDirectory, "wrangler.log"),
    WRANGLER_SEND_METRICS: "false",
};
const startedAt = new Date().toISOString();
const startedAtMs = performance.now();

try {
    await mkdir(bootstrapDirectory, { recursive: true });
    await writeFile(
        join(bootstrapDirectory, "package.json"),
        `${JSON.stringify({ name: "chardb-init-bootstrap", private: true }, null, 2)}\n`
    );
    run(
        "npm",
        ["install", "--ignore-scripts", "--no-audit", "--no-fund", tarballPath],
        bootstrapDirectory,
        environment
    );
    run(
        "npm",
        [
            "exec",
            "--",
            "chardb",
            "init",
            "generated app",
            "--core-package",
            `file:${tarballPath}`,
            "--react-package",
            `file:${reactTarballPath}`,
        ],
        bootstrapDirectory,
        environment
    );
    await proveInitBoundaries(smokeDirectory, bootstrapDirectory, environment, tarballPath, reactTarballPath);

    const installedChardb = JSON.parse(
        await readFile(join(bootstrapDirectory, "node_modules", "@chardb", "core", "package.json"), "utf8")
    );
    const packageJsonPath = join(projectDirectory, "package.json");
    const generatedPackageJson = await readFile(packageJsonPath, "utf8");
    const generatedTsconfig = await readFile(join(projectDirectory, "tsconfig.json"), "utf8");
    const generatedWrangler = await readFile(join(projectDirectory, "wrangler.toml"), "utf8");
    rejectMonorepoAliases(`${generatedPackageJson}\n${generatedTsconfig}`);
    assertNativeWranglerConfig(generatedWrangler);

    const packageJson = JSON.parse(generatedPackageJson);
    if (packageJson.dependencies["@chardb/core"] !== `file:${tarballPath}`) {
        throw new Error(
            `generated @chardb/core specifier ${String(packageJson.dependencies["@chardb/core"])} does not match file:${tarballPath}`
        );
    }
    if (packageJson.dependencies["@chardb/react"] !== `file:${reactTarballPath}`) {
        throw new Error(`generated @chardb/react specifier does not match file:${reactTarballPath}`);
    }
    if (packageJson.overrides?.["@chardb/core"] !== "$@chardb/core") {
        throw new Error("generated package does not bind the React peer to its direct core dependency");
    }
    assertExactDependencyVersions(packageJson, `file:${tarballPath}`, `file:${reactTarballPath}`);

    if (!JSON.stringify(packageJson).includes(`file:${tarballPath}`)) {
        throw new Error("generated package.json does not consume the packed chardb tarball");
    }
    run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund"], projectDirectory, environment);
    const installedReact = JSON.parse(
        await readFile(join(projectDirectory, "node_modules", "@chardb", "react", "package.json"), "utf8")
    );
    const versionOne = await regenerateVersionOneWithPackedCli(projectDirectory, environment);
    run("npm", ["run", "typecheck"], projectDirectory, environment);
    run("npm", ["run", "test"], projectDirectory, environment);
    run("npm", ["run", "build"], projectDirectory, environment);
    await assertGeneratedBrowserBundle(projectDirectory);
    run(
        join(projectDirectory, "node_modules", ".bin", "chardb"),
        ["doctor", "wrangler"],
        projectDirectory,
        environment
    );
    run("npm", ["exec", "--", "chardb", "doctor", "wrangler"], projectDirectory, environment);
    run("bunx", ["--bun", "--no-install", "chardb", "doctor", "wrangler"], projectDirectory, environment);
    await proveGeneratedDevProxy(projectDirectory, environment);
    const verticalSlice = await proveGeneratedVerticalSlice(projectDirectory, environment, versionOne);

    const wranglerPackage = JSON.parse(
        await readFile(join(projectDirectory, "node_modules", "wrangler", "package.json"), "utf8")
    );
    const miniflarePackage = JSON.parse(
        await readFile(join(projectDirectory, "node_modules", "miniflare", "package.json"), "utf8")
    );
    const report = buildGeneratedProjectReport({
        run: {
            id: `${Date.now().toString(36)}-${process.pid}`,
            startedAt,
            durationMs: performance.now() - startedAtMs,
        },
        packageEvidence: {
            name: installedChardb.name,
            version: installedChardb.version,
            tarball: await fingerprintFile(tarballPath),
        },
        reactPackageEvidence: {
            name: installedReact.name,
            version: installedReact.version,
            tarball: await fingerprintFile(reactTarballPath),
        },
        platform: {
            name: process.env.CDB_GENERATED_E2E_PLATFORM_NAME ?? `${platform()}-${arch()}`,
            operatingSystem: platform(),
            release: release(),
            architecture: arch(),
        },
        runtime: {
            bun: Bun.version,
            nodeCompatibility: process.versions.node,
            wrangler: wranglerPackage.version,
            miniflare: miniflarePackage.version,
        },
        migrations: {
            initial: {
                id: INITIAL_MIGRATION_ID,
                targetVersion: 1,
                activatedShards: verticalSlice.initialActivatedShards,
            },
            upgrade: {
                id: UPGRADE_MIGRATION_ID,
                fromVersion: 1,
                targetVersion: 2,
                activatedShards: verticalSlice.upgradeActivatedShards,
            },
        },
        invariants: {
            generatedByPackedCli: true,
            exactReactPackageInstalled: true,
            initialMigrationGeneratedByPackedCli: true,
            versionTwoMigrationGeneratedByPackedCli: true,
            versionThreeMigrationGeneratedByPackedCli: true,
            versionFourMigrationGeneratedByPackedCli: true,
            wranglerTomlDefault: true,
            immutableVersionOneSnapshot: true,
            immutableVersionOneJsonSnapshot: true,
            versionTwoDigestChainValidated: true,
            versionTwoAdditiveSqlGenerated: true,
            fullMigrationDigestChainValidated: true,
            immutablePriorMigrationHistoryPreserved: true,
            exactDependenciesPinned: true,
            bunInstallPassed: true,
            typecheckPassed: true,
            cloudflareVitestPassed: true,
            generatedBrowserBuilt: true,
            browserServerCodeErased: true,
            generatedDevStarted: true,
            generatedDevAuthenticatedWebSocket: true,
            wranglerDryRunPassed: true,
            doctorPassed: true,
            versionTwoTypecheckPassed: true,
            versionTwoWranglerDryRunPassed: true,
            versionFourTypecheckPassed: true,
            versionFourWranglerDryRunPassed: true,
            initialMigrationInterruptedAfterShardActivation: true,
            initialTrafficClosedBeforeRestart: true,
            initialWranglerRestartObserved: true,
            initialTrafficClosedAfterRestart: true,
            sameInitialMigrationIdResumed: true,
            authenticationCompleted: true,
            nativeBetterAuthOrganizationProvisioning: true,
            preUpgradeMutationAndReadCompleted: true,
            preUpgradeLiveReplacementObserved: true,
            v2WorkerFencedBeforeMigration: true,
            upgradeInterruptedAfterShardActivation: true,
            authenticatedReadClosedDuringUpgrade: true,
            authenticatedMutationClosedDuringUpgrade: true,
            upgradeWranglerRestartObserved: true,
            authenticatedTrafficClosedAfterUpgradeRestart: true,
            sameUpgradeMigrationIdResumed: true,
            preUpgradeRowsPreserved: true,
            postUpgradeMutationAndLiveReplacementObserved: true,
            persistedReadAfterUpgradeRestart: true,
        },
    });
    if (reportPath !== undefined) await writeJsonAtomically(reportPath, report);

    console.log(
        JSON.stringify({
            type: "chardb-generated-project-proof",
            version: 1,
            report: reportPath ?? null,
            package: report.package,
            migrations: report.migrations,
            invariants: report.invariants,
        })
    );
} finally {
    await rm(smokeDirectory, { recursive: true, force: true });
}

function rejectMonorepoAliases(generatedConfigText) {
    for (const forbidden of ["workspace:", '"paths"', '"baseUrl"']) {
        if (generatedConfigText.includes(forbidden)) {
            throw new Error(`generated config contains forbidden monorepo reference: ${forbidden}`);
        }
    }
    if (!projectDirectory.startsWith(tmpdir())) {
        throw new Error("generated project is not isolated under the system temporary directory");
    }
}

function assertExactDependencyVersions(packageJson, corePackage, reactPackage) {
    const dependencies = { ...packageJson.dependencies, ...packageJson.devDependencies };
    for (const [name, version] of Object.entries(dependencies)) {
        if (name === "@chardb/core" && version === corePackage) continue;
        if (name === "@chardb/react" && version === reactPackage) continue;
        if (typeof version !== "string" || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
            throw new Error(`generated dependency ${name} is not pinned to an exact version: ${String(version)}`);
        }
    }
}

async function proveInitBoundaries(root, bootstrap, extraEnvironment, candidate, reactCandidate) {
    run(
        "bunx",
        [
            "--bun",
            "--no-install",
            "@chardb/core",
            "init",
            "bun app",
            "--core-package",
            `file:${candidate}`,
            "--react-package",
            `file:${reactCandidate}`,
        ],
        bootstrap,
        extraEnvironment
    );
    const bunPackage = JSON.parse(await readFile(join(bootstrap, "bun app", "package.json"), "utf8"));
    if (bunPackage.name !== "bun-app") throw new Error("bunx @chardb/core init did not preserve the named target");
    if (bunPackage.dependencies["@chardb/core"] !== `file:${candidate}`) {
        throw new Error("bunx @chardb/core init did not preserve the exact package specifier");
    }
    if (bunPackage.dependencies["@chardb/react"] !== `file:${reactCandidate}`) {
        throw new Error("bunx @chardb/core init did not preserve the exact React package specifier");
    }
    if (bunPackage.overrides?.["@chardb/core"] !== "$@chardb/core") {
        throw new Error("bunx @chardb/core init did not bind the React peer to its direct core dependency");
    }
    run("bun", ["install", "--ignore-scripts"], join(bootstrap, "bun app"), extraEnvironment);
    for (const packageName of ["core", "react"]) {
        const manifest = JSON.parse(
            await readFile(join(bootstrap, "bun app", "node_modules", "@chardb", packageName, "package.json"), "utf8")
        );
        if (manifest.name !== `@chardb/${packageName}`) {
            throw new Error(`bun install resolved the wrong @chardb/${packageName} package`);
        }
    }

    for (const name of [".", "..", "../outside", "nested/app", "nested\\app", "/absolute", "C:\\absolute"]) {
        const failure = runResult("npm", ["exec", "--", "chardb", "init", name], bootstrap, extraEnvironment, "pipe");
        if (failure.error) throw failure.error;
        if (failure.status === 0 || !failure.stderr.includes("one name without path separators")) {
            throw new Error(`init accepted unsafe target ${JSON.stringify(name)}: ${failure.stderr}`);
        }
    }

    const blockedDirectory = join(bootstrap, "blocked app");
    const markerPath = join(blockedDirectory, "keep.txt");
    await mkdir(blockedDirectory);
    await writeFile(markerPath, "keep me\n");
    const blocked = runResult(
        "npm",
        ["exec", "--", "chardb", "init", "blocked app"],
        bootstrap,
        extraEnvironment,
        "pipe"
    );
    if (blocked.error) throw blocked.error;
    if (blocked.status === 0 || !blocked.stderr.includes("requires an empty directory")) {
        throw new Error(`init accepted a non-empty target (${String(blocked.status)}): ${blocked.stderr}`);
    }
    if ((await readFile(markerPath, "utf8")) !== "keep me\n") throw new Error("non-empty init changed existing data");

    const outside = join(root, "symlink outside");
    await mkdir(outside);
    await symlink(outside, join(bootstrap, "linked app"), "dir");
    const linked = runResult(
        "npm",
        ["exec", "--", "chardb", "init", "linked app"],
        bootstrap,
        extraEnvironment,
        "pipe"
    );
    if (linked.error) throw linked.error;
    if (linked.status === 0 || !linked.stderr.includes("not a real directory")) {
        throw new Error(`init followed a symlink target (${String(linked.status)}): ${linked.stderr}`);
    }
    if ((await readdir(outside)).length !== 0) throw new Error("symlink target was modified");
}

async function regenerateVersionOneWithPackedCli(cwd, extraEnvironment) {
    const migrationDirectory = join(cwd, "src", "migrations");
    const journalPath = join(cwd, "src", "migrations.ts");
    await rm(migrationDirectory, { recursive: true, force: true });
    await rm(journalPath, { force: true });
    runPackedMigrationGenerator(cwd, extraEnvironment, INITIAL_MIGRATION_NAME);

    const sourcePath = join(migrationDirectory, "v1.ts");
    const snapshotPath = join(migrationDirectory, "v1.json");
    const [source, snapshotBytes] = await Promise.all([readFile(sourcePath, "utf8"), readFile(snapshotPath, "utf8")]);
    assertStaticMigrationSource(source, 1);
    const snapshot = parseGeneratedSnapshot(snapshotBytes, {
        version: 1,
        name: INITIAL_MIGRATION_NAME,
        previousDigest: null,
    });
    const loaded = await importGeneratedMigration(sourcePath);
    if (
        loaded.initialSchema?.version !== 1 ||
        loaded.initialSchema?.name !== INITIAL_MIGRATION_NAME ||
        !Array.isArray(loaded.initialSchema?.statements) ||
        loaded.initialSchema.statements.length === 0
    ) {
        throw new Error("packed migration generator emitted an invalid executable version-one migration");
    }
    return Object.freeze({ source, snapshotBytes, snapshot });
}

function runPackedMigrationGenerator(cwd, extraEnvironment, name) {
    run(join(cwd, "node_modules", ".bin", "chardb"), ["migrations", "generate", "--name", name], cwd, extraEnvironment);
}

function assertStaticMigrationSource(source, version) {
    if (!source.includes("defineSchemaSnapshot(")) {
        throw new Error(`generated version-${version} migration is not a static schema snapshot`);
    }
    if (version === 1 && !source.includes(".initialMigration")) {
        throw new Error("generated version-one migration does not derive its initial migration from the snapshot");
    }
    for (const forbidden of [
        "defineSchemaBaseline",
        "better-auth",
        "drizzle-orm",
        'from "../schema.ts"',
        'from "../auth.ts"',
    ]) {
        if (source.includes(forbidden)) {
            throw new Error(`generated version-${version} migration retained mutable authoring code: ${forbidden}`);
        }
    }
}

function parseGeneratedSnapshot(bytes, expected) {
    if (!bytes.endsWith("\n") || bytes.slice(0, -1).includes("\n")) {
        throw new Error(`generated version-${expected.version} snapshot is not one canonical JSON line`);
    }
    let snapshot;
    try {
        snapshot = JSON.parse(bytes.slice(0, -1));
    } catch {
        throw new Error(`generated version-${expected.version} snapshot is not valid JSON`);
    }
    if (stableJson(snapshot) !== bytes.slice(0, -1)) {
        throw new Error(`generated version-${expected.version} snapshot is not canonical JSON`);
    }
    if (
        snapshot?.version !== expected.version ||
        snapshot?.name !== expected.name ||
        snapshot?.previousDigest !== expected.previousDigest ||
        typeof snapshot?.digest !== "string" ||
        !/^[0-9a-f]{64}$/.test(snapshot.digest)
    ) {
        throw new Error(`generated version-${expected.version} snapshot identity or digest chain drifted`);
    }
    return snapshot;
}

function stableJson(value) {
    if (value === null || typeof value !== "object") return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
    return `{${Object.keys(value)
        .sort()
        .map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`)
        .join(",")}}`;
}

async function importGeneratedMigration(path) {
    const moduleUrl = `${pathToFileURL(path).href}?proof=${Date.now().toString(36)}-${Math.random()}`;
    const preloadPath = join(
        projectDirectory,
        "node_modules",
        "@chardb",
        "core",
        "dist",
        "cli",
        "schema-inspector-preload.mjs"
    );
    const source = `
const loaded = await import(${JSON.stringify(moduleUrl)});
process.stdout.write(JSON.stringify({
  initialSchema: loaded.initialSchema ?? null,
  schemaSnapshot: loaded.schemaSnapshot ?? null,
  migration: loaded.migration ?? null,
}));
`;
    const result = spawnSync(process.execPath, ["--preload", preloadPath, "--eval", source], {
        cwd: projectDirectory,
        encoding: "utf8",
        maxBuffer: 16 * 1_024 * 1_024,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
        throw new Error(`generated migration import exited with ${String(result.status)}: ${result.stderr.trim()}`);
    }
    try {
        return JSON.parse(result.stdout);
    } catch {
        throw new Error("generated migration import returned malformed JSON");
    }
}

async function assertGeneratedBrowserBundle(directory) {
    const assetsDirectory = join(directory, "public", "assets");
    const files = (await readdir(assetsDirectory)).filter(file => file.endsWith(".js"));
    if (files.length === 0) throw new Error("generated browser build emitted no JavaScript asset");
    const code = (await Promise.all(files.map(file => readFile(join(assetsDirectory, file), "utf8")))).join("\n");
    for (const required of ["mutation#postMessage", "query#listMessages", "__chardbRef"]) {
        if (!code.includes(required)) throw new Error(`generated browser bundle is missing ${required}`);
    }
    for (const forbidden of [
        "cloudflare:workers",
        "CDB_FORBIDDEN: active organization does not match the routed partition",
        "defineSchemaBaseline",
    ]) {
        if (code.includes(forbidden)) throw new Error(`generated browser bundle contains server code: ${forbidden}`);
    }
}

async function proveGeneratedDevProxy(cwd, extraEnvironment) {
    const [workerPort, webPort, inspectorPort] = await reserveLocalPorts(3);
    const workerOrigin = new URL(`http://127.0.0.1:${workerPort}`);
    const webOrigin = new URL(`http://127.0.0.1:${webPort}`);
    const devPath = join(cwd, "scripts", "dev.mjs");
    await writeFile(devPath, injectGeneratedDevInspectorPort(await readFile(devPath, "utf8"), inspectorPort));
    await writeFile(
        join(cwd, ".env.local"),
        [
            "CHARDB_URL=https://production-worker.invalid",
            "CHARDB_WEB_URL=https://production-web.invalid",
            "CHARDB_ADMIN_TOKEN=production-admin-token-must-never-reach-local-dev",
            "BETTER_AUTH_SECRET=production-better-auth-secret-must-never-reach-local-dev",
            "",
        ].join("\n")
    );
    const managed = spawnManagedProcess([process.execPath, "run", "dev"], {
        label: "generated dev command",
        cwd,
        env: {
            ...process.env,
            ...extraEnvironment,
            CHARDB_DEV_URL: workerOrigin.origin,
            CHARDB_DEV_WEB_URL: webOrigin.origin,
        },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
    });
    const subprocess = managed.child;
    const stdout = new Response(subprocess.stdout).text();
    const stderr = new Response(subprocess.stderr).text();
    let failure;
    try {
        await waitForWrangler(subprocess, new URL("/health", webOrigin));
        const cookie = await signIn(webOrigin);
        const token = await issueToken(webOrigin, cookie);
        await expectWebSocketWelcome(webOrigin, token);
    } catch (error) {
        failure = error;
    }
    let cleanupFailure;
    let exitCode;
    try {
        exitCode = await managed.stop("SIGTERM");
    } catch (error) {
        cleanupFailure = error;
    }
    let out = "";
    let err = "";
    try {
        [out, err] = await settleBounded(() => Promise.all([stdout, stderr]), {
            label: "generated dev output drain",
            timeoutMs: 2_000,
        });
    } catch (error) {
        cleanupFailure = preserveFailure(cleanupFailure, error, "generated dev cleanup failed");
    }
    if (failure !== undefined) {
        const primary = new Error(
            `${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}\n${out}${err}`,
            { cause: failure }
        );
        throw preserveFailure(primary, cleanupFailure, "generated dev failed and cleanup also failed");
    }
    if (![0, 143].includes(exitCode)) {
        const primary = new Error(`generated dev command exited with ${String(exitCode)}\n${out}${err}`);
        throw preserveFailure(primary, cleanupFailure, "generated dev exit and cleanup failed");
    }
    if (cleanupFailure !== undefined) throw cleanupFailure;
}

async function reserveLocalPorts(count) {
    const reservations = Array.from({ length: count }, () =>
        Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response("reserved") })
    );
    const ports = reservations.map(reservation => reservation.port);
    await Promise.all(reservations.map(reservation => reservation.stop(true)));
    return ports;
}

async function expectWebSocketWelcome(origin, token) {
    const endpoint = new URL("/ws", origin);
    endpoint.protocol = "ws:";
    endpoint.searchParams.set("clientId", "generated-dev-proxy");
    await new Promise((resolve, reject) => {
        const socket = new WebSocket(endpoint);
        let settled = false;
        const finish = error => {
            if (settled) return;
            settled = true;
            clearTimeout(timeout);
            socket.close();
            if (error === undefined) resolve();
            else reject(error);
        };
        const timeout = setTimeout(() => finish(new Error("generated dev WebSocket welcome timed out")), 15_000);
        socket.onopen = () => {
            socket.send(
                JSON.stringify({
                    t: "hello",
                    protocolV: 3,
                    clientId: "generated-dev-proxy",
                    jwt: token,
                })
            );
        };
        socket.onmessage = event => {
            try {
                const message = JSON.parse(String(event.data));
                if (message?.t === "welcome" && message.protocolV === 3) finish();
                else finish(new Error(`generated dev WebSocket rejected authentication: ${String(event.data)}`));
            } catch {
                finish(new Error(`generated dev WebSocket returned invalid JSON: ${String(event.data)}`));
            }
        };
        socket.onerror = () => finish(new Error("generated dev WebSocket failed"));
        socket.onclose = event => {
            if (!settled) finish(new Error(`generated dev WebSocket closed before welcome (${event.code})`));
        };
    });
}

function assertNativeWranglerConfig(source) {
    const config = Bun.TOML.parse(source);
    const bindings = config.durable_objects?.bindings ?? [];
    const expectedBindings = [
        { name: "CDB_CATALOG", class_name: "Catalog" },
        { name: "CDB_SHARD", class_name: "Cdb" },
        { name: "CDB_GATEWAY", class_name: "Gateway" },
        { name: "CDB_RESHARD", class_name: "Resharder" },
    ];
    if (JSON.stringify(bindings) !== JSON.stringify(expectedBindings)) {
        throw new Error(`generated Wrangler Durable Object bindings drifted: ${JSON.stringify(bindings)}`);
    }
    const classes = (config.migrations ?? []).flatMap(migration => migration.new_sqlite_classes ?? []).sort();
    const expected = ["Catalog", "Cdb", "Gateway", "Resharder"];
    if (JSON.stringify(classes) !== JSON.stringify(expected)) {
        throw new Error(`generated Wrangler migrations drifted: ${JSON.stringify(classes)}`);
    }
    if (String(config.compatibility_date) < "2025-11-17") {
        throw new Error(
            `generated compatibility date predates native ctx.exports: ${String(config.compatibility_date)}`
        );
    }
}

async function proveGeneratedVerticalSlice(cwd, extraEnvironment, versionOne) {
    await instrumentLoopbackProbe(cwd);
    const persistencePath = join(cwd, ".wrangler", "smoke-state");
    let interrupted;
    let initialActivatedShards;
    try {
        interrupted = await startWrangler(cwd, extraEnvironment, persistencePath);
        await assertLoopbackShape(interrupted.origin);
        await assertSchemaState(interrupted.origin, { activeVersion: 0, status: "active" });
        initialActivatedShards = await interruptGeneratedMigration(interrupted.origin, INITIAL_MIGRATION_ID, 1);
        await assertSchemaState(interrupted.origin, {
            activeVersion: 0,
            status: "migrating",
            migrationId: INITIAL_MIGRATION_ID,
        });
        await assertInitialTrafficClosed(interrupted.origin, "before Wrangler restart");
    } finally {
        if (interrupted) await stopWrangler(interrupted);
    }
    if (initialActivatedShards === undefined) {
        throw new Error("generated initial migration interruption did not activate a shard");
    }

    const provisioned = await resumeInitialMigrationAndProvision(cwd, extraEnvironment, persistencePath);
    const versionTwo = await prepareVersionTwo(cwd, extraEnvironment, versionOne);
    const upgradeActivatedShards = await proveInterruptedUpgrade(
        cwd,
        extraEnvironment,
        persistencePath,
        provisioned.cookie,
        provisioned.token,
        provisioned.organizationId
    );
    await provePackedSequentialMigrations(cwd, extraEnvironment, versionOne, versionTwo);
    return { initialActivatedShards, upgradeActivatedShards };
}

async function resumeInitialMigrationAndProvision(cwd, extraEnvironment, persistencePath) {
    let active;
    let cookie;
    let token;
    let organizationId;
    let failure;
    try {
        active = await startWrangler(cwd, extraEnvironment, persistencePath);
        await assertLoopbackShape(active.origin);
        await assertSchemaState(active.origin, {
            activeVersion: 0,
            status: "migrating",
            migrationId: INITIAL_MIGRATION_ID,
        });
        await assertInitialTrafficClosed(active.origin, "after Wrangler restart");
        await migrateGeneratedWorker(cwd, active.origin, extraEnvironment, INITIAL_MIGRATION_ID, 1);
        await assertSchemaState(active.origin, { activeVersion: 1, status: "active" });

        const identity = await provisionOrganization(active.origin);
        ({ cookie, token, organizationId } = identity);

        await writeMessage(active.origin, token, organizationId, "message-1", "first");
        await assertMessages(active.origin, token, organizationId, ["message-1"]);
        await proveLiveQuery(cwd, active.origin, token, organizationId, ["message-1"], "message-2", "pre-upgrade");
    } catch (error) {
        failure = error;
    } finally {
        if (active) await stopWrangler(active, failure);
    }
    if (!cookie || !token || !organizationId) throw new Error("generated v1 organization session did not complete");
    return { cookie, token, organizationId };
}

async function prepareVersionTwo(cwd, extraEnvironment, versionOne) {
    const schemaPath = join(cwd, "src", "schema.ts");
    const schema = await readFile(schemaPath, "utf8");
    const schemaMarker = '    createdAt: integer("created_at").notNull(),';
    if (!schema.includes(schemaMarker)) throw new Error("generated schema version-one marker drifted");
    await writeFile(schemaPath, schema.replace(schemaMarker, `${schemaMarker}\n    editedAt: integer("edited_at"),`));

    runPackedMigrationGenerator(cwd, extraEnvironment, UPGRADE_MIGRATION_NAME);
    const migrationDirectory = join(cwd, "src", "migrations");
    const [currentV1, currentV1Json, versionTwoSource, versionTwoJson, journal] = await Promise.all([
        readFile(join(migrationDirectory, "v1.ts"), "utf8"),
        readFile(join(migrationDirectory, "v1.json"), "utf8"),
        readFile(join(migrationDirectory, "v2.ts"), "utf8"),
        readFile(join(migrationDirectory, "v2.json"), "utf8"),
        readFile(join(cwd, "src", "migrations.ts"), "utf8"),
    ]);
    if (currentV1 !== versionOne.source || currentV1Json !== versionOne.snapshotBytes) {
        throw new Error("packed version-two generation changed immutable version-one history");
    }
    assertStaticMigrationSource(versionTwoSource, 2);
    const versionTwo = parseGeneratedSnapshot(versionTwoJson, {
        version: 2,
        name: UPGRADE_MIGRATION_NAME,
        previousDigest: versionOne.snapshot.digest,
    });
    if (versionTwo.digest === versionOne.snapshot.digest) {
        throw new Error("generated version-two snapshot reused the version-one digest");
    }
    if (!journal.includes('import { migration as migrationV2 } from "./migrations/v2.ts";')) {
        throw new Error("packed version-two generator did not append its migration to the static journal");
    }
    const loaded = await importGeneratedMigration(join(migrationDirectory, "v2.ts"));
    if (
        loaded.schemaSnapshot?.digest !== versionTwo.digest ||
        loaded.schemaSnapshot?.previousDigest !== versionOne.snapshot.digest ||
        loaded.migration?.version !== 2 ||
        loaded.migration?.name !== UPGRADE_MIGRATION_NAME ||
        JSON.stringify(loaded.migration?.statements) !== JSON.stringify([UPGRADE_STATEMENT]) ||
        JSON.stringify(loaded.migration?.catalogStatements) !== "[]"
    ) {
        throw new Error("packed version-two generator did not emit the expected additive migration");
    }
    run("npm", ["run", "typecheck"], cwd, extraEnvironment);
    run("npm", ["run", "build"], cwd, extraEnvironment);
    return Object.freeze({ source: versionTwoSource, snapshotBytes: versionTwoJson, snapshot: versionTwo });
}

async function provePackedSequentialMigrations(cwd, extraEnvironment, versionOne, versionTwo) {
    const schemaPath = join(cwd, "src", "schema.ts");
    const migrationDirectory = join(cwd, "src", "migrations");
    const immutableThroughV2 = new Map(
        await Promise.all(
            ["v1.ts", "v1.json", "v2.ts", "v2.json"].map(async name => [
                name,
                await readFile(join(migrationDirectory, name), "utf8"),
            ])
        )
    );
    if (
        immutableThroughV2.get("v1.ts") !== versionOne.source ||
        immutableThroughV2.get("v1.json") !== versionOne.snapshotBytes ||
        immutableThroughV2.get("v2.ts") !== versionTwo.source ||
        immutableThroughV2.get("v2.json") !== versionTwo.snapshotBytes
    ) {
        throw new Error("packed sequential migration proof did not start from the verified v1/v2 history");
    }

    const versionTwoSchema = await readFile(schemaPath, "utf8");
    const versionTwoMarker = '    editedAt: integer("edited_at"),';
    if (!versionTwoSchema.includes(versionTwoMarker)) throw new Error("generated schema version-two marker drifted");
    await writeFile(
        schemaPath,
        versionTwoSchema.replace(versionTwoMarker, `${versionTwoMarker}\n    reviewedAt: integer("reviewed_at"),`)
    );
    runPackedMigrationGenerator(cwd, extraEnvironment, VERSION_THREE_MIGRATION_NAME);
    run("npm", ["run", "test"], cwd, extraEnvironment);

    for (const [name, contents] of immutableThroughV2) {
        if ((await readFile(join(migrationDirectory, name), "utf8")) !== contents) {
            throw new Error(`packed version-three generation changed immutable ${name}`);
        }
    }
    const [versionThreeSource, versionThreeJson] = await Promise.all([
        readFile(join(migrationDirectory, "v3.ts"), "utf8"),
        readFile(join(migrationDirectory, "v3.json"), "utf8"),
    ]);
    assertStaticMigrationSource(versionThreeSource, 3);
    const versionThree = parseGeneratedSnapshot(versionThreeJson, {
        version: 3,
        name: VERSION_THREE_MIGRATION_NAME,
        previousDigest: versionTwo.snapshot.digest,
    });
    const loadedThree = await importGeneratedMigration(join(migrationDirectory, "v3.ts"));
    if (
        loadedThree.schemaSnapshot?.digest !== versionThree.digest ||
        loadedThree.migration?.version !== 3 ||
        JSON.stringify(loadedThree.migration?.statements) !== JSON.stringify([VERSION_THREE_STATEMENT]) ||
        JSON.stringify(loadedThree.migration?.catalogStatements) !== "[]"
    ) {
        throw new Error("packed version-three generator did not emit the expected additive migration");
    }

    const versionThreeSchema = await readFile(schemaPath, "utf8");
    const versionThreeMarker = '    reviewedAt: integer("reviewed_at"),';
    if (!versionThreeSchema.includes(versionThreeMarker))
        throw new Error("generated schema version-three marker drifted");
    await writeFile(
        schemaPath,
        versionThreeSchema.replace(versionThreeMarker, `${versionThreeMarker}\n    archivedAt: integer("archived_at"),`)
    );
    runPackedMigrationGenerator(cwd, extraEnvironment, VERSION_FOUR_MIGRATION_NAME);
    run("npm", ["run", "test"], cwd, extraEnvironment);

    for (const [name, contents] of [
        ...immutableThroughV2,
        ["v3.ts", versionThreeSource],
        ["v3.json", versionThreeJson],
    ]) {
        if ((await readFile(join(migrationDirectory, name), "utf8")) !== contents) {
            throw new Error(`packed version-four generation changed immutable ${name}`);
        }
    }
    const [versionFourSource, versionFourJson, journal] = await Promise.all([
        readFile(join(migrationDirectory, "v4.ts"), "utf8"),
        readFile(join(migrationDirectory, "v4.json"), "utf8"),
        readFile(join(cwd, "src", "migrations.ts"), "utf8"),
    ]);
    assertStaticMigrationSource(versionFourSource, 4);
    const versionFour = parseGeneratedSnapshot(versionFourJson, {
        version: 4,
        name: VERSION_FOUR_MIGRATION_NAME,
        previousDigest: versionThree.digest,
    });
    const loadedFour = await importGeneratedMigration(join(migrationDirectory, "v4.ts"));
    if (
        loadedFour.schemaSnapshot?.digest !== versionFour.digest ||
        loadedFour.migration?.version !== 4 ||
        JSON.stringify(loadedFour.migration?.statements) !== JSON.stringify([VERSION_FOUR_STATEMENT]) ||
        JSON.stringify(loadedFour.migration?.catalogStatements) !== "[]"
    ) {
        throw new Error("packed version-four generator did not emit the expected additive migration");
    }
    for (const [version, binding] of [
        [2, "migrationV2"],
        [3, "migrationV3"],
        [4, "migrationV4"],
    ]) {
        if (!journal.includes(`import { migration as ${binding} } from "./migrations/v${version}.ts";`)) {
            throw new Error(`packed version-four journal is missing v${version}`);
        }
    }
    if (!journal.includes("  migrationV2,\n  migrationV3,\n  migrationV4,")) {
        throw new Error("packed version-four journal is not contiguous");
    }
    run("npm", ["run", "typecheck"], cwd, extraEnvironment);
    run("npm", ["run", "build"], cwd, extraEnvironment);
}

async function proveInterruptedUpgrade(cwd, extraEnvironment, persistencePath, cookie, oldToken, organizationId) {
    let beforeInterruption;
    let upgradeActivatedShards;
    try {
        beforeInterruption = await startWrangler(cwd, extraEnvironment, persistencePath);
        await assertLoopbackShape(beforeInterruption.origin);
        await assertSchemaState(beforeInterruption.origin, { activeVersion: 1, status: "active" });
        await assertAuthenticatedTrafficClosed(
            beforeInterruption.origin,
            oldToken,
            organizationId,
            "before migration",
            {
                activeVersion: 1,
                status: "active",
            }
        );
        upgradeActivatedShards = await interruptGeneratedMigration(beforeInterruption.origin, UPGRADE_MIGRATION_ID, 2);
        await assertSchemaState(beforeInterruption.origin, {
            activeVersion: 1,
            status: "migrating",
            migrationId: UPGRADE_MIGRATION_ID,
        });
        await assertAuthenticatedTrafficClosed(
            beforeInterruption.origin,
            oldToken,
            organizationId,
            "during migration",
            {
                activeVersion: 1,
                status: "migrating",
                migrationId: UPGRADE_MIGRATION_ID,
            }
        );
    } finally {
        if (beforeInterruption) await stopWrangler(beforeInterruption);
    }
    if (upgradeActivatedShards === undefined) {
        throw new Error("generated upgrade interruption did not activate a shard");
    }

    let reconstructed;
    let reconstructedFailure;
    try {
        reconstructed = await startWrangler(cwd, extraEnvironment, persistencePath);
        await assertLoopbackShape(reconstructed.origin);
        await assertSchemaState(reconstructed.origin, {
            activeVersion: 1,
            status: "migrating",
            migrationId: UPGRADE_MIGRATION_ID,
        });
        await assertAuthenticatedTrafficClosed(
            reconstructed.origin,
            oldToken,
            organizationId,
            "after upgrade restart",
            {
                activeVersion: 1,
                status: "migrating",
                migrationId: UPGRADE_MIGRATION_ID,
            }
        );
        await migrateGeneratedWorker(cwd, reconstructed.origin, extraEnvironment, UPGRADE_MIGRATION_ID, 2);
        await assertSchemaState(reconstructed.origin, { activeVersion: 2, status: "active" });
        const token = await issueToken(reconstructed.origin, cookie);
        await assertMessages(reconstructed.origin, token, organizationId, ["message-1", "message-2"]);
        await writeMessage(reconstructed.origin, token, organizationId, "message-3", "after upgrade");
        await proveLiveQuery(
            cwd,
            reconstructed.origin,
            token,
            organizationId,
            ["message-1", "message-2", "message-3"],
            "message-4",
            "post-upgrade"
        );
    } catch (error) {
        reconstructedFailure = error;
    } finally {
        if (reconstructed) await stopWrangler(reconstructed, reconstructedFailure);
    }

    let final;
    try {
        final = await startWrangler(cwd, extraEnvironment, persistencePath);
        await assertLoopbackShape(final.origin);
        await assertSchemaState(final.origin, { activeVersion: 2, status: "active" });
        const token = await issueToken(final.origin, cookie);
        await assertMessages(final.origin, token, organizationId, ["message-1", "message-2", "message-3", "message-4"]);
    } finally {
        if (final) await stopWrangler(final);
    }
    return upgradeActivatedShards;
}

async function startWrangler(cwd, extraEnvironment, persistencePath) {
    const [port, inspectorPort] = await reserveLocalPorts(2);
    const managed = spawnManagedProcess(
        [
            join(cwd, "node_modules", ".bin", "wrangler"),
            "dev",
            "--ip",
            "127.0.0.1",
            "--port",
            String(port),
            "--inspector-port",
            String(inspectorPort),
            "--persist-to",
            persistencePath,
            "--var",
            `CDB_ADMIN_TOKEN:${ADMIN_TOKEN}`,
            "--var",
            `BETTER_AUTH_SECRET:${AUTH_SECRET}`,
        ],
        {
            label: "Wrangler dev",
            cwd,
            env: { ...process.env, ...extraEnvironment },
            stdout: "pipe",
            stderr: "pipe",
        }
    );
    const subprocess = managed.child;
    const origin = new URL(`http://127.0.0.1:${port}`);
    const stdout = new Response(subprocess.stdout).text();
    const stderr = new Response(subprocess.stderr).text();
    try {
        await waitForWrangler(subprocess, new URL("/health", origin));
        return { managed, subprocess, origin, stdout, stderr };
    } catch (error) {
        let cleanupFailure;
        try {
            await managed.stop("SIGTERM");
        } catch (cleanupError) {
            cleanupFailure = cleanupError;
        }
        let out = "";
        let err = "";
        try {
            [out, err] = await settleBounded(() => Promise.all([stdout, stderr]), {
                label: "Wrangler startup output drain",
                timeoutMs: 2_000,
            });
        } catch (outputError) {
            cleanupFailure = preserveFailure(cleanupFailure, outputError, "Wrangler startup cleanup failed");
        }
        const primary = new Error(`${String(error)}\n${out}${err}`, { cause: error });
        throw preserveFailure(primary, cleanupFailure, "Wrangler startup and cleanup failed");
    }
}

async function stopWrangler(running, failure) {
    let cleanupFailure;
    let exitCode;
    try {
        exitCode = await running.managed.stop("SIGTERM");
    } catch (error) {
        cleanupFailure = error;
    }
    let out = "";
    let err = "";
    try {
        [out, err] = await settleBounded(() => Promise.all([running.stdout, running.stderr]), {
            label: "Wrangler output drain",
            timeoutMs: 2_000,
        });
    } catch (error) {
        cleanupFailure = preserveFailure(cleanupFailure, error, "Wrangler cleanup failed");
    }
    await Bun.sleep(500);
    let primary;
    if (![0, 143].includes(exitCode)) {
        primary = new Error(`wrangler dev exited with ${String(exitCode)}\n${out}${err}`);
    }
    if (failure !== undefined) {
        const proofFailure = new Error(
            `${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}\n${out}${err}`,
            {
                cause: failure,
            }
        );
        primary = preserveFailure(proofFailure, primary, "generated proof and Wrangler exit both failed");
    }
    const combined = preserveFailure(primary, cleanupFailure, "Wrangler failed and cleanup also failed");
    if (combined !== undefined) throw combined;
}

async function assertLoopbackShape(origin) {
    const response = await fetch(new URL("/__chardb_loopback_probe", origin));
    const text = await response.text();
    const probe = JSON.parse(text);
    if (
        !response.ok ||
        probe.exportCatalog?.type !== "function" ||
        probe.exportCatalog?.idFromName !== "function" ||
        probe.resolvedCatalog?.idFromName !== "function"
    ) {
        throw new Error(`Wrangler loopback shape drifted: ${text}`);
    }
}

async function assertSchemaState(origin, expected) {
    const response = await fetch(new URL("/_chardb/migrations/state", origin), {
        headers: { authorization: `Bearer ${ADMIN_TOKEN}` },
    });
    const text = await response.text();
    if (!response.ok) {
        throw new Error(`Wrangler native loopback request failed (${response.status}): ${text}`);
    }
    const body = JSON.parse(text);
    if (
        body?.state?.activeVersion !== expected.activeVersion ||
        body?.state?.status !== expected.status ||
        (expected.migrationId !== undefined && body?.state?.migrationId !== expected.migrationId)
    ) {
        throw new Error(`generated schema state drifted: ${text}`);
    }
}

async function migrationAdminRequest(origin, path, body) {
    const response = await fetch(new URL(`/_chardb/migrations/${path}`, origin), {
        method: body === undefined ? "GET" : "POST",
        headers: {
            authorization: `Bearer ${ADMIN_TOKEN}`,
            ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`generated migration ${path} failed (${response.status}): ${text}`);
    try {
        return JSON.parse(text);
    } catch {
        throw new Error(`generated migration ${path} returned invalid JSON: ${text}`);
    }
}

async function interruptGeneratedMigration(origin, migrationId, targetVersion) {
    const begun = await migrationAdminRequest(origin, "begin", {
        migrationId,
        targetVersion,
    });
    if (
        begun?.state?.status !== "migrating" ||
        begun?.state?.migrationId !== migrationId ||
        begun?.state?.targetVersion !== targetVersion
    ) {
        throw new Error(`generated migration did not begin with the expected owner: ${JSON.stringify(begun)}`);
    }
    const activated = await migrationAdminRequest(origin, "shard", {
        migrationId,
        shardId: "ShardDO_0",
    });
    if (activated?.shard?.shardId !== "ShardDO_0" || activated?.shard?.status !== "active") {
        throw new Error(`generated migration did not activate ShardDO_0: ${JSON.stringify(activated)}`);
    }
    return ["ShardDO_0"];
}

async function assertInitialTrafficClosed(origin, phase) {
    const health = await fetch(new URL("/health", origin));
    if (!health.ok) throw new Error(`generated Worker health failed ${phase}: ${health.status}`);
    const response = await fetch(new URL("/api/auth/sign-in/anonymous", origin), {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    const text = await response.text();
    let body;
    try {
        body = JSON.parse(text);
    } catch {
        body = null;
    }
    if (
        response.status !== 409 ||
        body?.error?.code !== "CDB_STALE_EPOCH" ||
        sessionCookies(response.headers).length > 0
    ) {
        throw new Error(`generated public traffic did not fail closed ${phase} (${response.status}): ${text}`);
    }
    await assertSchemaState(origin, {
        activeVersion: 0,
        status: "migrating",
        migrationId: INITIAL_MIGRATION_ID,
    });
}

async function assertAuthenticatedTrafficClosed(origin, token, organizationId, phase, expectedState) {
    const health = await fetch(new URL("/health", origin));
    if (!health.ok) throw new Error(`generated v2 Worker health failed ${phase}: ${health.status}`);

    const readUrl = new URL("/api/messages", origin);
    readUrl.searchParams.set("organizationId", organizationId);
    const read = await fetch(readUrl, { headers: { authorization: `Bearer ${token}` } });
    const readText = await read.text();
    if (read.ok) {
        throw new Error(`generated authenticated read did not fail closed ${phase}: ${readText}`);
    }

    const write = await fetch(new URL("/api/messages", origin), {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({
            id: `blocked-${phase.replaceAll(" ", "-")}`,
            organizationId,
            body: "must not commit",
            clientCreatedAt: Date.now(),
        }),
    });
    const writeText = await write.text();
    if (write.ok) {
        throw new Error(`generated authenticated mutation did not fail closed ${phase}: ${writeText}`);
    }
    await assertSchemaState(origin, expectedState);
}

function migrateGeneratedWorker(cwd, origin, extraEnvironment, migrationId, targetVersion) {
    run(
        "bun",
        [
            join(cwd, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs"),
            "migrate",
            "--url",
            origin.origin,
            "--id",
            migrationId,
            "--target",
            String(targetVersion),
            "--concurrency",
            "2",
        ],
        cwd,
        { ...extraEnvironment, CHARDB_ADMIN_TOKEN: ADMIN_TOKEN }
    );
}

function sessionCookies(headers) {
    const values = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [headers.get("set-cookie")];
    return values
        .filter(Boolean)
        .map(value => value.split(";", 1)[0])
        .join("; ");
}

async function signIn(origin) {
    const response = await fetch(new URL("/api/auth/sign-in/anonymous", origin), {
        method: "POST",
        headers: { "content-type": "application/json", origin: origin.origin },
        body: "{}",
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`generated anonymous sign-in failed (${response.status}): ${text}`);
    const cookie = sessionCookies(response.headers);
    if (!cookie) throw new Error("generated anonymous sign-in returned no session cookie");
    return cookie;
}

function mergeCookies(current, headers) {
    const cookies = new Map();
    for (const source of [current, sessionCookies(headers)]) {
        for (const pair of source.split(";")) {
            const trimmed = pair.trim();
            if (!trimmed) continue;
            const separator = trimmed.indexOf("=");
            if (separator <= 0) continue;
            cookies.set(trimmed.slice(0, separator), trimmed);
        }
    }
    return [...cookies.values()].join("; ");
}

async function postAuthJson(origin, path, cookie, body) {
    const response = await fetch(new URL(`/api/auth/${path}`, origin), {
        method: "POST",
        headers: {
            "content-type": "application/json",
            cookie,
            origin: origin.origin,
        },
        body: JSON.stringify(body),
    });
    const text = await response.text();
    let parsed;
    try {
        parsed = JSON.parse(text);
    } catch {
        throw new Error(`generated Better Auth ${path} returned invalid JSON (${response.status}): ${text}`);
    }
    if (!response.ok) {
        throw new Error(`generated Better Auth ${path} failed (${response.status}): ${text}`);
    }
    return { body: parsed, cookie: mergeCookies(cookie, response.headers) };
}

async function provisionOrganization(origin) {
    let cookie = await signIn(origin);
    const slug = `generated-proof-${Date.now().toString(36)}-${process.pid}`;
    const created = await postAuthJson(origin, "organization/create", cookie, {
        name: "Generated Project Proof",
        slug,
        keepCurrentActiveOrganization: true,
    });
    cookie = created.cookie;
    const organizationId = created.body?.id;
    if (typeof organizationId !== "string" || organizationId.length === 0) {
        throw new Error(`generated Better Auth organization/create returned no id: ${JSON.stringify(created.body)}`);
    }

    const activated = await postAuthJson(origin, "organization/set-active", cookie, { organizationId });
    cookie = activated.cookie;
    const session = await fetchJson(new URL("/api/auth/get-session", origin), { headers: { cookie } });
    if (!session.response.ok || session.body?.session?.activeOrganizationId !== organizationId) {
        throw new Error(
            `generated Better Auth session did not activate ${organizationId}: ${JSON.stringify(session.body)}`
        );
    }
    return { cookie, token: await issueToken(origin, cookie), organizationId };
}

async function issueToken(origin, cookie) {
    const response = await fetchJson(new URL("/api/auth/token", origin), { headers: { cookie } });
    if (!response.response.ok || typeof response.body?.token !== "string") {
        throw new Error(`generated JWT issue failed: ${JSON.stringify(response.body)}`);
    }
    return response.body.token;
}

async function writeMessage(origin, token, organizationId, id, body) {
    const response = await fetchJson(new URL("/api/messages", origin), {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify({ id, organizationId, body, clientCreatedAt: Date.now() }),
    });
    if (!response.response.ok || response.body?.id !== id) {
        throw new Error(`generated message write failed: ${JSON.stringify(response.body)}`);
    }
}

async function readMessages(origin, token, organizationId) {
    const url = new URL("/api/messages", origin);
    url.searchParams.set("organizationId", organizationId);
    const response = await fetchJson(url, { headers: { authorization: `Bearer ${token}` } });
    if (!response.response.ok || !Array.isArray(response.body)) {
        throw new Error(`generated message read failed: ${JSON.stringify(response.body)}`);
    }
    return response.body;
}

async function assertMessages(origin, token, organizationId, expectedIds) {
    const rows = await readMessages(origin, token, organizationId);
    const ids = rows.map(row => row.id).sort();
    if (JSON.stringify(ids) !== JSON.stringify([...expectedIds].sort())) {
        throw new Error(`generated message readback drifted: ${JSON.stringify(rows)}`);
    }
}

async function proveLiveQuery(cwd, origin, token, organizationId, existingIds, newId, phase) {
    const installedEntry = pathToFileURL(join(cwd, "node_modules", "@chardb", "core", "dist", "index.mjs")).href;
    const { createChardbClient } = await import(installedEntry);
    const endpoint = new URL("/ws", origin);
    endpoint.protocol = "ws:";
    const db = createChardbClient({
        endpoint: endpoint.toString(),
        clientId: `generated-smoke-client-${phase}`,
        getJwt: async () => token,
    });
    try {
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error("generated live query timed out")), 15_000);
            let wrote = false;
            const subscription = db.subscribe("query#listMessages", { organizationId }, (rows, state) => {
                if (state !== "live") return;
                const ids = rows.map(row => row.id).sort();
                const before = [...existingIds].sort();
                const after = [...existingIds, newId].sort();
                if (!wrote && JSON.stringify(ids) === JSON.stringify(before)) {
                    wrote = true;
                    void writeMessage(origin, token, organizationId, newId, `${phase} live mutation`).catch(reject);
                    return;
                }
                if (wrote && JSON.stringify(ids) === JSON.stringify(after)) {
                    clearTimeout(timeout);
                    subscription.unsubscribe();
                    resolve();
                }
            });
        });
    } finally {
        db.close();
    }
}

async function fetchJson(url, init) {
    const response = await fetch(url, init);
    const text = await response.text();
    let body;
    try {
        body = JSON.parse(text);
    } catch {
        throw new Error(`expected JSON from ${url}: ${text}`);
    }
    return { response, body };
}

async function instrumentLoopbackProbe(cwd) {
    const path = join(cwd, "src", "worker.ts");
    const source = await readFile(path, "utf8");
    await writeFile(path, injectGeneratedLoopbackProbe(source));
}

async function waitForWrangler(process, url) {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
        if (process.exitCode !== null) throw new Error(`wrangler dev exited before ${url}`);
        try {
            const response = await fetch(url);
            if (response.ok) return;
        } catch {
            // The local Workerd listener is still starting.
        }
        await Bun.sleep(100);
    }
    throw new Error(`timed out waiting for ${url}`);
}

function run(command, args, cwd, extraEnvironment) {
    const result = runResult(command, args, cwd, extraEnvironment, "inherit");
    if (result.error) throw result.error;
    if (result.status !== 0) {
        throw new Error(`${command} exited with status ${String(result.status)}`);
    }
}

function runResult(command, args, cwd, extraEnvironment, stdio) {
    const subprocessEnvironment = { ...process.env, ...extraEnvironment };
    for (const variable of [
        "CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_API_KEY",
        "CLOUDFLARE_API_TOKEN",
        "CLOUDFLARE_EMAIL",
        "CF_API_KEY",
        "CF_API_TOKEN",
        "CF_EMAIL",
    ]) {
        delete subprocessEnvironment[variable];
    }
    return spawnSync(command, args, {
        cwd,
        env: subprocessEnvironment,
        stdio,
        encoding: stdio === "pipe" ? "utf8" : undefined,
    });
}
