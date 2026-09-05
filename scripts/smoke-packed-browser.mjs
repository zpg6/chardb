import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { release, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { chromium } from "playwright-core";
import { buildBrowserProofReport, fingerprintFile, writeJsonAtomically } from "./browser-proof-report.mjs";
import { isolateProcessTree, preserveFailure, settleBounded, spawnManagedProcess } from "./process-lifecycle.mjs";

if (await isolateProcessTree(import.meta.url, { label: "packed browser smoke", timeoutMs: 15 * 60_000 })) {
    process.exit(0);
}

const ADMIN_TOKEN = "local-chardb-admin";
const tarballArgument = process.argv[2];
const reactArgumentIndex = process.argv.indexOf("--react");
const reactTarballArgument = reactArgumentIndex < 0 ? undefined : process.argv[reactArgumentIndex + 1];
if (!tarballArgument || !reactTarballArgument) {
    throw new Error("usage: bun scripts/smoke-packed-browser.mjs <core.tgz> --react <react.tgz>");
}

const tarballPath = resolve(tarballArgument);
const reactTarballPath = resolve(reactTarballArgument);
const reportPath = resolve(process.env.CDB_BROWSER_PROOF_REPORT ?? `${tarballPath}.browser-proof.json`);
const runStartedAt = new Date().toISOString();
const scratch = await mkdtemp(join(tmpdir(), "chardb-packed-browser-"));
const bootstrap = join(scratch, "bootstrap");
const project = join(scratch, "browser-app");
const npmCache = process.env.CHARDB_BROWSER_NPM_CACHE ?? join(scratch, "npm-cache");
const environment = {
    npm_config_cache: npmCache,
    WRANGLER_LOG_PATH: join(scratch, "wrangler.log"),
    WRANGLER_SEND_METRICS: "false",
};
const PROTECTED_GENERATED_FILES = [
    ".gitignore",
    "index.html",
    "package.json",
    "README.md",
    "tsconfig.json",
    "vite.config.ts",
    "wrangler.toml",
    "scripts/build.mjs",
    "scripts/dev.mjs",
    "src/api.ts",
    "src/auth.ts",
    "src/migrations.ts",
    "src/migrations/v1.ts",
    "src/queries.ts",
    "src/schema.ts",
    "src/web/App.tsx",
    "src/web/main.tsx",
    "src/web/styles.css",
    "src/worker.ts",
];

let browser;
let context;
let running;
let proofPassed = false;
let proofReport;
let restartEvidence;
const browserErrors = [];
try {
    await mkdir(bootstrap, { recursive: true });
    await writeFile(
        join(bootstrap, "package.json"),
        `${JSON.stringify({ name: "chardb-browser-bootstrap", private: true }, null, 2)}\n`
    );
    run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", tarballPath], bootstrap);
    const packageVersion = await readPackageVersion(bootstrap, "@chardb/core");
    run(
        join(bootstrap, "node_modules", ".bin", "chardb"),
        ["init", "browser-app", "--core-package", `file:${tarballPath}`, "--react-package", `file:${reactTarballPath}`],
        scratch
    );

    const protectedBefore = await fingerprintGeneratedApp(project);
    await installPackedDependency(project);
    await assertGeneratedAppUnchanged(project, protectedBefore);

    run("npm", ["run", "typecheck"], project);
    run("npm", ["run", "build"], project);
    await assertGeneratedAppUnchanged(project, protectedBefore);
    await assertPackedBrowserBundle(project);

    const [workerPort, webPort] = await reservePorts(2);
    running = await startGeneratedApp(project, workerPort, webPort);

    const browserExecutable = findChrome();
    browser = await chromium.launch({ executablePath: browserExecutable, headless: true });
    context = await browser.newContext();
    const authRoutes = [];
    let anonymousSignInRequests = 0;
    context.on("request", request => {
        const url = new URL(request.url());
        if (url.pathname === "/api/auth/sign-in/anonymous" && request.method() === "POST") {
            anonymousSignInRequests += 1;
        }
    });
    context.on("response", response => {
        const path = new URL(response.url()).pathname;
        if (path.startsWith("/api/auth/organization/")) {
            authRoutes.push({ method: response.request().method(), path, status: response.status() });
        }
    });

    const page = await context.newPage();
    collectBrowserErrors(page, browserErrors);
    await page.goto(running.webOrigin.origin, { waitUntil: "domcontentloaded" });
    const userId = await waitForAnonymousSignIn(page);

    const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const first = { name: `Alpha ${runId}`, slug: `alpha-${runId}` };
    const second = { name: `Beta ${runId}`, slug: `beta-${runId}` };
    const firstOrganizationId = await createOrganization(page, first);
    const firstMessage = `alpha message ${runId}`;
    const firstAttachment = Buffer.from(`alpha attachment ${runId}`);
    await sendMessage(page, firstMessage, {
        name: "alpha.png",
        mimeType: "image/png",
        buffer: firstAttachment,
    });
    const firstAttachmentUrl = await attachmentUrl(page, firstMessage);
    await expectAttachment(page, firstAttachmentUrl, firstAttachment);

    const secondOrganizationId = await createOrganization(page, second);
    assert(secondOrganizationId !== firstOrganizationId, "Better Auth returned one id for two organizations");
    await waitForMessageAbsent(page, firstMessage);
    const secondMessage = `beta message ${runId}`;
    const secondAttachment = Buffer.from(`beta attachment ${runId}`);
    await sendMessage(page, secondMessage, {
        name: "beta.png",
        mimeType: "image/png",
        buffer: secondAttachment,
    });
    const secondAttachmentUrl = await attachmentUrl(page, secondMessage);
    await expectAttachment(page, secondAttachmentUrl, secondAttachment);
    await expectAttachmentDenied(page, firstAttachmentUrl);

    await switchOrganization(page, firstOrganizationId);
    await waitForMessage(page, firstMessage);
    await waitForMessageAbsent(page, secondMessage);
    await expectAttachmentDenied(page, secondAttachmentUrl);

    const livePage = await context.newPage();
    collectBrowserErrors(livePage, browserErrors);
    await livePage.goto(running.webOrigin.origin, { waitUntil: "domcontentloaded" });
    const secondPageUserId = await waitForAnonymousSignIn(livePage);
    assert(secondPageUserId === userId, "the second page opened a different Better Auth session");
    await expectActiveOrganization(livePage, firstOrganizationId);
    await waitForMessage(livePage, firstMessage);
    await waitForLiveQuery(livePage);

    const firstAttachmentId = await attachmentId(page, firstMessage);
    const replacementAttachment = Buffer.from(`replacement attachment ${runId}`);
    await replaceAttachment(page, firstMessage, {
        name: "replacement.png",
        mimeType: "image/png",
        buffer: replacementAttachment,
    });
    const replacementAttachmentId = await waitForAttachmentReplacement(livePage, firstMessage, firstAttachmentId);
    assert(replacementAttachmentId !== firstAttachmentId, "attachment replacement reused the old FileId");
    await expectAttachment(livePage, firstAttachmentUrl, replacementAttachment);

    const liveMessage = `live message ${runId}`;
    await sendMessage(page, liveMessage);
    await waitForMessage(livePage, liveMessage);

    const reshardMigrationId = `browser_move_${runId}`;
    const chardbExecutable = join(project, "node_modules", ".bin", "chardb");
    const legacyReshard = runCaptured(
        chardbExecutable,
        [
            "shards",
            "split",
            "--url",
            running.workerOrigin.origin,
            "--id",
            reshardMigrationId,
            "--lo",
            "0",
            "--hi",
            "16383",
            "--to",
            "ShardDO_1",
            "--max-steps",
            "512",
        ],
        project,
        { CHARDB_ADMIN_TOKEN: ADMIN_TOKEN }
    );
    assert(legacyReshard.status === 2, `removed chardb shards command exited with ${String(legacyReshard.status)}`);
    assert(legacyReshard.signal === null, `removed chardb shards command received ${String(legacyReshard.signal)}`);
    assert(legacyReshard.stdout === "", "removed chardb shards command wrote unexpected stdout");
    assert(
        legacyReshard.stderr ===
            "chardb shards moved to chardb experimental shards; the old command is disabled and did not run\n",
        `removed chardb shards command returned an unexpected refusal: ${JSON.stringify(legacyReshard.stderr)}`
    );
    assert(
        (await readReshardState(running.workerOrigin, reshardMigrationId)) === null,
        "removed chardb shards command created migration state"
    );
    run(
        chardbExecutable,
        [
            "experimental",
            "shards",
            "split",
            "--url",
            running.workerOrigin.origin,
            "--id",
            reshardMigrationId,
            "--lo",
            "0",
            "--hi",
            "16383",
            "--to",
            "ShardDO_1",
            "--max-steps",
            "512",
        ],
        project,
        { CHARDB_ADMIN_TOKEN: ADMIN_TOKEN }
    );
    const reshardState = await readReshardState(running.workerOrigin, reshardMigrationId);
    assert(reshardState?.phase === 6, "packaged shard controller did not finish source cleanup");
    assert(reshardState?.terminal === true, "packaged shard controller returned a nonterminal movement");
    assert(reshardState?.destinationShard === "ShardDO_1", "packaged shard controller moved to the wrong shard");
    assert(reshardState?.rangeLo === 0 && reshardState?.rangeHi === 16_383, "packaged shard range changed");
    await waitForMessage(page, firstMessage);
    await waitForMessage(livePage, liveMessage);
    await expectAttachment(livePage, firstAttachmentUrl, replacementAttachment);

    await livePage.reload({ waitUntil: "domcontentloaded" });
    assert((await waitForAnonymousSignIn(livePage)) === userId, "reload changed the Better Auth user");
    await expectActiveOrganization(livePage, firstOrganizationId);
    await waitForMessage(livePage, liveMessage);

    const sessionBeforeRestart = await readBrowserSession(context, running.webOrigin);
    assert(sessionBeforeRestart.userId === userId, "pre-restart Better Auth session changed the user");
    assert(
        sessionBeforeRestart.activeOrganizationId === firstOrganizationId,
        "pre-restart Better Auth session changed the active organization"
    );
    const cookiesBeforeRestart = await fingerprintBrowserCookies(context, running.webOrigin);
    assert(cookiesBeforeRestart.length > 0, "pre-restart browser context had no cookies for the generated app");
    const anonymousSignInsBeforeRestart = anonymousSignInRequests;
    const beforeRestart = {
        pid: running.subprocess.pid,
        workerOrigin: running.workerOrigin.origin,
        webOrigin: running.webOrigin.origin,
    };

    await page.goto("about:blank");
    await livePage.goto("about:blank");
    assert(page.url() === "about:blank" && livePage.url() === "about:blank", "browser pages did not leave the app");
    await stopGeneratedApp(running);
    running = await startGeneratedApp(project, workerPort, webPort);
    const afterRestart = {
        pid: running.subprocess.pid,
        workerOrigin: running.workerOrigin.origin,
        webOrigin: running.webOrigin.origin,
    };
    const checkpointPages = { primary: page.url(), live: livePage.url() };
    assert(
        checkpointPages.primary === "about:blank" && checkpointPages.live === "about:blank",
        "browser pages navigated before the restart checkpoint"
    );

    const cookiesAfterRestart = await fingerprintBrowserCookies(context, running.webOrigin);
    assert(
        JSON.stringify(cookiesAfterRestart) === JSON.stringify(cookiesBeforeRestart),
        `dev restart changed the browser cookie jar before navigation: ${JSON.stringify({
            before: cookiesBeforeRestart,
            after: cookiesAfterRestart,
        })}`
    );
    const sessionAfterRestart = await readBrowserSession(context, running.webOrigin);
    assert(
        sessionAfterRestart.sessionId === sessionBeforeRestart.sessionId,
        "Wrangler restart changed the Better Auth session before the app loaded"
    );
    assert(
        sessionAfterRestart.userId === sessionBeforeRestart.userId,
        "Wrangler restart changed the Better Auth user before the app loaded"
    );
    assert(
        sessionAfterRestart.activeOrganizationId === sessionBeforeRestart.activeOrganizationId,
        "Wrangler restart changed the active organization before the app loaded"
    );
    assert(
        anonymousSignInRequests === anonymousSignInsBeforeRestart,
        "the pre-navigation restart checkpoint started a new anonymous session"
    );
    const anonymousSignInsAfterPreNavigation = anonymousSignInRequests;

    await livePage.goto(running.webOrigin.origin, { waitUntil: "domcontentloaded" });
    assert((await waitForAnonymousSignIn(livePage)) === userId, "Wrangler restart changed the Better Auth user");
    assert(
        anonymousSignInRequests === anonymousSignInsBeforeRestart,
        "the generated app signed in anonymously despite a valid persisted browser session"
    );
    await expectActiveOrganization(livePage, firstOrganizationId);
    await waitForMessage(livePage, firstMessage);
    await waitForMessage(livePage, liveMessage);
    await expectAttachment(livePage, firstAttachmentUrl, replacementAttachment);

    const freshContext = await browser.newContext();
    let freshAnonymousSignInRequests = 0;
    freshContext.on("request", request => {
        const url = new URL(request.url());
        if (url.pathname === "/api/auth/sign-in/anonymous" && request.method() === "POST") {
            freshAnonymousSignInRequests += 1;
        }
    });
    let freshSession;
    try {
        const freshPage = await freshContext.newPage();
        collectBrowserErrors(freshPage, browserErrors);
        await freshPage.goto(running.webOrigin.origin, { waitUntil: "domcontentloaded" });
        const freshUserId = await waitForAnonymousSignIn(freshPage);
        assert(freshUserId !== userId, "a fresh browser context unexpectedly reused the persisted browser identity");
        const freshOrganizationId = await createOrganization(freshPage, {
            name: `Fresh ${runId}`,
            slug: `fresh-${runId}`,
        });
        freshSession = await readBrowserSession(freshContext, running.webOrigin);
        assert(freshSession.userId === freshUserId, "fresh browser context changed user after organization creation");
        assert(
            freshSession.activeOrganizationId === freshOrganizationId,
            "fresh browser context did not retain its active organization"
        );
    } finally {
        await freshContext.close();
    }
    assert(freshAnonymousSignInRequests === 1, "fresh browser context did not perform exactly one anonymous sign-in");
    restartEvidence = {
        schema: "chardb.browser-restart-evidence.v1",
        checkpoint: "session-read-before-app-navigation",
        pages: checkpointPages,
        process: { beforePid: beforeRestart.pid, afterPid: afterRestart.pid },
        origins: {
            before: { worker: beforeRestart.workerOrigin, web: beforeRestart.webOrigin },
            after: { worker: afterRestart.workerOrigin, web: afterRestart.webOrigin },
        },
        session: {
            before: {
                idSha256: sha256(sessionBeforeRestart.sessionId),
                userId: sessionBeforeRestart.userId,
                activeOrganizationId: sessionBeforeRestart.activeOrganizationId,
            },
            after: {
                idSha256: sha256(sessionAfterRestart.sessionId),
                userId: sessionAfterRestart.userId,
                activeOrganizationId: sessionAfterRestart.activeOrganizationId,
            },
        },
        cookies: {
            count: cookiesBeforeRestart.length,
            beforeSha256: sha256(JSON.stringify(cookiesBeforeRestart)),
            afterSha256: sha256(JSON.stringify(cookiesAfterRestart)),
        },
        anonymousSignIns: {
            beforeRestart: anonymousSignInsBeforeRestart,
            afterPreNavigation: anonymousSignInsAfterPreNavigation,
            afterAppNavigation: anonymousSignInRequests,
            freshContext: freshAnonymousSignInRequests,
        },
        freshContext: {
            userId: freshSession.userId,
            sessionIdSha256: sha256(freshSession.sessionId),
            activeOrganizationId: freshSession.activeOrganizationId,
        },
    };

    await switchOrganization(livePage, secondOrganizationId);
    await waitForMessage(livePage, secondMessage);
    await waitForMessageAbsent(livePage, firstMessage);
    await expectAttachment(livePage, secondAttachmentUrl, secondAttachment);

    await switchOrganization(livePage, firstOrganizationId);
    await expectAttachment(livePage, firstAttachmentUrl, replacementAttachment);
    const deletionErrorCursor = browserErrors.length;
    await deleteOrganization(livePage, firstOrganizationId);
    await expectOrganizationAbsent(livePage, firstOrganizationId);
    await expectAttachmentDenied(livePage, firstAttachmentUrl);
    await switchOrganization(livePage, secondOrganizationId);
    await expectAttachment(livePage, secondAttachmentUrl, secondAttachment);

    const deletionErrors = browserErrors.splice(deletionErrorCursor);
    browserErrors.push(
        ...deletionErrors.filter(error => !/^CharDB WebSocket error CDB_FORBIDDEN for sub \d+$/.test(error))
    );

    assertBetterAuthRoutes(authRoutes);
    if (browserErrors.length > 0) throw new Error(`browser emitted errors: ${JSON.stringify(browserErrors)}`);

    const [wranglerVersion, miniflareVersion, reactVersion, viteVersion, betterAuthVersion] = await Promise.all([
        readPackageVersion(project, "wrangler"),
        readPackageVersion(project, "miniflare"),
        readPackageVersion(project, "react"),
        readPackageVersion(project, "vite"),
        readPackageVersion(project, "better-auth"),
    ]);
    proofReport = buildBrowserProofReport({
        run: { id: runId, startedAt: runStartedAt },
        package: { name: "@chardb/core", version: packageVersion, tarball: await fingerprintFile(tarballPath) },
        reactPackage: {
            name: "@chardb/react",
            version: await readPackageVersion(project, "@chardb/react"),
            tarball: await fingerprintFile(reactTarballPath),
        },
        platform: { operatingSystem: process.platform, release: release(), architecture: process.arch },
        runtime: {
            name: "generated-bun-dev-wrangler-miniflare-vite",
            bun: Bun.version,
            nodeCompatibility: process.versions.node,
            wrangler: wranglerVersion,
            miniflare: miniflareVersion,
            react: reactVersion,
            vite: viteVersion,
            betterAuth: betterAuthVersion,
            browser: { name: "chromium", version: browser.version() },
        },
        identity: { userId },
        organizations: {
            first: { id: firstOrganizationId, slug: first.slug },
            second: { id: secondOrganizationId, slug: second.slug },
        },
        files: {
            first: { bytes: firstAttachment.byteLength, sha256: sha256(firstAttachment) },
            replacement: { bytes: replacementAttachment.byteLength, sha256: sha256(replacementAttachment) },
            second: { bytes: secondAttachment.byteLength, sha256: sha256(secondAttachment) },
        },
        reshard: reshardState,
        betterAuthRoutes: authRoutes,
        restart: restartEvidence,
        invariants: {
            generatedAppUnchanged: true,
            nativeAnonymousSignIn: true,
            nativeOrganizationCreate: true,
            nativeOrganizationSwitch: true,
            organizationIsolation: true,
            liveReplacementObserved: true,
            reloadPersistenceObserved: true,
            wranglerRestartObserved: true,
            betterAuthSessionRestartObserved: true,
            noAnonymousResignInAfterRestart: true,
            freshBrowserAuthAfterRestartObserved: true,
            nativeR2UploadObserved: true,
            transactionalFileAttachObserved: true,
            authenticatedFileDownloadObserved: true,
            fileRestartPersistenceObserved: true,
            betterAuthDeletionFenceObserved: true,
            fileOrganizationIsolationObserved: true,
            fileReplacementObserved: true,
            activeOrganizationReshardObserved: true,
        },
    });
    proofPassed = true;
} finally {
    const cleanupFailures = [];
    if (context) {
        try {
            await settleBounded(() => context.close(), { label: "primary browser context close", timeoutMs: 5_000 });
            context = undefined;
        } catch (error) {
            cleanupFailures.push(error);
        }
    }
    if (browser) {
        try {
            await settleBounded(() => browser.close(), { label: "Chromium close", timeoutMs: 5_000 });
        } catch (error) {
            cleanupFailures.push(error);
        }
    }
    if (running) {
        try {
            await stopGeneratedApp(running);
        } catch (error) {
            cleanupFailures.push(error);
        }
    }
    try {
        if ((!proofPassed || cleanupFailures.length > 0) && process.env.CDB_BROWSER_KEEP_SCRATCH === "1") {
            console.error(`packed browser scratch retained at ${scratch}`);
        } else {
            await rm(scratch, { recursive: true, force: true });
        }
    } catch (error) {
        cleanupFailures.push(error);
    }
    if (cleanupFailures.length > 0) {
        console.error(new AggregateError(cleanupFailures, "packed browser cleanup failed"));
        process.exitCode = 1;
    }
}

if (process.exitCode !== 1 && proofReport !== undefined) {
    await writeJsonAtomically(reportPath, proofReport);
    console.log(JSON.stringify({ schema: proofReport.schema, ok: true, artifact: { path: reportPath } }));
}

async function fingerprintGeneratedApp(cwd) {
    return Object.fromEntries(
        await Promise.all(
            PROTECTED_GENERATED_FILES.map(async file => {
                const content = await readFile(join(cwd, file));
                return [file, createHash("sha256").update(content).digest("hex")];
            })
        )
    );
}

async function assertGeneratedAppUnchanged(cwd, expected) {
    const current = await fingerprintGeneratedApp(cwd);
    const changed = PROTECTED_GENERATED_FILES.filter(file => current[file] !== expected[file]);
    assert(changed.length === 0, `packed browser proof changed generated files: ${changed.join(", ")}`);
}

async function installPackedDependency(cwd) {
    const packagePath = join(cwd, "package.json");
    const generatedPackage = await readFile(packagePath, "utf8");
    const packageJson = JSON.parse(generatedPackage);
    packageJson.dependencies["@chardb/core"] = `file:${tarballPath}`;
    packageJson.dependencies["@chardb/react"] = `file:${reactTarballPath}`;
    await writeFile(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`);
    try {
        run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund"], cwd);
    } finally {
        await writeFile(packagePath, generatedPackage);
    }
}

async function assertPackedBrowserBundle(cwd) {
    const assets = await readdir(join(cwd, "public", "assets"));
    const code = (
        await Promise.all(
            assets
                .filter(name => name.endsWith(".js"))
                .map(name => readFile(join(cwd, "public", "assets", name), "utf8"))
        )
    ).join("\n");
    assert(code.includes("query#listMessages"), "generated browser bundle lost the planned query ref");
    assert(code.includes("mutation#postMessage"), "generated browser bundle lost the mutation ref");
    assert(
        code.includes("mutation#replaceMessageAttachment"),
        "generated browser bundle lost the replacement mutation ref"
    );
    assert(code.includes("/_chardb/files/"), "generated browser bundle lost the file client");
    assert(!code.includes("cloudflare:workers"), "generated browser bundle contains a workerd-only import");
    assert(!code.includes("defineSchemaBaseline"), "generated browser bundle contains server migration code");
    assert(
        !code.includes("better-auth/plugins/organization"),
        "generated browser bundle contains Better Auth server code"
    );
    assert(!code.includes("defineAuth"), "generated browser bundle contains the server auth schema");
}

async function readPackageVersion(cwd, packageName) {
    const packageSegments = packageName.split("/");
    const candidates = [
        join(cwd, "node_modules", ...packageSegments, "package.json"),
        join(cwd, "node_modules", "wrangler", "node_modules", ...packageSegments, "package.json"),
    ];
    for (const candidate of candidates) {
        try {
            const packageJson = JSON.parse(await readFile(candidate, "utf8"));
            if (typeof packageJson.version === "string" && packageJson.version.length > 0) return packageJson.version;
        } catch (error) {
            if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") continue;
            throw new Error(`could not read ${packageName} runtime metadata from ${candidate}`, { cause: error });
        }
    }
    throw new Error(`could not find installed package metadata for ${packageName}`);
}

async function reservePorts(count) {
    const reservations = Array.from({ length: count }, () =>
        Bun.serve({ port: 0, fetch: () => new Response("reserved") })
    );
    const ports = reservations.map(reservation => reservation.port);
    await Promise.all(reservations.map(reservation => reservation.stop(true)));
    return ports;
}

async function startGeneratedApp(cwd, workerPort, webPort) {
    const workerOrigin = new URL(`http://127.0.0.1:${workerPort}`);
    const webOrigin = new URL(`http://127.0.0.1:${webPort}`);
    const managed = spawnManagedProcess(["bun", "run", "dev"], {
        label: "generated bun run dev",
        cwd,
        env: {
            ...process.env,
            ...environment,
            CHARDB_DEV_URL: workerOrigin.origin,
            CHARDB_DEV_WEB_URL: webOrigin.origin,
        },
        stdout: "pipe",
        stderr: "pipe",
    });
    const subprocess = managed.child;
    const stdout = new Response(subprocess.stdout).text();
    const stderr = new Response(subprocess.stderr).text();
    try {
        await Promise.all([
            waitForUrl(subprocess, new URL("/health", workerOrigin)),
            waitForUrl(subprocess, webOrigin),
        ]);
        return { managed, subprocess, workerOrigin, webOrigin, stdout, stderr };
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
                label: "generated app startup output drain",
                timeoutMs: 2_000,
            });
        } catch (outputError) {
            cleanupFailure = preserveFailure(cleanupFailure, outputError, "generated app startup cleanup failed");
        }
        const primary = new Error(`${String(error)}\n${out}${err}`, { cause: error });
        throw preserveFailure(primary, cleanupFailure, "generated app startup and cleanup failed");
    }
}

async function stopGeneratedApp(instance) {
    if (!instance) return;
    let cleanupFailure;
    let exitCode;
    try {
        exitCode = await instance.managed.stop("SIGTERM");
    } catch (error) {
        cleanupFailure = error;
    }
    let out = "";
    let err = "";
    try {
        [out, err] = await settleBounded(() => Promise.all([instance.stdout, instance.stderr]), {
            label: "generated app output drain",
            timeoutMs: 2_000,
        });
    } catch (error) {
        cleanupFailure = preserveFailure(cleanupFailure, error, "generated app cleanup failed");
    }
    await Bun.sleep(500);
    if (running === instance) running = undefined;
    const primary = [0, 130, 143].includes(exitCode)
        ? undefined
        : new Error(`generated bun run dev exited with ${String(exitCode)}\n${out}${err}`);
    const combined = preserveFailure(primary, cleanupFailure, "generated app exit and cleanup failed");
    if (combined !== undefined) throw combined;
}

async function waitForUrl(subprocess, url) {
    const deadline = Date.now() + 45_000;
    while (Date.now() < deadline) {
        if (subprocess.exitCode !== null) throw new Error(`generated bun run dev exited before ${url}`);
        try {
            const response = await fetch(url);
            if (response.ok) return;
        } catch {
            // The generated local server is still opening its listener.
        }
        await Bun.sleep(100);
    }
    throw new Error(`timed out waiting for ${url}`);
}

function collectBrowserErrors(page, errors) {
    page.on("pageerror", error => errors.push(String(error)));
    page.on("response", response => {
        const url = new URL(response.url());
        if (url.pathname.startsWith("/api/auth/") && response.status() >= 400) {
            errors.push(`${response.request().method()} ${url.pathname} returned ${response.status()}`);
        }
    });
    page.on("console", message => {
        if (message.type() === "error" && !message.text().startsWith("Failed to load resource:")) {
            errors.push(message.text());
        }
    });
    page.on("websocket", socket => {
        socket.on("framereceived", event => {
            if (typeof event.payload !== "string") return;
            try {
                const message = JSON.parse(event.payload);
                if (message?.t === "error") {
                    errors.push(`CharDB WebSocket error ${String(message.code)} for sub ${String(message.subId)}`);
                }
            } catch {
                // Non-JSON frames are rejected by the client itself; they are not useful diagnostics here.
            }
        });
    });
}

async function waitForAnonymousSignIn(page) {
    const status = page.getByTestId("auth-status");
    await status.waitFor({ state: "visible", timeout: 20_000 });
    await page.waitForFunction(() => {
        const node = document.querySelector('[data-testid="auth-status"]');
        return Boolean(node?.getAttribute("data-user-id"));
    });
    const userId = await status.getAttribute("data-user-id");
    assert(userId, "generated auth status did not expose the signed-in Better Auth user id");
    return userId;
}

async function createOrganization(page, organization) {
    await page.getByTestId("create-organization-name").fill(organization.name);
    await page.getByTestId("create-organization-slug").fill(organization.slug);
    const responsePromise = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/api/auth/organization/create" && response.request().method() === "POST";
    });
    const setActiveResponsePromise = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/api/auth/organization/set-active" && response.request().method() === "POST";
    });
    await page.getByTestId("create-organization-submit").click();
    const response = await responsePromise;
    if (!response.ok()) {
        throw new Error(
            `Better Auth organization create failed with status ${response.status()}: ${await response.text()}`
        );
    }
    const setActiveResponse = await setActiveResponsePromise;
    if (!setActiveResponse.ok()) {
        throw new Error(
            `Better Auth set-active after create failed with status ${setActiveResponse.status()}: ${await setActiveResponse.text()}`
        );
    }
    const idHandle = await page.waitForFunction(slug => {
        const node = document.querySelector('[data-testid="organization-select"]');
        if (!(node instanceof HTMLSelectElement)) return false;
        return (
            [...node.options].find(option => option.dataset.slug === slug || option.textContent?.includes(slug))
                ?.value || false
        );
    }, organization.slug);
    const id = await idHandle.jsonValue();
    assert(id, `organization selector did not expose an id for ${organization.slug}`);
    await expectActiveOrganization(page, id);
    await waitForOrganizationView(page, id);
    return id;
}

async function switchOrganization(page, organizationId) {
    const responsePromise = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/api/auth/organization/set-active" && response.request().method() === "POST";
    });
    await page.getByTestId("organization-select").selectOption(organizationId);
    const response = await responsePromise;
    if (!response.ok()) {
        throw new Error(`Better Auth set-active failed with status ${response.status()}: ${await response.text()}`);
    }
    await expectActiveOrganization(page, organizationId);
    await waitForOrganizationView(page, organizationId);
}

async function expectActiveOrganization(page, organizationId) {
    try {
        await page.waitForFunction(id => {
            const node = document.querySelector('[data-testid="organization-select"]');
            return node instanceof HTMLSelectElement && node.value === id;
        }, organizationId);
    } catch (cause) {
        const state = await page.evaluate(async () => {
            const select = document.querySelector('[data-testid="organization-select"]');
            const sessionResponse = await fetch("/api/auth/get-session", { credentials: "include" });
            return {
                select:
                    select instanceof HTMLSelectElement
                        ? { value: select.value, options: [...select.options].map(option => option.value) }
                        : null,
                session: await sessionResponse.text(),
                pageError: document.querySelector(".error")?.textContent ?? null,
                bodyText: document.body.innerText,
                bodyHtml: document.body.innerHTML.slice(0, 4_000),
            };
        });
        throw new Error(
            `generated UI did not select Better Auth organization ${organizationId}: ${JSON.stringify({ ...state, browserErrors })}`,
            { cause }
        );
    }
}

async function waitForLiveQuery(page) {
    const organizationId = await page.getByTestId("organization-select").inputValue();
    assert(organizationId, "cannot wait for a live query without an active Better Auth organization");
    await waitForOrganizationView(page, organizationId);
}

async function waitForOrganizationView(page, organizationId) {
    try {
        await page.waitForFunction(id => {
            const state = document.querySelector('[data-testid="query-state"]');
            const list = document.querySelector('[data-testid="message-list"]');
            return (
                state?.getAttribute("data-organization-id") === id &&
                state.textContent === "live" &&
                list?.getAttribute("data-organization-id") === id
            );
        }, organizationId);
    } catch (cause) {
        const state = await page.evaluate(() => ({
            queryState: document.querySelector('[data-testid="query-state"]')?.textContent ?? null,
            queryOrganization:
                document.querySelector('[data-testid="query-state"]')?.getAttribute("data-organization-id") ?? null,
            pageError: document.querySelector(".error")?.textContent ?? null,
            bodyText: document.body.innerText,
        }));
        throw new Error(`organization view did not become live: ${JSON.stringify({ ...state, browserErrors })}`, {
            cause,
        });
    }
}

async function sendMessage(page, body, attachment) {
    await waitForLiveQuery(page);
    await page.getByLabel("Message", { exact: true }).fill(body);
    let uploadResponsePromise;
    if (attachment) {
        uploadResponsePromise = page.waitForResponse(response => {
            const url = new URL(response.url());
            return url.pathname === "/_chardb/files/upload" && response.request().method() === "PUT";
        });
        await page.getByTestId("message-file").setInputFiles(attachment);
    }
    await page.getByRole("button", { name: "Send" }).click();
    if (uploadResponsePromise) {
        const response = await uploadResponsePromise;
        if (!response.ok()) {
            throw new Error(`CharDB file upload failed with status ${response.status()}: ${await response.text()}`);
        }
    }
    await waitForMessage(page, body);
}

async function attachmentUrl(page, body) {
    const article = page.getByTestId("message-list").locator("article").filter({ hasText: body });
    const link = article.getByTestId("message-attachment");
    await link.waitFor({ state: "visible", timeout: 20_000 });
    const href = await link.getAttribute("href");
    assert(href, `message ${JSON.stringify(body)} did not expose its attachment URL`);
    return href;
}

async function attachmentId(page, body) {
    const article = page.getByTestId("message-list").locator("article").filter({ hasText: body });
    const id = await article.getAttribute("data-attachment-id");
    assert(id, `message ${JSON.stringify(body)} did not expose its attachment id`);
    return id;
}

async function replaceAttachment(page, body, attachment) {
    const article = page.getByTestId("message-list").locator("article").filter({ hasText: body });
    await article.getByTestId("message-replacement-file").setInputFiles(attachment);
    const responsePromise = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/_chardb/files/upload" && response.request().method() === "PUT";
    });
    await article.locator('button[type="submit"]').click();
    const response = await responsePromise;
    if (!response.ok()) {
        throw new Error(`replacement upload failed with status ${response.status()}: ${await response.text()}`);
    }
}

async function waitForAttachmentReplacement(page, body, previousId) {
    const handle = await page.waitForFunction(
        ({ text, previous }) => {
            const article = [...document.querySelectorAll("article")].find(node =>
                [...node.querySelectorAll("p")].some(paragraph => paragraph.textContent === text)
            );
            const current = article?.getAttribute("data-attachment-id");
            return current && current !== previous ? current : false;
        },
        { text: body, previous: previousId }
    );
    return handle.jsonValue();
}

async function expectAttachment(page, url, expected) {
    const actual = await page.evaluate(async path => {
        const response = await fetch(path, { credentials: "include" });
        return {
            status: response.status,
            contentType: response.headers.get("content-type"),
            disposition: response.headers.get("content-disposition"),
            bytes: [...new Uint8Array(await response.arrayBuffer())],
        };
    }, url);
    assert(actual.status === 200, `attachment ${url} returned ${actual.status}`);
    assert(actual.contentType === "image/png", `attachment ${url} returned ${actual.contentType}`);
    assert(actual.disposition === "attachment", `attachment ${url} was not forced to download`);
    assert(Buffer.from(actual.bytes).equals(expected), `attachment ${url} returned different bytes`);
}

async function expectAttachmentDenied(page, url) {
    await page.waitForFunction(async path => {
        const response = await fetch(path, { credentials: "include" });
        return response.status === 403 || response.status === 404;
    }, url);
}

async function deleteOrganization(page, organizationId) {
    await expectActiveOrganization(page, organizationId);
    const responsePromise = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/api/auth/organization/delete" && response.request().method() === "POST";
    });
    await page.getByTestId("delete-organization").click();
    const response = await responsePromise;
    if (!response.ok()) {
        throw new Error(`Better Auth organization delete failed with ${response.status()}: ${await response.text()}`);
    }
}

async function expectOrganizationAbsent(page, organizationId) {
    await page.waitForFunction(id => {
        const node = document.querySelector('[data-testid="organization-select"]');
        return node instanceof HTMLSelectElement && ![...node.options].some(option => option.value === id);
    }, organizationId);
}

async function waitForMessage(page, body) {
    await page
        .getByTestId("message-list")
        .getByText(body, { exact: true })
        .waitFor({ state: "visible", timeout: 20_000 });
}

async function waitForMessageAbsent(page, body) {
    await page.waitForFunction(text => {
        const list = document.querySelector('[data-testid="message-list"]');
        return Boolean(list) && ![...list.querySelectorAll("p")].some(node => node.textContent === text);
    }, body);
}

function assertBetterAuthRoutes(routes) {
    const createCalls = routes.filter(route => route.path === "/api/auth/organization/create" && route.status < 400);
    const setActiveCalls = routes.filter(
        route => route.path === "/api/auth/organization/set-active" && route.status < 400
    );
    const deleteCalls = routes.filter(route => route.path === "/api/auth/organization/delete" && route.status < 400);
    assert(
        createCalls.length >= 2,
        `expected two successful Better Auth organization/create calls, got ${createCalls.length}`
    );
    assert(
        setActiveCalls.length >= 2,
        `expected two successful Better Auth organization/set-active calls, got ${setActiveCalls.length}`
    );
    assert(deleteCalls.length >= 1, "expected one successful Better Auth organization/delete call");
}

async function readBrowserSession(context, webOrigin) {
    const response = await context.request.get(new URL("/api/auth/get-session", webOrigin).href, {
        headers: { origin: webOrigin.origin },
    });
    const text = await response.text();
    let body;
    try {
        body = JSON.parse(text);
    } catch {
        throw new Error(`Better Auth get-session returned invalid JSON (${response.status()}): ${text.slice(0, 512)}`);
    }
    if (!response.ok()) {
        throw new Error(
            `Better Auth get-session failed with status ${response.status()}: ${JSON.stringify({
                code: body?.code ?? null,
                message: body?.message ?? null,
            })}`
        );
    }
    const sessionId = body?.session?.id;
    const userId = body?.user?.id;
    if (typeof sessionId !== "string" || typeof userId !== "string") {
        throw new Error(
            `Better Auth get-session returned no active session: ${JSON.stringify({
                hasSession: Boolean(body?.session),
                hasUser: Boolean(body?.user),
            })}`
        );
    }
    return {
        sessionId,
        userId,
        activeOrganizationId: body.session.activeOrganizationId ?? null,
    };
}

async function fingerprintBrowserCookies(context, webOrigin) {
    const cookies = await context.cookies(webOrigin.origin);
    return cookies
        .map(cookie => ({
            name: cookie.name,
            domain: cookie.domain,
            path: cookie.path,
            secure: cookie.secure,
            httpOnly: cookie.httpOnly,
            sameSite: cookie.sameSite,
            valueSha256: sha256(cookie.value),
        }))
        .sort((left, right) =>
            `${left.name}\u0000${left.domain}\u0000${left.path}`.localeCompare(
                `${right.name}\u0000${right.domain}\u0000${right.path}`
            )
        );
}

function sha256(value) {
    return createHash("sha256").update(value).digest("hex");
}

function findChrome() {
    const candidates = [
        process.env.CHARDB_BROWSER_EXECUTABLE,
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
    ].filter(Boolean);
    const found = candidates.find(candidate => existsSync(candidate));
    if (!found) throw new Error("Chrome not found. Set CHARDB_BROWSER_EXECUTABLE to a Chrome or Chromium binary.");
    return found;
}

function run(command, args, cwd, environmentOverrides = {}) {
    const result = spawnSync(command, args, {
        cwd,
        env: commandEnvironment(environmentOverrides),
        stdio: "inherit",
    });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`${command} exited with status ${String(result.status)}`);
}

function runCaptured(command, args, cwd, environmentOverrides = {}) {
    const result = spawnSync(command, args, {
        cwd,
        encoding: "utf8",
        env: commandEnvironment(environmentOverrides),
        stdio: "pipe",
    });
    if (result.error) throw result.error;
    return result;
}

function commandEnvironment(environmentOverrides) {
    const subprocessEnvironment = { ...process.env, ...environment, ...environmentOverrides };
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
    return subprocessEnvironment;
}

async function readReshardState(workerOrigin, migrationId) {
    const response = await fetch(
        new URL(`/_chardb/shards/status?migrationId=${encodeURIComponent(migrationId)}`, workerOrigin),
        { headers: { authorization: `Bearer ${ADMIN_TOKEN}` } }
    );
    const body = await response.json();
    if (!response.ok || body?.ok !== true) {
        throw new Error(`packaged shard status failed with ${response.status}: ${JSON.stringify(body)}`);
    }
    return body.state;
}

function assert(condition, message) {
    if (!condition) throw new Error(message);
}
