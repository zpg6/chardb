import { readFileSync } from "node:fs";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { run } from "./test-correctness.mjs";

const ROOT = path.resolve(import.meta.dirname, "..");
const DEFAULT_OUTPUT_DIRECTORY = path.join(ROOT, ".chardb", "benchmarks", "latest");
const LIVE_SCENARIOS = ["sdk-two-tenant-mutation-fanout", "sdk-selective-subscription-refresh"];
const PLANNED_QUERY_SCENARIOS = ["planned-query-registered-pages", "native-binding-structured-select-pages"];
const MAX_SAMPLES = 20;
const WORKFLOW_JOB_BUDGET_MS = 120 * 60_000;
const WORKFLOW_SETUP_RESERVE_MS = 10 * 60_000;
const SAMPLE_PROCESS_OVERHEAD_MS = 60_000;

const PROFILE_FIELDS = {
    clientsPerTenant: { env: "CHARDB_WORKERD_CLIENTS_PER_TENANT", minimum: 1, maximum: 8 },
    mutationsPerTenant: { env: "CHARDB_WORKERD_MUTATIONS_PER_TENANT", minimum: 1, maximum: 1_024 },
    mutationBatch: { env: "CHARDB_WORKERD_MUTATION_BATCH", minimum: 1, maximum: 32 },
    subscriptions: { env: "CHARDB_WORKERD_SUBSCRIPTIONS", minimum: 1, maximum: 64 },
    refreshRounds: { env: "CHARDB_WORKERD_REFRESH_ROUNDS", minimum: 1, maximum: 64 },
    waitMs: { env: "CHARDB_WORKERD_WAIT_MS", minimum: 1_000, maximum: 60_000 },
    testTimeoutMs: { env: "CHARDB_WORKERD_TEST_TIMEOUT_MS", minimum: 5_000, maximum: 300_000 },
};

const PLANNED_QUERY_PROFILE_FIELDS = {
    channels: { env: "CDB_PLANNED_QUERY_BENCH_CHANNELS", minimum: 1, maximum: 64 },
    rowsPerChannel: { env: "CDB_PLANNED_QUERY_BENCH_ROWS_PER_CHANNEL", minimum: 1, maximum: 500 },
    registrations: { env: "CDB_PLANNED_QUERY_BENCH_REGISTRATIONS", minimum: 1, maximum: 128 },
    pageLimit: { env: "CDB_PLANNED_QUERY_BENCH_PAGE_LIMIT", minimum: 1, maximum: 100 },
    bindingQueries: { env: "CDB_PLANNED_QUERY_BENCH_BINDING_QUERIES", minimum: 1, maximum: 512 },
    bindingConcurrency: { env: "CDB_PLANNED_QUERY_BENCH_BINDING_CONCURRENCY", minimum: 1, maximum: 32 },
    testTimeoutMs: { env: "CDB_PLANNED_QUERY_BENCH_TEST_TIMEOUT_MS", minimum: 5_000, maximum: 300_000 },
};

function frozenProfile(values, defaultSamples) {
    return Object.freeze({ values: Object.freeze(values), defaultSamples });
}

export const SCALE_PROFILES = Object.freeze({
    "ci-smoke": frozenProfile(
        {
            clientsPerTenant: 1,
            mutationsPerTenant: 4,
            mutationBatch: 16,
            subscriptions: 4,
            refreshRounds: 2,
            waitMs: 5_000,
            testTimeoutMs: 30_000,
        },
        1
    ),
    "client-max-accepted": frozenProfile(
        {
            clientsPerTenant: 1,
            mutationsPerTenant: 32,
            mutationBatch: 32,
            subscriptions: 64,
            refreshRounds: 2,
            waitMs: 60_000,
            testTimeoutMs: 300_000,
        },
        3
    ),
    throughput: frozenProfile(
        {
            clientsPerTenant: 8,
            mutationsPerTenant: 1_024,
            mutationBatch: 32,
            subscriptions: 32,
            refreshRounds: 8,
            waitMs: 60_000,
            testTimeoutMs: 300_000,
        },
        5
    ),
});

export const PLANNED_QUERY_PROFILES = Object.freeze({
    "ci-smoke": frozenProfile(
        {
            channels: 8,
            rowsPerChannel: 100,
            registrations: 32,
            pageLimit: 25,
            bindingQueries: 32,
            bindingConcurrency: 8,
            testTimeoutMs: 30_000,
        },
        3
    ),
    throughput: frozenProfile(
        {
            channels: 32,
            rowsPerChannel: 500,
            registrations: 128,
            pageLimit: 100,
            bindingQueries: 512,
            bindingConcurrency: 32,
            testTimeoutMs: 300_000,
        },
        5
    ),
});

export const BENCHMARK_SUITES = Object.freeze({
    live: Object.freeze({
        id: "gateway-live-scaled-sdk",
        label: "gateway-live scaled SDK scenarios",
        command: Object.freeze(["bun", "run", "test:scale"]),
        scenarios: Object.freeze(LIVE_SCENARIOS),
        runtimeConfig: Object.freeze({
            compatibilityDate: "2025-09-01",
            compatibilityFlags: Object.freeze(["nodejs_compat"]),
        }),
        profileFields: Object.freeze(PROFILE_FIELDS),
        profiles: SCALE_PROFILES,
    }),
    "planned-query": Object.freeze({
        id: "planned-query-and-native-binding-select-v2",
        label: "planned query and native binding select scenarios",
        command: Object.freeze([
            "bun",
            "test",
            "test/workerd/planned-query.harness.test.ts",
            "--test-name-pattern",
            "scales exact ordered",
        ]),
        scenarios: Object.freeze(PLANNED_QUERY_SCENARIOS),
        runtimeConfig: Object.freeze({
            compatibilityDate: "2026-05-10",
            compatibilityFlags: Object.freeze(["nodejs_compat"]),
        }),
        profileFields: Object.freeze(PLANNED_QUERY_PROFILE_FIELDS),
        profiles: PLANNED_QUERY_PROFILES,
    }),
});

const BENCHMARK_RECORD_SCHEMAS = Object.freeze({
    live: Object.freeze({
        "sdk-two-tenant-mutation-fanout": Object.freeze({
            fields: Object.freeze([
                "clients",
                "mutations",
                "initialMs",
                "mutationMs",
                "mutationsPerSecond",
                "midConvergenceMs",
                "churnMs",
                "reconnectedClients",
                "reconnectedClientsPerSecond",
                "responseLossMutations",
                "responseLossReplayMs",
                "responseLossReplaysPerSecond",
                "exactReplayResults",
                "replayDuplicateRows",
                "committedRows",
                "opLogEntries",
                "changeSeqAdvance",
                "convergenceMs",
                "deliveryMs",
                "logicalRowDeliveries",
                "logicalRowDeliveriesPerSecond",
            ]),
            expected(profile) {
                const clients = profile.clientsPerTenant * 2;
                const mutations = profile.mutationsPerTenant * 2;
                const firstMutationCount = Math.ceil(profile.mutationsPerTenant / 2);
                const reconnectedClients = Math.ceil(profile.clientsPerTenant / 2) * 2;
                const responseLossMutations = Math.min(profile.mutationsPerTenant, profile.mutationBatch);
                const finalDeliveryRows =
                    profile.mutationsPerTenant > firstMutationCount ? clients * profile.mutationsPerTenant : 0;
                return {
                    clients,
                    mutations,
                    reconnectedClients,
                    responseLossMutations,
                    exactReplayResults: responseLossMutations,
                    replayDuplicateRows: 0,
                    committedRows: mutations + responseLossMutations,
                    opLogEntries: mutations + responseLossMutations,
                    changeSeqAdvance: mutations + responseLossMutations,
                    logicalRowDeliveries:
                        clients * firstMutationCount + reconnectedClients * firstMutationCount + finalDeliveryRows,
                };
            },
        }),
        "sdk-selective-subscription-refresh": Object.freeze({
            fields: Object.freeze([
                "subscriptions",
                "rounds",
                "writes",
                "registrationMs",
                "registrationsPerSecond",
                "recoveryMs",
                "recoveredRegistrations",
                "recoveredRegistrationsPerSecond",
                "committedRows",
                "opLogEntries",
                "changeSeqAdvance",
                "writeMs",
                "writesPerSecond",
                "refreshMs",
                "materializations",
                "materializationsPerSecond",
            ]),
            expected(profile) {
                const writes = profile.subscriptions * profile.refreshRounds;
                return {
                    subscriptions: profile.subscriptions,
                    rounds: profile.refreshRounds,
                    writes,
                    recoveredRegistrations: profile.subscriptions,
                    committedRows: writes,
                    opLogEntries: writes,
                    changeSeqAdvance: writes,
                    materializations: writes,
                };
            },
        }),
    }),
    "planned-query": Object.freeze({
        "planned-query-registered-pages": Object.freeze({
            fields: Object.freeze([
                "organizations",
                "channels",
                "rowsPerChannel",
                "seededRows",
                "registrations",
                "pageLimit",
                "exactOrderedIsolatedSnapshots",
                "registrationAndMaterializationMs",
                "registrationsPerSecond",
            ]),
            expected(profile) {
                return {
                    organizations: 2,
                    channels: profile.channels,
                    rowsPerChannel: profile.rowsPerChannel,
                    seededRows: 2 * profile.channels * profile.rowsPerChannel,
                    registrations: profile.registrations,
                    pageLimit: Math.min(profile.pageLimit, profile.rowsPerChannel),
                    exactOrderedIsolatedSnapshots: profile.registrations,
                };
            },
        }),
        "native-binding-structured-select-pages": Object.freeze({
            fields: Object.freeze([
                "organizations",
                "channels",
                "rowsPerChannel",
                "seededRows",
                "queries",
                "concurrency",
                "pageLimit",
                "exactOrderedIsolatedQueries",
                "elapsedMs",
                "queriesPerSecond",
                "minimumRequestLatencyMs",
                "p50RequestLatencyMs",
                "p95RequestLatencyMs",
                "maximumRequestLatencyMs",
            ]),
            expected(profile) {
                return {
                    organizations: 2,
                    channels: profile.channels,
                    rowsPerChannel: profile.rowsPerChannel,
                    seededRows: 2 * profile.channels * profile.rowsPerChannel,
                    queries: profile.bindingQueries,
                    concurrency: profile.bindingConcurrency,
                    pageLimit: Math.min(profile.pageLimit, profile.rowsPerChannel),
                    exactOrderedIsolatedQueries: profile.bindingQueries,
                };
            },
        }),
    }),
});

export function validateProfile(name, profile, fields = PROFILE_FIELDS) {
    if (typeof name !== "string" || name.length === 0) throw new Error("Scale profile name must be nonempty");
    if (profile === null || typeof profile !== "object" || Array.isArray(profile)) {
        throw new Error(`Scale profile ${name} must be an object`);
    }
    const keys = Object.keys(profile).sort();
    const expectedKeys = Object.keys(fields).sort();
    if (JSON.stringify(keys) !== JSON.stringify(expectedKeys)) {
        throw new Error(`Scale profile ${name} must define exactly ${expectedKeys.join(", ")}`);
    }
    for (const [field, bounds] of Object.entries(fields)) {
        const value = profile[field];
        if (!Number.isSafeInteger(value) || value < bounds.minimum || value > bounds.maximum) {
            throw new Error(
                `Scale profile ${name}.${field} must be an integer from ${bounds.minimum} through ${bounds.maximum}`
            );
        }
    }
    return profile;
}

for (const [name, profile] of Object.entries(SCALE_PROFILES)) {
    validateProfile(name, profile.values);
    if (
        !Number.isSafeInteger(profile.defaultSamples) ||
        profile.defaultSamples < 1 ||
        profile.defaultSamples > MAX_SAMPLES
    ) {
        throw new Error(`Scale profile ${name}.defaultSamples must be an integer from 1 through ${MAX_SAMPLES}`);
    }
}

for (const [name, profile] of Object.entries(PLANNED_QUERY_PROFILES)) {
    validateProfile(name, profile.values, PLANNED_QUERY_PROFILE_FIELDS);
    if (
        !Number.isSafeInteger(profile.defaultSamples) ||
        profile.defaultSamples < 1 ||
        profile.defaultSamples > MAX_SAMPLES
    ) {
        throw new Error(
            `Planned-query profile ${name}.defaultSamples must be an integer from 1 through ${MAX_SAMPLES}`
        );
    }
}

function parseBoundedInteger(name, raw, minimum, maximum) {
    if (!/^[0-9]+$/.test(raw)) throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
        throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
    }
    return value;
}

export function validateRunBudget(profile, samples, fields = PROFILE_FIELDS, scenarioCount = LIVE_SCENARIOS.length) {
    const validatedProfile = validateProfile("benchmark", profile, fields);
    const validatedSamples = parseBoundedInteger("samples", String(samples), 1, MAX_SAMPLES);
    const sampleMaximumMs = validatedProfile.testTimeoutMs * scenarioCount + SAMPLE_PROCESS_OVERHEAD_MS;
    const runMaximumMs = sampleMaximumMs * validatedSamples;
    const availableMs = WORKFLOW_JOB_BUDGET_MS - WORKFLOW_SETUP_RESERVE_MS;
    if (!Number.isSafeInteger(runMaximumMs) || runMaximumMs > availableMs) {
        throw new Error(
            `Scale run worst-case ${runMaximumMs} ms exceeds the ${availableMs} ms benchmark allowance inside the 120-minute workflow budget`
        );
    }
    return {
        workflowJobMs: WORKFLOW_JOB_BUDGET_MS,
        setupReserveMs: WORKFLOW_SETUP_RESERVE_MS,
        availableMs,
        sampleMaximumMs,
        runMaximumMs,
    };
}

export function parseScaleArgs(argv) {
    let suiteName = "live";
    let profileName = "ci-smoke";
    let samples;
    let outputDirectory = DEFAULT_OUTPUT_DIRECTORY;
    let help = false;
    for (let index = 0; index < argv.length; index++) {
        const argument = argv[index];
        if (argument === "--help" || argument === "-h") {
            help = true;
            continue;
        }
        if (
            argument !== "--suite" &&
            argument !== "--profile" &&
            argument !== "--samples" &&
            argument !== "--output-dir"
        ) {
            throw new Error(`Unknown scale benchmark argument ${JSON.stringify(argument)}`);
        }
        const value = argv[++index];
        if (value === undefined || value.length === 0) throw new Error(`${argument} requires a value`);
        if (argument === "--suite") suiteName = value;
        else if (argument === "--profile") profileName = value;
        else if (argument === "--samples") samples = parseBoundedInteger("--samples", value, 1, MAX_SAMPLES);
        else outputDirectory = path.resolve(value);
    }
    const suite = BENCHMARK_SUITES[suiteName];
    if (suite === undefined) {
        throw new Error(
            `Unknown scale suite ${JSON.stringify(suiteName)}; choose ${Object.keys(BENCHMARK_SUITES).join(", ")}`
        );
    }
    const profile = suite.profiles[profileName];
    if (profile === undefined) {
        throw new Error(
            `Unknown scale profile ${JSON.stringify(profileName)} for ${suiteName}; choose ${Object.keys(suite.profiles).join(", ")}`
        );
    }
    const resolvedSamples = samples ?? profile.defaultSamples;
    validateRunBudget(profile.values, resolvedSamples, suite.profileFields, suite.scenarios.length);
    return {
        help,
        suiteName,
        profileName,
        profile: profile.values,
        samples: resolvedSamples,
        outputDirectory,
    };
}

function profileEnvironment(profile, fields, baseEnvironment) {
    const environment = { ...baseEnvironment };
    for (const [field, bounds] of Object.entries(fields)) environment[bounds.env] = String(profile[field]);
    return environment;
}

function metricRecordFromLine(line, expectedScenarios) {
    const objectStart = line.indexOf("{");
    if (objectStart === -1) return undefined;
    let value;
    try {
        value = JSON.parse(line.slice(objectStart));
    } catch {
        return undefined;
    }
    if (value?.type !== "chardb-workerd-benchmark") return undefined;
    if (typeof value.scenario !== "string" || !expectedScenarios.includes(value.scenario)) {
        throw new Error(`Unknown workerd benchmark scenario ${JSON.stringify(value.scenario)}`);
    }
    for (const [key, metric] of Object.entries(value)) {
        if (key === "type" || key === "scenario") continue;
        if (typeof metric !== "number" || !Number.isFinite(metric) || metric < 0) {
            throw new Error(`Benchmark metric ${value.scenario}.${key} must be a finite non-negative number`);
        }
    }
    return value;
}

function validateSuiteMetricRecords(metrics, suiteName, profile) {
    const suiteSchemas = BENCHMARK_RECORD_SCHEMAS[suiteName];
    if (suiteSchemas === undefined) throw new Error(`Unknown benchmark metric schema ${JSON.stringify(suiteName)}`);
    for (const metric of metrics) {
        const schema = suiteSchemas[metric.scenario];
        if (schema === undefined) {
            throw new Error(`Missing benchmark metric schema for ${suiteName}.${metric.scenario}`);
        }
        const fields = Object.keys(metric)
            .filter(field => field !== "type" && field !== "scenario")
            .sort();
        const expectedFields = [...schema.fields].sort();
        if (JSON.stringify(fields) !== JSON.stringify(expectedFields)) {
            throw new Error(`Benchmark record ${metric.scenario} must define exactly ${expectedFields.join(", ")}`);
        }
        for (const [field, expected] of Object.entries(schema.expected(profile))) {
            if (metric[field] !== expected) {
                throw new Error(
                    `Benchmark record ${metric.scenario}.${field} must equal profile-derived value ${expected}, received ${metric[field]}`
                );
            }
        }
        if (metric.scenario === "native-binding-structured-select-pages") {
            const ordered = [
                metric.minimumRequestLatencyMs,
                metric.p50RequestLatencyMs,
                metric.p95RequestLatencyMs,
                metric.maximumRequestLatencyMs,
            ];
            if (ordered.some((value, index) => index > 0 && value < ordered[index - 1])) {
                throw new Error(`Benchmark record ${metric.scenario} request latency percentiles are out of order`);
            }
        }
    }
}

export function parseHarnessMetrics(output, expectedScenarios = LIVE_SCENARIOS, validation = undefined) {
    const metrics = output
        .split(/\r?\n/)
        .map(line => metricRecordFromLine(line, expectedScenarios))
        .filter(value => value !== undefined);
    if (metrics.length !== expectedScenarios.length) {
        throw new Error(`Expected ${expectedScenarios.length} benchmark records, received ${metrics.length}`);
    }
    const scenarios = metrics.map(metric => metric.scenario);
    for (const scenario of expectedScenarios) {
        if (scenarios.filter(candidate => candidate === scenario).length !== 1) {
            throw new Error(`Expected exactly one ${scenario} benchmark record`);
        }
    }
    if (validation !== undefined) {
        validateSuiteMetricRecords(metrics, validation.suiteName, validation.profile);
    }
    return metrics;
}

function round(value) {
    return Number(value.toFixed(4));
}

function nearestRank(values, percentile) {
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.max(0, Math.ceil(percentile * sorted.length) - 1)];
}

export function summarizeSamples(records, expectedScenarios = LIVE_SCENARIOS) {
    return expectedScenarios.map(scenario => {
        const scenarioRecords = records.filter(record => record.scenario === scenario);
        const metricNames = Object.keys(scenarioRecords[0]?.metrics ?? {})
            .filter(name => name.endsWith("Ms") || name.endsWith("PerSecond"))
            .sort();
        const metrics = {};
        for (const name of metricNames) {
            const values = scenarioRecords.map(record => record.metrics[name]);
            metrics[name] = {
                minimum: Math.min(...values),
                p50: nearestRank(values, 0.5),
                p95: nearestRank(values, 0.95),
                maximum: Math.max(...values),
                mean: round(values.reduce((sum, value) => sum + value, 0) / values.length),
            };
        }
        return { scenario, sampleCount: scenarioRecords.length, metrics };
    });
}

export function collectRunMetadata(environment, startedAt, randomUUID) {
    const packageVersion = relativePath => {
        try {
            const source = readFileSync(path.join(ROOT, relativePath), "utf8");
            const value = JSON.parse(source).version;
            return typeof value === "string" && value.length > 0 ? value : "unknown";
        } catch {
            return "unknown";
        }
    };
    const git = (args, fallback, allowEmpty = false) => {
        const result = Bun.spawnSync(["git", ...args], { cwd: ROOT, stdout: "pipe", stderr: "ignore" });
        if (result.exitCode !== 0) return fallback;
        const value = result.stdout.toString().trim();
        return value.length === 0 && !allowEmpty ? fallback : value;
    };
    const gitSha = environment.GITHUB_SHA ?? git(["rev-parse", "HEAD"], "unknown");
    // GitHub Actions checks out a detached HEAD. Keep a usable local identity
    // when callers deliberately omit GitHub metadata (as the test harness does).
    const gitRef =
        environment.GITHUB_REF ??
        git(["symbolic-ref", "--quiet", "--short", "HEAD"], git(["rev-parse", "--short", "HEAD"], "unknown"));
    const gitStatus = git(["status", "--porcelain", "--untracked-files=normal"], "unknown", true);
    const cpus = os.cpus();
    return {
        id: environment.GITHUB_RUN_ID
            ? `github-${environment.GITHUB_RUN_ID}-${environment.GITHUB_RUN_ATTEMPT ?? "1"}`
            : randomUUID(),
        startedAt,
        gitSha,
        gitRef,
        gitDirty: gitStatus === "unknown" ? "unknown" : gitStatus.length > 0,
        runtime: {
            chardbVersion: packageVersion("package.json"),
            bunVersion: Bun.version,
            miniflareVersion: packageVersion("node_modules/miniflare/package.json"),
            workerdVersion: packageVersion("node_modules/workerd/package.json"),
            platform: process.platform,
            osRelease: os.release(),
            architecture: process.arch,
            cpuModel: cpus[0]?.model ?? "unknown",
            logicalCpuCount: cpus.length,
            ci: environment.CI === "true",
            runnerName: environment.RUNNER_NAME ?? "unknown",
        },
    };
}

async function defaultHarnessRun({ environment, logPath, outerTimeoutMs, suite }) {
    const stderrPath = `${logPath.slice(0, -4)}.stderr.log`;
    let failure;
    try {
        await run(suite.label, suite.command, outerTimeoutMs, {
            cwd: ROOT,
            env: environment,
            stdout: Bun.file(logPath),
            stderr: Bun.file(stderrPath),
        });
    } catch (error) {
        failure = error;
    }
    const output = await readFile(logPath, "utf8").catch(() => "");
    const errorOutput = await readFile(stderrPath, "utf8").catch(() => "");
    if (output.length > 0) process.stdout.write(output);
    if (errorOutput.length > 0) process.stderr.write(errorOutput);
    if (failure !== undefined) throw failure;
    return output;
}

async function prepareOutputDirectory(outputDirectory) {
    try {
        const entries = await readdir(outputDirectory);
        if (entries.length > 0) {
            throw new Error(`Scale output directory must be empty: ${outputDirectory}`);
        }
    } catch (error) {
        if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") {
            await mkdir(outputDirectory, { recursive: true });
            return;
        }
        throw error;
    }
}

async function writeJsonAtomic(file, value) {
    const temporary = `${file}.tmp`;
    await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
    await rename(temporary, file);
}

function failureRecord(error) {
    if (!(error instanceof Error)) return { name: "Error", message: String(error) };
    const record = { name: error.name, message: error.message };
    for (const field of ["exitCode", "signalCode", "timedOut"]) {
        if (field in error) record[field] = error[field];
    }
    return record;
}

export async function runScaleBenchmark(options, dependencies = {}) {
    const suiteName = options.suiteName ?? "live";
    const suite = BENCHMARK_SUITES[suiteName];
    if (suite === undefined) throw new Error(`Unknown scale suite ${JSON.stringify(suiteName)}`);
    const profile = validateProfile(options.profileName, options.profile, suite.profileFields);
    const samples = parseBoundedInteger("samples", String(options.samples), 1, MAX_SAMPLES);
    const budget = validateRunBudget(profile, samples, suite.profileFields, suite.scenarios.length);
    const outputDirectory = path.resolve(options.outputDirectory);
    const environment = profileEnvironment(profile, suite.profileFields, dependencies.environment ?? process.env);
    const now = dependencies.now ?? (() => new Date().toISOString());
    const runMetadata =
        dependencies.runMetadata ??
        collectRunMetadata(environment, now(), dependencies.randomUUID ?? (() => crypto.randomUUID()));
    const runHarness = dependencies.runHarness ?? defaultHarnessRun;
    const profileMetadata = { name: options.profileName, values: { ...profile } };
    const workload = {
        suite: suiteName,
        id: suite.id,
        scenarios: [...suite.scenarios],
        runtimeConfig: suite.runtimeConfig,
        profile: profileMetadata,
    };
    const records = [];
    await prepareOutputDirectory(outputDirectory);
    const runPath = path.join(outputDirectory, "run.json");
    const runState = {
        schema: "chardb.scale.run.v1",
        suite: suite.id,
        status: "running",
        run: runMetadata,
        profile: profileMetadata,
        workload,
        samples,
        completedSamples: 0,
        records: 0,
        budget,
        finishedAt: null,
        failure: null,
    };
    await writeJsonAtomic(runPath, runState);

    try {
        for (let sampleIndex = 1; sampleIndex <= samples; sampleIndex++) {
            const logPath = path.join(outputDirectory, `sample-${String(sampleIndex).padStart(3, "0")}.log`);
            const output = await runHarness({
                environment,
                logPath,
                outerTimeoutMs: budget.sampleMaximumMs,
                sampleIndex,
                suite,
            });
            for (const metric of parseHarnessMetrics(output, suite.scenarios, { suiteName, profile })) {
                const { type: _type, scenario, ...values } = metric;
                records.push({
                    schema: "chardb.scale.sample.v1",
                    suite: suite.id,
                    run: runMetadata,
                    profile: profileMetadata,
                    workload,
                    sample: { index: sampleIndex, count: samples },
                    scenario,
                    correctness: "passed",
                    metrics: values,
                });
            }
            runState.completedSamples = sampleIndex;
            runState.records = records.length;
            await writeJsonAtomic(runPath, runState);
        }
    } catch (error) {
        runState.status = "failed";
        runState.finishedAt = now();
        runState.failure = failureRecord(error);
        await writeJsonAtomic(runPath, runState);
        throw error;
    }

    const ndjsonPath = path.join(outputDirectory, "samples.ndjson");
    const reportPath = path.join(outputDirectory, "report.json");
    const report = {
        schema: "chardb.scale.report.v1",
        suite: suite.id,
        run: runMetadata,
        profile: profileMetadata,
        workload,
        samples,
        records: records.length,
        summaries: summarizeSamples(records, suite.scenarios),
    };
    await writeFile(ndjsonPath, `${records.map(record => JSON.stringify(record)).join("\n")}\n`, "utf8");
    await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    runState.status = "completed";
    runState.finishedAt = now();
    await writeJsonAtomic(runPath, runState);
    console.info(
        JSON.stringify({
            type: "chardb-scale-run-report",
            suite: suiteName,
            profile: options.profileName,
            samples,
            ndjsonPath,
            reportPath,
        })
    );
    return { records, report, runPath, ndjsonPath, reportPath };
}

function usage() {
    return [
        "Usage: bun scripts/run-scale-benchmark.mjs [options]",
        "",
        `  --suite <name>       ${Object.keys(BENCHMARK_SUITES).join(" | ")} (default: live)`,
        "  --profile <name>     named profile for the selected suite (default: ci-smoke)",
        `  --samples <count>    1-${MAX_SAMPLES} (default: selected profile)`,
        "  --output-dir <path>  artifact directory (default: .chardb/benchmarks/latest)",
        "  --help               show this help",
    ].join("\n");
}

if (import.meta.main) {
    try {
        const options = parseScaleArgs(process.argv.slice(2));
        if (options.help) console.log(usage());
        else await runScaleBenchmark(options);
    } catch (error) {
        console.error(error instanceof Error ? error.message : error);
        process.exit(1);
    }
}
