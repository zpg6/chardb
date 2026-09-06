/** Keep the client modules an app declares current while the Vite dev server runs. */

import { type ChildProcess, spawn } from "node:child_process";
import { isAbsolute, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export interface GenerateRun {
    readonly exitCode: number;
    readonly stdout: string;
    readonly stderr: string;
}

export interface ClientGeneratorOptions {
    readonly root: string;
    readonly run: (root: string) => Promise<GenerateRun>;
    readonly log: { info(message: string): void; warn(message: string): void };
}

export interface ClientGenerator {
    /** Run now; resolves once every queued run has finished. */
    start(): Promise<void>;
    /** Note a changed file; resolves once the debounced run it may trigger has finished. */
    changed(file: string): Promise<void>;
}

export interface ViteResolvedConfigLike {
    readonly root: string;
    readonly command: "build" | "serve";
    readonly mode: string;
    readonly isPreview?: boolean;
    readonly logger: ClientGeneratorOptions["log"];
}

export interface ClientGenerationHooks {
    configResolved(config: ViteResolvedConfigLike): void;
    configureServer(): void;
    watchChange(id: string): void;
}

const SOURCE = /\.(?:[cm]?[jt]s|[jt]sx)$/;
const WROTE = "chardb: wrote ";
const NO_CLIENTS = "chardb: no clients configured";
const CLI_PREFIX = "chardb generate: ";
/** Where `clients` is declared, so a project without any only reruns when this file changes. */
const WORKER_ENTRY = "src/worker.ts";
const DEBOUNCE_MS = 150;
const BIN = fileURLToPath(
    new URL(import.meta.url.endsWith(".ts") ? "../cli/bin.ts" : "../cli/bin.mjs", import.meta.url)
);

let current: ChildProcess | null = null;

/** `relative()` answers with `..` for a sibling and, on Windows, an absolute path for another drive. */
const isOutside = (path: string): boolean => path.startsWith("..") || isAbsolute(path);

/** `chardb generate` in `root`, under Bun so the app's Worker modules can be evaluated. A run still going is replaced. */
export function runGenerate(root: string, check = false): Promise<GenerateRun> {
    return new Promise((done, reject) => {
        current?.kill();
        const child = spawn("bun", [BIN, "generate", ...(check ? ["--check"] : [])], {
            cwd: root,
            stdio: ["ignore", "pipe", "pipe"],
            windowsHide: true,
        });
        current = child;
        let stdout = "";
        let stderr = "";
        child.stdout.setEncoding("utf8").on("data", chunk => {
            stdout += chunk;
        });
        child.stderr.setEncoding("utf8").on("data", chunk => {
            stderr += chunk;
        });
        child.on("error", reject);
        child.on("close", code => {
            if (current === child) current = null;
            done({ exitCode: child.killed ? 0 : (code ?? 1), stdout, stderr });
        });
    });
}

export function createClientGenerator({ root, run, log }: ClientGeneratorOptions): ClientGenerator {
    const worker = resolve(root, WORKER_ENTRY);
    let inFlight: Promise<void> | null = null;
    let pending = false;
    let disabled = false;
    let dormant = false;
    let lastFailure = "";
    let timer: ReturnType<typeof setTimeout> | null = null;
    let idle: { readonly promise: Promise<void>; readonly resolve: () => void } | null = null;

    async function once(): Promise<void> {
        let result: GenerateRun;
        try {
            result = await run(root);
        } catch (error) {
            disabled = true;
            log.warn(
                `[chardb] generate is off for this session: ${error instanceof Error ? error.message : String(error)}`
            );
            return;
        }
        const lines = result.stdout.split("\n").map(line => line.trimEnd());
        for (const line of lines) if (line.startsWith(WROTE)) log.info(`[chardb] wrote ${line.slice(WROTE.length)}`);
        if (lines.includes(NO_CLIENTS) && !dormant) {
            log.info(`[chardb] no clients declared on chardb(); generate waits for ${WORKER_ENTRY} to change`);
        }
        dormant = lines.includes(NO_CLIENTS);
        const message = result.stderr.trim();
        const failure =
            result.exitCode === 0 ? "" : message.startsWith(CLI_PREFIX) ? message.slice(CLI_PREFIX.length) : message;
        if (failure && failure !== lastFailure) log.warn(`[chardb] generate failed: ${failure}`);
        lastFailure = failure;
    }

    async function pump(): Promise<void> {
        do {
            pending = false;
            await once();
        } while (pending && !disabled);
        inFlight = null;
    }

    function schedule(): Promise<void> {
        if (disabled) return Promise.resolve();
        if (inFlight) {
            pending = true;
            return inFlight;
        }
        inFlight = pump();
        return inFlight;
    }

    return {
        start: schedule,
        changed(file) {
            if (
                disabled ||
                !SOURCE.test(file) ||
                isOutside(relative(root, file)) ||
                (dormant && resolve(file) !== worker)
            ) {
                return Promise.resolve();
            }
            if (!idle) {
                let resolveIdle!: () => void;
                const promise = new Promise<void>(done => {
                    resolveIdle = done;
                });
                idle = { promise, resolve: resolveIdle };
            }
            const waiting = idle;
            if (timer) clearTimeout(timer);
            timer = setTimeout(() => {
                timer = null;
                idle = null;
                void schedule().then(waiting.resolve);
            }, DEBOUNCE_MS);
            return waiting.promise;
        },
    };
}

/** The plugin hooks that drive generation for the dev server only: never for `vite build`, `vite preview`, or Vitest. */
export function clientGenerationHooks(run: ClientGeneratorOptions["run"]): ClientGenerationHooks {
    let generator: ClientGenerator | null = null;
    return {
        configResolved(config) {
            if (config.command !== "serve" || config.isPreview || config.mode === "test") return;
            generator = createClientGenerator({ root: config.root, run, log: config.logger });
        },
        configureServer() {
            void generator?.start();
        },
        watchChange(id) {
            void generator?.changed(id);
        },
    };
}
