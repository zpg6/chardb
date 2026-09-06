import { extname, isAbsolute, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { ChardbClients } from "../../server/chardb.ts";
import { type ApiHandle, collectApiHandles } from "../api-handles.ts";
import type { CliContext, CliFileChange } from "../context.ts";
import { runInspection } from "../inspector.ts";
import { RUST_HEADER, renderRustModule } from "../rust-module.ts";
import { TS_HEADER, renderTsModule } from "../ts-module.ts";

export const WORKER_ENTRY = "src/worker.ts";
const LANGUAGES = {
    rust: { extension: ".rs", header: RUST_HEADER, render: renderRustModule },
    ts: { extension: ".ts", header: TS_HEADER, render: renderTsModule },
} as const;

export interface GenerateOptions {
    /** Report stale or missing modules instead of writing them. */
    readonly check: boolean;
}

/** What client generation reads from the app `src/worker.ts` exports. */
export interface GeneratedApp {
    readonly api: Readonly<Record<string, unknown>>;
    readonly schema: Record<string, unknown>;
    readonly clients?: ChardbClients;
}

function isGeneratedApp(value: unknown): value is GeneratedApp {
    if ((typeof value !== "object" && typeof value !== "function") || value === null) return false;
    const app = value as { readonly api?: unknown; readonly schema?: unknown; readonly DB?: unknown };
    return (
        typeof app.api === "object" &&
        app.api !== null &&
        typeof app.schema === "object" &&
        app.schema !== null &&
        typeof app.DB === "function"
    );
}

/** A declared module path: a relative source file with the language's extension. */
function modulePath(language: keyof ChardbClients, path: unknown, extension: string): string {
    const where = `clients.${language}`;
    if (typeof path !== "string") throw new Error(`${where} must be a string path`);
    if (isAbsolute(path) || /[\\/]$/.test(path)) throw new Error(`${where} must be a relative file path: ${path}`);
    if (extname(path) !== extension || path.endsWith(`.d${extension}`)) {
        throw new Error(`${where} must be a source module ending in ${extension}: ${path}`);
    }
    return path;
}

/** Render every configured client module and write the ones whose bytes changed. */
export async function generateClients(app: GeneratedApp, ctx: CliContext, check: boolean): Promise<void> {
    const targets: {
        readonly path: string;
        readonly header: string;
        readonly render: (handles: readonly ApiHandle[]) => string;
    }[] = [];
    for (const language of ["rust", "ts"] as const) {
        const path = app.clients?.[language];
        if (path === undefined) continue;
        const { extension, header, render } = LANGUAGES[language];
        targets.push({ path: modulePath(language, path, extension), header, render });
    }
    if (targets.length === 0) {
        ctx.stdout("chardb: no clients configured\n");
        return;
    }
    const handles = collectApiHandles(app.api, app.schema);
    const stale: string[] = [];
    const changes: CliFileChange[] = [];
    for (const { path, header, render } of targets) {
        const target = resolve(ctx.cwd, path);
        const contents = render(handles);
        const current = (await ctx.exists(target)) ? await ctx.read(target) : null;
        if (current === contents) continue;
        if (current !== null && !current.startsWith(header)) {
            throw new Error(`${path} is not a generated module; move it or point clients elsewhere`);
        }
        stale.push(`${path} is ${current === null ? "missing" : "stale"}`);
        changes.push({ path: target, contents, expectedContents: current });
    }
    if (changes.length === 0) return;
    if (check) throw new Error(`${stale.join(", ")}; run chardb generate`);
    if (!ctx.writeFilesAtomic) throw new Error("atomic client module writes are unavailable");
    await ctx.writeFilesAtomic(changes);
    for (const change of changes) ctx.stdout(`chardb: wrote ${relative(ctx.cwd, change.path)}\n`);
}

/** Hidden fresh-process boundary: load the app and generate in the child, where its modules can be evaluated. */
export async function runGenerateInspect(ctx: CliContext, options: GenerateOptions): Promise<void> {
    const worker = (await import(pathToFileURL(`${ctx.cwd}/${WORKER_ENTRY}`).href)) as Record<string, unknown>;
    const apps = new Set([worker.default, ...Object.values(worker)].filter(isGeneratedApp));
    if (apps.size !== 1) {
        throw new Error(
            apps.size === 0
                ? `${WORKER_ENTRY} must export the app returned by chardb()`
                : `${WORKER_ENTRY} exports more than one chardb() app`
        );
    }
    for (const app of apps) await generateClients(app, ctx, options.check);
}

/** Write the client modules `src/worker.ts` declares, or with `check` fail when any is stale. */
export async function runGenerate(ctx: CliContext, options: GenerateOptions): Promise<void> {
    if (!(await ctx.exists(`${ctx.cwd}/${WORKER_ENTRY}`))) throw new Error(`${WORKER_ENTRY} is missing`);
    const result = await runInspection(ctx, ["__generate-inspect", ...(options.check ? ["--check"] : [])]);
    if (result.stdout) ctx.stdout(result.stdout);
    if (result.exitCode === 0) return;
    // Warnings the app prints while loading are tolerated; they only surface next to a failure.
    const detail = result.stderr.trim().replaceAll(/\s+/g, " ").slice(0, 2_048);
    throw new Error(detail || "client generation failed in a fresh Bun process");
}
