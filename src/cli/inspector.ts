/** Fresh-process inspection shared by `migrations generate` and `api rust`. */

import { fileURLToPath } from "node:url";
import type { CliCommandResult, CliContext } from "./context.ts";

export const INSPECTION_TIMEOUT_MS = 15_000;
export const INSPECTION_OUTPUT_BYTES = 16 * 1_024 * 1_024 + 1_024;

/** Bun evaluates the app's modules, so the `cloudflare:workers` shim must be preloaded first. */
export function inspectorSelfArgs(executable: string, args: readonly string[]): readonly string[] {
    if (executable !== process.execPath) return args;
    const source = import.meta.url.endsWith(".ts");
    const preloadPath = fileURLToPath(
        new URL(source ? "./schema-inspector-preload.ts" : "../cli/schema-inspector-preload.mjs", import.meta.url)
    );
    return ["--preload", preloadPath, ...args];
}

/** Run a hidden inspection subcommand of this CLI in a fresh Bun process. */
export function runInspection(ctx: CliContext, subcommand: readonly string[]): Promise<CliCommandResult> {
    if (!ctx.runCommand || !ctx.selfCommand) throw new Error("fresh Bun inspection is unavailable");
    return ctx.runCommand({
        executable: ctx.selfCommand.executable,
        args: [...inspectorSelfArgs(ctx.selfCommand.executable, ctx.selfCommand.args), ...subcommand],
        cwd: ctx.cwd,
        timeoutMs: INSPECTION_TIMEOUT_MS,
        maxOutputBytes: INSPECTION_OUTPUT_BYTES,
    });
}
