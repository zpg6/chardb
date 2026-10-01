import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";

if (process.argv[2] === "--build") {
    const [history, version, outfile] = process.argv.slice(3);
    const root = resolve(import.meta.dir, "../..");
    const built = await Bun.build({
        entrypoints: [resolve(import.meta.dir, "chat-migration.entry.ts")],
        target: "browser",
        format: "esm",
        external: ["cloudflare:workers"],
        define: { CHAT_HISTORY: JSON.stringify(history), CHAT_VERSION: version },
        plugins: [
            {
                name: "one-chardb-runtime",
                setup(builder) {
                    builder.onResolve({ filter: /^@chardb\/core(?:\/server)?$/ }, ({ path }) => ({
                        path: resolve(root, path.endsWith("/server") ? "src/server/index.ts" : "src/index.ts"),
                    }));
                },
            },
        ],
    });
    if (!built.success) throw new Error(`chat migration bundle failed: ${built.logs.join("\n")}`);
    const source = await built.outputs[0].text();
    await writeFile(
        outfile,
        source
            .replace(
                "await import(this.#props.path.join(this.#props.migrationFolder, fileName))",
                'await Promise.reject(new Error("Node migrations unavailable"))'
            )
            .replace("await import(nodeSqlite)", 'await Promise.reject(new Error("Node sqlite unavailable"))')
    );
} else {
    await import("./migration.phase.mjs");
}
