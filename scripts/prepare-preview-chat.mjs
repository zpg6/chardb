import { cp, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fingerprintFile } from "./browser-benchmark-report.mjs";

const ROOT = path.resolve(import.meta.dirname, "..");
const CHAT = path.join(ROOT, "example", "chat");
const COPIED_FILES = ["index.html", "tsconfig.json", "vite.config.ts"];
const COPIED_DIRECTORIES = ["src"];

export function parsePreviewPrepareArgs(argv) {
    let tarball;
    let reactTarball;
    let output;
    let name = "chardb-preview";
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const argument = argv[index];
        if (argument === "--help" || argument === "-h") {
            help = true;
            continue;
        }
        if (
            argument !== "--tarball" &&
            argument !== "--react-tarball" &&
            argument !== "--output" &&
            argument !== "--name"
        ) {
            throw new Error(`Unknown preview prepare argument ${JSON.stringify(argument)}`);
        }
        const value = argv[++index];
        if (value === undefined || value.length === 0) throw new Error(`${argument} requires a value`);
        if (argument === "--tarball") tarball = value;
        else if (argument === "--react-tarball") reactTarball = value;
        else if (argument === "--output") output = value;
        else name = value;
    }
    if (!help && tarball === undefined) throw new Error("--tarball is required");
    if (!help && reactTarball === undefined) throw new Error("--react-tarball is required");
    if (!help && output === undefined) throw new Error("--output is required");
    if (!/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(name)) {
        throw new Error("--name must be a lowercase Cloudflare Worker name with at most 63 characters");
    }
    return { help, tarball, reactTarball, output, name };
}

export function renderPreviewWrangler(source, name, releaseSha256) {
    if (!/^[a-f0-9]{64}$/.test(releaseSha256)) throw new Error("preview release SHA-256 is invalid");
    if (!/^name\s*=\s*"[^"]+"$/m.test(source)) throw new Error("chat Wrangler template has no Worker name");
    const replaced = source.replace(/^name\s*=\s*"[^"]+"$/m, `name = ${JSON.stringify(name)}`);
    const release = `CDB_RELEASE_SHA256 = ${JSON.stringify(releaseSha256)}`;
    return /^\[vars\]\s*$/m.test(replaced)
        ? replaced.replace(/^\[vars\]\s*$/m, `[vars]\n${release}`)
        : `${replaced.trimEnd()}\n\n[vars]\n${release}\n`;
}

function usage() {
    return [
        "Usage: bun scripts/prepare-preview-chat.mjs [options]",
        "",
        "  --tarball <path> exact chardb package tarball",
        "  --react-tarball <path> exact @chardb/react package tarball",
        "  --output <path>  empty destination for the deployable chat",
        "  --name <name>    Cloudflare Worker name, default chardb-preview",
        "  --help           show this help",
    ].join("\n");
}

async function main() {
    const options = parsePreviewPrepareArgs(process.argv.slice(2));
    if (options.help) {
        console.log(usage());
        return;
    }
    const tarball = path.resolve(options.tarball);
    const reactTarball = path.resolve(options.reactTarball);
    const tarballFingerprint = await fingerprintFile(tarball);
    const output = path.resolve(options.output);
    await mkdir(output, { recursive: true });
    if ((await readdir(output)).length > 0) throw new Error(`preview output directory is not empty: ${output}`);

    for (const file of COPIED_FILES) await cp(path.join(CHAT, file), path.join(output, file));
    for (const directory of COPIED_DIRECTORIES) {
        await cp(path.join(CHAT, directory), path.join(output, directory), { recursive: true });
    }

    const packageJson = JSON.parse(await readFile(path.join(CHAT, "package.json"), "utf8"));
    const relativeTarball = path.relative(output, tarball).split(path.sep).join("/");
    const relativeReactTarball = path.relative(output, reactTarball).split(path.sep).join("/");
    packageJson.name = options.name;
    packageJson.scripts = {
        typecheck: "tsc --noEmit",
        build: "vite build",
        "deploy:dry": "wrangler deploy --dry-run --outdir worker-dist",
        deploy: "wrangler deploy",
    };
    const packageTarball = relativeTarball.startsWith(".") ? relativeTarball : `./${relativeTarball}`;
    packageJson.dependencies["@chardb/core"] = `file:${packageTarball}`;
    const packageReactTarball = relativeReactTarball.startsWith(".")
        ? relativeReactTarball
        : `./${relativeReactTarball}`;
    packageJson.dependencies["@chardb/react"] = `file:${packageReactTarball}`;
    await writeFile(path.join(output, "package.json"), `${JSON.stringify(packageJson, null, 2)}\n`);

    const wrangler = await readFile(path.join(CHAT, "wrangler.template.toml"), "utf8");
    await writeFile(
        path.join(output, "wrangler.toml"),
        renderPreviewWrangler(wrangler, options.name, tarballFingerprint.digest)
    );
    await writeFile(
        path.join(output, "preview-manifest.json"),
        `${JSON.stringify(
            {
                schema: "chardb.preview-deployment.v1",
                workerName: options.name,
                package: { tarball: tarballFingerprint, relativePath: relativeTarball },
                reactPackage: {
                    tarball: await fingerprintFile(reactTarball),
                    relativePath: relativeReactTarball,
                },
            },
            null,
            2
        )}\n`
    );
    console.log(`prepared ${options.name} in ${output}`);
}

if (import.meta.main) await main();
