import { exportRef } from "../server/refs.ts";
/**
 * Keep CharDB query and mutation handles safe to import from browser code.
 *
 * Browser builds receive ref-only handles. Query callbacks, mutation
 * handlers, validators, schema imports, and every other server dependency
 * disappear from the emitted module. Server builds keep the original module.
 * The transform rejects malformed or duplicate explicit refs seen during a
 * build.
 */

/**
 * Minimal vite Plugin shape used here; the full type comes from the user's
 * vite install. We avoid a hard `import type { Plugin } from "vite"` so the
 * package builds even without vite present in the dependency closure.
 */
interface VitePluginLike {
    name: string;
    enforce?: "pre" | "post";
    transform?(
        this: ViteTransformContextLike,
        code: string,
        id: string,
        options?: ViteTransformOptionsLike
    ): { code: string; map: null } | null;
}
type Plugin = VitePluginLike;

interface ViteTransformContextLike {
    readonly environment?: { readonly name?: string };
}

interface ViteTransformOptionsLike {
    readonly ssr?: boolean;
}

import { createRequire as nodeCreateRequire } from "node:module";

/**
 * Resolve the `typescript` peer dependency at runtime via `createRequire`.
 * The plugin only runs during `vite build` / `vite dev` (Node), so the
 * `node:module` import is always available; using `createRequire` keeps
 * us strict-ESM-clean and avoids strict-bundler warnings about a bare
 * `require()` in a published ESM artifact.
 */
let cachedTs: typeof import("typescript") | null | undefined = undefined;
function loadTypeScript(): typeof import("typescript") | null {
    if (cachedTs !== undefined) return cachedTs;
    try {
        const localRequire = nodeCreateRequire(import.meta.url);
        cachedTs = localRequire("typescript") as typeof import("typescript");
    } catch {
        cachedTs = null;
    }
    return cachedTs;
}

type HandleKind = "mutation" | "query";

interface SeenExport {
    readonly module: string;
    readonly exportName: string;
    readonly kind: HandleKind;
    readonly ref: string;
}

export function chardb(): Plugin {
    const seenExports: SeenExport[] = [];

    return {
        name: "chardb",
        enforce: "pre",
        transform(code, id, transformOptions) {
            const moduleId = cleanModuleId(id);
            if (!/\.(t|j)sx?$/.test(moduleId)) return null;
            const found = collectExports(code, moduleId);
            const nextSeenExports = seenExports.filter(entry => entry.module !== moduleId);
            if (found.length === 0) {
                seenExports.splice(0, seenExports.length, ...nextSeenExports);
                return null;
            }
            const refsInModule = new Set<string>();
            for (const entry of found) {
                if (refsInModule.has(entry.ref)) {
                    throw new Error(
                        `[@chardb/core/vite] Duplicate stable ref ${JSON.stringify(entry.ref)} in ${JSON.stringify(moduleId)}`
                    );
                }
                refsInModule.add(entry.ref);
                const duplicate = nextSeenExports.find(candidate => candidate.ref === entry.ref);
                if (duplicate) {
                    throw new Error(
                        `[@chardb/core/vite] Duplicate stable ref ${JSON.stringify(entry.ref)} from ` +
                            `${JSON.stringify(duplicate.module)}#${duplicate.exportName} and ` +
                            `${JSON.stringify(moduleId)}#${entry.exportName}`
                    );
                }
                nextSeenExports.push({
                    module: moduleId,
                    exportName: entry.exportName,
                    kind: entry.kind,
                    ref: entry.ref,
                });
            }
            seenExports.splice(0, seenExports.length, ...nextSeenExports);
            const hasPlannedQuery = found.some(entry => entry.kind === "query");
            const hasApiMutation = found.some(entry => entry.kind === "mutation");
            if (hasPlannedQuery || hasApiMutation) {
                const target = viteTransformTarget(this, transformOptions);
                if (target === "unknown") {
                    const description = hasApiMutation ? "api.mutation" : "planned-query";
                    throw new Error(
                        `[@chardb/core/vite] Cannot determine the Vite environment for ${description} module ${JSON.stringify(moduleId)}`
                    );
                }
                if (target === "browser") {
                    return { code: eraseBrowserHandleModule(code, moduleId, found), map: null };
                }
            }
            let mutated = code;
            for (const e of found) {
                const stamp = `;if (!${e.exportName}.__chardbExplicitRef) Object.defineProperty(${e.exportName}, "__chardbRef", { value: ${JSON.stringify(e.ref)}, enumerable: false, configurable: true });`;
                if (mutated.includes(stamp)) continue;
                mutated += `\n${stamp}`;
            }
            return mutated === code ? null : { code: mutated, map: null };
        },
    };
}

export default chardb;

interface FoundExport {
    readonly exportName: string;
    readonly kind: HandleKind;
    readonly ref: string;
}

/**
 * Discover exported calls to the public `api.query` and `api.mutation`
 * object. Aliased and namespaced imports are accepted. Local lookalikes are
 * ignored.
 */
function collectExports(code: string, id: string): FoundExport[] {
    const ts = loadTypeScript();
    if (!ts) return [];

    const apiObjects = new Set<string>();
    const namespaceAliases = new Set<string>();
    const sf = ts.createSourceFile(
        id,
        code,
        ts.ScriptTarget.Latest,
        true,
        /\.tsx?$/.test(id) ? ts.ScriptKind.TSX : ts.ScriptKind.TS
    );
    const isFromChardbServer = (mod: string): boolean => mod === "@chardb/core/server";

    const walkImports = (node: import("typescript").Node): void => {
        if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier)) {
            const mod = node.moduleSpecifier.text;
            if (!isFromChardbServer(mod)) return;
            const clause = node.importClause;
            if (!clause) return;
            const named = clause.namedBindings;
            if (named && ts.isNamedImports(named)) {
                for (const el of named.elements) {
                    const original = (el.propertyName ?? el.name).text;
                    const local = el.name.text;
                    if (original === "api") apiObjects.add(local);
                }
            } else if (named && ts.isNamespaceImport(named)) {
                namespaceAliases.add(named.name.text);
            }
        }
    };
    sf.forEachChild(walkImports);

    const kindOf = (expression: import("typescript").LeftHandSideExpression): HandleKind | undefined => {
        if (!ts.isPropertyAccessExpression(expression)) return undefined;
        const property = expression.name.text;
        if (property !== "mutation" && property !== "query") return undefined;
        if (ts.isIdentifier(expression.expression)) {
            const objectName = expression.expression.text;
            if (apiObjects.has(objectName)) return property;
        }
        if (
            ts.isPropertyAccessExpression(expression.expression) &&
            ts.isIdentifier(expression.expression.expression) &&
            namespaceAliases.has(expression.expression.expression.text) &&
            expression.expression.name.text === "api"
        ) {
            return property;
        }
        return undefined;
    };

    const configRef = (call: import("typescript").CallExpression, kind: HandleKind, exportName: string): string => {
        const config = call.arguments[0];
        if (!config || !ts.isObjectLiteralExpression(config) || call.arguments.length !== 1) {
            throw new Error(`[@chardb/core/vite] ${exportName} must use one inline config object`);
        }
        if (config.properties.some(property => ts.isSpreadAssignment(property))) {
            throw new Error(`[@chardb/core/vite] ${exportName} config cannot spread ref metadata`);
        }
        const exactNamedProperty = (name: string) =>
            config.properties.find(candidate => {
                if (!("name" in candidate) || !candidate.name) return false;
                return (
                    (ts.isIdentifier(candidate.name) || ts.isStringLiteral(candidate.name)) &&
                    candidate.name.text === name
                );
            });
        if (
            config.properties.some(
                candidate => "name" in candidate && candidate.name && ts.isComputedPropertyName(candidate.name)
            )
        ) {
            throw new Error(`[@chardb/core/vite] ${exportName} config cannot use computed properties`);
        }
        const implementation = exactNamedProperty(kind === "query" ? "query" : "handler");
        if (!implementation) {
            const field = kind === "query" ? "query" : "handler";
            throw new Error(`[@chardb/core/vite] ${kind} ${exportName} requires an inline ${field}`);
        }
        if (kind === "query") {
            const mixed = ["handler", "authority", "partitionKey", "intent"].filter(
                name => exactNamedProperty(name) !== undefined
            );
            if (mixed.length > 0) {
                throw new Error(`[@chardb/core/vite] Query ${exportName} cannot mix query with ${mixed.join(", ")}`);
            }
        }
        const refProperty = exactNamedProperty("ref");
        if (!refProperty) return exportRef(kind, exportName);
        if (!ts.isPropertyAssignment(refProperty) || !ts.isStringLiteralLike(refProperty.initializer)) {
            throw new Error(`[@chardb/core/vite] Explicit ref for ${exportName} must be a string literal`);
        }
        return validExplicitRef(refProperty.initializer.text, exportName);
    };

    const out: FoundExport[] = [];
    const visit = (node: import("typescript").Node): void => {
        if (ts.isVariableStatement(node) && node.modifiers?.some(m => m.kind === ts.SyntaxKind.ExportKeyword)) {
            for (const decl of node.declarationList.declarations) {
                if (!ts.isIdentifier(decl.name) || !decl.initializer || !ts.isCallExpression(decl.initializer)) {
                    continue;
                }
                const kind = kindOf(decl.initializer.expression);
                if (!kind) continue;
                const exportName = decl.name.text;
                out.push({ exportName, kind, ref: configRef(decl.initializer, kind, exportName) });
            }
        }
        ts.forEachChild(node, visit);
    };
    sf.forEachChild(visit);
    return out;
}

function validExplicitRef(ref: string, exportName: string): string {
    if (ref.length === 0 || !ref.includes("#")) {
        throw new Error(`[@chardb/core/vite] Explicit ref for ${exportName} must be a nonempty string containing #`);
    }
    return ref;
}

type ViteTransformTarget = "browser" | "server" | "unknown";

function viteTransformTarget(
    context: ViteTransformContextLike,
    options: ViteTransformOptionsLike | undefined
): ViteTransformTarget {
    const environmentName = context.environment?.name;
    if (environmentName === "client") return "browser";
    if (environmentName !== undefined || options?.ssr === true) return "server";
    // `ssr: false` also describes Worker transforms. Erasing on that signal
    // alone would remove the callback from Cloudflare's server bundle.
    return "unknown";
}

function eraseBrowserHandleModule(code: string, id: string, found: readonly FoundExport[]): string {
    const hasMutation = found.some(entry => entry.kind === "mutation");
    const handleNames = new Set(found.map(entry => entry.exportName));

    const ts = loadTypeScript();
    if (!ts) {
        throw new Error(`[@chardb/core/vite] Browser handle erasure for ${JSON.stringify(id)} requires TypeScript`);
    }
    const sf = ts.createSourceFile(
        id,
        code,
        ts.ScriptTarget.Latest,
        true,
        /\.tsx?$/.test(id) ? ts.ScriptKind.TSX : ts.ScriptKind.TS
    );
    const hasExportModifier = (statement: import("typescript").Statement): boolean =>
        ts.canHaveModifiers(statement) &&
        ts.getModifiers(statement)?.some(modifier => modifier.kind === ts.SyntaxKind.ExportKeyword) === true;
    const typeOnlyExport = (statement: import("typescript").ExportDeclaration): boolean =>
        statement.isTypeOnly ||
        (statement.exportClause !== undefined &&
            ts.isNamedExports(statement.exportClause) &&
            statement.exportClause.elements.every(element => element.isTypeOnly));
    const exportFailure = (): Error =>
        new Error(
            hasMutation
                ? `[@chardb/core/vite] Browser handle module ${JSON.stringify(id)} may export only erased handles and types`
                : `[@chardb/core/vite] Browser planned-query module ${JSON.stringify(id)} may export only planned queries and types`
        );

    for (const statement of sf.statements) {
        if (ts.isExportDeclaration(statement)) {
            if (typeOnlyExport(statement)) continue;
            throw exportFailure();
        }
        if (ts.isExportAssignment(statement)) {
            throw exportFailure();
        }
        if (!hasExportModifier(statement)) continue;
        if (ts.isInterfaceDeclaration(statement) || ts.isTypeAliasDeclaration(statement)) continue;
        if (ts.isVariableStatement(statement)) {
            const names = statement.declarationList.declarations.map(declaration =>
                ts.isIdentifier(declaration.name) ? declaration.name.text : undefined
            );
            if (names.length > 0 && names.every(name => name !== undefined && handleNames.has(name))) continue;
        }
        throw exportFailure();
    }

    const definitions = found
        .map(entry => {
            return `export const ${entry.exportName} = __chardbBrowserHandle(${JSON.stringify(entry.kind)}, ${JSON.stringify(entry.ref)});`;
        })
        .join("\n");
    return [
        "const __chardbBrowserHandle = (kind, ref) => Object.defineProperties(",
        "  function chardbBrowserHandle() {",
        "    throw new Error(`CharDB ${kind} handles cannot execute in the browser; pass the handle to the CharDB client or React hook`);",
        "  },",
        "  {",
        "    __chardbKind: { value: kind, enumerable: false, configurable: true },",
        "    __chardbRef: { value: ref, enumerable: false, configurable: true },",
        "    __chardbExplicitRef: { value: true, enumerable: false, configurable: true }",
        "  }",
        ");",
        definitions,
        "",
    ].join("\n");
}

function cleanModuleId(id: string): string {
    return id.replace(/[?#].*$/, "").replaceAll("\\", "/");
}
