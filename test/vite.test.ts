import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { build as viteBuild } from "vite";
import { api } from "../src/server/index.ts";
import { chardb } from "../src/vite/index.ts";
import { manifestFromExports } from "../src/vite/manifest.ts";

interface PluginShape {
    name: string;
    transform: (code: string, id: string, options?: { readonly ssr?: boolean }) => { code: string; map: null } | null;
}

function makePlugin(): PluginShape {
    return chardb() as unknown as PluginShape;
}

function transform(p: PluginShape, code: string, id: string): { code: string; map: null } {
    const out = p.transform.call({ environment: { name: "ssr" } }, code, id, { ssr: true });
    expect(out).not.toBeNull();
    if (!out) throw new Error(`Expected ${id} to be transformed`);
    return out;
}

function transformServer(p: PluginShape, code: string, id: string): { code: string; map: null } {
    const out = p.transform.call({ environment: { name: "ssr" } }, code, id, { ssr: true });
    expect(out).not.toBeNull();
    if (!out) throw new Error(`Expected ${id} to be transformed for SSR`);
    return out;
}

function emittedCode(build: Awaited<ReturnType<typeof viteBuild>>): string {
    const results = (Array.isArray(build) ? build : [build]) as unknown as readonly {
        readonly output: readonly { readonly type: string; readonly code?: string }[];
    }[];
    return results
        .flatMap(result => result.output)
        .filter(output => output.type === "chunk")
        .map(output => output.code ?? "")
        .join("\n");
}

describe("@chardb/core/vite", () => {
    test("browser refs match untransformed Worker registration", async () => {
        const source = `
import { api } from "@chardb/core/server";
export const save = api.mutation({ authority: "organization", partitionKey: "organizationId", handler: () => "saved" });
export const list = api.query({ query: () => { throw new Error("must not execute at registration"); } });
`;
        const worker = new Function(
            "api",
            `${source.replace('import { api } from "@chardb/core/server";', "").replaceAll("export const", "const")};return { save, list };`
        )(api);
        const manifest = manifestFromExports(worker);
        const output = makePlugin().transform.call(
            { environment: { name: "client" } },
            source,
            "/different/build/root/api.ts",
            { ssr: false }
        );
        if (!output) throw new Error("expected browser handles");
        const browser = await import(`data:text/javascript;base64,${Buffer.from(output.code).toString("base64")}`);
        expect(manifest.mutations.has(browser.save.__chardbRef)).toBe(true);
        expect(manifest.queries.has(browser.list.__chardbRef)).toBe(true);
        expect(browser.save.__chardbRef).toBe(worker.save.__chardbRef);
        expect(browser.list.__chardbRef).toBe(worker.list.__chardbRef);
        expect(output.code).not.toContain("must not execute");
    });

    test("ignores removed helper APIs", () => {
        const p = makePlugin();
        const code = `
      import { createApi, defineMutation } from "@chardb/core/server";
      const legacy = createApi();
      export const createPost = defineMutation(async () => ({}));
      export const deletePost = legacy.mutation({ ref: "api/posts#delete", handler: () => null });
    `;
        expect(p.transform(code, "/abs/proj/src/mutations/post.ts")).toBeNull();
    });

    test("does not expose virtual modules or unused Vite hooks", () => {
        const p = makePlugin();
        expect("resolveId" in p).toBe(false);
        expect("load" in p).toBe(false);
        expect("handleHotUpdate" in p).toBe(false);
    });

    test("recognizes aliased and namespaced public api imports", () => {
        const p = makePlugin();
        const code = `
      import { api as db } from "@chardb/core/server";
      import * as chardb from "@chardb/core/server";
      export const fancy = db.mutation({ ref: "api/posts#fancy", handler: () => ({}) });
      export const list = chardb.api.query({ ref: "api/posts#list", query: db => db.select().from(posts) });
    `;
        const out = transform(p, code, "/abs/proj/src/aliased.ts");
        expect(out.code).toContain("api/posts#fancy");
        expect(out.code).toContain("api/posts#list");
        expect(out.code).toContain("__chardbRef");
    });

    test("ignores local helpers that only look like chardb definitions", () => {
        const p = makePlugin();
        const code = `
          const defineMutation = (handler) => handler;
          const api = { query: (config) => config };
          export const localMutation = defineMutation(() => ({}));
          export const localQuery = api.query({ query: () => [] });
        `;
        expect(p.transform(code, "/abs/proj/src/unrelated.ts")).toBeNull();
    });

    test("preserves public mutation and query refs", () => {
        const p = makePlugin();
        const code = `
      import { api } from "@chardb/core/server";
      export const createPost = api.mutation({ ref: "api/posts#create", handler: () => ({}) });
      export const deletePost = api.mutation({ ref: "api/posts#delete", handler: () => ({}) });
      export const listPosts = api.query({ ref: "api/posts#list", query: db => db.select().from(posts) });
      export const getPost = api.query({ ref: "api/posts#get", query: db => db.select().from(posts).limit(1) });
    `;
        const out = transform(p, code, "/abs/proj/src/routes/posts.ts?worker");
        const refs = Array.from(out.code.matchAll(/value: "([^"]+)"/g), match => match[1]);
        expect(refs).toEqual(["api/posts#create", "api/posts#delete", "api/posts#list", "api/posts#get"]);
        expect(new Set(refs).size).toBe(4);
    });

    test("derives query refs without evaluating their callbacks", () => {
        const p = makePlugin();
        const code = `
      import { api } from "@chardb/core/server";
      export const listPosts = api.query({
        args: {},
        query: (db, args) => db.select({ id: posts.id }).from(posts).where(eq(posts.id, args.id)),
      });
    `;
        expect(transform(p, code, "/abs/proj/src/routes/planned.ts").code).toContain("query#listPosts");
    });

    test("preserves explicit refs for method-form public queries", () => {
        const p = makePlugin();
        const out = transformServer(
            p,
            `
      import { api } from "@chardb/core/server";
      export const listPosts = api.query({
        ref: "api/posts#planned-list",
        args: {},
        query(db, args) { return db.select().from(posts).where(eq(posts.id, args.id)); },
      });
    `,
            "/abs/proj/src/routes/planned.ts"
        );
        expect(out.code).toContain('value: "api/posts#planned-list"');
        expect(out.code).not.toContain("src/routes/planned.ts#listPosts");
    });

    test("rejects planned query configs that mix legacy query metadata", () => {
        for (const legacy of ["handler", "authority", "partitionKey", "intent"]) {
            const p = makePlugin();
            expect(() =>
                p.transform(
                    `
            import { api } from "@chardb/core/server";
            export const listPosts = api.query({
              ref: "api/posts#planned-list",
              args: {},
              query: (db) => db.select().from(posts),
              ${legacy}: ${legacy === "authority" ? '"organization"' : "() => null"},
            });
          `,
                    "/abs/proj/src/routes/planned.ts"
                )
            ).toThrow(`Query listPosts cannot mix query with ${legacy}`);
        }
    });

    test("does not treat objects inside a planned callback as legacy metadata", () => {
        const p = makePlugin();
        const out = transformServer(
            p,
            `
      import { api } from "@chardb/core/server";
      export const listPosts = api.query({
        ref: "api/posts#planned-list",
        query: (db) => {
          const diagnostic = { handler: "local", authority: "local", ...extra };
          return db.select({ id: posts.id }).from(posts);
        },
      });
    `,
            "/abs/proj/src/routes/planned.ts"
        );
        expect(out.code).toContain('value: "api/posts#planned-list"');
    });

    test("erases planned query server dependencies from the final browser chunk and retains them in SSR", async () => {
        const fixture = await mkdtemp(path.join(tmpdir(), "chardb-planned-query-vite-"));
        const entry = path.join(fixture, "queries.ts");
        const schema = path.join(fixture, "schema.ts");
        try {
            await writeFile(
                schema,
                `
export const plannedSchema = "PLANNED_SCHEMA_IMPORT_SENTINEL";
`
            );
            await writeFile(
                entry,
                `
import { api } from "@chardb/core/server";
import { plannedSchema } from "./schema.ts";

const callbackSentinel = "PLANNED_QUERY_CALLBACK_SENTINEL";
export const listPosts = api.query({
  ref: "api/posts#planned-browser-list",
  args: {},
  query: (db) => db.select().from(plannedSchema).where(callbackSentinel).limit(1),
});
`
            );
            const browser = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: false,
                    lib: { entry, formats: ["es"] },
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const browserCode = emittedCode(browser);
            expect(browserCode).toContain("api/posts#planned-browser-list");
            expect(browserCode).toContain("__chardbKind");
            expect(browserCode).toContain("__chardbRef");
            expect(browserCode).not.toContain("PLANNED_QUERY_CALLBACK_SENTINEL");
            expect(browserCode).not.toContain("PLANNED_SCHEMA_IMPORT_SENTINEL");
            expect(browserCode).not.toContain("@chardb/core/server");
            const emittedBrowser = path.join(fixture, "browser-output.mjs");
            await writeFile(emittedBrowser, browserCode);
            const browserModule = (await import(pathToFileURL(emittedBrowser).href)) as {
                readonly listPosts: {
                    (): never;
                    readonly __chardbKind: string;
                    readonly __chardbRef: string;
                };
            };
            expect(typeof browserModule.listPosts).toBe("function");
            expect(browserModule.listPosts.__chardbKind).toBe("query");
            expect(browserModule.listPosts.__chardbRef).toBe("api/posts#planned-browser-list");
            expect(() => browserModule.listPosts()).toThrow("cannot execute in the browser");

            const server = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: false,
                    ssr: entry,
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const serverCode = emittedCode(server);
            expect(serverCode).toContain("api/posts#planned-browser-list");
            expect(serverCode).toContain("PLANNED_QUERY_CALLBACK_SENTINEL");
            expect(serverCode).toContain("PLANNED_SCHEMA_IMPORT_SENTINEL");
            expect(serverCode).toContain("@chardb/core/server");
        } finally {
            await rm(fixture, { recursive: true, force: true });
        }
    });

    test("preserves automatic refs in minified browser output and SSR", async () => {
        const fixture = await mkdtemp(path.join(tmpdir(), "chardb-mutation-vite-"));
        const entry = path.join(fixture, "mutations.ts");
        const schema = path.join(fixture, "schema.ts");
        try {
            await writeFile(
                schema,
                `
export const mutationTable = "MUTATION_SCHEMA_IMPORT_SENTINEL";
`
            );
            await writeFile(
                entry,
                `
import { api } from "@chardb/core/server";
import { mutationTable } from "./schema.ts";

const handlerSentinel = "MUTATION_HANDLER_SENTINEL";
export const savePost = api.mutation({
  args: { "~standard": { version: 1, vendor: "fixture", validate: value => ({ value }) } },
  handler: (ctx, args) => {
    ctx.db.insert(mutationTable).values({ ...args, handlerSentinel }).run();
    return { ok: true };
  },
});
`
            );
            const browser = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: true,
                    lib: { entry, formats: ["es"] },
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const browserCode = emittedCode(browser);
            expect(browserCode).toContain("mutation#savePost");
            expect(browserCode).toContain("__chardbKind");
            expect(browserCode).toContain("__chardbRef");
            expect(browserCode).not.toContain("MUTATION_HANDLER_SENTINEL");
            expect(browserCode).not.toContain("MUTATION_SCHEMA_IMPORT_SENTINEL");
            expect(browserCode).not.toContain("@chardb/core/server");
            const emittedBrowser = path.join(fixture, "browser-output.mjs");
            await writeFile(emittedBrowser, browserCode);
            const browserModule = (await import(pathToFileURL(emittedBrowser).href)) as {
                readonly savePost: {
                    (): never;
                    readonly __chardbKind: string;
                    readonly __chardbRef: string;
                };
            };
            expect(typeof browserModule.savePost).toBe("function");
            expect(browserModule.savePost.__chardbKind).toBe("mutation");
            expect(browserModule.savePost.__chardbRef).toBe("mutation#savePost");
            expect(() => browserModule.savePost()).toThrow("cannot execute in the browser");

            const server = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: true,
                    ssr: entry,
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const serverCode = emittedCode(server);
            expect(serverCode).toContain("mutation#savePost");
            expect(serverCode).toContain("MUTATION_HANDLER_SENTINEL");
            expect(serverCode).toContain("MUTATION_SCHEMA_IMPORT_SENTINEL");
            expect(serverCode).toContain("@chardb/core/server");
        } finally {
            await rm(fixture, { recursive: true, force: true });
        }
    });

    test("keeps a generated-style browser entry free of schema, handlers, and server constants", async () => {
        const fixture = await mkdtemp(path.join(tmpdir(), "chardb-generated-vite-"));
        const entry = path.join(fixture, "entry.ts");
        try {
            await Promise.all([
                writeFile(path.join(fixture, "schema.ts"), 'export const messages = "GENERATED_SCHEMA_SENTINEL";\n'),
                writeFile(
                    path.join(fixture, "api.ts"),
                    `
import { api } from "@chardb/core/server";
import { messages } from "./schema.ts";
const serverSecret = "GENERATED_MUTATION_SECRET";
export const postMessage = api.mutation({
  ref: "src/api.ts#postMessage",
  handler: ctx => ctx.db.insert(messages).values({ body: serverSecret }).run(),
});
`
                ),
                writeFile(
                    path.join(fixture, "queries.ts"),
                    `
import { api } from "@chardb/core/server";
import { messages } from "./schema.ts";
const queryImplementation = "GENERATED_QUERY_IMPLEMENTATION";
export const listMessages = api.query({
  ref: "src/queries.ts#listMessages",
  query: db => db.select().from(messages).where(queryImplementation),
});
`
                ),
                writeFile(
                    entry,
                    `
export { postMessage } from "./api.ts";
export { listMessages } from "./queries.ts";
`
                ),
            ]);

            const browser = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: false,
                    lib: { entry, formats: ["es"] },
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const browserCode = emittedCode(browser);
            expect(browserCode).toContain("src/api.ts#postMessage");
            expect(browserCode).toContain("src/queries.ts#listMessages");
            expect(browserCode).not.toContain("GENERATED_SCHEMA_SENTINEL");
            expect(browserCode).not.toContain("GENERATED_MUTATION_SECRET");
            expect(browserCode).not.toContain("GENERATED_QUERY_IMPLEMENTATION");
            expect(browserCode).not.toContain("@chardb/core/server");

            const server = await viteBuild({
                configFile: false,
                logLevel: "silent",
                plugins: [chardb()],
                build: {
                    write: false,
                    minify: false,
                    ssr: entry,
                    rollupOptions: { external: ["@chardb/core/server"] },
                },
            });
            const serverCode = emittedCode(server);
            expect(serverCode).toContain("GENERATED_SCHEMA_SENTINEL");
            expect(serverCode).toContain("GENERATED_MUTATION_SECRET");
            expect(serverCode).toContain("GENERATED_QUERY_IMPLEMENTATION");
            expect(serverCode).toContain("@chardb/core/server");
        } finally {
            await rm(fixture, { recursive: true, force: true });
        }
    });

    test("browser mutation erasure fails closed for unknown targets, mixed exports, and positional calls", () => {
        const source = `
import { api } from "@chardb/core/server";
export const savePost = api.mutation({ ref: "api/posts#save", handler: () => null });
`;
        const unknownTarget = makePlugin();
        expect(() => unknownTarget.transform(source, "/abs/proj/src/routes/mutation.ts", { ssr: false })).toThrow(
            "Cannot determine the Vite environment for api.mutation module"
        );

        const mixed = makePlugin();
        expect(() =>
            mixed.transform.call(
                { environment: { name: "client" } },
                `${source}\nexport const runtimeValue = 1;`,
                "/abs/proj/src/routes/mixed-mutation.ts",
                { ssr: false }
            )
        ).toThrow("may export only erased handles and types");

        const positional = makePlugin();
        expect(() =>
            positional.transform.call(
                { environment: { name: "client" } },
                `
import { api } from "@chardb/core/server";
export const savePost = api.mutation((_ctx, args) => args, { ref: "api/posts#save" });
`,
                "/abs/proj/src/routes/positional-mutation.ts",
                { ssr: false }
            )
        ).toThrow("must use one inline config object");

        const implicitRef = makePlugin();
        expect(
            implicitRef.transform.call(
                { environment: { name: "client" } },
                `
import { api } from "@chardb/core/server";
export const savePost = api.mutation({ handler: () => null });
`,
                "/abs/proj/src/routes/implicit-ref.ts",
                { ssr: false }
            )?.code
        ).toContain("mutation#savePost");
    });

    test("erases planned queries and api mutations together without changing either handle kind", async () => {
        const p = makePlugin();
        const output = p.transform.call(
            { environment: { name: "client" } },
            `
import { api } from "@chardb/core/server";
export type SaveArgs = { id: string };
export const save = api.mutation({ ref: "api/items#save", handler: () => null });
export const list = api.query({ ref: "api/items#list", query: db => db.select().from(items) });
`,
            "/abs/proj/src/routes/handles.ts",
            { ssr: false }
        );
        expect(output?.code).toContain('__chardbBrowserHandle("mutation", "api/items#save")');
        expect(output?.code).toContain('__chardbBrowserHandle("query", "api/items#list")');
        expect(output?.code).not.toContain("@chardb/core/server");
        expect(output?.code).not.toContain("db.select");
    });

    test("browser erasure fails closed for mixed runtime exports and rejects callback queries", () => {
        const unknownTarget = makePlugin();
        expect(() =>
            unknownTarget.transform(
                `
import { api } from "@chardb/core/server";
export const listPosts = api.query({
  ref: "api/posts#unknown-target",
  query: (db) => db.select().from(posts).limit(1),
});
`,
                "/abs/proj/src/routes/unknown-target.ts",
                { ssr: false }
            )
        ).toThrow("Cannot determine the Vite environment for planned-query module");

        const planned = makePlugin();
        expect(() =>
            planned.transform.call(
                { environment: { name: "client" } },
                `
import { api } from "@chardb/core/server";
export const listPosts = api.query({
  ref: "api/posts#planned-list",
  query: (db) => db.select().from(posts).limit(1),
});
export const browserConstant = 1;
`,
                "/abs/proj/src/routes/mixed.ts",
                { ssr: false }
            )
        ).toThrow("may export only planned queries and types");

        const legacy = makePlugin();
        expect(() =>
            legacy.transform.call(
                { environment: { name: "client" } },
                `
import { api } from "@chardb/core/server";
export const listPosts = api.query({
  ref: "api/posts#legacy-list",
  handler: async () => "LEGACY_HANDLER_SENTINEL",
});
`,
                "/abs/proj/src/routes/legacy.ts",
                { ssr: false }
            )
        ).toThrow("requires an inline query");
    });

    test("preserves explicit config refs for two mutations and a query", () => {
        const p = makePlugin();
        const code = `
      import { api } from "@chardb/core/server";
      export const createPost = api.mutation({ ref: "api/posts#create", handler: () => ({}) });
      export const deletePost = api.mutation({ ref: "api/posts#delete", handler: () => ({}) });
      export const listPosts = api.query({ ref: "api/posts#list", query: db => db.select().from(posts) });
    `;
        const out = transform(p, code, "/abs/proj/src/routes/posts.ts");
        const refs = Array.from(out.code.matchAll(/value: "([^"]+)"/g), match => match[1]);
        expect(refs).toEqual(["api/posts#create", "api/posts#delete", "api/posts#list"]);
        expect(out.code).not.toContain("src/routes/posts.ts#createPost");
    });

    test("derives organization mutation refs", () => {
        const p = makePlugin();
        expect(
            transform(
                p,
                `
          import { api } from "@chardb/core/server";
          export const save = api.mutation({
            authority: "organization",
            partitionKey: "organizationId",
            handler: () => null,
          });
        `,
                "/abs/proj/src/authority.ts"
            ).code
        ).toContain("mutation#save");
    });

    test("derives planned query refs", () => {
        const p = makePlugin();
        expect(
            transform(
                p,
                `
          import { api } from "@chardb/core/server";
          export const list = api.query({
            query: db => db.select().from(items),
          });
        `,
                "/abs/proj/src/authority-query.ts"
            ).code
        ).toContain("query#list");
    });

    test("rejects duplicate and nonliteral explicit refs", () => {
        const duplicate = makePlugin();
        expect(() =>
            duplicate.transform(
                `
          import { api } from "@chardb/core/server";
          export const first = api.mutation({ ref: "api/posts#same", handler: () => null });
          export const second = api.mutation({ ref: "api/posts#same", handler: () => null });
        `,
                "/abs/proj/src/duplicate.ts"
            )
        ).toThrow('Duplicate stable ref "api/posts#same"');

        const dynamic = makePlugin();
        expect(() =>
            dynamic.transform(
                `
          import { api } from "@chardb/core/server";
          const ref = "api/posts#dynamic";
          export const save = api.mutation({ ref, handler: () => null });
        `,
                "/abs/proj/src/dynamic.ts"
            )
        ).toThrow("must be a string literal");
    });

    test("explicit refs survive moving the source module", () => {
        const code = `
      import { api } from "@chardb/core/server";
      export const save = api.mutation({ ref: "api/items#save", handler: () => null });
    `;
        const first = transform(makePlugin(), code, "/first/src/items.ts");
        const moved = transform(makePlugin(), code, "/moved/src/domain/items.ts");
        expect(first.code).toContain('value: "api/items#save"');
        expect(moved.code).toContain('value: "api/items#save"');
    });

    test("rejects variable, spread, shorthand, and computed api configs", () => {
        const cases = [
            `const config = { ref: "api/items#save", handler: () => null }; export const save = api.mutation(config);`,
            `const common = { handler: () => null }; export const save = api.mutation({ ...common, ref: "api/items#save" });`,
            `const ref = "api/items#save"; export const save = api.mutation({ ref, handler: () => null });`,
            `const key = "ref"; export const save = api.mutation({ [key]: "api/items#save", handler: () => null });`,
        ];
        for (const source of cases) {
            const p = makePlugin();
            expect(() =>
                p.transform(`import { api } from "@chardb/core/server"; ${source}`, "/abs/proj/src/rejected.ts")
            ).toThrow();
        }
    });

    test("fails clearly when two modules produce the same stable ref", () => {
        const p = makePlugin();
        const code = `
      import { api } from "@chardb/core/server";
      export const save = api.mutation({ ref: "src/shared.ts#save", handler: () => ({}) });
    `;
        transform(p, code, "/workspace/first/src/shared.ts");
        expect(() => p.transform(code, "/workspace/second/src/shared.ts")).toThrow(
            'Duplicate stable ref "src/shared.ts#save"'
        );
    });

    test("releases a stable ref when its module stops exporting the handle", () => {
        const p = makePlugin();
        const source = `
import { api } from "@chardb/core/server";
export const save = api.mutation({ ref: "api/items#save", handler: () => null });
`;
        transform(p, source, "/workspace/src/old.ts");
        expect(p.transform("export const ordinary = 1;", "/workspace/src/old.ts")).toBeNull();
        expect(() => transform(p, source, "/workspace/src/new.ts")).not.toThrow();
    });
});
