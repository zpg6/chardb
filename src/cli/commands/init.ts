import { resolve } from "node:path";
import type { CliContext } from "../context.ts";
import {
    GENERATED_WRANGLER_VERSION,
    renderCloudflareDeployScript,
    renderCloudflareSetupScript,
} from "../generated-cloudflare-workflow.ts";
import { renderInitialMigrationArtifacts } from "../migration-artifacts.ts";
import { SCAFFOLD_INITIAL_SNAPSHOT } from "../scaffold-initial-snapshot.ts";
import { renderWrangler } from "../wrangler_template.ts";

export interface InitOptions {
    readonly name: string;
    readonly directory?: string;
    readonly compatibilityDate?: string;
    readonly corePackage?: string;
    readonly reactPackage?: string;
}

const ALLOWED_EXISTING_ROOT_ENTRIES = new Set([".DS_Store", ".git"]);

const DEFAULT_COMPAT_DATE = "2026-05-10";
const generatedDeploymentId = (): string => `chardb.app.v1/${crypto.randomUUID()}`;

const PACKAGE_TEMPLATE = (name: string, corePackage: string, reactPackage: string) =>
    `${JSON.stringify(
        {
            name,
            private: true,
            version: "0.0.0",
            type: "module",
            packageManager: "bun@1.2.22",
            engines: { bun: ">=1.2.22", node: ">=22" },
            scripts: {
                typecheck: "tsc --noEmit && tsc --noEmit -p test/tsconfig.json",
                test: "bun scripts/test.mjs",
                build: "bun scripts/build.mjs",
                "build:web": "vite build",
                "build:worker": "wrangler deploy --dry-run --outdir dist/worker",
                dev: "bun scripts/dev.mjs",
                "dev:web": "vite",
                "setup:cloudflare": "bun scripts/setup-cloudflare.mjs",
                "deploy:bootstrap": "bun scripts/deploy.mjs --bootstrap",
                deploy: "bun scripts/deploy.mjs",
            },
            dependencies: {
                "better-auth": "1.6.30",
                "@chardb/core": corePackage,
                "@chardb/react": reactPackage,
                "drizzle-orm": "0.45.2",
                react: "18.3.1",
                "react-dom": "18.3.1",
                uuidv7: "1.2.1",
                zod: "4.4.3",
            },
            overrides: {
                "@chardb/core": "$@chardb/core",
            },
            devDependencies: {
                "@cloudflare/vitest-plugin": "1.1.2",
                "@cloudflare/workers-types": "5.20260820.1",
                "@msw/cloudflare": "0.0.1",
                "@types/react": "18.3.31",
                "@types/react-dom": "18.3.7",
                "@vitejs/plugin-react": "6.1.0",
                msw: "2.15.0",
                typescript: "5.9.3",
                vite: "8.2.2",
                vitest: "4.1.11",
                wrangler: GENERATED_WRANGLER_VERSION,
            },
        },
        null,
        2
    )}\n`;

const TSCONFIG_TEMPLATE = `${JSON.stringify(
    {
        compilerOptions: {
            lib: ["ES2023", "DOM", "DOM.Iterable"],
            target: "ES2022",
            module: "ESNext",
            moduleResolution: "Bundler",
            moduleDetection: "force",
            allowImportingTsExtensions: true,
            verbatimModuleSyntax: true,
            noEmit: true,
            strict: true,
            exactOptionalPropertyTypes: true,
            noUncheckedIndexedAccess: true,
            skipLibCheck: true,
            isolatedModules: true,
            jsx: "react-jsx",
            types: ["@cloudflare/workers-types"],
            preserveSymlinks: true,
        },
        include: ["src/**/*.ts", "src/**/*.tsx"],
        exclude: ["node_modules", "dist", "vite.config.ts"],
    },
    null,
    2
)}\n`;

const GITIGNORE_TEMPLATE = `node_modules/
dist/
.dev.vars
.env
.env.*
!.env.example
.wrangler/
worker-configuration.d.ts
`;

const ENV_EXAMPLE_TEMPLATE = `# Optional for a custom domain or when workers.dev is disabled:
# CHARDB_URL=https://your-worker.example.com
CHARDB_ADMIN_TOKEN=replace-with-32-to-512-byte-secret
BETTER_AUTH_SECRET=replace-with-at-least-32-byte-secret
`;

const INDEX_TEMPLATE = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>chardb messages</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/web/main.tsx"></script>
  </body>
</html>
`;

const VITEST_CONFIG_TEMPLATE = `import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.toml" },
      miniflare: {
        bindings: {
          CDB_ADMIN_TOKEN: "chardb-vitest-admin",
          BETTER_AUTH_SECRET: "chardb-vitest-auth-secret-at-least-32-characters",
        },
      },
    }),
  ],
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 60_000,
  },
});
`;

const TEST_SCRIPT_TEMPLATE = `import { realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const nodeRuntime = Bun.which("node");
if (!nodeRuntime) throw new Error("Cloudflare Vitest requires Node.js on PATH");

// A canonical CLI path prevents Windows drive-letter casing from loading the
// Vitest runner twice.
const project = realpathSync.native(fileURLToPath(new URL("..", import.meta.url)));
const vitestPackage = fileURLToPath(import.meta.resolve("vitest/package.json"));
const vitestCli = realpathSync.native(join(dirname(vitestPackage), "vitest.mjs"));
const child = Bun.spawn(
  [nodeRuntime, vitestCli, "run", "--no-file-parallelism", ...process.argv.slice(2)],
  { cwd: project, stdin: "inherit", stdout: "inherit", stderr: "inherit" },
);
const exitCode = await child.exited;
if (exitCode !== 0) process.exit(exitCode);
`;

const TEST_TSCONFIG_TEMPLATE = `${JSON.stringify(
    {
        extends: "../tsconfig.json",
        compilerOptions: {
            types: ["@cloudflare/workers-types", "@cloudflare/vitest-plugin/types"],
        },
        include: ["./**/*.ts"],
    },
    null,
    2
)}\n`;

const TEST_ENV_TEMPLATE = `declare namespace Cloudflare {
  interface Env {
    CDB_CATALOG: DurableObjectNamespace;
    CDB_SHARD: DurableObjectNamespace;
    CDB_GATEWAY: DurableObjectNamespace;
    CDB_RESHARD: DurableObjectNamespace;
    CDB_FILES: R2Bucket;
    CDB_ADMIN_TOKEN: string;
    BETTER_AUTH_SECRET: string;
  }

  interface GlobalProps {
    mainModule: typeof import("../src/worker.ts");
    durableNamespaces: "Catalog" | "Cdb" | "Gateway" | "Resharder";
  }
}
`;

const WORKER_TEST_TEMPLATE = `import { setupNetwork } from "@msw/cloudflare";
import { env, exports } from "cloudflare:workers";
import { HttpResponse, http } from "msw";
import { describe, expect, test } from "vitest";
import { migrations } from "../src/migrations.ts";

const origin = "https://chardb.test";
const migrationId = "vitest-initial-schema";

async function request(path: string, init?: RequestInit): Promise<Response> {
  return exports.default.fetch(new Request(new URL(path, origin), init));
}

async function json(path: string, init?: RequestInit): Promise<{ response: Response; body: any }> {
  const response = await request(path, init);
  const text = await response.text();
  let body: any;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Error(path + " returned invalid JSON (" + response.status + "): " + text);
  }
  return { response, body };
}

async function migration(path: string, body?: Record<string, unknown>): Promise<any> {
  const result = await json("/_chardb/migrations/" + path, {
    method: body ? "POST" : "GET",
    headers: {
      authorization: "Bearer " + env.CDB_ADMIN_TOKEN,
      ...(body ? { "content-type": "application/json" } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  expect(result.response.status, JSON.stringify(result.body)).toBe(200);
  expect(result.body.ok).toBe(true);
  return result.body;
}

function responseCookies(headers: Headers): string[] {
  return headers.getSetCookie();
}

function mergeCookies(current: string, headers: Headers): string {
  const cookies = new Map<string, string>();
  const pairs = [
    ...current.split("; "),
    ...responseCookies(headers).map(value => value.split(";", 1)[0] ?? ""),
  ];
  for (const cookie of pairs) {
    const separator = cookie.indexOf("=");
    if (separator > 0) cookies.set(cookie.slice(0, separator), cookie);
  }
  return [...cookies.values()].join("; ");
}

async function auth(path: string, cookie: string, body?: Record<string, unknown>) {
  const result = await json("/api/auth/" + path, {
    method: body ? "POST" : "GET",
    headers: {
      origin,
      ...(cookie ? { cookie } : {}),
      ...(body ? { "content-type": "application/json" } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  expect(result.response.status, JSON.stringify(result.body)).toBe(200);
  return {
    body: result.body,
    cookie: mergeCookies(cookie, result.response.headers),
  };
}

describe("generated Cloudflare Worker", () => {
  test("migrates, authenticates, and persists one organization", async () => {
    expect(typeof env.CDB_CATALOG.idFromName).toBe("function");
    expect(typeof env.CDB_SHARD.idFromName).toBe("function");
    expect(typeof env.CDB_GATEWAY.idFromName).toBe("function");
    expect(typeof env.CDB_RESHARD.idFromName).toBe("function");

    const r2Key = "vitest/native-binding";
    await env.CDB_FILES.put(r2Key, "bound through wrangler.toml");
    expect(await (await env.CDB_FILES.get(r2Key))?.text()).toBe("bound through wrangler.toml");
    await env.CDB_FILES.delete(r2Key);

    const health = await json("/health");
    expect(health.response.status, JSON.stringify(health.body)).toBe(200);
    expect(health.body).toMatchObject({
      ok: true,
      deploymentId: {{DEPLOYMENT_ID}},
      schemaVersion: migrations.version,
      schemaDigest: migrations.digest,
    });
    const targetVersion = migrations.version;

    const before = await migration("state");
    expect(before.state).toMatchObject({ status: "active" });
    expect(Number.isSafeInteger(before.state.activeVersion)).toBe(true);
    expect(before.state.activeVersion).toBeLessThanOrEqual(targetVersion);

    if (before.state.activeVersion < targetVersion) {
      await migration("begin", { migrationId, targetVersion });
      const inventory = await migration("shards?migrationId=" + migrationId);
      expect(inventory.shards.map((shard: { shardId: string }) => shard.shardId)).toEqual(["ShardDO_0"]);
      await migration("shard", { migrationId, shardId: "ShardDO_0" });
      for (let version = before.state.activeVersion + 1; version <= targetVersion; version++) {
        await migration("catalog", { migrationId, version });
      }
      await migration("complete", { migrationId });
    }

    const after = await migration("state");
    expect(after.state).toMatchObject({ activeVersion: targetVersion, status: "active", migrationId: null });

    const signedIn = await auth("sign-in/anonymous", "", {});
    expect(signedIn.cookie).not.toBe("");
    let cookie = signedIn.cookie;
    const slug = "vitest-" + crypto.randomUUID();
    const created = await auth("organization/create", cookie, {
      name: "Vitest organization",
      slug,
      keepCurrentActiveOrganization: true,
    });
    cookie = created.cookie;
    const organizationId = created.body.id;
    expect(typeof organizationId).toBe("string");

    const active = await auth("organization/set-active", cookie, { organizationId });
    cookie = active.cookie;
    const organizations = await auth("organization/list", cookie);
    expect(organizations.body).toEqual([
      expect.objectContaining({ id: organizationId, name: "Vitest organization", slug }),
    ]);
    const session = await auth("get-session", cookie);
    expect(session.body.session.activeOrganizationId).toBe(organizationId);

    const jwks = await json("/api/auth/jwks");
    expect(jwks.response.status, JSON.stringify(jwks.body)).toBe(200);
    let jwksRequests = 0;
    const network = setupNetwork();
    network.configure({ onUnhandledFrame: "error" });
    network.use(
      http.get(origin + "/api/auth/jwks", () => {
        jwksRequests += 1;
        return HttpResponse.json(jwks.body);
      }, { once: true }),
    );
    await network.enable();
    try {
      const token = await auth("token", cookie);
      expect(typeof token.body.token).toBe("string");

      const messageId = crypto.randomUUID();
      const written = await json("/api/messages", {
        method: "POST",
        headers: {
          authorization: "Bearer " + token.body.token,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          id: messageId,
          organizationId,
          body: "written inside workerd",
          clientCreatedAt: Date.now(),
        }),
      });
      expect(written.response.status, JSON.stringify(written.body)).toBe(200);
      expect(written.body.id).toBe(messageId);

      const read = await json("/api/messages?organizationId=" + encodeURIComponent(organizationId), {
        headers: { authorization: "Bearer " + token.body.token },
      });
      expect(read.response.status, JSON.stringify(read.body)).toBe(200);
      expect(read.body).toEqual([
        expect.objectContaining({ id: messageId, organizationId, body: "written inside workerd" }),
      ]);
      expect(jwksRequests).toBe(1);
    } finally {
      await network.disable();
    }
  });
});
`;

const README_TEMPLATE = (name: string) => `# ${name}

This project was generated by \`chardb init\`.

Use Bun 1.2.22 or newer and Node.js 22 or newer.

Start the local app:

\`\`\`bash
bun install
bun run typecheck
bun run test
bun run dev
\`\`\`

\`bun run dev\` starts Wrangler, migrates the local database, then starts Vite. Press Ctrl+C to stop both.

\`bun run build\` builds the browser through the CharDB Vite plugin, then asks Wrangler to validate and bundle the Worker without deploying.

\`bun run test\` runs the generated Worker through Cloudflare's Vitest integration and covers sign-in, organization creation, and one message write and read.

Application routes use the exported \`DB\` binding. CharDB uses the other generated Durable Object bindings internally.

Do not edit files under \`src/migrations\` after deployment. Change \`src/schema.ts\`, then append an additive migration:

\`\`\`bash
bunx @chardb/core migrations generate --name add_messages
\`\`\`

For the first Cloudflare deploy, authenticate Wrangler, copy the environment template, and replace every placeholder:

\`\`\`bash
bunx wrangler login
cp .env.example .env.local
bun run setup:cloudflare
bun run deploy:bootstrap
\`\`\`

\`setup:cloudflare\` probes or creates the exact R2 bucket named in \`wrangler.toml\`. It does not install an expiry rule because CharDB's private content-addressed objects are the authoritative file bytes. It does not infer Vectorize indexes. Provision any future vector bindings explicitly, then run \`bunx @chardb/core vectorize prepare\`.

The bootstrap command reads the first workers.dev URL from Wrangler's versioned deployment output and caches the nonsecret identity under \`.wrangler/\`. Set \`CHARDB_URL\` only to use a custom domain or when workers.dev is disabled. On the first Worker upload the command passes the two secrets through a mode-0600 temporary file, removes it, verifies the generated deployment identity at \`/health\`, and runs the packaged \`chardb migrate\` command with a content-derived migration ID. Rerunning it after an interrupted upload or migration resumes without changing secrets. Use \`bun run deploy\` for later releases. Routine deployment requires an existing Worker, checks that its current package and active migration agree, and never uploads secrets.

The current chardb package is experimental. Test its deployed recovery command before putting data you care about into it. See the CharDB deployment guide for recovery, Vectorize, and production URL setup.
`;

const AUTH_TEMPLATE = `import { anonymous } from "better-auth/plugins/anonymous";
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { defineAuth } from "@chardb/core/server";

function trustedDevelopmentOrigins(request?: Request): string[] {
  if (!request) return [];
  try {
    const worker = new URL(request.url);
    const candidate = new URL(request.headers.get("origin") ?? request.headers.get("referer") ?? "");
    const loopback = (hostname: string) =>
      hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
    if (worker.protocol !== "http:" || !loopback(worker.hostname)) return [];
    if (candidate.protocol !== "http:" || !loopback(candidate.hostname)) return [];
    return [candidate.origin];
  } catch {
    return [];
  }
}

export const auth = defineAuth({
  appName: {{APP_NAME}},
  plugins: [anonymous(), organization(), jwt()],
  trustedOrigins: trustedDevelopmentOrigins,
});
`;

const SCHEMA_TEMPLATE = `import { file } from "@chardb/core/files";
import { integer, text } from "drizzle-orm/sqlite-core";
import { forOrg } from "@chardb/core/server";
import { auth } from "./auth.ts";

const { cdbTable } = forOrg(auth);

export const messages = cdbTable(
  "messages",
  {
    id: text("id").primaryKey(),
    authorId: text("author_id")
      .notNull()
      .references(() => auth.user.id, { onDelete: "cascade" }),
    body: text("body").notNull(),
    attachment: file("attachment", { maxSize: 5 * 1_024 * 1_024, contentTypes: ["image/jpeg", "image/png"] }),
    createdAt: integer("created_at").notNull(),
  },
  {
    selfBy: "authorId",
    roles: {
      owner: "*",
      admin: "*",
      member: { read: "*", create: ["id", "body", "attachment", "createdAt"] },
      self: { read: "*", update: ["body", "attachment"], delete: true },
    },
  },
);
`;

const QUERIES_TEMPLATE = `import { api } from "@chardb/core/server";
import { desc, eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

export const listMessages = api.query({
  ref: "messages#list",
  args: z.object({
    organizationId: z.string(),
    limit: z.number().int().min(1).max(100).default(50),
  }),
  query: (db, args) => db
    .select()
    .from(messages)
    .where(eq(messages.organizationId, args.organizationId))
    .orderBy(desc(messages.createdAt), desc(messages.id))
    .limit(args.limit),
});
`;

const INITIAL_MIGRATION_ARTIFACTS = renderInitialMigrationArtifacts(SCAFFOLD_INITIAL_SNAPSHOT);

const VITE_CONFIG_TEMPLATE = `import react from "@vitejs/plugin-react";
import { chardb } from "@chardb/core/vite";
import { defineConfig } from "vite";

function localOrigin(raw: string | undefined, fallback: string, name: string): string {
  const url = new URL(raw ?? fallback);
  if (
    url.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) ||
    url.username || url.password || (url.pathname !== "" && url.pathname !== "/") || url.search || url.hash
  ) {
    throw new Error(name + " must be a loopback HTTP origin");
  }
  return url.origin;
}

const workerOrigin = localOrigin(process.env.CHARDB_DEV_URL, "http://127.0.0.1:8787", "CHARDB_DEV_URL");
const workerSocket = workerOrigin.replace(/^http/, "ws");

export default defineConfig({
  publicDir: false,
  plugins: [
    react(),
    chardb(),
  ],
  build: {
    outDir: "public",
    emptyOutDir: true,
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: {
      "/ws": { target: workerSocket, ws: true, changeOrigin: true },
      "/_chardb": {
        target: workerOrigin,
        changeOrigin: true,
        configure(proxy) {
          proxy.on("proxyReq", request => request.setHeader("origin", workerOrigin));
        },
      },
      "/api": workerOrigin,
      "/health": workerOrigin,
    },
  },
});
`;

const BUILD_SCRIPT_TEMPLATE = `const steps = [
  ["browser", [process.execPath, "run", "build:web"]],
  ["Worker", [process.execPath, "run", "build:worker"]],
];

for (const [label, args] of steps) {
  const child = Bun.spawn(args, { stdin: "inherit", stdout: "inherit", stderr: "inherit" });
  const exitCode = await child.exited;
  if (exitCode !== 0) throw new Error(label + " build exited with status " + exitCode);
}
`;

const WEB_MAIN_TEMPLATE = `import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App.tsx";
import "./styles.css";

const root = document.getElementById("root");
if (!root) throw new Error("missing #root element");

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
`;

const WEB_APP_TEMPLATE = `import { createAuthClient } from "better-auth/react";
import { type Organization, anonymousClient, jwtClient, organizationClient } from "better-auth/client/plugins";
import { fileRef } from "@chardb/core/files";
import { createChardbReactClient } from "@chardb/react";
import { type FormEvent, useEffect, useState } from "react";
import { uuidv7 } from "uuidv7";
import { postMessage, replaceMessageAttachment } from "../api.ts";
import { listMessages } from "../queries.ts";

const messageAttachment = fileRef("messages", "attachment");

const workerUrl = window.location.origin;
const db = createChardbReactClient({
  url: workerUrl,
  ownership: "organization",
  auth: ({ baseURL }) => createAuthClient({
    baseURL,
    plugins: [anonymousClient(), organizationClient(), jwtClient()],
  }),
});

let anonymousSignInRequest: ReturnType<typeof db.auth.signIn.anonymous> | undefined;

function signInAnonymously() {
  anonymousSignInRequest ??= db.auth.signIn.anonymous().finally(() => {
    anonymousSignInRequest = undefined;
  });
  return anonymousSignInRequest;
}

export function App() {
  const session = db.auth.useSession();
  const [authError, setAuthError] = useState<string | null>(null);

  useEffect(() => {
    if (session.isPending || session.data) return;
    let active = true;
    void (async () => {
      try {
        const result = await signInAnonymously();
        if (active && result.error) setAuthError(result.error.message);
      } catch (cause) {
        if (active) setAuthError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
    return () => {
      active = false;
    };
  }, [session.data, session.isPending]);

  if (!session.data) {
    return <main className="shell">{authError ? "Sign-in failed: " + authError : "Signing in..."}</main>;
  }

  return (
    <db.Provider>
      <Workspace />
    </db.Provider>
  );
}

function Workspace() {
  const identity = db.useIdentity();
  const organizations = db.auth.useListOrganizations();
  const activeOrganizationId = identity.organizationId;
  const userId = identity.user?.id;
  const [name, setName] = useState("");
  const [slug, setSlug] = useState("");
  const [savingOrganization, setSavingOrganization] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function selectOrganization(organizationId: string | null) {
    setSavingOrganization(true);
    setError(null);
    try {
      const result = await db.auth.organization.setActive({ organizationId });
      if (result.error) throw new Error(result.error.message);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSavingOrganization(false);
    }
  }

  async function createOrganization(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const organizationName = name.trim();
    const organizationSlug = slug.trim();
    if (!organizationName || !organizationSlug || savingOrganization) return;
    setSavingOrganization(true);
    setError(null);
    try {
      const created = await db.auth.organization.create({
        name: organizationName,
        slug: organizationSlug,
        keepCurrentActiveOrganization: true,
      });
      if (created.error || !created.data) {
        throw new Error(created.error?.message ?? "Better Auth did not return the new organization");
      }
      const active = await db.auth.organization.setActive({ organizationId: created.data.id });
      if (active.error) throw new Error(active.error.message);
      setName("");
      setSlug("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSavingOrganization(false);
    }
  }

  async function deleteActiveOrganization() {
    if (!activeOrganizationId || savingOrganization) return;
    setSavingOrganization(true);
    setError(null);
    try {
      const deleted = await db.auth.organization.delete({ organizationId: activeOrganizationId });
      if (deleted.error) throw new Error(deleted.error.message);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSavingOrganization(false);
    }
  }

  return (
    <main className="shell">
      <header>
        <div>
          <h1>chardb messages</h1>
          <p data-testid="auth-status" data-user-id={userId}>
            Signed in with Better Auth
          </p>
        </div>
      </header>

      <section className="organizations" aria-label="Organizations">
        <label>
          Active organization
          <select
            data-testid="organization-select"
            value={activeOrganizationId ?? ""}
            disabled={savingOrganization || organizations.isPending}
            onChange={event => void selectOrganization(event.target.value || null)}
          >
            <option value="">Choose an organization</option>
            {(organizations.data ?? []).map((organization: Organization) => (
              <option key={organization.id} value={organization.id} data-slug={organization.slug}>
                {organization.name}
              </option>
            ))}
          </select>
        </label>

        <form className="organization-form" onSubmit={createOrganization}>
          <input
            data-testid="create-organization-name"
            aria-label="Organization name"
            value={name}
            placeholder="Organization name"
            disabled={savingOrganization}
            onChange={event => setName(event.target.value)}
          />
          <input
            data-testid="create-organization-slug"
            aria-label="Organization slug"
            value={slug}
            placeholder="organization-slug"
            disabled={savingOrganization}
            onChange={event => setSlug(event.target.value)}
          />
          <button
            data-testid="create-organization-submit"
            type="submit"
            disabled={savingOrganization || !name.trim() || !slug.trim()}
          >
            {savingOrganization ? "Saving..." : "Create organization"}
          </button>
        </form>
        <button
          data-testid="delete-organization"
          type="button"
          disabled={savingOrganization || !activeOrganizationId}
          onClick={() => void deleteActiveOrganization()}
        >
          Delete active organization
        </button>
      </section>

      {activeOrganizationId && userId ? (
        <Messages organizationId={activeOrganizationId} userId={userId} />
      ) : (
        <section className="messages" data-testid="message-list">
          <p className="empty">Create or choose an organization to start.</p>
        </section>
      )}
      {error ? <p className="error">{error}</p> : null}
    </main>
  );
}

interface MessageRow {
  readonly id: string;
  readonly authorId: string;
  readonly body: string;
  readonly attachment: string | null;
}

function MessageCard({
  message,
  userId,
}: {
  readonly message: MessageRow;
  readonly userId: string;
}) {
  const attachment = db.useFile(messageAttachment);
  const replace = db.useMutation(replaceMessageAttachment);
  const [selected, setSelected] = useState<{ readonly file: File; readonly retryKey: string } | null>(null);
  const [replacing, setReplacing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submitReplacement(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selected || replacing) return;
    setReplacing(true);
    setError(null);
    try {
      const uploaded = await attachment.upload({
        file: selected.file,
        idempotencyKey: selected.retryKey,
      });
      await replace({ id: message.id, attachment: uploaded.fileId });
      setSelected(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setReplacing(false);
    }
  }

  return (
    <article
      className={message.authorId === userId ? "mine" : undefined}
      data-message-id={message.id}
      data-attachment-id={message.attachment ?? undefined}
    >
      <small>{message.authorId === userId ? "you" : message.authorId}</small>
      <p>{message.body}</p>
      {message.attachment ? (
        <a
          data-testid="message-attachment"
          href={attachment.downloadUrl({ rowId: message.id })}
        >
          Download attachment
        </a>
      ) : null}
      {message.authorId === userId ? (
        <form className="attachment-form" onSubmit={submitReplacement}>
          <input
            key={selected?.retryKey ?? "empty"}
            data-testid="message-replacement-file"
            aria-label={"Replace attachment for " + message.body}
            type="file"
            accept="image/jpeg,image/png"
            disabled={replacing}
            onChange={event => {
              const file = event.target.files?.[0];
              setSelected(file ? { file, retryKey: crypto.randomUUID() } : null);
            }}
          />
          <button type="submit" disabled={replacing || !selected}>
            {replacing ? "Replacing..." : "Replace attachment"}
          </button>
        </form>
      ) : null}
      {error ? <p className="error">{error}</p> : null}
    </article>
  );
}

function Messages({ organizationId, userId }: { readonly organizationId: string; readonly userId: string }) {
  const { data = [], state } = db.useQuery(listMessages, { limit: 50 });
  const mutate = db.useMutation(postMessage);
  const attachment = db.useFile(messageAttachment);
  const [body, setBody] = useState("");
  const [selectedFile, setSelectedFile] = useState<{ readonly file: File; readonly retryKey: string } | null>(null);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const message = body.trim();
    if (!message || sending) return;
    setSending(true);
    setError(null);
    try {
      const uploaded = selectedFile
        ? await attachment.upload({
            file: selectedFile.file,
            idempotencyKey: selectedFile.retryKey,
          })
        : null;
      await mutate({
        id: uuidv7(),
        body: message,
        attachment: uploaded?.fileId ?? null,
        clientCreatedAt: Date.now(),
      });
      setBody("");
      setSelectedFile(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSending(false);
    }
  }

  return (
    <>
      <div className="query-status">
        <code data-testid="query-state" data-organization-id={organizationId}>{state}</code>
      </div>

      <section
        className="messages"
        data-testid="message-list"
        data-organization-id={organizationId}
        aria-live="polite"
      >
        {data.length === 0 ? <p className="empty">No messages yet.</p> : null}
        {[...data].reverse().map(message => (
          <MessageCard
            key={message.id}
            message={message}
            userId={userId}
          />
        ))}
      </section>

      <form onSubmit={submit}>
        <input
          aria-label="Message"
          value={body}
          maxLength={2_000}
          placeholder="Write a message"
          disabled={sending}
          onChange={event => setBody(event.target.value)}
        />
        <input
          key={selectedFile?.retryKey ?? "empty"}
          data-testid="message-file"
          aria-label="Attachment"
          type="file"
          accept="image/jpeg,image/png"
          disabled={sending}
          onChange={event => {
            const selected = event.target.files?.[0];
            setSelectedFile(selected ? { file: selected, retryKey: crypto.randomUUID() } : null);
          }}
        />
        <button type="submit" disabled={sending || !body.trim()}>
          {sending ? "Sending..." : "Send"}
        </button>
      </form>
      {error ? <p className="error">{error}</p> : null}
    </>
  );
}
`;

const WEB_STYLES_TEMPLATE = `:root {
  color: #171717;
  background: #f3f1eb;
  font-family: Inter, ui-sans-serif, system-ui, sans-serif;
}

* { box-sizing: border-box; }
body { margin: 0; }
button, input { font: inherit; }
.shell { width: min(720px, calc(100% - 32px)); margin: 40px auto; }
header { display: flex; align-items: start; justify-content: space-between; gap: 24px; margin-bottom: 20px; }
h1, header p { margin: 0; }
h1 { font-size: 24px; }
header p, small, .empty { color: #69665e; }
header code { padding: 4px 8px; border: 1px solid #d4d0c7; border-radius: 999px; font-size: 12px; }
.organizations { display: grid; gap: 12px; margin-bottom: 16px; }
.organizations label { display: grid; gap: 6px; }
.organizations select { width: 100%; }
.organization-form { grid-template-columns: 1fr 1fr auto; margin: 0; }
.query-status { display: flex; justify-content: flex-end; margin-bottom: 8px; }
.query-status code { padding: 4px 8px; border: 1px solid #d4d0c7; border-radius: 999px; font-size: 12px; }
.messages { min-height: 360px; padding: 16px; border: 1px solid #d4d0c7; border-radius: 12px; background: #fff; }
article { width: fit-content; max-width: 75%; margin: 0 0 12px; padding: 10px 12px; border-radius: 10px; background: #eeeae1; }
article.mine { margin-left: auto; background: #dce8ff; }
article p { margin: 2px 0 0; white-space: pre-wrap; overflow-wrap: anywhere; }
form { display: grid; grid-template-columns: 1fr auto; gap: 8px; margin-top: 12px; }
input, select, button { padding: 10px 12px; border: 1px solid #c8c3b8; border-radius: 8px; }
input { background: #fff; }
button { color: #fff; background: #1b57d0; border-color: #1b57d0; cursor: pointer; }
button:disabled { cursor: default; opacity: 0.55; }
.error { color: #a21d1d; }
@media (max-width: 640px) { .organization-form { grid-template-columns: 1fr; } }
`;

const DEV_SCRIPT_TEMPLATE = `import { realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const WINDOWS_WATCHDOG_ARGUMENT = "--chardb-windows-watchdog";
const WINDOWS_UTILITY_TIMEOUT_MS = 5_000;
const WINDOWS_STDIN_CANCEL_TIMEOUT_MS = 1_000;
const READINESS_PROBE_TIMEOUT_MS = 1_000;

async function runWindowsUtility(command) {
  const child = Bun.spawn(command, { stdin: "ignore", stdout: "pipe", stderr: "pipe", windowsHide: true });
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  const outcome = await Promise.race([
    child.exited.then(exitCode => ({ exitCode })),
    Bun.sleep(WINDOWS_UTILITY_TIMEOUT_MS).then(() => null),
  ]);
  if (!outcome) {
    child.kill("SIGKILL");
    await Promise.race([child.exited, Bun.sleep(1_000)]);
    throw new Error(command[0] + " exceeded " + WINDOWS_UTILITY_TIMEOUT_MS + "ms");
  }
  return { exitCode: outcome.exitCode, stdout: await stdout, stderr: await stderr };
}

async function windowsProcessSnapshot() {
  const script = [
    "$ErrorActionPreference = 'Stop'",
    "@(Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId, CreationDate) | ConvertTo-Json -Compress",
  ].join("; ");
  const result = await runWindowsUtility([
    "powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script,
  ]);
  if (result.exitCode !== 0) {
    throw new Error("PowerShell process enumeration failed with " + result.exitCode + ": " + result.stderr.trim());
  }
  const parsed = JSON.parse(result.stdout || "[]");
  const rows = Array.isArray(parsed) ? parsed : [parsed];
  return rows
    .map(row => ({
      pid: Number(row.ProcessId),
      parentPid: Number(row.ParentProcessId),
      createdAt: typeof row.CreationDate === "string" ? row.CreationDate : "",
    }))
    .filter(row =>
      Number.isSafeInteger(row.pid) && row.pid > 0 &&
      Number.isSafeInteger(row.parentPid) && row.createdAt.length > 0
    );
}

function descendantProcesses(snapshot, rootPid) {
  const children = new Map();
  for (const row of snapshot) {
    const entries = children.get(row.parentPid) ?? [];
    entries.push(row);
    children.set(row.parentPid, entries);
  }
  const descendants = [];
  const pending = [...(children.get(rootPid) ?? [])];
  const seen = new Set([rootPid]);
  while (pending.length > 0) {
    const child = pending.pop();
    if (seen.has(child.pid)) continue;
    seen.add(child.pid);
    descendants.push(child);
    pending.push(...(children.get(child.pid) ?? []));
  }
  return descendants;
}

function descendantsOfProcessIdentity(snapshot, rootPid, rootCreatedAt) {
  const root = snapshot.find(row => row.pid === rootPid);
  if (root && root.createdAt !== rootCreatedAt) return [];
  return descendantProcesses(snapshot, rootPid);
}

async function forceWindowsProcessTree(pid) {
  const result = await runWindowsUtility(["taskkill.exe", "/PID", String(pid), "/T", "/F"]);
  if (result.exitCode !== 0) {
    const snapshot = await windowsProcessSnapshot();
    if (snapshot.some(row => row.pid === pid)) {
      throw new Error("taskkill failed for process " + pid + ": " + result.stderr.trim());
    }
  }
}

async function runWindowsWatchdog(rootPid) {
  const tracked = new Map();
  const initialSnapshot = await windowsProcessSnapshot();
  const root = initialSnapshot.find(row => row.pid === rootPid);
  if (!root) return;
  const rootCreatedAt = root.createdAt;
  const stdinReader = Bun.stdin.stream().getReader();
  let inputClosed = false;
  const observeInput = (async () => {
    try {
      while (!(await stdinReader.read()).done) {
        // The parent writes nothing. EOF means it closed the watchdog pipe.
      }
    } finally {
      inputClosed = true;
    }
  })().catch(() => undefined);
  try {
    while (!inputClosed) {
      const snapshot = await windowsProcessSnapshot();
      const currentRoot = snapshot.find(row => row.pid === rootPid);
      if (!currentRoot) {
        for (const child of descendantsOfProcessIdentity(snapshot, rootPid, rootCreatedAt)) {
          if (child.pid !== process.pid) tracked.set(child.pid, child.createdAt);
        }
        break;
      }
      if (currentRoot.createdAt !== rootCreatedAt) break;
      for (const child of descendantsOfProcessIdentity(snapshot, rootPid, rootCreatedAt)) {
        if (child.pid !== process.pid) tracked.set(child.pid, child.createdAt);
      }
      await Bun.sleep(50);
    }
  } finally {
    await Promise.race([
      stdinReader.cancel().catch(() => undefined),
      Bun.sleep(WINDOWS_STDIN_CANCEL_TIMEOUT_MS),
    ]);
    void observeInput;
  }
  for (let pass = 0; pass < 3; pass++) {
    const snapshot = await windowsProcessSnapshot();
    for (const child of descendantsOfProcessIdentity(snapshot, rootPid, rootCreatedAt)) {
      if (child.pid !== process.pid) tracked.set(child.pid, child.createdAt);
    }
    const live = new Map(snapshot.map(row => [row.pid, row.createdAt]));
    for (const [pid, createdAt] of tracked) {
      if (live.get(pid) === createdAt) await forceWindowsProcessTree(pid);
    }
    await Bun.sleep(25);
  }
}

const watchdogIndex = process.argv.indexOf(WINDOWS_WATCHDOG_ARGUMENT);
if (watchdogIndex !== -1) {
  const rootPid = Number(process.argv[watchdogIndex + 1]);
  if (!Number.isSafeInteger(rootPid) || rootPid <= 0) throw new Error("invalid Windows watchdog parent PID");
  await runWindowsWatchdog(rootPid);
  process.exit(0);
}

function localOrigin(raw, fallback, name) {
  const url = new URL(raw ?? fallback);
  if (
    url.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) ||
    url.username || url.password || (url.pathname !== "" && url.pathname !== "/") || url.search || url.hash
  ) {
    throw new Error(name + " must be a loopback HTTP origin");
  }
  return url;
}

function localChildEnvironment(extra = {}) {
  const env = { ...process.env };
  delete env.CHARDB_URL;
  delete env.CHARDB_WEB_URL;
  delete env.CHARDB_ADMIN_TOKEN;
  delete env.BETTER_AUTH_SECRET;
  return { ...env, ...extra };
}

const deploymentId = {{DEPLOYMENT_ID}};
const origin = localOrigin(process.env.CHARDB_DEV_URL, "http://127.0.0.1:8787", "CHARDB_DEV_URL");
const webOrigin = localOrigin(process.env.CHARDB_DEV_WEB_URL, "http://127.0.0.1:5173", "CHARDB_DEV_WEB_URL");
const adminToken = "local-chardb-admin";
const authSecret = "local-chardb-auth-secret-that-is-at-least-32-characters";
const projectRoot = realpathSync.native(fileURLToPath(new URL("..", import.meta.url)));
const persistTo = process.env.CHARDB_DEV_PERSIST_TO ?? join(projectRoot, ".wrangler", "state");
const wranglerModule = join(
  dirname(fileURLToPath(import.meta.resolve("wrangler/package.json"))),
  "bin",
  "wrangler.js",
);
const viteModule = realpathSync.native(
  join(dirname(fileURLToPath(import.meta.resolve("vite/package.json"))), "bin", "vite.js"),
);
const chardbBin = join(projectRoot, "node_modules", "@chardb", "core", "dist", "cli", "bin.mjs");
const nodeRuntime = Bun.which("node");

for (const path of [wranglerModule, viteModule, chardbBin]) {
  if (!(await Bun.file(path).exists())) throw new Error("missing local dependencies; run bun install first");
}
if (!nodeRuntime) throw new Error("Wrangler and Vite require Node.js on PATH");

const watchdog = process.platform === "win32"
  ? Bun.spawn([process.execPath, fileURLToPath(import.meta.url), WINDOWS_WATCHDOG_ARGUMENT, String(process.pid)], {
      stdin: "pipe", stdout: "inherit", stderr: "inherit", windowsHide: true,
    })
  : undefined;

const worker = Bun.spawn(
  [
    nodeRuntime,
    wranglerModule,
    "dev",
    "--ip",
    origin.hostname,
    "--port",
    origin.port || "8787",
    "--persist-to",
    persistTo,
    "--var",
    "CDB_ADMIN_TOKEN:" + adminToken,
    "--var",
    "BETTER_AUTH_SECRET:" + authSecret,
  ],
  {
    env: localChildEnvironment(),
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
    detached: process.platform !== "win32",
  },
);

let web;

function processGroupExists(pid) {
  if (process.platform === "win32") return false;
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ESRCH") return false;
    if (error && typeof error === "object" && "code" in error && error.code === "EPERM") return true;
    throw error;
  }
}

async function waitForProcessGroupExit(pid, waitMs) {
  const deadline = performance.now() + waitMs;
  while (processGroupExists(pid) && performance.now() < deadline) await Bun.sleep(10);
  return !processGroupExists(pid);
}

async function terminateProcessGroup(child, signal) {
  if (!child) return;
  if (process.platform === "win32") {
    await forceWindowsProcessTree(child.pid);
    const exited = await Promise.race([child.exited.then(() => true), Bun.sleep(2_000).then(() => false)]);
    if (!exited) throw new Error("process tree " + child.pid + " survived taskkill");
    return;
  }
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ESRCH") return;
    throw error;
  }
  if (await waitForProcessGroupExit(child.pid, 2_000)) {
    await child.exited;
    return;
  }
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ESRCH")) throw error;
  }
  if (!(await waitForProcessGroupExit(child.pid, 2_000))) {
    throw new Error("process group " + child.pid + " survived SIGKILL");
  }
  await child.exited;
}

let termination;
const stop = signal => {
  termination ??= (async () => {
    const results = await Promise.allSettled([
      terminateProcessGroup(web, signal),
      terminateProcessGroup(worker, signal),
    ]);
    if (watchdog) {
      watchdog.stdin.end();
      const exited = await Promise.race([watchdog.exited.then(() => true), Bun.sleep(2_000).then(() => false)]);
      if (!exited) watchdog.kill("SIGKILL");
    }
    const failures = results.filter(result => result.status === "rejected").map(result => result.reason);
    if (failures.length > 0) throw new AggregateError(failures, "generated dev process cleanup failed");
  })();
  void termination.catch(() => {});
  return termination;
};
process.on("SIGINT", () => void stop("SIGINT"));
process.on("SIGTERM", () => void stop("SIGTERM"));

async function waitForWorker() {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (worker.exitCode !== null) throw new Error("Wrangler exited before the health check passed");
    let response;
    try {
      response = await fetch(new URL("/health", origin), {
        signal: AbortSignal.timeout(READINESS_PROBE_TIMEOUT_MS),
      });
    } catch {
      // Wrangler has not opened its local listener yet.
      await Bun.sleep(100);
      continue;
    }
    if (response.ok) {
      let body;
      try {
        body = await response.json();
      } catch {
        // Wrangler can briefly return a successful response while its Worker
        // is still replacing the startup listener. Treat that response as a
        // readiness miss and keep polling within the existing deadline.
        await Bun.sleep(100);
        continue;
      }
      if (
        body && typeof body === "object" && body.ok === true && body.deploymentId === deploymentId &&
        Number.isSafeInteger(body.schemaVersion) && body.schemaVersion >= 1
      ) {
        return body.schemaVersion;
      }
      throw new Error("/health did not identify the expected local Worker " + deploymentId);
    }
    await Bun.sleep(100);
  }
  throw new Error("timed out waiting for " + origin.origin + "/health");
}

async function waitForWeb() {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (!web || web.exitCode !== null) throw new Error("Vite exited before the browser URL was ready");
    const outcome = await Promise.race([
      fetch(webOrigin, { signal: AbortSignal.timeout(READINESS_PROBE_TIMEOUT_MS) })
        .then(response => ({ kind: "response", response }))
        .catch(() => ({ kind: "retry" })),
      web.exited.then(exitCode => ({ kind: "exit", exitCode })),
    ]);
    if (outcome.kind === "exit") {
      throw new Error("Vite exited with status " + outcome.exitCode + " before the browser URL was ready");
    }
    if (outcome.kind === "response" && outcome.response.ok) return;
    await Bun.sleep(100);
  }
  throw new Error("timed out waiting for " + webOrigin.origin);
}

async function assertWebOriginAvailable() {
  try {
    const response = await fetch(webOrigin, {
      signal: AbortSignal.timeout(READINESS_PROBE_TIMEOUT_MS),
    });
    await response.body?.cancel();
  } catch {
    return;
  }
  throw new Error(webOrigin.origin + " is already serving another process");
}

async function applyMigrations(targetVersion) {
  const migration = Bun.spawn(
    [
      process.execPath,
      chardbBin,
      "migrate",
      "--url",
      origin.origin,
      "--id",
      "local-schema-v" + targetVersion,
      "--target",
      String(targetVersion),
      "--concurrency",
      "4",
    ],
    {
      env: localChildEnvironment({ CHARDB_ADMIN_TOKEN: adminToken }),
      stdout: "inherit",
      stderr: "inherit",
    },
  );
  const exitCode = await migration.exited;
  if (exitCode !== 0) {
    throw new Error("chardb migrate exited with status " + exitCode);
  }
}

try {
  const targetVersion = await waitForWorker();
  await applyMigrations(targetVersion);
  await assertWebOriginAvailable();
  web = Bun.spawn(
    [nodeRuntime, viteModule, "--host", webOrigin.hostname, "--port", webOrigin.port || "5173", "--strictPort"],
    {
      cwd: projectRoot,
      env: localChildEnvironment({ CHARDB_DEV_URL: origin.origin }),
      stdin: "inherit",
      stdout: "inherit",
      stderr: "inherit",
      detached: process.platform !== "win32",
    },
  );
  await waitForWeb();
  console.log("chardb app ready at " + webOrigin.origin + " with schema version " + targetVersion);
  process.exitCode = await Promise.race([worker.exited, web.exited]);
  await stop("SIGTERM");
} catch (error) {
  await stop("SIGTERM");
  await Promise.all([worker.exited, ...(web ? [web.exited] : [])]);
  throw error;
}
`;

const API_TEMPLATE = `import { CdbError } from "@chardb/core";
import { FileId } from "@chardb/core/files";
import { api } from "@chardb/core/server";
import { and, eq } from "drizzle-orm";
import { z } from "zod";
import { messages } from "./schema.ts";

export const postMessage = api.mutation({
  ref: "messages#create",
  authority: "organization",
  args: z.object({
    id: z.string(),
    organizationId: z.string(),
    body: z.string().trim().min(1).max(2_000),
    attachment: z.string().min(1).max(128).transform(FileId).nullable(),
    clientCreatedAt: z.number(),
  }),
  partitionKey: "organizationId",
  handler: (ctx, args) => {
    if (!ctx.auth.userId || !ctx.auth.tenantId || ctx.auth.tenantId !== args.organizationId) {
      throw new CdbError({
        code: "CDB_FORBIDDEN",
        message: "active organization does not match the routed partition",
      });
    }
    ctx.db.insert(messages).values({
      id: args.id,
      body: args.body,
      attachment: args.attachment,
      createdAt: args.clientCreatedAt,
    }).run();
    return { id: args.id };
  },
});

export const replaceMessageAttachment = api.mutation({
  ref: "messages#replaceAttachment",
  authority: "organization",
  args: z.object({
    id: z.string(),
    organizationId: z.string(),
    attachment: z.string().min(1).max(128).transform(FileId),
  }),
  partitionKey: "organizationId",
  handler: (ctx, args) => {
    if (!ctx.auth.userId || !ctx.auth.tenantId || ctx.auth.tenantId !== args.organizationId) {
      throw new CdbError({
        code: "CDB_FORBIDDEN",
        message: "active organization does not match the routed partition",
      });
    }
    ctx.db.update(messages)
      .set({ attachment: args.attachment })
      .where(and(eq(messages.id, args.id), eq(messages.organizationId, args.organizationId)))
      .run();
    return { id: args.id };
  },
});
`;

const WORKER_TEMPLATE = `import { client } from "@chardb/core";
import { FileId } from "@chardb/core/files";
import { chardb } from "@chardb/core/server";
import { desc, eq } from "drizzle-orm";
import * as api from "./api.ts";
import { auth } from "./auth.ts";
import { migrations } from "./migrations.ts";
import * as queries from "./queries.ts";
import * as domain from "./schema.ts";

// One factory call composes the runtime: merged Drizzle schema, lazy
// manifest from \`api\`'s exports, Hono router for non-reserved routes,
// and the Durable Object classes wired by the generated Wrangler config.
// The returned \`app\` is the wrangler-ready module. Chain routes on it.
export const app = chardb({
  ownership: "organization",
  auth,
  schema: domain,
  api: { ...api, ...queries },
  migrations,
});

app.get("/health", (c) => c.json({
  ok: true,
  deploymentId: {{DEPLOYMENT_ID}},
  schemaVersion: migrations.version,
  schemaDigest: migrations.digest,
}));
app.get("/api/messages", async (c) => {
  const token = c.req.header("authorization")?.replace(/^Bearer\\s+/i, "");
  if (!token) return c.json({ error: "missing bearer token" }, 401);
  const url = new URL(c.req.url);
  const organizationId = url.searchParams.get("organizationId") ?? "";
  const requestedLimit = Number(url.searchParams.get("limit") ?? "50");
  if (!Number.isInteger(requestedLimit) || requestedLimit < 1 || requestedLimit > 100) {
    return c.json({ error: "limit must be an integer from 1 through 100" }, 400);
  }
  const rows = await client(c.env.DB, { jwt: token, authOrigin: url.origin })
    .select()
    .from(domain.messages)
    .where(eq(domain.messages.organizationId, organizationId))
    .orderBy(desc(domain.messages.createdAt), desc(domain.messages.id))
    .limit(requestedLimit);
  return c.json(rows);
});
app.post("/api/messages", async (c) => {
  const token = c.req.header("authorization")?.replace(/^Bearer\\s+/i, "");
  if (!token) return c.json({ error: "missing bearer token" }, 401);
  const body = await c.req.json<{
    id: string;
    organizationId: string;
    body: string;
    attachment?: string | null;
    clientCreatedAt: number;
  }>();
  return c.json(await client(c.env.DB, { jwt: token, authOrigin: new URL(c.req.url).origin })
    .mutate(api.postMessage, {
      ...body,
      attachment: body.attachment == null ? null : FileId(body.attachment),
    }));
});

export default app;
export const { DB, Catalog, Cdb, Gateway, Resharder } = app;
`;

function generatedFilesBucket(name: string): string {
    const stem = name
        .toLowerCase()
        .replace(/[^a-z0-9-]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .slice(0, 57);
    return `${stem || "chardb"}-files`;
}

export function validateInitDirectoryName(directory: string): void {
    if (
        directory === "" ||
        directory === "." ||
        directory === ".." ||
        directory.includes("/") ||
        directory.includes("\\") ||
        directory.includes("\0") ||
        /^[A-Za-z]:/.test(directory)
    ) {
        throw new Error("project directory must be one name without path separators");
    }
}

function initRoot(cwd: string, directory: string | undefined): string {
    if (directory === undefined) return cwd;
    validateInitDirectoryName(directory);
    return resolve(cwd, directory);
}

function artifactDirectories(root: string, artifacts: readonly { readonly path: string }[]): readonly string[] {
    const directories = new Set<string>();
    for (const artifact of artifacts) {
        let directory = artifact.path.slice(0, artifact.path.lastIndexOf("/"));
        while (directory.length > root.length) {
            directories.add(directory);
            directory = directory.slice(0, directory.lastIndexOf("/"));
        }
    }
    return [...directories].sort((left, right) => right.length - left.length);
}

export async function runInit(ctx: CliContext, opts: InitOptions): Promise<void> {
    const root = initRoot(ctx.cwd, opts.directory);
    const compat = opts.compatibilityDate ?? DEFAULT_COMPAT_DATE;
    const corePackage = opts.corePackage ?? "0.1.0";
    if (!corePackage) throw new Error("core package specifier must not be empty");
    const reactPackage = opts.reactPackage ?? "0.1.0";
    if (!reactPackage) throw new Error("react package specifier must not be empty");
    const filesBucket = generatedFilesBucket(opts.name);
    const deploymentId = generatedDeploymentId();
    const wrangler = renderWrangler({
        name: opts.name,
        compatibilityDate: compat,
        assetsDir: "public",
        filesBucket,
    });
    const artifacts = [
        { path: `${root}/package.json`, contents: PACKAGE_TEMPLATE(opts.name, corePackage, reactPackage) },
        { path: `${root}/tsconfig.json`, contents: TSCONFIG_TEMPLATE },
        { path: `${root}/.gitignore`, contents: GITIGNORE_TEMPLATE },
        { path: `${root}/.env.example`, contents: ENV_EXAMPLE_TEMPLATE },
        { path: `${root}/README.md`, contents: README_TEMPLATE(opts.name) },
        { path: `${root}/wrangler.toml`, contents: `${wrangler}\n` },
        {
            path: `${root}/src/auth.ts`,
            contents: AUTH_TEMPLATE.replace("{{APP_NAME}}", JSON.stringify(opts.name)),
        },
        { path: `${root}/src/schema.ts`, contents: SCHEMA_TEMPLATE },
        { path: `${root}/src/api.ts`, contents: API_TEMPLATE },
        { path: `${root}/src/queries.ts`, contents: QUERIES_TEMPLATE },
        { path: `${root}/src/migrations.ts`, contents: INITIAL_MIGRATION_ARTIFACTS.journal },
        { path: `${root}/src/migrations/v1.ts`, contents: INITIAL_MIGRATION_ARTIFACTS.versionOne },
        { path: `${root}/src/migrations/v1.json`, contents: INITIAL_MIGRATION_ARTIFACTS.snapshotOne },
        {
            path: `${root}/src/worker.ts`,
            contents: WORKER_TEMPLATE.replace("{{DEPLOYMENT_ID}}", JSON.stringify(deploymentId)),
        },
        { path: `${root}/src/web/App.tsx`, contents: WEB_APP_TEMPLATE },
        { path: `${root}/src/web/main.tsx`, contents: WEB_MAIN_TEMPLATE },
        { path: `${root}/src/web/styles.css`, contents: WEB_STYLES_TEMPLATE },
        { path: `${root}/vite.config.ts`, contents: VITE_CONFIG_TEMPLATE },
        { path: `${root}/vitest.config.ts`, contents: VITEST_CONFIG_TEMPLATE },
        { path: `${root}/test/tsconfig.json`, contents: TEST_TSCONFIG_TEMPLATE },
        { path: `${root}/test/env.d.ts`, contents: TEST_ENV_TEMPLATE },
        {
            path: `${root}/test/worker.test.ts`,
            contents: WORKER_TEST_TEMPLATE.replace("{{DEPLOYMENT_ID}}", JSON.stringify(deploymentId)),
        },
        { path: `${root}/scripts/build.mjs`, contents: BUILD_SCRIPT_TEMPLATE },
        { path: `${root}/scripts/test.mjs`, contents: TEST_SCRIPT_TEMPLATE },
        {
            path: `${root}/scripts/dev.mjs`,
            contents: DEV_SCRIPT_TEMPLATE.replace("{{DEPLOYMENT_ID}}", JSON.stringify(deploymentId)),
        },
        {
            path: `${root}/scripts/setup-cloudflare.mjs`,
            contents: renderCloudflareSetupScript({
                workerName: opts.name,
                filesBucket,
                packageName: "@chardb/core",
                deploymentId,
            }),
        },
        {
            path: `${root}/scripts/deploy.mjs`,
            contents: renderCloudflareDeployScript({
                workerName: opts.name,
                filesBucket,
                packageName: "@chardb/core",
                deploymentId,
            }),
        },
        { path: `${root}/index.html`, contents: INDEX_TEMPLATE },
        { path: `${root}/public/.gitkeep`, contents: "" },
    ] as const;
    if (!ctx.readDirectory || !ctx.writeFilesExclusive) {
        throw new Error("chardb init cannot guarantee an exclusive scaffold in this environment");
    }
    const prepareDirectory = ctx.prepareDirectory;
    const removeDirectory = ctx.removeDirectory;
    let prepared: "created" | "existing" = "existing";
    if (opts.directory !== undefined) {
        if (!prepareDirectory || !removeDirectory) {
            throw new Error("chardb init cannot guarantee an exclusive target directory in this environment");
        }
        prepared = await prepareDirectory(root);
    }
    try {
        const [rootEntries, existingTargets] = await Promise.all([
            ctx.readDirectory(root),
            Promise.all(artifacts.map(async artifact => ((await ctx.exists(artifact.path)) ? artifact.path : null))),
        ]);
        const unexpectedEntries = rootEntries.filter(entry => !ALLOWED_EXISTING_ROOT_ENTRIES.has(entry)).sort();
        const conflicts = existingTargets
            .flatMap(path => (path === null ? [] : [path]))
            .map(path => path.slice(root.length + 1))
            .sort();
        if (unexpectedEntries.length > 0 || conflicts.length > 0) {
            const detail = [
                ...(unexpectedEntries.length > 0 ? [`top-level entries: ${unexpectedEntries.join(", ")}`] : []),
                ...(conflicts.length > 0 ? [`generated targets: ${conflicts.join(", ")}`] : []),
            ].join("; ");
            throw new Error(
                `chardb init requires an empty directory except for .git and .DS_Store; no files were written (${detail})`
            );
        }
        await ctx.writeFilesExclusive(artifacts);
    } catch (error) {
        if (removeDirectory) {
            for (const directory of artifactDirectories(root, artifacts)) {
                try {
                    await removeDirectory(directory);
                } catch {
                    // Only empty directories are removable, so concurrent files remain untouched.
                }
            }
        }
        if (prepared === "created" && removeDirectory) {
            try {
                await removeDirectory(root);
            } catch {
                // A non-empty target may contain a concurrent writer's file and must remain.
            }
        }
        throw error;
    }
    ctx.stdout(
        `chardb: initialised "${opts.name}" with @chardb/core ${corePackage}, @chardb/react ${reactPackage}, and compat date ${compat}\nnext: bun install && bun run typecheck && bun run dev\n`
    );
}
