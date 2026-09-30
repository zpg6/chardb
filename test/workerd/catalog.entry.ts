import { organization } from "better-auth/plugins/organization";
/**
 * Test worker entry for the workerd Catalog harness. It configures auth
 * before Catalog construction and proxies the RPCs under test over HTTP.
 */
import { bindAuthRuntime } from "../../src/auth/runtime.ts";
import { defineAuth, synthesizeAuthSchema } from "../../src/auth/synthesize.ts";
import { Catalog as ProductionCatalog } from "../../src/server/do/catalog.ts";
import { Resharder as ProductionResharder } from "../../src/server/do/resharder.ts";

const auth = defineAuth({ appName: "catalog-workerd-test", plugins: [organization()] });
bindAuthRuntime({
    schema: synthesizeAuthSchema(auth.options as never) as never,
    options: auth.options as { readonly [key: string]: unknown },
});

export class Catalog extends ProductionCatalog {
    private readonly fixtureId = crypto.randomUUID();

    fixtureInstanceId(): string {
        return this.fixtureId;
    }
}

export class Resharder extends ProductionResharder {}

interface Env {
    CATALOG: DurableObjectNamespace;
}

type Op =
    | "beginTopologyOperation"
    | "cutover"
    | "route"
    | "mutateAuth"
    | "authAdapterRpc"
    | "queryAuth"
    | "resolveOrganizationAuthority"
    | "resolveOrganizationAuthorityRoute"
    | "fixtureInstanceId";

export default {
    async fetch(req: Request, env: Env): Promise<Response> {
        const url = new URL(req.url);
        const id = env.CATALOG.idFromName("global");
        const stub = env.CATALOG.get(id);
        const op = url.pathname.slice(1) as Op;
        const body = req.method === "POST" ? ((await req.json()) as Record<string, unknown> | null) : null;
        const stubAny = stub as unknown as Record<Op, (arg?: unknown) => Promise<unknown>>;
        if (typeof stubAny[op] !== "function") {
            return new Response(`unknown op: ${op}`, { status: 404 });
        }
        try {
            let result: unknown;
            if (op === "route") {
                result = await stubAny.route(body?.vshard);
            } else if (op === "fixtureInstanceId") {
                result = await stubAny.fixtureInstanceId();
            } else if (op === "mutateAuth" || op === "queryAuth") {
                result = await (stubAny[op] as (arg: unknown, recoveryGeneration: number) => Promise<unknown>)(body, 0);
            } else {
                result = await stubAny[op](body);
            }
            return Response.json(result === undefined ? { ok: true } : result);
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            return Response.json({ error: message }, { status: 500 });
        }
    },
};
