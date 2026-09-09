import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { text } from "drizzle-orm/sqlite-core";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";
import { chardb, defineAuth, defineMigrations, defineSchemaBaseline } from "../../src/server/index.ts";
import { forOrg } from "../helpers/cdb-table.ts";

const auth = defineAuth({
    appName: "auto-split",
    baseURL: "https://auto-split.invalid",
    plugins: [
        organization(),
        jwt({
            jwt: { issuer: "https://auto-split.invalid", audience: "auto-split" },
            jwks: { remoteUrl: "https://auto-split.invalid/jwks", keyPairConfig: { alg: "ES256" } },
        }),
    ],
});
const { cdbTable } = forOrg();
const notes = cdbTable(
    "auto_split_notes",
    {
        id: text("id").primaryKey(),
        organizationId: text("organization_id")
            .notNull()
            .references(() => auth.organization.id),
        body: text("body").notNull(),
    },
    { roles: { member: { create: "*", read: "*" } } }
);
const migrations = defineMigrations([
    defineSchemaBaseline({ version: 1, name: "auto_split", domainSchema: { notes }, authOptions: auth.options }),
]);
const app = chardb({ ownership: "organization", auth, schema: { notes }, migrations });

/** Small enough that a couple of megabytes of rows cross it inside one test. */
const MARK = 1_280 * 1_024;
const SOURCE = "ShardDO_0";

interface Env {
    readonly CDB_CATALOG: DurableObjectNamespace;
    readonly CDB_SHARD: DurableObjectNamespace;
    readonly CDB_RESHARD: DurableObjectNamespace;
}

export class Cdb extends app.Cdb {
    protected override autoSplitBytes(): number {
        return MARK;
    }

    fixtureSeed(input: {
        readonly organizationIds: readonly string[];
        readonly rows: number;
        readonly bytes: number;
    }): void {
        const body = "x".repeat(input.bytes);
        this.ctx.storage.transactionSync(() => {
            const sql = adaptSqlStorage(this.ctx.storage.sql);
            for (const organizationId of input.organizationIds) {
                for (let index = 0; index < input.rows; index++) {
                    sql.exec(
                        "INSERT INTO auto_split_notes (id, organization_id, body) VALUES (?, ?, ?)",
                        `${organizationId}-${index}`,
                        organizationId,
                        body
                    );
                }
            }
        });
    }

    fixtureRows(): readonly { readonly organizationId: string; readonly rows: number }[] {
        return adaptSqlStorage(this.ctx.storage.sql).all(
            "SELECT organization_id AS organizationId, COUNT(*) AS rows FROM auto_split_notes GROUP BY 1 ORDER BY 1"
        );
    }

    fixtureBytes(): number {
        return this.ctx.storage.sql.databaseSize;
    }

    fixtureAlarm(): Promise<void> {
        return this.alarm();
    }
}

export class Resharder extends app.Resharder {
    protected override autoSplitBytes(): number {
        return MARK;
    }

    async fixtureHeadroom(): Promise<{
        readonly alarmAt: number | null;
        readonly shards: readonly Record<string, unknown>[];
        readonly migrations: readonly Record<string, unknown>[];
    }> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        return {
            alarmAt: await this.ctx.storage.getAlarm(),
            shards: sql.all("SELECT shard_id, bytes, judged_bytes, mig_id FROM headroom_shards ORDER BY shard_id"),
            migrations: sql.all(
                "SELECT mig_id, src_shard, dst_shard, range_lo, range_hi, phase FROM migration_state ORDER BY mig_id"
            ),
        };
    }
}

export const Catalog = app.Catalog;
export const DB = app.DB;

interface CatalogRpc {
    schemaState(): Promise<{ activeVersion: number }>;
    beginSchemaMigration(args: { migrationId: string; targetVersion: number }): Promise<unknown>;
    migrateSchemaShard(args: { migrationId: string; shardId: string }): Promise<unknown>;
    applyCatalogSchemaMigration(args: { migrationId: string; version: number }): Promise<unknown>;
    completeSchemaMigration(args: { migrationId: string }): Promise<unknown>;
    topology(): Promise<unknown>;
}

function catalog(env: Env): CatalogRpc {
    return env.CDB_CATALOG.get(env.CDB_CATALOG.idFromName("global")) as unknown as CatalogRpc;
}

function cdb(env: Env, shardId: string): Cdb {
    return env.CDB_SHARD.get(env.CDB_SHARD.idFromName(shardId)) as unknown as Cdb;
}

function resharder(env: Env): Resharder {
    return env.CDB_RESHARD.get(env.CDB_RESHARD.idFromName("global")) as unknown as Resharder;
}

async function activateSchema(env: Env): Promise<void> {
    const cat = catalog(env);
    if ((await cat.schemaState()).activeVersion !== 0) return;
    const migrationId = "auto-split-schema-v1";
    await cat.beginSchemaMigration({ migrationId, targetVersion: 1 });
    await cat.migrateSchemaShard({ migrationId, shardId: SOURCE });
    await cat.applyCatalogSchemaMigration({ migrationId, version: 1 });
    await cat.completeSchemaMigration({ migrationId });
}

export default {
    async fetch(request: Request, env: Env): Promise<Response> {
        const operation = new URL(request.url).pathname.slice(1);
        const body = (await request.json()) as Record<string, unknown>;
        try {
            if (operation === "setup") {
                await activateSchema(env);
                return Response.json({ ok: true });
            }
            if (operation === "seed") {
                await cdb(env, SOURCE).fixtureSeed(body as never);
                return Response.json({ bytes: await cdb(env, SOURCE).fixtureBytes(), mark: MARK });
            }
            if (operation === "alarm") {
                await cdb(env, String(body.shardId)).fixtureAlarm();
                return Response.json({ ok: true });
            }
            if (operation === "headroom") return Response.json(await resharder(env).fixtureHeadroom());
            if (operation === "topology") return Response.json(await catalog(env).topology());
            if (operation === "rows") return Response.json(await cdb(env, String(body.shardId)).fixtureRows());
            return Response.json({ error: `unknown operation ${operation}` }, { status: 404 });
        } catch (error) {
            return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 });
        }
    },
};
