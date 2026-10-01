import { chardb, defineAuth, defineMigrations } from "@chardb/core/server";
import { makeSignature, symmetricEncrypt } from "better-auth/crypto";
import { anonymous } from "better-auth/plugins/anonymous";
import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { SignJWT, exportJWK, generateKeyPair, importJWK, jwtVerify } from "jose";
import * as api from "../../example/chat/src/server/api.ts";
import { selectMigrationInputs } from "../../example/chat/src/server/migrations/history.ts";
import * as queries from "../../example/chat/src/server/queries.ts";
import * as schema from "../../example/chat/src/server/schema.ts";
import { initializeCatalogStorage } from "../../src/server/do/catalog-schema-store.ts";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";

declare const CHAT_HISTORY: "better-auth-1.6" | "better-auth-1.7";
declare const CHAT_VERSION: 1 | 2;
const SECRET = "persisted-chat-migration-secret-unchanged-123456789";
const TOKEN = "persisted-session-token";
const plugins = [anonymous(), organization(), jwt()] as const;
// The legacy fixture models the deployed SQL shape under the installed runtime.
if (CHAT_VERSION === 1 && CHAT_HISTORY === "better-auth-1.6") {
    const fields = plugins[2].schema.jwks.fields;
    plugins[2].schema.jwks.fields = Object.fromEntries(
        Object.entries(fields).filter(([name]) => name !== "alg" && name !== "crv")
    ) as typeof fields;
}
const auth = defineAuth({ plugins, secret: SECRET });
const inputs = selectMigrationInputs(CHAT_HISTORY);
const app = chardb({
    ownership: "organization",
    auth,
    schema,
    api: { ...api, ...queries },
    migrations: defineMigrations(CHAT_VERSION === 1 ? inputs.slice(0, 1) : inputs),
});

export class Catalog extends app.Catalog {
    seed(publicKey: string, privateKey: string): void {
        const sql = this.ctx.storage.sql;
        const now = Date.now();
        this.ctx.storage.transactionSync(() => {
            sql.exec(
                'INSERT INTO "user" (id, name, email, "emailVerified", "createdAt", "updatedAt") VALUES (?, ?, ?, 1, ?, ?)',
                "user",
                "Existing User",
                "existing@example.com",
                now,
                now
            );
            sql.exec(
                'INSERT INTO "organization" (id, name, slug, "createdAt") VALUES (?, ?, ?, ?)',
                "org",
                "Existing Org",
                "existing",
                now
            );
            sql.exec(
                'INSERT INTO "member" (id, "organizationId", "userId", role, "createdAt") VALUES (?, ?, ?, ?, ?)',
                "member",
                "org",
                "user",
                "owner",
                now
            );
            sql.exec(
                'INSERT INTO "session" (id, "expiresAt", token, "createdAt", "updatedAt", "userId", "activeOrganizationId") VALUES (?, ?, ?, ?, ?, ?, ?)',
                "session",
                now + 7 * 86_400_000,
                TOKEN,
                now,
                now,
                "user",
                "org"
            );
            sql.exec(
                'INSERT INTO "account" (id, "accountId", "providerId", "userId", password, "createdAt", "updatedAt") VALUES (?, ?, ?, ?, ?, ?, ?)',
                "account",
                "user",
                "credential",
                "user",
                "existing-password-hash",
                now,
                now
            );
            sql.exec(
                'INSERT INTO "jwks" (id, "publicKey", "privateKey", "createdAt") VALUES (?, ?, ?, ?)',
                "key",
                publicKey,
                privateKey,
                now
            );
        });
    }

    snapshot() {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        return {
            state: this.schemaState(),
            rows: Object.fromEntries(
                ["user", "organization", "member", "session", "account"].map(table => [
                    table,
                    sql.all(`SELECT * FROM "${table}" ORDER BY id`),
                ])
            ),
            keys: sql.all('SELECT id, "publicKey", "privateKey", "createdAt", "expiresAt" FROM jwks ORDER BY id'),
            columns: sql.all<{ name: string }>('PRAGMA table_info("jwks")').map(column => column.name),
            steps: sql.all("SELECT version, digest FROM catalog_schema_steps ORDER BY version"),
        };
    }

    checkHistory(history: string, corrupt = false) {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        if (corrupt) sql.exec("UPDATE catalog_schema_state SET active_digest = ?", "0".repeat(64));
        try {
            initializeCatalogStorage(sql, defineMigrations(selectMigrationInputs(history)));
            return { code: null };
        } catch (error) {
            return { code: (error as { code?: string }).code ?? null };
        }
    }
}

export class Cdb extends app.Cdb {
    seed(): void {
        this.ctx.storage.sql.exec(
            "INSERT INTO messages (id, organization_id, author_id, body, created_at) VALUES (?, ?, ?, ?, ?)",
            "message",
            "org",
            "user",
            "Existing message",
            1
        );
    }

    snapshot() {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        return {
            state: this.schemaState(),
            rows: sql.all("SELECT * FROM messages ORDER BY id"),
            steps: sql.all("SELECT version, digest FROM _chardb_schema_steps ORDER BY version"),
        };
    }
}
export const { Gateway, Resharder } = app;

type Env = {
    CDB_CATALOG: DurableObjectNamespace<Catalog>;
    CDB_SHARD: DurableObjectNamespace<Cdb>;
};

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const catalog = env.CDB_CATALOG.get(env.CDB_CATALOG.idFromName("global"));
        const shard = env.CDB_SHARD.get(env.CDB_SHARD.idFromName("ShardDO_0"));
        const url = new URL(request.url);
        if (url.pathname === "/fixture/seed") {
            const keys = await generateKeyPair("EdDSA", { extractable: true });
            const publicKey = await exportJWK(keys.publicKey);
            await catalog.seed(
                JSON.stringify(publicKey),
                JSON.stringify(
                    await symmetricEncrypt({ key: SECRET, data: JSON.stringify(await exportJWK(keys.privateKey)) })
                )
            );
            await shard.seed();
            const token = await new SignJWT({ id: "user", email: "existing@example.com" })
                .setSubject("user")
                .setIssuer("http://example.com")
                .setAudience("http://example.com")
                .setIssuedAt()
                .setExpirationTime("1h")
                .setProtectedHeader({ alg: "EdDSA", kid: "key" })
                .sign(keys.privateKey);
            return Response.json({ token });
        }
        if (url.pathname === "/fixture/state")
            return Response.json({ catalog: await catalog.snapshot(), shard: await shard.snapshot() });
        if (url.pathname === "/fixture/check-history") {
            const body = (await request.json()) as { history: string; corrupt?: boolean };
            return Response.json(await catalog.checkHistory(body.history, body.corrupt));
        }
        if (url.pathname === "/fixture/auth") {
            const body = (await request.json()) as { token: string };
            const cookie = `better-auth.session_token=${encodeURIComponent(`${TOKEN}.${await makeSignature(TOKEN, SECRET)}`)}`;
            const call = (path: string) =>
                app.fetch(
                    new Request(`http://example.com/api/auth/${path}`, { headers: { cookie } }),
                    env as never,
                    ctx
                );
            const sessionResponse = await call("get-session");
            if (!sessionResponse.ok)
                throw new Error(`session failed ${sessionResponse.status}: ${await sessionResponse.text()}`);
            const session = (await sessionResponse.json()) as { user: { id: string }; session: { id: string } };
            const jwksResponse = await call("jwks");
            const jwks = (await jwksResponse.json()) as { keys: { kid: string }[] };
            const key = jwks.keys.find(key => key.kid === "key");
            if (!key) throw new Error("existing key is missing from Better Auth JWKS");
            const publicKey = await importJWK(key, "EdDSA");
            const oldClaims = await jwtVerify(body.token, publicKey, {
                issuer: "http://example.com",
                audience: "http://example.com",
            });
            const tokenResponse = await call("token");
            if (!tokenResponse.ok)
                throw new Error(`token failed ${tokenResponse.status}: ${await tokenResponse.text()}`);
            const token = (await tokenResponse.json()) as { token: string };
            const freshClaims = await jwtVerify(token.token, publicKey, {
                issuer: "http://example.com",
                audience: "http://example.com",
            });
            return Response.json({
                userId: session.user.id,
                sessionId: session.session.id,
                oldSubject: oldClaims.payload.sub,
                newSubject: freshClaims.payload.sub,
            });
        }
        return app.fetch(request, env as never, ctx);
    },
};
