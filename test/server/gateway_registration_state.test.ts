import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { CdbError } from "../../src/errors.ts";
import type { SyncSql } from "../../src/oplog/wrapper.ts";
import {
    GATEWAY_REGISTRATION_DDL,
    type GatewayRegistrationAdvance,
    type GatewayRegistrationInstall,
    activateGatewaySubscription,
    advanceGatewayRegistration,
    claimDirtyGatewayRegistration,
    cleanupGatewayRegistration,
    ensureGatewayRegistrationColumns,
    installGatewayRegistration,
    listCurrentGatewayRegistrationsForConnection,
    retireCurrentGatewayRegistration,
    retireCurrentGatewayRegistrationsForConnection,
    retireGatewayRegistration,
} from "../../src/server/do/gateway-registration-store.ts";
import { projectCdbSubscriptionResponse } from "../../src/server/do/gateway.ts";
import type { LiveSubscriptionId } from "../../src/server/rpc.ts";
import { ChardbRef, ClientId, Cookie, PrincipalId, SubId, TenantId } from "../../src/types.ts";

function syncSql(db: Database): SyncSql {
    return {
        exec(query, ...params) {
            db.run(query, params as never[]);
        },
        one<T>(query: string, ...params: never[]): T | null {
            return (db.query(query).get(...params) as T | null) ?? null;
        },
        all<T>(query: string, ...params: never[]): T[] {
            return db.query(query).all(...params) as T[];
        },
        changes() {
            return Number((db.query("SELECT changes() AS changes").get() as { changes: number }).changes);
        },
    };
}

function registration(
    registrationId: string,
    principalId = "principal-1",
    overrides: Partial<GatewayRegistrationInstall> = {}
): GatewayRegistrationInstall {
    return {
        recoveryGeneration: 0,
        registrationId,
        principalId: PrincipalId(principalId),
        clientId: ClientId("client-shared"),
        subId: SubId(7),
        connectionId: `connection-${registrationId}`,
        organizationId: TenantId("org-1"),
        ref: ChardbRef("queries.ts#messages"),
        args: { z: 1, organizationId: "org-1" },
        intent: {
            kind: "select",
            tables: ["messages"],
            partitionKey: { table: "messages", column: "organization_id", values: ["org-1"] },
        },
        policyDigest: "policy-digest-1",
        queryHash: "query-hash-1",
        shardId: "shard-1",
        sourceCdbId: "cdb-object-1",
        schemaEpoch: 4,
        domainSchemaEpoch: 1,
        authEpochs: { global: 5, tenant: 6, principal: 7 },
        lastCookie: Cookie("cookie-0"),
        nowMs: 100,
        ...overrides,
    };
}

function advance(input: GatewayRegistrationInstall, overrides: Partial<GatewayRegistrationAdvance> = {}) {
    return {
        principalId: input.principalId,
        clientId: input.clientId,
        subId: input.subId,
        registrationId: input.registrationId,
        expectedRunVersion: 0,
        lifecycle: "active" as const,
        cdbState: "active" as const,
        dirtyVersion: 3,
        deliveredVersion: 2,
        lastCookie: Cookie("cookie-1"),
        retryCount: 2,
        retryAt: 500,
        retryError: "retryable",
        nowMs: 200,
        ...overrides,
    } satisfies GatewayRegistrationAdvance;
}

describe("Gateway durable registration generations", () => {
    let db: Database;
    let sql: SyncSql;

    beforeEach(() => {
        db = new Database(":memory:");
        db.exec("PRAGMA foreign_keys = ON");
        db.exec(GATEWAY_REGISTRATION_DDL);
        sql = syncSql(db);
    });

    afterEach(() => db.close());

    test("strictly validates both Cdb subscribe response branches", () => {
        const expected: LiveSubscriptionId = {
            gatewayId: "gateway-1",
            registrationId: "registration-1",
            connectionId: "connection-1",
            clientId: ClientId("client-1"),
            subId: SubId(1),
        };
        const success = { ok: true, subscription: expected, changeSeq: 0 } as const;
        const failure = {
            ok: false,
            registrationState: "absent",
            subscription: expected,
            error: new CdbError({ code: "CDB_RATE_LIMITED", retryAfterMs: 1_000 }).toJSON(),
        } as const;
        expect(projectCdbSubscriptionResponse(success, expected)).toEqual(success);
        expect(projectCdbSubscriptionResponse(failure, expected)).toEqual(failure);

        for (const malformed of [
            null,
            [],
            { ...success, extra: true },
            { ...success, ok: "true" },
            { ...success, changeSeq: -1 },
            { ...success, changeSeq: 1.5 },
            { ...success, changeSeq: Number.MAX_SAFE_INTEGER + 1 },
            { ...failure, subscription: { ...expected, registrationId: "wrong" } },
            { ...failure, error: { ...failure.error, retryable: false } },
            { ...failure, error: { ...failure.error, retryAfterMs: -1 } },
            { ...failure, error: { ...failure.error, retryAfterMs: 1.5 } },
            { ...failure, error: { ...failure.error, retryAfterMs: 2_147_483_648 } },
            { ...failure, error: { ...failure.error, extra: true } },
        ]) {
            expect(() => projectCdbSubscriptionResponse(malformed, expected)).toThrow();
        }
    });

    test("installs canonical state and supersedes the old generation into retiring", () => {
        const first = registration("registration-1");
        const firstInstall = db.transaction(() => installGatewayRegistration(sql, first))();
        expect(firstInstall).toEqual({ supersededRegistrationId: null });
        expect(
            (db.query("PRAGMA table_info('_gw_registration_generations')").all() as { name: string }[]).map(
                column => column.name
            )
        ).toContain("policy_digest");
        expect(
            db
                .query(
                    `SELECT connection_id, organization_id, ref, args_json, intent_json, policy_digest,
                            query_hash, shard_id,
                            source_cdb_id,
                            schema_epoch, auth_global_epoch, auth_tenant_epoch, auth_principal_epoch,
                            lifecycle, cdb_state, dirty_version, delivered_version,
                            run_token, run_target_version, run_version,
                            last_cookie, retry_count, retry_at, retry_error
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(first.registrationId)
        ).toEqual({
            connection_id: "connection-registration-1",
            organization_id: "org-1",
            ref: "queries.ts#messages",
            args_json: '{"organizationId":"org-1","z":1}',
            intent_json:
                '{"kind":"select","partitionKey":{"column":"organization_id","table":"messages","values":["org-1"]},"tables":["messages"]}',
            policy_digest: "policy-digest-1",
            query_hash: "query-hash-1",
            shard_id: "shard-1",
            source_cdb_id: "cdb-object-1",
            schema_epoch: 4,
            auth_global_epoch: 5,
            auth_tenant_epoch: 6,
            auth_principal_epoch: 7,
            lifecycle: "installing",
            cdb_state: "pending",
            dirty_version: 0,
            delivered_version: 0,
            run_token: null,
            run_target_version: null,
            run_version: 0,
            last_cookie: "cookie-0",
            retry_count: 0,
            retry_at: null,
            retry_error: null,
        });

        expect(db.transaction(() => advanceGatewayRegistration(sql, advance(first)))()).toBe(true);
        const replacement = registration("registration-2", "principal-1", { nowMs: 300 });
        expect(db.transaction(() => installGatewayRegistration(sql, replacement))()).toEqual({
            supersededRegistrationId: "registration-1",
        });

        expect(
            db
                .query(
                    `SELECT lifecycle, cdb_state, run_token, run_target_version, run_version
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(first.registrationId)
        ).toEqual({
            lifecycle: "retiring",
            cdb_state: "retiring",
            run_token: null,
            run_target_version: null,
            run_version: 2,
        });
        expect(
            db
                .query(
                    "SELECT registration_id FROM _gw_registration_heads WHERE principal_id = ? AND client_id = ? AND sub_id = ?"
                )
                .get(replacement.principalId, replacement.clientId, replacement.subId)
        ).toEqual({ registration_id: "registration-2" });
        expect(
            db.transaction(() =>
                advanceGatewayRegistration(sql, advance(first, { expectedRunVersion: 2, nowMs: 400 }))
            )()
        ).toBe(false);
        expect(db.transaction(() => advanceGatewayRegistration(sql, advance(replacement, { nowMs: 400 })))()).toBe(
            true
        );
        expect(
            db
                .query(
                    `SELECT lifecycle, cdb_state, dirty_version, delivered_version, run_token, run_version,
                            last_cookie, retry_count, retry_at, retry_error
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(replacement.registrationId)
        ).toEqual({
            lifecycle: "active",
            cdb_state: "active",
            dirty_version: 3,
            delivered_version: 2,
            run_token: null,
            run_version: 1,
            last_cookie: "cookie-1",
            retry_count: 2,
            retry_at: 500,
            retry_error: "retryable",
        });
        expect(db.transaction(() => advanceGatewayRegistration(sql, advance(replacement, { nowMs: 500 })))()).toBe(
            false
        );
    });

    test("explicit retire removes the head and retains a cleanup row", () => {
        const current = registration("registration-retire");
        db.transaction(() => installGatewayRegistration(sql, current))();
        expect(db.transaction(() => activateGatewaySubscription(sql, { ...current, changeSeq: 3, nowMs: 200 }))()).toBe(
            true
        );
        expect(
            db.transaction(() => claimDirtyGatewayRegistration(sql, { ...current, nowMs: 200, leaseExpiresAt: 300 }))()
        ).toMatchObject({ targetVersion: 3, runVersion: 1 });

        expect(db.transaction(() => retireGatewayRegistration(sql, current, current.registrationId, 250))()).toBe(true);
        expect(db.query("SELECT * FROM _gw_registration_heads").all()).toEqual([]);
        expect(
            db
                .query(
                    `SELECT lifecycle, cdb_state, delivered_version, run_token, run_target_version, run_version
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(current.registrationId)
        ).toEqual({
            lifecycle: "retiring",
            cdb_state: "retiring",
            delivered_version: 0,
            run_token: null,
            run_target_version: null,
            run_version: 2,
        });
    });

    test("isolates equal client and sub ids by principal", () => {
        const first = registration("registration-principal-1", "principal-1");
        const second = registration("registration-principal-2", "principal-2");
        db.transaction(() => installGatewayRegistration(sql, first))();
        db.transaction(() => installGatewayRegistration(sql, second))();

        expect(
            db.query("SELECT principal_id, registration_id FROM _gw_registration_heads ORDER BY principal_id").all()
        ).toEqual([
            { principal_id: "principal-1", registration_id: "registration-principal-1" },
            { principal_id: "principal-2", registration_id: "registration-principal-2" },
        ]);
        expect(db.transaction(() => retireGatewayRegistration(sql, first, first.registrationId, 300))()).toBe(true);
        expect(db.query("SELECT principal_id, registration_id FROM _gw_registration_heads").all()).toEqual([
            { principal_id: "principal-2", registration_id: "registration-principal-2" },
        ]);
    });

    test("old-generation cleanup cannot delete its replacement or current head", () => {
        const old = registration("registration-old");
        const replacement = registration("registration-new", "principal-1", { nowMs: 200 });
        db.transaction(() => {
            installGatewayRegistration(sql, old);
            installGatewayRegistration(sql, replacement);
        })();

        expect(db.transaction(() => cleanupGatewayRegistration(sql, old, old.registrationId))()).toBe(false);
        expect(db.transaction(() => cleanupGatewayRegistration(sql, replacement, replacement.registrationId))()).toBe(
            false
        );
        expect(() =>
            db
                .query("DELETE FROM _gw_registration_generations WHERE registration_id = ?")
                .run(replacement.registrationId)
        ).toThrow();
        expect(db.query("SELECT registration_id FROM _gw_registration_heads").all()).toEqual([
            { registration_id: "registration-new" },
        ]);
        expect(
            db.query("SELECT registration_id FROM _gw_registration_generations ORDER BY registration_id").all()
        ).toEqual([{ registration_id: "registration-new" }, { registration_id: "registration-old" }]);
    });

    test("bootstrap repairs a missing pending recovery deadline without making cleanup eligible", () => {
        const pending = registration("registration-pending");
        db.transaction(() => {
            installGatewayRegistration(sql, pending);
            retireGatewayRegistration(sql, pending, pending.registrationId, 150);
        })();
        expect(db.query("SELECT lifecycle, cdb_state, retry_at FROM _gw_registration_generations").get()).toEqual({
            lifecycle: "retiring",
            cdb_state: "pending",
            retry_at: null,
        });

        ensureGatewayRegistrationColumns(sql);

        expect(db.query("SELECT lifecycle, cdb_state, retry_at FROM _gw_registration_generations").get()).toEqual({
            lifecycle: "retiring",
            cdb_state: "pending",
            retry_at: 30_150,
        });
        expect(cleanupGatewayRegistration(sql, pending, pending.registrationId)).toBe(false);
    });

    test("lists and retires only exact current generations for one connection", () => {
        const first = registration("registration-connection-a1", "principal-1", {
            connectionId: "connection-a",
            subId: SubId(1),
            shardId: "logical-a1",
            sourceCdbId: "physical-a1",
        });
        const second = registration("registration-connection-a2", "principal-1", {
            connectionId: "connection-a",
            subId: SubId(2),
            shardId: "logical-a2",
            sourceCdbId: "physical-a2",
        });
        const other = registration("registration-connection-b", "principal-1", {
            connectionId: "connection-b",
            subId: SubId(3),
            shardId: "logical-b",
            sourceCdbId: "physical-b",
        });
        db.transaction(() => {
            installGatewayRegistration(sql, first);
            installGatewayRegistration(sql, second);
            installGatewayRegistration(sql, other);
        })();

        expect(listCurrentGatewayRegistrationsForConnection(sql, "connection-a")).toEqual([
            {
                principalId: PrincipalId("principal-1"),
                clientId: ClientId("client-shared"),
                subId: SubId(1),
                registrationId: "registration-connection-a1",
                connectionId: "connection-a",
                shardId: "logical-a1",
                sourceCdbId: "physical-a1",
            },
            {
                principalId: PrincipalId("principal-1"),
                clientId: ClientId("client-shared"),
                subId: SubId(2),
                registrationId: "registration-connection-a2",
                connectionId: "connection-a",
                shardId: "logical-a2",
                sourceCdbId: "physical-a2",
            },
        ]);
        expect(
            db.transaction(() =>
                retireCurrentGatewayRegistration(sql, { ...first, connectionId: "wrong-connection", nowMs: 200 })
            )()
        ).toBeNull();
        expect(
            db.transaction(() => retireCurrentGatewayRegistrationsForConnection(sql, "connection-a", 300))()
        ).toHaveLength(2);
        expect(db.query("SELECT registration_id FROM _gw_registration_heads ORDER BY registration_id").all()).toEqual([
            { registration_id: other.registrationId },
        ]);
        expect(
            db
                .query(
                    `SELECT registration_id, lifecycle, cdb_state, run_version
                     FROM _gw_registration_generations ORDER BY registration_id`
                )
                .all()
        ).toEqual([
            {
                registration_id: first.registrationId,
                lifecycle: "retiring",
                cdb_state: "pending",
                run_version: 1,
            },
            {
                registration_id: second.registrationId,
                lifecycle: "retiring",
                cdb_state: "pending",
                run_version: 1,
            },
            {
                registration_id: other.registrationId,
                lifecycle: "installing",
                cdb_state: "pending",
                run_version: 0,
            },
        ]);
    });

    test("fails closed for corrupt head identity and impossible generation state", () => {
        const corruptHead = registration("registration-corrupt-head");
        db.transaction(() => installGatewayRegistration(sql, corruptHead))();
        db.query("UPDATE _gw_registration_heads SET principal_id = 'corrupt-principal' WHERE registration_id = ?").run(
            corruptHead.registrationId
        );

        expect(
            db.transaction(() => activateGatewaySubscription(sql, { ...corruptHead, changeSeq: 1, nowMs: 200 }))()
        ).toBe(false);
        expect(listCurrentGatewayRegistrationsForConnection(sql, corruptHead.connectionId)).toEqual([]);
        expect(
            db.transaction(() => retireCurrentGatewayRegistration(sql, { ...corruptHead, nowMs: 300 }))()
        ).toBeNull();

        const corruptGeneration = registration("registration-corrupt-generation", "principal-2", {
            clientId: ClientId("client-2"),
        });
        db.transaction(() => installGatewayRegistration(sql, corruptGeneration))();
        db.query("UPDATE _gw_registration_generations SET lifecycle = 'active' WHERE registration_id = ?").run(
            corruptGeneration.registrationId
        );
        expect(
            db.transaction(() => activateGatewaySubscription(sql, { ...corruptGeneration, changeSeq: 1, nowMs: 200 }))()
        ).toBe(false);
    });

    test("retires active generations created before policy identity existed", () => {
        const legacy = new Database(":memory:");
        try {
            legacy.exec(`
                CREATE TABLE _gw_registration_generations (
                  registration_id TEXT PRIMARY KEY,
                  source_cdb_id TEXT,
                  lifecycle TEXT NOT NULL,
                  cdb_state TEXT NOT NULL,
                  run_token TEXT,
                  run_target_version INTEGER,
                  run_lease_expires_at INTEGER,
                  run_version INTEGER NOT NULL,
                  last_snapshot_cookie TEXT,
                  initial_snapshot_pending INTEGER NOT NULL,
                  retry_count INTEGER NOT NULL,
                  retry_at INTEGER,
                  retry_error TEXT,
                  updated_at INTEGER NOT NULL
                );
                CREATE TABLE _gw_registration_heads (registration_id TEXT PRIMARY KEY);
                CREATE TABLE _gw_snapshot_outbox (registration_id TEXT PRIMARY KEY);
                INSERT INTO _gw_registration_generations VALUES (
                  'registration-legacy', 'cdb-legacy', 'active', 'active',
                  'run-legacy', 2, 500, 3, 'cookie-legacy', 0, 4, 600, 'old error', 100
                );
                INSERT INTO _gw_registration_heads VALUES ('registration-legacy');
                INSERT INTO _gw_snapshot_outbox VALUES ('registration-legacy');
            `);
            const legacySql = syncSql(legacy);

            ensureGatewayRegistrationColumns(legacySql);

            expect(
                (legacy.query("PRAGMA table_info('_gw_registration_generations')").all() as { name: string }[]).map(
                    column => column.name
                )
            ).toContain("policy_digest");
            expect(
                legacy
                    .query(
                        `SELECT lifecycle, cdb_state, run_token, run_target_version, run_lease_expires_at,
                                run_version, retry_count, retry_at, retry_error, policy_digest
                         FROM _gw_registration_generations`
                    )
                    .get()
            ).toEqual({
                lifecycle: "retiring",
                cdb_state: "retiring",
                run_token: null,
                run_target_version: null,
                run_lease_expires_at: null,
                run_version: 4,
                retry_count: 0,
                retry_at: 100,
                retry_error: null,
                policy_digest: "",
            });
            expect(legacy.query("SELECT * FROM _gw_registration_heads").all()).toEqual([]);
            expect(legacy.query("SELECT * FROM _gw_snapshot_outbox").all()).toEqual([]);
        } finally {
            legacy.close();
        }
    });
});
