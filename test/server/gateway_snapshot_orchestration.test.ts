import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import type { SyncSql } from "../../src/oplog/wrapper.ts";
import type { VerifiedGwAttachment } from "../../src/server/do/gateway-auth-dispatch.ts";
import {
    GATEWAY_REGISTRATION_DDL,
    claimDirtyGatewayRegistration,
    installGatewayRegistration,
} from "../../src/server/do/gateway-registration-store.ts";
import { GatewaySnapshotDelivery } from "../../src/server/do/gateway-snapshot-delivery.ts";
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

describe("Gateway snapshot delivery orchestration", () => {
    let db: Database;

    afterEach(() => db.close());

    function fixture(
        options: {
            readonly throwOnSend?: boolean;
            readonly authorityDelayMs?: number;
            readonly socketExpiresAt?: number;
            readonly authority?: "fresh" | "changed" | "forbidden" | "refetch" | "retry";
            readonly policyDigest?: string;
        } = {}
    ) {
        db = new Database(":memory:");
        const sql = syncSql(db);
        sql.exec("PRAGMA foreign_keys = ON");
        for (const statement of GATEWAY_REGISTRATION_DDL.split(";")
            .map(value => value.trim())
            .filter(Boolean)) {
            sql.exec(statement);
        }
        const identity = {
            principalId: PrincipalId("principal-delivery"),
            clientId: ClientId("client-delivery"),
            subId: SubId(7),
            registrationId: "registration-delivery",
            connectionId: "connection-delivery",
        };
        db.transaction(() =>
            installGatewayRegistration(sql, {
                recoveryGeneration: 0,
                ...identity,
                organizationId: TenantId("org-delivery"),
                ref: ChardbRef("queries.ts#delivery"),
                args: { organizationId: "org-delivery" },
                intent: {
                    kind: "select",
                    tables: ["messages"],
                    partitionKey: { table: "messages", column: "organization_id", values: ["org-delivery"] },
                },
                policyDigest: "policy-delivery",
                queryHash: "query-delivery",
                shardId: "shard-delivery",
                sourceCdbId: "physical-delivery",
                schemaEpoch: 1,
                domainSchemaEpoch: 1,
                authEpochs: { global: 1, tenant: 2, principal: 3 },
                nowMs: 10,
            })
        )();
        db.query(
            `UPDATE _gw_registration_generations
             SET lifecycle = 'active', cdb_state = 'active', dirty_version = 5, delivered_version = 2,
                 retry_at = NULL
             WHERE registration_id = ?`
        ).run(identity.registrationId);
        const run = db.transaction(() =>
            claimDirtyGatewayRegistration(sql, {
                ...identity,
                nowMs: 20,
                leaseExpiresAt: 30_020,
            })
        )();
        if (!run) throw new Error("fixture could not claim its dirty registration");

        const alarms: number[] = [];
        const work: number[] = [];
        const sent: unknown[] = [];
        const retired: unknown[] = [];
        let nowMs = 25;
        const attachment: VerifiedGwAttachment = {
            kind: "verified",
            connectionId: identity.connectionId,
            authOrigin: "https://app.example",
            clientId: identity.clientId,
            principalId: identity.principalId,
            jwtExp: 1_000,
            lastCookie: Cookie("cookie-base"),
            snapshotSubIds: [identity.subId],
        };
        const delivery = new GatewaySnapshotDelivery({
            storage: {
                sql,
                transactionSync: <T>(callback: () => T): T => db.transaction(callback)(),
            },
            nowMs: () => nowMs,
            scheduleAlarm: async requestedAt => {
                alarms.push(requestedAt);
            },
            scheduleWork: async requestedAt => {
                work.push(requestedAt);
            },
            currentPolicyDigest: () => options.policyDigest ?? "policy-delivery",
            checkAuthority: async () => {
                await Promise.resolve();
                nowMs += options.authorityDelayMs ?? 0;
                if (options.authority === "changed") return { kind: "changed" };
                if (options.authority === "forbidden") return { kind: "retire", code: "CDB_FORBIDDEN" };
                if (options.authority === "refetch") return { kind: "refetch" };
                if (options.authority === "retry") return { kind: "retry", message: "Catalog unavailable" };
                return {
                    kind: "fresh",
                    auth: {
                        userId: identity.principalId,
                        tenantId: TenantId("org-delivery"),
                        role: "member",
                        roles: ["member"],
                        claims: {},
                        authEpochs: { global: 10, tenant: 11, principal: 12 },
                    },
                };
            },
            exactSocket: (_identity, checkedAt) =>
                checkedAt >= (options.socketExpiresAt ?? Number.POSITIVE_INFINITY)
                    ? { status: "terminal" }
                    : { status: "ready", ws: {} as WebSocket, attachment },
            settleRetired: (_identity, settlement) => {
                retired.push(settlement);
            },
            send: (_socket, message) => {
                if (options.throwOnSend) throw new Error("held transport failure");
                sent.push(message);
            },
        });
        const setNowMs = (value: number): void => {
            nowMs = value;
        };
        return { sql, identity, run, delivery, alarms, work, sent, retired, setNowMs };
    }

    test("stages, pre-arms, claims, sends, and acknowledges one exact durable snapshot", async () => {
        const { identity, run, delivery, alarms, work, sent } = fixture();
        const cookie = Cookie("snapshot-delivery-cookie");
        expect(
            await delivery.stage({
                recoveryGeneration: 0,
                ...identity,
                runToken: run.runToken,
                runVersion: run.runVersion,
                targetVersion: run.targetVersion,
                cookie,
                rows: [{ id: 1, body: "durable" }],
                authEpochs: { global: 10, tenant: 11, principal: 12 },
                nowMs: 25,
            })
        ).toBe(true);
        expect(alarms).toEqual([26]);

        const attempts = await delivery.claimDue(25);
        expect(alarms).toEqual([26, 10_025]);
        expect(attempts).toHaveLength(1);
        const attempt = attempts[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        await delivery.sendAttempt(attempt);
        expect(sent).toEqual([{ t: "snapshot", subId: identity.subId, cookie, rows: [{ id: 1, body: "durable" }] }]);

        const settlement = delivery.acknowledge({
            principalId: identity.principalId,
            clientId: identity.clientId,
            connectionId: identity.connectionId,
            cookie,
            nowMs: 30,
        });
        expect(settlement).toMatchObject({ kind: "current", acknowledged: true });
        expect(db.query("SELECT * FROM _gw_snapshot_outbox").get()).toBeNull();
        expect(
            db
                .query(
                    `SELECT dirty_version, delivered_version, last_cookie, last_snapshot_cookie
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(identity.registrationId)
        ).toEqual({ dirty_version: 5, delivered_version: 5, last_cookie: cookie, last_snapshot_cookie: cookie });
        expect(work).toEqual([]);
    });

    test.each([{ authorityDelayMs: 100, socketExpiresAt: 100 }, { authorityDelayMs: 10_000 }])(
        "does not send after authority lookup outlives socket or claim: %j",
        async options => {
            const { identity, run, delivery, sent } = fixture(options);
            await delivery.stage({
                recoveryGeneration: 0,
                ...identity,
                runToken: run.runToken,
                runVersion: run.runVersion,
                targetVersion: run.targetVersion,
                cookie: Cookie("snapshot-expired"),
                rows: [{ secret: "expired" }],
                authEpochs: { global: 10, tenant: 11, principal: 12 },
                nowMs: 25,
            });
            const attempt = (await delivery.claimDue(25))[0];
            if (!attempt) throw new Error("missing snapshot claim");
            await delivery.sendAttempt(attempt);
            expect(sent).toEqual([]);
            if (options.socketExpiresAt === undefined) {
                expect(await delivery.claimDue(10_025)).toHaveLength(1);
            } else {
                expect(db.query("SELECT * FROM _gw_registration_heads").get()).toBeNull();
            }
        }
    );

    test("records a bounded retry and preserves the staged rows when transport send throws", async () => {
        const { identity, run, delivery, work, setNowMs } = fixture({ throwOnSend: true });
        const cookie = Cookie("snapshot-retry-cookie");
        await delivery.stage({
            recoveryGeneration: 0,
            ...identity,
            runToken: run.runToken,
            runVersion: run.runVersion,
            targetVersion: run.targetVersion,
            cookie,
            rows: [{ id: 2, body: "retry" }],
            authEpochs: { global: 10, tenant: 11, principal: 12 },
            nowMs: 25,
        });
        const attempt = (await delivery.claimDue(25))[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        setNowMs(40);
        await delivery.sendAttempt(attempt);

        expect(work).toEqual([40]);
        expect(
            db
                .query(
                    `SELECT cookie, rows_json, send_attempts, claim_token, claim_expires_at, last_error, next_attempt_at
                     FROM _gw_snapshot_outbox WHERE registration_id = ?`
                )
                .get(identity.registrationId)
        ).toMatchObject({
            cookie,
            rows_json: '[{"body":"retry","id":2}]',
            send_attempts: 1,
            claim_token: null,
            claim_expires_at: null,
            last_error: "held transport failure",
            next_attempt_at: 1_040,
        });
    });

    test("discards a claimed snapshot when authority changed after staging", async () => {
        const { identity, run, delivery, work, sent } = fixture({ authority: "changed" });
        await delivery.stage({
            recoveryGeneration: 0,
            ...identity,
            runToken: run.runToken,
            runVersion: run.runVersion,
            targetVersion: run.targetVersion,
            cookie: Cookie("snapshot-stale-authority"),
            rows: [{ id: 3, secret: "stale" }],
            authEpochs: { global: 10, tenant: 11, principal: 12 },
            nowMs: 25,
        });
        const attempt = (await delivery.claimDue(25))[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        await delivery.sendAttempt(attempt);

        expect(sent).toEqual([]);
        expect(work).toEqual([25]);
        expect(db.query("SELECT * FROM _gw_snapshot_outbox").get()).toBeNull();
        expect(
            db
                .query(
                    `SELECT lifecycle, dirty_version, delivered_version
                     FROM _gw_registration_generations WHERE registration_id = ?`
                )
                .get(identity.registrationId)
        ).toEqual({ lifecycle: "active", dirty_version: 5, delivered_version: 2 });
    });

    test("retires a claimed snapshot when fresh authority is revoked", async () => {
        const { identity, run, delivery, sent, retired } = fixture({ authority: "forbidden" });
        await delivery.stage({
            recoveryGeneration: 0,
            ...identity,
            runToken: run.runToken,
            runVersion: run.runVersion,
            targetVersion: run.targetVersion,
            cookie: Cookie("snapshot-revoked-authority"),
            rows: [{ id: 4, secret: "revoked" }],
            authEpochs: { global: 10, tenant: 11, principal: 12 },
            nowMs: 25,
        });
        const attempt = (await delivery.claimDue(25))[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        await delivery.sendAttempt(attempt);

        expect(sent).toEqual([]);
        expect(retired).toEqual([{ kind: "error", code: "CDB_FORBIDDEN" }]);
        expect(db.query("SELECT * FROM _gw_registration_heads").get()).toBeNull();
        expect(db.query("SELECT * FROM _gw_snapshot_outbox").get()).toBeNull();
    });

    test("refetches a migrated claimed snapshot before evaluating its stale policy metadata", async () => {
        const { identity, run, delivery, sent, retired } = fixture({
            authority: "refetch",
            policyDigest: "policy-migrated",
        });
        await delivery.stage({
            recoveryGeneration: 0,
            ...identity,
            runToken: run.runToken,
            runVersion: run.runVersion,
            targetVersion: run.targetVersion,
            cookie: Cookie("snapshot-migrated-policy"),
            rows: [{ id: 5, secret: "superseded-schema" }],
            authEpochs: { global: 10, tenant: 11, principal: 12 },
            nowMs: 25,
        });
        const attempt = (await delivery.claimDue(25))[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        await delivery.sendAttempt(attempt);

        expect(sent).toEqual([]);
        expect(retired).toEqual([{ kind: "refetch", reason: "shardsChanged" }]);
        expect(db.query("SELECT * FROM _gw_registration_heads").get()).toBeNull();
        expect(db.query("SELECT * FROM _gw_snapshot_outbox").get()).toBeNull();
    });

    test("bounds Catalog freshness failures without dropping staged rows", async () => {
        const { identity, run, delivery, work, sent } = fixture({ authority: "retry" });
        const cookie = Cookie("snapshot-authority-retry");
        await delivery.stage({
            recoveryGeneration: 0,
            ...identity,
            runToken: run.runToken,
            runVersion: run.runVersion,
            targetVersion: run.targetVersion,
            cookie,
            rows: [{ id: 5 }],
            authEpochs: { global: 10, tenant: 11, principal: 12 },
            nowMs: 25,
        });
        const attempt = (await delivery.claimDue(25))[0];
        if (!attempt) throw new Error("delivery did not return its claimed snapshot");
        await delivery.sendAttempt(attempt);

        expect(sent).toEqual([]);
        expect(work).toEqual([25]);
        expect(
            db
                .query(
                    `SELECT cookie, send_attempts, claim_token, claim_expires_at, last_error, next_attempt_at
                     FROM _gw_snapshot_outbox WHERE registration_id = ?`
                )
                .get(identity.registrationId)
        ).toEqual({
            cookie,
            send_attempts: 1,
            claim_token: null,
            claim_expires_at: null,
            last_error: "Catalog unavailable",
            next_attempt_at: 1_025,
        });
    });
});
