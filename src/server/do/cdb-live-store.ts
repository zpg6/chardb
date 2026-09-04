import { CdbError } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { ChardbRef, ClientId, PrincipalId, type RawJson, SubId, TenantId } from "../../types.ts";
import { stableHashHex } from "../../util/canonical.ts";
import { snapshotCdbQueryArgs } from "../result_limits.ts";
import type {
    CdbSubscriptionRequest,
    CdbSubscriptionResponse,
    GatewayInvalidationAck,
    LiveSubscriptionId,
} from "../rpc.ts";

export const CDB_LIVE_STORE_DDL = `
CREATE TABLE IF NOT EXISTS _chardb_live_subscriptions (
  gateway_id TEXT NOT NULL,
  registration_id TEXT NOT NULL,
  connection_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  sub_id INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
  payload_hash TEXT,
  principal_id TEXT,
  organization_id TEXT,
  authority TEXT CHECK (authority IS NULL OR authority IN ('organization', 'user', 'global')),
  schema_epoch INTEGER CHECK (schema_epoch IS NULL OR schema_epoch > 0),
  recovery_generation INTEGER CHECK (recovery_generation IS NULL OR recovery_generation >= 0),
  vshard INTEGER CHECK (vshard IS NULL OR (vshard >= 0 AND vshard < 16384)),
  domain_schema_epoch INTEGER CHECK (domain_schema_epoch IS NULL OR domain_schema_epoch > 0),
  ref TEXT,
  args_json TEXT,
  policy_digest TEXT,
  query_hash TEXT,
  tables_json TEXT,
  intervals_json TEXT,
  PRIMARY KEY (gateway_id, registration_id),
  CHECK (
    (
      state = 'retired'
      AND payload_hash IS NULL
      AND principal_id IS NULL
      AND organization_id IS NULL
      AND schema_epoch IS NULL
      AND recovery_generation IS NULL
      AND vshard IS NULL
      AND domain_schema_epoch IS NULL
      AND ref IS NULL
      AND args_json IS NULL
      AND policy_digest IS NULL
      AND query_hash IS NULL
      AND tables_json IS NULL
      AND intervals_json IS NULL
    )
    OR (
      state = 'active'
      AND payload_hash IS NOT NULL
      AND principal_id IS NOT NULL
      AND organization_id IS NOT NULL
      AND schema_epoch IS NOT NULL
      AND recovery_generation IS NOT NULL
      AND vshard IS NOT NULL
      AND domain_schema_epoch IS NOT NULL
      AND ref IS NOT NULL
      AND args_json IS NOT NULL
      AND policy_digest IS NOT NULL
      AND query_hash IS NOT NULL
      AND tables_json IS NOT NULL
      AND intervals_json IS NOT NULL
    )
  )
);
CREATE INDEX IF NOT EXISTS _chardb_live_subscriptions_by_state
  ON _chardb_live_subscriptions (state, gateway_id, registration_id);
CREATE TABLE IF NOT EXISTS _chardb_change_clock (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  change_seq INTEGER NOT NULL CHECK (change_seq >= 0)
);
INSERT OR IGNORE INTO _chardb_change_clock (singleton, change_seq) VALUES (1, 0);
CREATE TABLE IF NOT EXISTS _chardb_live_subscription_tables (
  gateway_id TEXT NOT NULL,
  registration_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  PRIMARY KEY (gateway_id, registration_id, table_name),
  FOREIGN KEY (gateway_id, registration_id)
    REFERENCES _chardb_live_subscriptions (gateway_id, registration_id)
    ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS _chardb_live_subscription_tables_by_table
  ON _chardb_live_subscription_tables (table_name, gateway_id, registration_id);
CREATE TABLE IF NOT EXISTS _chardb_live_subscription_vectors (
  gateway_id TEXT NOT NULL,
  registration_id TEXT NOT NULL,
  resource_id TEXT NOT NULL CHECK (length(resource_id) = 68),
  PRIMARY KEY (gateway_id, registration_id),
  FOREIGN KEY (gateway_id, registration_id)
    REFERENCES _chardb_live_subscriptions (gateway_id, registration_id)
    ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS _chardb_live_subscription_vectors_by_resource
  ON _chardb_live_subscription_vectors (resource_id, gateway_id, registration_id);
CREATE TABLE IF NOT EXISTS _chardb_invalidation_outbox (
  gateway_id TEXT NOT NULL,
  registration_id TEXT NOT NULL,
  change_seq INTEGER NOT NULL CHECK (change_seq > 0),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at INTEGER NOT NULL DEFAULT 0 CHECK (next_attempt_at >= 0),
  last_error TEXT,
  dead_lettered_at INTEGER,
  PRIMARY KEY (gateway_id, registration_id),
  FOREIGN KEY (gateway_id, registration_id)
    REFERENCES _chardb_live_subscriptions (gateway_id, registration_id)
    ON DELETE CASCADE
);
` as const;

export interface StoredSubscriptionRow {
    readonly [column: string]: string | number | null;
    readonly gateway_id: string;
    readonly registration_id: string;
    readonly connection_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly state: "active" | "retired";
    readonly payload_hash: string | null;
    readonly principal_id: string | null;
    readonly organization_id: string | null;
    readonly authority: string | null;
    readonly schema_epoch: number | null;
    readonly recovery_generation: number | null;
    readonly vshard: number | null;
    readonly domain_schema_epoch: number | null;
    readonly ref: string | null;
    readonly args_json: string | null;
    readonly policy_digest: string | null;
    readonly query_hash: string | null;
    readonly tables_json: string | null;
    readonly intervals_json: string | null;
}

export interface StoredInvalidationRow {
    readonly [column: string]: string | number | null;
    readonly gateway_id: string;
    readonly registration_id: string;
    readonly connection_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly change_seq: number;
    readonly attempts: number;
    readonly next_attempt_at: number;
    readonly last_error: string | null;
    readonly dead_lettered_at: number | null;
}

export const INVALIDATION_BATCH_SIZE = 64;
export const INVALIDATION_MAX_ATTEMPTS = 8;
export const INVALIDATION_BASE_RETRY_MS = 1_000;
export const INVALIDATION_MAX_RETRY_MS = 60_000;

const CDB_MAX_ACTIVE_LIVE_REGISTRATIONS = 4_096;
const CDB_MAX_LIVE_REGISTRATION_ROWS = 8_192;
const CDB_MAX_RETIRED_TOMBSTONE_BYTES = 16 * 1_024 * 1_024;
const CDB_MAX_SUBSCRIPTION_IDENTITY_FIELD_BYTES = 256;
const CDB_MAX_INVALIDATION_OUTBOX_ROWS = 4_096;
const CDB_MAX_INVALIDATIONS_PER_MUTATION = 4_096;
const VECTOR_RESOURCE_ID = /^vr1_[a-f0-9]{64}$/;

export function subscriptionInvariant(message: string): CdbError {
    return new CdbError({ code: "CDB_INVARIANT", message });
}

function subscriptionCapacityExceeded(subject: string, limit: number): CdbError {
    return new CdbError({
        code: "CDB_RATE_LIMITED",
        message: `Cdb ${subject} capacity is limited to ${limit}`,
        retryAfterMs: INVALIDATION_BASE_RETRY_MS,
        hint: "unsubscribe unused live queries or wait for queued invalidations to drain",
    });
}

export function assertLiveSubscriptionIdentity(subscription: LiveSubscriptionId): void {
    for (const [name, value] of [
        ["gatewayId", subscription.gatewayId],
        ["registrationId", subscription.registrationId],
        ["connectionId", subscription.connectionId],
        ["clientId", subscription.clientId],
    ] as const) {
        if (typeof value !== "string" || value.length === 0) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: `live subscription ${name} must be nonempty` });
        }
        if (new TextEncoder().encode(value).byteLength > CDB_MAX_SUBSCRIPTION_IDENTITY_FIELD_BYTES) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: `live subscription ${name} exceeds ${CDB_MAX_SUBSCRIPTION_IDENTITY_FIELD_BYTES} UTF-8 bytes`,
            });
        }
    }
    if (!Number.isSafeInteger(subscription.subId) || subscription.subId < 0) {
        throw new CdbError({ code: "CDB_INVALID_ARGS", message: "live subscription subId must be non-negative" });
    }
}

function subscriptionIdentityBytes(subscription: LiveSubscriptionId): number {
    const encoder = new TextEncoder();
    return (
        encoder.encode(subscription.gatewayId).byteLength +
        encoder.encode(subscription.registrationId).byteLength +
        encoder.encode(subscription.connectionId).byteLength +
        encoder.encode(subscription.clientId).byteLength +
        8
    );
}

function subscriptionPayloadHash(args: CdbSubscriptionRequest, policyDigest: string): string {
    return stableHashHex({
        connectionId: args.subscription.connectionId,
        clientId: args.subscription.clientId,
        subId: args.subscription.subId,
        principalId: args.principalId,
        organizationId: args.organizationId,
        ...(args.placement === undefined ? {} : { placement: args.placement }),
        schemaEpoch: args.schemaEpoch,
        recoveryGeneration: args.recoveryGeneration,
        vshard: args.vshard,
        domainSchemaEpoch: args.domainSchemaEpoch,
        ref: args.ref,
        args: args.args,
        policyDigest,
        queryHash: args.queryHash,
        tables: args.tables,
        intervals: args.intervals,
    });
}

export function durableRowCount(sql: SyncSql, query: string, subject: string): number {
    const row = sql.one<{ count: number | bigint }>(query);
    const count = Number(row?.count);
    if (!Number.isSafeInteger(count) || count < 0) {
        throw subscriptionInvariant(`Cdb ${subject} count is missing or invalid`);
    }
    return count;
}

function retiredTombstoneBytes(sql: SyncSql): number {
    return durableRowCount(
        sql,
        `SELECT COALESCE(SUM(
             length(CAST(gateway_id AS BLOB)) +
             length(CAST(registration_id AS BLOB)) +
             length(CAST(connection_id AS BLOB)) +
             length(CAST(client_id AS BLOB)) + 8
         ), 0) AS count
         FROM _chardb_live_subscriptions
         WHERE state = 'retired'`,
        "retired live subscription tombstone bytes"
    );
}

export function initializeLiveStore(sql: SyncSql): void {
    const subscriptionColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info(_chardb_live_subscriptions)").map(column => column.name)
    );
    const needsRecoveryGenerationBackfill = !subscriptionColumns.has("recovery_generation");
    if (!subscriptionColumns.has("organization_id")) {
        sql.exec("ALTER TABLE _chardb_live_subscriptions ADD COLUMN organization_id TEXT");
    }
    if (!subscriptionColumns.has("authority")) {
        sql.exec(
            "ALTER TABLE _chardb_live_subscriptions ADD COLUMN authority TEXT CHECK (authority IS NULL OR authority IN ('organization', 'user', 'global'))"
        );
    }
    if (!subscriptionColumns.has("query_hash")) {
        sql.exec("ALTER TABLE _chardb_live_subscriptions ADD COLUMN query_hash TEXT");
    }
    if (!subscriptionColumns.has("policy_digest")) {
        sql.exec("ALTER TABLE _chardb_live_subscriptions ADD COLUMN policy_digest TEXT");
    }
    if (!subscriptionColumns.has("domain_schema_epoch")) {
        sql.exec(
            "ALTER TABLE _chardb_live_subscriptions ADD COLUMN domain_schema_epoch INTEGER CHECK (domain_schema_epoch IS NULL OR domain_schema_epoch > 0)"
        );
    }
    if (!subscriptionColumns.has("schema_epoch")) {
        sql.exec(
            "ALTER TABLE _chardb_live_subscriptions ADD COLUMN schema_epoch INTEGER CHECK (schema_epoch IS NULL OR schema_epoch > 0)"
        );
    }
    if (!subscriptionColumns.has("recovery_generation")) {
        sql.exec(
            "ALTER TABLE _chardb_live_subscriptions ADD COLUMN recovery_generation INTEGER CHECK (recovery_generation IS NULL OR recovery_generation >= 0)"
        );
    }
    if (!subscriptionColumns.has("vshard")) {
        sql.exec(
            "ALTER TABLE _chardb_live_subscriptions ADD COLUMN vshard INTEGER CHECK (vshard IS NULL OR (vshard >= 0 AND vshard < 16384))"
        );
    }
    if (needsRecoveryGenerationBackfill) backfillLegacyRecoveryGeneration(sql);

    const outboxColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info(_chardb_invalidation_outbox)").map(column => column.name)
    );
    const additions = [
        ["attempts", "attempts INTEGER NOT NULL DEFAULT 0"],
        ["next_attempt_at", "next_attempt_at INTEGER NOT NULL DEFAULT 0"],
        ["last_error", "last_error TEXT"],
        ["dead_lettered_at", "dead_lettered_at INTEGER"],
    ] as const;
    for (const [name, definition] of additions) {
        if (!outboxColumns.has(name)) sql.exec(`ALTER TABLE _chardb_invalidation_outbox ADD COLUMN ${definition}`);
    }
}

export function retireLegacyLiveSubscriptions(sql: SyncSql): void {
    sql.exec(
        `UPDATE _chardb_live_subscriptions
         SET state = 'retired',
             payload_hash = NULL,
             principal_id = NULL,
             organization_id = NULL,
             authority = NULL,
             schema_epoch = NULL,
             recovery_generation = NULL,
             vshard = NULL,
             domain_schema_epoch = NULL,
             ref = NULL,
             args_json = NULL,
             policy_digest = NULL,
             query_hash = NULL,
             tables_json = NULL,
             intervals_json = NULL
         WHERE state = 'active'
           AND (organization_id IS NULL OR query_hash IS NULL OR policy_digest IS NULL
                OR schema_epoch IS NULL OR recovery_generation IS NULL
                OR vshard IS NULL OR domain_schema_epoch IS NULL)`
    );
    sql.exec(
        `DELETE FROM _chardb_live_subscription_tables
         WHERE EXISTS (
           SELECT 1 FROM _chardb_live_subscriptions AS subscriptions
           WHERE subscriptions.gateway_id = _chardb_live_subscription_tables.gateway_id
             AND subscriptions.registration_id = _chardb_live_subscription_tables.registration_id
             AND subscriptions.state = 'retired'
         )`
    );
    sql.exec(
        `DELETE FROM _chardb_live_subscription_vectors
         WHERE EXISTS (
           SELECT 1 FROM _chardb_live_subscriptions AS subscriptions
           WHERE subscriptions.gateway_id = _chardb_live_subscription_vectors.gateway_id
             AND subscriptions.registration_id = _chardb_live_subscription_vectors.registration_id
             AND subscriptions.state = 'retired'
         )`
    );
    sql.exec(
        `DELETE FROM _chardb_invalidation_outbox
         WHERE EXISTS (
           SELECT 1 FROM _chardb_live_subscriptions AS subscriptions
           WHERE subscriptions.gateway_id = _chardb_invalidation_outbox.gateway_id
             AND subscriptions.registration_id = _chardb_invalidation_outbox.registration_id
             AND subscriptions.state = 'retired'
         )`
    );
}

export function currentChangeSeq(sql: SyncSql): number {
    const row = sql.one<{ change_seq: number }>("SELECT change_seq FROM _chardb_change_clock WHERE singleton = 1");
    if (!row || !Number.isSafeInteger(row.change_seq) || row.change_seq < 0) {
        throw subscriptionInvariant("Cdb change clock is missing or invalid");
    }
    return row.change_seq;
}

function storedSubscriptionTables(sql: SyncSql, subscription: LiveSubscriptionId): readonly string[] {
    return sql
        .all<{ table_name: string }>(
            `SELECT table_name
             FROM _chardb_live_subscription_tables
             WHERE gateway_id = ? AND registration_id = ?
             ORDER BY table_name`,
            subscription.gatewayId,
            subscription.registrationId
        )
        .map(row => row.table_name);
}

export function assertSubscriptionTables(
    sql: SyncSql,
    subscription: LiveSubscriptionId,
    expectedTables: readonly string[]
): void {
    const storedTables = storedSubscriptionTables(sql, subscription);
    if (
        storedTables.length !== expectedTables.length ||
        storedTables.some((tableName, index) => tableName !== expectedTables[index])
    ) {
        throw subscriptionInvariant("active live subscription table mappings do not match its persisted payload");
    }
}

function projectStoredSubscription(row: StoredSubscriptionRow, verifyPayloadHash: boolean): CdbSubscriptionRequest {
    if (
        row.state !== "active" ||
        row.payload_hash === null ||
        row.principal_id === null ||
        row.organization_id === null ||
        row.schema_epoch === null ||
        row.recovery_generation === null ||
        row.vshard === null ||
        row.domain_schema_epoch === null ||
        row.ref === null ||
        row.args_json === null ||
        row.policy_digest === null ||
        row.query_hash === null ||
        row.tables_json === null ||
        row.intervals_json === null
    ) {
        throw subscriptionInvariant("active live subscription is missing its persisted payload");
    }

    let parsedArgs: unknown;
    let tables: unknown;
    let intervals: unknown;
    try {
        parsedArgs = JSON.parse(row.args_json);
        tables = JSON.parse(row.tables_json);
        intervals = JSON.parse(row.intervals_json);
    } catch (error) {
        throw subscriptionInvariant(
            `active live subscription payload is corrupt: ${error instanceof Error ? error.message : "invalid JSON"}`
        );
    }
    const args = snapshotCdbQueryArgs(parsedArgs as RawJson);
    if (!Number.isSafeInteger(row.domain_schema_epoch) || row.domain_schema_epoch < 1) {
        throw subscriptionInvariant("active live subscription domain schema epoch is invalid");
    }
    if (!Number.isSafeInteger(row.schema_epoch) || row.schema_epoch < 1) {
        throw subscriptionInvariant("active live subscription routing generation is invalid");
    }
    if (!Number.isSafeInteger(row.recovery_generation) || row.recovery_generation < 0) {
        throw subscriptionInvariant("active live subscription recovery generation is invalid");
    }
    if (!Number.isSafeInteger(row.vshard) || row.vshard < 0 || row.vshard >= 16_384) {
        throw subscriptionInvariant("active live subscription vshard is invalid");
    }
    if (!Array.isArray(tables) || tables.some(table => typeof table !== "string")) {
        throw subscriptionInvariant("active live subscription tables payload is corrupt");
    }
    if (!Array.isArray(intervals)) {
        throw subscriptionInvariant("active live subscription intervals payload is corrupt");
    }

    const request: CdbSubscriptionRequest = {
        subscription: {
            gatewayId: row.gateway_id,
            registrationId: row.registration_id,
            connectionId: row.connection_id,
            clientId: ClientId(row.client_id),
            subId: SubId(row.sub_id),
        },
        principalId: PrincipalId(row.principal_id),
        organizationId: TenantId(row.organization_id),
        ...(row.authority === "organization" || row.authority === "user" || row.authority === "global"
            ? { placement: { authority: row.authority, partitionKey: row.organization_id } }
            : {}),
        schemaEpoch: row.schema_epoch,
        recoveryGeneration: row.recovery_generation,
        vshard: row.vshard,
        domainSchemaEpoch: row.domain_schema_epoch,
        ref: ChardbRef(row.ref),
        args,
        queryHash: row.query_hash,
        tables,
        intervals: intervals as CdbSubscriptionRequest["intervals"],
    };
    if (verifyPayloadHash && row.payload_hash !== subscriptionPayloadHash(request, row.policy_digest)) {
        throw subscriptionInvariant("active live subscription payload hash does not match its persisted payload");
    }
    return request;
}

export function parseStoredSubscription(row: StoredSubscriptionRow): CdbSubscriptionRequest {
    return projectStoredSubscription(row, true);
}

function backfillLegacyRecoveryGeneration(sql: SyncSql): void {
    const rows = sql.all<StoredSubscriptionRow>(
        `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard,
                domain_schema_epoch, ref, args_json, policy_digest, query_hash, tables_json, intervals_json
         FROM _chardb_live_subscriptions
         WHERE state = 'active' AND recovery_generation IS NULL`
    );
    for (const row of rows) {
        if (row.policy_digest === null) continue;
        try {
            const request = projectStoredSubscription({ ...row, recovery_generation: 0 }, false);
            sql.exec(
                `UPDATE _chardb_live_subscriptions
                 SET recovery_generation = 0, payload_hash = ?
                 WHERE gateway_id = ? AND registration_id = ?
                   AND state = 'active' AND recovery_generation IS NULL`,
                subscriptionPayloadHash(request, row.policy_digest),
                row.gateway_id,
                row.registration_id
            );
        } catch {
            // The general legacy retirement pass below removes malformed prior rows.
        }
    }
}

export function parseStoredSubscriptionRouting(row: StoredSubscriptionRow): {
    readonly subscription: LiveSubscriptionId;
    readonly tables: readonly string[];
    readonly intervals: CdbSubscriptionRequest["intervals"];
} {
    if (row.state !== "active" || row.tables_json === null || row.intervals_json === null) {
        throw subscriptionInvariant("active live subscription is missing its persisted routing metadata");
    }
    let tables: unknown;
    let intervals: unknown;
    try {
        tables = JSON.parse(row.tables_json);
        intervals = JSON.parse(row.intervals_json);
    } catch (error) {
        throw subscriptionInvariant(
            `active live subscription routing metadata is corrupt: ${error instanceof Error ? error.message : "invalid JSON"}`
        );
    }
    if (!Array.isArray(tables) || tables.some(table => typeof table !== "string") || !Array.isArray(intervals)) {
        throw subscriptionInvariant("active live subscription routing metadata is corrupt");
    }
    return {
        subscription: {
            gatewayId: row.gateway_id,
            registrationId: row.registration_id,
            connectionId: row.connection_id,
            clientId: ClientId(row.client_id),
            subId: SubId(row.sub_id),
        },
        tables,
        intervals: intervals as CdbSubscriptionRequest["intervals"],
    };
}

export function persistLiveSubscription(
    sql: SyncSql,
    args: CdbSubscriptionRequest,
    policyDigest: string
): CdbSubscriptionResponse {
    const payloadHash = subscriptionPayloadHash(args, policyDigest);
    const tableNames = [...new Set(args.tables)].sort();
    const existing = sql.one<StoredSubscriptionRow>(
        `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                ref, args_json, policy_digest, query_hash,
                tables_json, intervals_json
         FROM _chardb_live_subscriptions
         WHERE gateway_id = ? AND registration_id = ?`,
        args.subscription.gatewayId,
        args.subscription.registrationId
    );
    if (existing) {
        if (!sameSubscriptionIdentity(existing, args.subscription)) {
            throw subscriptionInvariant("live subscription registration identity changed across an RPC replay");
        }
        if (existing.state === "retired") {
            throw subscriptionInvariant("retired live subscription registration cannot be reactivated");
        }
        if (existing.payload_hash !== payloadHash) {
            throw subscriptionInvariant("live subscription registration payload changed across an RPC replay");
        }
        assertSubscriptionTables(sql, args.subscription, tableNames);
    } else {
        const registrationRows = durableRowCount(
            sql,
            "SELECT COUNT(*) AS count FROM _chardb_live_subscriptions",
            "live registration row"
        );
        if (registrationRows >= CDB_MAX_LIVE_REGISTRATION_ROWS) {
            return {
                ok: false,
                registrationState: "absent",
                subscription: args.subscription,
                error: subscriptionCapacityExceeded("live registration row", CDB_MAX_LIVE_REGISTRATION_ROWS).toJSON(),
            };
        }
        const activeRegistrations = durableRowCount(
            sql,
            "SELECT COUNT(*) AS count FROM _chardb_live_subscriptions WHERE state = 'active'",
            "active live registration"
        );
        if (activeRegistrations >= CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
            return {
                ok: false,
                registrationState: "absent",
                subscription: args.subscription,
                error: subscriptionCapacityExceeded(
                    "active live registration",
                    CDB_MAX_ACTIVE_LIVE_REGISTRATIONS
                ).toJSON(),
            };
        }
        sql.exec(
            `INSERT INTO _chardb_live_subscriptions
             (gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
              principal_id, organization_id, authority, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
              ref, args_json, policy_digest, query_hash,
              tables_json, intervals_json)
             VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            args.subscription.gatewayId,
            args.subscription.registrationId,
            args.subscription.connectionId,
            args.subscription.clientId,
            args.subscription.subId,
            payloadHash,
            args.principalId,
            args.organizationId,
            args.placement?.authority ?? null,
            args.schemaEpoch,
            args.recoveryGeneration,
            args.vshard,
            args.domainSchemaEpoch,
            args.ref,
            JSON.stringify(args.args),
            policyDigest,
            args.queryHash,
            JSON.stringify(args.tables),
            JSON.stringify(args.intervals)
        );
        for (const tableName of tableNames) {
            sql.exec(
                `INSERT INTO _chardb_live_subscription_tables
                 (gateway_id, registration_id, table_name)
                 VALUES (?, ?, ?)`,
                args.subscription.gatewayId,
                args.subscription.registrationId,
                tableName
            );
        }
    }
    return { ok: true, subscription: args.subscription, changeSeq: currentChangeSeq(sql) };
}

function vectorResourceId(value: string): string {
    if (typeof value !== "string" || !VECTOR_RESOURCE_ID.test(value)) {
        throw subscriptionInvariant("live vector dependency resource id is invalid");
    }
    return value;
}

/** Persist the ordinary registration and one exact vector dependency inside the caller's transaction. */
export function persistLiveSubscriptionWithVectorDependency(
    sql: SyncSql,
    args: CdbSubscriptionRequest,
    policyDigest: string,
    resourceId: string
): CdbSubscriptionResponse {
    const canonical = vectorResourceId(resourceId);
    const response = persistLiveSubscription(sql, args, policyDigest);
    if (!response.ok) return response;
    const existing = sql.one<{ resource_id: string }>(
        `SELECT resource_id FROM _chardb_live_subscription_vectors
         WHERE gateway_id = ? AND registration_id = ?`,
        args.subscription.gatewayId,
        args.subscription.registrationId
    );
    if (existing) {
        if (existing.resource_id !== canonical) {
            throw subscriptionInvariant("live vector dependency changed across an RPC replay");
        }
        return response;
    }
    sql.exec(
        `INSERT INTO _chardb_live_subscription_vectors (gateway_id, registration_id, resource_id)
         VALUES (?, ?, ?)`,
        args.subscription.gatewayId,
        args.subscription.registrationId,
        canonical
    );
    if (sql.changes() !== 1) throw subscriptionInvariant("live vector dependency was not persisted");
    return response;
}

/** Require the persisted dependency to match the plan that will execute or refetch. */
export function assertLiveVectorSubscriptionDependency(
    sql: SyncSql,
    subscription: LiveSubscriptionId,
    resourceId: string | null
): void {
    const rows = sql.all<{ resource_id: string }>(
        `SELECT resource_id FROM _chardb_live_subscription_vectors
         WHERE gateway_id = ? AND registration_id = ?
         ORDER BY resource_id
         LIMIT 2`,
        subscription.gatewayId,
        subscription.registrationId
    );
    if (resourceId === null) {
        if (rows.length !== 0) throw subscriptionInvariant("ordinary registered query retained a vector dependency");
        return;
    }
    if (rows.length !== 1 || rows[0]?.resource_id !== vectorResourceId(resourceId)) {
        throw subscriptionInvariant("registered vector query dependency does not match its compiled plan");
    }
}

/** Reconstruct exact private vector dependencies and reject stale schema identities at boot. */
export function assertLiveVectorDependencies(sql: SyncSql, resourceIds: readonly string[]): void {
    const configured = new Set(resourceIds.map(vectorResourceId));
    const rows = sql.all<{
        gateway_id: string;
        registration_id: string;
        resource_id: string;
        state: "active" | "retired";
    }>(
        `SELECT mappings.gateway_id, mappings.registration_id, mappings.resource_id, subscriptions.state
         FROM _chardb_live_subscription_vectors AS mappings
         INNER JOIN _chardb_live_subscriptions AS subscriptions
           ON subscriptions.gateway_id = mappings.gateway_id
          AND subscriptions.registration_id = mappings.registration_id
         ORDER BY mappings.gateway_id, mappings.registration_id
         LIMIT ?`,
        CDB_MAX_ACTIVE_LIVE_REGISTRATIONS + 1
    );
    if (rows.length > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("live vector dependency count exceeds the active registration cap");
    }
    for (const row of rows) {
        const resourceId = vectorResourceId(row.resource_id);
        if (row.state !== "active") throw subscriptionInvariant("retired registration retained a vector dependency");
        if (!configured.has(resourceId)) {
            throw new CdbError({
                code: "CDB_PARTITION_CONTRACT_CHANGED",
                message: "live vector dependency does not match an active resource descriptor",
            });
        }
    }
}

export function retireLiveSubscription(sql: SyncSql, subscription: LiveSubscriptionId): void {
    const existing = sql.one<StoredSubscriptionRow>(
        `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                principal_id, organization_id, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                ref, args_json, policy_digest, query_hash,
                tables_json, intervals_json
         FROM _chardb_live_subscriptions
         WHERE gateway_id = ? AND registration_id = ?`,
        subscription.gatewayId,
        subscription.registrationId
    );
    if (existing && !sameSubscriptionIdentity(existing, subscription)) {
        throw subscriptionInvariant("live subscription unregister identity does not match its registration");
    }
    if (!existing || existing.state === "active") {
        const rows = durableRowCount(
            sql,
            "SELECT COUNT(*) AS count FROM _chardb_live_subscriptions",
            "live registration row"
        );
        if (!existing && rows >= CDB_MAX_LIVE_REGISTRATION_ROWS) {
            throw subscriptionCapacityExceeded("live registration row", CDB_MAX_LIVE_REGISTRATION_ROWS);
        }
        const projectedBytes = retiredTombstoneBytes(sql) + subscriptionIdentityBytes(subscription);
        if (projectedBytes > CDB_MAX_RETIRED_TOMBSTONE_BYTES) {
            throw subscriptionCapacityExceeded(
                "retired live subscription tombstone bytes",
                CDB_MAX_RETIRED_TOMBSTONE_BYTES
            );
        }
    }
    sql.exec(
        `INSERT INTO _chardb_live_subscriptions
         (gateway_id, registration_id, connection_id, client_id, sub_id, state)
         VALUES (?, ?, ?, ?, ?, 'retired')
         ON CONFLICT(gateway_id, registration_id) DO UPDATE SET
           state = 'retired',
           payload_hash = NULL,
           principal_id = NULL,
           organization_id = NULL,
           authority = NULL,
           schema_epoch = NULL,
           recovery_generation = NULL,
           vshard = NULL,
           domain_schema_epoch = NULL,
           ref = NULL,
           args_json = NULL,
           policy_digest = NULL,
           query_hash = NULL,
           tables_json = NULL,
           intervals_json = NULL`,
        subscription.gatewayId,
        subscription.registrationId,
        subscription.connectionId,
        subscription.clientId,
        subscription.subId
    );
    sql.exec(
        `DELETE FROM _chardb_live_subscription_tables
         WHERE gateway_id = ? AND registration_id = ?`,
        subscription.gatewayId,
        subscription.registrationId
    );
    sql.exec(
        `DELETE FROM _chardb_live_subscription_vectors
         WHERE gateway_id = ? AND registration_id = ?`,
        subscription.gatewayId,
        subscription.registrationId
    );
    sql.exec(
        `DELETE FROM _chardb_invalidation_outbox
         WHERE gateway_id = ? AND registration_id = ?`,
        subscription.gatewayId,
        subscription.registrationId
    );
}

export function finalizeRetiredLiveSubscription(sql: SyncSql, subscription: LiveSubscriptionId): void {
    const existing = sql.one<StoredSubscriptionRow>(
        `SELECT gateway_id, registration_id, connection_id, client_id, sub_id, state, payload_hash,
                principal_id, organization_id, schema_epoch, recovery_generation, vshard, domain_schema_epoch,
                ref, args_json, policy_digest, query_hash,
                tables_json, intervals_json
         FROM _chardb_live_subscriptions
         WHERE gateway_id = ? AND registration_id = ?`,
        subscription.gatewayId,
        subscription.registrationId
    );
    if (!existing) return;
    if (!sameSubscriptionIdentity(existing, subscription)) {
        throw subscriptionInvariant("live subscription finalize identity does not match its tombstone");
    }
    if (existing.state !== "retired") {
        throw subscriptionInvariant("active live subscription cannot be finalized");
    }
    sql.exec(
        `DELETE FROM _chardb_live_subscriptions
         WHERE gateway_id = ? AND registration_id = ? AND connection_id = ?
           AND client_id = ? AND sub_id = ? AND state = 'retired'`,
        subscription.gatewayId,
        subscription.registrationId,
        subscription.connectionId,
        subscription.clientId,
        subscription.subId
    );
    if (sql.changes() !== 1) throw subscriptionInvariant("live subscription tombstone changed before finalize");
}

export function enqueueInvalidations(sql: SyncSql, touchedTables: readonly string[]): number {
    const uniqueTables = [...new Set(touchedTables)];
    const registrations =
        uniqueTables.length === 0
            ? []
            : sql
                  .all<{ gateway_id: string; registration_id: string }>(
                      `SELECT DISTINCT mappings.gateway_id, mappings.registration_id
                       FROM _chardb_live_subscription_tables AS mappings
                       INNER JOIN _chardb_live_subscriptions AS subscriptions
                         ON subscriptions.gateway_id = mappings.gateway_id
                        AND subscriptions.registration_id = mappings.registration_id
                       WHERE mappings.table_name IN (${uniqueTables.map(() => "?").join(", ")})
                         AND subscriptions.state = 'active'
                       ORDER BY mappings.gateway_id, mappings.registration_id
                       LIMIT ?`,
                      ...uniqueTables,
                      CDB_MAX_INVALIDATIONS_PER_MUTATION + 1
                  )
                  .map(row => ({ gatewayId: row.gateway_id, registrationId: row.registration_id }));
    if (registrations.length > CDB_MAX_INVALIDATIONS_PER_MUTATION) {
        throw subscriptionCapacityExceeded("mutation invalidation fanout", CDB_MAX_INVALIDATIONS_PER_MUTATION);
    }
    return enqueueRegistrationInvalidations(sql, registrations, true);
}

/** Dirty every quiet registration restored from an older recovery generation. */
export function enqueueRecoveryGenerationInvalidations(sql: SyncSql, recoveryGeneration: number): number {
    if (!Number.isSafeInteger(recoveryGeneration) || recoveryGeneration < 1) {
        throw subscriptionInvariant("recovery invalidation generation is invalid");
    }
    const registrations = sql
        .all<{ gateway_id: string; registration_id: string }>(
            `SELECT gateway_id, registration_id
             FROM _chardb_live_subscriptions
             WHERE state = 'active' AND recovery_generation < ?
             ORDER BY gateway_id, registration_id
             LIMIT ?`,
            recoveryGeneration,
            CDB_MAX_ACTIVE_LIVE_REGISTRATIONS + 1
        )
        .map(row => ({ gatewayId: row.gateway_id, registrationId: row.registration_id }));
    if (registrations.length > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("active live registration count exceeds its fixed cap");
    }
    return enqueueRegistrationInvalidations(sql, registrations);
}

/** Promote one freshly reauthorized registration after its post-recovery refetch succeeds. */
export function promoteLiveSubscriptionRecoveryGeneration(
    sql: SyncSql,
    row: StoredSubscriptionRow,
    recoveryGeneration: number
): void {
    if (row.recovery_generation === null || row.policy_digest === null) {
        throw subscriptionInvariant("active live subscription recovery identity is missing");
    }
    if (row.recovery_generation > recoveryGeneration) {
        throw new CdbError({ code: "CDB_STALE_EPOCH", message: "registered query recovery generation regressed" });
    }
    if (row.recovery_generation === recoveryGeneration) return;
    const request = parseStoredSubscription(row);
    const promoted = { ...request, recoveryGeneration };
    sql.exec(
        `UPDATE _chardb_live_subscriptions
         SET recovery_generation = ?, payload_hash = ?
         WHERE gateway_id = ? AND registration_id = ? AND state = 'active'
           AND recovery_generation = ? AND payload_hash = ?`,
        recoveryGeneration,
        subscriptionPayloadHash(promoted, row.policy_digest),
        row.gateway_id,
        row.registration_id,
        row.recovery_generation,
        row.payload_hash
    );
    if (sql.changes() !== 1) throw subscriptionInvariant("registered query changed before recovery promotion");
}

/** Dirty only active registrations bound to one canonical vector resource. */
export function enqueueVectorResourceInvalidations(
    sql: SyncSql,
    resourceId: string
): { readonly registrations: number; readonly changeSeq: number } {
    const canonical = vectorResourceId(resourceId);
    const registrationRow = sql.one<{ count: number | bigint }>(
        `SELECT COUNT(*) AS count
         FROM _chardb_live_subscription_vectors AS mappings
         INNER JOIN _chardb_live_subscriptions AS subscriptions
           ON subscriptions.gateway_id = mappings.gateway_id
          AND subscriptions.registration_id = mappings.registration_id
         WHERE mappings.resource_id = ? AND subscriptions.state = 'active'`,
        canonical
    );
    const registrations = Number(registrationRow?.count);
    if (!Number.isSafeInteger(registrations) || registrations < 0) {
        throw subscriptionInvariant("live vector dependency count is missing or invalid");
    }
    if (registrations > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("live vector dependency count exceeds the active registration cap");
    }
    if (registrations === 0) {
        return { registrations: 0, changeSeq: currentChangeSeq(sql) };
    }
    const missingRow = sql.one<{ count: number | bigint }>(
        `SELECT COUNT(*) AS count
         FROM _chardb_live_subscription_vectors AS mappings
         INNER JOIN _chardb_live_subscriptions AS subscriptions
           ON subscriptions.gateway_id = mappings.gateway_id
          AND subscriptions.registration_id = mappings.registration_id
         LEFT JOIN _chardb_invalidation_outbox AS outbox
           ON outbox.gateway_id = mappings.gateway_id
          AND outbox.registration_id = mappings.registration_id
         WHERE mappings.resource_id = ? AND subscriptions.state = 'active'
           AND outbox.gateway_id IS NULL`,
        canonical
    );
    const missing = Number(missingRow?.count);
    if (!Number.isSafeInteger(missing) || missing < 0 || missing > registrations) {
        throw subscriptionInvariant("live vector invalidation accounting is missing or invalid");
    }
    if (missing > 0) {
        const existing = durableRowCount(
            sql,
            "SELECT COUNT(*) AS count FROM _chardb_invalidation_outbox",
            "invalidation outbox"
        );
        if (existing + missing > CDB_MAX_INVALIDATION_OUTBOX_ROWS) {
            throw subscriptionCapacityExceeded("invalidation outbox", CDB_MAX_INVALIDATION_OUTBOX_ROWS);
        }
    }
    sql.exec("UPDATE _chardb_change_clock SET change_seq = change_seq + 1 WHERE singleton = 1");
    if (sql.changes() !== 1) throw subscriptionInvariant("Cdb change clock update did not affect one row");
    const changeSeq = currentChangeSeq(sql);
    sql.exec(
        `INSERT INTO _chardb_invalidation_outbox (gateway_id, registration_id, change_seq)
         SELECT mappings.gateway_id, mappings.registration_id, ?
         FROM _chardb_live_subscription_vectors AS mappings
         INNER JOIN _chardb_live_subscriptions AS subscriptions
           ON subscriptions.gateway_id = mappings.gateway_id
          AND subscriptions.registration_id = mappings.registration_id
         WHERE mappings.resource_id = ? AND subscriptions.state = 'active'
         ON CONFLICT(gateway_id, registration_id) DO UPDATE SET
           change_seq = MAX(change_seq, excluded.change_seq),
           attempts = CASE WHEN excluded.change_seq > change_seq THEN 0 ELSE attempts END,
           next_attempt_at = CASE WHEN excluded.change_seq > change_seq THEN 0 ELSE next_attempt_at END,
           last_error = CASE WHEN excluded.change_seq > change_seq THEN NULL ELSE last_error END,
           dead_lettered_at = CASE WHEN excluded.change_seq > change_seq THEN NULL ELSE dead_lettered_at END`,
        changeSeq,
        canonical
    );
    if (sql.changes() !== registrations) {
        throw subscriptionInvariant("live vector invalidation fanout changed before enqueue");
    }
    return {
        registrations,
        changeSeq,
    };
}

/** Wake every active subscription owned by a vshard range after source cutover. */
export function enqueueRoutingFenceInvalidations(sql: SyncSql, rangeLo: number, rangeHi: number): number {
    const registrations = sql
        .all<{ gateway_id: string; registration_id: string }>(
            `SELECT gateway_id, registration_id
             FROM _chardb_live_subscriptions
             WHERE state = 'active' AND vshard >= ? AND vshard <= ?
             ORDER BY gateway_id, registration_id
             LIMIT ?`,
            rangeLo,
            rangeHi,
            CDB_MAX_ACTIVE_LIVE_REGISTRATIONS + 1
        )
        .map(row => ({ gatewayId: row.gateway_id, registrationId: row.registration_id }));
    if (registrations.length > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("active live registration count exceeds its fixed cap");
    }
    return enqueueRegistrationInvalidations(sql, registrations);
}

/** Wake every active subscription after a domain schema epoch activates. */
export function enqueueSchemaMigrationInvalidations(sql: SyncSql): {
    readonly registrations: number;
    readonly changeSeq: number;
} {
    const registrations = sql
        .all<{ gateway_id: string; registration_id: string }>(
            `SELECT gateway_id, registration_id
             FROM _chardb_live_subscriptions
             WHERE state = 'active'
             ORDER BY gateway_id, registration_id
             LIMIT ?`,
            CDB_MAX_ACTIVE_LIVE_REGISTRATIONS + 1
        )
        .map(row => ({ gatewayId: row.gateway_id, registrationId: row.registration_id }));
    if (registrations.length > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("active live registration count exceeds its fixed cap");
    }
    if (registrations.length === 0) {
        return { registrations: 0, changeSeq: currentChangeSeq(sql) };
    }
    return {
        registrations: registrations.length,
        changeSeq: enqueueRegistrationInvalidations(sql, registrations),
    };
}

function enqueueRegistrationInvalidations(
    sql: SyncSql,
    registrations: readonly { readonly gatewayId: string; readonly registrationId: string }[],
    advanceWhenEmpty = false
): number {
    if (registrations.length === 0 && !advanceWhenEmpty) return currentChangeSeq(sql);
    let missingOutboxRows = 0;
    for (const registration of registrations) {
        const existing = sql.one<{ present: number }>(
            `SELECT 1 AS present FROM _chardb_invalidation_outbox
             WHERE gateway_id = ? AND registration_id = ?`,
            registration.gatewayId,
            registration.registrationId
        );
        if (!existing) missingOutboxRows++;
    }
    // Updating an already queued registration is the normal hot path. Count
    // the whole bounded outbox only when this mutation would add rows.
    if (missingOutboxRows > 0) {
        const existingOutboxRows = durableRowCount(
            sql,
            "SELECT COUNT(*) AS count FROM _chardb_invalidation_outbox",
            "invalidation outbox"
        );
        if (existingOutboxRows + missingOutboxRows > CDB_MAX_INVALIDATION_OUTBOX_ROWS) {
            throw subscriptionCapacityExceeded("invalidation outbox", CDB_MAX_INVALIDATION_OUTBOX_ROWS);
        }
    }
    sql.exec("UPDATE _chardb_change_clock SET change_seq = change_seq + 1 WHERE singleton = 1");
    if (sql.changes() !== 1) throw subscriptionInvariant("Cdb change clock update did not affect one row");
    const changeSeq = currentChangeSeq(sql);
    for (const registration of registrations) {
        sql.exec(
            `INSERT INTO _chardb_invalidation_outbox (gateway_id, registration_id, change_seq)
             VALUES (?, ?, ?)
             ON CONFLICT(gateway_id, registration_id) DO UPDATE SET
               change_seq = MAX(change_seq, excluded.change_seq),
               attempts = CASE WHEN excluded.change_seq > change_seq THEN 0 ELSE attempts END,
               next_attempt_at = CASE WHEN excluded.change_seq > change_seq THEN 0 ELSE next_attempt_at END,
               last_error = CASE WHEN excluded.change_seq > change_seq THEN NULL ELSE last_error END,
               dead_lettered_at = CASE WHEN excluded.change_seq > change_seq THEN NULL ELSE dead_lettered_at END`,
            registration.gatewayId,
            registration.registrationId,
            changeSeq
        );
    }
    return changeSeq;
}

/** Dirty only active registrations whose persisted authority identity matches one auth epoch scope. */
export function enqueueAuthScopeInvalidations(
    sql: SyncSql,
    scope: "global" | "tenant" | "principal",
    scopeId: string
): { readonly registrations: number; readonly changeSeq: number } {
    const predicate =
        scope === "global"
            ? { sql: "1 = 1", bindings: [] as readonly string[] }
            : scope === "tenant"
              ? { sql: "organization_id = ?", bindings: [scopeId] }
              : { sql: "principal_id = ?", bindings: [scopeId] };
    const registrations = sql
        .all<{ gateway_id: string; registration_id: string }>(
            `SELECT gateway_id, registration_id
             FROM _chardb_live_subscriptions
             WHERE state = 'active' AND ${predicate.sql}
             ORDER BY gateway_id, registration_id
             LIMIT ?`,
            ...predicate.bindings,
            CDB_MAX_ACTIVE_LIVE_REGISTRATIONS + 1
        )
        .map(row => ({ gatewayId: row.gateway_id, registrationId: row.registration_id }));
    if (registrations.length > CDB_MAX_ACTIVE_LIVE_REGISTRATIONS) {
        throw subscriptionInvariant("active live registration count exceeds its fixed cap");
    }
    if (registrations.length === 0) {
        return { registrations: 0, changeSeq: currentChangeSeq(sql) };
    }
    return {
        registrations: registrations.length,
        changeSeq: enqueueRegistrationInvalidations(sql, registrations),
    };
}

export function dueInvalidations(sql: SyncSql, nowMs: number): readonly StoredInvalidationRow[] {
    return sql.all<StoredInvalidationRow>(
        `SELECT outbox.gateway_id, outbox.registration_id, subscriptions.connection_id,
                subscriptions.client_id, subscriptions.sub_id, outbox.change_seq, outbox.attempts,
                outbox.next_attempt_at, outbox.last_error, outbox.dead_lettered_at
         FROM _chardb_invalidation_outbox AS outbox
         INNER JOIN _chardb_live_subscriptions AS subscriptions
           ON subscriptions.gateway_id = outbox.gateway_id
          AND subscriptions.registration_id = outbox.registration_id
         WHERE outbox.next_attempt_at <= ?
         ORDER BY outbox.next_attempt_at, outbox.gateway_id, outbox.registration_id
         LIMIT ?`,
        nowMs,
        INVALIDATION_BATCH_SIZE
    );
}

export function recordInvalidationFailures(
    sql: SyncSql,
    rows: readonly StoredInvalidationRow[],
    nowMs: number,
    retryDelayMs: (attempts: number) => number,
    error: unknown
): void {
    const message = invalidationErrorMessage(error);
    for (const row of rows) {
        const attempts = row.attempts + 1;
        const deadLetteredAt = row.dead_lettered_at ?? (attempts >= INVALIDATION_MAX_ATTEMPTS ? nowMs : null);
        const nextAttemptAt = nowMs + retryDelayMs(attempts);
        sql.exec(
            `UPDATE _chardb_invalidation_outbox
             SET attempts = ?, next_attempt_at = ?, last_error = ?, dead_lettered_at = ?
             WHERE gateway_id = ? AND registration_id = ? AND change_seq = ?`,
            attempts,
            nextAttemptAt,
            message,
            deadLetteredAt,
            row.gateway_id,
            row.registration_id,
            row.change_seq
        );
    }
}

export function acknowledgeInvalidations(
    sql: SyncSql,
    gatewayId: string,
    acknowledgements: readonly GatewayInvalidationAck[]
): void {
    for (const acknowledgement of acknowledgements) {
        sql.exec(
            `DELETE FROM _chardb_invalidation_outbox
             WHERE gateway_id = ? AND registration_id = ? AND change_seq = ?`,
            gatewayId,
            acknowledgement.registrationId,
            acknowledgement.changeSeq
        );
    }
}

export function nextInvalidationAlarmAt(sql: SyncSql): number | null {
    const row = sql.one<{ next_attempt_at: number | null }>(
        `SELECT MIN(next_attempt_at) AS next_attempt_at
         FROM _chardb_invalidation_outbox`
    );
    return row?.next_attempt_at ?? null;
}

export function sameSubscriptionIdentity(row: StoredSubscriptionRow, subscription: LiveSubscriptionId): boolean {
    return (
        row.gateway_id === subscription.gatewayId &&
        row.registration_id === subscription.registrationId &&
        row.connection_id === subscription.connectionId &&
        row.client_id === subscription.clientId &&
        row.sub_id === subscription.subId
    );
}

function invalidationErrorMessage(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error);
    return message.slice(0, 512);
}
