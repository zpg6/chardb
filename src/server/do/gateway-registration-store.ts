/**
 * Durable Gateway registration and snapshot persistence.
 *
 * This module owns the SQLite schema, stored row shapes, payload accounting,
 * and synchronous registration state transitions. Network and Durable Object
 * orchestration stay in gateway.ts.
 */

import { CdbError } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { ChardbRef, ClientId, Cookie, PrincipalId, type RawJson, SubId, TenantId } from "../../types.ts";
import { stableJson } from "../../util/canonical.ts";
import { rawJsonResult } from "../../util/raw_json.ts";
import type { CdbIntent } from "../../wire.ts";
import { snapshotCdbQueryArgs } from "../result_limits.ts";

export const GATEWAY_REGISTRATION_DDL = `
CREATE TABLE IF NOT EXISTS _gw_registration_generations (
  registration_id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  sub_id INTEGER NOT NULL CHECK (sub_id >= 0),
  connection_id TEXT NOT NULL,
  organization_id TEXT NOT NULL,
  ref TEXT NOT NULL,
  args_json TEXT NOT NULL,
  intent_json TEXT NOT NULL,
  policy_digest TEXT NOT NULL,
  query_hash TEXT NOT NULL,
  shard_id TEXT NOT NULL,
  source_cdb_id TEXT NOT NULL,
  schema_epoch INTEGER NOT NULL CHECK (schema_epoch >= 0),
  recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0),
  domain_schema_epoch INTEGER NOT NULL CHECK (domain_schema_epoch > 0),
  auth_global_epoch INTEGER NOT NULL CHECK (auth_global_epoch >= 0),
  auth_tenant_epoch INTEGER NOT NULL CHECK (auth_tenant_epoch >= 0),
  auth_principal_epoch INTEGER NOT NULL CHECK (auth_principal_epoch >= 0),
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('installing', 'active', 'retiring')),
  cdb_state TEXT NOT NULL CHECK (cdb_state IN ('pending', 'active', 'retiring', 'error')),
  dirty_version INTEGER NOT NULL CHECK (dirty_version >= 0),
  delivered_version INTEGER NOT NULL CHECK (delivered_version >= 0 AND delivered_version <= dirty_version),
  initial_snapshot_pending INTEGER NOT NULL DEFAULT 0 CHECK (initial_snapshot_pending IN (0, 1)),
  run_token TEXT,
  run_target_version INTEGER CHECK (run_target_version IS NULL OR (run_target_version >= 0 AND run_target_version <= dirty_version)),
  run_lease_expires_at INTEGER CHECK (run_lease_expires_at IS NULL OR run_lease_expires_at >= 0),
  run_version INTEGER NOT NULL CHECK (run_version >= 0),
  last_cookie TEXT,
  last_snapshot_cookie TEXT,
  retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
  retry_at INTEGER CHECK (retry_at IS NULL OR retry_at >= 0),
  retry_error TEXT,
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);
CREATE INDEX IF NOT EXISTS _gw_registration_generations_logical
  ON _gw_registration_generations (principal_id, client_id, sub_id, created_at);
CREATE TABLE IF NOT EXISTS _gw_registration_heads (
  principal_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  sub_id INTEGER NOT NULL CHECK (sub_id >= 0),
  registration_id TEXT NOT NULL,
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
  PRIMARY KEY (principal_id, client_id, sub_id),
  UNIQUE (registration_id),
  FOREIGN KEY (registration_id) REFERENCES _gw_registration_generations(registration_id)
);
CREATE TABLE IF NOT EXISTS _gw_maintenance_state (
  key TEXT PRIMARY KEY,
  integer_value INTEGER NOT NULL CHECK (integer_value >= 0)
);
CREATE TABLE IF NOT EXISTS _gw_snapshot_outbox (
  registration_id TEXT PRIMARY KEY,
  cookie TEXT NOT NULL UNIQUE,
  target_version INTEGER NOT NULL CHECK (target_version >= 0),
  rows_json TEXT NOT NULL,
  byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
  send_attempts INTEGER NOT NULL DEFAULT 0 CHECK (send_attempts >= 0),
  next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
  claim_token TEXT,
  claim_version INTEGER NOT NULL DEFAULT 0 CHECK (claim_version >= 0),
  claim_expires_at INTEGER CHECK (claim_expires_at IS NULL OR claim_expires_at >= 0),
  attachment_base_cookie TEXT,
  last_sent_at INTEGER CHECK (last_sent_at IS NULL OR last_sent_at >= 0),
  last_error TEXT,
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  FOREIGN KEY (registration_id) REFERENCES _gw_registration_generations(registration_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS _gw_snapshot_outbox_due
  ON _gw_snapshot_outbox (next_attempt_at, registration_id);
CREATE TABLE IF NOT EXISTS _gw_snapshot_replay (
  principal_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  sub_id INTEGER NOT NULL CHECK (sub_id >= 0),
  cookie TEXT NOT NULL UNIQUE,
  organization_id TEXT NOT NULL,
  ref TEXT NOT NULL,
  args_json TEXT NOT NULL,
  policy_digest TEXT NOT NULL,
  query_hash TEXT NOT NULL,
  shard_id TEXT NOT NULL,
  source_cdb_id TEXT NOT NULL,
  schema_epoch INTEGER NOT NULL CHECK (schema_epoch >= 0),
  recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0),
  domain_schema_epoch INTEGER NOT NULL CHECK (domain_schema_epoch > 0),
  auth_global_epoch INTEGER NOT NULL CHECK (auth_global_epoch >= 0),
  auth_tenant_epoch INTEGER NOT NULL CHECK (auth_tenant_epoch >= 0),
  auth_principal_epoch INTEGER NOT NULL CHECK (auth_principal_epoch >= 0),
  rows_json TEXT NOT NULL,
  byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
  created_at INTEGER NOT NULL CHECK (created_at >= 0),
  expires_at INTEGER NOT NULL CHECK (expires_at > created_at),
  PRIMARY KEY (principal_id, client_id, sub_id)
);
CREATE INDEX IF NOT EXISTS _gw_snapshot_replay_expiry
  ON _gw_snapshot_replay (expires_at, created_at, principal_id, client_id, sub_id);
` as const;

export const GATEWAY_CLEANUP_BATCH_SIZE = 32 as const;
export const GATEWAY_ABANDONED_REGISTRATION_BATCH_SIZE = 32 as const;
export const GATEWAY_CLEANUP_BASE_RETRY_MS = 1_000 as const;
export const GATEWAY_CLEANUP_MAX_RETRY_MS = 60_000 as const;
export const GATEWAY_CLEANUP_MAX_RETRY_COUNT = 30 as const;
export const GATEWAY_AUTH_REFRESH_PENDING_ERROR = "authentication refresh is in flight" as const;
export const GATEWAY_CLEANUP_MAX_ERROR_LENGTH = 512 as const;
export const GATEWAY_QUERY_BATCH_SIZE = 16 as const;
export const GATEWAY_SEND_BATCH_SIZE = 32 as const;
export const GATEWAY_QUERY_LEASE_MS = 30_000 as const;
export const GATEWAY_SEND_LEASE_MS = 10_000 as const;
export const GATEWAY_SUBSCRIBE_RECOVERY_MS = 30_000 as const;
export const GATEWAY_SNAPSHOT_REPLAY_RETENTION_MS = 30_000 as const;
export const GATEWAY_MAX_SNAPSHOT_REPLAY_ROWS = 256 as const;
export const GATEWAY_MAX_DURABLE_PAYLOAD_BYTES = 16 * 1024 * 1024;
export const GATEWAY_MAX_REGISTRATION_PAYLOAD_BYTES = 15 * 1024 * 1024;

const GATEWAY_GENERATION_PAYLOAD_COLUMNS = [
    "registration_id",
    "principal_id",
    "client_id",
    "connection_id",
    "organization_id",
    "ref",
    "args_json",
    "intent_json",
    "policy_digest",
    "query_hash",
    "shard_id",
    "source_cdb_id",
    "lifecycle",
    "cdb_state",
    "run_token",
    "last_cookie",
    "last_snapshot_cookie",
    "retry_error",
] as const;

const GATEWAY_SNAPSHOT_PAYLOAD_COLUMNS = [
    "registration_id",
    "cookie",
    "rows_json",
    "claim_token",
    "attachment_base_cookie",
    "last_error",
] as const;

const GATEWAY_REPLAY_PAYLOAD_COLUMNS = [
    "principal_id",
    "client_id",
    "cookie",
    "organization_id",
    "ref",
    "args_json",
    "policy_digest",
    "query_hash",
    "shard_id",
    "source_cdb_id",
    "rows_json",
] as const;

const GATEWAY_UUID_TOKEN_BYTES = 36;
// Retry text is sliced to 512 UTF-16 code units. Four bytes per unit is a
// conservative UTF-8 bound, including malformed surrogate input.
const GATEWAY_MAX_RETRY_ERROR_BYTES = GATEWAY_CLEANUP_MAX_ERROR_LENGTH * 4;
// A generated snapshot cookie contains a routed client id (at most 256
// UTF-16 code units), a safe integer, separators, and one UUID.
const GATEWAY_MAX_GENERATED_SNAPSHOT_COOKIE_BYTES = 1_080;

function gatewayPayloadByteExpression(sql: SyncSql, table: string, columns: readonly string[]): string {
    const available = new Set(sql.all<{ name: string }>(`PRAGMA table_info('${table}')`).map(column => column.name));
    const expressions = columns
        .filter(column => available.has(column))
        .map(column => `length(CAST(COALESCE(${column}, '') AS BLOB))`);
    return expressions.length === 0 ? "0" : expressions.join(" + ");
}

const GATEWAY_RETIRED_PAYLOAD_ASSIGNMENTS = `
    ref = '', args_json = 'null', intent_json = 'null',
    policy_digest = '', query_hash = '', shard_id = '',
    last_cookie = NULL, last_snapshot_cookie = NULL`;

export interface GatewayDurablePayloadUsage {
    readonly registrationBytes: number;
    readonly snapshotBytes: number;
    readonly replayBytes: number;
    readonly totalBytes: number;
    readonly registrationReservedBytes: number;
    readonly snapshotReservedBytes: number;
    readonly chargedRegistrationBytes: number;
    readonly chargedTotalBytes: number;
}

/** Derive stored payload usage from SQLite values, never advisory byte_size columns. */
export function gatewayDurablePayloadUsage(sql: SyncSql): GatewayDurablePayloadUsage {
    const generationExpression = gatewayPayloadByteExpression(
        sql,
        "_gw_registration_generations",
        GATEWAY_GENERATION_PAYLOAD_COLUMNS
    );
    const snapshotExpression = gatewayPayloadByteExpression(
        sql,
        "_gw_snapshot_outbox",
        GATEWAY_SNAPSHOT_PAYLOAD_COLUMNS
    );
    const replayExpression = gatewayPayloadByteExpression(sql, "_gw_snapshot_replay", GATEWAY_REPLAY_PAYLOAD_COLUMNS);
    const generationColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info('_gw_registration_generations')").map(column => column.name)
    );
    const snapshotColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info('_gw_snapshot_outbox')").map(column => column.name)
    );
    const storedBytes = (columns: ReadonlySet<string>, column: string, prefix = ""): string =>
        columns.has(column) ? `length(CAST(COALESCE(${prefix}${column}, '') AS BLOB))` : "0";
    const futureSnapshotCookieBytes = `MAX(
        ${GATEWAY_MAX_GENERATED_SNAPSHOT_COOKIE_BYTES},
        length(CAST(COALESCE(o.cookie, '') AS BLOB))
    )`;
    const generationRetryReservationExpression = `MAX(
        0,
        ${GATEWAY_MAX_RETRY_ERROR_BYTES} - ${storedBytes(generationColumns, "retry_error", "g.")}
    )`;
    const currentHeadReservationExpression = [
        [`${GATEWAY_UUID_TOKEN_BYTES}`, storedBytes(generationColumns, "run_token", "g.")],
        [futureSnapshotCookieBytes, storedBytes(generationColumns, "last_cookie", "g.")],
        [futureSnapshotCookieBytes, storedBytes(generationColumns, "last_snapshot_cookie", "g.")],
    ]
        .map(([limit, actual]) => `MAX(0, ${limit} - ${actual})`)
        .join(" + ");
    const snapshotReservationExpression = [
        [GATEWAY_UUID_TOKEN_BYTES, storedBytes(snapshotColumns, "claim_token")],
        [GATEWAY_MAX_RETRY_ERROR_BYTES, storedBytes(snapshotColumns, "last_error")],
    ]
        .map(([limit, actual]) => `MAX(0, ${limit} - ${actual})`)
        .join(" + ");
    const row = sql.one<{
        registration_bytes: number | bigint;
        snapshot_bytes: number | bigint;
        replay_bytes: number | bigint;
        registration_reserved_bytes: number | bigint;
        snapshot_reserved_bytes: number | bigint;
    }>(
        `SELECT
           COALESCE((SELECT SUM(${generationExpression})
                     FROM _gw_registration_generations), 0) AS registration_bytes,
           COALESCE((SELECT SUM(${snapshotExpression})
                     FROM _gw_snapshot_outbox), 0) AS snapshot_bytes,
           COALESCE((SELECT SUM(${replayExpression})
                     FROM _gw_snapshot_replay), 0) AS replay_bytes,
           (COALESCE((SELECT SUM(${generationRetryReservationExpression})
                      FROM _gw_registration_generations g), 0)
            + COALESCE((SELECT SUM(${currentHeadReservationExpression})
                     FROM _gw_registration_generations g
                     INNER JOIN _gw_registration_heads h ON h.registration_id = g.registration_id
                     LEFT JOIN _gw_snapshot_outbox o ON o.registration_id = g.registration_id), 0))
                     AS registration_reserved_bytes,
           COALESCE((SELECT SUM(${snapshotReservationExpression})
                     FROM _gw_snapshot_outbox), 0) AS snapshot_reserved_bytes`
    );
    const registrationBytes = Number(row?.registration_bytes ?? 0);
    const snapshotBytes = Number(row?.snapshot_bytes ?? 0);
    const replayBytes = Number(row?.replay_bytes ?? 0);
    const registrationReservedBytes = Number(row?.registration_reserved_bytes ?? 0);
    const snapshotReservedBytes = Number(row?.snapshot_reserved_bytes ?? 0);
    if (
        !Number.isSafeInteger(registrationBytes) ||
        registrationBytes < 0 ||
        !Number.isSafeInteger(snapshotBytes) ||
        snapshotBytes < 0 ||
        !Number.isSafeInteger(replayBytes) ||
        replayBytes < 0 ||
        !Number.isSafeInteger(registrationReservedBytes) ||
        registrationReservedBytes < 0 ||
        !Number.isSafeInteger(snapshotReservedBytes) ||
        snapshotReservedBytes < 0
    ) {
        throw gatewayInvalidationInvariant("Gateway durable payload usage is invalid");
    }
    const totalBytes = registrationBytes + snapshotBytes + replayBytes;
    const chargedRegistrationBytes = registrationBytes + registrationReservedBytes;
    const chargedTotalBytes = totalBytes + registrationReservedBytes + snapshotReservedBytes;
    if (
        !Number.isSafeInteger(totalBytes) ||
        !Number.isSafeInteger(chargedRegistrationBytes) ||
        !Number.isSafeInteger(chargedTotalBytes)
    ) {
        throw gatewayInvalidationInvariant("Gateway durable payload usage overflowed");
    }
    return {
        registrationBytes,
        snapshotBytes,
        replayBytes,
        totalBytes,
        registrationReservedBytes,
        snapshotReservedBytes,
        chargedRegistrationBytes,
        chargedTotalBytes,
    };
}

function assertGatewayDurablePayloadQuota(sql: SyncSql): void {
    const usage = gatewayDurablePayloadUsage(sql);
    if (
        usage.chargedRegistrationBytes > GATEWAY_MAX_REGISTRATION_PAYLOAD_BYTES ||
        usage.chargedTotalBytes > GATEWAY_MAX_DURABLE_PAYLOAD_BYTES
    ) {
        throw new CdbError({
            code: "CDB_RATE_LIMITED",
            message: "Gateway durable subscription payload quota exceeded",
            retryAfterMs: GATEWAY_CLEANUP_BASE_RETRY_MS,
            hint: "Retire an existing live query or wait for snapshot delivery before retrying.",
        });
    }
}

export const GATEWAY_ABANDONED_REGISTRATION_CURSOR_KEY = "abandoned-registration-cursor" as const;

export type GatewayRegistrationLifecycle = "installing" | "active" | "retiring";
export type GatewayRegistrationCdbState = "pending" | "active" | "retiring" | "error";

export interface GatewayRegistrationKey {
    readonly principalId: PrincipalId;
    readonly clientId: ClientId;
    readonly subId: SubId;
}

export interface GatewayRegistrationInstall extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly organizationId: TenantId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    readonly intent: CdbIntent;
    readonly policyDigest: string;
    readonly queryHash: string;
    /** Catalog's logical shard identifier. */
    readonly shardId: string;
    /** Physical Cdb Durable Object identifier that emits invalidations. */
    readonly sourceCdbId: string;
    readonly schemaEpoch: number;
    readonly recoveryGeneration: number;
    readonly domainSchemaEpoch: number;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
    readonly lastCookie?: Cookie;
    readonly nowMs: number;
}

export interface GatewayRegistrationAdvance extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly expectedRunVersion: number;
    readonly lifecycle: GatewayRegistrationLifecycle;
    readonly cdbState: GatewayRegistrationCdbState;
    readonly dirtyVersion: number;
    readonly deliveredVersion: number;
    readonly lastCookie: Cookie | null;
    readonly retryCount: number;
    readonly retryAt: number | null;
    readonly retryError: string | null;
    readonly nowMs: number;
}

interface GatewaySubscriptionActivation extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly changeSeq: number;
    readonly nowMs: number;
}

export interface GatewayDirtyRunClaim extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly nowMs: number;
    readonly leaseExpiresAt: number;
}

export interface GatewayDirtyRun {
    readonly targetVersion: number;
    readonly runToken: string;
    readonly runVersion: number;
    readonly leaseExpiresAt: number;
    readonly reclaimed: boolean;
    readonly organizationId: TenantId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    readonly shardId: string;
    readonly sourceCdbId: string;
    readonly schemaEpoch: number;
    readonly recoveryGeneration: number;
    readonly domainSchemaEpoch: number;
    readonly intentJson: string;
    readonly policyDigest: string;
}

export interface GatewaySnapshotStage extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly runToken: string;
    readonly runVersion: number;
    readonly targetVersion: number;
    readonly cookie: Cookie;
    readonly rows: readonly RawJson[];
    readonly recoveryGeneration: number;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
    readonly nowMs: number;
}

export interface GatewaySnapshotReplayLookup extends GatewayRegistrationKey {
    readonly cookie: Cookie;
    readonly organizationId: TenantId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    readonly policyDigest: string;
    readonly queryHash: string;
    readonly shardId: string;
    readonly sourceCdbId: string;
    readonly schemaEpoch: number;
    readonly recoveryGeneration: number;
    readonly domainSchemaEpoch: number;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
    readonly nowMs: number;
}

export interface GatewaySnapshotReplay {
    readonly subId: SubId;
    readonly cookie: Cookie;
    readonly rows: readonly RawJson[];
}

export interface GatewaySnapshotSendClaim {
    readonly nowMs: number;
    readonly attemptExpiresAt: number;
    readonly connectionId?: string;
    readonly excludedConnectionIds?: readonly string[];
}

export interface GatewaySnapshotSendAttempt extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly cookie: Cookie;
    readonly targetVersion: number;
    readonly rows: readonly RawJson[];
    readonly byteSize: number;
    readonly sendAttempts: number;
    readonly nextAttemptAt: number;
    readonly claimToken: string;
    readonly claimVersion: number;
    readonly organizationId: TenantId;
    readonly ref: ChardbRef;
    readonly args: RawJson;
    readonly intentJson: string;
    readonly policyDigest: string;
    readonly queryHash: string;
    readonly shardId: string;
    readonly sourceCdbId: string;
    readonly schemaEpoch: number;
    readonly recoveryGeneration: number;
    readonly domainSchemaEpoch: number;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
}

export interface GatewayDirtyRunFailure extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly runToken: string;
    readonly runVersion: number;
    readonly nowMs: number;
    readonly retryNotBeforeMs?: number;
    readonly error: unknown;
}

export interface GatewayDirtyRunDeferral extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly nowMs: number;
    readonly retryAt: number;
}

export interface GatewayClaimedRunRetire extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly runToken: string;
    readonly runVersion: number;
    readonly nowMs: number;
}

export interface GatewayClaimedSnapshotRetire extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly cookie: Cookie;
    readonly claimToken: string;
    readonly claimVersion: number;
    readonly nowMs: number;
}

export interface GatewaySnapshotSendFailure {
    readonly registrationId: string;
    readonly cookie: Cookie;
    readonly claimToken: string;
    readonly claimVersion: number;
    readonly nowMs: number;
    readonly retryNotBeforeMs?: number;
    readonly error: unknown;
}

export interface GatewaySnapshotAcknowledge extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly cookie: Cookie;
    readonly nowMs: number;
}

export interface GatewaySnapshotAckLookup {
    readonly principalId: PrincipalId;
    readonly clientId: ClientId;
    readonly connectionId: string;
    readonly cookie: Cookie;
}

export interface GatewaySnapshotAckIdentity extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly cookie: Cookie;
    readonly alreadyAcknowledged: boolean;
    readonly attachmentBaseCookie: Cookie | null;
}

export interface GatewayCurrentRegistration extends GatewayRegistrationKey {
    readonly registrationId: string;
    readonly connectionId: string;
    /** Catalog's logical shard identifier. */
    readonly shardId: string;
    /** Physical Cdb Durable Object identifier used for unsubscribe cleanup. */
    readonly sourceCdbId: string | null;
}

export interface GatewayCurrentRegistrationRetire extends GatewayRegistrationKey {
    readonly connectionId: string;
    readonly nowMs: number;
}

export interface StoredGatewayCleanupRow {
    readonly principal_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly registration_id: string;
    readonly connection_id: string;
    readonly source_cdb_id: string | null;
    readonly organization_id: string;
    readonly recovery_generation: number;
    readonly retry_count: number;
}

export interface StoredGatewayActiveHead {
    readonly generation_rowid: number;
    readonly principal_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly registration_id: string;
    readonly connection_id: string;
}

export interface StoredGatewayRunCandidate {
    readonly principal_id: string;
    readonly client_id: string;
    readonly sub_id: number;
    readonly registration_id: string;
    readonly connection_id: string;
}

export function installGatewayRegistration(
    sql: SyncSql,
    input: GatewayRegistrationInstall
): { readonly supersededRegistrationId: string | null } {
    assertGatewayRegistrationInstall(input);
    const previous = sql.one<{ registration_id: string }>(
        `SELECT registration_id FROM _gw_registration_heads
         WHERE principal_id = ? AND client_id = ? AND sub_id = ?`,
        input.principalId,
        input.clientId,
        input.subId
    );
    if (previous) retainCurrentGatewaySnapshotReplay(sql, input, input.nowMs, true);
    sql.exec(
        `INSERT INTO _gw_registration_generations
         (registration_id, principal_id, client_id, sub_id, connection_id, organization_id,
          ref, args_json, intent_json, policy_digest, query_hash, shard_id, source_cdb_id, schema_epoch,
          recovery_generation, domain_schema_epoch,
          auth_global_epoch, auth_tenant_epoch, auth_principal_epoch,
          lifecycle, cdb_state, dirty_version, delivered_version, run_token, run_target_version,
          run_lease_expires_at, run_version,
          last_cookie, retry_count, retry_at, retry_error, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 'installing', 'pending', 0, 0, NULL, NULL, NULL, 0, ?, 0, NULL, NULL, ?, ?)`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.organizationId,
        input.ref,
        stableJson(input.args),
        stableJson(input.intent),
        input.policyDigest,
        input.queryHash,
        input.shardId,
        input.sourceCdbId,
        input.schemaEpoch,
        input.recoveryGeneration,
        input.domainSchemaEpoch,
        input.authEpochs.global,
        input.authEpochs.tenant,
        input.authEpochs.principal,
        input.lastCookie ?? null,
        input.nowMs,
        input.nowMs
    );
    if (previous) {
        sql.exec(
            `UPDATE _gw_registration_generations
             SET ${GATEWAY_RETIRED_PAYLOAD_ASSIGNMENTS},
                 lifecycle = 'retiring',
                 cdb_state = CASE WHEN cdb_state = 'pending' THEN 'pending' ELSE 'retiring' END,
                 run_token = NULL, run_target_version = NULL,
                 run_lease_expires_at = NULL,
                 run_version = run_version + 1, retry_count = 0,
                 retry_at = CASE WHEN cdb_state = 'pending' THEN retry_at ELSE ? END,
                 retry_error = NULL, updated_at = ?
             WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?`,
            input.nowMs,
            input.nowMs,
            previous.registration_id,
            input.principalId,
            input.clientId,
            input.subId
        );
        sql.exec("DELETE FROM _gw_snapshot_outbox WHERE registration_id = ?", previous.registration_id);
        pruneGatewaySnapshotReplays(sql, input.nowMs);
    }
    sql.exec(
        `INSERT INTO _gw_registration_heads
         (principal_id, client_id, sub_id, registration_id, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (principal_id, client_id, sub_id)
         DO UPDATE SET registration_id = excluded.registration_id, updated_at = excluded.updated_at`,
        input.principalId,
        input.clientId,
        input.subId,
        input.registrationId,
        input.nowMs
    );
    assertGatewayDurablePayloadQuota(sql);
    return { supersededRegistrationId: previous?.registration_id ?? null };
}

export function armGatewaySubscriptionRecovery(
    sql: SyncSql,
    input: GatewayRegistrationKey & {
        readonly registrationId: string;
        readonly connectionId: string;
        readonly recoveryAt: number;
        readonly nowMs: number;
    }
): boolean {
    assertGatewayRegistrationIdentity(input);
    assertNonnegativeSafeInteger(input.recoveryAt, "recoveryAt");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    if (input.recoveryAt <= input.nowMs) throw new TypeError("recoveryAt must be later than nowMs");
    sql.exec(
        `UPDATE _gw_registration_generations
         SET retry_at = ?, retry_error = 'subscription install recovery', updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'installing' AND cdb_state = 'pending'
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        input.recoveryAt,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return sql.changes() === 1;
}

export function activateGatewaySubscription(sql: SyncSql, input: GatewaySubscriptionActivation): boolean {
    assertGatewayRegistrationIdentity(input);
    assertNonnegativeSafeInteger(input.changeSeq, "changeSeq");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    sql.exec(
        `UPDATE _gw_registration_generations
         SET lifecycle = 'active', cdb_state = 'active', dirty_version = MAX(dirty_version, ?),
             initial_snapshot_pending = 1,
             retry_count = 0, retry_at = NULL, retry_error = NULL, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'installing' AND cdb_state = 'pending'
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        input.changeSeq,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return sql.changes() === 1;
}

export function deleteNeverRegisteredGatewaySubscription(
    sql: SyncSql,
    input: GatewayRegistrationKey & { readonly registrationId: string; readonly connectionId: string }
): boolean {
    assertGatewayRegistrationIdentity(input);
    const pending = sql.one<{ registration_id: string }>(
        `SELECT registration_id FROM _gw_registration_generations
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND cdb_state = 'pending'`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (!pending) return false;
    sql.exec(
        `DELETE FROM _gw_registration_heads
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND registration_id = ?`,
        input.principalId,
        input.clientId,
        input.subId,
        input.registrationId
    );
    sql.exec("DELETE FROM _gw_snapshot_outbox WHERE registration_id = ?", input.registrationId);
    sql.exec(
        `DELETE FROM _gw_registration_generations
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND cdb_state = 'pending'`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return sql.changes() === 1;
}

export function markPendingGatewaySubscriptionAmbiguous(
    sql: SyncSql,
    input: GatewayRegistrationKey & {
        readonly registrationId: string;
        readonly connectionId: string;
        readonly nowMs: number;
    }
): boolean {
    assertGatewayRegistrationIdentity(input);
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    const pending = sql.one<{ registration_id: string }>(
        `SELECT registration_id FROM _gw_registration_generations
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND cdb_state = 'pending'`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (!pending) return false;
    sql.exec(
        `DELETE FROM _gw_registration_heads
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND registration_id = ?`,
        input.principalId,
        input.clientId,
        input.subId,
        input.registrationId
    );
    sql.exec(
        `UPDATE _gw_registration_generations
         SET ${GATEWAY_RETIRED_PAYLOAD_ASSIGNMENTS},
             lifecycle = 'retiring', cdb_state = 'retiring',
             run_token = NULL, run_target_version = NULL, run_lease_expires_at = NULL,
             run_version = run_version + 1, retry_count = 0, retry_at = ?, retry_error = NULL, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND cdb_state = 'pending'`,
        input.nowMs,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (sql.changes() !== 1) return false;
    sql.exec("DELETE FROM _gw_snapshot_outbox WHERE registration_id = ?", input.registrationId);
    return true;
}

/** Advance state only when this registration still owns its logical head and run version. */
export function advanceGatewayRegistration(sql: SyncSql, input: GatewayRegistrationAdvance): boolean {
    assertNonnegativeSafeInteger(input.expectedRunVersion, "expectedRunVersion");
    assertNonnegativeSafeInteger(input.dirtyVersion, "dirtyVersion");
    assertNonnegativeSafeInteger(input.deliveredVersion, "deliveredVersion");
    assertNonnegativeSafeInteger(input.retryCount, "retryCount");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    if (input.retryAt !== null) assertNonnegativeSafeInteger(input.retryAt, "retryAt");
    if (input.deliveredVersion > input.dirtyVersion) {
        throw new TypeError("deliveredVersion cannot exceed dirtyVersion");
    }
    const retryError = input.retryError === null ? null : gatewayRetryError(input.retryError);
    sql.exec(
        `UPDATE _gw_registration_generations
         SET lifecycle = ?, cdb_state = ?, dirty_version = ?, delivered_version = ?,
             run_token = NULL, run_target_version = NULL, run_lease_expires_at = NULL,
             run_version = run_version + 1, last_cookie = ?,
             retry_count = ?, retry_at = ?, retry_error = ?, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND run_version = ?
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.principal_id = ? AND h.client_id = ? AND h.sub_id = ?
               AND h.registration_id = _gw_registration_generations.registration_id
           )`,
        input.lifecycle,
        input.cdbState,
        input.dirtyVersion,
        input.deliveredVersion,
        input.lastCookie,
        input.retryCount,
        input.retryAt,
        retryError,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.expectedRunVersion,
        input.principalId,
        input.clientId,
        input.subId
    );
    const advanced = sql.changes() === 1;
    if (advanced) assertGatewayDurablePayloadQuota(sql);
    return advanced;
}

/**
 * Claim an exact current dirty generation. An expired owner may be replaced,
 * but a staged snapshot blocks another query until its cookie is acknowledged.
 * The caller must wrap the select and update in one transaction.
 */
export function claimDirtyGatewayRegistration(sql: SyncSql, input: GatewayDirtyRunClaim): GatewayDirtyRun | null {
    assertGatewayRegistrationIdentity(input);
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    assertNonnegativeSafeInteger(input.leaseExpiresAt, "leaseExpiresAt");
    if (input.leaseExpiresAt <= input.nowMs) throw new TypeError("leaseExpiresAt must be later than nowMs");
    const current = sql.one<{
        dirty_version: number;
        delivered_version: number;
        run_token: string | null;
        run_version: number;
        organization_id: string;
        ref: string;
        args_json: string;
        shard_id: string;
        source_cdb_id: string;
        schema_epoch: number;
        recovery_generation: number;
        domain_schema_epoch: number;
        intent_json: string;
        policy_digest: string;
    }>(
        `SELECT g.dirty_version, g.delivered_version, g.run_token, g.run_version,
                g.organization_id, g.ref, g.args_json, g.shard_id, g.source_cdb_id, g.schema_epoch,
                g.recovery_generation,
                g.domain_schema_epoch,
                g.intent_json, g.policy_digest
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE g.registration_id = ? AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ?
           AND g.connection_id = ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           AND g.source_cdb_id IS NOT NULL AND g.source_cdb_id <> ''
           AND (g.initial_snapshot_pending = 1 OR g.dirty_version > g.delivered_version)
           AND (g.retry_at IS NULL OR g.retry_at <= ?)
           AND NOT EXISTS (
             SELECT 1 FROM _gw_snapshot_outbox o WHERE o.registration_id = g.registration_id
           )
           AND (
             (g.run_token IS NULL AND g.run_target_version IS NULL AND g.run_lease_expires_at IS NULL)
             OR
             (g.run_token IS NOT NULL AND g.run_target_version IS NOT NULL
              AND g.run_lease_expires_at IS NOT NULL AND g.run_lease_expires_at <= ?)
           )`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.nowMs,
        input.nowMs
    );
    if (!current) return null;

    const runToken = crypto.randomUUID();
    const runVersion = current.run_version + 1;
    const targetVersion = current.dirty_version;
    sql.exec(
        `UPDATE _gw_registration_generations
         SET run_token = ?, run_target_version = ?, run_lease_expires_at = ?,
             run_version = run_version + 1, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND dirty_version = ? AND delivered_version = ? AND run_version = ?
           AND (retry_at IS NULL OR retry_at <= ?)
           AND NOT EXISTS (
             SELECT 1 FROM _gw_snapshot_outbox o
             WHERE o.registration_id = _gw_registration_generations.registration_id
           )
           AND (
             (run_token IS NULL AND run_target_version IS NULL AND run_lease_expires_at IS NULL)
             OR
             (run_token IS NOT NULL AND run_target_version IS NOT NULL
              AND run_lease_expires_at IS NOT NULL AND run_lease_expires_at <= ?)
           )
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        runToken,
        targetVersion,
        input.leaseExpiresAt,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        current.dirty_version,
        current.delivered_version,
        current.run_version,
        input.nowMs,
        input.nowMs
    );
    if (sql.changes() !== 1) return null;
    const args = snapshotCdbQueryArgs(JSON.parse(current.args_json) as RawJson);
    return {
        targetVersion,
        runToken,
        runVersion,
        leaseExpiresAt: input.leaseExpiresAt,
        reclaimed: current.run_token !== null,
        organizationId: TenantId(current.organization_id),
        ref: current.ref as ChardbRef,
        args,
        shardId: current.shard_id,
        sourceCdbId: current.source_cdb_id,
        schemaEpoch: current.schema_epoch,
        recoveryGeneration: current.recovery_generation,
        domainSchemaEpoch: current.domain_schema_epoch,
        intentJson: current.intent_json,
        policyDigest: current.policy_digest,
    };
}

/** Stage query rows durably without claiming client delivery. Wrap this helper in one transaction. */
export function stageGatewaySnapshot(sql: SyncSql, input: GatewaySnapshotStage): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.runToken.length === 0) throw new TypeError("runToken must be nonempty");
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    assertNonnegativeSafeInteger(input.runVersion, "runVersion");
    assertNonnegativeSafeInteger(input.targetVersion, "targetVersion");
    assertNonnegativeSafeInteger(input.recoveryGeneration, "recoveryGeneration");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    for (const [name, value] of [
        ["authEpochs.global", input.authEpochs.global],
        ["authEpochs.tenant", input.authEpochs.tenant],
        ["authEpochs.principal", input.authEpochs.principal],
    ] as const) {
        assertNonnegativeSafeInteger(value, name);
    }
    if (!Array.isArray(input.rows)) throw new TypeError("rows must be a JSON array");
    const rows = rawJsonResult(input.rows, "Gateway snapshot rows");
    if (!Array.isArray(rows)) throw new TypeError("rows must be a JSON array");
    const rowsJson = stableJson(rows);
    const byteSize = new TextEncoder().encode(rowsJson).byteLength;

    sql.exec(
        `UPDATE _gw_registration_generations
         SET run_token = NULL, run_target_version = NULL, run_lease_expires_at = NULL,
             run_version = run_version + 1,
             recovery_generation = ?, auth_global_epoch = ?, auth_tenant_epoch = ?, auth_principal_epoch = ?,
             retry_count = 0, retry_at = NULL, retry_error = NULL, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND run_token = ? AND run_target_version = ? AND run_version = ?
           AND NOT EXISTS (
             SELECT 1 FROM _gw_snapshot_outbox o
             WHERE o.registration_id = _gw_registration_generations.registration_id
           )
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        input.recoveryGeneration,
        input.authEpochs.global,
        input.authEpochs.tenant,
        input.authEpochs.principal,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.runToken,
        input.targetVersion,
        input.runVersion
    );
    if (sql.changes() !== 1) return false;
    sql.exec(
        `INSERT INTO _gw_snapshot_outbox
         (registration_id, cookie, target_version, rows_json, byte_size,
          send_attempts, next_attempt_at, claim_token, claim_version, claim_expires_at,
          last_sent_at, last_error, created_at)
         VALUES (?, ?, ?, ?, ?, 0, ?, NULL, 0, NULL, NULL, NULL, ?)`,
        input.registrationId,
        input.cookie,
        input.targetVersion,
        rowsJson,
        byteSize,
        input.nowMs,
        input.nowMs
    );
    assertGatewayDurablePayloadQuota(sql);
    return true;
}

/** Claim the oldest due staged snapshot and defer its next send attempt. The caller must use one transaction. */
export function claimDueGatewaySnapshot(
    sql: SyncSql,
    input: GatewaySnapshotSendClaim
): GatewaySnapshotSendAttempt | null {
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    assertNonnegativeSafeInteger(input.attemptExpiresAt, "attemptExpiresAt");
    if (input.attemptExpiresAt <= input.nowMs) {
        throw new TypeError("attemptExpiresAt must be later than nowMs");
    }
    if (input.connectionId !== undefined && input.connectionId.length === 0) {
        throw new TypeError("connectionId must be nonempty");
    }
    if (input.excludedConnectionIds?.some(connectionId => connectionId.length === 0)) {
        throw new TypeError("excluded connection IDs must be nonempty");
    }
    const connectionFilter = input.connectionId === undefined ? "" : "AND g.connection_id = ?";
    const excludedConnectionIds = [...new Set(input.excludedConnectionIds ?? [])];
    const exclusionFilter =
        excludedConnectionIds.length === 0
            ? ""
            : `AND g.connection_id NOT IN (${excludedConnectionIds.map(() => "?").join(", ")})`;
    const due = sql.one<{
        registration_id: string;
        principal_id: string;
        client_id: string;
        sub_id: number;
        connection_id: string;
        cookie: string;
        target_version: number;
        rows_json: string;
        byte_size: number;
        send_attempts: number;
        next_attempt_at: number;
        claim_token: string | null;
        claim_version: number;
        organization_id: string;
        ref: string;
        args_json: string;
        intent_json: string;
        policy_digest: string;
        query_hash: string;
        shard_id: string;
        source_cdb_id: string;
        schema_epoch: number;
        recovery_generation: number;
        domain_schema_epoch: number;
        auth_global_epoch: number;
        auth_tenant_epoch: number;
        auth_principal_epoch: number;
    }>(
        `SELECT o.registration_id, g.principal_id, g.client_id, g.sub_id, g.connection_id,
                o.cookie, o.target_version, o.rows_json, o.byte_size, o.send_attempts, o.next_attempt_at,
                o.claim_token, o.claim_version, g.organization_id, g.ref, g.args_json,
                g.intent_json, g.policy_digest, g.query_hash, g.shard_id, g.source_cdb_id,
                g.schema_epoch, g.recovery_generation, g.domain_schema_epoch,
                g.auth_global_epoch, g.auth_tenant_epoch, g.auth_principal_epoch
         FROM _gw_snapshot_outbox o
         INNER JOIN _gw_registration_generations g ON g.registration_id = o.registration_id
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE o.next_attempt_at <= ?
           AND (o.claim_token IS NULL OR o.claim_expires_at IS NULL OR o.claim_expires_at <= ?)
           AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           ${connectionFilter}
           ${exclusionFilter}
         ORDER BY o.next_attempt_at, o.registration_id
         LIMIT 1`,
        input.nowMs,
        input.nowMs,
        ...(input.connectionId === undefined ? [] : [input.connectionId]),
        ...excludedConnectionIds
    );
    if (!due) return null;
    const claimToken = crypto.randomUUID();
    const claimVersion = due.claim_version + 1;
    sql.exec(
        `UPDATE _gw_snapshot_outbox
         SET send_attempts = MIN(send_attempts + 1, ?), next_attempt_at = ?,
             claim_token = ?, claim_version = claim_version + 1, claim_expires_at = ?, last_sent_at = ?
         WHERE registration_id = ? AND cookie = ? AND target_version = ?
           AND send_attempts = ? AND next_attempt_at = ? AND claim_version = ?
           AND ((claim_token IS NULL AND ? IS NULL) OR claim_token = ?)
           AND next_attempt_at <= ?
           AND (claim_token IS NULL OR claim_expires_at IS NULL OR claim_expires_at <= ?)`,
        GATEWAY_CLEANUP_MAX_RETRY_COUNT,
        input.attemptExpiresAt,
        claimToken,
        input.attemptExpiresAt,
        input.nowMs,
        due.registration_id,
        due.cookie,
        due.target_version,
        due.send_attempts,
        due.next_attempt_at,
        due.claim_version,
        due.claim_token,
        due.claim_token,
        input.nowMs,
        input.nowMs
    );
    if (sql.changes() !== 1) return null;
    let decoded: unknown;
    try {
        decoded = JSON.parse(due.rows_json);
    } catch (cause) {
        throw gatewayInvalidationInvariant("staged Gateway snapshot rows are not valid JSON", cause);
    }
    const rows = rawJsonResult(decoded, "staged Gateway snapshot rows");
    if (!Array.isArray(rows)) throw gatewayInvalidationInvariant("staged Gateway snapshot rows are not an array");
    const args = snapshotCdbQueryArgs(JSON.parse(due.args_json) as RawJson);
    return {
        principalId: PrincipalId(due.principal_id),
        clientId: ClientId(due.client_id),
        subId: SubId(due.sub_id),
        registrationId: due.registration_id,
        connectionId: due.connection_id,
        cookie: Cookie(due.cookie),
        targetVersion: due.target_version,
        rows,
        byteSize: due.byte_size,
        sendAttempts: Math.min(due.send_attempts + 1, GATEWAY_CLEANUP_MAX_RETRY_COUNT),
        nextAttemptAt: input.attemptExpiresAt,
        claimToken,
        claimVersion,
        organizationId: TenantId(due.organization_id),
        ref: ChardbRef(due.ref),
        args,
        intentJson: due.intent_json,
        policyDigest: due.policy_digest,
        queryHash: due.query_hash,
        shardId: due.shard_id,
        sourceCdbId: due.source_cdb_id,
        schemaEpoch: due.schema_epoch,
        recoveryGeneration: due.recovery_generation,
        domainSchemaEpoch: due.domain_schema_epoch,
        authEpochs: {
            global: due.auth_global_epoch,
            tenant: due.auth_tenant_epoch,
            principal: due.auth_principal_epoch,
        },
    };
}

/** Drop one stale claimed payload while leaving its undelivered generation eligible for a fresh query. */
export function discardClaimedGatewaySnapshot(sql: SyncSql, input: GatewayClaimedSnapshotRetire): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    if (input.claimToken.length === 0) throw new TypeError("claimToken must be nonempty");
    assertNonnegativeSafeInteger(input.claimVersion, "claimVersion");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    sql.exec(
        `DELETE FROM _gw_snapshot_outbox
         WHERE registration_id = ? AND cookie = ? AND claim_token = ? AND claim_version = ?
           AND EXISTS (
             SELECT 1
             FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.registration_id = _gw_snapshot_outbox.registration_id
               AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ? AND g.connection_id = ?
               AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           )`,
        input.registrationId,
        input.cookie,
        input.claimToken,
        input.claimVersion,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return sql.changes() === 1;
}

/** Clear one exact failed query run and retain its dirty target for a bounded retry. */
export function failGatewayDirtyRun(sql: SyncSql, input: GatewayDirtyRunFailure): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.runToken.length === 0) throw new TypeError("runToken must be nonempty");
    assertNonnegativeSafeInteger(input.runVersion, "runVersion");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    if (input.retryNotBeforeMs !== undefined) {
        assertNonnegativeSafeInteger(input.retryNotBeforeMs, "retryNotBeforeMs");
    }
    const current = sql.one<{ retry_count: number }>(
        `SELECT g.retry_count
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE g.registration_id = ? AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ?
           AND g.connection_id = ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           AND g.run_token = ? AND g.run_version = ?`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.runToken,
        input.runVersion
    );
    if (!current) return false;
    const attempts = Math.min(current.retry_count + 1, GATEWAY_CLEANUP_MAX_RETRY_COUNT);
    const retryAt = Math.max(input.nowMs + gatewayRetryDelayMs(attempts), input.retryNotBeforeMs ?? input.nowMs);
    const message = gatewayRetryError(input.error);
    sql.exec(
        `UPDATE _gw_registration_generations
         SET run_token = NULL, run_target_version = NULL, run_lease_expires_at = NULL,
             run_version = run_version + 1, retry_count = ?, retry_at = ?, retry_error = ?, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND run_token = ? AND run_version = ?
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        attempts,
        retryAt,
        message,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.runToken,
        input.runVersion
    );
    return sql.changes() === 1;
}

/** Defer unclaimed dirty work until an expired socket's bounded auth-refresh grace ends. */
export function deferGatewayDirtyRun(sql: SyncSql, input: GatewayDirtyRunDeferral): boolean {
    assertGatewayRegistrationIdentity(input);
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    assertNonnegativeSafeInteger(input.retryAt, "retryAt");
    if (input.retryAt <= input.nowMs) throw new TypeError("retryAt must be later than nowMs");
    sql.exec(
        `UPDATE _gw_registration_generations
         SET retry_at = MAX(COALESCE(retry_at, 0), ?), retry_error = ?, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND run_token IS NULL AND run_target_version IS NULL AND run_lease_expires_at IS NULL
           AND (initial_snapshot_pending = 1 OR dirty_version > delivered_version)
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        input.retryAt,
        GATEWAY_AUTH_REFRESH_PENDING_ERROR,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return sql.changes() === 1;
}

/** Release one exact failed send claim without changing its immutable payload or cookie. */
export function failGatewaySnapshotSend(sql: SyncSql, input: GatewaySnapshotSendFailure): boolean {
    if (input.registrationId.length === 0) throw new TypeError("registrationId must be nonempty");
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    if (input.claimToken.length === 0) throw new TypeError("claimToken must be nonempty");
    assertNonnegativeSafeInteger(input.claimVersion, "claimVersion");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    if (input.retryNotBeforeMs !== undefined) {
        assertNonnegativeSafeInteger(input.retryNotBeforeMs, "retryNotBeforeMs");
    }
    const current = sql.one<{ send_attempts: number }>(
        `SELECT send_attempts FROM _gw_snapshot_outbox
         WHERE registration_id = ? AND cookie = ? AND claim_token = ? AND claim_version = ?`,
        input.registrationId,
        input.cookie,
        input.claimToken,
        input.claimVersion
    );
    if (!current) return false;
    const retryAt = Math.max(
        input.nowMs + gatewayRetryDelayMs(Math.max(1, current.send_attempts)),
        input.retryNotBeforeMs ?? input.nowMs
    );
    sql.exec(
        `UPDATE _gw_snapshot_outbox
         SET claim_token = NULL, claim_expires_at = NULL, next_attempt_at = ?, last_error = ?
         WHERE registration_id = ? AND cookie = ? AND claim_token = ? AND claim_version = ?`,
        retryAt,
        gatewayRetryError(input.error),
        input.registrationId,
        input.cookie,
        input.claimToken,
        input.claimVersion
    );
    return sql.changes() === 1;
}

/** Bind one exact send claim to the socket cookie observed immediately before send. */
export function markGatewaySnapshotSendBaseCookie(
    sql: SyncSql,
    input: GatewaySnapshotSendAttempt,
    baseCookie: Cookie | null,
    nowMs: number
): "marked" | "retired" | "stale" {
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    sql.exec(
        `UPDATE _gw_snapshot_outbox
         SET attachment_base_cookie = COALESCE(attachment_base_cookie, ?)
         WHERE registration_id = ? AND cookie = ? AND claim_token = ? AND claim_version = ?
           AND EXISTS (
             SELECT 1
             FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.registration_id = _gw_snapshot_outbox.registration_id
               AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ? AND g.connection_id = ?
               AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           )`,
        baseCookie,
        input.registrationId,
        input.cookie,
        input.claimToken,
        input.claimVersion,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (sql.changes() !== 1) return "stale";
    if (gatewayDurablePayloadUsage(sql).chargedTotalBytes <= GATEWAY_MAX_DURABLE_PAYLOAD_BYTES) return "marked";
    if (
        !retireClaimedGatewaySnapshot(sql, {
            ...input,
            nowMs,
        })
    ) {
        throw gatewayInvalidationInvariant("over-quota Gateway snapshot claimant could not retire atomically");
    }
    return "retired";
}

/** Resolve a staged cookie only within one verified socket identity. */
export function resolveGatewaySnapshotAck(
    sql: SyncSql,
    input: GatewaySnapshotAckLookup
): GatewaySnapshotAckIdentity | null {
    if (input.connectionId.length === 0) throw new TypeError("connectionId must be nonempty");
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    const staged = sql.one<{
        registration_id: string;
        sub_id: number;
        already_acknowledged: number;
        attachment_base_cookie: string | null;
    }>(
        `SELECT g.registration_id, g.sub_id,
                CASE WHEN o.registration_id IS NULL THEN 1 ELSE 0 END AS already_acknowledged,
                o.attachment_base_cookie
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         LEFT JOIN _gw_snapshot_outbox o ON o.registration_id = g.registration_id
         WHERE g.principal_id = ? AND g.client_id = ? AND g.connection_id = ?
           AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           AND (
             (o.cookie = ? AND o.send_attempts > 0 AND o.last_sent_at IS NOT NULL AND o.claim_token IS NOT NULL)
             OR (o.registration_id IS NULL AND g.last_snapshot_cookie = ?)
           )`,
        input.principalId,
        input.clientId,
        input.connectionId,
        input.cookie,
        input.cookie
    );
    if (!staged) return null;
    return {
        principalId: input.principalId,
        clientId: input.clientId,
        subId: SubId(staged.sub_id),
        registrationId: staged.registration_id,
        connectionId: input.connectionId,
        cookie: input.cookie,
        alreadyAcknowledged: staged.already_acknowledged === 1,
        attachmentBaseCookie: staged.attachment_base_cookie === null ? null : Cookie(staged.attachment_base_cookie),
    };
}

/** Advance delivery only to the version owned by one exact staged cookie. The caller must use one transaction. */
export function acknowledgeGatewaySnapshot(sql: SyncSql, input: GatewaySnapshotAcknowledge): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    const staged = sql.one<{ target_version: number }>(
        `SELECT o.target_version
         FROM _gw_snapshot_outbox o
         INNER JOIN _gw_registration_generations g ON g.registration_id = o.registration_id
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE o.registration_id = ? AND o.cookie = ?
           AND o.send_attempts > 0 AND o.last_sent_at IS NOT NULL AND o.claim_token IS NOT NULL
           AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ? AND g.connection_id = ?
           AND g.lifecycle = 'active' AND g.cdb_state = 'active'`,
        input.registrationId,
        input.cookie,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (!staged) {
        return Boolean(
            sql.one<{ registration_id: string }>(
                `SELECT g.registration_id
                 FROM _gw_registration_generations g
                 INNER JOIN _gw_registration_heads h
                   ON h.registration_id = g.registration_id
                  AND h.principal_id = g.principal_id
                  AND h.client_id = g.client_id
                  AND h.sub_id = g.sub_id
                 WHERE g.registration_id = ? AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ?
                   AND g.connection_id = ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
                   AND g.last_snapshot_cookie = ?`,
                input.registrationId,
                input.principalId,
                input.clientId,
                input.subId,
                input.connectionId,
                input.cookie
            )
        );
    }
    sql.exec(
        `UPDATE _gw_registration_generations
         SET delivered_version = MAX(delivered_version, ?), initial_snapshot_pending = 0,
             last_cookie = ?, last_snapshot_cookie = ?, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND delivered_version <= ? AND dirty_version >= ?
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        staged.target_version,
        input.cookie,
        input.cookie,
        input.nowMs,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        staged.target_version,
        staged.target_version
    );
    if (sql.changes() !== 1) return false;
    sql.exec(
        "DELETE FROM _gw_snapshot_outbox WHERE registration_id = ? AND cookie = ? AND target_version = ?",
        input.registrationId,
        input.cookie,
        staged.target_version
    );
    if (sql.changes() !== 1) {
        throw gatewayInvalidationInvariant("staged Gateway snapshot disappeared during acknowledgement");
    }
    sql.exec(
        `DELETE FROM _gw_snapshot_replay
         WHERE principal_id = ? AND client_id = ? AND sub_id = ?`,
        input.principalId,
        input.clientId,
        input.subId
    );
    assertGatewayDurablePayloadQuota(sql);
    return true;
}

/** Remove expired or oldest replay rows until both hard retention bounds hold. */
export function pruneGatewaySnapshotReplays(sql: SyncSql, nowMs: number): number {
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    let removed = 0;
    sql.exec("DELETE FROM _gw_snapshot_replay WHERE expires_at <= ?", nowMs);
    removed += sql.changes();
    sql.exec(
        `DELETE FROM _gw_snapshot_replay
         WHERE rowid IN (
           SELECT rowid FROM _gw_snapshot_replay
           ORDER BY created_at DESC, principal_id DESC, client_id DESC, sub_id DESC
           LIMIT -1 OFFSET ?
         )`,
        GATEWAY_MAX_SNAPSHOT_REPLAY_ROWS
    );
    removed += sql.changes();
    while (gatewayDurablePayloadUsage(sql).chargedTotalBytes > GATEWAY_MAX_DURABLE_PAYLOAD_BYTES) {
        sql.exec(
            `DELETE FROM _gw_snapshot_replay
             WHERE rowid = (
               SELECT rowid FROM _gw_snapshot_replay
               ORDER BY created_at, principal_id, client_id, sub_id
               LIMIT 1
             )`
        );
        const changed = sql.changes();
        removed += changed;
        if (changed === 0) break;
    }
    return removed;
}

/**
 * Retain the latest snapshot that was handed to the transport for one logical
 * subscription. The original send timestamp fixes the retention deadline, so
 * repeated reconnects cannot extend it.
 */
export function retainCurrentGatewaySnapshotReplay(
    sql: SyncSql,
    key: GatewayRegistrationKey,
    nowMs: number,
    deferPrune = false
): boolean {
    assertGatewayRegistrationKey(key);
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    sql.exec(
        `INSERT INTO _gw_snapshot_replay
         (principal_id, client_id, sub_id, cookie, organization_id, ref, args_json,
          policy_digest, query_hash, shard_id, source_cdb_id, schema_epoch, recovery_generation, domain_schema_epoch,
          auth_global_epoch, auth_tenant_epoch, auth_principal_epoch,
          rows_json, byte_size, created_at, expires_at)
         SELECT g.principal_id, g.client_id, g.sub_id, o.cookie, g.organization_id, g.ref, g.args_json,
                g.policy_digest, g.query_hash, g.shard_id, g.source_cdb_id, g.schema_epoch, g.recovery_generation,
                g.domain_schema_epoch, g.auth_global_epoch, g.auth_tenant_epoch, g.auth_principal_epoch,
                o.rows_json, o.byte_size, o.last_sent_at, o.last_sent_at + ?
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         INNER JOIN _gw_snapshot_outbox o ON o.registration_id = g.registration_id
         WHERE g.principal_id = ? AND g.client_id = ? AND g.sub_id = ?
           AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           AND o.send_attempts > 0 AND o.last_sent_at IS NOT NULL AND o.claim_token IS NOT NULL
           AND o.last_sent_at + ? > ?
         ON CONFLICT (principal_id, client_id, sub_id) DO UPDATE SET
           cookie = excluded.cookie,
           organization_id = excluded.organization_id,
           ref = excluded.ref,
           args_json = excluded.args_json,
           policy_digest = excluded.policy_digest,
           query_hash = excluded.query_hash,
           shard_id = excluded.shard_id,
           source_cdb_id = excluded.source_cdb_id,
           schema_epoch = excluded.schema_epoch,
           recovery_generation = excluded.recovery_generation,
           domain_schema_epoch = excluded.domain_schema_epoch,
           auth_global_epoch = excluded.auth_global_epoch,
           auth_tenant_epoch = excluded.auth_tenant_epoch,
           auth_principal_epoch = excluded.auth_principal_epoch,
           rows_json = excluded.rows_json,
           byte_size = excluded.byte_size,
           created_at = excluded.created_at,
           expires_at = excluded.expires_at
         WHERE excluded.created_at >= _gw_snapshot_replay.created_at`,
        GATEWAY_SNAPSHOT_REPLAY_RETENTION_MS,
        key.principalId,
        key.clientId,
        key.subId,
        GATEWAY_SNAPSHOT_REPLAY_RETENTION_MS,
        nowMs
    );
    const retained = sql.changes() === 1;
    if (!deferPrune) pruneGatewaySnapshotReplays(sql, nowMs);
    return retained;
}

/** Resolve replay only when the resumed transport and current authority describe the exact old query. */
export function resolveGatewaySnapshotReplay(
    sql: SyncSql,
    input: GatewaySnapshotReplayLookup
): GatewaySnapshotReplay | null {
    assertGatewayRegistrationKey(input);
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    assertNonnegativeSafeInteger(input.schemaEpoch, "schemaEpoch");
    assertNonnegativeSafeInteger(input.recoveryGeneration, "recoveryGeneration");
    assertNonnegativeSafeInteger(input.domainSchemaEpoch, "domainSchemaEpoch");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    pruneGatewaySnapshotReplays(sql, input.nowMs);
    const replay = sql.one<{ rows_json: string }>(
        `SELECT rows_json FROM _gw_snapshot_replay
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND cookie = ?
           AND organization_id = ? AND ref = ? AND args_json = ?
           AND policy_digest = ? AND query_hash = ? AND shard_id = ? AND source_cdb_id = ?
           AND schema_epoch = ? AND recovery_generation = ? AND domain_schema_epoch = ?
           AND auth_global_epoch = ? AND auth_tenant_epoch = ? AND auth_principal_epoch = ?
           AND expires_at > ?`,
        input.principalId,
        input.clientId,
        input.subId,
        input.cookie,
        input.organizationId,
        input.ref,
        stableJson(input.args),
        input.policyDigest,
        input.queryHash,
        input.shardId,
        input.sourceCdbId,
        input.schemaEpoch,
        input.recoveryGeneration,
        input.domainSchemaEpoch,
        input.authEpochs.global,
        input.authEpochs.tenant,
        input.authEpochs.principal,
        input.nowMs
    );
    if (!replay) return null;
    try {
        const rows = rawJsonResult(JSON.parse(replay.rows_json), "Gateway replay snapshot rows");
        if (!Array.isArray(rows)) throw new TypeError("Gateway replay snapshot rows must be an array");
        return { subId: input.subId, cookie: input.cookie, rows };
    } catch {
        sql.exec(
            `DELETE FROM _gw_snapshot_replay
             WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND cookie = ?`,
            input.principalId,
            input.clientId,
            input.subId,
            input.cookie
        );
        return null;
    }
}

/** Consume one exact replay cookie after a verified replacement socket acknowledges it. */
export function acknowledgeGatewaySnapshotReplay(
    sql: SyncSql,
    input: Pick<GatewaySnapshotReplayLookup, "principalId" | "clientId" | "cookie" | "nowMs">
): SubId | null {
    assertGatewayRegistrationKey({ principalId: input.principalId, clientId: input.clientId, subId: SubId(0) });
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    pruneGatewaySnapshotReplays(sql, input.nowMs);
    const replay = sql.one<{ sub_id: number }>(
        `SELECT sub_id FROM _gw_snapshot_replay
         WHERE principal_id = ? AND client_id = ? AND cookie = ? AND expires_at > ?
         ORDER BY sub_id LIMIT 1`,
        input.principalId,
        input.clientId,
        input.cookie,
        input.nowMs
    );
    if (!replay) return null;
    sql.exec(
        `DELETE FROM _gw_snapshot_replay
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND cookie = ?`,
        input.principalId,
        input.clientId,
        replay.sub_id,
        input.cookie
    );
    return sql.changes() === 1 ? SubId(replay.sub_id) : null;
}

/** Return current, internally consistent generations owned by one socket generation. */
export function listCurrentGatewayRegistrationsForConnection(
    sql: SyncSql,
    connectionId: string
): readonly GatewayCurrentRegistration[] {
    if (connectionId.length === 0) throw new TypeError("connectionId must be nonempty");
    return sql
        .all<{
            principal_id: string;
            client_id: string;
            sub_id: number;
            registration_id: string;
            connection_id: string;
            shard_id: string;
            source_cdb_id: string | null;
        }>(
            `SELECT g.principal_id, g.client_id, g.sub_id, g.registration_id,
                    g.connection_id, g.shard_id, g.source_cdb_id
             FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.connection_id = ?
             ORDER BY g.principal_id, g.client_id, g.sub_id, g.registration_id`,
            connectionId
        )
        .map(row => ({
            principalId: PrincipalId(row.principal_id),
            clientId: ClientId(row.client_id),
            subId: SubId(row.sub_id),
            registrationId: row.registration_id,
            connectionId: row.connection_id,
            shardId: row.shard_id,
            sourceCdbId: row.source_cdb_id,
        }));
}

/**
 * Retire one exact current generation while retaining its cleanup row. The
 * caller must wrap this multi-statement helper in the Gateway transaction.
 */
export function retireCurrentGatewayRegistration(
    sql: SyncSql,
    input: GatewayCurrentRegistrationRetire
): GatewayCurrentRegistration | null {
    assertGatewayRegistrationKey(input);
    if (input.connectionId.length === 0) throw new TypeError("connectionId must be nonempty");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    const current = sql.one<{
        registration_id: string;
        shard_id: string;
        source_cdb_id: string | null;
    }>(
        `SELECT g.registration_id, g.shard_id, g.source_cdb_id
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE g.principal_id = ? AND g.client_id = ? AND g.sub_id = ? AND g.connection_id = ?`,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (!current) return null;
    sql.exec(
        `UPDATE _gw_registration_generations
         SET ${GATEWAY_RETIRED_PAYLOAD_ASSIGNMENTS},
             lifecycle = 'retiring',
             cdb_state = CASE WHEN cdb_state = 'pending' THEN 'pending' ELSE 'retiring' END,
             run_token = NULL, run_target_version = NULL,
             run_lease_expires_at = NULL,
             run_version = run_version + 1, retry_count = 0,
             retry_at = CASE WHEN cdb_state = 'pending' THEN retry_at ELSE ? END,
             retry_error = NULL, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND connection_id = ?`,
        input.nowMs,
        input.nowMs,
        current.registration_id,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    if (sql.changes() !== 1) {
        throw gatewayInvalidationInvariant("current Gateway registration disappeared during retirement");
    }
    sql.exec("DELETE FROM _gw_snapshot_outbox WHERE registration_id = ?", current.registration_id);
    sql.exec(
        `DELETE FROM _gw_registration_heads
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND registration_id = ?`,
        input.principalId,
        input.clientId,
        input.subId,
        current.registration_id
    );
    if (sql.changes() !== 1) {
        throw gatewayInvalidationInvariant("current Gateway registration head disappeared during retirement");
    }
    return {
        principalId: input.principalId,
        clientId: input.clientId,
        subId: input.subId,
        registrationId: current.registration_id,
        connectionId: input.connectionId,
        shardId: current.shard_id,
        sourceCdbId: current.source_cdb_id,
    };
}

/**
 * Retire every internally consistent current generation owned by one socket
 * generation. The caller must wrap this helper in the Gateway transaction.
 */
export function retireCurrentGatewayRegistrationsForConnection(
    sql: SyncSql,
    connectionId: string,
    nowMs: number
): readonly GatewayCurrentRegistration[] {
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    const registrations = listCurrentGatewayRegistrationsForConnection(sql, connectionId);
    for (const registration of registrations) {
        retainCurrentGatewaySnapshotReplay(sql, registration, nowMs, true);
        const retired = retireCurrentGatewayRegistration(sql, { ...registration, nowMs });
        if (!retired || retired.registrationId !== registration.registrationId) {
            throw gatewayInvalidationInvariant("current Gateway registration changed during connection retirement");
        }
    }
    pruneGatewaySnapshotReplays(sql, nowMs);
    return registrations;
}

/**
 * Remove a current logical head while retaining its generation for Cdb cleanup.
 * The caller must wrap the head deletion and generation update in one transaction.
 */
export function retireGatewayRegistration(
    sql: SyncSql,
    key: GatewayRegistrationKey,
    registrationId: string,
    nowMs: number
): boolean {
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    sql.exec(
        `DELETE FROM _gw_registration_heads
         WHERE principal_id = ? AND client_id = ? AND sub_id = ? AND registration_id = ?`,
        key.principalId,
        key.clientId,
        key.subId,
        registrationId
    );
    const removedHead = sql.changes() === 1;
    if (!removedHead) return false;
    sql.exec(
        `UPDATE _gw_registration_generations
         SET ${GATEWAY_RETIRED_PAYLOAD_ASSIGNMENTS},
             lifecycle = 'retiring',
             cdb_state = CASE WHEN cdb_state = 'pending' THEN 'pending' ELSE 'retiring' END,
             run_token = NULL, run_target_version = NULL,
             run_lease_expires_at = NULL,
             run_version = run_version + 1, retry_count = 0,
             retry_at = CASE WHEN cdb_state = 'pending' THEN retry_at ELSE ? END,
             retry_error = NULL, updated_at = ?
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?`,
        nowMs,
        nowMs,
        registrationId,
        key.principalId,
        key.clientId,
        key.subId
    );
    sql.exec("DELETE FROM _gw_snapshot_outbox WHERE registration_id = ?", registrationId);
    return true;
}

/** Make work paused for auth refresh immediately eligible after a verified refresh succeeds. */
export function resumeGatewayAuthDeferredWork(sql: SyncSql, connectionId: string, nowMs: number): boolean {
    if (connectionId.length === 0) throw new TypeError("connectionId must be nonempty");
    assertNonnegativeSafeInteger(nowMs, "nowMs");
    sql.exec(
        `UPDATE _gw_registration_generations
         SET retry_at = ?, retry_error = NULL, updated_at = ?
         WHERE connection_id = ? AND lifecycle = 'active' AND cdb_state = 'active'
           AND retry_error = ?
           AND run_token IS NULL AND run_target_version IS NULL AND run_lease_expires_at IS NULL
           AND (initial_snapshot_pending = 1 OR dirty_version > delivered_version)
           AND EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
               AND h.principal_id = _gw_registration_generations.principal_id
               AND h.client_id = _gw_registration_generations.client_id
               AND h.sub_id = _gw_registration_generations.sub_id
           )`,
        nowMs,
        nowMs,
        connectionId,
        GATEWAY_AUTH_REFRESH_PENDING_ERROR
    );
    const resumedRuns = sql.changes();
    sql.exec(
        `UPDATE _gw_snapshot_outbox
         SET next_attempt_at = ?, last_error = NULL
         WHERE last_error = ?
           AND EXISTS (
             SELECT 1 FROM _gw_registration_generations g
             INNER JOIN _gw_registration_heads h
               ON h.registration_id = g.registration_id
              AND h.principal_id = g.principal_id
              AND h.client_id = g.client_id
              AND h.sub_id = g.sub_id
             WHERE g.registration_id = _gw_snapshot_outbox.registration_id
               AND g.connection_id = ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           )`,
        nowMs,
        GATEWAY_AUTH_REFRESH_PENDING_ERROR,
        connectionId
    );
    return resumedRuns > 0 || sql.changes() > 0;
}

/** Retire only while one exact leased run still owns this current generation. */
export function retireClaimedGatewayRegistration(sql: SyncSql, input: GatewayClaimedRunRetire): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.runToken.length === 0) throw new TypeError("runToken must be nonempty");
    assertNonnegativeSafeInteger(input.runVersion, "runVersion");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    const owned = sql.one<{ registration_id: string }>(
        `SELECT g.registration_id
         FROM _gw_registration_generations g
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE g.registration_id = ? AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ?
           AND g.connection_id = ? AND g.lifecycle = 'active' AND g.cdb_state = 'active'
           AND g.run_token = ? AND g.run_version = ?`,
        input.registrationId,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId,
        input.runToken,
        input.runVersion
    );
    return owned ? retireGatewayRegistration(sql, input, input.registrationId, input.nowMs) : false;
}

/** Retire only while one exact staged-send attempt still owns this generation. */
export function retireClaimedGatewaySnapshot(sql: SyncSql, input: GatewayClaimedSnapshotRetire): boolean {
    assertGatewayRegistrationIdentity(input);
    if (input.cookie.length === 0) throw new TypeError("cookie must be nonempty");
    if (input.claimToken.length === 0) throw new TypeError("claimToken must be nonempty");
    assertNonnegativeSafeInteger(input.claimVersion, "claimVersion");
    assertNonnegativeSafeInteger(input.nowMs, "nowMs");
    const owned = sql.one<{ registration_id: string }>(
        `SELECT g.registration_id
         FROM _gw_snapshot_outbox o
         INNER JOIN _gw_registration_generations g ON g.registration_id = o.registration_id
         INNER JOIN _gw_registration_heads h
           ON h.registration_id = g.registration_id
          AND h.principal_id = g.principal_id
          AND h.client_id = g.client_id
          AND h.sub_id = g.sub_id
         WHERE o.registration_id = ? AND o.cookie = ? AND o.claim_token = ? AND o.claim_version = ?
           AND g.principal_id = ? AND g.client_id = ? AND g.sub_id = ? AND g.connection_id = ?
           AND g.lifecycle = 'active' AND g.cdb_state = 'active'`,
        input.registrationId,
        input.cookie,
        input.claimToken,
        input.claimVersion,
        input.principalId,
        input.clientId,
        input.subId,
        input.connectionId
    );
    return owned ? retireGatewayRegistration(sql, input, input.registrationId, input.nowMs) : false;
}

/** Delete one retired cleanup row without touching any logical head. */
export function cleanupGatewayRegistration(sql: SyncSql, key: GatewayRegistrationKey, registrationId: string): boolean {
    sql.exec(
        `DELETE FROM _gw_registration_generations
         WHERE registration_id = ? AND principal_id = ? AND client_id = ? AND sub_id = ?
           AND lifecycle = 'retiring' AND cdb_state = 'retiring'
           AND NOT EXISTS (
             SELECT 1 FROM _gw_registration_heads h
             WHERE h.registration_id = _gw_registration_generations.registration_id
           )`,
        registrationId,
        key.principalId,
        key.clientId,
        key.subId
    );
    return sql.changes() === 1;
}

function assertGatewayRegistrationInstall(input: GatewayRegistrationInstall): void {
    for (const [name, value] of [
        ["subId", input.subId],
        ["schemaEpoch", input.schemaEpoch],
        ["recoveryGeneration", input.recoveryGeneration],
        ["domainSchemaEpoch", input.domainSchemaEpoch],
        ["authEpochs.global", input.authEpochs.global],
        ["authEpochs.tenant", input.authEpochs.tenant],
        ["authEpochs.principal", input.authEpochs.principal],
        ["nowMs", input.nowMs],
    ] as const) {
        assertNonnegativeSafeInteger(value, name);
    }
    for (const [name, value] of [
        ["registrationId", input.registrationId],
        ["connectionId", input.connectionId],
        ["organizationId", input.organizationId],
        ["ref", input.ref],
        ["policyDigest", input.policyDigest],
        ["queryHash", input.queryHash],
        ["shardId", input.shardId],
        ["sourceCdbId", input.sourceCdbId],
    ] as const) {
        if (value.length === 0) throw new TypeError(`${name} must be nonempty`);
    }
}

function assertGatewayRegistrationIdentity(input: {
    readonly registrationId: string;
    readonly connectionId: string;
    readonly principalId: PrincipalId;
    readonly clientId: ClientId;
    readonly subId: SubId;
}): void {
    assertGatewayRegistrationKey(input);
    for (const [name, value] of [
        ["registrationId", input.registrationId],
        ["connectionId", input.connectionId],
    ] as const) {
        if (value.length === 0) throw new TypeError(`${name} must be nonempty`);
    }
}

function assertGatewayRegistrationKey(input: GatewayRegistrationKey): void {
    assertNonnegativeSafeInteger(input.subId, "subId");
    for (const [name, value] of [
        ["principalId", input.principalId],
        ["clientId", input.clientId],
    ] as const) {
        if (value.length === 0) throw new TypeError(`${name} must be nonempty`);
    }
}

export function assertNonnegativeSafeInteger(value: number, name: string): void {
    if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a nonnegative safe integer`);
}

export function gatewayRetryDelayMs(attempts: number): number {
    const exponent = Math.max(0, Math.min(attempts - 1, GATEWAY_CLEANUP_MAX_RETRY_COUNT - 1));
    return Math.min(GATEWAY_CLEANUP_MAX_RETRY_MS, GATEWAY_CLEANUP_BASE_RETRY_MS * 2 ** exponent);
}

function gatewayRetryError(error: unknown): string {
    return (error instanceof Error ? error.message : String(error)).slice(0, GATEWAY_CLEANUP_MAX_ERROR_LENGTH);
}

export function ensureGatewayRegistrationColumns(sql: SyncSql): void {
    const columns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info('_gw_registration_generations')").map(column => column.name)
    );
    if (!columns.has("source_cdb_id")) {
        // Existing generations predate physical Cdb identity. A null source is
        // intentionally stale until that logical subscription is replaced.
        sql.exec("ALTER TABLE _gw_registration_generations ADD COLUMN source_cdb_id TEXT");
    }
    if (!columns.has("domain_schema_epoch")) {
        // Existing generations predate domain-version fencing. Retire them
        // below so no old registration can execute under an assumed epoch.
        sql.exec(
            "ALTER TABLE _gw_registration_generations ADD COLUMN domain_schema_epoch INTEGER CHECK (domain_schema_epoch IS NULL OR domain_schema_epoch > 0)"
        );
    }
    if (!columns.has("recovery_generation")) {
        sql.exec(
            "ALTER TABLE _gw_registration_generations ADD COLUMN recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0)"
        );
    }
    const replayColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info('_gw_snapshot_replay')").map(column => column.name)
    );
    if (replayColumns.size > 0 && !replayColumns.has("recovery_generation")) {
        sql.exec(
            "ALTER TABLE _gw_snapshot_replay ADD COLUMN recovery_generation INTEGER NOT NULL DEFAULT 0 CHECK (recovery_generation >= 0)"
        );
    }
    if (!columns.has("policy_digest")) {
        sql.exec("ALTER TABLE _gw_registration_generations ADD COLUMN policy_digest TEXT");
    }
    if (!columns.has("run_target_version")) {
        sql.exec(
            `ALTER TABLE _gw_registration_generations
             ADD COLUMN run_target_version INTEGER
             CHECK (run_target_version IS NULL OR (run_target_version >= 0 AND run_target_version <= dirty_version))`
        );
    }
    if (!columns.has("run_lease_expires_at")) {
        sql.exec(
            `ALTER TABLE _gw_registration_generations
             ADD COLUMN run_lease_expires_at INTEGER
             CHECK (run_lease_expires_at IS NULL OR run_lease_expires_at >= 0)`
        );
    }
    if (!columns.has("last_snapshot_cookie")) {
        sql.exec("ALTER TABLE _gw_registration_generations ADD COLUMN last_snapshot_cookie TEXT");
    }
    if (!columns.has("initial_snapshot_pending")) {
        sql.exec(
            `ALTER TABLE _gw_registration_generations
             ADD COLUMN initial_snapshot_pending INTEGER NOT NULL DEFAULT 0
             CHECK (initial_snapshot_pending IN (0, 1))`
        );
    }
    const currentColumns = new Set(
        sql.all<{ name: string }>("PRAGMA table_info('_gw_registration_generations')").map(column => column.name)
    );
    const legacyRetiredPayloadAssignments = [
        ["ref", "ref = ''"],
        ["args_json", "args_json = 'null'"],
        ["intent_json", "intent_json = 'null'"],
        ["policy_digest", "policy_digest = ''"],
        ["query_hash", "query_hash = ''"],
        ["shard_id", "shard_id = ''"],
        ["last_cookie", "last_cookie = NULL"],
        ["last_snapshot_cookie", "last_snapshot_cookie = NULL"],
    ]
        .filter(([column]) => currentColumns.has(column as string))
        .map(([, assignment]) => assignment)
        .join(", ");
    sql.exec(
        `DELETE FROM _gw_registration_heads
         WHERE registration_id IN (
           SELECT registration_id FROM _gw_registration_generations
           WHERE policy_digest IS NULL OR domain_schema_epoch IS NULL
         )`
    );
    sql.exec(
        `DELETE FROM _gw_snapshot_outbox
         WHERE registration_id IN (
           SELECT registration_id FROM _gw_registration_generations
           WHERE policy_digest IS NULL OR domain_schema_epoch IS NULL
         )`
    );
    sql.exec(
        `UPDATE _gw_registration_generations
         SET ${legacyRetiredPayloadAssignments},
             lifecycle = 'retiring', cdb_state = 'retiring', run_token = NULL,
             run_target_version = NULL, run_lease_expires_at = NULL,
             run_version = run_version + 1, retry_count = 0,
             retry_at = updated_at, retry_error = NULL
         WHERE (policy_digest IS NULL OR domain_schema_epoch IS NULL) AND lifecycle != 'retiring'`
    );
    // Legacy retired generations can predate payload compaction. Keep the
    // recovery owner with the exact identity needed for fenced Cdb cleanup.
    sql.exec(
        `DELETE FROM _gw_snapshot_outbox
         WHERE registration_id IN (
           SELECT registration_id FROM _gw_registration_generations WHERE lifecycle = 'retiring'
         )`
    );
    sql.exec(
        `UPDATE _gw_registration_generations
         SET ${legacyRetiredPayloadAssignments},
             run_token = NULL, run_target_version = NULL, run_lease_expires_at = NULL,
             retry_error = NULL
         WHERE lifecycle = 'retiring'`
    );
    sql.exec(
        `UPDATE _gw_registration_generations
         SET retry_at = updated_at + ?, retry_error = 'subscription install recovery'
         WHERE cdb_state = 'pending' AND retry_at IS NULL`,
        GATEWAY_SUBSCRIBE_RECOVERY_MS
    );
    // Run this after every restart. A crash after ALTER but before repair must
    // not leave a partial run triple that no claimant can recover.
    sql.exec(
        `UPDATE _gw_registration_generations
         SET run_token = NULL, run_target_version = NULL,
             run_lease_expires_at = NULL, run_version = run_version + 1
         WHERE NOT (
           (run_token IS NULL AND run_target_version IS NULL AND run_lease_expires_at IS NULL)
           OR
           (run_token IS NOT NULL AND run_target_version IS NOT NULL AND run_lease_expires_at IS NOT NULL)
         )`
    );
}

export function ensureGatewaySnapshotOutboxColumns(sql: SyncSql): void {
    const columns = new Set(sql.all<{ name: string }>("PRAGMA table_info('_gw_snapshot_outbox')").map(row => row.name));
    const additions = [
        ["claim_token", "claim_token TEXT"],
        ["claim_version", "claim_version INTEGER NOT NULL DEFAULT 0"],
        ["claim_expires_at", "claim_expires_at INTEGER"],
        ["attachment_base_cookie", "attachment_base_cookie TEXT"],
    ] as const;
    for (const [name, definition] of additions) {
        if (!columns.has(name)) sql.exec(`ALTER TABLE _gw_snapshot_outbox ADD COLUMN ${definition}`);
    }
    sql.exec(
        `UPDATE _gw_snapshot_outbox
         SET claim_token = NULL, claim_expires_at = NULL, claim_version = claim_version + 1
         WHERE (claim_token IS NULL) <> (claim_expires_at IS NULL)
            OR (claim_token IS NOT NULL AND claim_version = 0)`
    );
}

export function gatewayInvalidationInvariant(message: string, cause?: unknown): CdbError {
    return new CdbError({ code: "CDB_INVARIANT", message, ...(cause === undefined ? {} : { cause }) });
}
