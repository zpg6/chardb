import { CdbError } from "../../errors.ts";
import type { JsonText, SqlParam, SyncSql } from "../../oplog/wrapper.ts";
import type { RangeFilter } from "../../reshard/range.ts";
import { sha256Hex } from "../../util/canonical.ts";
import { VSHARD_COUNT, vshardOf } from "../../vshard.ts";
import {
    CDB_VECTOR_DELETE_UNPROVEN_TERMINAL_ERROR,
    CDB_VECTOR_MAX_DELETE_ID_BYTES,
    CDB_VECTOR_MAX_DIMENSIONS,
    CDB_VECTOR_MAX_ERROR_BYTES,
    CDB_VECTOR_MAX_METADATA_BYTES,
    CDB_VECTOR_MAX_VALUES_BYTES,
    validateCdbVectorDeletePhysicalIds,
} from "./cdb-vector-outbox-store.ts";
import {
    CdbVectorReshardProvenanceStore,
    type CdbVectorReshardRecordIdentity,
    cdbVectorReshardPhysicalRowFingerprint,
    initializeCdbVectorReshardProvenance,
} from "./cdb-vector-reshard-provenance.ts";
import { normalizeCdbVectorReshardCursor } from "./cdb-vector-reshard-records.ts";

const TEXT = new TextEncoder();
const ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const TOKEN = /^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$/;
const MAX_IMAGE_BYTES = 256 * 1_024;

export const CDB_VECTOR_HEAD_TAIL_TABLE = "_chardb_vectors";
export const CDB_VECTOR_OUTBOX_TAIL_TABLE = "_chardb_vector_outbox";
export const CDB_VECTOR_ATTEMPT_TAIL_TABLE = "_chardb_vector_attempts";

const VECTOR_TABLES = new Set([
    CDB_VECTOR_HEAD_TAIL_TABLE,
    CDB_VECTOR_OUTBOX_TAIL_TABLE,
    CDB_VECTOR_ATTEMPT_TAIL_TABLE,
]);

const HEAD_KEYS = [
    "created_seq",
    "delivered_version",
    "dimensions",
    "metadata_json",
    "organization_id",
    "placement_vshard",
    "resource_id",
    "row_pk",
    "state",
    "updated_at",
    "values_hex",
    "vector_id",
    "version",
] as const;
const OUTBOX_KEYS = [
    "accepted_at",
    "attempts",
    "last_error",
    "lease_token",
    "leased_until",
    "mutation_id",
    "next_attempt_at",
    "operation",
    "phase",
    "placement_vshard",
    "target_version",
    "terminal_failure",
    "vector_id",
    "verify_ids_json",
] as const;
const ATTEMPT_KEYS = [
    "delete_claim_token",
    "delete_confirmed",
    "first_sent_at",
    "physical_version",
    "placement_vshard",
    "response_ambiguous",
    "settle_after",
    "vector_id",
    "visibility_confirmed",
] as const;

export interface CdbVectorSystemTailEntry {
    readonly lsn: number;
    readonly op: "ins" | "upd" | "del";
    readonly table_name: string;
    readonly pk: string;
    readonly before: JsonText | null;
    readonly after: JsonText | null;
}

type HeadState = "pending" | "ready" | "deleting";

interface HeadImage extends Record<string, unknown> {
    readonly vector_id: string;
    readonly created_seq: number;
    readonly organization_id: string;
    readonly placement_vshard: number;
    readonly resource_id: string;
    readonly row_pk: string;
    readonly dimensions: number;
    readonly version: number;
    readonly delivered_version: number;
    readonly values_hex: string | null;
    readonly metadata_json: string;
    readonly state: HeadState;
    readonly updated_at: number;
}

interface OutboxImage extends Record<string, unknown> {
    readonly vector_id: string;
    readonly placement_vshard: number;
    readonly target_version: number;
    readonly operation: "upsert" | "delete";
    readonly phase: "submit" | "verify";
    readonly mutation_id: string | null;
    readonly accepted_at: number | null;
    readonly verify_ids_json: string | null;
    readonly attempts: number;
    readonly next_attempt_at: number;
    readonly leased_until: number | null;
    readonly lease_token: string | null;
    readonly terminal_failure: 0 | 1;
    readonly last_error: string | null;
}

interface AttemptImage extends Record<string, unknown> {
    readonly vector_id: string;
    readonly placement_vshard: number;
    readonly physical_version: number;
    readonly first_sent_at: number;
    readonly settle_after: number;
    readonly visibility_confirmed: 0 | 1;
    readonly response_ambiguous: 0 | 1;
    readonly delete_confirmed: 0 | 1;
    readonly delete_claim_token: string | null;
}

function mismatch(message: string): never {
    throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message: `vector tail: ${message}` });
}

function safeInteger(value: unknown, subject: string, minimum = 0): number {
    if (!Number.isSafeInteger(value) || (value as number) < minimum) mismatch(`${subject} is invalid`);
    return value as number;
}

function boundedText(value: unknown, subject: string, maxBytes = 256): string {
    if (typeof value !== "string" || value.length === 0 || TEXT.encode(value).byteLength > maxBytes) {
        mismatch(`${subject} is invalid`);
    }
    return value;
}

function identifier(value: unknown, subject: string): string {
    if (typeof value !== "string" || !ID.test(value)) mismatch(`${subject} is invalid`);
    return value;
}

function nullableText(value: unknown, subject: string, maxBytes: number): string | null {
    return value === null ? null : boundedText(value, subject, maxBytes);
}

function flag(value: unknown, subject: string): 0 | 1 {
    const projected = safeInteger(value, subject);
    if (projected !== 0 && projected !== 1) mismatch(`${subject} is invalid`);
    return projected;
}

function exactPlacement(organizationId: string, value: unknown): number {
    const placement = safeInteger(value, "placement virtual shard");
    if (placement >= VSHARD_COUNT || placement !== Number(vshardOf([organizationId]))) {
        mismatch("placement virtual shard does not match organization identity");
    }
    return placement;
}

function childPlacement(value: unknown): number {
    const placement = safeInteger(value, "child placement");
    if (placement >= VSHARD_COUNT) mismatch("child placement is invalid");
    return placement;
}

function parseExactImage(value: JsonText | null, keys: readonly string[], subject: string): Record<string, unknown> {
    if (value === null || typeof value !== "string") mismatch(`${subject} image is missing`);
    if (TEXT.encode(value).byteLength > MAX_IMAGE_BYTES) mismatch(`${subject} image exceeds its byte bound`);
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        mismatch(`${subject} image is not valid JSON`);
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) mismatch(`${subject} image is not an object`);
    const actual = Object.keys(parsed).sort();
    if (actual.length !== keys.length || actual.some((key, index) => key !== keys[index])) {
        mismatch(`${subject} image fields are not exact (${actual.join(",")})`);
    }
    return parsed as Record<string, unknown>;
}

function validateJsonValue(value: unknown, depth: number, budget: { nodes: number }): void {
    budget.nodes++;
    if (budget.nodes > 2_048 || depth > 16) mismatch("metadata structure exceeds its bound");
    if (value === null || typeof value === "boolean" || typeof value === "string") return;
    if (typeof value === "number" && Number.isFinite(value)) return;
    if (Array.isArray(value)) {
        for (const item of value) validateJsonValue(item, depth + 1, budget);
        return;
    }
    if (!value || typeof value !== "object") mismatch("metadata contains a non-JSON value");
    for (const [key, item] of Object.entries(value)) {
        if (TEXT.encode(key).byteLength > 256) mismatch("metadata key exceeds its byte bound");
        validateJsonValue(item, depth + 1, budget);
    }
}

function metadataJson(value: unknown): string {
    if (typeof value !== "string" || TEXT.encode(value).byteLength > CDB_VECTOR_MAX_METADATA_BYTES) {
        mismatch("metadata JSON exceeds its byte bound");
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        mismatch("metadata JSON is malformed");
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) mismatch("metadata JSON is not an object");
    validateJsonValue(parsed, 0, { nodes: 0 });
    return value;
}

function decodeValuesHex(value: unknown, dimensions: number): Uint8Array | null {
    if (value === null) return null;
    if (
        typeof value !== "string" ||
        value.length === 0 ||
        value.length > CDB_VECTOR_MAX_VALUES_BYTES * 2 ||
        value.length !== dimensions * 8 ||
        !/^(?:[0-9a-f]{2})+$/.test(value)
    ) {
        mismatch("embedding hex is not canonical or does not match dimensions");
    }
    const bytes = new Uint8Array(value.length / 2);
    for (let index = 0; index < bytes.length; index++)
        bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
    const view = new DataView(bytes.buffer);
    for (let offset = 0; offset < bytes.byteLength; offset += 4) {
        if (!Number.isFinite(view.getFloat32(offset, true))) mismatch("embedding contains a non-finite value");
    }
    return bytes;
}

function headImage(value: JsonText | null): HeadImage {
    const raw = parseExactImage(value, HEAD_KEYS, "head");
    const vectorId = identifier(raw.vector_id, "vector id");
    const organizationId = boundedText(raw.organization_id, "organization id");
    const dimensions = safeInteger(raw.dimensions, "dimensions", 1);
    if (dimensions > CDB_VECTOR_MAX_DIMENSIONS) mismatch("dimensions exceed their bound");
    const version = safeInteger(raw.version, "head version", 1);
    const deliveredVersion = safeInteger(raw.delivered_version, "delivered version");
    if (deliveredVersion > version) mismatch("delivered version exceeds head version");
    if (raw.state !== "pending" && raw.state !== "ready" && raw.state !== "deleting") mismatch("head state is invalid");
    if (
        (raw.state === "ready" && deliveredVersion !== version) ||
        (raw.state !== "ready" && deliveredVersion >= version)
    ) {
        mismatch("delivered version does not match head state");
    }
    const values = decodeValuesHex(raw.values_hex, dimensions);
    if ((raw.state === "deleting") !== (values === null)) mismatch("embedding does not match head state");
    return Object.freeze({
        vector_id: vectorId,
        created_seq: safeInteger(raw.created_seq, "head insertion generation", 1),
        organization_id: organizationId,
        placement_vshard: exactPlacement(organizationId, raw.placement_vshard),
        resource_id: identifier(raw.resource_id, "resource id"),
        row_pk: boundedText(raw.row_pk, "row primary key"),
        dimensions,
        version,
        delivered_version: deliveredVersion,
        values_hex: raw.values_hex as string | null,
        metadata_json: metadataJson(raw.metadata_json),
        state: raw.state,
        updated_at: safeInteger(raw.updated_at, "update time"),
    });
}

function outboxImage(value: JsonText | null): OutboxImage {
    const raw = parseExactImage(value, OUTBOX_KEYS, "outbox");
    if (raw.operation !== "upsert" && raw.operation !== "delete") mismatch("outbox operation is invalid");
    if (raw.phase !== "submit" && raw.phase !== "verify") mismatch("outbox phase is invalid");
    const mutationId = nullableText(raw.mutation_id, "mutation id", 128);
    const acceptedAt = raw.accepted_at === null ? null : safeInteger(raw.accepted_at, "acceptance time");
    if (
        (raw.phase === "submit" && (mutationId !== null || acceptedAt !== null)) ||
        (raw.phase === "verify" && (mutationId === null || acceptedAt === null))
    ) {
        mismatch("outbox receipt does not match phase");
    }
    const leasedUntil = raw.leased_until === null ? null : safeInteger(raw.leased_until, "lease deadline");
    const leaseToken = raw.lease_token === null ? null : identifier(raw.lease_token, "lease token");
    if ((leasedUntil === null) !== (leaseToken === null) || (leaseToken !== null && !TOKEN.test(leaseToken))) {
        mismatch("outbox lease identity is invalid");
    }
    const verifyIdsJson = nullableText(raw.verify_ids_json, "verification ids", CDB_VECTOR_MAX_DELETE_ID_BYTES);
    if (raw.operation === "upsert" && verifyIdsJson !== null) mismatch("upsert outbox has verification ids");
    const terminalFailure = safeInteger(raw.terminal_failure, "outbox terminal failure state");
    if (terminalFailure > 1) mismatch("outbox terminal failure state is invalid");
    if (terminalFailure === 1 && leaseToken !== null) mismatch("terminally failed outbox cannot retain a lease");
    if (
        terminalFailure === 1 &&
        (raw.operation !== "delete" || raw.last_error !== CDB_VECTOR_DELETE_UNPROVEN_TERMINAL_ERROR)
    ) {
        mismatch("terminally failed outbox shape is invalid");
    }
    return Object.freeze({
        vector_id: identifier(raw.vector_id, "vector id"),
        placement_vshard: childPlacement(raw.placement_vshard),
        target_version: safeInteger(raw.target_version, "target version", 1),
        operation: raw.operation,
        phase: raw.phase,
        mutation_id: mutationId,
        accepted_at: acceptedAt,
        verify_ids_json: verifyIdsJson,
        attempts: safeInteger(raw.attempts, "attempt count"),
        next_attempt_at: safeInteger(raw.next_attempt_at, "next attempt time"),
        leased_until: leasedUntil,
        lease_token: leaseToken,
        terminal_failure: terminalFailure as 0 | 1,
        last_error: nullableText(raw.last_error, "last error", CDB_VECTOR_MAX_ERROR_BYTES),
    });
}

function attemptImage(value: JsonText | null, subject = "attempt"): AttemptImage {
    const raw = parseExactImage(value, ATTEMPT_KEYS, subject);
    const firstSentAt = safeInteger(raw.first_sent_at, "first sent time");
    const settleAfter = safeInteger(raw.settle_after, "settlement time");
    if (settleAfter < firstSentAt) mismatch("settlement time predates first send");
    const deleteClaimToken =
        raw.delete_claim_token === null ? null : identifier(raw.delete_claim_token, "delete claim token");
    if (deleteClaimToken !== null && !TOKEN.test(deleteClaimToken)) mismatch("delete claim token is invalid");
    return Object.freeze({
        vector_id: identifier(raw.vector_id, "vector id"),
        placement_vshard: childPlacement(raw.placement_vshard),
        physical_version: safeInteger(raw.physical_version, "physical version", 1),
        first_sent_at: firstSentAt,
        settle_after: settleAfter,
        visibility_confirmed: flag(raw.visibility_confirmed, "visibility confirmation"),
        response_ambiguous: flag(raw.response_ambiguous, "response ambiguity"),
        delete_confirmed: flag(raw.delete_confirmed, "delete confirmation"),
        delete_claim_token: deleteClaimToken,
    });
}

function exact(left: Record<string, unknown>, right: Record<string, unknown>): boolean {
    const keys = Object.keys(left);
    return keys.length === Object.keys(right).length && keys.every(key => Object.is(left[key], right[key]));
}

function exactChild(left: Record<string, unknown>, right: Record<string, unknown>): boolean {
    const { placement_vshard: _leftPlacement, ...leftPhysical } = left;
    const { placement_vshard: _rightPlacement, ...rightPhysical } = right;
    return exact(leftPhysical, rightPhysical);
}

function exactHeadPhysical(left: HeadImage, right: HeadImage): boolean {
    const { created_seq: _leftSequence, ...leftPhysical } = left;
    const { created_seq: _rightSequence, ...rightPhysical } = right;
    return exact(leftPhysical, rightPhysical);
}

function allocateHeadSequence(sql: SyncSql): number {
    const row = sql.one<{ last_seq: number | bigint }>(
        "SELECT last_seq FROM _chardb_vector_head_sequence WHERE singleton = 1"
    );
    const current = Number(row?.last_seq);
    if (!Number.isSafeInteger(current) || current < 0 || current >= Number.MAX_SAFE_INTEGER) {
        mismatch("destination head insertion sequence is exhausted");
    }
    const next = current + 1;
    sql.exec(
        "UPDATE _chardb_vector_head_sequence SET last_seq = ? WHERE singleton = 1 AND last_seq = ?",
        next,
        current
    );
    if (sql.changes() !== 1) mismatch("destination head insertion sequence changed");
    return next;
}

function hexBytes(value: string | null): Uint8Array | null {
    if (value === null) return null;
    const bytes = new Uint8Array(value.length / 2);
    for (let index = 0; index < bytes.length; index++)
        bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
    return bytes;
}

function readHead(sql: SyncSql, vectorId: string): HeadImage | null {
    const row = sql.one<Record<string, unknown>>(
        `SELECT vector_id, created_seq, organization_id, placement_vshard, resource_id, row_pk, dimensions,
                version, delivered_version,
                CASE WHEN values_enc IS NULL THEN NULL ELSE lower(hex(values_enc)) END AS values_hex,
                metadata_json, state, updated_at
         FROM _chardb_vectors WHERE vector_id = ?`,
        vectorId
    );
    return row ? headImage(JSON.stringify(row) as JsonText) : null;
}

function readOutbox(sql: SyncSql, vectorId: string): OutboxImage | null {
    const row = sql.one<Record<string, unknown>>(
        `SELECT outbox.*, head.placement_vshard
         FROM _chardb_vector_outbox AS outbox
         JOIN _chardb_vectors AS head ON head.vector_id = outbox.vector_id
         WHERE outbox.vector_id = ?`,
        vectorId
    );
    return row ? outboxImage(JSON.stringify(row) as JsonText) : null;
}

function readAttempt(sql: SyncSql, vectorId: string, physicalVersion: number): AttemptImage | null {
    const row = sql.one<Record<string, unknown>>(
        `SELECT attempt.*, head.placement_vshard
         FROM _chardb_vector_attempts AS attempt
         JOIN _chardb_vectors AS head ON head.vector_id = attempt.vector_id
         WHERE attempt.vector_id = ? AND attempt.physical_version = ?`,
        vectorId,
        physicalVersion
    );
    return row ? attemptImage(JSON.stringify(row) as JsonText) : null;
}

function assertRange(placement: number, range: RangeFilter): void {
    if (placement < range.lo || placement > range.hi) mismatch("placement is outside the moving range");
}

function assertHeadTransition(before: HeadImage, after: HeadImage): void {
    for (const key of [
        "vector_id",
        "created_seq",
        "organization_id",
        "placement_vshard",
        "resource_id",
        "row_pk",
        "dimensions",
    ] as const) {
        if (!Object.is(before[key], after[key])) mismatch(`head changed immutable ${key}`);
    }
    if (after.version === before.version) {
        if (
            before.state !== "pending" ||
            after.state !== "ready" ||
            after.delivered_version !== after.version ||
            before.values_hex !== after.values_hex ||
            before.metadata_json !== after.metadata_json
        ) {
            mismatch("head same-version transition is not a delivery acknowledgement");
        }
        return;
    }
    if (
        after.version !== before.version + 1 ||
        after.delivered_version !== before.delivered_version ||
        (after.state !== "pending" && after.state !== "deleting")
    ) {
        mismatch("head version transition is not a single staged mutation");
    }
    if (after.state === "deleting" && before.metadata_json !== after.metadata_json) {
        mismatch("head delete staging changed metadata");
    }
}

function assertOutboxAgainstHead(sql: SyncSql, image: OutboxImage, range: RangeFilter): void {
    const head = readHead(sql, image.vector_id);
    if (!head) mismatch(`outbox ${image.vector_id} has no vector head`);
    assertRange(head.placement_vshard, range);
    if (image.placement_vshard !== head.placement_vshard) mismatch("outbox placement does not match its head");
    if (image.target_version !== head.version) mismatch("outbox target does not match head version");
    if (
        (image.operation === "upsert" && head.state !== "pending" && head.state !== "ready") ||
        (image.operation === "delete" && head.state !== "ready" && head.state !== "deleting")
    ) {
        mismatch("outbox operation does not match head state");
    }
    if (image.verify_ids_json !== null) {
        let ids: unknown;
        try {
            ids = JSON.parse(image.verify_ids_json);
        } catch {
            mismatch("verification ids are malformed");
        }
        try {
            validateCdbVectorDeletePhysicalIds(ids, {
                resourceId: head.resource_id,
                vectorId: head.vector_id,
                targetVersion: image.target_version,
            });
        } catch {
            mismatch("verification ids are invalid");
        }
    }
}

function assertAttemptAgainstHead(sql: SyncSql, image: AttemptImage, range: RangeFilter): void {
    const head = readHead(sql, image.vector_id);
    if (!head) mismatch(`attempt ${image.vector_id} has no vector head`);
    assertRange(head.placement_vshard, range);
    if (image.placement_vshard !== head.placement_vshard) mismatch("attempt placement does not match its head");
    if (image.physical_version > head.version) mismatch("attempt physical version exceeds head version");
}

function fingerprint(entry: CdbVectorSystemTailEntry): string {
    return sha256Hex(JSON.stringify([entry.table_name, entry.op, entry.pk, entry.before, entry.after]));
}

function tailProvenance(
    entry: CdbVectorSystemTailEntry
): CdbVectorReshardRecordIdentity & { readonly placementVshard: number; readonly imageFingerprint: string | null } {
    if (entry.table_name === CDB_VECTOR_HEAD_TAIL_TABLE) {
        const image = entry.op === "del" ? headImage(entry.before) : headImage(entry.after);
        return Object.freeze({
            kind: "head",
            vectorId: image.vector_id,
            physicalVersion: 0,
            placementVshard: image.placement_vshard,
            imageFingerprint: entry.op === "del" ? null : cdbVectorReshardPhysicalRowFingerprint("head", image),
        });
    }
    if (entry.table_name === CDB_VECTOR_OUTBOX_TAIL_TABLE) {
        const image = entry.op === "del" ? outboxImage(entry.before) : outboxImage(entry.after);
        return Object.freeze({
            kind: "outbox",
            vectorId: image.vector_id,
            physicalVersion: 0,
            placementVshard: image.placement_vshard,
            imageFingerprint: entry.op === "del" ? null : cdbVectorReshardPhysicalRowFingerprint("outbox", image),
        });
    }
    const image = entry.op === "del" ? attemptImage(entry.before) : attemptImage(entry.after);
    return Object.freeze({
        kind: "attempt",
        vectorId: image.vector_id,
        physicalVersion: image.physical_version,
        placementVshard: image.placement_vshard,
        imageFingerprint: entry.op === "del" ? null : cdbVectorReshardPhysicalRowFingerprint("attempt", image),
    });
}

function assertEntryIdentity(migId: string, entry: CdbVectorSystemTailEntry): void {
    if (typeof migId !== "string" || migId.length === 0 || TEXT.encode(migId).byteLength > 256) {
        mismatch("migration identity is invalid");
    }
    if (!Number.isSafeInteger(entry.lsn) || entry.lsn < 1) mismatch("LSN is invalid");
    if (entry.op !== "ins" && entry.op !== "upd" && entry.op !== "del") mismatch("operation is invalid");
    boundedText(entry.pk, "primary key");
}

/** Validate the full captured envelope before any snapshot-coverage fast path can accept it. */
function assertEntryEnvelope(entry: CdbVectorSystemTailEntry): void {
    if (entry.table_name === CDB_VECTOR_HEAD_TAIL_TABLE) {
        if (entry.op === "ins") {
            if (entry.before !== null) mismatch("head insert includes a pre-image");
            const after = headImage(entry.after);
            if (entry.pk !== after.vector_id) mismatch("head primary key changed");
            if (after.version !== 1 || after.delivered_version !== 0 || after.state !== "pending") {
                mismatch("head insert does not begin in pending version one");
            }
            return;
        }
        const before = headImage(entry.before);
        if (entry.pk !== before.vector_id) mismatch("head primary key changed");
        if (entry.op === "del") {
            if (entry.after !== null) mismatch("head delete includes a post-image");
            if (before.state !== "deleting") mismatch("head delete does not own deleting state");
            return;
        }
        const after = headImage(entry.after);
        if (entry.pk !== after.vector_id) mismatch("head primary key changed");
        assertHeadTransition(before, after);
        return;
    }
    if (entry.table_name === CDB_VECTOR_OUTBOX_TAIL_TABLE) {
        const before = entry.op === "ins" ? null : outboxImage(entry.before);
        const after = entry.op === "del" ? null : outboxImage(entry.after);
        if ((entry.op === "ins" && entry.before !== null) || (entry.op === "del" && entry.after !== null)) {
            mismatch(`outbox ${entry.op === "ins" ? "insert includes a pre-image" : "delete includes a post-image"}`);
        }
        const image = before ?? after;
        if (!image || entry.pk !== image.vector_id) mismatch("outbox primary key changed");
        if (
            before &&
            after &&
            (before.vector_id !== after.vector_id || before.placement_vshard !== after.placement_vshard)
        ) {
            mismatch("outbox update changed identity or placement");
        }
        return;
    }
    const before = entry.op === "ins" ? null : attemptImage(entry.before, `attempt before LSN ${entry.lsn}`);
    const after = entry.op === "del" ? null : attemptImage(entry.after, `attempt after LSN ${entry.lsn}`);
    if ((entry.op === "ins" && entry.before !== null) || (entry.op === "del" && entry.after !== null)) {
        mismatch(`attempt ${entry.op === "ins" ? "insert includes a pre-image" : "delete includes a post-image"}`);
    }
    const [vectorId, physicalVersion] = attemptPk(entry.pk);
    const image = before ?? after;
    if (!image || vectorId !== image.vector_id || physicalVersion !== image.physical_version) {
        mismatch("attempt primary key changed");
    }
    if (
        before &&
        after &&
        (before.vector_id !== after.vector_id ||
            before.physical_version !== after.physical_version ||
            before.placement_vshard !== after.placement_vshard)
    ) {
        mismatch("attempt update changed identity or placement");
    }
}

function applyHead(sql: SyncSql, entry: CdbVectorSystemTailEntry, range: RangeFilter, hasProvenance: boolean): boolean {
    if (entry.op === "ins") {
        if (entry.before !== null) mismatch("head insert includes a pre-image");
        const after = headImage(entry.after);
        if (after.version !== 1 || after.delivered_version !== 0 || after.state !== "pending") {
            mismatch("head insert does not begin in pending version one");
        }
        if (entry.pk !== after.vector_id) mismatch("head primary key changed");
        assertRange(after.placement_vshard, range);
        const existing = readHead(sql, after.vector_id);
        if (existing) {
            if (!exactHeadPhysical(existing, after)) mismatch(`head ${after.vector_id} collides during insert`);
            if (!hasProvenance) mismatch(`head ${after.vector_id} predates this migration`);
            return false;
        }
        const destinationSequence = allocateHeadSequence(sql);
        sql.exec(
            `INSERT INTO _chardb_vectors
               (vector_id, created_seq, organization_id, placement_vshard, resource_id, row_pk, dimensions,
                version, delivered_version, values_enc, metadata_json, state, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            after.vector_id,
            destinationSequence,
            after.organization_id,
            after.placement_vshard,
            after.resource_id,
            after.row_pk,
            after.dimensions,
            after.version,
            after.delivered_version,
            hexBytes(after.values_hex),
            after.metadata_json,
            after.state,
            after.updated_at
        );
        return true;
    }

    const before = headImage(entry.before);
    if (entry.pk !== before.vector_id) mismatch("head primary key changed");
    assertRange(before.placement_vshard, range);
    const existing = readHead(sql, before.vector_id);
    if (entry.op === "del") {
        if (entry.after !== null) mismatch("head delete includes a post-image");
        if (before.state !== "deleting") mismatch("head delete does not own deleting state");
        if (!existing) return false;
        if (!exactHeadPhysical(existing, before)) mismatch(`head ${before.vector_id} pre-image changed during delete`);
        sql.exec("DELETE FROM _chardb_vectors WHERE vector_id = ?", before.vector_id);
        if (sql.changes() !== 1) mismatch(`head ${before.vector_id} changed during delete`);
        return false;
    }

    const after = headImage(entry.after);
    if (entry.pk !== after.vector_id) mismatch("head primary key changed");
    assertHeadTransition(before, after);
    if (existing && exactHeadPhysical(existing, after)) return false;
    if (!existing || !exactHeadPhysical(existing, before))
        mismatch(`head ${before.vector_id} pre-image changed during update`);
    sql.exec(
        `UPDATE _chardb_vectors SET delivered_version = ?, values_enc = ?, metadata_json = ?, state = ?,
                version = ?, updated_at = ? WHERE vector_id = ?`,
        after.delivered_version,
        hexBytes(after.values_hex),
        after.metadata_json,
        after.state,
        after.version,
        after.updated_at,
        after.vector_id
    );
    if (sql.changes() !== 1) mismatch(`head ${after.vector_id} changed during update`);
    return false;
}

function outboxParams(image: OutboxImage): readonly SqlParam[] {
    return [
        image.vector_id,
        image.target_version,
        image.operation,
        image.phase,
        image.mutation_id,
        image.accepted_at,
        image.verify_ids_json,
        image.attempts,
        image.next_attempt_at,
        image.leased_until,
        image.lease_token,
        image.terminal_failure,
        image.last_error,
    ];
}

function applyOutbox(
    sql: SyncSql,
    entry: CdbVectorSystemTailEntry,
    range: RangeFilter,
    hasProvenance: boolean
): boolean {
    const image = entry.op === "ins" ? outboxImage(entry.after) : outboxImage(entry.before);
    if (entry.pk !== image.vector_id) mismatch("outbox primary key changed");
    const existing = readOutbox(sql, image.vector_id);
    if (entry.op === "del" && !existing) {
        if (entry.after !== null) mismatch("outbox delete includes a post-image");
        return false;
    }
    if (entry.op === "ins") {
        assertOutboxAgainstHead(sql, image, range);
        if (entry.before !== null) mismatch("outbox insert includes a pre-image");
        if (existing) {
            if (!exactChild(existing, image)) mismatch(`outbox ${image.vector_id} collides during insert`);
            if (!hasProvenance) mismatch(`outbox ${image.vector_id} predates this migration`);
            return false;
        }
        sql.exec(
            `INSERT INTO _chardb_vector_outbox
               (vector_id, target_version, operation, phase, mutation_id, accepted_at, verify_ids_json,
                attempts, next_attempt_at, leased_until, lease_token, terminal_failure, last_error)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            ...outboxParams(image)
        );
        return true;
    }
    if (entry.op === "del") {
        assertOutboxAgainstHead(sql, image, range);
        if (entry.after !== null) mismatch("outbox delete includes a post-image");
        if (!existing || !exactChild(existing, image))
            mismatch(`outbox ${image.vector_id} pre-image changed during delete`);
        sql.exec("DELETE FROM _chardb_vector_outbox WHERE vector_id = ?", image.vector_id);
        if (sql.changes() !== 1) mismatch(`outbox ${image.vector_id} changed during delete`);
        return false;
    }
    const after = outboxImage(entry.after);
    if (entry.pk !== after.vector_id || image.vector_id !== after.vector_id) mismatch("outbox primary key changed");
    assertOutboxAgainstHead(sql, after, range);
    if (existing && exactChild(existing, after)) return false;
    if (!existing || !exactChild(existing, image))
        mismatch(`outbox ${image.vector_id} pre-image changed during update`);
    sql.exec(
        `UPDATE _chardb_vector_outbox SET target_version = ?, operation = ?, phase = ?, mutation_id = ?,
                accepted_at = ?, verify_ids_json = ?, attempts = ?, next_attempt_at = ?, leased_until = ?,
                lease_token = ?, terminal_failure = ?, last_error = ? WHERE vector_id = ?`,
        after.target_version,
        after.operation,
        after.phase,
        after.mutation_id,
        after.accepted_at,
        after.verify_ids_json,
        after.attempts,
        after.next_attempt_at,
        after.leased_until,
        after.lease_token,
        after.terminal_failure,
        after.last_error,
        after.vector_id
    );
    if (sql.changes() !== 1) mismatch(`outbox ${after.vector_id} changed during update`);
    return false;
}

function attemptPk(value: string): readonly [string, number] {
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        mismatch("attempt primary key is malformed");
    }
    if (
        !Array.isArray(parsed) ||
        parsed.length !== 2 ||
        typeof parsed[0] !== "string" ||
        !Number.isSafeInteger(parsed[1]) ||
        JSON.stringify(parsed) !== value
    ) {
        mismatch("attempt primary key is not canonical");
    }
    return [identifier(parsed[0], "attempt vector id"), safeInteger(parsed[1], "attempt physical version", 1)];
}

function attemptParams(image: AttemptImage): readonly SqlParam[] {
    return [
        image.vector_id,
        image.physical_version,
        image.first_sent_at,
        image.settle_after,
        image.visibility_confirmed,
        image.response_ambiguous,
        image.delete_confirmed,
        image.delete_claim_token,
    ];
}

function applyAttempt(
    sql: SyncSql,
    entry: CdbVectorSystemTailEntry,
    range: RangeFilter,
    hasProvenance: boolean
): boolean {
    const image =
        entry.op === "ins"
            ? attemptImage(entry.after, `attempt after LSN ${entry.lsn}`)
            : attemptImage(entry.before, `attempt before LSN ${entry.lsn}`);
    const [vectorId, physicalVersion] = attemptPk(entry.pk);
    if (vectorId !== image.vector_id || physicalVersion !== image.physical_version)
        mismatch("attempt primary key changed");
    const existing = readAttempt(sql, vectorId, physicalVersion);
    if (entry.op === "del" && !existing) {
        if (entry.after !== null) mismatch("attempt delete includes a post-image");
        return false;
    }
    assertAttemptAgainstHead(sql, image, range);
    if (entry.op === "ins") {
        if (entry.before !== null) mismatch("attempt insert includes a pre-image");
        if (existing) {
            if (!exactChild(existing, image)) mismatch(`attempt ${entry.pk} collides during insert`);
            if (!hasProvenance) mismatch(`attempt ${entry.pk} predates this migration`);
            return false;
        }
        sql.exec(
            `INSERT INTO _chardb_vector_attempts
               (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                response_ambiguous, delete_confirmed, delete_claim_token)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            ...attemptParams(image)
        );
        return true;
    }
    if (entry.op === "del") {
        if (entry.after !== null) mismatch("attempt delete includes a post-image");
        if (!existing || !exactChild(existing, image)) mismatch(`attempt ${entry.pk} pre-image changed during delete`);
        sql.exec(
            "DELETE FROM _chardb_vector_attempts WHERE vector_id = ? AND physical_version = ?",
            vectorId,
            physicalVersion
        );
        if (sql.changes() !== 1) mismatch(`attempt ${entry.pk} changed during delete`);
        return false;
    }
    const after = attemptImage(entry.after, `attempt after LSN ${entry.lsn}`);
    if (after.vector_id !== vectorId || after.physical_version !== physicalVersion)
        mismatch("attempt primary key changed");
    assertAttemptAgainstHead(sql, after, range);
    if (existing && exactChild(existing, after)) return false;
    if (!existing || !exactChild(existing, image)) mismatch(`attempt ${entry.pk} pre-image changed during update`);
    sql.exec(
        `UPDATE _chardb_vector_attempts SET first_sent_at = ?, settle_after = ?, visibility_confirmed = ?,
                response_ambiguous = ?, delete_confirmed = ?, delete_claim_token = ?
         WHERE vector_id = ? AND physical_version = ?`,
        after.first_sent_at,
        after.settle_after,
        after.visibility_confirmed,
        after.response_ambiguous,
        after.delete_confirmed,
        after.delete_claim_token,
        vectorId,
        physicalVersion
    );
    if (sql.changes() !== 1) mismatch(`attempt ${entry.pk} changed during update`);
    return false;
}

export function initializeCdbVectorReshardTailStore(sql: SyncSql): void {
    initializeCdbVectorReshardProvenance(sql);
}

export function isCdbVectorTailTable(tableName: string): boolean {
    return VECTOR_TABLES.has(tableName);
}

/** Validate an already-applied vector entry before the destination cursor skips it. */
export function assertCdbVectorTailReplay(
    sql: SyncSql,
    migId: string,
    entry: CdbVectorSystemTailEntry,
    range: RangeFilter
): boolean {
    if (!isCdbVectorTailTable(entry.table_name)) return false;
    assertEntryIdentity(migId, entry);
    assertEntryEnvelope(entry);
    const store = new CdbVectorReshardProvenanceStore(sql);
    store.assertReceipt(
        { migId, rangeLo: range.lo, rangeHi: range.hi },
        { lsn: entry.lsn, tableName: entry.table_name, fingerprint: fingerprint(entry) }
    );
    return true;
}

/** Apply one exact vector head, outbox, or attempt transition and retain its byte fingerprint. */
export function applyCdbVectorTailEntry(
    sql: SyncSql,
    migId: string,
    entry: CdbVectorSystemTailEntry,
    range: RangeFilter
): boolean {
    if (!isCdbVectorTailTable(entry.table_name)) return false;
    assertEntryIdentity(migId, entry);
    assertEntryEnvelope(entry);
    const identity = { migId, rangeLo: range.lo, rangeHi: range.hi };
    const store = new CdbVectorReshardProvenanceStore(sql);
    store.bind(identity);
    const receiptInput = { lsn: entry.lsn, tableName: entry.table_name, fingerprint: fingerprint(entry) };
    if (store.hasReceipt(identity, receiptInput)) return true;
    const provenance = tailProvenance(entry);
    assertRange(provenance.placementVshard, range);
    const prior = store.read(identity, provenance);
    if (
        prior?.snapshotThroughLsn !== null &&
        prior?.snapshotThroughLsn !== undefined &&
        prior.snapshotThroughLsn >= entry.lsn
    ) {
        store.recordReceipt(identity, receiptInput);
        return true;
    }
    if (
        prior === null &&
        entry.op !== "ins" &&
        store.coversSnapshotAbsence(identity, {
            ...provenance,
            lsn: entry.lsn,
        })
    ) {
        store.recordReceipt(identity, receiptInput);
        return true;
    }
    if (prior === null && entry.op !== "ins") {
        mismatch("missing transition has no snapshot absence coverage");
    }
    const inserted =
        entry.table_name === CDB_VECTOR_HEAD_TAIL_TABLE
            ? applyHead(sql, entry, range, prior !== null)
            : entry.table_name === CDB_VECTOR_OUTBOX_TAIL_TABLE
              ? applyOutbox(sql, entry, range, prior !== null)
              : applyAttempt(sql, entry, range, prior !== null);
    store.recordTail(identity, {
        ...provenance,
        lsn: entry.lsn,
        present: provenance.imageFingerprint !== null,
        inserted,
    });
    store.recordReceipt(identity, receiptInput);
    if (
        entry.op === "del" &&
        sql.one<{ present: number }>(
            `SELECT 1 AS present FROM sqlite_master
             WHERE type = 'table' AND name = '_chardb_vector_reshard_dest_sessions'`
        )
    ) {
        const session = sql.one<{ terminal: number; expected_cursor_json: string }>(
            `SELECT terminal, expected_cursor_json FROM _chardb_vector_reshard_dest_sessions
             WHERE mig_id = ? AND range_lo = ? AND range_hi = ?`,
            migId,
            range.lo,
            range.hi
        );
        if (session?.terminal === 1) {
            let rawCursor: unknown;
            try {
                rawCursor = JSON.parse(session.expected_cursor_json);
            } catch {
                mismatch("destination snapshot cursor is malformed during tombstone compaction");
            }
            const snapshotCursor = normalizeCdbVectorReshardCursor(rawCursor);
            if (snapshotCursor.kind !== "done") mismatch("terminal destination snapshot cursor is not done");
            store.compactTombstones(identity, {
                snapshotCursor,
                acknowledgedThroughLsn: entry.lsn,
                limit: 500,
            });
        }
    }
    return true;
}
