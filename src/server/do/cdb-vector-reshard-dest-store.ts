import { CdbError } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import { sha256Hex } from "../../util/canonical.ts";
import { CDB_SPLIT_IDENTITY_LIMIT } from "./cdb-reshard-identity-store.ts";
import { CDB_RESHARD_MAX_BATCH_BYTES } from "./cdb-reshard-relational.ts";
import {
    CDB_VECTOR_MAX_ATTEMPT_ROWS,
    CDB_VECTOR_MAX_ATTEMPT_VERSIONS,
    CDB_VECTOR_MAX_HEADS,
    CDB_VECTOR_MAX_OUTBOX_ROWS,
    CDB_VECTOR_MAX_STORED_BYTES,
} from "./cdb-vector-outbox-store.ts";
import {
    CDB_VECTOR_RESHARD_ABORT_START_CURSOR,
    type CdbVectorReshardAbortCursor,
    type CdbVectorReshardProvenance,
    CdbVectorReshardProvenanceStore,
    type CdbVectorReshardRecordIdentity,
    cdbVectorReshardPhysicalRowFingerprint,
    cdbVectorReshardSnapshotRecordFingerprint,
    initializeCdbVectorReshardProvenance,
} from "./cdb-vector-reshard-provenance.ts";
import {
    CDB_VECTOR_RESHARD_PAGE_SIZE,
    CDB_VECTOR_RESHARD_PARITY_START_CURSOR,
    type CdbVectorReshardAttemptRecord,
    type CdbVectorReshardCursor,
    type CdbVectorReshardHeadRecord,
    type CdbVectorReshardIdentity,
    type CdbVectorReshardOutboxRecord,
    type CdbVectorReshardRecord,
    CdbVectorReshardSnapshotReader,
    decodeCdbVectorReshardPage,
    encodeCdbVectorReshardPage,
    normalizeCdbVectorReshardCursor,
} from "./cdb-vector-reshard-records.ts";

const TEXT = new TextEncoder();
const DIGEST = /^[a-f0-9]{64}$/;
const KIND_ORDER = Object.freeze({ head: 0, outbox: 1, attempt: 2, done: 3 } as const);
const PARITY_START_JSON = JSON.stringify(CDB_VECTOR_RESHARD_PARITY_START_CURSOR);
const ABORT_START_JSON = JSON.stringify(CDB_VECTOR_RESHARD_ABORT_START_CURSOR);

export const CDB_VECTOR_RESHARD_DEST_DDL = `
CREATE TABLE IF NOT EXISTS _chardb_vector_reshard_dest_sessions (
  mig_id                    TEXT PRIMARY KEY,
  range_lo                  INTEGER NOT NULL CHECK (range_lo BETWEEN 0 AND 16383),
  range_hi                  INTEGER NOT NULL CHECK (range_hi BETWEEN range_lo AND 16383),
  through_head_seq          INTEGER NOT NULL CHECK (through_head_seq BETWEEN 0 AND 9007199254740991),
  expected_cursor_json      TEXT NOT NULL,
  next_page_number          INTEGER NOT NULL CHECK (next_page_number BETWEEN 0 AND 9007199254740991),
  last_page_number          INTEGER,
  last_input_cursor_json    TEXT,
  last_page_digest          TEXT,
  last_page_enc             TEXT,
  last_applied              INTEGER NOT NULL DEFAULT 0 CHECK (last_applied BETWEEN 0 AND 500),
  last_inserted             INTEGER NOT NULL DEFAULT 0 CHECK (last_inserted BETWEEN 0 AND 500),
  last_skipped              INTEGER NOT NULL DEFAULT 0 CHECK (last_skipped BETWEEN 0 AND 500),
  last_through_lsn          INTEGER NOT NULL DEFAULT 0 CHECK (last_through_lsn >= 0),
  terminal                  INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
  parity_cursor_json        TEXT NOT NULL DEFAULT '${PARITY_START_JSON}',
  parity_page_number        INTEGER NOT NULL DEFAULT 0 CHECK (parity_page_number BETWEEN 0 AND 9007199254740991),
  parity_through_lsn        INTEGER CHECK (parity_through_lsn IS NULL OR parity_through_lsn >= 0),
  parity_complete           INTEGER NOT NULL DEFAULT 0 CHECK (parity_complete IN (0, 1)),
  parity_last_page_number   INTEGER,
  parity_last_input_cursor_json TEXT,
  parity_last_page_digest   TEXT,
  abort_cursor_json         TEXT NOT NULL DEFAULT '${ABORT_START_JSON}',
  outcome                   TEXT NOT NULL DEFAULT 'active'
                            CHECK (outcome IN ('active', 'aborting', 'aborted', 'finalized', 'cleaned')),
  cleaned                   INTEGER NOT NULL DEFAULT 0 CHECK (cleaned IN (0, 1)),
  CHECK ((last_page_number IS NULL AND last_input_cursor_json IS NULL AND last_page_digest IS NULL
      AND last_page_enc IS NULL) OR (last_page_number IS NOT NULL AND last_input_cursor_json IS NOT NULL
      AND last_page_digest IS NOT NULL AND last_page_enc IS NOT NULL)),
  CHECK (last_page_number IS NULL OR last_page_number + 1 = next_page_number),
  CHECK (cleaned = 0 OR (last_page_number IS NULL AND last_input_cursor_json IS NULL
      AND last_page_digest IS NULL AND last_page_enc IS NULL)),
  CHECK (parity_complete = 0 OR parity_through_lsn IS NOT NULL),
  CHECK ((parity_last_page_number IS NULL AND parity_last_input_cursor_json IS NULL
      AND parity_last_page_digest IS NULL) OR (parity_last_page_number IS NOT NULL
      AND parity_last_input_cursor_json IS NOT NULL AND parity_last_page_digest IS NOT NULL)),
  CHECK ((outcome = 'cleaned') = (cleaned = 1)),
  CHECK (last_page_enc IS NULL OR length(CAST(last_page_enc AS BLOB)) <= ${CDB_RESHARD_MAX_BATCH_BYTES})
);
` as const;

interface StoredSession {
    readonly range_lo: number | bigint;
    readonly range_hi: number | bigint;
    readonly through_head_seq: number | bigint;
    readonly expected_cursor_json: string;
    readonly next_page_number: number | bigint;
    readonly last_page_number: number | bigint | null;
    readonly last_input_cursor_json: string | null;
    readonly last_page_digest: string | null;
    readonly last_page_enc: string | null;
    readonly last_applied: number | bigint;
    readonly last_inserted: number | bigint;
    readonly last_skipped: number | bigint;
    readonly last_through_lsn: number | bigint;
    readonly terminal: number | bigint;
    readonly parity_cursor_json: string;
    readonly parity_page_number: number | bigint;
    readonly parity_through_lsn: number | bigint | null;
    readonly parity_complete: number | bigint;
    readonly parity_last_page_number: number | bigint | null;
    readonly parity_last_input_cursor_json: string | null;
    readonly parity_last_page_digest: string | null;
    readonly abort_cursor_json: string;
    readonly outcome: "active" | "aborting" | "aborted" | "finalized" | "cleaned";
    readonly cleaned: number | bigint;
}

export interface CdbVectorReshardDestRequest {
    readonly pageNumber: number;
    readonly cursor: CdbVectorReshardCursor;
    readonly encodedPage: string;
    readonly throughLsn: number;
}

export interface CdbVectorReshardDestResult {
    readonly applied: number;
    readonly inserted: number;
    readonly skipped: number;
    readonly replayed: boolean;
    readonly next: CdbVectorReshardCursor;
    readonly done: boolean;
}

export interface CdbVectorReshardParityRequest {
    readonly pageNumber: number;
    readonly cursor: CdbVectorReshardCursor;
    readonly encodedSourcePage: string;
    readonly throughLsn: number;
}

export interface CdbVectorReshardAbortResult {
    readonly deleted: number;
    readonly next: CdbVectorReshardAbortCursor;
    readonly done: boolean;
}

function mismatch(message: string): never {
    throw new CdbError({ code: "CDB_RESHARD_PHASE_MISMATCH", message: `vector reshard destination: ${message}` });
}

function invalid(message: string): never {
    throw new CdbError({ code: "CDB_INVALID_ARGS", message: `vector reshard destination: ${message}` });
}

function limited(message: string): never {
    throw new CdbError({ code: "CDB_RATE_LIMITED", message: `vector reshard destination: ${message}` });
}

function safeInteger(value: unknown, subject: string, minimum = 0): number {
    const number = typeof value === "number" || typeof value === "bigint" ? Number(value) : Number.NaN;
    if (!Number.isSafeInteger(number) || number < minimum) mismatch(`${subject} is invalid`);
    return number;
}

function inputInteger(value: unknown, subject: string, minimum = 0): number {
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) invalid(`${subject} is invalid`);
    return value;
}

function storedFlag(value: unknown, subject: string): boolean {
    const number = safeInteger(value, subject);
    if (number !== 0 && number !== 1) mismatch(`${subject} is invalid`);
    return number === 1;
}

function cursorJson(value: unknown): string {
    return JSON.stringify(normalizeCdbVectorReshardCursor(value));
}

function parseCursor(value: string, subject: string): CdbVectorReshardCursor {
    if (typeof value !== "string" || TEXT.encode(value).byteLength > 1_024) mismatch(`${subject} is invalid`);
    let parsed: unknown;
    try {
        parsed = JSON.parse(value);
    } catch {
        mismatch(`${subject} is invalid`);
    }
    return normalizeCdbVectorReshardCursor(parsed);
}

function abortCursorJson(cursor: CdbVectorReshardAbortCursor): string {
    return JSON.stringify(cursor);
}

function parseAbortCursor(value: string): CdbVectorReshardAbortCursor {
    if (typeof value !== "string" || TEXT.encode(value).byteLength > 1_024) mismatch("stored abort cursor is invalid");
    let raw: unknown;
    try {
        raw = JSON.parse(value);
    } catch {
        mismatch("stored abort cursor is invalid");
    }
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) mismatch("stored abort cursor is invalid");
    const record = raw as Record<string, unknown>;
    if (
        Object.keys(record).sort().join(",") !== "afterPhysicalVersion,afterVectorId,kind" ||
        (record.kind !== "attempt" && record.kind !== "outbox" && record.kind !== "head" && record.kind !== "done") ||
        typeof record.afterVectorId !== "string" ||
        TEXT.encode(record.afterVectorId).byteLength > 256 ||
        typeof record.afterPhysicalVersion !== "number" ||
        !Number.isSafeInteger(record.afterPhysicalVersion) ||
        record.afterPhysicalVersion < 0 ||
        ((record.kind === "outbox" || record.kind === "head" || record.kind === "done") &&
            record.afterPhysicalVersion !== 0) ||
        (record.kind === "done" && record.afterVectorId !== "")
    ) {
        mismatch("stored abort cursor is invalid");
    }
    return Object.freeze({
        kind: record.kind,
        afterVectorId: record.afterVectorId,
        afterPhysicalVersion: record.afterPhysicalVersion,
    });
}

function lowerHex(value: Uint8Array | null): string | null {
    if (value === null) return null;
    let encoded = "";
    for (const byte of value) encoded += byte.toString(16).padStart(2, "0");
    return encoded;
}

function recordIdentity(record: CdbVectorReshardRecord): CdbVectorReshardRecordIdentity {
    return Object.freeze({
        kind: record.kind,
        vectorId: record.vectorId,
        physicalVersion: record.kind === "attempt" ? record.physicalVersion : 0,
    });
}

function compareRecordToCursor(record: CdbVectorReshardRecord, cursor: CdbVectorReshardCursor): number {
    const kind = KIND_ORDER[record.kind] - KIND_ORDER[cursor.kind];
    if (kind !== 0) return kind;
    const placement = record.placementVshard - cursor.afterPlacement;
    if (placement !== 0) return placement;
    const vector = record.vectorId < cursor.afterVectorId ? -1 : record.vectorId > cursor.afterVectorId ? 1 : 0;
    if (vector !== 0) return vector;
    return record.kind === "attempt" ? record.physicalVersion - cursor.afterPhysicalVersion : 0;
}

function cursorAfter(record: CdbVectorReshardRecord, throughHeadSeq: number): CdbVectorReshardCursor {
    return Object.freeze({
        kind: record.kind,
        throughHeadSeq,
        afterPlacement: record.placementVshard,
        afterVectorId: record.vectorId,
        afterPhysicalVersion: record.kind === "attempt" ? record.physicalVersion : 0,
    });
}

function assertPageTransition(
    identity: CdbVectorReshardIdentity,
    input: CdbVectorReshardCursor,
    records: readonly CdbVectorReshardRecord[],
    next: CdbVectorReshardCursor
): void {
    if (next.throughHeadSeq !== input.throughHeadSeq) mismatch("page changed the head watermark");
    if (input.kind === "done") mismatch("page input cursor is already terminal");
    const successorKind = input.kind === "head" ? "outbox" : input.kind === "outbox" ? "attempt" : "done";
    let prior = input;
    for (const record of records) {
        if (record.kind !== input.kind) mismatch("page record kind does not match its input cursor");
        if (record.placementVshard < identity.rangeLo || record.placementVshard > identity.rangeHi) {
            mismatch(`vector ${record.vectorId} is outside the moving range`);
        }
        if (compareRecordToCursor(record, prior) <= 0) mismatch("page records are not in strict cursor order");
        prior = cursorAfter(record, input.throughHeadSeq);
    }
    const last = records.at(-1);
    if (last) {
        const sameKind = next.kind === last.kind;
        if (sameKind && cursorJson(next) !== cursorJson(cursorAfter(last, input.throughHeadSeq))) {
            mismatch("page successor does not match its last record");
        }
        if (!sameKind) {
            if (
                next.kind !== successorKind ||
                next.afterPlacement !== -1 ||
                next.afterVectorId !== "" ||
                next.afterPhysicalVersion !== 0
            ) {
                mismatch("page successor does not advance to a later record kind");
            }
        }
    } else if (
        next.kind !== successorKind ||
        next.afterPlacement !== -1 ||
        next.afterVectorId !== "" ||
        next.afterPhysicalVersion !== 0
    ) {
        mismatch("empty page does not advance exactly one record kind");
    }
}

function decodeBase64(value: string): Uint8Array {
    return Uint8Array.from(atob(value), character => character.charCodeAt(0));
}

export function initializeCdbVectorReshardDestStore(sql: SyncSql): void {
    initializeCdbVectorReshardProvenance(sql);
    const statements = CDB_VECTOR_RESHARD_DEST_DDL.split(";")
        .map(value => value.trim())
        .filter(Boolean);
    for (const statement of statements) sql.exec(statement);
    const expected = [
        "mig_id",
        "range_lo",
        "range_hi",
        "through_head_seq",
        "expected_cursor_json",
        "next_page_number",
        "last_page_number",
        "last_input_cursor_json",
        "last_page_digest",
        "last_page_enc",
        "last_applied",
        "last_inserted",
        "last_skipped",
        "last_through_lsn",
        "terminal",
        "parity_cursor_json",
        "parity_page_number",
        "parity_through_lsn",
        "parity_complete",
        "parity_last_page_number",
        "parity_last_input_cursor_json",
        "parity_last_page_digest",
        "abort_cursor_json",
        "outcome",
        "cleaned",
    ] as const;
    let actual = sql
        .all<{ name: string }>("PRAGMA table_info('_chardb_vector_reshard_dest_sessions')")
        .map(column => column.name);
    if (actual.join("\u0000") !== expected.join("\u0000")) {
        const rows = safeInteger(
            sql.one<{ count: number | bigint }>("SELECT COUNT(*) AS count FROM _chardb_vector_reshard_dest_sessions")
                ?.count ?? 0,
            "legacy destination session row count"
        );
        if (rows > 0) mismatch("destination session schema is incompatible with an active movement");
        sql.exec("DROP TABLE _chardb_vector_reshard_dest_sessions");
        for (const statement of statements) sql.exec(statement);
        actual = sql
            .all<{ name: string }>("PRAGMA table_info('_chardb_vector_reshard_dest_sessions')")
            .map(column => column.name);
        if (actual.join("\u0000") !== expected.join("\u0000")) mismatch("destination session schema is incompatible");
    }
}

/**
 * Applies strict, numbered vector snapshot pages to a destination.
 *
 * Mutating methods must run inside the destination Cdb transaction. The store
 * keeps one page body and result, matching the source session's bounded
 * response-loss replay contract. A successor permanently closes its predecessor.
 */
export class CdbVectorReshardDestStore {
    private readonly parityReader: CdbVectorReshardSnapshotReader;
    private readonly provenance: CdbVectorReshardProvenanceStore;

    constructor(private readonly sql: SyncSql) {
        this.parityReader = new CdbVectorReshardSnapshotReader(sql, "dest");
        this.provenance = new CdbVectorReshardProvenanceStore(sql);
    }

    begin(identity: CdbVectorReshardIdentity, throughHeadSeq: number): CdbVectorReshardCursor {
        this.parityReader.assertIdentity(identity);
        const watermark = inputInteger(throughHeadSeq, "head watermark");
        const initial = Object.freeze({
            kind: "head" as const,
            throughHeadSeq: watermark,
            afterPlacement: -1,
            afterVectorId: "",
            afterPhysicalVersion: 0,
        });
        const existing = this.session(identity.migId);
        if (existing) {
            this.validateSession(identity, existing, watermark);
            if (storedFlag(existing.cleaned, "cleanup flag")) mismatch("destination snapshot session was cleaned");
            this.provenance.bind(identity);
            if (existing.outcome !== "active") mismatch(`destination movement is ${existing.outcome}`);
            this.provenance.assertSnapshotIntervalCount(
                identity,
                safeInteger(existing.next_page_number, "next page number")
            );
            return parseCursor(existing.expected_cursor_json, "stored expected cursor");
        }
        this.assertSessionCapacity();
        this.provenance.bind(identity);
        this.sql.exec(
            `INSERT INTO _chardb_vector_reshard_dest_sessions
               (mig_id, range_lo, range_hi, through_head_seq, expected_cursor_json, next_page_number)
             VALUES (?, ?, ?, ?, ?, 0)`,
            identity.migId,
            identity.rangeLo,
            identity.rangeHi,
            watermark,
            cursorJson(initial)
        );
        if (this.sql.changes() !== 1) mismatch("destination session begin lost its identity row");
        return initial;
    }

    apply(identity: CdbVectorReshardIdentity, request: CdbVectorReshardDestRequest): CdbVectorReshardDestResult {
        const pageNumber = inputInteger(request.pageNumber, "page number");
        const throughLsn = inputInteger(request.throughLsn, "snapshot tail watermark");
        if (typeof request.encodedPage !== "string") invalid("encoded page is invalid");
        const inputCursor = normalizeCdbVectorReshardCursor(request.cursor);
        const encodedCursor = cursorJson(inputCursor);
        const row = this.requiredSession(identity, inputCursor.throughHeadSeq);
        if (row.outcome !== "active") mismatch(`destination movement is ${row.outcome}`);
        this.provenance.assertSnapshotIntervalCount(identity, safeInteger(row.next_page_number, "next page number"));
        if (row.last_page_number !== null && pageNumber === safeInteger(row.last_page_number, "last page number")) {
            return this.replay(identity, request, inputCursor, row);
        }
        if (storedFlag(row.terminal, "terminal flag")) mismatch("snapshot destination is already terminal");
        if (pageNumber !== safeInteger(row.next_page_number, "next page number")) {
            mismatch("page number is not the next expected page");
        }
        if (encodedCursor !== row.expected_cursor_json) mismatch("page cursor is not the next expected cursor");
        if (throughLsn < safeInteger(row.last_through_lsn, "last snapshot tail watermark")) {
            mismatch("snapshot tail watermark regressed");
        }
        if (pageNumber >= Number.MAX_SAFE_INTEGER) mismatch("page number cannot advance");

        const page = decodeCdbVectorReshardPage(request.encodedPage);
        if (encodeCdbVectorReshardPage(page) !== request.encodedPage) mismatch("page encoding is not canonical");
        assertPageTransition(identity, inputCursor, page.records, page.next);
        let inserted = 0;
        let skipped = 0;
        const attemptCounts = new Map<string, number>();
        for (const record of page.records) {
            const result = this.applyRecord(identity, record, throughLsn, attemptCounts);
            inserted += result.inserted ? 1 : 0;
            skipped += result.skipped ? 1 : 0;
        }
        const applied = page.records.length - skipped;
        const digest = sha256Hex(request.encodedPage);
        this.provenance.recordSnapshotInterval(identity, {
            pageNumber,
            cursor: inputCursor,
            next: page.next,
            throughLsn,
            pageDigest: digest,
        });
        this.sql.exec(
            `UPDATE _chardb_vector_reshard_dest_sessions
             SET expected_cursor_json = ?, next_page_number = ?, last_page_number = ?,
                 last_input_cursor_json = ?, last_page_digest = ?, last_page_enc = ?,
                 last_applied = ?, last_inserted = ?, last_skipped = ?, last_through_lsn = ?, terminal = ?
             WHERE mig_id = ? AND range_lo = ? AND range_hi = ? AND through_head_seq = ?
               AND expected_cursor_json = ? AND next_page_number = ? AND terminal = 0`,
            cursorJson(page.next),
            pageNumber + 1,
            pageNumber,
            encodedCursor,
            digest,
            request.encodedPage,
            applied,
            inserted,
            skipped,
            throughLsn,
            page.done ? 1 : 0,
            identity.migId,
            identity.rangeLo,
            identity.rangeHi,
            inputCursor.throughHeadSeq,
            encodedCursor,
            pageNumber
        );
        if (this.sql.changes() !== 1) mismatch("snapshot page lost its destination cursor");
        return Object.freeze({ applied, inserted, skipped, replayed: false, next: page.next, done: page.done });
    }

    /** Same exact record order as the source reader, without a snapshot-generation cutoff. */
    readParityPage(
        identity: CdbVectorReshardIdentity,
        cursor: CdbVectorReshardCursor = CDB_VECTOR_RESHARD_PARITY_START_CURSOR,
        limit = CDB_VECTOR_RESHARD_PAGE_SIZE
    ) {
        const normalized = normalizeCdbVectorReshardCursor(cursor);
        if (normalized.throughHeadSeq !== Number.MAX_SAFE_INTEGER) invalid("parity cursor has the wrong watermark");
        return this.parityReader.read(identity, normalized, limit);
    }

    /** Persist one exact source/destination parity step after the tail is fenced and converged. */
    verifyParityPage(
        identity: CdbVectorReshardIdentity,
        request: CdbVectorReshardParityRequest
    ): { readonly next: CdbVectorReshardCursor; readonly done: boolean } {
        const row = this.requiredSession(identity);
        if (!storedFlag(row.terminal, "terminal flag")) mismatch("snapshot is not terminal before parity");
        if (row.outcome !== "active") mismatch(`destination movement is ${row.outcome}`);
        this.provenance.assertSnapshotIntervalCount(identity, safeInteger(row.next_page_number, "next page number"));
        const pageNumber = inputInteger(request.pageNumber, "parity page number");
        const throughLsn = inputInteger(request.throughLsn, "parity tail watermark");
        const cursor = normalizeCdbVectorReshardCursor(request.cursor);
        if (cursor.throughHeadSeq !== Number.MAX_SAFE_INTEGER) invalid("parity cursor has the wrong watermark");
        if (typeof request.encodedSourcePage !== "string") invalid("encoded source parity page is invalid");
        const sourcePage = decodeCdbVectorReshardPage(request.encodedSourcePage);
        if (encodeCdbVectorReshardPage(sourcePage) !== request.encodedSourcePage) {
            mismatch("source parity page encoding is not canonical");
        }
        assertPageTransition(identity, cursor, sourcePage.records, sourcePage.next);
        const sourceDigest = sha256Hex(request.encodedSourcePage);
        if (
            row.parity_last_page_number !== null &&
            pageNumber === safeInteger(row.parity_last_page_number, "last parity page number")
        ) {
            if (
                row.parity_last_input_cursor_json !== cursorJson(cursor) ||
                row.parity_last_page_digest !== sourceDigest ||
                row.parity_through_lsn === null ||
                safeInteger(row.parity_through_lsn, "parity tail watermark") !== throughLsn
            ) {
                mismatch("duplicate parity page changed its identity");
            }
            const next = parseCursor(row.parity_cursor_json, "stored parity cursor");
            return Object.freeze({ next, done: storedFlag(row.parity_complete, "parity completion flag") });
        }
        if (pageNumber !== safeInteger(row.parity_page_number, "stored parity page number")) {
            mismatch("parity page number is not the next expected page");
        }
        if (cursorJson(cursor) !== row.parity_cursor_json) mismatch("parity cursor is not the next expected cursor");
        if (
            row.parity_through_lsn !== null &&
            safeInteger(row.parity_through_lsn, "parity tail watermark") !== throughLsn
        ) {
            mismatch("parity tail watermark changed during validation");
        }
        const destinationPage = this.parityReader.read(identity, cursor, CDB_VECTOR_RESHARD_PAGE_SIZE);
        if (encodeCdbVectorReshardPage(destinationPage) !== request.encodedSourcePage) {
            mismatch("source and destination vector side-state do not match");
        }
        if (pageNumber >= Number.MAX_SAFE_INTEGER) mismatch("parity page number cannot advance");
        this.sql.exec(
            `UPDATE _chardb_vector_reshard_dest_sessions
             SET parity_cursor_json = ?, parity_page_number = ?, parity_through_lsn = ?, parity_complete = ?,
                 parity_last_page_number = ?, parity_last_input_cursor_json = ?, parity_last_page_digest = ?
             WHERE mig_id = ? AND outcome = 'active' AND parity_page_number = ? AND parity_cursor_json = ?
               AND (parity_through_lsn IS NULL OR parity_through_lsn = ?) AND parity_complete = 0`,
            cursorJson(sourcePage.next),
            pageNumber + 1,
            throughLsn,
            sourcePage.done ? 1 : 0,
            pageNumber,
            cursorJson(cursor),
            sourceDigest,
            identity.migId,
            pageNumber,
            cursorJson(cursor),
            throughLsn
        );
        if (this.sql.changes() !== 1) mismatch("parity page lost its durable cursor");
        return Object.freeze({ next: sourcePage.next, done: sourcePage.done });
    }

    /** Record explicit cutover authorization after tail convergence and exact parity. */
    finalize(identity: CdbVectorReshardIdentity, throughLsn: number): { readonly finalized: boolean } {
        const row = this.requiredSession(identity);
        if (row.outcome === "finalized") return Object.freeze({ finalized: false });
        if (row.outcome !== "active") mismatch(`destination movement is ${row.outcome}`);
        this.provenance.assertSnapshotIntervalCount(identity, safeInteger(row.next_page_number, "next page number"));
        const watermark = inputInteger(throughLsn, "finalized tail watermark");
        if (
            !storedFlag(row.terminal, "terminal flag") ||
            !storedFlag(row.parity_complete, "parity completion flag") ||
            row.parity_through_lsn === null ||
            safeInteger(row.parity_through_lsn, "parity tail watermark") !== watermark
        ) {
            mismatch("destination movement has no matching terminal parity receipt");
        }
        this.assertLocalVectorState();
        this.sql.exec(
            `UPDATE _chardb_vector_reshard_dest_sessions SET outcome = 'finalized'
             WHERE mig_id = ? AND outcome = 'active' AND terminal = 1 AND parity_complete = 1
               AND parity_through_lsn = ?`,
            identity.migId,
            watermark
        );
        if (this.sql.changes() !== 1) mismatch("destination finalization lost its lifecycle state");
        return Object.freeze({ finalized: true });
    }

    /** Roll back only rows whose exact current image is owned by this movement. */
    abort(
        identity: CdbVectorReshardIdentity,
        limit = 500,
        options: { readonly allowFinalized?: boolean } = {}
    ): CdbVectorReshardAbortResult {
        const boundedLimit = inputInteger(limit, "abort page limit", 1);
        if (boundedLimit > 500) invalid("abort page limit exceeds 500");
        let row = this.requiredSession(identity);
        if (row.outcome === "aborted") {
            return Object.freeze({
                deleted: 0,
                next: Object.freeze({ kind: "done", afterVectorId: "", afterPhysicalVersion: 0 }),
                done: true,
            });
        }
        if (row.outcome === "cleaned" || (row.outcome === "finalized" && !options.allowFinalized)) {
            mismatch(`destination movement is ${row.outcome}`);
        }
        if (row.outcome === "active" || row.outcome === "finalized") {
            this.sql.exec(
                `UPDATE _chardb_vector_reshard_dest_sessions SET outcome = 'aborting'
                 WHERE mig_id = ? AND outcome = ?`,
                identity.migId,
                row.outcome
            );
            if (this.sql.changes() !== 1) mismatch("destination abort lost its lifecycle state");
            row = this.requiredSession(identity);
        }
        const cursor = parseAbortCursor(row.abort_cursor_json);
        if (abortCursorJson(cursor) !== row.abort_cursor_json) mismatch("stored abort cursor is not canonical");
        const page = this.provenance.readAbortPage(identity, cursor, boundedLimit);
        let deleted = 0;
        for (const prior of page.records) {
            const present = this.readPhysicalFingerprint(prior);
            if (!prior.present) {
                if (present !== null) mismatch(`abort tombstone ${prior.kind} ${prior.vectorId} has a live row`);
            } else {
                if (present === null || present !== prior.imageFingerprint) {
                    mismatch(`abort ${prior.kind} ${prior.vectorId} changed after migration apply`);
                }
                if (prior.inserted) {
                    this.deletePhysical(prior);
                    deleted++;
                }
            }
            this.provenance.removeForAbort(identity, prior);
        }
        this.sql.exec(
            `UPDATE _chardb_vector_reshard_dest_sessions SET abort_cursor_json = ?
             WHERE mig_id = ? AND outcome = 'aborting' AND abort_cursor_json = ?`,
            abortCursorJson(page.next),
            identity.migId,
            abortCursorJson(cursor)
        );
        if (this.sql.changes() !== 1) mismatch("destination abort lost its durable cursor");
        if (page.next.kind === "done") {
            this.assertLocalVectorState();
            const counts = this.provenance.counts(identity);
            if (counts.records !== 0) mismatch("destination abort retained record ownership");
            const cleanup = this.provenance.cleanup(identity, boundedLimit);
            if (cleanup.cleaned) {
                this.sql.exec(
                    "UPDATE _chardb_vector_reshard_dest_sessions SET outcome = 'aborted' WHERE mig_id = ? AND outcome = 'aborting'",
                    identity.migId
                );
                if (this.sql.changes() !== 1) mismatch("destination abort did not finish");
            }
            return Object.freeze({ deleted, next: page.next, done: cleanup.cleaned });
        }
        return Object.freeze({ deleted, next: page.next, done: false });
    }

    /** Release the replay body after exact parity succeeds. */
    cleanup(identity: CdbVectorReshardIdentity): { readonly cleaned: boolean } {
        this.parityReader.assertIdentity(identity);
        const row = this.session(identity.migId);
        if (!row) mismatch(`migration ${identity.migId} has no destination snapshot session`);
        this.validateSession(identity, row);
        if (storedFlag(row.cleaned, "cleanup flag")) return Object.freeze({ cleaned: false });
        if (row.outcome !== "finalized") mismatch("destination movement is not finalized");
        const provenance = this.provenance.cleanup(identity);
        if (!provenance.cleaned) return Object.freeze({ cleaned: false });
        this.sql.exec(
            `UPDATE _chardb_vector_reshard_dest_sessions
             SET last_page_number = NULL, last_input_cursor_json = NULL, last_page_digest = NULL,
                 last_page_enc = NULL, last_applied = 0, last_inserted = 0, last_skipped = 0,
                 outcome = 'cleaned', cleaned = 1
             WHERE mig_id = ? AND terminal = 1 AND parity_complete = 1 AND outcome = 'finalized' AND cleaned = 0`,
            identity.migId
        );
        if (this.sql.changes() !== 1) mismatch("destination snapshot cleanup lost its session");
        return Object.freeze({ cleaned: true });
    }

    private replay(
        identity: CdbVectorReshardIdentity,
        request: CdbVectorReshardDestRequest,
        cursor: CdbVectorReshardCursor,
        stored: StoredSession
    ): CdbVectorReshardDestResult {
        if (stored.last_input_cursor_json !== cursorJson(cursor)) mismatch("duplicate page changed its input cursor");
        if (
            stored.last_page_enc !== request.encodedPage ||
            stored.last_page_digest !== sha256Hex(request.encodedPage)
        ) {
            mismatch("duplicate page changed its encoded body");
        }
        if (safeInteger(stored.last_through_lsn, "stored page tail watermark") !== request.throughLsn) {
            mismatch("duplicate page changed its tail watermark");
        }
        const page = decodeCdbVectorReshardPage(request.encodedPage);
        if (encodeCdbVectorReshardPage(page) !== request.encodedPage) mismatch("cached page encoding is not canonical");
        assertPageTransition(identity, cursor, page.records, page.next);
        return Object.freeze({
            applied: safeInteger(stored.last_applied, "stored applied count"),
            inserted: safeInteger(stored.last_inserted, "stored inserted count"),
            skipped: safeInteger(stored.last_skipped, "stored skipped count"),
            replayed: true,
            next: page.next,
            done: page.done,
        });
    }

    private applyRecord(
        identity: CdbVectorReshardIdentity,
        record: CdbVectorReshardRecord,
        throughLsn: number,
        attemptCounts: Map<string, number>
    ): { readonly inserted: boolean; readonly skipped: boolean } {
        const key = recordIdentity(record);
        const prior = this.provenance.read(identity, key);
        const headPrior =
            record.kind === "head"
                ? prior
                : this.provenance.read(identity, { kind: "head", vectorId: record.vectorId, physicalVersion: 0 });
        const tailAfterSnapshot = (value: CdbVectorReshardProvenance | null): boolean =>
            value?.latestTailLsn !== null && value?.latestTailLsn !== undefined && value.latestTailLsn > throughLsn;
        if (tailAfterSnapshot(prior) || tailAfterSnapshot(headPrior))
            return Object.freeze({ inserted: false, skipped: true });
        if (record.kind !== "head" && headPrior?.snapshotThroughLsn === null) {
            mismatch(`${record.kind} ${record.vectorId} has no migration-owned snapshot head`);
        }
        if (record.kind !== "head" && headPrior === null) {
            mismatch(`${record.kind} ${record.vectorId} has no migration-owned snapshot head`);
        }
        if (prior?.snapshotThroughLsn !== null && prior?.snapshotThroughLsn !== undefined) {
            mismatch(`${record.kind} ${record.vectorId} appears in more than one snapshot page`);
        }

        const inserted =
            record.kind === "head"
                ? this.applyHead(record, prior !== null)
                : record.kind === "outbox"
                  ? this.applyOutbox(record, prior !== null)
                  : this.applyAttempt(record, prior !== null, attemptCounts);
        const fingerprint = cdbVectorReshardSnapshotRecordFingerprint(record);
        this.provenance.recordSnapshotFromRead(
            identity,
            {
                ...key,
                throughLsn,
                placementVshard: record.placementVshard,
                inserted,
                imageFingerprint: fingerprint,
            },
            prior
        );
        return Object.freeze({ inserted, skipped: false });
    }

    private applyHead(record: CdbVectorReshardHeadRecord, hasTailProvenance: boolean): boolean {
        const existing = this.sql.one<{ present: number }>(
            "SELECT 1 AS present FROM _chardb_vectors WHERE vector_id = ?",
            record.vectorId
        );
        if (existing && !hasTailProvenance) mismatch(`destination head ${record.vectorId} predates this migration`);
        const values = record.valuesEncBase64 === null ? null : decodeBase64(record.valuesEncBase64);
        const collidingOwner = this.sql.one<{ vector_id: string }>(
            `SELECT vector_id FROM _chardb_vectors
             WHERE resource_id = ? AND organization_id = ? AND row_pk = ?`,
            record.resourceId,
            record.organizationId,
            record.rowPk
        );
        if (collidingOwner && collidingOwner.vector_id !== record.vectorId) {
            mismatch(`destination resource row belongs to ${collidingOwner.vector_id}`);
        }
        this.assertHeadCapacity(
            record.vectorId,
            existing !== null,
            values?.byteLength ?? 0,
            TEXT.encode(record.metadataJson).byteLength
        );
        if (existing) {
            this.sql.exec(
                `UPDATE _chardb_vectors SET organization_id = ?, placement_vshard = ?, resource_id = ?, row_pk = ?,
                    dimensions = ?, version = ?, delivered_version = ?, values_enc = ?, metadata_json = ?, state = ?,
                    updated_at = ? WHERE vector_id = ?`,
                record.organizationId,
                record.placementVshard,
                record.resourceId,
                record.rowPk,
                record.dimensions,
                record.headVersion,
                record.deliveredVersion,
                values,
                record.metadataJson,
                record.state,
                record.updatedAt,
                record.vectorId
            );
            if (this.sql.changes() !== 1) mismatch(`destination head ${record.vectorId} changed during apply`);
            return false;
        }
        const createdSeq = this.allocateHeadSequence();
        this.sql.exec(
            `INSERT INTO _chardb_vectors
               (vector_id, created_seq, organization_id, placement_vshard, resource_id, row_pk, dimensions,
                version, delivered_version, values_enc, metadata_json, state, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            record.vectorId,
            createdSeq,
            record.organizationId,
            record.placementVshard,
            record.resourceId,
            record.rowPk,
            record.dimensions,
            record.headVersion,
            record.deliveredVersion,
            values,
            record.metadataJson,
            record.state,
            record.updatedAt
        );
        if (this.sql.changes() !== 1) mismatch(`destination head ${record.vectorId} was not inserted`);
        return true;
    }

    private applyOutbox(record: CdbVectorReshardOutboxRecord, hasTailProvenance: boolean): boolean {
        this.assertSnapshotHeadOwner(record);
        const existing = this.sql.one<{ present: number }>(
            "SELECT 1 AS present FROM _chardb_vector_outbox WHERE vector_id = ?",
            record.vectorId
        );
        if (existing && !hasTailProvenance) mismatch(`destination outbox ${record.vectorId} predates this migration`);
        this.assertChildCapacity("outbox", existing !== null, record.vectorId);
        const params = [
            record.targetVersion,
            record.operation,
            record.phase,
            record.mutationId,
            record.acceptedAt,
            record.verifyIdsJson,
            record.attempts,
            record.nextAttemptAt,
            record.leasedUntil,
            record.leaseToken,
            record.terminalFailure,
            record.lastError,
        ] as const;
        if (existing) {
            this.sql.exec(
                `UPDATE _chardb_vector_outbox SET target_version = ?, operation = ?, phase = ?, mutation_id = ?,
                    accepted_at = ?, verify_ids_json = ?, attempts = ?, next_attempt_at = ?, leased_until = ?,
                    lease_token = ?, terminal_failure = ?, last_error = ? WHERE vector_id = ?`,
                ...params,
                record.vectorId
            );
            if (this.sql.changes() !== 1) mismatch(`destination outbox ${record.vectorId} changed during apply`);
            return false;
        }
        this.sql.exec(
            `INSERT INTO _chardb_vector_outbox
               (vector_id, target_version, operation, phase, mutation_id, accepted_at, verify_ids_json, attempts,
                next_attempt_at, leased_until, lease_token, terminal_failure, last_error)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            record.vectorId,
            ...params
        );
        return true;
    }

    private applyAttempt(
        record: CdbVectorReshardAttemptRecord,
        hasTailProvenance: boolean,
        attemptCounts: Map<string, number>
    ): boolean {
        this.assertSnapshotHeadOwner(record);
        const existing = this.sql.one<{ present: number }>(
            `SELECT 1 AS present FROM _chardb_vector_attempts
             WHERE vector_id = ? AND physical_version = ?`,
            record.vectorId,
            record.physicalVersion
        );
        if (existing && !hasTailProvenance) {
            mismatch(`destination attempt ${record.vectorId}/${record.physicalVersion} predates this migration`);
        }
        this.assertChildCapacity("attempt", existing !== null, record.vectorId, attemptCounts);
        const params = [
            record.firstSentAt,
            record.settleAfter,
            record.visibilityConfirmed,
            record.responseAmbiguous,
            record.deleteConfirmed,
            record.deleteClaimToken,
        ] as const;
        if (existing) {
            this.sql.exec(
                `UPDATE _chardb_vector_attempts SET first_sent_at = ?, settle_after = ?, visibility_confirmed = ?,
                    response_ambiguous = ?, delete_confirmed = ?, delete_claim_token = ?
                 WHERE vector_id = ? AND physical_version = ?`,
                ...params,
                record.vectorId,
                record.physicalVersion
            );
            if (this.sql.changes() !== 1) mismatch("destination attempt changed during apply");
            return false;
        }
        this.sql.exec(
            `INSERT INTO _chardb_vector_attempts
               (vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                response_ambiguous, delete_confirmed, delete_claim_token)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            record.vectorId,
            record.physicalVersion,
            ...params
        );
        return true;
    }

    private assertSnapshotHeadOwner(record: CdbVectorReshardOutboxRecord | CdbVectorReshardAttemptRecord): void {
        const head = this.sql.one<{
            organization_id: string;
            placement_vshard: number | bigint;
            resource_id: string;
        }>(
            `SELECT organization_id, placement_vshard, resource_id
             FROM _chardb_vectors WHERE vector_id = ?`,
            record.vectorId
        );
        if (
            !head ||
            head.organization_id !== record.organizationId ||
            safeInteger(head.placement_vshard, "stored head placement") !== record.placementVshard ||
            head.resource_id !== record.resourceId
        ) {
            mismatch(`${record.kind} ${record.vectorId} does not match its destination head owner`);
        }
    }

    private assertHeadCapacity(vectorId: string, existing: boolean, valuesBytes: number, metadataBytes: number): void {
        const capacity = this.capacity();
        const prior = existing
            ? this.sql.one<{ bytes: number | bigint }>(
                  `SELECT COALESCE(length(values_enc), 0) + length(metadata_json) AS bytes
                   FROM _chardb_vectors WHERE vector_id = ?`,
                  vectorId
              )
            : null;
        if (!existing && capacity.heads >= CDB_VECTOR_MAX_HEADS) limited("vector head capacity is exhausted");
        const projected =
            capacity.storedBytes + valuesBytes + metadataBytes - safeInteger(prior?.bytes ?? 0, "old head bytes");
        if (projected > CDB_VECTOR_MAX_STORED_BYTES) limited("stored vector bytes exceed their limit");
    }

    private assertChildCapacity(
        kind: "outbox" | "attempt",
        existing: boolean,
        vectorId: string,
        attemptCounts?: Map<string, number>
    ): void {
        if (existing) return;
        const capacity = this.capacity();
        if (kind === "outbox" && capacity.outbox >= CDB_VECTOR_MAX_OUTBOX_ROWS) limited("vector outbox is full");
        if (kind === "attempt") {
            if (capacity.attempts >= CDB_VECTOR_MAX_ATTEMPT_ROWS) limited("vector attempt ledger is full");
            let count = attemptCounts?.get(vectorId);
            if (count === undefined) {
                const row = this.sql.one<{ count: number | bigint }>(
                    "SELECT COUNT(*) AS count FROM _chardb_vector_attempts WHERE vector_id = ?",
                    vectorId
                );
                count = safeInteger(row?.count ?? 0, "attempt version count");
            }
            if (count >= CDB_VECTOR_MAX_ATTEMPT_VERSIONS) {
                limited("vector attempt history reached its per-head limit");
            }
            attemptCounts?.set(vectorId, count + 1);
        }
    }

    private capacity(): {
        readonly heads: number;
        readonly storedBytes: number;
        readonly outbox: number;
        readonly attempts: number;
    } {
        const row = this.sql.one<{
            reconciled: number | bigint;
            head_count: number | bigint;
            stored_bytes: number | bigint;
            outbox_rows: number | bigint;
            attempt_rows: number | bigint;
        }>("SELECT * FROM _chardb_vector_capacity WHERE singleton = 1");
        if (!row || !storedFlag(row.reconciled, "capacity reconciliation flag"))
            mismatch("vector capacity is unavailable");
        return Object.freeze({
            heads: safeInteger(row.head_count, "head count"),
            storedBytes: safeInteger(row.stored_bytes, "stored vector bytes"),
            outbox: safeInteger(row.outbox_rows, "outbox row count"),
            attempts: safeInteger(row.attempt_rows, "attempt row count"),
        });
    }

    private readPhysicalFingerprint(prior: CdbVectorReshardProvenance): string | null {
        if (prior.kind === "head") {
            const row = this.sql.one<Record<string, unknown>>(
                `SELECT vector_id, organization_id, placement_vshard, resource_id, row_pk, dimensions, version,
                        delivered_version, values_enc, metadata_json, state, updated_at
                 FROM _chardb_vectors WHERE vector_id = ?`,
                prior.vectorId
            );
            if (!row) return null;
            return cdbVectorReshardPhysicalRowFingerprint("head", {
                ...row,
                values_hex: lowerHex((row.values_enc as Uint8Array | null) ?? null),
            });
        }
        if (prior.kind === "outbox") {
            const row = this.sql.one<Record<string, unknown>>(
                `SELECT vector_id, target_version, operation, phase, mutation_id, accepted_at, verify_ids_json,
                        attempts, next_attempt_at, leased_until, lease_token, terminal_failure, last_error
                 FROM _chardb_vector_outbox WHERE vector_id = ?`,
                prior.vectorId
            );
            return row ? cdbVectorReshardPhysicalRowFingerprint("outbox", row) : null;
        }
        const row = this.sql.one<Record<string, unknown>>(
            `SELECT vector_id, physical_version, first_sent_at, settle_after, visibility_confirmed,
                    response_ambiguous, delete_confirmed, delete_claim_token
             FROM _chardb_vector_attempts WHERE vector_id = ? AND physical_version = ?`,
            prior.vectorId,
            prior.physicalVersion
        );
        return row ? cdbVectorReshardPhysicalRowFingerprint("attempt", row) : null;
    }

    private deletePhysical(prior: CdbVectorReshardProvenance): void {
        if (prior.kind === "attempt") {
            this.sql.exec(
                "DELETE FROM _chardb_vector_attempts WHERE vector_id = ? AND physical_version = ?",
                prior.vectorId,
                prior.physicalVersion
            );
        } else if (prior.kind === "outbox") {
            this.sql.exec("DELETE FROM _chardb_vector_outbox WHERE vector_id = ?", prior.vectorId);
        } else {
            this.sql.exec("DELETE FROM _chardb_vectors WHERE vector_id = ?", prior.vectorId);
        }
        if (this.sql.changes() !== 1) mismatch(`abort ${prior.kind} ${prior.vectorId} changed during delete`);
    }

    private assertLocalVectorState(): void {
        const actual = this.sql.one<{
            heads: number | bigint;
            stored_bytes: number | bigint;
            outboxes: number | bigint;
            attempts: number | bigint;
            max_seq: number | bigint;
            distinct_seq: number | bigint;
        }>(
            `SELECT (SELECT COUNT(*) FROM _chardb_vectors) AS heads,
                    (SELECT COALESCE(SUM(COALESCE(length(values_enc), 0) + length(metadata_json)), 0)
                       FROM _chardb_vectors) AS stored_bytes,
                    (SELECT COUNT(*) FROM _chardb_vector_outbox) AS outboxes,
                    (SELECT COUNT(*) FROM _chardb_vector_attempts) AS attempts,
                    (SELECT COALESCE(MAX(created_seq), 0) FROM _chardb_vectors) AS max_seq,
                    (SELECT COUNT(DISTINCT created_seq) FROM _chardb_vectors) AS distinct_seq`
        );
        const capacity = this.capacity();
        const heads = safeInteger(actual?.heads ?? -1, "audited head count");
        if (
            capacity.heads !== heads ||
            capacity.storedBytes !== safeInteger(actual?.stored_bytes ?? -1, "audited stored bytes") ||
            capacity.outbox !== safeInteger(actual?.outboxes ?? -1, "audited outbox count") ||
            capacity.attempts !== safeInteger(actual?.attempts ?? -1, "audited attempt count") ||
            safeInteger(actual?.distinct_seq ?? -1, "audited distinct head sequences") !== heads
        ) {
            mismatch("local vector capacity does not match physical rows");
        }
        const sequence = this.sql.one<{ last_seq: number | bigint }>(
            "SELECT last_seq FROM _chardb_vector_head_sequence WHERE singleton = 1"
        );
        if (
            !sequence ||
            safeInteger(sequence.last_seq, "stored head sequence") <
                safeInteger(actual?.max_seq ?? -1, "audited head sequence")
        ) {
            mismatch("local vector head sequence is behind physical rows");
        }
        const scheduler = this.sql.one<{ next_vshard: number | bigint }>(
            "SELECT next_vshard FROM _chardb_vector_scheduler WHERE singleton = 1"
        );
        if (!scheduler) mismatch("local vector scheduler is missing");
        const nextVshard = safeInteger(scheduler.next_vshard, "local vector scheduler cursor");
        if (nextVshard >= 16_384) mismatch("local vector scheduler cursor is invalid");
    }

    private allocateHeadSequence(): number {
        const row = this.sql.one<{ last_seq: number | bigint }>(
            "SELECT last_seq FROM _chardb_vector_head_sequence WHERE singleton = 1"
        );
        const current = safeInteger(row?.last_seq ?? -1, "vector head insertion sequence");
        if (current >= Number.MAX_SAFE_INTEGER) limited("vector head insertion sequence is exhausted");
        this.sql.exec(
            "UPDATE _chardb_vector_head_sequence SET last_seq = ? WHERE singleton = 1 AND last_seq = ?",
            current + 1,
            current
        );
        if (this.sql.changes() !== 1) mismatch("vector head insertion sequence changed");
        return current + 1;
    }

    private session(migId: string): StoredSession | null {
        return this.sql.one<StoredSession>(
            "SELECT * FROM _chardb_vector_reshard_dest_sessions WHERE mig_id = ?",
            migId
        );
    }

    private requiredSession(identity: CdbVectorReshardIdentity, throughHeadSeq?: number): StoredSession {
        this.parityReader.assertIdentity(identity);
        const row = this.session(identity.migId);
        if (!row) mismatch(`migration ${identity.migId} has no destination snapshot session`);
        this.validateSession(identity, row, throughHeadSeq);
        if (storedFlag(row.cleaned, "cleanup flag")) mismatch("destination snapshot session was cleaned");
        return row;
    }

    private validateSession(identity: CdbVectorReshardIdentity, row: StoredSession, throughHeadSeq?: number): void {
        if (
            safeInteger(row.range_lo, "stored range start") !== identity.rangeLo ||
            safeInteger(row.range_hi, "stored range end") !== identity.rangeHi ||
            (throughHeadSeq !== undefined &&
                safeInteger(row.through_head_seq, "stored head watermark") !== throughHeadSeq)
        ) {
            mismatch(`migration ${identity.migId} does not match its destination snapshot identity`);
        }
        const storedWatermark = safeInteger(row.through_head_seq, "stored head watermark");
        const expected = parseCursor(row.expected_cursor_json, "stored expected cursor");
        if (cursorJson(expected) !== row.expected_cursor_json || expected.throughHeadSeq !== storedWatermark) {
            mismatch("stored expected cursor is not canonical");
        }
        const nextPageNumber = safeInteger(row.next_page_number, "next page number");
        const terminal = storedFlag(row.terminal, "terminal flag");
        const parityComplete = storedFlag(row.parity_complete, "parity completion flag");
        const cleaned = storedFlag(row.cleaned, "cleanup flag");
        if (
            row.outcome !== "active" &&
            row.outcome !== "aborting" &&
            row.outcome !== "aborted" &&
            row.outcome !== "finalized" &&
            row.outcome !== "cleaned"
        ) {
            mismatch("stored destination movement outcome is invalid");
        }
        if ((row.outcome === "cleaned") !== cleaned) mismatch("destination cleanup outcome is inconsistent");
        const applied = safeInteger(row.last_applied, "stored applied count");
        const inserted = safeInteger(row.last_inserted, "stored inserted count");
        const skipped = safeInteger(row.last_skipped, "stored skipped count");
        safeInteger(row.last_through_lsn, "stored snapshot tail watermark");
        const parityCursor = parseCursor(row.parity_cursor_json, "stored parity cursor");
        if (
            cursorJson(parityCursor) !== row.parity_cursor_json ||
            parityCursor.throughHeadSeq !== Number.MAX_SAFE_INTEGER
        ) {
            mismatch("stored parity cursor is not canonical");
        }
        const parityPageNumber = safeInteger(row.parity_page_number, "stored parity page number");
        const parityThroughLsn =
            row.parity_through_lsn === null
                ? null
                : safeInteger(row.parity_through_lsn, "stored parity tail watermark");
        if (
            parityComplete !== (parityCursor.kind === "done") ||
            (parityComplete && parityThroughLsn === null) ||
            (parityPageNumber === 0) !== (parityThroughLsn === null)
        ) {
            mismatch("stored parity completion is inconsistent");
        }
        if (row.parity_last_page_number === null) {
            if (
                parityPageNumber !== 0 ||
                row.parity_last_input_cursor_json !== null ||
                row.parity_last_page_digest !== null
            ) {
                mismatch("stored parity replay state is inconsistent");
            }
        } else {
            const lastParityPage = safeInteger(row.parity_last_page_number, "last parity page number");
            if (
                lastParityPage + 1 !== parityPageNumber ||
                row.parity_last_input_cursor_json === null ||
                row.parity_last_page_digest === null ||
                !DIGEST.test(row.parity_last_page_digest)
            ) {
                mismatch("stored parity replay state is inconsistent");
            }
            const parityInput = parseCursor(row.parity_last_input_cursor_json, "stored parity input cursor");
            if (cursorJson(parityInput) !== row.parity_last_input_cursor_json) {
                mismatch("stored parity input cursor is not canonical");
            }
        }
        if ((row.outcome === "finalized" || row.outcome === "cleaned") && (!terminal || !parityComplete)) {
            mismatch("successful destination outcome lacks terminal parity");
        }
        if (inserted > applied || applied + skipped > CDB_VECTOR_RESHARD_PAGE_SIZE) {
            mismatch("stored page result counters are invalid");
        }
        if (cleaned) {
            if (
                !terminal ||
                row.last_page_number !== null ||
                row.last_input_cursor_json !== null ||
                row.last_page_digest !== null ||
                row.last_page_enc !== null ||
                applied !== 0 ||
                inserted !== 0 ||
                skipped !== 0
            ) {
                mismatch("cleaned destination snapshot retained replay state");
            }
            return;
        }
        if (row.last_page_number === null) {
            if (
                nextPageNumber !== 0 ||
                terminal ||
                row.last_input_cursor_json !== null ||
                row.last_page_digest !== null ||
                row.last_page_enc !== null ||
                applied !== 0 ||
                inserted !== 0 ||
                skipped !== 0
            ) {
                mismatch("new destination snapshot has invalid replay state");
            }
            return;
        }
        const lastPageNumber = safeInteger(row.last_page_number, "last page number");
        if (lastPageNumber + 1 !== nextPageNumber || lastPageNumber >= Number.MAX_SAFE_INTEGER) {
            mismatch("stored page numbers are not consecutive");
        }
        if (
            row.last_input_cursor_json === null ||
            row.last_page_digest === null ||
            row.last_page_enc === null ||
            !DIGEST.test(row.last_page_digest) ||
            sha256Hex(row.last_page_enc) !== row.last_page_digest
        ) {
            mismatch("cached destination page identity is invalid");
        }
        const input = parseCursor(row.last_input_cursor_json, "cached input cursor");
        if (cursorJson(input) !== row.last_input_cursor_json || input.throughHeadSeq !== storedWatermark) {
            mismatch("cached input cursor is not canonical");
        }
        const page = decodeCdbVectorReshardPage(row.last_page_enc);
        if (encodeCdbVectorReshardPage(page) !== row.last_page_enc) mismatch("cached page encoding is not canonical");
        assertPageTransition(identity, input, page.records, page.next);
        if (cursorJson(page.next) !== row.expected_cursor_json || page.done !== terminal) {
            mismatch("cached page successor does not match the destination session");
        }
        if (applied + skipped !== page.records.length) mismatch("cached page result counters do not match its records");
    }

    private assertSessionCapacity(): void {
        if (
            this.sql.one<{ present: number }>(
                "SELECT 1 AS present FROM _chardb_vector_reshard_dest_sessions ORDER BY mig_id LIMIT 1 OFFSET ?",
                CDB_SPLIT_IDENTITY_LIMIT - 1
            )
        ) {
            limited("destination snapshot session history reached its row limit");
        }
    }
}
