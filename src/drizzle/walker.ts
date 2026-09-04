/**
 * Translate Drizzle `where: SQL` into the partition-routing fields of
 * `CdbIntent`. The chunk shapes we walk are produced by Drizzle's condition
 * builders and the `sql` template tag — see
 * https://github.com/drizzle-team/drizzle-orm/blob/main/drizzle-orm/src/sql/expressions/conditions.ts
 * and
 * https://github.com/drizzle-team/drizzle-orm/blob/main/drizzle-orm/src/sql/sql.ts.
 *
 * The walker is intentionally conservative: any unrecognized chunk shape, or
 * any predicate touching a non-partition column inside a disjunction, collapses
 * to the cross-partition fallback rather than guessing.
 */

import { Column, Param, SQL, type SQLChunk, StringChunk, getTableName, is } from "drizzle-orm";
import type { CdbIntent, RawJson, WireEndpoint, WireInterval } from "../wire.ts";
import type { ExtractArgs, IntentExtractor } from "./dialect.ts";

export type PartitionMap = Readonly<Record<string, string>>;

type AtomOp = "eq" | "in" | "between" | "gt" | "gte" | "lt" | "lte";

interface Atom {
    readonly op: AtomOp;
    readonly values: readonly RawJson[];
}

type Predicate =
    | { readonly kind: "atom"; readonly atom: Atom }
    | { readonly kind: "and"; readonly children: readonly Predicate[] }
    | { readonly kind: "or"; readonly children: readonly Predicate[] }
    | { readonly kind: "other" };

interface PartitionInfo {
    /** Enumerable set of partition values; `undefined` ⇒ range / unknown. */
    readonly values: readonly RawJson[] | undefined;
    /** Intervals on the partition column; `"full"` ⇒ unconstrained. */
    readonly intervals: readonly WireInterval[] | "full";
}

export type ObservedPredicateIntervals = readonly WireInterval[] | "full";

const NEG_INF: WireEndpoint = { kind: "neg_inf" };
const POS_INF: WireEndpoint = { kind: "pos_inf" };
const FULL_INFO: PartitionInfo = { values: undefined, intervals: "full" };

const BINARY_OPS: Readonly<Record<string, AtomOp>> = {
    " = ": "eq",
    " > ": "gt",
    " >= ": "gte",
    " < ": "lt",
    " <= ": "lte",
};

const valueEndpoint = (v: RawJson, inclusive: boolean): WireEndpoint => ({
    kind: "value",
    value: [v],
    inclusive,
});

const point = (v: RawJson): WireInterval => ({
    kind: "range",
    lo: valueEndpoint(v, true),
    hi: valueEndpoint(v, true),
});

/** Single isolated boundary cast: `Param.value` is `unknown` upstream. */
function paramValue(p: Param): RawJson {
    return p.value as RawJson;
}

function chunkText(c: SQLChunk): string | undefined {
    return is(c, StringChunk) ? c.value.join("") : undefined;
}

/** Drop chunks the `sql` tag injects as zero-width separators. */
function compact(chunks: readonly SQLChunk[]): SQLChunk[] {
    const out: SQLChunk[] = [];
    for (const c of chunks) {
        if (c === undefined) continue;
        if (is(c, StringChunk) && c.value.join("") === "") continue;
        out.push(c);
    }
    return out;
}

function isPartitionColumn(col: Column, table: string, column: string): boolean {
    return getTableName(col.table) === table && col.name === column;
}

export function intervalsForAtom(atom: Atom): WireInterval[] {
    switch (atom.op) {
        case "eq": {
            const v = atom.values[0];
            if (v === undefined) return [];
            return [point(v)];
        }
        case "in":
            return atom.values.map(point);
        case "between": {
            const lo = atom.values[0];
            const hi = atom.values[1];
            if (lo === undefined || hi === undefined) return [];
            return [{ kind: "range", lo: valueEndpoint(lo, true), hi: valueEndpoint(hi, true) }];
        }
        case "gt": {
            const v = atom.values[0];
            if (v === undefined) return [];
            return [{ kind: "range", lo: valueEndpoint(v, false), hi: POS_INF }];
        }
        case "gte": {
            const v = atom.values[0];
            if (v === undefined) return [];
            return [{ kind: "range", lo: valueEndpoint(v, true), hi: POS_INF }];
        }
        case "lt": {
            const v = atom.values[0];
            if (v === undefined) return [];
            return [{ kind: "range", lo: NEG_INF, hi: valueEndpoint(v, false) }];
        }
        case "lte": {
            const v = atom.values[0];
            if (v === undefined) return [];
            return [{ kind: "range", lo: NEG_INF, hi: valueEndpoint(v, true) }];
        }
    }
}

function predicateInfo(p: Predicate): PartitionInfo {
    if (p.kind === "other") return FULL_INFO;
    if (p.kind === "atom") {
        const enumerable = p.atom.op === "eq" || p.atom.op === "in";
        return {
            values: enumerable ? [...p.atom.values] : undefined,
            intervals: intervalsForAtom(p.atom),
        };
    }
    const childInfos = p.children.map(predicateInfo);
    return p.kind === "and" ? combineAnd(childInfos) : combineOr(childInfos);
}

function combineAnd(children: readonly PartitionInfo[]): PartitionInfo {
    let values: readonly RawJson[] | undefined = undefined;
    let intervals: readonly WireInterval[] | "full" = "full";
    for (const c of children) {
        if (c.values !== undefined) {
            values = values === undefined ? c.values : intersectValues(values, c.values);
        }
        if (c.intervals !== "full") {
            intervals = intervals === "full" ? c.intervals : intersectIntervals(intervals, c.intervals);
        }
    }
    return { values, intervals };
}

function combineOr(children: readonly PartitionInfo[]): PartitionInfo {
    if (children.length === 0) return FULL_INFO;
    let values: readonly RawJson[] | undefined = [];
    let intervals: readonly WireInterval[] | "full" = [];
    for (const c of children) {
        values = values === undefined || c.values === undefined ? undefined : [...values, ...c.values];
        if (intervals !== "full") {
            intervals = c.intervals === "full" ? "full" : [...intervals, ...c.intervals];
        }
    }
    return { values, intervals };
}

function rawJsonEq(a: RawJson, b: RawJson): boolean {
    return a === b;
}

function intersectValues(a: readonly RawJson[], b: readonly RawJson[]): RawJson[] {
    return a.filter(x => b.some(y => rawJsonEq(x, y)));
}

/** Pairwise interval intersection (union ∩ union = ⋃ pairwise ∩). */
function intersectIntervals(a: readonly WireInterval[], b: readonly WireInterval[]): WireInterval[] {
    const out: WireInterval[] = [];
    for (const ai of a) {
        for (const bi of b) {
            const m = intersectPair(ai, bi);
            if (m !== undefined) out.push(m);
        }
    }
    return out;
}

function intersectPair(a: WireInterval, b: WireInterval): WireInterval | undefined {
    if (a.kind === "full") return b;
    if (b.kind === "full") return a;
    const lo = maxLo(a.lo, b.lo);
    const hi = minHi(a.hi, b.hi);
    if (!leLoHi(lo, hi)) return undefined;
    return { kind: "range", lo, hi };
}

function maxLo(a: WireEndpoint, b: WireEndpoint): WireEndpoint {
    return cmpEndpoint(a, b, "lo") >= 0 ? a : b;
}

function minHi(a: WireEndpoint, b: WireEndpoint): WireEndpoint {
    return cmpEndpoint(a, b, "hi") <= 0 ? a : b;
}

function cmpEndpoint(a: WireEndpoint, b: WireEndpoint, side: "lo" | "hi"): number {
    if (a.kind === "neg_inf") return b.kind === "neg_inf" ? 0 : -1;
    if (b.kind === "neg_inf") return 1;
    if (a.kind === "pos_inf") return b.kind === "pos_inf" ? 0 : 1;
    if (b.kind === "pos_inf") return -1;
    const c = cmpKey(a.value, b.value);
    if (c !== 0) return c;
    if (a.inclusive === b.inclusive) return 0;
    if (side === "lo") return a.inclusive ? -1 : 1;
    return a.inclusive ? 1 : -1;
}

function cmpKey(a: readonly RawJson[], b: readonly RawJson[]): number {
    const len = Math.min(a.length, b.length);
    for (let i = 0; i < len; i++) {
        const c = cmpScalar(a[i], b[i]);
        if (c !== 0) return c;
    }
    return a.length - b.length;
}

function cmpScalar(a: RawJson | undefined, b: RawJson | undefined): number {
    if (a === undefined || b === undefined) return 0;
    if (typeof a === "string" && typeof b === "string") return a < b ? -1 : a > b ? 1 : 0;
    if (typeof a === "number" && typeof b === "number") return a - b;
    if (typeof a === "boolean" && typeof b === "boolean") return (a ? 1 : 0) - (b ? 1 : 0);
    return 0;
}

function leLoHi(loEp: WireEndpoint, hiEp: WireEndpoint): boolean {
    if (loEp.kind === "neg_inf" || hiEp.kind === "pos_inf") return true;
    if (loEp.kind === "pos_inf" || hiEp.kind === "neg_inf") return false;
    const c = cmpKey(loEp.value, hiEp.value);
    if (c !== 0) return c < 0;
    return loEp.inclusive && hiEp.inclusive;
}

function walk(sql: SQL, table: string, column: string): Predicate {
    const chunks = compact(sql.queryChunks);

    if (chunks.length === 1) {
        const only = chunks[0];
        if (only !== undefined && is(only, SQL)) return walk(only, table, column);
    }

    const conj = matchConjunction(chunks);
    if (conj !== undefined) {
        return { kind: conj.op, children: conj.children.map(c => walk(c, table, column)) };
    }

    if (chunks.length === 2) {
        const head = chunks[0];
        if (head !== undefined && chunkText(head)?.startsWith("not") === true) return { kind: "other" };
    }

    if (chunks.length === 3) {
        const [a, b, c] = chunks;
        if (a !== undefined && b !== undefined && c !== undefined && is(a, Column)) {
            const opText = chunkText(b);
            if (opText !== undefined) {
                if (opText === " in " && Array.isArray(c)) {
                    if (!isPartitionColumn(a, table, column)) return { kind: "other" };
                    const values: RawJson[] = [];
                    for (const elem of c) {
                        if (elem === undefined || !is(elem, Param)) return { kind: "other" };
                        values.push(paramValue(elem));
                    }
                    return { kind: "atom", atom: { op: "in", values } };
                }
                const op = BINARY_OPS[opText];
                if (op !== undefined && is(c, Param)) {
                    if (!isPartitionColumn(a, table, column)) return { kind: "other" };
                    return { kind: "atom", atom: { op, values: [paramValue(c)] } };
                }
            }
        }
    }

    if (chunks.length === 5) {
        const [a, b, c, d, e] = chunks;
        if (
            a !== undefined &&
            b !== undefined &&
            c !== undefined &&
            d !== undefined &&
            e !== undefined &&
            is(a, Column) &&
            chunkText(b) === " between " &&
            is(c, Param) &&
            chunkText(d) === " and " &&
            is(e, Param)
        ) {
            if (!isPartitionColumn(a, table, column)) return { kind: "other" };
            return { kind: "atom", atom: { op: "between", values: [paramValue(c), paramValue(e)] } };
        }
    }

    return { kind: "other" };
}

/**
 * Conservatively project one typed Drizzle predicate onto one SQL column.
 * Unsupported shapes return `"full"`, never a guessed narrow interval.
 */
export function intervalsForColumnPredicate(predicate: SQL, table: string, column: string): ObservedPredicateIntervals {
    return predicateInfo(walk(predicate, table, column)).intervals;
}

function matchConjunction(chunks: readonly SQLChunk[]): { op: "and" | "or"; children: SQL[] } | undefined {
    if (chunks.length !== 3) return undefined;
    const [open, mid, close] = chunks;
    if (open === undefined || mid === undefined || close === undefined) return undefined;
    if (chunkText(open) !== "(" || chunkText(close) !== ")" || !is(mid, SQL)) return undefined;
    const inner = compact(mid.queryChunks);
    if (inner.length < 3 || inner.length % 2 === 0) return undefined;
    const sep = inner[1];
    if (sep === undefined) return undefined;
    const sepText = chunkText(sep);
    const op: "and" | "or" | undefined = sepText === " and " ? "and" : sepText === " or " ? "or" : undefined;
    if (op === undefined) return undefined;
    const children: SQL[] = [];
    for (let i = 0; i < inner.length; i++) {
        const item = inner[i];
        if (item === undefined) return undefined;
        if (i % 2 === 0) {
            if (!is(item, SQL)) return undefined;
            children.push(item);
        } else if (chunkText(item) !== sepText) {
            return undefined;
        }
    }
    return { op, children };
}

function buildIntent(args: ExtractArgs, kind: CdbIntent["kind"], partitionMap: PartitionMap): CdbIntent {
    const tables = [...args.tables];
    const partitionTable = tables.find(t => partitionMap[t] !== undefined);
    const partitionColumn = partitionTable !== undefined ? partitionMap[partitionTable] : undefined;

    if (args.where === undefined || partitionTable === undefined || partitionColumn === undefined) {
        return { kind, tables, joinShape: "cross-partition" };
    }

    const pred = walk(args.where, partitionTable, partitionColumn);
    const info = predicateInfo(pred);

    const intervalsField =
        info.intervals === "full" || info.intervals.length === 0
            ? undefined
            : ([{ table: partitionTable, indexName: partitionColumn, intervals: [...info.intervals] }] as const);

    if (info.values !== undefined && info.values.length > 0) {
        return {
            kind,
            tables,
            partitionKey: { table: partitionTable, column: partitionColumn, values: [...new Set(info.values)] },
            joinShape: "colocated",
            ...(intervalsField !== undefined ? { intervals: intervalsField } : {}),
        };
    }

    return {
        kind,
        tables,
        joinShape: "cross-partition",
        ...(intervalsField !== undefined ? { intervals: intervalsField } : {}),
    };
}

/**
 * `IntentExtractor` that compiles `where: SQL` into partition-key + interval
 * routing data. Construct with the per-table partition column map; pass into
 * `new CdbDialect(extractor)` to enable.
 */
export class StaticIntentExtractor implements IntentExtractor {
    constructor(private readonly partitionMap: PartitionMap) {}

    forSelect(args: ExtractArgs): CdbIntent {
        return buildIntent(args, "select", this.partitionMap);
    }

    forInsert(args: ExtractArgs): CdbIntent {
        return buildIntent(args, "insert", this.partitionMap);
    }

    forUpdate(args: ExtractArgs): CdbIntent {
        return buildIntent(args, "update", this.partitionMap);
    }

    forDelete(args: ExtractArgs): CdbIntent {
        return buildIntent(args, "delete", this.partitionMap);
    }
}
