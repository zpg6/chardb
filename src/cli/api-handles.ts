/** The handles an app registered through `chardb({ api })`, described once for every client renderer. */

import type { StandardJSONSchemaV1, StandardSchemaV1 } from "@standard-schema/spec";
import { type Column, getTableColumns, getTableName, is } from "drizzle-orm";
import { SQLiteTable } from "drizzle-orm/sqlite-core";
import { type ChardbFunctionKind, type ChardbRefMarker, isRefMarked } from "../server/refs.ts";
import type { RegisteredQueryPlan } from "../server/registered-query-plan.ts";

export type JsonSchema = Record<string, unknown>;

/** The table and selected columns of a planned select. */
export interface ApiRow {
    readonly table: string;
    readonly columns: readonly { readonly key: string; readonly column: Column }[];
}

export interface ApiHandle {
    readonly name: string;
    readonly ref: string;
    readonly kind: ChardbFunctionKind;
    /** Standard JSON Schema of the `args` validator; null without one. */
    readonly argsSchema: JsonSchema | null;
    /** null when the result is untyped JSON: mutations and queries without a planned select. */
    readonly row: ApiRow | null;
}

interface MarkedHandle extends ChardbRefMarker {
    readonly __chardbArgs?: StandardSchemaV1;
    readonly __chardbCompilePlan?: (args: unknown) => RegisteredQueryPlan;
}

const WORD = /\p{Lu}+(?=\p{Lu}\p{Ll})|\p{Lu}?[\p{Ll}\p{N}]+|\p{Lu}+/gu;

/** Lowercase words split on separators and camel-case boundaries. */
export function words(text: string): string[] {
    return (text.match(WORD) ?? []).map(word => word.toLowerCase());
}

export function pascal(text: string): string {
    const joined = words(text)
        .map(word => word.replace(/^./u, first => first.toUpperCase()))
        .join("");
    return /^\p{N}/u.test(joined) ? `N${joined}` : joined || "Unnamed";
}

/** Distinct wire names stay distinct: a repeat gets a counter while the wire key is kept elsewhere. */
export function unique(taken: Set<string>, ident: string): string {
    let name = ident;
    for (let n = 2; taken.has(name); n++) name = `${ident}_${n}`;
    taken.add(name);
    return name;
}

function withRef<T>(ref: string, run: () => T): T {
    try {
        return run();
    } catch (error) {
        throw new Error(`${ref}: ${error instanceof Error ? error.message : String(error)}`);
    }
}

function jsonSchemaOf(ref: string, validator: StandardSchemaV1 | undefined): JsonSchema | null {
    if (!validator) return null;
    const props = validator["~standard"] as StandardSchemaV1.Props & Partial<StandardJSONSchemaV1.Props>;
    if (!props.jsonSchema) {
        throw new Error(`${ref}: ${props.vendor} args validator does not implement Standard JSON Schema`);
    }
    // zod describes what it cannot represent as `{}`, which renders as untyped JSON instead of aborting the run.
    const options = { target: "draft-2020-12", libraryOptions: { unrepresentable: "any" } } as const;
    return withRef(ref, () => props.jsonSchema?.input(options) ?? null);
}

/** One value per schema node, enough to compile the planned select; it need not satisfy every constraint. */
function sampleOf(schema: unknown): unknown {
    if (typeof schema !== "object" || schema === null) return null;
    const node = schema as JsonSchema;
    if ("const" in node) return node.const;
    if (Array.isArray(node.enum) && node.enum.length > 0) return node.enum[0];
    const union = node.anyOf ?? node.oneOf;
    if (Array.isArray(union) && union.length > 0) return sampleOf(union[0]);
    switch (Array.isArray(node.type) ? node.type[0] : node.type) {
        case "string":
            return "x";
        case "integer":
        case "number":
            // Planned selects reject limit(0), so the sample never drops below one.
            return typeof node.minimum === "number" ? Math.max(node.minimum, 1) : 1;
        case "boolean":
            return true;
        case "array":
            return [sampleOf(node.items)];
        case "object":
            return Object.fromEntries(
                Object.entries((node.properties as JsonSchema | undefined) ?? {}).map(([key, value]) => [
                    key,
                    sampleOf(value),
                ])
            );
        default:
            return null;
    }
}

function rowOf(ref: string, plan: RegisteredQueryPlan, schema: Record<string, unknown>): ApiRow | null {
    if (plan.kind !== "select") return null;
    const table = Object.values(schema).find(
        (value): value is SQLiteTable => is(value, SQLiteTable) && getTableName(value) === plan.plan.table
    );
    if (!table) throw new Error(`${ref}: table ${plan.plan.table} is not part of the worker schema`);
    const columns = getTableColumns(table) as Record<string, Column>;
    return {
        table: plan.plan.table,
        columns: plan.projection.map(({ key }) => {
            const column = columns[key];
            if (!column) throw new Error(`${ref}: column ${key} is not part of ${plan.plan.table}`);
            return { key, column };
        }),
    };
}

/** Describe every registered handle, sorted by export name; a handle exported under two names appears twice. */
export function collectApiHandles(
    refs: Readonly<Record<string, unknown>>,
    schema: Record<string, unknown>
): ApiHandle[] {
    const entries = Object.entries(refs)
        .filter((entry): entry is [string, MarkedHandle] => isRefMarked(entry[1]))
        .sort(([left], [right]) => (left < right ? -1 : 1));
    const seen = new Map<string, MarkedHandle>();
    const handles: ApiHandle[] = [];
    for (const [name, handle] of entries) {
        const ref = handle.__chardbRef;
        const previous = seen.get(ref);
        if (previous && previous !== handle) throw new Error(`${ref}: registered by two different handles`);
        seen.set(ref, handle);
        const argsSchema = jsonSchemaOf(ref, handle.__chardbArgs);
        const plan =
            handle.__chardbKind === "query"
                ? withRef(ref, () => handle.__chardbCompilePlan?.(argsSchema ? sampleOf(argsSchema) : undefined))
                : undefined;
        handles.push({ name, ref, kind: handle.__chardbKind, argsSchema, row: plan ? rowOf(ref, plan, schema) : null });
    }
    return handles;
}
