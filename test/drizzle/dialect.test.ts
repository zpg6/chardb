/**
 * Round-trip tests for `CdbDialect` and the `attachIntent` / `getIntent`
 * stash. These cover the surface that `chardb`'s session relies on to ship
 * a `CdbIntent` over the wire alongside the user's SQL — without the
 * stash, the routing layer is blind to which partition the query targets.
 */
import { describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { CDB_INTENT, CdbDialect, PASSTHROUGH_EXTRACTOR, attachIntent, getIntent } from "../../src/drizzle/dialect.ts";
import { StaticIntentExtractor } from "../../src/drizzle/walker.ts";

const orgs = sqliteTable("orgs", {
    id: text("id").primaryKey(),
    name: text("name"),
});

describe("CdbDialect — intent stash round-trip", () => {
    test("attachIntent / getIntent preserves the value across a non-enumerable property", () => {
        const target = {};
        const intent = { kind: "select", tables: ["orgs"], joinShape: "colocated" } as const;
        attachIntent(target, intent);
        expect(getIntent(target)).toEqual(intent);
        // Non-enumerable: doesn't show up in JSON.stringify or Object.keys.
        expect(Object.keys(target)).toEqual([]);
        expect(JSON.parse(JSON.stringify(target))).toEqual({});
    });

    test("getIntent on a vanilla object returns undefined", () => {
        expect(getIntent({})).toBeUndefined();
        expect(getIntent({ foo: 1 })).toBeUndefined();
    });

    test("CDB_INTENT is the canonical Symbol.for handle so cross-bundle stashes interop", () => {
        expect(CDB_INTENT === Symbol.for("chardb.intent")).toBe(true);
    });

    test("CdbDialect.buildIntent dispatches by kind to the configured extractor", () => {
        const extractor = new StaticIntentExtractor({ orgs: "id" });
        const dialect = new CdbDialect(extractor);
        const select = dialect.buildIntent({
            kind: "select",
            tables: ["orgs"],
            where: eq(orgs.id, "o1"),
        });
        expect(select.kind).toBe("select");
        expect(select.partitionKey).toEqual({ table: "orgs", column: "id", values: ["o1"] });
        expect(select.joinShape).toBe("colocated");

        const exec = dialect.buildIntent({ kind: "execute", tables: ["orgs"] });
        expect(exec).toEqual({ kind: "execute", tables: ["orgs"] });
    });

    test("PASSTHROUGH_EXTRACTOR yields cross-partition intents for every kind", () => {
        for (const kind of ["select", "insert", "update", "delete"] as const) {
            const i = PASSTHROUGH_EXTRACTOR[
                `for${kind.charAt(0).toUpperCase()}${kind.slice(1)}` as
                    | "forSelect"
                    | "forInsert"
                    | "forUpdate"
                    | "forDelete"
            ]({ tables: ["t"] });
            expect(i.kind).toBe(kind);
            expect(i.joinShape).toBe("cross-partition");
            expect(i.partitionKey).toBeUndefined();
        }
    });
});
