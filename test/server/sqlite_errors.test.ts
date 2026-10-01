import { expect, test } from "bun:test";
import { CdbError } from "../../src/errors.ts";
import { isSqliteUniqueConstraint } from "../../src/server/do/sqlite-errors.ts";

for (const constraint of ["UNIQUE", "PRIMARYKEY", "NOTNULL", "CHECK", "FOREIGNKEY", "TRIGGER"]) {
    test(`classifies SQLite ${constraint} without treating other constraints as uniqueness`, () => {
        const expected = constraint === "UNIQUE" || constraint === "PRIMARYKEY";
        const message = `constraint failed: table.column: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_${constraint})`;
        expect(isSqliteUniqueConstraint(new Error(message))).toBe(expected);
        expect(
            isSqliteUniqueConstraint(
                Object.assign(new Error("constraint failed"), { code: `SQLITE_CONSTRAINT_${constraint}` })
            )
        ).toBe(expected);
    });
}

test("preserves typed and encoded CharDB errors and rejects unrelated failures", () => {
    const message = "UNIQUE constraint failed: table.column: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_UNIQUE)";
    expect(isSqliteUniqueConstraint(new CdbError({ code: "CDB_INVARIANT", message }))).toBe(false);
    expect(isSqliteUniqueConstraint(new Error(`CDB_FORBIDDEN: ${message}`))).toBe(false);
    expect(isSqliteUniqueConstraint(new Error("UNIQUE constraint failed without a SQLite error code"))).toBe(false);
    expect(isSqliteUniqueConstraint(new Error("handler failed"))).toBe(false);
    expect(isSqliteUniqueConstraint({ message })).toBe(false);
});
