import { isCdbError, isCdbErrorCode } from "../../errors.ts";

export function isSqliteUniqueConstraint(error: unknown): error is Error {
    if (!(error instanceof Error) || isCdbError(error)) return false;
    const encoded = /^(CDB_[A-Z_]+)(?::\s*)?/.exec(error.message);
    if (encoded?.[1] && isCdbErrorCode(encoded[1])) return false;
    return (
        ("code" in error &&
            (error.code === "SQLITE_CONSTRAINT_UNIQUE" || error.code === "SQLITE_CONSTRAINT_PRIMARYKEY")) ||
        /: SQLITE_CONSTRAINT \(extended: SQLITE_CONSTRAINT_(?:UNIQUE|PRIMARYKEY)\)$/.test(error.message)
    );
}
