import { initialSchema17 } from "./v1-better-auth-17.ts";
import { initialSchema } from "./v1.ts";
import { betterAuth17, existingAuth17 } from "./v2.ts";

export type SchemaHistory = "better-auth-1.6" | "better-auth-1.7";

export function selectMigrationInputs(history: unknown) {
    if (history === undefined || history === "better-auth-1.6") {
        return Object.freeze([initialSchema, betterAuth17]);
    }
    if (history === "better-auth-1.7") {
        return Object.freeze([initialSchema17, existingAuth17]);
    }
    throw new Error('CHAT_SCHEMA_HISTORY must be "better-auth-1.6" or "better-auth-1.7"');
}
