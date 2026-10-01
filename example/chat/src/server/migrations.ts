import { env } from "cloudflare:workers";
import { defineMigrations } from "@chardb/core/server";
import { selectMigrationInputs } from "./migrations/history.ts";

export const migrationInputs = selectMigrationInputs(
    (env as { readonly CHAT_SCHEMA_HISTORY?: string }).CHAT_SCHEMA_HISTORY
);
export const migrations = defineMigrations(migrationInputs);
