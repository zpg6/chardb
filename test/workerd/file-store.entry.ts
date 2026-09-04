import { DurableObject } from "cloudflare:workers";
import { CdbFileRuntime } from "../../src/server/do/cdb-file-runtime.ts";
import { CdbFileStore, initializeFileStore } from "../../src/server/do/cdb-file-store.ts";
import { adaptSqlStorage } from "../../src/server/do/sql_adapter.ts";
import type { ChardbEnv } from "../../src/server/entrypoint.ts";
import { refreshRecoverableFile } from "../../src/server/file-retention.ts";
import { renderFileAttachmentTriggers } from "../../src/server/file-triggers.ts";
import { handleOrganizationFileRequest } from "../../src/server/organization-file-http.ts";

const HASH_A = "f28d6cfd0ebc466e6358e1f4f90edc071d0ba3d413255cdc0ec7917189033ad8";
const HASH_B = "804f51f71254c4081e37e7c887073560f4a6fa6cdad202e9ac67e032c43ed1e1";
const HASH_SURVIVOR = "7a01ac37408614bcf58069bb6b6a543f6c473cdded552c491de4eb36aacce235";

function retainedObjectKey(sha256: string): string {
    return `_chardb/retained/sha256/${sha256}`;
}

async function sha256(value: string): Promise<string> {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
    return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

interface FileStoreEnv {
    readonly CDB_FILES: R2Bucket;
}

interface FileProofWorkerEnv extends FileStoreEnv {
    readonly CDB_SHARD: DurableObjectNamespace;
    readonly CDB_CATALOG: DurableObjectNamespace;
}

const RESOURCE = {
    kind: "file" as const,
    version: 1 as const,
    table: "messages",
    column: "attachment",
    primaryKey: "id",
    organizationColumn: "organization_id",
    maxSize: 20,
    contentTypes: ["image/png"] as const,
};

export class FileStoreProof extends DurableObject<FileStoreEnv> {
    constructor(state: DurableObjectState, env: FileStoreEnv) {
        super(state, env);
        state.blockConcurrencyWhile(async () => {
            const sql = adaptSqlStorage(state.storage.sql);
            initializeFileStore(sql);
            sql.exec(
                "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, organization_id TEXT NOT NULL, attachment TEXT)"
            );
            for (const statement of renderFileAttachmentTriggers(RESOURCE)) {
                sql.exec(statement);
            }
        });
    }

    async seed(): Promise<Record<string, unknown>> {
        await Promise.all([
            this.env.CDB_FILES.put("v1/org-1/file_old", "old!", {
                httpMetadata: { contentType: "image/png" },
                customMetadata: { chardbFileId: "file_old", chardbSha256: HASH_A },
            }),
            this.env.CDB_FILES.put("v1/org-1/file_new", "newer", {
                httpMetadata: { contentType: "image/png" },
                customMetadata: { chardbFileId: "file_new", chardbSha256: HASH_B },
            }),
            this.env.CDB_FILES.put("v1/org-1/file_abandoned", "ab", {
                httpMetadata: { contentType: "image/png" },
                customMetadata: { chardbFileId: "file_abandoned" },
            }),
        ]);
        const fileStore = () =>
            new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql), {
                organizationQuotaBytes: 40,
                maxPendingPerOrganization: 3,
            });
        this.ctx.storage.transactionSync(() => {
            const store = fileStore();
            store.reserve({
                fileId: "file_old",
                organizationId: "org-1",
                table: "messages",
                column: "attachment",
                contentType: "image/png",
                size: 4,
                nowMs: 100,
            });
            store.markReady("file_old", HASH_A, 4, 101);
            store.sql.exec(
                "INSERT INTO messages (id, organization_id, attachment) VALUES (?, ?, ?)",
                "row-1",
                "org-1",
                "file_old"
            );
            store.reserve({
                fileId: "file_new",
                organizationId: "org-1",
                table: "messages",
                column: "attachment",
                contentType: "image/png",
                size: 5,
                nowMs: 103,
            });
            store.markReady("file_new", HASH_B, 5, 104);
            store.sql.exec("UPDATE messages SET attachment = ? WHERE id = ?", "file_new", "row-1");
            store.reserve({
                fileId: "file_abandoned",
                organizationId: "org-1",
                table: "messages",
                column: "attachment",
                contentType: "image/png",
                size: 2,
                nowMs: 106,
            });
            store.queueDelete("file_abandoned", 107);
        });
        const seededStore = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql));
        for (const fileId of ["file_old", "file_new"] as const) {
            const file = seededStore.read(fileId);
            if (!file) throw new Error(`missing seeded file ${fileId}`);
            await refreshRecoverableFile(this.env.CDB_FILES, file);
        }

        let rolledBack = false;
        try {
            this.ctx.storage.transactionSync(() => {
                const store = fileStore();
                store.reserve({
                    fileId: "file_rollback",
                    organizationId: "org-1",
                    table: "messages",
                    column: "attachment",
                    contentType: "image/png",
                    size: 1,
                    nowMs: 108,
                });
                throw new Error("rollback proof");
            });
        } catch {
            rolledBack = true;
        }
        return { rolledBack, ...this.inspect() };
    }

    private runtime(): CdbFileRuntime {
        return new CdbFileRuntime({
            storage: this.ctx.storage,
            bucket: this.env.CDB_FILES,
            resources: () => [RESOURCE],
            assertActiveEpoch: epoch => {
                if (epoch !== 1) throw new Error("stale epoch");
            },
        });
    }

    async reserveFile(request: Parameters<CdbFileRuntime["reserve"]>[0]) {
        return this.runtime().reserve(request);
    }

    async markFileReady(request: Parameters<CdbFileRuntime["markReady"]>[0]) {
        return this.runtime().markReady(request);
    }

    async resolveFileDownload(request: {
        readonly organizationId: string;
        readonly table: string;
        readonly column: string;
        readonly rowId: string;
    }) {
        if (request.table !== RESOURCE.table || request.column !== RESOURCE.column) return null;
        const row = adaptSqlStorage(this.ctx.storage.sql).one<{ attachment: string | null }>(
            "SELECT attachment FROM messages WHERE id = ? AND organization_id = ?",
            request.rowId,
            request.organizationId
        );
        if (!row?.attachment) return null;
        const stored = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql)).read(row.attachment);
        return stored?.status === "attached" && stored.rowId === request.rowId ? stored : null;
    }

    async object(fileId: string): Promise<Record<string, unknown>> {
        const stored = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql)).read(fileId);
        const object = stored?.sha256 ? await this.env.CDB_FILES.head(retainedObjectKey(stored.sha256)) : null;
        return {
            stored,
            object: object
                ? { key: object.key, size: object.size, customMetadata: { ...object.customMetadata } }
                : null,
        };
    }

    async cleanup(uploadFileId: string): Promise<Record<string, unknown>> {
        const schedules: number[] = [];
        const runtime = new CdbFileRuntime({
            storage: this.ctx.storage,
            bucket: this.env.CDB_FILES,
            resources: () => [RESOURCE],
            assertActiveEpoch: () => {},
        });
        await runtime.maintain(1_000, async deadline => {
            schedules.push(deadline);
        });
        const uploadFile = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql)).read(uploadFileId);
        const [old, replacement, abandoned, upload] = await Promise.all([
            this.env.CDB_FILES.head("v1/org-1/file_old"),
            this.env.CDB_FILES.head("v1/org-1/file_new"),
            this.env.CDB_FILES.head("v1/org-1/file_abandoned"),
            uploadFile?.sha256 ? this.env.CDB_FILES.head(retainedObjectKey(uploadFile.sha256)) : null,
        ]);
        return {
            ...this.inspect(),
            schedules,
            objects: {
                old: old !== null,
                replacement: replacement !== null,
                abandoned: abandoned !== null,
                upload: upload !== null,
            },
        };
    }

    async bulkOrganizationDeletion(): Promise<Record<string, unknown>> {
        const fileIds = Array.from({ length: 40 }, (_, index) => `bulk_${String(index).padStart(2, "0")}`);
        const hashes = await Promise.all(fileIds.map(fileId => sha256(fileId)));
        await Promise.all([
            ...fileIds.map((fileId, index) =>
                this.env.CDB_FILES.put(`v1/org-bulk/${fileId}`, fileId, {
                    httpMetadata: { contentType: "image/png" },
                    customMetadata: { chardbFileId: fileId, chardbSha256: hashes[index] as string },
                })
            ),
            this.env.CDB_FILES.put("v1/org-safe/survivor", "survivor", {
                httpMetadata: { contentType: "image/png" },
                customMetadata: { chardbFileId: "survivor", chardbSha256: HASH_SURVIVOR },
            }),
        ]);
        this.ctx.storage.transactionSync(() => {
            const store = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql));
            for (let index = 0; index < fileIds.length; index++) {
                const fileId = fileIds[index] as string;
                store.reserve({
                    fileId,
                    organizationId: "org-bulk",
                    table: "messages",
                    column: "attachment",
                    contentType: "image/png",
                    size: fileId.length,
                    nowMs: 2_000 + index,
                });
                store.markReady(fileId, hashes[index] as string, fileId.length, 3_000 + index);
            }
            store.reserve({
                fileId: "survivor",
                organizationId: "org-safe",
                table: "messages",
                column: "attachment",
                contentType: "image/png",
                size: 8,
                nowMs: 4_000,
            });
            store.markReady("survivor", HASH_SURVIVOR, 8, 4_001);
        });
        const survivorFile = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql)).read("survivor");
        if (!survivorFile) throw new Error("missing survivor file");
        await refreshRecoverableFile(this.env.CDB_FILES, survivorFile);

        const runtime = this.runtime();
        const accepted = runtime.deleteOrganization({
            organizationId: "org-bulk",
            nowMs: 5_000,
            domainSchemaEpoch: 1,
            recoveryGeneration: 0,
        });
        const schedules: number[] = [];
        await runtime.maintain(5_001, async deadline => {
            schedules.push(deadline);
        });
        const store = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql));
        const afterFirst = {
            remaining: store.organizationFileCount("org-bulk"),
            due: store.dueDeletes().length,
        };
        await runtime.maintain(6_001, async deadline => {
            schedules.push(deadline);
        });
        const [deletedHeads, survivor] = await Promise.all([
            Promise.all(fileIds.map(fileId => this.env.CDB_FILES.head(`v1/org-bulk/${fileId}`))),
            this.env.CDB_FILES.get("v1/org-safe/survivor"),
        ]);
        return {
            accepted,
            afterFirst,
            afterSecond: {
                remaining: store.organizationFileCount("org-bulk"),
                due: store.dueDeletes().length,
            },
            deletedObjectsRemaining: deletedHeads.filter(Boolean).length,
            survivor: survivor ? { body: await survivor.text(), size: survivor.size } : null,
            survivorMetadata: store.read("survivor"),
            tombstoned: store.isOrganizationDeleted("org-bulk"),
            schedules,
        };
    }

    inspect(): Record<string, unknown> {
        const sql = adaptSqlStorage(this.ctx.storage.sql);
        return {
            rows: sql.all<Record<string, string | number | null>>(
                `SELECT file_id, object_key, status, row_id, size, sha256
                 FROM _chardb_files
                 ORDER BY file_id`
            ),
            dueDeletes: new CdbFileStore(sql).dueDeletes().map(file => file.fileId),
        };
    }

    organizationDeletionState(organizationId: string): Record<string, unknown> {
        const store = new CdbFileStore(adaptSqlStorage(this.ctx.storage.sql));
        return {
            organizationId,
            tombstoned: store.isOrganizationDeleted(organizationId),
            remaining: store.organizationFileCount(organizationId),
        };
    }
}

export class FileCatalogProof extends DurableObject {
    async resolveOrganizationAuthority() {
        return {
            principalId: "user-1",
            organizationId: "org-1",
            role: "member",
            roles: ["member"],
            authEpochs: { global: 1, tenant: 1, principal: 1 },
            recoveryGeneration: 0,
        };
    }

    async resolveOrganizationAuthorityRoute() {
        return {
            authority: await this.resolveOrganizationAuthority(),
            route: { shardId: "proof", recoveryGeneration: 0, schemaEpoch: 1, domainSchemaEpoch: 1 },
        };
    }
}

export default {
    async fetch(request: Request, env: FileProofWorkerEnv): Promise<Response> {
        const url = new URL(request.url);
        if (url.pathname === "/_chardb/files/upload" || url.pathname === "/_chardb/files/download") {
            return handleOrganizationFileRequest({
                request,
                env: {
                    ...env,
                    CDB_GATEWAY: env.CDB_SHARD,
                } as ChardbEnv,
                auth: {
                    api: {
                        getSession: async () => ({
                            user: { id: "user-1" },
                            session: { activeOrganizationId: "org-1" },
                        }),
                    },
                },
                resources: [RESOURCE],
                nowMs: 90,
            });
        }
        const stub = env.CDB_SHARD.get(env.CDB_SHARD.idFromName("proof")) as unknown as FileStoreProof;
        const pathname = url.pathname;
        if (pathname === "/seed") return Response.json(await stub.seed());
        if (pathname === "/object") return Response.json(await stub.object(url.searchParams.get("fileId") ?? ""));
        if (pathname === "/cleanup") return Response.json(await stub.cleanup(url.searchParams.get("fileId") ?? ""));
        if (pathname === "/bulk-delete") return Response.json(await stub.bulkOrganizationDeletion());
        if (pathname === "/inspect") return Response.json(await stub.inspect());
        if (pathname === "/deletion-state") {
            return Response.json(await stub.organizationDeletionState(url.searchParams.get("organizationId") ?? ""));
        }
        return new Response("not found", { status: 404 });
    },
};
