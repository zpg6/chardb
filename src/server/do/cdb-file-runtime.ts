import { CdbError } from "../../errors.ts";
import type { AuthCtx } from "../define.ts";
import { deleteRecoverableFile } from "../file-retention.ts";
import type { ChardbFileResourceDescriptor } from "../resource-descriptors.ts";
import {
    CDB_FILE_DELETE_BATCH_SIZE,
    CDB_FILE_PENDING_TTL_MS,
    CdbFileStore,
    type StoredFile,
} from "./cdb-file-store.ts";
import { adaptSqlStorage } from "./sql_adapter.ts";

export const CDB_FILE_CLEANUP_RETRY_MS = 1_000;

export interface CdbFileReserveRequest {
    readonly recoveryGeneration: number;
    readonly fileId: string;
    readonly organizationId: string;
    readonly table: string;
    readonly column: string;
    readonly contentType: string;
    readonly size: number;
    readonly nowMs: number;
    readonly domainSchemaEpoch: number;
    readonly auth: AuthCtx;
}

export interface CdbFileReadyRequest {
    readonly recoveryGeneration: number;
    readonly fileId: string;
    readonly organizationId: string;
    readonly sha256: string;
    readonly size: number;
    readonly nowMs: number;
    readonly domainSchemaEpoch: number;
    readonly auth: AuthCtx;
}

export interface CdbFileDownloadRequest {
    readonly recoveryGeneration: number;
    readonly organizationId: string;
    readonly table: string;
    readonly column: string;
    readonly rowId: string;
    readonly domainSchemaEpoch: number;
    readonly auth: AuthCtx;
}

export interface CdbOrganizationFileDeletionRequest {
    readonly recoveryGeneration: number;
    readonly organizationId: string;
    readonly nowMs: number;
    readonly domainSchemaEpoch: number;
}

export interface CdbOrganizationFileDeletionResult {
    readonly organizationId: string;
    readonly accepted: true;
}

function forbidden(): never {
    throw new CdbError({ code: "CDB_FORBIDDEN", message: "file organization authority is invalid" });
}

export class CdbFileRuntime {
    readonly storage: DurableObjectStorage;
    readonly bucket: R2Bucket | undefined;
    readonly resources: () => readonly ChardbFileResourceDescriptor[];
    readonly assertActiveEpoch: (epoch: number) => void;
    readonly assertOwnership: (organizationId: string) => void;
    readonly metadataTransaction: <T>(organizationId: string, callback: (store: CdbFileStore) => T) => T;

    constructor(input: {
        readonly storage: DurableObjectStorage;
        readonly bucket: R2Bucket | undefined;
        readonly resources: () => readonly ChardbFileResourceDescriptor[];
        readonly assertActiveEpoch: (epoch: number) => void;
        readonly assertOwnership?: (organizationId: string) => void;
        readonly metadataTransaction?: <T>(organizationId: string, callback: (store: CdbFileStore) => T) => T;
    }) {
        this.storage = input.storage;
        this.bucket = input.bucket;
        this.resources = input.resources;
        this.assertActiveEpoch = input.assertActiveEpoch;
        this.assertOwnership = input.assertOwnership ?? (() => undefined);
        this.metadataTransaction =
            input.metadataTransaction ??
            (<T>(_organizationId: string, callback: (store: CdbFileStore) => T): T =>
                this.storage.transactionSync(() => callback(new CdbFileStore(adaptSqlStorage(this.storage.sql)))));
    }

    reserve(request: CdbFileReserveRequest): StoredFile {
        this.assertRequestAuthority(request.organizationId, request.auth);
        this.assertActiveEpoch(request.domainSchemaEpoch);
        this.assertOwnership(request.organizationId);
        if (this.organizationDeleted(request.organizationId)) forbidden();
        const resource = this.resource(request.table, request.column);
        if (request.size > resource.maxSize) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "file exceeds the configured column size" });
        }
        const contentType = request.contentType.trim().toLowerCase();
        if (resource.contentTypes !== "*" && !resource.contentTypes.includes(contentType)) {
            throw new CdbError({ code: "CDB_INVALID_ARGS", message: "file content type is not accepted" });
        }
        let reserved: StoredFile | undefined;
        this.metadataTransaction(request.organizationId, store => {
            this.assertActiveEpoch(request.domainSchemaEpoch);
            reserved = store.reserve({
                ...request,
                contentType,
            });
        });
        if (!reserved) throw new CdbError({ code: "CDB_INVARIANT", message: "file reservation was not recorded" });
        return reserved;
    }

    markReady(request: CdbFileReadyRequest): StoredFile {
        this.assertRequestAuthority(request.organizationId, request.auth);
        this.assertActiveEpoch(request.domainSchemaEpoch);
        this.assertOwnership(request.organizationId);
        let ready: StoredFile | undefined;
        this.metadataTransaction(request.organizationId, store => {
            this.assertActiveEpoch(request.domainSchemaEpoch);
            const existing = store.read(request.fileId);
            if (!existing || existing.organizationId !== request.organizationId) forbidden();
            ready = store.markReady(request.fileId, request.sha256, request.size, request.nowMs);
        });
        if (!ready) throw new CdbError({ code: "CDB_INVARIANT", message: "file readiness was not recorded" });
        return ready;
    }

    /** Resolve an attached object only after the caller's policy-wrapped point read exposes its FileId. */
    async resolveDownload(
        request: CdbFileDownloadRequest,
        readAttachment: (resource: ChardbFileResourceDescriptor) => Promise<unknown>
    ): Promise<StoredFile | null> {
        this.assertRequestAuthority(request.organizationId, request.auth);
        this.assertActiveEpoch(request.domainSchemaEpoch);
        this.assertOwnership(request.organizationId);
        if (this.organizationDeleted(request.organizationId)) forbidden();
        const resource = this.resource(request.table, request.column);
        if (
            typeof request.rowId !== "string" ||
            request.rowId.length === 0 ||
            new TextEncoder().encode(request.rowId).byteLength > 256
        ) {
            return null;
        }
        const value = await readAttachment(resource);
        this.assertActiveEpoch(request.domainSchemaEpoch);
        this.assertOwnership(request.organizationId);
        if (this.organizationDeleted(request.organizationId)) forbidden();
        if (typeof value !== "string") return null;
        const stored = new CdbFileStore(adaptSqlStorage(this.storage.sql)).read(value);
        if (
            !stored ||
            stored.status !== "attached" ||
            stored.organizationId !== request.organizationId ||
            stored.table !== request.table ||
            stored.column !== request.column ||
            stored.rowId !== request.rowId
        ) {
            throw new CdbError({ code: "CDB_INVARIANT", message: "attached file metadata does not match its row" });
        }
        return stored;
    }

    deleteOrganization(request: CdbOrganizationFileDeletionRequest): CdbOrganizationFileDeletionResult {
        this.assertActiveEpoch(request.domainSchemaEpoch);
        this.assertOwnership(request.organizationId);
        this.metadataTransaction(request.organizationId, store => {
            this.assertActiveEpoch(request.domainSchemaEpoch);
            store.fenceOrganizationDeletion(request.organizationId, request.nowMs);
        });
        return Object.freeze({ organizationId: request.organizationId, accepted: true });
    }

    async maintain(nowMs: number, scheduleNoLaterThan: (deadline: number) => Promise<void>): Promise<void> {
        const fileStoreExists =
            adaptSqlStorage(this.storage.sql).one<{ present: number }>(
                "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = '_chardb_files'"
            ) !== null;
        if (!fileStoreExists) return;
        const reshardStoreExists =
            adaptSqlStorage(this.storage.sql).one<{ present: number }>(
                "SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = '_chardb_split_file_cursor'"
            ) !== null;
        if (!reshardStoreExists) {
            const partialReshardState = adaptSqlStorage(this.storage.sql).one<{ present: number }>(
                `SELECT 1 AS present FROM sqlite_master
                 WHERE (type = 'table' AND name IN ('_chardb_split_file_applied', '_chardb_split_capture_tx'))
                    OR (type = 'trigger' AND name GLOB '_chardb_filecapt_*')
                 LIMIT 1`
            );
            if (partialReshardState) {
                throw new CdbError({
                    code: "CDB_INVARIANT",
                    message: "file maintenance found incomplete reshard ownership storage",
                });
            }
        }
        const maintenanceOptions = reshardStoreExists ? { ownedOnly: true as const } : {};
        let deletes: readonly StoredFile[] = [];
        let nextExpiry: number | null = null;
        let retry = false;
        const initial = new CdbFileStore(adaptSqlStorage(this.storage.sql));
        const candidates = initial.maintenanceCandidates(
            Math.max(0, nowMs - CDB_FILE_PENDING_TTL_MS),
            maintenanceOptions
        );
        for (const file of candidates) {
            try {
                this.metadataTransaction(file.organizationId, store => store.queueDelete(file.fileId, nowMs));
            } catch (error) {
                if (!(error instanceof CdbError) || error.code !== "CDB_STALE_EPOCH") throw error;
                retry = true;
            }
        }
        const afterTransitions = new CdbFileStore(adaptSqlStorage(this.storage.sql));
        deletes = afterTransitions.dueDeletes(CDB_FILE_DELETE_BATCH_SIZE, maintenanceOptions);
        nextExpiry = afterTransitions.nextUnattachedExpiryAt(CDB_FILE_PENDING_TTL_MS, maintenanceOptions);
        if (deletes.length > 0 && !this.bucket) retry = true;
        for (const file of deletes) {
            if (!this.bucket) break;
            try {
                this.assertOwnership(file.organizationId);
                await deleteRecoverableFile(this.bucket, file);
                this.metadataTransaction(file.organizationId, store => {
                    const current = store.read(file.fileId);
                    if (current?.status === "deleting" && current.objectKey === file.objectKey) {
                        store.completeDelete(file.fileId);
                    }
                });
            } catch {
                retry = true;
            }
        }
        const finalStore = new CdbFileStore(adaptSqlStorage(this.storage.sql));
        const remaining =
            finalStore.dueDeletes(1, maintenanceOptions).length > 0 ||
            finalStore.hasTombstonedMaterializedFiles(maintenanceOptions);
        if (retry || remaining) await scheduleNoLaterThan(nowMs + CDB_FILE_CLEANUP_RETRY_MS);
        if (nextExpiry !== null) await scheduleNoLaterThan(Math.max(nowMs + 1, nextExpiry));
    }

    private assertRequestAuthority(organizationId: string, auth: AuthCtx): void {
        if (auth.tenantId !== organizationId || typeof auth.userId !== "string" || auth.userId.length === 0)
            forbidden();
    }

    private organizationDeleted(organizationId: string): boolean {
        return new CdbFileStore(adaptSqlStorage(this.storage.sql)).isOrganizationDeleted(organizationId);
    }

    private resource(table: string, column: string): ChardbFileResourceDescriptor {
        const resource = this.resources().find(candidate => candidate.table === table && candidate.column === column);
        if (!resource) throw new CdbError({ code: "CDB_INVALID_COLUMN", message: "file locator is not configured" });
        return resource;
    }
}
