import { CdbError } from "../../errors.ts";
import { ShardId, type Vshard } from "../../types.ts";
import { VshardMap, type VshardRange } from "../../vshard.ts";
import type { CatalogSql } from "./catalog-schema-store.ts";

export interface CatalogCutoverRequest {
    readonly migId: string;
    readonly lo: number;
    readonly hi: number;
    readonly fromShard: string;
    readonly toShard: string;
}

export interface CatalogCutoverResult {
    readonly applied: boolean;
    readonly newEpoch: number;
}

export interface CatalogRoutingStorage {
    readonly sql: CatalogSql;
    transactionSync<T>(callback: () => T): T;
}

export interface CatalogCutoverHooks {
    before(currentEpoch: number): void;
    after?(newEpoch: number, applied: boolean): void;
}

/** Owns Catalog range persistence, routing cache publication, and routing epoch transitions. */
export class CatalogRoutingStore {
    private cachedMap: VshardMap | null = null;

    constructor(private readonly storage: CatalogRoutingStorage) {}

    route(vshard: number): { readonly shardId: ShardId; readonly schemaEpoch: number } {
        return {
            shardId: this.map().routeVshard(vshard as Vshard),
            schemaEpoch: this.readSchemaEpoch(),
        };
    }

    listRanges(): readonly VshardRange[] {
        return this.map().ranges_();
    }

    listShardIds(): readonly ShardId[] {
        return this.storage.sql
            .all<{ shard_id: string }>("SELECT DISTINCT shard_id FROM catalog_ranges ORDER BY shard_id ASC")
            .map(row => ShardId(row.shard_id));
    }

    listShardIdsPage(afterExclusive: string | null, limit: number): readonly ShardId[] {
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > 64) {
            throw new CdbError({
                code: "CDB_INVALID_ARGS",
                message: "Catalog shard page limit must be from 1 through 64",
            });
        }
        return this.storage.sql
            .all<{ shard_id: string }>(
                `SELECT DISTINCT shard_id FROM catalog_ranges
                 WHERE (? IS NULL OR shard_id > ?)
                 ORDER BY shard_id ASC LIMIT ?`,
                afterExclusive,
                afterExclusive,
                limit
            )
            .map(row => ShardId(row.shard_id));
    }

    ownsRange(lo: number, hi: number, shardId: string): boolean {
        return this.map()
            .ranges_()
            .some(range => range.lo <= lo && hi <= range.hi && range.shardId === shardId);
    }

    cutover(args: CatalogCutoverRequest, hooks?: CatalogCutoverHooks): CatalogCutoverResult {
        let applied = false;
        let newEpoch = 0;
        let committedMap: VshardMap | null = null;
        this.storage.transactionSync(() => {
            const currentEpoch = this.readSchemaEpoch();
            hooks?.before(currentEpoch);
            const guard = this.storage.sql.one<{ v: string }>(
                "SELECT v FROM catalog_meta WHERE k = ?",
                `cutover:${args.migId}`
            );
            if (guard) {
                newEpoch = currentEpoch;
                hooks?.after?.(newEpoch, false);
                return;
            }
            const current = this.map();
            const expectedSource = ShardId(args.fromShard);
            if (
                current.routeVshard(args.lo as Vshard) !== expectedSource ||
                current.routeVshard(args.hi as Vshard) !== expectedSource
            ) {
                throw new CdbError({
                    code: "CDB_STALE_EPOCH",
                    message: "cutover source shard does not own the requested range",
                });
            }
            const next = current.split(args.lo, args.hi, ShardId(args.toShard));
            this.replaceMap(next);
            this.storage.sql.exec(
                "UPDATE catalog_epoch SET epoch = epoch + 1 WHERE scope = 'schema' AND scope_id = 'global'"
            );
            this.storage.sql.exec(
                "INSERT INTO catalog_meta (k, v) VALUES (?, ?)",
                `cutover:${args.migId}`,
                args.fromShard
            );
            applied = true;
            newEpoch = this.readSchemaEpoch();
            committedMap = next;
            hooks?.after?.(newEpoch, true);
        });
        if (committedMap) this.cachedMap = committedMap;
        return { applied, newEpoch };
    }

    splitRange(lo: number, hi: number, toShard: string, before?: () => void): void {
        const next = this.map().split(lo, hi, ShardId(toShard));
        this.storage.transactionSync(() => {
            before?.();
            this.replaceMap(next);
            this.storage.sql.exec(
                "UPDATE catalog_epoch SET epoch = epoch + 1 WHERE scope = 'schema' AND scope_id = 'global'"
            );
        });
        this.cachedMap = next;
    }

    private map(): VshardMap {
        if (this.cachedMap) return this.cachedMap;
        const ranges = this.storage.sql
            .all<{ lo: number; hi: number; shard_id: string }>(
                "SELECT lo, hi, shard_id FROM catalog_ranges ORDER BY lo ASC"
            )
            .map(row => ({ lo: row.lo, hi: row.hi, shardId: ShardId(row.shard_id) }));
        this.cachedMap = new VshardMap(ranges);
        return this.cachedMap;
    }

    private replaceMap(next: VshardMap): void {
        this.storage.sql.exec("DELETE FROM catalog_ranges");
        for (const range of next.ranges_()) {
            this.storage.sql.exec(
                "INSERT INTO catalog_ranges (lo, hi, shard_id) VALUES (?, ?, ?)",
                range.lo,
                range.hi,
                range.shardId
            );
        }
    }

    private readSchemaEpoch(): number {
        return (
            this.storage.sql.one<{ epoch: number }>(
                "SELECT epoch FROM catalog_epoch WHERE scope = 'schema' AND scope_id = 'global'"
            )?.epoch ?? 0
        );
    }
}
