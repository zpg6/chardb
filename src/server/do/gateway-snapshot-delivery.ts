import type { CdbErrorCode } from "../../errors.ts";
import type { SyncSql } from "../../oplog/wrapper.ts";
import type { ClientId, Cookie, PrincipalId } from "../../types.ts";
import type { Down } from "../../wire.ts";
import { nextGatewaySnapshotSendAt } from "./gateway-alarm-store.ts";
import type { VerifiedGwAttachment } from "./gateway-auth-dispatch.ts";
import type { GatewayAuthorityFreshness } from "./gateway-authority-freshness.ts";
import {
    GATEWAY_AUTH_REFRESH_PENDING_ERROR,
    GATEWAY_SEND_BATCH_SIZE,
    GATEWAY_SEND_LEASE_MS,
    type GatewayRegistrationKey,
    type GatewaySnapshotAckIdentity,
    type GatewaySnapshotSendAttempt,
    type GatewaySnapshotStage,
    acknowledgeGatewaySnapshot,
    acknowledgeGatewaySnapshotReplay,
    claimDueGatewaySnapshot,
    discardClaimedGatewaySnapshot,
    failGatewaySnapshotSend,
    markGatewaySnapshotSendBaseCookie,
    resolveGatewaySnapshotAck,
    retireClaimedGatewaySnapshot,
    stageGatewaySnapshot,
} from "./gateway-registration-store.ts";

export interface GatewaySnapshotStorage {
    readonly sql: SyncSql;
    transactionSync<T>(callback: () => T): T;
}

export type GatewayExactSnapshotSocket =
    | { readonly status: "ready"; readonly ws: WebSocket; readonly attachment: VerifiedGwAttachment }
    | { readonly status: "refreshing"; readonly retryAt: number }
    | { readonly status: "terminal" };

export type GatewaySnapshotRetirement =
    | { readonly kind: "error"; readonly code: CdbErrorCode }
    | { readonly kind: "refetch"; readonly reason: "shardsChanged" };

export type GatewaySnapshotAckSettlement =
    | { readonly kind: "current"; readonly identity: GatewaySnapshotAckIdentity; readonly acknowledged: boolean }
    | { readonly kind: "replay"; readonly subId: number }
    | null;

export interface GatewaySnapshotDeliveryDeps {
    readonly storage: GatewaySnapshotStorage;
    readonly nowMs: () => number;
    readonly scheduleAlarm: (requestedAt: number) => Promise<void>;
    readonly scheduleWork: (nowMs: number) => Promise<void>;
    readonly currentPolicyDigest: (intentJson: string) => string | null;
    readonly checkAuthority: (attempt: GatewaySnapshotSendAttempt) => Promise<GatewayAuthorityFreshness>;
    readonly exactSocket: (
        identity: GatewayRegistrationKey & { readonly connectionId: string },
        nowMs: number
    ) => GatewayExactSnapshotSocket;
    readonly settleRetired: (
        identity: GatewayRegistrationKey & { readonly connectionId: string },
        settlement: GatewaySnapshotRetirement
    ) => void;
    readonly send: (socket: WebSocket, message: Extract<Down, { readonly t: "snapshot" }>) => void;
}

export class GatewaySnapshotDelivery {
    constructor(private readonly deps: GatewaySnapshotDeliveryDeps) {}

    async stage(input: GatewaySnapshotStage): Promise<boolean> {
        const staged = this.deps.storage.transactionSync(() => stageGatewaySnapshot(this.deps.storage.sql, input));
        if (staged) await this.deps.scheduleAlarm(input.nowMs + 1);
        return staged;
    }

    async claimDue(
        nowMs: number,
        connectionId?: string,
        excludedConnectionIds: readonly string[] = []
    ): Promise<readonly GatewaySnapshotSendAttempt[]> {
        const dueAt = nextGatewaySnapshotSendAt(this.deps.storage.sql, excludedConnectionIds);
        if (dueAt !== null && dueAt <= nowMs) {
            await this.deps.scheduleAlarm(nowMs + GATEWAY_SEND_LEASE_MS);
        }
        const attempts: GatewaySnapshotSendAttempt[] = [];
        for (let index = 0; index < GATEWAY_SEND_BATCH_SIZE; index++) {
            const attempt = this.deps.storage.transactionSync(() =>
                claimDueGatewaySnapshot(this.deps.storage.sql, {
                    nowMs,
                    attemptExpiresAt: nowMs + GATEWAY_SEND_LEASE_MS,
                    ...(connectionId === undefined ? {} : { connectionId }),
                    ...(excludedConnectionIds.length === 0 ? {} : { excludedConnectionIds }),
                })
            );
            if (!attempt) break;
            attempts.push(attempt);
        }
        return attempts;
    }

    async sendAttempt(attempt: GatewaySnapshotSendAttempt): Promise<void> {
        let freshness: GatewayAuthorityFreshness;
        try {
            freshness = await this.deps.checkAuthority(attempt);
        } catch (error) {
            this.deps.storage.transactionSync(() => {
                failGatewaySnapshotSend(this.deps.storage.sql, {
                    registrationId: attempt.registrationId,
                    cookie: attempt.cookie,
                    claimToken: attempt.claimToken,
                    claimVersion: attempt.claimVersion,
                    nowMs: this.deps.nowMs(),
                    error,
                });
            });
            await this.deps.scheduleWork(this.deps.nowMs());
            return;
        }
        const sendNowMs = this.deps.nowMs();
        if (freshness.kind === "retry") {
            this.deps.storage.transactionSync(() => {
                failGatewaySnapshotSend(this.deps.storage.sql, {
                    registrationId: attempt.registrationId,
                    cookie: attempt.cookie,
                    claimToken: attempt.claimToken,
                    claimVersion: attempt.claimVersion,
                    nowMs: sendNowMs,
                    error: freshness.message,
                });
            });
            await this.deps.scheduleWork(this.deps.nowMs());
            return;
        }
        if (freshness.kind === "changed") {
            const discarded = this.deps.storage.transactionSync(() =>
                discardClaimedGatewaySnapshot(this.deps.storage.sql, { ...attempt, nowMs: sendNowMs })
            );
            if (discarded) await this.deps.scheduleWork(sendNowMs);
            return;
        }
        if (freshness.kind === "retire" || freshness.kind === "refetch") {
            if (this.retireAttempt(attempt, sendNowMs)) {
                this.deps.settleRetired(
                    attempt,
                    freshness.kind === "refetch"
                        ? { kind: "refetch", reason: "shardsChanged" }
                        : { kind: "error", code: freshness.code }
                );
                await this.deps.scheduleWork(sendNowMs).catch(() => {});
            }
            return;
        }
        const currentPolicyDigest = this.deps.currentPolicyDigest(attempt.intentJson);
        if (currentPolicyDigest !== null && currentPolicyDigest !== attempt.policyDigest) {
            if (this.retireAttempt(attempt, sendNowMs)) {
                this.deps.settleRetired(attempt, { kind: "error", code: "CDB_INVARIANT" });
                await this.deps.scheduleWork(sendNowMs).catch(() => {});
            }
            return;
        }
        const socket = this.deps.exactSocket(attempt, sendNowMs);
        if (socket.status === "refreshing") {
            this.deps.storage.transactionSync(() => {
                failGatewaySnapshotSend(this.deps.storage.sql, {
                    registrationId: attempt.registrationId,
                    cookie: attempt.cookie,
                    claimToken: attempt.claimToken,
                    claimVersion: attempt.claimVersion,
                    nowMs: sendNowMs,
                    retryNotBeforeMs: socket.retryAt,
                    error: GATEWAY_AUTH_REFRESH_PENDING_ERROR,
                });
            });
            await this.deps.scheduleWork(this.deps.nowMs());
            return;
        }
        if (socket.status === "terminal") {
            if (this.retireAttempt(attempt, sendNowMs)) {
                await this.deps.scheduleWork(sendNowMs).catch(() => {});
            }
            return;
        }
        const markResult = this.deps.storage.transactionSync(() =>
            markGatewaySnapshotSendBaseCookie(
                this.deps.storage.sql,
                attempt,
                socket.attachment.lastCookie ?? null,
                sendNowMs
            )
        );
        if (markResult === "stale") return;
        if (markResult === "retired") {
            this.deps.settleRetired(attempt, { kind: "error", code: "CDB_RATE_LIMITED" });
            await this.deps.scheduleWork(sendNowMs).catch(() => {});
            return;
        }
        try {
            this.deps.send(socket.ws, {
                t: "snapshot",
                subId: attempt.subId,
                cookie: attempt.cookie,
                rows: attempt.rows,
            });
        } catch (error) {
            this.deps.storage.transactionSync(() => {
                failGatewaySnapshotSend(this.deps.storage.sql, {
                    registrationId: attempt.registrationId,
                    cookie: attempt.cookie,
                    claimToken: attempt.claimToken,
                    claimVersion: attempt.claimVersion,
                    nowMs: this.deps.nowMs(),
                    error,
                });
            });
            await this.deps.scheduleWork(this.deps.nowMs());
        }
    }

    acknowledge(input: {
        readonly principalId: PrincipalId;
        readonly clientId: ClientId;
        readonly connectionId: string;
        readonly cookie: Cookie;
        readonly nowMs: number;
    }): GatewaySnapshotAckSettlement {
        return this.deps.storage.transactionSync(() => {
            const identity = resolveGatewaySnapshotAck(this.deps.storage.sql, input);
            if (identity) {
                return {
                    kind: "current" as const,
                    identity,
                    acknowledged: acknowledgeGatewaySnapshot(this.deps.storage.sql, {
                        ...identity,
                        nowMs: input.nowMs,
                    }),
                };
            }
            const replaySubId = acknowledgeGatewaySnapshotReplay(this.deps.storage.sql, input);
            return replaySubId === null ? null : { kind: "replay" as const, subId: replaySubId };
        });
    }

    private retireAttempt(attempt: GatewaySnapshotSendAttempt, nowMs: number): boolean {
        return this.deps.storage.transactionSync(() =>
            retireClaimedGatewaySnapshot(this.deps.storage.sql, {
                ...attempt,
                nowMs,
            })
        );
    }
}
