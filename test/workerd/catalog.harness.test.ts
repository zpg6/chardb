/** Workerd-level Catalog persistence and authority integration tests. */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import * as path from "node:path";
import { Miniflare } from "miniflare";
import { disposeMiniflareBounded } from "../../scripts/miniflare-lifecycle.mjs";
import { vshardOf } from "../../src/vshard.ts";

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ENTRY = path.join(HERE, "catalog.entry.ts");
const WORKER_NAME = "catalog-restart-worker";

let mf: Miniflare | undefined;

async function buildWorker(): Promise<string> {
    const out = path.join(HERE, ".test-catalog.bundle.mjs");
    const proc = Bun.spawn(
        ["bun", "build", ENTRY, "--target=browser", "--format=esm", "--external=cloudflare:workers", "--outfile", out],
        { stdout: "pipe", stderr: "pipe" }
    );
    const exitCode = await proc.exited;
    if (exitCode !== 0) {
        const stderr = await new Response(proc.stderr).text();
        throw new Error(`bundle failed (exit ${exitCode}):\n${stderr}`);
    }
    return Bun.file(out).text();
}

beforeAll(async () => {
    const workerSource = await buildWorker();
    mf = new Miniflare({
        name: WORKER_NAME,
        modules: true,
        script: workerSource,
        durableObjects: {
            CATALOG: { className: "Catalog", useSQLite: true },
            CDB_RESHARD: { className: "Resharder", useSQLite: true },
        },
        compatibilityDate: "2024-09-23",
        compatibilityFlags: ["nodejs_compat"],
    });
    await mf.ready;
});

afterAll(async () => {
    await disposeMiniflareBounded(mf, { label: "Catalog fixture final teardown" });
    mf = undefined;
});

async function call(op: string, body?: unknown): Promise<unknown> {
    if (!mf) throw new Error("miniflare not initialized");
    const url = `http://example.com/${op}`;
    const res = await mf.dispatchFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body ?? {}),
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`rpc ${op} → HTTP ${res.status}: ${text}`);
    }
    return res.json();
}

async function callFailure(op: string, body?: unknown): Promise<string> {
    if (!mf) throw new Error("miniflare not initialized");
    const res = await mf.dispatchFetch(`http://example.com/${op}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body ?? {}),
    });
    expect(res.status).toBe(500);
    const payload = (await res.json()) as { readonly error?: unknown };
    if (typeof payload.error !== "string") throw new Error(`rpc ${op} returned a malformed error`);
    return payload.error;
}

interface AuthRow {
    readonly [key: string]: unknown;
}

interface OrganizationAuthority {
    readonly principalId: string;
    readonly organizationId: string;
    readonly role: string;
    readonly roles: readonly string[];
    readonly userRole?: string;
    readonly authEpochs: {
        readonly global: number;
        readonly tenant: number;
        readonly principal: number;
    };
}

describe("workerd Catalog persistence", () => {
    test("returns duplicate auth writes as errors without throwing across RPC", async () => {
        const now = Date.parse("2026-09-29T00:00:00Z");
        const payload = {
            id: "unique-user",
            name: "Original",
            email: "unique@example.com",
            emailVerified: true,
            createdAt: now,
            updatedAt: now,
        };
        const mutate = (args: unknown) =>
            call("authAdapterRpc", {
                operation: "mutate",
                recoveryGeneration: 0,
                args,
            });
        expect(await mutate({ model: "user", op: "create", payload })).toMatchObject({ ok: true });
        for (const duplicate of [
            { model: "user", op: "create", payload: { ...payload, id: "duplicate-email" } },
            { model: "user", op: "create", payload: { ...payload, email: "other@example.com" } },
        ]) {
            expect(await mutate(duplicate)).toMatchObject({
                ok: false,
                error: { code: "CDB_UNIQUE_VIOLATION", message: expect.stringContaining("UNIQUE constraint failed:") },
            });
        }
        expect(
            await mutate({
                model: "user",
                op: "create",
                payload: { ...payload, id: "second-user", email: "second@example.com" },
            })
        ).toMatchObject({ ok: true });
        expect(
            await mutate({
                model: "user",
                op: "update",
                where: { id: "second-user" },
                payload: { email: payload.email },
            })
        ).toMatchObject({ ok: false, error: { code: "CDB_UNIQUE_VIOLATION" } });
        expect(
            await call("queryAuth", {
                model: "user",
                where: [{ field: "id", operator: "eq", value: "second-user" }],
            })
        ).toMatchObject([{ id: "second-user", email: "second@example.com" }]);
        expect(
            await mutate({
                model: "user",
                op: "update",
                where: { id: payload.id },
                payload: { name: "Still available" },
            })
        ).toMatchObject({ ok: true });
        expect(
            await call("queryAuth", {
                model: "user",
                where: [{ field: "id", operator: "eq", value: payload.id }],
            })
        ).toMatchObject([
            {
                ...payload,
                name: "Still available",
                createdAt: new Date(now).toISOString(),
                updatedAt: new Date(now).toISOString(),
            },
        ]);
    });

    test("Catalog reconstruction keeps auth tables and stored authority rows", async () => {
        if (!mf) throw new Error("miniflare not initialized");
        const now = Date.parse("2026-08-23T00:00:00Z");
        const expiresAt = Date.parse("2026-08-24T00:00:00Z");

        for (const input of [
            {
                model: "user",
                op: "create",
                payload: {
                    id: "restart-user",
                    name: "Restart User",
                    email: "catalog-restart@example.com",
                    emailVerified: true,
                    createdAt: now,
                    updatedAt: now,
                },
            },
            {
                model: "session",
                op: "create",
                payload: {
                    id: "restart-session",
                    token: "catalog-restart-token",
                    userId: "restart-user",
                    expiresAt,
                    createdAt: now,
                    updatedAt: now,
                },
            },
            {
                model: "organization",
                op: "create",
                payload: {
                    id: "restart-org",
                    name: "Restart Org",
                    slug: "catalog-restart-org",
                    createdAt: now,
                },
            },
            {
                model: "member",
                op: "create",
                payload: {
                    id: "restart-member",
                    organizationId: "restart-org",
                    userId: "restart-user",
                    role: "owner,member",
                    createdAt: now,
                },
            },
        ] as const) {
            await call("mutateAuth", input);
        }

        const queryOne = async (model: string, where: Record<string, string>): Promise<AuthRow> => {
            const rows = (await call("queryAuth", {
                model,
                where: Object.entries(where).map(([field, value]) => ({ field, operator: "eq", value })),
                limit: 1,
            })) as readonly AuthRow[];
            expect(rows).toHaveLength(1);
            const row = rows[0];
            if (!row) throw new Error(`missing stored ${model} row`);
            return row;
        };
        const readStoredAuth = async () => ({
            session: await queryOne("session", { token: "catalog-restart-token" }),
            organization: await queryOne("organization", { slug: "catalog-restart-org" }),
            membership: await queryOne("member", {
                organizationId: "restart-org",
                userId: "restart-user",
            }),
            authority: (await call("resolveOrganizationAuthority", {
                principalId: "restart-user",
                organizationId: "restart-org",
            })) as OrganizationAuthority,
        });

        const before = await readStoredAuth();
        expect(before.session).toMatchObject({
            id: "restart-session",
            token: "catalog-restart-token",
            userId: "restart-user",
            expiresAt: new Date(expiresAt).toISOString(),
        });
        expect(before.organization).toMatchObject({
            id: "restart-org",
            name: "Restart Org",
            slug: "catalog-restart-org",
        });
        expect(before.membership).toMatchObject({
            id: "restart-member",
            organizationId: "restart-org",
            userId: "restart-user",
            role: "owner,member",
        });
        expect(before.authority).toMatchObject({
            principalId: "restart-user",
            organizationId: "restart-org",
            role: "member,owner",
            roles: ["member", "owner"],
            userRole: "user",
        });

        const firstInstanceId = (await call("fixtureInstanceId")) as string;
        await mf.unsafeEvictDurableObject(WORKER_NAME, "Catalog", { name: "global" });
        const secondInstanceId = (await call("fixtureInstanceId")) as string;
        expect(secondInstanceId).not.toBe(firstInstanceId);

        expect(await readStoredAuth()).toEqual(before);
    });

    test("file-free organization deletion permanently retires the id and removes authority", async () => {
        const now = Date.parse("2026-08-28T00:00:00Z");
        await call("mutateAuth", {
            model: "user",
            op: "create",
            payload: {
                id: "retired-org-user",
                name: "Retired Org User",
                email: "retired-org-user@example.com",
                emailVerified: true,
                createdAt: now,
                updatedAt: now,
            },
        });
        await call("mutateAuth", {
            model: "organization",
            op: "create",
            payload: {
                id: "retired-org",
                name: "Retired Org",
                slug: "retired-org",
                createdAt: now,
            },
        });
        await call("mutateAuth", {
            model: "member",
            op: "create",
            payload: {
                id: "retired-org-member",
                organizationId: "retired-org",
                userId: "retired-org-user",
                role: "owner",
                createdAt: now,
            },
        });

        expect(
            await call("resolveOrganizationAuthority", {
                principalId: "retired-org-user",
                organizationId: "retired-org",
            })
        ).toMatchObject({ organizationId: "retired-org", roles: ["owner"] });
        expect(
            await call("resolveOrganizationAuthorityRoute", {
                principalId: "retired-org-user",
                organizationId: "retired-org",
                vshard: Number(vshardOf(["retired-org"])),
            })
        ).toMatchObject({
            authority: { organizationId: "retired-org", roles: ["owner"] },
            route: { shardId: expect.any(String) },
        });

        await call("mutateAuth", {
            model: "member",
            op: "delete",
            where: { id: "retired-org-member" },
            limitOne: true,
        });
        await call("mutateAuth", {
            model: "organization",
            op: "delete",
            where: { id: "retired-org" },
            limitOne: true,
        });

        expect(
            await call("resolveOrganizationAuthority", {
                principalId: "retired-org-user",
                organizationId: "retired-org",
            })
        ).toBeNull();
        expect(
            await call("resolveOrganizationAuthorityRoute", {
                principalId: "retired-org-user",
                organizationId: "retired-org",
                vshard: Number(vshardOf(["retired-org"])),
            })
        ).toEqual({ authority: null });
        await expect(
            callFailure("mutateAuth", {
                model: "organization",
                op: "create",
                payload: {
                    id: "retired-org",
                    name: "Replacement Org",
                    slug: "replacement-org",
                    createdAt: now + 1,
                },
            })
        ).resolves.toContain("organization id was permanently retired after deletion");

        await mf?.unsafeEvictDurableObject(WORKER_NAME, "Catalog", { name: "global" });
        expect(
            await call("resolveOrganizationAuthority", {
                principalId: "retired-org-user",
                organizationId: "retired-org",
            })
        ).toBeNull();
        await expect(
            callFailure("mutateAuth", {
                model: "organization",
                op: "create",
                payload: {
                    id: "retired-org",
                    name: "Replacement Org After Restart",
                    slug: "replacement-org-after-restart",
                    createdAt: now + 2,
                },
            })
        ).resolves.toContain("organization id was permanently retired after deletion");
    });
});
