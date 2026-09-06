import { describe, expect, test } from "bun:test";
import type { ChardbClient, MutationHandle, QueryHandle } from "../../src/index.ts";
import type { RawJson } from "../../src/types.ts";
import {
    type ListMessagesArgs,
    type MessagesRow,
    type PostMessageArgs,
    listMessages,
    postMessage,
} from "./fixtures/generated_api.ts";

describe("generated TypeScript module", () => {
    test("stamps each handle with its kind and wire ref and refuses to execute", () => {
        expect(listMessages.__chardbKind).toBe("query");
        expect(String(listMessages.__chardbRef)).toBe("src/queries.ts#listMessages");
        expect(postMessage.__chardbKind).toBe("mutation");
        expect(String(postMessage.__chardbRef)).toBe("src/api.ts#postMessage");
        expect(() => (listMessages as unknown as () => never)()).toThrow(/pass it to a CharDB client/);
    });

    test("binds argument and row types the way the app declared them", () => {
        const row: MessagesRow = {
            id: "m1",
            organizationId: "org",
            body: "hi",
            createdAt: 1,
            pinned: false,
            score: null,
            meta: { nested: [1, "two", null] },
        };
        const args: ListMessagesArgs = { organizationId: "org", kind: "pinned" };
        listMessages satisfies QueryHandle<ListMessagesArgs, MessagesRow[]>;
        postMessage satisfies MutationHandle<PostMessageArgs, RawJson>;
        row satisfies RawJson;
        // @ts-expect-error the argument key is misspelled
        listMessages satisfies QueryHandle<{ organisationId: string }, MessagesRow[]>;
        // @ts-expect-error a wire ref string is not a handle
        const ref: QueryHandle<ListMessagesArgs, MessagesRow[]> = "src/queries.ts#listMessages";
        expect([row.id, args.organizationId, typeof ref]).toEqual(["m1", "org", "string"]);
    });

    test("rejects arguments the handle does not declare, even where they would infer", () => {
        const calls: unknown[] = [];
        const client = {
            subscribe: (...call: unknown[]) => {
                calls.push(call);
                return { unsubscribe() {} };
            },
            mutate: async (...call: unknown[]) => calls.push(call),
        } as unknown as ChardbClient;
        // @ts-expect-error `typo` is not part of ListMessagesArgs
        client.subscribe(listMessages, { organizationId: "org", typo: 1 }, () => {});
        // @ts-expect-error `body` is required
        void client.mutate(postMessage, { id: "m", organizationId: "org", type: null });
        expect(calls).toHaveLength(2);
    });
});
