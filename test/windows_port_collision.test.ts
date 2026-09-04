import { describe, expect, test } from "bun:test";
import { type Socket, createConnection } from "node:net";
import { createPortBlocker, isOccupiedPortFailure } from "./helpers/windows-port-collision.ts";

test("a port blocker disconnects readiness probes without releasing its listener", async () => {
    const server = createPortBlocker();
    let accepted: Socket | undefined;
    server.once("connection", socket => {
        accepted = socket;
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing fixture port");
    const client = createConnection(address.port, "127.0.0.1");
    client.on("error", () => undefined);
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
        await Promise.race([
            new Promise<void>(resolve => client.once("close", () => resolve())),
            new Promise<never>((_, reject) => {
                timeout = setTimeout(() => reject(new Error("port blocker retained the readiness probe")), 2_000);
            }),
        ]);
        expect(accepted).toBeDefined();
        expect(server.listening).toBe(true);
    } finally {
        clearTimeout(timeout);
        client.destroy();
        accepted?.destroy();
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    }
    expect(server.listening).toBe(false);
});

describe("Windows dev-tree occupied-port output", () => {
    test("accepts POSIX and EADDRINUSE output", () => {
        expect(isOccupiedPortFailure("listen EADDRINUSE: address already in use 127.0.0.1:8787", 8787)).toBe(true);
    });

    test("accepts Workerd's Windows socket error", () => {
        const output =
            "failed to bind 127.0.0.1:8787: #10013 An attempt was made to access a socket in a way forbidden by its access permissions";
        expect(isOccupiedPortFailure(output, 8787)).toBe(true);
    });

    test("rejects an occupied-port error for another port", () => {
        expect(isOccupiedPortFailure("listen EADDRINUSE: address already in use 127.0.0.1:18787", 8787)).toBe(false);
    });

    test("rejects an unrelated failure that mentions the port", () => {
        expect(isOccupiedPortFailure("could not load wrangler.toml for http://127.0.0.1:8787", 8787)).toBe(false);
    });
});
