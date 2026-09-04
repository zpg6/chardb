import { createServer } from "node:net";

export function createPortBlocker() {
    return createServer(socket => socket.destroy());
}

const OCCUPIED_SOCKET_ERROR = /(?:address already in use|eaddrinuse|failed to listen|port .*already in use|#10013\b)/i;

export function isOccupiedPortFailure(output: string, port: number): boolean {
    if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) return false;
    const exactPort = new RegExp(`(?:^|\\D)${port}(?:\\D|$)`).test(output);
    return exactPort && OCCUPIED_SOCKET_ERROR.test(output);
}
