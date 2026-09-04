import { describe, expect, test } from "bun:test";
import { descendantProcesses } from "./helpers/process-tree.ts";

const row = (pid: number, parentPid: number, createdAtMs: number) => ({ pid, parentPid, createdAtMs });
const pids = (rows: { pid: number }[]) => rows.map(r => r.pid).sort((a, b) => a - b);

describe("descendantProcesses", () => {
    test("walks the live tree under the root", () => {
        const snapshot = [row(1, 0, 0), row(100, 1, 10), row(200, 100, 11), row(300, 200, 12), row(900, 1, 5)];
        expect(pids(descendantProcesses(snapshot, 100))).toEqual([200, 300]);
    });

    test("excludes orphans whose dead parent's PID was recycled into the tree", () => {
        const snapshot = [
            row(100, 1, 10),
            row(300, 100, 12), // fresh child that received a recycled PID
            row(528, 300, 1), // boot-time orphan still naming PID 300 as parent
            row(692, 528, 2),
            row(400, 300, 13), // real grandchild
        ];
        expect(pids(descendantProcesses(snapshot, 100))).toEqual([300, 400]);
    });

    test("returns nothing when the root is gone", () => {
        expect(descendantProcesses([row(200, 100, 11)], 100)).toEqual([]);
    });
});
