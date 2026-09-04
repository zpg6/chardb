export interface ProcessRow {
    pid: number;
    parentPid: number;
    createdAtMs: number;
}

// Windows recycles PIDs, so an orphan still names its dead parent's PID. A child
// created before its parent belongs to an earlier process and is not in this tree.
export function descendantProcesses<T extends ProcessRow>(snapshot: readonly T[], rootPid: number): T[] {
    const byPid = new Map<number, T>();
    const children = new Map<number, T[]>();
    for (const row of snapshot) {
        byPid.set(row.pid, row);
        const entries = children.get(row.parentPid);
        if (entries) entries.push(row);
        else children.set(row.parentPid, [row]);
    }
    const result: T[] = [];
    const seen = new Set([rootPid]);
    const root = byPid.get(rootPid);
    const pending = root ? [root] : [];
    for (let parent = pending.pop(); parent; parent = pending.pop()) {
        for (const child of children.get(parent.pid) ?? []) {
            if (seen.has(child.pid) || child.createdAtMs < parent.createdAtMs) continue;
            seen.add(child.pid);
            result.push(child);
            pending.push(child);
        }
    }
    return result;
}
