/** Handle identity shared by runtime API registration and browser transforms. */

import { ChardbRef } from "../types.ts";

const REF_KEY = "__chardbRef" as const;

export type ChardbFunctionKind = "mutation" | "query";

/** Marker carried on every helper-produced value. */
export interface ChardbRefMarker {
    readonly [REF_KEY]: ChardbRef;
    readonly __chardbKind: ChardbFunctionKind;
}

export function attachRef<T extends object>(target: T, kind: ChardbFunctionKind, ref?: string): T & ChardbRefMarker {
    const value: ChardbRef = ChardbRef(ref ?? autoRef(target, kind));
    Object.defineProperty(target, REF_KEY, { value, enumerable: false, configurable: true });
    if (ref === undefined) Object.defineProperty(target, "__chardbAutoRef", { value: true });
    Object.defineProperty(target, "__chardbKind", {
        value: kind,
        enumerable: false,
        configurable: true,
    });
    return target as T & ChardbRefMarker;
}

export function readRef(target: unknown): ChardbRef {
    if (target === null || (typeof target !== "object" && typeof target !== "function")) {
        throw new TypeError("readRef: target is not an object or function");
    }
    const ref = (target as Record<string, unknown>)[REF_KEY];
    if (typeof ref !== "string") {
        throw new TypeError("readRef: target has no __chardbRef (was it defined with @chardb/core/server?)");
    }
    return ChardbRef(ref);
}

function autoRef(target: object, kind: ChardbFunctionKind): string {
    const name =
        typeof target === "function" && typeof (target as { name?: string }).name === "string"
            ? (target as { name: string }).name
            : "anonymous";
    return `${kind}#${name || "anonymous"}`;
}

export function exportRef(kind: ChardbFunctionKind, name: string): string {
    return `${kind}#${name}`;
}

export function bindExportRefs(exports: Record<string, unknown>): void {
    const seen = new Set<unknown>();
    for (const [name, value] of Object.entries(exports)) {
        if (typeof value !== "function" || seen.has(value)) continue;
        seen.add(value);
        const handle = value as unknown as ChardbRefMarker & {
            readonly __chardbAutoRef?: boolean;
            readonly __chardbExportName?: string;
        };
        if (!handle.__chardbAutoRef) continue;
        if (handle.__chardbExportName !== undefined && handle.__chardbExportName !== name) {
            throw new TypeError(`chardb: API export ${name} was already registered as ${handle.__chardbExportName}`);
        }
        Object.defineProperty(handle, "__chardbExportName", { value: name, configurable: true });
        Object.defineProperty(handle, REF_KEY, {
            value: ChardbRef(exportRef(handle.__chardbKind, name)),
            configurable: true,
        });
    }
}
