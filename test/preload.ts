/**
 * bun:test preload. `@chardb/core/server` re-exports the DO base classes
 * (`Cdb`, `Catalog`, `Gateway`),
 * which transitively import `cloudflare:workers` for `WorkerEntrypoint`
 * and `DurableObject`. Outside workerd that module doesn't exist; pure-
 * layer tests just need the shapes for type assertion / structural
 * `instanceof`-free reads.
 *
 * Workerd-integration tests under `test/workerd/` boot miniflare in a
 * subprocess and bypass this stub — they hit the real DO base classes
 * inside the workerd runtime.
 */

import { plugin } from "bun";

plugin({
    name: "cloudflare-workers-shim",
    setup(build) {
        build.module("cloudflare:workers", () => ({
            contents: `
        export class WorkerEntrypoint {
          ctx; env;
          constructor(ctx, env) { this.ctx = ctx; this.env = env; }
          async fetch() { return new Response("workerd stub", { status: 501 }); }
        }
        export class DurableObject {
          ctx; env;
          constructor(ctx, env) { this.ctx = ctx; this.env = env; }
        }
        export const waitUntilCalls = [];
        export const env = {};
        export function waitUntil(promise) { waitUntilCalls.push(promise); }
      `,
            loader: "ts",
        }));
    },
});
