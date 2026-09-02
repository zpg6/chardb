/**
 * Request-runtime values exposed to Better Auth callbacks.
 *
 * Better Auth plugin callbacks (for example `magicLink({ sendMagicLink })`)
 * receive a generic endpoint context rather than a Cloudflare Worker `env`.
 * Chardb creates the Better Auth instance lazily for each Worker env, so it
 * can safely attach that env to the server-only Better Auth options object.
 *
 * The value is intentionally not placed in module state: Workers may handle
 * concurrent requests and a global "current env" would be unsafe.
 */

import type { BetterAuthOptions } from "better-auth";
import type { ChardbEnv } from "./entrypoint.ts";

const CHARDB_AUTH_RUNTIME_ENV = "__chardbAuthRuntimeEnv";

type OptionsWithRuntimeEnv = BetterAuthOptions & {
    readonly [CHARDB_AUTH_RUNTIME_ENV]?: ChardbEnv;
};

/**
 * Add the current Worker environment to the Better Auth options passed to its
 * server runtime. This is an internal construction helper, exported for the
 * focused runtime-contract test only; application code should use
 * `getChardbAuthEnv()` in callbacks.
 */
export function attachChardbAuthRuntimeEnv(options: BetterAuthOptions, env: ChardbEnv): BetterAuthOptions {
    return { ...options, [CHARDB_AUTH_RUNTIME_ENV]: env } as OptionsWithRuntimeEnv;
}

/**
 * Return the Cloudflare Worker environment associated with a Better Auth
 * endpoint callback.
 *
 * Use this from server-only plugin callbacks. The generic lets an application
 * describe its additional bindings without weakening Chardb's base binding
 * contract:
 *
 * ```ts
 * sendMagicLink: async (data, context) => {
 *   const env = getChardbAuthEnv<ChardbEnv & { EMAIL: SendEmail }>(context);
 *   await env.EMAIL.send({ to: data.email, from: "auth@example.com", subject: "Sign in", text: data.url });
 * }
 * ```
 */
export function getChardbAuthEnv<TEnv extends ChardbEnv = ChardbEnv>(context: unknown): TEnv {
    const options = (context as { readonly context?: { readonly options?: unknown } } | undefined)?.context?.options;
    const env = (options as OptionsWithRuntimeEnv | undefined)?.[CHARDB_AUTH_RUNTIME_ENV];
    if (!env) {
        throw new Error(
            "Chardb auth runtime environment is unavailable. Call this only from a Better Auth callback handled by chardb()."
        );
    }
    return env as TEnv;
}
