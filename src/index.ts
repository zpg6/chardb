/** Public clients and their shared error contract. */

export {
    CDB_ERROR_CODES,
    CdbError,
    docsUrlFor,
    isCdbError,
    isRetryable,
    type CdbErrorCode,
    type CdbErrorInit,
} from "./errors.ts";
export { createChardbClient, type ChardbClient, type ChardbClientOptions } from "./client/index.ts";
export type { MutationHandle, QueryHandle, QueryRow, WireResult } from "./handles.ts";
export type { RawJson } from "./types.ts";
export {
    client,
    type ChardbBinding,
    type ChardbBindingAuth,
    type ChardbBindingClient,
    type ChardbBindingMutationOptions,
} from "./binding.ts";
