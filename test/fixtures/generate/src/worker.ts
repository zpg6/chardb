import { chardb } from "../../../../src/server/index.ts";
import * as api from "./api.ts";
import { auth } from "./auth.ts";
import * as queries from "./queries.ts";
import * as schema from "./schema.ts";

export const app = chardb({
    ownership: "organization",
    auth,
    schema,
    api: { ...api, ...queries },
    clients: {
        rust: "../../../rust/chardb/tests/fixtures/generated_api.rs",
        ts: "../../cli/fixtures/generated_api.ts",
    },
});
export default app;
