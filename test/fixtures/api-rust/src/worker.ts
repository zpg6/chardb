import { chardb } from "../../../../src/server/index.ts";
import * as api from "./api.ts";
import { auth } from "./auth.ts";
import * as queries from "./queries.ts";
import * as schema from "./schema.ts";

export const app = chardb({ ownership: "organization", auth, schema, api: { ...api, ...queries } });
export default app;
