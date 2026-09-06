import { jwt } from "better-auth/plugins/jwt";
import { organization } from "better-auth/plugins/organization";
import { defineAuth } from "../../../../src/server/index.ts";

export const auth = defineAuth({ plugins: [organization(), jwt()] });
