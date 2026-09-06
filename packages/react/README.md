# @chardb/react

React hooks for CharDB. Configure Better Auth once, then query and mutate with the authenticated user or organization scope.

```tsx
import { organizationClient, jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
import { createChardbReactClient } from "@chardb/react";

const workerUrl = new URL(window.location.origin).origin;

export const db = createChardbReactClient({
    url: workerUrl,
    ownership: "organization",
    auth: ({ baseURL }) =>
        createAuthClient({
            baseURL,
            plugins: [organizationClient(), jwtClient()],
        }),
});
```

The generated project contains the complete Better Auth and Worker setup. Create it with `bunx @chardb/core init my-chardb-app`.
