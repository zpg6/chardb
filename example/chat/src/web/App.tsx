import { createChardbReactClient } from "@chardb/react";
import { type Organization, anonymousClient, jwtClient, organizationClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
import { type FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { uuidv7 } from "uuidv7";
import { deleteMessage, editMessage, postMessage } from "../server/api.ts";
import { listMessages } from "../server/queries.ts";

const db = createChardbReactClient({
    url: window.location.origin,
    ownership: "organization",
    auth: ({ baseURL }) =>
        createAuthClient({
            baseURL,
            plugins: [anonymousClient(), organizationClient(), jwtClient()],
        }),
});

const time = new Intl.DateTimeFormat(undefined, { dateStyle: "short", timeStyle: "short" });

let anonymousSignInRequest: ReturnType<typeof db.auth.signIn.anonymous> | undefined;

function signInAnonymously() {
    anonymousSignInRequest ??= db.auth.signIn.anonymous().finally(() => {
        anonymousSignInRequest = undefined;
    });
    return anonymousSignInRequest;
}

function describe(cause: unknown): string {
    return cause instanceof Error ? cause.message : String(cause);
}

/** One in-flight task at a time, with its error kept until the next run or a reset. */
function useAction() {
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    // The lock is a ref: two clicks in one frame both see the pre-update `busy` state.
    const running = useRef(false);
    const run = async (task: () => Promise<unknown>) => {
        if (running.current) return;
        running.current = true;
        setBusy(true);
        setError(null);
        try {
            await task();
        } catch (cause) {
            setError(describe(cause));
        } finally {
            running.current = false;
            setBusy(false);
        }
    };
    return { busy, error, run, reset: () => setError(null) };
}

export function App() {
    const session = db.auth.useSession();
    const [authError, setAuthError] = useState<string | null>(null);

    useEffect(() => {
        if (session.isPending || session.data) return;
        let active = true;
        void (async () => {
            try {
                const result = await signInAnonymously();
                if (active && result.error) setAuthError(result.error.message);
            } catch (cause) {
                if (active) setAuthError(describe(cause));
            }
        })();
        return () => {
            active = false;
        };
    }, [session.data, session.isPending]);

    if (!session.data) {
        return <main className="shell">{authError ? `Sign-in failed: ${authError}` : "Signing in..."}</main>;
    }

    return (
        <db.Provider>
            <Workspace />
        </db.Provider>
    );
}

function Workspace() {
    const identity = db.useIdentity();
    const organizations = db.auth.useListOrganizations();
    const activeOrganizationId = identity.organizationId;
    const userId = identity.user?.id;
    const [name, setName] = useState("");
    const [slug, setSlug] = useState("");
    const saving = useAction();

    const selectOrganization = (organizationId: string | null) =>
        saving.run(async () => {
            const result = await db.auth.organization.setActive({ organizationId });
            if (result.error) throw new Error(result.error.message);
        });

    function createOrganization(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const organizationName = name.trim();
        const organizationSlug = slug.trim();
        if (!organizationName || !organizationSlug) return;
        void saving.run(async () => {
            const created = await db.auth.organization.create({
                name: organizationName,
                slug: organizationSlug,
                keepCurrentActiveOrganization: true,
            });
            if (created.error || !created.data) {
                throw new Error(created.error?.message ?? "Better Auth did not return the new organization");
            }
            const active = await db.auth.organization.setActive({ organizationId: created.data.id });
            if (active.error) throw new Error(active.error.message);
            setName("");
            setSlug("");
        });
    }

    return (
        <main className="shell">
            <header>
                <div>
                    <h1>chardb chat</h1>
                    <p data-testid="auth-status" data-user-id={userId}>
                        Signed in with Better Auth
                    </p>
                </div>
            </header>

            <section className="organizations" aria-label="Organizations">
                <label>
                    Active organization
                    <select
                        data-testid="organization-select"
                        value={activeOrganizationId ?? ""}
                        disabled={saving.busy || organizations.isPending}
                        onChange={event => void selectOrganization(event.target.value || null)}
                    >
                        <option value="">Choose an organization</option>
                        {(organizations.data ?? []).map((organization: Organization) => (
                            <option key={organization.id} value={organization.id} data-slug={organization.slug}>
                                {organization.name}
                            </option>
                        ))}
                    </select>
                </label>

                <form className="organization-form" onSubmit={createOrganization}>
                    <input
                        data-testid="create-organization-name"
                        aria-label="Organization name"
                        value={name}
                        placeholder="Organization name"
                        disabled={saving.busy}
                        onChange={event => setName(event.target.value)}
                    />
                    <input
                        data-testid="create-organization-slug"
                        aria-label="Organization slug"
                        value={slug}
                        placeholder="organization-slug"
                        disabled={saving.busy}
                        onChange={event => setSlug(event.target.value)}
                    />
                    <button
                        data-testid="create-organization-submit"
                        type="submit"
                        disabled={saving.busy || !name.trim() || !slug.trim()}
                    >
                        {saving.busy ? "Saving..." : "Create organization"}
                    </button>
                </form>
            </section>

            {activeOrganizationId && userId ? (
                <Messages key={activeOrganizationId} organizationId={activeOrganizationId} userId={userId} />
            ) : (
                <section className="messages" data-testid="message-list">
                    <p className="empty">Create or choose an organization to start.</p>
                </section>
            )}
            {saving.error ? <p className="error">{saving.error}</p> : null}
        </main>
    );
}

/** The composer edits the row named by `id`, or composes a new message when it is absent. */
interface Draft {
    readonly id?: string;
    readonly body: string;
}

const EMPTY: Draft = { body: "" };

function Messages({ organizationId, userId }: { readonly organizationId: string; readonly userId: string }) {
    const { data = [], state } = db.useQuery(listMessages, { limit: 50 });
    const post = db.useMutation(postMessage);
    const edit = db.useMutation(editMessage);
    const remove = db.useMutation(deleteMessage);
    const [draft, setDraft] = useState(EMPTY);
    const action = useAction();
    const rows = useMemo(() => [...data].reverse(), [data]);
    const loading = state === "pending" || state === "refetching";
    const failed = state === "error" || state === "closed";

    // A row deleted elsewhere while it is being edited drops the composer back to a new message.
    useEffect(() => {
        if (draft.id !== undefined && !data.some(message => message.id === draft.id)) setDraft(EMPTY);
    }, [data, draft.id]);

    function compose(next: Draft) {
        setDraft(next);
        action.reset();
    }

    function submit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const body = draft.body.trim();
        if (!body) return;
        void action.run(async () => {
            if (draft.id !== undefined) await edit({ id: draft.id, body });
            else await post({ id: uuidv7(), body, clientCreatedAt: Date.now() });
            setDraft(EMPTY);
        });
    }

    return (
        <>
            <div className="query-status">
                <code data-testid="query-state" data-organization-id={organizationId}>
                    {state}
                </code>
            </div>

            <section
                className="messages"
                data-testid="message-list"
                data-organization-id={organizationId}
                aria-live="polite"
            >
                {loading ? <p className="empty">Loading messages...</p> : null}
                {failed ? (
                    <p role="alert" className="error">
                        Could not load messages. Check your connection and organization access.
                    </p>
                ) : null}
                {state === "live" && rows.length === 0 ? <p className="empty">No messages yet.</p> : null}
                {rows.map(message => (
                    <article key={message.id} className={message.authorId === userId ? "mine" : undefined}>
                        <small>{message.authorId === userId ? "you" : message.authorId}</small>
                        <p>{message.body}</p>
                        <small>{time.format(message.createdAt)}</small>
                        {message.authorId === userId ? (
                            <div className="message-actions">
                                <button
                                    type="button"
                                    disabled={action.busy}
                                    onClick={() => compose({ id: message.id, body: message.body })}
                                >
                                    Edit
                                </button>
                                <button
                                    type="button"
                                    disabled={action.busy}
                                    onClick={() => void action.run(() => remove({ id: message.id }))}
                                >
                                    Delete
                                </button>
                            </div>
                        ) : null}
                    </article>
                ))}
            </section>

            <form onSubmit={submit}>
                <input
                    aria-label="Message"
                    value={draft.body}
                    maxLength={2_000}
                    placeholder={draft.id !== undefined ? "Edit your message" : "Write a message"}
                    disabled={action.busy}
                    onChange={event => setDraft({ ...draft, body: event.target.value })}
                />
                <button type="submit" disabled={action.busy || !draft.body.trim()}>
                    {action.busy ? "Saving..." : draft.id !== undefined ? "Save" : "Send"}
                </button>
            </form>
            {draft.id !== undefined ? (
                <button type="button" disabled={action.busy} onClick={() => compose(EMPTY)}>
                    Cancel edit
                </button>
            ) : null}
            {action.error ? (
                <p role="alert" className="error">
                    {action.error}
                </p>
            ) : null}
        </>
    );
}
