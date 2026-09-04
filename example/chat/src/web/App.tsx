import { createChardbReactClient } from "@chardb/react";
import { type Organization, anonymousClient, jwtClient, organizationClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";
import { type FormEvent, useEffect, useState } from "react";
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

let anonymousSignInRequest: ReturnType<typeof db.auth.signIn.anonymous> | undefined;

function signInAnonymously() {
    anonymousSignInRequest ??= db.auth.signIn.anonymous().finally(() => {
        anonymousSignInRequest = undefined;
    });
    return anonymousSignInRequest;
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
                if (active) setAuthError(cause instanceof Error ? cause.message : String(cause));
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
    const [savingOrganization, setSavingOrganization] = useState(false);
    const [error, setError] = useState<string | null>(null);

    async function selectOrganization(organizationId: string | null) {
        setSavingOrganization(true);
        setError(null);
        try {
            const result = await db.auth.organization.setActive({ organizationId });
            if (result.error) throw new Error(result.error.message);
        } catch (cause) {
            setError(cause instanceof Error ? cause.message : String(cause));
        } finally {
            setSavingOrganization(false);
        }
    }

    async function createOrganization(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const organizationName = name.trim();
        const organizationSlug = slug.trim();
        if (!organizationName || !organizationSlug || savingOrganization) return;
        setSavingOrganization(true);
        setError(null);
        try {
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
        } catch (cause) {
            setError(cause instanceof Error ? cause.message : String(cause));
        } finally {
            setSavingOrganization(false);
        }
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
                        disabled={savingOrganization || organizations.isPending}
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
                        disabled={savingOrganization}
                        onChange={event => setName(event.target.value)}
                    />
                    <input
                        data-testid="create-organization-slug"
                        aria-label="Organization slug"
                        value={slug}
                        placeholder="organization-slug"
                        disabled={savingOrganization}
                        onChange={event => setSlug(event.target.value)}
                    />
                    <button
                        data-testid="create-organization-submit"
                        type="submit"
                        disabled={savingOrganization || !name.trim() || !slug.trim()}
                    >
                        {savingOrganization ? "Saving..." : "Create organization"}
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
            {error ? <p className="error">{error}</p> : null}
        </main>
    );
}

function Messages({ organizationId, userId }: { readonly organizationId: string; readonly userId: string }) {
    const { data = [], state } = db.useQuery(listMessages, { limit: 50 });
    const mutate = db.useMutation(postMessage);
    const edit = db.useMutation(editMessage);
    const remove = db.useMutation(deleteMessage);
    const [editingId, setEditingId] = useState<string | null>(null);
    const [body, setBody] = useState("");
    const [sending, setSending] = useState(false);
    const [error, setError] = useState<string | null>(null);

    async function submit(event: FormEvent<HTMLFormElement>) {
        event.preventDefault();
        const message = body.trim();
        if (!message || sending) return;
        setSending(true);
        setError(null);
        try {
            if (editingId) {
                await edit({ id: editingId, body: message });
            } else {
                await mutate({ id: uuidv7(), body: message, clientCreatedAt: Date.now() });
            }
            setEditingId(null);
            setBody("");
        } catch (cause) {
            setError(cause instanceof Error ? cause.message : String(cause));
        } finally {
            setSending(false);
        }
    }

    async function deleteRow(id: string) {
        if (sending) return;
        setSending(true);
        setError(null);
        try {
            await remove({ id });
            if (editingId === id) {
                setEditingId(null);
                setBody("");
            }
        } catch (cause) {
            setError(cause instanceof Error ? cause.message : String(cause));
        } finally {
            setSending(false);
        }
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
                {state === "pending" ? <p className="empty">Loading messages...</p> : null}
                {state === "error" ? (
                    <p role="alert" className="error">
                        Could not load messages. Check your connection and organization access.
                    </p>
                ) : null}
                {state === "live" && data.length === 0 ? <p className="empty">No messages yet.</p> : null}
                {[...data].reverse().map(message => (
                    <article key={message.id} className={message.authorId === userId ? "mine" : undefined}>
                        <small>{message.authorId === userId ? "you" : message.authorId}</small>
                        <p>{message.body}</p>
                        <small>{new Date(message.createdAt).toLocaleTimeString()}</small>
                        {message.authorId === userId ? (
                            <div className="message-actions">
                                <button
                                    type="button"
                                    disabled={sending}
                                    onClick={() => {
                                        setEditingId(message.id);
                                        setBody(message.body);
                                        setError(null);
                                    }}
                                >
                                    Edit
                                </button>
                                <button type="button" disabled={sending} onClick={() => void deleteRow(message.id)}>
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
                    value={body}
                    maxLength={2_000}
                    placeholder={editingId ? "Edit your message" : "Write a message"}
                    disabled={sending}
                    onChange={event => setBody(event.target.value)}
                />
                <button type="submit" disabled={sending || !body.trim()}>
                    {sending ? "Saving..." : editingId ? "Save" : "Send"}
                </button>
            </form>
            {editingId ? (
                <button
                    type="button"
                    disabled={sending}
                    onClick={() => {
                        setEditingId(null);
                        setBody("");
                        setError(null);
                    }}
                >
                    Cancel edit
                </button>
            ) : null}
            {error ? (
                <p role="alert" className="error">
                    {error}
                </p>
            ) : null}
        </>
    );
}
