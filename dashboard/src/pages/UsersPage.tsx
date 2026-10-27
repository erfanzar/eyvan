// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Copy, KeyRound, UserPlus } from "lucide-react";
import { useCallback, useEffect, useState, type FormEvent } from "react";

import type { TeamInvite, TeamInviteCreated, TeamInviteInput, TeamToken, TeamTokenCreated, TeamUser } from "../types";

export interface TeamClient {
  listTeamUsers: (signal?: AbortSignal) => Promise<TeamUser[]>;
  setTeamUserDisabled: (subject: string, disabled: boolean) => Promise<TeamUser>;
  listTeamInvites: (signal?: AbortSignal) => Promise<TeamInvite[]>;
  createTeamInvite: (input: TeamInviteInput) => Promise<TeamInviteCreated>;
  revokeTeamInvite: (inviteId: string) => Promise<TeamInvite>;
  listTeamTokens: (signal?: AbortSignal) => Promise<TeamToken[]>;
  createTeamToken: (name: string, expiresInDays: number | null) => Promise<TeamTokenCreated>;
  revokeTeamToken: (tokenId: string) => Promise<TeamToken>;
}

interface UsersPageProps {
  client: TeamClient;
  /** Fleet administrators manage accounts and invites; everyone manages their own tokens. */
  canAdminister: boolean;
  defaultOrg: string;
  defaultProject: string;
}

const ROLES = [
  "project_submitter",
  "project_operator",
  "project_viewer",
  "project_admin",
  "org_admin",
  "fleet_admin",
] as const;

const message = (error: unknown, fallback: string) => (error instanceof Error ? error.message : fallback);
const when = (value: string | null) => (value ? new Date(value).toLocaleString() : "—");

function CopyOnce({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="panel-note" role="status">
      <strong>{label}</strong> (shown once):{" "}
      <code className="monospace" data-testid="secret-once">
        {value}
      </code>{" "}
      <button
        className="button button-secondary"
        type="button"
        onClick={() => {
          void navigator.clipboard?.writeText(value).then(() => setCopied(true));
        }}
      >
        <Copy size={14} aria-hidden="true" /> {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

export function UsersPage({ client, canAdminister, defaultOrg, defaultProject }: UsersPageProps) {
  const [users, setUsers] = useState<TeamUser[]>([]);
  const [invites, setInvites] = useState<TeamInvite[]>([]);
  const [tokens, setTokens] = useState<TeamToken[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);

  const [email, setEmail] = useState("");
  const [subject, setSubject] = useState("");
  const [role, setRole] = useState<string>("project_submitter");
  const [inviteBusy, setInviteBusy] = useState(false);
  const [inviteLink, setInviteLink] = useState<string | null>(null);

  const [tokenName, setTokenName] = useState("");
  const [tokenBusy, setTokenBusy] = useState(false);
  const [newToken, setNewToken] = useState<string | null>(null);

  const reload = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    const reads: Promise<unknown>[] = [
      client.listTeamTokens(controller.signal).then((value) => {
        if (!controller.signal.aborted) setTokens(value);
      }),
    ];
    if (canAdminister) {
      reads.push(
        client.listTeamUsers(controller.signal).then((value) => {
          if (!controller.signal.aborted) setUsers(value);
        }),
        client.listTeamInvites(controller.signal).then((value) => {
          if (!controller.signal.aborted) setInvites(value);
        }),
      );
    }
    Promise.all(reads).catch((failure: unknown) => {
      if (!controller.signal.aborted) setError(message(failure, "Could not read team accounts"));
    });
    return () => controller.abort();
  }, [canAdminister, client, revision]);

  const act = async (action: () => Promise<unknown>) => {
    setError(null);
    try {
      await action();
      reload();
    } catch (failure) {
      setError(message(failure, "The change was not applied"));
    }
  };

  const invite = async (event: FormEvent) => {
    event.preventDefault();
    setInviteBusy(true);
    setError(null);
    setInviteLink(null);
    const scoped = role !== "fleet_admin";
    try {
      const created = await client.createTeamInvite({
        email: email.trim(),
        subject: (subject.trim() || email.split("@", 1)[0]).toLowerCase(),
        role,
        org_id: scoped ? defaultOrg : null,
        project_id: role.startsWith("project_") ? defaultProject : null,
      });
      setInviteLink(created.url);
      setEmail("");
      setSubject("");
      reload();
    } catch (failure) {
      setError(message(failure, "Could not create the invite"));
    } finally {
      setInviteBusy(false);
    }
  };

  const createToken = async (event: FormEvent) => {
    event.preventDefault();
    setTokenBusy(true);
    setError(null);
    setNewToken(null);
    try {
      const created = await client.createTeamToken(tokenName.trim(), null);
      setNewToken(created.token);
      setTokenName("");
      reload();
    } catch (failure) {
      setError(message(failure, "Could not create the token"));
    } finally {
      setTokenBusy(false);
    }
  };

  return (
    <div className="page-stack">
      {error && (
        <div className="inline-error" role="alert">
          <AlertTriangle size={16} aria-hidden="true" />
          {error}
        </div>
      )}

      {canAdminister && (
        <section className="panel data-panel" aria-label="Invite a user">
          <div className="panel-header">
            <div>
              <h2>Invite a user</h2>
              <p>
                They open the link, choose a password and are signed in. Project roles apply to {defaultOrg}/
                {defaultProject}.
              </p>
            </div>
          </div>
          <form className="form-grid form-grid-two" onSubmit={(event) => void invite(event)}>
            <label className="field">
              <span>Email</span>
              <input type="email" value={email} onChange={(event) => setEmail(event.target.value)} required />
            </label>
            <label className="field">
              <span>User name (optional)</span>
              <input
                value={subject}
                placeholder="From the email when empty"
                onChange={(event) => setSubject(event.target.value)}
              />
            </label>
            <label className="field">
              <span>Role</span>
              <select value={role} onChange={(event) => setRole(event.target.value)}>
                {ROLES.map((name) => (
                  <option key={name} value={name}>
                    {name.replace("_", " ")}
                  </option>
                ))}
              </select>
            </label>
            <div className="field">
              <span>&nbsp;</span>
              <button className="button button-primary" type="submit" disabled={inviteBusy || !email.trim()}>
                <UserPlus size={16} aria-hidden="true" /> Create invite
              </button>
            </div>
          </form>
          {inviteLink && <CopyOnce label="Invite link — send it privately" value={inviteLink} />}
        </section>
      )}

      {canAdminister && (
        <section className="panel data-panel" aria-label="Users">
          <div className="panel-header">
            <div>
              <h2>Users</h2>
              <p>Disabling an account signs it out everywhere and revokes its tokens.</p>
            </div>
          </div>
          {users.length === 0 ? (
            <p className="panel-note">No accounts yet. Invite someone above.</p>
          ) : (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>User</th>
                    <th>Email</th>
                    <th>Status</th>
                    <th>Created</th>
                    <th aria-label="Actions" />
                  </tr>
                </thead>
                <tbody>
                  {users.map((user) => (
                    <tr key={user.subject}>
                      <td className="monospace">{user.subject}</td>
                      <td>{user.email}</td>
                      <td>{user.disabled ? "disabled" : "active"}</td>
                      <td>{when(user.created_at)}</td>
                      <td>
                        <button
                          className="button button-secondary"
                          type="button"
                          onClick={() => void act(() => client.setTeamUserDisabled(user.subject, !user.disabled))}
                        >
                          {user.disabled ? "Enable" : "Disable"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {canAdminister && (
        <section className="panel data-panel" aria-label="Invites">
          <div className="panel-header">
            <div>
              <h2>Invites</h2>
            </div>
          </div>
          {invites.length === 0 ? (
            <p className="panel-note">No invites.</p>
          ) : (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Email</th>
                    <th>User</th>
                    <th>Role</th>
                    <th>State</th>
                    <th aria-label="Actions" />
                  </tr>
                </thead>
                <tbody>
                  {invites.map((item) => {
                    const open = !item.revoked && !item.accepted_at && new Date(item.expires_at) > new Date();
                    const state = item.accepted_at
                      ? "accepted"
                      : item.revoked
                        ? "revoked"
                        : open
                          ? `open until ${when(item.expires_at)}`
                          : "expired";
                    return (
                      <tr key={item.id}>
                        <td>{item.email}</td>
                        <td className="monospace">{item.subject}</td>
                        <td>{item.roles.map((grant) => grant.role).join(", ") || "—"}</td>
                        <td>{state}</td>
                        <td>
                          {open && (
                            <button
                              className="button button-secondary"
                              type="button"
                              onClick={() => void act(() => client.revokeTeamInvite(item.id))}
                            >
                              Revoke
                            </button>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      <section className="panel data-panel" aria-label="My API tokens">
        <div className="panel-header">
          <div>
            <h2>My API tokens</h2>
            <p>
              For scripts and CI: set <span className="monospace">EYVAN_TOKEN</span>. On your own machine,{" "}
              <span className="monospace">eyvan login</span> creates one for you.
            </p>
          </div>
        </div>
        <form className="form-grid form-grid-two" onSubmit={(event) => void createToken(event)}>
          <label className="field">
            <span>Token name</span>
            <input value={tokenName} onChange={(event) => setTokenName(event.target.value)} maxLength={64} />
          </label>
          <div className="field">
            <span>&nbsp;</span>
            <button className="button button-primary" type="submit" disabled={tokenBusy || !tokenName.trim()}>
              <KeyRound size={16} aria-hidden="true" /> Create token
            </button>
          </div>
        </form>
        {newToken && <CopyOnce label="New token" value={newToken} />}
        {tokens.length === 0 ? (
          <p className="panel-note">No personal tokens.</p>
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Starts with</th>
                  <th>Last used</th>
                  <th>State</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {tokens.map((token) => (
                  <tr key={token.id}>
                    <td>{token.name}</td>
                    <td className="monospace">{token.prefix}…</td>
                    <td>{when(token.last_used_at)}</td>
                    <td>{token.revoked_at ? "revoked" : "active"}</td>
                    <td>
                      {!token.revoked_at && (
                        <button
                          className="button button-secondary"
                          type="button"
                          onClick={() => void act(() => client.revokeTeamToken(token.id))}
                        >
                          Revoke
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
