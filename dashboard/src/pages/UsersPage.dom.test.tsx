// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { EyvanApiClient } from "../api/client";
import type { TeamInvite, TeamToken, TeamUser } from "../types";
import { UsersPage, type TeamClient } from "./UsersPage";

const USER: TeamUser = {
  subject: "alice",
  email: "alice@example.com",
  display_name: "Alice",
  disabled: false,
  created_at: "2026-10-27T10:00:00+00:00",
  created_by: "root",
};
const TOKEN: TeamToken = {
  id: "tok_1",
  name: "laptop",
  prefix: "eyv_abcd",
  created_at: "2026-10-27T10:00:00+00:00",
  expires_at: null,
  last_used_at: null,
  revoked_at: null,
};
const INVITE: TeamInvite = {
  id: "inv_1",
  email: "carol@example.com",
  subject: "carol",
  roles: [{ role: "project_viewer", org_id: "acme", project_id: "r" }],
  created_by: "alice",
  created_at: "2026-10-27T10:00:00+00:00",
  expires_at: "2999-01-01T00:00:00+00:00",
  accepted_at: null,
  revoked: false,
};

function mockClient(): TeamClient {
  return {
    listTeamUsers: vi.fn(async () => [USER]),
    setTeamUserDisabled: vi.fn(async () => ({ ...USER, disabled: true })),
    listTeamInvites: vi.fn(async () => [INVITE]),
    createTeamInvite: vi.fn(async () => ({
      invite: { ...INVITE, id: "inv_2", email: "bob@example.com", subject: "bob" },
      url: "https://plane.example.com/#/accept-invite/secret-link",
      token: "secret-link",
    })),
    revokeTeamInvite: vi.fn(async () => ({ ...INVITE, revoked: true })),
    listTeamTokens: vi.fn(async () => [TOKEN]),
    createTeamToken: vi.fn(async () => ({ token: "eyv_newsecret", record: { ...TOKEN, id: "tok_2", name: "ci" } })),
    revokeTeamToken: vi.fn(async () => ({ ...TOKEN, revoked_at: "2026-10-27T11:00:00+00:00" })),
  };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Settings → Users & tokens", () => {
  it("lets a fleet admin see users and invites, invite someone and copy the link once", async () => {
    const client = mockClient();
    render(<UsersPage client={client} canAdminister defaultOrg="acme" defaultProject="r" />);

    const users = await screen.findByRole("region", { name: "Users" });
    await within(users).findByText("alice@example.com");
    await within(screen.getByRole("region", { name: "Invites" })).findByText("carol@example.com");

    const invite = screen.getByRole("region", { name: "Invite a user" });
    fireEvent.change(within(invite).getByLabelText("Email"), { target: { value: "Bob@example.com" } });
    fireEvent.click(within(invite).getByRole("button", { name: /Create invite/ }));

    await waitFor(() =>
      expect(within(invite).getByTestId("secret-once").textContent).toBe(
        "https://plane.example.com/#/accept-invite/secret-link",
      ),
    );
    expect(client.createTeamInvite).toHaveBeenCalledWith({
      email: "Bob@example.com",
      subject: "bob",
      role: "project_submitter",
      org_id: "acme",
      project_id: "r",
    });
  });

  it("disables a user and revokes an open invite", async () => {
    const client = mockClient();
    render(<UsersPage client={client} canAdminister defaultOrg="acme" defaultProject="r" />);

    const users = await screen.findByRole("region", { name: "Users" });
    fireEvent.click(await within(users).findByRole("button", { name: "Disable" }));
    await waitFor(() => expect(client.setTeamUserDisabled).toHaveBeenCalledWith("alice", true));

    const invites = screen.getByRole("region", { name: "Invites" });
    fireEvent.click(await within(invites).findByRole("button", { name: "Revoke" }));
    await waitFor(() => expect(client.revokeTeamInvite).toHaveBeenCalledWith("inv_1"));
  });

  it("shows a non-admin only their own tokens, and a new token exactly once", async () => {
    const client = mockClient();
    render(<UsersPage client={client} canAdminister={false} defaultOrg="acme" defaultProject="r" />);

    const tokens = await screen.findByRole("region", { name: "My API tokens" });
    await within(tokens).findByText("laptop");
    expect(screen.queryByRole("region", { name: "Invite a user" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Users" })).toBeNull();
    expect(client.listTeamUsers).not.toHaveBeenCalled();
    expect(client.listTeamInvites).not.toHaveBeenCalled();

    fireEvent.change(within(tokens).getByLabelText("Token name"), { target: { value: "ci" } });
    fireEvent.click(within(tokens).getByRole("button", { name: /Create token/ }));
    await waitFor(() => expect(within(tokens).getByTestId("secret-once").textContent).toBe("eyv_newsecret"));
    expect(client.createTeamToken).toHaveBeenCalledWith("ci", null);

    fireEvent.click(within(tokens).getAllByRole("button", { name: "Revoke" })[0]);
    await waitFor(() => expect(client.revokeTeamToken).toHaveBeenCalledWith("tok_1"));
  });

  it("surfaces a refused read instead of an empty page", async () => {
    const client = mockClient();
    vi.mocked(client.listTeamUsers).mockRejectedValueOnce(new Error("Eyvan API request failed with 403"));
    render(<UsersPage client={client} canAdminister defaultOrg="acme" defaultProject="r" />);

    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("403"));
  });

  it("renders with the real API client class (methods are called on the client, not detached)", async () => {
    const client = new EyvanApiClient("/api/v1", true);
    render(<UsersPage client={client} canAdminister defaultOrg="acme" defaultProject="r" />);

    await screen.findByText("No accounts yet. Invite someone above.");
    expect(screen.getByText("No personal tokens.")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
