// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { EyvanApiClient } from "../api/client";
import type { AssistantTurn, TeamRequest } from "../types";
import { AssistantPage, type AssistantClient } from "./AssistantPage";

const REQUEST: TeamRequest = {
  id: "req_0123456789abcdef",
  kind: "capacity",
  subject: "alice",
  org_id: "acme",
  project_id: "r",
  summary: "need a v5p-32",
  details: {},
  state: "open",
  created_at: "2026-10-27T10:00:00+00:00",
  resolved_at: null,
  resolved_by: null,
  resolution_note: "",
};

function turn(overrides: Partial<AssistantTurn> = {}): AssistantTurn {
  return { reply: "ok", steps: 1, tool_calls: [], drafts: [], requests: [], ...overrides };
}

function mockClient(chat: AssistantTurn = turn()): AssistantClient {
  return {
    getAssistantStatus: vi.fn(async () => ({ enabled: true, model: "test-model" })),
    assistantChat: vi.fn(async () => chat),
    submitAssistantDraft: vi.fn(async () => ({ id: "wl_1" })),
    listTeamRequests: vi.fn(async () => [REQUEST]),
    resolveTeamRequest: vi.fn(async () => ({ ...REQUEST, state: "approved" as const })),
  };
}

async function ask(text: string) {
  const form = await screen.findByRole("form", { name: "Ask" });
  fireEvent.change(within(form).getByLabelText("Message"), { target: { value: text } });
  fireEvent.submit(form);
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Assistant page", () => {
  it("explains an unconfigured plane instead of showing a chat box", async () => {
    const client = mockClient();
    vi.mocked(client.getAssistantStatus).mockResolvedValueOnce({ enabled: false, model: null });
    render(<AssistantPage client={client} canAdminister={false} teamMode={false} />);
    await screen.findByText(/no assistant configured/);
    expect(screen.queryByRole("form", { name: "Ask" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Requests" })).toBeNull();
  });

  it("sends the conversation and shows the reply", async () => {
    const client = mockClient(turn({ reply: "Use a v5p-8." }));
    render(<AssistantPage client={client} canAdminister={false} teamMode />);
    await ask("how big a TPU for llama 8b?");
    await screen.findByText("Use a v5p-8.");
    expect(client.assistantChat).toHaveBeenCalledWith([{ role: "user", content: "how big a TPU for llama 8b?" }]);
  });

  it("never submits a draft until the user confirms it", async () => {
    const draft = { org_id: "acme", project_id: "r", spec: { name: "llama-sft", execution: { kind: "command" } } };
    const client = mockClient(turn({ reply: "Draft ready.", drafts: [draft] }));
    render(<AssistantPage client={client} canAdminister={false} teamMode />);
    await ask("draft my job");
    const drafted = await screen.findByTestId("assistant-draft");
    expect(within(drafted).getByText("llama-sft")).toBeTruthy();
    expect(client.submitAssistantDraft).not.toHaveBeenCalled();

    fireEvent.click(within(drafted).getByRole("button", { name: /Submit this workload/ }));
    await within(drafted).findByText("Submitted wl_1");
    expect(client.submitAssistantDraft).toHaveBeenCalledWith(draft);
  });

  it("a dismissed draft is never submitted", async () => {
    const draft = { org_id: "acme", project_id: "r", spec: { name: "x" } };
    const client = mockClient(turn({ drafts: [draft] }));
    render(<AssistantPage client={client} canAdminister={false} teamMode />);
    await ask("draft");
    const drafted = await screen.findByTestId("assistant-draft");
    fireEvent.click(within(drafted).getByRole("button", { name: /Dismiss/ }));
    await within(drafted).findByText("Dismissed");
    expect(client.submitAssistantDraft).not.toHaveBeenCalled();
  });

  it("lets an admin approve and a filer withdraw an open request", async () => {
    const admin = mockClient();
    render(<AssistantPage client={admin} canAdminister teamMode />);
    const requests = await screen.findByRole("region", { name: "Requests" });
    await within(requests).findByText("need a v5p-32");
    fireEvent.click(within(requests).getByRole("button", { name: "Approve" }));
    await waitFor(() => expect(admin.resolveTeamRequest).toHaveBeenCalledWith(REQUEST.id, "approved"));
    cleanup();

    const filer = mockClient();
    render(<AssistantPage client={filer} canAdminister={false} teamMode />);
    const mine = await screen.findByRole("region", { name: "Requests" });
    await within(mine).findByText("need a v5p-32");
    expect(within(mine).queryByRole("button", { name: "Approve" })).toBeNull();
    fireEvent.click(within(mine).getByRole("button", { name: "Withdraw" }));
    await waitFor(() => expect(filer.resolveTeamRequest).toHaveBeenCalledWith(REQUEST.id, "withdrawn"));
  });

  it("surfaces a provider failure as an alert", async () => {
    const client = mockClient();
    vi.mocked(client.assistantChat).mockRejectedValueOnce(new Error("Eyvan API request failed with 502"));
    render(<AssistantPage client={client} canAdminister={false} teamMode={false} />);
    await ask("hi");
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("502"));
  });

  it("renders with the real API client class (methods are called on the client, not detached)", async () => {
    render(<AssistantPage client={new EyvanApiClient("/api/v1", true)} canAdminister teamMode />);
    await screen.findByText(/no assistant configured/);
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
