// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { EyvanApiClient } from "../api/client";
import type { AssistantProvider, AssistantTurn, TeamRequest } from "../types";
import { AssistantDock, CONVERSATION_KEY, type AssistantDockClient, type AssistantDockProps } from "./AssistantDock";

const PRIMARY: AssistantProvider = {
  id: "provider_1",
  name: "Primary",
  models: ["quick", "careful"],
  default_model: "careful",
  enabled: true,
};
const OTHER: AssistantProvider = {
  id: "provider_2",
  name: "Other",
  models: ["small", "large"],
  default_model: "small",
};
const DISABLED: AssistantProvider = {
  id: "provider_3",
  name: "Off",
  models: ["x"],
  default_model: "x",
  enabled: false,
};

const REQUEST: TeamRequest = {
  id: "req_1",
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

const turn = (overrides: Partial<AssistantTurn> = {}): AssistantTurn => ({
  reply: "ok",
  steps: 1,
  tool_calls: [],
  drafts: [],
  requests: [],
  ...overrides,
});

function mockClient(chat: AssistantTurn = turn(), providers: AssistantProvider[] = [PRIMARY, OTHER, DISABLED]) {
  return {
    listAssistantProviders: vi.fn(async () => ({ providers, can_manage: false })),
    assistantChat: vi.fn<AssistantDockClient["assistantChat"]>(async () => chat),
    submitAssistantDraft: vi.fn(async () => ({ id: "wl_1" })),
    listTeamRequests: vi.fn(async () => [REQUEST]),
    resolveTeamRequest: vi.fn(async () => ({ ...REQUEST, state: "approved" as const })),
  } satisfies AssistantDockClient;
}

function renderDock(client: AssistantDockClient, props: Partial<AssistantDockProps> = {}) {
  return render(
    <AssistantDock
      client={client}
      open
      width={420}
      onWidthChange={() => undefined}
      onClose={() => undefined}
      canAdminister={false}
      teamMode={false}
      {...props}
    />,
  );
}

async function ask(text: string) {
  const box = await screen.findByLabelText("Message the assistant");
  fireEvent.change(box, { target: { value: text } });
  fireEvent.keyDown(box, { key: "Enter" });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  sessionStorage.clear();
  localStorage.clear();
});

describe("AssistantDock", () => {
  it("is a labelled region, hidden when closed, and reads providers only once opened", async () => {
    const client = mockClient();
    const { rerender } = renderDock(client, { open: false });
    expect(screen.queryByRole("complementary", { name: "Assistant panel" })).toBeNull();
    expect(client.listAssistantProviders).not.toHaveBeenCalled();
    rerender(
      <AssistantDock
        client={client}
        open
        width={420}
        onWidthChange={() => undefined}
        onClose={() => undefined}
        canAdminister={false}
        teamMode={false}
      />,
    );
    expect(await screen.findByRole("complementary", { name: "Assistant panel" })).toBeTruthy();
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(1);
  });

  it("lists enabled providers as provider · model and sends the chosen selection", async () => {
    const client = mockClient(turn({ reply: "Use a v5p-8." }));
    renderDock(client);
    const picker = await screen.findByRole("button", { name: "Assistant model" });
    expect(picker.textContent).toContain("Primary");
    expect(picker.textContent).toContain("careful");
    fireEvent.click(picker);
    const list = screen.getByRole("listbox", { name: "Assistant models" });
    expect(
      within(list)
        .getAllByRole("option")
        .map((option) => option.getAttribute("aria-label")),
    ).toEqual(["Primary · quick", "Primary · careful", "Other · small", "Other · large"]);
    fireEvent.change(screen.getByRole("combobox", { name: "Search models" }), { target: { value: "lar" } });
    expect(within(list).getAllByRole("option")).toHaveLength(1);
    fireEvent.click(within(list).getByRole("option", { name: "Other · large" }));
    expect(screen.queryByRole("listbox")).toBeNull();

    await ask("how big a TPU for llama 8b?");
    await screen.findByText("Use a v5p-8.");
    expect(client.assistantChat).toHaveBeenCalledWith(
      [{ role: "user", content: "how big a TPU for llama 8b?" }],
      { provider_id: OTHER.id, model: "large" },
      undefined,
      expect.any(AbortSignal),
    );
  });

  it("sends on Enter but not on Shift+Enter, and starter suggestions send too", async () => {
    const client = mockClient();
    renderDock(client);
    const box = (await screen.findByLabelText("Message the assistant")) as HTMLTextAreaElement;
    expect(box.tagName).toBe("TEXTAREA");
    fireEvent.change(box, { target: { value: "line one" } });
    fireEvent.keyDown(box, { key: "Enter", shiftKey: true });
    expect(client.assistantChat).not.toHaveBeenCalled();
    fireEvent.keyDown(box, { key: "Enter" });
    await waitFor(() => expect(client.assistantChat).toHaveBeenCalledTimes(1));
    await screen.findByText("ok");
    expect(box.value).toBe("");
    cleanup();

    const fresh = mockClient();
    sessionStorage.clear();
    renderDock(fresh);
    fireEvent.click(await screen.findByRole("button", { name: "What's my quota?" }));
    await waitFor(() =>
      expect(fresh.assistantChat.mock.calls[0][0]).toEqual([{ role: "user", content: "What's my quota?" }]),
    );
  });

  it("shows tool-call chips and a typing indicator while waiting", async () => {
    let finish: (value: AssistantTurn) => void = () => undefined;
    const client = mockClient();
    client.assistantChat.mockImplementationOnce(() => new Promise((resolve) => (finish = resolve)));
    renderDock(client);
    await ask("quota?");
    expect(await screen.findByRole("status", { name: "Assistant is thinking" })).toBeTruthy();
    finish(turn({ reply: "You have 8.", tool_calls: [{ tool: "get_quota", ok: true }] }));
    await screen.findByText("You have 8.");
    expect(screen.getByText(/used get_quota ✓/)).toBeTruthy();
    expect(screen.queryByRole("status", { name: "Assistant is thinking" })).toBeNull();
  });

  it("stops a pending answer by aborting its request", async () => {
    let signal: AbortSignal | undefined;
    const client = mockClient();
    client.assistantChat.mockImplementationOnce(
      (_messages, _selection, _scope, given) =>
        new Promise((_resolve, reject) => {
          signal = given;
          given?.addEventListener("abort", () => reject(new Error("aborted")));
        }),
    );
    renderDock(client);
    await ask("long question");
    fireEvent.click(await screen.findByRole("button", { name: "Stop generating" }));
    expect(signal?.aborted).toBe(true);
    await screen.findByRole("button", { name: "Send message" });
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("never submits a drafted workload until the user confirms it", async () => {
    const draft = { org_id: "acme", project_id: "r", spec: { name: "llama-sft", execution: { kind: "command" } } };
    const client = mockClient(turn({ reply: "Draft ready.", drafts: [draft] }));
    const onOpenWorkload = vi.fn();
    renderDock(client, { onOpenWorkload });
    await ask("draft my job");
    const card = await screen.findByTestId("assistant-draft");
    expect(within(card).getByText("llama-sft")).toBeTruthy();
    fireEvent.click(within(card).getByRole("button", { name: "Submit" }));
    expect(client.submitAssistantDraft).not.toHaveBeenCalled();
    fireEvent.click(within(card).getByRole("button", { name: "Cancel" }));
    fireEvent.click(within(card).getByRole("button", { name: "Submit" }));
    fireEvent.click(within(card).getByRole("button", { name: "Confirm submit" }));
    await within(card).findByText("Submitted wl_1");
    expect(client.submitAssistantDraft).toHaveBeenCalledWith(draft);
    fireEvent.click(within(card).getByRole("button", { name: "Open" }));
    expect(onOpenWorkload).toHaveBeenCalledWith("wl_1");
  });

  it("a dismissed draft is never submitted, and filed requests render as notes", async () => {
    const draft = { org_id: "acme", project_id: "r", spec: { name: "x" } };
    const client = mockClient(turn({ drafts: [draft], requests: [REQUEST] }));
    renderDock(client);
    await ask("draft");
    const card = await screen.findByTestId("assistant-draft");
    fireEvent.click(within(card).getByRole("button", { name: "Dismiss" }));
    await within(card).findByText("Dismissed");
    expect(client.submitAssistantDraft).not.toHaveBeenCalled();
    expect(screen.getByTestId("assistant-request").textContent).toContain("need a v5p-32");
  });

  it("tells an admin to set up a provider and everyone else to ask an admin", async () => {
    const onOpenSettings = vi.fn();
    renderDock(mockClient(turn(), []), { canAdminister: true, onOpenSettings });
    await screen.findByText("No assistant provider is configured yet.");
    fireEvent.click(screen.getByRole("button", { name: "Set up a provider" }));
    expect(onOpenSettings).toHaveBeenCalled();
    expect(screen.queryByLabelText("Message the assistant")).toBeNull();
    cleanup();

    renderDock(mockClient(turn(), [DISABLED]), { canAdminister: false });
    await screen.findByText("No assistant provider is configured yet.");
    expect(screen.getByText("Ask a fleet admin to add one.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Set up a provider" })).toBeNull();
  });

  it("keeps the conversation across remounts and clears it on New chat", async () => {
    const client = mockClient(turn({ reply: "Remembered answer" }));
    renderDock(client);
    await ask("remember me");
    await screen.findByText("Remembered answer");
    cleanup();

    renderDock(mockClient());
    expect(await screen.findByText("Remembered answer")).toBeTruthy();
    expect(screen.getByText("remember me")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "New chat" }));
    expect(screen.queryByText("Remembered answer")).toBeNull();
    expect(sessionStorage.getItem(CONVERSATION_KEY)).toBeNull();
  });

  it("renders replies as text: HTML in a reply is never interpreted", async () => {
    const reply = [
      'Look: <img src="x" onerror="alert(1)"> and <script>alert(2)</script>',
      "",
      "- use `v5p-8`",
      "- **spot** capacity",
      "",
      "```bash",
      "eyvan submit <job>",
      "```",
    ].join("\n");
    const client = mockClient(turn({ reply }));
    const { container } = renderDock(client);
    await ask("hi");
    await screen.findByText(/Look:/);
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("script")).toBeNull();
    expect(container.textContent).toContain('<img src="x" onerror="alert(1)">');
    const log = screen.getByRole("log", { name: "Conversation" });
    expect(within(log).getAllByRole("listitem")).toHaveLength(2);
    expect(log.querySelector("li code")?.textContent).toBe("v5p-8");
    expect(log.querySelector("li strong")?.textContent).toBe("spot");
    expect(log.querySelector("pre code")?.textContent).toBe("eyvan submit <job>");
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    fireEvent.click(within(log).getByRole("button", { name: "Copy code" }));
    expect(writeText).toHaveBeenCalledWith("eyvan submit <job>");
  });

  it("closes on Escape and from its close button", async () => {
    const onClose = vi.fn();
    renderDock(mockClient(), { onClose });
    await screen.findByLabelText("Message the assistant");
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("button", { name: "Close assistant" }));
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it("resizes from the keyboard on its left-edge handle", async () => {
    const onWidthChange = vi.fn();
    renderDock(mockClient(), { onWidthChange });
    const handle = await screen.findByRole("separator", { name: "Resize assistant panel" });
    fireEvent.keyDown(handle, { key: "ArrowLeft" });
    expect(onWidthChange).toHaveBeenCalledWith(436);
  });

  it("shows requests in a second tab where admins approve and filers withdraw", async () => {
    const admin = mockClient();
    renderDock(admin, { canAdminister: true, teamMode: true });
    fireEvent.click(await screen.findByRole("tab", { name: "Requests" }));
    const panel = screen.getByRole("tabpanel", { name: "Requests" });
    await within(panel).findByText("need a v5p-32");
    fireEvent.click(within(panel).getByRole("button", { name: "Approve" }));
    await waitFor(() => expect(admin.resolveTeamRequest).toHaveBeenCalledWith(REQUEST.id, "approved"));
    cleanup();

    const filer = mockClient();
    renderDock(filer, { teamMode: true });
    fireEvent.click(await screen.findByRole("tab", { name: "Requests" }));
    const mine = screen.getByRole("tabpanel", { name: "Requests" });
    await within(mine).findByText("need a v5p-32");
    expect(within(mine).queryByRole("button", { name: "Approve" })).toBeNull();
    fireEvent.click(within(mine).getByRole("button", { name: "Withdraw" }));
    await waitFor(() => expect(filer.resolveTeamRequest).toHaveBeenCalledWith(REQUEST.id, "withdrawn"));
  });

  it("surfaces a provider failure as an alert", async () => {
    const client = mockClient();
    client.assistantChat.mockRejectedValueOnce(new Error("Eyvan API request failed with 502"));
    renderDock(client);
    await ask("hi");
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("502"));
  });

  it("renders with the real API client class in demo mode", async () => {
    renderDock(new EyvanApiClient("/api/v1", true), { canAdminister: true, onOpenSettings: () => undefined });
    await screen.findByText("No assistant provider is configured yet.");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("chats through the real API client class without detaching its methods", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const path = String(input);
      let body: unknown;
      if (path === "/api/v1/assistant/providers") body = { providers: [PRIMARY], can_manage: false };
      else if (path === "/api/v1/assistant/chat" && init?.method === "POST") body = turn({ reply: "real answer" });
      else throw new Error(`Unexpected request: ${path}`);
      return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
    });
    renderDock(new EyvanApiClient("/api/v1", false));
    await ask("hello");
    await screen.findByText("real answer");
    const chat = fetch.mock.calls.find(([path]) => String(path) === "/api/v1/assistant/chat");
    expect(JSON.parse(String(chat?.[1]?.body))).toMatchObject({
      messages: [{ role: "user", content: "hello" }],
      provider_id: PRIMARY.id,
      model: "careful",
    });
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
