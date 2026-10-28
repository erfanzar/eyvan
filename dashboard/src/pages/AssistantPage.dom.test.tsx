// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { EyvanApiClient } from "../api/client";
import type { AssistantProvider, AssistantProviderUpdate, AssistantTurn, TeamRequest } from "../types";
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

const PROVIDER: AssistantProvider = {
  id: "provider_1",
  name: "Primary",
  base_url: "https://api.example.com/v1",
  models: ["quick", "careful"],
  default_model: "careful",
  max_steps: 8,
  enabled: true,
  has_api_key: true,
};

const OTHER_PROVIDER: AssistantProvider = {
  id: "provider_2",
  name: "Alternative",
  models: ["small", "large"],
  default_model: "small",
};

function mockClient(chat: AssistantTurn = turn(), providers: AssistantProvider[] = [PROVIDER]): AssistantClient {
  return {
    getAssistantStatus: vi.fn(async () => ({
      enabled: providers.some((provider) => provider.enabled !== false),
      model: providers[0]?.default_model ?? null,
      providers: providers.length,
    })),
    listAssistantProviders: vi.fn(async () => ({ providers, can_manage: true })),
    createAssistantProvider: vi
      .fn<NonNullable<AssistantClient["createAssistantProvider"]>>()
      .mockResolvedValue(PROVIDER),
    updateAssistantProvider: vi
      .fn<NonNullable<AssistantClient["updateAssistantProvider"]>>()
      .mockResolvedValue(PROVIDER),
    deleteAssistantProvider: vi.fn<NonNullable<AssistantClient["deleteAssistantProvider"]>>().mockResolvedValue({
      deleted: PROVIDER.id,
    }),
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
    const client = mockClient(turn(), []);
    vi.mocked(client.listAssistantProviders).mockResolvedValue({ providers: [], can_manage: false });
    render(<AssistantPage client={client} canAdminister={false} teamMode={false} />);
    await screen.findByText("No assistant provider is configured yet.");
    expect(screen.getByText("Ask a fleet admin to add one.")).toBeTruthy();
    expect(screen.queryByRole("form", { name: "Ask" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Assistant providers" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Requests" })).toBeNull();
  });

  it("sends the conversation and shows the reply", async () => {
    const client = mockClient(turn({ reply: "Use a v5p-8." }));
    render(<AssistantPage client={client} canAdminister={false} teamMode />);
    await ask("how big a TPU for llama 8b?");
    await screen.findByText("Use a v5p-8.");
    expect(client.assistantChat).toHaveBeenCalledWith([{ role: "user", content: "how big a TPU for llama 8b?" }], {
      provider_id: PROVIDER.id,
      model: PROVIDER.default_model,
    });
  });

  it("lists providers and their models, resets the model on provider changes, and sends each selection", async () => {
    const client = mockClient(turn(), [PROVIDER, OTHER_PROVIDER]);
    render(<AssistantPage client={client} canAdminister={false} teamMode={false} />);
    const provider = (await screen.findByLabelText("Provider")) as HTMLSelectElement;
    const model = screen.getByLabelText("Model") as HTMLSelectElement;
    expect(
      within(provider)
        .getAllByRole("option")
        .map((option) => option.textContent),
    ).toEqual(["Primary", "Alternative"]);
    expect(
      within(model)
        .getAllByRole("option")
        .map((option) => option.textContent),
    ).toEqual(["quick", "careful"]);
    expect(model.value).toBe("careful");
    expect(screen.queryByRole("region", { name: "Assistant providers" })).toBeNull();

    fireEvent.change(model, { target: { value: "quick" } });
    await ask("first question");
    await waitFor(() => expect(client.assistantChat).toHaveBeenCalledTimes(1));
    await screen.findByText("ok");
    expect(client.assistantChat).toHaveBeenLastCalledWith([{ role: "user", content: "first question" }], {
      provider_id: PROVIDER.id,
      model: "quick",
    });

    fireEvent.change(provider, { target: { value: OTHER_PROVIDER.id } });
    expect(
      within(model)
        .getAllByRole("option")
        .map((option) => option.textContent),
    ).toEqual(["small", "large"]);
    expect(model.value).toBe("small");
    fireEvent.change(model, { target: { value: "large" } });
    await ask("second question");
    await waitFor(() => expect(client.assistantChat).toHaveBeenCalledTimes(2));
    expect(client.assistantChat).toHaveBeenLastCalledWith(
      [
        { role: "user", content: "first question" },
        { role: "assistant", content: "ok" },
        { role: "user", content: "second question" },
      ],
      { provider_id: OTHER_PROVIDER.id, model: "large" },
    );
  });

  it("lets an admin add a provider, reloads the configuration, and clears its write-only key", async () => {
    const client = mockClient(turn(), []);
    const { container } = render(<AssistantPage client={client} canAdminister teamMode={false} />);
    await screen.findByText("Add one under Assistant providers below.");
    const form = screen.getByRole("form", { name: "Add provider" });
    const fields = within(form);
    fireEvent.change(fields.getByLabelText("Name"), { target: { value: " Primary " } });
    fireEvent.change(fields.getByLabelText("Base URL"), { target: { value: "https://api.example.com/v1" } });
    const key = fields.getByLabelText("API key") as HTMLInputElement;
    expect(key.type).toBe("password");
    expect(key.autocomplete).toBe("off");
    fireEvent.change(key, { target: { value: "write-only-secret-123" } });
    fireEvent.change(fields.getByLabelText("Models (comma separated)"), {
      target: { value: " quick, careful , , " },
    });
    expect((fields.getByLabelText("Default model") as HTMLSelectElement).value).toBe("quick");
    fireEvent.change(fields.getByLabelText("Default model"), { target: { value: "careful" } });
    fireEvent.change(fields.getByLabelText("Max steps"), { target: { value: "7" } });
    fireEvent.click(fields.getByLabelText("Enabled"));
    vi.mocked(client.listAssistantProviders).mockResolvedValue({ providers: [PROVIDER], can_manage: true });
    vi.mocked(client.getAssistantStatus).mockResolvedValue({ enabled: true, model: "careful", providers: 1 });
    fireEvent.submit(form);

    await waitFor(() =>
      expect(client.createAssistantProvider).toHaveBeenCalledWith({
        name: "Primary",
        base_url: "https://api.example.com/v1",
        api_key: "write-only-secret-123",
        models: ["quick", "careful"],
        default_model: "careful",
        max_steps: 7,
        enabled: false,
      }),
    );
    await screen.findByText("key set");
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(2);
    expect(client.getAssistantStatus).toHaveBeenCalledTimes(2);
    expect(
      (within(screen.getByRole("form", { name: "Add provider" })).getByLabelText("API key") as HTMLInputElement).value,
    ).toBe("");
    expect(container.innerHTML).not.toContain("write-only-secret-123");
    expect(container.textContent).not.toContain("write-only-secret-123");
  });

  it("edits a provider without sending an empty API key or clearing the stored key", async () => {
    const client = mockClient();
    render(<AssistantPage client={client} canAdminister teamMode={false} />);
    const row = await screen.findByRole("row", { name: /Primary/ });
    fireEvent.click(within(row).getByRole("button", { name: "Edit" }));
    const form = screen.getByRole("form", { name: "Edit Primary" });
    const key = within(form).getByLabelText("API key") as HTMLInputElement;
    expect(key.value).toBe("");
    expect(key.placeholder).toMatch(/kept when left empty/);
    fireEvent.change(within(form).getByLabelText("Name"), { target: { value: "Renamed" } });
    fireEvent.submit(form);
    await waitFor(() =>
      expect(client.updateAssistantProvider).toHaveBeenCalledWith(PROVIDER.id, {
        name: "Renamed",
        base_url: PROVIDER.base_url,
        models: PROVIDER.models,
        default_model: PROVIDER.default_model,
        max_steps: 8,
        enabled: true,
      }),
    );
    await screen.findByRole("form", { name: "Add provider" });
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(2);
    expect(client.getAssistantStatus).toHaveBeenCalledTimes(2);
  });

  it("explicitly removes a stored key without sending a replacement", async () => {
    const client = mockClient();
    render(<AssistantPage client={client} canAdminister teamMode={false} />);
    const row = await screen.findByRole("row", { name: /Primary/ });
    fireEvent.click(within(row).getByRole("button", { name: "Edit" }));
    const form = screen.getByRole("form", { name: "Edit Primary" });
    fireEvent.change(within(form).getByLabelText("API key"), { target: { value: "discarded-replacement" } });
    fireEvent.click(within(form).getByLabelText("Remove stored key"));
    expect((within(form).getByLabelText("API key") as HTMLInputElement).value).toBe("");
    fireEvent.submit(form);
    await waitFor(() =>
      expect(client.updateAssistantProvider).toHaveBeenCalledWith(PROVIDER.id, {
        name: PROVIDER.name,
        base_url: PROVIDER.base_url,
        models: PROVIDER.models,
        default_model: PROVIDER.default_model,
        max_steps: 8,
        enabled: true,
        clear_api_key: true,
      }),
    );
    await screen.findByRole("form", { name: "Add provider" });
  });

  it("asks for confirmation before deleting a provider and reloads after confirmation", async () => {
    const client = mockClient();
    const confirm = vi.spyOn(window, "confirm").mockReturnValueOnce(false).mockReturnValueOnce(true);
    render(<AssistantPage client={client} canAdminister teamMode={false} />);
    const row = await screen.findByRole("row", { name: /Primary/ });
    fireEvent.click(within(row).getByRole("button", { name: "Delete" }));
    expect(confirm).toHaveBeenCalledWith(expect.stringContaining("Primary"));
    expect(client.deleteAssistantProvider).not.toHaveBeenCalled();
    vi.mocked(client.listAssistantProviders).mockResolvedValue({ providers: [], can_manage: true });
    vi.mocked(client.getAssistantStatus).mockResolvedValue({ enabled: false, model: null, providers: 0 });
    fireEvent.click(within(row).getByRole("button", { name: "Delete" }));
    await screen.findByText("No assistant provider is configured yet.");
    expect(client.deleteAssistantProvider).toHaveBeenCalledWith(PROVIDER.id);
    expect(confirm).toHaveBeenCalledTimes(2);
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(2);
    expect(client.getAssistantStatus).toHaveBeenCalledTimes(2);
    expect(screen.queryByLabelText("Provider")).toBeNull();
  });

  it("toggles a provider with a partial update and keeps disabled providers out of the chat picker", async () => {
    const client = mockClient();
    render(<AssistantPage client={client} canAdminister teamMode={false} />);
    const row = await screen.findByRole("row", { name: /Primary/ });
    vi.mocked(client.listAssistantProviders).mockResolvedValue({
      providers: [{ ...PROVIDER, enabled: false }],
      can_manage: true,
    });
    vi.mocked(client.getAssistantStatus).mockResolvedValue({ enabled: false, model: null, providers: 0 });
    fireEvent.click(within(row).getByRole("button", { name: "Disable" }));
    await screen.findByRole("button", { name: "Enable" });
    expect(client.updateAssistantProvider).toHaveBeenLastCalledWith(PROVIDER.id, { enabled: false });
    expect(screen.queryByLabelText("Provider")).toBeNull();
    vi.mocked(client.listAssistantProviders).mockResolvedValue({ providers: [PROVIDER], can_manage: true });
    vi.mocked(client.getAssistantStatus).mockResolvedValue({ enabled: true, model: "careful", providers: 1 });
    fireEvent.click(within(row).getByRole("button", { name: "Enable" }));
    await screen.findByLabelText("Provider");
    expect(client.updateAssistantProvider).toHaveBeenLastCalledWith(PROVIDER.id, { enabled: true });
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(3);
    expect(client.getAssistantStatus).toHaveBeenCalledTimes(3);
  });

  it("shows provider management failures in the existing alert", async () => {
    const client = mockClient();
    vi.mocked(client.updateAssistantProvider!).mockRejectedValueOnce(new Error("Provider name already exists"));
    render(<AssistantPage client={client} canAdminister teamMode={false} />);
    const row = await screen.findByRole("row", { name: /Primary/ });
    fireEvent.click(within(row).getByRole("button", { name: "Disable" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Provider name already exists"));
    expect(client.listAssistantProviders).toHaveBeenCalledTimes(1);
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

  it("manages providers through the real API client class without detaching its methods", async () => {
    let providers: AssistantProvider[] = [];
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const path = String(input);
      let response: unknown;
      if (path === "/api/v1/assistant/status") {
        response = {
          enabled: providers.some((provider) => provider.enabled),
          model: "careful",
          providers: providers.length,
        };
      } else if (path === "/api/v1/assistant/providers" && init?.method === "POST") {
        providers = [PROVIDER];
        response = PROVIDER;
      } else if (path === "/api/v1/assistant/providers") {
        response = { providers, can_manage: true };
      } else if (path === `/api/v1/assistant/providers/${PROVIDER.id}` && init?.method === "PUT") {
        const changes = JSON.parse(String(init.body)) as AssistantProviderUpdate;
        providers = providers.map((provider) => ({
          ...provider,
          name: changes.name ?? provider.name,
          enabled: changes.enabled ?? provider.enabled,
        }));
        response = providers[0];
      } else if (path === `/api/v1/assistant/providers/${PROVIDER.id}` && init?.method === "DELETE") {
        providers = [];
        response = { deleted: PROVIDER.id };
      } else {
        throw new Error(`Unexpected request: ${path}`);
      }
      return new Response(JSON.stringify(response), { headers: { "content-type": "application/json" } });
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<AssistantPage client={new EyvanApiClient("/api/v1", false)} canAdminister teamMode={false} />);
    await screen.findByText("No assistant provider is configured yet.");
    const form = screen.getByRole("form", { name: "Add provider" });
    fireEvent.change(within(form).getByLabelText("Name"), { target: { value: PROVIDER.name } });
    fireEvent.change(within(form).getByLabelText("Base URL"), { target: { value: PROVIDER.base_url } });
    fireEvent.change(within(form).getByLabelText("Models (comma separated)"), {
      target: { value: PROVIDER.models.join(", ") },
    });
    fireEvent.submit(form);
    const row = await screen.findByRole("row", { name: /Primary/ });
    fireEvent.click(within(row).getByRole("button", { name: "Edit" }));
    const edit = screen.getByRole("form", { name: "Edit Primary" });
    fireEvent.change(within(edit).getByLabelText("Name"), { target: { value: "Updated" } });
    fireEvent.submit(edit);
    const updated = await screen.findByRole("row", { name: /Updated/ });
    fireEvent.click(within(updated).getByRole("button", { name: "Disable" }));
    await within(updated).findByRole("button", { name: "Enable" });
    fireEvent.click(within(updated).getByRole("button", { name: "Delete" }));
    await screen.findByText("No assistant provider is configured yet.");
    expect(fetch.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    expect(fetch.mock.calls.filter(([, init]) => init?.method === "PUT")).toHaveLength(2);
    expect(fetch.mock.calls.filter(([, init]) => init?.method === "DELETE")).toHaveLength(1);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("renders with the real API client class (methods are called on the client, not detached)", async () => {
    render(<AssistantPage client={new EyvanApiClient("/api/v1", true)} canAdminister teamMode />);
    await screen.findByText("No assistant provider is configured yet.");
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
