// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { afterEach, describe, expect, it, vi } from "vitest";

import { EyvanApiClient } from "./client";
import { configuredScope } from "../auth/oidc";
import type { AssistantMessage, AssistantProvider, AssistantProviderInput } from "../types";

const PROVIDER: AssistantProvider = {
  id: "provider_1",
  name: "Primary",
  base_url: "https://api.example.com/v1",
  models: ["quick", "careful"],
  default_model: "careful",
  enabled: true,
  has_api_key: true,
};

function mockResponse(body: unknown, status = 200) {
  const fetch = vi.fn().mockImplementation(
    async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

afterEach(() => vi.unstubAllGlobals());

describe("assistant provider API", () => {
  it("returns no providers in demo mode without issuing a request", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", true);
    await expect(client.listAssistantProviders()).resolves.toEqual({ providers: [], can_manage: false });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("reads the provider list with the configured API base and forwards cancellation", async () => {
    const body = { providers: [PROVIDER], can_manage: true };
    const fetch = mockResponse(body);
    const controller = new AbortController();
    const client = new EyvanApiClient("https://plane.example/api/v1", false);
    const result = client.listAssistantProviders(controller.signal);
    expect(fetch.mock.calls[0][0]).toBe("https://plane.example/api/v1/assistant/providers");
    const signal = fetch.mock.calls[0][1].signal as AbortSignal;
    controller.abort();
    expect(signal.aborted).toBe(true);
    await expect(result).resolves.toEqual(body);
  });

  it("creates a provider with its write-only key", async () => {
    const fetch = mockResponse(PROVIDER, 201);
    const input: AssistantProviderInput = {
      name: "Primary",
      base_url: "https://api.example.com/v1",
      models: ["quick", "careful"],
      default_model: "careful",
      max_steps: 8,
      enabled: true,
      api_key: "write-only-secret",
    };
    const result = await new EyvanApiClient("/api/v1", false).createAssistantProvider(input);
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/assistant/providers");
    expect(fetch.mock.calls[0][1].method).toBe("POST");
    expect(JSON.parse(fetch.mock.calls[0][1].body as string)).toEqual(input);
    expect(result).toEqual(PROVIDER);
    expect(result).not.toHaveProperty("api_key");
  });

  it("updates only supplied fields and encodes the provider id", async () => {
    const fetch = mockResponse(PROVIDER);
    await new EyvanApiClient("/api/v1", false).updateAssistantProvider("provider/1", { name: "Renamed" });
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/assistant/providers/provider%2F1");
    expect(fetch.mock.calls[0][1].method).toBe("PUT");
    expect(JSON.parse(fetch.mock.calls[0][1].body as string)).toEqual({ name: "Renamed" });
  });

  it("sends explicit key removal separately from a replacement key", async () => {
    const fetch = mockResponse(PROVIDER);
    const client = new EyvanApiClient("/api/v1", false);
    await client.updateAssistantProvider(PROVIDER.id, { clear_api_key: true });
    await client.updateAssistantProvider(PROVIDER.id, { api_key: "replacement-secret" });
    expect(JSON.parse(fetch.mock.calls[0][1].body as string)).toEqual({ clear_api_key: true });
    expect(JSON.parse(fetch.mock.calls[1][1].body as string)).toEqual({ api_key: "replacement-secret" });
  });

  it("deletes the requested provider", async () => {
    const fetch = mockResponse({ deleted: PROVIDER.id });
    await expect(new EyvanApiClient("/api/v1", false).deleteAssistantProvider(PROVIDER.id)).resolves.toEqual({
      deleted: PROVIDER.id,
    });
    expect(fetch.mock.calls[0][0]).toBe(`/api/v1/assistant/providers/${PROVIDER.id}`);
    expect(fetch.mock.calls[0][1].method).toBe("DELETE");
    expect(fetch.mock.calls[0][1].body).toBeUndefined();
  });

  it("preserves the additive provider count in assistant status", async () => {
    const body = { enabled: true, model: "careful", providers: 2 };
    const fetch = mockResponse(body);
    await expect(new EyvanApiClient("/api/v1", false).getAssistantStatus()).resolves.toEqual(body);
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/assistant/status");
  });

  it.each([undefined, null])("keeps unselected chat requests working (%s)", async (selection) => {
    const turn = { reply: "ok", steps: 1, tool_calls: [], drafts: [], requests: [] };
    const fetch = mockResponse(turn);
    const messages: AssistantMessage[] = [{ role: "user", content: "hi" }];
    const client = new EyvanApiClient("/api/v1", false);
    if (selection === undefined) await expect(client.assistantChat(messages)).resolves.toEqual(turn);
    else await expect(client.assistantChat(messages, selection)).resolves.toEqual(turn);
    const scope = configuredScope();
    expect(fetch.mock.calls[0][0]).toBe("/api/v1/assistant/chat");
    expect(fetch.mock.calls[0][1].method).toBe("POST");
    expect(JSON.parse(fetch.mock.calls[0][1].body as string)).toEqual({
      messages,
      org_id: scope.orgId,
      project_id: scope.projectId,
    });
  });

  it("sends the selected provider and model with an explicit chat scope", async () => {
    const fetch = mockResponse({ reply: "ok", steps: 1, tool_calls: [], drafts: [], requests: [] });
    const client = new EyvanApiClient("/api/v1", false);
    const messages: AssistantMessage[] = [{ role: "user", content: "hi" }];
    await client.assistantChat(
      messages,
      { provider_id: PROVIDER.id, model: "quick" },
      {
        orgId: "other",
        projectId: "research",
      },
    );
    expect(JSON.parse(fetch.mock.calls[0][1].body as string)).toEqual({
      messages,
      org_id: "other",
      project_id: "research",
      provider_id: PROVIDER.id,
      model: "quick",
    });
  });
});
