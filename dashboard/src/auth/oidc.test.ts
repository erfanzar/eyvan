// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  __resetRuntimeOidcConfigForTests,
  apiAuthHeaders,
  completeLogin,
  consumeSessionNotice,
  currentIdentity,
  getAccessToken,
  isOidcConfigured,
  loadRuntimeOidcConfig,
} from "./oidc";

function memoryStorage(initial: Record<string, string> = {}): Storage {
  const values = new Map(Object.entries(initial));
  return {
    get length() { return values.size; },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => { values.delete(key); },
    setItem: (key, value) => { values.set(key, value); },
  };
}

describe("OIDC browser authentication", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  });

  it("exchanges an authorization code with its PKCE verifier and keeps the bearer token in session storage", async () => {
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "https://identity.example.test");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "eyvan-dashboard");
    vi.stubGlobal("sessionStorage", memoryStorage({
      "eyvan.oidc.state": "expected-state",
      "eyvan.oidc.verifier": "pkce-verifier",
    }));
    vi.stubGlobal("location", {
      search: "?code=authorization-code&state=expected-state",
      origin: "https://eyvan.example.test",
      pathname: "/",
      hash: "#overview",
    });
    const replaceState = vi.fn();
    vi.stubGlobal("history", { replaceState });
    vi.stubGlobal("document", { title: "Eyvan" });
    const fetch = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        authorization_endpoint: "https://identity.example.test/authorize",
        token_endpoint: "https://identity.example.test/token",
      }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        access_token: "session-access-token",
        token_type: "Bearer",
        expires_in: 3600,
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetch);

    await expect(completeLogin()).resolves.toBe(true);

    expect(fetch.mock.calls[1][0]).toBe("https://identity.example.test/token");
    expect(String(fetch.mock.calls[1][1].body)).toContain("code_verifier=pkce-verifier");
    expect(apiAuthHeaders()).toEqual({ Authorization: "Bearer session-access-token" });
    expect(currentIdentity()).toMatchObject({ mode: "oidc" });
    expect(replaceState).toHaveBeenCalledWith({}, "Eyvan", "/#overview");
  });

  it("rejects a callback whose state does not match the initiating browser session", async () => {
    vi.stubGlobal("sessionStorage", memoryStorage({
      "eyvan.oidc.state": "expected-state",
      "eyvan.oidc.verifier": "pkce-verifier",
    }));

    await expect(completeLogin(new URLSearchParams("code=authorization-code&state=other-state"))).rejects.toThrow(
      "OIDC callback state could not be verified",
    );
  });

  it("returns the stored token while it is still valid", () => {
    const session = memoryStorage({
      "eyvan.oidc.access_token": "valid-token",
      "eyvan.oidc.expires_at": String(Date.now() + 60_000),
    });
    vi.stubGlobal("sessionStorage", session);

    expect(getAccessToken()).toBe("valid-token");
    expect(consumeSessionNotice()).toBeUndefined();
    expect(session.getItem("eyvan.oidc.access_token")).toBe("valid-token");
  });

  it("expires stored sessions with an explicit re-login notice instead of silently clearing", () => {
    const dispatch = vi.fn();
    const session = memoryStorage({
      "eyvan.oidc.access_token": "expired-token",
      "eyvan.oidc.expires_at": String(Date.now() - 1000),
    });
    vi.stubGlobal("sessionStorage", session);
    vi.stubGlobal("dispatchEvent", dispatch);

    expect(getAccessToken()).toBeUndefined();
    expect(session.getItem("eyvan.oidc.access_token")).toBeNull();
    expect(dispatch).toHaveBeenCalledWith(expect.objectContaining({ type: "eyvan:auth-changed" }));
    expect(consumeSessionNotice()).toMatch(/expired/i);
    expect(consumeSessionNotice()).toBeUndefined();
  });
});

describe("runtime OIDC configuration", () => {
  afterEach(() => {
    __resetRuntimeOidcConfigForTests();
    vi.unstubAllEnvs();
  });

  it("asks the control plane when the bundle carries no issuer", async () => {
    // The gap this closes: the issuer and client id were compiled in by VITE_
    // build arguments, so one image served exactly one identity provider.
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ issuer: "https://idp.example.com", client_id: "eyvan-dashboard" }), {
        status: 200,
      }),
    );

    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);

    expect(fetchImpl).toHaveBeenCalledWith("/api/v1/dashboard/config");
    expect(isOidcConfigured()).toBe(true);
  });

  it("does not ask when the bundle already carries both values", async () => {
    // A round trip whose answer is discarded, on the critical path of every
    // page load. Existing images must behave exactly as they did.
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "https://baked.example.com");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "baked-client");
    const fetchImpl = vi.fn();

    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);

    expect(fetchImpl).not.toHaveBeenCalled();
    expect(isOidcConfigured()).toBe(true);
  });

  it("treats an unreachable control plane as unconfigured rather than as an auth error", async () => {
    // An unreachable control plane is not an auth misconfiguration. The failure
    // belongs at the first API call, not as "OIDC is not configured".
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    const fetchImpl = vi.fn(async () => {
      throw new Error("network down");
    });

    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);

    expect(isOidcConfigured()).toBe(false);
  });

  it("ignores a control plane that answers without an issuer", async () => {
    // A plane running with no OIDC answers with nulls rather than a 404, so the
    // dashboard can tell "not configured" from "too old to answer".
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    const fetchImpl = vi.fn(async () =>
      new Response(JSON.stringify({ issuer: null, client_id: null }), { status: 200 }),
    );

    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);

    expect(isOidcConfigured()).toBe(false);
  });
});
