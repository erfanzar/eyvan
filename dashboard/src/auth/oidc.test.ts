// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  __resetRuntimeOidcConfigForTests,
  apiAuthHeaders,
  beginLogin,
  capabilitiesForPermissions,
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

function unsignedToken(claims: Record<string, unknown>): string {
  const encode = (value: unknown) => btoa(JSON.stringify(value)).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
  return `${encode({ alg: "none" })}.${encode(claims)}.`;
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

describe("dashboard permission capabilities", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  });

  it.each([
    [
      ["read", "submit", "operate", "manage_project", "manage_own_capacity", "manage_fleet"],
      { submitWorkloads: true, operateWorkloads: true, manageProject: true, manageFleet: true },
    ],
    [
      ["read", "submit", "operate", "manage_project", "manage_own_capacity"],
      { submitWorkloads: true, operateWorkloads: true, manageProject: true, manageFleet: false },
    ],
    [
      ["read", "submit", "operate", "manage_project"],
      { submitWorkloads: true, operateWorkloads: true, manageProject: true, manageFleet: false },
    ],
    [
      ["read", "submit", "operate"],
      { submitWorkloads: true, operateWorkloads: true, manageProject: false, manageFleet: false },
    ],
    [
      ["submit"],
      { submitWorkloads: true, operateWorkloads: false, manageProject: false, manageFleet: false },
    ],
    [
      ["read"],
      { submitWorkloads: false, operateWorkloads: false, manageProject: false, manageFleet: false },
    ],
  ] as const)("maps server permissions to presentation controls", (permissions, expected) => {
    expect(capabilitiesForPermissions(permissions)).toEqual(expected);
  });

  it("never treats access-token grants as effective browser authority", () => {
    vi.stubEnv("VITE_EYVAN_ORG", "acme");
    vi.stubEnv("VITE_EYVAN_PROJECT", "research");
    vi.stubGlobal("sessionStorage", memoryStorage({
      "eyvan.oidc.access_token": unsignedToken({
        sub: "alice",
        eyvan_grants: [{ role: "fleet_admin" }],
      }),
    }));

    expect(currentIdentity()).toMatchObject({
      mode: "oidc",
      roles: [],
      capabilities: {
        submitWorkloads: false,
        operateWorkloads: false,
        manageProject: false,
        manageFleet: false,
      },
    });
  });

  it("does not use a development admin role when requests use a bearer session", () => {
    vi.stubEnv("VITE_EYVAN_DEV_AUTH", "true");
    vi.stubEnv("VITE_EYVAN_DEV_ROLE", "fleet_admin");
    vi.stubGlobal("sessionStorage", memoryStorage({
      "eyvan.oidc.access_token": unsignedToken({ sub: "alice" }),
    }));

    expect(currentIdentity()).toMatchObject({
      mode: "oidc",
      roles: [],
      capabilities: {
        submitWorkloads: false,
        operateWorkloads: false,
        manageProject: false,
        manageFleet: false,
      },
    });
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

  it("does not ask when the bundle deliberately carries every runtime value", async () => {
    // A round trip whose answer is discarded, on the critical path of every
    // page load. Existing images must behave exactly as they did.
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "https://baked.example.com");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "baked-client");
    vi.stubEnv("VITE_EYVAN_OIDC_AUDIENCE", "baked-audience");
    const fetchImpl = vi.fn();

    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);

    expect(fetchImpl).not.toHaveBeenCalled();
    expect(isOidcConfigured()).toBe(true);
  });

  it("requests the runtime API audience during authorization", async () => {
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    vi.stubEnv("VITE_EYVAN_OIDC_AUDIENCE", "");
    vi.stubGlobal("sessionStorage", memoryStorage());
    const assign = vi.fn();
    vi.stubGlobal("location", {
      search: "",
      origin: "https://eyvan.example.test",
      pathname: "/",
      hash: "",
      assign,
    });
    vi.stubGlobal("crypto", {
      getRandomValues: (bytes: Uint8Array) => bytes.fill(7),
      subtle: { digest: async () => new Uint8Array(32).buffer },
    });
    const fetchImpl = vi.fn(async () =>
      new Response(
        JSON.stringify({
          issuer: "https://idp.example.test",
          client_id: "eyvan-dashboard",
          audience: "eyvan-api",
        }),
        { status: 200 },
      ),
    );
    await loadRuntimeOidcConfig(fetchImpl as unknown as typeof fetch);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            authorization_endpoint: "https://idp.example.test/authorize",
            token_endpoint: "https://idp.example.test/token",
          }),
          { status: 200 },
        ),
      ),
    );

    await beginLogin();

    expect(assign).toHaveBeenCalledOnce();
    expect(String(assign.mock.calls[0][0])).toContain("audience=eyvan-api");
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
