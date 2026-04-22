/*
 * Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiAuthHeaders, completeLogin, consumeSessionNotice, currentIdentity, getAccessToken } from "./oidc";

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
