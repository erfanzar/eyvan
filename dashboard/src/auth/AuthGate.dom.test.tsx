// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

/**
 * The first screen of the dashboard, for a plane that has no OIDC.
 *
 * A laptop plane has none by design, and the screen used to greet it as
 * "Authentication is not configured", naming VITE_EYVAN_OIDC_ISSUER and
 * VITE_EYVAN_OIDC_CLIENT_ID -- build arguments of a bundle whoever is reading
 * the screen did not build, on a plane where OIDC was never the answer.
 */

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AuthGate } from "./AuthGate";
import { __resetRuntimeOidcConfigForTests } from "./oidc";

function planeAnswering(document: Record<string, unknown>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify(document), { status: 200 })),
  );
}

describe("the sign-in screen", () => {
  afterEach(() => {
    __resetRuntimeOidcConfigForTests();
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("renders the fleet on a plane that authenticates its own page", async () => {
    // The bundle is served by the plane, so its calls are same-origin and the
    // plane recognises them. A sign-in screen here would be asking somebody to
    // authenticate to their own laptop.
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    planeAnswering({ issuer: null, client_id: null, development_auth: "loopback_default" });

    render(
      <AuthGate>
        <p>fleet</p>
      </AuthGate>,
    );

    await waitFor(() => expect(screen.getByText("fleet")).toBeTruthy());
  });

  it("still calls a plane with no OIDC and no development auth misconfigured", async () => {
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    planeAnswering({ issuer: null, client_id: null, development_auth: "disabled" });

    render(
      <AuthGate>
        <p>fleet</p>
      </AuthGate>,
    );

    await waitFor(() => expect(screen.getByText("Authentication is not configured")).toBeTruthy());
    expect(screen.getByRole("alert")).toBeTruthy();
  });

  it("offers sign-in when the plane does have an identity provider", async () => {
    vi.stubEnv("VITE_EYVAN_OIDC_ISSUER", "");
    vi.stubEnv("VITE_EYVAN_OIDC_CLIENT_ID", "");
    planeAnswering({ issuer: "https://idp.example.com", client_id: "eyvan", development_auth: "loopback_default" });

    render(
      <AuthGate>
        <p>fleet</p>
      </AuthGate>,
    );

    await waitFor(() => expect(screen.getByText("Sign in to Eyvan")).toBeTruthy());
  });
});
