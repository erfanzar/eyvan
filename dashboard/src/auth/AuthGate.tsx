// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Boxes, LoaderCircle, LogIn } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";

import {
  AUTH_CHANGED_EVENT,
  beginLogin,
  completeLogin,
  consumeSessionNotice,
  currentIdentity,
  isDevelopmentAuthEnabled,
  isDemoMode,
  isOidcConfigured,
  loadRuntimeOidcConfig,
  planeRequiresLocalCredential,
} from "./oidc";

export function AuthGate({ children }: { children: ReactNode }) {
  const [checking, setChecking] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [identity, setIdentity] = useState(() => currentIdentity());

  useEffect(() => {
    let cancelled = false;
    // Before anything reads the OIDC configuration. Build-time values still
    // win; this only fills the gap they leave, which is what lets one image
    // serve two identity providers.
    loadRuntimeOidcConfig()
      .then(() => completeLogin())
      .then(() => {
        if (!cancelled) setIdentity(currentIdentity());
      })
      .catch((authError) => {
        if (!cancelled) setError(authError instanceof Error ? authError.message : "Unable to complete sign-in");
      })
      .finally(() => {
        if (!cancelled) setChecking(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const onAuthChanged = () => {
      setIdentity(currentIdentity());
      setError(consumeSessionNotice() ?? null);
    };
    globalThis.addEventListener(AUTH_CHANGED_EVENT, onAuthChanged);
    return () => globalThis.removeEventListener(AUTH_CHANGED_EVENT, onAuthChanged);
  }, []);

  if (checking) {
    return <AuthState icon={<LoaderCircle className="spin" size={22} />} title="Completing secure sign-in" />;
  }
  if (identity || isDemoMode() || isDevelopmentAuthEnabled()) return children;

  const configured = isOidcConfigured();
  // A local plane is not a misconfigured one. It has no OIDC by design, and it
  // holds a credential no browser can read -- so the honest screen names the
  // command that supplies it rather than two build variables belonging to a
  // bundle whoever is reading this never built.
  const local = !configured && planeRequiresLocalCredential();
  return (
    <AuthState
      icon={error || (!configured && !local) ? <AlertTriangle size={22} /> : <Boxes size={22} />}
      title={
        error
          ? "Sign-in failed"
          : configured
            ? "Sign in to Eyvan"
            : local
              ? "This is a local development plane"
              : "Authentication is not configured"
      }
      detail={
        error ??
        (configured
          ? "Use your organization identity to access the fleet control plane."
          : local
            ? "It holds an owner-only credential this page cannot read. Run `eyvan dashboard-dev` to open the same dashboard with that credential supplied, or append ?demo=1 to look around sample data."
            : "Configure VITE_EYVAN_OIDC_ISSUER and VITE_EYVAN_OIDC_CLIENT_ID, enable explicit development auth, or open ?demo=1.")
      }
      isError={Boolean(error) || (!configured && !local)}
      action={
        configured ? (
          <button
            className="button button-primary"
            type="button"
            onClick={() => {
              setError(null);
              void beginLogin().catch((authError) =>
                setError(authError instanceof Error ? authError.message : "Unable to begin sign-in"),
              );
            }}
          >
            <LogIn size={17} /> Sign in with OIDC
          </button>
        ) : undefined
      }
    />
  );
}

function AuthState({
  icon,
  title,
  detail,
  isError = false,
  action,
}: {
  icon: ReactNode;
  title: string;
  detail?: string;
  isError?: boolean;
  action?: ReactNode;
}) {
  return (
    <main className="auth-screen">
      <section className="auth-panel" role={isError ? "alert" : "status"}>
        <span className="brand-mark">{icon}</span>
        <h1>{title}</h1>
        {detail && <p>{detail}</p>}
        {action}
      </section>
    </main>
  );
}
