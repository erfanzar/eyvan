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
} from "./oidc";

export function AuthGate({ children }: { children: ReactNode }) {
  const [checking, setChecking] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [identity, setIdentity] = useState(() => currentIdentity());

  useEffect(() => {
    let cancelled = false;
    completeLogin()
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
  return (
    <AuthState
      icon={error || !configured ? <AlertTriangle size={22} /> : <Boxes size={22} />}
      title={error ? "Sign-in failed" : configured ? "Sign in to Eyvan" : "Authentication is not configured"}
      detail={
        error ??
        (configured
          ? "Use your organization identity to access the fleet control plane."
          : "Configure VITE_EYVAN_OIDC_ISSUER and VITE_EYVAN_OIDC_CLIENT_ID, enable explicit development auth, or open ?demo=1.")
      }
      isError={Boolean(error) || !configured}
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
