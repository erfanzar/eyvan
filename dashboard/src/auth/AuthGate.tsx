// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Boxes, LoaderCircle, LogIn } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";

import {
  AUTH_CHANGED_EVENT,
  absorbLocalCredentialFromLocation,
  authRevision,
  beginLogin,
  consumeSessionNotice,
  currentIdentity,
  isDevelopmentAuthEnabled,
  isDemoMode,
  isOidcConfigured,
  loadRuntimeOidcConfig,
  localDevelopmentCredential,
  planeRequiresLocalCredential,
  serverIdentity,
} from "./oidc";

export function AuthGate({ children }: { children: ReactNode }) {
  const [checking, setChecking] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [identity, setIdentity] = useState(() => currentIdentity());
  // The identity object can be identical before and after an auth change -- a
  // refused loopback credential leaves no identity at all -- so React would
  // skip the re-render and keep this gate open behind a page whose every call
  // is now refused. The counter forces the decision below to run again.
  const [, setAuthRevision] = useState(0);

  useEffect(() => {
    let cancelled = false;
    // A failed sign-in round-trips through the identity provider and comes
    // back as an auth_error parameter the server's callback put on the
    // redirect. It is shown once, then stripped so a refresh cannot resurrect
    // it.
    const query = new URLSearchParams(globalThis.location?.search ?? "");
    const authError = query.get("auth_error");
    if (authError) {
      query.delete("auth_error");
      const remaining = query.toString();
      globalThis.history?.replaceState(
        null,
        "",
        `${globalThis.location.pathname}${remaining ? `?${remaining}` : ""}${globalThis.location.hash}`,
      );
      if (!cancelled) setError(authError);
    }
    // First, before anything reads the OIDC configuration or issues an API
    // call: a credential may have arrived in this URL's fragment, and it must
    // be in session storage -- with the address bar cleaned -- before the
    // first request could otherwise leave without it.
    absorbLocalCredentialFromLocation();
    // Before anything reads the OIDC configuration. Build-time values still
    // win; this only fills the gap they leave, which is what lets one image
    // serve two identity providers.
    //
    // Then ask the server whether a dashboard session already exists. The
    // cookie is unreadable from here by design, so the authority endpoint is
    // the only witness; a 401 simply means "show the sign-in screen".
    //
    // The probe can outlive the session it asked about: any other call that
    // comes back 401 rejects the session mid-flight, and an answer captured
    // under the revoked cookie must not re-open the gate. So the auth
    // revision is read before the probe starts, and a result from a world
    // that has since changed is dropped -- same shape as `cancelled`.
    const startRevision = authRevision();
    loadRuntimeOidcConfig()
      .then(() => serverIdentity())
      .then((server) => {
        if (!cancelled && authRevision() === startRevision) setIdentity((current) => current ?? server);
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
      setAuthRevision((revision) => revision + 1);
      setError(consumeSessionNotice() ?? null);
    };
    globalThis.addEventListener(AUTH_CHANGED_EVENT, onAuthChanged);
    return () => globalThis.removeEventListener(AUTH_CHANGED_EVENT, onAuthChanged);
  }, []);

  if (checking) {
    return <AuthState icon={<LoaderCircle className="spin" size={22} />} title="Completing secure sign-in" />;
  }
  // `planeRequiresLocalCredential` is the plane saying it runs loopback
  // development auth and demands its owner-only credential from every caller
  // -- the headers that identify this page as the plane's own are forgeable
  // by any local process, so they no longer authenticate anything. This
  // bundle is served *by* that plane, but being its page is not being its
  // owner: the browser cannot read the 0600 file the credential lives in.
  // `eyvan dashboard` bridges it through the URL fragment, so the page renders
  // the fleet only once that value actually arrived.
  //
  // Behind OIDC, though, and deliberately: a plane that names an identity
  // provider wants a real identity, and the plane's own authenticator agrees --
  // a presented token is resolved before the development path is reached.
  const ownPlane = planeRequiresLocalCredential() && !isOidcConfigured();
  const ownPlaneAuthenticated = ownPlane && localDevelopmentCredential() !== undefined;
  if (identity || isDemoMode() || isDevelopmentAuthEnabled() || ownPlaneAuthenticated) return children;

  // The plane's own page, opened without the credential -- a raw URL typed by
  // hand. This is reachable and ordinary, not an error state to hide: the fix
  // is one command in the owner's terminal, and naming it here is the
  // difference between "Unable to load Eyvan state" on every panel and a
  // working dashboard.
  if (ownPlane) {
    return (
      <AuthState
        icon={<AlertTriangle size={22} />}
        title="This plane requires its local credential"
        detail={
          "Every caller must present this machine's Eyvan development credential, and a browser cannot read the file it lives in. Run `eyvan dashboard` in your terminal: it opens this page with the credential attached, and nothing needs to be typed here."
        }
        isError
      />
    );
  }

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
              try {
                beginLogin();
              } catch (authError) {
                setError(authError instanceof Error ? authError.message : "Unable to begin sign-in");
              }
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
