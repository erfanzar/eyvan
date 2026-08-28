// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import type { SessionPermission } from "../types";

const ACCESS_TOKEN_KEY = "eyvan.oidc.access_token";
const ID_TOKEN_KEY = "eyvan.oidc.id_token";
const EXPIRES_AT_KEY = "eyvan.oidc.expires_at";
const STATE_KEY = "eyvan.oidc.state";
const VERIFIER_KEY = "eyvan.oidc.verifier";
const NONCE_KEY = "eyvan.oidc.nonce";
/**
 * The loopback development credential this browser was handed, if it was.
 *
 * The plane demands its owner-only credential from every caller once it has
 * issued one, this page included: the headers that identify the plane's own
 * dashboard are forgeable by any local process, so they cannot authenticate
 * anything. The browser cannot read the 0600 file the value lives in -- the
 * owner's terminal can, which is why `eyvan dashboard` opens this page with
 * the value in the URL fragment and {@link absorbLocalCredentialFromLocation}
 * moves it here before anything else runs.
 */
export const LOCAL_CREDENTIAL_KEY = "eyvan.local.credential";
export const LOCAL_CREDENTIAL_FRAGMENT_PARAM = "eyvan-local-credential";
export const AUTH_CHANGED_EVENT = "eyvan:auth-changed";

interface OidcMetadata {
  authorization_endpoint: string;
  token_endpoint: string;
  end_session_endpoint?: string;
}

interface TokenResponse {
  access_token: string;
  id_token?: string;
  expires_in?: number;
  token_type: string;
}

export interface AuthCapabilities {
  submitWorkloads: boolean;
  operateWorkloads: boolean;
  manageProject: boolean;
  manageFleet: boolean;
}

export interface AuthScope {
  orgId: string;
  projectId: string;
}

export interface AuthIdentity {
  displayName: string;
  detail: string;
  mode: "oidc" | "development" | "demo";
  /** Server-resolved roles applicable to the selected dashboard scope. */
  roles: string[];
  scope: AuthScope;
  /** Presentation derived only from the authenticated server authority response. */
  capabilities: AuthCapabilities;
}

export const NO_AUTH_CAPABILITIES: AuthCapabilities = Object.freeze({
  submitWorkloads: false,
  operateWorkloads: false,
  manageProject: false,
  manageFleet: false,
});

/** Derive presentation controls from permissions evaluated by the server Authorizer. */
export function capabilitiesForPermissions(
  permissionNames: Iterable<SessionPermission>,
  acceptsNewWork = true,
): AuthCapabilities {
  const permissions = new Set(permissionNames);
  return {
    // A grant can outlive tenant registration. Server-reported permissions say
    // what the principal may do; acceptsNewWork says whether this registered
    // scope currently accepts any tenant mutation. Reads remain independent so
    // disabled projects keep their durable history visible.
    submitWorkloads: acceptsNewWork && permissions.has("submit"),
    operateWorkloads: acceptsNewWork && permissions.has("operate"),
    manageProject: acceptsNewWork && permissions.has("manage_project"),
    manageFleet: permissions.has("manage_fleet"),
  };
}

/**
 * The org and project every scoped request is made in.
 *
 * The build-time values win, then what the control plane reports, and only then
 * a literal. That order matters: the literal used to be `acme`/`research` while
 * the CLI defaults to `default`/`default`, so a job submitted with `eyvan run`
 * never appeared on this page. Nothing errored, because both are free-form
 * scopes a plane answers for -- the dashboard was simply asking about a
 * different project than the one the operator was using.
 *
 * The remaining literal is reached only by a build with no configured scope
 * talking to a plane too old to report one, and it now matches the CLI.
 */
export function configuredScope(): AuthScope {
  return {
    orgId: import.meta.env.VITE_EYVAN_ORG || runtimeConfig?.default_org || "default",
    projectId: import.meta.env.VITE_EYVAN_PROJECT || runtimeConfig?.default_project || "default",
  };
}

export function isDemoMode(): boolean {
  return (
    import.meta.env.VITE_EYVAN_DEMO === "true" ||
    new URLSearchParams(globalThis.location?.search ?? "").get("demo") === "1"
  );
}

export function isDevelopmentAuthEnabled(): boolean {
  return import.meta.env.VITE_EYVAN_DEV_AUTH === "true";
}

/**
 * Whether the control plane itself accepts development identity headers.
 *
 * Distinct from {@link isDevelopmentAuthEnabled}, which asks whether *this
 * bundle* was built to send them. A plane can accept them while the bundle
 * serving its dashboard was never built to send any -- which is exactly the
 * bundle a laptop plane serves -- and the difference is what turns the sign-in
 * screen from a recital of build variables into the one command that works.
 */
export function planeDevelopmentAuthMode(): string | undefined {
  return runtimeConfig?.development_auth ?? undefined;
}

/** Whether the plane requires a credential the browser is unable to read. */
export function planeRequiresLocalCredential(): boolean {
  return planeDevelopmentAuthMode() === "loopback_default";
}

function storage(): Storage | undefined {
  try {
    return globalThis.sessionStorage;
  } catch {
    return undefined;
  }
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function randomValue(): string {
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  return base64Url(bytes);
}

function redirectUri(): string {
  return import.meta.env.VITE_EYVAN_OIDC_REDIRECT_URI ?? `${globalThis.location.origin}${globalThis.location.pathname}`;
}

/**
 * Identity configuration served by the control plane at runtime.
 *
 * The build arguments below compile an issuer and client id into the bundle, so
 * one image could serve exactly one identity provider -- and an operator with a
 * staging and a production realm had to build two images that differed in
 * nothing else.
 *
 * Build-time values still win when present, so every existing image behaves
 * identically. This only fills the gap they leave.
 */
let runtimeConfig:
  | {
      issuer?: string | null;
      client_id?: string | null;
      audience?: string | null;
      development_auth?: string | null;
      default_org?: string | null;
      default_project?: string | null;
    }
  | undefined;

export async function loadRuntimeOidcConfig(fetchImpl: typeof fetch = fetch): Promise<void> {
  if (runtimeConfig !== undefined) return;
  if (bakedIssuer() && bakedClientId() && bakedAudience()) {
    // Every runtime value is deliberately overridden by this build, and a
    // bundle that carries its own OIDC never reaches the local-plane screen
    // that is the plane's posture's only reader. So the round trip stays
    // skipped: it is on the critical path of every page load.
    runtimeConfig = {};
    return;
  }
  try {
    const response = await fetchImpl("/api/v1/dashboard/config");
    runtimeConfig = response.ok ? await response.json() : {};
  } catch {
    // An unreachable control plane is not an auth misconfiguration. Falling back
    // to the build-time values keeps the failure where it belongs -- the first
    // API call -- rather than turning it into "OIDC is not configured".
    runtimeConfig = {};
  }
}

export function __resetRuntimeOidcConfigForTests(): void {
  runtimeConfig = undefined;
}

/**
 * A declared-but-unset build argument becomes the empty string, not undefined.
 *
 * That is the shape the shipped Dockerfile produces: `ARG VITE_EYVAN_OIDC_ISSUER`
 * with no value. Using `??` here would treat `""` as configured, keep it, and
 * defeat the runtime fallback in exactly the deployment it exists for -- which
 * is what a test caught before this shipped.
 */
function bakedIssuer(): string | undefined {
  return import.meta.env.VITE_EYVAN_OIDC_ISSUER || undefined;
}

function bakedClientId(): string | undefined {
  return import.meta.env.VITE_EYVAN_OIDC_CLIENT_ID || undefined;
}

function bakedAudience(): string | undefined {
  return import.meta.env.VITE_EYVAN_OIDC_AUDIENCE || undefined;
}

function audience(): string | undefined {
  return bakedAudience() ?? runtimeConfig?.audience ?? undefined;
}

function issuer(): string | undefined {
  return (bakedIssuer() ?? runtimeConfig?.issuer ?? undefined)?.replace(/\/$/, "");
}

function clientId(): string {
  const configuredClientId = bakedClientId() ?? runtimeConfig?.client_id;
  if (!configuredClientId) throw new Error("VITE_EYVAN_OIDC_CLIENT_ID is not configured");
  return configuredClientId;
}

export function isOidcConfigured(): boolean {
  return Boolean(issuer() && (bakedClientId() ?? runtimeConfig?.client_id));
}

async function metadata(): Promise<OidcMetadata> {
  const configuredIssuer = issuer();
  if (!configuredIssuer) throw new Error("VITE_EYVAN_OIDC_ISSUER is not configured");
  const response = await fetch(`${configuredIssuer}/.well-known/openid-configuration`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`OIDC discovery failed with ${response.status}`);
  const document = (await response.json()) as Partial<OidcMetadata>;
  if (!document.authorization_endpoint || !document.token_endpoint) {
    throw new Error("OIDC discovery response is missing authorization or token endpoints");
  }
  return document as OidcMetadata;
}

let sessionNotice: string | undefined;

function endSession(notice: string): void {
  sessionNotice = notice;
  clearTokens();
  if ("dispatchEvent" in globalThis) globalThis.dispatchEvent(new Event(AUTH_CHANGED_EVENT));
}

export function consumeSessionNotice(): string | undefined {
  const notice = sessionNotice;
  sessionNotice = undefined;
  return notice;
}

export function rejectSession(): void {
  endSession("The control plane rejected your session. Sign in again to continue.");
}

export function getAccessToken(): string | undefined {
  const session = storage();
  const token = session?.getItem(ACCESS_TOKEN_KEY) ?? undefined;
  const expiresAt = Number(session?.getItem(EXPIRES_AT_KEY) ?? 0);
  if (token && expiresAt && Date.now() >= expiresAt) {
    endSession("Your sign-in session expired. Sign in again to continue.");
    return undefined;
  }
  return token;
}

/** The credential this browser holds for its loopback plane, if any. */
export function localDevelopmentCredential(): string | undefined {
  const bridged = storage()?.getItem(LOCAL_CREDENTIAL_KEY);
  if (bridged) return bridged;
  // A declared-but-unset build argument becomes the empty string, not undefined.
  return import.meta.env.VITE_EYVAN_LOCAL_CREDENTIAL || undefined;
}

/**
 * Move a bridged development credential out of the URL and into this tab.
 *
 * `eyvan dashboard` puts the value in the fragment -- the one part of a URL
 * no server, proxy, or `Referer` header ever sees -- because that is the only
 * way the owner's terminal can hand it to a browser that cannot read the
 * file it lives in. A fragment still lives in history and in URLs people
 * copy, so it is moved to session storage and stripped from the address bar
 * immediately; the tab keeps it for its lifetime and no longer than that.
 *
 * Returns the absorbed value so the caller can act on the transition.
 */
export function absorbLocalCredentialFromLocation(
  location: Pick<Location, "hash" | "pathname" | "search"> = globalThis.location,
  historyLike: Pick<History, "replaceState"> = globalThis.history,
): string | undefined {
  const hash = location?.hash ?? "";
  if (!hash.startsWith("#")) return undefined;
  const params = new URLSearchParams(hash.slice(1));
  const value = params.get(LOCAL_CREDENTIAL_FRAGMENT_PARAM);
  if (!value) return undefined;
  storage()?.setItem(LOCAL_CREDENTIAL_KEY, value);
  params.delete(LOCAL_CREDENTIAL_FRAGMENT_PARAM);
  const remaining = params.toString();
  historyLike?.replaceState(null, "", `${location.pathname}${location.search}${remaining ? `#${remaining}` : ""}`);
  return value;
}

export function apiAuthHeaders(): Record<string, string> {
  const token = getAccessToken();
  if (token) return { Authorization: `Bearer ${token}` };
  const localCredential = localDevelopmentCredential();
  if (!isDevelopmentAuthEnabled() && !localCredential) return {};
  const headers: Record<string, string> = {};
  if (isDevelopmentAuthEnabled()) {
    headers["X-Eyvan-User"] = import.meta.env.VITE_EYVAN_DEV_USER ?? "dashboard-dev";
    headers["X-Eyvan-Org"] = import.meta.env.VITE_EYVAN_ORG ?? "acme";
    headers["X-Eyvan-Project"] = import.meta.env.VITE_EYVAN_PROJECT ?? "research";
    headers["X-Eyvan-Roles"] = import.meta.env.VITE_EYVAN_DEV_ROLE ?? "viewer";
  }
  // The credential travels without the identity headers above: those are for
  // a bundle *built* to claim a development identity, while a plane serving
  // its own bundle fills that page's identity itself -- this machine's owner,
  // fleet_admin over the default scope, exactly what the CLI elects on
  // loopback. What the page cannot get from anywhere but its owner is the
  // credential, so that is the one thing this header set insists on.
  //
  // For the source-checkout dev server the value still arrives build-time:
  //   VITE_EYVAN_LOCAL_CREDENTIAL=$(cat ~/.eyvan/local-development-credential) npm run dev
  if (localCredential) headers["X-Eyvan-Local-Credential"] = localCredential;
  return headers;
}

export async function beginLogin(): Promise<void> {
  if (!isOidcConfigured()) throw new Error("OIDC client configuration is incomplete");
  const session = storage();
  if (!session) throw new Error("Session storage is unavailable");
  const verifier = randomValue();
  const state = randomValue();
  const nonce = randomValue();
  const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  session.setItem(VERIFIER_KEY, verifier);
  session.setItem(STATE_KEY, state);
  session.setItem(NONCE_KEY, nonce);

  const oidcMetadata = await metadata();
  const parameters = new URLSearchParams({
    response_type: "code",
    client_id: clientId(),
    redirect_uri: redirectUri(),
    scope: import.meta.env.VITE_EYVAN_OIDC_SCOPE ?? "openid profile email",
    state,
    nonce,
    code_challenge: base64Url(new Uint8Array(digest)),
    code_challenge_method: "S256",
  });
  const configuredAudience = audience();
  if (configuredAudience) parameters.set("audience", configuredAudience);
  globalThis.location.assign(`${oidcMetadata.authorization_endpoint}?${parameters.toString()}`);
}

export async function completeLogin(parameters = new URLSearchParams(globalThis.location.search)): Promise<boolean> {
  const code = parameters.get("code");
  const returnedState = parameters.get("state");
  const oidcError = parameters.get("error");
  if (oidcError) {
    throw new Error(parameters.get("error_description") ?? `Identity provider returned ${oidcError}`);
  }
  if (!code) return false;

  const session = storage();
  const verifier = session?.getItem(VERIFIER_KEY);
  const expectedState = session?.getItem(STATE_KEY);
  const expectedNonce = session?.getItem(NONCE_KEY);
  if (!session || !verifier || !returnedState || returnedState !== expectedState) {
    throw new Error("OIDC callback state could not be verified");
  }

  const oidcMetadata = await metadata();
  const response = await fetch(oidcMetadata.token_endpoint, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      client_id: clientId(),
      code,
      redirect_uri: redirectUri(),
      code_verifier: verifier,
    }),
  });
  if (!response.ok) throw new Error(`OIDC token exchange failed with ${response.status}`);
  const token = (await response.json()) as Partial<TokenResponse>;
  if (!token.access_token || token.token_type?.toLowerCase() !== "bearer") {
    throw new Error("OIDC token response did not contain a bearer access token");
  }
  if (token.id_token && expectedNonce && decodeClaims(token.id_token).nonce !== expectedNonce) {
    throw new Error("OIDC ID token nonce could not be verified");
  }
  session.setItem(ACCESS_TOKEN_KEY, token.access_token);
  if (token.id_token) session.setItem(ID_TOKEN_KEY, token.id_token);
  if (token.expires_in) session.setItem(EXPIRES_AT_KEY, String(Date.now() + token.expires_in * 1000 - 30_000));
  session.removeItem(STATE_KEY);
  session.removeItem(VERIFIER_KEY);
  session.removeItem(NONCE_KEY);

  const cleanUrl = `${globalThis.location.pathname}${globalThis.location.hash}`;
  globalThis.history.replaceState({}, globalThis.document.title, cleanUrl);
  return true;
}

function decodeClaims(token: string | null): Record<string, unknown> {
  if (!token) return {};
  try {
    const payload = token.split(".")[1]?.replaceAll("-", "+").replaceAll("_", "/");
    if (!payload) return {};
    return JSON.parse(atob(payload)) as Record<string, unknown>;
  } catch {
    return {};
  }
}

export function currentIdentity(): AuthIdentity | undefined {
  const scope = configuredScope();
  if (isDemoMode()) {
    const permissions: SessionPermission[] = [
      "read",
      "submit",
      "operate",
      "manage_project",
      "manage_own_capacity",
      "manage_fleet",
    ];
    return {
      displayName: "Demo dataset",
      detail: "Local sample data",
      mode: "demo",
      roles: ["fleet_admin"],
      scope,
      capabilities: capabilitiesForPermissions(permissions),
    };
  }

  // Claims are decoded only for display. Role grants and action authority come
  // exclusively from GET /api/v1/session after server authentication and
  // durable-grant unioning.
  const accessToken = getAccessToken();
  if (accessToken) {
    const accessClaims = decodeClaims(accessToken);
    const idClaims = decodeClaims(storage()?.getItem(ID_TOKEN_KEY) ?? null);
    const displayName = String(
      idClaims.name ??
      idClaims.preferred_username ??
      idClaims.email ??
      idClaims.sub ??
      accessClaims.name ??
      accessClaims.preferred_username ??
      accessClaims.email ??
      accessClaims.sub ??
      "Signed-in user",
    );
    const detail = String(
      idClaims.email ??
      idClaims.preferred_username ??
      accessClaims.email ??
      accessClaims.preferred_username ??
      "OIDC session",
    );
    return {
      displayName,
      detail,
      mode: "oidc",
      roles: [],
      scope,
      capabilities: NO_AUTH_CAPABILITIES,
    };
  }

  if (isDevelopmentAuthEnabled()) {
    return {
      displayName: import.meta.env.VITE_EYVAN_DEV_USER ?? "dashboard-dev",
      detail: "Development identity; authority pending server verification",
      mode: "development",
      roles: [],
      scope,
      capabilities: NO_AUTH_CAPABILITIES,
    };
  }
  return undefined;
}

export function clearTokens(): void {
  const session = storage();
  [ACCESS_TOKEN_KEY, ID_TOKEN_KEY, EXPIRES_AT_KEY, STATE_KEY, VERIFIER_KEY, NONCE_KEY].forEach((key) => session?.removeItem(key));
}

export async function logout(): Promise<void> {
  const idToken = storage()?.getItem(ID_TOKEN_KEY) ?? undefined;
  clearTokens();
  if ("dispatchEvent" in globalThis) globalThis.dispatchEvent(new Event(AUTH_CHANGED_EVENT));
  if (isDemoMode()) {
    const url = new URL(globalThis.location.href);
    url.searchParams.delete("demo");
    globalThis.location.assign(`${url.pathname}${url.search}${url.hash}`);
    return;
  }
  if (!isOidcConfigured()) return;
  try {
    const oidcMetadata = await metadata();
    if (oidcMetadata.end_session_endpoint) {
      const parameters = new URLSearchParams({ post_logout_redirect_uri: redirectUri() });
      if (idToken) parameters.set("id_token_hint", idToken);
      globalThis.location.assign(`${oidcMetadata.end_session_endpoint}?${parameters.toString()}`);
      return;
    }
  } catch {
    // Local logout is complete even when provider discovery is unavailable.
  }
  globalThis.location.assign(redirectUri());
}
