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
const ACCESS_TOKEN_KEY = "eyvan.oidc.access_token";
const ID_TOKEN_KEY = "eyvan.oidc.id_token";
const EXPIRES_AT_KEY = "eyvan.oidc.expires_at";
const STATE_KEY = "eyvan.oidc.state";
const VERIFIER_KEY = "eyvan.oidc.verifier";
const NONCE_KEY = "eyvan.oidc.nonce";
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

export interface AuthIdentity {
  displayName: string;
  detail: string;
  mode: "oidc" | "development" | "demo";
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

function issuer(): string | undefined {
  return import.meta.env.VITE_EYVAN_OIDC_ISSUER?.replace(/\/$/, "");
}

function clientId(): string {
  const configuredClientId = import.meta.env.VITE_EYVAN_OIDC_CLIENT_ID;
  if (!configuredClientId) throw new Error("VITE_EYVAN_OIDC_CLIENT_ID is not configured");
  return configuredClientId;
}

export function isOidcConfigured(): boolean {
  return Boolean(issuer() && import.meta.env.VITE_EYVAN_OIDC_CLIENT_ID);
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

export function apiAuthHeaders(): Record<string, string> {
  const token = getAccessToken();
  if (token) return { Authorization: `Bearer ${token}` };
  if (!isDevelopmentAuthEnabled()) return {};
  const headers: Record<string, string> = {
    "X-Eyvan-User": import.meta.env.VITE_EYVAN_DEV_USER ?? "dashboard-dev",
    "X-Eyvan-Org": import.meta.env.VITE_EYVAN_ORG ?? "acme",
    "X-Eyvan-Project": import.meta.env.VITE_EYVAN_PROJECT ?? "research",
    "X-Eyvan-Roles": import.meta.env.VITE_EYVAN_DEV_ROLE ?? "viewer",
  };
  // A default `eyvan-server` runs in LOOPBACK_DEFAULT auth mode, which requires
  // this owner-only credential in addition to the identity headers. Without it
  // every request 401s against a stock local server. The browser cannot read the
  // 0600 file itself, so the developer supplies it:
  //
  //   VITE_EYVAN_LOCAL_CREDENTIAL=$(cat ~/.eyvan/local-development-credential) npm run dev
  //
  // It is absent for OIDC or explicitly-dev-auth servers, which need no header.
  const localCredential = import.meta.env.VITE_EYVAN_LOCAL_CREDENTIAL;
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
  if (import.meta.env.VITE_EYVAN_OIDC_AUDIENCE) {
    parameters.set("audience", import.meta.env.VITE_EYVAN_OIDC_AUDIENCE);
  }
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
  if (isDemoMode()) return { displayName: "Demo dataset", detail: "Local sample data", mode: "demo" };
  if (isDevelopmentAuthEnabled()) {
    return {
      displayName: import.meta.env.VITE_EYVAN_DEV_USER ?? "dashboard-dev",
      detail: `Development ${import.meta.env.VITE_EYVAN_DEV_ROLE ?? "viewer"}`,
      mode: "development",
    };
  }
  if (!getAccessToken()) return undefined;
  const claims = decodeClaims(storage()?.getItem(ID_TOKEN_KEY) ?? null);
  const displayName = String(claims.name ?? claims.preferred_username ?? claims.email ?? claims.sub ?? "Signed-in user");
  const detail = String(claims.email ?? claims.preferred_username ?? "OIDC session");
  return { displayName, detail, mode: "oidc" };
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
