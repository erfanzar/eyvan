# Eyvan dashboard

The dashboard uses the canonical `/api/v1` API. Sample data is disabled by
default and is available only with `?demo=1` or an explicit
`VITE_EYVAN_DEMO=true` development build.

## Distribution contract

The Python wheel intentionally contains neither these Node sources nor built
static assets. The production `Dockerfile` builds them into the control-plane
image and sets `EYVAN_DASHBOARD_DIR`; `eyvan dashboard` from any installed wheel
opens that hosted UI. `eyvan dashboard-dev` is the explicitly source-only Vite
launcher and fails with a source-checkout message when those files are absent.

## Browser authentication

Production uses an OIDC public client with authorization-code PKCE. The hosted
bundle reads issuer and client ID from `/api/v1/dashboard/config`, populated by
`EYVAN_OIDC_ISSUER` and `EYVAN_OIDC_DASHBOARD_CLIENT_ID` on the control plane.
This keeps one signed image digest promotable between environments. Optional
`VITE_EYVAN_*` build settings still override runtime discovery for an intentional
custom build:

```bash
VITE_EYVAN_OIDC_ISSUER=https://identity.example.com \
VITE_EYVAN_OIDC_CLIENT_ID=eyvan-dashboard \
VITE_EYVAN_OIDC_AUDIENCE=eyvan-api \
VITE_EYVAN_OIDC_REDIRECT_URI=https://eyvan.example.com/ \
  npm run build
```

`VITE_EYVAN_OIDC_SCOPE` defaults to `openid profile email`. The identity
provider must register the redirect URI, allow authorization-code PKCE for a
public browser client, and permit browser access to discovery and token
endpoints. Access and ID tokens are held in `sessionStorage`; no client secret
or static bearer token is compiled into the dashboard.

Set `VITE_EYVAN_ORG` and `VITE_EYVAN_PROJECT` to select the API scope; they
default to `acme` and `research` for local development.

Local identity headers are opt-in and should be paired with
`EYVAN_DEV_AUTH_ENABLED=1` on the API. From the repository root:

```bash
npm --prefix dashboard install
uv run eyvan dashboard-dev
```
