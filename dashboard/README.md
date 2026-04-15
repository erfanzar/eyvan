# Eyvan dashboard

The dashboard uses the canonical `/api/v1` API. Sample data is disabled by
default and is available only with `?demo=1` or an explicit
`VITE_EYVAN_DEMO=true` build.

## Browser authentication

Production builds use an OIDC public client with authorization-code PKCE:

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

Set `VITE_EYVAN_ORG` and `VITE_EYVAN_PROJECT` to select the API scope;
they default to `acme` and `research` for local development.

Local identity headers are opt-in and should be paired with
`EYVAN_DEV_AUTH_ENABLED=1` on the API:

```bash
VITE_EYVAN_DEV_AUTH=true \
VITE_EYVAN_DEV_USER=dashboard-dev \
VITE_EYVAN_DEV_ROLE=viewer \
  npm run dev
```
