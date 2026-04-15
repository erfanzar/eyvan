/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_EYVAN_DEMO?: string;
  readonly VITE_EYVAN_DEV_AUTH?: string;
  readonly VITE_EYVAN_DEV_USER?: string;
  readonly VITE_EYVAN_DEV_ROLE?: string;
  readonly VITE_EYVAN_ORG?: string;
  readonly VITE_EYVAN_PROJECT?: string;
  readonly VITE_EYVAN_LOCAL_CREDENTIAL?: string;
  readonly VITE_EYVAN_OIDC_ISSUER?: string;
  readonly VITE_EYVAN_OIDC_CLIENT_ID?: string;
  readonly VITE_EYVAN_OIDC_REDIRECT_URI?: string;
  readonly VITE_EYVAN_OIDC_SCOPE?: string;
  readonly VITE_EYVAN_OIDC_AUDIENCE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
