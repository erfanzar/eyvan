# Part 12 — Security

Full model: `docs/security.md`. This part is the working summary.

## The authority boundary

- The **plane** is the authority for placement, leasing, and reap.
- **Agents** own machines and execute what the plane leases to them.
- **Your callable** runs on worker hosts under the native runtime. It is workload
  code, not trusted code. The posture: signed, digest-verified inputs; least
  privilege on the worker; no plane credentials in the workload environment.

## The secret table

| secret | who reads it | what it protects |
|---|---|---|
| `EYVAN_AGENT_SHARED_SECRET` | plane, agent | the plane↔agent API |
| `EYVAN_NATIVE_WORKER_ROOT_SECRET` | agent | native worker bring-up; derives per-cell credentials — reusing the agent secret collapses capability boundaries |
| `EYVAN_ARTIFACT_TOKEN_KEY` | plane | HMAC signing of artifact/secret capability tokens; ≥32 bytes, identical on every replica |
| `EYVAN_METRICS_TOKEN` | metrics clients | `/metrics` scrape auth (required on network binds) |
| `EYVAN_BOOTSTRAP_TOKEN` | plane bootstrap | first-touch provisioning of a head |

Secrets enter via environment or the runtime profile
(`~/.eyvan/gcp-native-runtime.json`, written `0600`) — **never** via fleet configs
or the repository. `eyvan head up` generates its secrets from the system CSPRNG and
delivers them on stdin (never in a command argument where `ps` would show them),
writing `/etc/eyvan/head.env` mode 0640 root-owned. A re-run **keeps** existing
secrets; rotating a live artifact signing key invalidates every token already
minted, so that needs `--rotate-secrets`.

## Exposure and refusal-at-startup

The loopback plane binds `127.0.0.1` and requires none of the off-loopback
secrets. Binding wider is **refused at startup** until:

- `EYVAN_REPOSITORY_BACKEND=postgres` (the local store has no cross-replica leader
  election),
- `EYVAN_METRICS_TOKEN` (a network `/metrics` requires scrape auth),
- `EYVAN_ARTIFACT_TOKEN_KEY` (capability tokens must be replica-stable),

plus the OIDC and artifact-store prerequisites enumerated in Part 16. `eyvan
doctor --server-host 0.0.0.0` names every refusal before you start — the same list
the server would refuse with, asked in advance.

Mutual-TLS modes exist for the service boundary (`EYVAN_MUTUAL_TLS_MODE`,
`EYVAN_MUTUAL_TLS_CLIENT_ENABLED`, `EYVAN_MUTUAL_TLS_TRUST_BUNDLE`,
`EYVAN_MUTUAL_TLS_CERTIFICATE`, `EYVAN_MUTUAL_TLS_PRIVATE_KEY`), implemented in
`eyvan/identity/mtls.py`. Provider credentials (`EYVAN_AWS_*`, `EYVAN_AZURE_*`,
gcloud's own) stay scoped to the agent or plane process that talks to that cloud.

## What a job can and cannot do

May: use its leased hardware; read its inputs from the artifact store by digest;
write its result manifest; — with `can_submit_children=True` — submit child
workloads under your identity (a capability nobody asked for is a credential nobody
is watching, so it defaults off).

May not: name other tenants' artifacts (scoped identity `(org_id, project_id,
uri)` forbids it), hold plane credentials, or outlive its lease silently — cells
are reaped on the fleet's idle timeout regardless of what the workload believes.

## Design records with honest status

The security page lists accepted design records **with their real implementation
state** — do not read a design as a provider claim:

- Scoped artifact lifecycle (0001): implemented at the host/PostgreSQL boundary;
  live migration and destructive-collection evidence still required.
- Global log ranks/cursors (0002): the M1.3 surface implemented; strict
  topology-bound rejection and the full archive manifest pending.
- Service health and metrics ownership (0003): implementation-pending.
- Federation trust (0004): implementation-pending; requires external PKI and an
  external quorum-backed lease/CAS service — deliberately not an Eyvan-built CA or
  consensus protocol.

## Ingestion hygiene in the code

A few concrete places where the posture is enforced rather than hoped for:

- Dev-auth headers are gated by `is_own_dashboard_request()` — same-origin fetch,
  loopback literal Host — defending against DNS rebinding (Part 9).
- The worker's stage-actions route enforces the `EYVAN_CELL_STAGE_SOURCES`
  allowlist (empty by default) so it can never become an arbitrary-read primitive
  (Part 10).
- Secret comparisons use `compare_digest`/`hmac.compare_digest`; non-ASCII
  bootstrap credentials yield 401, not a 500.
- Signing keys are refused on remote planes (`reject_remote_callable_signing_key`)
  — your secrets never leave for a plane you do not own.
- `EYVAN_TRUSTED_CALLABLE_PROJECTS`: a pickled callable executes arbitrary code;
  empty trusts nobody — callable submissions are refused until projects are
  explicitly trusted (command workloads still run).
