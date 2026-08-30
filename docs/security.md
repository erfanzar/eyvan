# Security model

## Production boundary decisions

These accepted design records have distinct implementation and qualification
states and must not be read as provider claims:

- [artifact scope and generation-fenced deletion](design/0001-scoped-artifact-lifecycle.md)
  aligns the metadata key with the tenant-scoped byte store and is
  implemented at the host/PostgreSQL boundary; live migration, object-store,
  and destructive collection evidence is still required;
- [log rank and cursor compatibility](design/0002-global-log-ranks-and-cursors.md)
  treats an opaque cursor as untrusted state, not as a credential; the
  narrower M1.3 global-rank/cursor/archive surface is implemented, but strict
  topology-bound rejection and the full archive manifest remain pending along
  with multi-host hardware evidence;
- [service health and metrics ownership](design/0003-service-health-and-metrics.md)
  remains implementation-pending and keeps requests, responses, and raw
  application telemetry out of Eyvan; and
- [federation trust and witnessed ownership](design/0004-federation-consistency.md)
  remains implementation-pending and requires external PKI and an external
  quorum-backed lease/CAS service rather than an Eyvan-built CA or consensus
  protocol.

## The authority boundary

The plane is the authority for placement, leasing, and reap. Agents own
machines and execute what the plane leases to them. Your callable runs on
worker hosts under the native runtime — it is workload code, not trusted
code, and the security posture is: signed, digest-verified inputs; least
privilege on the worker; no plane credentials in the workload environment.

## Who a request is

- **SDK → plane:** your submission is a callable artifact (signed with the
  submitter's key context) plus a workload record. The plane verifies and
  stores the artifact by content digest before anything is scheduled.
- **Plane → agent:** bearer authentication with
  `EYVAN_AGENT_SHARED_SECRET`. A mismatch does not fail closed silently —
  it 401-loops visibly in both logs (receipt: 2026-08-29, a bare
  `eyvan-server` restart against a profiled agent; fixed by restarting the
  plane with the profile's secret). `--allow-unauthenticated` on the agent
  exists for local development only.
- **Agent → native workers:** a separate root secret
  (`EYVAN_NATIVE_WORKER_ROOT_SECRET`) fences worker initialization.

## Capability tokens and secrets

| secret | who reads it | what it protects |
|---|---|---|
| `EYVAN_AGENT_SHARED_SECRET` | plane, agent | plane↔agent API |
| `EYVAN_NATIVE_WORKER_ROOT_SECRET` | agent | native worker bring-up |
| `EYVAN_ARTIFACT_TOKEN_KEY` | plane | artifact store tokens |
| `EYVAN_METRICS_TOKEN` | metrics clients | metrics scrape auth |
| `EYVAN_BOOTSTRAP_TOKEN` | plane bootstrap | first-touch provisioning |

Secrets enter via environment or the runtime profile
(`~/.eyvan/gcp-native-runtime.json`, written `0600`), never via fleet configs
or the repository. The full surface is tabulated in
[env-surface](env-surface.md).

## What jobs can do

A callable may: use its leased hardware, read its inputs from the artifact
store by digest, and write its result manifest. It may not: name other
tenants' artifacts, hold plane credentials, or outlive its lease silently —
cells are reaped on the fleet's idle timeout regardless of what the workload
believes. Destructive artifact collection is disabled unless explicitly
enabled (see [artifact-safety](artifact-safety.md)).

## Exposure

The loopback plane binds `127.0.0.1` and requires none of the off-loopback
secrets; binding wider is refused at startup until
`EYVAN_REPOSITORY_BACKEND=postgres`, `EYVAN_METRICS_TOKEN`, and
`EYVAN_ARTIFACT_TOKEN_KEY` are set (`eyvan doctor --server-host 0.0.0.0`
enumerates the refusals before you start). Mutual-TLS modes exist for the
service boundary (`EYVAN_MUTUAL_TLS_MODE`,
`EYVAN_MUTUAL_TLS_CLIENT_ENABLED`, `EYVAN_MUTUAL_TLS_TRUST_BUNDLE`,
`EYVAN_MUTUAL_TLS_CERTIFICATE`, `EYVAN_MUTUAL_TLS_PRIVATE_KEY`); provider
credentials (`EYVAN_AWS_ACCESS_KEY_ID`, `EYVAN_AWS_SECRET_ACCESS_KEY`,
`EYVAN_AWS_SESSION_TOKEN`, `EYVAN_AWS_REGION`, `EYVAN_AZURE_ACCESS_TOKEN`,
`EYVAN_AZURE_SUBSCRIPTION_ID`, `EYVAN_AZURE_RESOURCE_GROUP`,
`EYVAN_AZURE_LOCATION`, and gcloud's own) stay scoped to the agent or plane
process that talks to that cloud.
