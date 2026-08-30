# The `EYVAN_*` environment surface

Every `EYVAN_*` variable the package reads, generated from the source
(`rg -o '"EYVAN_[A-Z0-9_]+"' eyvan/ | sort -u` reproduces the list). The
decision-critical ones are annotated; the rest are tuning knobs with safe
defaults in `eyvan/config.py` — read the default next to the `os.getenv`
call before changing one. No new environment knobs are added lightly; prefer
the config surface.

## Auth and secrets (refused-without, when bound off-loopback)

| variable | read by | effect |
|---|---|---|
| `EYVAN_AGENT_SHARED_SECRET` | plane, agent | bearer secret for the plane↔agent API; missing on the agent is a startup refusal, missing on the plane is a 401 loop |
| `EYVAN_NATIVE_WORKER_ROOT_SECRET` | agent | required to start native worker execution |
| `EYVAN_ARTIFACT_TOKEN_KEY` | plane | keys artifact-store tokens; required off-loopback |
| `EYVAN_METRICS_TOKEN` | metrics clients | metrics scrape auth; required off-loopback |
| `EYVAN_BOOTSTRAP_TOKEN` | plane | first-touch bootstrap |
| `EYVAN_DEV_AUTH_ENABLED` | plane | development auth (never in production) |

## Backends and repositories

| variable | effect |
|---|---|
| `EYVAN_REPOSITORY_BACKEND` / `EYVAN_DATABASE_URL` | durable model: loopback JSON vs PostgreSQL |
| `EYVAN_CAPACITY_BACKEND` / `EYVAN_EXECUTION_BACKEND` | which capacity/execution implementations the plane runs |
| `EYVAN_FLEET_STATE_URI` | where fleet state lives (e.g. the GCS fleet-state object) |
| `EYVAN_ARTIFACT_STORE_URI` | artifact store root |
| `EYVAN_AWS_*`, `EYVAN_AZURE_*` | provider credentials, scoped to the process talking to that cloud |

## Capacity, cells, and dispatch

`EYVAN_CAPACITY_RACE_WIDTH`, `EYVAN_CAPACITY_SUPPORTS_EXEC`,
`EYVAN_CELL_RECONCILE_INTERVAL_S`, `EYVAN_CELL_RECONCILE_CONCURRENCY`,
`EYVAN_CELL_BILLING_INTERVAL_S`, `EYVAN_CELL_STAGE_*` (dir, budget bytes,
eviction policy/enablement, interval, max attempts/bytes, sources),
`EYVAN_MAX_EXPANSION_CHILDREN`, `EYVAN_EXPANSION_WAVE_SIZE`,
`EYVAN_ATOMIC_GROUP_PLACEMENT`, `EYVAN_HOST_REPLACEMENT_*`,
`EYVAN_PREEMPTION_*`-adjacent controller knobs — reconcile cadence, staging
limits, and how aggressively the planner fans out. Defaults in
`eyvan/config.py`.

## Gang (multi-host) initialization

`EYVAN_GANG_INIT_DEADLINE_S`, `EYVAN_GANG_HEARTBEAT_TIMEOUT_S`,
`EYVAN_GANG_INIT_FATAL` — the deadlines and liveness rules for bringing up
one allocation across several hosts.

## Artifacts and retention

`EYVAN_ARTIFACT_*` (`COLLECTION_INTERVAL_S`,
`DESTRUCTIVE_COLLECTION_ENABLED`, `MAX_CALLABLE/RESULT/WORKSPACE_BYTES`,
`RECOVERY_GRACE_S`, `RETENTION_S`, `TOKEN_TTL_S`), plus
`EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP` — size caps, retention windows, the
insecure-HTTP escape hatch (development), and the destructive-collection
kill switch (off by default; see [artifact-safety](artifact-safety.md)).

## Controller and service

`EYVAN_CONTROLLER_ENABLED`, `EYVAN_AUTO_CREATE_SCHEMA`,
`EYVAN_AUDIT_RETENTION_S`, `EYVAN_IDEMPOTENCY_RETENTION_S`,
`EYVAN_GRANT_EXPIRY_INTERVAL_S`, `EYVAN_CHECKPOINT_GRACE_S`,
`EYVAN_CREDENTIAL_ROTATION_OVERLAP_S`, `EYVAN_PROVIDER_METADATA_*`,
`EYVAN_MUTUAL_TLS_*` (mode, client enablement, trust bundle, cert, key).

## Reading it in code

The authoritative resolution is `EyvanSettings` in `eyvan/config.py` — every
`os.getenv("EYVAN_…")` sits next to its default, and the profile-based path
(`from_gcp_native_runtime_profile`) is how a profiled setup supplies the
secret subset without environment plumbing. When a setting exists both as
code and as environment, the environment wins only where the comment says
so; prefer constructing settings explicitly in code.
