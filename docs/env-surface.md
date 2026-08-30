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
| `EYVAN_AWS_ACCESS_KEY_ID`, `EYVAN_AWS_REGION`, `EYVAN_AWS_SECRET_ACCESS_KEY`, `EYVAN_AWS_SESSION_TOKEN` | AWS provider credentials |
| `EYVAN_AZURE_ACCESS_TOKEN`, `EYVAN_AZURE_LOCATION`, `EYVAN_AZURE_RESOURCE_GROUP`, `EYVAN_AZURE_SUBSCRIPTION_ID` | Azure provider credentials |

## Capacity, cells, and dispatch

`EYVAN_CAPACITY_RACE_WIDTH`, `EYVAN_CAPACITY_SUPPORTS_EXEC`,
`EYVAN_CELL_RECONCILE_INTERVAL_S`, `EYVAN_CELL_RECONCILE_CONCURRENCY`,
`EYVAN_CELL_BILLING_INTERVAL_S`, `EYVAN_CELL_STAGE_BUDGET_BYTES`,
`EYVAN_CELL_STAGE_DIR`, `EYVAN_CELL_STAGE_EVICTION_ENABLED`,
`EYVAN_CELL_STAGE_EVICTION_POLICY`, `EYVAN_CELL_STAGE_INTERVAL_S`,
`EYVAN_CELL_STAGE_MAX_ATTEMPTS`, `EYVAN_CELL_STAGE_MAX_BYTES`,
`EYVAN_CELL_STAGE_SOURCES`, `EYVAN_MAX_EXPANSION_CHILDREN`,
`EYVAN_EXPANSION_WAVE_SIZE`, `EYVAN_ATOMIC_GROUP_PLACEMENT`,
`EYVAN_HOST_REPLACEMENT_BUDGET`, `EYVAN_HOST_REPLACEMENT_WINDOW_S`,
`EYVAN_PREEMPTION_STOP_GRACE_S` — reconcile cadence, staging limits, and how
aggressively the planner fans out. Defaults in `eyvan/config.py`.

## Gang (multi-host) initialization

`EYVAN_GANG_INIT_DEADLINE_S`, `EYVAN_GANG_HEARTBEAT_TIMEOUT_S`,
`EYVAN_GANG_INIT_FATAL` — the deadlines and liveness rules for bringing up
one allocation across several hosts.

## Artifacts and retention

`EYVAN_ARTIFACT_COLLECTION_INTERVAL_S`,
`EYVAN_ARTIFACT_DESTRUCTIVE_COLLECTION_ENABLED`,
`EYVAN_ARTIFACT_MAX_CALLABLE_BYTES`, `EYVAN_ARTIFACT_MAX_RESULT_BYTES`,
`EYVAN_ARTIFACT_MAX_WORKSPACE_BYTES`, `EYVAN_ARTIFACT_RECOVERY_GRACE_S`,
`EYVAN_ARTIFACT_RETENTION_S`, `EYVAN_ARTIFACT_TOKEN_TTL_S`,
`EYVAN_ARTIFACT_URI`, and `EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP` — size caps,
retention windows, the insecure-HTTP escape hatch (development), and the
destructive-collection kill switch (off by default; see
[artifact-safety](artifact-safety.md)).

## Controller and service

`EYVAN_CONTROLLER_ENABLED`, `EYVAN_AUTO_CREATE_SCHEMA`,
`EYVAN_AUDIT_RETENTION_S`, `EYVAN_IDEMPOTENCY_RETENTION_S`,
`EYVAN_GRANT_EXPIRY_INTERVAL_S`, `EYVAN_CHECKPOINT_GRACE_S`,
`EYVAN_CREDENTIAL_ROTATION_OVERLAP_S`,
`EYVAN_PROVIDER_METADATA_IDENTITY_ENABLED`,
`EYVAN_PROVIDER_METADATA_IDENTITY_TIMEOUT_S`,
`EYVAN_MUTUAL_TLS_MODE`, `EYVAN_MUTUAL_TLS_CLIENT_ENABLED`,
`EYVAN_MUTUAL_TLS_TRUST_BUNDLE`, `EYVAN_MUTUAL_TLS_CERTIFICATE`,
`EYVAN_MUTUAL_TLS_PRIVATE_KEY`.

## Reading it in code

The authoritative resolution is `EyvanSettings` in `eyvan/config.py` — every
`os.getenv("EYVAN_…")` sits next to its default, and the profile-based path
(`from_gcp_native_runtime_profile`) is how a profiled setup supplies the
secret subset without environment plumbing. When a setting exists both as
code and as environment, the environment wins only where the comment says
so; prefer constructing settings explicitly in code.
