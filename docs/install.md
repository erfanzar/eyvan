# Installing Eyvan

A network-bound deployment needs fifteen separately configured things. Local
`eyvan up` needs none of them; it is a loopback development path, not a remote
installation recipe.

## The fifteen

| # | What | Why it cannot be defaulted | Missing looks like |
|---|------|---------------------------|--------------------|
| 1 | PostgreSQL and `EYVAN_DATABASE_URL` | The local repository has no cross-replica leader election. | Startup refuses a network bind/backend mismatch. |
| 2 | The schema, managed by `eyvan-migrate` | Production disables auto-create. Existing-schema changes require maintenance acknowledgement. | Startup reports `SchemaRevisionMismatch`, or the migration safety gate refuses. |
| 3 | `EYVAN_OIDC_ISSUER` | Development auth is refused on a network bind. | Every request is 401. |
| 4 | `EYVAN_OIDC_AUDIENCE` | A token for another service must not authenticate here. | IdP-valid tokens are rejected by Eyvan. |
| 5 | The non-secret `EYVAN_OIDC_DASHBOARD_CLIENT_ID` | The browser is a public PKCE client and must know its client identifier at runtime. | Hosted dashboard login cannot start. |
| 6 | An IdP `eyvan_grants` claim or durable grants from `eyvan grant add` | Roles need an authoritative source. | Authenticated requests are 403. |
| 7 | One artifact signing key of at least 32 bytes, identical on every replica | Capability tokens are HMACs. | Intermittent artifact/secret 403s track replica count. |
| 8 | `EYVAN_ARTIFACT_STORE_URI` (`gs://...`) | A pod filesystem is not shared. | One replica cannot read another's result. |
| 9 | `EYVAN_PUBLIC_URL` over HTTPS | Jobs receive capability URLs built from it. | Jobs cannot fetch callables or secrets. |
| 10 | `EYVAN_TRUSTED_CALLABLE_PROJECTS` | A pickled callable executes arbitrary code; empty trusts nobody. | Callable submissions are refused; command workloads still work. |
| 11 | A high-entropy `EYVAN_METRICS_TOKEN` | `/metrics` exposes spend labels and a network server requires scrape authentication. | The server exits naming the setting. |
| 12 | One agent shared secret | The control plane authenticates cell-agent mutations. | Agent operations are refused. |
| 13 | A distinct, stable native-worker root secret | It derives per-cell credentials; reusing the agent secret collapses capability boundaries. | Workers fail authentication or rotation breaks live cells. |
| 14 | `platform.yaml` and one agent per capacity boundary, each with its own `EYVAN_FLEET_STATE_URI` | Shared fleet-state ownership fences competing agents. | Pool upserts lack a route, or cells flap on fence conflicts. |
| 15 | A native worker artifact pinned by digest (Kubernetes) or wheel/wheelhouse SHA-256 plus GCS generation (GCP) | Tags and object names alone can be replaced; private-egress VMs cannot reach PyPI. | Workers pull different code, fail verification, or never register. |

TLS termination, secret-manager integration, and mTLS in front of every external
agent are additional platform prerequisites.

## Secret and non-secret configuration

The raw manifests deliberately do not create credentials. Provision these after
creating `eyvan-system` and before migration:

* `eyvan-secrets`: `database-url`, `oidc-issuer`, `public-url`,
  `artifact-store-uri`, `artifact-token-key`, `trusted-callable-projects`,
  `agent-shared-secret`, and `metrics-token`.
* `eyvan-agent-secrets`: `shared-secret` (the same bytes as the control-plane
  agent secret) and a distinct `native-worker-root-secret`.

The dashboard client identifier is **not** a secret. Replace the raw manifest's
`EYVAN_OIDC_DASHBOARD_CLIENT_ID: eyvan-dashboard` value with the public PKCE
client registered at the issuer. Do not move it into a Secret or compile it into
a different image for each environment.

The Helm chart uses one `existingSecret`; non-secret issuer, audience, dashboard
client ID, public URL, artifact URI, callable allowlist, capacity backend, and
worker namespace are chart values. Service-account annotations are also values,
which supports GKE Workload Identity without embedding cloud credentials. The
in-cluster cell agent is disabled by default; enabling it requires exactly one
`Recreate` replica and a canonical durable `gs://bucket/non-empty-object` fleet
state URI.

## Raw manifest order

The raw files are intentionally split. A failed Job does not stop an unrelated
Deployment in the same directory, and filename order is not a migration barrier.
The raw cell agent is also fixed at one `Recreate` replica; replace its fleet-state
placeholder with one durable `gs://bucket/non-empty-object`, never a pod-local path.

1. Replace `PROJECT_ID`, all bucket/wheel/hash/generation placeholders, and every
   image marker. `deploy/prerequisites.yaml` maps both `eyvan-control-plane` and
   `eyvan-cell-agent` KSAs to separate GCP service accounts; the former needs GCS
   artifact-store access and the latter needs fleet-state/bootstrap access. On a
   cluster without GKE Workload Identity, remove both example annotations. The
   migration and control-plane references must be the same registry-reported
   `repository@sha256:...` digest. The cell-agent image is a separate digest from
   `Dockerfile.agent`.
2. Apply prerequisites, then provision both Secrets.
3. Recreate and wait for the safe migration Job.
4. Only after it completes, apply the agent and control-plane Deployments.

```bash
kubectl apply -f deploy/prerequisites.yaml
# Provision eyvan-secrets and eyvan-agent-secrets with your secret manager here.
kubectl -n eyvan-system get secret eyvan-secrets eyvan-agent-secrets

kubectl -n eyvan-system delete job eyvan-schema-migration --ignore-not-found
kubectl apply -f deploy/migration-job.yaml
kubectl -n eyvan-system wait \
  --for=condition=complete --timeout=10m job/eyvan-schema-migration

kubectl apply -f deploy/cell-agent.yaml
kubectl apply -f deploy/control-plane.yaml
kubectl -n eyvan-system rollout status deployment/eyvan-cell-agent --timeout=10m
kubectl -n eyvan-system rollout status deployment/eyvan-control-plane --timeout=10m
```

Do not use `kubectl apply -f deploy/`. Repeat the delete/apply/wait sequence
before every raw upgrade.

The raw platform ConfigMap does not restart or refresh the existing `subPath`
mount when it changes. ServiceAccount annotation and externally provisioned
Secret changes also do not alter a Deployment pod template. After changing any
of them, apply the object and explicitly restart and wait for every affected
Deployment; in particular, changing the `eyvan-control-plane` WIF annotation
requires a control-plane rollout:

```bash
kubectl apply -f deploy/prerequisites.yaml
kubectl -n eyvan-system rollout restart deployment/eyvan-control-plane deployment/eyvan-cell-agent
kubectl -n eyvan-system rollout status deployment/eyvan-control-plane --timeout=10m
kubectl -n eyvan-system rollout status deployment/eyvan-cell-agent --timeout=10m
```

The Helm chart places a content checksum in the pod template, so a `platform.*`
value change causes a rollout automatically. External Secret content and
ServiceAccount annotation changes are not checksummed there either; restart the
affected Helm-managed Deployments after either change.

## Safe schema upgrades

`eyvan-migrate` first inspects tables and Alembic revisions. A fresh database and
an existing database already at packaged head proceed without acknowledgement.
A non-empty schema at any other revision refuses to upgrade unless the operator
passes `--maintenance-acknowledged`. That keeps a normal rolling or `--atomic`
release from silently committing a schema change that application rollback
cannot reverse.

Before an actual schema change:

1. Quiesce every control-plane replica and any other database writer.
2. Create and verify a backup; rehearse restoring it into an isolated database.
3. Run the candidate image's `eyvan-migrate --check`.
4. For the one maintenance run, add
   `args: ["--maintenance-acknowledged"]` to `deploy/migration-job.yaml`, apply and
   wait for it, verify the revision, then remove the argument before the next
   same-head deployment.
5. Deploy the candidate and resume writers only after health/revision checks.

If rollout fails after migration, keep writers quiesced and repair forward. Never
use `alembic downgrade` or another schema downgrade as release rollback. A
verified backup restore is disaster recovery, not an automatic rollback. Eyvan
does not currently promise application/schema N/N-1 compatibility.

The Helm equivalent and its one-release
`migration.maintenanceAcknowledged=true` value are documented in
[`deploy/helm/eyvan/README.md`](../deploy/helm/eyvan/README.md).

## Native bootstrap integrity

For each GCS wheel and wheelhouse, record the exact lowercase SHA-256 of the
uploaded bytes and the positive decimal GCS object generation. Raw manifests use:

* `EYVAN_NATIVE_WORKER_PACKAGE_SHA256` and
  `EYVAN_NATIVE_WORKER_PACKAGE_GENERATION`;
* `EYVAN_NATIVE_WORKER_WHEELHOUSE_SHA256` and
  `EYVAN_NATIVE_WORKER_WHEELHOUSE_GENERATION`.

Bootstrap qualifies the source URI with the generation, downloads it, and runs a
strict `sha256sum` check before pip or zip sees the file. A mismatch or missing
hash fails bootstrap. The Kubernetes capacity path does not download these
artifacts: it accepts only the separately built `Dockerfile.worker` image as a
`repository@sha256:...` reference.

## NetworkPolicy, metrics, and dashboard distribution

Every Helm-created Kubernetes worker pod carries `eyvan.dev/api-client=true`.
For a separate worker namespace, the Helm NetworkPolicy ANDs that pod selector
with the automatic exact
`kubernetes.io/metadata.name=<configured-worker-namespace>` namespace label; the
chart rejects a value that is not the exact Kubernetes DNS-label namespace name.
For the raw policy, a cross-namespace API client needs
`eyvan.dev/api-client=true` on both its namespace and its pod. Same-namespace API
clients need the pod label only. Neither policy contains a bare namespace peer.

The server reads `EYVAN_METRICS_TOKEN`; a scraper presents it as an
`Authorization: Bearer` credential. The optional Helm `ServiceMonitor` selects
the same Secret key. A same-namespace scraper needs
`eyvan.dev/metrics-scraper=true` on its pod; a cross-namespace scraper needs it on
both namespace and pod. `/metrics` and the API share TCP 8080, so NetworkPolicy
cannot distinguish URL paths. The scrape bearer and API authentication still
authorize the two application surfaces separately.

The Python distributions contain the CLI, API, worker entrypoints, and packaged
Alembic migrations, but neither Node sources nor built dashboard assets. The
production `Dockerfile` builds the static dashboard. From an installed wheel,
`eyvan dashboard` opens that hosted UI; `eyvan dashboard-dev` is explicitly
source-checkout-only.

## Capacity behaviour that ships disabled

Four settings gate behaviour that exists but is off, so an upgrade changes
nothing until an operator opts in. Each is off because it trades one failure for
another and only the operator knows which they prefer.

| Setting | Default | What turning it on does |
|---|---|---|
| `EYVAN_HOST_REPLACEMENT_BUDGET` | `0` | A machine whose own output names an unrecoverable accelerator fault is replaced instead of retried. Zero keeps today's retry. Each replacement costs a provision *and* a destroy, so the budget bounds a zone of bad hosts from becoming an infinite provision loop. |
| `EYVAN_HOST_REPLACEMENT_WINDOW_S` | `3600` | The window the budget above is counted over, per pool (or per provider/zone when a cell has no pool). |
| `EYVAN_CELL_STAGE_SOURCES` | empty | Comma-separated prefixes a cell may stage a named asset from. Named assets are user input, so an executor that fetched anything would be an arbitrary-read primitive. Empty reads as "no executor", never as a refusal: a refusal is terminal and would block every asset in the fleet over one unset variable. |
| `EYVAN_CELL_STAGE_DIR` | agent state directory | Where a cell keeps staged bytes. |

Staging additionally requires `EYVAN_CELL_STAGE_BUDGET_BYTES` above zero; with
the default of zero the staging phase plans nothing at all.

## Reproducible local checks

Run the focused Python checks plus the mandatory pinned deployment and artifact
smokes:

```bash
pytest tests/test_deployment_manifests.py tests/test_helm_chart.py   tests/test_packaging.py tests/test_native_bootstrap.py
scripts/validate-deployment.sh
scripts/smoke-package-artifacts.sh
```

`validate-deployment.sh` requires Helm v3.18.6 and kubeconform v0.7.0; it fails
rather than silently skipping a missing schema tool. The package smoke builds a
wheel and sdist, installs each in a clean virtual environment outside the source
tree, and invokes every installed console entrypoint.

These checks prove parsing, rendering, schema validation, packaging, and policy
logic only. They do not prove cluster admission, image pulls, a completed live
migration, readiness, authenticated scraping, provider behavior, or performance.
