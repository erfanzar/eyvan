# The Eyvan Helm chart

The chart installs the control plane, platform routing ConfigMap, schema-policy
migration hook, and, when explicitly enabled, a separately built cell agent. In
Kubernetes capacity mode it also installs namespaced worker RBAC. Release
resources use `.Release.Namespace`; an optional worker namespace defaults to it.

## Prerequisites and Secret contract

The `pre-install` hook runs before ordinary chart resources. Create the namespace and Secret before the first install; specifically, create the release
namespace and `existingSecret`, then keep `--create-namespace` on install
commands. `existingSecret` defaults to `eyvan-secrets` and must contain:

| key | consumer |
|---|---|
| `database-url` | migration and control plane |
| `artifact-token-key` | control plane; identical on every replica |
| `agent-shared-secret` | control plane and cell agent |
| `metrics-token` | control plane and optional `ServiceMonitor` |
| `native-worker-root-secret` | cell agent; distinct from the agent secret |

The chart creates neither credentials nor PostgreSQL. Regenerating
`artifact-token-key` invalidates issued capabilities, and tying a database to a
release makes `helm uninstall` a data-loss operation.

## Build and qualify immutable images

Every Dockerfile expects the Eyvan package directory as its build context. The
Kubernetes worker has its own `Dockerfile.worker` and explicit
`eyvan-native-worker` entrypoint. Its two base-stage arguments are intentionally
required: set each to an immutable `repository@sha256:...` reference from the
release lock, not a tag.

```bash
: "${EYVAN_WORKER_BASE_IMAGE:?set an immutable Python base image reference}"
: "${EYVAN_UV_IMAGE:?set an immutable uv image reference}"

# From the EasyDeL workspace root:
docker build -f libs/eyvan/Dockerfile \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_CONTROL_PLANE_REPOSITORY:$BUILD_ID" libs/eyvan
docker build -f libs/eyvan/Dockerfile.agent \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_AGENT_REPOSITORY:$BUILD_ID" libs/eyvan
docker build -f libs/eyvan/Dockerfile.worker \
  --build-arg EYVAN_WORKER_BASE_IMAGE="$EYVAN_WORKER_BASE_IMAGE" \
  --build-arg EYVAN_UV_IMAGE="$EYVAN_UV_IMAGE" \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_WORKER_REPOSITORY:$BUILD_ID" libs/eyvan

# From a standalone Eyvan checkout:
docker build -f Dockerfile \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_CONTROL_PLANE_REPOSITORY:$BUILD_ID" .
docker build -f Dockerfile.agent \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_AGENT_REPOSITORY:$BUILD_ID" .
docker build -f Dockerfile.worker \
  --build-arg EYVAN_WORKER_BASE_IMAGE="$EYVAN_WORKER_BASE_IMAGE" \
  --build-arg EYVAN_UV_IMAGE="$EYVAN_UV_IMAGE" \
  --build-arg EYVAN_REVISION="$SOURCE_REVISION" \
  -t "$EYVAN_WORKER_REPOSITORY:$BUILD_ID" .
```

Push only unique build tags, then obtain the registry-reported digest of each
artifact. Qualification values have `sha256:` followed by 64 lowercase hex
characters; this document deliberately supplies no example digest.

```bash
: "${EYVAN_CONTROL_PLANE_DIGEST:?set the registry-reported control-plane digest}"
: "${EYVAN_AGENT_DIGEST:?set the registry-reported agent digest}"
: "${EYVAN_WORKER_DIGEST:?set the registry-reported worker digest for Kubernetes mode}"
```

The release pipeline must fail closed on High/Critical vulnerability findings,
emit an SPDX or CycloneDX SBOM for every deployed digest, sign those digest
references, and verify signatures with the deployment identity. All three
Dockerfiles carry OCI version, revision, and license labels. These are release
gates, not claims that this repository run performed a registry scan or push.

## GCP capacity mode

`cellAgent.enabled` defaults to `false`; set an external mTLS agent endpoint or
enable the in-cluster owner deliberately. When enabled, rendering fails unless
`cellAgent.replicas` is exactly `1` and `cellAgent.fleetStateUri` is a canonical
`gs://bucket/non-empty-object` URI. Its Deployment uses `Recreate`, so an upgrade
cannot overlap two owners of the same fleet-state object.

`cellAgent.capacityBackend` is explicit and defaults to `gcp`. Native VM
bootstrap downloads the wheel and wheelhouse only from the configured GCS object
generations and verifies each file's SHA-256 before extraction or installation.
Record both values after upload (for example from your release metadata or an
object describe operation); a content-addressed object name alone does not
replace the check.

```bash
helm upgrade --install eyvan ./deploy/helm/eyvan \
  --namespace eyvan-system --create-namespace --wait --wait-for-jobs \
  --set-string image.repository="$EYVAN_CONTROL_PLANE_REPOSITORY" \
  --set-string image.digest="$EYVAN_CONTROL_PLANE_DIGEST" \
  --set-string agentImage.repository="$EYVAN_AGENT_REPOSITORY" \
  --set-string agentImage.digest="$EYVAN_AGENT_DIGEST" \
  --set-string controlPlane.publicUrl=https://eyvan.example.com \
  --set-string controlPlane.artifactStoreUri=gs://your-bucket/eyvan \
  --set-string controlPlane.oidc.issuer=https://idp.example.com \
  --set-string controlPlane.oidc.dashboardClientId=eyvan-dashboard \
  --set cellAgent.enabled=true \
  --set-string cellAgent.capacityBackend=gcp \
  --set-string cellAgent.fleetStateUri=gs://your-state-bucket/eyvan/fleet-state.json \
  --set-string cellAgent.nativeWorkerPackage="$EYVAN_WORKER_WHEEL_URI" \
  --set-string cellAgent.nativeWorkerPackageSha256="$EYVAN_WORKER_WHEEL_SHA256" \
  --set-string cellAgent.nativeWorkerPackageGeneration="$EYVAN_WORKER_WHEEL_GENERATION" \
  --set-string cellAgent.nativeWorkerWheelhouse="$EYVAN_WORKER_WHEELHOUSE_URI" \
  --set-string cellAgent.nativeWorkerWheelhouseSha256="$EYVAN_WORKER_WHEELHOUSE_SHA256" \
  --set-string cellAgent.nativeWorkerWheelhouseGeneration="$EYVAN_WORKER_WHEELHOUSE_GENERATION"
```

For GKE Workload Identity, set annotations as values rather than embedding cloud
credentials. For example:

```yaml
serviceAccounts:
  controlPlane:
    annotations:
      iam.gke.io/gcp-service-account: eyvan-control-plane@PROJECT_ID.iam.gserviceaccount.com
  cellAgent:
    annotations:
      iam.gke.io/gcp-service-account: eyvan-agent@PROJECT_ID.iam.gserviceaccount.com
```

## Kubernetes capacity mode

Kubernetes mode runs one immutable `Dockerfile.worker` artifact per worker in an
Indexed Job. The worker namespace defaults to the release namespace. If
`cellAgent.kubernetes.workerNamespace` differs, create that namespace before the
release; the chart deliberately does not own its lifecycle. Configure a durable,
agent-unique `cellAgent.fleetStateUri`; the example uses GCS and the cell-agent
Workload Identity annotation supplies its cloud identity.

```bash
helm upgrade --install eyvan ./deploy/helm/eyvan \
  --namespace eyvan-system --create-namespace --wait --wait-for-jobs \
  --set-string image.repository="$EYVAN_CONTROL_PLANE_REPOSITORY" \
  --set-string image.digest="$EYVAN_CONTROL_PLANE_DIGEST" \
  --set-string agentImage.repository="$EYVAN_AGENT_REPOSITORY" \
  --set-string agentImage.digest="$EYVAN_AGENT_DIGEST" \
  --set cellAgent.enabled=true \
  --set-string cellAgent.capacityBackend=kubernetes \
  --set-string cellAgent.fleetStateUri=gs://your-state-bucket/eyvan/kubernetes-fleet-state.json \
  --set-string cellAgent.kubernetes.workerNamespace=eyvan-workers \
  --set-string workerImage.repository="$EYVAN_WORKER_REPOSITORY" \
  --set-string workerImage.digest="$EYVAN_WORKER_DIGEST"
```

The chart creates `eyvan-native-worker` in the worker namespace and a Role plus
RoleBinding for the release namespace's `eyvan-cell-agent`. The Role can create,
get, and delete Jobs, Services, and Secrets, and read Pods/events. It grants no
cluster-wide role, pod exec, pod logs, or arbitrary mutation. Optional Kueue
ClusterQueue inspection is cluster scoped and therefore is not granted by the
stock Role; opt-in Kueue needs a separately reviewed read grant. Worker
service-account annotations are configurable at
`serviceAccounts.worker.annotations`.

Every generated worker pod carries `eyvan.dev/api-client=true`. For a separate
worker namespace, the API ingress peer ANDs that pod selector with Kubernetes'
automatic exact `kubernetes.io/metadata.name=<worker-namespace>` namespace label;
the chart validates the configured namespace as the exact DNS-label value. A
same-release-namespace client still needs the narrow pod label, and no bare
namespace selector admits unrelated pods.

Every worker image is rendered as `repository@sha256:...`; Kubernetes mode fails
template rendering when `workerImage.digest` is absent or malformed. There is no
worker tag fallback.

## Schema changes are maintenance, not rolling rollout

The hook inspects the database before calling Alembic:

* a database with no tables may be initialized;
* a database already at the packaged head proceeds as a no-op, so ordinary
  same-head rollouts remain usable;
* a non-empty schema at another revision is refused by default.

`--atomic` cannot reverse a committed database migration. Automatic rollback may
instead restart the old image against the new schema. Do not enable maintenance
acknowledgement as a standing value, and do not infer application/schema
N/N-1 compatibility; no such compatibility contract is implemented.

For a release that actually changes the schema:

1. Quiesce every control-plane replica and any other database writer.
2. Create and verify a backup, then rehearse restoring it to an isolated database.
3. Run `eyvan-migrate --check` from the candidate image. A refusal on the
   non-empty old schema is expected until maintenance is acknowledged.
4. Set `migration.maintenanceAcknowledged=true` for this one release and run the
   migration/upgrade without relying on `--atomic` as database rollback.
5. Verify the new revision and application health, then reset the value to false
   and resume writers.

If application rollout fails after migration, hold writers quiesced and repair
forward with a corrected image or migration. Never run `alembic downgrade` (or
any schema downgrade) as Helm rollback. Backup restore is an operator-controlled
disaster-recovery action, not an automatic release rollback.

## Platform config and metrics

When `cellAgent.enabled=true`, the default route is
`http://eyvan-cell-agent.<release-namespace>.svc.cluster.local:8090`. With the
safe disabled default, set `platform.defaultAgentEndpoint` to the external mTLS
agent. The control-plane pod template includes a checksum of the ConfigMap
content, so Helm changes to `platform.*` trigger a rollout.

External Secret contents and ServiceAccount annotations are not part of the pod
template checksum. After changing either, explicitly restart and wait for every
affected control-plane or cell-agent Deployment so no pod keeps the old identity
or credential.

`serviceMonitor.enabled` remains false because its CRD may not exist. When
explicitly enabled, it reads the required `metrics-token` key. A scraper in the
release namespace needs `eyvan.dev/metrics-scraper=true` on its pod; a
cross-namespace scraper needs that label on both namespace and pod. `/metrics`
and the API share TCP 8080, so NetworkPolicy cannot distinguish their paths;
the metrics bearer and API authentication remain the authorization boundary.

## Mandatory local render/schema validation

The CI-ready validation command fails when either tool is absent or has the
wrong version. It runs pinned `helm lint`, `helm template`, and kubeconform over
the safe disabled default, qualified GCP, and Kubernetes namespace/RBAC modes,
then runs strict kubeconform over every render and the raw
manifests:

```bash
scripts/validate-deployment.sh
```

It pins Helm v3.18.6 and kubeconform v0.7.0. It does not silently skip missing
schema tools. The separate artifact smoke builds and installs both distributions
in clean virtual environments outside the source tree:

```bash
scripts/smoke-package-artifacts.sh
```

These checks do not prove Kubernetes admission, image pullability, migration
completion, readiness, agent registration, an authenticated scrape, or any
provider/cluster behavior. Those require the exact qualified artifacts and a
disposable environment.
