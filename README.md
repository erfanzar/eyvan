# Eyvan (ایوان)

Eyvan is a durable, standalone global control plane for CPU and accelerator
workloads across GCP Compute Engine and Cloud TPU fleets. Its name comes from
the Persian eyvan: a vaulted gateway into the infrastructure it manages. Eyvan
combines global admission, priority, hierarchical fair share, budgets,
whole-gang placement, checkpoint-aware preemption, capacity leases, native GCP
CPU/GPU/TPU lifecycle management, and native local execution for single-host
development.

## Documentation

This page is the reference. The task-oriented pages are shorter and are where to
start:

- [docs/install.md](docs/install.md) — the thirteen things a remote deployment
  needs, what each is for, and how each fails when it is missing.
- [docs/operate.md](docs/operate.md) — daily operation: what is queued and why,
  cost, storage, durability, debugging one workload.
- [docs/security.md](docs/security.md) — the authority boundary, the three
  capability tokens, secrets, tenancy, and what is deliberately not covered.
- [docs/troubleshoot.md](docs/troubleshoot.md) — symptom first, routed to the
  command that answers it.
- [docs/proofs.md](docs/proofs.md) — the hardware proofs that have **not** been
  run, each with its commands, pass criterion, shape and cost. Everything in
  this package is CPU-verified; none of that is evidence about a TPU.


## Authority boundary

- Eyvan owns workload admission, capacity envelopes, leases, policy,
  placement, preemption, durable desired state, GCP Compute instances and TPU
  queued resources, and cell execution.
- Cell agents reconcile provider capacity and execution through Eyvan-owned
  backend contracts. The generic `gcp` capacity backend routes each cell to
  Compute Engine or Cloud TPU; capacity and execution backends are selected
  independently.
- eray is an independent sibling project and is not imported, installed, or
  mutated by Eyvan.
- A normal workload occupies one cell. A GCP TPU callable with
  `TpuConfig(pods=N)` occupies one atomic Cloud TPU Multislice cell containing
  `N` complete pool workers. Eyvan provisions one queued resource with
  `node_count=N`, then starts, observes, stops, and settles the whole allocation.

## Local development

Install all workspace packages from the repository root:

```bash
uv sync
```

The core Eyvan package has no Ray dependency. A temporary Ray Jobs adapter
is available only when explicitly installed:

```bash
uv sync --package eyvan --extra ray
```

Start the API with its durable owner-only local repository:

```bash
uv run eyvan-server --reload
```

The default bind is the literal loopback address `127.0.0.1`. In that mode,
`eyvan-server` enables development identity headers automatically, so the local
CLI and SDK work without an authentication setup. A non-loopback bind never
gets that implicit behavior. Set `EYVAN_DEV_AUTH_ENABLED=0` only when a local
server must reject those headers.

The unconfigured loopback server persists workloads, fleet pools, leases,
outbox commands, and audit history in `~/.eyvan/control-plane.json`. It also
creates one owner-only managed-artifact signing key at
`~/.eyvan/artifact-token.key`, so a server restart does not invalidate a
running local callable's artifact transfer. A second server cannot open the
same local state file. This local mode is deliberately unavailable on a
network bind; use PostgreSQL for a shared or network-exposed control plane.

`eyvan-server` starts the scheduler and capacity controller by default. Its
startup record and `/api/v1/health` report `controller: active`; an
intentionally API-only replica reports `controller: api_only` and leaves
workloads and capacity unreconciled. Set `EYVAN_CONTROLLER_ENABLED=0` only for
that API-only mode. Starting the default local server does not start an
agent or create cloud capacity.

For the PostgreSQL control plane, migrate first; the same server process joins
leader election and runs the durable outbox controller:

```bash
docker compose -f libs/eyvan/docker-compose.yml up -d postgres
uv run alembic -c libs/eyvan/alembic.ini upgrade head
EYVAN_REPOSITORY_BACKEND=postgres \
  uv run eyvan-server --reload
```

Installed wheels and the container images ship the same migrations inside the
`eyvan` package; the `eyvan-migrate` entrypoint runs `alembic upgrade head`
against them using `EYVAN_DATABASE_URL`, with no `alembic.ini` required:

```bash
EYVAN_DATABASE_URL=postgresql+asyncpg://eyvan:eyvan@127.0.0.1:5432/eyvan eyvan-migrate
```

In another terminal, start the dashboard:

```bash
cd libs/eyvan/dashboard
npm install
VITE_EYVAN_DEV_AUTH=true \
VITE_EYVAN_DEV_ROLE=fleet_admin \
  npm run dev
```

The API defaults to `http://127.0.0.1:8080`; the dashboard defaults to
`http://127.0.0.1:4173` and proxies `/api/v1` to the API. Development identity
headers are accepted only when `EYVAN_DEV_AUTH_ENABLED=1`. Mock data is never
used as an outage fallback; open `http://127.0.0.1:4173/?demo=1` only when an
explicit demo is wanted.

The CLI automatically uses the local OS user with the `fleet_admin` development
role when its control-plane URL is loopback, so local commands do not need
`--user` or auth environment variables. It never synthesizes this identity for
a remote URL, and an explicit bearer token always takes precedence.

Production authentication uses `EYVAN_OIDC_ISSUER` and
`EYVAN_OIDC_AUDIENCE`; standard OIDC discovery supplies the JWKS URI, or it
can be pinned with `EYVAN_OIDC_JWKS_URL`. The canonical API prefix is
`/api/v1`; `/v1` remains a compatibility alias.

Every Eyvan SDK and CLI request carries its version in `X-Eyvan-Client-Version`.
A deployment that has dropped support for older clients sets
`EYVAN_MIN_CLIENT_VERSION` (dotted integers, for example `0.2.0`); a request
from an older client is then rejected with `426 Upgrade Required` and a
`client_too_old` code before it reaches authentication, instead of failing
later with a confusing schema error. The gate is one-sided by design: a request
that sends no version header is accepted, because browsers, `curl`, and other
non-SDK callers never send one. A malformed floor fails at server startup
rather than rejecting live traffic.

The production dashboard uses authorization-code PKCE. Configure
`VITE_EYVAN_OIDC_ISSUER` and `VITE_EYVAN_OIDC_CLIENT_ID` when building
the image; optional settings are documented in `dashboard/README.md`.

```bash
docker build -f libs/eyvan/Dockerfile -t eyvan:latest \
  --build-arg VITE_EYVAN_OIDC_ISSUER=https://identity.example.com \
  --build-arg VITE_EYVAN_OIDC_CLIENT_ID=eyvan-dashboard .
```

## Declarative fleet operations

Operators manage fleets from one YAML file instead of constructing cell API
requests. The same schema describes CPU, GPU, and TPU pools, their initial and
minimum capacity, autoscaling bounds, purchase class, placement labels, native
runtime setup, and the cell execution backend. Start from
[`examples/fleet.yaml`](examples/fleet.yaml), then run:

```bash
uv run eyvan config validate libs/eyvan/examples/fleet.yaml
uv run eyvan up libs/eyvan/examples/fleet.yaml
uv run eyvan status libs/eyvan/examples/fleet.yaml
```

[`examples/easydel-v5p16.yaml`](examples/easydel-v5p16.yaml) is the matching
scale-from-zero Spot `v5p-16` pool for native EasyDeL work. Each worker is one
complete TPU worker allocation. A multi-worker callable provisions its
requested workers as one physical Cloud TPU Multislice rather than joining
unrelated queued resources.
Validate it before replacing its project, artifact, and image placeholders:

```bash
uv run eyvan config validate libs/eyvan/examples/easydel-v5p16.yaml
```

### Native runtime YAML

`runtime` is the native equivalent of the useful parts of Ray cluster setup:
operator-owned artifacts, one-time host setup, and workload environment
defaults. It belongs in the fleet YAML, not in a Python decorator, curl body,
or a hand-built `startup_script`.

```yaml
runtime:                       # fleet defaults
  env:
    HF_HOME: /var/lib/eyvan/workload/.cache/huggingface
    WANDB_MODE: offline

pools:
  v5p-16:
    label: v5p-16             # optional; must equal the mapping key
    # CPU, host memory, and TPU count come from the sourced hardware catalog.
    min_workers: 4            # four independent v5p-16 workers stay warm
    max_workers: 0            # unbounded YAML spelling; internally capped at 10,000
    runtime:                   # node override
      env:
        WANDB_MODE: online
        JAX_COMPILATION_CACHE_DIR: /var/lib/eyvan/workload/.cache/jax
      artifacts:
        - source: gs://research-runtime/releases/easydel.whl
          destination: /opt/eyvan-runtime/easydel.whl
      init_commands:
        - /opt/eyvan-native/venv/bin/python -m pip install --no-deps /opt/eyvan-runtime/easydel.whl
    provider_options:
      bootstrap_egress: private-google-access

  v5p-32:
    label: v5p-32
    min_workers: 8
    max_workers: 0
    provider_options:
      bootstrap_egress: private-google-access
```

Fleet-level `env` values merge into the pool; pool values win. Artifact
and `init_commands` lists run in fleet-then-node order on every host in the
cell, as the trusted bootstrap user after Eyvan has prepared its native worker
virtual environment. Artifacts
are staged before commands, so a command can install a staged wheel or invoke a
staged setup script. A job's explicit environment overrides `runtime.env`, and
Eyvan-owned workload and JAX rank variables override both. Runtime changes are
part of the capacity template fingerprint, so `eyvan up` drains and replaces
old cells instead of changing a live runtime under work.

`runtime.env` is durable fleet configuration, not a secret store. Do not put
credentials in it. Eyvan rejects `EYVAN_*`, JAX coordinator, and process-rank
variables because it owns those values. Today artifacts must be `gs://` URIs
and destinations must be absolute VM paths.

`runtime.vm_image` is supported only for `gcp-compute` node types and means a
GCP **VM OS image**. Cloud TPU queued resources use
`provider_options.runtime_version` instead.

#### OCI containers

`runtime.container` selects the optional OCI workload runtime for an
`eyvan-native` pool. It is a typed Docker contract, not a pass-through for raw
`docker run` arguments:

Build and pin the workload image before applying the fleet. The acceptance
recipe builds a Linux EasyDeL/JAX TPU image without Eyvan, eray, or Ray:

```bash
IMAGE=us-east5-docker.pkg.dev/my-gcp-project/eyvan/easydel-runtime:build-1
gcloud builds submit --config scripts/cloudbuild.eyvan-oci-smoke.yaml \
  --substitutions _IMAGE="$IMAGE" .
```

Stage application code separately; artifacts are deliberately not baked into
the image build:

```bash
gcloud storage cp --recursive training/ gs://my-eyvan-artifacts/training-code/
```

```yaml
runtime:
  artifacts:
    - source: gs://my-eyvan-artifacts/training-code/
      destination: /opt/eyvan-runtime/training
      recursive: true
  init_commands:
    # Ubuntu example. A prebuilt VM image or internal package mirror is often
    # preferable when the pool uses private Google access.
    - |
        if ! command -v docker >/dev/null 2>&1; then
          apt-get update
          apt-get install -y --no-install-recommends docker.io
        fi
        systemctl enable --now docker
        # Match this host to runtime.container.image exactly.
        gcloud auth configure-docker us-east5-docker.pkg.dev --quiet
  container:
    # Placeholder only. Pin a real production image by digest.
    image: us-east5-docker.pkg.dev/my-gcp-project/eyvan/easydel-runtime@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    pull_policy: if-not-present  # always, if-not-present, or never
    working_dir: /var/lib/eyvan/workload
    privileged: true             # required for TPU workloads
    # Add typed bind mounts only for paths created on every VM host.
```

`image` must contain a compatible JAX and EasyDeL installation as well as
`/bin/sh`. Eyvan bypasses the image's own entrypoint and launches each command
through `/bin/sh -c`, preserving the image's declared environment, including
its `PATH`. It always uses host networking for native OCI workloads;
there is no YAML switch to change that networking mode. `pull_policy` controls
whether Docker pulls the image as part of the transient workload unit. Submission
stays nonblocking while that unit owns and exposes pull progress through normal
status and journal observation. The VM service account needs the appropriate
registry-read permission for private images. For Artifact
Registry, configure Docker's credential helper on every host (for example,
`gcloud auth configure-docker us-east5-docker.pkg.dev --quiet`) in
`runtime.init_commands` or the VM image; IAM alone does not configure the
Docker client.

`runtime.init_commands` must leave a working Docker daemon available on every
host. Eyvan runs `docker version` after those commands and fails capacity
bootstrap when the engine is unavailable. The host workload directory is
automatically mounted read-write at `/var/lib/eyvan/workload`; Eyvan's
multi-host support directory and every staged runtime artifact are
automatically mounted read-only at the same absolute path inside the container.
Configured mounts are additional typed bind mounts and must not overlap those
automatic destinations.

TPU workload gangs are rejected unless `runtime.container.privileged: true` is
set. This makes the OCI path suitable only for mutually trusted fleet code; it
does not create a multi-tenant security boundary. The native OCI path has been
validated from zero on one Spot GCP Multislice containing two `v5p-16` workers:
all four TPU VM hosts joined one four-process, 16-device JAX runtime and ran an
EasyDeL Llama loss step. GPU hardware remains unvalidated. Treat each image,
accelerator family, and network policy as its own production qualification.

Once the fleet policy exists, queue work by logical node type. Provider,
region, accelerator shape, purchase class, executor, topology, and the full
worker resource vector are all derived from the fleet; they are not job flags:

```bash
uv run eyvan up fleet.yaml
uv run eyvan run tpu-workers -- python train.py --config train.yaml
```

A successful `eyvan up` saves that fleet as the active local context. When no
context has been saved, `eyvan run TYPE` queries the control plane and uses the
only submit-capable fleet automatically. Use `eyvan use FLEET` to select among
multiple applied fleets, or pass `FLEET/TYPE` for a one-command override.
Organization and project default to `default`; configure them only when the
deployment uses explicit tenant scopes.

If `tpu-workers` has `min_workers: 0`, this command leaves the workload pending,
creates exactly one matching Eyvan-managed capacity cell on demand, and
preserves that queue position within the pool's `max_workers` bound. The command
returns the durable workload immediately; use `eyvan workload status`,
`eyvan workload logs`, or `eyvan workload cancel` with its workload ID. Logical pool submissions retry a lost or
scheduler-preempted execution cell up to three times by default.
`--max-retries 0` disables automatic recovery; a remote program that exits
`FAILED` remains terminal and is never retried automatically.

Quota-backed autoscale discovery establishes which Spot TPU types the project
is allowed to request and the corresponding quota ceilings. It cannot reveal
free hardware. Eyvan first records `REQUESTING` while it sends one atomic
queued-resource request to GCP. It learns real capacity only after GCP accepts
that request: `WAITING_FOR_RESOURCES` means it is in GCP's queue,
`PROVISIONING` means hardware is being assigned, and `ACTIVE` means it is
allocated. A queue-admission denial means no queued resource was created; it
is an IAM or policy problem, not evidence that the zone has no hardware.

Authentication and control-plane location come from `EYVAN_TOKEN` and
`EYVAN_URL`, or from the existing development identity flags. `up` sends the
whole fleet in one idempotent API mutation: supplied pools are created or
updated and pools omitted from a later file are disabled. The controller
materializes `initial_workers` (or `min_workers` when it is omitted) and then
reconciles demand within each pool's `min_workers`/`max_workers` bounds. These
operator-facing fields map to Eyvan's internal cell bounds. The legacy
`initial_cells`, `min_cells`, and `max_cells` spellings remain accepted, but a
file cannot give conflicting values using both spellings. Pool policies also
include `idle_timeout_s`, `queue_timeout_s`, `provisioning_timeout_s`,
`upscaling_speed`, and `launch_batch`. `queue_timeout_s` limits how long a
provider request may remain queued waiting for capacity; omit it or set it to
`null` for no queue deadline. `provisioning_timeout_s` is separate and limits a
worker that has already started provisioning. A worker that stays unready past
that provisioning timeout is drained and replaced. Use `--dry-run` to inspect
the normalized plan without making API calls.

```bash
uv run eyvan up fleet.yaml --dry-run
uv run eyvan down fleet.yaml
```

`down` first disables every server-side pool for the fleet so the autoscaler
cannot recreate capacity, then drains its cells and releases each cell after
its active leases finish. Pass `--yes` for non-interactive automation. Status
does not require the local file after creation: `eyvan status research`
queries the durable server-side aggregate directly. In a terminal it renders a
compact fleet, worker-pool, shared-quota, and attention summary without platform
routing fields. Redirected output remains JSON for automation; pass `--json` to
force the complete server record in a terminal.

### Generated TPU autoscale fleets

`eyvan autoscale generate` creates one cached, valid fleet YAML per GCP zone.
It reads the zone's TPU accelerator catalog and builds one fleet containing the
admitted complete-worker pools for that zone. Generated files live under
`~/.eyvan/autoscale/configs/`; the accompanying index stores only non-secret
generation time, selected family counts, and shared generation-quota
provenance. The generator defaults every TPU pool to Spot/preemptible capacity.
Its operator settings are YAML, never autoscale environment variables.

```bash
# Prompts once, then writes ~/.eyvan/autoscale/defaults.yaml.
uv run eyvan autoscale init

# Create one zone fleet containing the admitted v5p worker types in this zone.
uv run eyvan autoscale generate --zone us-east5-a --tpu-generation v5p

# With no YAML path, choose from the cached configs in a terminal selector.
uv run eyvan up
```

The setup command writes a complete, editable profile. `project` is suggested
from the active `gcloud` configuration only during that first prompt; Eyvan
saves the selected value in YAML and does not use it as runtime configuration.
The setup wizard rejects a `gs://...` bucket URI where a GCP project ID is
required, offers the supported bootstrap-network routes as a selector, shows a
review table, and asks for confirmation before saving.
Eyvan uses the project's default Compute Engine service account automatically
for native workers. It grants that identity access to the per-cell bootstrap
secret when capacity is created; neither the wizard nor generated YAML asks for
an account. The cell-agent route is also platform-owned and is never prompted
for or written into this user profile.

```yaml
# ~/.eyvan/autoscale/defaults.yaml
version: 1
project: my-gcp-project
fleet_prefix: research
bootstrap_egress: public-internet
min_workers: 0
max_workers: 0
```

`fleet_prefix`, `bootstrap_egress`, `min_workers`, and `max_workers` are
prefilled in the setup prompt but remain ordinary YAML fields. Set
`bootstrap_egress` for the actual network: use `public-internet` only when the
VM has an intentional external address. `max_workers: 0` means unbounded
scale-out (with Eyvan's internal safety ceiling), while a positive
`min_workers` keeps that many full TPU workers provisioned. Each generated
fleet copies its GCP project and bootstrap-network selection into its own YAML,
so applying a cached config does not depend on this defaults file remaining
present.

The control-plane deployment owns the default cell-agent route in
`/etc/eyvan/platform.yaml`. The supplied
[`deploy/control-plane.yaml`](deploy/control-plane.yaml) mounts:

```yaml
version: 1
agents:
  default:
    endpoint: http://eyvan-cell-agent.eyvan-system.svc.cluster.local:8090
```

Generated fleets omit `agent_endpoint`; the API resolves and persists that
platform value. An explicit per-pool `agent_endpoint` remains an advanced
override for a separate VPC or project boundary.

A loopback server with development auth enabled defaults this route to
`http://127.0.0.1:8090`, matching `eyvan-agent`'s local port. Remote deployments
must still provide the platform YAML route.

For a team-managed profile, keep the same document in the repository and pass
it explicitly:

```bash
uv run eyvan autoscale generate \
  --defaults infra/eyvan-autoscale.yaml \
  --zone us-east5-a --tpu-generation v5p
```

Omit `--zone` and `--tpu-generation` to discover the dynamic GCP TPU catalog
in every matching quota zone. Eyvan groups selected types by zone, then shows
the matching accelerator-family counts and one shared Spot quota fact for each
generation. `--select FLEET` makes the no-file path safe for non-interactive
automation.

```bash
uv run eyvan up --select research-us-central1-a
```

`eyvan up --quota-based` queries Cloud Quotas and the TPU accelerator catalog,
prints the discovered Spot quota limits and family counts, caches only matching
zone fleets, and applies the selected config. `--preemptible` explicitly
verifies that the selected fleet consists of Spot TPU pools; quota-based
generation is already Spot-only by default. Add `--dry-run` to inspect an
uncached, read-only preview; it does not write defaults or generated fleet
files.

After an interactive apply, `up` prints only the selected fleet summary. It
does not dump API pool records, every cached zone, or skipped accelerator
catalog entries. Use `--json` when those complete machine-readable diagnostics
are explicitly needed; redirected non-interactive output remains JSON for
automation.

```bash
uv run eyvan up --quota-based --preemptible \
  --zone us-east5-a --tpu-generation v5p
```

Quota values are project limits, not a reservation or a claim that physical TPU
capacity is currently free. Eyvan still creates Cloud TPU queued resources and
waits for provider allocation before dispatching work. A quota is shared by
the accelerator types in one generation and zone, so quota-based generation
creates one controller-enforced generation budget rather than copying the full
limit into each pool as an independent `max_workers` reservation. Each TPU
shape consumes its own quota-core cost from that shared budget before Eyvan
creates a queued resource. An unlimited provider quota remains explicit in the
cached provenance and preserves the public `max_workers: 0` spelling.

Each CLI mutation gets a fresh idempotency key so `up`, `down`, then `up` works
as expected. For an ambiguous network retry of the same operation, pass the
same explicit `--idempotency-key` again.

Each `pools` entry describes atomic Eyvan workers. A CPU or GPU
`gcp-compute` worker is one Compute Engine VM. A TPU worker is one complete TPU
worker allocation, even when that allocation contains multiple physical TPU VM
hosts; it is not one worker per TPU VM host. A one-worker workload uses a
singleton cell. A `pods=N` GCP TPU workload uses one indivisible cell
containing `N` worker units, and that cell counts as `N` against pool bounds and
shared quota. An omitted TPU purchase class defaults to Spot; an explicit pool
or fleet `capacity_class` still wins. `min_workers` is the durable floor. A
fleet YAML `max_workers: 0` means unbounded scale-out, capped internally at
10,000 workers; set `max_workers` equal to `min_workers` to pin a pool at its
floor. Legacy `node_types` remains a migration input only when every entry is
already one worker. One agent in generic `gcp` mode can route both
Compute Engine and Cloud TPU cells. Mixed fleets can also set `agent_endpoint`
per pool when projects or VPC boundaries require separate agents.

Known TPU pool names inherit positive per-host CPU, memory, and TPU count from
`eyvan/capacity/data/accelerator_shapes.yaml`; each catalog entry cites the
versioned Google Cloud page that establishes those values. A positive explicit
CPU or memory value is a deliberate schedulable override, while omitted values
keep the catalog value. Both `resources` and an explicitly supplied
`allocatable` vector must advertise exactly the catalog accelerator count.
Unknown TPU shapes and all manually described GPU shapes are intentionally
allowed only with an explicit complete `resources` vector containing positive
CPU, `memory_gib`, and the matching accelerator resource; an `allocatable`
vector, when set, must be complete in the same way.
Quota/accelerator discovery does not report host RAM, so autoscale skips an
uncatalogued discovered shape rather than generating a zero-memory pool. After
upgrading from a build that produced zero-CPU or zero-memory pools, remove or
replace explicit zero resource overrides and reapply with `eyvan up
<fleet.yaml>`; for a quota-generated cached fleet, regenerate with `eyvan
autoscale generate` first or rerun `eyvan up --quota-based --preemptible`.
`eyvan doctor` names any enabled legacy accelerator pools still needing repair.

`v5p-8` is a built-in Compute Engine-managed TPU profile. The operator supplies
the public TPU shape and worker bootstrap identity; Eyvan derives its sourced
per-host CPU and memory, four-chip resource vector, `ct5p-hightpu-4t`, the TPU
image, integrated accelerator metadata, Spot provisioning,
terminate-on-maintenance, and delete-on-preemption:

```yaml
provider:
  type: gcp
  project: my-gcp-project
  region: us-east5
  zone: us-east5-a

executor: eyvan-native
capacity_class: spot

pools:
  v5p-8:
    min_workers: 0
    max_workers: 1
    agent_endpoint: https://eyvan-agent.example.com
    provider_options:
      bootstrap_egress: private-google-access
```

Do not add machine, image, accelerator, or purchase-class flags to the workload
or `TpuConfig`. An explicit `capacity_backend: gcp-tpu` selects the legacy Cloud
TPU path and disables this Compute profile instead of mixing provider options.
For `eyvan-native`, Eyvan renders its worker bootstrap plus the typed
`runtime` block, starts an `eyvan-native-worker` systemd service on the
provisioned VM or every host in the TPU cell, and waits for its authenticated
health API before the cell becomes schedulable. `provider_options.startup_script`
is retained only as a legacy bootstrap hook and cannot be combined with `runtime`.
Set `EYVAN_NATIVE_WORKER_PACKAGE` on the
provisioning agent to a pinned `gs://` wheel while Eyvan is unreleased. For an
internal-only `private-google-access` VM, also set
`EYVAN_NATIVE_WORKER_WHEELHOUSE` to a pinned `gs://` zip containing every
Python dependency wheel; Private Google Access reaches GCS but not PyPI. This
is fleet bootstrap, never a job-level flag. Native pools require an explicit
`bootstrap_egress` route:
`private-google-access` or `cloud-nat` for an internal-only VM, or
`public-internet` only when the VM is deliberately given an external address.
Set `provider_options.service_account` only for an advanced custom identity;
otherwise Eyvan leaves the provider flag unset and GCP uses the project default
Compute Engine service account.

The native worker executes submitted code as the unprivileged
`eyvan-workload` user. `runtime.init_commands` execute during trusted capacity
bootstrap after Eyvan creates its shared worker virtual environment; staged
artifacts are root-owned but readable by the workload account. A legacy
`startup_script` that creates its own virtual environment must still use an
interpreter the workload user can traverse. For example, when bootstrapping
Python with uv as root, install that managed Python beneath the shared Eyvan
runtime path rather than under `/root`:

```bash
uv python install --install-dir /opt/eyvan-native/python 3.11
UV_PYTHON_INSTALL_DIR=/opt/eyvan-native/python \
  uv venv --python 3.11 --seed /opt/eyvan-native/venv
chmod -R a+rX /opt/eyvan-native/python /opt/eyvan-native/venv
```

Eyvan validates this before it starts the native worker, so a root-private
interpreter cannot appear healthy and fail only after a workload is queued.

## Cell agent

A cell agent should run in the GCP project/VPC that owns its Compute Engine and
Cloud TPU capacity:

```bash
docker build -f libs/eyvan/Dockerfile.agent \
  -t eyvan-agent:latest .
```

The standalone agent image includes the Google Cloud CLI required by the
generic `gcp` capacity backend. It installs neither eray nor the optional Ray
adapter.

Each cell receives a random durable Eyvan management UID. The GCP providers
revalidate that UID and the requested resource identity before adopting or
deleting a Compute instance or TPU queued resource, so a predictable resource
name cannot transfer ownership.

```bash
EYVAN_AGENT_SHARED_SECRET=replace-me \
EYVAN_NATIVE_WORKER_ROOT_SECRET=replace-with-a-separate-stable-secret \
EYVAN_FLEET_STATE_URI=gs://fleet-state/eyvan/fleet-state.json \
EYVAN_CAPACITY_BACKEND=gcp \
EYVAN_EXECUTION_BACKEND=eyvan-native \
EYVAN_NATIVE_WORKER_PACKAGE=gs://artifacts/eyvan/eyvan-0.1.0-py3-none-any.whl \
EYVAN_NATIVE_WORKER_WHEELHOUSE=gs://artifacts/eyvan/eyvan-wheelhouse.zip \
  uv run eyvan-agent --port 8090
```

### Kubernetes capacity

One cell is one `batch/v1` Job with `completionMode: Indexed`, `parallelism ==
completions == worker_count`, and `backoffLimit: 0`. A headless Service plus the
pods' `subdomain` gives each worker a stable DNS name, which matters because
`ExecutionDriverResolver` caches drivers on the endpoint fingerprint: pod-IP
churn would thrash that cache and could repoint an executor at a different pod.

```bash
EYVAN_CAPACITY_BACKEND=kubernetes \
EYVAN_KUBERNETES_NAMESPACE=eyvan \
EYVAN_KUBERNETES_WORKER_IMAGE=registry.example/eyvan-native-worker:1.2.3 \
  uv run eyvan-agent --port 8090
```

**Kueue is opt-in.** Set `provider_options.queue_name` on a pool to route its
Jobs through Kueue; without it, kube-scheduler and the cluster autoscaler place
the pods and `provisioning_timeout_s` is the only bound.

**A ClusterQueue Eyvan uses must be exclusively Eyvan's, with preemption
disabled.** Kueue eviction destroys a pod with no checkpoint hook, bypassing
Eyvan's entire `PreemptionPlan` / `CHECKPOINT_AND_STOP` / `grace_period_seconds`
machinery -- a training job loses hours of progress and nothing in Eyvan's ledger
records why. `eyvan doctor` checks this: it reads the ClusterQueue and fails
unless `preemption.withinClusterQueue` and `reclaimWithinCohort` are both
`Never`, and reports any Workload in the queue that lacks the
`eyvan.dev/cell-id` label.

**Cost, stated plainly.** Eyvan's budgets and fair share reason about cells it
asked for, not the cluster's real free capacity, so an Eyvan pool can sit
`WAITING` while the cluster is full of non-Eyvan work. `CapacityBelief` will
*learn* that -- repeated timeouts raise `route_priority` -- but Eyvan will never
*plan* around it.

### One fleet-state URI per agent

`EYVAN_FLEET_STATE_URI` is a per-agent namespace, not a fleet-wide one. Two
agents sharing one share a `FleetCellRecord` namespace, which makes `exclusive()`
a cross-backend lock and lets one backend's agent resurrect another's cell
record. Give each agent its own URI. This cannot be enforced in code -- an agent
cannot see its peers -- so it is a deployment requirement.

Relatedly, one agent endpoint serves exactly one capacity backend *family*.
`gcp`, `gcp-tpu` and `gcp-compute` share a driver and may share an endpoint;
`kubernetes` and `k8s` likewise. Mixing families on one endpoint is rejected when
the fleet config is parsed, because it would otherwise validate cleanly and then
fail forever in the outbox with an opaque `ValueError`.

`eyvan-native` is the default trusted single-tenant backend for Compute Engine
CPU, GPU, and the Compute-managed `v5p-8` TPU profile. It runs the fenced
command in a fresh systemd-owned cgroup on the provisioned VM, persists logs
and status on that VM, and exposes only Eyvan's own native-worker HTTP API.
Each command runs as the unprivileged `eyvan-workload` account with a
user-owned working directory and cache; the worker service, state, and
cell-scoped secret remain root-owned. The provisioning agent derives a
per-cell bearer credential from `EYVAN_NATIVE_WORKER_ROOT_SECRET` and writes it to a cell-owned
Secret Manager version; startup metadata contains only the Secret Manager
resource name, never the credential. The agent grants the selected VM service
account (the GCP default unless explicitly overridden)
`roles/secretmanager.secretAccessor` on that one secret. Its
own identity needs permission to create, version, bind, and delete those
cell-owned secrets. `EYVAN_AGENT_SHARED_SECRET` authenticates the cell-agent
API and can rotate independently without invalidating running worker tokens.
The bootstrap uses `ExitType=cgroup` on systemd 250 or newer. The default
Ubuntu 22.04 TPU image uses a cgroup-observation fallback, so an exited shell
cannot be reported successful while a background workload process remains.
`eyvan-native` deliberately leaves `/tmp` shared because TPU runtimes use the
host-wide libtpu lock. It raises memlock and file-descriptor limits for every
workload service.

An Eyvan-native VM shares one Compute service account and a workload account
across its jobs. It is therefore for mutually trusted code in one security
domain, not an isolation boundary between untrusted organizations or projects.
Use a separate fleet/VPC/service account for a separate trust domain. True
multi-tenant native execution requires workload identity and metadata-service
isolation, which Eyvan does not yet provide.
Keep the worker endpoint private to the VPC: create an ingress firewall rule
that permits the agent subnet or service CIDR to the worker port only, and do
not expose that port publicly. Native worker transport is authenticated bearer
HTTP within that trusted private network; terminate TLS or use an authenticated
tunnel before crossing an untrusted network boundary.

The owner-only local native GCP runtime is the narrow development exception.
For a profile-managed worker with a public IP, Eyvan derives a per-profile
network tag and maintains one deterministic VPC firewall rule for the current
local public IPv4 `/32` and the native worker port. It never uses a
`0.0.0.0/0` source range, does not add this rule to fleet YAML, and keeps
private/VPC-local agents on their private route. Existing untagged workers
must be recreated before the local profile can reach them.

`native-local` remains a development backend. It runs on the cell-agent host,
accepts exactly one replica, and does not provide container, cgroup,
accelerator, memory, or CPU isolation. It is not selected by the fleet default.
`eyvan-native` accepts one complete Compute VM or TPU allocation. A singleton
`v5p-16` cell is one Spot queued resource. A `pods=N` cell is one Spot queued
resource with `node_count=N`, created dynamically by the autoscaler. Eyvan
starts one authenticated worker on every TPU VM host, uses GCP's authoritative
Multislice metadata for JAX discovery, and fences/stops all reachable ranks
before retrying a partial loss. `node_count` remains an internal provider detail
and is never configured in fleet YAML. Multi-host
checkpoint hooks are deliberately rejected until Eyvan has a rank-aware
checkpoint protocol; do not configure checkpoint preemption for that pool yet.
Legacy Cloud TPU queued resources are a native worker target only when the pool
supplies the bootstrap egress route and typed runtime configuration.

Provider readiness is not enough to make a cell schedulable. The agent also
probes the selected execution backend. For `eyvan-native`, a VM that is merely
running remains `REGISTERING` until the worker's authenticated health endpoint
responds.

The controller emits transition-only operator records prefixed
`capacity lifecycle` for `planned`, `provisioning`, `provider_transition`,
`ready`, `preempted`, `unhealthy`, `lost`, `retry`, `replacement`, and
`removed` events. Repeated polls of the same persisted state do not emit
another record. Each record identifies the fleet, pool, cell, provider
location, resource shape, worker count, capacity class, and previous/current
cell and provider states.
Preemption, unhealthy capacity, loss, and replacement are logged at warning
level; the other lifecycle transitions are informational.

Production deployments terminate mTLS in front of the agent and configure a
shared bearer credential as defense in depth. The agent rejects stale lease
generations and fencing tokens before mutating provider or execution state.
Cell mutations and workload submissions are serialized through Eyvan's
fleet-state lease; lease contention is a neutral retry and does not count as a
provider failure. Durable cell tombstones prevent a delayed provision request
from resurrecting drained or released capacity. Durable workload
high-watermarks retire and observe an older execution attempt as terminal
before a newer generation can submit. Cross-cell supersession is sent to the
source cell agent and must finish before dispatch proceeds on the target cell.
Capacity polling defaults to 15 seconds with eight concurrent cell probes;
override it with `EYVAN_CELL_RECONCILE_INTERVAL_S` and
`EYVAN_CELL_RECONCILE_CONCURRENCY`. Storage and provider gcloud calls are
bounded by `EYVAN_GCLOUD_STORAGE_TIMEOUT_S` (default 30 seconds),
`EYVAN_GCLOUD_COMPUTE_TIMEOUT_S` (default 600 seconds), and
`EYVAN_GCLOUD_TPU_TIMEOUT_S` (default 600 seconds). Raise the storage timeout
on a slow link: a fleet-state write that times out is *indeterminate*, so the
agent re-reads the object to learn whether it landed and only reports
`FleetStateUnavailableError` when it demonstrably did not. That is deliberately
not a conflict — nothing raced — because a caller that cannot see the store has
learned nothing and must not conclude anything about the cells it describes.
For the same reason, an agent that fails to answer at all raises
`CellAgentUnavailableError` and leaves the cell record untouched rather than
recording a provider failure the provider never reported.

`cell-drain` stops new placement but does not terminate running workloads. It
fences further provisioning for that cell incarnation; use `cell-delete` only
after every lease has ended. Deletion advances the incarnation before provider
capacity is released.

## Workloads

Command workloads submit an entrypoint and backend-neutral runtime environment.
Callable workloads are cloudpickle artifacts. A loopback development server
with development auth enabled trusts only the conventional `default/default`
scope automatically so the saved local context works without another setting.
Every other scope must be listed in `EYVAN_TRUSTED_CALLABLE_PROJECTS`;
deserialize callables only in a pinned, trusted runtime image.

`eyvan run` is the normal submission path. It accepts a logical node type and
command intent; fleet selection comes from the last successful `eyvan up` or
the only submit-capable fleet:

```bash
uv run eyvan run tpu-workers -- python train.py
```

Every successful submission prints its durable workload ID and the exact
commands to inspect, read logs for, or cancel that job. The workload keeps
running if the submitting shell exits:

```bash
eyvan workload status WORKLOAD_ID
eyvan workload logs WORKLOAD_ID
eyvan workload explain WORKLOAD_ID
eyvan workload process-status WORKLOAD_ID
eyvan workload cancel WORKLOAD_ID
```

`process-status` is for the case `status` cannot distinguish: a job that is
RUNNING but not progressing. It reports each rank's systemd state, main PID, and
cgroup process list, so a healthy step loop is separable from a hung collective.
An unreadable cgroup reports `pids: null` rather than an empty list — "we could
not tell" must never look like "nothing is running". A host that cannot be
reached is reported in place, so one bad rank does not hide the others.

`eyvan doctor` answers the question that comes *before* `explain`: is this
client talking to a working control plane at all, with an identity it accepts
and a fleet it can submit to. It checks the configured URL, control-plane
reachability, whether that replica actually runs the controller or is API-only,
the organization/project scope, and whether a submittable fleet is applied.
Each check reports `PASS`, `WARN`, `FAIL`, or `SKIP` — `SKIP` means the check
could not run and says why, which is deliberately not the same as passing.
`doctor` exits non-zero only on `FAIL`, so it is safe in CI as a preflight.
Terminal runs print a readable summary on stderr; stdout stays JSON.

`explain` answers "why is this still pending". It re-runs the pure scheduler
against current state rather than reporting the verdict cached by the last
scheduling pass, so it reflects the fleet as it is now. The response names the
placement cell when the workload is placeable, and otherwise gives the
top-level `InfeasibilityCode`, the per-cell rejection reason for each cell that
was considered, and any durable autoscale block or queue-admission denial.
`considered_cell_count` is the true number of rejected cells, so a truncated
list is never mistaken for the whole fleet. Terminal runs print a compact
summary on stderr; stdout stays JSON for automation.

When more than one fleet is applied, select one once with `eyvan use FLEET` or
submit explicitly with `eyvan run FLEET/NODE_TYPE -- COMMAND`.

The server atomically resolves the node type and derives its provider, region,
accelerator, capacity class, executor, topology, and resources. It also pins the
workload to that exact pool, so another compatible or cheaper node type cannot
capture the demand. The lower-level `eyvan workload submit` command remains for
advanced resource-shaped workloads and compatibility.

Execution backends report the closed `ExecutionWorkloadStatus` enum. An
`INDETERMINATE` observation means the backend attempted to read a workload but
could not establish a definitive state; it is never a fallback for a missing or
unrecognized value. The controller fails closed on that state and waits for a
definitive observation or operator recovery. Unsupported backend statuses are
rejected at the adapter boundary.

Each dispatch carries the complete resource gang, and each execution backend
must reject resource shapes it cannot enforce. `native-local` accepts only a
single replica. `eyvan-native` coordinates multi-cell gangs through the
authenticated native workers attached to every leased cell. Terminal and preempted
leases atomically settle their estimated reserved cost and accelerator hours
into the project budget before capacity is reusable. When a newer workload
generation supersedes an old attempt, terminal retirement waits up to
`EYVAN_SUPERSEDE_WAIT_TIMEOUT_S` (default 300 seconds), polling at
`EYVAN_SUPERSEDE_POLL_S` (default 1 second). A preemption stop must become
terminal within `EYVAN_PREEMPTION_STOP_GRACE_S` (default 300 seconds) before
its lease is settled.

### Transitional Ray adapter

Ray is not part of Eyvan's authority or base installation. During migration,
cells that still require Ray Jobs can install the `ray` extra and set
`EYVAN_EXECUTION_BACKEND=ray-jobs`. That adapter maps each replica to a Ray
placement-group bundle and honors `EYVAN_GANG_READY_TIMEOUT_S` (default 600
seconds). Capacity lifecycle, fencing, scheduling, budgets, and audit remain
Eyvan-owned, so removing the adapter does not change those contracts.

Preemption is one durable plan, not independent victim commands. All
checkpoints complete before the plan enters its stopping phase; terminal
outcomes and per-victim stop progress survive worker failover. Cancellation is
temporarily rejected while remote stopping is in progress. If an operator has
independently verified that every unresolved victim is terminal, a fleet admin
can recover an otherwise irrecoverable stop failure with an audited command:

```bash
uv run eyvan preemption-force-finalize PREEMPTION_ID \
  --reason "verified all backend workloads terminal"
```

The preemption id is exposed as `preemption_id` on its pending target workload.

The synchronous and asynchronous SDKs expose the same versioned contract:

```python
from eyvan import ClientIdentity, EyvanClient

client = EyvanClient(
    identity=ClientIdentity(token="OIDC access token"),
)
job = client.run(
    "research",
    "tpu-workers",
    "python train.py",
    org_id="acme",
    project_id="research",
)
workloads = client.list_workloads(org_id="acme", project_id="research")
```

### Callable execution

Trusted Python callables have a Ray-like submission surface without importing
Ray or eray. Fleet configuration remains the infrastructure policy: project,
zone, spot/on-demand class, executor, and `min_workers`/`max_workers` do not
belong in `TpuConfig`. The config names the exact TPU worker shape and an
optional bounded capacity-loss retry budget; it does not retry user-code
failures.

Select a persistent context once from the CLI. The context stores the fleet,
organization, project, and control-plane URL, but never credentials:

```bash
uv run eyvan --url https://eyvan.example.com \
  use research --org acme --project research
```

Then decorate ordinary Python functions. No process-level `eyvan.init()` or
artifact bucket is required:

```python
import eyvan

@eyvan.execute(eyvan.TpuConfig(type="v5p-512"))
def train(config):
    return run_training(config)

result = train({"steps": 1000})
job = train.remote({"steps": 1000})
result = job.result()
```

`remote()` immediately prints the same durable job receipt, including the
requested worker shape and atomic-allocation marker, with `status`, `logs`,
and `cancel` commands. The Python handle exposes the matching
`job.status()`, `job.logs()`, and `job.cancel()` operations.

`eyvan context` shows the active context. Explicit `eyvan.init(...)` arguments
and `EYVAN_FLEET`, `EYVAN_ORG`, `EYVAN_PROJECT`, and `EYVAN_URL` remain
available for automation and override the saved context. Credentials still
come from `EYVAN_TOKEN` or the development identity environment. Set
`EYVAN_CONFIG_FILE` to isolate or relocate the context file.

By default, the SDK uploads bound functions and reads results through Eyvan's
authenticated managed-artifact API. The logical `eyvan://` references in a
workload do not reveal the server's storage backend. Operators choose that
backend with `EYVAN_ARTIFACT_STORE_URI`: a local path is suitable for a single
development server, while `gs://bucket/prefix` provides shared production
storage. All control-plane replicas must share one
`EYVAN_ARTIFACT_TOKEN_KEY` of at least 32 bytes, and production dispatch requires an HTTPS
`EYVAN_PUBLIC_URL`. `EYVAN_ARTIFACT_URI` remains an advanced client-side
override for direct local or GCS artifact access.

A decorator submission currently uploads one cloudpickle artifact containing
the callable and its bound arguments only. It does not package the caller's
working directory or arbitrary project files, so application modules and
dependencies must already be available through the fleet runtime, staged
artifacts, or container image. The caller's `os.environ` is not forwarded
automatically. Fleet `runtime.env` values are plaintext, non-secret
configuration merged into every worker; job-specific and Eyvan-owned values
retain the precedence described above. Working-directory packaging is not
implemented yet.

The Kubernetes deployment expects `public-url`, `artifact-store-uri`,
`artifact-token-key`, and `trusted-callable-projects` keys in its existing
`eyvan-secrets` Secret. `trusted-callable-projects` is the comma-separated
`org/project` allowlist. The control-plane image includes the Google Cloud CLI
used when the shared store is a GCS URI.

Callable downloads use expiring scoped tokens. Result uploads use a durable
capability bound to one server-owned workload generation, so long-running jobs
do not lose their result after a fixed wall-clock TTL; retrying the workload
rotates the destination and revokes the stale attempt. Worker transfers retry
bounded transient HTTP failures and honor numeric `Retry-After` values.

Callable projects must be explicitly trusted by the server through
`EYVAN_TRUSTED_CALLABLE_PROJECTS` in production. The only convenience exception
is `default/default` when development auth is enabled and `eyvan-server` is
bound to a literal loopback address. The server defaults to `127.0.0.1`; a
hostname, wildcard, or other network bind never receives implicit trust, even
when `EYVAN_PUBLIC_URL` advertises a loopback URL. Non-default local scopes
remain denied unless listed. The bound function and arguments are stored under
a content digest. Results are separate immutable, integrity-checked artifacts,
not parsed from logs. Gang callables execute on every replica, but only replica
zero publishes the one durable Python result returned by `job.result()`.

Decoration is side-effect free. Calling `train` or `train.remote` creates the
durable workload demand. Therefore a matching pool with `min_workers: 0`
creates its TPU worker allocation on demand and later returns to its configured
minimum after the idle timeout. `TpuConfig` uses three capacity-loss retries by default;
set `max_retries=0` to make a lost Spot cell terminal immediately.

If the selected fleet has no enabled pool matching `TpuConfig.type`, decorator
submission raises `EyvanPlacementError`. The exception exposes the requested
type and `available_accelerator_types`, containing the fleet's unambiguous TPU
types that can accept the requested `pods` count.

`TpuConfig(pods=N)` requests one atomic GCP Multislice containing `N` complete
TPU workers from the pool keyed by accelerator type. For example,
`TpuConfig(type="v5p-16", pods=2)` creates one Spot queued resource containing
two `v5p-16` workers. Pool `min_workers`, `max_workers`, launch planning, and
shared quota all count those two worker units. Warm singleton cells cannot be
joined after provisioning; when necessary, the autoscaler drains idle capacity
and replaces it with the exact physical group while preserving active leases.

PostgreSQL serialization/deadlock conflicts return `503` with `Retry-After`.
SDK mutation methods retry that response up to three times while preserving one
idempotency key; direct HTTP clients should do the same.

Run the package checks with:

```bash
uv run pytest libs/eyvan/tests
uv run ruff check libs/eyvan
uv run lint-imports
```

Real GCP CPU/GPU/TPU provisioning and recovery require a configured gcloud
identity and hardware validation; local tests use fake drivers and do not make
cloud performance or availability claims.