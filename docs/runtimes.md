# Runtimes and images

How a cell gets its Python, your libraries, and — optionally — your Docker
image. Grounded companion to [operate](operate.md); installation
prerequisites live in [install](install.md).

## Two provisioning models

| | venv model | OCI model |
|---|---|---|
| who provides Python | the fleet, via `runtime.python` | your image |
| who provides libraries | `runtime.init_commands` (and per-job `dependencies`) | the image (`runtime.artifacts` adds code objects) |
| who provides the container | none — native venv at `/opt/eyvan-native/venv` | `runtime.container.image` |
| generated fleets | default; driven by the [defaults profile](#the-defaults-profile) | yes, when `runtime_container_image` is set |
| typical use | fast iteration, `uv pip install` per fleet | pinned, reproducible training runtime |

The interpreter contract is checked **before provisioning** when
`runtime.python` is declared: a callable pickled on a different
`major.minor` is refused at submission, not on the accelerator — a live
multislice run paid for that lesson once (see [proofs](proofs.md)).

Both models describe hosts Eyvan bootstraps (GCP). On `onprem-ssh` and
`manual` pools Eyvan installs nothing, so fleet validation refuses
`runtime.container`, `runtime.init_commands` and `runtime.artifacts` there;
configure the worker's container runtime file on the host instead
([setting up an on-prem SSH host](reference/capacity-and-providers.md#setting-up-an-on-prem-ssh-host)).
`runtime.env` and `runtime.python` work on every provider.

## Two allocation modes

Independent of the runtime: **how a job's `pods` workers are allocated**.

- **elastic (default)** — `pods` is a worker count, satisfied from any
  combination of same-type cells: warm slices are packed first (biggest
  first, so the fewest hardware worlds) and only the deficit is
  provisioned. Resizing `pods` 2→4 keeps the two warm slices and asks for
  two more. The job runs as one lease group, one hardware world per cell —
  framework-level parallelism across worlds.
- **atomic** — `TpuConfig(..., atomic=True)`: one exact-shape provider
  resource, one hardware world spanning every pod. Required when the job's
  math needs the whole allocation in one JAX world (single-mesh training,
  an all-reduce across every chip). Resizing rebuilds from zero — a
  provider topology cannot grow — and the plane drains now-unusable
  smaller leftovers instead of billing them to the idle timeout.

All-or-nothing holds in both: nothing is leased unless the full worker
count is covered, so a deficit that never grants holds no warm cell
hostage.

## The defaults profile

`~/.eyvan/autoscale/defaults.yaml` is the one place an operator pins what
every generated zone fleet derives:

```yaml
version: 1
project: my-gcp-project
fleet_prefix: research
bootstrap_egress: public-internet
runtime_python: '3.13'
runtime_init_commands:
- uv pip install --python /opt/eyvan-native/venv/bin/python --prerelease=allow 'easydel[tpu,profile,torch]'
# runtime_container_image: us-east5-docker.pkg.dev/my-gcp-project/eyvan/runtime@sha256:...
min_workers: 0
max_workers: 0
```

- `runtime_python` — the interpreter every worker runs; arms the
  pre-provisioning mismatch refusal.
- `runtime_init_commands` — run on every host after the worker environment
  exists and before the worker unit starts. The window to install what the
  interpreter needs.
- `runtime_container_image` — pin one blessed OCI image (prefer an
  immutable digest) and every generated fleet declares it as its container.
  The generator states the image but never the host preparation: docker
  installation and `gcloud auth configure-docker <registry-host>` belong in
  `runtime_init_commands`, where the one command that must match the
  image's registry stays visible instead of derived.

`eyvan autoscale init` edits this profile interactively; the file is
owner-only (`0600`) and machine round-trippable.

The native worker host requires **Python 3.11 through 3.13**, including when
workloads run in an OCI container. Declare `runtime_python` in the defaults
profile, or `runtime.python` in a hand-written fleet, when the VM image does
not supply a supported interpreter. Built-in Ubuntu 22 TPU images can expose
Python 3.10 as `python3`; leaving the declaration unset can therefore fail
bootstrap before any workload starts. `examples/fleet.yaml` explicitly selects
`runtime.python: "3.11"`; node-level declarations can override it.

An explicit declaration builds a managed interpreter environment rather than
inheriting the image's system-site packages. Install matching host accelerator
libraries through `runtime_init_commands`, or supply workload dependencies in
your OCI image. Undeclared custom-image behavior and global generator defaults
remain unchanged; do not assume Eyvan silently upgrades an image's Python.

## A zone fleet from scratch

```bash
eyvan autoscale init                     # once: the profile above
eyvan autoscale generate --zone us-central1-a   # caches configs/<prefix>-<zone>.yaml from live quota
eyvan up --select research-us-central1-a --preemptible   # apply
```

With `fleet_prefix: a1` the fleet is `a1-us-central1-a` — your "A1 up".
Regenerate after changing defaults; generated files are derived, so never
hand-edit them (a regeneration will not preserve it). Teardown drains every
cell the fleet manages:

```bash
eyvan down ~/.eyvan/autoscale/configs/research-us-central1-a.yaml -y
```

## Customizing the Docker image (OCI model)

The complete worked shape is
[examples/easydel-v5p16.yaml](../examples/easydel-v5p16.yaml):

1. Build the image with JAX/EasyDeL baked in; push to Artifact Registry;
   reference it by **immutable digest**.
2. Declare it per pool (or once, via `runtime_container_image`):
   `image`, `pull_policy: if-not-present`,
   `working_dir: /var/lib/eyvan/workload`, `privileged: true` — native OCI
   TPU jobs reject launch unprivileged.
3. Make hosts container-ready in `init_commands` (docker installed,
   `gcloud auth configure-docker` for the exact registry host).
4. Ship application code as `runtime.artifacts` (for example
   `gs://…/training-code/` → `/opt/eyvan-runtime/training`) with
   `env: PYTHONPATH` pointing at it — the image build does not implicitly
   publish application code; upload the object before `eyvan up`.
5. `bootstrap_egress: public-internet` for the docker install and pull, or
   a prebaked engine plus wheelhouse with `private-google-access`.

### Attached TPUs (existing hardware)

A TPU Eyvan did not create gets its image at attach time:

```bash
eyvan gcp connect --ssh you@HOST --python 3.13 \
  --container-image us-east5-docker.pkg.dev/PROJECT/REPO/runtime@sha256:... \
  --init-command "gcloud auth configure-docker us-east5-docker.pkg.dev --quiet"
```

The attach path states the same container block a generated fleet does
(`privileged: true`, `pull_policy: if-not-present`,
`working_dir: /var/lib/eyvan/workload`); without `privileged` every TPU job
would be refused at launch. Docker must already run on every host (Cloud TPU
VM images ship it). To change the image later, re-run `connect`; the
dashboard's fleet init-script editor refuses attached TPUs because Eyvan never
creates their hosts.

### Per-job packages and workspaces inside an image

`eyvan run --with PKG` and `--workspace DIR` also work in a container whose
image has no Eyvan installed. Their wrappers are staged, standard-library
only, into the read-only bridge mount
(`/opt/eyvan-native/oci-callable-runner`) beside the callable runner, and the
container executor rewrites `python -m eyvan.worker.runtime.<wrapper>` to
those files. The image still needs a Python 3.11+ on `PATH` as `python`, and
`--with` needs `pip` (or `uv`) in that interpreter.

## Per-job additions

`TpuConfig(dependencies=("flax",))` installs packages on the cell before
the callable loads — adding a library should not mean rebuilding an image.
The job gets its own environment, built from the worker's interpreter and
layered over the worker's environment (`/opt/eyvan-native/venv`: Eyvan, JAX,
libtpu) and the host's system packages, so a package it pins wins and
everything else resolves to what the cell already has. Environments are cached
per requirement set and interpreter in `/var/lib/eyvan/runtime-envs`, which the
bootstrap creates for the workload user, so a second job or rank using the same
interpreter reuses the install. The cache identity includes the format version,
resolved interpreter path, Python version, and inherited parent site-packages
paths. Older cache entries remain untouched for jobs that may still use them.
`@execute(..., workspace="src/")` ships a source tree to the worker's
working directory for the callable to import.

## GPU hosts

A cell is GPU-aware when its pool's `accelerator_type` is a catalog GPU row:
`a3-highgpu-8g`, `a2-highgpu-1g` and `g2-standard-48` on GCP, or
`hgx-h200-8g` for an operator-owned 8x H200 (141 GB, SXM) node under
`onprem-ssh` or `manual`. The driver then publishes `cell_accelerator`, which
does two things:

- **Readiness holds the host to the count.** Before a GPU cell becomes
  schedulable the agent asks its native worker
  (`GET /api/v1/accelerators?expected_gpu_devices=N`) to look at `/dev`. The
  cell stays `EXECUTION_CONNECTING`, with the reason as its detail, while:
  - the NVIDIA driver is loaded but exposes no `/dev/nvidiaN` (a GPU the
    driver cannot drive),
  - no NVIDIA node exists at all (driver not loaded),
  - fewer than `N` GPU nodes exist (a GPU fell off the bus or is unbound),
  - or, on the native (non-container) path, the workload user cannot open a
    node read-write.

  A worker too old to answer is reported not ready rather than assumed fine.
  TPU, CPU and uncatalogued cells are not asked. The probe reads device nodes
  only: it does not catch Xid or ECC faults, and it does not exercise the
  container path's NVIDIA toolkit (that needs an image the worker does not own,
  so a container cell's first workload is its toolkit check).
- **Multi-cell gangs get per-rank devices** (`CUDA_VISIBLE_DEVICES` and
  friends), which needs every cell to report a catalog GPU shape.

An on-prem H200 pool still states its own complete `resources` vector (CPU,
memory, `gpu: 8`); the catalog does not know the host's CPU or memory.

No GPU row is hardware-qualified: no Eyvan workload has run on an H200.

### NVIDIA device access (native path, operator-owned hosts)

Native workloads run as `eyvan-workload`, which must open `/dev/nvidiactl`,
`/dev/nvidia-uvm` and every `/dev/nvidiaN` read-write. The GCP bootstrap grants
this with `setfacl`, which a reboot or driver reload undoes; on a host Eyvan
does not create, install a udev rule instead so the grant is reapplied each time
the nodes appear:

```bash
sudo apt-get install -y acl
sudo tee /etc/udev/rules.d/70-eyvan-nvidia.rules >/dev/null <<'RULE'
KERNEL=="nvidia[0-9]*|nvidiactl|nvidia-uvm|nvidia-uvm-tools|nvidia-modeset", \
  RUN+="/usr/bin/setfacl -m u:eyvan-workload:rw /dev/%k"
RULE
sudo udevadm control --reload && sudo udevadm trigger
```

Some driver installs create the nodes with `nvidia-modprobe` rather than
through udev, and then the rule never fires; reboot once and check
`getfacl /dev/nvidia0`. If the grant is missing, run the same `setfacl` from a
oneshot systemd unit ordered `After=nvidia-persistenced.service` instead.

Check with `ls -l /dev/nvidia*` first: many Ubuntu driver packages already
create the nodes mode `0666`, in which case nothing is needed. Readiness names the nodes
it cannot open if the grant is missing. The container path is unaffected:
Docker opens the devices as root.

## Local components

`eyvan runtime init` stages the local native runtime profile
(`~/.eyvan/gcp-native-runtime.json`: agent shared secret, native worker
root secret, artifact root, wheel URIs); `eyvan runtime deploy` rebuilds
and redeploys the local plane and agent from current source. A plane
restarted without the profile's secrets 401-loops against a running agent
— [troubleshoot](troubleshoot.md) has the table.
