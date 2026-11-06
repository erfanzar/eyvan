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
`major.minor` is refused at submission.

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

### GPU containers

A container that requests a GPU (`gpu > 0` per replica) is launched with
`--gpus`, `--ulimit memlock=-1:-1` and, unless the container block says
otherwise, a private `/dev/shm` of 16 GiB (`--shm-size 16g`). The host's IPC
namespace is shared only with `ipc: host`. Docker's own defaults are a 64 MiB `/dev/shm` and
dockerd's memlock limit; the first makes NCCL's shared-memory transport and
PyTorch DataLoader workers die with a bus error, and the transient unit's
`LimitMEMLOCK=infinity` reaches only the `docker` client, not the container.
Two container fields change the IPC choice:

| Field | Values | Effect |
|---|---|---|
| `ipc` | `host`, `private`, unset | Unset: a private namespace; GPU workloads with no `shm_size` get `--shm-size 16g`. Set: passed as `--ipc` for every workload. |
| `shm_size` | positive integer with optional `b`/`k`/`m`/`g`, e.g. `16g` | A private `/dev/shm` of that size. Refused beside `ipc: host`, which ignores it. |

### Container network and lockdown

By default a container shares the host's network, so every port the workload
opens, and every port opened by anything it starts (a cluster runtime's own
head and object store, a debugger, a dashboard), is open on the host's
addresses. For a workload that serves something, take it off the host network
and publish only what it serves:

```yaml
runtime:
  container:
    image: registry.example/server@sha256:...
    network: bridge
    ports:
      - {container_port: 8000, host_port: 8003}   # host_ip defaults to 127.0.0.1
    user: "997:997"
    read_only: true
    tmpfs: [{target: /tmp, size: 16g}]
    drop_capabilities: true
    init: true
    shm_size: 16g
```

| Field | Values | Effect |
|---|---|---|
| `network` | `host`, `bridge`, `none`, unset | Unset is `host`, as before. `bridge` gives the container its own network namespace; `none` has no network. |
| `ports` | `container_port`, `host_port`, `host_ip` (default `127.0.0.1`), `protocol` (`tcp`/`udp`) | Published with `--publish`; only with `network: bridge`. A port reaches the network only when its `host_ip` says so. |
| `user` | numeric `UID` or `UID:GID` | Runs the workload as that user instead of the image's. Numeric only: a name would resolve inside the image, not on the host whose mounts it writes. Make the cell's workload directory (the container's `HOME`) writable by it. |
| `read_only` | boolean | The image's root filesystem is read-only; pair it with `tmpfs`. |
| `tmpfs` | `target`, optional `size` | A writable in-memory filesystem (`rw,nosuid,nodev`). |
| `drop_capabilities` | boolean | `--cap-drop ALL --security-opt no-new-privileges`. Refused beside `privileged`. |
| `init` | boolean | `--init`: a minimal PID 1 that forwards signals and reaps orphans. |

Unset fields are left out of the serialized block, so a block that uses none
of them reads the same to a worker that predates them.

One container per published host port: two cells publishing the same
`host_port` on one host cannot both run. Give each cell's worker its own
runtime file with its own port; the slicing layout below already has one
worker per cell.

### Slicing one host's GPUs between cells

One machine can carry several cells: run one native worker per slice, each with
its own port (`worker_port`) and `--state-dir`, and give each the host GPUs it
owns with `--gpu-devices` (default: `EYVAN_NATIVE_WORKER_GPU_DEVICES`; empty or
unset means unsliced). For example, an eight-GPU host cut 1/1/2/4:

```bash
eyvan-native-worker --systemd-transient --port 8101 --state-dir /var/lib/eyvan/cell-a --gpu-devices 0
eyvan-native-worker --systemd-transient --port 8102 --state-dir /var/lib/eyvan/cell-b --gpu-devices 1
eyvan-native-worker --systemd-transient --port 8103 --state-dir /var/lib/eyvan/cell-c --gpu-devices 2,3
eyvan-native-worker --systemd-transient --port 8104 --state-dir /var/lib/eyvan/cell-d --gpu-devices 4,5,6,7
```

(Other flags as usual.) Indices are the `N` of `/dev/nvidiaN`, which equals
`nvidia-smi`'s index wherever the driver probed in PCI bus order;
`nvidia-smi -q` prints each GPU's `Minor Number` if in doubt. The list is
sorted and must be non-empty, non-negative and duplicate-free. Nothing checks
that two workers on one host name disjoint lists; that is the operator's
layout. A four-GPU host split in two is `--gpu-devices 0,1` and `--gpu-devices 2,3`.

When the pool's `accelerator_type` names a catalog row, set the pool's
`gpu_devices_per_host` (`manual` and `onprem-ssh`) to the slice size, so ranks
are planned on the slice's GPUs rather than the whole host's (readiness reports
the mismatch until it is set). A slice is held to its own devices in readiness;
other GPUs on the host do not affect it.

What the list does:

- **Container cells** run GPU workloads with `--gpus '"device=4,5,6,7"'`
  instead of `--gpus all`, and drop `NVIDIA_VISIBLE_DEVICES` from the
  container environment so the device request is the only authority. They
  also name the slice's device nodes and `/dev/nvidiactl` and
  `/dev/nvidia-uvm` with `--device`: under Docker's systemd cgroup driver, a
  later `systemctl daemon-reload` on the host rewrites the container's device
  policy from what Docker told systemd, which leaves out devices that
  `--gpus` added, and the running workload loses its GPUs ("Failed to
  initialize NVML: Unknown Error"). Devices named with `--device` survive.
- **Native cells** run every unit with `DevicePolicy=closed` and `DeviceAllow`
  for `/dev/nvidiactl`, `/dev/nvidia-modeset`, `char-nvidia-uvm` and the listed
  `/dev/nvidiaN`. `closed` still permits the standard pseudo devices
  (`/dev/null`, `/dev/zero`, `/dev/random`, …) but denies everything else,
  including FUSE and InfiniBand verbs; a sliced cell that needs RDMA is not
  supported yet. Diagnostic `exec` units beside a workload are confined the
  same way.
- **Both** number the slice `0..n-1` for the workload and set
  `CUDA_DEVICE_ORDER=PCI_BUS_ID` and `CUDA_VISIBLE_DEVICES=0,…,n-1` below the
  job's environment. A job (or a GPU-coordinated gang rank) may narrow that to
  a subset of `0..n-1`; any other `CUDA_VISIBLE_DEVICES` or
  `NVIDIA_VISIBLE_DEVICES` — host indices, `all`, a UUID — is refused at
  submission, and so is either name in `--runtime-env-file` beside a list.
- **GPU-coordinated gangs** need no translation: `plan_gpu_devices` already
  numbers a cell's GPUs `0..n-1`, so its split is the slice's split, provided
  the catalog counts the slice's GPUs rather than the machine's (a plan for
  more GPUs than the slice has is refused).

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

`TpuConfig(dependencies=("flax",))` installs packages before the callable loads.
The job gets its own environment, built from the interpreter running the
wrapper and inheriting that interpreter's packages.

For native venv workers, this includes `/opt/eyvan-native/venv` (Eyvan, JAX,
libtpu) and the host's system packages. Environments are cached per requirement
set and interpreter in `/var/lib/eyvan/runtime-envs`, which the bootstrap
creates for the workload user. A second job or rank using the same interpreter
can reuse the install. The cache identity includes the format version, resolved
interpreter path, Python version, and inherited parent site-packages paths.
Older cache entries remain untouched for jobs that may still use them.

In OCI mode, the environment inherits the image's Python packages, not the
host venv. Containers are removed after execution; cross-job cache reuse needs
an explicit persistent mount. The native host cache is not mounted
automatically.

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
the nodes appear. Install the `acl` package (it provides `setfacl`) with the
host's package manager, then:

```bash
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
