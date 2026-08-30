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

## Per-job additions

`TpuConfig(dependencies=("flax",))` installs packages on the cell before
the callable loads — adding a library should not mean rebuilding an image.
`@execute(..., workspace="src/")` ships a source tree to the worker's
working directory for the callable to import.

## Local components

`eyvan runtime init` stages the local native runtime profile
(`~/.eyvan/gcp-native-runtime.json`: agent shared secret, native worker
root secret, artifact root, wheel URIs); `eyvan runtime deploy` rebuilds
and redeploys the local plane and agent from current source. A plane
restarted without the profile's secrets 401-loops against a running agent
— [troubleshoot](troubleshoot.md) has the table.
