# Part 2 — Getting started

This part takes you from a fresh checkout to a completed callable on the loopback
fleet — no cloud account, no secrets. Eyvan requires Python `>=3.10,<3.14` (the
range declared in `pyproject.toml`).

## 1. Install

```bash
git clone <this repo> && cd eyvan
uv sync --extra dev        # editable install + dev dependencies (pytest, pytest-asyncio)
```

`uv sync` installs the package editable, so the `eyvan`, `eyvan-server`,
`eyvan-agent`, `eyvan-native-worker`, and `eyvan-migrate` console scripts all come
from your working tree.

## 2. Bring up the laptop fleet

```bash
uv run eyvan up
```

`eyvan up` with no argument applies `examples/laptop.yaml`-style local capacity:
one CPU pool served by the `local` backend, on a loopback URL. On a loopback
address, `up` also starts and supervises the control plane for you. Do **not**
start `eyvan-server` first in this path — `up` refuses a plane it cannot prove it
owns, because adopting a process it did not supervise is how two control planes end
up sharing one database.

What lands on disk (all under `~/.eyvan/`):

| path | what it is |
|---|---|
| `control-plane.json` | the loopback plane's durable world (workloads, cells, leases) |
| `autoscale/defaults.yaml` | the profile generated zone fleets derive from |
| `autoscale/configs/` | generated per-zone fleet configs |
| `cell-agent-*` | cell-agent pid/lock/log records |
| `local-development-credential` | the owner-only loopback credential (0600) |

## 3. Check what exists

```bash
uv run eyvan start      # discovers: a local plane, or heads in your project
uv run eyvan doctor     # what is missing, named together
```

`eyvan doctor --server-host 0.0.0.0` previews what a network-bound server would
refuse to start without (`EYVAN_REPOSITORY_BACKEND=postgres`,
`EYVAN_METRICS_TOKEN`, `EYVAN_ARTIFACT_TOKEN_KEY`) — none of which the loopback
path needs. The loopback plane binds `127.0.0.1` and requires no secrets.

## 4. Submit your first callable

```python
from eyvan import CpuConfig, execute, init
from eyvan.records import CapacityClass

init(fleet="laptop")     # or rely on defaults: loopback plane, default fleet

@execute(CpuConfig(type="cpu", capacity_class=CapacityClass.ON_DEMAND))
def hello():
    import platform
    return f"hello from {platform.node()}"

print(hello())           # blocks, narrating progress, then prints your value
```

Run it with `uv run python myscript.py`. You will see the honest narration:

```
queued → provisioning → dispatching → running → (your result)
```

If capacity is refused, you see `blocked | … | the plane is retrying (attempt N)`
with the provider's words. The job is durable: you can Ctrl-C the client and
follow with `uv run eyvan workload status <id>`.

### The same thing on TPUs

Nothing changes but the shape — this is the point of "shape, not pool":

```python
from eyvan import TpuConfig, execute, init
from eyvan.records import CapacityClass

init(fleet="research-us-central1-a")

@execute(TpuConfig(type="v5p-16", pods=2, capacity_class=CapacityClass.SPOT))
def train():
    import jax
    return jax.device_count()
```

This requires a real GCP fleet (Part 6). For local development, the CPU path
exercises the entire machinery — submission, scheduling, leasing, dispatch,
results — against an unisolated local cell.

## 5. Detach, follow, intervene

```python
job = hello.submit()        # EyvanJob — submit without blocking
job.status()                # WorkloadState
job.logs()                  # the flattened, globally-ranked log stream
job.wait(timeout=600, progress=True)
job.result(timeout=3600)    # your value, or EyvanJobError with the reason
job.cancel()                # durable cancel
```

From the shell, the same lifecycle:

```bash
uv run eyvan workload list
uv run eyvan workload status <id>
uv run eyvan workload logs <id>
uv run eyvan workload explain <id>    # placement truth: placeable, candidates, delivery
uv run eyvan workload cancel <id>
uv run eyvan cells                    # every cell and its stage
```

## 6. Run the plane and agent by hand (optional)

When you use accelerators, the plane and the agent run as separate processes:

```bash
uv run eyvan-server --log-level info            # the plane
uv run eyvan-agent --port 8090 --log-level info # a cell agent
```

The agent requires `EYVAN_AGENT_SHARED_SECRET` (and, for native workers,
`EYVAN_NATIVE_WORKER_ROOT_SECRET`). For the profiled local GCP setup both live in
`~/.eyvan/gcp-native-runtime.json` — a plane restarted **without** that profile's
secrets 401-loops against a still-running agent (see Part 14, Troubleshooting).

## 7. Verify the dashboard (optional)

```bash
npm --prefix dashboard ci
npm --prefix dashboard test
npm --prefix dashboard run build
```

Then see Part 13 for what the console shows.

## 8. Run the test suite

```bash
uv run pytest tests -q           # full suite (~7 min; host + disposable Postgres)
uv run ruff check eyvan tests    # lint (line 121, py3.10)
```

## Where you are

You have submitted a callable, watched it move through durable states, and gotten
the return value back. Next: Part 3 dissects the SDK you just used, and Part 6
shows how to replace the laptop fleet with real GCP zone fleets.
