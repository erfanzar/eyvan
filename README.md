# Eyvan (ایوان)

Eyvan is a capacity plane for research compute. You state the shape of work you
need — `2 × v5p-16, spot, this fleet` — as a signed Python callable, and the
plane acquires the machines, boots them, dispatches your callable to them, and
hands you the result. Every claim it makes about capacity is grounded in
recorded receipts, not vibes: what was requested, what a provider accepted or
refused, and how long the handover actually took.

## The sixty-second version

```bash
uv sync                                  # workspace install
eyvan start                              # find or point at a control plane
eyvan doctor                             # what is missing, named together
```

```python
from eyvan import TpuConfig, execute, init
from eyvan.records import CapacityClass

init(fleet="laptop")                     # or a GCP zone fleet


@execute(TpuConfig(type="v5p-16", pods=2, capacity_class=CapacityClass.SPOT))
def train():
    import jax

    return jax.device_count()
```

Calling `train()` submits the callable and waits, narrating progress
honestly: `queued`, `provisioning`, `blocked … the plane is retrying
(attempt N)`, `dispatching`, then your result. The job is durable — it keeps
running if your client exits (`eyvan workload status <id>` to follow).

## The ideas everything else follows from

1. **You state the shape, not the pool.** Workloads name an accelerator type,
   a worker count, and a capacity class; the scheduler and the capacity
   planner decide where that can exist.
2. **Receipts or it did not happen.** Capacity claims cite recorded runs;
   [`eyvan proof`](docs/proofs.md) turns a pass criterion into an exit code,
   including the verdict that a run *did not exercise the behaviour at all*.
3. **Failure narrates itself.** A refused capacity attempt renders as
   `blocked | … | the plane is retrying (attempt N)` with the provider's own
   diagnostic attached — never a bare `FAILED`.
4. **Nothing that decides names a provider.** Scheduler, planner, and the
   durable model are provider-agnostic by test; clouds plug in through a
   driver + capabilities contract ([providers](docs/providers.md)).
5. **Durable state survives the plane.** Workloads, cells, fleets, and leases
   live in the plane's durable store; a restarted `eyvan-server` reconciles
   all of it ([operate](docs/operate.md)).

## Concepts

| term    | meaning                                                            |
|---------|--------------------------------------------------------------------|
| plane   | the control plane (`eyvan-server`) — scheduler, planner, state     |
| agent   | a cell agent (`eyvan-agent`) owning machines on one backend        |
| fleet   | a named set of capacity pools you can submit into                  |
| pool    | one (backend, accelerator, capacity class, zone) combination       |
| cell    | one acquired allocation (e.g. one spot queued resource)            |
| workload| your callable + its requested shape, moving through durable states |

## Where it runs

- **Laptop:** `eyvan up` on the loopback path, a `laptop-cpu` fleet, no cloud.
- **GCP:** generated per-zone fleets (`research-<zone>`) driving TPU queued
  resources, spot or on-demand, under `~/.eyvan/autoscale/configs/`.
- Other clouds exist as capacity providers; see [providers](docs/providers.md).

## Operating it

Day-to-day is three verbs — [`eyvan up`](docs/install.md),
[`eyvan workload …`](docs/operate.md), and [`eyvan capacity …`](docs/operate.md)
— plus [troubleshooting](docs/troubleshoot.md) when a provider says no.
The [security model](docs/security.md), the
[`EYVAN_*` environment surface](docs/env-surface.md), and
[artifact safety](docs/artifact-safety.md) close the picture.

## What is proven

See [proofs](docs/proofs.md). Known rough edges are listed there too —
including the quarantined-lease retry loop after a crash mid-dispatch, and
that a bare `eyvan-server` restart needs the runtime profile's secrets or it
401-loops against a still-running agent.
