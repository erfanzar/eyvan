# Eyvan (ایوان)

A fleet control plane for accelerator work. You declare the capacity you want,
submit workloads against it, and Eyvan decides what runs where — admitting on
quota and budget, placing gangs atomically, leasing cells with fencing tokens,
and surviving preemption and its own restart.

It is not a distributed runtime. Eyvan does not own your process model, your
collectives, or your framework. It owns *capacity, admission, placement, and
lifecycle*, and hands a fenced lease to whatever you actually run.

```bash
eyvan head up fleet.yaml          # provision a control plane on GCP
eyvan up fleet.yaml               # reconcile declared capacity
eyvan run --tpu v5p-8 -- python train.py
eyvan status
```

`eyvan run` streams the job's output and exits with its outcome. Ctrl-C
detaches without cancelling; `--no-wait` prints the submission document and
returns instead.

## Status, as of this commit

Honest about the line between what runs and what is proven, because the
difference costs money:

- **Works, verified against real GCP:** `eyvan head up` provisions a control
  plane end to end — VM, VPC-only firewall, generated secrets, PostgreSQL,
  migrations, systemd — and `eyvan head down` removes it. It installs the wheel
  it is running; no package index is involved.
- **Runs work, verified against real GCP:** a `pods=2` callable submitted with
  the `@execute` decorator acquired one Cloud TPU Multislice queued resource in
  `us-east5-a`, provisioned two spot `v5p-16` slices, dispatched to all four
  ranks, and printed `jax.device_count() == 16`. Nothing was configured by hand:
  the head published its own worker wheel, named the fleet-state document its
  agents share, and provisioned JAX through the fleet's declarative runtime.
- **Reachable without an identity provider:** a head with no `--oidc-issuer`
  authenticates with a generated bootstrap token, and on the head itself the CLI
  and the Python SDK discover the plane's address and credential from the node,
  so `eyvan up` and `@execute` need no environment at all.
- **Not published:** Eyvan is not on PyPI. Install from source, or let
  `eyvan head up` carry its own wheel.
- **Partly qualified:** the execution path above has a receipt; the numbered
  proofs -- structured multi-host logs, in-job submission, per-attempt secrets,
  spot-reclamation accounting, and the soak -- do not. See
  [docs/proofs.md](docs/proofs.md), which keeps those apart on purpose.
- **Two things a first run still needs to know:** this command terminates no
  TLS, so a callable's artifact token would travel in clear text and is refused
  unless you pass `--allow-insecure-artifact-http` (defensible only because
  ingress is VPC-only); and a serialized callable cannot load on an interpreter
  other than the one that pickled it, so a pool that declares `runtime.python`
  will refuse a mismatch at submission rather than after the accelerator is
  built.

## Where it runs

| Backend | What it is | Qualification |
|---|---|---|
| `local` | processes on this machine | host-tested; used by `--local-daemon` |
| `manual` | cells you register yourself | host-tested |
| `gcp`, `gcp-compute`, `gcp-tpu` | Compute Engine VMs and Cloud TPU, provisioned and reclaimed by Eyvan | one live TPU fleet; see [docs/proofs.md](docs/proofs.md) |
| `k8s`, `kubernetes` | any conformant cluster, via Jobs and optionally Kueue | one GKE run; not re-qualified for the current candidate |
| `aws`, `aws-ec2` | EC2 instances, on-demand or spot, fenced by instance tags | passes the conformance suite against a fake EC2; **no run against a real AWS account** |
| `azure`, `azure-vm` | Azure virtual machines, on-demand or Spot, fenced by resource tags | passes the conformance suite against a fake ARM; **no run against a real subscription** |
| `onprem-ssh` | hosts you already own, reached over SSH | code-complete, **no run against a real SSH host** |
| your own | the `eyvan.capacity_providers` entry point, with a conformance suite | ABI published; no external provider written yet |

Breadth and evidence are different axes and this table keeps them apart on
purpose. "Runs on Kubernetes" is a property of the code; "was proven on GKE" is a
property of one receipt. [docs/proofs.md](docs/proofs.md) lists which is which,
and what is still unproven.

## The ideas everything else follows from

**A lease is a fencing token, not a lock.** Every mutation carries the
generation it was planned against. A worker that wakes from a network partition
holding a stale generation cannot write, so a partitioned agent cannot corrupt a
reassigned cell — it does not need to be reachable to be made safe.

**The scheduler is a pure function.** `Scheduler.schedule` takes a snapshot and
returns decisions. It performs no I/O, so it is exhaustively testable, and every
decision it makes is revalidated inside a transaction before it takes effect.
Placement that was legal when planned and illegal when applied is discarded.

**Unknown is a state, not a default.** An unreadable cluster is neither full nor
empty; an unreachable health probe is neither healthy nor unhealthy; an
unfinished stage is not a cache hit. Eyvan reports the third answer instead of
rounding it into one of the other two, because the rounding is always in the
direction that loses work or spends money.

**Never hold a lock across I/O.** No database transaction is held open across a
provider call, an agent call, or an object-store read. The alternative is a
control plane whose tail latency is a cloud API's tail latency.

**A gang is placed atomically or not at all.** Partial placement of a
multi-host job is worse than no placement: it bills for capacity that cannot
make progress.

## Concepts

- **Pool** — declared capacity of one shape, with bounds, a scaling mode, and a
  fallback order.
- **Cell** — one schedulable unit: a VM, a TPU slice, a Kubernetes node, an SSH
  host.
- **Lease** — a fenced claim on a cell by one attempt of one workload.
- **Attempt** — one execution of a workload. Preemption ends an attempt, not the
  workload.
- **Fleet state** — the durable record every agent reconciles against, addressed
  by `EYVAN_FLEET_STATE_URI`.

## Submitting work

```python
from eyvan import execute, TpuConfig

@execute(TpuConfig(type="v5p-16", pods=2))
def train():
    import jax

    print(jax.device_count())

train()
```

No connect call, because there is nothing to say yet: a bare `@execute` resolves
a plane the first time it submits, and on a laptop or on a head that resolution
is already right. Name one when it is not, or when you want to know which one
you got:

```python
import eyvan

eyvan.init()                                       # the default: this machine's plane
eyvan.init("http://10.0.0.5:8080")                 # a head, positionally
eyvan.init(fleet="research-us-east5-a")            # pin the fleet, keep the plane

print(eyvan.init().base_url)                       # which plane would a bare @execute use
```

Resolution order is the argument, then `EYVAN_URL`, then the context saved by
`eyvan use`, then this node's own plane if it serves one, then loopback. The
returned context carries the `base_url` that won, so a machine with both a
laptop plane and a head never has to guess which it is submitting to.

From the CLI:

```bash
eyvan run --tpu v5p-8 -- python train.py     # streams; exits with the job's outcome
eyvan run --tpu v5p-8 --pods 2 -- python train.py
eyvan workload list
eyvan workload logs WORKLOAD_ID --follow     # reattach to a running job
```

Batch, arrays, and small DAGs are normal durable workloads with bounded fan-out
and backpressure, not a second execution system.

## What happens when you submit

1. **Admission** — quota, budget, and policy are checked. Refusal here is cheap;
   refusal after provisioning is not.
2. **Placement** — the pure scheduler proposes a cell, or a whole gang at once.
3. **Revalidation** — the proposal is re-checked inside a transaction against
   the state that actually exists, and discarded if it no longer holds.
4. **Lease** — a fenced claim is written, and an outbox entry with it.
5. **Dispatch** — an agent picks up the lease and starts the attempt, carrying
   the generation it must present on every later write.
6. **Observation** — status, logs, and progress stream back. Preemption starts a
   new attempt from the last checkpoint.

## Operating it

```bash
eyvan doctor                 # what is misconfigured, before it bites
eyvan capacity list
eyvan cells                  # what exists and what it costs
eyvan leases
eyvan budget status
eyvan events                 # the audit trail
eyvan service health ID      # replicas, with unknown kept distinct
```

98 leaf commands across 34 groups; `eyvan --help` is the index.

## Deployment

A control plane, a PostgreSQL database, and one agent per fleet:

```bash
docker compose up            # local: plane, database, dashboard
helm install eyvan deploy/helm/eyvan   # Kubernetes
```

Images are published per commit to `ghcr.io/erfanzar/eyvan`,
`…/eyvan-agent`, and `…/eyvan-worker`, tagged with the version and the source
SHA — never `latest`, because a moving tag makes "which image is running"
unanswerable afterwards.

Schema changes are explicit: `eyvan-migrate` applies them, and the server
refuses to start against a database it does not recognise rather than guessing.

## Installing

Not yet on PyPI. From source, in a checkout:

```bash
uv venv --python 3.13 && uv pip install --require-hashes -r requirements-docker.txt
uv pip install --no-build-isolation --no-deps -e .
```

Every install path uses the hash-pinned exports that ship with the package;
`--require-hashes` refuses a substituted artifact. See
[docs/install.md](docs/install.md), which states the real number of separately
configured prerequisites rather than implying there is one.

## What is guaranteed, and what is not

Guaranteed by tests that run on every commit: the scheduler is pure and its
decisions are revalidated transactionally; stale generations cannot write;
gangs place atomically or not at all; budgets and quotas gate admission;
migrations round-trip; the API authorizes by server-resolved principal, never by
a browser-decoded claim.

Not guaranteed: that any of it has been proven on hardware for the current
commit. Eyvan distinguishes code-complete, host-tested, PostgreSQL-tested,
provider-tested, and hardware-qualified, and the current candidate holds host
and PostgreSQL evidence only. CPU and mock tests are not provider qualification,
and a historical receipt never qualifies a later commit.
[docs/proofs.md](docs/proofs.md) is the ledger; [ROADMAP.md](ROADMAP.md) is the
remaining work.

## Architecture

```
eyvan/domain/        pure policy: scheduler, fairness, budgets, pricing
eyvan/server/        API, persistence, migrations, leader phases
eyvan/capacity/      providers, autoscale, fleet state, hardware catalog
eyvan/worker/        agent, native runtime, staging, distributed environment
eyvan/sdk/           client, CLI, decorators
dashboard/           operations UI
```

The domain layer imports nothing from the server; that is enforced by import
contracts in CI, not by convention.

## Development

```bash
uv sync
uv run pytest tests                    # 4737 tests
EYVAN_REQUIRE_POSTGRES=1 uv run pytest tests   # against the real backend
uv run ruff check eyvan tests
npm --prefix dashboard test
```

CI runs the suite against PostgreSQL, builds the dashboard, and installs the
built wheel to exercise every entry point.

## Documentation

- [docs/eyvan-explained.md](docs/eyvan-explained.md) — the whole system, end to end
- [docs/install.md](docs/install.md) — prerequisites and first run
- [docs/operate.md](docs/operate.md) — day-to-day operation
- [docs/security.md](docs/security.md) — authority, secrets, isolation
- [docs/providers.md](docs/providers.md) — backends and writing your own
- [docs/artifact-safety.md](docs/artifact-safety.md) — artifact lifecycle
- [docs/troubleshoot.md](docs/troubleshoot.md) — when something is wrong
- [docs/proofs.md](docs/proofs.md) — what is proven, and what is not

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
