# Eyvan, explained

What it is, how it is built, how it behaves, and how to use it. Every number and
name below was read out of the tree it describes, not recalled.

---

## 1. What Eyvan is

Eyvan is a **fleet control plane**. You declare capacity, submit workloads, and
it owns the rest: provisioning machines, admitting work, enforcing fairness and
budgets, holding leases, running processes, collecting logs, and cleaning up.

It runs CPU, GPU, and TPU work on two substrates -- **GCP virtual machines** and
**Kubernetes** -- through one workload path. It depends on no other package in
this monorepo, and it does not use Ray.

The one-sentence positioning:

> Ray is a distributed runtime. Iris is a job orchestrator.
> **Eyvan is the control plane that owns capacity, admission, and money** --
> with execution as a first-class but subordinate concern.

Eyvan deliberately does *not* implement an object store, an actor model, or a
training library. Replacing Ray means removing the reason to run Ray *as an
orchestrator*, not reimplementing its runtime.

### Size, measured

| | |
|---|---|
| Python | 222 modules |
| Tests | 4,488 passing against real PostgreSQL 17.5 |
| HTTP routes | 103 |
| CLI commands | 95 leaf commands in 32 groups |
| Durable tables | 26 |
| Migrations | 21, single head `0018_service_operations` |
| Leader phases | 25 |
| Layering contracts | 7, CI-enforced |

---

## 2. The five ideas everything else follows from

**1. The scheduler is pure.** `Scheduler.schedule` reads a snapshot and returns
placements. It writes nothing. The controller then revalidates inside a
transaction and commits, so a decision made against a stale snapshot cannot
land.

**2. Everything is generation-fenced.** Leases, cells, workloads, artifacts,
deployments, observations, deletions -- each carries a generation and a fencing
token. A superseded actor loses. This is what makes a control-plane restart, a
leader election, or a network partition safe.

**3. No external I/O under a lock.** Reserve durably, call the provider, finalize
fenced. A provider call inside a transaction turns a slow API into a stuck cell.

**4. "I could not find out" is never written down as a fact.** This is the rule
the whole package is organised around. A probe that times out is UNKNOWN, not
unhealthy. A cluster Eyvan cannot read is UNKNOWN, never full and never empty. A
stage whose presence cannot be confirmed is unproven -- never a cache hit, always
a budget charge. A proof that never exercised its behaviour exits **2**, not 0.

**5. One workload path over two substrates.** A native worker in a Kubernetes pod
and a native worker on a TPU VM speak the same protocol. The substrate is a
capacity concern; execution does not know which one it is on.

---

## 3. Architecture

```
                    ┌───────────────────────────────────────────┐
   you ────────────▶│ SDK · CLI · dashboard · OpenAPI           │
                    └───────────────────┬───────────────────────┘
                                        │ every request resolves to a Principal
                                        │ server-side; the browser's decoded
                                        │ claims are display-only
                    ┌───────────────────▼───────────────────────┐
                    │ CONTROL PLANE            (server/service)  │
                    │  admission · quota · fair share · budgets  │
                    │  tenancy · RBAC · audit · idempotency      │
                    │  ┌──────────────────────────────────────┐  │
                    │  │ pure scheduler kernel (domain/)      │  │
                    │  └──────────────────────────────────────┘  │
                    └───────────────────┬───────────────────────┘
                                        │ transactional outbox, fenced
                    ┌───────────────────▼───────────────────────┐
                    │ CONTROLLER            (server/controller)  │
                    │  25 leader phases, one leader at a time    │
                    └───────────────────┬───────────────────────┘
                                        │
                    ┌───────────────────▼───────────────────────┐
                    │ CAPACITY                     (capacity/)   │
                    │  pools · beliefs · autoscale · catalog     │
                    │  staging · provider drivers                │
                    └──────┬───────────────────────────┬─────────┘
                           │                           │
              ┌────────────▼─────────┐      ┌──────────▼──────────────┐
              │ VM substrate         │      │ Kubernetes substrate    │
              │ TPU queued resources │      │ one cell = one Indexed  │
              │ Compute instances    │      │ Job; Kueue admits       │
              │ on-prem SSH          │      │ capacity only           │
              └────────────┬─────────┘      └──────────┬──────────────┘
                           └───────────┬───────────────┘
                                       │ CellRecord (fenced, durable)
                         ┌─────────────▼──────────────┐
                         │ CELL AGENT   (worker/agent) │
                         │  one per cell; owns the     │
                         │  machine, not the schedule  │
                         └─────────────┬──────────────┘
                                       │
                         ┌─────────────▼──────────────┐
                         │ NATIVE WORKER (worker/rt)   │
                         │  gang · slice · in-job ·    │
                         │  services · staging         │
                         └────────────────────────────┘
```

### The layering contract, enforced in CI

```
_internal → domain → records.py → artifacts → capacity → worker → server → sdk
```

Seven contracts hold, checked by `uv run lint-imports`. The one worth naming:
**the host-side worker can never import the control plane or the SDK.** A
compromised or crashed worker cannot reach into scheduling authority.

### Where the code lives

| path | lines | owns |
|---|---:|---|
| `server/service/` | 12k | the authority: admission, quota, fairness, tenancy, audit |
| `worker/runtime/` | 10k | execution: native worker, gangs, slices, systemd units, containers |
| `sdk/commands/` | 9.5k | 32 CLI groups |
| `server/controller/` | 9k | the leader loop |
| `capacity/providers/` | 6k | GCP and Kubernetes |
| `server/persistence/` | 6k | memory / local-JSON / SQLAlchemy backends + migrations |
| `artifacts/` | 4.5k | managed artifact lifecycle, capability tokens, secrets |
| `server/api/` | 4.4k | FastAPI, SSE, OIDC, access logging |
| `capacity/` | 3.3k | scheduler kernel, hardware catalog, fleet config |

---

## 4. The object model

**Pool** — a declaration of capacity you are willing to pay for: a provider, a
zone, an accelerator type, a class (spot or on-demand), and bounds.

**Cell** — one unit of provisioned capacity. On GCP that is a TPU slice or a VM;
on Kubernetes it is one Indexed Job. A cell has a generation; a recreate bumps
it, and anything holding the old generation is refused.

**Lease** — the durable claim a workload holds on a cell. Leases are what make
placement survive a restart: the schedule is not in memory, it is in the
database with a fencing token.

**Workload** — the unit you submit. States:

```
pending → admitted → leased → dispatching → running ⇄ checkpointing → succeeded
                                    │                                  ↘ failed
                                    └────────── cancelling → cancelled
```

**Attempt** — one execution of a workload. A retry is a new attempt with a new
rank space; cursors, logs, and archives are per-attempt, never shared.

**Artifact** — managed storage with a real lifecycle: reserve → write → activate,
and claim → delete → finalize. Only `ACTIVE` artifacts may be read or referenced;
anything unproved stays `QUARANTINED`.

---

## 5. How a workload actually runs

1. **Submit.** `workload submit` posts a spec. The service authenticates the
   caller into a `Principal`, checks the project's quota and fair share, refuses
   an oversized fan-out *here* (where you can be told), and writes the workload
   plus an idempotency key in one transaction.
2. **Schedule.** The leader's `schedule` phase runs the pure kernel over a
   snapshot, then revalidates and commits leases inside a transaction. A gang
   takes every lease or none.
3. **Dispatch.** The attempt's **rank space is frozen** here -- ordered cells,
   generations, widths, offsets, and a digest -- into its own table. Reads never
   recompute it from live cells, because a reconciliation that changed one cell's
   width would silently shift every later rank.
4. **Launch.** The controller sends the spec to the cell agent, which forwards to
   the native worker. Eyvan **owns the topology environment**: caller-supplied
   `JAX_*`, `MEGASCALE_*`, `TPU_*`, `CUDA_*` values are stripped and replaced, so
   a submission cannot spoof its own rank.
5. **Run.** The worker launches one process per endpoint under a systemd
   transient unit (or a container), journals stdout and stderr, and reports
   progress.
6. **Observe.** `observe-workloads` polls, reconciles, and settles. A host whose
   own output names an unrecoverable accelerator fault is **replaced**, not
   retried forever.
7. **Settle.** On terminal state the logs are archived *before* the lease is
   released -- releasing a cell destroys its journal, so the archive is the only
   durable copy. Then billing, budget, artifacts, and cleanup.

---

## 6. Using it

### Install and start locally

```bash
uv sync
uv run eyvan up                 # loopback development fleet, no cloud account
uv run eyvan status
uv run eyvan dashboard          # hosted UI
```

`eyvan up` needs nothing configured. A *network* deployment needs fifteen things
configured; they are enumerated in `docs/install.md`.

### Submit work

```bash
# one workload: you state the shape you need, not the pool it lands on --
# placement is the scheduler's decision, made against quota and fair share
uv run eyvan workload submit --org acme --project research \
    --name train --command "python train.py" --cpu 4 --memory-gib 16

# a TPU gang: two v5p-16 slices, checkpointed so a preemption is survivable
uv run eyvan workload submit --org acme --project research \
    --name pretrain --command "python train.py" \
    --accelerator-type v5p-16 --replicas 2 --capacity-class spot \
    --checkpointable --checkpoint-output-uri gs://bucket/ckpt \
    --max-preemption-retries 8

# an array of N identical tasks; each sees EYVAN_BATCH_INDEX
uv run eyvan workload submit-batch --org acme --project research \
    --count 64 --command "python shard.py" --cpu 2 --memory-gib 8

# a pipeline of dependent nodes, admitted in one transaction
uv run eyvan workload submit-dag --org acme --project research --file pipeline.yaml
```

Note what is *absent*: there is no `--pool`. You declare the shape and the class;
the scheduler chooses the cell, because it is the only component that can see
quota, fair share, warmth, locality, and live capacity at once.

### Watch it

```bash
uv run eyvan workload status  WORKLOAD_ID
uv run eyvan workload explain WORKLOAD_ID     # why it is or is not placeable
uv run eyvan workload log-page WORKLOAD_ID    # structured, per-rank, resumable
uv run eyvan workload process-status WORKLOAD_ID
uv run eyvan capacity queue-trend             # how deep the queue has been
uv run eyvan capacity invariants              # what the fleet is violating now
```

`workload explain` is the one to reach for first: it answers *why*, in the
operator's terms -- an exhausted quota, a shape no pool carries, or a
ClusterQueue with no room, with the numbers behind it.

### From Python

```python
from eyvan import EyvanClient, ClientIdentity

client = EyvanClient(identity=ClientIdentity(token="OIDC access token"))
job = client.run("research", "tpu-workers", command="python train.py")
job.wait()
```

Or submit a trusted callable directly -- a decorator, no Ray and no eray.
``pods=N`` is one atomic Cloud TPU Multislice cell of N complete slices, not N
loose workers:

```python
from eyvan import execute, TpuConfig

@execute(TpuConfig(type="v5p-16", pods=2))
def train():
    import jax
    print(jax.device_count())

train()
```

Inside a running workload, `report_progress`, `submit_child`, and
`in_job_context` let a job report and fan out under its owner's identity, with a
generation-pinned capability token.

### Operate

```bash
uv run eyvan capacity list · pool get · cell get · lease get
uv run eyvan capacity billing · warmth · launch-latency · orphans
uv run eyvan quota set · budget set · grant add       # tenancy
uv run eyvan artifacts list · usage · inventory · hold
uv run eyvan doctor                                    # configuration findings
uv run eyvan events                                    # the audit trail
```

Two escape hatches, both audited and both refusing until a threshold is met:
`workload-force-finalize` and `preemption-force-finalize`.

### The five processes

| binary | role |
|---|---|
| `eyvan-server` | the control plane API |
| `eyvan-agent` | one cell agent per cell |
| `eyvan-native-worker` | the process that runs your code |
| `eyvan-migrate` | schema migrations, never automatic in production |
| `eyvan` | the CLI |

---

## 7. What it guarantees, and what it does not

**Guaranteed, and tested:**
placement survives a control-plane restart · a superseded controller cannot
mutate a cell a newer incarnation owns · a gang places whole or not at all · a
workload cannot spoof its own rank · quotas and budgets hold under contention ·
logs are per-rank and resumable, and a lost rank is reported *in place* rather
than as a shorter complete-looking page · terminal logs are archived before the
capacity is released · only proved artifacts are readable.

**Not guaranteed, and stated rather than implied:**
nothing here is qualified on real hardware beyond three recorded proofs
(P1, P6, P7) · four proofs (P2–P5) have never exercised their behaviour and exit
**2** · no image has been published, signed, or deployed · GPU support is
modelled and reachable but no GPU has executed an Eyvan workload · federation is
deferred until there is real PKI.

The proof vocabulary is the honest part of this system, so it is worth stating
plainly: **exit 0 passed, exit 1 failed, exit 2 means the run never put the
behaviour under test.** A check that passes because there was nothing to check
is the failure mode this package was built to refuse.
