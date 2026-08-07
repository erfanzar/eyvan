# Eyvan (ایوان)

A durable control plane for CPU, GPU, and TPU workloads.

Its name comes from the Persian *eyvan*: a vaulted gateway into the
infrastructure it manages. You declare capacity, submit work, and Eyvan owns the
rest — provisioning, admission, fairness, budgets, leases, execution, logs, and
cleanup — on virtual machines and on Kubernetes, through one workload path.

Eyvan is a **control plane**, not a distributed runtime. It has no object store,
no actor model, and no training library. What it owns is capacity, admission,
and money.

```bash
uv sync
uv run eyvan up                    # loopback fleet; no cloud account needed
uv run eyvan workload submit --org acme --project research \
    --name hello --command "python -c 'print(1)'" --cpu 1 --memory-gib 1
uv run eyvan workload list --org acme --project research
```

---

## Where it runs

One workload path over every substrate: a native worker in a Kubernetes pod and
a native worker on a TPU VM speak the same protocol, so the substrate is a
capacity concern that execution never sees.

| backend | provisions | qualification |
|---|---|---|
| `gcp-tpu` | Cloud TPU queued resources, including Multislice | live v5p-16, P1 exit 0 |
| `gcp-compute` | Compute Engine CPU and GPU instances | live, P6 exit 0 |
| `kubernetes` / `k8s` | one cell per Indexed Job on any conformant cluster | live GKE, P7 exit 0 |
| `onprem-ssh` | hosts you already own, claimed over SSH with a fenced marker | host-tested; no run against a real SSH host |
| `manual` | capacity an operator registers by hand | host-tested |
| `local` | single-host development | host-tested |

Anything else is a plugin. `eyvan.capacity_providers` is a published
entry-point ABI with a version contract and a conformance suite an external
provider imports and must pass — see [docs/providers.md](docs/providers.md).

GCP has the most depth because that is where the hardware evidence was
gathered, not because the design assumes it. The parts that are hard to get
right — fencing, leak detection, inconclusive-versus-failed reporting — are
backend-neutral, and the Kubernetes driver is deliberately a near-clone of the
manual one to keep proving that.

---

## The five ideas everything else follows from

**1. The scheduler is pure.** `Scheduler.schedule` reads a snapshot and returns
placements; it writes nothing. The controller revalidates inside a transaction
and commits, so a decision made against a stale snapshot cannot land.

**2. Everything is generation-fenced.** Leases, cells, workloads, artifacts,
deployments, observations, deletions. A superseded actor loses. This is what
makes a restart, a leader election, or a partition safe.

**3. No external I/O under a lock.** Reserve durably, call the provider,
finalize fenced. A provider call inside a transaction turns a slow API into a
stuck cell.

**4. "I could not find out" is never written down as a fact.** A probe that
times out is UNKNOWN, not unhealthy. A cluster Eyvan cannot read is UNKNOWN,
never full and never empty. A stage whose presence cannot be confirmed is
unproven — never a cache hit, always a budget charge. A proof that never
exercised its behaviour exits **2**, not 0.

**5. Authority is resolved server-side.** Every request becomes a `Principal` on
the server. The browser's decoded claims are display only.

---

## Concepts

**Pool** — a declaration of capacity you are willing to pay for: provider, zone,
accelerator type, class (spot or on-demand), and bounds.

**Cell** — one unit of provisioned capacity. On GCP a TPU slice or a VM; on
Kubernetes one Indexed Job. A cell has a generation; a recreate bumps it and
anything holding the old one is refused.

**Lease** — the durable claim a workload holds on a cell. Leases are why
placement survives a restart: the schedule is not in memory, it is in the
database with a fencing token.

**Workload** — what you submit.

```
pending → admitted → leased → dispatching → running ⇄ checkpointing → succeeded
                                   │                                   ↘ failed
                                   └─────────── cancelling → cancelled
```

**Attempt** — one execution of a workload. A retry is a new attempt with a new
rank space; cursors, logs, and archives are per-attempt and never shared.

**Artifact** — managed storage with a real lifecycle: reserve → write →
activate, and claim → delete → finalize. Only `ACTIVE` artifacts may be read or
referenced; anything unproved stays `QUARANTINED`.

---

## Submitting work

You declare the **shape** you need. You do not choose the cell — the scheduler
does, because it is the only component that can see quota, fair share, warmth,
locality, and live capacity at once.

```bash
# one workload
uv run eyvan workload submit --org acme --project research \
    --name train --command "python train.py" --cpu 4 --memory-gib 16

# a TPU gang: two v5p-16 slices, checkpointed so preemption is survivable
uv run eyvan workload submit --org acme --project research \
    --name pretrain --command "python train.py" \
    --accelerator-type v5p-16 --replicas 2 --capacity-class spot \
    --checkpointable --checkpoint-output-uri gs://bucket/ckpt \
    --max-preemption-retries 8

# an array; each task sees EYVAN_BATCH_INDEX
uv run eyvan workload submit-batch --org acme --project research \
    --count 64 --command "python shard.py" --cpu 2 --memory-gib 8

# a pipeline of dependent nodes, admitted in one transaction
uv run eyvan workload submit-dag --org acme --project research --file pipeline.yaml
```

### From Python

```python
from eyvan import ClientIdentity, EyvanClient

client = EyvanClient(identity=ClientIdentity(token="OIDC access token"))
job = client.run("research", "tpu-workers", command="python train.py")
job.wait()
```

Or submit a trusted callable directly. `pods=N` is one atomic Cloud TPU
Multislice cell of N complete slices, not N loose workers:

```python
from eyvan import TpuConfig, execute

@execute(TpuConfig(type="v5p-16", pods=2))
def train():
    import jax
    print(jax.device_count())

train()
```

Inside a running workload, `report_progress`, `submit_child`, and
`in_job_context` let a job report and fan out under its owner's identity with a
generation-pinned capability token.

---

## What happens when you submit

1. **Admission.** The service authenticates you into a `Principal`, checks quota
   and fair share, refuses an oversized fan-out *here* — where you can be told —
   and writes the workload with an idempotency key in one transaction.
2. **Scheduling.** The leader runs the pure kernel over a snapshot, then
   revalidates and commits leases transactionally. A gang takes every lease or
   none.
3. **Dispatch.** The attempt's **rank space is frozen**: ordered cells,
   generations, widths, offsets, and a digest, in its own table. Reads never
   recompute it from live cells, because a reconciliation that changed one
   cell's width would silently shift every later rank.
4. **Launch.** Eyvan **owns the topology environment** — caller-supplied `JAX_*`,
   `MEGASCALE_*`, `TPU_*`, and `CUDA_*` values are stripped and replaced, so a
   submission cannot spoof its own rank.
5. **Execution.** One process per endpoint under a systemd transient unit or a
   container, with journald capture and progress reporting.
6. **Observation.** A host whose own output names an unrecoverable accelerator
   fault is **replaced**, not retried forever.
7. **Settlement.** Logs are archived *before* the lease is released — releasing a
   cell destroys its journal, so the archive is the only durable copy. Then
   billing, budgets, artifacts, cleanup.

---

## Operating it

```bash
uv run eyvan status                  # discovers the running fleet
uv run eyvan cells                   # every registered cell
uv run eyvan leases                  # who holds what
uv run eyvan capacity invariants     # what the fleet is violating right now
uv run eyvan capacity billing        # accrued and idle cell cost
uv run eyvan capacity queue-trend    # how deep the queue has been
uv run eyvan doctor                  # configuration findings
uv run eyvan events                  # the audit trail
```

**`eyvan workload explain` is the one to reach for first.** It answers *why* in
operator terms — an exhausted quota, a shape no pool carries, a ClusterQueue
with no room — with the numbers behind it.

```bash
uv run eyvan workload explain WORKLOAD_ID
uv run eyvan workload log-page WORKLOAD_ID       # structured, per-rank, resumable
uv run eyvan workload process-status WORKLOAD_ID
```

Two escape hatches, both audited and both refusing until a threshold is met:
`eyvan workload-force-finalize` and `eyvan preemption-force-finalize`.

### Tenancy, quota, budgets

```bash
uv run eyvan org create · project create · grant add
uv run eyvan quota set · budget set · policy set
```

Hierarchical fair share, priority bands, and budgets are enforced at admission,
not discovered at settlement. Budgets are optional — and if none exists, the
dashboard says **"No budget"** rather than reporting `$0`, because cell cost
accrues either way.

### Structured logs

Per-rank, resumable, and honest about gaps. A cursor names a position in one
attempt's frozen rank space; a rank that could not be reached is reported *in
place* rather than as a shorter, complete-looking page. Terminal attempts get a
schema-2 archive manifest naming every expected rank with its object's digest
and size, so a missing rank is a named gap and never a silent truncation.

---

## Deployment

Local development needs nothing configured. A network deployment needs fifteen
separately configured things; each is enumerated with what it is for and how it
fails when missing in [docs/install.md](docs/install.md).

The five processes:

| binary | role |
|---|---|
| `eyvan-server` | the control plane API |
| `eyvan-agent` | one cell agent per cell |
| `eyvan-native-worker` | the process that runs your code |
| `eyvan-migrate` | schema migrations, never automatic in production |
| `eyvan` | the CLI |

Helm charts and rendered manifests live in `deploy/`. Images are built from
`Dockerfile`, `Dockerfile.agent`, and `Dockerfile.worker`; the release pipeline
signs them, attaches SBOMs and provenance, and promotes only digests that
passed every gate.

### Kubernetes and Kueue

One cell is one Indexed Job. **Kueue admits capacity; Eyvan admits workloads** —
and a ClusterQueue Eyvan uses must be exclusively Eyvan's with preemption
disabled, verified before a Job is created. Two admission controllers over the
same nodes fight twice: Kueue can evict an admitted Workload for higher-priority
work, destroying a pod with no checkpoint hook and bypassing Eyvan's whole
`PreemptionPlan` / `CHECKPOINT_AND_STOP` / `grace_period_seconds` machinery.

The honest cost, stated here because it is real: Eyvan's budgets reason about
cells it asked for, not the cluster's true free capacity, so a pool can sit
WAITING while the cluster is full of non-Eyvan work. It observes that capacity
and explains it; it does not plan around it.

---

## What is guaranteed, and what is not

**Guaranteed, and tested:** placement survives a control-plane restart · a
superseded controller cannot mutate a cell a newer incarnation owns · a gang
places whole or not at all · a workload cannot spoof its own rank · quotas and
budgets hold under contention · a lost rank is reported in place · terminal logs
are archived before capacity is released · only proved artifacts are readable.

**Not guaranteed, and stated rather than implied:** three proofs have real
hardware behind them (P1, P6, P7 — all exit 0); **four have never exercised
their behaviour and exit 2** (P2, P3, P4, P5); P8 is not yet qualifiable. No
image has been published, signed, or deployed. GPU support is modelled and
reachable but no GPU has executed an Eyvan workload. Federation is deferred
until there is externally qualified PKI.

The proof vocabulary is the honest part of this system:

> **exit 0** passed · **exit 1** failed · **exit 2** the run never put the
> behaviour under test.

A check that passes because there was nothing to check is the failure mode this
package was built to refuse. See [docs/proofs.md](docs/proofs.md).

---

## Architecture

```
SDK · CLI · dashboard · OpenAPI
            │  server-resolved Principal
CONTROL PLANE   admission · quota · fair share · budgets · tenancy · audit
            │  pure scheduler kernel
CONTROLLER      25 leader phases, one leader at a time
            │  transactional outbox, fenced
CAPACITY        pools · beliefs · autoscale · hardware catalog · staging
            ├──────────────┬──────────────┐
        VM substrate   Kubernetes     on-prem / manual
            └──────────────┴──────────────┘
                     │  CellRecord (fenced, durable)
              CELL AGENT → NATIVE WORKER
                     gang · slice · in-job · services · staging
```

Seven CI-enforced layering contracts hold the shape:

```
_internal → domain → records.py → artifacts → capacity → worker → server → sdk
```

The one worth naming: **the host-side worker can never import the control plane
or the SDK.** A compromised or crashed worker cannot reach into scheduling
authority.

Eyvan imports no other workspace package, and does not use Ray.

---

## Development

```bash
uv sync
uv run pytest libs/eyvan/tests            # the suite
uv run ruff check libs/eyvan
uv run lint-imports                       # the seven contracts
npm --prefix libs/eyvan/dashboard test
```

Against a real PostgreSQL, which is the production backend:

```bash
EYVAN_REQUIRE_POSTGRES=1 EYVAN_TEST_POSTGRES_BINDIR=/path/to/pg/bin \
  uv run pytest libs/eyvan/tests
```

The test bar: assert public API outputs, state transitions, persisted rows, or
emitted bytes. Not that a private helper was called, not that a constructor did
not raise, and never production logic compared against itself.

---

## Documentation

- [docs/eyvan-explained.md](docs/eyvan-explained.md) — architecture and usage in one pass
- [docs/install.md](docs/install.md) — the fifteen things a remote deployment needs
- [docs/operate.md](docs/operate.md) — daily operation, cost, storage, debugging
- [docs/security.md](docs/security.md) — authority boundary, capability tokens, secrets, tenancy
- [docs/troubleshoot.md](docs/troubleshoot.md) — symptom first, routed to the command that answers it
- [docs/providers.md](docs/providers.md) — the capacity provider ABI and its conformance suite
- [docs/proofs.md](docs/proofs.md) — every hardware proof, its criterion, shape, and cost
- [docs/artifact-safety.md](docs/artifact-safety.md) — artifact lifecycle and deletion guarantees
- [ROADMAP.md](ROADMAP.md) — what is built, what is partial, what is deferred

## License

AGPL-3.0-or-later. See [LICENSE](LICENSE).
