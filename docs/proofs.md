# Outstanding hardware proofs

Everything in this package is CPU-verified: 2652 tests, real PostgreSQL, seven
import contracts, every new behaviour mutation-checked. None of that is evidence
about a TPU.

This page exists so approving a spend is a decision about a known quantity. Each
proof states what it establishes, the exact commands, the pass criterion, the
shape, and roughly what it costs. Nothing here has been run.

Fleet configs live in the monorepo's `configs/`. All are spot.

## Why these and not a general soak of everything

Each row proves something a CPU cannot: Pallas lowering, real provider
semantics, multi-host behaviour, or the passage of wall-clock time. Anything a
CPU *could* establish is already established, and re-proving it on an
accelerator would be spending to learn what is known.

---

## P1 — the hardware catalog is authoritative for hosts per slice

**Establishes** that pool resources come from the shape catalog rather than
hand-written fleet YAML, and that `observed_tpu_vms` equals the catalog's host
count rather than however many endpoints the provider happened to return. A
provider returning 1 of 2 hosts yields a silently half-sized JAX cluster; only
real hardware produces that case.

```
eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan capacity pool get research/v5p --json     # resources from the catalog, not the YAML
eyvan cells --json                              # observed_tpu_vms == catalog hosts
eyvan down --yes
```

**Passes when** the pool's resources match the catalog entry for `v5p-16` and
`observed_tpu_vms` is 2, with no `resources:` block in the fleet YAML.
**Shape** v5p-16, one slice. **~35 minutes.**

## P2 — multi-host structured logs, with a rank lost mid-stream

**Establishes** per-rank attribution and that an unreachable rank is reported
*in place* rather than omitted. A missing rank that looks like silence is the
failure this exists to prevent, and it needs two real hosts.

```
eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name logs --command 'python -c "import time;[print(i) or time.sleep(1) for i in range(600)]"' --tpu 8
eyvan workload log-page <id> --json              # records attributed per rank
# kill one host out of band, then:
eyvan workload log-page <id> --after <cursor> --json
eyvan down --yes
```

**Passes when** the second page reports the killed rank as unavailable while
still returning the surviving rank's records, and the archived blob exists after
terminality so the cell can be released without losing logs.
**Shape** v5p-16 multi-host. **~45 minutes.**

## P3 — in-job submission and exec

**Establishes** that a job submits children under its parent's identity, that the
width ceiling refuses at 409, that an attempt-1 capability stops working after a
retry bumps the generation, and that `exec` runs beside a trainer without
disturbing it.

```
eyvan up --config configs/eyvan-live-cpu-us-east5-a.yaml
# a callable submitted with can_submit_children=True that fans out three ways
eyvan workload children <parent> --json          # created_by is the parent's owner
eyvan down --yes

eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload exec <id> -- py-spy dump --pid <pid>
eyvan down --yes
```

**Passes when** every child's `created_by` is the submitting human rather than
`workload:<id>`, the ceiling returns 409, a stale-generation token is refused,
and `py-spy` returns a stack from every rank with the trainer still RUNNING.
**Shape** CPU fleet, then v5p-16. **~40 minutes.**

## P4 — secret delivery, per attempt

**Establishes** that a workload reads a Secret Manager secret and that the value
appears in no durable record, log line or API response; that the ledger shows one
issuance and one read; and that the capability is refused once the attempt ends.

```
eyvan policy set --allow-secret-reference gcp-secret://<project>/eyvan-proof-
eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name secret-proof --command 'python -c "import os;print(len(os.environ[\"HF_TOKEN\"]))"' \
  --secret HF_TOKEN=gcp-secret://<project>/eyvan-proof-token --tpu 8
eyvan workload logs <id>                         # the length, never the value
eyvan events --json | grep secret.               # one secret.issued, one secret.read
eyvan down --yes
```

**Passes when** the job prints the length, `eyvan events` shows exactly one
`secret.issued` and one `secret.read` for that attempt, and grepping the workload
record, the audit payloads and the logs finds the value nowhere.
**Shape** v5p-16. **~30 minutes.**

## P5 — spot reclamation is charged to the preemption budget

**Establishes** the Phase 8 classification end to end: that GCP reclaiming a spot
slice produces `PREEMPTED` rather than `PROVIDER_ERROR`, that it consumes
`max_preemption_retries` and not `max_retries`, that the pool is *not* marked
stocked-out, and that the workload resumes from its checkpoint.

```
eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name preempt-proof --command '<checkpointing trainer>' \
  --tpu 8 --checkpointable --checkpoint-hook ... --checkpoint-output-uri gs://... \
  --max-retries 0 --max-preemption-retries 5
# wait for GCP to reclaim
eyvan cells --json                               # provider_failure_code == preempted
eyvan workload get <id> --json                   # preemption_count 1, capacity_loss_count 0
eyvan capacity list --json                       # the pool has no stockout cooldown
```

**Passes when** all four hold. `--max-retries 0` is deliberate: under the old
shared budget the workload would have failed, so this is the distinguishing case.
**Shape** v5p-16 spot, run until reclaimed. **1–3 hours, opportunistic** — run it
alongside another proof rather than waiting on it.

## P6 — a warm window held and released across its own boundary

**Establishes** that a window configured in fleet YAML holds a cell warm through
its hours and releases after, and that the billing clock accrues idle cost for
exactly that period. Needs wall-clock time; nothing else does.

```
# add warm_windows to configs/eyvan-live-cpu-us-east5-a.yaml
eyvan up --config configs/eyvan-live-cpu-us-east5-a.yaml
eyvan capacity warmth --json                     # window_active, floor held
# after the window closes
eyvan capacity warmth --json                     # window_active false, floor released
eyvan capacity billing --json                    # idle cost matches the window
eyvan down --yes
```

**Passes when** the floor is held for the window's duration and released within
one reconcile interval of its end.
**Shape** CPU fleet. **~1 hour** (window length plus margin).

## P7 — Kubernetes against a real API server

**Establishes** what `httpx.MockTransport` cannot: 409 `AlreadyExists` replay, a
leaked previous incarnation deleted rather than adopted, pods Pending past
`provisioning_timeout_s`, and with Kueue installed an `Inadmissible` Workload
mapping to `QUEUE_ADMISSION_DENIED` rather than terminal failure.

**Passes when** each maps to the documented `CapacityProviderFailureCode`, and
`eyvan capacity pool` refuses a ClusterQueue whose preemption could evict Eyvan's
work.
**Shape** GKE, CPU node pool then one GPU node pool. **~2 hours, modest cost.**

## P8 — the soak

**The one the plan calls disqualifying if absent.** Everything above is minutes
to hours with a human watching. This is the only evidence about days.

```
eyvan up --config configs/eyvan-live-cpu-us-east5-a.yaml
eyvan up --config configs/eyvan-live-v5p16-us-east5-a.yaml
# submit a mixed load: short callables, a long checkpointed trainer,
# a batch fan-out, and a service
eyvan soak --hours 72 --interval-s 60 --output soak.json
```

**Passes when** `soak.json` reports `"healthy": true` — no invariant violated
persistently across 72 hours, meaning no stranded cells, no leases outliving
their workloads, no stuck outbox commands, no workload non-terminal without a
lease, budget spend monotone and matching settled cost, and audit retention
actually pruning.

Read `artifact_growth` even on a pass: it is the number that says whether
collection ran at all, and a total alone cannot.
**Shape** CPU fleet plus v5p-16. **72 hours.** This is the largest single spend
here and the only one that cannot be shortened without changing what it proves.

---

## Order

P5 is opportunistic — start it and let it run while P1 through P4 execute on
other slices. P6 needs wall-clock time and nothing else, so it can overlap
anything. P8 goes last, after the others have confirmed the individual
behaviours it assumes.

## What a failure means

Every proof above has a CPU-verified counterpart that already passes. A failure
here therefore means the CPU test was testing the wrong thing — not that the
feature is broken in a way the suite could have caught. That is the point of
running them, and it is why "the tests pass" has never appeared in this
repository as an argument that something works on hardware.
