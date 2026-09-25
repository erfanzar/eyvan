# Proofs and receipts

Host, static, and real-PostgreSQL suites exercise this package, with exact
counts recorded in CI rather than copied into prose. None of that is
evidence about a TPU, GPU, provider lifecycle, or the current release image.
This page exists so approving a spend is a decision about a known quantity.
Each proof states what it establishes, the commands, the pass criterion, the
shape, and roughly what it costs. **No historical run qualifies the current
candidate without an immutable exact-candidate receipt.**

Hardware runs do not substitute for CPU-establishable software gates: a pass
here never closes a host-suite gap, and a host-suite pass never claims a
device. The two ledgers stay separate on purpose.

**The criteria are executable.** `eyvan proof catalog | children | logs |
preemption | warm-window | secret | kubernetes` evaluates the stated
criterion against the live plane and exits 0 on pass, 1 on failure, and 2
when the run did not exercise the behaviour at all. That third verdict is
the one that matters: a spot slice that was never reclaimed, a pool that
never got a cell, an observation taken outside the warm window -- each
produces a clean-looking run that establishes nothing, and reading it off a
screen at the end of a spend is exactly when nobody is inclined to notice.
The judgement lives in `eyvan/sdk/commands/proof.py` and is tested without
hardware; the commands only fetch. A refusal -- a mistyped option, a
receipt path already taken -- exits 3, because a run that never happened
must not wear a verdict's exit code.

## P0 — a callable runs on a multislice allocation

Establishes that the decorator path returns real device truth from a
multi-host spot slice, end to end: submission, atomic capacity, dispatch,
result.

**Passes when** the decorator returns the device count of the whole
allocation -- 16 per host times hosts, not per-process -- and the workload
completes without manual intervention.

**Shape** v5p-16 x 2, spot, one multislice queued resource. **~25 minutes.**

Status: passed 2026-08 (us-east5-a). A head provisioned by `eyvan head up`
was adopted on a re-run and used to dispatch the workload.

## P1 — the hardware catalog is authoritative for hosts per slice

Establishes that generated pools carry per-host CPU, memory, and TPU counts
from the sourced catalog, not from operator guesswork.

**Passes when** the pool's positive CPU, memory, and TPU resources match
the catalog row for the slice type in that zone.

**Shape** v5p-16, one slice; the multislice case is now covered too.

## P2 — multi-host structured logs, with a rank lost mid-stream

Establishes the flattened global-rank log stream and its cursor under a real
failure, not a simulated one.

**Passes when** every rank appears, records carry timestamps, streams page
consistently, and a rank killed mid-stream is reported unavailable on the
next page rather than silently truncating history.

**Shape** v5p-16 multi-host. **~45 minutes.**

Status: passed 2026-08-13, on the seventh attempt. The six failures are the
receipt's value: each names a real refusal mode (spot reclamation before
boot, quota exhaustion, admission denial).

## P3 — in-job submission and exec

Establishes that a job can hold the child-submission capability and exec
surfaces under its owner's identity.

**Passes when** every child's `created_by` is the submitting human rather
than a service identity, and exec answers on every rank with the trainer
still RUNNING.

**Shape** CPU fleet, then v5p-16. **~40 minutes.**

Status: the exec half passed 2026-08-13; in-job submission on accelerators
remains open.

## P4 — secret delivery, per attempt

Establishes that `secret_mounts` references resolve at dispatch into the
attempt's environment, for every attempt, not just the first.

**Passes when** the job exits 0 having read the value (it exits 1 when the
variable is missing), across at least two attempts. `eyvan proof secret`
judges the ledger *per attempt*: exactly one `secret.issued` and one
`secret.read` in each attempt, no read without an issuance, and the value in
no log, record or audit payload; a multi-attempt run adds a `redelivered`
check. (Until 2026-09-24 it counted events across the whole workload and so
failed the two-attempt run this section describes.)

**Shape** any cell; CPU is enough. **~15 minutes.**

## P5 — spot reclamation is charged to the preemption budget

Establishes that a reclaimed slice restarts under `max_preemption_retries`
and is never charged to the job's own failure budget.

**Passes when** all four hold: the reclamation is recorded as preemption,
the restart count increments the preemption budget only, the job resumes
(checkpointed where declared), and `--max-retries 0` does not fail it.

**Shape** v5p-16 spot, run until reclaimed. **1-3 hours, opportunistic** --
run it adjacent to real work, not as a dedicated spend.

## P6 — a warm window held and released across its own boundary

Establishes that a declared warm window holds a floor and releases it when
it ends, without operator action.

**Passes when** the floor is held for the window's duration and released
within one controller pass of the boundary, with events recording both.

**Shape** CPU fleet. **~1 hour** (window length plus margin).

## P7 — Kubernetes against a real API server

Establishes the Kubernetes capacity provider against a live API server, not
injected clients.

**Passes when** provider lifecycle failures map to the documented
`CapacityProviderFailureCode` set and node-pool claims round-trip.

**Shape** GKE, CPU node pool then one GPU node pool. **~2 hours, modest
cost.**

## Live receipts, 2026-08-29 (current candidate)

### The v5p-16 spot zone race

One probe per zone, identical signed callable (`jax.device_count()`),
v5p-16 x 1, spot, submitted the same instant:

| zone | ask | answer |
|---|---|---|
| `us-central1-a` | 1 slice, spot | **granted + cell ready in 409.7 s** (plane-recorded launch latency); idle zone, 0/1024 chips in use |
| `us-east5-a` | 1 slice, spot | **no grant in ~14 min** -- queued resource sat `WAITING_FOR_RESOURCES`; zone congested (960/2048 chips held) |
| `europe-west4-b` | 1 slice, spot | **refused at queue admission**: no permission to submit v5p queued resources in that location; its quota row reads *usage unknown* for this reason |

Reading: quota is permission to be *billed*, queue admission is permission
to *ask*; a zone needs both. Race zones before big spends; rerun
`eyvan capacity launch-latency` for the machine-readable table.

### Concurrent submission of identical callables

Three zones, one callable digest, same instant: the winner's `--no-clobber`
upload landed the digest-named artifact; both losers **crashed in
submission** on `HTTPError 412`. Fixed in `e7abd59` -- the losing upload
verifies the winner's digest and continues -- with three tests in
`tests/test_callable_execution.py`.

### Plane death and full reconciliation

The loopback plane died silently mid-dispatch. `eyvan-server`, restarted
**with the runtime profile's secrets**, reconciled the entire world from
`~/.eyvan/control-plane.json`: 113 workloads, cells, fleets, leases.
Durability held.

### The credentialed-restart 401 loop (rough edge)

A **bare** `eyvan-server` restart (no `EYVAN_AGENT_SHARED_SECRET`) came up
"healthy" and then 401-looped against the still-running cell agent: cells
flipping `unavailable`, a recreate loop. Restarting with the secret from
`~/.eyvan/gcp-native-runtime.json` cleared it in one pass.

### Quarantined lease after a crash mid-dispatch (rough edge)

The crash above landed between lease and dispatch: `workload.dispatch`
retried 10 times over ~23 minutes against a cell already draining --
`delivery_error: cell … has not recovered the quarantined lease` -- without
requeueing the workload. `eyvan workload cancel` resolved it. A
quarantined lease should requeue (or fail) after bounded attempts rather
than retry into a draining cell.

### Renderer honesty

Refused capacity attempts render as `blocked | <shape> | <zone>` with
`capacity request rejected; the plane is retrying (attempt N)` and the
plane's diagnostic (commit `6f9990f`). Queue-admission denials remain
terminal failures carrying the provider's sentence verbatim.

When a historical proof matters for a new spend, re-run it against the
current candidate and record the new receipt here.
