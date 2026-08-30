# Proofs and receipts

Host, static, and real-PostgreSQL suites exercise this package, with exact
counts recorded in CI rather than copied into prose. None of that is
evidence about a TPU, GPU, provider lifecycle, or the current release image.
This page exists so approving a spend is a decision about a known quantity.

**No historical run qualifies the current candidate without an immutable
exact-candidate receipt.**

## The criteria are executable

`eyvan proof catalog | children | logs | preemption | warm-window | secret |
kubernetes` evaluates a stated criterion against the live plane and exits:

- **0** — pass,
- **1** — failure,
- **2** — the run did **not exercise the behaviour at all** (a spot slice
  never reclaimed, a pool that never got a cell, an observation outside the
  warm window: each produces a clean-looking run that establishes nothing),
- **3** — refusal (a mistyped option, a receipt path already taken): a run
  that never happened must not wear a verdict's exit code.

The judgement lives in `eyvan/sdk/commands/proof.py`, tested without
hardware; the commands only fetch.

## Live receipts, 2026-08-29 (current candidate)

### The v5p-16 spot zone race

One probe per zone, identical signed callable (`jax.device_count()`),
v5p-16 × 1, spot, submitted the same instant:

| zone | ask | answer |
|---|---|---|
| `us-central1-a` | 1 slice, spot | **granted + cell ready in 409.7 s** (plane-recorded launch latency); idle zone, 0/1024 chips in use |
| `us-east5-a` | 1 slice, spot | **no grant in ~14 min** — queued resource sat `WAITING_FOR_RESOURCES`; zone congested (960/2048 chips held) |
| `europe-west4-b` | 1 slice, spot | **refused at queue admission**: "User does not have permission to submit requests … for accelerator type v5p-16 in location europe-west4-b" — the project's quota row for the zone reads *usage unknown* for this reason |

Reading: quota is permission to be *billed*, queue admission is permission
to *ask*; a zone needs both. Race zones before big spends; rerun
`eyvan capacity launch-latency` for the machine-readable table.

### Concurrent submission of identical callables

Three zones, one callable digest, same instant: the winner's `--no-clobber`
upload landed the digest-named artifact; both losers **crashed in
submission** on `HTTPError 412`. Fixed in `e7abd59` — the losing upload now
verifies the winner's digest and continues — with three tests in
`tests/test_callable_execution.py`. Lesson recorded: for a content-addressed
name, "already exists" is the desired end state, not a failure.

### Plane death and full reconciliation

The loopback plane died silently mid-dispatch (first dispatch of the race's
central1 workload). `eyvan-server`, restarted **with the runtime profile's
secrets**, reconciled the entire world from `~/.eyvan/control-plane.json`:
113 workloads, cells, fleets, leases. One workload needed hands — see the
rough edge below. Durability held.

### The credentialed-restart 401 loop (rough edge)

A **bare** `eyvan-server` restart (no `EYVAN_AGENT_SHARED_SECRET`) came up
"healthy" and then 401-looped against the still-running cell agent:
`cells/…/reconcile → 401`, cells flipping `unavailable`, a recreate loop.
Restarting with the secret from `~/.eyvan/gcp-native-runtime.json` cleared
it in one pass. Documented in [operate](operate.md) and
[troubleshoot](troubleshoot.md); a follow-up to auto-load the profile would
remove the trap.

### Quarantined lease after a crash mid-dispatch (rough edge)

The crash above landed between lease and dispatch: `workload.dispatch`
retried 10 times over ~23 minutes against a cell already draining —
`delivery_error: cell … has not recovered the quarantined lease` — without
requeueing the workload. `eyvan workload cancel` resolved it; the cell and
its provider resource reaped cleanly afterward. Known rough edge: a
quarantined lease should requeue (or fail) after a bounded number of
attempts rather than retrying into a draining cell.

### Renderer honesty

Refused capacity attempts render as `blocked | <shape> | <zone>` with
`capacity request rejected; the plane is retrying (attempt N)` and the
plane's diagnostic — not a per-refusal `FAILED` obituary (commit `6f9990f`).
Queue-admission denials remain terminal failures carrying the provider's
sentence verbatim.

## Historical hardware proofs (pre-2026-08-29; tied to their candidates)

Retained for context; **none of these qualifies the current candidate**:

- **P0 (v5p-16 × 2, us-east5-a, spot):** a multi-host spot cell can be
  granted, booted, and dispatched end to end; `jax.device_count()` returned
  the true topology. Established the atomic multi-worker path on GCP.
- **P1:** plane/agent redeploy from current source, fleets reconciled from
  generated configs; `eyvan capacity builds` reporting pool truth.
- **P2:** passed 2026-08-13 on the seventh attempt — the run's value is the
  six failures: each names a real refusal mode (spot reclamation before
  boot, quota exhaustion, admission denial).
- **P3 (exec half):** passed 2026-08-13 — exec surfaces on a real cell;
  the non-exec half remains open.
- A GPU cell can run a callable and have no usable GPU; a wedged
  unattended-upgrades takes the cell on any image; three gates — quota,
  admission permission, and supply — decide whether a shape can be
  requested, not one.

When a historical proof matters for a new spend, re-run it against the
current candidate and record the new receipt here.
