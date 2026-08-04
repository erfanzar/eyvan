# Operating Eyvan

## Production design contracts

The accepted records have different implementation boundaries:

- [scoped artifact identity and fenced upload/deletion](design/0001-scoped-artifact-lifecycle.md)
  is implemented and PostgreSQL-tested, but existing-deployment migration,
  object-store reconciliation, and destructive live collection remain gated;
- [flattened log ranks, opaque cursors, and archive compatibility](design/0002-global-log-ranks-and-cursors.md)
  is partially implemented: the narrower M1.3 global-rank/cursor/archive surface
  is host-tested, while strict topology-bound cursor rejection and the full
  archive manifest remain pending in addition to real multi-host qualification;
- [managed-service health and metrics ownership](design/0003-service-health-and-metrics.md)
  remains an accepted, implementation-pending boundary; and
- [federation trust, consistency, and witnessed ownership](design/0004-federation-consistency.md)
  remains implementation-pending and disabled.

Keep destructive artifact collection and federation/failover behind their
stated evidence gates. Implemented software is not authorization to mutate an
existing deployment.

## Every day

```
eyvan status                  # fleet-level summary; discovers the running fleet
eyvan workload list             # what is queued and running
eyvan capacity list             # pools this project may submit to
```

## When something is queued and not running

`eyvan workload explain <id>` is the first command, always. It reports the
scheduler's actual verdict rather than a guess: which pools were considered,
which constraint each failed, and whether the answer is capacity, quota, budget
or policy.

`eyvan capacity queue-trend` answers the follow-up -- whether this is a moment
or a pattern. Instantaneous queue depth cannot distinguish a fleet that is
briefly busy from one that has been saturated for eight hours.

`eyvan capacity launch-latency` answers "why did my job land in the slow
region". It reports the same p95 the scheduler ranks pools on, with the sample
count beside every figure: a p95 over three provisions is not a p95, and a pool
reporting no samples is unmeasured rather than instant. A pool with cells in
cooldown is not being asked, so its latency figure is frozen at whatever it was
before the failures started -- which is why that count is on the same line.

## Cost

```
eyvan capacity billing          # accrued and idle cell cost
eyvan capacity warmth           # which idle cost is warm capacity you asked for
eyvan budget status             # spend against limit, per project
eyvan workload estimate --tpu 8 --accelerator-type v5p-8 --duration-s 7200
```

Read `warmth` alongside `billing`. Warm capacity is *deliberately* idle, and
without the pairing the only safe reaction to an unexplained idle figure is to
delete the warm floor -- undoing the fix that put it there.

`eyvan capacity carbon` reports grid intensity per region and will say
`no_provider` until you configure one. That is the answer, not a gap in it.

## Cost across more than one control plane

> **Current boundary:** the commands below create and enforce grant records in
> this plane's database. The current release does not authenticate or replicate
> them to the peer URL and does not transfer workload ownership. Treat this as a
> manual/experimental control, not secure federation; DR-0004 defines the gated
> production protocol linked above.

A budget is one counter in one database, so a tenant with projects on three
planes has three independent limits -- each locally compliant while the tenant
spends three times what you set. Set `EYVAN_PLANE_ID` on every plane and cut the
peers slices of the home plane's limit:

```
eyvan federation peer add eu-plane --endpoint https://eu.example
eyvan federation grant issue eu-plane --org acme --project research \
  --amount 250 --expires-at 2026-12-31
eyvan federation grant list                    # who holds headroom right now
eyvan federation grant list --include-inactive # who used to, and stopped
```

A plane with no `EYVAN_PLANE_ID` refuses to issue and does not consult grants at
admission, which is the correct behaviour for the single-plane deployment and
the reason federation stays off until you name your planes.

`--expires-at` is required. A grant that cannot lapse turns a home plane going
away permanently into unbounded spend on every peer that ever held one.
Re-issuing to a peer *replaces* its slice rather than adding to it, so a peer's
authority is always one number you can read.

Revocation stops new admission and says nothing about what already ran; read
`reported_spend` on the returned record for that.

Quota has the same hole and the same fix, in the other currency. Money and chips
are separate limits and a fleet can hold one while breaching the other -- a
tenant on a small budget can still exhaust an accelerator pool everyone else is
queued behind:

```
eyvan federation quota-grant issue eu-plane --org acme --project research \
  --tpu 8 --cpu 64 --expires-at 2026-12-31
eyvan federation quota-grant list
```

Dimensions you do not name are zero, not unlimited. A grantee's effective ceiling
is the tighter of the slice and any local quota, so a grant can never raise a
limit a local operator set. Revoking one stops new admission and leaves running
work alone: a quota is a concurrent ceiling, and evicting live work to enforce a
ceiling change would make revocation an outage rather than an administrative act.

## Storage

```
eyvan artifacts usage           # stored bytes against quota
eyvan artifacts collectable     # what the collector would remove, and why
eyvan artifacts references --uri eyvan://results/...   # who still needs it
eyvan artifacts hold --uri ... --reason "cited in the paper"
```

Read `collectable` before enabling collection, not after. It is the only view
that distinguishes "unreferenced and past its window" from "still referenced by
a queued workload", and those look identical in a bucket listing.

## Durability

```
eyvan capacity invariants       # I1-I9, checked against live state
```

An empty `violations` list is a real claim: every check has a test that makes it
fire, so silence means the checks ran and found nothing. The two invariants
about *change over time* -- budget monotonicity and storage growth -- cannot be
settled by one call and are not reported here.

## The unattended evaluator

The soak command is a fail-closed **host-side evaluator**, not a provider or
hardware attestation. A proof run supplies one opaque 256-bit proof ID, a frozen
configuration snapshot, and the exact workload/lease/cell joins whose durable
run transitions it expects:

```
eyvan soak \
  --candidate-sha <full-candidate-sha> \
  --source-sha <full-source-commit-sha> \
  --proof-id sha256:<64-lowercase-hex> \
  --config <sanitized-cpu-run-config.yaml> \
  --config <sanitized-tpu-run-config.yaml> \
  --config-snapshot-dir <durable-local-snapshot-directory> \
  --expect-execution <cpu-workload>,<cpu-lease>,<cpu-cell>,command,false,none,eyvan-native,gcp-compute,eyvan-native,gcp,cpu,spot \
  --expect-execution <tpu-workload>,<tpu-lease>,<tpu-cell>,callable,true,none,eyvan-native,gcp-tpu,eyvan-native,gcp,tpu,spot \
  --max-rss-growth-bytes <bound> \
  --max-fd-growth <bound> \
  --max-database-growth-bytes <bound> \
  --max-artifact-net-growth-bytes <bound> \
  --hours 72 --interval-s 60 --output soak.json
```

All numeric options are parsed as bounded decimal/integer text inside the
command, after the output path is known. Decimal coefficients, digit counts and
base-10 exponents are bounded explicitly; hours-to-seconds scaling and slot
ceiling use Python integers and do not consult the process-wide Decimal context.
`nan`, `inf`, out-of-bound exponents or coefficients, counter overflow, and an
excessive observation grid produce an atomic `not_started`/`inconclusive`
receipt. A duration or interval whose shortest float rendering does not
round-trip to the same Decimal is rejected rather than rounded into different
proof evidence. In particular, `--hours 0.07 --interval-s 0.1` means exactly
252 seconds and 2,520 slots even when the ambient Decimal precision is one; no
binary-float or Decimal-context rounding changes that grid.

The command reads each `--config` once, bounds the total bytes/file count, and
atomically materializes those captured bytes under their combined digest in
`--config-snapshot-dir`. It never polls the mutable source paths afterwards.
The workloads used by the run must be launched from that frozen snapshot and
must persist both bindings in `WorkloadDefinition.metadata`:

- `eyvan.soak.proof_id` = the exact `--proof-id`; and
- `eyvan.soak.config_sha256` = the running receipt's `config_hash`.

A matching string is durable state evidence, not proof that a remote process
actually read those bytes. Keep the frozen snapshot and provider-side evidence
with the qualification bundle. Start observation before the listed leases cross
their durable run boundary. Coverage advances only for an exact manifest join
where the workload is past queueing, `run_started_at` is set, the workload/lease/
cell IDs agree, the cell generation is current and READY/DRAINING, and the two
metadata bindings match. Unrelated fleet traffic, a PENDING workload, a stale
cell generation, or three independently matching capability/backend/provider
projections cannot satisfy one expected **joint** identity. The evaluator
requires at least one CPU and one TPU joint path. Provider/resource fields come
from durable records and are explicitly reported as
`durable_control_plane_state`; `hardware_attested` remains false.

Only `timeout` and transport `unreachable` failures may consume the two explicit
availability limits. Authorization, server, request/schema/counter overflow,
filesystem, and internal failures always block pass even when the availability
limits are non-zero. Budget samples carry stable period identity and
`period_started_at` separately from revision. Revision may advance during
ordinary settlement and spend is still compared; a reset is accepted only when
both period time and revision prove a forward period transition.

Receipts have a closed export schema. Reasons are fixed codes with counts;
violation subjects are domain-separated `sha256:` IDs; detail text is discarded.
Invalid candidates, sources, participant specs, arbitrary extra reasons,
exception text, tenant/project names, URIs, buckets, and secrets are never
copied into the receipt. The output is fsynced and atomically replaced with a
blocking receipt before the first request and after every attempt.

The evaluator also requires bounded RSS peak growth, FD peak growth, database
peak growth and artifact net growth; reconciled spend; and final cleanup with an
empty provider inventory. These values must arrive as proof-bound
`eyvan.soak.external-evidence.v1` samples. The current control-plane invariant
endpoint deliberately returns `p8_evidence: null`: it cannot truthfully infer
process/DB size or provider inventory from durable cell rows. Therefore a run
against the current endpoint exits **2 / inconclusive** with explicit
`*_evidence_missing` codes. It must not be called a P8 result.

Verdicts are:

- **1 / fail** — a persistent invariant, monotonicity defect, exceeded growth
  bound, unreconciled spend, residual provider inventory, or incomplete cleanup
  was observed;
- **2 / inconclusive** — the run/input/evidence/coverage is incomplete or a
  non-transient observation error occurred; and
- **0 / pass** — reserved for the evaluator only when every closed criterion is
  supplied and satisfied. It is still not hardware attestation by itself.

No receipt in this repository claims a provider, TPU, GPU, or P8 qualification
run has completed; see [proofs.md](proofs.md).


## Debugging one workload

```
eyvan workload status <id>
eyvan workload explain <id>
eyvan workload process-status <id>     # live process detail
eyvan workload logs <id>               # concatenated text
eyvan workload log-page <id>           # structured, per-rank, resumable
eyvan workload children <id>           # the job tree
eyvan workload exec <id> -- py-spy dump --pid ...   # a diagnostic beside it
```

`workload exec` runs *next to* the workload, not inside its cgroup: an exec'd
process sharing the unit's memory allocation could take the trainer down with
it.

## Break glass

```
eyvan cell-drain <id>
eyvan cell-delete <id>
eyvan preemption-force-finalize <command-id> --reason "..."
eyvan system-reconcile
```

These are CLI-only and deliberately not on the dashboard. Each is irreversible
in a way a mis-click should not be able to reach.
