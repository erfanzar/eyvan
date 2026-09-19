# Troubleshooting

Symptom first, because that is what you have.

## A workload sits PENDING

`eyvan workload explain <id>`. It reports the scheduler's actual verdict, not a
guess. The five answers and what each means:

- **no matching pool** -- the request names a shape, provider or region no
  enabled pool offers. `eyvan capacity list` shows what exists.
- **quota** -- `eyvan quota get`. The limit binds per (org, project, principal),
  so a per-principal quota can bind while the project has room.
- **budget** -- `eyvan budget status`. A hard limit refuses admission; a soft
  one does not.
- **policy** -- `eyvan policy get`. Provider, region and capacity-class
  allow-lists live here.
- **capacity** -- the pool matches and has none. `eyvan capacity queue-trend`
  says whether this is a moment or a pattern.

## A workload is RUNNING and nothing is happening

`eyvan workload process-status <id>` shows live process detail including the
control group and pids. `eyvan workload exec <id> -- py-spy dump --pid <pid>`
runs a diagnostic beside it, with `EYVAN_SOURCE_JOB_ID` and
`EYVAN_SOURCE_CONTROL_GROUP` set so the tool can find its target.

## A multi-host job's logs are confusing

`eyvan workload log-page <id>` instead of `workload logs`. The latter
concatenates with `===== rank N =====` banners; the former keeps records
attributed per rank and resumable by cursor, and reports an unreachable rank *in
place* rather than omitting it.

If the attempt changed mid-read you get one `log.attempt_changed` record and a
reset cursor. Silently continuing would show the new attempt's first lines
appended to the old attempt's tail, and you would conclude the job resumed.

## Cells exist that nothing claims

`eyvan capacity orphans` lists cells an agent still holds that this control
plane does not recognize. `eyvan capacity invariants` reports I1 for cells whose
pool is gone and I9 for leases pointing at cells that are not.

Both sweeps run on the leader loop and wait out a grace period first, so give it
a cycle before intervening: a cell mid-provisioning looks exactly like a
stranded one.

## Capacity is billing and no tenant is charged

I2 in `eyvan capacity invariants`. A lease its workload finished with, vanished
from, or retried away from used to be reachable by nothing; `settle-orphan-leases`
now reclaims it, and I5 catches the accounting half -- a budget disagreeing with
the sum of its settled leases.

## A workload will not leave CANCELLING, and its cell will not release

Check the head's log for `cannot safely verify that systemd execution ...
stopped`, repeating with the same outbox command id. The worker stopped the
process, its transient unit disappeared before recording a terminal state, and
older builds then re-derived that unknown on every retry and refused to confirm
the stop -- so the lease was never released and the accelerator went on billing.
Builds after that fix treat "systemd holds no such unit" as proof nothing is
running; an unreachable systemd still refuses, which is the case the refusal is
for.

If the plane in front of you is older, or systemd genuinely cannot be reached:

```bash
eyvan workload-force-finalize <id> --reason "<what you checked>"
```

Break glass. It settles the workload and **gives up the cell rather than
returning it**: the process may still be running on a host nobody could stop, so
scheduling onto that cell again would be worse than the deadlock it escapes.
Delete the cell afterwards with `eyvan cell-delete`, and confirm at the provider
that the capacity is gone -- a released cell is not a deleted VM.

## Alerting before an operator has to notice

`eyvan_invariant_violations{invariant="I3"}` and friends are published on the
metrics export interval, one series per named invariant, zero when satisfied.
They are the plane's own durability checks -- I1 stranded cells, I2 leases
outliving their workloads, I3 a stuck outbox command, I9 a lease naming a cell
that is gone -- and until they were exported the only way to see one was to run
`eyvan capacity invariants` by hand.

Alert on any of them being non-zero for more than a scrape or two. I3 and I9 were
both true of a live fleet for twenty minutes on 2026-08-12 while four TPU VMs
billed, and each was found by reading journald rather than by anything saying so.

## The whole leader loop is failing every cycle

`global scheduling reconciliation failed` in the head's log, repeating on the
scheduler interval, means one leader phase raised and everything after it in
that cycle did not run: no observation, no settlement, no release. The workloads
look frozen because they are.

The message names the phase in its traceback. Historic cause worth recognizing:
`lease references unknown cell` from `reconcile_capacity_pools_once`, which
happened when a pool stopped supplying capacity -- `eyvan down`, or a cell
entering `draining` -- while one of its cells still held a live lease. Fixed by
keeping a leased cell in the snapshot as draining, and by isolating that phase
so a planner can no longer stop the phases that release capacity.

If you meet it on an older build, re-enabling the pool (`eyvan up <fleet.yaml>`)
clears it immediately, which is also how to confirm you are looking at this and
not something else.

## Something behaves like an older build than the one deployed

```bash
eyvan capacity builds
```

Every component of Eyvan reports the same version string, so it cannot tell you
whether the agent holding your capacity is the code you shipped. This puts the
control plane's package digest beside every reachable agent's.

Each cell's native worker ranks are listed under their agent. They are the
processes that run the workload, and they install from the wheel their cell was
provisioned with -- which a re-staged head does not change, so an agent that
matches says nothing about them.

It exits non-zero on proven skew and also when agreement could not be shown --
an unreachable agent, a rank reporting no digest, or an agent predating the
route -- because none of those agreed with anything. Only endpoints holding a
cell are scanned; a pool with no cell is named rather than passed over.

## A provider keeps failing

`eyvan cells` shows `provider_failure_code` and `unavailable_detail`. The codes
route differently on purpose:

- `PREEMPTED` -- spot reclamation. Expected, cheap to retry, charged to
  `max_preemption_retries`, and deliberately **not** recorded as evidence the
  pool is empty.
- `STOCKOUT` -- this pool has nothing. Another zone might.
- `QUOTA_EXCEEDED` -- nothing will, until a human raises the limit.
- `CONFIGURATION` -- will fail identically forever. Read
  `unavailable_detail`; it carries the provider's own words.
- `PROVIDER_UNAVAILABLE` -- the API, not the capacity. Retried.

## A replica is serving errors

`/readyz` reports whether that replica can reach its database; `/healthz`
reports only that the process is alive. A replica failing readiness leaves the
Service endpoints on its own. If *every* replica is failing readiness, the
database is the problem and no amount of restarting helps -- which is why
liveness deliberately does not check it.

## The audit trail

`eyvan events` is scoped to (org, project). Every capability redemption appears
there, granted or refused, and a refusal with no matching issuance in the same
attempt is the shape a leaked token takes.
