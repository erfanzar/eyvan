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
