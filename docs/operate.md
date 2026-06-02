# Operating Eyvan

## Every day

```
eyvan status                    # fleet-level summary
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

## The unattended run

```
eyvan soak --hours 72 --interval-s 60 --output soak.json
```

Polls the invariants, keeps each tick's snapshot so the over-time properties can
be decided at all, and exits non-zero if anything was violated *persistently*. A
violation seen once is not a finding: the point-in-time checks race with the
sweeps that fix them, and a run that failed on that race would fail every time
while proving nothing.

It does not stop on the first problem, and an unreachable control plane does not
end it. Surviving a restart is among the things the run exists to observe.

This is the evidence no code change substitutes for. Everything else Eyvan
claims was proven with a human watching, over minutes.

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
