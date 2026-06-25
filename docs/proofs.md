# Outstanding hardware proofs

Everything in this package is CPU-verified: 2652 tests, real PostgreSQL, seven
import contracts, every new behaviour mutation-checked. None of that is evidence
about a TPU.

This page exists so approving a spend is a decision about a known quantity. Each
proof states what it establishes, the exact commands, the pass criterion, the
shape, and roughly what it costs. Nothing here has been run.

**The criteria are executable.** `eyvan proof catalog | preemption | warm-window
| secret` evaluates the stated criterion against the live plane and exits 0 on
pass, 1 on failure, and **2 when the run did not exercise the behaviour at all**.
That third verdict is the one that matters: a spot slice that was never
reclaimed, a pool that never got a cell, an observation taken outside the warm
window -- each produces a clean-looking run that establishes nothing, and reading
it off a screen at the end of a spend is exactly when nobody is inclined to
notice. The judgement lives in `eyvan/sdk/commands/proof.py` and is tested
without hardware; the commands only fetch.

P2 and P3 stay procedural on purpose: they need a host killed mid-stream and a
job that submits its own children, and an "automated" verdict over a run whose
interesting half is manual would be worse than none.

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
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan capacity pool get research/v5p --json     # resources from the catalog, not the YAML
eyvan cells                              # observed_tpu_vms == catalog hosts
eyvan down --yes
```

```
eyvan proof catalog --accelerator-type v5p-16      # exit 0, 1 or 2
```

**Passes when** the pool's resources match the catalog entry for `v5p-16` and
`observed_tpu_vms` is 2, with no `resources:` block in the fleet YAML.

**Run 2026-08-06, exit 0.** v5p-16 spot in `us-central1-a`, slice ready 7m9s
after apply. With no `resources:` block the pool resolved to `cpu 32, tpu 8` --
the catalog's `host_cpu` and `chips` -- and the cell reported
`observed_tpu_vms: 2`, the catalog's `hosts`. The provider is authoritative for
how many slices; the catalog is authoritative for hosts per slice, which is the
property a provider returning one host of two would violate silently.

One thing this exposed: the catalog sets `host_memory_gib` for no shape, so a
pool built from it advertises **zero memory**. `eyvan workload submit` defaults
to 1 GiB, so the default submit cannot place on a catalog-derived TPU pool at
all -- it reports `insufficient_capacity`, which is true and unhelpful. Making
`resources:` optional is what put that path in reach; nothing else exercises
it. A cell
that never reported `observed_tpu_vms`, or a catalog shape with no recorded host
count, is **inconclusive** rather than passing -- the check that matters would
otherwise be skipped silently.
**Shape** v5p-16, one slice. **~35 minutes.**

## P2 — multi-host structured logs, with a rank lost mid-stream

**Establishes** per-rank attribution and that an unreachable rank is reported
*in place* rather than omitted. A missing rank that looks like silence is the
failure this exists to prevent, and it needs two real hosts.

```
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name logs --command 'python -c "import time;[print(i) or time.sleep(1) for i in range(600)]"' --tpu 8
eyvan workload log-page <id>              # records attributed per rank
# kill one host out of band, then:
eyvan workload log-page <id> --after <cursor>
eyvan down --yes
```

**Passes when** the second page reports the killed rank as unavailable while
still returning the surviving rank's records, and the archived blob exists after
terminality so the cell can be released without losing logs.

**Attempted 2026-08-06: not runnable on this path.** `eyvan workload log-page`
answered `unavailable: true` with
`execution backend 'eyvan-native' does not expose structured logs`. Only
`systemd_transient` implements `StructuredLogExecutionDriver`; the remote
native-worker protocol that TPU fleets use does not, so per-rank attribution
cannot be demonstrated on the executor this proof targets. The route degraded
honestly rather than returning an empty page -- "this backend has no structured
logs" and "this workload produced no output" stay distinguishable, which is what
that code was written to preserve -- but the capability is absent where it
matters.
**Shape** v5p-16 multi-host. **~45 minutes.**

## P3 — in-job submission and exec

**Establishes** that a job submits children under its parent's identity, that the
width ceiling refuses at 409, that an attempt-1 capability stops working after a
retry bumps the generation, and that `exec` runs beside a trainer without
disturbing it.

```
eyvan up configs/eyvan-live-cpu-us-east5-a.yaml
# a callable submitted with can_submit_children=True that fans out three ways
eyvan workload children <parent>          # created_by is the parent's owner
eyvan down --yes

eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload exec <id> -- py-spy dump --pid <pid>
eyvan down --yes
```

```
eyvan proof children <parent-id> --expected 3 --late-submit-status 409
```

**Passes when** every child's `created_by` is the submitting human rather than
`workload:<id>`, the ceiling returns 409, a stale-generation token is refused,
and `py-spy` returns a stack from every rank with the trainer still RUNNING.

**Run 2026-08-05, exit 2 (inconclusive).** On a local cell, because the children
URL is the same loopback address the secret URL is and a remote worker cannot
reach it. A running job submitted three children with its capability, all
accepted 201; every child carried `created_by='erfan'` -- the submitting human,
not `workload:<id>` -- and every one was parented into the submitter's own
scope. Reusing the capability after the parent reached SUCCEEDED was refused
with 409, so the capability is bounded by the parent's state rather than by an
expiry.

Inconclusive because two criteria were not exercised: the live-children ceiling
needs 256 concurrent children, and the `exec` half needs a TPU -- the local
driver reports `supports_exec: False`, correctly, since a shell on this host is
already available without asking Eyvan for one.
**Shape** CPU fleet, then v5p-16. **~40 minutes.**

## P4 — secret delivery, per attempt

**Establishes** that a workload reads a Secret Manager secret and that the value
appears in no durable record, log line or API response; that the ledger shows one
issuance and one read; and that the capability is refused once the attempt ends.

```
eyvan policy set --allow-secret-reference gcp-secret://<project>/eyvan-proof-
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name secret-proof --command 'python -c "import os;print(len(os.environ[\"HF_TOKEN\"]))"' \
  --secret HF_TOKEN=gcp-secret://<project>/eyvan-proof-token --tpu 8
eyvan workload logs <id>                         # the length, never the value
eyvan events --org <org> --project <project> | grep secret.               # one secret.issued, one secret.read
eyvan down --yes
```

```
eyvan proof secret <workload-id> --value-file /tmp/expected   # 0600, deleted after
```

The value comes from a file rather than an argument: a secret on the command line
lands in shell history and in every process listing on the host.

**Run 2026-08-05, exit 2 (inconclusive).** The confidentiality half held: the
value appeared in no workload record and no audit payload, and one
`secret.issued` was written. Delivery did not happen -- the worker got
`Connection refused` fetching the secret, because this control plane advertises
`http://127.0.0.1:8081` and a GCE worker resolves that on its own host. A plane
whose `EYVAN_PUBLIC_URL` a worker cannot reach cannot run this proof; dispatch
now refuses such a workload immediately rather than failing eight minutes later.

**Passes when** the job prints the length, `eyvan events` shows exactly one
`secret.issued` and one `secret.read` for that attempt, and grepping the workload
record, the audit payloads and the logs finds the value nowhere. The audit
payloads are the half a person checking by hand forgets, and the one place a
well-meaning "context" field would put the value -- in the store retained for
ninety days.
**Shape** v5p-16. **~30 minutes.**

## P5 — spot reclamation is charged to the preemption budget

**Establishes** the Phase 8 classification end to end: that GCP reclaiming a spot
slice produces `PREEMPTED` rather than `PROVIDER_ERROR`, that it consumes
`max_preemption_retries` and not `max_retries`, that the pool is *not* marked
stocked-out, and that the workload resumes from its checkpoint.

```
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name preempt-proof --command '<checkpointing trainer>' \
  --tpu 8 --checkpointable --checkpoint-hook ... --checkpoint-output-uri gs://... \
  --max-retries 0 --max-preemption-retries 5
# wait for GCP to reclaim
eyvan cells                               # provider_failure_code == preempted
eyvan workload get <id>                   # preemption_count 1, capacity_loss_count 0
eyvan capacity list --json                       # the pool has no stockout cooldown
```

```
eyvan proof preemption <workload-id>
```

**Passes when** all four hold. `--max-retries 0` is deliberate: under the old
shared budget the workload would have failed, so this is the distinguishing case.
A run GCP never reclaimed exits 2: "we were not preempted" is not "preemption is
classified correctly". The cooldown check is scoped to this proof's own pool,
because an unrelated pool's backoff failing every preemption run is how a proof
becomes something people re-run until it is green.
**Shape** v5p-16 spot, run until reclaimed. **1–3 hours, opportunistic** — run it
alongside another proof rather than waiting on it.

## P6 — a warm window held and released across its own boundary

**Establishes** that a window configured in fleet YAML holds a cell warm through
its hours and releases after, and that the billing clock accrues idle cost for
exactly that period. Needs wall-clock time; nothing else does.

```
# add warm_windows to configs/eyvan-live-cpu-us-east5-a.yaml
eyvan up configs/eyvan-live-cpu-us-east5-a.yaml
eyvan capacity warmth --json                     # window_active, floor held
# after the window closes
eyvan capacity warmth --json                     # window_active false, floor released
eyvan capacity billing --json                    # idle cost matches the window
eyvan down --yes
```

```
eyvan capacity warmth --json > inside.json         # during the window
eyvan proof warm-window --pool research/cpu --inside inside.json   # after it closes
```

Two observations, because a floor that is held and a floor that is *never*
released look identical from inside the window.

**Passes when** the floor is held for the window's duration and released within
one reconcile interval of its end.

**Run 2026-08-05, exit 0.** Window 13:00-14:00 UTC on a spot `n2-standard-4` in
`us-east5-a`. Cell ready 5m12s after apply; floor held 1 of 1 through the window;
floor fell to 0 at the boundary; 9894s idle accrued $0.0305. Teardown released
the cell and deleted the VM -- slowly, because every provider call from a laptop
goes through gcloud, which is why the cell sat in `deleting` for six minutes
after the VM was already `STOPPING`. If the pool carries no `unit_prices` the cost
half exits 2 rather than passing: zero cost on an unpriced fleet is not evidence
that idle cost was measured, it is evidence that nothing could be.
**Shape** CPU fleet. **~1 hour** (window length plus margin).

## P7 — Kubernetes against a real API server

**Establishes** what `httpx.MockTransport` cannot: 409 `AlreadyExists` replay, a
leaked previous incarnation deleted rather than adopted, pods Pending past
`provisioning_timeout_s`, and with Kueue installed an `Inadmissible` Workload
mapping to `QUEUE_ADMISSION_DENIED` rather than terminal failure.

```
eyvan proof kubernetes --api-server https://<master-ip> \
  --token-file <token> --ca-file <ca.crt>
```

Credentials are files, not arguments: a bearer token on a command line lands in
shell history and in every process listing on the host.

**Passes when** each maps to the documented `CapacityProviderFailureCode`, and
`eyvan capacity pool` refuses a ClusterQueue whose preemption could evict Eyvan's
work.

**Run 2026-08-05, exit 0.** GKE 1.35 in `us-east5-a`, Kueue v0.10.1, one
ClusterQueue with preemption `Never`/`Never` and one with
`LowerPriority`/`Any`. No queue and the safe queue were allowed; the unsafe
queue and a missing one were both refused with `configuration`, the documented
code. The transport itself was exercised for the first time against a real API
server: `get_job` on an absent Job returned `None`, `create` returned `True`
then `False` on replay -- a 409 `AlreadyExists` handled as idempotent rather
than raised -- and a Foreground delete returned 200.

Everything decided here is driver-to-API-server, so no workload had to run and
no worker had to reach the control plane. What that leaves unproven is the full
lifecycle: pods registering back and a workload executing need a control plane
the cluster can route to, which a laptop advertising loopback is not.
**Shape** GKE, CPU node pool then one GPU node pool. **~2 hours, modest cost.**

## P8 — the soak

**The one the plan calls disqualifying if absent.** Everything above is minutes
to hours with a human watching. This is the only evidence about days.

```
eyvan up configs/eyvan-live-cpu-us-east5-a.yaml
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
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

**Run 2026-08-05: a 30-minute soak, exit 0. This is not P8.** 30 observations,
0 unreachable, no invariant violated once let alone persistently, and no
over-time finding. 76 workloads churned through a local fleet during it -- short
jobs every 25s and a three-minute job every tenth -- so the leases, settlement
and outbox paths were genuinely exercised rather than watched while idle.

What it does not establish is what P8 is for. Thirty minutes is not three days,
and the plan is explicit that the duration is the load-bearing part. There was
no TPU fleet. And `artifact_growth` was 0 because command workloads store no
results, so the one criterion this page singles out -- whether collection runs
at all -- was not exercised. A shorter soak answers "did anything break just
now", which is the question that was already answerable.
**Shape** CPU fleet plus v5p-16. **72 hours.** This is the largest single spend
here and the only one that cannot be shortened without changing what it proves.

---

## Order

P5 is opportunistic — start it and let it run while P1 through P4 execute on
other slices. P6 needs wall-clock time and nothing else, so it can overlap
anything. P8 goes last, after the others have confirmed the individual
behaviours it assumes.

## A defect this page's runs found, still open

**A cancel the worker cannot complete makes a slice unreleasable.** Observed
2026-08-06 on the v5p-16 in `us-central1-a`. The native worker answered
`500 Internal Server Error` on
`POST /api/v1/workloads/<id>/stop`; the agent retried 24 times; the workload
stayed in `cancelling` for the rest of its life, never released its lease, and
`eyvan down` could not drain the cell -- 13 minutes in `draining` with the
queued resource still ACTIVE. The slice had to be released with
`gcloud compute tpus queued-resources delete --force`.

Two things make it worse than a stuck job. The capacity keeps billing, and it is
a TPU. And `eyvan capacity invariants` reported **no violations** throughout.

That second half is fixed: I3 listed only PENDING and PROCESSING commands, and a
command that keeps failing lives in FAILED between attempts -- so the check could
only ever catch a command nobody had picked up. With FAILED included it now names
both stuck commands with their topic and age, verified against this exact live
state (`workload.cancel` failed for 3198s, `capacity.drain` for 2227s).

**The state itself is still unrecoverable, and that is the open half.** The chain
deadlocks: the workload never leaves `cancelling`, so its lease stays active, so
`eyvan cell-delete` refuses with *"still owns 1 active lease(s)"*, so the cell
stays `draining`, so `eyvan down` never completes. `preemption-force-finalize`
does not apply -- it attests *preemption* victims, and this is an ordinary
cancel. There is no operator command that resolves it, and a subsequent fleet in
the same zone did not provision while it persisted.

Bounding the retry is not obviously right either: forcing the workload terminal
means declaring a process dead that the worker never confirmed stopping, which is
a decision about replacing the cell rather than a retry constant. Making it
visible was the part that could be fixed without deciding that.

## What a failure means

Every proof above has a CPU-verified counterpart that already passes. A failure
here therefore means the CPU test was testing the wrong thing — not that the
feature is broken in a way the suite could have caught. That is the point of
running them, and it is why "the tests pass" has never appeared in this
repository as an argument that something works on hardware.
