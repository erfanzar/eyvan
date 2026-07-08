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

**Run 2026-08-07, exit 2 (inconclusive).** Runnable now: the worker exposes
`/log-page`, `NativeWorkerClient` fetches it, and `NativeSliceExecutor` fans out
across the hosts inside a cell. On a CPU cell with the current wheel the page
carried 50 records, all timestamped, stdout and stderr preserved, and real
journald cursors -- everything structured logs add over concatenated text.

Inconclusive on the two checks that need a slice: a single host cannot show
per-rank attribution, and no rank was lost, so the report-in-place path was not
exercised. Both say so rather than passing. Killing a host mid-stream on a
two-host slice is what remains, and it needs a v5p-16 carrying the new worker
wheel.

Two defects were found by reading one real page, both invisible to the unit
tests: the decoder fed an already-decoded stream name back through the *priority*
mapper, so every stdout and stderr read as `unknown`; and the control plane's
decoder omitted `timestamp` entirely, which matters because `merge_pages` orders
by it -- every multi-host page would have collapsed to rank order with the
interleaving silently gone.

**Superseded, kept for the record -- 2026-08-06: not runnable on this path.** `eyvan workload log-page`
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

**Run 2026-08-07, exit 2 (inconclusive).** A v5p-16 spot slice in
`us-central1-a` ran a checkpointing workload for 4h20m with
`--max-retries 0 --max-preemption-retries 5`. GCP never reclaimed it, so the
classification was never exercised and the verdict says exactly that:
*"the slice was never reclaimed; nothing about classification was exercised"*.

This is the outcome the three-verdict scheme exists for. Four hours of an
unpreempted slice is not evidence that a reclamation would be classified
correctly, and a pass/fail verdict would have had to call it a pass. The run was
stopped rather than extended: the slice bills the whole time it waits, and the
plan's own estimate of 1-3 hours had already been exceeded by half again.

Do not simulate the reclamation to close this. Deleting the VM by hand produces
no GCE preemption signal, so it exercises the provider-error path instead and
would report a failure that is not a defect.
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

## P8 — the soak (not yet qualifiable)

P8 is the release-candidate 72-hour mixed CPU + TPU run described by the
production-readiness plan. **No qualifying P8 receipt exists.** The current
`eyvan soak` command closes false-pass paths in the evaluator, but the live
control-plane route does not produce the independent resource, database, cost,
or provider-inventory evidence required for a P8 decision.

The intended evaluator invocation is documented in
[operate.md](operate.md#the-unattended-evaluator). Its immutable inputs are:

- full candidate and source SHAs;
- one opaque `sha256:` proof/run ID;
- a materialized content-addressed snapshot of every execution config;
- exact workload/lease/cell participant joins with expected **joint**
  capability/backend/provider identities, including CPU and TPU; and
- finite explicit bounds for RSS, FD, database and artifact net growth.

Every participating workload must persist the same proof ID and frozen config
digest in `eyvan.soak.proof_id` and `eyvan.soak.config_sha256` metadata and be
launched from the frozen snapshot. The scoped invariant route counts a path only
after a non-PENDING/non-ADMITTED workload and non-PENDING lease have a durable
`run_started_at`, the exact lease joins the exact workload and cell, the cell is
READY/DRAINING at the fenced generation, and both metadata bindings match.
Counters are keyed by opaque participant ID plus the whole joint identity;
unrelated workload, artifact, budget, lease, or cell traffic is excluded. This
is control-plane state evidence only: the receipt fixes
`hardware_attested: false`.

The receipt never exports raw violation subjects/details, invalid identity
inputs, arbitrary reason text, project/tenant names, locations, URIs, buckets,
or exception messages. It contains only closed reason codes/counts, opaque
subject/participant/scope IDs, closed identity enums, and bounded numeric
summaries. Only timeout/unreachable windows may consume availability budget;
auth, server, request/schema/overflow, filesystem, and internal errors make the
result inconclusive regardless of that budget. Bounded Decimal CLI text is
reduced to integer coefficient/exponent ratios, so process-wide Decimal
precision cannot change scheduling (`0.07h / 0.1s = 2520` slots); input whose
shortest float rendering does not round-trip to the same Decimal is rejected.
Budget spend remains monotone across normal revision increments and
may reset only after a separately proved forward period transition.

A real P8 evaluator additionally needs a proof-bound sample at every successful
observation with:

- process RSS and open-FD counts, judged by explicit peak-growth bounds;
- database bytes, judged by an explicit peak-growth bound;
- spend-reconciliation status;
- provider inventory count; and
- cleanup completion.

The final receipt also requires artifact creation and collection, artifact net
growth within its bound, reconciled spend, cleanup complete, and final provider
inventory exactly zero. Missing evidence is never inferred from `cell_count` or
from an absent error.

The current `/api/v1/system/invariants/soak` response returns
`p8_evidence: null` deliberately. It has no trustworthy process/DB-size or live
provider-inventory source. Consequently current live invocations end
`inconclusive`/exit 2 with explicit codes including
`rss_growth_evidence_missing`, `fd_growth_evidence_missing`,
`database_growth_evidence_missing`, `spend_reconciliation_evidence_missing`,
`provider_inventory_evidence_missing`, and `cleanup_evidence_missing`. The docs
do **not** define exit 0 from the current endpoint as P8.

Future work may connect a bounded, authenticated external monitor that emits the
closed `eyvan.soak.external-evidence.v1` sample. Only after that source is
implemented, independently reviewed, and exercised against the frozen candidate
may exit 0 mean that the **evaluator criteria** passed. Provider operation and
CPU/TPU hardware qualification still require their own immutable evidence; the
durable state receipt alone is not attestation.

**Passes when** a future reviewed external monitor supplies every proof-bound
sample, the evaluator returns exit 0 with every closed criterion satisfied, and
the separate immutable provider/hardware evidence confirms the claimed CPU/TPU
operations. The current endpoint cannot meet this criterion and exits 2.

**Shape** Release-candidate mixed CPU + TPU fleet for 72 hours, exercising short
callables, a checkpointed trainer, batch/DAG/service activity, artifact
lifecycle, controlled control-plane/agent/database restarts, cost reconciliation
and complete provider cleanup. A timeout or unavailable required provider event
is inconclusive, not pass.


---

## Order

P5 is opportunistic — start it and let it run while P1 through P4 execute on
other slices. P6 needs wall-clock time and nothing else, so it can overlap
anything. P8 goes last, after the others have confirmed the individual
behaviours it assumes.

## Historical notes are not proof receipts

Unversioned operator notes and point-in-time invariant output can motivate a
fix, but they do not identify an immutable candidate, prove the requested
duration, or show which criteria were exercised. They are intentionally not
promoted to qualification claims on this page. Only an immutable versioned
receipt stored outside git can change the supported evidence matrix.

## What a failure means

Every proof above has a CPU-verified counterpart that already passes. A failure
here therefore means the CPU test was testing the wrong thing — not that the
feature is broken in a way the suite could have caught. That is the point of
running them, and it is why "the tests pass" has never appeared in this
repository as an argument that something works on hardware.
