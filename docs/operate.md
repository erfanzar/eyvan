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

## Running a job on accelerators

Two commands. Apply a scale-from-zero fleet, then submit the workload:

```
uv run --locked eyvan up ~/.eyvan-configs/eyvan-live-v6e8-pods2-us-central2-b.yaml
uv run python scripts/eyvan_esurge_multislice.py \
    --accelerator-type v6e-8 --pods 2 --confirm-provider-mutations
```

The fleet scales from zero, so nothing bills until a workload asks for it, and
the cell is released when the workload finishes.

**Do not provision with raw `gcloud` instead.** Eyvan is the thing under test:
driving GCP directly yields evidence about the workload framework and none about
the control plane, and a hand-run job is not a result. If Eyvan cannot express
what you need, that gap is the finding — report it rather than working around
it. A `PreToolUse` hook denies raw TPU provisioning for exactly this reason;
`list`, `describe`, `ssh` and `delete` remain available for diagnosis and for
reaping strays after a crash.

Shape guidance, measured 2026-08-15 in `example-project`: request **2x v6e-8**
(16 chips, a real multislice) rather than 2x v6e-16. The 32-chip shape was
accepted in all four zones holding v6e spot quota and reclaimed before a host
finished booting, three separate times; 16 chips were granted in about two
minutes, repeatedly.

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

## Where can I run this?

```
eyvan capacity quota                        # provider TPU quota, usage, and free balance
eyvan capacity quota --fits v6e-8:2         # only zones that fit two of that slice
eyvan capacity quota --zone us-central2-b   # bound the provider read to one zone
eyvan capacity quota --refresh              # re-read instead of reusing a cached reading
```

The same limits and live usage `eyvan autoscale generate` reads, for the person
choosing a zone rather than only for the generator. Spot and on-demand are
separate provider quota metrics and are reported separately, because capacity in
one cannot be spent as the other.

Read the `unknown` rows as carefully as the numbers. A zone can have a published
quota limit and no usage series behind it, and treating that as zero usage would
advertise a full zone as entirely free -- so those zones are listed under their
own heading with no balance at all. They are not proven empty and not proven
full, and `--fits` keeps them visible rather than answering a question it cannot
answer.

Every reading states its own age, including a cached one. The provider's usage
gauge lags by minutes at best, so a balance without its timestamp is a guess
wearing a number. The same view is on the dashboard's **Quota & capacity** page,
which reads the same route.

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

## Native-worker ingress rules outlive their heads

Provisioning never deletes a native-worker ingress firewall rule, and that is
deliberate: the rule has to survive a scale-to-zero so the next scale-up can
still reach its workers. `ensure` in
`eyvan/capacity/providers/gcp/native_worker_ingress.py` creates and reconciles
it. `eyvan head down` deletes the head's *own* rules, the control-plane port and
SSH, which is why a clean teardown still leaves these behind.

Removal is therefore an operator decision rather than an automatic one, and
`eyvan capacity ingress` is where it lives. It runs locally against gcloud, with
no control plane involved, because that is how the rule was created:

```
eyvan capacity ingress list --project <p>
eyvan capacity ingress release --project <p> --rule <name>
eyvan capacity ingress release --project <p> --all      # prompts unless --yes
```

`release` re-reads each rule and refuses anything without this runtime's exact
ownership marker, so it cannot remove a rule someone else made -- nor the
`eyvan-control-plane-head:v1` rules a live head is using, which carry a
different marker and are not native-worker ingress at all.

One accumulates per head, because the target tag is
`HMAC(native_worker_root_secret, project_id)` and each head is given its own
secret. The source range is the public IPv4 of whatever machine ran the CLI, so
a laptop on a new network adds another. GCP allows 100 firewall rules per
project by default, and reaching that stops *every* fleet in the project from
provisioning -- including other people's, on a shared project.

List them, and read before deleting:

```
gcloud compute firewall-rules list --project <p> \
  --filter='description ~ eyvan-native-worker-ingress' \
  --format='table(name, creationTimestamp, sourceRanges.list(), targetTags.list())'
```

A rule is reclaimable when no live head answers on the IP in `sourceRanges` and
no instance carries its `targetTags`. Neither is implied by the cells being
gone: the tag belongs to the *head's* secret rather than to a cell, so a rule
whose slices were all deleted may still be the one a running head needs to reach
the workers it provisions next. Delete on that evidence, not on age.

```
gcloud compute firewall-rules delete <name> --project <p>
```

Observed 2026-08-13: two rules dated 2026-07-13 and 2026-08-10 in
`example-project`, both orphaned, after a `head down` that correctly removed
the two rules it did own.

## Several GPU ranks on one host

A GPU gang places one process per native-worker endpoint. By default a host
carries one of them: that process is handed every device Eyvan gave the machine
and is expected to launch one worker per device, so `WORLD_SIZE` counts devices
and `RANK`/`LOCAL_RANK` are left to that launcher. A gang whose endpoints name
one machine twice is *not* GPU-coordinated -- both ranks would be told they own
all of it -- and is dispatched with whatever device visibility the submission
sent, which is the behaviour every release so far has had.

Set `EYVAN_GPU_RANK_LAYOUT=rank-process` on an **agent** to say the opposite:
that the process Eyvan starts is itself the rank. Several ranks may then share a
host, each gets a disjoint slice of that host's devices, `LOCAL_RANK` names which
slice, and `WORLD_SIZE` counts processes rather than devices. A host whose device
count does not divide evenly among the ranks placed on it, or whose two cells
disagree about what it carries, is left uncoordinated rather than split on a
guess. The variable is spelled `host-launcher` or `rank-process`; anything else
is refused when the agent starts rather than read as the default.

This is a device *assignment*. No GPU has executed an Eyvan workload; see
[proofs.md](proofs.md).

Two independently scheduled workloads packed onto the same cell are two gangs,
and they are still given the same devices -- Eyvan records no per-lease device
range.

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
`eyvan.soak.external-evidence.v1` samples, and the control plane never invents
them: it cannot truthfully infer its own process size or provider inventory from
durable cell rows. With no external monitor running the invariant endpoint
returns `p8_evidence: null` and the run exits **2 / inconclusive** with explicit
`*_evidence_missing` codes.

Run the producer beside the control plane for the whole soak:

```
eyvan soak-monitor \
  --proof-id sha256:<64-lowercase-hex> \
  --control-plane-pid-file <supervisor-pid-file> \
  --database-url <postgresql-url> \
  --provider-inventory-command <script printing the provider's live cell count> \
  --interval-s 30
```

It measures RSS and open descriptors of the control-plane *process* from
outside it, database bytes from PostgreSQL (or `--state-path` for a local
plane), spend reconciliation from the ledger, live cell count from the
provider, and cleanup from the fleet's own cells. A field it cannot establish
suppresses the whole sample rather than becoming a zero, and a sample older
than `EYVAN_SOAK_EVIDENCE_MAX_AGE_S` reads as absent, so a dead monitor makes a
run inconclusive rather than passing on its last reading. Post and poll the same
replica: the sample is held in the process that received it.

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


## Starting from nothing

```
eyvan quickstart          # what is missing between here and a first job
eyvan quickstart --yes    # ...and run that first job
```

Each step is either already true or printed as the exact command that makes it
true: a control plane, an applied fleet, capacity to run on. It stops at the
first missing one rather than reporting all three, because the next thing to
type is the only thing worth knowing.

It never provisions and never submits on its own. Standing up a head and
allocating accelerators cost real money, so the first prints the command and
the second needs `--yes`. Before submitting it prints what the job will cost,
and says *unknown* rather than `$0.00` when a pool declares no price -- a
confident zero is the number somebody budgets against.

## Reading anything

```
eyvan list                       # the vocabulary
eyvan list workloads --filter state=running
eyvan list cells
eyvan get workloads wl_abc123
```

One grammar over `workloads`, `cells`, `pools`, `leases`, `events`, `fleets`,
`quotas` and `budgets`, each emitting the same JSON every other command emits.
A filter a resource does not accept is refused by name rather than forwarded
and ignored -- a listing that silently returned everything would look filtered.

The per-noun commands stay. `workload explain` and `workload estimate` say
things no record projection can.

## Submitting one job

```
eyvan run --tpu v6e-8 -- python train.py            # shape, not a pool name
eyvan run --tpu v5p-8 --pods 2 -- python train.py   # two slices, allocated together
eyvan run --tpu v5p-8 --zone us-east5-b -- python train.py
eyvan run research/v5p-workers -- python train.py   # pin one pool exactly
eyvan run --no-wait --tpu v6e-8 -- python train.py  # for scripts: JSON on stdout
```

`--tpu`, `--gpu` and `--cpu` name hardware; the fleet is asked which pool
provides it, and interchangeable pools of that shape in other zones become
alternates the planner can fall to. Naming `FLEET/TYPE` still pins one pool.

`--pods N` is the multislice knob and means exactly what `TpuConfig(pods=N)`
means to the decorator: N slices allocated atomically, all or nothing. A pool
that cannot host that shape refuses before anything is provisioned.

`--zone` is a **constraint, not a preference**. With it set only a pool in that
zone may run the job and no cross-zone alternate is offered, which is the point:
a job pinned to data or to a reservation that quietly ran elsewhere has lost the
reason it was pinned.

The job streams to stdout and its progress to stderr; the command exits 0, 1 or
2 for succeeded, failed or cancelled, so `eyvan run ... && next-step` means what
it looks like. **Ctrl-C detaches and does not cancel** -- the job is durable and
outlives whoever submitted it. Reattach with `eyvan workload logs ID --follow`.

## Naming a job, and naming a batch

```
eyvan workload cancel wl_9f2c...              # a full id
eyvan workload cancel baseline-run            # its name, when only one carries it
eyvan workload cancel --prefix sweep-lr- --yes
eyvan workload kick --prefix sweep-lr- --yes  # move a whole sweep off a bad zone
```

`cancel`, `retry`, `checkpoint` and `kick` accept a full id, a unique id prefix
or a name. An ambiguous reference lists the candidates rather than picking one:
the command that follows is usually a cancel, and picking wrong ends somebody
else's run.

`--prefix` applies to every match. More than one match is listed and refused
until `--yes`, and an empty prefix is refused outright -- everything starts
with the empty string.

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

It runs on **every rank** by default, because the question exec usually answers
is "which rank is stuck" and one rank cannot tell a hung collective from a slow
step. A rank whose host cannot be reached is reported in its place rather than
omitted, so a half-dead slice reads as half-dead instead of as complete.

It is off until a fleet asks for it. Set `EYVAN_CAPACITY_SUPPORTS_EXEC=1` in the
head's environment and restart **both** `eyvan-head.service` and
`eyvan-head-agent.service` -- the capacity driver the gate consults lives in the
agent, so restarting the head alone changes nothing. Without it the plane refuses
with *"this fleet does not permit exec"*. Off by default because the command runs
whatever it is given on a machine holding someone's training job, so it is an
admission decision an operator makes for a fleet rather than a capability that
arrives switched on.

## Where the time and the money went

```
eyvan workload timeline <id>     # submitted, admitted, leased, dispatched, settled
eyvan workload cost <id>
eyvan workload cost <id> --on-demand-price 4.00
```

`timeline` is one column with elapsed times, built from the audit ledger and
ordered by its sequence rather than by clock -- two events committed together
share a timestamp, and sorting those by time reorders cause and effect. This is
what turns "it failed" into "it spent eight minutes queued, thirty
provisioning, and failed two minutes in".

`cost` reports accelerator-hours, which are measured and therefore always
known, and money only where a pool declares a unit price. **An unpriced pool
settles to zero**, so a run on one reports *unknown* rather than `$0.00`.

The on-demand comparison needs an on-demand price, and Eyvan carries no price
catalogue and no spot-discount constant. It uses one an on-demand pool of the
same shape declares, or one you pass with `--on-demand-price`; without either
it says so. An invented saving is the figure a team puts in a budget.

## Logs after the machine is gone

Both log commands read the live cell while one exists, and fall back to this
attempt's archive once it does not. That is deliberate: the read an operator
actually needs is usually the one *after* the capacity disappeared.

Two writers fill that archive.

* **While the workload runs**, the `archive-live-logs` phase copies new output
  into immutable per-attempt chunks every `EYVAN_LOG_ARCHIVE_INTERVAL_S` seconds
  (60 by default; `0` switches it off). This is what makes a **preempted** run
  readable at all -- a reclaimed slice is gone before the control plane learns
  anything about it, so nothing on the release path can save its journal. What a
  preemption costs is bounded by that interval, not by the length of the run.
* **On the release path**, settlement archives what is left, resuming from where
  the last chunk stopped so no line is stored or replayed twice.

Both refuse to archive a workload that mounts secrets, for the reason
`artifact-safety.md` gives: streaming redaction cannot be reconstructed after
the fact without persisting the plaintext values.

A replay says what it is. `rank N archived (incomplete)` means that rank's
output is known to be partial -- the tail between the last chunk and the
reclamation, a rank whose host was unreachable, or an object that would not
open. A chunk that could not be read is named in the page's `detail` rather
than skipped, because a shortened log that reads as complete is worse than one
that admits a hole.

Archived logs are kept for `EYVAN_LOG_RETENTION_S` (30 days by default),
separately from the general artifact window: a callable can be rebuilt from its
source, and a log cannot.

## Reading a managed service

```
eyvan workload endpoints <id>          # the replicas currently able to serve
eyvan service health <id>              # per-replica health, three-valued
eyvan service decisions <id>           # what was observed, decided, and why
eyvan service decisions <id> --limit 5 # the newest few
```

`service health` reports `healthy`, `unhealthy` and `unknown` as three separate
counts that add up to the number of replicas listed. **`unknown` is not a soft
`unhealthy`.** It means this control plane could not establish the replica's
state -- a timeout, a refused connection, or a service that declares no health
path and is therefore never probed at all. A replica that has never been asked
anything reports `"probed": false`, and `probing_enabled` is `false` for the
whole service. Nothing in that picture is a reason to destroy an accelerator; a
`503` from a replica that answered is.

`service decisions` returns the newest committed scale and replacement
decisions, each carrying the observation it was taken from rather than a pointer
to state that has since moved on. That is what makes "why did it scale in at
04:12" answerable the next morning.

## Break glass

```
eyvan cell-drain <id>
eyvan cell-delete <id>
eyvan preemption-force-finalize <command-id> --reason "..."
eyvan system-reconcile
```

These are CLI-only and deliberately not on the dashboard. Each is irreversible
in a way a mis-click should not be able to reach.
