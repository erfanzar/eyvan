# Outstanding hardware proofs

Host, static, and real-PostgreSQL suites exercise this package, with exact
counts and source identities recorded in the EasyDeL production-readiness
journal rather than copied into prose here. None of that is evidence about a
TPU, GPU, provider lifecycle, or the current release image.

This page exists so approving a spend is a decision about a known quantity. Each
proof states what it establishes, the exact commands, the pass criterion, the
shape, and roughly what it costs. Historical runs are retained below, but they
were tied to their own candidates and some ended with exit 2. **No historical
run qualifies the current candidate without an immutable exact-candidate
receipt.**

**The criteria are executable.** `eyvan proof catalog | children | logs |
preemption | warm-window | secret | kubernetes` evaluates the stated criterion
against the live plane and exits 0 on
pass, 1 on failure, and **2 when the run did not exercise the behaviour at all**.
That third verdict is the one that matters: a spot slice that was never
reclaimed, a pool that never got a cell, an observation taken outside the warm
window -- each produces a clean-looking run that establishes nothing, and reading
it off a screen at the end of a spend is exactly when nobody is inclined to
notice. The judgement lives in `eyvan/sdk/commands/proof.py` and is tested
without hardware; the commands only fetch. A refusal -- a mistyped option, a
receipt path already taken -- exits **3**, because a run that never happened
must not wear a verdict's exit code.

**The attribution is executable too.** Every proof command takes `--receipt
PATH`, which writes the verdict together with the things prose cannot carry: the
sha256 of the evaluator function that decided it, the build digest of the client
that ran it, the build digest the plane reported for itself, and a computed list
of the dimensions the run left unestablished. Two builds of this package report
the same version string, so the digest is the only thing that distinguishes
them; `GET /api/v1/build` is what a plane answers with. `--evidence PATH` writes
the documents the verdict was read from, owner-only and redacted of any secret
value the run was given.

```bash
eyvan proof preemption <workload-id> --receipt ~/receipts/p5.json --evidence ~/receipts/p5-evidence.json
eyvan proof verify ~/receipts/p5.json     # 0 only if it qualifies the build in front of it
```

`eyvan proof verify` is the sentence at the top of this page as a command: it
refuses a receipt whose criterion has changed since the run, whose candidate
build is not the one running, or whose recorded verdict was not a pass. It never
upgrades an outcome. A receipt is written once -- an existing path is refused
before the plane is polled, while the run is still free -- and its `receipt_digest`
detects editing, not forgery: there is no signing key here, and the receipt says
so in its own `unverified` list rather than implying one.

The agents and the workers are asked too. `eyvan capacity builds` reports the
digest of every reachable cell agent, and of every native worker process inside
its cells, beside the plane's -- and a proof run records that sweep. It exits
non-zero on proven skew *and* when agreement could not be shown: an agent that
did not answer is not an agent that agreed, and a proof has already been run
against a stale agent and read as evidence.

The worker ranks are in that sweep because they execute the workload and their
wheel outlives a re-staged head. That is not theoretical -- during the
2026-08-12 run the head's wheel was replaced twice while the running TPU workers
kept the one their cell was provisioned with.

A receipt from a fleet that answered therefore leaves no build dimension open:
the client that submitted, the plane that scheduled, the agent that held the
capacity and the process that ran the callable are all identified by content.

P2 and P3 stay procedural on purpose: they need a host killed mid-stream and a
job that submits its own children, and an "automated" verdict over a run whose
interesting half is manual would be worse than none.

Fleet configs live in the monorepo's `configs/`. GCP inputs are checked in as
sanitized `*.yaml.in` templates rather than runnable files. Render one to an
owner-controlled path only after supplying the target project, bucket, and an
immutable OCI image digest:

```bash
uv run python scripts/render_eyvan_config.py \
  configs/eyvan-live-v5p16-pods2-us-east5-a.yaml.in \
  --output /tmp/eyvan-live-v5p16.yaml \
  --set GCP_PROJECT_ID=your-project \
  --set GCS_BUCKET=your-bucket \
  --set OCI_IMAGE=REGION-docker.pkg.dev/PROJECT/REPOSITORY/IMAGE@sha256:64_HEX_DIGEST
```

The renderer refuses mutable image tags, unresolved placeholders, unsafe YAML
characters, and an existing output unless `--force` is explicit. It writes mode
0600 and never applies the result. Commands below that name a config refer to
that rendered output, not to the `.in` template. Applying a GCP fleet provisions
billable resources; obtain operator approval before `eyvan up`, out-of-band host
failure, `eyvan down CONFIG`, or any direct live harness. The live Python harnesses also require
`--confirm-provider-mutations`.

All proof fleets are spot unless a proof explicitly states otherwise.

## The 2026-08-12 regression run

Not a proof. The two defects that stranded capacity that morning were fixed the
same day and the fixes had only ever run against tests, which is a weaker claim
than it sounds: both bugs were in the paths that give an accelerator back, and
both were invisible until a fleet met them.

So the sequence that produced them was run again on the fixed build. Spot TPU in
`us-east5-a` was queued behind other tenants for over an hour, so the workload
was placed on a CPU cell instead -- deliberately, and for the reason the CPU
fleet config already exists: the lease, the transient unit, the stop, the
simulation snapshot and the release are the same code on either shape, and the
only thing an accelerator adds to this particular question is an hour of waiting
per attempt.

What it established, with the plane's package digest matching the checkout's:

- a cancel of a running workload settled at `delivery_attempts = 1`, without
  `workload-force-finalize`. That is the path that took twenty minutes and a
  break-glass on 2026-08-12;
- **zero** `lease references unknown cell` across the whole cancel-and-`down`
  sequence, where that morning produced one every two seconds;
- the age-based outbox backoff pacing a real failing command -- "failed on
  attempt 1 after 68s, retrying in 2s", then 100s/3s, then 134s/4s -- against a
  cell agent that was busy deleting a queued resource;
- `eyvan capacity builds` reporting the agent's digest against the plane's, and
  refusing to claim agreement for pools holding no cell.

It also found two. The build sweep reported a cell's workers as an empty list,
which means "this backend has no separate worker", while a native worker was
active on the VM and had never been asked. And `@execute(CpuConfig())` -- a
config this package exports and the decorator accepts -- was refused against a
healthy CPU pool with "is not a TPU pool", after the callable had been
serialized and uploaded. Both fixed; the second was then run end to end on a GCE
cell and returned its value.

What it did **not** establish: anything about multislice, TPU provisioning, or
any of P1-P8. A CPU cell is not evidence about an accelerator, and this section
is here so nobody reads it as one.

## A GPU cell can run a callable and have no usable GPU

Run 2026-08-13, on a real `nvidia-tesla-p100` spot cell in `us-central1-c`.

The placement half now works. A `GpuConfig` callable was serialized, uploaded,
placed on that cell, executed and returned its result -- the control plane has a
GPU pool gate beside the CPU and TPU ones, and the kind is decided from the
requested accelerator type before any pool is consulted, so a pool declaring
`eyvan_resource_kind: gpu` still cannot answer a request for `v5p-16`.

The result it returned is the finding:

```
GPU CALLABLE RESULT: host=research-nvidia-tesla-p100-...  gpu=NVIDIA-SMI has failed
because it couldn't communicate with the NVIDIA driver.
```

The GPU is attached -- `lspci` reports `GP100GL [Tesla P100 PCIe 16GB]` -- and
the cell booted the right image, carrying the `nvidia-580` and
`ubuntu-2404-lts-accelerated` licences. The kernel says what is wrong:

```
NVRM: The NVIDIA GPU 0000:00:04.0 (PCI ID: 10de:15f8)
NVRM: nvidia.ko because it does not include the required GPU
NVRM: None of the NVIDIA devices were initialized.
```

`_DEFAULT_GPU_IMAGE` pins `...with-nvidia-580`, and the 580 branch dropped
Pascal and Volta. This project's GPU quota is `PREEMPTIBLE_NVIDIA_P100_GPUS` (16)
and `NVIDIA_V100_GPUS` (8) -- both dropped generations -- so every GPU cell it
can currently afford boots with a driver that cannot open its own GPU. Turing and
later (T4, L4, A100, H100) are unaffected, and `vm_image` is settable per pool,
which is the workaround.

**The workload succeeded.** That is the part worth keeping: nothing in the run
reported a missing accelerator, no probe failed, and the verdict was SUCCEEDED.
A training job would have been slow, or would have failed somewhere far from the
cause. The client-side refusal therefore stays, with its reason corrected from
"cannot place" -- now false -- to what actually goes wrong.

Closing it properly needs one of two things: a cell-side check that refuses to
register a GPU cell whose driver cannot see its GPU, which is the general fix, or
a default image chosen against the pool's accelerator generation.

### The T4 half, run the same day

The driver theory holds from the other side. A `nvidia-tesla-t4` spot cell on the
same image reports `Tesla T4, 580.173.02`, six loaded modules and a `/dev/nvidia0`
-- Turing is supported, Pascal is not, and the default image is right for one and
wrong for the other. So GPU callables are qualifiable on this project today; the
P100 was simply the wrong GPU to try them on.

That run did not finish, and why is a second finding. The cell's bootstrap died:

```
startup-script: E: Package 'python3-venv' has no installation candidate
Script "startup-script" failed with error: exit status 100
```

There were no `eyvan*` units on the host at all and the native worker was
inactive, so nothing ever connected. **The workload sat in `connecting`
indefinitely rather than failing.** A cell that cannot run a worker still
registered as capacity, took the lease, and looked exactly like a slow one --
the same shape as the driver hazard above, where a broken cell is
indistinguishable from a working one until someone reads the output by hand.

The apt failure itself is *not* explained. The bootstrap does run
`eyvan_apt_get update` before the install and the script runs under
`set -euo pipefail`, so `apt-get update` exited 0 and still left an index with no
candidate for a package that is normally in `main` -- apt reports success when
individual fetches fail. The P100 cell earlier the same day, on the same image
family, bootstrapped fine and ran its callable, so this is not universal and one
observation is not a root cause. Recorded as observed rather than diagnosed.

What both findings share is the thing worth fixing first: a cell that cannot do
the job it was provisioned for should refuse the work rather than accept it and
be quiet about it. Neither a missing driver nor a missing worker was reported by
anything the plane could see.

## Three gates decide whether a shape can be requested, not one

Worth writing down because getting this wrong cost a session. Quota is the gate
people check, and it is the least decisive of the three. A request needs all of:

1. **Quota** for the metric in that region. Read it from the Cloud Quotas API,
   not `gcloud compute regions describe` -- the regions endpoint reports only
   v5e (`TPU_LITE_*`) and shows nothing for v5p or v6e. Note also that a
   per-zone *override* does not mean other zones have none: the metric's
   `defaultLimit` applies to every zone in `supportedLocations`. Reading the
   `us-east5-a` override as the only grant is what sent every attempt into the
   one congested zone.
2. **The zone offering the shape.** `gcloud compute tpus accelerator-types list
   --zone Z`. `europe-west4-a` has v5p quota and offers no v5p at all -- only
   v2, v3, v5litepod and v6e. GCP says so exactly: *"the accelerator v5p-16 was
   not found in zone europe-west4-a"*.
3. **Permission to submit into that queue.** Separate from both. `europe-west4-b`
   does offer v5p-16 and the project does have the quota, and the request is
   still refused: *"User does not have permission to submit requests into this
   queue for accelerator type v5p-16 in location europe-west4-b."*

For this project, v5p-16 passes all three only in `us-east5-a`.

Eyvan's handling of gates 2 and 3 is worth noting since both were exercised: the
provider error is surfaced verbatim with its EID, the cell is marked
PROVIDER_ERROR, the apply is **not** retried -- a request that can never succeed
is not retried -- and `capacity_loss_count` stays 0, so a permission refusal is
not charged to the workload as a preemption.

## What blocked P2 on 2026-08-13

Worth recording, because "not attempted" and "attempted and blocked" call for
different next steps. P2 needs a workload running across two or more hosts, and
then one of those hosts taken away.

`v5p-16` in `us-east5-a` was unavailable for the whole session: another tenant
held six `v5p-128` slices, with two more `v5p-64` requests queued ahead of ours
that were still waiting hours later.

`v6e-16` in `europe-west4-a` was tried instead -- it is the other granted shape
that spans more than one host, and the zone held nothing of ours. Three attempts
were made. Capacity was granted each time and then withdrawn during creation:
the node reached `CREATING` with its four endpoints allocated, disappeared, and
was retried, four times across the last attempt, never reaching `READY`.

Eyvan's own behaviour through that is the one thing the attempt does establish.
The cell stayed `registering`/`PROVISIONING` with no failure code and no state
churn of its own while the provider flapped underneath it, which is the correct
response: a node that GCP is still retrying is not a capacity loss, and treating
it as one would have burned the workload's retry budget on the provider's
indecision.

So P2 remains at exit 2 for want of a slice that stays up, not for want of a
run. The `v5p-16` route needs the zone to drain; the `v6e-16` route needs a
grant that holds long enough to boot.

## A partial observation of v6e-16 topology

`v6e-8` and `v6e-16` are the only catalogued shapes with no `hosts`, and the
entry says why: Eyvan compares `observed_tpu_vms` against that number, Google's
table counts physical hosts rather than VMs, and no receipt here established the
endpoint count -- so a guess would make Eyvan refuse a correctly provisioned
slice.

On 2026-08-13 a `v6e-16` queued resource in `europe-west4-a` reported **four
`networkEndpoints` on port 8470**, which is the quantity `observed_tpu_vms`
carries. That is consistent with the four-chip VMs the entry's per-VM
`host_cpu: 180` / `host_memory_gib: 720` describe.

**It is not recorded in the catalog, and should not be until it is repeated.**
The observation was taken while the node was still `CREATING`, on a resource
that was being torn down and is now gone, so it cannot be re-read. Writing
`hosts: 4` on that basis flips this shape from "the provider decides" to "Eyvan
refuses a slice whose endpoint count disagrees" -- and the cost of being wrong is
refusing capacity that is fine. One mid-creation reading is not enough to buy
that.

What would be: the same count from a `READY` v6e-16, which anyone with capacity
can take in one command. Two receipts and the entry can be filled in, with P1's
host check working for the shape for the first time.

```bash
gcloud compute tpus tpu-vm describe <node> --zone europe-west4-a \
  --format="value(state, networkEndpoints.len())"
```

## Why these and not a general soak of everything

Each row proves something a CPU cannot: Pallas lowering, real provider
semantics, multi-host behaviour, or the passage of wall-clock time. These
hardware runs do not substitute for CPU-establishable software gates: remaining
host gaps, including the strict cursor/topology/archive contracts tracked in the
roadmap, must close independently before a hardware receipt can qualify them.

---

## P0 — a callable runs on a multislice allocation

**Establishes** the property every other proof presumes: that a submission
becomes running work. Placement, leasing and provisioning were all separately
evidenced before this; that a workload ever *executed* was not.

```
eyvan head up FLEET --allow-insecure-artifact-http --yes
eyvan up FLEET                                    # on the head; no URL or token needed
python train.py                                   # the callable below
eyvan head down FLEET --yes
```

```python
from eyvan import TpuConfig, execute


@execute(TpuConfig(type="v5p-16", pods=2))
def train():
    import jax

    print(jax.device_count())


train()
```

**Passes when** the decorator returns the device count of the whole allocation --
for `v5p-16 x 2` that is 16, the catalog's 8 chips per slice times two slices --
and the queued resource GCP created carries more than one node.

**Run 2026-08-12, exit 0.** One Cloud TPU Multislice queued resource in
`us-east5-a`, spot, with two `v5p-16` nodes (`...-node-0`, `...-node-1`, each
carrying GCP's own `multisliceNode` marker). Both slices provisioned, all four
ranks -- two slices by two hosts -- received the dispatch, and the callable
returned `16`. Submitted from a bare `@execute` decorator with no `EYVAN_URL` and
no `EYVAN_TOKEN`: on the head, the SDK reads the plane's address and credential
from `/etc/eyvan/node.json` and `/etc/eyvan/head.env`.

Nothing was configured by hand. `eyvan head up` published its own worker wheel to
the artifact store with a digest and object generation, named the fleet-state
document its agents reconcile against, generated the native-worker root secret,
and staged the operations dashboard. JAX reached the workers through the fleet's
`runtime.init_commands`.

Two conditions this run had to satisfy and a first-time operator will too. The
head terminates no TLS, so the artifact capability token would travel in clear
text and is refused without `--allow-insecure-artifact-http`. And a serialized
callable cannot load on another interpreter: the head pickles on Python 3.12 and
the Cloud TPU image ships 3.10, so the submission came from a 3.10 environment
matching the pool's declared `runtime.python`. An earlier attempt without that
match provisioned two slices, dispatched to four ranks, and had every rank die
on the version check -- which is why that check now runs at submission.

**Shape** v5p-16 x 2, spot, one multislice queued resource. **~25 minutes.**

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
eyvan down CONFIG --yes
```

```
eyvan proof catalog --accelerator-type v5p-16      # exit 0, 1 or 2
```

**Passes when** the pool's positive CPU, memory, and TPU resources match the
catalog entry for `v5p-16`, `observed_tpu_vms` is 2, and the fleet YAML has no
`resources:` block.

**Run 2026-08-06, exit 0.** v5p-16 spot in `us-central1-a`, slice ready 7m9s
after apply. With no `resources:` block the pool resolved to `cpu 32, tpu 8` --
the catalog's `host_cpu` and `chips` -- and the cell reported
`observed_tpu_vms: 2`, the catalog's `hosts`. The provider is authoritative for
how many slices; the catalog is authoritative for hosts per slice, which is the
property a provider returning one host of two would violate silently.

That run also exposed the historical zero-memory defect: the catalog then set
`host_memory_gib` for no shape, so the otherwise successful pool could not place
the 1 GiB default workload. Catalog revision `2026-08-04.2` sources positive
per-VM CPU and memory and the fleet/autoscale compilers persist both. The run
above remains evidence for host-count authority, not evidence for the corrected
resource vector; re-run P1 after reapplying the fleet to qualify that change.
`eyvan doctor` reports old durable pools that still advertise zero memory. A cell
that never reported `observed_tpu_vms`, a catalog shape with no recorded host
count, or a cell with no usable slice count is **inconclusive** rather than
passing -- the check that matters would otherwise be skipped silently.

**Run 2026-08-12, exit 1 -- and the failure was the criterion's, not the
fleet's.** Pointed at a live multislice `v5p-16 x 2`, P1 reported
`expected 2 host(s) per slice; observed {'research-...-0001': 4}`. Nothing was
wrong: the catalog states hosts per *slice*, a multislice cell holds
`worker_count` of them under one allocation, and four VMs across two slices is
the right answer. The criterion was comparing a total against a per-slice number,
so it would have failed every correct multislice fleet -- at the end of a spend,
where the natural response is to stop believing it. Re-evaluating the same
recorded observations with the corrected criterion returns
`every cell reports 2 host(s) per slice across 2 slice(s)`, exit 0. A live
re-run against a freshly staged head is what turns that into a receipt.
**Shape** v5p-16, one slice; the multislice case is now covered too.
**~35 minutes.**

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
eyvan down CONFIG --yes
```

**Passes when** the second page reports the killed rank as unavailable while
still returning the surviving rank's records, and the archived blob exists after
terminality so the cell can be released without losing logs.

**Run 2026-08-12, exit 2 (inconclusive) -- and the half that needed hardware
now holds.** On a live multislice `v5p-16 x 2` in `us-east5-a`, four ranks of
one workload were read through `eyvan proof logs --ranks 4`: 240 records, every
one timestamped, stdout and stderr preserved, backend cursors present, and
**four ranks accounted for**. That is per-rank attribution across real hosts,
which is exactly what the 2026-08-07 run could not show and what the criterion
had never been exercised against.

Still inconclusive on one check, and the verdict says which:
*"no rank was unreachable during this run, so the reporting path was not
exercised"*. Taking a host out from under a running job is an operator action --
`gcloud alpha compute tpus tpu-vm ssh <node> --worker 1 --command 'sudo
systemctl stop eyvan-native-worker.service'`, then re-run the command above --
and it was not taken here.

The receipt is machine-readable: `eyvan proof verify` on it reports the receipt
intact, the criterion current, and the candidate build current, and still refuses
to let it qualify anything, because an inconclusive run qualifies nothing. That
refusal is the whole point of writing receipts.

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

Rank width is represented separately from allocation width. ``worker_count`` is
the number of TPU slices in a cell; ``runtime_process_count`` records an
observed native-worker/JAX endpoint count.

The DR-0002 manifest is now written: schema 2 carries the topology frozen at
dispatch with its digest, and every expected rank's status, object URI, sha256
and size -- expected ranks coming from the declared width rather than from
whichever ranks answered, because a rank that was never reachable leaves no
trace anywhere else. **Its writer is off by default**
(``EYVAN_LOG_ARCHIVE_MANIFEST_V2_ENABLED``): DR-0002 rolls the reader out first,
so a replica that could not read a manifest never has to meet one. A P2 run
that means to exercise it has to turn it on, and a run that does not is
evidence about the v1 sparse-rank index instead. Neither reconciliation proves a
stable topology on its own.

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
eyvan down CONFIG --yes

eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload exec <id> -- py-spy dump --pid <pid>
eyvan down CONFIG --yes
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

**Run 2026-08-13, exit 0 (pass) for the children half.** On a real GCE CPU cell
in `us-central1-a`, against a head running the checkout's own wheel. All five
criteria the `children` evaluator states passed: 256 children created, every one
attributed to the parent's owner rather than `workload:<id>`, every one parented
into the parent's scope, the width ceiling refusing the 257th with 409
(*"already has 256 live children; at most 256 may run at once"*), and the
capability refused with 409 once the parent was terminal (*"is cancelled; it
cannot gain new children"*). Receipt `p3-final.json`, and `eyvan proof verify`
re-reads it as intact with the criterion and candidate both current.

Three things had to be fixed to run it at all, which is why it sat inconclusive
for eight days rather than failing:

* **Nothing could request the capability.** `can_submit_children` was complete
  server-side and set by no CLI option and no SDK config, so the only workloads
  that ever held it were services, which settlement grants it to on their behalf.
  `eyvan workload submit --can-submit-children` now asks for it.
* **The ceiling is not reachable without retrying.** Around 230 children the
  plane answers a PostgreSQL serialization conflict (40001) with a *retryable*
  503 -- its own detail says so, and `eyvan.worker.runtime.http_retry` treats it
  as retryable. A harness posting with bare `urllib` reports that transient
  conflict as the ceiling and stops 26 short. With the shipped retry policy it
  reaches exactly 256. The plane is behaving correctly under contention; a
  launcher fanning out this wide has to use the retry the SDK already ships.
* **The expiry check cannot live inside the job.** Only the job holds the token,
  and the runtime stops the workload's transient unit and reaps its whole cgroup,
  so a detached probe is killed mid-check -- correctly, since a workload runtime
  that leaked processes past cancellation would be the worse bug. The capability
  has to be left somewhere that outlives the process and replayed from outside.

Note the two refusals are both 409 and are told apart only by their detail: the
width ceiling and a terminated parent would otherwise be indistinguishable to
anything checking status codes.

Still unqualified: the `exec` half, which needs a TPU.
**Shape** CPU fleet, then v5p-16. **~40 minutes.**

## P4 — secret delivery, per attempt

**Establishes** that a workload reads a Secret Manager secret and that the value
appears in no durable record, log line or API response; that the ledger shows one
issuance and one read; and that the capability is refused once the attempt ends.

```
# A root is matched by scheme/authority/path *segment*, never as a raw string
# prefix, so a trailing partial segment like `eyvan-proof-` matches nothing and
# the submission below is refused with 403. Name the whole segment, or stop at
# the authority to allow every secret in the project.
eyvan policy set --allow-secret-reference gcp-secret://<project>/eyvan-proof-token
eyvan up configs/eyvan-live-v5p16-us-east5-a.yaml
eyvan workload submit --name secret-proof --command 'python -c "import os;print(len(os.environ[\"HF_TOKEN\"]))"' \
  --secret HF_TOKEN=gcp-secret://<project>/eyvan-proof-token --tpu 8
eyvan workload logs <id>                         # the length, never the value
eyvan events --org <org> --project <project> | grep secret.               # one secret.issued, one secret.read
eyvan down CONFIG --yes
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

**Run 2026-08-13, exit 0 (pass).** On a real GCE CPU cell in `us-central1-a` --
nothing about this proof is TPU-specific, and the `--tpu 8` above is incidental.
Delivery, the half that failed in August, worked: the job read `HF_TOKEN` and
exited 0, where it exits 1 on a missing value. Exactly one `secret.issued` and
one `secret.read`, the read matched to its issuance, and the value in no workload
record and no audit payload. Receipt `p4.json`; `eyvan proof verify` re-reads it
as intact with criterion and candidate current.

Two things had to be corrected first, both in the proof rather than the plane:

* **The proof could not read the logs route.** It decoded
  `/api/v1/workloads/{id}/logs` as JSON; that route is declared
  `PlainTextResponse`, so the decode fails for *every* workload. It surfaced only
  now because the run before this one never got as far as reading logs.
* **The job cannot prove delivery by printing.** The plane serves no log output
  at all for a workload that mounts secrets -- deliberately, since the job's own
  stdout may contain the value -- so "the job prints the length" is not
  observable for any workload this proof can be pointed at, and the log grep is
  vacuous for all of them. The exit status carries that evidence instead.

**Passes when** the job exits 0 having read the value (it exits 1 when the
variable is absent, so the exit status is the delivery evidence -- the log cannot
be, because the plane suppresses output for secret-bearing workloads),
`eyvan events` shows exactly one `secret.issued` and one `secret.read` for that
attempt, and grepping the workload record and the audit payloads finds the value
nowhere. The audit payloads are the half a person checking by hand forgets, and
the one place a well-meaning "context" field would put the value -- in the store
retained for ninety days.
**Shape** any cell; CPU is enough. **~15 minutes.**

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
eyvan down CONFIG --yes
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

`/api/v1/system/invariants/soak` still returns `p8_evidence: null` on its own,
and deliberately: the control plane has no trustworthy view of its own process
size and cannot see provider inventory without a provider call. It only
republishes what an external monitor measured and posted.

### The external monitor

`eyvan soak-monitor` is that monitor. It runs as a separate process beside the
control plane, measures it from outside, and posts one closed
`eyvan.soak.external-evidence.v1` sample per interval to
`/api/v1/system/soak-evidence` (fleet administration, authenticated):

```
eyvan soak-monitor \
  --proof-id sha256:<64-lowercase-hex> \
  --control-plane-pid-file <supervisor-pid-file> \
  --database-url <postgresql-url> \
  --provider-inventory-command <script printing the provider's live cell count> \
  --interval-s 30
```

- `rss_bytes` / `fd_count` — read from `/proc/<pid>` where it exists, `ps` and
  `lsof` otherwise, always for the control plane's pid and never the monitor's
  own. `--control-plane-pid-file` is re-read every tick so a deliberately
  restarted control plane keeps being measured; `--control-plane-pid` pins one
  process instead.
- `database_bytes` — `pg_database_size(current_database())` with
  `--database-url`, or the size of the local state document with
  `--state-path`. Exactly one of the two.
- `spend_reconciled` — the control plane's own ledger: invariant `I5` not
  violated **and** no cell contributing zero cost for want of a unit price. The
  terminated run's `$3.68` was a floor with 42 of 46 cells unpriced, which is
  not a reconciled ledger.
- `provider_inventory_count` — whatever the operator's command prints on
  stdout, as a single bounded integer. It is a provider call by construction:
  the argv is executed without a shell, and a non-zero exit, unparseable
  output, or timeout is unknown rather than zero.
- `cleanup_complete` — every cell the control plane lists has reached
  `deleted`. `deleting` counts as outstanding, and a cell state this build does
  not recognise makes the field unknown rather than complete.

**A partial sample is not a sample.** The schema has no encoding for "unknown",
so a monitor that cannot establish one field posts nothing, prints
`{"outcome": "incomplete", "unmeasurable": [...]}`, and exits non-zero for a
bounded `--samples` run. The evaluator then reports the specific
`*_evidence_missing` code, which is the honest outcome. No zero is substituted
and no previous value is carried forward.

**A stale sample is not evidence.** The control plane keeps only the most
recent sample, only in the replica that received it, and republishes it only to
the run whose `proof_id` it names. Past `EYVAN_SOAK_EVIDENCE_MAX_AGE_S`
(default 300s) it reports `null` again, so a monitor that died an hour ago
reads as missing rather than as the last thing it happened to see. Age is the
larger of the monotonic and wall-clock deltas, because `CLOCK_MONOTONIC` does
not advance while a host sleeps — which is how the terminated run lost 31.5
hours. The sample is deliberately not durable: it is a current measurement, not
fleet state, and a restarted replica has stopped having one rather than lost a
record. Run the monitor and the evaluator against the same replica.

Without a monitor running, every response is byte-identical to what it was
before this route existed, and live invocations still end `inconclusive`/exit 2
with `rss_growth_evidence_missing`, `fd_growth_evidence_missing`,
`database_growth_evidence_missing`, `spend_reconciliation_evidence_missing`,
`provider_inventory_evidence_missing` and `cleanup_evidence_missing`.

**Passes when** the monitor supplies a proof-bound sample at every successful
observation, the evaluator returns exit 0 with every closed criterion
satisfied, and the separate immutable provider/hardware evidence confirms the
claimed CPU/TPU operations. Exit 0 means the **evaluator criteria** passed and
nothing more: it is not provider operation evidence and not hardware
attestation, and it still requires a mixed CPU + TPU fleet and 72 hours with no
unobserved gap. No qualifying receipt exists.

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

