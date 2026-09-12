# Operational status

Measured evidence for Eyvan's operational claims. Every number here was
observed on the date attached to it — this file never contains a goal
restated as a result. The intent is a living SLO record: each entry should
grow a history, and a claim that stops being re-measured moves to
"unverified" rather than silently staying green.

## Live Spot TPU priority checkpoint restoration (2026-09-12)

Campaign `priority-3ee7d85be890` passed through Eyvan on one Spot
`v5p-8` cell (four TPU chips), after both the controller JSON and systemd stop
fixes below. The batch workload checkpointed at step 20, yielded the cell to an
interactive TPU workload, then resumed automatically on attempt/generation 2
and completed step 40. Both workloads succeeded. The restored training result
matched the uninterrupted control; loss decreased from 31.135807 to
0.000008222324. The recorded checkpoint URI matched the workload's durable URI.

The training receipt reports TPU backend, four devices, restored step 20,
final step 40, and matching checkpoint/restoration instance identities. Workload
failure, provider-preemption and capacity-loss counters remained zero. This is
**scheduler priority interruption and restoration on the same Spot VM**, not
physical provider reclamation or replacement-host recovery.

Receipt: `.pytest_cache/spot-validation/priority-3ee7d85be890.json`;
runner exit 0. Batch workload `wl_d5e6ed3f5a6843f5bc4e7b112dfc86de`, interactive
workload `wl_638839702a294f049777d439bd31df38`. Provider queries independently
confirmed Spot provisioning. Fleet teardown returned 200 without cleanup
errors; the VM subsequently disappeared from the provider's filtered listing.
The failed predecessor's state was preserved separately, rather than repaired
by silently rewriting records or allowing its pending workloads into this run.

## Collected systemd stop follow-up (2026-09-12; local verification)

The next Spot attempt reached a ready TPU and completed its checkpoint
hook, but the victim remained in checkpointing while the interactive workload
waited. Agent queries reported `INDETERMINATE` and independently confirmed the
systemd unit was absent. The stop path could confirm termination yet leave an
unknown durable status; the controller consequently kept waiting for a terminal
observation. This attempt did **not** establish priority restoration.

The corrected path records `STOPPED` only after successful systemd stop, verified
unit absence, successful container stop stages where applicable, and renewed
fence validation. It does not invent a successful exit. Unrequested disappearance,
a nonzero stop, or an unobservable manager remain unknown. The new regression
failed before the fix; **84** systemd/container/TLS/preemption tests passed with
required PostgreSQL, and independent read-only review found no actionable defect.

Full Python 3.11/required-PostgreSQL verification: **6,782 passed, 17 monorepo-only
skips**, 164 warnings, exit 0 in 355.13 seconds. Receipt stem:
`.pytest_cache/python311-postgres-after-collected-stop`. Ruff check/format, mypy,
import contracts, lock/generated-reference checks and dashboard coverage, build,
lint and formatting passed. These checks preceded the successful bounded
hardware rerun documented above.

Cleanup exposed an operational limit: cancellation was rejected while the
preemption was executing, and fleet teardown did not remove its leased VM.
The test was interrupted, its pool disabled, and that exact test VM explicitly
deleted; a subsequent GCP listing returned no matching instance. The isolated
agent release was replayed and its provider state contains zero cells. Stranded
workload rows and a failed preemption command were retained as diagnostic
evidence; they must not be allowed to consume the next campaign's capacity.

## Priority-preemption and typing follow-up (2026-09-12; local verification)

A live Spot TPU priority-preemption attempt reached checkpointing but exposed
non-JSON datetime values in the durable command payload. The four phase
payload timestamps now use timezone-aware ISO strings; ordinary record datetime
fields are unchanged. Regression tests intercept intermediate writes before
repository serialization, including successful phases and checkpoint abort.
Those six cases pass with memory, SQLite and required PostgreSQL.

Hosted mypy reporting differed between editable and non-editable installations.
A clean non-editable installation reproduced 2,456 imported-module errors while
checking the declared 15-file ratchet. Explicit `follow_imports = "silent"`
keeps imported type analysis but limits diagnostic reporting to that ratchet;
it does not skip imported modules. The same clean environment now passes, and
an isolated copied-source control assigning an imported resource vector to an
integer still fails with an assignment error.

Fresh Python 3.11/required-PostgreSQL verification: **6,780 passed, 17
monorepo-only skips**, 164 warnings, exit 0 in 356.15 seconds. Receipt stem:
`.pytest_cache/python311-postgres-after-mypy-scope`. Ruff check/format (678
files), import contracts, lock check, generated API/environment checks and
dashboard coverage/build/lint/format also passed. This is local evidence, not a
new hosted-CI pass. The hosted 1,000-cell snapshot p99 violation (27.843 ms
against 25 ms) remains under investigation; the budget has not been relaxed.

Subsequent hardware retries encountered laptop sleep, mismatched component
builds, and a validation restart that omitted the isolated fleet-state setting.
These attempts do **not** establish priority checkpoint restoration or physical
provider-preemption recovery. The misplaced-state cleanup was verified against
GCP, and the original release was replayed against the restored isolated agent.
Further hardware qualification must retain the same state configuration and
verify component build agreement before admission.

## Hosted-CI regression follow-up (2026-09-11; local verification)

The first pushed suite exposed a nonportable stale-PID fixture on Linux and
an unintended PostgreSQL serialization race in the accelerated renewal-loss
test. Fixtures now explicitly cover the existing endpoint/profile reuse policy
and order renewal after the injected expiry; runtime behavior is unchanged.
The OCI workflow's job-level runner-context references moved to step-level
environment declarations, and actionlint 1.7.7 accepts all workflows.

Fresh Python 3.11/required-PostgreSQL verification: **6,773 passed, 17
monorepo-only skips**, 188 warnings, exit 0 in 331.90 seconds. Receipt stem:
`.pytest_cache/python311-postgres-after-hosted-ci-doc-fix`. Dashboard: **291
passed**, 82.7% line coverage, build/lint/format checks passed. Ruff, mypy,
import contracts, locked exports and generated-reference checks also passed.
These local receipts do not certify the subsequent hosted workflow rerun.

## Live four-device checkpoint retry and cancellation (2026-09-11)

Campaign `c814dc37e004` used Eyvan to provision another Spot `v5p-8` cell in
`example-spot-project`, `us-east5-a`, with Python 3.11 and JAX 0.6.2:

- Workload `wl_4af929a916274dffadbff0cdbcc3a9a8` ran `jax.pmap` across four
  devices, averaging gradients with `lax.pmean`. At step 20 it wrote model
  weights to an immutable-generation GCS object and deliberately exited 17.
- Eyvan retried it automatically: the observed attempt changed from 1 to 2,
  with `failure_count=1`. Attempt 2 downloaded the checkpoint and completed
  step 40. Its weights matched a separately calculated uninterrupted 40-step
  control (`rtol=1e-5`, `atol=1e-6`). The observed losses were approximately
  **31.1358 → 0.00983065 → 0.00000822232**.
- The checkpoint and restoration instance names were identical. This proves
  durable-object restoration after process failure on a real Spot TPU, **not
  node replacement or physical Spot-preemption recovery**.
- A second workload reached its readiness marker, was cancelled through
  Eyvan, and a third workload subsequently succeeded on the same capacity.
- Fleet teardown returned HTTP 200; a subsequent independent provider listing
  returned no matching VMs. Local receipt:
  `.pytest_cache/spot-validation/training-retry-c814dc37e004.json`.

## Live Spot JAX computation (2026-09-11)

After pushing the 16-commit series ending at `6cb0d97`, Eyvan acquired a
`v5p-8` Compute VM in `example-spot-project`, `us-east5-a`. The provider independently
reported `provisioningModel: SPOT`; the workload also checked the project and
preemptible metadata from inside the VM.

- Workload `wl_50e647e82d0e47d2bcdb788e43706c8e` progressed through provisioning,
  native-worker readiness, running and **succeeded** through Eyvan's API.
- The declared Python 3.11 bootstrap succeeded: the worker reported Python
  **3.11.16**, JAX **0.6.2**, `jaxlib` **0.6.2**, and `libtpu` **0.0.17.1**.
- JAX selected the **TPU** backend and enumerated **four TPU devices**. A
  1,024-square float32 matrix product returned the expected **1,024.0** entry;
  25 subsequent synchronized iterations completed. This is a computation
  smoke test, not a distributed-throughput benchmark or proof of using all
  four devices simultaneously.
- Eyvan returned the payload's success marker in its workload logs.
  Fleet teardown returned HTTP 200, and an independent provider listing
  subsequently returned no matching VMs.
- Private local receipts: `.pytest_cache/spot-validation/jax-93d9a677a715.json`
  and `jax-93d9a677a715-serial.log`. Raw serial output is not a public artifact.

This establishes actual Spot TPU computation and the explicitly declared
interpreter path, superseding the failed attempt below for those narrow claims.
It does not establish physical preemption recovery, fleet mTLS, sustained load,
or release provenance. The follow-up training/retry campaign is separate.

## Spot attempt and Python example (2026-09-11)

Authorized validation used `example-spot-project`, an isolated loopback control plane
at port 18180, a separate runtime profile, and one-cell Spot-only fleet
`roadmap-spot` in `us-east5-a`. No on-demand fallback was used.

- Eyvan acquired a `v5p-8` Compute VM; the provider reported
  `provisioningModel: SPOT`. This proves allocation, **not workload success**.
  Its startup log rejected the image interpreter with
  `Eyvan requires Python 3.11 through 3.13; set runtime.python to a supported version`.
- A retry explicitly declaring `runtime.python: "3.11"` encountered
  `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS` (HTTP 503, reason `stockout`).
  Eyvan subsequently surfaced `provider_failure_code: stockout`; the workload
  did not execute. The declared interpreter path therefore remains unverified
  on this attempted hardware allocation.
- Both attempts requested workload cancellation and fleet teardown. Provider
  listing returned no matching VMs, local provider state contained no cells,
  and all three attempt-specific worker secrets returned `NOT_FOUND`.
  Runtime profile and artifact uploads are retained for subsequent validation.
- Local receipts: `.pytest_cache/spot-validation/provider-spot-instance.json`,
  `spot-smoke-system-python-receipt.json`, `spot-smoke-receipt.json`, and the
  bootstrap diagnostic in `serial.log`. These are local operational receipts,
  not published release or hosted-CI attestations. The private `runtime.json`
  is not a publishable receipt.

`examples/fleet.yaml` now explicitly selects Python 3.11. Three new regression
cases failed before the edit and passed afterward, including preservation of
node-level Python 3.12/3.13 overrides. Bootstrap algorithms and global generator
defaults were not changed. `docs/runtimes.md` explains interpreter declarations
and the non-inheritance of system accelerator packages.

Verification after the example change:

- Focused fleet/bootstrap selection: **82 passed**, four warnings.
- Python **3.11.14**, required PostgreSQL **17.11** full suite: **6,767 passed,
  17 monorepo-only skips, 188 warnings**, exit **0**, **383.52 seconds**.
  Receipts: `.pytest_cache/python311-postgres-after-spot-example.log` and `.exit`.
- Ruff check/format (**675 files**), mypy (**15 files**), four import contracts,
  lock check, all three locked dependency/hash exports, generated API/env
  references and diff checks passed.
- Dashboard: **291 tests across 21 files**, **82.7% line coverage**; build,
  lint and formatting checks passed.

This does not qualify TPU computation, checkpoint restore, fleet mTLS rollout,
physical failover, or the complete roadmap. The test project's observed blocker here is
Spot capacity, not the earlier project's Secret Manager permission denial.

## Renewal-loss checkpoint (2026-09-11; local qualification)

`tests/test_dispatch_claim_takeover.py` now exercises the real heartbeat's
rejection of an expired persisted claim before a second controller takes over.
The outer delivery exits while the inner dispatch remains pending; assertions
wait for explicit inner completion before checking the winner's entire snapshot
and one actual native-child execution. Six memory/LocalJson/PostgreSQL cases
cover both held-response variants. A seventh regression failed when an outer
cleanup timeout skipped inner draining, then passed with nested cleanup that
attempts bounded joins for both controllers before transport teardown.
Independent read-only review found no confirmed remaining issue in that fix.
No production implementation changed.

- Focused Python 3.11/required-PostgreSQL replay/takeover/CI selection: **42 passed**.
- Final Python 3.11/required-PostgreSQL full run: **6,764 passed, 17 monorepo-only
  skips, 188 warnings**, exit **0**, **307.07 seconds**. Receipts:
  `.pytest_cache/python311-postgres-after-renewal-cleanup.log` and `.exit`.
  The earlier 6,763-pass renewal run predates the cleanup regression.
- Ruff check and format (674 files), mypy (15 files), four import contracts,
  generated API/env checks, lock check and all three locked exports passed.
  Export comparison excludes the two-line generated command header, whose
  output path differs in scratch receipts. Docs/CI selection: **415 passed**.
- Dashboard: **291 tests across 21 files**, coverage **82.7% lines**;
  build, lint and format checks passed.

This is deliberate local claim expiry, not a database outage, process crash,
TCP failure, deployed leadership failover or Spot proof. Broader generated
histories and external qualification remain unfinished.

## Validation environment

- Date: 2026-09-07
- Plane: Eyvan 0.0.1 from this checkout, isolated loopback control plane
  (ports 18080/18090), SQLite persistence, isolated runtime profile.
- Cloud: GCP project where the operator is `roles/owner`; **Spot-only** end to
  end — every capacity request used Spot (queued resources / preemptible
  VM), with no on-demand fallback anywhere.

## Hardware proofs (live Spot TPU, acquired and driven by Eyvan)

| Shape | Path | Zone | Acquire → ready | Job result |
|---|---|---|---|---|
| v5p-8 (4 TPU workers) | `gcp-compute` | us-east5-a | ~3–4 min from instance create | **succeeded** — JAX: `backend=tpu`, `device_count=4`, `matrix_result=32.0` |
| v6e-8 (4 TPU workers) | `gcp-tpu` queued resources | us-east5-b | ~50 min in `PROVISIONING` (provider-side queue, Spot) | **succeeded** — JAX: `backend=tpu`, `device_count=8`, `matrix_result=32.0` |

Observations that generalize:

- The v6e us-east5-a **queue admission was denied by Google** (code 7), while
  us-east5-b admitted and provisioned the identical request — zone choice is a
  real operational variable, not a config nicety.
- Both cells released cleanly after the workload; the v5p cell was reclaimed
  by the 120s idle timeout exactly as configured.

## Engineering checkpoints (2026-09-08; not a final release verdict)

- Python support now starts at 3.11. The full Python 3.11 checkpoint with
  PostgreSQL 17.11 required passed **6,376 tests, 17 skipped**, exit 0, in
  335.82 seconds. The remaining skips are monorepo-only workflow/hardware-proof
  inputs absent from this checkout, not database-availability skips.
- PostgreSQL 17.11 was built from checksum-verified official source under the
  workspace cache. Forced database semantics/chaos/auth-audit/group-lifecycle
  checks passed **57 tests**, with one monorepo-workflow-only skip. Independent
  API-only replica reads passed separately. This is not provider or production
  failover evidence.
- The 1,000-cycle repository soak passed on memory and SQLite; its local JUnit
  receipt recorded 265.434 seconds total. The pure 10k-cell scheduler simulation
  also passed its explicit timing budgets; methodology is documented separately.
- `eyvan demo --json` returned 45 from a separate local worker and created no
  cloud resources. Cleanup has success, failure and timeout regressions.
- The pinned Python dependency audit, run with `--disable-pip --require-hashes`
  on the runtime export, reported **no known vulnerabilities**, with no skipped
  dependencies. This resolves the earlier invocation gap below; it does not
  constitute an independent security review.
- The roadmap remains unfinished. Hosted provenance issuance, Spot validation
  of the changed surface, and other remaining criteria are not inferred from
  these local checks.

## Engineering checkpoint (2026-09-10; local, not release qualification)

- Full Python 3.11 suite with disposable PostgreSQL required: **6,538 passed,
  17 skipped, 141 warnings**, exit 0, 370.17 seconds. The skips are missing
  monorepo-only inputs, not unavailable PostgreSQL. This run includes the
  settlement properties, daemon liveness fix, directive-preserving license
  maintenance, doctor unknown-storage reporting and BFF claim/CSRF hardening.
- Dashboard: **291 tests passed**, 82.7% line coverage; production build,
  ESLint and Prettier checks passed. Ruff and its format check passed; mypy's
  current 15-file scope and all four import contracts passed. Locked runtime,
  development and build exports matched their requirements and hashes.
- A disposable local browser qualification passed in Chrome **152.0.7977.83**:
  HTTPS authorization-code/PKCE login, HttpOnly/Secure/SameSite=Lax cookies,
  authenticated fetch, no page errors, rejection of an actual cross-site
  top-level logout POST with no request cookie, preservation of the browser
  session after that attempt, and same-origin sign-out clearing the session.
  This initial check used a scratch harness, a fake IdP, an injected locally
  signed-token verifier and browser acceptance of self-signed TLS. It does not
  qualify production certificates, a real IdP, ingress affinity or replicas.
- The follow-up ships `dashboard/e2e/bff.mjs` and
  `tests/browser_bff_server.py`, with a blocking Python 3.11 browser CI step and
  retained JSON receipts. The shipped harness passed locally with pinned
  Playwright 1.63.0 / Chromium **153.0.8010.12**, including provider denial
  without session admission. Browser requests are confined to the two fixture
  origins. Fixture lifecycle and CI-policy checks passed **35 tests**; this
  does not claim a hosted Actions run. Reproduction is in `CONTRIBUTING.md`.
- A subsequent npm audit found GHSA-82fw-gwwq-j7x9 in Vitest's mocker. Vitest
  and its coverage package were upgraded together to **4.1.11**. A clean
  `npm ci`, dashboard coverage run and fresh audit passed; the audit reported
  **0 vulnerabilities**. npm 10.9.8's resolver failed while updating the peer
  graph; an ephemeral npm 12.0.2 resolved the lock, and ordinary npm 10.9.8
  successfully reproduced it with `npm ci`. No global npm upgrade was made.
- `eyvan doctor --json` still reports missing
  `secretmanager.secrets.setIamPolicy` in `example-research-project`. No IAM changes,
  account switching or cloud provisioning were performed for this checkpoint.
  New Spot hardware validation remains blocked pending the required permission.

## Follow-up checkpoint (2026-09-10; local)

- The next full run exposed a CI-policy test that confused browser installation
  with Python dependency installation: **6,544 passed, 17 skipped, one failed**.
  The policy now identifies Python installer commands rather than display names.
- A separate regression demonstrated that an introspection test left allocation
  tracing enabled for subsequent tests. Test cleanup now restores the prior
  off-state, and a subprocess regression verifies it. Production allocation
  tracking behavior is unchanged.
- After these fixes, the full Python 3.11 suite with disposable PostgreSQL
  required passed **6,553 tests, 17 monorepo-only skips, 141 warnings**, exit 0,
  in 507.48 seconds. The local receipt is
  `.pytest_cache/python311-postgres-after-isolation.log` with its matching
  `.exit` file. This is a checkpoint, not hosted CI or release qualification.
- The Python 3.11 fast gate passed **146 tests in 5.38 seconds**, with 6,424
  deselected. Ruff check/format, mypy's 15-file scope, four import contracts,
  generated API/env references, lock/export hash comparison and diff checks
  passed. The new design write-up and proof-receipt checks passed 425 tests.
- Dashboard coverage, TypeScript/Vite build, ESLint and Prettier checks passed.
  All six shipped HTTPS browser checks passed on Chromium **153.0.8010.12**.
  This machine's browser cache is workspace-local, so reproduction used
  `PLAYWRIGHT_BROWSERS_PATH="$PWD/.pytest_cache/playwright-browsers" npm --prefix dashboard run test:browser`.
  Running without that variable failed because the default browser cache did
  not contain the executable; it did not establish a dashboard regression.
- The read-only doctor preflight still denies
  `secretmanager.secrets.setIamPolicy` in `example-research-project`. No account,
  IAM or provisioning changes were made. The receipts/elastic-packing design
  defines a comparison protocol but contains no Slurm/Volcano/kube-batch results.

## Trace transport checkpoint (2026-09-10; local)

- Opt-in controller-to-agent `traceparent` generation and authenticated
  extraction are implemented. Seventeen focused trace tests cover disabled
  admission, ordinary headers, bounded invalid input, task/thread isolation,
  endpoint-failure cleanup and a separate process receiving a serialized parent
  through ASGI. This is not a deployed network/collector qualification.
- The full Python 3.11 suite with disposable PostgreSQL required passed
  **6,609 tests, 17 monorepo-only skips, 141 warnings**, exit 0, in 301.35
  seconds. Receipt: `.pytest_cache/python311-postgres-after-trace-transport.log`
  and its matching `.exit` file. The focused agent/correlation/docs selection
  passed **487 tests** with PostgreSQL required.
- Ruff check/format (662 files), mypy's 15-file scope, four import contracts,
  generated API/env references, lock check, dependency/hash export comparisons
  and `git diff --check` passed. Dashboard coverage tests, TypeScript/Vite build,
  lint and format checks each exited 0.
- Review prompted explicit tests of real outbox flag-off delivery and ASGI
  exception cleanup. Generated trace headers exclude baggage/vendor state;
  operator-provided client headers are intentionally not rewritten.
- Native-worker trace propagation, durable-queue parent continuity, deployed
  export and the rest of the roadmap remain open. A fresh read-only doctor
  check still reports missing `secretmanager.secrets.setIamPolicy` in
  `example-research-project`; no IAM, account or provisioning changes were made.

## Native trace-hop checkpoint (2026-09-10; local)

- The next opt-in hop joins the native-worker request span to the agent span.
  Thirteen regressions verify bearer/retry compatibility, authenticated parent
  continuity across both API hops, safe route-template attributes, failure
  privacy, cleanup and validation before one-use secret consumption.
- A two-host staging regression reproduced lost context inside the thread
  pool. A separate context copy per task fixed it; the combined staging/native/
  trace selection passed **84 tests** on Python 3.11 with PostgreSQL required.
  Read-only review found no concrete security, compatibility or context-lifecycle
  defects in these additions.
- The refreshed full Python 3.11/required-PostgreSQL suite passed **6,622 tests,
  17 monorepo-only skips, 141 warnings**, exit 0, in 304.05 seconds. Receipt:
  `.pytest_cache/python311-postgres-after-native-trace.log` and `.exit`.
  Ruff check/format (663 files), mypy, four import contracts and diff checks
  passed. The changed environment reference was regenerated and its check passed.
  The Python 3.13 fast selection passed **150 tests, 6,489 deselected**, in 1.60s.
- These are local SDK/ASGI and repository checks, not cloud bootstrap rollout,
  deployed collector verification or workload-process tracing. Durable-queue
  trace-parent continuity and external roadmap evidence remain open.

## Undispatched retry checkpoint (2026-09-10; local)

- Generated admission/requeue histories now check all durable prior releases,
  unchanged nonzero budget spend/revision/accelerator hours, reserved resources,
  generation fencing and dispatch-may-have-started refusal. The combined
  settlement/orphan/retry/secret-dispatch/CI selection passed **77 tests** on
  Python 3.11 with PostgreSQL required (seed 20260910).
- The full refresh passed **6,635 tests, 17 monorepo-only skips, 141 warnings**,
  exit 0, in 298.50s. Receipts:
  `.pytest_cache/python311-postgres-after-retry-properties.log` and `.exit`.
  Ruff check/format (664 files), dashboard coverage tests/build/lint/format,
  API schema, lock and all three dependency-export comparisons passed.
- This full checkpoint predates the later seven snapshot-validation scheduler
  regressions. Those pass locally and detect nine isolated mutants. A fresh
  1,576-mutant campaign subsequently killed 1,215, with 361 surviving, no verdict
  errors and unchanged input hashes. See
  [mutation qualification](reference/scheduler-mutations.md) for unseeded-run
  comparison limits; this is not full-domain or all-roadmap completion.
- Docker remains unavailable (`Executable not found in $PATH: "docker"`).
  Disposable Compose configuration/docs checks pass, but real local PostgreSQL
  tests do not establish Compose runtime behavior. The certificate-bootstrap
  plan also leaves issuer/SAN/rotation decisions and deployed qualification open.

## Snapshot-validation and costing checkpoint (2026-09-10, local only)

- Full Python 3.11/required-PostgreSQL run after the snapshot-validation
  regressions: **6,642 passed, 17 monorepo-only skips, 141 warnings**, exit 0,
  295.27 seconds. Local receipt:
  `.pytest_cache/python311-postgres-after-snapshot-validation.log` and `.exit`.
- Ruff check/format (664 files), mypy (15 files), diff whitespace and 389
  documentation/benchmark tests passed. These are not hosted-CI receipts.
- The subsequent empty-charge-total regression passes with 36 other
  costing/invariant/run-cost tests. The separate costing mutation campaign
  improved from 21/22 detected to **22/22 detected**, with no verdict errors and
  unchanged input hashes. The full-suite checkpoint above predates this one
  additional test; production money arithmetic was not changed.
- Read-only `eyvan doctor --json` again exited 1: the configured account still
  lacks `secretmanager.secrets.setIamPolicy` in `example-research-project`. The loopback
  control plane was not running either. No IAM, account or provisioning action
  was taken; local tests do not substitute for the blocked Spot qualification.

## Spend-arithmetic checkpoint (2026-09-10, local only)

- Reporting totals now use decimal accumulation rather than reintroducing
  binary drift across settled/reserved charges and accelerator hours. Returned
  fields remain floats; pricing-unknown and settlement-boundary rules remain
  unchanged. Six fractional/tiny-value regressions pass, and a scoped independent
  read-only review found no actionable issue.
- Full Python 3.11/required-PostgreSQL run: **6,649 passed, 17 monorepo-only skips,
  141 warnings**, exit 0, 288.27 seconds. Receipt:
  `.pytest_cache/python311-postgres-after-spend-arithmetic.log` and `.exit`.
- That full checkpoint predates six subsequent invalid-number guard cases.
  The current focused costing/invariant/run-cost selection passes **49 tests**.
  A fresh spend mutation campaign detects 74 of 76 mutants; the two retained
  internal-label mutations are explicitly classified in the
  [mutation report](reference/scheduler-mutations.md). RunSpend property methods
  were not included in the generated mutation set.
- Ruff check/format (664 files), mypy (15 files), all four import contracts,
  generated API/env references, lock and all three dependency exports passed.
  Dashboard build, lint and formatting passed; coverage tests passed **291 tests
  in 21 files**, with 82.7% lines. No hosted-CI, cloud or rollout claim follows.

## Test and build status (2026-09-07 historical checkpoint)

| Check | Result |
|---|---|
| Full Python suite (SQLite default; Postgres variants skip without a server) | **6,099 passed, 107 skipped**, exit 0, ~4.5 min |
| Dashboard | 281 tests passed, `tsc --noEmit && vite build` clean |
| Dashboard coverage (v8, ratchet baseline) | 82.8% lines / 79.6% statements / 79.4% functions / 65.4% branches |
| Python coverage | see CI coverage step (`--cov=eyvan` ratchet; floor recorded in the workflow) |
| ruff check + format | clean on all 582 Python files |
| `uv lock --check` and hash-pinned exports | in sync |
| npm audit | 0 vulnerabilities (dashboard job gates at `--audit-level=low`) |
| Python CVE audit | **gap**: pip-audit rejected 2026-09-07 (venv bootstrap failure under uv CPython); see roadmap |
| Git-history secret scan | clean (only test placeholders and redaction field names) |

## Decision-logic cross-checks

- Elastic packing: 1,104 generated packing scenarios matched brute-force
  feasibility and minimum-cell counts exactly (scenario suite run 2026-09-07;
  encoded as `tests/test_elastic_packing_preserves_feasible_subsets.py`).
- Scheduler invariants are additionally covered by generated property tests
  (`tests/test_scheduler_properties.py`, Hypothesis): whole cells only, exact
  worker totals, no per-cell overcommit, input-order independence.

## Local seeded-mutation checkpoint — 2026-09-10

- Two independent fixed-seed scheduler campaigns produced identical input and
  verdict maps: 1,213 detected / 363 surviving mutants. A permanent explicit
  affordable-frontier example detects three further semantic changes in
  isolated probes; the fresh aggregate is 1,216 / 360, with unchanged inputs
  and no lost baseline detections. See the [mutation record](reference/scheduler-mutations.md)
  for scope, receipts and the tool's decorated-code blind spot. Survivors remain;
  this is not a passing full-domain mutation gate.
- Full Python 3.11 suite with real disposable PostgreSQL required: **6,656 passed,
  17 skipped, 141 warnings**, exit 0 in 302.00 seconds. The skips remain
  monorepo-only workflow/proof inputs. Receipts:
  `.pytest_cache/python311-postgres-after-seeded-mutations.log` and `.exit`.
  This includes the numeric spend guards, seed-profile test and explicit example.
- No hosted CI, deployed exporter, certificate rollout or new Spot experiment
  is implied by this local checkpoint.

## Local running-retry checkpoint — 2026-09-10

- Generated service-state running failure/retry histories now cover bounded
  elapsed monetary/accelerator-hour conservation, historical release immutability,
  stale fences and idempotent terminal replay. Twenty memory examples plus
  twelve memory/SQLite/PostgreSQL cases include zero-rate usage accounting.
  The combined history/accounting/CI selection passed **104 tests** with
  PostgreSQL required and seed 20260910. Nightly inclusion is locally tested;
  this does not claim a hosted run or physical/controller dispatch.
- Independent read-only review found no actionable issue in this bounded scope.
  Ruff check/format passed (666 files); docs/CI tests passed 415 cases.
- Fresh Python 3.11 full suite with real disposable PostgreSQL required:
  **6,669 passed, 17 monorepo-only skips, 141 warnings**, exit 0 in 303.65 seconds.
  Receipts: `.pytest_cache/python311-postgres-after-running-retry.log` and `.exit`.

## Local mixed-preemption checkpoint — 2026-09-10

- Bounded service-state provider-loss/capacity-loss/recycle histories passed
  thirteen tests (25 generated memory examples plus twelve fixed repository
  cases). They check distinct counters, checkpoint URI retention, elapsed
  spend/usage, historical releases and replay/fencing. They do not exercise
  actual cloud preemption, controller ownership or checkpoint restoration.
- Nightly inclusion is locally policy-tested. Combined history/accounting/CI
  selection: **117 passed** with real PostgreSQL required, seed 20260910.
  Independent read-only review found no actionable issue. Ruff check/format
  passed (667 files); docs/CI checks passed 415 tests.
- Fresh full Python 3.11/required-PostgreSQL suite: **6,682 passed, 17 monorepo-only
  skips, 141 warnings**, exit 0 in 307.52 seconds. Receipts:
  `.pytest_cache/python311-postgres-after-preemption-properties.log` and `.exit`.
- A fresh read-only `eyvan doctor --json` still exits 1: the active account lacks
  `secretmanager.secrets.setIamPolicy` in `example-research-project`. No IAM, account or
  cloud-resource changes were made. This local checkpoint is not Spot rollout
  evidence, and certificate bootstrap still requires an issuer/distribution decision.

## Local controller-dispatch history checkpoint — 2026-09-10

- `tests/test_controller_retry_properties.py`: thirteen tests passed with
  PostgreSQL required (15 generated memory examples plus twelve fixed memory,
  shipped local-repository and PostgreSQL cases). Real controller/outbox calls
  dispatch to a fake agent and observe running/failure/success across 0–2 retries.
  Admission uses the service lease API; this is not scheduler or hardware proof.
- Read-only review prompted additional remote-ownership, historical outbox,
  unchanged-cell and settlement-audit checks. All thirteen cases passed again.
  An isolated in-process fault that corrupts the finalized remote ID was caught
  by the new ownership assertion; its unmodified control passed. No production
  source was mutated. Failed submit deliveries and concurrent failover remain open.
- Fresh full Python 3.11/required-PostgreSQL suite: **6,695 passed, 17 monorepo-only
  skips, 141 warnings**, exit 0 in 290.10 seconds. Receipts:
  `.pytest_cache/python311-postgres-after-controller-histories.log` and `.exit`.
- Ruff/format (668 files), mypy (15 files), four import contracts, generated
  API/environment checks and 415 docs/CI tests passed. Dashboard coverage tests,
  build, lint and format checks exited 0; all three hash-pinned requirements
  exports match the lock. No hosted CI or Spot qualification follows from this.

## Local failed-submit delivery checkpoint — 2026-09-10

- `tests/test_dispatch_delivery_properties.py` covers 0–3 ordinary submit RPC
  exceptions followed by success, using actual outbox/reservation transactions
  and agent doubles. Fifteen generated memory examples plus twelve fixed
  memory/local/PostgreSQL cases check unchanged budgets and user retry counters,
  stable submission identity, reservation cleanup, ownership and audit counts.
- Independent review prompted successful reservation identity checks and a
  probe asserting submit runs outside the public repository transaction.
  The combined new histories/CI selection passed **54 tests** with PostgreSQL
  required; nightly inclusion is policy-tested. An isolated cleanup-disabling
  fault was detected by the audit assertion, while its unmodified control passed.
- Fresh full Python 3.11/required-PostgreSQL suite: **6,708 passed, 17 monorepo-only
  skips, 141 warnings**, exit 0 in 293.80 seconds. Receipts:
  `.pytest_cache/python311-postgres-after-delivery-histories.log` and `.exit`.
  Ruff/format passed (669 files), as did 415 docs/CI checks and diff whitespace.
- This does not qualify ambiguous response loss, busy/unavailable error handling,
  priority-preemption ownership, concurrent failover, hosted CI or hardware.
  No production behavior or cloud/IAM/account configuration changed in this slice.

## Local native TLS bootstrap component — 2026-09-11

- The low-level bootstrap renderer accepts an immutable Secret Manager TLS
  bundle reference. The new helper validates and stages owner-only files without
  logging material, rejects malformed/encrypted/mismatched keys, and leaves no
  partial destination on failure. A separate pre-edit capture proved exact
  TLS-off script parity. This is not yet a GCP fleet configuration option.
- Independent read-only review found no confirmed defects and suggested added
  lifecycle/input regressions. The first full run then caught a missing
  `provider_cli_env()` on the new gcloud boundary. A real child-process regression
  reproduced it; the scrubber fix preserves cloud configuration while removing
  interpreter steering. Helper/environment selection: **31 passed**.
- Fresh full Python 3.11/required-PostgreSQL suite: **6,734 passed, 17 monorepo-only
  skips, 141 warnings**, exit 0 in 312.69 seconds. Receipts:
  `.pytest_cache/python311-postgres-after-tls-bootstrap-env.log` and `.exit`.
  The earlier failed `python311-postgres-after-tls-bootstrap` receipt is retained.
- Ruff/format (671 files), mypy (15), import contracts, API/env reference checks,
  locked exports, docs and diff checks passed. Dashboard: **291 tests passed**,
  82.7% line coverage; build, lint and format checks exited 0.
- GCP still publishes provider-selected IP endpoints. Stable DNS/reserved-IP SAN
  identity, per-host issuance and fleet rotation/replacement remain unresolved;
  hostname verification is not weakened. A fresh doctor still exits 1 for missing
  `secretmanager.secrets.setIamPolicy` in `example-research-project`. No IAM, account or
  cloud provisioning changes were made, and no deployed/Spot qualification is
  claimed.

## Priority-preemption ownership checkpoint (2026-09-11; local qualification)

- `tests/test_priority_preemption_properties.py`: **16 passed** with Python 3.11,
  real PostgreSQL required and seed 20260910. Eighteen generated memory examples
  and fixed memory/LocalJson/PostgreSQL cases cover mixed group outcomes, fenced
  per-cell stops, actual completed-command redelivery and untouched generation-2
  successors after stale-plan dispatch. Successor state is directly seeded;
  this does not prove concurrent failover, physical stops or money conservation.
- Independent read-only review found no actionable issue in that scope. The
  nightly policy regression failed before workflow inclusion and passed after it;
  combined docs/CI selection: **415 passed**.
- Full Python 3.11/required-PostgreSQL suite: **6,750 passed, 17 monorepo-only
  skips, 157 warnings**, exit 0 in **308.42 seconds**. Receipts:
  `.pytest_cache/python311-postgres-after-priority-histories.log` and `.exit`.
- Ruff check/format (672 files), mypy (15 files), all four import contracts,
  generated API/env checks, lock check and three hash-pinned export comparisons
  passed. Dashboard coverage tests, lint, format check and build exited 0.
- Fresh `eyvan doctor --json` still exits 1 for missing
  `secretmanager.secrets.setIamPolicy` in `example-research-project`; no cloud/IAM/account
  changes occurred. Ambiguous response-loss and interrupted/concurrent preemption
  histories, deployed qualification and the broader roadmap remain open.

## Admission-response loss checkpoint (2026-09-11; local qualification)

- `tests/test_dispatch_response_loss.py`: **seven passing cases**. Real agent
  and native-worker ASGI handlers launch a local child; one/three successful
  admission responses are withheld, then service objects are recreated over
  durable state. Memory, shipped local storage and required PostgreSQL retain
  one execution marker, stable lease ownership and one completed dispatch.
- A process-local fault disabling native replay return produced two execution
  markers and failed the regression; the unmodified control passed. Production
  source hashes stayed unchanged. Independent review found missing failure-path
  child cleanup; a forced post-admission assertion reproduced a running child.
  Bounded fenced retirement now leaves it stopped with its PID absent, while
  retaining the original failure. Borrowed HTTP transports remain context-owned.
- Full Python 3.11/required-PostgreSQL suite: **6,757 passed, 17 monorepo-only
  skips, 176 warnings**, exit 0 in **308.64 seconds**. Receipts:
  `.pytest_cache/python311-postgres-after-response-loss.log` and `.exit`.
- Ruff/format (673 files), mypy (15 files), all four import contracts,
  `git diff --check` and **415 docs/CI tests** passed. Dashboard coverage,
  build, lint and format check exited 0. The nightly selection includes these
  fixed regressions; hosted execution is not established by local workflow tests.
- These are sequential replays after the local execution completes, not TCP
  loss, replay while running, process crashes, concurrent failover, monetary
  conservation, systemd/GCP or Spot qualification. No production code or cloud
  resources changed in this test-only slice.

## Dispatch-claim takeover checkpoint (2026-09-11; local qualification)

- Three memory/local/required-PostgreSQL cases hold A's actual admission
  response while its child runs, expire the persisted claim, let B finalize,
  and then return A's response. B's complete persisted snapshot stays unchanged;
  the native execution stays running and ultimately produces one marker.
  Review found no actionable issue. Explicit renewal timing and a returned-response
  assertion prevent a barrier timeout being mistaken for a late response.
- Takeover/replay/nightly-policy selection: **38 passed**. Full Python 3.11
  with required PostgreSQL: **6,760 passed, 17 monorepo-only skips, 182 warnings**,
  exit 0 in **315.84 seconds**, receipt stem
  `.pytest_cache/python311-postgres-after-claim-takeover` (`.log` and `.exit`).
- Ruff/check and format (674 files), **415 docs/CI tests**, diff check and
  fast selection (**157 passed in 2.77 seconds**) passed. The Python 3.11-backed
  Chromium **153.0.8010.12** disposable HTTPS BFF check also passed.
- This explicitly expired claim is not real leader/heartbeat failure, OS
  process crash, TCP failure, priced conservation or cloud qualification.
  Fresh doctor still exits 1 for missing `secretmanager.secrets.setIamPolicy`
  in `example-research-project`; no IAM/accounts/cloud resources were changed.

## Limits recorded at the 2026-09-07 checkpoint (historical)

The first two limits below describe that earlier snapshot. The later local
PostgreSQL, replica-read and installed-wheel upgrade checkpoints above supersede
them; they are retained as history, not claims about current test coverage.

- Postgres-only test variants were **not executed locally** (no local server);
  they run in CI against a real Postgres 17 service. Treat local-green /
  CI-green as the full claim, not local-green alone.
- No multi-replica or upgrade-path testing exists yet; the plane is exercised
  single-node.
- Latency numbers for pack/scheduling decisions live in `test_launch_latency.py`
  and run in CI; they are excluded from the coverage-ratchet run because the
  coverage trace skews wall-clock thresholds.
