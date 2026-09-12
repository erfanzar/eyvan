# Roadmap to 10/10

What it takes to push every assessed aspect of Eyvan to its ceiling, and what
has already landed. Status legend: **[done]** shipped in this change ·
**[next]** remaining implementation or verification · **[upstream]** belongs in the upstream
monorepo, deliberately not done in this mirror · **[ongoing]** a practice,
not a patch.

Ratings refer to the 2026-09-07 assessment session (two adversarial bug
hunts, live Spot TPU validation of v5p and v6e, DeepScan quality/security
reviews).

## Architecture (9 → 10)
- **[done]** Control-plane extraction: the former 453 KB module is now a
  stable facade over private transactional subsystems, each below 50 KB.
  All 192 method ASTs, signatures and descriptor types matched the baseline.
  The extracted-tree checkpoint passed 6,258 tests (97 skipped); a wheel built
  with the pinned backend and installed against hash-verified exports passed
  isolated-import submission/cancellation checks. This is not PostgreSQL or
  hardware evidence.
- **[done]** Import-linter contracts now live in this mirror and run in CI:
  subsystems cannot import siblings or the facade; services cannot import
  API/controller layers; domain cannot import server/SDK layers. The actual
  dependency direction is API and controllers toward services, not services
  toward controllers.
- **[done]** Architecture tour and diagram in
  `docs/reference/control-plane-architecture.md`, with a regression checking
  that documented implementation paths exist.

## Correctness (8 → 10)
- **[done]** Hypothesis property tests pin the scheduler invariants the found
  bugs violated (`tests/test_scheduler_properties.py`): whole cells only,
  exact worker totals, no per-cell overcommit under contention,
  input-order independence.
- **[done]** Packing-vs-budget property cross-check: 200 generated bounded
  snapshots compare scheduler feasibility with exhaustive whole-cell subsets
  (`tests/test_scheduler_budget_properties.py`), asserting worker conservation,
  no repeated cells and affordable placements. Seed 20260910 passed locally.
- **[done]** Bounded terminal-settlement conservation properties generate
  multi-lease widths, rates, durations and replay counts, rebuilding the service
  between replays. Persisted costs sum to budget spend, replay preserves budget
  revision and settlement timestamps, and cell widths/lease resources survive.
  Thirty generated memory examples and fixed memory/SQLite/PostgreSQL cases
  passed (`tests/test_settlement_properties.py`); the nightly lane includes them.
- **[done]** Bounded orphan-settlement histories cover missing, terminal and
  moved-on workloads, plus refusal to reclaim leases still claimed by live work.
  Stale generations leave accounting untouched; actual held time determines
  priced cost and accelerator hours, and service reconstruction/replay preserves
  budget revision, lease settlement, audit events and unrelated records.
  Forty generated memory examples and 24 memory/SQLite/PostgreSQL cases passed
  (`tests/test_orphan_settlement_properties.py`, seed 20260910); the nightly
  seeded lane includes them. These are service-boundary histories, not a
  randomized controller/failover simulation.
- **[done]** Bounded undispatched retry histories exercise actual lease admission
  and requeue APIs over 1–4 generations, varying reserved rates/hours. They
  preserve all earlier released leases, budget spend/revision/accelerator hours,
  reservations and cells; stale replay and dispatch-may-have-started refusal
  leave durable state untouched. Thirty generated memory examples and fixed
  memory/SQLite/PostgreSQL cases are wired into the seeded nightly lane
  (`tests/test_unstarted_retry_properties.py`). The combined settlement/orphan/
  retry/secret-dispatch/CI selection passed 77 tests on Python 3.11 with
  PostgreSQL required, seed 20260910. No physical dispatch was performed.
- **[done]** Bounded service-state running failure/retry histories cover 1–3
  failures followed by success, elapsed-cost bounds, accelerator-hour accounting,
  immutable historical releases and fenced/idempotent replay. Twenty generated
  memory examples and fixed memory/SQLite/PostgreSQL cases are wired into the
  seeded nightly lane (`tests/test_running_retry_properties.py`). The combined
  accounting/history/CI selection passed 104 tests with PostgreSQL required,
  seed 20260910. These tests mark execution running through service APIs;
  they do not dispatch a command to an agent or execute it on hardware.
- **[done]** Bounded mixed provider-loss/capacity-loss/recycle settlement histories
  check distinct retry counters, checkpoint retention, elapsed spend/usage,
  immutable prior releases and fenced/idempotent replay. Twenty-five generated
  memory examples and twelve fixed memory/SQLite/PostgreSQL cases are wired
  into nightly fuzz (`tests/test_preemption_settlement_properties.py`). The
  combined history/accounting/CI selection passed 117 tests with PostgreSQL
  required, seed 20260910. This is service-state qualification, not actual
  cloud preemption, priority-preemption ownership or checkpoint restoration.
- **[done]** Bounded controller-dispatched execution failure/retry histories
  now exercise outbox delivery and agent observation, with persisted remote
  ownership, one completed dispatch per generation, conserved spend/usage,
  unchanged cells, settlement audit records and no-op repeated polling.
  `tests/test_controller_retry_properties.py` passes thirteen cases: fifteen
  generated memory examples plus fixed memory, shipped local-repository and
  PostgreSQL histories. The agent is a test double; admission uses the public
  lease API, not scheduler placement. Nightly wiring is locally policy-tested.
- **[done]** Bounded failed-submit delivery histories now cover 0–3 ordinary
  RPC exceptions before success through real outbox claims and reservation
  transactions. They verify stable submission identity, failed reservation
  removal, successful reservation ownership, unchanged budget/user retry
  counters, audit counts and calls outside the public repository transaction.
  `tests/test_dispatch_delivery_properties.py` has generated memory and fixed
  memory/local/PostgreSQL cases, wired into seeded nightly fuzz. These are
  agent-double tests, not ambiguous response-loss or hardware qualification.
- **[done]** Bounded priority-preemption ownership histories now execute real
  outbox dispatch with per-cell agent doubles, mixed terminal outcomes, fenced
  group stops, completed-command redelivery and a seeded generation-2 successor.
  `tests/test_priority_preemption_properties.py` passes sixteen tests across
  memory, shipped local storage and required PostgreSQL (seed 20260910), with
  eighteen generated memory examples. The seeded nightly command includes it.
  Independent review confirmed the bounded assertions; no physical stop,
  concurrent-controller run or monetary-conservation claim follows.
- **[done]** Bounded sequential admission-response loss now traverses actual
  agent/native ASGI handlers and a real local child. After one or three lost
  successful responses, recreated service objects replay durable state without
  launching a second execution (`tests/test_dispatch_response_loss.py`: seven
  passing tests, including memory/local/required-PostgreSQL and failure cleanup).
  A process-local replay-guard fault produced two marker lines and was detected;
  unchanged production source passed. The nightly lane includes the regression.
  This is completed local admission replay, not TCP loss, in-flight replay,
  systemd/GCP execution or monetary conservation.
- **[done]** Bounded dispatch-claim takeover while the native child is running:
  hold controller A's admitted response, expire its persisted claim, let B
  reclaim/finalize, then return A's response. Three memory/local/PostgreSQL
  cases preserve B's complete snapshot and one live execution, followed by one
  completion marker (`tests/test_dispatch_claim_takeover.py`). Independently
  reviewed and nightly-wired; this is an explicit claim-expiry fault, not an
  actual leadership/renewal failure, process crash or deployed failover.
- **[done]** Bounded renewal refusal after persisted claim expiry: the real
  heartbeat rejects A's expired claim before B takes over, leaving A's inner
  dispatch pending. Six takeover cases now join that inner dispatch before
  asserting the winner's snapshot and single execution. A seventh regression
  proves outer cleanup timeout cannot skip inner draining; cleanup attempts
  bounded joins for both controllers before transport teardown. Independently reviewed;
  the existing nightly file selection includes these cases. This is local
  memory/LocalJson/PostgreSQL qualification, not a database outage, process
  crash or deployed leadership failure.
- **[next]** Extend generated histories to renewal failures, interrupted
  priority-preemption phases and concurrent failover. The bounded
  controller and service properties do not establish conservation across every
  transition.
- **[next]** Verify the nightly seeded invariant lane on GitHub. CI now runs
  generated scheduler/budget properties, fixed feasible-subset regressions and
  PostgreSQL chaos cases together; the run number seeds Hypothesis and is
  retained beside JUnit results for replay. Local seed 20260910 plus workflow
  checks passed 41 tests with one monorepo-only skip. The existing PostgreSQL
  chaos cases remain deterministic, not randomized state-machine histories.
- **[next]** Complete mutation qualification across `eyvan/domain/`.
  The pinned, isolated [scheduler campaign](reference/scheduler-mutations.md)
  tested 1,576 mutants: the latest fresh seeded run detected 1,216 with 360
  surviving. Two independent seeded baselines had identical inputs/verdicts
  (1,213 detected / 363 survived). An explicit affordable-frontier example
  independently detects the three additional mutants; no baseline detection
  was lost. Nine earlier snapshot-validation mutations were independently
  detected. Source hashes stayed unchanged. Historical unseeded comparisons
  retain their stochastic/corpus caveat; they are not retroactively seeded.
  A separate costing campaign detected all 22 mutants after adding empty-total
  coverage (initially 21/22). Adjacent spend aggregation no longer reintroduces
  binary drift; its fresh campaign detected 74/76 mutants, with two explicitly
  classified equivalent internal-label changes. The latest costing/invariant/
  run-cost selection passed 49 tests. RunSpend properties were not mutated.
  Availability dataclass/property contracts now pass 23 new deterministic cases
  (29 with existing availability tests). Eighteen selected process-local
  sensitivity probes are detected with an unmodified control and unchanged
  source/test hashes; these are not a generated-mutant coverage percentage.
  Accelerator ordering now rejects nondecimal digit suffixes from numeric
  parsing instead of crashing diagnostics. Its fresh 15-mutant campaign improved
  from 13 to 14 detected after adding a bare-family/zero-size ordering contract;
  the sole remaining negative-sentinel change is equivalent for public sorting.
  Eleven fixed contracts and the configuration selection pass 23 tests; full
  required-PostgreSQL verification passes 6,900 with 17 monorepo-only skips.
  Carbon now rejects nonfinite intensities/windows; 24 new contracts pass
  (42 with existing tests), and 16 selected process-local changes are detected
  with an unmodified control. A separate fresh 34-mutant campaign improves from
  26 to 29 detected; five visible diagnostic changes remain unsuppressed and
  are not declared equivalent. Neither generated totals nor selected probes
  establish full dataclass coverage. Predictive budget undercounting is corrected
  using exact decimal integer ratios; 14 regressions pass (29 with existing
  policy tests). Its fresh 75-mutant campaign improves from 71 to 73 detected;
  two diagnostic-message changes remain unsuppressed and dataclass methods remain
  outside the generated denominator. Pricing adds 34 public contract cases
  (56 related tests pass), improving its fresh generated campaign from 56/85 to
  74/85 with unchanged production pricing source. Two separate dataclass probes
  detect missing sample normalization/time validation. Eleven generated survivors
  remain unsuppressed; these bounded results do not qualify all dataclass methods.
  Remaining scheduler survivors and other domain modules are still open;
  the tool's exit 0 with survivors is not a passing mutation gate.

## Testing (9 → 10)
- **[done]** Coverage ratchet: pytest-cov floor enforced in CI (Python) and
  v8 thresholds in `dashboard/vite.config.ts` (82.8% lines baseline, measured
  2026-09-07).
- **[done]** CI matrix configured across Python 3.11 / 3.12 / 3.13,
  matching `requires-python >=3.11,<3.14`. Python 3.10 support was dropped
  explicitly; hosted matrix execution remains separate from local checks.
- **[done]** `detect-secrets` baseline scan (130 known test-fixture entries)
  failing CI on new secrets.
- **[done]** Python dependency audit with pinned `pip-audit==2.10.1` and
  `--disable-pip --require-hashes` against the complete runtime export.
  This avoids the temporary-venv bootstrap that failed in the earlier
  attempt. Verified locally on Python 3.11 and 3.13: no known vulnerabilities
  and no skipped dependencies in the receipt. The blocking CI step uses the
  same command; advisory results are point-in-time, not a security guarantee.
- **[done]** Bounded fast smoke subset: `uv run pytest tests -m fast`,
  documented as the default iteration command in CONTRIBUTING.md. The local
  run passed 141 tests in 4.11 seconds, with 6,152 deselected. Explicit module
  selection lives in `tests/conftest.py`; full-suite collection is unchanged.
  The broader `not slow` subset is not the under-minute target. Nightly
  full-suite wiring remains tracked in CI/CD below.
- **[done]** Static gates: mypy ratchet in CI over the pinned scope in
  `pyproject [tool.mypy]` (domain, config, records, and the API policy
  modules — clean after it surfaced a stale preemption sort-key alias and two
  cross-loop variable rebinding bugs); ESLint 9 + Prettier for the dashboard
  (`dashboard/eslint.config.js`, `lint`/`format:check` scripts, both gated in
  CI); ruff check + format added to `.pre-commit-config.yaml`. The mypy scope
  grows, never shrinks.
- **[next]** Qualify the disposable developer PostgreSQL service at runtime.
  `docker-compose.test.yml` supplies a loopback-only, tmpfs-backed database
  requiring an explicit test password; CONTRIBUTING documents required-PostgreSQL
  full tests as the default developer gate. Configuration/documentation tests
  exist, but Docker is unavailable on this host (`Executable not found in
  $PATH: "docker"`). The real locally built PostgreSQL suite is separate evidence,
  not a successful Compose startup.

## Security (6.5 → 10) — the long pole
- **[done]** `.gitignore` covers key/certificate/credential filenames.
- **[done]** Compose Postgres binds loopback only.
- **[done]** GCP preflight: `eyvan up` fails fast, before creating anything,
  when the active account cannot bind cell secrets — with the exact role to
  request (`GcloudNativeWorkerSecretStore.ensure_caller_can_bind`, plus
  `tests/test_gcp_compute_provider.py` regressions and
  `docs/provider-gcp-iam.md`).
- **[done]** TLS by default: non-loopback binds refuse to start without
  `EYVAN_TLS_CERT_FILE`/`EYVAN_TLS_KEY_FILE` or an explicit
  `--insecure-plaintext` opt-in (`unsatisfied_startup_requirements`, the same
  list `eyvan doctor` reads), wired into both uvicorn call sites, with the
  refusal sentences pinned in `tests/test_startup_preflight.py`, the policy
  and a live TLS boot in `tests/test_server_tls.py`, and the helm chart /
  raw manifest updated (`controlPlane.tls.existingSecret` /
  `controlPlane.insecurePlaintextBind`). Rendered Helm health probes follow
  the listener protocol (`tests/test_helm_tls_probes.py`).
- **[next]** Complete internal-edge HTTPS and off-loopback mTLS defaults.
  Listener protection alone does not complete transport hardening; mTLS
  remains opt-in via `EYVAN_MUTUAL_TLS_*`. Preparatory low-level worker bootstrap
  now accepts an immutable Secret Manager TLS bundle and stages validated
  owner-only files, with exact disabled-output parity and local regressions.
  It is intentionally not wired into GCP fleets: stable endpoint/SAN identity,
  multi-host naming, issuance and rotation/replacement are still required.
  See `docs/security.md` for the supported boundary.
- **[next]** Finish security review of the implemented dashboard cookie flow.
  Dashboard tokens now use HttpOnly/Secure/SameSite cookies
  (backend-for-frontend): the server runs the OIDC flow end to end
  (`eyvan/server/api/dashboard_sessions.py` — login/callback/logout, PKCE,
  single-use transactions, nonce check, in-process session store), the API
  authenticates the `eyvan_session` cookie by verifying the stored access
  token, the dashboard builds no Authorization header from JavaScript-readable
  storage, and its HTML answers with a strict CSP plus `nosniff`/`DENY`
  companions. Covered by `tests/test_dashboard_auth_bff.py` (12 cases) and the
  rewritten `dashboard/src/auth` suites. Stated limits in `docs/security.md`:
  in-process store (sticky sessions for multi-replica), Secure cookie requires
  HTTPS. Follow-up regressions cover server-side lifetime limits and
  same-origin enforcement for cookie writes and logout
  (`tests/test_dashboard_session_expiry.py`, `tests/test_dashboard_cookie_csrf.py`).
  Store admission now caps each map at 1,024 entries without evicting live
  users (`tests/test_dashboard_session_capacity.py`). UI regressions cover
  failed logout reporting and the sign-out control for cookie-only identities.
  Provider responses now bind discovery/ID-token issuer, client audience,
  authorized party, subject, nonce and expiry before opening a session;
  malformed claims, cached-issuer mismatch and Unicode nonce failures have
  regressions. Provider error text is not copied into redirect URLs, and
  cross-origin logout cannot emit a deletion cookie even without a request
  cookie. The shipped Playwright browser harness passed login/logout,
  provider denial and an actual cross-site cookieless logout attack against a
  disposable HTTPS plane and fake IdP (see `docs/status.md`). A blocking CI
  step retains its JSON receipt. Hosted execution and verification of the
  deployed IdP, certificates and multi-replica routing remain open.
- **[done]** Remove the static default database password: the settings,
  Alembic, and migration defaults are now the password-less loopback URL
  (`postgresql+asyncpg://eyvan@127.0.0.1:5432/eyvan`, trust/peer auth),
  docker-compose already required an explicit operator-supplied Postgres
  password, the Kubernetes/helm paths always read `database-url` from a
  Secret, and `tests/test_no_static_default_database_password.py` scans every
  shipped surface so the credential cannot return.
- **[done]** Working threat model and per-release review checklist in
  `docs/design/0005-threat-model.md`, linked from the security model and checked
  by the docs suite. It states implemented controls and unresolved boundaries.
- **[next]** Maintainer approval of the checklist and independent external
  security review before 1.0; neither is inferred from agent review.

## Documentation (9 → 10)
- **[done]** `docs/provider-gcp-iam.md`: the exact permission inventory and
  the one grant that bites, written from a real multi-hour IAM outage.
- **[done]** `docs/status.md`: measured operational numbers with an explicit
  no-restatement-of-goals rule.
- **[done]** `eyvan demo` runs a disposable loopback API/agent/fleet and a
  real callable in a separate local process. It ignores saved contexts and
  creates no cloud resources. CLI success, failure and timeout cleanup have
  regressions; the command itself returned the expected result locally.
- **[done]** Generated REST API index and OpenAPI snapshot via
  `scripts/generate_api_reference.py`, with deterministic regeneration,
  stale-file detection and a CI check. Hidden routes are explicitly excluded
  from the schema-derived index; see `docs/reference/api-generated.md`.

## CI/CD & supply chain (9 → 10)
- **[done]** SBOM (CycloneDX 1.6) generation from the hashed docker export
  is wired into publishing; multi-Python matrix, detect-secrets and the
  verified no-pip-resolution Python dependency-audit command are CI gates.
- **[next]** Verify release provenance in a real GitHub Actions run.
  The pinned `actions/attest-build-provenance` step is now wired before
  publication for wheels, sdists, and the SBOM, and its commit is included
  in the canonical release lock. Local CI/release-policy tests pass; no
  attestation issuance or public release has been performed locally.
  Publishing explicitly selects wheel/sdist files, excluding SBOM JSON.
- **[next]** Complete the nightly lane: the full PostgreSQL suite now has a
  daily schedule, guarded by a workflow regression. The extended repository
  soak is wired with 1,000 cycles and mandatory PostgreSQL. Local memory and
  SQLite runs passed (265.4 seconds total). The scheduler-budget and 10k-cell
  lanes are also configured and pass locally; real nightly execution remains
  unverified.
- **[done]** Flaky-test investigation policy with quarantine tracking in
  `docs/testing-policy.md`: collection validates owner/issue/expiry metadata;
  no skip or xfail is injected. Nightly diagnostics repeat marked cases while
  the full suite stays blocking. Metadata tests and the empty-queue shell
  path were verified locally.

## DevEx (7.5 → 10)
- **[done]** The native fleet example explicitly declares Python 3.11 and
  preserves node-level interpreter overrides. Three red/green regressions and
  the 6,767-pass required-PostgreSQL checkpoint cover the configuration fix.
  The Spot attempt exposed an unsupported image interpreter; its
  explicit-version retry hit provider stockout before execution. Allocation
  and cleanup evidence in `docs/status.md` are not successful workload proof.
- **[done]** The IAM preflight above is the biggest DevEx fix: a 3-hour
  debugging session becomes a 30-second error message.
- **[done]** `eyvan doctor` local preflight suite
  (`eyvan/sdk/commands/doctor_preflights.py`): gcloud authentication, the
  Secret Manager bind permission (surfaced from the existing preflight via a
  queryable `caller_bind_state` that keeps unknown-is-unknown), artifact-store
  reachability/writability, runtime-profile validity, and control-plane port
  ownership. All five answer before anything is started, fail with the fixing
  command, and report not-applicable as PASS-with-reason. Covered by
  `tests/test_doctor_local_preflights.py`.
- **[done]** Per-cell provider errors appear in CLI fleet status and the
  dashboard, including detail-only diagnostics. CLI literal-text rendering
  and dashboard API/DOM regressions cover the behavior; live provider
  qualification remains separate from these local checks.
- **[done]** Versioned provider extension ABI 1.0: the existing
  `eyvan.capacity_providers` entry-point group requires explicit operator
  trust, refuses incompatible versions and backend-name collisions, and
  exposes a contract-validation gate. Discovery, ABI conformance and boundary
  tests pass; `docs/providers.md` now documents the actual API and distinguishes
  ABI versions from package versions and real-provider qualification.

## Observability (8 → 10)
- **[done]** `deploy/monitoring/prometheus-alerts.yaml`: six alert rules over
  verified metric names (invariant violations, unavailable capacity, stuck
  workloads, API error rate, scheduler p99, plane uptime).
- **[next]** Finish deployment qualification of the opt-in correlation flow.
  `EYVAN_CORRELATION_ENABLED=1` now links authoritative workload/cell ownership,
  persisted outbox records, mutation forwarding and provider-operation logs;
  relevant API responses expose the same safe identity. Flag-off wire parity,
  PostgreSQL round-trips, legacy IDs, task/thread scope and forwarding paths
  have regressions. This does not rewrite every arbitrary application log;
  see `docs/reference/correlation.md` for the exact scope.
- **[next]** Qualify deployed OpenTelemetry export and cross-process trace
  context. Outbox/provider spans and opt-in controller-to-agent W3C
  `traceparent` generation/extraction are implemented. Local SDK/ASGI tests
  cover authentication ordering, concurrent isolation, exception cleanup,
  malformed parents and a separate process receiving the serialized parent.
  Automatic propagation excludes baggage/vendor state. Exception-privacy
  regressions prevent raw provider errors escaping in span events/status.
  The opt-in native-worker hop now has local SDK/ASGI qualification, including
  authenticated parent continuity and parallel staging context propagation.
  Bootstrap rollout, durable queue trace-parent continuity and a deployed
  collector remain unqualified; see `docs/reference/correlation.md`.
- **[done]** Protected-API authentication failures can be retained as
  credential-free durable audit events with `EYVAN_AUTH_AUDIT_ENABLED=1`
  (off by default for compatibility). Memory, SQLite and PostgreSQL tests
  cover redaction and unscoped identity; a separate failure-path regression
  preserves the denial when audit storage fails. Existing role-grant and policy
  event ordering/actor/scope are covered by a new regression. Browser-login
  callback and external IdP audit coverage are not claimed; see
  `docs/security.md`.

## Performance (7 → 10)
- **[done]** Synthetic scheduler benchmark and CI budgets in
  `scripts/benchmark_scheduler.py` and `scripts/scheduler-budgets.json`:
  domain snapshot construction, ordinary scheduling and elastic packing,
  with correctness assertions and raw timing receipts. Local 1k/10k budgeted
  runs pass; methodology and limits are in
  `docs/reference/scheduler-benchmarks.md`.
- **[done]** Ordinary placement now materializes only its selected lease,
  preserving candidate scoring and admission checks. Twelve regressions and
  500 differential snapshots pass; local 1k/10k ordinary p99 improved from
  38.640/393.905 ms to 25.894/257.776 ms with unchanged budgets. These are
  local Python 3.11 samples, not proof of resolving the hosted tail failure.
  The post-change required-PostgreSQL suite passed 6,819 tests with 17 skips.
- **[next]** Verify the configured nightly 10k-cell simulation on GitHub's
  runner. The lane records 100 samples and retains p99 plus raw JSON.
  Full controller/database reconciliation-loop latency remains a separate
  measurement, not implied by these pure-scheduler timings.
- **[done]** Shared-primary multi-replica story in
  `docs/reference/high-availability.md`: real PostgreSQL 17.11 tests exercised
  advisory-lock transfer, terminated-holder recovery, concurrent claims and
  independent API-only reads. The guide states session-affinity requirements
  and distinguishes these checks from DB-primary promotion, production SLAs,
  and live-provider failover.

## Open-source readiness (8 → 10)
- **[done]** SECURITY.md, CODE_OF_CONDUCT.md, issue/PR templates (this
  session's earlier pass); `eyvan` confirmed available on PyPI.
- **[next]** Second maintainer with CODEOWNERS per subsystem — the true
  10/10 blocker; everything else here is process.
- **[next]** Roadmap + governance doc, monthly semver cadence, `good first
  issue` grooming.
- **[ongoing]** CHANGELOG discipline (Keep a Changelog + SemVer already
  stated; the Unreleased section exists).

## Innovation (9 → 10)
- **[next]** Complete Slurm / Volcano / kube-batch comparison experiments.
  The design write-up in `docs/design/0006-receipts-and-elastic-packing.md`
  now explains submission versus proof receipts, attribution limits, exact
  whole-cell packing and affordable alternatives, with a comparison protocol.
  It links existing local scheduler measurements; no competitor measurements
  or performance superiority are claimed.
- **[done]** Bounded live scheduler-priority checkpoint restoration on a
  four-chip Spot TPU: the batch trainer checkpointed at step 20,
  yielded to an interactive TPU job, and resumed on attempt 2 through step 40,
  matching an uninterrupted control. Both jobs succeeded and provider-side
  VM cleanup was verified (`docs/status.md`, campaign `priority-3ee7d85be890`).
  The checkpoint and restore used the same VM; this is not provider reclamation.
- **[done]** Bounded replacement-host recovery after user-authorized deletion
  of one owned Spot TPU VM: campaign `capacity-loss-a1c2f24faa77` resumed on a
  different Spot instance at attempt 3, restored step 20, completed step 40 and
  matched uninterrupted training. One capacity-loss event, zero failure/provider
  preemption events; teardown independently verified. The raw harness exit 1
  was a final log-field parser error; corrected verification of the unchanged
  receipt passed, including negative controls (`docs/status.md`). This was
  injected physical capacity loss, not natural provider reclamation.
- **[done]** Reusable dry-run-first `examples/spot_priority_demo.py` and portable
  training payload, documented in [tutorial 17](tutorial/17-spot-priority-demo.md).
  Campaign `8a7612f7f4974c36a456609f15ee16ca` ran the shipped example through
  Eyvan on four-chip Spot capacity: checkpoint 20, interactive yield,
  same-host batch attempt 2 restoration to step 40 matching uninterrupted control.
  Owned teardown had no reported errors; independent provider listing was empty.
  The example contains no destructive loss injection or private runtime profile.
- **[next]** Natural Spot reclamation and longer-running training qualification.
  The reusable example proves same-host priority restoration; the separately
  authorized private campaign above proves injected replacement-host recovery,
  not natural provider reclamation.

## Maturity (6 → 10)
- **[next]** Qualify upgrade behavior against an actual published release.
  `tests/test_wheel_upgrade.py` now builds an immutable development-baseline
  commit and a scratch-versioned candidate, installs both in sequence, runs
  migration and verifies PostgreSQL data survives. This passed locally and
  runs in CI. There are no release tags yet; it is not presented as a
  published-tag N to N+1 qualification.
- **[next]** 4–6 week production pilot on the Spot fleet with published SLOs;
  `docs/status.md` is the template this entry started.
