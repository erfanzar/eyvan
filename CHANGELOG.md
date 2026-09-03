# Changelog

All notable changes to Eyvan are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — 0.0.1

First public release candidate. Development history begins with the initial
import on 2026-04-15 and runs through 2026-08-31 (the latest commit at the time
of writing); no git tag has been cut yet, so this section collects everything
that 0.0.1 will contain.

### Added

#### SDK and workloads

- `@execute` decorator over `TpuConfig`, `GpuConfig`, and `CpuConfig`: submit a
  signed-or-unsigned Python callable, block with live progress narration, or
  detach into an `EyvanJob` handle with `status()`, `logs()`, `wait()`,
  `result()`, and `cancel()`.
- Elastic worker packing by default (`atomic=False`): pack warm cells of the
  same type and provision only the deficit, with one JAX world per cell; exact
  single-resource shapes remain opt-in via `atomic=True`.
- `workspace=` shipping of a directory tree to the worker's working directory,
  extracted per content digest, with reuse of a completed extraction and the
  caller's working directory shipped by default.
- Per-job `dependencies` installed on the cell before the callable loads, so
  adding a library no longer requires rebuilding an image.
- Checkpointing surface: `checkpointable`, `checkpoint_hook`, and
  `checkpoint_output_uri`, with preemption-checkpoint time granted only when a
  real hook is named; gang checkpointing across several leased cells.
- In-job submission: `can_submit_children` mints a submit capability under the
  caller's identity, plus `eyvan.report_progress`, priority flags on submit,
  and job addressing by name or prefix.
- Placement controls: a submitter can say where a job may run and what it falls
  back to; jobs can name hardware directly instead of a pool.

#### Control plane

- Durable control plane (`eyvan-server`): scheduler, capacity planner, and
  durable state on SQLAlchemy with Alembic migrations; a restarted plane
  reconciles the world from its store, with the outbox claimed at
  `READ COMMITTED`.
- Capacity providers behind a provider ABI with a conformance suite:
  `local`, `gcp-tpu`, `gcp-compute`, the `gcp` umbrella, `aws-ec2` and the
  `aws` umbrella, `azure-vm` and the `azure` umbrella, `kubernetes` (`k8s`),
  `manual`, and `onprem-ssh`. Nothing that decides names a provider; the
  boundary is enforced by a test.
- A single sourced hardware catalog drives accelerator shapes, per-host CPU /
  memory / TPU resources, and TPU profiles; v6e and v5p slice-size coverage was
  corrected against it.
- Multi-host TPU slices and multislice allocations, with the attempt's global
  rank topology frozen at dispatch and multi-cell gangs preempted whole.
- Warm windows and cost estimates reachable through the API; a run's cost
  totalled in the plane; reclaimed Spot capacity reported and budgeted as
  preemption, never as stockout.
- Warm-cell reuse: a freed cell is held for reuse, leased cells are excluded
  from the stale-build recycle sweep, and warm cells count toward an elastic
  workload's progress.
- Predictive scale-up gated behind a funded budget; carbon contract modelled
  and deliberately kept out of placement.
- Managed services: service submission, replica endpoint registration, health
  probes, replacement of failed replicas, and autoscaling.
- I1–I9 invariant checks published by the plane, with a read-only SQL console
  and a process-introspection route for operators.
- Federation between peer control planes, including federated quota grants
  consulted at admission.

#### Artifacts, logs, and secrets

- Content-addressed artifact store: identical callables deduplicate by digest,
  a lost upload race verifies the winner, and the durable artifact lifecycle
  state machine is fenced against stale races. Managed artifacts are served and
  collected through the plane API with tenant isolation, versioned storage, and
  garbage-collection planning from workload references.
- Structured logs: per-rank pages served from journald, an archived
  schema-2 log manifest with cursor-based resumption, SSE follow, log retention
  decoupled from the artifact window, and log lines attributed to the worker
  (and reachable address) that produced them.
- Secret delivery by reference: `secret_mounts` resolve at dispatch into each
  attempt's environment, with dispatch-time hardening and rotation of the
  workload and artifact capability secrets.

#### Identity and tenancy

- OIDC authentication, durable role grants, organizations and projects, pool
  ownership, and cross-tenant isolation tests gating new routes.
- Workload identity: credential rotation, metadata, and an mTLS surface for
  agents (`eyvan/identity/mtls.py`).
- Scoped dashboard authority: the loopback plane authenticates the dashboard
  it serves, and a refused credential closes the dashboard it had opened.

#### CLI, dashboard, and operations

- Full CLI: `start`, `connect`, `init`, `doctor`, `up`, `down`, `run`,
  `workload` (including `exec` and `kick`), `capacity`, `quota`, `budget`,
  `policy`, `grant`, `org`, `project`, `federation`, `service`, `lease` and
  cell management, `query`, `events`, `webhook`, `proof`, `soak`, `head`,
  `runtime`, and `artifacts` — including eleven previously unreachable API
  routes wired into commands.
- Web dashboard: home, fleets, workloads, quotas, budgets, and billing pages;
  live dispatch pipeline stage strip; placement-explain and warm-cost panels;
  SSE-driven refresh; browser log streaming with a resume protocol; in-page
  exec and host out-of-service actions; dark and light themes on the Eyvan
  Console design system.
- Executable proofs (`eyvan proof catalog | children | logs | preemption |
  warm-window | secret | kubernetes`) with four exit codes: pass, fail,
  inconclusive (did not exercise the behaviour), and refusal — plus a soak
  observer for unattended runs, recorded in `docs/proofs.md`.
- `eyvan head up`: one-command GCP head bring-up that provisions or adopts a
  labelled VM, generates required credentials, runs migrations, and installs
  the plane under systemd.
- Local loopback path: `eyvan up` on a laptop fleet with a local daemon that
  owns its PID record, adopts healthy planes, and survives restarts.

#### Packaging and deployment

- Five console entry points: `eyvan`, `eyvan-server`, `eyvan-agent`,
  `eyvan-native-worker`, `eyvan-migrate`.
- Helm chart (`deploy/helm/eyvan`) with a pre-install migration hook, hardened
  shipped manifests, split liveness/readiness probes, and runtime-served
  dashboard OIDC config.
- Dockerfiles with pinned interpreter versions for the plane, agent, and
  worker; docker-compose for local deployment.
- CI: Python suite against a real PostgreSQL service (plus a weekly scheduled
  fault-matrix run), dashboard tests and build, package build with every entry
  point smoke-tested from an installed wheel, hash-pinned requirement exports
  checked against `uv.lock`, and pinned-tool ruff lint and format gates.
- Tag-driven publish workflow using PyPI trusted publishing (no stored token),
  refusing a tag that disagrees with `pyproject.toml`, and a gated OCI release
  pipeline.

### Changed

- Relicensed under AGPL-3.0-or-later (previously Apache-2.0); source headers
  carry the AGPL SPDX marker, enforced by `scripts/license_headers.py`.
- Removed the Ray Jobs adapter and the Ray dependency; scheduling, gangs, and
  logs are Eyvan-native.
- Regrouped the package into named architectural layers and split
  `EyvanController` into one mixin per concern.
- README and docs rewritten from the current code (twice), grounded in
  receipts; stale ROADMAP and SECURITY pages deleted in favour of
  `docs/proofs.md` and `docs/security.md`.
- Dashboard restyled from accumulated values onto a design system, then onto
  the Linear-inspired Eyvan Console language.
- One profile-pinned image becomes every generated fleet's container.
- A generated fleet can declare the Python its workers run, and the worker
  environment is built on it with uv, including the accelerator stack.
- Second and third clouds added under the conformance suite, with late-arriving
  and timed-out capacity modelled.

### Fixed

- Local native runtime path that provisioned TPUs but never ran anything.
- Startup races: a losing race no longer destroys the winner's PID record, and
  the winner is adopted on its record rather than a fresh health probe.
- A refused capacity attempt is no longer read as the job's death; an
  unreadable rank is no longer read as a dead one; an unreachable agent is no
  longer treated as a provider verdict.
- Provider request timeouts no longer fire for requests that were never made;
  the autoscaler stops asking a dead zone forever, and re-apply no longer
  throws away capacity.
- Spot preemptions are charged to the preemption retry budget, not the job's
  own failure budget and not to stockouts — including reclamations observed
  after `PREEMPTED` faded.
- Fleet-state API path stays alive across an expired token; waits survive a
  client restart; `eyvan down` no longer wedges the plane or kills the plane
  it started.
- Dispatch refuses cells running a different build, cells whose PYTHONPATH
  cannot reach their workers, callback addresses a worker cannot reach, and
  secret mounts on executors that ignore them.
- Quota page no longer times out on its own read; 91 of 93 v5p slice sizes a
  zone offers are no longer refused; one row per TPU quota with seventh-
  generation support.
- v5p zone racing recorded: identical submissions race zones, a lost upload
  race verifies the winner instead of crashing the submitter, and concurrent
  identical submissions are safe.
- Gang-join heartbeats and unit-supervisor bookkeeping kept out of transcripts;
  blocking waits stream output like `ray` does; same-state progress renders
  without reprinting headings.
- Dashboard: forwarded log reads bounded at five seconds, placeholder columns
  dropped on archive-replayed lines, identifiers kept whole, detail denials
  fenced, and a slow log fetch no longer blanks the workload page.
- User-facing errors name their exits; durable commands that keep failing back
  off and say when they stop being normal; `gcloud` failures report the CLI's
  own reason.

### Security

- Callable trust enforced on every submission route, with trusted projects
  configurable via `EYVAN_TRUSTED_CALLABLE_PROJECTS`; a GPU callable is refused
  before upload rather than after, and a callable pinned to another interpreter
  is refused before a TPU is built for it.
- Secret material is never delivered in clear text without explicit consent;
  the reference (never the value) is what persists.
- Bearer token required on `/metrics`; workload, artifact, and node credentials
  rotate; an unreadable node credential is reported rather than breaking every
  command.
- Log cursors are kept out of access logs and redacted from httpx's outbound
  request logs; bad cursors are refused instead of restarting the read.
- Profile directories and per-owner records are created with private
  permissions whichever component makes them; the plane's own dashboard
  credential is readable only by its owner.
- Dependency supply chain: hash-pinned requirement exports
  (`--require-hashes`), a pinned-and-hashed `uv-build` backend on every build
  path, pinned action SHAs in workflows, and trusted publishing with no PyPI
  token in the repository.
- Pre-open-sourcing scrub: working notes, real account names, and real project
  IDs removed from the tree.

[Unreleased]: https://github.com/erfanzar/eyvan/commits/main
