# Release readiness — 0.0.1

> Historical audit, not the current support matrix. Python now requires
> `>=3.11,<3.14`; references below to Python 3.10 and missing Ruff setup
> describe the audited tree. See `docs/roadmap.md` for current work.

Audit performed 2026-09-03 on `main` at `e57fa18` (latest commit dated
2026-08-31). Scope: version consistency, quickstart accuracy, licensing,
release artifacts, open TODOs, CI gates from configuration plus local runs,
and the gaps named by prior scans. The full pytest suite was deliberately not
run for this audit.

**Verdict: NOT READY** — two blockers, both trivially fixable (one ruff format
run; one README line). Everything else is should-fix or acceptable for a
0.0.1.

## Verification commands run

| command | result |
|---|---|
| `uv lock --check` | exit 0 — `Resolved 67 packages in 9ms`; the lock matches the manifest |
| `uv run ruff check eyvan tests` | exit 0 — `All checks passed!` (ruff 0.14.13) |
| `uv tool run --from ruff==0.15.16 ruff check eyvan tests` | exit 0 — `All checks passed!` (the version CI pins) |
| `uv run python -c "import eyvan; print(eyvan.__version__)"` | exit 0 — prints `0.0.1` |
| `uv sync` | exit 0 — built and installed `eyvan==0.0.1` editable plus 40 packages |
| `uv run eyvan --help` (after `uv sync`) | exit 0 — `start`, `doctor`, `up`, `proof` and the rest of the command tree present |

## Findings

### BLOCKER

1. **CI ruff format gate is red on current `main`.** CI pins
   `ruff==0.15.16` (`.github/workflows/ci.yaml`, "Ruff" step) and runs
   `ruff format --check eyvan tests`. Locally, that exact command reports
   **13 files would be reformatted** (565 already formatted), including
   `eyvan/server/api/app.py`, `eyvan/domain/scheduler.py`, `eyvan/secrets.py`,
   and ten test files. `ruff check` passes; only the format step fails.
   Remedy: one `uv tool run --from ruff==0.15.16 ruff format eyvan tests`
   pass. Pure whitespace — no behavior change — but it touches files another
   in-flight change (license headers) may also touch, so sequence it.

2. **README developer quickstart installs no dev dependencies.**
   `pyproject.toml` declares dev tooling under
   `[project.optional-dependencies] dev` (pytest, pytest-asyncio, aiosqlite),
   which is a pip *extra*, not a uv *dependency-group*. Verified: after plain
   `uv sync`, `uv pip list` contains no pytest. So the README's
   "uv sync — editable install, dev deps included" followed by
   `uv run pytest tests -q` fails for a new contributor on the second step.
   Remedy: `uv sync --extra dev`, or move the extra to `[dependency-groups]`.

### SHOULD-FIX

3. **README "sixty-second version" omits the `uv run` prefix.** After
   `uv sync`, bare `eyvan start` / `eyvan doctor` only work with the venv
   activated; the commands themselves exist and work
   (`uv run eyvan --help` lists both). One-line doc fix.

4. **README lint comment is stale:** it says "lint (line 121, py3.11)" but
   `pyproject.toml` sets `target-version = "py310"` (and `requires-python`
   allows 3.10). Also, ruff is not in the dev extras at all, so
   `uv run ruff …` resolves whatever ruff uv finds (0.14.13 here) while CI
   pins 0.15.16 — exactly the kind of drift that produced blocker 1. Pin ruff
   in the dev extra.

5. **Dashboard version drift.** `dashboard/package.json` says `0.1.0` while
   `pyproject.toml`, `eyvan/_version.py`, and `deploy/helm/eyvan/Chart.yaml`
   (`version` and `appVersion`) all say `0.0.1`. If the dashboard is
   versioned with the plane, align it; if independently, say so in
   `dashboard/`.

6. **Pre-commit hook description names the wrong license.**
   `.pre-commit-config.yaml` describes the hook as "Apache-2.0 header on
   Eyvan sources", but `scripts/license_headers.py` actually enforces
   `SPDX-License-Identifier: AGPL-3.0-or-later`. Cosmetic wording bug in the
   hook metadata, not in the headers. (License-header content itself is owned
   by a separate change.)

7. **No type checker gate in CI.** A commit once "cleared eyvan's
   basedpyright errors", but no workflow runs basedpyright/mypy, so type
   regressions re-enter silently. Add a CI step, or accept the drift
   explicitly.

8. **No coverage measurement or gate.** Tests are extensive (host, static,
   real-PostgreSQL suites per `docs/proofs.md`), but nothing reports what
   they miss. For 0.0.1 this is tolerable; add `coverage` or `pytest-cov`
   reporting before 0.1.0 claims of maturity.

### ACCEPTABLE for 0.0.1

9. **Version consistency is otherwise clean:** `pyproject.toml` 0.0.1,
   `eyvan/_version.py` `__version__ = "0.0.1"`, Helm `Chart.yaml`
   `version: 0.0.1` / `appVersion: "0.0.1"`. Docs quote no version numbers
   that disagree.

10. **LICENSE ↔ declaration match.** `LICENSE` is the GNU AGPL v3 text and
    `pyproject.toml` declares `AGPL-3.0-or-later`; source SPDX markers agree.

11. **No git tag or GitHub release yet.** `git tag --list` is empty. This is
    by design: `publish.yaml` is tag-driven (`v*`) and refuses a tag that
    disagrees with `pyproject.toml`. Cutting `v0.0.1` is the release act
    itself, not missing work. A `CHANGELOG.md` now exists (added with this
    audit).

12. **No open TODOs/FIXMEs in source.** Grep over `eyvan/` finds zero; the
    single `tests/` hit is a test asserting that a bare "TODO" is not an
    acceptable justification string — meta, not debt.

13. **No TLS on the main uvicorn surfaces.** `eyvan-server`, `eyvan-agent`,
    and `eyvan-native-worker` call `uvicorn.run` without `ssl_*` kwargs; TLS
    exists only on the mTLS agent surface (`eyvan/identity/mtls.py`).
    `docs/install.md` already lists "TLS termination … in front of every
    external agent" as a platform prerequisite, and the default bind is
    loopback with dev auth refused on network binds. Acceptable for 0.0.1 as
    a documented deployment requirement; native TLS options belong on the
    post-0.0.1 list.

14. **Callable signing is optional.** The standalone HTTP API path submits
    unsigned callables (`signing_key` defaults to `None`), gated by
    `EYVAN_TRUSTED_CALLABLE_PROJECTS` trust rather than signatures.
    Documented in `docs/security.md` and `docs/install.md` (item 10 of "the
    fifteen"). Acceptable for 0.0.1; requiring signatures for
    network-exposed planes is a hardening candidate.

15. **Full pytest suite not executed for this audit** (explicitly out of
    scope). CI runs it against real PostgreSQL with
    `EYVAN_REQUIRE_POSTGRES=1`; the README cites ~7 minutes. Its current
    green/red state is unverified here — run it before tagging.

16. **Hardware qualification is partial, and honestly so.** `docs/proofs.md`
    records P0–P2 passes and open items (P3's in-job submission on
    accelerators, P5/P6/P7 pending). The docs themselves insist no historical
    run qualifies the current candidate without an exact-candidate receipt —
    that is a policy the release inherits, not a defect.

## Release checklist implied by the above

1. Run `ruff format` with the CI-pinned 0.15.16 and commit (blocker 1).
2. Fix the two README lines (blocker 2, should-fix 3–4).
3. Run the full suite (`uv run pytest tests -q` with Postgres) once, green.
4. `git tag v0.0.1` and push — `publish.yaml` verifies tag ↔ pyproject
   agreement and builds with the locked backend; the OCI workflow
   (`eyvan-oci-release.yaml`) and Helm chart are already in the tree.

## Current follow-up — 2026-09-03

This section supersedes the historical *current-main* statements above without
rewriting the audit record. The repository is still at `e57fa18` (2026-08-31),
but the working tree is not a release candidate: it contains extensive
uncommitted changes, and this documentation file is untracked. The observations
below therefore describe the present working tree, not a tagged or approved
release artifact.

### Documentation fixes observed

The two README-related blockers from the historical audit are no longer true
of the current tree: the development quickstart uses `uv sync --extra dev`,
commands use the `uv run` prefix, and the lint note says `py3.10`. The related
stale metadata is also corrected in the current tree: `dashboard/package.json`
is `0.0.1`, and `.pre-commit-config.yaml` names the AGPL-3.0 header it checks.
These are observed documentation/metadata changes only; no release workflow or
runtime code was changed by this follow-up.

### Verified checks (current working tree)

| check | result |
|---|---|
| `uv lock --check` | passed; uv reported `Resolved 67 packages in 14ms` |
| `uv tool run --from ruff==0.15.16 ruff check eyvan tests` | passed; `All checks passed!` |
| `uv tool run --from ruff==0.15.16 ruff format --check eyvan tests` | passed; `578 files already formatted` |
| `npm --prefix dashboard run build` | passed; TypeScript check and Vite production build completed |
| `npm --prefix dashboard test -- --run --reporter=dot` | passed; 20 files and 270 tests |
| `uv build --wheel --sdist --out-dir dist .` | passed; wheel and sdist built for `eyvan==0.0.1` |
| installed-wheel CLI smoke | passed; all five entry points answered `--help`, import/version returned `0.0.1` |
| source TODO/FIXME scan (`grep -R -E 'TODO|FIXME' eyvan`) | no matches (exit 1, as expected for no matches) |
| `git tag --list` | no tags present |

### Checks not run or not passing

The dashboard test gate was initially red because DOM-dependent suites lacked
their `@vitest-environment jsdom` opt-in markers. Those test fixtures were
corrected in the working tree and the full dashboard suite now passes:
`npm --prefix dashboard test -- --run --reporter=dot` reports 20 files and
270 tests passed.

The full Python suite against PostgreSQL and the complete CI workflow were
not completed in this follow-up. The pinned development dependencies were
installed into `.venv`, but the PostgreSQL-backed run could not connect and
was stopped after its connection wait; `pg_isready` and `docker` are absent
locally. No live control-plane, provider, or spend operation was performed.

The dashboard dependency audit initially reported one high-severity
`browserslist` vulnerability. Updating the transitive dependency with
`npm --prefix dashboard update browserslist --save-dev` resolved it; a fresh
audit now reports `found 0 vulnerabilities`.

### Release blockers

* The full PostgreSQL-backed Python suite remains unverified because local
  PostgreSQL is unavailable.
* A clean, reviewed candidate SHA is still required. The dirty worktree means
  the passing static checks above do not qualify `e57fa18` or any release tag.
* Deployment/migration/rollback rehearsal, provider qualification, hardware
  proofs, and release-owner approval remain outstanding.

The historical Ruff-format and README blockers should not be carried forward
as open blockers: the current tree provides direct evidence that those specific
conditions are fixed. This does **not** establish release readiness.

### Deployment and hardware qualification

No deployment was installed or exercised in this follow-up. `docs/install.md`
still requires PostgreSQL, schema/migration handling, identity configuration,
and TLS termination for an external deployment. `docs/proofs.md` explicitly
states that historical hardware runs do not qualify the current candidate
without an immutable exact-candidate receipt; P3 accelerator submission and
later proof items remain open there. Accordingly, deployment and accelerator
qualification are **not verified** for the current candidate.

### Migration and rollback

No migration, backup/restore, or rollback rehearsal was run. The documented
maintenance-only migration sequence and rollback matrix remain prerequisites;
see `docs/install.md` and `docs/artifact-safety.md`. Do not infer migration
safety from the lock, lint, build, or unit-test results above.

### Operator approval

No operator or release-owner approval is recorded by this follow-up. Approval,
release signing/tagging, deployment authorization, and the associated exact
candidate receipt remain pending explicit human review.
