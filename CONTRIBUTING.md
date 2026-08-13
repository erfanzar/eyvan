# Contributing to Eyvan

## Before you start

Eyvan is developed in a monorepo and mirrored here, so a patch to this
repository is applied upstream and mirrored back. Open an issue before a large
change; a rejected 2,000-line branch helps nobody.

## Running the tests

```bash
uv venv --python 3.13 && uv pip install --require-hashes -r requirements-docker.txt
uv pip install --require-hashes -r requirements-dev.txt
uv pip install --no-build-isolation --no-deps -e .
pytest tests
```

The suite runs on SQLite by default. Anything touching persistence must also
pass against the backend that is actually deployed:

```bash
EYVAN_REQUIRE_POSTGRES=1 EYVAN_TEST_POSTGRES_URL=postgresql+asyncpg://... pytest tests
```

Dashboard:

```bash
npm --prefix dashboard ci && npm --prefix dashboard test
npm --prefix dashboard run build      # vitest does not type-check; the build does
```

## What a good change looks like

- **Tests assert behaviour, not implementation.** Assert a persisted row, an
  emitted byte, an exit code, a returned value. A test that asserts a mock was
  called passes when the feature is broken.
- **Unknown is a state.** Never render an unreadable value as zero, empty, or
  healthy. If something cannot be determined, say so and keep it distinct.
- **No external I/O inside a database transaction.** Not a provider call, not an
  agent call, not an object-store read.
- **The scheduler stays pure.** `Scheduler.schedule` takes a snapshot and returns
  decisions; every decision is revalidated in a transaction before it applies.
- **A behaviour change to a wire format or an operator-visible default ships
  behind a flag that defaults to today's behaviour.**
- **Do not claim what you did not verify.** If a change is untested on hardware,
  say so in the commit message. Overstating evidence is the one thing that will
  get a patch rejected outright.

## Style

`ruff check` and `ruff format` must pass, line length 121. Google-style
docstrings. Import contracts are enforced: `uv run lint-imports` upstream, and
the layering is checked in CI.

Commit subjects are imperative and under 72 characters. No self-credit trailers.
