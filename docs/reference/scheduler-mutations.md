# Scheduler mutation testing

Mutation testing asks whether tests detect deliberately changed behavior. A
passing unmodified suite is necessary but not sufficient: a test called
“unpriced capacity is refused” can accidentally establish only that another
cell wins a tie-break.

## Reproduce in an isolated snapshot

The optional `[tool.mutmut]` configuration in `pyproject.toml` targets
`eyvan/domain/scheduler.py`. The current selection includes ordinary scheduling,
elastic feasibility/budget properties and atomic gang-preemption tests. It is
not the full Python suite and does not cover every other domain module.
The profile now passes `--hypothesis-seed=20260910` explicitly; a configuration
regression test guards that replay input. The first four historical scheduler
campaigns below were unseeded. The separate seeded comparison does not
retroactively claim that those older campaigns used the current profile.

Use **mutmut 3.7.0** with Python 3.11 and the checkout's pinned development
requirements. Keep the main checkout out of the mutation runner's working
directory. For example, from the repository root, create a fresh snapshot:

```bash
mkdir -p .pytest_cache/scheduler-mutation-snapshot
rsync -a --exclude=__pycache__ eyvan tests pyproject.toml README.md requirements-dev.txt .pytest_cache/scheduler-mutation-snapshot/
cd .pytest_cache/scheduler-mutation-snapshot
uvx --python 3.11 --from mutmut==3.7.0 --with-requirements requirements-dev.txt mutmut run --max-children 2
uvx --python 3.11 --from mutmut==3.7.0 mutmut results
```

Use an empty snapshot directory for an independent campaign. Do not copy a new
candidate over a prior run and assume old verdicts are fresh. Record hashes of
the source, selected tests, configuration and requirements alongside the result.
The tool writes instrumented copies below `mutants/`; **do not run `mutmut apply`
in the main checkout**. The installed tool's extra dependencies are resolved in
its isolated environment; this optional experiment is not part of the package's
hash-pinned runtime export or the blocking CI dependency gate.

`mutmut show MUTANT_NAME` displays a mutation. A manual reproduction can set
`MUTANT_UNDER_TEST=MUTANT_NAME` while running the selected pytest case from the
snapshot's generated `mutants/` directory using the same tool environment.
Always run the unmutated case too. A test-collection failure is not evidence
that the test detected the intended behavioral change.

## Local observations, 2026-09-10

All campaigns used isolated copies, not changes applied to the working source.

| Selection | Mutants tested | Killed | Survived |
|---|---:|---:|---:|
| Scheduler and elastic/budget properties | 1,576 | 1,168 | 408 |
| Same selection plus existing gang-preemption tests | 1,576 | 1,189 | 387 |
| Same selection plus new budget/diagnostic regressions, fresh snapshot | 1,576 | 1,204 | 372 |
| Same selection plus snapshot-validation regressions, fresh snapshot | 1,576 | 1,215 | 361 |

The tool reported no timeout/error/no-test verdicts in these runs. **Its process
exit status was 0 despite survivors.** That exit status is not a zero-survivor
quality gate, nor are these figures a score for the complete repository suite.
The last campaign confirmed all nine targeted snapshot-validation mutants were
killed and all recorded snapshot source, test, configuration and requirements
hashes were unchanged; no previously killed mutant regressed to surviving.

Local raw metadata was retained in
`.pytest_cache/scheduler-mutation-initial.meta.json` and each snapshot's
`mutants/eyvan/domain/scheduler.py.meta`, with input hashes in
`.pytest_cache/scheduler-mutation-inputs-initial.json`,
`.pytest_cache/scheduler-mutation-inputs.json`,
`.pytest_cache/scheduler-mutation-strengthened.inputs.json` and
`.pytest_cache/scheduler-mutation-validation.inputs.json`. The newest snapshot
is `.pytest_cache/domain-mutation-validation/`, with its `.exit` and
`.hash-check.json` receipts under `.pytest_cache/scheduler-mutation-validation`.
These workspace-cache files are local receipts, not hosted or signed artifacts.

## What survivor inspection established

- Replacing the lease-by-workload index value with `None` survived the initial
  selection. The existing gang-preemption tests killed it: the instrumented
  unmutated tests passed seven cases, while the mutation failed six. This was a
  **selection gap**, not a reason to invent a new production fix.
- Inverting the early hard-budget guard also survived. The old test offered a
  partially priced alternative, which could win even with the guard disabled.
  A new test offers only unpriced capacity and requires admission refusal with
  `UNPRICED_CELL`. It passes on the real implementation and fails on that mutant.
- Replacing a grouped-placement rejection diagnostic with `None` survived.
  New parameterized cases require both rejected cells and their typed reasons
  to remain visible for single, cell-group and elastic placement. The group case
  fails on this mutant while all three pass on the real implementation.
- Some inspected survivors do not change observable behavior: the private
  lease-by-ID index is written but not read, and changing a falsy optional
  argument from `False` to `None` preserves the tested condition. Other mutations
  only decorate diagnostic prose. These observations classify individual cases,
  not all remaining survivors.

Do not add assertions against unused private storage merely to improve the
score, suppress real mutants with broad exclusions, or rewrite public error
text just to match a mutation tool. Prefer observable admission, resource,
budget, fairness, preemption and diagnostic contracts.

## Targeted snapshot-validation follow-up

A later inspection found three disabled validation guards among the survivors:
active/pending duplicate attempts, leases referencing unknown cells, and
budgets using another currency. Seven new cases in `tests/test_scheduler.py`
exercise these failures and valid counterparts (a different attempt, a known
cell, and matching USD/EUR budgets). The ordinary scheduler file passes 51 tests.

An independent copy of the instrumented tree, not the previous campaign's
working directory, was probed with the strengthened tests. All nine targeted
`_validate_snapshot` mutants (`15`, `18`, `19`, `20`, `24`, `25`, `39`, `42`, `43`)
failed the intended test, while the unmutated instrumented scheduler passed.
Input hashes, including production scheduler source, remained unchanged.
The local receipt is `.pytest_cache/scheduler-snapshot-validation-probes.json`.
The initial probe had a collection/import-path failure; only the corrected,
explicitly isolated import-path run is detection evidence.

The subsequent fresh aggregate campaign detected 1,215 mutants, leaving 361
survivors. Nine newly detected mutations are independently tied to the new
regressions above; two others are in elastic scheduling and their changed
verdicts are not attributed to these tests without further isolation. These
campaigns did not pin a Hypothesis seed, so stochastic/corpus effects are a
comparison limitation. Other snapshot-validation survivors that change only
diagnostic field-label text were not automatically classified as equivalent
or suppressed. This is still not full-domain mutation qualification.

## Reproducible seeded scheduler comparison

Two independent fresh snapshots (A/B) with seed `20260910` produced identical
normalized input hashes and full verdict maps: **1,213 detected / 363 survived**
of 1,576. Both exited 0 with unchanged inputs. The two verdict differences from
the last unseeded campaign were elastic mutants `208` and `209`; neither was
reliably detected by that seeded baseline.

The budget property now retains an explicit example: cell `(workers, rate)`
pairs `[(3, 3), (2, 1), (2, 2), (1, 4)]`, a three-worker request and budget 48.
Each worker has eight TPUs and the reservation lasts one hour, so the whole
three-worker cell costs 72 at its per-TPU hourly rate. The cheaper two-worker
cell plus the one-worker cell costs exactly 48. The dynamic
program must retain the cheapest intermediate frontier and must start its
empty selection at zero cost.

A separate instrumented control passes; mutants `194`, `208` and `209` each
fail this **explicit** example. These are semantic assertion failures, not
collection errors. The receipt is `.pytest_cache/scheduler-frontier-probes.json`.
Fresh seeded campaign C then reports **1,216 detected / 360 survived** of 1,576,
exit 0, unchanged inputs, and no error/timeout/no-test verdicts. Its only verdict
changes relative to A are these three newly detected mutants; no old detection
was lost. Receipts use `.pytest_cache/scheduler-mutation-seeded-{a,b,c}` and each
snapshot's scheduler metadata. Production scheduler code was not changed.

This is a reproducible bounded improvement, not a passing full-domain gate.
Inspection of mutmut 3.7.0's `file_mutation.py` also confirms that it deliberately
skips decorated classes and most decorated functions, including properties.
Dataclass/property-heavy domain code needs an additional qualification strategy;
zero generated mutants there must not be reported as complete coverage.

## Costing-domain follow-up

A separate isolated `eyvan/domain/costing.py` campaign used the same pinned
mutmut/Python environment and only `tests/test_costing.py`. Those selected tests
are deterministic and do not use Hypothesis. In the snapshot's configuration,
the scope overrides are:

```toml
only_mutate = ["eyvan/domain/costing.py"]
pytest_add_cli_args_test_selection = ["tests/test_costing.py"]
```

The initial run detected 21 of 22 mutants. The sole survivor,
`eyvan.domain.costing.x_total__mutmut_5`, removed `Decimal(0)` from `sum`'s initial
value. For an empty charge collection, that makes the sum an integer and
`_to_float` raises `AttributeError` instead of returning a JSON-safe zero.
`test_an_empty_charge_collection_has_json_safe_zero_total` now asserts the
observable empty-total result and its float type. Production code was already
correct and was not changed.

A fresh campaign with that regression detected **all 22 generated mutants**,
including the previous survivor; no timeout/error/no-test verdicts occurred.
Both campaign exits were 0 and their recorded input hashes were unchanged.
The local snapshots are `.pytest_cache/domain-mutation-costing/` and
`.pytest_cache/domain-mutation-costing-strengthened/`; receipts use stems
`.pytest_cache/costing-mutation` and
`.pytest_cache/costing-strengthened-mutation` (`.inputs.json`, `.hash-check.json`,
`.exit`, `.log`). The minimum-Python costing/invariant/run-cost selection passed
37 tests. This qualifies only this generated costing mutation set, not every
possible arithmetic fault or the entire domain.

## Spend aggregation follow-up

Inspection of the adjacent `spend_of` reporting helper found a real arithmetic
inconsistency: adding three persisted `0.1` charges returned
`0.30000000000000004`, reintroducing the drift settlement avoids. Settled cost,
reserved cost and accelerator-hours now accumulate through `costing.money` and
return to floats only at the wire boundary. Per-contribution quantization is
intentionally avoided so tiny measurements survive. Six regressions cover all
three totals with `0.1` and `4e-11` contributions. The first three cases failed
before the fix; the resulting 43-test costing/invariant/run-cost selection
passed. A bounded independent read-only review found no actionable regression.

A separate deterministic `tests/test_run_cost.py` campaign generated 76 mutants
in `spend_of`: initially 72 were detected. Six additional invalid-number cases
preserve the existing exclusion of bools, strings, nulls and containers, while
still totaling valid neighboring leases. A fresh campaign then detected **74**;
two survived, with no error/timeout/no-test verdicts and unchanged input hashes.

The retained survivors are `x_spend_of__mutmut_37` and `38`. They replace the
internal target label `"reserved"` with `"XXreservedXX"` or `"RESERVED"`. That
label is only compared against `"settled"`, is never returned, and both new
strings take the same reservation branch as the original for every lease
document. They are classified as behaviorally equivalent for this interface,
not silently suppressed or counted as tool-detected. Raw verdicts remain 74/76.
The generated set contains no mutants for the `RunSpend` property methods;
these results do not qualify those methods or every possible reporting fault.

Local snapshots are `.pytest_cache/domain-mutation-spend/` and
`.pytest_cache/domain-mutation-spend-strengthened/`. Receipt stems are
`.pytest_cache/spend-mutation` and `.pytest_cache/spend-strengthened-mutation`,
with the same suffixes as the costing campaign above. Scope overrides select
`eyvan/domain/spend.py` and `tests/test_run_cost.py`; all other pinned-tool
instructions are unchanged.

## Still open

Verify fresh seeded verdict stability, classify remaining scheduler survivors,
expand relevant test selection and extend to the other `eyvan/domain/` modules.
Re-run aggregate campaigns whenever their selection or tests change. A future
blocking lane needs an explicit reviewed treatment of equivalent mutants and
meaningful surviving mutations. These local results do not close the entire
mutation-testing roadmap item.
