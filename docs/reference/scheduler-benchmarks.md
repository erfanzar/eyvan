# Scheduler regression benchmarks

Behavioral sensitivity is measured separately by the
[scheduler mutation campaign](scheduler-mutations.md); a timing budget passing
is not evidence that tests reject incorrect scheduling decisions.

Run the same deterministic CPU simulation as CI:

```bash
uv run python scripts/benchmark_scheduler.py --cells 1000 --iterations 20 \
  --budget-file scripts/scheduler-budgets.json --output benchmark-results/scheduler-1000.json
uv run python scripts/benchmark_scheduler.py --cells 10000 --iterations 100 \
  --budget-file scripts/scheduler-budgets.json --output benchmark-results/scheduler-10000.json
```

## What is measured

Each independent iteration constructs mixed-width domain capacity cells, places
four ordinary workloads, then packs one elastic workload with up to 32 workers.
There is one unrecorded warm-up iteration. The harness verifies that every
workload was placed and the elastic worker total is exact before accepting the
sample. It reports nearest-rank p50/p99 and retains all raw samples, interpreter
and platform identity, input sizes, and budget violations.

The three phases are domain snapshot construction, `Scheduler.schedule` for the
ordinary workloads, and elastic packing. This is **not** a database snapshot,
full controller reconciliation loop, provider API, or accelerator benchmark.
The Spot capacity class is modeled data only: no cloud machines are acquired.
A p99 from a finite simulation sample is not a production tail-latency promise.

## Initial local observations

Measured on the local Darwin/Python 3.13 environment during this change:

| Cells | Recorded samples | Snapshot p99 ms | Schedule p99 ms | Elastic p99 ms |
|---:|---:|---:|---:|---:|
| 1,000 | 20 | 4.1 | 15.2 | 15.0 |
| 10,000 | 100 | 62.8 | 178.6 | 144.4 |

The corresponding checked-in budgets are 25/75/75 ms for 1,000 cells and
300/900/750 ms for 10,000 cells. These are deliberately coarse regression guards
with hardware variance allowance, not statistical confidence intervals or
measured GitHub-runner baselines. Both budgeted runs passed locally. Raising a
budget requires an explained performance change and retained before/after
receipts; do not automatically fit budgets to the newest run.

The script writes its receipt before exiting 1 for budget violations. Missing,
malformed, incomplete, negative or non-finite budgets are errors, not a reason
to disable enforcement. Tests exercise a passing scenario and an intentionally
zero-budget failure that still preserves its receipt.

## CI

The Python 3.12 matrix entry runs the 1,000-cell guard on normal CI events. The
nightly schedule adds the 10,000-cell simulation with 100 recorded samples.
Both use `scripts/scheduler-budgets.json`; workflow artifacts retain the raw
JSON even when a budget check fails. A prior build/test failure may prevent the
benchmark from running, in which case no performance result has been measured.

Real runner measurements, controller/database scaling and hardware validation
remain distinct evidence. In particular, the repository soak is a different
workload and is documented in `docs/testing-policy.md`.

## Investigating tail failures without waiving the gate

Run an additional diagnostic sample with `--diagnostics` and a different output
path. It adds per-phase process CPU-time samples and collection-count deltas
for each GC generation. It does not force collection, disable GC, change the
scenario, discard slow samples, or change the wall-time budgets. Instrumentation
itself adds overhead, so do not silently substitute its receipt for the original.
With 20 samples, nearest-rank p99 is the maximum; inspect the whole distribution.
Collection counts show overlap, not how much elapsed time GC consumed. CPU versus
wall time is another diagnostic signal, not proof of a specific contention source.

CI now schedules this additional sample only when the original 1,000-cell budget
step fails. Both receipts are retained; even a passing diagnostic rerun leaves
the original failure blocking. This non-waiving behavior was observed in
[run 34698658335](https://github.com/erfanzar/eyvan/actions/runs/34698658335)
at `054279c`: ordinary scheduling failed at **77.317 ms** (budget 75), while
the separate diagnostic sample passed at **64.122 ms**. The Python 3.12 job
remained failed. Its slowest diagnostic scheduling iteration used **60.747 ms
CPU**; its one generation-2 collection occurred in a different, faster sample.
This does not attribute the original uninstrumented failure to GC or host
contention.

The retained Python 3.12 receipt from
[run 34695978824](https://github.com/erfanzar/eyvan/actions/runs/34695978824)
at `762610b` recorded snapshot p99 **20.698 ms** (budget 25), ordinary scheduling
p99 **90.610 ms** (budget 75), and elastic p99 **60.931 ms** (budget 75).
Ordinary scheduling p50 was **45.032 ms**. That run failed the ordinary scheduling
budget, not snapshot construction. These samples alone do not establish whether
GC, host scheduling, or implementation work caused the tail. All checked-in
budgets remain unchanged; no hosted green result is inferred from local timings.

## Deferred ordinary lease construction (2026-09-12; local)

Profiling the unchanged 1,000-cell ordinary scenario found 26,600 candidate
lease constructions for 80 selected placements over 20 iterations. Ordinary
placement now scores every eligible candidate with the same ordering and
budget/quota checks, retaining the best candidate's fields and constructing
only the selected lease. Cell-group and elastic algorithms are unchanged.

Sequential before/after measurements on the same Darwin/Python **3.11.14** host,
with no diagnostic instrumentation and unchanged scenarios/budgets:

| Cells | Samples | Ordinary p50 before / after (ms) | Ordinary p99 before / after (ms) |
|---:|---:|---:|---:|
| 1,000 | 100 | 26.665 / 19.077 | 38.640 / 25.894 |
| 10,000 | 30 | 291.497 / 195.542 | 393.905 / 257.776 |

All four local receipts passed existing budgets. They are retained as
`.pytest_cache/scheduler-lazy-{before,after}-{1000,10000}.json`. The baseline
scheduler SHA-256 is `7259767ead43c72efc9847945674bc58cb578291e65446eaa31b7e9f14e5707d`;
the candidate is `a0758a6ff57969cc7be032f3b357f2936ba219f68af8beba170396499049321c`.
These sequential local samples are not controlled GitHub-runner measurements
or proof that the hosted budget failure is resolved. The 30-sample larger run
is not a replacement for the scheduled 100-sample lane.

Twelve permutation/preference regressions compare the entire selected decision
and its costs/resources while requiring only one materialized lease. A retained
read-only differential probe compares complete results against the immutable
`054279c` scheduler for 500 seeded snapshots, including budgets, quotas, pricing,
preferences and existing lower-priority leases: 537 placements, 21 preemption
plans and 958 infeasibilities match exactly. This is bounded parity evidence,
not exhaustive equivalence or renewed full-domain mutation qualification.

## Elastic candidate resource reuse (2026-09-12)

The next bounded change reuses the immutable per-cell resource vector between
static validation and candidate costing instead of scaling it twice. Selected
lease construction still performs its own scaling; this is not a claim to
eliminate all resource-vector allocations. Six permutation regressions failed
before the edit and pass afterward. A separate 500-case seeded comparison
against `6f19bd5` matched all outcomes: 172 placements and 328 infeasibilities,
including quotas, budgets, hard constraints, empty supply and capacity limits.
The candidate source SHA-256 is
`285868351241bf88ff84c9ddfdeeeb6dcf227366d30848afb912710e84363c08`;
`.pytest_cache/scheduler-elastic-reuse-parity.json` retains source identities.

Timing does **not** yet establish a tail-latency improvement. In the first
1,000-cell/100-iteration samples, elastic p50/p99 changed from
**19.040/33.831 ms** to **16.726/82.699 ms**. The candidate **failed** the unchanged
75 ms p99 limit. Those raw receipts remain
`.pytest_cache/scheduler-elastic-{before,after}-1000.json`. A separate diagnostic
run passed; that does not waive or explain the earlier failure.

After full-suite completion, four predetermined 20-iteration blocks in
reference/candidate/candidate/reference order all passed the unchanged budgets.
Their elastic p99 values were **21.196 / 33.751 / 17.543 / 19.085 ms**,
respectively (`.pytest_cache/scheduler-elastic-reuse-abba.json`). No GC, scenario
or budget changes were made. This remains local variability evidence, not
hosted qualification or a causal explanation for the failed sample.

Full Python 3.11/required-PostgreSQL validation passed **6,889 tests**, with
**17 monorepo-only skips** and 164 warnings in 358.33 seconds. The focused
scheduler/property selection passed 67 tests. Scoped read-only review found no
changed placement/accounting semantics and highlighted the intentionally
unchanged selected-lease scaling. Ruff, formatting, mypy, import contracts and
lock checks passed. Current hosted CI run **34721399020** could not start jobs:
GitHub reports failed account payments or an insufficient spending limit.
Billing intervention is separate from resolving the earlier hosted snapshot
and elastic-budget failures; no account settings were changed.
