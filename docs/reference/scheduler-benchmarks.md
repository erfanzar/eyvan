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
the original failure blocking. Hosted qualification of this diagnostic step is
still pending.

The retained Python 3.12 receipt from
[run 34695978824](https://github.com/erfanzar/eyvan/actions/runs/34695978824)
at `762610b` recorded snapshot p99 **20.698 ms** (budget 25), ordinary scheduling
p99 **90.610 ms** (budget 75), and elastic p99 **60.931 ms** (budget 75).
Ordinary scheduling p50 was **45.032 ms**. That run failed the ordinary scheduling
budget, not snapshot construction. These samples alone do not establish whether
GC, host scheduling, or implementation work caused the tail. All checked-in
budgets remain unchanged; no hosted green result is inferred from local timings.
