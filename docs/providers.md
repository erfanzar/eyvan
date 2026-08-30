# Writing an Eyvan capacity provider

A provider is one driver, one capability entry, and a transport — with no edit
to the scheduler, the planner, or the durable model. If you find yourself
reaching into `eyvan.server` or copying a driver to make your provider work,
that is a bug in this contract: open an issue rather than forking.

`aws-ec2` is the worked example to read, because it was written after this
contract existed: one driver, one capability entry, one transport, and two
real ownership bugs in it were found by the conformance suite rather than its
own tests.

## 1. The shape

```text
eyvan_acme_provider/
├── __init__.py        # driver + capability entry + transport registration
└── pyproject.toml     # entry point registering the package with eyvan
```

The driver implements the provider interface the capacity layer calls
(request capacity, describe, delete, reconcile); the capability entry tells
the core what your backend can do; the transport is how your cloud is
reached. Nothing else. The core never learns your cloud's name.

## 2. Turning it on

Install the package into the plane's environment and enable it through the
capacity backend selection (`EYVAN_CAPACITY_BACKEND`) or the fleet config's
provider block. A fleet then references your backend; pools appear in
`eyvan capacity list` once quota/fleet state says they exist.

## 3. The version contract

Providers declare versions; the plane records which provider version served
each cell and workload. Receipts are tied to the exact candidate — a proof
run against provider X vN says nothing about vN+1. Keep the conformance
suite green per release; it is the qualification surface for this contract,
and it is deliberately harsher than unit tests (it found ownership bugs in
`aws-ec2` that its own tests missed).

## 4. What Eyvan guarantees to you

- The scheduler, the capacity planner, and the durable model will not name
  your provider or special-case it — enforced by
  `tests/test_provider_boundary.py`.
- Your driver decides machine-level truth (exists? booting? gone?) and the
  core trusts your answers, including "unavailable/inconclusive" — ambiguity
  is surfaced, never converted into a prettier verdict.
- Capabilities are declarative: declare what your backend can do and
  multislice-shaped work becomes reachable; do not declare it and every cell
  you build is one machine — the safe reading of a silent driver.

## 5. What you guarantee back

- Every machine you create is trackable to a cell: if you created it, the
  plane can name and reap it. Ownership bugs are the worst kind — the
  conformance suite exists to catch them.
- Refusals carry their reason. The renderer prints your diagnostic verbatim
  under `blocked … the plane is retrying`; a bare failure code helps nobody.
- Deletion is best-effort-and-reported: an uncertain delete result is
  retryable state, never success (see [artifact-safety](artifact-safety.md)
  for the same rule on the object side).

## Capability entries

`BackendCapabilities` (in `capacity/drivers/base.py`) is how a backend says
what it can do — for example, allocating **one cell spanning several
machines** (multislice). Declaring it is what makes multi-worker atomic
shapes schedulable on your cloud; without the declaration the core refuses
to plan what it cannot honestly build.

## Qualification

Host and disposable-PostgreSQL suites exercise the core with fake providers;
your provider qualifies through the conformance suite plus a recorded spend
on real infrastructure. A provider claim without an immutable
exact-candidate receipt is not a claim — see [proofs](proofs.md).
