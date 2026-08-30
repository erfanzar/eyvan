# DR-0002: Flattened global log ranks and opaque cursors

**Status:** Substantially implemented; host-tested only. Strict
topology-bound rejection and the full archive manifest remain pending, along
with multi-host hardware evidence.

## Context

A workload's logs come from many writers — the plane, agents, native
workers, one per host — and every consumer wants to page through them in one
consistent order. Per-host sequence numbers cannot provide that: hosts start
and restart independently, so "host 3, seq 100" is not a position anyone can
sort against "host 1, seq 8500". The attempt topology itself is only known
at dispatch.

## Decision

1. **Flattened global ranks.** Log records carry a rank assigned from a flat
   global space, not a (host, sequence) pair, so a single sort is the
   timeline regardless of how many hosts contributed.
2. **The attempt topology is frozen at dispatch.** Rank allocation for a
   workload happens against the topology recorded when that workload was
   dispatched; later topology changes do not renumber history.
3. **Cursors are opaque and untrusted.** A cursor handed back by a client is
   state, not a credential and not a promise: it is validated and decoded
   strictly, and an invalid, stale, or incompatible cursor yields a typed
   outcome — never a silent reset to the beginning and never a leak of
   internal structure.

## Invariants

- One sort order over all writers, stable across reads within a workload.
- No cursor is honored beyond validation: an untrusted cursor can neither
  authorize anything nor corrupt a stream position.
- Typed cursor outcomes (invalid / incompatible) are distinguishable at the
  API surface; clients can tell "you are too old" from "you are wrong".

## Implemented surface

The M1.3 surface — global ranks, the discriminated cursor codec with typed
invalid/incompatible outcomes, and the archive read path — is implemented
and host-tested (`eyvan workload logs` pages through it).

## Open

- Strict topology-bound rejection: a cursor from a different topology must
  be refused with its own typed outcome rather than approximated.
- The full archive manifest, so long-retention archives are self-describing.
- Multi-host hardware evidence: rank assignment under real concurrent
  writers on real pods, where clock skew and restarts are not simulated.

Consumers should treat cursor handling as settled (opaque, typed) and rank
order as the only ordering contract.
