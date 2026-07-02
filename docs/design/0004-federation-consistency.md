# DR-0004: Federation consistency, trust, and witnessed ownership

**Status:** Accepted architecture; implementation is pending. No external PKI or
ownership-witness provider is qualified, so secure federation and automatic
failover remain disabled.

**Scope:** Peer trust, replicated grants, remote workloads, and cross-plane
failover. Existing peer/grant records do not become an authenticated protocol.

## Context

Today a peer is an operator-entered plane ID and HTTP(S) URL. Budget/quota grants
are local admission records; there is no authenticated replication, remote
workload owner, or cross-database fence. Independent databases cannot elect one
owner by comparing local rows. Timestamps, plane-ID tie-breaks, or a majority of
currently reachable peers can produce two winners under partition. A local
PostgreSQL advisory leader coordinates one database only.

Eyvan will not build a CA, signing algorithm, Raft/Paxos, membership protocol, or
quorum store.

## Decision

### External trust

Production peers use mutual TLS rooted in an operator-managed, reviewed PKI or
workload-identity system. An authenticated certificate/SVID maps out of band to
one configured `plane_id`; JSON cannot self-assert identity. The external system
owns trust bundles, issuance, key protection, revocation, and rotation. Plain
HTTP is loopback/development-only and never federation authority.

Online calls use mTLS. Queueable/retriable messages use a versioned signed
envelope from a standard reviewed format/library and keys rooted in the same
trust decision; Eyvan invents neither canonicalization nor algorithms. A
production provider profile must select the format/algorithm allowlist,
SAN/identity mapping, rotation overlap, and revocation behavior.

Every envelope binds version, request/message ID, source and audience planes,
org/project, kind, issue/expiry times, nonce, payload, and grant version or owner
epoch. Receivers enforce audience/scope/size/time, durable replay/idempotency,
and monotonic domain versions. Authentication still requires local authorization.
Naming SPIFFE/SPIRE or a managed CA is not approval; the concrete profile must
pass the gates below.

### Consistency classes and grants

- **Budget/quota:** the home issues bounded expiring grants. A peer admits locally
  from a verified cached grant and tighter local limit, without a hot-path home
  call. Messages converge by issuer epoch and grant version. Expired, revoked,
  unknown, or unverifiable grants allow no new admission.
- **Status/events/results/cost:** at-least-once, idempotent, possibly stale, and
  never authority.
- **Remote commands:** idempotent by home/remote IDs, request, attempt, and owner
  epoch; targets persist the highest epoch and reject lower ones.
- **Failover ownership:** linearized only by an external witness, not replication
  or an Eyvan peer vote.

A grant is also a home-side reservation: home local authority is the ungranted
remainder, not the full limit. For hard budget, remote spend monotonically
converts reserved amount to settled spend. Expiry/revoke returns only a remainder
proven unused by final report; if the peer is unreachable it stays reserved. For
concurrent quota, home effective ceiling plus peer reservations never exceeds
the global vector, and capacity used by already-admitted work remains reserved
until terminal/release proof. This may strand headroom but cannot use it twice.

Single-plane behavior stays local. A configured grantee whose grant expires
fails closed and never falls back to a larger local limit.

### External ownership witness

Automatic failover requires a separately operated, reviewed service with:

1. linearizable read and compare-and-swap;
2. server-authoritative leased ownership/renewal;
3. a monotonic revision/fencing token on every acquisition, including after
   restore; and
4. quorum-backed durability and authenticated clients.

Eyvan consumes a small adapter and conformance suite. It does not count Eyvan
peers or call a single-node store a quorum. Provider selection is a separate
security/operations decision; no best-effort adapter is valid.

Witness keys cover tenant authority `(org, project)` for grant issuance and
federated workload `(home_plane, home_workload)` for remote dispatch. The witness
revision is `owner_epoch`; database counters, clocks, backups, and plane IDs
cannot manufacture it. An owner persists lease/epoch with intent before acting
and issues/renews/dispatches only with a fresh lease before a conservative
renewal deadline. Quorum/renewal loss stops new authority; existing grants remain
bounded until their expiry/accounting resolution.

A higher tenant epoch does not erase old grants. The new owner needs a complete
replicated ledger and counts every unexpired lower-epoch grant until expiry or
acknowledged revoke. A peer aware of a higher epoch rejects later lower-epoch
messages; a partitioned peer can use only its old, already-counted reservation.
Missing ledger history blocks issuance.

Before a remote command, the owner durably records intent and target. Failover
requires a replicated completeness watermark. Every target stores max epoch and
passes it through attempt/agent fences. A higher-epoch owner loads complete
intent/targets, fences/cancels or proves terminal every lower-epoch execution,
and only then dispatches. Unknown/unreachable target, intent, or watermark means
wait. Availability loses rather than two workloads run.

This delegates one lease/epoch decision to the external service; it is not
consensus implemented inside Eyvan.

### Protocol stages

1. Authenticated handshake and envelopes; move no grants/workloads.
2. Replicate and enforce grants; spend/status reports remain observations.
3. Fixed-home remote lifecycle with durable IDs, idempotent submit/cancel/status/
   event/result/cost relay, and scoped artifact handoff reference until terminal.
4. Replicate intent, acquire higher external epoch, reconcile old execution, then
   opt into failover after partition evidence.

DAGs, heterogeneous groups, multislice fragments, and cross-plane preemption are
not split initially; later they require one failure-atomic ownership unit.

## Invariants

1. External authenticated identity plus local policy—not URL or claimed ID—grants
   peer authority.
2. Home-local authority plus peer reservations never exceeds global budget/quota;
   settled spend is monotonic and local limits only tighten.
3. One witnessed epoch may issue new grant generations/remote dispatch per key;
   no target accepts a lower epoch.
4. New ownership dispatches only after complete lower-epoch reconciliation;
   unknown means stop.
5. Duplicate/delayed/out-of-order messages cannot raise authority.
6. Loss of trust, quorum, freshness, or handoff completeness fails closed. Local
   database leadership is never called cross-plane consensus.

## Compatibility, rollout, and rollback

Existing peer rows/grants are `legacy-untrusted` at this boundary: they may keep
current experimental local behavior but are not converted to production peer
authority. Rebind peers to authenticated identities and reissue grants through
the new protocol; legacy grants expire.

Roll out stages disabled/observe-only. Peers negotiate explicit protocol
capabilities; unknown mandatory version/field is rejected. Remote workloads wait
until every participant enforces epochs; failover waits for qualified witness
profile, complete replicated intent, and partition tests.

Rollback stops new issue/handoff/failover, preserves highest epochs/replay state,
and lets bounded grants expire or revokes them. Epochs never decrement/reuse and
an old database never becomes owner. Downgrade to an epoch-unaware binary
requires federated admission off, all grants resolved, and every remote execution
proved terminal; otherwise refuse.

## Failure modes

Wrong SAN/audience/scope is rejected before domain action; rotation uses reviewed
overlap and revoked trust stops new authority; replay/out-of-order is idempotent
or stale; home partition leaves only bounded grants; witness loss prevents
acquire/renew/dispatch; stale owners are rejected by targets; missing intent or
unreachable old execution blocks failover; external epochs dominate database
restore; witness time is authoritative and grant/envelope skew is conservative.
A compromised trusted plane remains a security incident bounded by its grants
and policy, not something consensus can cure.

## Alternatives rejected

Eyvan-built CA/consensus duplicates specialist systems; reachable-peer majority
has inconsistent membership; timestamp/plane-ID last-writer does not fence;
advisory locks do not cross databases; synchronous home admission couples
regions without solving failover; static home remains the safe interim but has
no failover; dispatch-before-reconcile accepts double-run.

## Evidence gates

- reviewed PKI profile covering identity/SAN mapping, trust/key distribution,
  rotation, revocation, audit, and break glass;
- reviewed witness profile proving linearizable CAS, lease/revision, real quorum,
  restore, and client auth under conformance;
- wrong SAN/audience/tenant, replay/expiry, rotation/revocation integration tests;
- concurrent acquire, partition, delayed renew, old-owner reconnect, restore, and
  monotonic-epoch witness tests;
- two independent databases with duplicate/out-of-order messages, restarts, home
  loss, expiry, and partitions: no over-issue, uncertain reservation reuse, or
  second dispatch;
- unreachable old-target test that keeps the new owner fenced;
- multi-day two-plane receipt tying identities, witness revisions, grants,
  epochs, attempts, artifacts, spend, and cleanup. Inconclusive trust/quorum or
  target evidence blocks enablement.
