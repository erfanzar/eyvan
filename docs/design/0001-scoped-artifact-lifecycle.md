# DR-0001: Scoped managed-artifact identity and fenced lifecycle

**Status:** Implemented at the host/PostgreSQL software boundary. Migration of an
existing deployment, live object-store reconciliation, and destructive
collection remain externally unqualified and require their separate gates.

**Scope:** Managed callables, workspaces, results, and archived logs. This record
preceded M1.5/M1.6 and still does not authorize collection on an existing
deployment. The context below records the pre-implementation state that led to
the decision.

## Context

The byte store is tenant-scoped by `storage_scope(org_id, project_id)`, but the
index is keyed only by URI. `get_managed_artifact()` takes only a URI,
`artifact_reference_id()` omits tenant scope, and live-reference queries can do
the same. Equal logical URIs in two physical tenant prefixes can therefore share
one index row. Upload also commits the index before bytes, while collection
checks references, performs an unconditional store delete, and observes a racing
hold only afterward. The scheduled controller can call this destructive path; a
grace period is not a fence.

## Decision

### Identity and storage fence

The only artifact identity is:

```
ArtifactKey = (org_id, project_id, canonical_uri)
```

This tuple is the database key and is required by every repository read/write,
usage query, reference, hold, transfer, collection, and audit operation. A
reference ID also includes this key and workload ID. `eyvan://...` remains a
scope-relative logical name for API compatibility; it is neither globally
unique nor an authorization boundary. Equal bytes in different projects have
independent objects, rows, quota, holds, and references. Cross-tenant
deduplication is rejected.

Each row has a monotonic `record_generation` and an opaque `object_version` from
the backend. The backend contract is immutable put-if-absent returning version,
size, and digest; stat/read of an exact version; and delete-if-version returning
`deleted`, `already_absent`, or `precondition_failed`. GCS uses object generation,
not a weak ETag. Local storage uses a generation-specific immutable path or an
equally atomic primitive; stat-then-unconditional-unlink is insufficient.

Database generation fences stale controllers; backend version fences a stale
external delete after re-upload. Quota reservation is serialized per tenant
scope and includes pending reservations, since locking one artifact cannot stop
two different uploads from oversubscribing the same quota.

### State machine

| State | Meaning | Next |
|---|---|---|
| `PENDING_UPLOAD` | Quota reserved for one generation; not readable or collectable. | `ACTIVE`, `DELETED`, `QUARANTINED` |
| `ACTIVE` | Row and exact object version agree; readable/referenceable/holdable. | `DELETING`, `QUARANTINED` |
| `DELETING` | Eligibility claimed for one generation/version; mutations retry. | `DELETED`, `QUARANTINED` |
| `DELETED` | Zero-byte tombstone retaining generation and audit history. | `PENDING_UPLOAD` at generation + 1 |
| `QUARANTINED` | Ownership/version cannot be proved; no automatic read or delete. | audited resolution only |

Upload is reserve/write/activate. A transaction locks scope quota and scoped row,
reserves bytes, and writes `PENDING_UPLOAD`; the backend writes immutable bytes;
a second transaction activates only the unchanged generation with matching
size/digest/version. Success is returned only after activation. Reconciliation
activates an exact match, releases an expired reservation only after proving the
object absent, and quarantines mismatch or uncertainty.

Deletion has two phases. First, a transaction locks the row and rechecks
`ACTIVE`, retention, hold, and scoped live references before persisting a claim
ID, generation, and object version in `DELETING`. Candidate scans are hints only.
Second, the collector conditionally deletes that version and transactionally
finalizes the same claim as `DELETED`; `already_absent` is idempotent and a
precondition failure quarantines. It never retries unconditionally.

Reference creation, holds, and re-upload take the same row lock and require
`ACTIVE` (re-upload waits for deletion resolution). A stale collector may finish
its old version but cannot remove a later one.

## Invariants

1. One artifact key owns exactly one object, quota charge, hold set, and
   reference set.
2. A successful read names an `ACTIVE` row and exact backend version.
3. A live reference/hold and `DELETING` cannot coexist after commit.
4. `DELETED` charges zero; pending reserves intended bytes; active, deleting,
   and uncertain quarantined rows remain conservatively charged.
5. No delete lacks exact scope, durable claim/audit, record generation, and
   backend version. Unknown inventory is preserved, never inferred or deleted.

## Maintenance migration and inventory sequence

This is not a mixed-version migration: old URI-only binaries cannot coexist.

1. **Read-only preflight:** disable scheduled collection; inventory database and
   every known tenant prefix with scope, URI, size/digest, and object version.
   Report cross-scope references, missing/unindexed objects, unknown hashed
   prefixes, and mismatches. Repair/delete nothing; take a checked backup and
   immutable inventory.
2. **Quiesce:** stop submissions and every writer, including result uploads and
   terminal log archives, and prove the write count stays zero.
3. **M1.5 scope switch:** add composite keys and scoped reference indexes. Move a
   legacy row only to the scope it records; never clone ownership from matching
   URI/workload names. Deploy scope-required code with collection disabled.
4. **Inventory reconcile:** rerun dry, review, then explicitly reindex only
   objects whose known physical prefix and provider metadata prove scope/size.
   Ambiguous result/log ownership or unknown prefixes remain protected. No delete.
5. **M1.6 lifecycle switch:** assign generation 1 and `ACTIVE` only after exact
   version verification; quarantine the rest. Enable new upload/reconcilers,
   then report-only collection. Destructive collection waits for evidence gates.
6. **Contract later:** remove URI-only compatibility no earlier than the next
   release and only after retaining migration/inventory receipts.

## Compatibility, rollout, and rollback

Logical URIs and scoped transfer parameters remain valid. Repository and local
persistence shapes intentionally break old writers; memory, local-document, and
PostgreSQL backends switch together. Before the first new-schema write, rollback
restores the backup with collection disabled. After scoped rows/generations are
written, downgrade is not lossless: roll forward or export/repair into the
backup, never use a destructive migration downgrade. Migration/inventory do not
delete objects, so their listing remains the rollback reference.

## Failure modes

- Reservation/write crashes are reconciled to exact match, proved absence, or
  quarantine; quota remains conservative meanwhile.
- Delete/finalize crashes resume the same claim and accept `already_absent`.
- Reference/hold/delete races have one row-lock winner; losers retry.
- Re-upload cannot be removed by an old version condition.
- Out-of-band replacement, restore, or incomplete inventory quarantines and
  blocks rollout rather than manufacturing agreement.

## Alternatives rejected

URI filters are too late to disambiguate lookup; global dedup couples tenants;
index-after-upload leaks uncharged bytes; index-before-upload without state
leaves false active rows; grace/double-checks do not exclude races; deleting the
row first creates bytes no collector can find.

## Evidence gates

- memory, local-document, SQLite, and PostgreSQL parity for two projects sharing
  a URI with independent quota, holds, and references;
- deterministic quota, reference/hold/reupload-vs-delete, and every crash-boundary
  test;
- local/GCS conformance for immutable put and conditional delete, including
  forced precondition failure;
- reviewed dry/apply inventories with exact count/byte reconciliation;
- legacy migration, restart, backup/restore, and old-writer refusal rehearsal;
- scoped generation/audit assertions for activation, quarantine, claim, and
  finalization. Destructive collection stays disabled until all pass.
