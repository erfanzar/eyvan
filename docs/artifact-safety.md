# Managed artifact tenancy, migration, and inventory safety

Status: M1.6 lifecycle and local-schema-v2 implementation contract. This page
does **not** authorize a live migration, object deletion, provider rollout,
or recovery action without a rehearsed maintenance window and matching
database/object-store backups.

## Implemented safety contract

1. The durable identity of an artifact is `(org_id, project_id, uri)`.
   Managed rows, references, holds, usage, service reads, and API
   authorization use the complete scope.
2. Lifecycle transitions are `PENDING_UPLOAD -> ACTIVE -> DELETING ->
   DELETED`, with `QUARANTINED` for legacy, incomplete, or contradicted
   evidence. Only `ACTIVE` rows may be read, held, or referenced.
3. Upload reserves quota and a logical record generation in a transaction,
   writes outside the transaction, then activates only after URI, size,
   backend-observed SHA-256, and exact provider object version all match the
   reservation. Failed or uncertain writes remain charged until exact
   cleanup or recovery proves an outcome.
4. Deletion first commits a claim that revalidates retention, holds, and
   references. Physical deletion happens outside the transaction and is
   conditional on the recorded provider version. Finalization revalidates
   the claim and generation; an uncertain provider result is retryable, not
   success.
5. Physical inventory is bounded, scope-explicit, current-object-only, and
   report-only by default. Reconciliation exact-reads each object before
   activating a row. It is additive and never treats a listing, size, or URI
   alone as proof for deletion.
6. Recovery scans lifecycle rows and artifact-owned scopes with explicit
   caps. It resumes stranded uploads/deletions, quarantines contradictions,
   and reports when its finite scan was capped. Destructive collection
   remains disabled unless `EYVAN_ARTIFACT_DESTRUCTIVE_COLLECTION_ENABLED`
   is explicitly set.
7. The local backend performs deletion through a per-object,
   descriptor-bound generation transaction beneath the trusted artifact
   root. It never pathname-unlinks a generation while validation/deletion is
   active; bounded crash receipts are durably retired only after
   cleanup-safe completion, while unvalidated payload evidence is retained.
   GCS uses generation preconditions for create, exact read, and delete,
   with bounded subprocess timeouts.
8. Provider or filesystem ambiguity raises an unavailable/inconclusive
   result (HTTP 503 at the service boundary). It is never converted into a
   not-found, conflict, or successful deletion by parsing diagnostics.

## The client-side store: digest addressing and the no-clobber race

The SDK's `GCSArtifactStore` writes callable/workspace payloads to
**digest-named** objects — `gs://…/callables/<sha256>.pkl` — describe-checks
first, then uploads with `--no-clobber`. Two consequences:

- A pre-existing object under a digest name is **fetched and re-hashed**; a
  corrupted or foreign object is rejected, never trusted.
- On 2026-08-29, three zones raced one identical probe callable at the same
  instant: the first upload landed the object, and the losers — whose
  `--no-clobber` copy answered `HTTPError 412` — **crashed in submission**
  because "the object I was uploading now exists" was treated as failure
  (commit `e7abd59` fixed it: the loser verifies the winner's digest and
  continues). Identical callables are one object; none of their submitters
  fails. Pinned by
  `test_a_concurrent_upload_of_the_same_digest_wins_instead_of_crashing`,
  `test_a_precondition_loss_still_rejects_a_foreign_object`, and
  `test_any_other_upload_failure_still_names_itself` in
  `tests/test_callable_execution.py`.

CLI failures still travel with the CLI's own words (`GcsCommandError`
carries the failing command and its stderr) — a swallowed diagnostic turns
every upload failure into a mystery.

## Qualification boundary

The lifecycle, migration, race, and recovery paths have host and
disposable-PostgreSQL tests. Local-filesystem adversary tests and mocked GCS
command tests are not qualification of every production filesystem, GCS IAM
policy, bucket versioning/soft-delete policy, visibility delay, timeout
mode, or live concurrent rollout. Run the immutable release candidate
against the approved PostgreSQL, object-store, Kubernetes, backup/restore,
and rollback matrix before enabling destructive collection.

Do not describe `collectable` or inventory output as a deletion claim.
Missing bytes, physical-only objects, and size discrepancies remain evidence
to reconcile, not permission to delete. Recovery is exact lifecycle replay,
additive metadata repair, a forward code/database repair, or restoration
from matching snapshots — not mutation based on inference.

## Migrations and API transitions

Old clients receive **426** on the scoped API surface — the transition is
forced, not silently widened. The 0012–0014 migration sequence is
maintenance-only cutover: schema first, verified inventory second, destructive
collection last, each behind its own rehearsal and backup gate. See
[design/0001](design/0001-scoped-artifact-lifecycle.md) for the record.
