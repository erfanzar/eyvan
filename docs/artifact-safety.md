# Managed artifact tenancy, migration, and inventory safety

Status: M1.5 implementation and operations contract. This page does **not** authorize a live migration, object deletion, or rollout without a tested maintenance window.

## What M1.5 guarantees

1. The durable identity of an artifact is `(org_id, project_id, uri)`. Managed rows, references, holds, usage, service reads, and API authorization use the complete scope.
2. Hold, release, and reference requests authorize the caller-supplied scope before looking up a row. A URI is never used to discover its tenant.
3. Physical inventory is bounded and scope-explicit. It lists only the opaque prefix computed from a supplied `(org_id, project_id)` and never scans all `scopes/` prefixes to guess ownership.
4. Every observed physical object reports authoritative size and a replacement identity captured in the same backend observation: GCS object generation or a local file device/inode/ctime identity.
5. Inventory is report-only by default. Optional reindexing requires an exact scope acknowledgement, validates the complete page before writing, adds only still-missing rows from observed metadata, and records an audit event. Inventory never deletes bytes or rewrites a discrepancy.

## What M1.5 does **not** guarantee

M1.5 is deliberately before the upload/deletion state machine. It does not serialize a reference, hold, or re-upload against deletion; reserve quota through a pending upload; persist an object generation in the artifact row; or conditionally delete an exact generation. The existing collector seam predates these guarantees and still has check/delete races. M1.5 does not enable that seam when it wires the API's physical store for inventory, and an inventory result is not permission to run collection.

Do not describe `collectable` output as a safe deletion claim. Missing physical bytes, physical-only objects, and size discrepancies remain reports. Recovery is additive metadata repair, a forward code/database repair, or restoration from matching snapshots—not object deletion based on inference.

The zero-configuration local JSON document remains schema v1 and does not serialize managed-artifact rows or references. Its in-process repository obeys scoped identity, but those ledgers do not survive a local daemon restart. Do not present local JSON reindexing as durable reconciliation; a versioned local document change is deferred until the M1.6 record shape is final.

## Scoped persistence contract

`ManagedArtifactRecord` is keyed by `(org_id, project_id, uri)`. Point reads require all three values. Project listings require `org_id` and `project_id`; organization and fleet accounting use dedicated aggregates rather than unscoped row listings.

`ArtifactReferenceRecord` stores non-null typed `org_id` and `project_id`. Its primary key is `(org_id, project_id, uri, workload_id)`. Its display `id` is deterministic over those four fields, but correctness does not depend on a truncated digest being collision-free. The JSON record and typed `id` move together in migration 0012.

The in-memory repository uses the same tuple keys. SQLAlchemy names the current primary-key constraints `pk_managed_artifacts_scoped` and `pk_artifact_references_scoped`, matching migration 0012 so a database created directly at head follows the same conditional downgrade path as an upgraded database.

## API transition: old clients receive 426

The following v1 calls now require explicit `org_id` and `project_id`:

- `POST /api/v1/artifacts/holds` in the JSON body;
- `DELETE /api/v1/artifacts/holds` in the query; and
- `GET /api/v1/artifacts/references` in the query.

An old URI-only call receives HTTP `426 Upgrade Required` with code `artifact_scope_required`, the required fields, and the `X-Eyvan-Required-Client-Feature: scoped-artifact-identity` header. Other malformed requests remain 422 responses. This is intentional: silently looking up a URI would be unsafe, while a generic validation response gives an old client no actionable transition.

Current CLI commands accept `--org` and `--project` (or an explicitly selected context):

```bash
eyvan artifacts hold --org acme --project research --uri eyvan://results/run-1 --reason 'incident 41'
eyvan artifacts release-hold --org acme --project research --uri eyvan://results/run-1
eyvan artifacts references --org acme --project research --uri eyvan://results/run-1
```

## Migration 0012: maintenance-only cutover

0011 binaries perform URI-only reads and writes. They cannot coexist with the 0012 composite keys. The exact-revision startup check intentionally rejects a mixed fleet; this is not a rolling migration.

### Required sequence

1. Announce a maintenance window and stop admission of artifact uploads and workload submissions that create references.
2. Stop controller reconciliation and collection, then quiesce **every** 0011 API/controller writer. Database table locks protect the migration transaction, not a process that resumes after it commits.
3. Take checksummed database and object-store snapshots from the same cutover point. Restore both into a disposable environment.
4. Rehearse `0011 -> 0012`, the populated preflight cases, application startup, dry-run inventory, and the conditional downgrade/restore decision on PostgreSQL.
5. Apply 0012. It fails closed if a managed typed scope is missing or differs from JSON, if reference JSON lacks scope, if typed URI/workload identity differs from JSON, or if reference scope differs from its workload's durable scope.
6. Deploy only scoped-client/scoped-server binaries. URI-only lifecycle calls will receive 426 rather than being guessed into a tenant.
7. Run report-only inventory for each known project, page through every `next_cursor`, and retain the reports before considering explicit reindex.

0012 preserves the one managed row in the scope already recorded on that row. It never clones that row into other scopes found in references: URI-only history may have overwritten a tenant's ledger, and SQL cannot reconstruct its size or ownership. Cross-scope references are preserved and reported as inventory evidence.

Upgrade recomputes both `artifact_references.id` and `artifact_references.data.id` over `(org_id, project_id, uri, workload_id)` in one statement. A permitted downgrade recomputes both representations over the legacy `(uri, workload_id)` identity.

Downgrade first locks and checks the populated tables. It refuses before any schema change if scoped managed rows share a URI, scoped references collapse onto one legacy URI/workload pair, or legacy reference IDs would collide. After scoped writes, the normal recovery path is forward repair or restore of the pre-migration database **and matching object-store snapshot**. Deployment tooling must not advertise an automatic/atomic rollback as safe.

## Bounded inventory and additive reindex

Run one known scope at a time:

```bash
# Report only (default), at most 100 entries.
eyvan artifacts inventory --org acme --project research

# Continue with the exact next_cursor returned by the preceding page.
eyvan artifacts inventory --org acme --project research \
  --cursor eyvan://results/run-123 --limit 100

# Add only physical-only metadata after reviewing the dry-run report.
eyvan artifacts inventory --org acme --project research \
  --reindex-missing --acknowledge-scope acme/research
```

The API equivalents are:

- `GET /api/v1/artifacts/inventory` for a report-only page; and
- `POST /api/v1/artifacts/inventory/reindex` for explicitly acknowledged additive reindexing.

The maximum page size is 1000. The report statuses are:

- `matched`: object and index agree on URI and size;
- `physical_only`: an authoritative scoped physical observation has no row;
- `index_only`: a scoped row points to no observable object; and
- `size_mismatch`: both exist but authoritative physical size differs.

Each physical observation includes `observed_object_version`. M1.5 does not store that version in `ManagedArtifactRecord`, so it cannot report a durable version mismatch or use the value for deletion. That belongs to M1.6.

Reindex validates every physical-only entry before its single database transaction. Content-addressed callable/workspace objects are read through the store's digest-checking path. After those reads, every candidate is statted again and must still be the same current live provider generation (or local file identity) observed by the page; a noncurrent, deleted, or replaced generation aborts before the first database write. The transaction rechecks that each row is still absent, applies quota to the total addition, inserts only missing rows, and appends `artifact.inventory_reindexed` events. Existing rows, missing bytes, and mismatches are unchanged. Inventory never deletes an object.

### Backend observation contracts

- Local inventory opens regular files without following symlinks, captures `st_size` and a `local:<device>:<inode>:<ctime_ns>` identity from one `fstat`, rejects unexpected directories/files, and keeps response memory bounded to one page. Store-created objects use the compatible `local:2:<sha256>:<device>:<inode>:<ctime_ns>` form and a private generation anchor.
- Local conditional deletion treats canonical entries below `scopes/` as adversarial, but the configured root and its owner-writable `.locks` and `.generations` control directories are a trusted operator boundary. A process that can write those hidden directories can forge transactions and must be treated as a store administrator. Deletion moves the canonical name into a deterministic owner-only transaction directory, rehashes the exact detached descriptor, durably records validation, and truncates that descriptor before retiring its anchor. It never unlinks the detached pathname while the transaction is active. A different entry swapped into that name is preserved first and, when valid for the URI, copied back as a later current generation. Completed transactions are durably renamed to private `.trash-*` receipts. Bounded cleanup proceeds only with the stronger marker written after descriptor truncation and durable replacement restoration; a complete-only receipt that still contains a payload-bearing entry is retained. Cleanup unlinks only the fixed receipt-entry names through no-follow directory descriptors, fsyncs the emptied receipt, removes it, and fsyncs the generation directory. A crash may leave a partial receipt, which the next put or delete resumes under the object lock. Receipt and generation scans have fixed limits; an excess entry count, unknown receipt name, or nested receipt directory is retained and reported as inconclusive. Cleanup never recursively traverses receipt content, never removes active `.delete-*` transactions or generation anchors, and inventory never traverses the control directory.
- GCS inventory uses the pinned JSON API object-list endpoint directly with server-side `prefix`, `versions=false`, `softDeleted=false`, `projection=noAcl`, a restricted field set, and `maxResults=limit+1`. One public page consumes exactly one bounded provider request. Provider `nextPageToken` is bounded continuation evidence; the existing canonical managed-URI cursor resumes with a strict-successor `startOffset` (the complete canonical filename plus `!`, which sorts before every allowed continuation character), so even a short provider page cannot spend its only slot repeating the cursor. Point stat is an exact object metadata GET with no generation, so it can return only the current live object; a 404 means no live object even when retained noncurrent generations exist.
- The JSON path obtains an ephemeral bearer token from ambient `gcloud auth print-access-token`, falling back to the fixed Google workload metadata identity. Tokens remain in memory only and are never placed in URLs, errors, or logs. Requests do not follow redirects and bound timeouts, URL/body sizes, JSON fields, page tokens, object names, integers, and public cursors. Provider bodies, URLs, and transport diagnostics are not reflected into API errors.

No inventory request lists the bucket root or the global `scopes/` prefix, calls a delete endpoint, or infers an organization/project from an opaque prefix. Unknown prefixes therefore remain unknown; M1.5 does not scan them and assign owners.

## M1.6 serial dependency

The follow-up migration must add pending/active/deleting/deleted state, monotonically increasing record generation, quota reservation, and persisted immutable object version. Upload, reference, hold, and deletion transitions must lock/compare the same scoped row, and external deletion must be conditional on the recorded object version. Deterministic crash and concurrency tests are required around every database/object-store boundary.

Until that lands, CPU tests establish scoped logic, SQL shape, inventory parsing, and SQLite behavior only. The PostgreSQL migration tests use the repository's explicit PostgreSQL availability/required gate; a skipped PostgreSQL suite is not a successful migration rehearsal, and no host-only result is a real-database claim.
