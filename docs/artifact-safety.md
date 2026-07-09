# Managed artifact tenancy, migration, and inventory safety

Status: M1.6 lifecycle and local-schema-v2 implementation contract. This page does **not** authorize a live migration, object deletion, provider rollout, or recovery action without a rehearsed maintenance window and matching database/object-store backups.

## Implemented safety contract

1. The durable identity of an artifact is `(org_id, project_id, uri)`. Managed rows, references, holds, usage, service reads, and API authorization use the complete scope.
2. Lifecycle transitions are `PENDING_UPLOAD -> ACTIVE -> DELETING -> DELETED`, with `QUARANTINED` for legacy, incomplete, or contradicted evidence. Only `ACTIVE` rows may be read, held, or referenced.
3. Upload reserves quota and a logical record generation in a transaction, writes outside the transaction, then activates only after URI, size, backend-observed SHA-256, and exact provider object version all match the reservation. Failed or uncertain writes remain charged until exact cleanup or recovery proves an outcome.
4. Deletion first commits a claim that revalidates retention, holds, and references. Physical deletion happens outside the transaction and is conditional on the recorded provider version. Finalization revalidates the claim and generation; an uncertain provider result is retryable, not success.
5. Physical inventory is bounded, scope-explicit, current-object-only, and report-only by default. Reconciliation exact-reads each object before activating a row. It is additive and never treats a listing, size, or URI alone as proof for deletion.
6. Recovery scans lifecycle rows and artifact-owned scopes with explicit caps. It resumes stranded uploads/deletions, quarantines contradictions, and reports when its finite scan was capped. Destructive collection remains disabled unless `artifact_destructive_collection_enabled` is explicitly set.
7. The local backend performs deletion through a per-object, descriptor-bound generation transaction beneath the trusted artifact root. It never pathname-unlinks a generation while validation/deletion is active; bounded crash receipts are durably retired only after cleanup-safe completion, while unvalidated payload evidence is retained. GCS uses generation preconditions for create, exact read, and delete, with bounded subprocess timeouts.
8. Provider or filesystem ambiguity raises an unavailable/inconclusive result (HTTP 503 at the service boundary). It is never converted into a not-found, conflict, or successful deletion by parsing diagnostics.

## Qualification boundary

The lifecycle, migration, race, and recovery paths have host and disposable-PostgreSQL tests. Local-filesystem adversary tests and mocked GCS command tests are not qualification of every production filesystem, GCS IAM policy, bucket versioning/soft-delete policy, visibility delay, timeout mode, or live concurrent rollout. Run the immutable release candidate against the approved PostgreSQL, object-store, Kubernetes, backup/restore, and rollback matrix before enabling destructive collection.

Do not describe `collectable` or inventory output as a deletion claim. Missing bytes, physical-only objects, and size discrepancies remain evidence to reconcile, not permission to delete. Recovery is exact lifecycle replay, additive metadata repair, a forward code/database repair, or restoration from matching snapshots—not mutation based on inference.

The zero-configuration local JSON writer emits schema v2 and persists the complete control-plane metadata state,
including scoped managed-artifact lifecycle rows and references. Artifact payload bytes remain exclusively in the
configured physical artifact store; the JSON file contains ledger metadata, sizes, digests, and object-version fences,
not object contents. The reader accepts an actual schema-v1 document and initializes the six v2-only ledgers empty.

Before the first v2 write replaces a v1 file, Eyvan preserves the exact v1 bytes as
`control-plane.json.v1.bak`. The backup is installed owner-only, fsynced, and its directory entry is fsynced before the
v2 replace. A read-only start does not force an upgrade. Eyvan refuses to proceed if a pre-existing backup is not
owner-only or does not exactly match the v1 file being replaced; it never silently overwrites that recovery point.

### Local JSON v1-to-v2 recovery and validated export

A v1 binary cannot read a v2 document. The `.v1.bak` file is a pre-upgrade recovery point, not a lossless downgrade:
restoring it discards every workload change, event sequence, progress report, federation grant, artifact lifecycle
transition, tombstone, and reference committed after the upgrade. Artifact bytes are separate again, so restoring
metadata without the matching physical-store snapshot can also make the ledger disagree with reality.

For recovery, stop the local daemon first and preserve owner-only copies of the current v2 file, its v1 backup, the
artifact-token key, and the configured artifact-store directory. Prefer forward repair: run the same or a newer
v2-aware Eyvan build against a copy. An intact document can be validated and exported deterministically without
starting controllers:

```bash
umask 077
uv run python - ~/.eyvan/control-plane.json ./control-plane-v2-export.json <<'PY'
import json
import os
import stat
import sys
from pathlib import Path

from eyvan.server.persistence.local import (
    LOCAL_CONTROL_PLANE_MAX_BYTES,
    LocalControlPlaneDocument,
    LocalControlPlaneStore,
)

source = Path(sys.argv[1])
output = Path(sys.argv[2])
if source.absolute() == output.absolute():
    raise SystemExit("input and output must differ")
state = LocalControlPlaneStore(source).load()
document = LocalControlPlaneDocument.from_state(state).model_dump(mode="json", by_alias=True)
encoder = json.JSONEncoder(indent=2, sort_keys=True, allow_nan=False, ensure_ascii=True)
parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
parent_fd = os.open(output.parent, parent_flags)
output_identity = None
created = False
fd = -1
try:
    parent_metadata = os.fstat(parent_fd)
    parent_path_metadata = output.parent.lstat()
    parent_identity = (parent_metadata.st_dev, parent_metadata.st_ino)
    if not stat.S_ISDIR(parent_metadata.st_mode) or parent_identity != (
        parent_path_metadata.st_dev,
        parent_path_metadata.st_ino,
    ):
        raise OSError("export parent pathname changed")

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(output.name, flags, 0o600, dir_fd=parent_fd)
    output_metadata = os.fstat(fd)
    output_identity = (output_metadata.st_dev, output_metadata.st_ino)
    created = True
    if not stat.S_ISREG(output_metadata.st_mode):
        raise OSError("export is not a regular file")
    os.fchmod(fd, 0o600)

    written = 0
    stream = os.fdopen(fd, "wb")
    fd = -1  # ``stream`` now owns the descriptor.
    with stream:
        for text in encoder.iterencode(document):
            # ``iterencode`` may emit one very large string token. Slice the
            # guaranteed-ASCII text before encoding so the transient byte
            # allocation, each write, and the cumulative output are bounded.
            for offset in range(0, len(text), 65_536):
                chunk = text[offset : offset + 65_536].encode("ascii")
                if written + len(chunk) > LOCAL_CONTROL_PLANE_MAX_BYTES:
                    raise ValueError("export exceeds the local JSON size limit")
                stream.write(chunk)
                written += len(chunk)
        if written + 1 > LOCAL_CONTROL_PLANE_MAX_BYTES:
            raise ValueError("export exceeds the local JSON size limit")
        stream.write(b"\n")
        stream.flush()
        os.fsync(stream.fileno())

    os.fsync(parent_fd)
    parent_path_after = output.parent.lstat()
    output_path_after = os.stat(output.name, dir_fd=parent_fd, follow_symlinks=False)
    if parent_identity != (parent_path_after.st_dev, parent_path_after.st_ino):
        raise OSError("export parent pathname changed")
    if output_identity != (output_path_after.st_dev, output_path_after.st_ino):
        raise OSError("export pathname changed")
except BaseException:
    if fd >= 0:
        os.close(fd)
    # Remove only the exact inode this process created. A same-UID pathname
    # replacement is evidence of interference, not permission to unlink it.
    if created and output_identity is not None:
        try:
            current = os.stat(output.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if output_identity == (current.st_dev, current.st_ino):
                os.unlink(output.name, dir_fd=parent_fd)
                os.fsync(parent_fd)
    raise
finally:
    os.close(parent_fd)
PY
```

Keep the original files unchanged while diagnosing a validation failure. Repair a copy with a release that explicitly
supports its schema and only then replace the stopped daemon's state atomically. Using `.v1.bak` is an explicit restore
to the pre-upgrade point and must never be advertised as preserving v2-only state.

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

## Migrations 0012–0014: maintenance-only cutover

0011 binaries perform URI-only reads and writes. They cannot coexist with the 0012 composite keys or the 0014 lifecycle state. The two historical 0012 heads are joined by `0013_merge_artifact_audit`; the deployable head is `0014_artifact_lifecycle`. The exact-revision startup check intentionally rejects a mixed fleet, so this is not a rolling migration.

### Required sequence

1. Announce a maintenance window and stop admission of artifact uploads and workload submissions that create references.
2. Stop controller reconciliation and collection, then quiesce **every** 0011 API/controller writer. Database table locks protect the migration transaction, not a process that resumes after it commits.
3. Take checksummed database and object-store snapshots from the same cutover point. Restore both into a disposable environment.
4. Rehearse `0011 -> 0012 heads -> 0013 -> 0014`, populated preflight and quarantine cases, application startup, exact dry-run inventory, and the downgrade/restore decision on PostgreSQL.
5. Apply the complete upgrade to `0014_artifact_lifecycle`. The 0012 identity migration fails closed on typed/JSON scope or reference contradictions. The 0014 migration quarantines every legacy row because SQL metadata cannot prove its live provider generation.
6. Deploy only schema-0014, scoped-client/scoped-server binaries. URI-only lifecycle calls receive 426 rather than being guessed into a tenant; older servers fail their exact-revision startup check.
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

- `matched`: lifecycle metadata and the current object agree on URI, size, and, for active/deleting rows, version;
- `physical_only`: an authoritative scoped physical observation has no row;
- `index_only`: a scoped row points to no observable current object;
- `size_mismatch`: both exist but authoritative physical size differs; and
- `version_mismatch`: an active/deleting row names a different provider generation.

Each physical observation includes `observed_object_version`; active and deleting rows persist that fence. A matched row in any non-active lifecycle state is still not readable. In particular, migrated legacy rows remain quarantined until exact reconciliation observes and digests the current version.

Reindex validates every physical-only object and every matched quarantined row before its single database transaction. It exact-reads the observed version, requires a backend-observed SHA-256, independently hashes the returned bytes, and checks content-addressed callable/workspace URIs. After those reads, every candidate is statted again and must still be the same current live provider generation observed by the page; a noncurrent, deleted, or replaced generation aborts before the first database write. The transaction rechecks the row and scope, applies quota to additions, inserts or activates only exact verified rows, and appends `artifact.inventory_reindexed` events. Missing bytes and mismatches are unchanged. Inventory never deletes an object.

### Backend observation contracts

- Local inventory opens regular files without following symlinks, captures `st_size` and a `local:<device>:<inode>:<ctime_ns>` identity from one `fstat`, rejects unexpected directories/files, and keeps response memory bounded to one page. Store-created objects use the compatible `local:2:<sha256>:<device>:<inode>:<ctime_ns>` form and a private generation anchor.
- Local conditional deletion treats canonical entries below `scopes/` as adversarial, but the configured root and its owner-writable `.locks` and `.generations` control directories are a trusted operator boundary. A process that can write those hidden directories can forge transactions and must be treated as a store administrator. Deletion moves the canonical name into a deterministic owner-only transaction directory, rehashes the exact detached descriptor, durably records validation, and truncates that descriptor before retiring its anchor. It never unlinks the detached pathname while the transaction is active. A different entry swapped into that name is preserved first and, when valid for the URI, copied back as a later current generation. Completed transactions are durably renamed to private `.trash-*` receipts. Bounded cleanup proceeds only with the stronger marker written after descriptor truncation and durable replacement restoration; a complete-only receipt that still contains a payload-bearing entry is retained. Cleanup unlinks only the fixed receipt-entry names through no-follow directory descriptors, fsyncs the emptied receipt, removes it, and fsyncs the generation directory. A crash may leave a partial receipt, which the next put or delete resumes under the object lock. Receipt and generation scans have fixed limits; an excess entry count, unknown receipt name, or nested receipt directory is retained and reported as inconclusive. Cleanup never recursively traverses receipt content, never removes active `.delete-*` transactions or generation anchors, and inventory never traverses the control directory.
- GCS inventory uses the pinned JSON API object-list endpoint directly with server-side `prefix`, `versions=false`, `softDeleted=false`, `projection=noAcl`, a restricted field set, and `maxResults=limit+1`. One public page consumes exactly one bounded provider request. Provider `nextPageToken` is bounded continuation evidence; the existing canonical managed-URI cursor resumes with a strict-successor `startOffset` (the complete canonical filename plus `!`, which sorts before every allowed continuation character), so even a short provider page cannot spend its only slot repeating the cursor. Point stat is an exact object metadata GET with no generation, so it can return only the current live object; a 404 means no live object even when retained noncurrent generations exist.
- The JSON path obtains an ephemeral bearer token from ambient `gcloud auth print-access-token`, falling back to the fixed Google workload metadata identity. Tokens remain in memory only and are never placed in URLs, errors, or logs. Requests do not follow redirects and bound timeouts, URL/body sizes, JSON fields, page tokens, object names, integers, and public cursors. Provider bodies, URLs, and transport diagnostics are not reflected into API errors.

No inventory request lists the bucket root or the global `scopes/` prefix, calls a delete endpoint, or infers an organization/project from an opaque prefix. Unknown prefixes therefore remain unknown; M1.5 does not scan them and assign owners.

## Release and evidence boundary

M1.6 is the serial dependency for the schema-v2 local document and for any future destructive collector rollout. Keep old writers quiesced until the schema-0014 binary is healthy, then reconcile quarantined rows with report-only inventory before considering collection. A successful Helm `--atomic` rollback does not roll the database or object store back.

The PostgreSQL migration tests use the repository's explicit availability/required gate; a skipped suite is not a successful rehearsal. Likewise, CPU/local/mock tests establish code behavior, not live PostgreSQL operations, GCS correctness, Kubernetes identity, or hardware qualification. Retain immutable receipts for the exact release commit, database snapshot/restore, object-store policy and inventory, image digest, and deployment identity.
