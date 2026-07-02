# DR-0002: Flattened global log ranks and opaque cursors

**Status:** Accepted design boundary; implementation is pending.

**Scope:** Live structured logs, multi-worker cells, multi-cell gangs, resume
cursors, terminal archives, and legacy readers.

## Context

`NativeSliceExecutor` preserves inner host ranks and returns per-host cursor
state. The controller then enumerates each leased cell as one outer rank and
`_log_page_from_document()` replaces every inner rank with it; inner
`truncated_ranks` are not translated. Multi-host cells therefore collapse
workers. The current `attempt:rank=cursor|...` grammar is also nested around a
cell cursor that may itself contain `|`, so it is not reversible. Archives use
the collapsed rank and stop scanning at the first missing object, making an
unreachable host look like a complete smaller archive.

## Decision

### One rank space

For one attempt, order cells by the same stable cell-ID order used by native gang
execution and preserve stable worker order inside each cell. For worker counts
`w[i]`:

```
offset(i) = sum(w[j] for j < i)
global_rank(i, local_rank) = offset(i) + local_rank
```

Ranks are contiguous `0..sum(w)-1`. Persist the attempt topology—cell identity
and generation, worker count/order—and its digest. A local rank outside that
shape, endpoint-count mismatch, or mid-attempt topology change makes the page
explicitly incomplete; Eyvan does not renumber it.

`LogRecord.rank`, progress correlation, `truncated_ranks`, cursors, archive names,
and replay all use global rank. Additive source-cell/local-rank fields are only
diagnostics. An unreachable cell contributes its entire declared global range.

### Cursor v2

Callers treat a cursor as opaque, but it is not confidential, authenticated, or
a credential. The server emits a bounded base64url JSON envelope with a prefix
and explicit version, conceptually:

```
eyvan-log-v2.<base64url({
  "v": 2, "kind": "workload-log", "attempt": 3,
  "topology": "sha256:...", "mode": "live",
  "positions": [{"rank": 0, "cell": "cell-a", "local": 0,
                 "cursor": "backend-opaque"}]
})>
```

JSON permits arbitrary supported leaf cursor strings without delimiter escaping.
The decoder rejects duplicate ranks/keys and bounds total bytes, positions, and
leaf length before fan-out. Unknown version/kind, malformed encoding, negative
rank, wrong attempt, or topology mismatch returns an explicit invalid/
incompatible-cursor response. Only empty input means start; corrupt input never
does.

The envelope carries one leaf position per global rank. The controller groups
positions by cell, gives executors a v2 local-rank cursor, and maps returned
positions back. An unavailable rank retains its prior position. Attempt change
is reported and begins the new attempt at empty positions. The authenticated
workload/attempt is rechecked because base64 opacity provides no authorization;
cursors are not logged at ordinary verbosity.

### Archive compatibility

Each terminal attempt writes one immutable JSONL object per global rank,
preserving timestamp, stream, source cursor, PID, global/source/local rank, plus
an immutable manifest with schema version, topology digest, every expected rank,
and per-rank `archived`, `unreachable`, `truncated`, or `failed` status with
object digest/size. Rank blobs and manifest use the scoped lifecycle in
[DR-0001](0001-scoped-artifact-lifecycle.md); a direct unindexed put is not a
committed archive.

The manifest—not the first miss—defines completeness. Every unavailable expected
rank is returned in global `truncated_ranks`. Settlement remains best effort and
must not strand capacity, but partial capture is never presented as complete.

Archived records retain leaf source cursors, so a v2 live cursor can resume after
release. The reader finds the exact source position, then emits immutable
archive-offset positions. Missing source position or object digest mismatch is
an explicit cursor gap, never silent replay/skip. Legacy archives have no
manifest and may contain outer ranks; serve them only as `legacy_rank_identity`
with `completeness_unknown`, never relabeled as proven global ranks.

## Invariants

1. One physical worker has one stable global rank per attempt; no collisions.
2. Live records, truncation, cursor positions, archive, and replay share the map.
3. A leaf cursor returns only to its issuing source.
4. Corrupt/stale/incompatible cursors fail explicitly, not as start/end.
5. Partial output names missing ranks, or says completeness is unknown for
   legacy data. Cursor input is bounded and untrusted.

## Compatibility, rollout, and rollback

Clients already round-trip `next_cursor` opaquely and response additions are
compatible. Server rollout is two-stage:

1. Ship strict v1/v2 decoders, topology/global-rank/archive readers, and a
   feature-disabled v2 writer; keep old emission until every serving and
   rollback image has the decoder. Accept v1 only when the expected single-level
   rank set makes it reversible. Duplicate/nested/extra ranks return
   `cursor_reset_required`, not a guess.
2. Switch flattened rank emission, v2 cursors, and v2 manifests together. Keep
   the v1 decoder for N/N-1 and retained links, measuring use before removal.

Rollback disables v2 emission but uses an image that retains v2 decoding and the
global/archive reader. Returning to the original delimiter parser after v2
cursors escape would cause silent replay; without a compatible rollback image,
pause following and require an explicit reset.

## Failure modes

- Delimiters/Unicode in a leaf cursor are ordinary JSON strings.
- One host/cell unavailable retains position and maps exact global truncation.
- Topology change stops resume as an invariant violation.
- A missing middle archive rank remains visible and later ranks are still read.
- Archive crash leaves an incomplete manifest; immutable retry is idempotent.
- A live cursor after release moves to archive mode or reports a gap.
- Oversized/malicious cursors are rejected before allocation/fan-out.

## Alternatives rejected

Outer+local compound rank burdens every consumer; escaped delimiters remain a
nested grammar; one merged cursor cannot resume multiple journals; first-miss
archive scans manufacture completeness; PostgreSQL/audit events have the wrong
body-store ordering, volume, and retention contract.

## Evidence gates

- heterogeneous worker-count vectors with source/local/global and truncation
  mapping;
- two multi-worker cells, every rank exactly once across repeated pages;
- delimiter/Unicode/empty, duplicate, malformed, oversized, and unknown-version
  cursor cases;
- simple v1 continuation and explicit ambiguous-v1 refusal;
- live-to-archive continuation without loss/duplication, missing middle rank,
  archive crash/retry, attempt change, and legacy unknown completeness;
- real P2 inner-rank loss receipt containing immutable topology, global
  truncation, and archive manifest.
