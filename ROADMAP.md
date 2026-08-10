# Eyvan roadmap

This roadmap keeps Eyvan's control plane independent of Ray and eray. The
transitional Ray Jobs adapter has been removed; items are ordered by the
capability needed to replace what it did,
not by UI visibility.

## Status vocabulary and qualification boundary

Status is about the checked-in software surface, not about a provider claim:

- **Code-complete** means the public surface and its host, static, and where
  applicable PostgreSQL contracts are implemented.
- **Partial/advisory** means useful scaffolding exists but the item is not a
  production-supported end-to-end capability. Advisory automation stays off by
  default.
- **Deferred** means the design or local records may exist, but enablement is
  intentionally blocked.
- **Externally unqualified** means no immutable receipt for the current
  candidate proves the real image, provider, Kubernetes, object-store, or
  accelerator path. It does not mean the host-tested software path is absent.

CPU/mock tests, rendered manifests, and historical runs never remove the last
status. The current frozen workspace candidate has host and PostgreSQL evidence,
but **no current-candidate CPU/GPU/TPU provider, GCS, Kubernetes, OCI-runtime, or
hardware qualification receipt**. See [outstanding proofs](docs/proofs.md) and
the production-readiness journal in the EasyDeL workspace. In the matrix,
**C**, **P**, **D**, and **U** abbreviate the four statuses above; a PostgreSQL
entry describes only the named persistence contract, never the whole capability.

| Roadmap surface | Host/static | PostgreSQL | Image/deployment | Provider/hardware | Receipt/SHA boundary |
|---|---|---|---|---|---|
| native single-host, multi-host, and TPU Multislice execution | C | durable state/fence contracts | U | U: CPU/GPU/TPU | workspace journal; no external receipt |
| hardware catalog | C for sourced entries | n/a | U | U: breadth and corrected-resource rerun | workspace journal; no provider receipt |
| preemption, retry, checkpoint, and resume | C | durable retry contracts | U | U: reclamation/recovery | workspace journal; no provider receipt |
| structured logs and progress | C: strict codec, frozen topology, schema-2 manifest | persistence contracts | U | U: real multi-host loss; P2 at exit 2 | workspace journal; no hardware receipt |
| explain and launch-latency reporting | C; historical samples only | query contracts | U | U; no guaranteed ETA | workspace journal |
| current pool/class portfolios | C for current surface | policy-state contracts | U | U; broader routes deferred | workspace journal |
| predictive warmth | P; advisory/off by default | policy-state contracts | U | U: backtest/spend/provider | no qualification receipt |
| locality-aware cache | C: managed staging, budget, eviction, worker executor, multi-host slice fan-out | state contracts | U | U: no fleet has staged bytes | no qualification receipt |
| managed services | C: health with explicit unknown, replica lifecycle, bounded autoscaling, HTTP routes and CLI | persistence contracts | U | U: no deployed service | no qualification receipt |
| batch, DAG, and groups | C: admission ceiling, bounded waves, atomic heterogeneous placement | persistence contracts | U | U: no fleet-scale run | no qualification receipt |
| economics and carbon | P/advisory | persistence contracts | U | U: prices/reconciliation/webhooks/carbon source | no qualification receipt |
| artifact lifecycle | C | C: exact M1.6 receipt | U | U: GCS/destructive collection | workspace M1.6 PostgreSQL receipt only |
| CPU/GPU configs and coordinated groups | C: GPU catalog, per-rank devices including several ranks per host, reached from an observation | n/a | U | U: no GPU has executed; no per-lease device isolation | no qualification receipt |
| provider plugins | C: published ABI, conformance suite, on-prem provider | n/a | U | U: no external provider written | static evidence only |
| secret delivery and workload identity | secret delivery C; provider identity P | C: dispatch transactions | U | U: identity/rotation/metadata/mTLS | workspace journal; no provider receipt |
| federation | D/off | local-record tests only | U | U: PKI/witness/failover absent | design record only |
| resilience certification | P: host chaos/evaluator | PostgreSQL fault contracts | U | U: provider certificate/final soak | no qualification receipt |
| fleet topology UI | P; scoped authority/composition C | authority contracts | U | U: deployed browser/provider E2E | workspace journal/static build only |

## Now: native execution path (remaining contracts and qualification open)

1. **Code-complete; externally unqualified: Native Compute worker runtime.** The software path bootstraps a fenced native worker daemon for Compute Engine
   CPU, GPU, and TPU cells, probes it before admission, and directly submits,
   observes, stops, checkpoints, and retires one complete VM or slice without
   Ray. Real provider and accelerator execution remains externally unqualified.
2. **Code-complete; externally unqualified: Native multi-host worker runtime.** The ranked execution path runs one Eyvan worker daemon per host, distributes
   one fenced attempt across the hosts in a cell, streams status, and terminates
   the whole attempt consistently. No current receipt proves that behavior on a
   real multi-host accelerator cell.
3. **Code-complete; externally unqualified: Atomic TPU Multislice cells.** Shape-aware worker-unit planning,
   one multi-node queued resource, coordinated native startup, status,
   cancellation, and settlement make `TpuConfig(pods=N)` mean one physical GCP
   Multislice containing N complete TPU slices. Generic multi-cell groups remain
   available for non-TPU orchestration. Rank-aware checkpoint/resume plumbing is implemented; real Multislice
   lifecycle qualification remains a separate gate.
4. **Code-complete for sourced entries; provider breadth unqualified: Hardware shape catalog.** A versioned, sourced catalog supplies the current TPU topology, host count,
   chips, CPU, memory, and enforceable-resource entries so fleet YAML stays
   logical and invalid shapes fail before queueing. Ungrounded topology is
   deliberately omitted; this is not a universal TPU/GPU provider catalog.
5. **Code-complete; provider recovery unqualified: Preemption-aware retries.** Durable retry intent distinguishes provider preemption, bad capacity,
   user-code failure, and infrastructure failure, applies bounded budgets and
   backoff, and preserves checkpoint/resume metadata. Real provider reclamation
   and recovery remain unqualified.
6. **Partial; strict cursor/manifest and multi-host proof open: First-class logs and progress.** Durable global-rank stdout/stderr records, bounded opaque cursors, sparse
   archive retrieval, follow/tail APIs, and fenced progress updates are
   implemented without using logs as the result channel. Strict attempt/
   topology/kind/mode cursor rejection and the full per-rank archive manifest
   remain pending; rank-loss and retention are also hardware-unproved.
7. **Code-complete; historical estimates only: Operational explain tools.** `eyvan doctor`, capacity diagnostics, and workload explanation expose
   credentials, quotas, queue reasons, bounded placement rejections, and
   autoscaler state. Launch-latency median/p95 values are historical samples,
   not a reservation or guaranteed ETA.

## Next: make autoscaling faster and cheaper

1. **Code-complete for current pool/class fallback; broader routes deferred: Capacity portfolios.** Current pool/class portfolios represent min/max capacity, scaling mode,
   cooldown, accelerator constraints, and ordered fallback pools while
   preserving policy and budget limits. Broader cross-zone/provider route
   portfolios remain deferred.
2. **Partial/advisory and disabled by default: Warm and predictive pools.** Combine scale-to-zero with scheduled minimums,
   image-aware warm workers, launch-time history, and conservative predictive
   scale-up for recurring jobs.
3. **Partial, soft placement hints only: Locality-aware caches.** Cache runtime images, callable artifacts,
   datasets, and checkpoints near workers; make cache state a placement input
   without weakening tenant isolation.
4. **Partial; health and decisions now reachable over HTTP and the CLI: Services as a workload kind.** Add long-lived deployments, health checks,
   rolling updates, request/queue/accelerator-utilization autoscaling, and
   explicit disruption budgets.
5. **Partial: Batch primitives.** Provide map, array-job, and small DAG APIs whose tasks
   remain normal durable Eyvan workloads with bounded fan-out and backpressure.
6. **Partial/advisory: Cost controls.** Show price and carbon estimates before submission, enforce
   project budgets and alert thresholds, and attribute actual idle, queued, and
   running accelerator cost.
7. **Code-complete at host/PostgreSQL boundary; live deletion unqualified: Artifact lifecycle policy.** Scoped identity, immutable versions, references, quotas, retention, legal
   holds, collectability, dry-run-first collector commands, and auditable
   deletion are implemented. Destructive collection remains disabled by default
   and object-store execution is externally unqualified.

## Later: broaden the platform

1. **Partial; GPU and heterogeneous runtime unqualified: GPU and CPU decorator configs.** `GpuConfig` and `CpuConfig` surfaces and coordinated-group scaffolding exist
   with fleet-policy separation. Atomic heterogeneous reservation semantics and
   GPU runtime qualification remain open.
2. **Partial; external conformance and GKE lifecycle unqualified: Portable provider plugins.** Capacity/runtime registries and manual, GCP, and Kubernetes built-ins exist.
   There is no qualified external plugin ABI/conformance suite, additional-cloud
   support, or GKE lifecycle receipt.
3. **Partial; secret delivery implemented, provider identity/mTLS open: Workload identity and secrets.** Reference-only secret specs, allowlists, redaction, and short-lived
   attempt/generation/fence-bound secret capabilities are implemented. Provider
   workload identity, per-workload principals, rotation, metadata isolation, and
   native mTLS remain open.
4. **Deferred and disabled: Federated admission.** Coordinate independent regional control planes with
   explicit ownership, replicated queue intent, and deterministic failover
   rather than one fragile global scheduler process.
5. **Partial; provider certification open: Resilience certification.** Host chaos helpers and a deterministic evaluator cover controller failover,
   duplicate messages, stale agents, partial loss, provider API outages, and
   storage faults. They are not a provider certification or soak receipt.
6. **Partial; scoped authority/composition code-complete: Fleet topology UI.** The scoped dashboard composes durable queue, workload, fleet, budget, audit,
   logs/progress, and explanation APIs. Full topology visualization and deployed
   browser/provider end-to-end qualification remain open.

## Design gates

- Ray is gone and eray remains an independent sibling, never a control-plane
  dependencies.
- A workload is dispatched only when the selected runtime can enforce its full
  resource and fencing contract.
- A GCP TPU Multislice is one provider-backed cell and is never fabricated by
  joining independently provisioned queued resources.
- CPU tests prove logic, not TPU/GPU lowering, availability, or performance.
- New automation must remain inspectable through durable state and audit events.
- **"I could not find out" is never recorded as "I found out it is broken."** An
  agent that cannot answer, a conditional write that times out, and a provider
  that reports a failure are three different facts. Conflating the first two
  with the third destroys healthy capacity and poisons placement beliefs against
  regions that did nothing wrong — each of those was a live incident, not a
  hypothetical.

## Deliberate non-goals

- **Platform-driven profiling (`profile_task`).** On a TPU host the only useful
  profile is `jax.profiler`/libtpu tracing, which must start *inside* the
  training process: it needs the JAX runtime's device handles, there is no
  ptrace equivalent for TPU device state, and libtpu holds an exclusive
  per-host lock. So it cannot be built on the sibling-process precedent that
  makes logs and `exec` work. Eyvan instead injects a per-rank
  `EYVAN_PROFILE_URI` that a callable can pass to
  `jax.profiler.start_trace(...)`, and documents `py-spy dump` against
  `process_status.main_pid` for the stuck-process case.
- **Traffic routing or load balancing for services.** The endpoint registry is
  discovery only. Eyvan is not a proxy.
- **Carbon-aware placement.** Ships as a contract that returns
  `CarbonUnavailable` with a reason code and is structurally excluded from
  placement. A wrong carbon number silently influencing placement is worse than
  no number.
