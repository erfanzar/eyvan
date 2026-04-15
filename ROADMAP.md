# Eyvan roadmap

This roadmap keeps Eyvan's control plane independent of Ray and eray. Items are
ordered by the capability needed to replace the transitional Ray Jobs adapter,
not by UI visibility.

## Now: complete the native execution path

1. **Completed: Native Compute worker runtime.** Eyvan now bootstraps a fenced native
   worker daemon on Compute Engine CPU, GPU, and `v5p-8` TPU VMs, probes it
   before admission, and directly submits, observes, stops, checkpoints, and
   retires one complete VM or slice without Ray.
2. **Completed: Native multi-host worker runtime.** Eyvan runs one Eyvan worker daemon on every
   physical TPU VM or GPU host, distribute one fenced attempt across the hosts
   in a cell, stream status, and terminate the whole attempt consistently.
3. **Completed: Atomic TPU Multislice cells.** Shape-aware worker-unit planning,
   one multi-node queued resource, coordinated native startup, status,
   cancellation, and settlement make `TpuConfig(pods=N)` mean one physical GCP
   Multislice containing N complete TPU slices. Generic multi-cell groups remain
   available for non-TPU orchestration. Rank-aware checkpointing remains a
   separate follow-up.
4. **Hardware shape catalog.** Extend the initial `v5p-16` and `v5p-32`
   profiles into a versioned provider catalog for TPU topology, host count, chips, CPU,
   memory, GPU model, and enforceable resources from a versioned provider
   catalog so fleet YAML stays logical and invalid shapes fail before queueing.
5. **Preemption-aware retries.** Build on the current bounded capacity-loss
   retries by distinguishing provider preemption, bad capacity, user-code
   failure, and infrastructure failure; apply separate retry budgets and resume
   from the latest committed checkpoint.
6. **First-class logs and progress.** Persist structured stdout/stderr events,
   expose follow/tail APIs, and let callable jobs publish progress and metrics
   without using logs as their result channel.
7. **Operational explain tools.** Add `eyvan doctor`, `eyvan capacity`, and
   `eyvan workload explain` for credentials, quotas, queue reason, placement
   constraints, autoscaler decisions, and estimated launch time.

## Next: make autoscaling faster and cheaper

1. **Capacity portfolios.** Let one logical pool try ordered zones, spot and
   on-demand classes, reservations, and compatible accelerator generations
   while preserving explicit policy and budget limits.
2. **Warm and predictive pools.** Combine scale-to-zero with scheduled minimums,
   image-aware warm workers, launch-time history, and conservative predictive
   scale-up for recurring jobs.
3. **Locality-aware caches.** Cache runtime images, callable artifacts,
   datasets, and checkpoints near workers; make cache state a placement input
   without weakening tenant isolation.
4. **Services as a workload kind.** Add long-lived deployments, health checks,
   rolling updates, request/queue/accelerator-utilization autoscaling, and
   explicit disruption budgets.
5. **Batch primitives.** Provide map, array-job, and small DAG APIs whose tasks
   remain normal durable Eyvan workloads with bounded fan-out and backpressure.
6. **Cost controls.** Show price and carbon estimates before submission, enforce
   project budgets and alert thresholds, and attribute actual idle, queued, and
   running accelerator cost.
7. **Artifact lifecycle policy.** Add per-project storage quotas, retention,
   garbage collection for unreferenced callables and superseded attempt results,
   legal holds, and auditable deletion without weakening content immutability.

## Later: broaden the platform

1. **GPU and CPU decorator configs.** Add `GpuConfig` and `CpuConfig` with the
   same fleet-policy separation as `TpuConfig`, followed by heterogeneous
   coordinated groups for trainer, evaluator, and data workers.
2. **Portable provider plugins.** Support Kubernetes, on-premises hosts, and
   additional clouds behind the existing capacity and execution contracts.
3. **Workload identity and secrets.** Issue short-lived per-attempt identities,
   mount scoped secrets without persisting values in workload specs, and record
   every access in the audit log.
4. **Federated admission.** Coordinate independent regional control planes with
   explicit ownership, replicated queue intent, and deterministic failover
   rather than one fragile global scheduler process.
5. **Resilience certification.** Continuously test controller failover,
   duplicate messages, stale agents, partial slice loss, provider API outages,
   and storage corruption with reproducible fault scenarios.
6. **Fleet topology UI.** Visualize queue pressure, cells, leases, launch
   latency, failures, cost, and autoscaler reasoning from the durable APIs.

## Design gates

- Ray and eray remain optional migration adapters, never control-plane
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
