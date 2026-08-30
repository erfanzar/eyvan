# DR-0003: Service health and metrics ownership

**Status:** Accepted design boundary; typed health and autoscaling signals
pending. Eyvan remains a control plane, not the metrics system.

## Context

A control plane is a dependency of everything, so everything asks it two
questions: "are you up?" and "how are you doing?" Left unspecified, those
questions rot into the two failure modes this record exists to prevent: a
health endpoint that lies (returns green while the controller loop is wedged)
and a metrics surface that quietly becomes an application-data pipeline
(request bodies, user payloads, model telemetry flowing through plane
telemetry into dashboards).

## Scope

Managed-service startup, readiness, liveness, discovery, rolling
availability, and autoscaling signals. Explicitly out of scope: anything
about the *workloads'* application metrics.

## Decision

1. **Health is typed and component-resolved.** Liveness (the process serves)
   is separate from readiness (the controller loop is advancing and the
   durable backend answers). `/healthz` reports process liveness plus
   controller status — `{"status":"ok","service":"eyvan","controller":
   "active"}` — and the typed surface distinguishes degraded components
   rather than flattening them to green.
2. **Eyvan owns its own telemetry, no one else's.** Requests, responses, and
   raw application telemetry stay out of the plane's metrics. The plane
   emits control-plane facts — cell stage transitions, workload state
   counts, reconcile outcomes, capacity handover latencies — authenticated
   by `EYVAN_METRICS_TOKEN` when bound off-loopback.
3. **Autoscaling signals are plane-owned observations.** Fleet scaling
   decisions read plane-recorded facts (queue depth, launch latency,
   provisioning outcomes), not inferred user intent; the autoscaling-signal
   surface that external scalers consume is pending.
4. **Discovery is declarative.** `eyvan start` discovers what exists (local
   plane reachable, heads in the project) and connects; it creates nothing.
   Health answers precede provisioning actions.

## Invariants

- A wedged controller must not report healthy readiness.
- No metric payload may carry workload data across the tenant boundary.
- Health and metrics endpoints authenticate like every other surface when
  bound off-loopback.

## Open

The typed health enum beyond liveness+controller, readiness gates per
backend (durable store reachable, agents reachable), and the autoscaling
signal feed. Until they land, treat `/healthz` controller status as the
liveness truth and plane events (`eyvan events`) as the operational feed —
the pair that caught a silent plane death and a 401 loop on 2026-08-29.
