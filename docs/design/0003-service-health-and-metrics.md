# DR-0003: Service health and metrics ownership

**Status:** Accepted design boundary; typed health and autoscaling are pending.

**Scope:** Managed-service startup, readiness, liveness, discovery, rolling
availability, and autoscaling signals. Eyvan remains a control plane, not the
service traffic or observability data plane.

## Context

A service is currently a coordinator whose children are ordinary workloads. A
child registers an endpoint; discovery returns it while the child is `RUNNING`
and on the current revision. Registration never expires, so it is treated as
readiness until workload state changes, and rolling counts live rather than
ready processes. Eyvan also cannot infer request rate or queue depth. Proxying
requests or scraping arbitrary tenant URLs would move service data, credentials,
latency, and failure modes into the scheduler's authority tier.

## Decision

### Ownership

| Concern | Owner |
|---|---|
| Code, endpoint behavior, probe definition, application telemetry | Service owner |
| Bounded local probes | Current fenced cell agent |
| Health validation/storage, discovery, roll/restart/autoscale decisions | Eyvan control plane |
| Routing, balancing, retries, TLS, connection draining | External gateway/client |
| Raw metrics/logs/traces and long-term telemetry | Owner's observability system |
| Conversion to bounded autoscale signals | Authenticated reporter/adapter |

Eyvan publishes discovery but never forwards service requests. It stores probe
outcomes and bounded aggregate signals, never request/response bodies, headers,
raw application series, traces, or service logs as metrics.

### Health model

A typed revision may declare independent probes:

- **startup:** initialization completed; until success, readiness/liveness do not
  prematurely roll a loading model, but a bounded deadline can fail;
- **readiness:** may enter discovery and count toward roll availability; failure
  removes it without declaring the process dead; and
- **liveness:** the service reconciler should replace the replica; it does not
  make the endpoint ready.

Initial automatic probes are HTTP/TCP from the agent to a replica-local target
derived by the execution backend from a declared port/path, without arbitrary
hostnames or redirects. The advertised endpoint is not a probe URL. Script/exec
probes are deferred: current diagnostic exec runs beside the workload, not
inside its cgroup, and is not a health-isolation boundary. Probe timing,
thresholds, and diagnostics are bounded; bodies and secrets are never reported.
The control plane does not fetch tenant URLs.

The agent reports service/revision, replica workload, attempt, workload
fence/generation, monotonic sequence, probe states, observed time, and bounded
expiry using current attempt/agent authority. The server rejects wrong scope or
revision, stale attempt, replay/decreasing sequence, and excessive expiry.
Server receipt time starts TTL; agent time is diagnostic.

Discovery requires an ordinary `RUNNING` child, current attempt/revision, valid
endpoint, successful configured startup, and unexpired `READY`. Missing/expired
health is `UNKNOWN`, never healthy. It leaves new discovery and the availability
count; existing connections remain the gateway's concern. Thresholds stabilize
flaps. Confirmed startup/liveness failure is a durable reason to cancel the
ordinary child; the service coordinator replaces it under existing fencing and
backoff.

Roll bounds use ready current **and ready stale** replicas, not all non-terminal
children. A new revision cannot retire an old ready replica unless the readiness
floor permits it. Weighted traffic remains outside Eyvan.

### Autoscaling signals

Request/concurrency data enters only through a versioned authenticated report
with a closed metric/unit set (for example interval request rate, in-flight,
queue depth, accelerator utilization), never arbitrary PromQL, names, or labels.
Reports bind org/project/service/revision/source/interval, sequence, sample time,
expiry, unit, and bounded value. A replica/sidecar uses its attempt fence; a
gateway adapter uses separate service-scoped identity. Eyvan holds no scrape
credentials and queries no arbitrary metrics system.

Duplicates are idempotent; out-of-order windows cannot replace newer ones;
aggregation records source coverage. Only short-retention normalized rollups
needed to explain/reproduce decisions are stored. Eyvan's `/metrics` may expose
bounded controller aggregates such as desired/ready counts, stale-signal counts,
and decision reasons, not raw series, endpoints, or per-replica/user-chosen
labels.

Missing/stale/partial/invalid signals are unavailable, not zero, and cannot
scale in. Desired capacity holds no lower than configured minimum while the
reason is reported. Budget, quota, capacity, rate, cooldown, and stabilization
remain hard bounds. Automatic scaling is observe-only until enabled per qualified
service.

## Invariants

1. No service request/response transits an Eyvan control-plane process.
2. Only the current fenced attempt changes its endpoint or health.
3. `RUNNING` is insufficient; missing/expired health and metrics are not healthy
   or zero.
4. Discovery contains only current-revision, registered, unexpired ready replicas.
5. Rolling availability is measured in ready replicas.
6. Autoscale decisions reproduce from bounded authenticated rollups and cannot
   bypass budget/capacity limits or scale in on signal loss.

## Compatibility, rollout, and rollback

1. Land typed service records/API and translate existing coordinator metadata on
   read; replica workloads stay ordinary.
2. Add health storage and agent probes with enforcement off; distinguish
   `LEGACY_REGISTERED`, `READY`, `NOT_READY`, and `UNKNOWN`.
3. Canary and compare with existing endpoint discovery. Opt revisions into
   health-required discovery, changing the default only after agent capability
   coverage.
4. Add metrics observe-only, compare adapter rollups to their source, then enable
   autoscaling per service. Deprecate registration-as-readiness after N/N-1;
   no-probe services retain explicit legacy behavior.

Rollback independently disables health-required discovery/autoscaling while
retaining additive records. Restoring legacy endpoint eligibility is visibly
degraded semantics. Do not downgrade to a binary that loses typed fields while
in use; drain/export first. Stale samples never become current on rollback.

## Failure modes

Agent/control-plane partition expires discovery but not existing connections;
stale attempts/replays are fenced; thresholds bound health flap; startup grace
protects slow initialization; replica-local target derivation avoids control-
plane SSRF; poisoned/partial metrics are bounded and disable automation rather
than scale-in; adapter outage holds safe desired capacity; durable TTL/sequence
prevent controller restart from resurrecting endpoints.

## Alternatives rejected

Eyvan proxying duplicates mature gateways and couples user/data-plane failure;
control-plane scraping creates SSRF/credential ownership; registration or
`RUNNING` cannot detect hung replicas; arbitrary PromQL creates an unbounded
query surface; missing-as-zero causes destructive scale-in during the least
observable outage.

## Evidence gates

- startup/readiness/liveness threshold, expiry, flap, and ready-roll tests;
- stale attempt/replay, wrong scope/revision, clock skew, oversized detail, and
  malicious target tests;
- two-controller PostgreSQL exclusion for health sequence and roll/replacement;
- discovery expiry/old-revision tests with healthy siblings retained;
- duplicate/out-of-order/partial/stale metrics and no-scale-in-on-loss tests;
- real external gateway/adapter receipt proving requests, bodies, credentials,
  and raw series do not cross Eyvan, including health flap/adapter outage;
- observe-only error/cost evidence before automatic enablement.
