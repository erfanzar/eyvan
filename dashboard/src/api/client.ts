/*
 * Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  cloneMock,
  createMockWorkload,
  getMockWorkload,
  mockAuditEvents,
  mockBudgets,
  mockCells,
  mockLeases,
  mockPolicies,
  mockWorkloads,
} from "./mockData";
import { apiAuthHeaders, isDemoMode, rejectSession } from "../auth/oidc";
import type {
  AuditEvent,
  Budget,
  CapacityLease,
  Cell,
  ControlPlaneHealth,
  LogEntry,
  OverviewData,
  PriorityBand,
  SchedulingPolicy,
  SubmitWorkloadInput,
  TimelineEvent,
  WorkloadDetail,
  WorkloadStatus,
  WorkloadSummary,
} from "../types";

interface RawResources {
  cpu: number;
  memory_gib: number;
  gpu: number;
  tpu: number;
  custom: Record<string, number>;
}

interface RawWorkload {
  id: string;
  org_id: string;
  project_id: string;
  state: string;
  attempt: number;
  lease_id?: string;
  checkpoint_uri?: string;
  scheduling_reason?: string;
  created_by: string;
  created_at: string;
  updated_at: string;
  spec: {
    name: string;
    execution: { kind: "command"; command: string } | { kind: "callable"; artifact_uri: string };
    gang: { replicas: number; per_replica: RawResources; required_topology: Record<string, string> };
    metadata: Record<string, string>;
    accelerator_type?: string;
    priority_band: PriorityBand;
  };
}

interface RawCell {
  id: string;
  provider: string;
  provider_project?: string;
  region: string;
  zone?: string;
  state: string;
  capacity: RawResources;
  allocatable: RawResources;
  accelerator_type?: string;
  capacity_class?: string;
  labels: Record<string, string>;
  last_heartbeat_at?: string;
}

interface RawLease {
  id: string;
  workload_id: string;
  project_id: string;
  cell_id: string;
  state: string;
  resources: { replicas: number; per_replica: RawResources };
  hourly_cost: number;
  expires_at: string;
}

interface RawBudget {
  org_id: string;
  project_id: string;
  spent: number;
  limit: number;
  period: string;
  hard_limit: boolean;
  accelerator_hour_limits: Record<string, number>;
  accelerator_hours_spent: Record<string, number>;
  currency: string;
}

interface RawQueueTrend {
  observed_at: string;
  bucket_seconds: number;
  points: Array<{ started_at: string; submitted: number; admitted: number }>;
}

interface RawBudgetStatus {
  org_id: string;
  project_id: string;
  limit: number;
  spent: number;
  forecast: number | null;
}

interface RawPolicy {
  org_id: string;
  project_id: string;
  allowed_regions: string[];
  allowed_capacity_classes: string[];
  preemption_enabled: boolean;
  allowed_providers: string[];
  denied_accelerator_types: string[];
  max_priority: number;
  updated_at: string;
}

interface RawEvent {
  id: string;
  sequence: number;
  timestamp: string;
  actor: string;
  entity_type: string;
  entity_id: string;
  action: string;
  previous_state?: string;
  new_state?: string;
  payload: Record<string, unknown>;
}

/** Subset of the server WorkloadProgress contract used by the dashboard. */
interface RawWorkloadProgress {
  workload_id: string;
  workload_state: string;
  phase: string;
  scheduling_reason?: string | null;
  failure_reason?: string | null;
  request: { cells: number; workers_per_cell: number };
  capacity: { ready_workers: number };
}

export interface WorkloadFilters {
  search?: string;
  status?: string;
  priority?: string;
}

export class ApiError extends Error {
  constructor(message: string, public readonly status?: number) {
    super(message);
    this.name = "ApiError";
  }
}

export function workloadStatus(value: string): WorkloadStatus {
  const states: Record<string, WorkloadStatus> = {
    pending: "queued",
    admitted: "planning",
    leased: "provisioning",
    dispatching: "provisioning",
    running: "running",
    checkpointing: "checkpointing",
    cancelling: "preempting",
    succeeded: "completed",
    failed: "failed",
    // A cancellation is not a failure. Collapsing them told an operator their
    // job had broken when they had stopped it themselves, and inflated the
    // Failed tile that is the first thing read on this page.
    cancelled: "cancelled",
  };
  return states[value] ?? "queued";
}

function mapWorkload(raw: RawWorkload): WorkloadDetail {
  const resources = raw.spec.gang.per_replica;
  const accelerator = resources.tpu ? raw.spec.accelerator_type ?? "TPU" : undefined;
  return {
    id: raw.id,
    name: raw.spec.name,
    project: raw.project_id,
    user: raw.created_by,
    status: workloadStatus(raw.state),
    priority: raw.spec.priority_band,
    resources: {
      cpu: resources.cpu,
      memoryGb: resources.memory_gib,
      accelerator,
      acceleratorCount: resources.tpu || undefined,
      replicas: raw.spec.gang.replicas,
      topology: Object.values(raw.spec.gang.required_topology).join(", ") || undefined,
    },
    submittedAt: raw.created_at,
    startedAt: raw.state === "running" ? raw.updated_at : undefined,
    queueReason: raw.scheduling_reason,
    command: raw.spec.execution.kind === "command" ? raw.spec.execution.command : raw.spec.execution.artifact_uri,
    checkpointUri: raw.checkpoint_uri,
    leaseId: raw.lease_id,
    // Populated by getWorkload from this workload's leases; the list view has no
    // allocation to show.
    allocation: [],
    timeline: [],
    logs: [],
    labels: raw.spec.metadata,
  };
}

/** Summarize one lease's per-replica resources for the allocation panel. */
function describeResources(resources: RawResources, replicas: number): string {
  const parts: string[] = [];
  if (resources.tpu) parts.push(`${resources.tpu} TPU`);
  if (resources.gpu) parts.push(`${resources.gpu} GPU`);
  if (resources.cpu) parts.push(`${resources.cpu} CPU`);
  const summary = parts.join(", ") || "no reserved resources";
  return replicas > 1 ? `${replicas} x ${summary}` : summary;
}

/** Map a durable lease state onto the three states the panel renders. */
/** Map a durable cell state to how the fleet page should read it.
 *
 * The previous expression was `ready ? healthy : draining ? maintenance :
 * degraded`, which made every other state degraded. Two of them are not:
 * `registering` is the normal state of a cell queued for capacity -- on a Spot
 * TPU that is the majority of its life -- and `deleted` is a cell that no longer
 * exists. Rendering both in red made a healthy fleet waiting on a Spot grant
 * look broken, which is the fastest way to make an operator distrust the page.
 */
export function cellStatus(state: string): "healthy" | "starting" | "maintenance" | "degraded" {
  if (state === "ready") return "healthy";
  if (state === "registering") return "starting";
  if (state === "draining" || state === "deleting") return "maintenance";
  return "degraded";
}

function leaseAllocationState(state: string): "ready" | "starting" | "draining" {
  if (state === "active") return "ready";
  if (state === "revoking" || state === "released" || state === "expired") return "draining";
  return "starting";
}

function resourceRows(raw: RawResources): Array<{ name: string; total: number }> {
  const resources: Array<[string, number]> = [
    ["CPU", raw.cpu],
    ["Memory GiB", raw.memory_gib],
    ["TPU", raw.tpu],
    ...Object.entries(raw.custom),
  ];
  return resources.filter(([, total]) => total > 0).map(([name, total]) => ({ name, total }));
}

function mapPolicy(policy: RawPolicy): SchedulingPolicy {
  const constraints = [
    policy.allowed_regions.join(", ") || "all regions",
    policy.allowed_capacity_classes.join(", ") || "all capacity classes",
    policy.allowed_providers.join(", ") || "all providers",
  ].join("; ");
  return {
    id: `${policy.org_id}/${policy.project_id}`,
    name: `Placement policy (${policy.project_id})`,
    scope: policy.project_id,
    type: "placement",
    summary: `${constraints}; preemption ${policy.preemption_enabled ? "enabled" : "disabled"}`,
    preemptionEnabled: policy.preemption_enabled,
    updatedAt: policy.updated_at,
    configuration: {
      allowedProviders: policy.allowed_providers,
      allowedRegions: policy.allowed_regions,
      allowedCapacityClasses: policy.allowed_capacity_classes,
      deniedAcceleratorTypes: policy.denied_accelerator_types,
      maxPriority: policy.max_priority,
    },
  };
}

const LOG_LEVELS: Record<string, NonNullable<LogEntry["level"]>> = {
  TRACE: "DEBUG",
  DEBUG: "DEBUG",
  INFO: "INFO",
  WARN: "WARN",
  WARNING: "WARN",
  ERROR: "ERROR",
  CRITICAL: "ERROR",
  FATAL: "ERROR",
};

/** Extract timestamp, level, and source from a log line, leaving them unset when absent. */
function parseLogLine(line: string): Pick<LogEntry, "timestamp" | "level" | "source" | "message"> {
  let rest = line;
  let timestamp: string | undefined;
  const stamp = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?)(Z|[+-]\d{2}:?\d{2})?\s+/.exec(rest);
  if (stamp) {
    timestamp = `${stamp[1]}T${stamp[2].replace(",", ".")}${stamp[3] ?? "Z"}`;
    rest = rest.slice(stamp[0].length);
  }
  let level: LogEntry["level"];
  const levelMatch = /^(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)[\s:]+/i.exec(rest);
  if (levelMatch) {
    level = LOG_LEVELS[levelMatch[1].toUpperCase()];
    rest = rest.slice(levelMatch[0].length);
  }
  let source: string | undefined;
  const sourceMatch = /^\[([^\]]+)\]\s+/.exec(rest);
  if (sourceMatch) {
    source = sourceMatch[1];
    rest = rest.slice(sourceMatch[0].length);
  }
  return { timestamp, level, source, message: rest || line };
}

/** Derive a capacity-acquisition percentage from the workload progress contract. */
function mapProgressPercent(progress: RawWorkloadProgress): number | undefined {
  if (progress.phase === "terminal") {
    return progress.workload_state === "succeeded" ? 100 : undefined;
  }
  const requestedWorkers = progress.request.cells * progress.request.workers_per_cell;
  if (requestedWorkers <= 0) return undefined;
  const ready = Math.min(progress.capacity.ready_workers, requestedWorkers);
  return Math.round((ready / requestedWorkers) * 100);
}

/** Compute the overview from already-fetched dashboard data so each resource is requested once per refresh. */
export function computeOverview(
  workloads: WorkloadSummary[],
  cells: Cell[],
  budgets: Budget[],
  // Passed in rather than derived here: the trend comes from the audit ledger,
  // which this function does not have and should not fetch.
  queueTrend: OverviewData["queueTrend"] = [],
): OverviewData {
  const queued = workloads.filter((item) => item.status === "queued").length;
  const running = workloads.filter((item) => item.status === "running").length;
  const terminal = workloads.filter((item) => ["completed", "failed", "cancelled"].includes(item.status));
  const resourceBreakdown = new Map<string, { used: number; total: number; pending: number }>();
  cells.flatMap((cell) => cell.resources).forEach((resource) => {
    const current = resourceBreakdown.get(resource.name) ?? { used: 0, total: 0, pending: 0 };
    current.used += resource.used;
    current.total += resource.total;
    resourceBreakdown.set(resource.name, current);
  });
  workloads.filter((workload) => ["queued", "planning"].includes(workload.status)).forEach((workload) => {
    const pendingResources: Array<[string, number]> = [
      ["CPU", workload.resources.cpu * workload.resources.replicas],
      ["Memory GiB", workload.resources.memoryGb * workload.resources.replicas],
      ["TPU", (workload.resources.acceleratorCount ?? 0) * workload.resources.replicas],
    ];
    pendingResources.forEach(([name, pending]) => {
      const current = resourceBreakdown.get(name) ?? { used: 0, total: 0, pending: 0 };
      current.pending += pending;
      resourceBreakdown.set(name, current);
    });
  });
  const priorityQueue = (["production", "interactive", "batch"] as PriorityBand[]).map((priority) => {
    const queuedInBand = workloads.filter((item) => item.status === "queued" && item.priority === priority);
    const oldestSubmittedAt = queuedInBand.reduce(
      (oldest, item) => Math.min(oldest, Date.parse(item.submittedAt)),
      Date.now(),
    );
    return {
      priority,
      count: queuedInBand.length,
      oldestMinutes: queuedInBand.length ? Math.max(0, Math.floor((Date.now() - oldestSubmittedAt) / 60_000)) : 0,
    };
  });
  return {
    queued,
    running,
    fleetUtilization: cells.length ? Math.round(cells.reduce((sum, cell) => sum + cell.utilization, 0) / cells.length) : 0,
    monthlySpend: budgets.reduce((sum, budget) => sum + budget.spent, 0),
    monthlyBudget: budgets.reduce((sum, budget) => sum + budget.limit, 0),
    successRate: terminal.length ? (terminal.filter((item) => item.status === "completed").length / terminal.length) * 100 : undefined,
    queueTrend,
    priorityQueue,
    fleetBreakdown: [...resourceBreakdown].map(([resource, values]) => ({ resource, ...values })),
    alerts: budgets.filter((budget) => budget.forecast !== undefined && budget.forecast > budget.limit).map((budget) => ({ id: budget.id, severity: "warning", title: "Budget forecast above limit", detail: budget.owner, timestamp: new Date().toISOString() })),
  };
}

export class EyvanApiClient {
  private readonly org = import.meta.env.VITE_EYVAN_ORG ?? "acme";
  private readonly project = import.meta.env.VITE_EYVAN_PROJECT ?? "research";

  constructor(
    private readonly baseUrl = "/api/v1",
    private readonly forceDemo = isDemoMode(),
  ) {}

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const controller = new AbortController();
    const timeout = globalThis.setTimeout(() => controller.abort(), 3500);
    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        ...init,
        headers: { Accept: "application/json", "Content-Type": "application/json", ...apiAuthHeaders(), ...init.headers },
        signal: controller.signal,
      });
      if (!response.ok) {
        let detail = "";
        try {
          const body = (await response.json()) as { detail?: string };
          detail = body.detail ? `: ${body.detail}` : "";
        } catch {
          // The HTTP status remains actionable when an upstream returns a non-JSON error body.
        }
        if (response.status === 401) {
          rejectSession();
        }
        throw new ApiError(`Eyvan API request failed with ${response.status}${detail}`, response.status);
      }
      return (await response.json()) as T;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      if (controller.signal.aborted) throw new ApiError("Eyvan API request timed out");
      throw new ApiError(error instanceof Error ? `Unable to reach Eyvan API: ${error.message}` : "Unable to reach Eyvan API");
    } finally {
      globalThis.clearTimeout(timeout);
    }
  }

  private async requestText(path: string): Promise<string> {
    const controller = new AbortController();
    const timeout = globalThis.setTimeout(() => controller.abort(), 3500);
    try {
      const response = await fetch(`${this.baseUrl}${path}`, { headers: apiAuthHeaders(), signal: controller.signal });
      if (!response.ok) {
        if (response.status === 401) {
          rejectSession();
        }
        throw new ApiError(`Eyvan API request failed with ${response.status}`, response.status);
      }
      return await response.text();
    } catch (error) {
      if (error instanceof ApiError) throw error;
      if (controller.signal.aborted) throw new ApiError("Eyvan API request timed out");
      throw new ApiError(error instanceof Error ? `Unable to reach Eyvan API: ${error.message}` : "Unable to reach Eyvan API");
    } finally {
      globalThis.clearTimeout(timeout);
    }
  }

  private scope(path: string): string {
    const join = path.includes("?") ? "&" : "?";
    return `${path}${join}org_id=${encodeURIComponent(this.org)}&project_id=${encodeURIComponent(this.project)}`;
  }

  async getHealth(): Promise<ControlPlaneHealth> {
    if (this.forceDemo) return { status: "demo", service: "eyvan" };
    return this.request<ControlPlaneHealth>("/health");
  }

  async getOverview(): Promise<OverviewData> {
    const [workloads, cells, budgets, queueTrend] = await Promise.all([
      this.listWorkloads(),
      this.listCells(),
      this.listBudgets(),
      this.getQueueTrend(),
    ]);
    return computeOverview(workloads, cells, budgets, queueTrend);
  }

  async getQueueTrend(): Promise<OverviewData["queueTrend"]> {
    if (this.forceDemo) return [];
    try {
      const raw = await this.request<RawQueueTrend>(this.scope("/capacity/queue-trend"), { method: "GET" });
      return raw.points.map((point) => ({
        label: new Date(point.started_at).toISOString().slice(11, 16),
        queued: point.submitted,
        admitted: point.admitted,
      }));
    } catch {
      // A missing trend must not blank the whole overview: every other panel on
      // it is independent, and an empty chart is a smaller failure than none.
      return [];
    }
  }

  async listWorkloads(filters: WorkloadFilters = {}): Promise<WorkloadSummary[]> {
    const workloads: WorkloadSummary[] = this.forceDemo
      ? cloneMock(mockWorkloads)
      : (await this.request<RawWorkload[]>(this.scope("/workloads"), { method: "GET" })).map(mapWorkload);
    const search = filters.search?.toLowerCase();
    return workloads.filter((workload) =>
      (!search || workload.name.toLowerCase().includes(search) || workload.id.toLowerCase().includes(search) || workload.user.toLowerCase().includes(search)) &&
      (!filters.status || workload.status === filters.status) &&
      (!filters.priority || workload.priority === filters.priority));
  }

  async getWorkload(id: string): Promise<WorkloadDetail> {
    if (this.forceDemo) {
      const mock = getMockWorkload(id);
      if (!mock) throw new ApiError(`Workload ${id} was not found`, 404);
      return cloneMock(mock);
    }
    const raw = await this.request<RawWorkload>(`/workloads/${encodeURIComponent(id)}`, { method: "GET" });
    const detail = mapWorkload(raw);
    const [events, logs, progress, leases] = await Promise.all([
      this.request<RawEvent[]>(this.scope(`/events?entity_type=workload&entity_id=${encodeURIComponent(id)}`), { method: "GET" }),
      this.requestText(`/workloads/${encodeURIComponent(id)}/logs`),
      // Progress is best-effort: an older control plane without the endpoint must not break the detail page.
      this.request<RawWorkloadProgress>(`/workloads/${encodeURIComponent(id)}/progress`, { method: "GET" }).catch(() => undefined),
      // Allocation comes from this workload's own leases. It used to be hardcoded
      // to [], so the panel rendered nothing against a live server while the mock
      // data populated it — the page looked implemented and was not.
      this.request<RawLease[]>(this.scope(`/leases?workload_id=${encodeURIComponent(id)}`), { method: "GET" }).catch(() => []),
    ]);
    detail.allocation = leases.map((lease) => ({
      host: lease.cell_id,
      rack: lease.state,
      resource: describeResources(lease.resources.per_replica, lease.resources.replicas),
      state: leaseAllocationState(lease.state),
    }));
    detail.timeline = events.map((event): TimelineEvent => ({ id: event.id, timestamp: event.timestamp, title: event.action, detail: `${event.previous_state ?? "new"} -> ${event.new_state ?? "recorded"}`, severity: event.action.includes("failed") ? "critical" : "info", actor: event.actor }));
    detail.logs = logs.split("\n").filter(Boolean).map((line, index): LogEntry => ({ id: `${id}-log-${index}`, ...parseLogLine(line) }));
    if (progress) {
      detail.progress = mapProgressPercent(progress);
      detail.queueReason ??= progress.failure_reason ?? progress.scheduling_reason ?? undefined;
    }
    return detail;
  }

  async listCells(knownLeases?: CapacityLease[]): Promise<Cell[]> {
    if (this.forceDemo) return cloneMock(mockCells);
    const [raw, leases] = knownLeases
      ? [await this.request<RawCell[]>("/cells", { method: "GET" }), knownLeases]
      : await Promise.all([
          this.request<RawCell[]>("/cells", { method: "GET" }),
          this.listLeases(),
        ]);
    // A deleted cell is capacity that no longer exists. The control plane keeps
    // the record for audit and replay, but listing it as fleet inventory means
    // one churny afternoon leaves a Fleet page that is mostly tombstones -- and
    // every one of them counted against the health summary.
    return raw
      .filter((cell) => cell.state !== "deleted")
      .map((cell) => {
      const activeLeases = leases.filter(
        (lease) => lease.cell === cell.id && ["active", "activating"].includes(lease.state),
      );
      const resources = resourceRows(cell.capacity).map((item) => ({
        ...item,
        used: activeLeases.reduce((sum, lease) => sum + (lease.resourceTotals?.[item.name] ?? 0), 0),
      }));
      const utilization = Math.round(
        Math.max(0, ...resources.map((resource) => resource.total ? resource.used / resource.total * 100 : 0)),
      );
      return {
        id: cell.id,
        name: cell.id,
        capacityClass: cell.capacity_class as Cell["capacityClass"],
        backend: `${cell.provider}${cell.provider_project ? ` / ${cell.provider_project}` : ""}`,
        region: cell.region,
        zone: cell.zone ?? "-",
        status: cellStatus(cell.state),
        utilization,
        activeWorkloads: new Set(activeLeases.map((lease) => lease.workloadId)).size,
        topology: cell.accelerator_type ?? "TPU",
        resources,
        lastHeartbeat: cell.last_heartbeat_at ?? new Date(0).toISOString(),
      };
    });
  }

  async listLeases(): Promise<CapacityLease[]> {
    if (this.forceDemo) return cloneMock(mockLeases);
    // Raw cells, not this.listCells(): that method resolves leases to compute
    // utilization, so calling it from here would recurse. The join needs one
    // field, which the raw record already carries.
    const [raw, cells] = await Promise.all([
      this.request<RawLease[]>(this.scope("/leases"), { method: "GET" }),
      this.request<RawCell[]>("/cells", { method: "GET" }).catch(() => [] as RawCell[]),
    ]);
    const cellClasses = new Map(
      cells.map((cell) => [cell.id, cell.capacity_class as CapacityLease["class"]]),
    );
    return raw.map((lease) => ({
      id: lease.id,
      workloadId: lease.workload_id,
      owner: lease.project_id,
      cell: lease.cell_id,
      resource: lease.resources.per_replica.tpu ? "TPU" : "CPU",
      amount: `${lease.resources.replicas} x ${lease.resources.per_replica.tpu || lease.resources.per_replica.cpu}`,
      state: lease.state === "pending" ? "activating" : lease.state === "released" || lease.state === "expired" ? "expired" : lease.state === "quarantined" ? "revoking" : lease.state as CapacityLease["state"],
      expiresAt: lease.expires_at,
      spendPerHour: lease.hourly_cost,
      resourceTotals: Object.fromEntries(
        resourceRows(lease.resources.per_replica).map((resource) => [
          resource.name,
          resource.total * lease.resources.replicas,
        ]),
      ),
      // Joined client-side from the cell the lease landed on. The server has no
      // capacity class on a lease record, and the dashboard already holds both
      // collections -- so this costs nothing and stops the borrowed-capacity
      // count from being permanently zero.
      class: cellClasses.get(lease.cell_id),
    }));
  }

  async listBudgets(): Promise<Budget[]> {
    if (this.forceDemo) return cloneMock(mockBudgets);
    const raw = await this.request<RawBudget[]>(`/budgets?org_id=${encodeURIComponent(this.org)}`, { method: "GET" });
    // The forecast is derived server-side from active leases and the period
    // window, so it needs one call per budget. Settled independently: a budget
    // whose status is unavailable still renders with its spend and limit, which
    // is strictly more than the previous behaviour of never showing a forecast.
    const forecasts = await Promise.all(
      raw.map(async (budget) => {
        try {
          const status = await this.request<RawBudgetStatus>(
            `/budgets/${encodeURIComponent(budget.org_id)}/${encodeURIComponent(budget.project_id)}/status`,
            { method: "GET" },
          );
          return status.forecast ?? undefined;
        } catch {
          return undefined;
        }
      }),
    );
    return raw.map((budget, index) => ({ id: `${budget.org_id}/${budget.project_id}`, owner: budget.project_id, scope: "project", spent: budget.spent, limit: budget.limit, forecast: forecasts[index], period: budget.period, enforcement: budget.hard_limit ? "hard" : "soft" }));
  }

  async listPolicies(): Promise<SchedulingPolicy[]> {
    if (this.forceDemo) return cloneMock(mockPolicies);
    const raw = await this.request<RawPolicy[]>(`/policies?org_id=${encodeURIComponent(this.org)}`, { method: "GET" });
    return raw.map(mapPolicy);
  }

  async listAuditEvents(): Promise<AuditEvent[]> {
    if (this.forceDemo) return cloneMock(mockAuditEvents);
    const raw = await this.request<RawEvent[]>("/fleet/events", { method: "GET" });
    return raw.map((event) => ({
      id: event.id,
      sequence: event.sequence,
      timestamp: event.timestamp,
      action: event.action,
      actor: event.actor,
      target: event.entity_id,
      category: event.entity_type === "workload" ? "workload" : ["cell", "lease"].includes(event.entity_type) ? "capacity" : ["policy", "quota"].includes(event.entity_type) ? "policy" : event.entity_type === "budget" ? "budget" : "system",
      severity: event.action.includes("failed") ? "critical" : "info",
      detail: `${event.previous_state ?? "new"} -> ${event.new_state ?? "recorded"}`,
    }));
  }

  async submitWorkload(input: SubmitWorkloadInput): Promise<WorkloadDetail> {
    if (this.forceDemo) return createMockWorkload(input);
    const accelerator = input.resources.accelerator;
    const payload = {
      org_id: this.org,
      project_id: input.project,
      spec: {
        name: input.name,
        execution: { kind: "command", command: input.command },
        gang: {
          replicas: input.resources.replicas,
          per_replica: { cpu: input.resources.cpu, memory_gib: input.resources.memoryGb, gpu: 0, tpu: input.resources.acceleratorCount ?? 0 },
          required_topology: input.resources.topology ? { slice: input.resources.topology } : {},
        },
        provider: "gcp",
        accelerator_type: accelerator,
        capacity_class: input.capacityClass,
        priority_band: input.priority,
        estimated_duration_seconds: input.estimatedDurationSeconds,
        checkpointable: input.preemptible,
        checkpoint_hook: input.checkpointHook,
        checkpoint_output_uri: input.checkpointUri,
        metadata: input.labels,
      },
    };
    const raw = await this.request<RawWorkload>("/workloads", { method: "POST", headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` }, body: JSON.stringify(payload) });
    return mapWorkload(raw);
  }

  async workloadAction(id: string, action: "cancel" | "checkpoint" | "retry"): Promise<WorkloadDetail> {
    if (this.forceDemo) {
      const mock = getMockWorkload(id);
      if (!mock) throw new ApiError(`Workload ${id} was not found`, 404);
      return { ...cloneMock(mock), status: action === "cancel" ? "preempting" : action === "checkpoint" ? "checkpointing" : "queued" };
    }
    const raw = await this.request<RawWorkload>(
      `/workloads/${encodeURIComponent(id)}/${action}`,
      {
        method: "POST",
        headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
      },
    );
    return mapWorkload(raw);
  }

  async updatePolicy(policy: SchedulingPolicy, preemptionEnabled: boolean): Promise<SchedulingPolicy> {
    if (this.forceDemo) return { ...cloneMock(policy), preemptionEnabled, updatedAt: new Date().toISOString() };
    const configuration = policy.configuration;
    if (!configuration) throw new ApiError(`Policy ${policy.id} is missing its editable configuration`);
    const raw = await this.request<RawPolicy>(
      `/policies/${encodeURIComponent(this.org)}/${encodeURIComponent(policy.scope)}`,
      {
        method: "PUT",
        headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
        body: JSON.stringify({
          allowed_providers: configuration.allowedProviders,
          allowed_regions: configuration.allowedRegions,
          allowed_capacity_classes: configuration.allowedCapacityClasses,
          denied_accelerator_types: configuration.deniedAcceleratorTypes,
          max_priority: configuration.maxPriority,
          preemption_enabled: preemptionEnabled,
        }),
      },
    );
    return mapPolicy(raw);
  }
}

export const apiClient = new EyvanApiClient();
