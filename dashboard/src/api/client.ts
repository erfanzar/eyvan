// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import {
  cloneMock,
  createMockWorkload,
  getMockWorkload,
  mockAuditEvents,
  mockBudgets,
  mockAvailableShapes,
  mockCells,
  mockLeases,
  mockPolicies,
  mockWorkloads,
} from "./mockData";
import { apiAuthHeaders, configuredScope, isDemoMode, rejectSession, type AuthScope } from "../auth/oidc";
import { EventStreamHttpError, followEventStream, type StreamEvent } from "../lib/eventStream";
import type {
  AuditEvent,
  AvailableShape,
  BillingReport,
  CapacityPoolSummary,
  ProjectUsage,
  Budget,
  CapacityLease,
  Cell,
  ControlPlaneHealth,
  EffectiveProjectScope,
  LaunchLatencyReport,
  LogEntry,
  OverviewData,
  PriorityBand,
  SchedulingPolicy,
  SessionAuthority,
  SessionPermission,
  SubmitWorkloadInput,
  TpuQuotaSnapshot,
  WarmthReport,
  WorkloadExplanation,
  WorkloadProcessStatus,
  WorkloadSpend,
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
  /**
   * How the plane accounted for every attempt after the first. A spot
   * reclamation and a capacity loss are charged to different budgets because
   * they mean opposite things about whether running again can work, and only
   * one of them is the expected ending of a spot job. Optional: a plane that
   * predates the split reports neither, and absent must read as unknown rather
   * than as zero preemptions.
   */
  preemption_count?: number;
  capacity_loss_count?: number;
  failure_reason?: string | null;
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
  /** Slices allocated together (pods=N). Absent on planes predating multislice. */
  cell_group_count?: number;
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
  topology?: Record<string, string>;
  last_heartbeat_at?: string;
  availability?: {
    cooldown_until?: string | null;
    consecutive_failures?: number;
    interruptions?: number;
  };
}

/** One advertised pool from `/capacity`, readable with `submit` alone. */
interface RawAvailablePool {
  fleet: string;
  pool: string;
  accelerator_type: string;
  capacity_class: string;
  region: string;
  zone?: string | null;
  enabled: boolean;
}

/** One run's totalled spend, as the workload cost route reports it. */
interface RawWorkloadSpend {
  workload_id: string;
  state: string;
  accelerator_hours: Record<string, number>;
  settled: number;
  priced: boolean;
  reserved: number;
  leases: number;
  shape?: string | null;
  capacity_class?: string | null;
  on_demand_hourly?: number | null;
  on_demand_source: string;
}

interface RawLease {
  id: string;
  workload_id: string;
  project_id: string;
  cell_id: string;
  state: string;
  resources: { replicas: number; slices?: number; per_replica: RawResources };
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

interface RawSessionScope {
  org_id: string;
  project_id: string;
  display_name: string;
  registered: boolean;
  enabled: boolean;
  accepts_new_work: boolean;
  roles: string[];
  permissions: SessionPermission[];
}

interface RawSessionAuthority {
  subject: string;
  display_name?: string | null;
  global_permissions: SessionPermission[];
  scopes: RawSessionScope[];
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
    org: raw.org_id,
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
      slices: typeof raw.spec.cell_group_count === "number" ? raw.spec.cell_group_count : undefined,
      topology: Object.values(raw.spec.gang.required_topology).join(", ") || undefined,
    },
    submittedAt: raw.created_at,
    startedAt: raw.state === "running" ? raw.updated_at : undefined,
    queueReason: raw.scheduling_reason,
    command: raw.spec.execution.kind === "command" ? raw.spec.execution.command : raw.spec.execution.artifact_uri,
    attempt: raw.attempt,
    preemptionCount: raw.preemption_count,
    capacityLossCount: raw.capacity_loss_count,
    failureReason: raw.failure_reason ?? undefined,
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

function describeTopology(topology: Record<string, string> | undefined): string {
  const entries = Object.entries(topology ?? {}).sort(([left], [right]) => left.localeCompare(right));
  return entries.map(([key, value]) => `${key}=${value}`).join(", ") || "Unspecified";
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

/** One structured log record as the follow stream frames it. */
interface RawLogRecord {
  message: string;
  stream?: string;
  timestamp?: string | null;
  rank?: number;
  pid?: number | null;
}

/**
 * Project one streamed record onto the entry the log view renders.
 *
 * The rank leads the source column because in a gang it is the identity that
 * matters: sixteen hosts printing the same line is normal, and one of them
 * printing something different is the entire diagnosis. A bracketed tag inside
 * the line is kept alongside it rather than instead of it -- the program's own
 * idea of where the line came from does not replace which process said it.
 *
 * The record's capture time is a fallback, not the preference. A line that
 * stamps itself dates the event; the record dates the read, and on a host that
 * has been buffering those differ by however long the buffer held.
 */
function mapLogRecord(record: RawLogRecord, id: string): LogEntry {
  const parsed = parseLogLine(record.message);
  const rank = typeof record.rank === "number" ? `rank ${record.rank}` : undefined;
  const source = [rank, parsed.source].filter(Boolean).join(" · ") || undefined;
  const stream =
    record.stream === "stdout" || record.stream === "stderr" ? record.stream : ("unknown" as const);
  return {
    id,
    timestamp: parsed.timestamp ?? record.timestamp ?? undefined,
    level: parsed.level,
    source,
    stream,
    message: parsed.message,
  };
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

/** Budget for a control-plane read that is answered from its own store. */
const CONTROL_PLANE_TIMEOUT_MS = 3_500;

/**
 * Budget for a read the control plane forwards to a worker.
 *
 * Logs are fetched from the host that produced them, so the latency is a
 * network round trip to a TPU VM rather than a database read -- 12s is ordinary
 * for a live slice. Sharing the control-plane budget meant the workload detail
 * page timed out on every workload whose logs were remote, which is every
 * workload that has actually run somewhere.
 */
const WORKER_FORWARDED_TIMEOUT_MS = 30_000;

/**
 * Budget for a read the control plane answers by calling the cloud provider.
 *
 * Provider quota discovery lists accelerator types in every quota zone a
 * project can see: `gcloud` calls, not a database read, and the better part of
 * a minute on a cold cache. On the control-plane budget the quota page timed
 * out on every first visit and showed "Eyvan API request timed out" -- the read
 * itself was fine and finishing, and nothing was ever waiting for it.
 *
 * Comfortably above the server's own deadline, which answers with the previous
 * reading or a stated failure rather than hanging. This budget exists to
 * outlast that answer, not to replace it.
 */
const PROVIDER_READ_TIMEOUT_MS = 45_000;

export class EyvanApiClient {
  private activeScope: AuthScope;

  constructor(
    private readonly baseUrl = "/api/v1",
    private readonly forceDemo = isDemoMode(),
    initialScope: AuthScope = configuredScope(),
  ) {
    this.activeScope = { ...initialScope };
  }

  getScope(): AuthScope {
    return { ...this.activeScope };
  }

  setScope(scope: AuthScope): void {
    if (!scope.orgId || !scope.projectId) throw new Error("dashboard scope requires an organization and project");
    this.activeScope = { ...scope };
  }

  private async request<T>(path: string, init: RequestInit = {}, timeoutMs = CONTROL_PLANE_TIMEOUT_MS): Promise<T> {
    const controller = new AbortController();
    const externalSignal = init.signal;
    const cancel = () => controller.abort();
    if (externalSignal?.aborted) cancel();
    else externalSignal?.addEventListener("abort", cancel, { once: true });
    const timeout = globalThis.setTimeout(cancel, timeoutMs);
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
      externalSignal?.removeEventListener("abort", cancel);
      globalThis.clearTimeout(timeout);
    }
  }

  private async requestText(
    path: string,
    timeoutMs = CONTROL_PLANE_TIMEOUT_MS,
    externalSignal?: AbortSignal,
  ): Promise<string> {
    const controller = new AbortController();
    const cancel = () => controller.abort();
    if (externalSignal?.aborted) cancel();
    else externalSignal?.addEventListener("abort", cancel, { once: true });
    const timeout = globalThis.setTimeout(cancel, timeoutMs);
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
      externalSignal?.removeEventListener("abort", cancel);
      globalThis.clearTimeout(timeout);
    }
  }

  private scopeSnapshot(scope: AuthScope = this.activeScope): AuthScope {
    return { orgId: scope.orgId, projectId: scope.projectId };
  }

  private scope(path: string, scope: AuthScope): string {
    const join = path.includes("?") ? "&" : "?";
    return `${path}${join}org_id=${encodeURIComponent(scope.orgId)}&project_id=${encodeURIComponent(scope.projectId)}`;
  }

  async getSessionAuthority(
    preferred: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<SessionAuthority> {
    const preferredScope = this.scopeSnapshot(preferred);
    if (this.forceDemo) {
      const scope: EffectiveProjectScope = {
        ...preferredScope,
        displayName: "Demo project",
        registered: true,
        enabled: true,
        acceptsNewWork: true,
        roles: ["fleet_admin"],
        permissions: ["read", "submit", "operate", "manage_project", "manage_own_capacity", "manage_fleet"],
      };
      return {
        subject: "dashboard-demo",
        displayName: "Demo dataset",
        globalPermissions: ["read", "submit", "operate", "manage_project", "manage_own_capacity", "manage_fleet"],
        scopes: [scope],
      };
    }
    const raw = await this.request<RawSessionAuthority>(this.scope("/session", preferredScope), {
      method: "GET",
      signal,
    });
    return {
      subject: raw.subject,
      displayName: raw.display_name ?? undefined,
      globalPermissions: raw.global_permissions,
      scopes: raw.scopes.map((scope) => ({
        orgId: scope.org_id,
        projectId: scope.project_id,
        displayName: scope.display_name,
        registered: scope.registered,
        enabled: scope.enabled,
        acceptsNewWork: scope.accepts_new_work,
        roles: scope.roles,
        permissions: scope.permissions,
      })),
    };
  }

  /**
   * Follow this tenant's audit stream, calling `onEvent` as changes commit.
   *
   * A change *notification*, not a data channel. The stream carries audit
   * events for one (org, project); the dashboard's own model spans fleet
   * capacity too, so an event means "refetch", and the slow poll beside it
   * still covers what this stream cannot see -- another tenant's work landing
   * on a shared cell, or a cell going unhealthy without an audit event.
   *
   * Resolves only when `signal` aborts. In demo mode it resolves immediately:
   * there is no plane to stream from, and pretending otherwise would show a
   * "live" indicator over mock data.
   */
  async followEvents(
    onEvent: (event: StreamEvent) => void,
    signal: AbortSignal,
    onConnectionChange?: (connected: boolean) => void,
    scope: AuthScope = this.activeScope,
  ): Promise<void> {
    if (this.forceDemo) return;
    const requestScope = this.scopeSnapshot(scope);
    try {
      await followEventStream({
        url: `${this.baseUrl}${this.scope("/events/stream", requestScope)}`,
        headers: apiAuthHeaders(),
        signal,
        onEvent,
        onConnectionChange,
      });
    } catch (streamError) {
      if (streamError instanceof EventStreamHttpError) {
        if (streamError.status === 401) rejectSession();
        throw new ApiError(streamError.message, streamError.status);
      }
      throw streamError;
    }
  }

  /**
   * Follow one workload's output as it is produced, record by record.
   *
   * A data channel, unlike `followEvents`: these frames carry the lines
   * themselves, so nothing is refetched and no page read can substitute. That
   * is why it is a separate stream -- the audit stream is keyed on a global
   * sequence for one (org, project), while log records are per
   * (workload, attempt, rank) and have no global order at all.
   *
   * `onAttemptChanged` is not a notification the caller may ignore. A retry
   * replaces the remote execution, so the lines that follow belong to a
   * different process; appending them to what is already on screen would show
   * a job that resumed where it left off, which is the one story the reader
   * must not be told.
   *
   * Resolves only when `signal` aborts. In demo mode it resolves immediately:
   * there is no process to follow, and replaying canned lines under a "live"
   * indicator is a lie the reader cannot detect.
   */
  async followWorkloadLogs(
    id: string,
    handlers: {
      onRecord: (entry: LogEntry) => void;
      onAttemptChanged: (attempt: number) => void;
      onConnectionChange?: (connected: boolean) => void;
    },
    signal: AbortSignal,
  ): Promise<void> {
    if (this.forceDemo) return;
    // Only ever incremented, and never derived from the record's own content:
    // two ranks emit identical lines at the same millisecond routinely, and a
    // key collision makes React reuse one row for both.
    let delivered = 0;
    try {
      await followEventStream({
        url: `${this.baseUrl}/workloads/${encodeURIComponent(id)}/logs/stream`,
        headers: apiAuthHeaders(),
        signal,
        onConnectionChange: handlers.onConnectionChange,
        onEvent: (event) => {
          if (event.event === "log.attempt_changed") {
            const attempt = (event.data as { attempt?: number } | null)?.attempt;
            if (typeof attempt === "number") handlers.onAttemptChanged(attempt);
            return;
          }
          if (event.event !== "log.record") return;
          const record = event.data as RawLogRecord | null;
          if (!record || typeof record.message !== "string") return;
          delivered += 1;
          handlers.onRecord(mapLogRecord(record, `${id}-stream-${delivered}`));
        },
      });
    } catch (streamError) {
      if (streamError instanceof EventStreamHttpError) {
        if (streamError.status === 401) rejectSession();
        throw new ApiError(streamError.message, streamError.status);
      }
      throw streamError;
    }
  }

  async getHealth(signal?: AbortSignal): Promise<ControlPlaneHealth> {
    if (this.forceDemo) return { status: "demo", service: "eyvan" };
    return this.request<ControlPlaneHealth>("/health", { signal });
  }

  async getOverview(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<OverviewData> {
    const requestScope = this.scopeSnapshot(scope);
    const [workloads, cells, budgets, queueTrend] = await Promise.all([
      this.listWorkloads({}, requestScope, signal),
      this.listCells(undefined, requestScope, signal),
      this.listBudgets(requestScope, signal),
      this.getQueueTrend(requestScope, signal),
    ]);
    return computeOverview(workloads, cells, budgets, queueTrend);
  }

  async getQueueTrend(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<OverviewData["queueTrend"]> {
    if (this.forceDemo) return [];
    const requestScope = this.scopeSnapshot(scope);
    // Seven explicit 24-hour buckets match the seven chart columns. These are
    // submission/admission event counts, not instantaneous queue depth.
    const raw = await this.request<RawQueueTrend>(
      this.scope("/capacity/queue-trend?window_s=86400&buckets=7", requestScope),
      { method: "GET", signal },
    );
    return raw.points.map((point) => ({
      label: new Date(point.started_at).toISOString().slice(11, 16),
      submitted: point.submitted,
      admitted: point.admitted,
    }));
  }

  async listWorkloads(
    filters: WorkloadFilters = {},
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadSummary[]> {
    const requestScope = this.scopeSnapshot(scope);
    const workloads: WorkloadSummary[] = this.forceDemo
      ? cloneMock(mockWorkloads)
      : (
          await this.request<RawWorkload[]>(this.scope("/workloads", requestScope), {
            method: "GET",
            signal,
          })
        ).map(mapWorkload);
    const search = filters.search?.toLowerCase();
    return workloads
      .filter((workload) =>
        (!search || workload.name.toLowerCase().includes(search) || workload.id.toLowerCase().includes(search) || workload.user.toLowerCase().includes(search)) &&
        (!filters.status || workload.status === filters.status) &&
        (!filters.priority || workload.priority === filters.priority))
      // Newest first. The control plane returns insertion order, so a fleet that
      // has run for a while buried the job you just submitted under every job
      // you had ever submitted -- and the one you want is almost always the last
      // one. Ties break on id so the order is stable across polls rather than
      // shuffling under the reader.
      .sort((left, right) => {
        const delta = Date.parse(right.submittedAt) - Date.parse(left.submittedAt);
        return delta !== 0 && Number.isFinite(delta) ? delta : right.id.localeCompare(left.id);
      });
  }

  async getWorkload(
    id: string,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadDetail> {
    // Capture the caller's UI scope before the first await. The resource route
    // resolves its own immutable scope server-side; the snapshot fences this
    // composition from a later project-switch mutating the active client.
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    if (this.forceDemo) {
      const mock = getMockWorkload(id);
      if (!mock) throw new ApiError(`Workload ${id} was not found`, 404);
      return cloneMock(mock);
    }
    const raw = await this.request<RawWorkload>(`/workloads/${encodeURIComponent(id)}`, {
      method: "GET",
      signal,
    });
    const detail = mapWorkload(raw);
    const workloadScope = { orgId: raw.org_id, projectId: raw.project_id };
    const [events, logs, progress, leases] = await Promise.all([
      // Also best-effort. The timeline is context; losing it should cost the
      // timeline, not the page.
      this.request<RawEvent[]>(
        this.scope(`/events?entity_type=workload&entity_id=${encodeURIComponent(id)}`, workloadScope), {
        method: "GET",
        signal,
      }).catch(() => [] as RawEvent[]),
      // Best-effort, and slow on purpose: this is forwarded to the worker. It
      // used to share the 3.5s control-plane budget *and* sit in this
      // `Promise.all` with no fallback, so one slow log fetch blanked a page
      // whose workload, events and progress had all answered in under half a
      // second. An empty detail page reads as "this workload does not exist".
      this.requestText(`/workloads/${encodeURIComponent(id)}/logs`, WORKER_FORWARDED_TIMEOUT_MS, signal).catch(
        (error: unknown) => `logs unavailable: ${error instanceof Error ? error.message : "unknown error"}`,
      ),
      // Progress is best-effort: an older control plane without the endpoint must not break the detail page.
      this.request<RawWorkloadProgress>(`/workloads/${encodeURIComponent(id)}/progress`, {
        method: "GET",
        signal,
      }).catch(() => undefined),
      // Allocation comes from this workload's own leases. It used to be hardcoded
      // to [], so the panel rendered nothing against a live server while the mock
      // data populated it — the page looked implemented and was not.
      this.request<RawLease[]>(
        this.scope(`/leases?workload_id=${encodeURIComponent(id)}`, workloadScope),
        { method: "GET", signal },
      ).catch(() => []),
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

  /**
   * Why one workload is or is not currently placeable.
   *
   * `scheduling_reason` is one flat sentence, and it is what the dashboard has
   * always shown. This is the per-cell detail behind it: which cells were
   * considered, which constraint each failed, and whether any are still being
   * built -- the difference between "waiting for capacity" and "can never be
   * placed", which the flat reason cannot express.
   */
  async explainWorkload(
    id: string,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadExplanation> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    return this.request<WorkloadExplanation>(`/workloads/${encodeURIComponent(id)}/explain`, { signal });
  }

  /**
   * What one run used and was charged, totalled by the control plane.
   *
   * Computed there rather than here: the honest answer needs pool unit prices,
   * which are a fleet read, so a browser assembling it would either be wrong or
   * be limited to fleet administrators. It is authorized as a read of the
   * workload, which is the permission the question implies.
   */
  async getWorkloadCost(
    id: string,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadSpend> {
    void this.scopeSnapshot(scope);
    const raw = await this.request<RawWorkloadSpend>(`/workloads/${encodeURIComponent(id)}/cost`, { signal });
    return {
      workloadId: raw.workload_id,
      state: raw.state,
      acceleratorHours: raw.accelerator_hours ?? {},
      settled: raw.settled,
      priced: raw.priced,
      reserved: raw.reserved,
      leases: raw.leases,
      shape: raw.shape ?? undefined,
      capacityClass: raw.capacity_class ?? undefined,
      onDemandHourly: raw.on_demand_hourly ?? undefined,
      onDemandSource: raw.on_demand_source,
    };
  }

  /**
   * Live process detail for a running attempt.
   *
   * Answers "it says RUNNING and nothing is happening", which no other view
   * does: the control group and pids are what a diagnostic needs to attach to.
   */
  async workloadProcessStatus(
    id: string,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadProcessStatus> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    return this.request<WorkloadProcessStatus>(`/workloads/${encodeURIComponent(id)}/process-status`, { signal });
  }

  /**
   * Each pool's warm floor and whether it is currently held.
   *
   * Read beside billing. Warm capacity is deliberately idle, and without the
   * pairing the only safe reaction to an unexplained idle figure is to delete
   * the warm floor -- undoing the fix that put it there.
   */
  /** Provisioning latency per pool, from the samples the scheduler ranks on. */
  async getLaunchLatency(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<LaunchLatencyReport> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    return this.request<LaunchLatencyReport>("/capacity/launch-latency", { signal });
  }

  async getWarmth(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<WarmthReport> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    return this.request<WarmthReport>("/capacity/warmth", { signal });
  }

  /** Accrued and idle cell cost across the fleet. */
  async getBilling(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<BillingReport> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    return this.request<BillingReport>("/capacity/billing", { signal });
  }

  /**
   * Provider TPU quota, live usage, and free balance per zone and type.
   *
   * The numbers `eyvan autoscale generate` has always read, for the person
   * choosing where to run. Deliberately not merged into any other panel: a
   * quota ceiling is a property of the cloud project, not of this fleet's
   * cells, and the two disagree constantly.
   *
   * `refresh` re-reads the provider instead of reusing a cached reading. It is
   * an explicit action because a read costs a burst of provider calls, and
   * because the snapshot already states its own age.
   */
  async getTpuQuota(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
    options: { project?: string; refresh?: boolean } = {},
  ): Promise<TpuQuotaSnapshot> {
    const requestScope = this.scopeSnapshot(scope);
    void requestScope;
    const query = new URLSearchParams();
    if (options.project) query.set("project", options.project);
    if (options.refresh) query.set("refresh", "true");
    const suffix = query.size ? `?${query.toString()}` : "";
    return this.request<TpuQuotaSnapshot>(`/capacity/quota${suffix}`, { signal }, PROVIDER_READ_TIMEOUT_MS);
  }

  async listCells(
    knownLeases?: CapacityLease[],
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<Cell[]> {
    if (this.forceDemo) return cloneMock(mockCells);
    const requestScope = this.scopeSnapshot(scope);
    const [raw, leases] = knownLeases
      ? [await this.request<RawCell[]>("/cells", { method: "GET", signal }), knownLeases]
      : await Promise.all([
          this.request<RawCell[]>("/cells", { method: "GET", signal }),
          this.listLeases(requestScope, signal, true),
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
          Math.max(0, ...resources.map((resource) => (resource.total ? (resource.used / resource.total) * 100 : 0))),
        );
        return {
          id: cell.id,
          name: cell.id,
          capacityClass: cell.capacity_class as Cell["capacityClass"],
          backend: `${cell.provider}${cell.provider_project ? ` / ${cell.provider_project}` : ""}`,
          region: cell.region,
          zone: cell.zone ?? "-",
          status: cellStatus(cell.state),
          state: cell.state,
          availability: cell.availability
            ? {
                cooldownUntil: cell.availability.cooldown_until ?? undefined,
                consecutiveFailures: cell.availability.consecutive_failures ?? 0,
                interruptions: cell.availability.interruptions ?? 0,
              }
            : undefined,
          utilization,
          activeWorkloads: new Set(activeLeases.map((lease) => lease.workloadId)).size,
          // Topology is the durable placement/failure-domain contract. The
          // accelerator type is a resource model, not a topology, and presenting
          // it here made distinct slices look interchangeable.
          topology: describeTopology(cell.topology),
          resources,
          lastHeartbeat: cell.last_heartbeat_at ?? new Date(0).toISOString(),
        };
      });
  }

  async listLeases(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
    includeFleetMetadata = true,
  ): Promise<CapacityLease[]> {
    if (this.forceDemo) return cloneMock(mockLeases);
    const requestScope = this.scopeSnapshot(scope);
    // Cell metadata is fleet-wide. Project readers must not probe that endpoint
    // merely to decorate a scoped lease with an optional capacity class.
    const cellsPromise = includeFleetMetadata
      ? this.request<RawCell[]>("/cells", { method: "GET", signal })
      : Promise.resolve([] as RawCell[]);
    const [raw, cells] = await Promise.all([
      this.request<RawLease[]>(this.scope("/leases", requestScope), { method: "GET", signal }),
      cellsPromise,
    ]);
    const cellClasses = new Map(cells.map((cell) => [cell.id, cell.capacity_class as CapacityLease["class"]]));
    return raw.map((lease) => ({
      id: lease.id,
      workloadId: lease.workload_id,
      owner: lease.project_id,
      cell: lease.cell_id,
      resource: lease.resources.per_replica.tpu ? "TPU" : "CPU",
      amount: `${lease.resources.replicas} x ${lease.resources.per_replica.tpu || lease.resources.per_replica.cpu}`,
      state:
        lease.state === "pending"
          ? "activating"
          : lease.state === "released" || lease.state === "expired"
            ? "expired"
            : lease.state === "quarantined"
              ? "revoking"
              : (lease.state as CapacityLease["state"]),
      expiresAt: lease.expires_at,
      spendPerHour: lease.hourly_cost,
      resourceTotals: Object.fromEntries(
        resourceRows(lease.resources.per_replica).map((resource) => [
          resource.name,
          resource.total * lease.resources.replicas,
        ]),
      ),
      class: cellClasses.get(lease.cell_id),
    }));
  }

  /**
   * Recent fleet-wide capacity history, newest first.
   *
   * The fleet ledger rather than the project one: a cell's events carry no
   * org or project, because capacity belongs to the fleet and not to whoever
   * happens to be running on it. A project-scoped read returns none of them,
   * which is why this page was previously unable to show what a zone did.
   */
  async listCapacityEvents(limit = 200, signal?: AbortSignal): Promise<AuditEvent[]> {
    if (this.forceDemo) return cloneMock(mockAuditEvents).filter((event) => event.category === "capacity");
    const raw = await this.request<RawEvent[]>(`/fleet/events?entity_type=cell&limit=${limit}`, { signal });
    return raw
      .map((event) => ({
        id: event.id,
        sequence: event.sequence,
        timestamp: event.timestamp,
        action: event.action,
        actor: event.actor,
        target: event.entity_id,
        category: "capacity" as const,
        severity: event.action.includes("failed") ? ("critical" as const) : ("info" as const),
        detail: `${event.previous_state ?? "new"} -> ${event.new_state ?? "recorded"}`,
      }))
      .sort((left, right) => (right.sequence ?? 0) - (left.sequence ?? 0));
  }

  async listBudgets(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<Budget[]> {
    if (this.forceDemo) return cloneMock(mockBudgets);
    const requestScope = this.scopeSnapshot(scope);
    let raw: RawBudget[];
    try {
      raw = [
        await this.request<RawBudget>(
          `/budgets/${encodeURIComponent(requestScope.orgId)}/${encodeURIComponent(requestScope.projectId)}`,
          { method: "GET", signal },
        ),
      ];
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) raw = [];
      else throw error;
    }
    const forecasts = await Promise.all(
      raw.map(async (budget) => {
        try {
          const status = await this.request<RawBudgetStatus>(
            `/budgets/${encodeURIComponent(budget.org_id)}/${encodeURIComponent(budget.project_id)}/status`,
            { method: "GET", signal },
          );
          return status.forecast ?? undefined;
        } catch (error) {
          // A cancellation or authority denial must abort the coherent snapshot;
          // only a genuinely unavailable forecast is optional decoration.
          if (signal?.aborted || (error instanceof ApiError && error.status === 403)) throw error;
          return undefined;
        }
      }),
    );
    return raw.map((budget, index) => ({
      id: `${budget.org_id}/${budget.project_id}`,
      owner: budget.project_id,
      scope: "project",
      spent: budget.spent,
      limit: budget.limit,
      forecast: forecasts[index],
      period: budget.period,
      enforcement: budget.hard_limit ? "hard" : "soft",
    }));
  }

  async listPolicies(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<SchedulingPolicy[]> {
    if (this.forceDemo) return cloneMock(mockPolicies);
    const requestScope = this.scopeSnapshot(scope);
    try {
      const raw = await this.request<RawPolicy>(
        `/policies/${encodeURIComponent(requestScope.orgId)}/${encodeURIComponent(requestScope.projectId)}`,
        { method: "GET", signal },
      );
      return [mapPolicy(raw)];
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) return [];
      throw error;
    }
  }

  async listAuditEvents(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<AuditEvent[]> {
    if (this.forceDemo) return cloneMock(mockAuditEvents);
    const requestScope = this.scopeSnapshot(scope);
    // Project audit is a project read. Calling /fleet/events first and degrading
    // its inevitable 403 made every non-fleet reader generate a forbidden call
    // and then presented an empty ledger as if the project had no history.
    const raw = await this.request<RawEvent[]>(this.scope("/events?limit=1000", requestScope), {
      method: "GET",
      signal,
    });
    return raw.map((event) => ({
      id: event.id,
      sequence: event.sequence,
      timestamp: event.timestamp,
      action: event.action,
      actor: event.actor,
      target: event.entity_id,
      category:
        event.entity_type === "workload"
          ? "workload"
          : ["cell", "lease"].includes(event.entity_type)
            ? "capacity"
            : ["policy", "quota"].includes(event.entity_type)
              ? "policy"
              : event.entity_type === "budget"
                ? "budget"
                : "system",
      severity: event.action.includes("failed") ? "critical" : "info",
      detail: `${event.previous_state ?? "new"} -> ${event.new_state ?? "recorded"}`,
    }));
  }

  /**
   * Every shape this scope may submit to, across applied fleets.
   *
   * Readable with `submit` alone, unlike the pool records behind it, and that
   * is the point: choosing what to run on should not require permission to
   * manage the fleet. It is also the only honest source for the form's
   * choices -- a hard-coded list of accelerator types is a list of shapes some
   * other fleet had.
   */
  async listAvailableShapes(
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<AvailableShape[]> {
    if (this.forceDemo) return cloneMock(mockAvailableShapes);
    const requestScope = this.scopeSnapshot(scope);
    const payload = await this.request<{ pools: RawAvailablePool[] }>(
      this.scope("/capacity", requestScope),
      { signal },
    );
    return payload.pools
      .filter((pool) => pool.enabled)
      .map((pool) => ({
        fleet: pool.fleet,
        pool: pool.pool,
        shape: pool.accelerator_type,
        capacityClass: pool.capacity_class as AvailableShape["capacityClass"],
        region: pool.region,
        zone: pool.zone ?? undefined,
      }));
  }

  /**
   * Submit one command against a fleet shape, the way `eyvan run` does.
   *
   * The pool supplies the infrastructure contract: CPU, memory, accelerator
   * count, topology and provider all come from the node type this lands on.
   * This used to build a spec by hand against the advanced `/workloads` route,
   * which meant the browser inventing every one of those -- it sent
   * `provider: "gcp"` on every submission, which is a hard constraint that
   * makes the job unplaceable on any other cloud, and a `required_topology` of
   * `slice: "single-slice"` whose values no pool declares, so the default
   * submission was refused with TOPOLOGY_MISMATCH against a value the user
   * never typed.
   */
  async submitWorkload(
    input: SubmitWorkloadInput,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadDetail> {
    if (this.forceDemo) return createMockWorkload(input);
    const requestScope = this.scopeSnapshot(scope);
    const payload = {
      org_id: requestScope.orgId,
      project_id: requestScope.projectId,
      command: input.command,
      pods: input.pods,
      zone: input.zone,
      name: input.name,
      environment: input.environment,
      metadata: input.labels,
      priority_band: input.priority,
      estimated_duration_seconds: input.estimatedDurationSeconds,
      checkpointable: input.checkpointable,
      checkpoint_hook: input.checkpointHook,
      checkpoint_output_uri: input.checkpointUri,
    };
    const raw = await this.request<RawWorkload>(
      `/fleets/${encodeURIComponent(input.fleet)}/node-types/${encodeURIComponent(input.shape)}/workloads`,
      {
        method: "POST",
        headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
        body: JSON.stringify(payload),
        signal,
      },
    );
    return mapWorkload(raw);
  }

  async workloadAction(
    id: string,
    action: "cancel" | "checkpoint" | "retry" | "kick",
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<WorkloadDetail> {
    const requestScope = this.scopeSnapshot(scope);
    if (this.forceDemo) {
      const mock = getMockWorkload(id);
      if (!mock) throw new ApiError(`Workload ${id} was not found`, 404);
      return {
        ...cloneMock(mock),
        // A kick ends this attempt and requeues the job, so it reads as queued
        // exactly like a retry -- the difference is which budget pays for it,
        // and that is the control plane's to record rather than the mock's.
        status: action === "cancel" ? "preempting" : action === "checkpoint" ? "checkpointing" : "queued",
      };
    }
    const raw = await this.request<RawWorkload>(`/workloads/${encodeURIComponent(id)}/${action}`, {
      method: "POST",
      headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
      signal,
    });
    if (raw.org_id !== requestScope.orgId || raw.project_id !== requestScope.projectId) {
      throw new ApiError("Workload mutation returned a resource outside the immutable requested scope", 409);
    }
    return mapWorkload(raw);
  }

  /**
   * Stop new placement on one cell and ask its agent to drain.
   *
   * Returns the state the control plane moved it to rather than a remapped
   * `Cell`: a cell's rendered shape is a join against its active leases, and
   * rebuilding that from one record would put a figure on the page that the
   * next refresh disagrees with. The caller refreshes inventory instead.
   *
   * Not offered in demo mode. A drain that appears to work against a dataset
   * with no fleet behind it teaches the gesture without its consequence.
   */
  async drainCell(cellId: string, signal?: AbortSignal): Promise<{ id: string; state: string }> {
    if (this.forceDemo) throw new ApiError("Demo mode has no fleet to drain", 400);
    const raw = await this.request<RawCell>(`/cells/${encodeURIComponent(cellId)}/drain`, {
      method: "POST",
      headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
      signal,
    });
    return { id: raw.id, state: raw.state };
  }

  /**
   * The pools this project may submit to, as registered rather than as running.
   *
   * The fleet page could only ever show cells -- what is provisioned right now
   * -- so a fleet with twenty pools and nothing running looked like an empty
   * fleet, and there was no way to see what had been applied at all.
   */
  async listCapacityPools(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<CapacityPoolSummary[]> {
    if (this.forceDemo) return [];
    const raw = await this.request<Record<string, unknown>[]>(
      `/capacity-pools?org_id=${encodeURIComponent(scope.orgId)}&project_id=${encodeURIComponent(scope.projectId)}`,
      { signal },
    );
    return (Array.isArray(raw) ? raw : []).map((pool) => ({
      id: String(pool.id ?? ""),
      fleetName: String(pool.fleet_name ?? ""),
      acceleratorType: (pool.accelerator_type as string | null) ?? null,
      capacityClass: String(pool.capacity_class ?? ""),
      zone: (pool.zone as string | null) ?? null,
      region: (pool.region as string | null) ?? null,
      maxCells: Number(pool.max_cells ?? 0),
      minCells: Number(pool.min_cells ?? 0),
      enabled: pool.enabled !== false,
    }));
  }

  /**
   * Who in this project used what, and what anybody can say it cost.
   *
   * The cost is zero wherever a pool declares no unit price, and the caller has
   * to be able to tell that from free -- hence `unpricedLeases` travelling
   * beside it rather than being folded in.
   */
  async getProjectUsage(scope: AuthScope = this.activeScope, signal?: AbortSignal): Promise<ProjectUsage> {
    const raw = await this.request<Record<string, unknown>>(
      `/usage?org_id=${encodeURIComponent(scope.orgId)}&project_id=${encodeURIComponent(scope.projectId)}`,
      { signal },
    );
    const subjects = Array.isArray(raw.subjects) ? (raw.subjects as Record<string, unknown>[]) : [];
    return {
      orgId: String(raw.org_id ?? scope.orgId),
      projectId: String(raw.project_id ?? scope.projectId),
      subjects: subjects.map((entry) => ({
        subject: String(entry.subject ?? "unknown"),
        workloads: Number(entry.workloads ?? 0),
        settledWorkloads: Number(entry.settled_workloads ?? 0),
        leases: Number(entry.leases ?? 0),
        acceleratorHours: Number(entry.accelerator_hours ?? 0),
        runningSeconds: Number(entry.running_seconds ?? 0),
        settledCost: Number(entry.settled_cost ?? 0),
        unpricedLeases: Number(entry.unpriced_leases ?? 0),
      })),
    };
  }

  /**
   * Drain a whole fleet to zero.
   *
   * The route is DELETE and its name is "disable": it turns every pool off and
   * sets its ceiling to zero so the autoscaler releases what is running. The
   * fleet's definition survives, which is why the UI offers this as stopping a
   * fleet rather than deleting one -- re-applying the same config brings it
   * back without re-generating anything.
   */
  async disableFleet(fleetName: string, signal?: AbortSignal): Promise<number> {
    if (this.forceDemo) throw new ApiError("Demo mode has no fleet to stop", 400);
    const pools = await this.request<unknown[]>(`/fleets/${encodeURIComponent(fleetName)}`, {
      method: "DELETE",
      headers: { "Idempotency-Key": globalThis.crypto?.randomUUID?.() ?? `dashboard-${Date.now()}` },
      signal,
    });
    return Array.isArray(pools) ? pools.length : 0;
  }

  async updatePolicy(
    policy: SchedulingPolicy,
    preemptionEnabled: boolean,
    scope: AuthScope = this.activeScope,
    signal?: AbortSignal,
  ): Promise<SchedulingPolicy> {
    if (this.forceDemo) return { ...cloneMock(policy), preemptionEnabled, updatedAt: new Date().toISOString() };
    const requestScope = this.scopeSnapshot(scope);
    const configuration = policy.configuration;
    if (!configuration) throw new ApiError(`Policy ${policy.id} is missing its editable configuration`);
    const raw = await this.request<RawPolicy>(
      `/policies/${encodeURIComponent(requestScope.orgId)}/${encodeURIComponent(requestScope.projectId)}`,
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
        signal,
      },
    );
    if (raw.org_id !== requestScope.orgId || raw.project_id !== requestScope.projectId) {
      throw new ApiError("Policy mutation returned a resource outside the immutable requested scope", 409);
    }
    return mapPolicy(raw);
  }

}

export const apiClient = new EyvanApiClient();
