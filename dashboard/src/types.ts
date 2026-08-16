// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

export type WorkloadStatus =
  | "queued"
  | "planning"
  | "provisioning"
  | "running"
  | "checkpointing"
  | "preempting"
  | "completed"
  | "cancelled"
  | "failed";

export type PriorityBand = "production" | "interactive" | "batch";

export type Severity = "info" | "success" | "warning" | "critical";

export type SessionPermission =
  | "read"
  | "submit"
  | "operate"
  | "manage_project"
  | "manage_own_capacity"
  | "manage_fleet";

export interface EffectiveProjectScope {
  orgId: string;
  projectId: string;
  displayName: string;
  registered: boolean;
  enabled: boolean;
  /** Server truth: false for disabled or registry-invalid scopes, true for legacy scopes. */
  acceptsNewWork: boolean;
  roles: string[];
  permissions: SessionPermission[];
}

export interface SessionAuthority {
  subject: string;
  displayName?: string;
  globalPermissions: SessionPermission[];
  scopes: EffectiveProjectScope[];
}

export interface ResourceRequest {
  cpu: number;
  memoryGb: number;
  accelerator?: string;
  acceleratorCount?: number;
  replicas: number;
  topology?: string;
}

export interface WorkloadSummary {
  id: string;
  name: string;
  org: string;
  project: string;
  user: string;
  status: WorkloadStatus;
  priority: PriorityBand;
  resources: ResourceRequest;
  cell?: string;
  submittedAt: string;
  startedAt?: string;
  progress?: number;
  /** Per-workload spend is not reported by the control plane; only demo data sets these. */
  budgetSpent?: number;
  budgetLimit?: number;
  queueReason?: string;
  /**
   * Which attempt is running, and what the earlier ones were charged to.
   *
   * The timeline carries the events, but counting them by hand is not the
   * question an operator has in front of a spot job: it is whether this job is
   * about to run out of retries, and whether what keeps killing it is the cloud
   * taking the capacity back or the pool failing to give any. Those are
   * separate budgets, and only one of them is the expected ending of a spot job.
   *
   * Optional throughout, because a plane that predates the split reports
   * neither, and absent must render as unknown rather than as zero.
   */
  attempt?: number;
  preemptionCount?: number;
  capacityLossCount?: number;
  failureReason?: string;
}

export interface TimelineEvent {
  id: string;
  timestamp: string;
  title: string;
  detail: string;
  severity: Severity;
  actor?: string;
}

export interface LogEntry {
  id: string;
  /** Present only when the log line carries a parseable timestamp. */
  timestamp?: string;
  /** Present only when the log line carries a recognized level token. */
  level?: "DEBUG" | "INFO" | "WARN" | "ERROR";
  /** Present only when the log line carries a bracketed source tag. */
  source?: string;
  /**
   * Which of the process's two streams produced the line.
   *
   * Populated only by the structured follow, because only it knows: the
   * one-shot text endpoint returns lines already merged, and inferring the
   * stream from the text would invent an attribution. Deliberately not folded
   * into `level` -- plenty of healthy programs write progress to stderr, and
   * rendering every one of those lines as an error is how a working job comes
   * to look like a failing one.
   */
  stream?: "stdout" | "stderr" | "unknown";
  message: string;
}

export interface WorkloadDetail extends WorkloadSummary {
  command: string;
  checkpointUri?: string;
  leaseId?: string;
  allocation: Array<{
    host: string;
    rack: string;
    resource: string;
    state: "ready" | "starting" | "draining";
  }>;
  timeline: TimelineEvent[];
  logs: LogEntry[];
  labels: Record<string, string>;
}

export interface OverviewData {
  queued: number;
  running: number;
  fleetUtilization: number;
  monthlySpend: number;
  monthlyBudget: number;
  successRate?: number;
  queueTrend: Array<{ label: string; submitted: number; admitted: number }>;
  priorityQueue: Array<{ priority: PriorityBand; count: number; oldestMinutes: number }>;
  fleetBreakdown: Array<{
    resource: string;
    used: number;
    total: number;
    pending: number;
  }>;
  alerts: Array<{
    id: string;
    severity: Severity;
    title: string;
    detail: string;
    timestamp: string;
  }>;
}

export interface Cell {
  id: string;
  name: string;
  backend: string;
  region: string;
  zone: string;
  status: "healthy" | "starting" | "degraded" | "maintenance";
  /**
   * The control plane's own word for what this cell is doing.
   *
   * Kept beside `status` rather than folded into it, because the four statuses
   * are presentation tones and two distinct states share one of them: a cell
   * being drained and a cell being released both read as `maintenance`, and an
   * operator deciding what to do about a machine needs to know which. Absent in
   * demo data, which has no plane to have said anything.
   */
  state?: string;
  /** The capacity class the cell was provisioned as; absent on older records. */
  capacityClass?: "reserved" | "on_demand" | "spot";
  utilization: number;
  activeWorkloads: number;
  /** The control plane does not report a per-cell queue depth; only demo data sets this. */
  queuedWorkloads?: number;
  topology: string;
  resources: Array<{
    name: string;
    used: number;
    total: number;
  }>;
  lastHeartbeat: string;
}

export interface CapacityLease {
  id: string;
  workloadId?: string;
  owner: string;
  cell: string;
  resource: string;
  amount: string;
  /**
   * The class of the cell this lease landed on, joined client-side.
   *
   * Narrowed to the server's real `CapacityClass`: the previous union invented
   * `borrowed` and `warm`, which no server enum has, so the value could never be
   * populated and the borrowed-capacity count was permanently zero.
   */
  class?: "reserved" | "on_demand" | "spot";
  state: "active" | "activating" | "revoking" | "expired";
  expiresAt: string;
  spendPerHour: number;
  resourceTotals?: Record<string, number>;
}

export interface Budget {
  id: string;
  owner: string;
  scope: "organization" | "project" | "user";
  spent: number;
  limit: number;
  forecast?: number;
  period: string;
  enforcement: "hard" | "soft";
}

export interface SchedulingPolicy {
  id: string;
  name: string;
  scope: string;
  type: "priority" | "fairness" | "preemption" | "placement";
  summary: string;
  /**
   * Whether this project's workloads may be preempted for higher-priority work.
   *
   * Named for what it controls. It was `enabled`, which read as "is this policy
   * in force" -- so a toggle labelled that way silently turned preemption off
   * for the project rather than disabling the policy.
   */
  preemptionEnabled: boolean;
  updatedAt: string;
  configuration?: {
    allowedProviders: string[];
    allowedRegions: string[];
    allowedCapacityClasses: string[];
    deniedAcceleratorTypes: string[];
    maxPriority: number;
  };
}

export interface AuditEvent {
  id: string;
  sequence?: number;
  timestamp: string;
  action: string;
  actor: string;
  target: string;
  category: "workload" | "capacity" | "policy" | "budget" | "system";
  severity: Severity;
  detail: string;
  requestId?: string;
}

export interface ControlPlaneHealth {
  status: string;
  service: string;
}

export interface SubmitWorkloadInput {
  name: string;
  project: string;
  priority: PriorityBand;
  capacityClass: "spot" | "on_demand";
  command: string;
  resources: ResourceRequest;
  estimatedDurationSeconds: number;
  preemptible: boolean;
  checkpointHook?: string;
  checkpointUri?: string;
  labels: Record<string, string>;
}

export interface DashboardData {
  health: ControlPlaneHealth;
  overview: OverviewData;
  workloads: WorkloadSummary[];
  cells: Cell[];
  leases: CapacityLease[];
  budgets: Budget[];
  policies: SchedulingPolicy[];
  auditEvents: AuditEvent[];
  access: {
    /** True only when the fleet-wide cell inventory answered successfully. */
    fleetInventory: boolean;
    /** True only when the fleet-wide audit ledger answered successfully. */
    fleetAudit: boolean;
  };
}

/** Per-cell placement detail behind a workload's flat scheduling reason. */
export interface CellRejection {
  cell_id: string;
  reason: string;
}

export interface WorkloadExplanation {
  workload_id: string;
  state: string;
  placeable: boolean;
  placement_cell_id: string | null;
  code: string | null;
  message: string | null;
  cells: CellRejection[];
  considered_cell_count: number;
  /**
   * Cells the autoscaler is building for this workload.
   *
   * Load-bearing for the panel: without it a workload waiting on capacity looks
   * identical to one that can never be placed, because both report that no cell
   * satisfies their constraints and only one is a problem.
   */
  provisioning_cell_ids: string[];
  scheduling_reason: string | null;
}

export interface WorkloadProcessStatus {
  remote_id?: string | null;
  control_group?: string | null;
  pids?: number[];
  [key: string]: unknown;
}

export interface PoolWarmth {
  pool_id: string;
  floor: number;
  window_active: boolean;
  warm_cells: number;
  warm_idle_cells: number;
  hourly_cost: number;
}

export interface WarmthReport {
  observed_at: string;
  pools: PoolWarmth[];
}

export interface PoolLaunchLatency {
  pool_id: string;
  accelerator_type?: string | null;
  /** Null when the pool has never provisioned successfully -- not zero. */
  p95_seconds: number | null;
  median_seconds: number | null;
  slowest_seconds: number | null;
  samples: number;
  cells_in_cooldown: number;
}

export interface LaunchLatencyReport {
  observed_at: string;
  pools: PoolLaunchLatency[];
}

export interface BillingReport {
  observed_at: string;
  accrued_cost: number;
  idle_cost: number;
  [key: string]: unknown;
}

/**
 * What one quota row is able to say about free capacity.
 *
 * `usage_unknown` is the reason this union exists rather than a nullable
 * number. Cloud Quotas reports a limit for a zone whether or not Cloud
 * Monitoring has a matching usage series, so a row rendered as zero usage
 * claims the whole quota is free -- the one wrong answer here that costs money
 * to act on.
 */
export type TpuQuotaState = "known" | "usage_unknown" | "unlimited";

export type TpuQuotaStatus = "ok" | "unavailable" | "unconfigured";

export interface TpuQuotaEntry {
  zone: string;
  region: string;
  generation: string;
  accelerator_type: string;
  capacity_class: "reserved" | "on_demand" | "spot";
  metric: string;
  /** Quota units one slice of this type consumes; the public accelerator suffix. */
  chips_per_slice: number;
  /** Null means the provider reports no ceiling -- never zero. */
  limit_chips: number | null;
  /** Null means nobody could measure it. Never render this as 0. */
  in_use_chips: number | null;
  free_chips: number | null;
  /** How many whole slices of this type the free balance fits. */
  free_slices: number | null;
  state: TpuQuotaState;
  usage_observed_at: string | null;
  /** How old the usage observation was when the snapshot was assembled. */
  usage_age_s: number | null;
}

export interface TpuQuotaFailure {
  capacity_class: "reserved" | "on_demand" | "spot";
  reason: string;
}

export interface TpuQuotaSnapshot {
  observed_at: string;
  provider: string;
  project: string | null;
  available_projects: string[];
  status: TpuQuotaStatus;
  /** When the provider was actually read; null when it never was. */
  refreshed_at: string | null;
  /** Distance between the provider read and this response. */
  age_s: number | null;
  cached: boolean;
  ttl_s: number;
  entries: TpuQuotaEntry[];
  failures: TpuQuotaFailure[];
}
