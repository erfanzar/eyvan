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
  queueTrend: Array<{ label: string; queued: number; admitted: number }>;
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
}
