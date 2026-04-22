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
  Activity,
  AlertTriangle,
  ArrowRight,
  CircleCheck,
  CircleDollarSign,
  Clock3,
  Layers3,
  Server,
} from "lucide-react";

import { MetricCard } from "../components/MetricCard";
import { EmptyState } from "../components/EmptyState";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { formatMoney, formatRelativeTime, percent } from "../lib/format";
import type { OverviewData, WorkloadSummary } from "../types";

interface OverviewPageProps {
  overview: OverviewData;
  workloads: WorkloadSummary[];
  onOpenWorkload: (id: string) => void;
  onShowWorkloads: () => void;
}

export function OverviewPage({
  overview,
  workloads,
  onOpenWorkload,
  onShowWorkloads,
}: OverviewPageProps) {
  const activeWorkloads = workloads
    .filter((workload) => ["running", "provisioning", "checkpointing"].includes(workload.status))
    .slice(0, 5);
  const maxQueue = Math.max(...overview.queueTrend.map((point) => point.queued), 1);
  const budgetUsed = percent(overview.monthlySpend, overview.monthlyBudget);

  return (
    <div className="page-stack">
      <section className="metric-grid" aria-label="Operations metrics">
        <MetricCard
          label="Queued workloads"
          value={overview.queued.toString()}
          detail="production, interactive, and batch"
          icon={Clock3}
          tone="warning"
        />
        <MetricCard
          label="Running workloads"
          value={overview.running.toString()}
          detail="currently reported by the control plane"
          icon={Activity}
          tone="positive"
        />
        <MetricCard
          label="Scoped utilization"
          value={`${overview.fleetUtilization}%`}
          detail="active leases in the configured project"
          icon={Server}
        />
        <MetricCard
          label="Month spend"
          value={formatMoney(overview.monthlySpend, true)}
          detail={`${budgetUsed}% of ${formatMoney(overview.monthlyBudget, true)}`}
          icon={CircleDollarSign}
          tone="warning"
        />
        <MetricCard
          label="Completion rate"
          value={overview.successRate === undefined ? "Unavailable" : `${Math.round(overview.successRate)}%`}
          detail="terminal workloads in the current scope"
          icon={CircleCheck}
          tone="positive"
        />
      </section>

      <section className="overview-primary-grid">
        <div className="panel queue-panel">
          <div className="panel-header">
            <div>
              <h2>Queue pressure</h2>
              <p>Historical pending and admitted workload samples</p>
            </div>
            <button className="text-button" type="button" onClick={onShowWorkloads}>
              View queue
              <ArrowRight size={15} aria-hidden="true" />
            </button>
          </div>
          {overview.queueTrend.length ? <div className="queue-chart" aria-label="Queue pressure chart">
            <div className="chart-legend">
              <span><i className="legend-box legend-queued" />Queued</span>
              <span><i className="legend-box legend-admitted" />Admitted</span>
            </div>
            <div className="bar-chart">
              {overview.queueTrend.map((point) => (
                <div className="bar-group" key={point.label}>
                  <div className="bar-values">
                    <span
                      className="bar bar-queued"
                      style={{ height: `${Math.max(8, (point.queued / maxQueue) * 100)}%` }}
                      title={`${point.queued} queued at ${point.label}`}
                    />
                    <span
                      className="bar bar-admitted"
                      style={{ height: `${Math.max(8, (point.admitted / maxQueue) * 100)}%` }}
                      title={`${point.admitted} admitted at ${point.label}`}
                    />
                  </div>
                  <span>{point.label}</span>
                </div>
              ))}
            </div>
          </div> : <EmptyState title="No queue history available" detail="The control plane did not return historical queue samples for this scope." />}
          <div className="priority-strip">
            {overview.priorityQueue.map((item) => (
              <div className="priority-summary" key={item.priority}>
                <StatusBadge value={item.priority} tone={item.priority} dot={false} />
                <strong>{item.count}</strong>
                <span>oldest {item.oldestMinutes}m</span>
              </div>
            ))}
          </div>
        </div>

        <div className="panel fleet-panel">
          <div className="panel-header">
            <div>
              <h2>Fleet capacity</h2>
              <p>Committed resources and pending demand</p>
            </div>
            <Layers3 size={18} aria-hidden="true" />
          </div>
          <div className="resource-list">
            {overview.fleetBreakdown.map((resource) => {
              const utilization = percent(resource.used, resource.total);
              return (
                <div className="resource-row" key={resource.resource}>
                  <div className="resource-row-heading">
                    <span>{resource.resource}</span>
                    <strong>{resource.used.toLocaleString()} / {resource.total.toLocaleString()}</strong>
                  </div>
                  <ProgressBar
                    value={utilization}
                    label={`${resource.resource} utilization`}
                    tone={utilization > 85 ? "amber" : "green"}
                  />
                  <div className="resource-row-detail">
                    <span>{utilization}% allocated</span>
                    <span>{resource.pending.toLocaleString()} pending</span>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="budget-inline">
            <div>
              <span>Monthly budget</span>
              <strong>{formatMoney(overview.monthlySpend)} / {formatMoney(overview.monthlyBudget)}</strong>
            </div>
            <ProgressBar value={budgetUsed} label="Monthly budget utilization" tone={budgetUsed > 85 ? "red" : "blue"} />
          </div>
        </div>
      </section>

      <section className="overview-secondary-grid">
        <div className="panel active-workloads-panel">
          <div className="panel-header">
            <div>
              <h2>Active workloads</h2>
              <p>Recently scheduled and currently progressing</p>
            </div>
          </div>
          <div className="compact-workload-list">
            {activeWorkloads.map((workload) => (
              <button
                className="compact-workload"
                type="button"
                key={workload.id}
                onClick={() => onOpenWorkload(workload.id)}
              >
                <span className="workload-primary">
                  <strong>{workload.name}</strong>
                  <small>{workload.project} · {workload.cell ?? "placement pending"}</small>
                </span>
                <StatusBadge value={workload.status} tone={workload.status} />
                <span className="mini-progress">
                  {workload.progress === undefined ? (
                    <small className="muted">—</small>
                  ) : (
                    <>
                      <ProgressBar value={workload.progress} label={`${workload.name} progress`} />
                      <small>{workload.progress}%</small>
                    </>
                  )}
                </span>
                <ArrowRight size={16} aria-hidden="true" />
              </button>
            ))}
          </div>
        </div>

        <div className="panel alert-panel">
          <div className="panel-header">
            <div>
              <h2>Needs attention</h2>
              <p>Capacity and policy conditions</p>
            </div>
            <AlertTriangle size={18} aria-hidden="true" />
          </div>
          <div className="alert-list">
            {overview.alerts.map((alert) => (
              <div className={`alert-item alert-${alert.severity}`} key={alert.id}>
                <span className="alert-icon" aria-hidden="true">
                  <AlertTriangle size={16} />
                </span>
                <span>
                  <strong>{alert.title}</strong>
                  <p>{alert.detail}</p>
                  <small>{formatRelativeTime(alert.timestamp)}</small>
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
