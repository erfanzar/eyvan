// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
  fleetAvailable: boolean;
}

export function OverviewPage({
  overview,
  workloads,
  onOpenWorkload,
  onShowWorkloads,
  fleetAvailable,
}: OverviewPageProps) {
  const activeWorkloads = workloads
    .filter((workload) => ["running", "provisioning", "checkpointing"].includes(workload.status))
    .slice(0, 5);
  const hasQueueActivity = overview.queueTrend.some((point) => point.submitted > 0 || point.admitted > 0);
  const maxQueueActivity = Math.max(
    ...overview.queueTrend.flatMap((point) => [point.submitted, point.admitted]),
    1,
  );
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
          value={fleetAvailable ? `${overview.fleetUtilization}%` : "Restricted"}
          detail={fleetAvailable ? "active leases in the configured project" : "fleet inventory requires fleet administration"}
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
              <h2>Submission and admission activity</h2>
              <p>Workload events in seven buckets over the last 24 hours</p>
            </div>
            <button className="text-button" type="button" onClick={onShowWorkloads}>
              View queue
              <ArrowRight size={15} aria-hidden="true" />
            </button>
          </div>
          {hasQueueActivity ? <div className="queue-chart" aria-label="Submission and admission activity chart">
            <div className="chart-legend">
              <span><i className="legend-box legend-queued" />Submitted</span>
              <span><i className="legend-box legend-admitted" />Admitted</span>
            </div>
            <div className="bar-chart">
              {overview.queueTrend.map((point) => (
                <div className="bar-group" key={point.label}>
                  <div className="bar-values">
                    <span
                      className="bar bar-queued"
                      style={{ height: `${point.submitted > 0 ? Math.max(8, (point.submitted / maxQueueActivity) * 100) : 0}%` }}
                      title={`${point.submitted} submitted at ${point.label}`}
                    />
                    <span
                      className="bar bar-admitted"
                      style={{ height: `${point.admitted > 0 ? Math.max(8, (point.admitted / maxQueueActivity) * 100) : 0}%` }}
                      title={`${point.admitted} admitted at ${point.label}`}
                    />
                  </div>
                  <span>{point.label}</span>
                </div>
              ))}
            </div>
          </div> : <EmptyState title="No submission or admission activity" detail="No workload submission or admission event occurred in this scope during the selected window." />}
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
          {fleetAvailable ? (
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
          ) : (
            <EmptyState
              title="Fleet inventory is restricted"
              detail="Workload queue and budget data remain scoped to your project."
            />
          )}
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
