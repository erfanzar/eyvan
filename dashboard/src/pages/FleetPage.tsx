// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import {
  Cloud,
  AlertTriangle,
  Cpu,
  Gauge,
  KeyRound,
  MapPin,
  Network,
  RefreshCw,
  Server,
} from "lucide-react";
import { useMemo, useState } from "react";

import { MetricCard } from "../components/MetricCard";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { formatDateTime, formatMoney, percent } from "../lib/format";
import type { CapacityLease, Cell } from "../types";

interface FleetPageProps {
  cells: Cell[];
  leases: CapacityLease[];
  onRefresh: () => Promise<void>;
}

export function FleetPage({ cells, leases, onRefresh }: FleetPageProps) {
  const [tab, setTab] = useState<"cells" | "leases">("cells");
  const [region, setRegion] = useState("all");
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [refreshed, setRefreshed] = useState(false);
  const regions = useMemo(() => Array.from(new Set(cells.map((cell) => cell.region))), [cells]);
  const visibleCells = region === "all" ? cells : cells.filter((cell) => cell.region === region);
  const activeLeases = leases.filter((lease) => ["active", "activating"].includes(lease.state));
  const totalSpend = activeLeases.reduce((sum, lease) => sum + lease.spendPerHour, 0);
  const healthyCount = cells.filter((cell) => cell.status === "healthy").length;
  const averageUtilization = Math.round(cells.reduce((sum, cell) => sum + cell.utilization, 0) / Math.max(cells.length, 1));

  const refreshInventory = async () => {
    setRefreshing(true);
    setRefreshError(null);
    setRefreshed(false);
    try {
      await onRefresh();
      setRefreshed(true);
    } catch (error) {
      setRefreshError(error instanceof Error ? error.message : "Unable to refresh fleet inventory");
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div className="page-stack">
      <section className="metric-grid metric-grid-four" aria-label="Fleet metrics">
        <MetricCard label="Cells online" value={`${healthyCount}/${cells.length}`} detail={`${cells.length - healthyCount} not ready`} icon={Server} tone={healthyCount === cells.length ? "positive" : "warning"} />
        <MetricCard label="Mean scoped utilization" value={`${averageUtilization}%`} detail="active leases in the configured project" icon={Gauge} />
        <MetricCard label="Active leases" value={activeLeases.length.toString()} detail={`${leases.filter((lease) => lease.class === "spot").length} on preemptible capacity`} icon={KeyRound} />
        <MetricCard label="Lease spend" value={`${formatMoney(totalSpend)}/h`} detail="active and activating leases" icon={Cloud} tone="positive" />
      </section>

      <section className="panel topology-panel">
        <div className="panel-header">
          <div><h2>Fleet topology</h2><p>Scheduling cells grouped by failure region</p></div>
          <button className="button button-ghost" type="button" disabled={refreshing} onClick={() => void refreshInventory()}>
            <RefreshCw className={refreshing ? "spin" : undefined} size={16} aria-hidden="true" />
            {refreshing ? "Refreshing" : "Refresh inventory"}
          </button>
        </div>
        {refreshError && <div className="inline-error" role="alert"><AlertTriangle size={16} />{refreshError}</div>}
        {refreshed && <div className="inline-status" role="status">Fleet inventory reloaded from Eyvan.</div>}
        <div className="topology-regions">
          {regions.map((regionName) => {
            const regionCells = cells.filter((cell) => cell.region === regionName);
            return (
              <div className="topology-region" key={regionName}>
                <div className="topology-region-label"><MapPin size={15} aria-hidden="true" /><strong>{regionName}</strong><span>{regionCells.length} cells</span></div>
                <div className="topology-cell-row">
                  {regionCells.map((cell) => (
                    <button className="topology-node" type="button" key={cell.id} onClick={() => { setRegion(regionName); setTab("cells"); }}>
                      <span className={`topology-node-state topology-${cell.status}`} aria-hidden="true" />
                      <span><strong>{cell.name}</strong><small>{cell.backend} · {cell.utilization}%</small></span>
                    </button>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </section>

      <div className="tabs fleet-tabs" role="tablist" aria-label="Fleet views">
        <button type="button" role="tab" aria-selected={tab === "cells"} className={tab === "cells" ? "tab-active" : ""} onClick={() => setTab("cells")}>Cells ({cells.length})</button>
        <button type="button" role="tab" aria-selected={tab === "leases"} className={tab === "leases" ? "tab-active" : ""} onClick={() => setTab("leases")}>Capacity leases ({leases.length})</button>
      </div>

      {tab === "cells" && (
        <section className="fleet-cells-section">
          <div className="section-toolbar">
            <div><h2>Scheduling cells</h2><p>Backend inventory, topology, and committed resources</p></div>
            <label className="select-field">
              <span className="sr-only">Filter by region</span>
              <select value={region} onChange={(event) => setRegion(event.target.value)}>
                <option value="all">All regions</option>
                {regions.map((item) => <option value={item} key={item}>{item}</option>)}
              </select>
            </label>
          </div>
          <div className="cell-grid">
            {visibleCells.map((cell) => (
              <article className="cell-card" key={cell.id}>
                <div className="cell-card-heading">
                  <div className="cell-icon"><Server size={18} aria-hidden="true" /></div>
                  <div><h3>{cell.name}</h3><p>{cell.backend} · {cell.zone}</p></div>
                  <StatusBadge value={cell.status} tone={cell.status} />
                </div>
                <div className="cell-stat-row">
                  <span><Network size={15} /><small>Topology</small><strong>{cell.topology}</strong></span>
                  <span><Cpu size={15} /><small>Workloads</small><strong>{cell.activeWorkloads} active</strong></span>
                </div>
                <div className="cell-resource-list">
                  {cell.resources.map((resource) => {
                    const utilization = percent(resource.used, resource.total);
                    return (
                      <div key={resource.name}>
                        <div className="resource-row-heading"><span>{resource.name}</span><strong>{resource.used} / {resource.total}</strong></div>
                        <ProgressBar value={utilization} label={`${cell.name} ${resource.name} utilization`} tone={utilization > 85 ? "amber" : "green"} />
                      </div>
                    );
                  })}
                </div>
                <div className="cell-card-footer"><span>Scoped utilization <strong>{cell.utilization}%</strong></span><span>Heartbeat {formatDateTime(cell.lastHeartbeat)}</span></div>
              </article>
            ))}
          </div>
        </section>
      )}

      {tab === "leases" && (
        <section className="panel data-panel">
          <div className="panel-header"><div><h2>Capacity leases</h2><p>Fenced workload and warm-pool allocations</p></div></div>
          <div className="table-scroll">
            <table className="data-table">
              <thead><tr><th>Lease</th><th>Owner</th><th>Cell</th><th>Capacity</th><th>Class</th><th>State</th><th>Spend</th><th>Expires</th></tr></thead>
              <tbody>
                {leases.map((lease) => (
                  <tr key={lease.id}>
                    <td><span className="resource-cell"><strong className="monospace">{lease.id}</strong><small>{lease.workloadId ?? "fleet warm pool"}</small></span></td>
                    <td>{lease.owner}</td>
                    <td>{lease.cell}</td>
                    <td><span className="resource-cell"><strong>{lease.resource}</strong><small>{lease.amount}</small></span></td>
                    <td>{lease.class ? <StatusBadge value={lease.class} tone={lease.class} dot={false} /> : <span className="muted">—</span>}</td>
                    <td><StatusBadge value={lease.state} tone={lease.state} /></td>
                    <td>{formatMoney(lease.spendPerHour)}/h</td>
                    <td>{formatDateTime(lease.expiresAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
