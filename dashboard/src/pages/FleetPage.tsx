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
  Timer,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { apiClient } from "../api/client";
import { MetricCard } from "../components/MetricCard";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { formatDateTime, formatMoney, percent } from "../lib/format";
import type { BillingReport, CapacityLease, Cell, LaunchLatencyReport, WarmthReport } from "../types";

interface FleetPageProps {
  cells: Cell[];
  leases: CapacityLease[];
  onRefresh: () => Promise<void>;
}

export function FleetPage({ cells, leases, onRefresh }: FleetPageProps) {
  const [tab, setTab] = useState<"cells" | "leases">("cells");
  const [region, setRegion] = useState("all");
  const [refreshing, setRefreshing] = useState(false);
  const [warmth, setWarmth] = useState<WarmthReport | null>(null);
  const [latency, setLatency] = useState<LaunchLatencyReport | null>(null);
  const [latencyError, setLatencyError] = useState<string | null>(null);
  const [billing, setBilling] = useState<BillingReport | null>(null);
  const [costError, setCostError] = useState<string | null>(null);
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

  useEffect(() => {
    let cancelled = false;
    Promise.all([apiClient.getWarmth(), apiClient.getBilling()])
      .then(([warmthReport, billingReport]) => {
        if (cancelled) return;
        setWarmth(warmthReport);
        setBilling(billingReport);
      })
      .catch((error: unknown) => {
        // Named, not swallowed. A cost panel that silently shows nothing reads
        // as "nothing is being spent", which is the most expensive possible
        // thing for it to say incorrectly.
        if (!cancelled) setCostError(error instanceof Error ? error.message : "Unable to read fleet cost");
      });
    apiClient
      .getLaunchLatency()
      .then((report) => {
        if (!cancelled) setLatency(report);
      })
      .catch((error: unknown) => {
        // Named for the same reason the cost error is: an empty latency panel
        // reads as "every pool is instant", which is the most misleading thing
        // it could say.
        if (!cancelled) setLatencyError(error instanceof Error ? error.message : "Unable to read launch latency");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const warmHourlyCost = (warmth?.pools ?? []).reduce((total, pool) => total + pool.hourly_cost, 0);

  return (
    <div className="page-stack">
      <section className="metric-grid metric-grid-four" aria-label="Fleet metrics">
        <MetricCard label="Cells online" value={`${healthyCount}/${cells.length}`} detail={`${cells.length - healthyCount} not ready`} icon={Server} tone={healthyCount === cells.length ? "positive" : "warning"} />
        <MetricCard label="Mean scoped utilization" value={`${averageUtilization}%`} detail="active leases in the configured project" icon={Gauge} />
        <MetricCard label="Active leases" value={activeLeases.length.toString()} detail={`${leases.filter((lease) => lease.class === "spot").length} on preemptible capacity`} icon={KeyRound} />
        <MetricCard label="Lease spend" value={`${formatMoney(totalSpend)}/h`} detail="active and activating leases" icon={Cloud} tone="positive" />
      </section>

      {/* Warmth and billing are reported together on purpose. Warm capacity is
          deliberately idle, and an operator reading an idle figure without the
          warm floor beside it has exactly one safe reaction -- delete the warm
          floor, undoing the fix that put it there. */}
      <section className="panel data-panel" aria-label="Warm capacity and cost">
        <div className="panel-header">
          <div><h2>Warm capacity and cost</h2><p>Which idle cost you asked for, and which you did not</p></div>
          <Gauge size={18} />
        </div>
        {costError && <div className="resource-row-detail"><span>{costError}</span></div>}
        {!costError && !billing && <div className="resource-row-detail"><span>Reading the billing clock…</span></div>}
        {billing && (
          <div className="resource-row-detail">
            <span>{formatMoney(billing.accrued_cost)} accrued</span>
            <span>{formatMoney(billing.idle_cost)} idle</span>
            <span>{formatMoney(warmHourlyCost)}/h held warm</span>
          </div>
        )}
        {warmth && warmth.pools.length > 0 && (
          <dl className="definition-grid">
            {warmth.pools.map((pool) => (
              <div key={pool.pool_id}>
                <dt className="monospace">{pool.pool_id}</dt>
                <dd>
                  {pool.warm_cells}/{pool.floor} held{pool.window_active ? ", window active" : ""}
                  {pool.warm_idle_cells ? `, ${pool.warm_idle_cells} idle` : ""}
                </dd>
              </div>
            ))}
          </dl>
        )}
        {warmth && warmth.pools.length === 0 && (
          <div className="resource-row-detail"><span>No pool declares a warm floor.</span></div>
        )}
      </section>

      {/* The number the scheduler has ranked pools on since it existed, shown to
          the person choosing a pool. Sample counts are rendered beside every
          figure because a p95 over three provisions is not a p95, and a pool
          with no samples sorts to the top of any latency table as if it were
          the fastest thing available. */}
      <section className="panel data-panel" aria-label="Launch latency">
        <div className="panel-header">
          <div><h2>Launch latency</h2><p>How long each pool takes to hand over usable capacity</p></div>
          <Timer size={18} />
        </div>
        {latencyError && <div className="resource-row-detail"><span>{latencyError}</span></div>}
        {!latencyError && !latency && <div className="resource-row-detail"><span>Reading provisioning history…</span></div>}
        {latency && latency.pools.length === 0 && (
          <div className="resource-row-detail"><span>No capacity pool is configured.</span></div>
        )}
        {latency && latency.pools.length > 0 && (
          <dl className="definition-grid">
            {latency.pools.map((pool) => (
              <div key={pool.pool_id}>
                <dt className="monospace">{pool.pool_id}</dt>
                <dd>
                  {pool.samples === 0
                    ? "never provisioned - no measurement"
                    : `p95 ${Math.round(pool.p95_seconds ?? 0)}s over ${pool.samples} provision${pool.samples === 1 ? "" : "s"}`}
                  {pool.cells_in_cooldown > 0 ? `, ${pool.cells_in_cooldown} in cooldown` : ""}
                </dd>
              </div>
            ))}
          </dl>
        )}
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
