// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, FilterX, Gauge, HelpCircle, MapPin, RefreshCw, Search } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import { EmptyState } from "../components/EmptyState";
import { MetricCard } from "../components/MetricCard";
import { StatusBadge } from "../components/StatusBadge";
import { formatAgeSeconds } from "../lib/format";
import type { TpuQuotaEntry, TpuQuotaSnapshot } from "../types";

interface CapacityPageProps {
  loadQuota: (signal: AbortSignal, options?: { refresh?: boolean }) => Promise<TpuQuotaSnapshot>;
}

type SortKey = "free" | "limit" | "zone";

/** The marker for a number nobody could measure. Never "0", never blank. */
const UNKNOWN = "unknown";

/**
 * A row carries a usable free balance only in the `known` state.
 *
 * `usage_unknown` and `unlimited` rows have no balance at all, and the single
 * most expensive thing this page could do is let one of them be compared,
 * sorted, or filtered as if it did.
 */
function isMeasured(entry: TpuQuotaEntry): boolean {
  return entry.state === "known" && entry.free_chips !== null;
}

function chips(value: number | null): string {
  return value === null ? UNKNOWN : value.toString();
}

function reasonForNoBalance(entry: TpuQuotaEntry): string {
  return entry.state === "unlimited"
    ? "no quota ceiling -- the provider reports no limit to be free of"
    : "usage unavailable -- the limit is known and how much of it is in use is not";
}

export function CapacityPage({ loadQuota }: CapacityPageProps) {
  const [snapshot, setSnapshot] = useState<TpuQuotaSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [zoneQuery, setZoneQuery] = useState("");
  const [capacityClass, setCapacityClass] = useState("all");
  const [generation, setGeneration] = useState("all");
  const [sortKey, setSortKey] = useState<SortKey>("free");
  const [fitsType, setFitsType] = useState("any");
  const [fitsCount, setFitsCount] = useState(1);

  const load = useCallback(
    (signal: AbortSignal, refresh = false) =>
      loadQuota(signal, { refresh })
        .then((value) => {
          if (!signal.aborted) {
            setSnapshot(value);
            setError(null);
          }
        })
        .catch((loadError: unknown) => {
          // Named, never swallowed. An empty quota table reads as "there is no
          // capacity anywhere", which is a statement about the fleet rather
          // than about this request having failed.
          if (!signal.aborted) {
            setError(loadError instanceof Error ? loadError.message : "Unable to read provider quota");
          }
        }),
    [loadQuota],
  );

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);

  const refresh = async () => {
    const controller = new AbortController();
    setRefreshing(true);
    try {
      await load(controller.signal, true);
    } finally {
      setRefreshing(false);
    }
  };

  const entries = useMemo(() => snapshot?.entries ?? [], [snapshot]);
  const generations = useMemo(
    () => Array.from(new Set(entries.map((entry) => entry.generation))).sort(),
    [entries],
  );
  const acceleratorTypes = useMemo(
    () => Array.from(new Set(entries.map((entry) => entry.accelerator_type))).sort(),
    [entries],
  );

  const selected = useMemo(() => {
    const query = zoneQuery.trim().toLowerCase();
    return entries.filter(
      (entry) =>
        (!query || entry.zone.toLowerCase().includes(query) || entry.region.toLowerCase().includes(query)) &&
        (capacityClass === "all" || entry.capacity_class === capacityClass) &&
        (generation === "all" || entry.generation === generation) &&
        (fitsType === "any" || entry.accelerator_type === fitsType),
    );
  }, [capacityClass, entries, fitsType, generation, zoneQuery]);

  const measured = useMemo(() => {
    const rows = selected.filter(
      (entry) => isMeasured(entry) && (fitsType === "any" || (entry.free_slices ?? 0) >= fitsCount),
    );
    return [...rows].sort((left, right) => {
      if (sortKey === "zone") return left.zone.localeCompare(right.zone) || left.accelerator_type.localeCompare(right.accelerator_type);
      if (sortKey === "limit") return (right.limit_chips ?? 0) - (left.limit_chips ?? 0);
      return (right.free_chips ?? 0) - (left.free_chips ?? 0);
    });
  }, [fitsCount, fitsType, selected, sortKey]);

  // Never merged into the sorted table and never dropped by the fit filter.
  // "Nobody could measure it" is a different answer from "it does not fit", and
  // only one of them should stop an operator trying.
  const unmeasured = useMemo(() => selected.filter((entry) => !isMeasured(entry)), [selected]);

  const filtered = Boolean(zoneQuery || capacityClass !== "all" || generation !== "all" || fitsType !== "any");
  const clearFilters = () => {
    setZoneQuery("");
    setCapacityClass("all");
    setGeneration("all");
    setFitsType("any");
    setFitsCount(1);
  };

  const freeChips = measured.reduce((total, entry) => total + (entry.free_chips ?? 0), 0);
  const measuredZones = new Set(measured.filter((entry) => (entry.free_chips ?? 0) > 0).map((entry) => entry.zone));
  const unmeasuredZones = new Set(unmeasured.map((entry) => entry.zone));
  const age = snapshot?.age_s ?? null;
  const stale = snapshot !== null && snapshot.cached && age !== null && age > snapshot.ttl_s;

  if (error) {
    return (
      <div className="page-stack">
        <section className="panel data-panel" aria-label="Provider quota">
          <div className="panel-header">
            <div>
              <h2>Provider quota</h2>
              <p>Where TPU quota is free right now</p>
            </div>
            <Gauge size={18} />
          </div>
          <div className="inline-error" role="alert">
            <AlertTriangle size={16} aria-hidden="true" />
            {error}
          </div>
          <div className="resource-row-detail">
            <span>No zone is shown as empty, because none of them were read.</span>
          </div>
        </section>
      </div>
    );
  }

  if (!snapshot) {
    return (
      <div className="page-stack">
        <section className="panel data-panel" aria-label="Provider quota">
          <div className="resource-row-detail">
            <span>Reading provider quota…</span>
          </div>
        </section>
      </div>
    );
  }

  return (
    <div className="page-stack">
      <section className="panel data-panel" aria-label="Provider quota source">
        <div className="panel-header">
          <div>
            <h2>Provider quota</h2>
            <p>
              {snapshot.project
                ? `${snapshot.provider} project ${snapshot.project}`
                : "no provider project is configured"}
            </p>
          </div>
          <button className="button button-ghost" type="button" disabled={refreshing} onClick={() => void refresh()}>
            <RefreshCw className={refreshing ? "spin" : undefined} size={16} aria-hidden="true" />
            {refreshing ? "Reading provider" : "Re-read provider"}
          </button>
        </div>
        {/* A cached number presented as a live one is the quietest way this page
            could lie, so the age of the reading is stated beside every use of
            it rather than being available on request. */}
        <div className="resource-row-detail">
          <span>
            {snapshot.refreshed_at === null
              ? "The provider has not been read"
              : `Provider read ${formatAgeSeconds(age)}${snapshot.cached ? " (cached reading, not re-read for this view)" : ""}`}
          </span>
          {stale && <span>Older than the {Math.round(snapshot.ttl_s)}s refresh window</span>}
          {snapshot.available_projects.length > 1 && (
            <span>Also declared by this fleet: {snapshot.available_projects.filter((name) => name !== snapshot.project).join(", ")}</span>
          )}
        </div>
        {snapshot.status === "unconfigured" && (
          <div className="resource-row-detail">
            <span>
              No GCP capacity pool declares a provider project, so there is nothing to read quota for. This is not an
              empty quota.
            </span>
          </div>
        )}
        {snapshot.failures.map((failure) => (
          <div className="inline-warning" role="status" key={failure.capacity_class}>
            <AlertTriangle size={16} aria-hidden="true" />
            <span>
              {failure.capacity_class === "spot" ? "Spot" : "On-demand"} quota could not be read, so no zone below
              reports it: {failure.reason}
            </span>
          </div>
        ))}
      </section>

      {snapshot.status !== "ok" && (
        <section className="panel data-panel" aria-label="No quota to report">
          <EmptyState
            title={
              snapshot.status === "unconfigured"
                ? "This fleet declares no provider project"
                : "No provider quota could be read"
            }
            detail={
              snapshot.status === "unconfigured"
                ? "Apply a GCP capacity pool, or read another project explicitly. Nothing here is a statement about capacity."
                : "No zone is counted, listed, or shown as empty, because none of them were read. The reasons are above."
            }
          />
        </section>
      )}

      {snapshot.status === "ok" && (
        <>
          <section className="metric-grid metric-grid-four" aria-label="Quota summary">
            <MetricCard
              label="Zones with free capacity"
              value={measuredZones.size.toString()}
              detail="measured balance above zero"
              icon={MapPin}
              tone={measuredZones.size ? "positive" : "warning"}
            />
            <MetricCard
              label="Free chips"
              value={freeChips.toString()}
              detail="quota units across measured rows only"
              icon={Gauge}
            />
            <MetricCard
              label="Zones with unknown usage"
              value={unmeasuredZones.size.toString()}
              detail="not empty and not full -- nobody could tell"
              icon={HelpCircle}
              tone={unmeasuredZones.size ? "warning" : "neutral"}
            />
            <MetricCard
              label="Rows read"
              value={entries.length.toString()}
              detail={`${snapshot.entries.filter((entry) => entry.capacity_class === "spot").length} on preemptible quota`}
              icon={Search}
            />
          </section>

          <section className="panel data-panel" aria-label="Free capacity by zone">
            <div className="table-toolbar">
              <div className="filter-group">
                <label className="search-field">
                  <Search size={16} aria-hidden="true" />
                  <span className="sr-only">Filter by zone or region</span>
                  <input
                    type="search"
                    placeholder="Zone or region"
                    value={zoneQuery}
                    onChange={(event) => setZoneQuery(event.target.value)}
                  />
                </label>
                <label className="select-field">
                  <span className="sr-only">Filter by purchase class</span>
                  <select value={capacityClass} onChange={(event) => setCapacityClass(event.target.value)}>
                    <option value="all">Spot and on-demand</option>
                    <option value="spot">Spot only</option>
                    <option value="on_demand">On-demand only</option>
                  </select>
                </label>
                <label className="select-field">
                  <span className="sr-only">Filter by generation</span>
                  <select value={generation} onChange={(event) => setGeneration(event.target.value)}>
                    <option value="all">All generations</option>
                    {generations.map((item) => (
                      <option value={item} key={item}>
                        {item}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="select-field">
                  <span className="sr-only">Only zones that fit this accelerator type</span>
                  <select value={fitsType} onChange={(event) => setFitsType(event.target.value)}>
                    <option value="any">Fits anything</option>
                    {acceleratorTypes.map((item) => (
                      <option value={item} key={item}>
                        Fits {item}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="select-field">
                  <span className="sr-only">How many of that accelerator type</span>
                  <input
                    type="number"
                    min={1}
                    value={fitsCount}
                    disabled={fitsType === "any"}
                    onChange={(event) => setFitsCount(Math.max(1, Number(event.target.value) || 1))}
                  />
                </label>
                <label className="select-field">
                  <span className="sr-only">Sort rows</span>
                  <select value={sortKey} onChange={(event) => setSortKey(event.target.value as SortKey)}>
                    <option value="free">Most free capacity</option>
                    <option value="limit">Largest quota</option>
                    <option value="zone">Zone</option>
                  </select>
                </label>
                {filtered && (
                  <button className="button button-ghost" type="button" onClick={clearFilters}>
                    <FilterX size={16} aria-hidden="true" />
                    Clear
                  </button>
                )}
              </div>
            </div>

            {measured.length === 0 ? (
              <EmptyState
                title={
                  fitsType === "any"
                    ? "No zone reports a measured free balance"
                    : `No measured zone fits ${fitsCount} x ${fitsType}`
                }
                detail={
                  unmeasured.length
                    ? `${unmeasuredZones.size} zone(s) could not be measured and are listed below. They are not proven full.`
                    : "Change the filters, or re-read the provider."
                }
              />
            ) : (
              <div className="table-scroll">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Zone</th>
                      <th>Type</th>
                      <th>Class</th>
                      <th>Limit</th>
                      <th>In use</th>
                      <th>Free</th>
                      <th>Fits</th>
                      <th>Usage read</th>
                    </tr>
                  </thead>
                  <tbody>
                    {measured.map((entry) => (
                      <tr key={`${entry.zone}:${entry.accelerator_type}:${entry.capacity_class}`}>
                        <td>
                          <span className="resource-cell">
                            <strong className="monospace">{entry.zone}</strong>
                            <small>{entry.region}</small>
                          </span>
                        </td>
                        <td>{entry.accelerator_type}</td>
                        <td>
                          <StatusBadge value={entry.capacity_class} tone={entry.capacity_class} dot={false} />
                        </td>
                        <td>{chips(entry.limit_chips)}</td>
                        <td>{chips(entry.in_use_chips)}</td>
                        <td>
                          <strong>{chips(entry.free_chips)}</strong>
                        </td>
                        <td>{entry.free_slices === null ? UNKNOWN : `${entry.free_slices} x ${entry.accelerator_type}`}</td>
                        {/* The balance was measured at a point in time by a gauge
                            that lags by minutes. Presenting it without that is
                            how a stale number becomes a placement decision. */}
                        <td>{formatAgeSeconds(entry.usage_age_s)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {unmeasured.length > 0 && (
            <section className="panel data-panel" aria-label="Zones with no measurable free balance">
              <div className="panel-header">
                <div>
                  <h2>Usage unknown</h2>
                  <p>Not empty and not full. These zones were never measured, so they carry no free balance.</p>
                </div>
                <HelpCircle size={18} />
              </div>
              <div className="table-scroll">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Zone</th>
                      <th>Type</th>
                      <th>Class</th>
                      <th>Limit</th>
                      <th>Free</th>
                      <th>Why</th>
                    </tr>
                  </thead>
                  <tbody>
                    {unmeasured.map((entry) => (
                      <tr key={`${entry.zone}:${entry.accelerator_type}:${entry.capacity_class}`}>
                        <td>
                          <span className="resource-cell">
                            <strong className="monospace">{entry.zone}</strong>
                            <small>{entry.region}</small>
                          </span>
                        </td>
                        <td>{entry.accelerator_type}</td>
                        <td>
                          <StatusBadge value={entry.capacity_class} tone={entry.capacity_class} dot={false} />
                        </td>
                        <td>{chips(entry.limit_chips)}</td>
                        <td>{UNKNOWN}</td>
                        <td>{reasonForNoBalance(entry)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}
