// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { FilterX, Plus, Search } from "lucide-react";
import { useMemo, useState } from "react";

import { EmptyState } from "../components/EmptyState";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { formatAcceleratorRequest, formatDateTime, formatMoney, percent } from "../lib/format";
import type { WorkloadSummary } from "../types";

interface WorkloadsPageProps {
  workloads: WorkloadSummary[];
  onOpenWorkload: (id: string) => void;
  onSubmit: () => void;
  canSubmit: boolean;
}

export function WorkloadsPage({ workloads, onOpenWorkload, onSubmit, canSubmit }: WorkloadsPageProps) {
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("all");
  const [priority, setPriority] = useState("all");

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    return workloads.filter(
      (workload) =>
        (!query ||
          workload.name.toLowerCase().includes(query) ||
          workload.id.toLowerCase().includes(query) ||
          workload.user.toLowerCase().includes(query) ||
          workload.project.toLowerCase().includes(query)) &&
        (status === "all" || workload.status === status) &&
        (priority === "all" || workload.priority === priority),
    );
  }, [priority, search, status, workloads]);

  const clearFilters = () => {
    setSearch("");
    setStatus("all");
    setPriority("all");
  };

  return (
    <div className="page-stack">
      <section className="workload-summary-strip" aria-label="Workload status summary">
        <div><span>All</span><strong>{workloads.length}</strong></div>
        <div><span>Queued</span><strong>{workloads.filter((w) => w.status === "queued").length}</strong></div>
        <div><span>In progress</span><strong>{workloads.filter((w) => ["running", "planning", "provisioning", "checkpointing"].includes(w.status)).length}</strong></div>
        <div><span>Failed</span><strong>{workloads.filter((w) => w.status === "failed").length}</strong></div>
        <div><span>Cancelled</span><strong>{workloads.filter((w) => w.status === "cancelled").length}</strong></div>
      </section>

      <section className="panel data-panel">
        <div className="table-toolbar">
          <div className="filter-group">
            <label className="search-field">
              <Search size={16} aria-hidden="true" />
              <span className="sr-only">Search workloads</span>
              <input
                type="search"
                placeholder="Search name, ID, project, or user"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </label>
            <label className="select-field">
              <span className="sr-only">Filter by status</span>
              <select value={status} onChange={(event) => setStatus(event.target.value)}>
                <option value="all">All statuses</option>
                <option value="queued">Queued</option>
                <option value="planning">Planning</option>
                <option value="provisioning">Provisioning</option>
                <option value="running">Running</option>
                <option value="checkpointing">Checkpointing</option>
                <option value="preempting">Preempting</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed</option>
                <option value="cancelled">Cancelled</option>
              </select>
            </label>
            <label className="select-field">
              <span className="sr-only">Filter by priority</span>
              <select value={priority} onChange={(event) => setPriority(event.target.value)}>
                <option value="all">All priorities</option>
                <option value="production">Production</option>
                <option value="interactive">Interactive</option>
                <option value="batch">Batch</option>
              </select>
            </label>
            {(search || status !== "all" || priority !== "all") && (
              <button className="button button-ghost" type="button" onClick={clearFilters}>
                <FilterX size={16} aria-hidden="true" />
                Clear
              </button>
            )}
          </div>
          {canSubmit && (
            <button className="button button-secondary" type="button" onClick={onSubmit}>
              <Plus size={16} aria-hidden="true" />
              New workload
            </button>
          )}
        </div>

        {filtered.length === 0 ? (
          <EmptyState
            title="No workloads match these filters"
            detail="Change the search or clear the current filters."
          />
        ) : (
          <div className="table-scroll">
            <table className="data-table workloads-table">
              <thead>
                <tr>
                  <th>Workload</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Resources</th>
                  <th>Cell</th>
                  <th>Progress</th>
                  <th>Budget</th>
                  <th>Submitted</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((workload) => {
                  const budgetTracked = workload.budgetSpent !== undefined && workload.budgetLimit !== undefined;
                  const budgetPercent = budgetTracked ? percent(workload.budgetSpent!, workload.budgetLimit!) : 0;
                  return (
                    // The row opens the workload, not just the name in it. Rows
                    // already lit up on hover, so the whole width read as
                    // clickable and only 250px of it was -- a click anywhere
                    // else did nothing at all. The button stays for keyboard
                    // and screen-reader users, and stops the event so one click
                    // is one navigation.
                    <tr
                      key={workload.id}
                      className="row-clickable"
                      onClick={() => onOpenWorkload(workload.id)}
                    >
                      <td>
                        <button
                          className="table-primary-link"
                          type="button"
                          onClick={(event) => {
                            event.stopPropagation();
                            onOpenWorkload(workload.id);
                          }}
                        >
                          <strong>{workload.name}</strong>
                          <span>{workload.id} · {workload.project} · {workload.user}</span>
                        </button>
                      </td>
                      <td>
                        <StatusBadge value={workload.status} tone={workload.status} />
                        {workload.queueReason && <small className="cell-note">{workload.queueReason}</small>}
                      </td>
                      <td><StatusBadge value={workload.priority} tone={workload.priority} dot={false} /></td>
                      <td>
                        <span className="resource-cell">
                          <strong>
                            {workload.resources.accelerator
                              ? formatAcceleratorRequest(
                                  workload.resources.accelerator,
                                  workload.resources.acceleratorCount,
                                  workload.resources.replicas,
                                )
                              : `${workload.resources.cpu} CPU`}
                          </strong>
                          <small>
                            {workload.resources.replicas} replicas
                            {/* A TPU pool declares no memory, so the record carries 0. Printing
                                "0 GB" asserts the workload has none, which is false -- the pool
                                simply does not track it. Omit what is unknown. */}
                            {workload.resources.memoryGb > 0 ? ` · ${workload.resources.memoryGb} GB` : ""}
                          </small>
                        </span>
                      </td>
                      <td>{workload.cell ?? <span className="muted">Unassigned</span>}</td>
                      <td>
                        {workload.progress === undefined ? (
                          <span className="muted">—</span>
                        ) : (
                          <span className="table-progress">
                            <ProgressBar value={workload.progress} label={`${workload.name} progress`} />
                            <small>{workload.progress}%</small>
                          </span>
                        )}
                      </td>
                      <td>
                        {budgetTracked ? (
                          <span className="budget-cell">
                            <strong>{formatMoney(workload.budgetSpent!)}</strong>
                            <ProgressBar
                              value={budgetPercent}
                              label={`${workload.name} budget utilization`}
                              tone={budgetPercent > 90 ? "red" : "blue"}
                            />
                            <small>of {formatMoney(workload.budgetLimit!)}</small>
                          </span>
                        ) : (
                          <span className="muted">Not tracked</span>
                        )}
                      </td>
                      <td><span className="nowrap">{formatDateTime(workload.submittedAt)}</span></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <div className="table-footer">
          <span>Showing {filtered.length} of {workloads.length} workloads</span>
          <span>State refreshed from durable scheduler history</span>
        </div>
      </section>
    </div>
  );
}
