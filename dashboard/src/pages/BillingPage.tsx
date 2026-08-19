// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Clock, Receipt, Users } from "lucide-react";
import { useEffect, useState } from "react";

import { MetricCard } from "../components/MetricCard";
import { EmptyState } from "../components/EmptyState";
import { LoadingState } from "../components/PageState";
import { formatSpend } from "../lib/format";
import type { BillingReport, ProjectUsage } from "../types";

interface BillingPageProps {
  loadBilling: (signal: AbortSignal) => Promise<BillingReport>;
  loadUsage: (signal: AbortSignal) => Promise<ProjectUsage>;
}

/** Hours rendered the way somebody reads a bill, not as raw seconds. */
function hours(seconds: number | undefined): string {
  if (!seconds) return "0h";
  const value = seconds / 3600;
  return value < 1 ? `${Math.round(seconds / 60)}m` : `${value.toFixed(1)}h`;
}

export function BillingPage({ loadBilling, loadUsage }: BillingPageProps) {
  const [billing, setBilling] = useState<BillingReport | null>(null);
  const [usage, setUsage] = useState<ProjectUsage | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([loadBilling(controller.signal), loadUsage(controller.signal)])
      .then(([bill, use]) => {
        setBilling(bill);
        setUsage(use);
      })
      .catch((failure) => {
        if (controller.signal.aborted) return;
        setError(failure instanceof Error ? failure.message : "Could not read billing");
      });
    return () => controller.abort();
  }, [loadBilling, loadUsage]);

  if (error) {
    return (
      <div className="page-stack">
        <div className="inline-error" role="alert">
          <AlertTriangle size={16} aria-hidden="true" />
          {error}
        </div>
      </div>
    );
  }
  if (!billing || !usage) {
    return (
      <div className="page-stack">
        <LoadingState label="Reading what the fleet has run" />
      </div>
    );
  }

  // The distinction this page exists to make. Eyvan measures hours whether or
  // not anybody declared a price, so a zero cost beside real hours means "no
  // price is declared", never "this was free" -- and a fleet running on a
  // research grant looks identical to one nobody has costed until it is said.
  const unpriced = billing.unpriced_cells > 0;
  const everyCellUnpriced = billing.unpriced_cells >= billing.cells && billing.cells > 0;

  return (
    <div className="page-stack">
      <section className="metric-grid" aria-label="What the fleet has run">
        <MetricCard
          icon={Clock}
          label="Cell time"
          value={hours(billing.billed_seconds)}
          detail={`across ${billing.cells} cell(s)`}
        />
        <MetricCard
          icon={Clock}
          label="Idle"
          value={hours(billing.idle_seconds)}
          detail="provisioned, holding no lease"
        />
        <MetricCard
          icon={Receipt}
          label="Accrued"
          value={everyCellUnpriced ? "Not costed" : formatSpend(billing.accrued_cost)}
          detail={everyCellUnpriced ? "no pool declares a unit price" : "at declared unit prices"}
        />
        <MetricCard
          icon={Users}
          label="People"
          value={String(usage.subjects.length)}
          detail="submitted work in this project"
        />
      </section>

      {unpriced && (
        <section className="panel data-panel" aria-label="Why cost is not reported">
          <div className="panel-header">
            <div>
              <h2>{everyCellUnpriced ? "Nothing here is costed" : "Some of this is not costed"}</h2>
              <p>
                {billing.unpriced_cells} of {billing.cells} cell(s) belong to a pool that declares no unit price
              </p>
            </div>
            <AlertTriangle size={18} aria-hidden="true" />
          </div>
          <div className="resource-row-detail">
            <span>
              The hours above are measured and real. The money is not: Eyvan multiplies usage by a price somebody
              declares, and with none declared every total is structurally zero — which reads exactly like a free
              fleet. Declare <code>unit_prices</code> on a pool to cost it, including <code>0</code> for capacity
              you hold under a grant, so this page can say <em>free</em> rather than <em>unknown</em>.
            </span>
          </div>
        </section>
      )}

      <section className="panel data-panel" aria-label="Usage by person">
        <div className="panel-header">
          <div>
            <h2>Who used what</h2>
            <p>Settled leases, attributed to whoever submitted the workload</p>
          </div>
          <Users size={18} aria-hidden="true" />
        </div>
        {usage.subjects.length === 0 ? (
          <EmptyState title="Nobody has submitted work yet" detail="Usage appears here once a workload settles." />
        ) : (
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Person</th>
                  <th>Runs</th>
                  <th>Accelerator hours</th>
                  <th>Running</th>
                  <th>Cost</th>
                </tr>
              </thead>
              <tbody>
                {usage.subjects.map((subject) => (
                  <tr key={subject.subject}>
                    <td>
                      <strong>{subject.subject}</strong>
                    </td>
                    <td>
                      <span className="resource-cell">
                        <strong>{subject.workloads}</strong>
                        {subject.settledWorkloads !== subject.workloads && (
                          <small>{subject.settledWorkloads} settled</small>
                        )}
                      </span>
                    </td>
                    <td>{subject.acceleratorHours.toFixed(2)}</td>
                    <td>{hours(subject.runningSeconds)}</td>
                    {/* Zero beside real hours is a missing price, not a free
                        run, and saying so here is the whole point of the page. */}
                    <td>
                      {subject.unpricedLeases > 0 ? (
                        <span className="resource-cell">
                          <strong>Not costed</strong>
                          <small>{subject.unpricedLeases} unpriced lease(s)</small>
                        </span>
                      ) : (
                        formatSpend(subject.settledCost)
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
