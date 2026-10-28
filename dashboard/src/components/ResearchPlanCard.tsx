// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { FlaskConical } from "lucide-react";
import { useEffect, useState } from "react";

import { formatChipHours, formatDurationSeconds } from "../lib/format";
import { routeHash } from "../routing";
import type { ResearchBudget, ResearchPlanState, ResearchPlanSummary } from "../types";

/*
 * A research plan is approved as a whole, and only by the person who proposed
 * it; the plane then submits its jobs as that person. It is never a workload
 * draft, so nothing here can reach the workload submission path: the only
 * mutations are approve and stop.
 */

/** States in which the plane is (or may soon be) running the plan's jobs. */
export const ACTIVE_PLAN_STATES: ReadonlySet<ResearchPlanState> = new Set(["approved", "running", "stopping"]);
/** States a stop request is accepted in by this UI (the plane also accepts `proposed`, as a withdrawal). */
const STOPPABLE_STATES: ReadonlySet<ResearchPlanState> = new Set(["approved", "running", "stopping"]);

const STATE_TONE: Record<ResearchPlanState, string> = {
  proposed: "planning",
  approved: "activating",
  running: "running",
  stopping: "draining",
  finished: "completed",
  stopped: "cancelled",
  failed: "failed",
  over_budget: "expired",
};

const STATE_LABEL: Record<ResearchPlanState, string> = {
  proposed: "proposed",
  approved: "approved",
  running: "running",
  stopping: "stopping",
  finished: "finished",
  stopped: "stopped",
  failed: "failed",
  over_budget: "over budget",
};

export const planStateLabel = (state: string): string => STATE_LABEL[state as ResearchPlanState] ?? state;

export function ResearchStateBadge({ state }: { state: string }) {
  const tone = STATE_TONE[state as ResearchPlanState] ?? "unknown";
  return (
    <span className={`status-badge status-${tone}`} data-testid="research-state">
      <span className="status-dot" aria-hidden="true" />
      {planStateLabel(state)}
    </span>
  );
}

const plural = (count: number, word: string) => `${count} ${word}${count === 1 ? "" : "s"}`;

/** "2 jobs · 1 chip-hour · 10 min · 3 rounds": every limit the plane enforces, in one line. */
export function budgetSummary(budget: ResearchBudget): string {
  return [
    plural(budget.max_jobs, "job"),
    formatChipHours(budget.max_chip_hours),
    formatDurationSeconds(budget.max_wall_seconds),
    budget.max_rounds === null || budget.max_rounds === undefined
      ? "default round limit"
      : plural(budget.max_rounds, "round"),
  ].join(" · ");
}

type ActionStatus = "idle" | "confirm-approve" | "approving" | "confirm-stop" | "stopping";

const message = (error: unknown, fallback: string) => (error instanceof Error ? error.message : fallback);

export interface PlanActionsProps<T extends ResearchPlanSummary> {
  plan: T;
  /** Only the plan's owner may approve it; the plane refuses anyone else. */
  canApprove: boolean;
  /** The owner or a fleet admin may stop it. */
  canStop: boolean;
  approve: (planId: string) => Promise<T>;
  stop: (planId: string) => Promise<T>;
  /** The plan as the plane answered after an action. */
  onUpdated: (plan: T) => void;
}

/** Approve and Stop, each behind an explicit confirmation, with its pending and failure states. */
export function PlanActions<T extends ResearchPlanSummary>({
  plan,
  canApprove,
  canStop,
  approve,
  stop,
  onUpdated,
}: PlanActionsProps<T>) {
  const [status, setStatus] = useState<ActionStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const where = `${plan.org_id}/${plan.project_id}`;
  const approvable = canApprove && plan.state === "proposed";
  const stoppable = canStop && STOPPABLE_STATES.has(plan.state);

  const run = async (kind: "approving" | "stopping") => {
    setStatus(kind);
    setError(null);
    try {
      const updated = await (kind === "approving" ? approve(plan.id) : stop(plan.id));
      onUpdated(updated);
      setStatus("idle");
    } catch (failure) {
      setStatus("idle");
      setError(message(failure, kind === "approving" ? "The plan was not approved" : "The plan was not stopped"));
    }
  };

  return (
    <>
      {plan.state === "proposed" && (
        <p className="research-gate">
          Nothing runs until you approve.
          {!canApprove && " Only the person who proposed this plan can approve it."}
        </p>
      )}
      {error && (
        <p className="dock-card-error" role="alert">
          {error}
        </p>
      )}
      <div className="dock-card-actions">
        {status === "idle" && (
          <>
            {approvable && (
              <button
                className="button button-primary"
                type="button"
                onClick={() => {
                  setError(null);
                  setStatus("confirm-approve");
                }}
              >
                Approve
              </button>
            )}
            {stoppable && (
              <button
                className="button button-secondary"
                type="button"
                onClick={() => {
                  setError(null);
                  setStatus("confirm-stop");
                }}
              >
                Stop
              </button>
            )}
          </>
        )}
        {status === "confirm-approve" && (
          <>
            <span className="dock-card-confirm">
              Approve and start it? The plane submits up to {plural(plan.budget.max_jobs, "job")} in {where} under your
              identity, within this budget.
            </span>
            <button className="button button-primary" type="button" onClick={() => void run("approving")}>
              Confirm approve
            </button>
            <button className="button button-secondary" type="button" onClick={() => setStatus("idle")}>
              Cancel
            </button>
          </>
        )}
        {status === "confirm-stop" && (
          <>
            <span className="dock-card-confirm">Stop this plan? Its running workloads are cancelled now.</span>
            <button className="button button-danger" type="button" onClick={() => void run("stopping")}>
              Confirm stop
            </button>
            <button className="button button-secondary" type="button" onClick={() => setStatus("idle")}>
              Cancel
            </button>
          </>
        )}
        {status === "approving" && (
          <>
            <span role="status">Approving…</span>
            <button className="button button-primary" type="button" disabled>
              Confirm approve
            </button>
          </>
        )}
        {status === "stopping" && (
          <>
            <span role="status">Stopping…</span>
            <button className="button button-danger" type="button" disabled>
              Confirm stop
            </button>
          </>
        )}
      </div>
    </>
  );
}

export interface ResearchPlanCardProps {
  plan: ResearchPlanSummary;
  canApprove: boolean;
  canStop: boolean;
  approve: (planId: string) => Promise<ResearchPlanSummary>;
  stop: (planId: string) => Promise<ResearchPlanSummary>;
  /** A fresher reading of the plan arrived (an action's answer, or a refresh). */
  onUpdated?: (plan: ResearchPlanSummary) => void;
  /**
   * Read the plan's current state from the plane. A card reopened from a saved
   * chat otherwise shows the plan as it was when proposed, offering Approve on
   * a plan that already ran.
   */
  refresh?: (planId: string, signal: AbortSignal) => Promise<ResearchPlanSummary>;
}

/** The assistant dock's card for a proposed research plan. */
export function ResearchPlanCard({
  plan,
  canApprove,
  canStop,
  approve,
  stop,
  onUpdated,
  refresh,
}: ResearchPlanCardProps) {
  const [current, setCurrent] = useState(plan);
  useEffect(() => setCurrent(plan), [plan]);
  const planId = plan.id;
  useEffect(() => {
    if (!refresh) return;
    const controller = new AbortController();
    refresh(planId, controller.signal).then(
      (fresh) => {
        if (!controller.signal.aborted) setCurrent(fresh);
      },
      () => {
        // Keep the saved reading; an action still asks the plane, which refuses a stale one.
      },
    );
    return () => controller.abort();
  }, [planId, refresh]);
  const settled = !ACTIVE_PLAN_STATES.has(current.state) && current.state !== "proposed";
  return (
    <div
      className={`dock-card dock-card-plan dock-card-plan-${current.state}${settled ? " dock-card-plan-settled" : ""}`}
      data-testid="assistant-plan"
      role="group"
      aria-label={`Research plan ${current.title}`}
    >
      <div className="dock-card-title">
        <strong>
          <FlaskConical size={12} aria-hidden="true" /> {current.title}
        </strong>
        <ResearchStateBadge state={current.state} />
      </div>
      <span className="dock-card-meta">
        research plan · {plural(current.jobs_planned, "job")} · {current.org_id}/{current.project_id}
      </span>
      <dl className="research-card-facts">
        <div>
          <dt>Budget</dt>
          <dd data-testid="plan-budget">{budgetSummary(current.budget)}</dd>
        </div>
        <div>
          <dt>Iterates</dt>
          <dd>{current.iterate ? "yes — may plan further rounds within the budget" : "no — runs only these jobs"}</dd>
        </div>
        {current.stop_reason && (
          <div>
            <dt>Stopped</dt>
            <dd>{current.stop_reason}</dd>
          </div>
        )}
      </dl>
      <PlanActions
        plan={current}
        canApprove={canApprove}
        canStop={canStop}
        approve={approve}
        stop={stop}
        onUpdated={(updated) => {
          setCurrent(updated);
          onUpdated?.(updated);
        }}
      />
      <a className="text-button research-card-link" href={routeHash({ section: "research", planId: current.id })}>
        Open in Research
      </a>
    </div>
  );
}
