import { CircleCheck, CircleDashed, Loader2 } from "lucide-react";
import type { TimelineEvent } from "../types";

/**
 * The eight macro-stages every Eyvan job walks through, in order. One place
 * names them so the pipeline strip and the timeline agree by construction.
 */
export const PIPELINE_STAGES = [
  { key: "submit", label: "Submitted" },
  { key: "admitted", label: "Admitted" },
  { key: "capacity", label: "Capacity" },
  { key: "provision", label: "Provisioning" },
  { key: "dispatch", label: "Dispatched" },
  { key: "workers", label: "Workers ready" },
  { key: "running", label: "Running" },
  { key: "settled", label: "Settled" },
] as const;

/** Progress-contract phase -> which pipeline step it sits inside. */
const PHASE_TO_STAGE: Record<string, string> = {
  waiting_for_autoscaler: "capacity",
  requesting_capacity: "capacity",
  waiting_for_capacity: "capacity",
  provisioning: "provision",
  capacity_allocated: "dispatch",
  native_workers_connecting: "workers",
  waiting_for_scheduler: "dispatch",
  waiting_for_dispatch: "dispatch",
  ready_for_dispatch: "dispatch",
  provider_failure: "capacity",
};

const STATE_TO_STAGE: Record<string, string> = {
  pending: "admitted",
  queued: "admitted",
  leased: "capacity",
  dispatching: "dispatch",
  running: "running",
  succeeded: "settled",
  failed: "settled",
  cancelled: "settled",
};

/** Duration of each named stage, read off the audit timeline. */
function stageDurations(timeline: TimelineEvent[]): Map<string, number> {
  const stamps = timeline
    .map((event) => ({ action: event.title, at: new Date(event.timestamp).getTime() }))
    .filter((entry) => Number.isFinite(entry.at))
    .sort((a, b) => a.at - b.at);
  const gaps = new Map<string, number>();
  for (let i = 1; i < stamps.length; i += 1) {
    const seconds = Math.round((stamps[i].at - stamps[i - 1].at) / 1000);
    if (!gaps.has(stamps[i - 1].action)) gaps.set(stamps[i - 1].action, seconds);
  }
  return gaps;
}

function fmt(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

/**
 * Which pipeline index a workload has reached, and whether it is still moving.
 * Derived, never stored: the record's own facts are the only input.
 */
export function pipelinePosition(status: string, phase?: string): { active: number; activeLabel: string } {
  const fromPhase = phase ? PHASE_TO_STAGE[phase] : undefined;
  const fromState = STATE_TO_STAGE[status];
  const key = fromPhase ?? fromState ?? "submit";
  let active = PIPELINE_STAGES.findIndex((stage) => stage.key === key);
  if (active < 0) active = 0;
  return { active, activeLabel: PIPELINE_STAGES[active].label };
}

export function StagePipeline({
  status,
  phase,
  timeline,
}: {
  status: string;
  phase?: string;
  timeline: TimelineEvent[];
}) {
  const terminal = status === "succeeded" || status === "failed" || status === "cancelled";
  const { active, activeLabel } = pipelinePosition(status, phase);
  const durations = stageDurations(timeline);
  const actionFor = (key: string): number | undefined => {
    const labels: Record<string, string[]> = {
      submit: ["submitted"],
      admitted: ["admitted", "queue admission denied"],
      capacity: ["leased capacity"],
      provision: [],
      dispatch: ["dispatch reserved", "dispatched to its cells"],
      workers: [],
      running: [],
      settled: ["failed", "succeeded", "cancelled", "settled"],
    };
    const hits = labels[key]?.map((needle) => durations.get(needle)).filter((value): value is number => value !== undefined);
    return hits && hits.length > 0 ? Math.max(...hits) : undefined;
  };

  return (
    <ol className="stage-pipeline" aria-label="Dispatch pipeline">
      {PIPELINE_STAGES.map((stage, index) => {
        const done = terminal ? index <= active : index < active;
        const current = !terminal && index === active;
        const wait = done ? actionFor(stage.key) : undefined;
        return (
          <li
            key={stage.key}
            className={`stage-node${done ? " stage-done" : ""}${current ? " stage-active" : ""}`}
            aria-current={current ? "step" : undefined}
          >
            <span className="stage-icon">
              {done ? <CircleCheck size={15} aria-hidden /> : current ? <Loader2 size={15} className="spin" aria-hidden /> : <CircleDashed size={15} aria-hidden />}
            </span>
            <span className="stage-label">{stage.label}</span>
            {wait !== undefined && <span className="timeline-gap">{fmt(wait)}</span>}
            {index < PIPELINE_STAGES.length - 1 && <span className="stage-link" aria-hidden />}
          </li>
        );
      })}
    </ol>
  );
}
