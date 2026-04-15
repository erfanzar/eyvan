import {
  ArrowLeft,
  Ban,
  Check,
  Clipboard,
  Clock3,
  Code2,
  Database,
  HardDrive,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  WalletCards,
} from "lucide-react";
import { useMemo, useState } from "react";

import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { formatDateTime, formatMoney, percent } from "../lib/format";
import type { LogEntry, WorkloadDetail } from "../types";

type DetailTab = "overview" | "timeline" | "logs";

interface WorkloadDetailPageProps {
  workload: WorkloadDetail;
  onBack: () => void;
  onAction: (action: "cancel" | "checkpoint" | "retry") => void;
}

export function WorkloadDetailPage({ workload, onBack, onAction }: WorkloadDetailPageProps) {
  const [tab, setTab] = useState<DetailTab>("overview");
  const [logQuery, setLogQuery] = useState("");
  const [logLevel, setLogLevel] = useState("all");
  const [copied, setCopied] = useState(false);
  const budgetTracked = workload.budgetSpent !== undefined && workload.budgetLimit !== undefined;
  const budgetPercent = budgetTracked ? percent(workload.budgetSpent!, workload.budgetLimit!) : 0;
  const cancellable = !["completed", "failed", "cancelled", "preempting"].includes(workload.status);
  const checkpointable = ["running", "provisioning", "checkpointing"].includes(workload.status);

  const filteredLogs = useMemo(() => {
    const query = logQuery.trim().toLowerCase();
    return workload.logs.filter(
      (log) =>
        (logLevel === "all" || log.level === logLevel) &&
        (!query || log.message.toLowerCase().includes(query) || (log.source?.toLowerCase().includes(query) ?? false)),
    );
  }, [logLevel, logQuery, workload.logs]);

  const copyCommand = async () => {
    await navigator.clipboard.writeText(workload.command);
    setCopied(true);
    globalThis.setTimeout(() => setCopied(false), 1800);
  };

  return (
    <div className="page-stack">
      <div className="detail-heading">
        <div className="detail-title-row">
          <button className="icon-button" type="button" onClick={onBack} aria-label="Back to workloads" title="Back to workloads">
            <ArrowLeft size={19} />
          </button>
          <div>
            <div className="detail-title-with-status">
              <h2>{workload.name}</h2>
              <StatusBadge value={workload.status} tone={workload.status} />
            </div>
            <p>{workload.id} · {workload.project} · submitted by {workload.user}</p>
          </div>
        </div>
        <div className="detail-actions">
          {workload.status === "failed" && (
            <button className="button button-primary" type="button" onClick={() => onAction("retry")}>
              <RefreshCw size={16} aria-hidden="true" />
              Retry
            </button>
          )}
          {checkpointable && (
            <button className="button button-primary" type="button" onClick={() => onAction("checkpoint")}>
              <HardDrive size={16} aria-hidden="true" />
              Checkpoint
            </button>
          )}
          {cancellable && (
            <button className="button button-secondary" type="button" onClick={() => onAction("cancel")}>
              <Ban size={16} aria-hidden="true" />
              Cancel
            </button>
          )}
        </div>
      </div>

      <section className="detail-metric-grid" aria-label="Workload metrics">
        <div className="detail-metric"><Clock3 size={17} /><span><small>Submitted</small><strong>{formatDateTime(workload.submittedAt)}</strong></span></div>
        <div className="detail-metric"><Server size={17} /><span><small>Placement</small><strong>{workload.cell ?? "Pending"}</strong></span></div>
        <div className="detail-metric"><WalletCards size={17} /><span><small>Spend</small><strong>{budgetTracked ? `${formatMoney(workload.budgetSpent!)} / ${formatMoney(workload.budgetLimit!)}` : "Not tracked"}</strong></span></div>
        <div className="detail-metric"><ShieldCheck size={17} /><span><small>Priority</small><strong>{workload.priority}</strong></span></div>
        <div className="detail-progress-metric">
          <span><small>Progress</small><strong>{workload.progress === undefined ? "—" : `${workload.progress}%`}</strong></span>
          {workload.progress !== undefined && <ProgressBar value={workload.progress} label="Workload progress" />}
        </div>
      </section>

      <div className="tabs" role="tablist" aria-label="Workload detail sections">
        {(["overview", "timeline", "logs"] as DetailTab[]).map((item) => (
          <button
            key={item}
            type="button"
            role="tab"
            aria-selected={tab === item}
            className={tab === item ? "tab-active" : ""}
            onClick={() => setTab(item)}
          >
            {item === "overview" ? "Overview" : item === "timeline" ? `Timeline (${workload.timeline.length})` : `Logs (${workload.logs.length})`}
          </button>
        ))}
      </div>

      {tab === "overview" && (
        <div className="detail-overview-grid">
          <section className="panel detail-section">
            <div className="panel-header"><div><h2>Resource request</h2><p>Gang and topology requirements</p></div><Server size={18} /></div>
            <dl className="definition-grid">
              <div><dt>Replicas</dt><dd>{workload.resources.replicas}</dd></div>
              <div><dt>CPU</dt><dd>{workload.resources.cpu} cores</dd></div>
              <div><dt>Memory</dt><dd>{workload.resources.memoryGb} GB</dd></div>
              <div><dt>Accelerators</dt><dd>{workload.resources.accelerator ? `${workload.resources.acceleratorCount}x ${workload.resources.accelerator}` : "None"}</dd></div>
              <div><dt>Topology</dt><dd>{workload.resources.topology ?? "No strict constraint"}</dd></div>
              <div><dt>Lease</dt><dd className="monospace">{workload.leaseId ?? "Not active"}</dd></div>
            </dl>
          </section>

          <section className="panel detail-section">
            <div className="panel-header"><div><h2>Budget</h2><p>Workload-level spend guardrail</p></div><WalletCards size={18} /></div>
            {budgetTracked ? (
              <>
                <div className="large-budget-value">
                  <strong>{formatMoney(workload.budgetSpent!)}</strong>
                  <span>of {formatMoney(workload.budgetLimit!)}</span>
                </div>
                <ProgressBar value={budgetPercent} label="Workload budget utilization" tone={budgetPercent > 90 ? "red" : "blue"} />
                <div className="resource-row-detail"><span>{budgetPercent}% consumed</span><span>{formatMoney(workload.budgetLimit! - workload.budgetSpent!)} remaining</span></div>
              </>
            ) : (
              <div className="resource-row-detail"><span>Per-workload spend is not reported by the control plane.</span></div>
            )}
          </section>

          <section className="panel allocation-section">
            <div className="panel-header"><div><h2>Allocation</h2><p>Committed hosts and failure domains</p></div><Database size={18} /></div>
            <div className="allocation-grid">
              {workload.allocation.map((allocation) => (
                <div className="allocation-item" key={allocation.host}>
                  <span className={`allocation-state allocation-${allocation.state}`} aria-hidden="true" />
                  <span><strong>{allocation.host}</strong><small>{allocation.rack} · {allocation.resource}</small></span>
                  <StatusBadge value={allocation.state} tone={allocation.state === "ready" ? "healthy" : allocation.state === "starting" ? "activating" : "revoking"} />
                </div>
              ))}
            </div>
          </section>

          <section className="panel runtime-section">
            <div className="panel-header"><div><h2>Runtime</h2><p>Entrypoint and durable state</p></div><Code2 size={18} /></div>
            <div className="code-block">
              <code>{workload.command}</code>
              <button className="icon-button code-copy" type="button" onClick={copyCommand} aria-label="Copy command" title="Copy command">
                {copied ? <Check size={16} /> : <Clipboard size={16} />}
              </button>
            </div>
            <dl className="runtime-list">
              <div><dt>Checkpoint</dt><dd className="monospace">{workload.checkpointUri ?? "No checkpoint"}</dd></div>
              <div><dt>Labels</dt><dd>{Object.entries(workload.labels).map(([key, value]) => <span className="label-chip" key={key}>{key}={value}</span>)}</dd></div>
            </dl>
          </section>
        </div>
      )}

      {tab === "timeline" && (
        <section className="panel timeline-panel">
          <div className="panel-header"><div><h2>Lifecycle timeline</h2><p>Admission, placement, lease, and runtime transitions</p></div></div>
          <ol className="timeline-list">
            {workload.timeline.map((event) => (
              <li key={event.id} className={`timeline-${event.severity}`}>
                <span className="timeline-marker" aria-hidden="true" />
                <div>
                  <span className="timeline-heading"><strong>{event.title}</strong><time>{formatDateTime(event.timestamp)}</time></span>
                  <p>{event.detail}</p>
                  {event.actor && <small>{event.actor}</small>}
                </div>
              </li>
            ))}
          </ol>
        </section>
      )}

      {tab === "logs" && (
        <section className="panel logs-panel">
          <div className="table-toolbar log-toolbar">
            <label className="search-field">
              <Search size={16} aria-hidden="true" />
              <span className="sr-only">Search logs</span>
              <input type="search" placeholder="Search log output" value={logQuery} onChange={(event) => setLogQuery(event.target.value)} />
            </label>
            <label className="select-field">
              <span className="sr-only">Filter log level</span>
              <select value={logLevel} onChange={(event) => setLogLevel(event.target.value)}>
                <option value="all">All levels</option>
                <option value="DEBUG">Debug</option>
                <option value="INFO">Info</option>
                <option value="WARN">Warning</option>
                <option value="ERROR">Error</option>
              </select>
            </label>
          </div>
          <div className="log-stream" role="log" aria-label="Workload logs">
            {filteredLogs.map((log: LogEntry) => (
              <div className={`log-line${log.level ? ` log-${log.level.toLowerCase()}` : ""}`} key={log.id}>
                <time>{log.timestamp ? log.timestamp.slice(11, 23) : "—"}</time>
                <span className="log-level">{log.level ?? "—"}</span>
                <span className="log-source">{log.source ?? "—"}</span>
                <code>{log.message}</code>
              </div>
            ))}
            {filteredLogs.length === 0 && <div className="log-empty">No log entries match this filter.</div>}
          </div>
        </section>
      )}
    </div>
  );
}
