import { AlertCircle, LoaderCircle, Plus, X } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";

import type { PriorityBand, SubmitWorkloadInput, WorkloadDetail } from "../types";

interface SubmitWorkloadModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (input: SubmitWorkloadInput) => Promise<WorkloadDetail>;
}

const initialForm = {
  name: "",
  project: "research",
  priority: "interactive" as PriorityBand,
  capacityClass: "spot" as "spot" | "on_demand",
  command: "python -m easydel.scripts.elarge --config configs/train.yaml",
  replicas: 1,
  cpu: 64,
  memoryGb: 512,
  accelerator: "v5p-8",
  acceleratorCount: 4,
  topology: "single-slice",
  estimatedDurationHours: 8,
  preemptible: true,
  checkpointHook: "python scripts/checkpoint.py",
  checkpointUri: "gs://eyvan-checkpoints/research/run",
  labels: "environment=production,team=research",
};

export function SubmitWorkloadModal({ open, onClose, onSubmit }: SubmitWorkloadModalProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const previousFocus = useRef<HTMLElement | null>(null);
  const [form, setForm] = useState(initialForm);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    previousFocus.current = document.activeElement as HTMLElement;
    document.body.classList.add("modal-open");
    globalThis.setTimeout(() => dialogRef.current?.querySelector<HTMLInputElement>("input")?.focus(), 0);

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
      if (event.key !== "Tab" || !dialogRef.current) return;
      const focusable = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(
          'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled])',
        ),
      );
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    globalThis.addEventListener("keydown", onKeyDown);
    return () => {
      globalThis.removeEventListener("keydown", onKeyDown);
      document.body.classList.remove("modal-open");
      previousFocus.current?.focus();
    };
  }, [onClose, open]);

  if (!open) return null;

  const set = <K extends keyof typeof form>(key: K, value: (typeof form)[K]) => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    if (!form.name.trim() || !form.project.trim() || !form.command.trim()) {
      setError("Name, project, and command are required.");
      return;
    }
    if (form.replicas < 1 || form.cpu < 1 || form.memoryGb < 1 || form.estimatedDurationHours <= 0) {
      setError("Resource counts and estimated duration must be greater than zero.");
      return;
    }
    if (form.accelerator !== "none" && form.acceleratorCount < 1) {
      setError("Accelerator-backed workloads require at least one accelerator per replica.");
      return;
    }
    if (form.preemptible && (!form.checkpointHook.trim() || !form.checkpointUri.trim())) {
      setError("Preemptible workloads require a checkpoint hook and durable output URI.");
      return;
    }

    const labels = Object.fromEntries(
      form.labels
        .split(",")
        .map((entry) => entry.trim().split("=", 2))
        .filter((entry): entry is [string, string] => entry.length === 2 && Boolean(entry[0]) && Boolean(entry[1])),
    );

    setSubmitting(true);
    try {
      await onSubmit({
        name: form.name.trim(),
        project: form.project.trim(),
        priority: form.priority,
        capacityClass: form.capacityClass,
        command: form.command.trim(),
        resources: {
          replicas: form.replicas,
          cpu: form.cpu,
          memoryGb: form.memoryGb,
          accelerator: form.accelerator === "none" ? undefined : form.accelerator,
          acceleratorCount: form.accelerator === "none" ? undefined : form.acceleratorCount,
          topology: form.topology === "none" ? undefined : form.topology,
        },
        estimatedDurationSeconds: Math.round(form.estimatedDurationHours * 3600),
        preemptible: form.preemptible,
        checkpointHook: form.preemptible ? form.checkpointHook.trim() : undefined,
        checkpointUri: form.preemptible ? form.checkpointUri.trim() : undefined,
        labels,
      });
      setForm(initialForm);
      onClose();
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Workload submission failed.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="modal-layer" role="presentation">
      <button className="modal-scrim" type="button" onClick={onClose} aria-label="Close submit workload dialog" />
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby="submit-workload-title" ref={dialogRef}>
        <div className="modal-header">
          <div><h2 id="submit-workload-title">Submit workload</h2><p>Create an admitted workload request for the global scheduler.</p></div>
          <button className="icon-button" type="button" onClick={onClose} aria-label="Close dialog" title="Close dialog"><X size={19} /></button>
        </div>
        <form onSubmit={submit}>
          <div className="modal-body">
            {error && <div className="form-error" role="alert"><AlertCircle size={16} />{error}</div>}

            <fieldset>
              <legend>Identity and admission</legend>
              <div className="form-grid form-grid-two">
                <label className="field field-wide"><span>Workload name</span><input value={form.name} onChange={(event) => set("name", event.target.value)} placeholder="llama-70b-sft-v5" required /></label>
                <label className="field"><span>Project</span><input value={form.project} onChange={(event) => set("project", event.target.value)} required /></label>
                <label className="field"><span>Priority band</span><select value={form.priority} onChange={(event) => set("priority", event.target.value as PriorityBand)}><option value="production">Production</option><option value="interactive">Interactive</option><option value="batch">Batch</option></select></label>
                <label className="field"><span>Capacity class</span><select value={form.capacityClass} onChange={(event) => set("capacityClass", event.target.value as "spot" | "on_demand")}><option value="spot">Spot</option><option value="on_demand">On-demand</option></select></label>
              </div>
            </fieldset>

            <fieldset>
              <legend>Gang resources</legend>
              <div className="form-grid form-grid-three">
                <label className="field"><span>Replicas</span><input type="number" min="1" value={form.replicas} onChange={(event) => set("replicas", Number(event.target.value))} /></label>
                <label className="field"><span>CPU per replica</span><input type="number" min="1" value={form.cpu} onChange={(event) => set("cpu", Number(event.target.value))} /></label>
                <label className="field"><span>Memory per replica (GB)</span><input type="number" min="1" value={form.memoryGb} onChange={(event) => set("memoryGb", Number(event.target.value))} /></label>
                <label className="field"><span>Accelerator type</span><select value={form.accelerator} onChange={(event) => set("accelerator", event.target.value)}><option value="v6e-8">Trillium v6e-8</option><option value="v5p-8">TPU v5p-8</option><option value="v5p-16">TPU v5p-16</option><option value="v5e-8">TPU v5e-8</option><option value="v4-8">TPU v4-8</option><option value="none">CPU only (no accelerator)</option></select></label>
                <label className="field"><span>Accelerator count</span><input type="number" min="0" disabled={form.accelerator === "none"} value={form.acceleratorCount} onChange={(event) => set("acceleratorCount", Number(event.target.value))} /></label>
                <label className="field"><span>Gang topology</span><select value={form.topology} onChange={(event) => set("topology", event.target.value)}><option value="single-slice">Single TPU slice</option><option value="same-zone">Same zone</option><option value="multi-slice">Multi-slice pod</option><option value="none">No strict constraint</option></select></label>
              </div>
            </fieldset>

            <fieldset>
              <legend>Runtime and controls</legend>
              <div className="form-grid form-grid-two">
                <label className="field field-wide"><span>Command</span><textarea rows={3} value={form.command} onChange={(event) => set("command", event.target.value)} required /></label>
                <label className="field"><span>Estimated duration (hours)</span><input type="number" min="0.25" step="0.25" value={form.estimatedDurationHours} onChange={(event) => set("estimatedDurationHours", Number(event.target.value))} /></label>
                <label className="field"><span>Labels</span><input value={form.labels} onChange={(event) => set("labels", event.target.value)} placeholder="team=research,env=prod" /></label>
                <label className="checkbox-field field-wide"><input type="checkbox" checked={form.preemptible} onChange={(event) => set("preemptible", event.target.checked)} /><span><strong>Checkpointable and preemptible</strong><small>Allow whole-gang preemption after the configured checkpoint grace.</small></span></label>
                {form.preemptible && <><label className="field field-wide"><span>Checkpoint hook</span><input value={form.checkpointHook} onChange={(event) => set("checkpointHook", event.target.value)} required /></label><label className="field field-wide"><span>Checkpoint output URI</span><input value={form.checkpointUri} onChange={(event) => set("checkpointUri", event.target.value)} required /></label></>}
              </div>
            </fieldset>
          </div>
          <div className="modal-footer">
            <button className="button button-secondary" type="button" onClick={onClose}>Cancel</button>
            <button className="button button-primary" type="submit" disabled={submitting}>
              {submitting ? <LoaderCircle className="spin" size={16} /> : <Plus size={16} />}
              {submitting ? "Submitting" : "Submit workload"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
