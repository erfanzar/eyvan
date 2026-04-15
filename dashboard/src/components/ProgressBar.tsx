interface ProgressBarProps {
  value: number;
  label: string;
  tone?: "green" | "blue" | "amber" | "red" | "violet";
}

export function ProgressBar({ value, label, tone = "green" }: ProgressBarProps) {
  const normalized = Math.max(0, Math.min(100, value));
  return (
    <div className="progress-wrap">
      <div
        className="progress-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={normalized}
      >
        <span className={`progress-fill progress-${tone}`} style={{ width: `${normalized}%` }} />
      </div>
    </div>
  );
}
