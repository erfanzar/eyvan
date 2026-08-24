// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

interface ProgressBarProps {
  value: number;
  label: string;
  tone?: "accent" | "green" | "blue" | "amber" | "red" | "violet";
}

export function ProgressBar({ value, label, tone = "accent" }: ProgressBarProps) {
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
        {/* No fill at all at zero. The 2px minimum keeps a 1% bar visible,
            and on an empty one it draws a tick that reads as "some". */}
        {normalized > 0 && <span className={`progress-fill progress-${tone}`} style={{ width: `${normalized}%` }} />}
      </div>
    </div>
  );
}
