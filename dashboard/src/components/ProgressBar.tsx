/*
 * Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
