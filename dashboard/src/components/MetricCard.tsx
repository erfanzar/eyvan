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
import type { LucideIcon } from "lucide-react";
import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react";

interface MetricCardProps {
  label: string;
  value: string;
  detail: string;
  icon: LucideIcon;
  trend?: number;
  tone?: "neutral" | "positive" | "warning" | "critical";
}

export function MetricCard({
  label,
  value,
  detail,
  icon: Icon,
  trend,
  tone = "neutral",
}: MetricCardProps) {
  const TrendIcon = trend === undefined || trend === 0 ? Minus : trend > 0 ? ArrowUpRight : ArrowDownRight;
  return (
    <article className={`metric-card metric-${tone}`}>
      <div className="metric-heading">
        <span>{label}</span>
        <span className="metric-icon" aria-hidden="true">
          <Icon size={17} strokeWidth={1.9} />
        </span>
      </div>
      <strong className="metric-value">{value}</strong>
      <div className="metric-detail">
        {trend !== undefined && (
          <span className={trend > 0 ? "trend-up" : trend < 0 ? "trend-down" : "trend-flat"}>
            <TrendIcon size={14} aria-hidden="true" />
            {Math.abs(trend)}%
          </span>
        )}
        <span>{detail}</span>
      </div>
    </article>
  );
}
