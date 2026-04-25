// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
