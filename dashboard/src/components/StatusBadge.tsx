// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import type { PriorityBand, Severity, WorkloadStatus } from "../types";
import { titleCase } from "../lib/format";

type BadgeTone =
  | WorkloadStatus
  | PriorityBand
  | Severity
  | "healthy"
  | "starting"
  | "degraded"
  | "maintenance"
  | "active"
  | "activating"
  | "revoking"
  | "expired"
  | "reserved"
  | "on_demand"
  | "spot"
  | "hard"
  | "soft";

interface StatusBadgeProps {
  value: string;
  tone?: BadgeTone;
  dot?: boolean;
}

export function StatusBadge({ value, tone, dot = true }: StatusBadgeProps) {
  const className = `status-badge status-${tone ?? value.toLowerCase()}`;
  return (
    <span className={className}>
      {dot && <span className="status-dot" aria-hidden="true" />}
      {titleCase(value)}
    </span>
  );
}
