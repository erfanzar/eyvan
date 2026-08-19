// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

export function formatMoney(value: number, compact = false): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: compact ? 1 : 0,
    notation: compact ? "compact" : "standard",
  }).format(value);
}

/**
 * Money to the cent, for a figure that is often less than a dollar.
 *
 * Not `formatMoney`, which rounds to whole dollars. That is right for a budget
 * in the thousands and wrong for one run: a forty-cent job would render as
 * "$0", which reads as free. The two decimals also match what `eyvan workload
 * cost` prints, so the terminal and the browser cannot disagree about a number
 * somebody is comparing across them.
 */
export function formatSpend(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatDateTime(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

export function formatRelativeTime(value: string): string {
  const deltaSeconds = Math.round((new Date(value).getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
  const ranges: Array<[Intl.RelativeTimeFormatUnit, number]> = [
    ["year", 31_536_000],
    ["month", 2_592_000],
    ["day", 86_400],
    ["hour", 3_600],
    ["minute", 60],
  ];
  for (const [unit, seconds] of ranges) {
    if (Math.abs(deltaSeconds) >= seconds) {
      return formatter.format(Math.round(deltaSeconds / seconds), unit);
    }
  }
  return formatter.format(deltaSeconds, "second");
}

export function percent(value: number, total: number): number {
  if (total <= 0) return 0;
  return Math.max(0, Math.min(100, Math.round((value / total) * 100)));
}

export function titleCase(value: string): string {
  return value
    .split(/[_-]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

/**
 * Render an age in seconds, or say it is unknown.
 *
 * `unknown` is a value here, not a fallback. A quota row whose usage nobody
 * could read has no age, and rendering that as "0s ago" would present the
 * absence of a measurement as a fresh one.
 */
export function formatAgeSeconds(seconds: number | null | undefined): string {
  if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0) return "unknown";
  if (seconds < 60) return `${Math.floor(seconds)}s ago`;
  if (seconds < 5400) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
}

/**
 * Describe what a workload asked for, without inflating it.
 *
 * The accelerator count is *chips*, and a TPU slice type already states its own
 * size: `v5p-16` is one slice of 8 chips. Rendering the pair as `8x v5p-16`
 * therefore claimed eight slices -- 64 chips -- for a request that asked for
 * one, and multiplied every shape on the page by its own chip count.
 *
 * The multiplier belongs to replicas, which is what `pods=N` actually
 * allocates. The chip count is kept as detail, because it is the number
 * admission checks and the one a person compares against their quota.
 */
export function formatAcceleratorRequest(
  accelerator: string | undefined,
  chips: number | undefined,
  replicas: number,
): string {
  if (!accelerator) return chips ? `${chips} chips` : "None";
  const slices = replicas > 1 ? `${replicas} x ${accelerator}` : accelerator;
  return chips ? `${slices} · ${chips} chips` : slices;
}
