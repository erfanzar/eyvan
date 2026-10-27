// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

export type Section =
  | "overview"
  | "workloads"
  | "runs"
  | "fleet"
  | "capacity"
  | "billing"
  | "budgets"
  | "audit"
  | "assistant"
  | "settings";

export type RunTab = "charts" | "episodes" | "histograms" | "rollouts" | "completions" | "media";

export type Route =
  | { section: Section }
  | { section: "workloads"; workloadId: string }
  | { section: "runs"; runId: string; tab?: RunTab };

export type SectionAvailability = Partial<Record<Section, boolean>>;

/** Keep a forbidden deep link from mounting panels that issue fleet-only reads. */
export function accessibleRoute(route: Route, availability: SectionAvailability): Route {
  return availability[route.section] === false ? { section: "overview" } : route;
}

export function parseRoute(hash = globalThis.location?.hash ?? ""): Route {
  const path = hash.replace(/^#\/?/, "");
  const [section, id, tab] = path.split("/");
  if ((section === "workloads" || section === "runs") && id) {
    try {
      return section === "runs"
        ? {
            section: "runs",
            runId: decodeURIComponent(id),
            ...(tab === "episodes" ||
            tab === "histograms" ||
            tab === "rollouts" ||
            tab === "completions" ||
            tab === "media"
              ? { tab }
              : {}),
          }
        : { section: "workloads", workloadId: decodeURIComponent(id) };
    } catch (error) {
      if (!(error instanceof URIError)) throw error;
      // Browser hashes may come from malformed external links, not routeHash.
      // Keep the list usable without requesting an invalid workload detail.
      return { section };
    }
  }
  // Every section the rail offers, or the rail has an item that navigates to
  // the overview. `billing` was missing here while it had a nav entry, a page,
  // and an availability flag -- so pressing Billing set the hash, the hash
  // parsed back to overview, and the section could only be reached by not
  // reloading.
  if (
    ["workloads", "runs", "fleet", "capacity", "billing", "budgets", "audit", "assistant", "settings"].includes(section)
  ) {
    return { section: section as Section };
  }
  // Team accounts first shipped as their own `#/users` section; keep those links working.
  if (section === "users") return { section: "settings" };
  return { section: "overview" };
}

export function routeHash(route: Route): string {
  if ("runId" in route) {
    const suffix = route.tab && route.tab !== "charts" ? `/${route.tab}` : "";
    return `#/runs/${encodeURIComponent(route.runId)}${suffix}`;
  }
  if ("workloadId" in route) {
    return `#/workloads/${encodeURIComponent(route.workloadId)}`;
  }
  return `#/${route.section}`;
}

export function navigate(route: Route): void {
  globalThis.location.hash = routeHash(route);
}
