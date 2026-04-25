// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

export type Section = "overview" | "workloads" | "fleet" | "budgets" | "audit";

export type Route =
  | { section: Section }
  | { section: "workloads"; workloadId: string };

export function parseRoute(hash = globalThis.location?.hash ?? ""): Route {
  const path = hash.replace(/^#\/?/, "");
  const [section, id] = path.split("/");
  if (section === "workloads" && id) {
    return { section: "workloads", workloadId: decodeURIComponent(id) };
  }
  if (["workloads", "fleet", "budgets", "audit"].includes(section)) {
    return { section: section as Section };
  }
  return { section: "overview" };
}

export function routeHash(route: Route): string {
  if ("workloadId" in route) {
    return `#/workloads/${encodeURIComponent(route.workloadId)}`;
  }
  return `#/${route.section}`;
}

export function navigate(route: Route): void {
  globalThis.location.hash = routeHash(route);
}
