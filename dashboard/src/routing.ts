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
