// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { describe, expect, it } from "vitest";

import { accessibleRoute, parseRoute, routeHash } from "./routing";

describe("dashboard routing", () => {
  it("keeps project-scoped routes when fleet panels are unavailable", () => {
    const availability = { fleet: false, audit: false };

    expect(accessibleRoute({ section: "workloads" }, availability)).toEqual({ section: "workloads" });
    expect(accessibleRoute({ section: "workloads", workloadId: "wl/1" }, availability)).toEqual({
      section: "workloads",
      workloadId: "wl/1",
    });
    expect(accessibleRoute({ section: "budgets" }, availability)).toEqual({ section: "budgets" });
  });

  it("falls back from a forbidden fleet deep link without treating navigation as authority", () => {
    expect(accessibleRoute(parseRoute("#/fleet"), { fleet: false })).toEqual({ section: "overview" });
    expect(accessibleRoute(parseRoute("#/audit"), { audit: false })).toEqual({ section: "overview" });
    expect(routeHash({ section: "fleet" })).toBe("#/fleet");
  });

  it("preserves a fleet route when the independently loaded endpoint is available", () => {
    expect(accessibleRoute({ section: "fleet" }, { fleet: true })).toEqual({ section: "fleet" });
  });
});

describe("the quota section", () => {
  it("is a first-class route so a zone choice can be linked to", () => {
    expect(parseRoute("#/capacity")).toEqual({ section: "capacity" });
    expect(routeHash({ section: "capacity" })).toBe("#/capacity");
  });

  it("falls back when the identity cannot read the pools the quota is resolved from", () => {
    // The route resolves its provider project from capacity pool records, so a
    // reader without fleet authority must not mount it and issue a 403.
    expect(accessibleRoute(parseRoute("#/capacity"), { capacity: false })).toEqual({ section: "overview" });
    expect(accessibleRoute(parseRoute("#/capacity"), { capacity: true })).toEqual({ section: "capacity" });
  });
});
