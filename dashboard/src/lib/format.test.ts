// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { describe, expect, it } from "vitest";

import { summarizeShapes } from "./format";

describe("summarizeShapes", () => {
  it("never hides a generation behind a count", () => {
    // The regression: shapes sort by generation, so "first three and a count"
    // rendered a fleet's whole v6e half as part of "+14 more".
    const fleet = [
      "v5p-16", "v5p-32", "v5p-64", "v5p-128", "v5p-256", "v5p-384",
      "v5p-512", "v5p-640", "v5p-768", "v5p-896", "v5p-1024",
      "v6e-8", "v6e-16", "v6e-32", "v6e-64", "v6e-128", "v6e-256",
    ];
    const summary = summarizeShapes(fleet);
    expect(summary).toBe("v5p 16-1024, v6e 8-256");
    expect(summary).toContain("v6e");
  });

  it("keeps a lone shape exact rather than turning it into a range", () => {
    expect(summarizeShapes(["v5p-16"])).toBe("v5p-16");
  });

  it("passes through a name that is not generation-size", () => {
    expect(summarizeShapes(["cpu"])).toBe("cpu");
  });

  it("renders nothing for no shapes", () => {
    expect(summarizeShapes([])).toBe("");
  });
});
