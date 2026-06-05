// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

/**
 * The two panels, rendered.
 *
 * The dashboard had zero component tests: 38 assertions, all at the data layer.
 * That layer proves the request shape and the error type, and proves nothing
 * about what a person sees -- so the claim these panels were added for, that a
 * failure is *reported* rather than rendered as an empty section, was untested
 * at the only layer where it is true or false.
 *
 * An empty explain panel reads as "nothing to report", which is the one thing it
 * must not say when the request failed. An empty cost panel reads as "nothing is
 * being spent", which is the most expensive thing it could say incorrectly.
 * Both are asserted here against a real DOM.
 */

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "../api/client";
import { getMockWorkload, mockWorkloads } from "../api/mockData";
import type { CapacityLease, Cell, WorkloadDetail } from "../types";
import { FleetPage } from "./FleetPage";
import { WorkloadDetailPage } from "./WorkloadDetailPage";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function workload(): WorkloadDetail {
  // The shipped mock rather than a hand-built literal. A partial fixture makes
  // the page throw on a field this test does not care about, and the resulting
  // failure points at the fixture instead of at the panel.
  const detail = getMockWorkload(mockWorkloads[0].id);
  if (!detail) throw new Error("the mock dataset has no workloads");
  return detail;
}


describe("the placement verdict panel", () => {
  it("shows the per-cell detail behind the flat reason", async () => {
    vi.spyOn(apiClient, "explainWorkload").mockResolvedValue({
      workload_id: "wl-1",
      state: "pending",
      placeable: false,
      placement_cell_id: null,
      code: "no_matching_cell",
      message: "no cell satisfies all hard workload constraints",
      cells: [{ cell_id: "cell-a", reason: "insufficient tpu" }],
      considered_cell_count: 7,
      provisioning_cell_ids: ["cell-b"],
      scheduling_reason: "waiting for capacity",
    });

    render(<WorkloadDetailPage workload={workload()} onBack={() => {}} onAction={async () => {}} />);

    await waitFor(() => expect(screen.getByText("Placement verdict")).toBeDefined());
    expect(screen.getByText("no cell satisfies all hard workload constraints")).toBeDefined();
    expect(screen.getByText("cell-a")).toBeDefined();
    // The number that separates "waiting for capacity" from "can never be
    // placed" -- both report the same code, and only one is a problem.
    expect(screen.getByText("Being provisioned")).toBeDefined();
  });

  it("reports a failure rather than rendering an empty panel", async () => {
    // The claim the panel was added for, asserted where it is true or false.
    vi.spyOn(apiClient, "explainWorkload").mockRejectedValue(new Error("Eyvan API request timed out"));

    render(<WorkloadDetailPage workload={workload()} onBack={() => {}} onAction={async () => {}} />);

    await waitFor(() => expect(screen.getByText("Eyvan API request timed out")).toBeDefined());
  });
});

describe("the warm capacity and cost panel", () => {
  const cells: Cell[] = [];
  const leases: CapacityLease[] = [];

  it("reports warm capacity beside idle cost", async () => {
    vi.spyOn(apiClient, "getWarmth").mockResolvedValue({
      observed_at: "2026-07-29T12:00:00Z",
      pools: [
        { pool_id: "research/v5p", floor: 2, window_active: true, warm_cells: 2, warm_idle_cells: 1, hourly_cost: 8 },
      ],
    });
    vi.spyOn(apiClient, "getBilling").mockResolvedValue({
      observed_at: "2026-07-29T12:00:00Z",
      accrued_cost: 120.5,
      idle_cost: 30.25,
    });

    render(<FleetPage cells={cells} leases={leases} onRefresh={async () => {}} />);

    await waitFor(() => expect(screen.getByText("Warm capacity and cost")).toBeDefined());
    expect(screen.getByText("research/v5p")).toBeDefined();
    // Warm capacity is deliberately idle. Without the pairing the only safe
    // reaction to an unexplained idle figure is to delete the warm floor.
    expect(screen.getByText(/window active/)).toBeDefined();
  });

  it("names a cost failure rather than showing nothing", async () => {
    vi.spyOn(apiClient, "getWarmth").mockRejectedValue(new Error("Unable to reach Eyvan API"));
    vi.spyOn(apiClient, "getBilling").mockRejectedValue(new Error("Unable to reach Eyvan API"));

    render(<FleetPage cells={cells} leases={leases} onRefresh={async () => {}} />);

    await waitFor(() => expect(screen.getByText("Unable to reach Eyvan API")).toBeDefined());
  });

  it("says so when no pool declares a warm floor", async () => {
    // Distinct from a failure, and it has to look distinct: "nothing is warm"
    // is a fact about the fleet, while an empty panel is a fact about the page.
    vi.spyOn(apiClient, "getWarmth").mockResolvedValue({ observed_at: "2026-07-29T12:00:00Z", pools: [] });
    vi.spyOn(apiClient, "getBilling").mockResolvedValue({
      observed_at: "2026-07-29T12:00:00Z",
      accrued_cost: 0,
      idle_cost: 0,
    });

    render(<FleetPage cells={cells} leases={leases} onRefresh={async () => {}} />);

    await waitFor(() => expect(screen.getByText("No pool declares a warm floor.")).toBeDefined());
  });
});
