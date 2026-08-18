// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

/**
 * Seeing what is applied, and stopping it, without leaving the browser.
 *
 * The fleet page could only ever show cells -- capacity provisioned right now
 * -- so a fleet with twenty pools and nothing running was indistinguishable
 * from no fleet at all. And the route that stops a fleet has existed since
 * fleets did, with nothing in the dashboard calling it: applying and tearing
 * down were the two steps a team had to leave the UI for.
 */

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FleetPage } from "./FleetPage";
import type { CapacityPoolSummary } from "../types";

const POOLS: CapacityPoolSummary[] = [
  { id: "research-v6e-8", fleetName: "research", acceleratorType: "v6e-8", capacityClass: "spot", zone: "asia-east1-c", region: "asia-east1", maxCells: 10000, minCells: 0, enabled: true },
  { id: "research-v6e-64", fleetName: "research", acceleratorType: "v6e-64", capacityClass: "spot", zone: "asia-east1-c", region: "asia-east1", maxCells: 10000, minCells: 0, enabled: true },
  { id: "laptop-cpu", fleetName: "laptop", acceleratorType: null, capacityClass: "on_demand", zone: null, region: "local", maxCells: 2, minCells: 1, enabled: true },
];

function renderPage(overrides: Partial<Parameters<typeof FleetPage>[0]> = {}) {
  return render(
    <FleetPage
      cells={[]}
      leases={[]}
      onRefresh={async () => {}}
      loadWarmth={async () => ({ observed_at: "", pools: [] })}
      loadBilling={async () => ({ observed_at: "", total_accrued: 0, total_idle: 0, cells: [] }) as never}
      loadLaunchLatency={async () => ({ observed_at: "", pools: [] })}
      loadPools={async () => POOLS}
      {...overrides}
    />,
  );
}

afterEach(cleanup);

function appliedFleets(): HTMLElement {
  return screen.getByRole("region", { name: "Applied fleets" });
}

describe("applied fleets", () => {
  it("lists what is registered even when nothing is running", async () => {
    renderPage();

    await waitFor(() => expect(appliedFleets()).toBeTruthy());
    const rows = within(appliedFleets()).getAllByRole("row").slice(1);

    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.textContent)).toEqual([
      expect.stringContaining("laptop"),
      expect.stringContaining("research"),
    ]);
  });

  it("groups a fleet's pools rather than listing each one", async () => {
    renderPage();

    await waitFor(() => expect(appliedFleets()).toBeTruthy());
    const research = within(appliedFleets())
      .getAllByRole("row")
      .find((row) => row.textContent?.includes("research"))!;

    expect(research.textContent).toContain("v6e-8");
    expect(research.textContent).toContain("asia-east1-c");
  });

  it("offers no stop control to a principal who may not manage the fleet", async () => {
    // The route would refuse them anyway; offering the button teaches a gesture
    // that ends in a 403.
    renderPage({ onStopFleet: undefined });

    await waitFor(() => expect(appliedFleets()).toBeTruthy());

    expect(within(appliedFleets()).queryByRole("button", { name: "Stop" })).toBeNull();
  });

  it("confirms before stopping, and names what stopping does", async () => {
    const onStopFleet = vi.fn(async () => {});
    renderPage({ onStopFleet });

    await waitFor(() => expect(appliedFleets()).toBeTruthy());
    fireEvent.click(within(appliedFleets()).getAllByRole("button", { name: "Stop" })[0]);

    // Not fired on the click that opened the dialog.
    expect(onStopFleet).not.toHaveBeenCalled();
    expect(screen.getByText(/ceiling set to zero/)).toBeTruthy();
    // The definition surviving is the difference between this and a delete, and
    // it is what makes the button safe to press.
    expect(screen.getByText(/definition survives/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Stop fleet" }));
    await waitFor(() => expect(onStopFleet).toHaveBeenCalledWith("laptop"));
  });

  it("reports a refused stop instead of appearing to succeed", async () => {
    const onStopFleet = vi.fn(async () => {
      throw new Error("This session may not manage fleet capacity");
    });
    renderPage({ onStopFleet });

    await waitFor(() => expect(appliedFleets()).toBeTruthy());
    fireEvent.click(within(appliedFleets()).getAllByRole("button", { name: "Stop" })[0]);
    fireEvent.click(screen.getByRole("button", { name: "Stop fleet" }));

    await waitFor(() => expect(screen.getByText(/may not manage fleet capacity/)).toBeTruthy());
  });
});
