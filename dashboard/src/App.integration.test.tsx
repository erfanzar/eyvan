// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { ApiError, type EyvanApiClient } from "./api/client";
import {
  getMockWorkload,
  mockAuditEvents,
  mockBudgets,
  mockCells,
  mockLeases,
  mockPolicies,
  mockWorkloads,
} from "./api/mockData";
import { NO_AUTH_CAPABILITIES, type AuthIdentity, type AuthScope } from "./auth/oidc";
import type {
  EffectiveProjectScope,
  SchedulingPolicy,
  SessionAuthority,
  SessionPermission,
  WorkloadDetail,
  WorkloadSummary,
} from "./types";

const PERMISSIONS: Record<string, SessionPermission[]> = {
  project_viewer: ["read"],
  project_submitter: ["submit"],
  project_operator: ["read", "submit", "operate"],
  project_admin: ["read", "submit", "operate", "manage_project"],
  org_admin: ["read", "submit", "operate", "manage_project", "manage_own_capacity"],
  fleet_admin: ["read", "submit", "operate", "manage_project", "manage_own_capacity", "manage_fleet"],
};

function effectiveScope(
  role: string,
  scope: AuthScope = { orgId: "acme", projectId: "research" },
  overrides: Partial<EffectiveProjectScope> = {},
): EffectiveProjectScope {
  return {
    ...scope,
    displayName: `${scope.orgId}/${scope.projectId}`,
    registered: true,
    enabled: true,
    acceptsNewWork: true,
    roles: [role],
    permissions: PERMISSIONS[role] ?? [],
    ...overrides,
  };
}

function authority(role: string, scopes = [effectiveScope(role)]): SessionAuthority {
  return {
    subject: "alice",
    displayName: "Alice",
    globalPermissions: role === "fleet_admin" ? [...PERMISSIONS.fleet_admin] : [],
    scopes,
  };
}

function identity(): AuthIdentity {
  return {
    displayName: "Alice",
    detail: "alice@example.com",
    mode: "oidc",
    roles: [],
    scope: { orgId: "acme", projectId: "research" },
    capabilities: NO_AUTH_CAPABILITIES,
  };
}

function scopedWorkload(scope: AuthScope, name = `${scope.orgId}-${scope.projectId}-workload`): WorkloadDetail {
  const detail = getMockWorkload(mockWorkloads[0].id);
  if (!detail) throw new Error("mock workload detail is missing");
  return {
    ...structuredClone(detail),
    id: `wl-${scope.orgId}-${scope.projectId}`,
    name,
    org: scope.orgId,
    project: scope.projectId,
  };
}

function policyFor(scope: AuthScope): SchedulingPolicy {
  return {
    ...mockPolicies[0],
    id: `${scope.orgId}/${scope.projectId}`,
    name: `Placement policy (${scope.orgId}/${scope.projectId})`,
    scope: scope.projectId,
    type: "placement",
    configuration: {
      allowedProviders: ["gcp"],
      allowedRegions: [],
      allowedCapacityClasses: [],
      deniedAcceleratorTypes: [],
      maxPriority: 1000,
    },
  };
}

type FakeOverrides = Partial<{
  getSessionAuthority: EyvanApiClient["getSessionAuthority"];
  listWorkloads: EyvanApiClient["listWorkloads"];
  listPolicies: EyvanApiClient["listPolicies"];
  submitWorkload: EyvanApiClient["submitWorkload"];
  updatePolicy: EyvanApiClient["updatePolicy"];
  getWorkload: EyvanApiClient["getWorkload"];
  workloadAction: EyvanApiClient["workloadAction"];
  followEvents: EyvanApiClient["followEvents"];
}>;

function fakeClient(session: SessionAuthority, overrides: FakeOverrides = {}) {
  const streamCallbacks: Array<() => void> = [];
  const client = {
    getSessionAuthority: vi.fn(overrides.getSessionAuthority ?? (async () => session)),
    getHealth: vi.fn(async () => ({ status: "ok", service: "eyvan" })),
    listWorkloads: vi.fn(
      overrides.listWorkloads ??
        (async (_filters, scope) => [scopedWorkload(scope ?? { orgId: "acme", projectId: "research" })]),
    ),
    listLeases: vi.fn(async () => mockLeases.slice(0, 1)),
    listBudgets: vi.fn(async () => mockBudgets.slice(0, 1)),
    listPolicies: vi.fn(
      overrides.listPolicies ??
        (async (scope) => [policyFor(scope ?? { orgId: "acme", projectId: "research" })]),
    ),
    getQueueTrend: vi.fn(async () => [{ label: "10:00", submitted: 1, admitted: 1 }]),
    listCells: vi.fn(async () => mockCells.slice(0, 1)),
    listAuditEvents: vi.fn(async () => mockAuditEvents.slice(0, 1)),
    followEvents: vi.fn(
      overrides.followEvents ??
        (async (
          onEvent: () => void,
          signal: AbortSignal,
          onConnectionChange?: (connected: boolean) => void,
        ) => {
          streamCallbacks.push(onEvent);
          onConnectionChange?.(true);
          await new Promise<void>((resolve) => {
            if (signal.aborted) resolve();
            else signal.addEventListener("abort", () => resolve(), { once: true });
          });
        }),
    ),
    getWorkload: vi.fn(
      overrides.getWorkload ??
        (async (id, scope) => ({
          ...scopedWorkload(scope ?? { orgId: "acme", projectId: "research" }),
          id,
        })),
    ),
    explainWorkload: vi.fn(async (id: string) => ({
      workload_id: id,
      state: "running",
      placeable: true,
      placement_cell_id: "cell-a",
      code: null,
      message: null,
      cells: [],
      considered_cell_count: 1,
      provisioning_cell_ids: [],
      scheduling_reason: null,
    })),
    workloadProcessStatus: vi.fn(async () => ({})),
    getWarmth: vi.fn(async () => ({ observed_at: "2026-01-01T00:00:00Z", pools: [] })),
    getBilling: vi.fn(async () => ({ observed_at: "2026-01-01T00:00:00Z", accrued_cost: 0, idle_cost: 0 })),
    getLaunchLatency: vi.fn(async () => ({ observed_at: "2026-01-01T00:00:00Z", pools: [] })),
    submitWorkload: vi.fn(
      overrides.submitWorkload ??
        (async (input, scope) => scopedWorkload(scope ?? { orgId: "acme", projectId: input.project }, input.name)),
    ),
    workloadAction: vi.fn(
      overrides.workloadAction ??
        (async (id, action, scope) => ({
          ...scopedWorkload(scope ?? { orgId: "acme", projectId: "research" }),
          id,
          status: action === "cancel" ? "preempting" : action === "checkpoint" ? "checkpointing" : "queued",
        })),
    ),
    updatePolicy: vi.fn(
      overrides.updatePolicy ??
        (async (policy, enabled, scope) => ({
          ...policy,
          id: `${scope?.orgId}/${scope?.projectId}`,
          scope: scope?.projectId ?? policy.scope,
          preemptionEnabled: enabled,
        })),
    ),
  };
  return { client: client as unknown as EyvanApiClient, calls: client, streamCallbacks };
}

function renderApp(client: EyvanApiClient) {
  return render(<App client={client} identityProvider={identity} />);
}

async function showPanel(name: string) {
  fireEvent.click(screen.getByRole("button", { name }));
  await waitFor(() => expect(screen.getByRole("heading", { name })).toBeDefined());
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

afterEach(() => {
  cleanup();
  window.history.replaceState(null, "", "/");
  vi.restoreAllMocks();
});

describe("App composes server session authority", () => {
  it.each([
    ["project_viewer", false, false, false],
    ["project_submitter", true, false, false],
    ["project_operator", true, false, false],
    ["project_admin", true, true, false],
    ["org_admin", true, true, false],
    ["fleet_admin", true, true, true],
  ])(
    "renders real panels and actions for %s",
    async (role, canSubmit, canManageProject, canManageFleet) => {
      const { client } = fakeClient(authority(role));
      renderApp(client);

      await waitFor(() => expect(screen.getByRole("heading", { name: "Operations overview" })).toBeDefined());
      expect(screen.queryByRole("button", { name: "Submit workload" }) !== null).toBe(canSubmit);
      expect(screen.queryByRole("button", { name: "Fleet" }) !== null).toBe(canManageFleet);

      await showPanel("Workloads");
      expect(screen.queryByRole("button", { name: "New workload" }) !== null).toBe(canSubmit);

      if (role !== "project_submitter") {
        await showPanel("Budgets & policies");
        expect(screen.queryAllByRole("checkbox").length > 0).toBe(canManageProject);
        await showPanel("Audit events");
        expect(screen.getByText(mockAuditEvents[0].action)).toBeDefined();
      }

      if (canManageFleet) {
        await showPanel("Fleet");
        expect(screen.getByText("Fleet topology")).toBeDefined();
      }
    },
  );

  it.each([
    ["project_viewer", false],
    ["project_operator", true],
    ["project_admin", true],
    ["org_admin", true],
    ["fleet_admin", true],
  ])("uses resource-scoped session authority for %s detail actions", async (role, canOperate) => {
    window.location.hash = "#/workloads/wl-deep";
    const { client, calls } = fakeClient(authority(role));
    renderApp(client);

    await waitFor(() => expect(screen.getByRole("heading", { name: "Workload detail" })).toBeDefined());
    await waitFor(() => expect(calls.getWorkload).toHaveBeenCalled());
    await waitFor(() => expect(screen.getByText("acme-research-workload")).toBeDefined());
    expect(screen.queryByRole("button", { name: "Cancel" }) !== null).toBe(canOperate);
    expect(calls.getSessionAuthority).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "acme", projectId: "research" }),
      expect.any(AbortSignal),
    );
  });

  it("keeps a disabled registered project selectable and readable but suppresses every tenant mutation", async () => {
    const disabled = effectiveScope("project_admin", undefined, {
      enabled: false,
      acceptsNewWork: false,
    });
    const { client } = fakeClient(authority("project_admin", [disabled]));
    renderApp(client);

    await waitFor(() => expect(screen.getByRole("heading", { name: "Operations overview" })).toBeDefined());
    const selector = screen.getByRole("combobox", { name: "Project scope" }) as HTMLSelectElement;
    expect(selector.options[0].disabled).toBe(false);
    expect(selector.options[0].textContent).toContain("history only");
    expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull();

    await showPanel("Budgets & policies");
    expect(screen.queryByRole("checkbox")).toBeNull();
    expect(screen.getByText("Read only")).toBeDefined();
  });
});

describe("App scope and authority race guards", () => {
  it("never lets a deferred A refresh overwrite a fast B scope", async () => {
    const a = { orgId: "acme", projectId: "research" };
    const b = { orgId: "other", projectId: "research" };
    const session = authority("project_viewer", [
      effectiveScope("project_viewer", a),
      effectiveScope("project_viewer", b),
    ]);
    const staleA = deferred<WorkloadSummary[]>();
    let aReads = 0;
    const { client, calls } = fakeClient(session, {
      listWorkloads: async (_filters, scope) => {
        if (scope?.orgId === "acme") {
          aReads += 1;
          if (aReads > 1) return staleA.promise;
          return [scopedWorkload(a, "A current")];
        }
        return [scopedWorkload(b, "B current")];
      },
    });
    renderApp(client);
    await waitFor(() => expect(screen.getByText("A current")).toBeDefined());

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.listWorkloads).toHaveBeenCalledTimes(2));
    fireEvent.change(screen.getByRole("combobox", { name: "Project scope" }), {
      target: { value: JSON.stringify([b.orgId, b.projectId]) },
    });
    await waitFor(() => expect(screen.getByText("B current")).toBeDefined());

    await act(async () => staleA.resolve([scopedWorkload(a, "A stale")]));
    expect(screen.queryByText("A stale")).toBeNull();
    expect(screen.getByText("B current")).toBeDefined();
  });

  it("binds a pending policy mutation to immutable A, never same-named project B", async () => {
    const a = { orgId: "acme", projectId: "research" };
    const b = { orgId: "other", projectId: "research" };
    const session = authority("project_admin", [
      effectiveScope("project_admin", a),
      effectiveScope("project_admin", b),
    ]);
    const pending = deferred<SchedulingPolicy>();
    const updatePolicy = vi.fn(async (policy, enabled, scope) => {
      await pending.promise;
      return { ...policy, id: `${scope?.orgId}/${scope?.projectId}`, preemptionEnabled: enabled };
    });
    const { client } = fakeClient(session, { updatePolicy });
    renderApp(client);
    await waitFor(() => expect(screen.getByRole("heading", { name: "Operations overview" })).toBeDefined());
    await showPanel("Budgets & policies");

    fireEvent.click(screen.getAllByRole("checkbox")[0]);
    await waitFor(() => expect(updatePolicy).toHaveBeenCalled());
    expect(updatePolicy.mock.calls[0][2]).toEqual(expect.objectContaining(a));

    fireEvent.change(screen.getByRole("combobox", { name: "Project scope" }), {
      target: { value: JSON.stringify([b.orgId, b.projectId]) },
    });
    await waitFor(() => expect(screen.getByText(`Placement policy (${b.orgId}/${b.projectId})`)).toBeDefined());
    await act(async () => pending.resolve(policyFor(a)));
    await showPanel("Budgets & policies");

    expect(screen.getByText(`Placement policy (${b.orgId}/${b.projectId})`)).toBeDefined();
    expect(updatePolicy.mock.calls[0][2]).not.toEqual(expect.objectContaining(b));
  });

  it("refreshes durable additions and revocations and reconciles the selected scope", async () => {
    const a = { orgId: "acme", projectId: "research" };
    const b = { orgId: "acme", projectId: "platform" };
    const sessions = [
      authority("project_operator", [effectiveScope("project_operator", a)]),
      authority("project_viewer", [
        effectiveScope("project_viewer", a),
        effectiveScope("project_operator", b),
      ]),
      authority("project_operator", [effectiveScope("project_operator", b)]),
    ];
    let index = 0;
    const { client, calls } = fakeClient(sessions[0], {
      getSessionAuthority: async () => sessions[Math.min(index++, sessions.length - 1)],
    });
    renderApp(client);
    await waitFor(() => expect(screen.getByRole("button", { name: "Submit workload" })).toBeDefined());

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull());
    expect(screen.getByRole("combobox", { name: "Project scope" }).querySelectorAll("option")).toHaveLength(2);

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(screen.getByRole("button", { name: "Submit workload" })).toBeDefined());
    expect((screen.getByRole("combobox", { name: "Project scope" }) as HTMLSelectElement).value).toBe(
      JSON.stringify([b.orgId, b.projectId]),
    );
  });

  it("re-resolves /session after a 403 before rendering any stale capability", async () => {
    const operator = authority("project_operator");
    const viewer = authority("project_viewer");
    let sessionCall = 0;
    let workloadCall = 0;
    const { client, calls } = fakeClient(operator, {
      getSessionAuthority: async () => (sessionCall++ < 1 ? operator : viewer),
      listWorkloads: async (_filters, scope) => {
        workloadCall += 1;
        if (workloadCall === 2) throw new ApiError("revoked", 403);
        return [scopedWorkload(scope ?? { orgId: "acme", projectId: "research" })];
      },
    });
    renderApp(client);
    await waitFor(() => expect(screen.getByRole("button", { name: "Submit workload" })).toBeDefined());

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull());
  });

  it("drops stale workload actions when a mutation 403 refreshes durable authority", async () => {
    window.location.hash = "#/workloads/wl-revoked";
    const operator = authority("project_operator");
    const viewer = authority("project_viewer");
    let current = operator;
    const { client, calls } = fakeClient(operator, {
      getSessionAuthority: async () => current,
      workloadAction: async () => {
        current = viewer;
        throw new ApiError("operate permission revoked", 403);
      },
    });
    renderApp(client);

    await waitFor(() => expect(screen.getByRole("button", { name: "Cancel" })).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => expect(calls.workloadAction).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull());
    expect(calls.getSessionAuthority.mock.calls.length).toBeGreaterThanOrEqual(3);
  });

  it("keeps one scoped SSE subscription when refresh decodes fresh scope objects", async () => {
    const session = authority("project_viewer");
    const followEvents = vi.fn(
      async (
        onEvent: Parameters<EyvanApiClient["followEvents"]>[0],
        signal: AbortSignal,
        onConnectionChange?: Parameters<EyvanApiClient["followEvents"]>[2],
      ) => {
        onConnectionChange?.(true);
        // The endpoint replays retained history when no Last-Event-ID is sent.
        // Re-subscribing after this refresh would replay it again and create a
        // self-sustaining refresh/reconnect loop.
        onEvent({ id: "1", event: "workload.submitted", data: { sequence: 1 } });
        await new Promise<void>((resolve) => {
          if (signal.aborted) resolve();
          else signal.addEventListener("abort", () => resolve(), { once: true });
        });
      },
    );
    const { client, calls } = fakeClient(session, {
      // Match production decoding: every /session response materializes new
      // authority and scope objects even when their values are unchanged.
      getSessionAuthority: async () => structuredClone(session),
      followEvents,
    });
    renderApp(client);

    await waitFor(() => expect(followEvents).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(2), { timeout: 1_500 });
    await act(async () => {
      await new Promise((resolve) => globalThis.setTimeout(resolve, 550));
    });

    expect(followEvents).toHaveBeenCalledTimes(1);
    expect(calls.getSessionAuthority).toHaveBeenCalledTimes(2);
  });

  it("fences stale controls on a dashboard 403 before failed reconciliation completes", async () => {
    const operator = authority("project_operator");
    const reconciliation = deferred<SessionAuthority>();
    let sessionCall = 0;
    let workloadCall = 0;
    const { client, calls } = fakeClient(operator, {
      getSessionAuthority: async () => {
        sessionCall += 1;
        if (sessionCall < 3) return structuredClone(operator);
        return reconciliation.promise;
      },
      listWorkloads: async (_filters, scope) => {
        workloadCall += 1;
        if (workloadCall === 2) throw new ApiError("authority revoked", 403);
        return [scopedWorkload(scope ?? { orgId: "acme", projectId: "research" })];
      },
    });
    renderApp(client);
    await waitFor(() => expect(screen.getByRole("button", { name: "Submit workload" })).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: "Submit workload" }));
    expect(screen.getByRole("dialog", { name: "Submit workload" })).toBeDefined();

    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(3));

    // /session is still pending: the first typed denial, not its eventual
    // reconciliation result, must remove every stale mutation affordance.
    expect(screen.queryByRole("dialog", { name: "Submit workload" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull();

    await act(async () => reconciliation.reject(new ApiError("session unavailable", 503)));
    await waitFor(() => expect(screen.getByText("session unavailable")).toBeDefined());
    expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull();
  });

  it("fences detail actions and an open modal when SSE denial reconciliation fails", async () => {
    window.location.hash = "#/workloads/wl-stream-revoked";
    const operator = authority("project_operator");
    const streamDenied = deferred<void>();
    const reconciliation = deferred<SessionAuthority>();
    let sessionCall = 0;
    const followEvents = vi.fn(async () => {
      await streamDenied.promise;
      throw new ApiError("event stream denied", 403);
    });
    const { client, calls } = fakeClient(operator, {
      getSessionAuthority: async () => {
        sessionCall += 1;
        if (sessionCall < 3) return structuredClone(operator);
        return reconciliation.promise;
      },
      followEvents,
    });
    renderApp(client);

    await waitFor(() => expect(screen.getByRole("button", { name: "Cancel" })).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: "Submit workload" }));
    expect(screen.getByRole("dialog", { name: "Submit workload" })).toBeDefined();

    await act(async () => streamDenied.resolve());
    await waitFor(() => expect(calls.getSessionAuthority).toHaveBeenCalledTimes(3));
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
    expect(screen.queryByRole("dialog", { name: "Submit workload" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull();

    await act(async () => reconciliation.reject(new ApiError("session reconciliation failed", 503)));
    await waitFor(() => expect(screen.getByText("session reconciliation failed")).toBeDefined());
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Submit workload" })).toBeNull();
  });

  it("retries bootstrap by resolving /session again", async () => {
    const session = authority("project_viewer");
    let attempts = 0;
    const { client, calls } = fakeClient(session, {
      getSessionAuthority: async () => {
        attempts += 1;
        if (attempts === 1) throw new ApiError("session unavailable", 503);
        return session;
      },
    });
    renderApp(client);

    await waitFor(() => expect(screen.getByText("session unavailable")).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(screen.getByRole("heading", { name: "Operations overview" })).toBeDefined());
    expect(calls.getSessionAuthority).toHaveBeenCalledTimes(2);
  });
});

describe("submit-only and wildcard deep links", () => {
  it("shows a durable accessible submit-only acknowledgement and issues no detail or poll reads", async () => {
    const session = authority("project_submitter");
    const { client, calls } = fakeClient(session);
    renderApp(client);
    await waitFor(() => expect(screen.getByRole("button", { name: "Submit workload" })).toBeDefined());

    fireEvent.click(screen.getByRole("button", { name: "Submit workload" }));
    const dialog = screen.getByRole("dialog", { name: "Submit workload" });
    fireEvent.change(within(dialog).getByLabelText("Workload name"), { target: { value: "submit-only-job" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Submit workload" }));

    await waitFor(() => expect(screen.getByText("Workload submitted")).toBeDefined());
    expect(screen.getByText(/submit-only-job.*cannot read workload detail/)).toBeDefined();
    act(() => document.dispatchEvent(new Event("visibilitychange")));
    await waitFor(() => expect(calls.getSessionAuthority.mock.calls.length).toBeGreaterThan(1));

    expect(calls.listWorkloads).not.toHaveBeenCalled();
    expect(calls.listLeases).not.toHaveBeenCalled();
    expect(calls.getQueueTrend).not.toHaveBeenCalled();
    expect(calls.getWorkload).not.toHaveBeenCalled();
    expect(calls.explainWorkload).not.toHaveBeenCalled();
  });

  it("uses an authenticated scoped session query for an org-admin deep link into legacy acme/other", async () => {
    window.location.hash = "#/workloads/wl-other";
    const research = effectiveScope("org_admin", { orgId: "acme", projectId: "research" }, { registered: false });
    const other = effectiveScope("org_admin", { orgId: "acme", projectId: "other" }, { registered: false });
    const initial = authority("org_admin", [research]);
    const scoped = authority("org_admin", [research, other]);
    const { client, calls } = fakeClient(initial, {
      getSessionAuthority: async (preferred) => (preferred?.projectId === "other" ? scoped : initial),
      getWorkload: async () => ({
        ...scopedWorkload({ orgId: "acme", projectId: "other" }, "legacy other"),
        id: "wl-other",
      }),
    });
    renderApp(client);

    await waitFor(() => expect(screen.getByText("legacy other")).toBeDefined());
    expect(screen.getByRole("button", { name: "Cancel" })).toBeDefined();
    expect(calls.getSessionAuthority).toHaveBeenCalledWith(
      { orgId: "acme", projectId: "other" },
      expect.any(AbortSignal),
    );
  });
});
