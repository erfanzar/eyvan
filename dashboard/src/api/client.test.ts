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
import { afterEach, describe, expect, it, vi } from "vitest";

import { cellStatus, computeOverview, EyvanApiClient, workloadStatus } from "./client";
import { consumeSessionNotice } from "../auth/oidc";

describe("EyvanApiClient", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  });

  it("reports an unreachable live API instead of substituting demo data", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("offline")));
    const client = new EyvanApiClient("/api/v1", false);

    await expect(client.listWorkloads()).rejects.toEqual(
      expect.objectContaining({ name: "ApiError", message: "Unable to reach Eyvan API: offline" }),
    );
  });

  it("reports server failures instead of substituting demo data", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(() => Promise.resolve(
        new Response(JSON.stringify({ detail: "database unavailable" }), {
          status: 503,
          headers: { "content-type": "application/json" },
        }),
      )),
    );
    const client = new EyvanApiClient("/api/v1", false);

    await expect(client.listCells()).rejects.toEqual(
      expect.objectContaining({
        name: "ApiError",
        status: 503,
        message: "Eyvan API request failed with 503: database unavailable",
      }),
    );
  });

  it("uses sample data only when demo mode is explicit", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", true);

    const workloads = await client.listWorkloads();

    expect(workloads.length).toBeGreaterThan(0);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("maps the versioned workload contract without granting default development identity", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([rawWorkload]), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const workloads = await client.listWorkloads();

    expect(workloads[0]).toMatchObject({ id: "wl-1", status: "running", priority: "production" });
    expect(fetch.mock.calls[0][0]).toContain("org_id=acme");
    expect(fetch.mock.calls[0][1].headers).not.toHaveProperty("X-Eyvan-User");
    expect(fetch.mock.calls[0][1].headers).not.toHaveProperty("Authorization");
  });

  it("does not fabricate per-workload budget or progress figures", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([rawWorkload]), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const workloads = await client.listWorkloads();

    expect(workloads[0].budgetSpent).toBeUndefined();
    expect(workloads[0].budgetLimit).toBeUndefined();
    expect(workloads[0].progress).toBeUndefined();
  });

  it("sends development identity headers only when explicitly enabled", async () => {
    vi.stubEnv("VITE_EYVAN_DEV_AUTH", "true");
    vi.stubEnv("VITE_EYVAN_DEV_USER", "local-operator");
    vi.stubEnv("VITE_EYVAN_DEV_ROLE", "operator");
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([rawWorkload]), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);

    await new EyvanApiClient("/api/v1", false).listWorkloads();

    expect(fetch.mock.calls[0][1].headers).toMatchObject({
      "X-Eyvan-User": "local-operator",
      "X-Eyvan-Roles": "operator",
    });
  });

  it("updates preemption through the versioned policy API without discarding placement constraints", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ...rawPolicy, preemption_enabled: false }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);
    const policy = {
      id: "acme/research",
      name: "Checkpoint-aware preemption",
      scope: "research",
      type: "preemption" as const,
      summary: "us-central2; spot",
      preemptionEnabled: true,
      updatedAt: rawPolicy.updated_at,
      configuration: {
        allowedProviders: ["gcp"],
        allowedRegions: ["us-central2"],
        allowedCapacityClasses: ["spot"],
        deniedAcceleratorTypes: ["v4"],
        maxPriority: 500,
      },
    };

    await expect(client.updatePolicy(policy, false)).resolves.toMatchObject({ preemptionEnabled: false });

    expect(fetch.mock.calls[0][0]).toBe("/api/v1/policies/acme/research");
    expect(fetch.mock.calls[0][1].method).toBe("PUT");
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toMatchObject({
      allowed_regions: ["us-central2"],
      denied_accelerator_types: ["v4"],
      preemption_enabled: false,
    });
  });

  it("maps policies from server fields instead of hardcoding preemption", async () => {
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([rawPolicy]), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const policies = await client.listPolicies();

    expect(policies[0]).toMatchObject({
      id: "acme/research",
      type: "placement",
      preemptionEnabled: true,
      summary: expect.stringContaining("us-central2"),
    });
    expect(policies[0].name).not.toBe("Checkpoint-aware preemption");
  });

  it("fetches each resource once when computing the overview from shared data", async () => {
    const fetch = vi.fn().mockImplementation((url: string) => {
      const response = (body: unknown, status = 200) =>
        new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
      if (url === "/api/v1/workloads") return Promise.resolve(response([rawWorkload]));
      if (url === "/api/v1/cells") return Promise.resolve(response([rawCell]));
      if (url === "/api/v1/budgets?org_id=acme") return Promise.resolve(response([rawBudget]));
      if (url === "/api/v1/leases?org_id=acme&project_id=research") return Promise.resolve(response([rawLease]));
      return Promise.resolve(response([]));
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const [workloads, leases, budgets] = await Promise.all([
      client.listWorkloads(),
      client.listLeases(),
      client.listBudgets(),
    ]);
    const cells = await client.listCells(leases);
    const overview = computeOverview(workloads, cells, budgets);

    const leaseCalls = fetch.mock.calls.filter(([path]) => String(path).startsWith("/api/v1/leases"));
    expect(leaseCalls).toHaveLength(1);
    expect(overview.queued).toBe(0);
    expect(cells[0].queuedWorkloads).toBeUndefined();
    expect(leases[0].class).toBeUndefined();
  });

  it("fetches and maps workload progress on the detail page", async () => {
    const progress = {
      workload_id: "wl-1",
      workload_state: "running",
      phase: "running",
      request: { cells: 4, workers_per_cell: 2 },
      capacity: { ready_workers: 5 },
    };
    const fetch = vi.fn().mockImplementation((url: string) => {
      const response = (body: unknown, status = 200) =>
        new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
      if (url === "/api/v1/workloads/wl-1") return Promise.resolve(response(rawWorkload));
      if (url === "/api/v1/workloads/wl-1/progress") return Promise.resolve(response(progress));
      if (url.startsWith("/api/v1/events")) return Promise.resolve(response([]));
      return Promise.resolve(new Response("", { status: 200 }));
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const detail = await client.getWorkload("wl-1");

    expect(detail.progress).toBe(63); // round(5 / 8 * 100) = 63
  });

  it("keeps the detail page usable when the progress endpoint is absent", async () => {
    const fetch = vi.fn().mockImplementation((url: string) => {
      const response = (body: unknown, status = 200) =>
        new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
      if (url === "/api/v1/workloads/wl-1") return Promise.resolve(response(rawWorkload));
      if (url === "/api/v1/workloads/wl-1/progress") return Promise.resolve(new Response("Not Found", { status: 404 }));
      if (url.startsWith("/api/v1/events")) return Promise.resolve(response([]));
      return Promise.resolve(new Response("", { status: 200 }));
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    await expect(client.getWorkload("wl-1")).resolves.toMatchObject({ id: "wl-1" });
  });

  it("parses timestamps, levels, and sources from log lines when present", async () => {
    const fetch = vi.fn().mockImplementation((url: string) => {
      const response = (body: unknown, status = 200) =>
        new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
      if (url === "/api/v1/workloads/wl-1") return Promise.resolve(response(rawWorkload));
      if (url.startsWith("/api/v1/events")) return Promise.resolve(response([]));
      if (url === "/api/v1/workloads/wl-1/logs") {
        return Promise.resolve(
          new Response(
            [
              "2026-01-01T00:00:10.123Z INFO [trainer] step=10 loss=2.1",
              "2026-01-01T00:00:11Z WARN shard latency high",
              "plain executor line without metadata",
            ].join("\n"),
            { status: 200 },
          ),
        );
      }
      return Promise.resolve(new Response("", { status: 200 }));
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const detail = await client.getWorkload("wl-1");

    expect(detail.logs[0]).toMatchObject({ timestamp: "2026-01-01T00:00:10.123Z", level: "INFO", source: "trainer", message: "step=10 loss=2.1" });
    expect(detail.logs[1]).toMatchObject({ timestamp: "2026-01-01T00:00:11Z", level: "WARN", source: undefined });
    expect(detail.logs[2]).toMatchObject({ timestamp: undefined, level: undefined, source: undefined, message: "plain executor line without metadata" });
  });

  it("sets a re-login notice when the control plane rejects the session", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "unauthorized" }), {
          status: 401,
          headers: { "content-type": "application/json" },
        }),
      ),
    );
    const client = new EyvanApiClient("/api/v1", false);

    await expect(client.listWorkloads()).rejects.toEqual(
      expect.objectContaining({ status: 401 }),
    );

    expect(consumeSessionNotice()).toMatch(/rejected/i);
  });

  it("registers demo workloads so detail lookups and actions resolve", async () => {
    const client = new EyvanApiClient("/api/v1", true);

    const workload = await client.submitWorkload({
      name: "demo-run",
      project: "research",
      priority: "batch",
      capacityClass: "spot",
      command: "python demo.py",
      resources: { cpu: 2, memoryGb: 4, replicas: 1 },
      estimatedDurationSeconds: 3600,
      preemptible: false,
      labels: {},
    });

    await expect(client.getWorkload(workload.id)).resolves.toMatchObject({ id: workload.id, name: "demo-run" });
    await expect(client.workloadAction(workload.id, "cancel")).resolves.toMatchObject({ status: "preempting" });
  });
  it("populates the allocation panel from the workload's own leases", async () => {
    // This used to be hardcoded to [], so the panel rendered nothing against a
    // live server while the mock data filled it in: the page looked implemented
    // and was not.
    const routes: Record<string, unknown> = {
      "/workloads/wl-1": rawWorkload,
      "/events": [],
      "/workloads/wl-1/progress": undefined,
      "/leases": [rawLease],
    };
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation((url: string) => {
        if (url.includes("/logs")) return Promise.resolve(new Response("", { status: 200 }));
        // Longest first: "/workloads/wl-1" would otherwise shadow
        // "/workloads/wl-1/progress".
        const key = Object.keys(routes)
          .sort((a, b) => b.length - a.length)
          .find((route) => url.includes(route));
        const body = key ? routes[key] : undefined;
        return Promise.resolve(
          new Response(JSON.stringify(body ?? null), {
            status: body === undefined ? 404 : 200,
            headers: { "content-type": "application/json" },
          }),
        );
      }),
    );

    const detail = await new EyvanApiClient("/api/v1", false).getWorkload("wl-1");

    expect(detail.allocation).toEqual([
      { host: "cell-1", rack: "active", resource: "8 TPU, 8 CPU", state: "ready" },
    ]);
  });

});

const rawWorkload = {
  id: "wl-1",
  org_id: "acme",
  project_id: "research",
  state: "running",
  attempt: 1,
  created_by: "alice",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:01:00Z",
  spec: {
    name: "train",
    execution: { kind: "command", command: "python train.py" },
    gang: {
      replicas: 1,
      per_replica: { cpu: 8, memory_gib: 64, gpu: 0, tpu: 8, custom: {} },
      required_topology: { slice: "single-slice" },
    },
    metadata: {},
    accelerator_type: "v5p-8",
    priority_band: "production",
  },
};

const rawPolicy = {
  org_id: "acme",
  project_id: "research",
  allowed_providers: ["gcp"],
  allowed_regions: ["us-central2"],
  allowed_capacity_classes: ["spot"],
  denied_accelerator_types: ["v4"],
  max_priority: 500,
  preemption_enabled: true,
  updated_at: "2026-01-01T00:00:00Z",
};

const rawCell = {
  id: "cell-1",
  provider: "gcp",
  provider_project: "demo",
  region: "us-central",
  zone: "usc1-a",
  state: "ready",
  capacity: { cpu: 100, memory_gib: 1024, gpu: 0, tpu: 16, custom: {} },
  allocatable: { cpu: 80, memory_gib: 800, gpu: 0, tpu: 12, custom: {} },
  accelerator_type: "v5p-8",
  labels: {},
};

const rawBudget = {
  org_id: "acme",
  project_id: "research",
  spent: 100,
  limit: 1000,
  period: "monthly",
  hard_limit: false,
  accelerator_hour_limits: {},
  accelerator_hours_spent: {},
  currency: "USD",
};

const rawLease = {
  id: "lse-1",
  workload_id: "wl-1",
  project_id: "research",
  cell_id: "cell-1",
  state: "active",
  resources: { replicas: 1, per_replica: { cpu: 8, memory_gib: 64, gpu: 0, tpu: 8, custom: {} } },
  hourly_cost: 12,
  expires_at: "2026-01-02T00:00:00Z",
};

describe("dead field revival", () => {
  it("populates queueTrend from the ledger-derived route instead of hard-coding []", async () => {
    const fetch = vi.fn().mockImplementation((url: string) => {
      if (String(url).includes("/capacity/queue-trend")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              observed_at: "2026-08-01T00:00:00Z",
              bucket_seconds: 3600,
              points: [
                { started_at: "2026-07-31T22:00:00Z", submitted: 4, admitted: 2 },
                { started_at: "2026-07-31T23:00:00Z", submitted: 1, admitted: 5 },
              ],
            }),
            { status: 200, headers: { "content-type": "application/json" } },
          ),
        );
      }
      return Promise.resolve(
        new Response(JSON.stringify([]), { status: 200, headers: { "content-type": "application/json" } }),
      );
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const trend = await client.getQueueTrend();

    expect(trend).toEqual([
      { label: "22:00", queued: 4, admitted: 2 },
      { label: "23:00", queued: 1, admitted: 5 },
    ]);
  });

  it("renders an empty trend rather than failing the whole overview", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response("nope", { status: 500 }));
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    await expect(client.getQueueTrend()).resolves.toEqual([]);
  });

  it("joins a lease's capacity class from the cell it landed on", async () => {
    const fetch = vi.fn().mockImplementation((url: string) => {
      const body = String(url).includes("/cells")
        ? [
            {
              id: "cell-a",
              provider: "gcp",
              region: "us-east5",
              state: "ready",
              capacity: { cpu: 8, memory_gib: 32, gpu: 0, tpu: 8, custom: {} },
              allocatable: { cpu: 8, memory_gib: 32, gpu: 0, tpu: 8, custom: {} },
              capacity_class: "spot",
              labels: {},
            },
          ]
        : [
            {
              id: "lease-1",
              workload_id: "wl-1",
              project_id: "research",
              cell_id: "cell-a",
              state: "active",
              resources: { replicas: 1, per_replica: { cpu: 1, memory_gib: 1, gpu: 0, tpu: 8, custom: {} } },
              expires_at: "2026-08-02T00:00:00Z",
              hourly_cost: 12,
            },
          ];
      return Promise.resolve(
        new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } }),
      );
    });
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    const leases = await client.listLeases();

    expect(leases[0].class).toBe("spot");
  });

  it("does not recurse between listLeases and listCells", async () => {
    // listCells resolves leases to compute utilization, so a naive join in
    // listLeases would loop forever. Bounded call count is the assertion.
    const fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify([]), { status: 200, headers: { "content-type": "application/json" } }),
    );
    vi.stubGlobal("fetch", fetch);
    const client = new EyvanApiClient("/api/v1", false);

    await client.listLeases();

    expect(fetch.mock.calls.length).toBeLessThanOrEqual(2);
  });
});

describe("workload status mapping", () => {
  it("keeps a cancellation distinct from a failure", () => {
    // Seen live on 2026-08-01: four workloads the operator cancelled were
    // rendered "Failed", and the Failed tile read 5 when one job had failed.
    // "Your job broke" and "you stopped it" are different facts, and the tile
    // is the first thing read on the page.
    expect(workloadStatus("cancelled")).toBe("cancelled");
    expect(workloadStatus("failed")).toBe("failed");
  });

  it("still reports an unknown state as queued rather than inventing one", () => {
    expect(workloadStatus("some-future-state")).toBe("queued");
  });
});

describe("cell status mapping", () => {
  it("does not call a cell waiting for capacity degraded", () => {
    // On a Spot TPU pool `registering` is where a cell spends most of its life:
    // the queued resource exists and Google has not granted it yet. Rendering
    // that in red made a healthy fleet look broken, which is the fastest way to
    // make an operator stop trusting the page.
    expect(cellStatus("registering")).toBe("starting");
    expect(cellStatus("ready")).toBe("healthy");
  });

  it("keeps a genuinely broken cell degraded", () => {
    expect(cellStatus("unavailable")).toBe("degraded");
  });

  it("treats teardown as maintenance rather than failure", () => {
    expect(cellStatus("draining")).toBe("maintenance");
    expect(cellStatus("deleting")).toBe("maintenance");
  });
});
