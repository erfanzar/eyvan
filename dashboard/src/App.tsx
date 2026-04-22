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
import { AlertTriangle, CheckCircle2, X } from "lucide-react";
import { useCallback, useEffect, useState, type ReactNode } from "react";

import { apiClient, computeOverview } from "./api/client";
import { currentIdentity, logout } from "./auth/oidc";
import { ErrorState, LoadingState } from "./components/PageState";
import { Shell } from "./components/Shell";
import { SubmitWorkloadModal } from "./components/SubmitWorkloadModal";
import { useVisibilityPolling } from "./lib/useVisibilityPolling";
import { AuditPage } from "./pages/AuditPage";
import { BudgetsPage } from "./pages/BudgetsPage";
import { FleetPage } from "./pages/FleetPage";
import { OverviewPage } from "./pages/OverviewPage";
import { WorkloadDetailPage } from "./pages/WorkloadDetailPage";
import { WorkloadsPage } from "./pages/WorkloadsPage";
import { navigate, parseRoute, type Route, type Section } from "./routing";
import type { DashboardData, SchedulingPolicy, SubmitWorkloadInput, WorkloadDetail } from "./types";

const REFRESH_INTERVAL_MS = 8_000;

export default function App() {
  const [route, setRoute] = useState<Route>(() => parseRoute());
  const [data, setData] = useState<DashboardData | null>(null);
  const [selectedWorkload, setSelectedWorkload] = useState<WorkloadDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [submitOpen, setSubmitOpen] = useState(false);
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const [toast, setToast] = useState<{ message: string; error?: boolean } | null>(null);

  // Each resource is fetched once per refresh; the overview is computed from the shared data.
  const loadDashboard = useCallback(async (background = false) => {
    try {
      const [health, workloads, leases, budgets, policies, auditEvents] = await Promise.all([
        apiClient.getHealth(),
        apiClient.listWorkloads(),
        apiClient.listLeases(),
        apiClient.listBudgets(),
        apiClient.listPolicies(),
        apiClient.listAuditEvents(),
      ]);
      const cells = await apiClient.listCells(leases);
      const overview = computeOverview(workloads, cells, budgets);
      setData({ health, overview, workloads, cells, leases, budgets, policies, auditEvents });
      setError(null);
    } catch (loadError) {
      // Background refresh failures keep the last good data on screen.
      if (!background) setError(loadError instanceof Error ? loadError.message : "Unexpected dashboard error");
    }
  }, []);

  const loadWorkloadDetail = useCallback(async (id: string, background = false) => {
    if (!background) {
      setLoadingDetail(true);
      setDetailError(null);
    }
    try {
      const workload = await apiClient.getWorkload(id);
      setSelectedWorkload(workload);
    } catch (loadError) {
      if (!background) setDetailError(loadError instanceof Error ? loadError.message : "Unable to load workload");
    } finally {
      if (!background) setLoadingDetail(false);
    }
  }, []);

  useEffect(() => {
    void loadDashboard();
    const onHashChange = () => setRoute(parseRoute());
    globalThis.addEventListener("hashchange", onHashChange);
    return () => globalThis.removeEventListener("hashchange", onHashChange);
  }, [loadDashboard]);

  useVisibilityPolling(() => void loadDashboard(true), REFRESH_INTERVAL_MS);

  useEffect(() => {
    if (!("workloadId" in route)) {
      setDetailError(null);
      return;
    }
    if (selectedWorkload?.id === route.workloadId) return;
    void loadWorkloadDetail(route.workloadId);
  }, [route, selectedWorkload?.id, loadWorkloadDetail]);

  const detailRouteId = "workloadId" in route ? route.workloadId : undefined;
  useVisibilityPolling(
    () => {
      if (detailRouteId) void loadWorkloadDetail(detailRouteId, true);
    },
    REFRESH_INTERVAL_MS,
    detailRouteId !== undefined,
  );

  useEffect(() => {
    if (!toast) return;
    const timeout = globalThis.setTimeout(() => setToast(null), 3200);
    return () => globalThis.clearTimeout(timeout);
  }, [toast]);

  const goToSection = (section: Section) => navigate({ section });
  const openWorkload = (id: string) => navigate({ section: "workloads", workloadId: id });

  const submitWorkload = async (input: SubmitWorkloadInput) => {
    const workload = await apiClient.submitWorkload(input);
    setData((current) =>
      current
        ? { ...current, workloads: [workload, ...current.workloads.filter((item) => item.id !== workload.id)] }
        : current,
    );
    setSelectedWorkload(workload);
    setToast({ message: `${workload.name} submitted to the ${workload.priority} band` });
    openWorkload(workload.id);
    return workload;
  };

  const refreshFleet = async () => {
    const leases = await apiClient.listLeases();
    const cells = await apiClient.listCells(leases);
    setData((current) => current ? { ...current, cells, leases } : current);
  };

  const updatePolicy = async (policy: SchedulingPolicy, enabled: boolean) => {
    const updated = await apiClient.updatePolicy(policy, enabled);
    setData((current) => current ? {
      ...current,
      policies: current.policies.map((item) => item.id === updated.id ? updated : item),
    } : current);
    setToast({ message: `${updated.name} ${updated.preemptionEnabled ? "enabled" : "disabled"}` });
    return updated;
  };

  const operateWorkload = async (action: "cancel" | "checkpoint" | "retry") => {
    if (!selectedWorkload) return;
    try {
      const workload = await apiClient.workloadAction(selectedWorkload.id, action);
      setSelectedWorkload(workload);
      setData((current) => current ? {
        ...current,
        workloads: current.workloads.map((item) => item.id === workload.id ? workload : item),
      } : current);
      setToast({ message: `${action} requested for ${workload.name}` });
    } catch (operationError) {
      setToast({
        message: operationError instanceof Error ? operationError.message : `Unable to ${action} workload`,
        error: true,
      });
    }
  };

  let content: ReactNode;
  if (error) {
    content = <ErrorState message={error} onRetry={() => void loadDashboard()} />;
  } else if (!data) {
    content = <LoadingState />;
  } else if ("workloadId" in route) {
    if (detailError) {
      content = <ErrorState message={detailError} onRetry={() => { setSelectedWorkload(null); setRoute({ ...route }); }} />;
    } else if (loadingDetail || selectedWorkload?.id !== route.workloadId) {
      content = <LoadingState />;
    } else {
      content = (
        <WorkloadDetailPage
          workload={selectedWorkload}
          onBack={() => goToSection("workloads")}
          onAction={(action) => void operateWorkload(action)}
        />
      );
    }
  } else {
    switch (route.section) {
      case "workloads":
        content = <WorkloadsPage workloads={data.workloads} onOpenWorkload={openWorkload} onSubmit={() => setSubmitOpen(true)} />;
        break;
      case "fleet":
        content = <FleetPage cells={data.cells} leases={data.leases} onRefresh={refreshFleet} />;
        break;
      case "budgets":
        content = <BudgetsPage budgets={data.budgets} policies={data.policies} onPolicyToggle={updatePolicy} />;
        break;
      case "audit":
        content = <AuditPage events={data.auditEvents} onAction={(message) => setToast({ message })} />;
        break;
      default:
        content = (
          <OverviewPage
            overview={data.overview}
            workloads={data.workloads}
            onOpenWorkload={openWorkload}
            onShowWorkloads={() => goToSection("workloads")}
          />
        );
    }
  }

  return (
    <>
      <Shell
        route={route}
        mobileNavigationOpen={mobileNavigationOpen}
        onMobileNavigationChange={setMobileNavigationOpen}
        onNavigate={goToSection}
        onSubmit={() => setSubmitOpen(true)}
        health={data?.health}
        identity={currentIdentity()}
        onLogout={currentIdentity()?.mode === "development" ? undefined : () => void logout()}
      >
        {content}
      </Shell>
      <SubmitWorkloadModal open={submitOpen} onClose={() => setSubmitOpen(false)} onSubmit={submitWorkload} />
      {toast && (
        <div className={`toast ${toast.error ? "toast-error" : ""}`} role={toast.error ? "alert" : "status"}>
          {toast.error ? <AlertTriangle size={17} aria-hidden="true" /> : <CheckCircle2 size={17} aria-hidden="true" />}
          <span>{toast.message}</span>
          <button className="icon-button icon-button-small" type="button" aria-label="Dismiss notification" title="Dismiss" onClick={() => setToast(null)}><X size={15} /></button>
        </div>
      )}
    </>
  );
}
