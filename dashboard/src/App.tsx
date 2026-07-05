// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, CheckCircle2, X } from "lucide-react";
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";

import { ApiError, apiClient, computeOverview, type EyvanApiClient } from "./api/client";
import {
  capabilitiesForPermissions,
  configuredScope,
  currentIdentity,
  logout,
  NO_AUTH_CAPABILITIES,
  type AuthCapabilities,
  type AuthIdentity,
  type AuthScope,
} from "./auth/oidc";
import { ErrorState, LoadingState } from "./components/PageState";
import { Shell } from "./components/Shell";
import { SubmitWorkloadModal } from "./components/SubmitWorkloadModal";
import { useLiveRefresh } from "./lib/useLiveRefresh";
import { useVisibilityPolling } from "./lib/useVisibilityPolling";
import { AuditPage } from "./pages/AuditPage";
import { BudgetsPage } from "./pages/BudgetsPage";
import { FleetPage } from "./pages/FleetPage";
import { OverviewPage } from "./pages/OverviewPage";
import { WorkloadDetailPage } from "./pages/WorkloadDetailPage";
import { WorkloadsPage } from "./pages/WorkloadsPage";
import {
  accessibleRoute,
  navigate,
  parseRoute,
  type Route,
  type Section,
  type SectionAvailability,
} from "./routing";
import type {
  DashboardData,
  EffectiveProjectScope,
  SchedulingPolicy,
  SessionAuthority,
  SessionPermission,
  SubmitWorkloadInput,
  WorkloadDetail,
} from "./types";

// The fallback sweep, not the update path. Updates arrive as the control plane
// commits them; this covers a proxy that buffers `text/event-stream` and changes
// with no tenant audit event. Every sweep resolves /session again first.
const REFRESH_INTERVAL_MS = 30_000;

type DashboardClient = Pick<
  EyvanApiClient,
  | "getHealth"
  | "listWorkloads"
  | "listLeases"
  | "listBudgets"
  | "listPolicies"
  | "getQueueTrend"
  | "listCells"
  | "listAuditEvents"
>;

interface DashboardLoadAuthority {
  canRead: boolean;
  canManageFleet: boolean;
}

const FULL_DASHBOARD_AUTHORITY: DashboardLoadAuthority = { canRead: true, canManageFleet: true };

/** Load one coherent dashboard snapshot without issuing known-forbidden reads. */
export async function loadDashboardData(
  client: DashboardClient = apiClient,
  authority: DashboardLoadAuthority = FULL_DASHBOARD_AUTHORITY,
  scope: AuthScope = configuredScope(),
  signal?: AbortSignal,
): Promise<DashboardData> {
  const immutableScope = { orgId: scope.orgId, projectId: scope.projectId };
  const healthPromise = client.getHealth(signal);
  if (!authority.canRead) {
    const health = await healthPromise;
    const cells = authority.canManageFleet
      ? await client.listCells([], immutableScope, signal)
      : [];
    return {
      health,
      overview: computeOverview([], cells, [], []),
      workloads: [],
      cells,
      leases: [],
      budgets: [],
      policies: [],
      auditEvents: [],
      access: { fleetInventory: authority.canManageFleet, fleetAudit: false },
    };
  }

  const [health, workloads, leases, budgets, policies, queueTrend, auditEvents] = await Promise.all([
    healthPromise,
    client.listWorkloads({}, immutableScope, signal),
    // The optional cell-class join is a fleet read. Project readers never probe
    // it merely to decorate their otherwise-authorized scoped leases.
    client.listLeases(immutableScope, signal, authority.canManageFleet),
    client.listBudgets(immutableScope, signal),
    client.listPolicies(immutableScope, signal),
    client.getQueueTrend(immutableScope, signal),
    client.listAuditEvents(immutableScope, signal),
  ]);
  const cells = authority.canManageFleet
    ? await client.listCells(leases, immutableScope, signal)
    : [];
  return {
    health,
    overview: computeOverview(workloads, cells, budgets, queueTrend),
    workloads,
    cells,
    leases,
    budgets,
    policies,
    auditEvents,
    access: {
      fleetInventory: authority.canManageFleet,
      // Audit is deliberately project-scoped. This flag means the authorized
      // audit panel loaded, not that a fleet-wide endpoint was called.
      fleetAudit: true,
    },
  };
}

function scopeKey(scope: AuthScope): string {
  return JSON.stringify([scope.orgId, scope.projectId]);
}

function scopeSnapshot(scope: AuthScope): AuthScope {
  return { orgId: scope.orgId, projectId: scope.projectId };
}

export function chooseInitialScope(
  authority: SessionAuthority,
  preferred: AuthScope,
): EffectiveProjectScope | undefined {
  const exact = authority.scopes.find((scope) => scopeKey(scope) === scopeKey(preferred));
  if (exact) return exact;
  // A disabled project with READ remains a valid history view. Prefer a useful
  // readable scope, then an enabled submit-only scope, before falling back to a
  // server-returned scope with no current affordance.
  return (
    authority.scopes.find((scope) => scope.permissions.includes("read")) ??
    authority.scopes.find((scope) => scope.acceptsNewWork && scope.permissions.includes("submit")) ??
    authority.scopes[0]
  );
}

function scopeForResource(
  authority: SessionAuthority | null,
  orgId: string,
  projectId: string,
): EffectiveProjectScope | undefined {
  return authority?.scopes.find((scope) => scope.orgId === orgId && scope.projectId === projectId);
}

function permissionsForResource(
  authority: SessionAuthority | null,
  orgId: string,
  projectId: string,
): SessionPermission[] {
  if (!authority) return [];
  const scoped = scopeForResource(authority, orgId, projectId);
  return [...new Set([...authority.globalPermissions, ...(scoped?.permissions ?? [])])];
}

function capabilitiesForResource(
  authority: SessionAuthority | null,
  orgId: string,
  projectId: string,
): AuthCapabilities {
  const scoped = scopeForResource(authority, orgId, projectId);
  return capabilitiesForPermissions(
    permissionsForResource(authority, orgId, projectId),
    // A scoped /session query always materializes wildcard authority at this
    // resource before any resource mutation is offered. Missing server truth is
    // therefore read-only rather than an optimistic mutation grant.
    scoped?.acceptsNewWork ?? false,
  );
}

function hasAnyRead(authority: SessionAuthority | null): boolean {
  return Boolean(
    authority?.globalPermissions.includes("read") ||
      authority?.scopes.some((scope) => scope.permissions.includes("read")),
  );
}

function authorityFingerprint(authority: SessionAuthority): string {
  return JSON.stringify({
    global: authority.globalPermissions,
    scopes: authority.scopes.map((scope) => ({
      key: scopeKey(scope),
      permissions: scope.permissions,
      acceptsNewWork: scope.acceptsNewWork,
      enabled: scope.enabled,
    })),
  });
}

interface AppSnapshot {
  authority: SessionAuthority | null;
  selectedScope: EffectiveProjectScope | null;
  data: DashboardData | null;
}

interface SubmissionAcknowledgement {
  id: string;
  name: string;
  orgId: string;
  projectId: string;
}

export interface AppProps {
  client?: EyvanApiClient;
  identityProvider?: () => AuthIdentity | undefined;
}

export default function App({ client = apiClient, identityProvider = currentIdentity }: AppProps) {
  const [route, setRoute] = useState<Route>(() => parseRoute());
  const [snapshot, setSnapshot] = useState<AppSnapshot>({ authority: null, selectedScope: null, data: null });
  const [selectedWorkload, setSelectedWorkload] = useState<WorkloadDetail | null>(null);
  const [detailAuthority, setDetailAuthority] = useState<SessionAuthority | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [submitOpen, setSubmitOpen] = useState(false);
  const [submissionAcknowledgement, setSubmissionAcknowledgement] =
    useState<SubmissionAcknowledgement | null>(null);
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const [toast, setToast] = useState<{ message: string; error?: boolean } | null>(null);
  const [refreshWarning, setRefreshWarning] = useState<string | null>(null);

  const mountedRef = useRef(false);
  const snapshotRef = useRef(snapshot);
  snapshotRef.current = snapshot;
  const desiredScopeRef = useRef<AuthScope>(configuredScope());
  const dashboardRequestRef = useRef(0);
  const dashboardAbortRef = useRef<AbortController | null>(null);
  const detailRequestRef = useRef(0);
  const detailAbortRef = useRef<AbortController | null>(null);
  const scopeEpochRef = useRef(0);
  const scopeOperationAbortRef = useRef<AbortController>(new AbortController());
  const detailRouteIdRef = useRef<string | undefined>(undefined);

  const updateSnapshot = useCallback((update: (current: AppSnapshot) => AppSnapshot) => {
    setSnapshot((current) => {
      const next = update(current);
      snapshotRef.current = next;
      return next;
    });
  }, []);

  const invalidateScopeOperations = useCallback(() => {
    scopeEpochRef.current += 1;
    scopeOperationAbortRef.current.abort();
    scopeOperationAbortRef.current = new AbortController();
    detailAbortRef.current?.abort();
    detailRequestRef.current += 1;
  }, []);

  const fencePresentationAuthority = useCallback(() => {
    // A typed denial is server truth. Drop every presentation capability before
    // starting reconciliation so a slow or failed /session request cannot leave
    // stale mutation controls live over a loading/error state.
    invalidateScopeOperations();
    const fenced: AppSnapshot = { authority: null, selectedScope: null, data: null };
    snapshotRef.current = fenced;
    setSnapshot(fenced);
    setSelectedWorkload(null);
    setDetailAuthority(null);
    setDetailError(null);
    setSubmitOpen(false);
    setSubmissionAcknowledgement(null);
    setToast(null);
    setRefreshWarning(null);
    setError(null);
  }, [invalidateScopeOperations]);

  const refreshDashboard = useCallback(
    async (preferred: AuthScope = desiredScopeRef.current, background = false): Promise<boolean> => {
      const preferredScope = scopeSnapshot(preferred);
      const requestId = ++dashboardRequestRef.current;
      dashboardAbortRef.current?.abort();
      const controller = new AbortController();
      dashboardAbortRef.current = controller;

      const currentRequest = () =>
        mountedRef.current && !controller.signal.aborted && dashboardRequestRef.current === requestId;
      let sawForbidden = false;

      try {
        let session: SessionAuthority | undefined;
        let selected: EffectiveProjectScope | undefined;
        let data: DashboardData | undefined;
        for (let attempt = 0; attempt < 2; attempt += 1) {
          session = await client.getSessionAuthority(preferredScope, controller.signal);
          if (!currentRequest()) return false;
          selected = chooseInitialScope(session, preferredScope);
          if (!selected) {
            const noScopeMessage = "This identity has no effective Eyvan project scope";
            invalidateScopeOperations();
            desiredScopeRef.current = preferredScope;
            const next = { authority: session, selectedScope: null, data: null };
            snapshotRef.current = next;
            setSnapshot(next);
            setSelectedWorkload(null);
            setDetailAuthority(null);
            setDetailError(null);
            setError(noScopeMessage);
            setRefreshWarning(null);
            return false;
          }
          const permissions = permissionsForResource(session, selected.orgId, selected.projectId);
          const capabilities = capabilitiesForResource(session, selected.orgId, selected.projectId);
          try {
            data = await loadDashboardData(
              client,
              {
                canRead: permissions.includes("read"),
                canManageFleet: capabilities.manageFleet,
              },
              selected,
              controller.signal,
            );
            break;
          } catch (loadError) {
            if (!currentRequest()) return false;
            // A 403 means authority changed after /session was read. Fence the
            // old controls synchronously, then re-resolve once and rebuild the
            // whole snapshot from fresh server authority.
            if (loadError instanceof ApiError && loadError.status === 403) {
              sawForbidden = true;
              fencePresentationAuthority();
            }
            if (!(loadError instanceof ApiError && loadError.status === 403) || attempt === 1) throw loadError;
          }
        }
        if (!session || !selected || !data || !currentRequest()) return false;

        const previous = snapshotRef.current;
        const scopeChanged =
          previous.selectedScope === null || scopeKey(previous.selectedScope) !== scopeKey(selected);
        const authorityChanged =
          previous.authority === null ||
          authorityFingerprint(previous.authority) !== authorityFingerprint(session);
        if (scopeChanged || authorityChanged) {
          invalidateScopeOperations();
          // Resource-scoped authority is a separate server snapshot. Never let
          // an old detail grant outlive a durable session change; clearing the
          // pair makes the route reload workload + scoped /session atomically.
          setSelectedWorkload(null);
          setDetailAuthority(null);
          setDetailError(null);
        }
        desiredScopeRef.current = scopeSnapshot(selected);
        const next = { authority: session, selectedScope: selected, data };
        snapshotRef.current = next;
        setSnapshot(next);
        setError(null);
        setRefreshWarning(null);
        return true;
      } catch (loadError) {
        if (!currentRequest()) return false;
        const message = loadError instanceof Error ? loadError.message : "Unexpected dashboard error";
        const authorizationFailure =
          sawForbidden || (loadError instanceof ApiError && loadError.status === 403);
        if (authorizationFailure) {
          // A stale data snapshot is acceptable during a transient outage, but
          // stale authority is not acceptable after an explicit denial.
          fencePresentationAuthority();
        }
        if (background && snapshotRef.current.data && !authorizationFailure) {
          setRefreshWarning(`Live refresh failed; showing the last successful snapshot. ${message}`);
        } else {
          setError(message);
          setRefreshWarning(null);
        }
        return false;
      }
    },
    [client, fencePresentationAuthority, invalidateScopeOperations],
  );

  const refreshAuthorityAfterForbidden = useCallback(
    async (errorValue: unknown, relevant = true) => {
      if (relevant && errorValue instanceof ApiError && errorValue.status === 403) {
        // Fence before the first reconciliation await. If /session is slow or
        // unavailable, no stale control survives while server truth is unknown.
        fencePresentationAuthority();
        await refreshDashboard(desiredScopeRef.current, false);
      }
    },
    [fencePresentationAuthority, refreshDashboard],
  );

  const withAuthorityRefresh = useCallback(
    async <T,>(request: () => Promise<T>): Promise<T> => {
      const epoch = scopeEpochRef.current;
      const selected = snapshotRef.current.selectedScope;
      const requestScope = selected ? scopeSnapshot(selected) : null;
      try {
        return await request();
      } catch (requestError) {
        const latest = snapshotRef.current.selectedScope;
        const relevant =
          epoch === scopeEpochRef.current &&
          requestScope !== null &&
          latest !== null &&
          scopeKey(requestScope) === scopeKey(latest);
        await refreshAuthorityAfterForbidden(requestError, relevant);
        throw requestError;
      }
    },
    [refreshAuthorityAfterForbidden],
  );

  const loadWorkloadDetail = useCallback(
    async (id: string, background = false) => {
      const starting = snapshotRef.current;
      if (!starting.selectedScope || !hasAnyRead(starting.authority)) return;
      const requestScope = scopeSnapshot(starting.selectedScope);
      const epoch = scopeEpochRef.current;
      const requestId = ++detailRequestRef.current;
      detailAbortRef.current?.abort();
      const controller = new AbortController();
      detailAbortRef.current = controller;
      if (!background) {
        setLoadingDetail(true);
        setDetailError(null);
      }
      const currentRequest = () =>
        mountedRef.current &&
        !controller.signal.aborted &&
        detailRequestRef.current === requestId &&
        scopeEpochRef.current === epoch &&
        detailRouteIdRef.current === id;
      try {
        const workload = await client.getWorkload(id, requestScope, controller.signal);
        if (!currentRequest()) return;
        const workloadScope = { orgId: workload.org, projectId: workload.project };
        // This authenticated resource-scoped query is load-bearing for wildcard
        // org/fleet grants over legacy projects. The browser never decodes grants
        // to infer that authority.
        const resourceAuthority = await client.getSessionAuthority(workloadScope, controller.signal);
        if (!currentRequest()) return;
        const permissions = permissionsForResource(
          resourceAuthority,
          workloadScope.orgId,
          workloadScope.projectId,
        );
        if (!permissions.includes("read")) throw new ApiError("Workload detail is no longer readable", 403);
        setDetailAuthority(resourceAuthority);
        setSelectedWorkload(workload);
        setDetailError(null);
      } catch (loadError) {
        const relevant = currentRequest();
        if (relevant && !background) {
          setDetailError(loadError instanceof Error ? loadError.message : "Unable to load workload");
        }
        await refreshAuthorityAfterForbidden(loadError, relevant);
      } finally {
        if (currentRequest() && !background) setLoadingDetail(false);
      }
    },
    [client, refreshAuthorityAfterForbidden],
  );

  useEffect(() => {
    mountedRef.current = true;
    void refreshDashboard(configuredScope());
    const onHashChange = () => setRoute(parseRoute());
    globalThis.addEventListener("hashchange", onHashChange);
    return () => {
      mountedRef.current = false;
      dashboardAbortRef.current?.abort();
      detailAbortRef.current?.abort();
      scopeOperationAbortRef.current.abort();
      globalThis.removeEventListener("hashchange", onHashChange);
    };
  }, [refreshDashboard]);

  const { authority, selectedScope, data } = snapshot;
  const selectedPermissions = selectedScope
    ? permissionsForResource(authority, selectedScope.orgId, selectedScope.projectId)
    : [];
  const canReadSelectedScope = selectedPermissions.includes("read");
  const capabilities = selectedScope
    ? capabilitiesForResource(authority, selectedScope.orgId, selectedScope.projectId)
    : NO_AUTH_CAPABILITIES;

  useVisibilityPolling(
    () => void refreshDashboard(desiredScopeRef.current, true),
    REFRESH_INTERVAL_MS,
    authority !== null && selectedScope !== null,
  );

  const selectedOrgId = selectedScope?.orgId;
  const selectedProjectId = selectedScope?.projectId;
  const subscribe = useCallback(
    (onEvent: () => void, signal: AbortSignal, onConnectionChange: (connected: boolean) => void) => {
      if (!selectedOrgId || !selectedProjectId || !canReadSelectedScope) return Promise.resolve();
      const immutableScope = { orgId: selectedOrgId, projectId: selectedProjectId };
      return client
        .followEvents(onEvent, signal, onConnectionChange, immutableScope)
        .catch(async (streamError: unknown) => {
          // A revoked stream must refresh the same server authority used by the
          // polling path, and terminal stream failures must not become unhandled
          // promise rejections in the hook. An obsolete scope's stream cannot
          // revoke the newly selected scope while its abort is racing the error.
          const latest = snapshotRef.current.selectedScope;
          const relevant =
            !signal.aborted && latest !== null && scopeKey(latest) === scopeKey(immutableScope);
          await refreshAuthorityAfterForbidden(streamError, relevant);
        });
    },
    [
      canReadSelectedScope,
      client,
      refreshAuthorityAfterForbidden,
      selectedOrgId,
      selectedProjectId,
    ],
  );
  const live = useLiveRefresh({
    subscribe,
    active: canReadSelectedScope,
    onChange: () => {
      void refreshDashboard(desiredScopeRef.current, true);
      if (detailRouteIdRef.current) void loadWorkloadDetail(detailRouteIdRef.current, true);
    },
  });

  const sectionAvailability: SectionAvailability = {
    workloads: canReadSelectedScope || capabilities.submitWorkloads,
    budgets: canReadSelectedScope,
    fleet: capabilities.manageFleet && (data?.access.fleetInventory ?? true),
    audit: canReadSelectedScope,
  };
  const routed = accessibleRoute(route, sectionAvailability);
  // SUBMIT does not imply READ. A submit-only deep link must not mount the
  // detail loader and accidentally issue get/explain/poll reads.
  const activeRoute: Route =
    "workloadId" in routed && !hasAnyRead(authority) ? { section: "workloads" } : routed;
  const detailRouteId = "workloadId" in activeRoute ? activeRoute.workloadId : undefined;
  detailRouteIdRef.current = detailRouteId;

  useEffect(() => {
    if (!detailRouteId || error) {
      detailAbortRef.current?.abort();
      detailRequestRef.current += 1;
      setDetailError(null);
      setSelectedWorkload(null);
      setDetailAuthority(null);
      return;
    }
    if (selectedWorkload?.id === detailRouteId) return;
    void loadWorkloadDetail(detailRouteId);
  }, [detailRouteId, error, loadWorkloadDetail, selectedWorkload?.id]);

  useVisibilityPolling(
    () => {
      if (detailRouteId) void loadWorkloadDetail(detailRouteId, true);
    },
    REFRESH_INTERVAL_MS,
    detailRouteId !== undefined && error === null && hasAnyRead(authority),
  );

  useEffect(() => {
    if (!toast) return;
    const timeout = globalThis.setTimeout(() => setToast(null), 3200);
    return () => globalThis.clearTimeout(timeout);
  }, [toast]);

  const goToSection = (section: Section) => navigate({ section });
  const openWorkload = (id: string) => navigate({ section: "workloads", workloadId: id });

  const selectScope = (scope: EffectiveProjectScope) => {
    if (!authority || scopeKey(scope) === (selectedScope ? scopeKey(selectedScope) : "")) return;
    const immutableScope = scopeSnapshot(scope);
    desiredScopeRef.current = immutableScope;
    dashboardAbortRef.current?.abort();
    dashboardRequestRef.current += 1;
    invalidateScopeOperations();
    const next = { authority, selectedScope: scope, data: null };
    snapshotRef.current = next;
    setSnapshot(next);
    setSelectedWorkload(null);
    setDetailAuthority(null);
    setDetailError(null);
    setSubmitOpen(false);
    setSubmissionAcknowledgement(null);
    setError(null);
    navigate({ section: "overview" });
    void refreshDashboard(immutableScope);
  };

  const mutationIsCurrent = (epoch: number, scope: AuthScope) =>
    mountedRef.current &&
    scopeEpochRef.current === epoch &&
    snapshotRef.current.selectedScope !== null &&
    scopeKey(snapshotRef.current.selectedScope) === scopeKey(scope);

  const submitWorkload = async (input: SubmitWorkloadInput) => {
    const current = snapshotRef.current;
    if (!current.selectedScope) throw new ApiError("No project scope is selected");
    const immutableScope = scopeSnapshot(current.selectedScope);
    const currentCapabilities = capabilitiesForResource(
      current.authority,
      immutableScope.orgId,
      immutableScope.projectId,
    );
    if (!currentCapabilities.submitWorkloads) throw new ApiError("This scope does not accept workload submissions", 403);
    const epoch = scopeEpochRef.current;
    try {
      const workload = await client.submitWorkload(
        input,
        immutableScope,
        scopeOperationAbortRef.current.signal,
      );
      if (!mutationIsCurrent(epoch, immutableScope)) return workload;
      const readable = permissionsForResource(
        current.authority,
        immutableScope.orgId,
        immutableScope.projectId,
      ).includes("read");
      if (readable) {
        updateSnapshot((latest) => ({
          ...latest,
          data: latest.data
            ? {
                ...latest.data,
                workloads: [
                  workload,
                  ...latest.data.workloads.filter((item) => item.id !== workload.id),
                ],
              }
            : latest.data,
        }));
        setSelectedWorkload(workload);
        setDetailAuthority(current.authority);
        setSubmissionAcknowledgement(null);
        openWorkload(workload.id);
      } else {
        // POST acknowledgement is the only resource representation a submit-only
        // principal receives. Keep it visible and never turn it into a GET.
        setSubmissionAcknowledgement({
          id: workload.id,
          name: workload.name,
          orgId: workload.org,
          projectId: workload.project,
        });
        navigate({ section: "workloads" });
      }
      setToast({ message: `${workload.name} submitted to the ${workload.priority} band` });
      return workload;
    } catch (submitError) {
      await refreshAuthorityAfterForbidden(submitError, mutationIsCurrent(epoch, immutableScope));
      throw submitError;
    }
  };

  const refreshFleet = async () => {
    const refreshed = await refreshDashboard(desiredScopeRef.current, true);
    if (!refreshed) throw new Error("Fleet inventory refresh did not complete");
  };

  const updatePolicy = async (policy: SchedulingPolicy, enabled: boolean) => {
    const current = snapshotRef.current;
    if (!current.selectedScope) throw new ApiError("No project scope is selected");
    const immutableScope = scopeSnapshot(current.selectedScope);
    const currentCapabilities = capabilitiesForResource(
      current.authority,
      immutableScope.orgId,
      immutableScope.projectId,
    );
    if (!currentCapabilities.manageProject) throw new ApiError("This scope is read-only", 403);
    const epoch = scopeEpochRef.current;
    try {
      // Both path components come from the selected immutable scope. policy.scope
      // is display data and can collide across organizations.
      const updated = await client.updatePolicy(
        policy,
        enabled,
        immutableScope,
        scopeOperationAbortRef.current.signal,
      );
      if (mutationIsCurrent(epoch, immutableScope)) {
        updateSnapshot((latest) => ({
          ...latest,
          data: latest.data
            ? {
                ...latest.data,
                policies: latest.data.policies.map((item) =>
                  item.id === updated.id ? updated : item,
                ),
              }
            : latest.data,
        }));
        setToast({ message: `${updated.name} ${updated.preemptionEnabled ? "enabled" : "disabled"}` });
      }
      return updated;
    } catch (policyError) {
      await refreshAuthorityAfterForbidden(policyError, mutationIsCurrent(epoch, immutableScope));
      throw policyError;
    }
  };

  const operateWorkload = async (action: "cancel" | "checkpoint" | "retry") => {
    const workload = selectedWorkload;
    const resourceAuthority = detailAuthority;
    if (!workload) return;
    const immutableScope = { orgId: workload.org, projectId: workload.project };
    const selectedScopeSnapshot = snapshotRef.current.selectedScope
      ? scopeSnapshot(snapshotRef.current.selectedScope)
      : immutableScope;
    const currentCapabilities = capabilitiesForResource(
      resourceAuthority,
      immutableScope.orgId,
      immutableScope.projectId,
    );
    if (!currentCapabilities.operateWorkloads) return;
    const epoch = scopeEpochRef.current;
    try {
      const updated = await client.workloadAction(
        workload.id,
        action,
        immutableScope,
        scopeOperationAbortRef.current.signal,
      );
      if (!mutationIsCurrent(epoch, selectedScopeSnapshot)) return;
      if (detailRouteIdRef.current === workload.id) setSelectedWorkload(updated);
      // A wildcard deep link can point outside the selected dashboard scope.
      // Its mutation is resource-scoped, but its response must not contaminate
      // the selected project's list snapshot.
      if (scopeKey(immutableScope) === scopeKey(selectedScopeSnapshot)) {
        updateSnapshot((latest) => ({
          ...latest,
          data: latest.data
            ? {
                ...latest.data,
                workloads: latest.data.workloads.map((item) =>
                  item.id === updated.id ? updated : item,
                ),
              }
            : latest.data,
        }));
      }
      if (detailRouteIdRef.current === workload.id) {
        setToast({ message: `${action} requested for ${updated.name}` });
      }
    } catch (operationError) {
      const relevant = mutationIsCurrent(epoch, selectedScopeSnapshot);
      await refreshAuthorityAfterForbidden(operationError, relevant);
      if (mutationIsCurrent(epoch, selectedScopeSnapshot) && detailRouteIdRef.current === workload.id) {
        setToast({
          message:
            operationError instanceof Error
              ? operationError.message
              : `Unable to ${action} workload`,
          error: true,
        });
      }
    }
  };

  const fleetScope = selectedScope ? scopeSnapshot(selectedScope) : configuredScope();
  const loadWarmth = useCallback(
    (signal: AbortSignal) => withAuthorityRefresh(() => client.getWarmth(fleetScope, signal)),
    [client, fleetScope.orgId, fleetScope.projectId, withAuthorityRefresh],
  );
  const loadBilling = useCallback(
    (signal: AbortSignal) => withAuthorityRefresh(() => client.getBilling(fleetScope, signal)),
    [client, fleetScope.orgId, fleetScope.projectId, withAuthorityRefresh],
  );
  const loadLaunchLatency = useCallback(
    (signal: AbortSignal) => withAuthorityRefresh(() => client.getLaunchLatency(fleetScope, signal)),
    [client, fleetScope.orgId, fleetScope.projectId, withAuthorityRefresh],
  );
  const explanationScope = selectedWorkload
    ? { orgId: selectedWorkload.org, projectId: selectedWorkload.project }
    : fleetScope;
  const loadExplanation = useCallback(
    (id: string, signal: AbortSignal) =>
      withAuthorityRefresh(() => client.explainWorkload(id, explanationScope, signal)),
    [client, explanationScope.orgId, explanationScope.projectId, withAuthorityRefresh],
  );

  const browserIdentity = identityProvider();
  const identity: AuthIdentity | undefined = browserIdentity
    ? {
        ...browserIdentity,
        displayName:
          authority?.displayName ?? browserIdentity.displayName ?? authority?.subject ?? "Authenticated user",
        detail: selectedScope
          ? `${selectedScope.orgId}/${selectedScope.projectId}${selectedScope.roles.length ? ` · ${selectedScope.roles.join(", ")}` : ""}`
          : "Authority pending server verification",
        roles: selectedScope?.roles ?? [],
        scope: selectedScope ?? browserIdentity.scope,
        capabilities,
      }
    : undefined;
  const resourceAuthority = detailAuthority ?? authority;
  const detailCapabilities = selectedWorkload
    ? capabilitiesForResource(resourceAuthority, selectedWorkload.org, selectedWorkload.project)
    : capabilities;

  let content: ReactNode;
  if (error) {
    content = (
      <ErrorState
        message={error}
        onRetry={() => void refreshDashboard(desiredScopeRef.current)}
      />
    );
  } else if (!data) {
    content = <LoadingState />;
  } else if ("workloadId" in activeRoute) {
    if (detailError) {
      content = (
        <ErrorState
          message={detailError}
          onRetry={() => {
            setSelectedWorkload(null);
            void loadWorkloadDetail(activeRoute.workloadId);
          }}
        />
      );
    } else if (loadingDetail || selectedWorkload?.id !== activeRoute.workloadId) {
      content = <LoadingState />;
    } else {
      content = (
        <WorkloadDetailPage
          workload={selectedWorkload}
          onBack={() => goToSection("workloads")}
          onAction={(action) => void operateWorkload(action)}
          canOperate={detailCapabilities.operateWorkloads}
          loadExplanation={loadExplanation}
        />
      );
    }
  } else {
    switch (activeRoute.section) {
      case "workloads":
        content = (
          <WorkloadsPage
            workloads={data.workloads}
            onOpenWorkload={openWorkload}
            onSubmit={() => setSubmitOpen(true)}
            canSubmit={capabilities.submitWorkloads}
          />
        );
        break;
      case "fleet":
        content = (
          <FleetPage
            cells={data.cells}
            leases={data.leases}
            onRefresh={refreshFleet}
            loadWarmth={loadWarmth}
            loadBilling={loadBilling}
            loadLaunchLatency={loadLaunchLatency}
          />
        );
        break;
      case "budgets":
        content = (
          <BudgetsPage
            budgets={data.budgets}
            policies={data.policies}
            onPolicyToggle={updatePolicy}
            canManageProject={capabilities.manageProject}
          />
        );
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
            fleetAvailable={data.access.fleetInventory}
          />
        );
    }
  }

  return (
    <>
      <Shell
        route={activeRoute}
        mobileNavigationOpen={mobileNavigationOpen}
        onMobileNavigationChange={setMobileNavigationOpen}
        onNavigate={goToSection}
        onSubmit={() => setSubmitOpen(true)}
        sectionAvailability={sectionAvailability}
        health={data?.health}
        live={canReadSelectedScope ? live : undefined}
        identity={identity}
        scopes={authority?.scopes ?? []}
        selectedScope={selectedScope ?? undefined}
        onScopeChange={selectScope}
        onLogout={identity?.mode === "development" ? undefined : () => void logout()}
      >
        {refreshWarning && (
          <div className="inline-warning" role="status">
            <AlertTriangle size={16} aria-hidden="true" />
            <span>{refreshWarning}</span>
          </div>
        )}
        {submissionAcknowledgement && (
          <div className="submission-acknowledgement" role="status" aria-live="polite">
            <CheckCircle2 size={18} aria-hidden="true" />
            <span>
              <strong>Workload submitted</strong>
              {submissionAcknowledgement.name} ({submissionAcknowledgement.id}) was accepted for {" "}
              {submissionAcknowledgement.orgId}/{submissionAcknowledgement.projectId}. This role cannot read
              workload detail.
            </span>
            <button
              className="icon-button icon-button-small"
              type="button"
              aria-label="Dismiss submission acknowledgement"
              onClick={() => setSubmissionAcknowledgement(null)}
            >
              <X size={15} />
            </button>
          </div>
        )}
        {content}
      </Shell>
      {capabilities.submitWorkloads && selectedScope && (
        <SubmitWorkloadModal
          open={submitOpen}
          project={selectedScope.projectId}
          onClose={() => setSubmitOpen(false)}
          onSubmit={submitWorkload}
        />
      )}
      {toast && (
        <div className={`toast ${toast.error ? "toast-error" : ""}`} role={toast.error ? "alert" : "status"}>
          {toast.error ? (
            <AlertTriangle size={17} aria-hidden="true" />
          ) : (
            <CheckCircle2 size={17} aria-hidden="true" />
          )}
          <span>{toast.message}</span>
          <button
            className="icon-button icon-button-small"
            type="button"
            aria-label="Dismiss notification"
            title="Dismiss"
            onClick={() => setToast(null)}
          >
            <X size={15} />
          </button>
        </div>
      )}
    </>
  );
}
