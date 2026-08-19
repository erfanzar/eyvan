// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, CheckCircle2, X } from "lucide-react";
import { Fragment, useCallback, useEffect, useRef, useState, type ReactNode } from "react";

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
import { BillingPage } from "./pages/BillingPage";
import { BudgetsPage } from "./pages/BudgetsPage";
import { CapacityPage } from "./pages/CapacityPage";
import { FleetPage } from "./pages/FleetPage";
import { OverviewPage } from "./pages/OverviewPage";
import { WorkloadDetailPage, type LogFollowHandlers } from "./pages/WorkloadDetailPage";
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
  AvailableShape,
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

interface DetailFailure {
  workloadId: string;
  error: Error;
}

interface SubmitInteraction {
  id: number;
  scope: AuthScope;
  epoch: number;
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
  const [detailFailure, setDetailFailure] = useState<DetailFailure | null>(null);
  const [submitInteraction, setSubmitInteraction] = useState<SubmitInteraction | null>(null);
  // Read when the form opens rather than with the dashboard sweep: it is only
  // ever used by the form, and a submit-only principal has no dashboard sweep
  // to carry it.
  const [availableShapes, setAvailableShapes] = useState<{
    shapes: AvailableShape[];
    loading: boolean;
    error: string | null;
  }>({ shapes: [], loading: false, error: null });
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
  const detailFailureRef = useRef<DetailFailure | null>(detailFailure);
  detailFailureRef.current = detailFailure;
  const submitInteractionRef = useRef<SubmitInteraction | null>(submitInteraction);
  submitInteractionRef.current = submitInteraction;
  const submitInteractionSequenceRef = useRef(0);

  const updateSnapshot = useCallback((update: (current: AppSnapshot) => AppSnapshot) => {
    setSnapshot((current) => {
      const next = update(current);
      snapshotRef.current = next;
      return next;
    });
  }, []);

  const updateDetailFailure = useCallback((failure: DetailFailure | null) => {
    detailFailureRef.current = failure;
    setDetailFailure(failure);
  }, []);

  const closeSubmitInteraction = useCallback((interactionId?: number) => {
    const current = submitInteractionRef.current;
    if (interactionId !== undefined && current?.id !== interactionId) return;
    submitInteractionRef.current = null;
    setSubmitInteraction(null);
  }, []);

  const invalidateScopeOperations = useCallback(() => {
    scopeEpochRef.current += 1;
    scopeOperationAbortRef.current.abort();
    scopeOperationAbortRef.current = new AbortController();
    detailAbortRef.current?.abort();
    detailRequestRef.current += 1;
  }, []);

  const fencePresentationAuthority = useCallback(
    (preservedDetailFailure: DetailFailure | null = null) => {
      // A typed denial is server truth. Drop every presentation capability and
      // scope-bound interaction before starting reconciliation. A detail denial
      // may remain as inert route state, but no authority or action survives.
      invalidateScopeOperations();
      const fenced: AppSnapshot = { authority: null, selectedScope: null, data: null };
      snapshotRef.current = fenced;
      setSnapshot(fenced);
      setSelectedWorkload(null);
      setDetailAuthority(null);
      setLoadingDetail(false);
      updateDetailFailure(preservedDetailFailure);
      closeSubmitInteraction();
      setSubmissionAcknowledgement(null);
      setMobileNavigationOpen(false);
      setToast(null);
      setRefreshWarning(null);
      setError(null);
    },
    [closeSubmitInteraction, invalidateScopeOperations, updateDetailFailure],
  );

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
            setLoadingDetail(false);
            updateDetailFailure(null);
            closeSubmitInteraction();
            setSubmissionAcknowledgement(null);
            setMobileNavigationOpen(false);
            setToast(null);
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
          const pendingDetailFailure = detailFailureRef.current;
          const preserveDetailFailure =
            pendingDetailFailure !== null && pendingDetailFailure.workloadId === detailRouteIdRef.current;
          invalidateScopeOperations();
          // Resource-scoped authority is a separate server snapshot. Never let
          // an old detail grant or any open scope-bound interaction outlive a
          // durable session change. A typed detail denial is inert route state
          // and remains visible after the one reconciliation it requested.
          setSelectedWorkload(null);
          setDetailAuthority(null);
          setLoadingDetail(false);
          updateDetailFailure(preserveDetailFailure ? pendingDetailFailure : null);
          closeSubmitInteraction();
          setSubmissionAcknowledgement(null);
          setMobileNavigationOpen(false);
          setToast(null);
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
    [
      client,
      closeSubmitInteraction,
      fencePresentationAuthority,
      invalidateScopeOperations,
      updateDetailFailure,
    ],
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
        updateDetailFailure(null);
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
        updateDetailFailure(null);
      } catch (loadError) {
        const relevant = currentRequest();
        if (!relevant) return;
        const failure: DetailFailure = {
          workloadId: id,
          error: loadError instanceof Error ? loadError : new Error("Unable to load workload"),
        };
        if (loadError instanceof ApiError && loadError.status === 403) {
          // Preserve the typed denial as inert route state while fencing every
          // stale action. One fresh dashboard/session reconciliation follows;
          // this request is not automatically replayed into another 403 loop.
          fencePresentationAuthority(failure);
          await refreshDashboard(requestScope, false);
        } else if (!background) {
          updateDetailFailure(failure);
        }
      } finally {
        if (currentRequest() && !background) setLoadingDetail(false);
      }
    },
    [client, fencePresentationAuthority, refreshDashboard, updateDetailFailure],
  );

  const refreshCurrentDetailAutomatically = useCallback(() => {
    const workloadId = detailRouteIdRef.current;
    // A typed denial is stable while its route remains current. Only the visible
    // Retry action clears it; retained SSE events and polls stay dashboard-only.
    if (!workloadId || detailFailureRef.current?.workloadId === workloadId) return;
    void loadWorkloadDetail(workloadId, true);
  }, [loadWorkloadDetail]);

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
      refreshCurrentDetailAutomatically();
    },
  });

  const sectionAvailability: SectionAvailability = {
    workloads: canReadSelectedScope || capabilities.submitWorkloads,
    budgets: canReadSelectedScope,
    fleet: capabilities.manageFleet && (data?.access.fleetInventory ?? true),
    // Provider quota is a property of the cloud project this fleet's pools
    // declare, so it is gated by the same authority that can read those pools.
    capacity: capabilities.manageFleet,
    audit: canReadSelectedScope,
  };
  const routed = accessibleRoute(route, sectionAvailability);
  // SUBMIT does not imply READ. A submit-only deep link must not mount the
  // detail loader and accidentally issue get/explain/poll reads. An already
  // typed denial is inert and may remain visible without mounting those reads.
  const activeRoute: Route =
    "workloadId" in routed &&
    !hasAnyRead(authority) &&
    detailFailure?.workloadId !== routed.workloadId
      ? { section: "workloads" }
      : routed;
  const routeDetailId = "workloadId" in route ? route.workloadId : undefined;
  const detailRouteId = "workloadId" in activeRoute ? activeRoute.workloadId : undefined;
  // Track the immutable URL target, not its temporarily inaccessible projection
  // while a denial has fenced authority. This keeps the denial relevant through
  // reconciliation while still rejecting it immediately after navigation.
  detailRouteIdRef.current = routeDetailId;
  const selectedScopeIdentity = selectedScope ? scopeKey(selectedScope) : "authority-pending";

  useEffect(() => {
    if (!detailRouteId || error) {
      detailAbortRef.current?.abort();
      detailRequestRef.current += 1;
      setLoadingDetail(false);
      setSelectedWorkload(null);
      setDetailAuthority(null);
      if (!routeDetailId || detailFailureRef.current?.workloadId !== routeDetailId) {
        updateDetailFailure(null);
      }
      return;
    }
    if (detailFailureRef.current?.workloadId === detailRouteId) {
      setLoadingDetail(false);
      return;
    }
    if (selectedWorkload?.id === detailRouteId) return;
    void loadWorkloadDetail(detailRouteId);
  }, [
    detailFailure,
    detailRouteId,
    error,
    loadWorkloadDetail,
    routeDetailId,
    selectedScopeIdentity,
    selectedWorkload?.id,
    updateDetailFailure,
  ]);

  useVisibilityPolling(
    refreshCurrentDetailAutomatically,
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

  const openSubmit = () => {
    const current = snapshotRef.current;
    if (!current.selectedScope) return;
    const immutableScope = scopeSnapshot(current.selectedScope);
    const currentCapabilities = capabilitiesForResource(
      current.authority,
      immutableScope.orgId,
      immutableScope.projectId,
    );
    if (!currentCapabilities.submitWorkloads) return;
    const interaction: SubmitInteraction = {
      id: ++submitInteractionSequenceRef.current,
      scope: immutableScope,
      epoch: scopeEpochRef.current,
    };
    submitInteractionRef.current = interaction;
    setSubmitInteraction(interaction);
    setAvailableShapes({ shapes: [], loading: true, error: null });
    client
      .listAvailableShapes(immutableScope, scopeOperationAbortRef.current.signal)
      .then((shapes) => {
        // Discarded when the scope moved under it. Offering another project's
        // hardware would submit against a fleet this scope may not use, and the
        // refusal would arrive after the form was filled in.
        if (submitInteractionRef.current?.id !== interaction.id) return;
        setAvailableShapes({ shapes, loading: false, error: null });
      })
      .catch((shapesError: unknown) => {
        if (submitInteractionRef.current?.id !== interaction.id) return;
        // Named rather than left empty. An empty list means "no fleet is
        // applied", which sends somebody to run `eyvan up` when the truth is
        // that the read failed.
        setAvailableShapes({
          shapes: [],
          loading: false,
          error: shapesError instanceof Error ? shapesError.message : "Unable to read what this scope can run on",
        });
      });
  };

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
    setLoadingDetail(false);
    updateDetailFailure(null);
    closeSubmitInteraction();
    setSubmissionAcknowledgement(null);
    setMobileNavigationOpen(false);
    setToast(null);
    setRefreshWarning(null);
    setError(null);
    navigate({ section: "overview" });
    void refreshDashboard(immutableScope);
  };

  const mutationIsCurrent = (epoch: number, scope: AuthScope) =>
    mountedRef.current &&
    scopeEpochRef.current === epoch &&
    snapshotRef.current.selectedScope !== null &&
    scopeKey(snapshotRef.current.selectedScope) === scopeKey(scope);

  const submitWorkload = async (input: SubmitWorkloadInput, interaction: SubmitInteraction) => {
    const current = snapshotRef.current;
    const activeInteraction = submitInteractionRef.current;
    // The opening scope is part of the form identity. Never retarget a form by
    // re-reading the selected scope when its submit event eventually runs.
    const immutableScope = scopeSnapshot(interaction.scope);
    if (
      activeInteraction?.id !== interaction.id ||
      interaction.epoch !== scopeEpochRef.current ||
      !current.selectedScope ||
      scopeKey(current.selectedScope) !== scopeKey(immutableScope)
    ) {
      throw new ApiError("The project scope changed; reopen the submission form", 409);
    }
    const currentCapabilities = capabilitiesForResource(
      current.authority,
      immutableScope.orgId,
      immutableScope.projectId,
    );
    if (!currentCapabilities.submitWorkloads) throw new ApiError("This scope does not accept workload submissions", 403);
    const epoch = interaction.epoch;
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

  const drainCell = async (cellId: string) => {
    // Re-checked here rather than trusted from the render that hid the button.
    // Authority is refreshed on every sweep, and a grant can be revoked between
    // the page being drawn and the button being pressed.
    if (!capabilitiesForResource(snapshotRef.current.authority, fleetScope.orgId, fleetScope.projectId).manageFleet) {
      throw new ApiError("This session may not manage fleet capacity", 403);
    }
    await client.drainCell(cellId, scopeOperationAbortRef.current.signal);
    // Inventory rather than the one record: a cell's rendered shape is a join
    // against its live leases, and reconstructing that here would put a figure
    // on the page the next refresh disagrees with.
    await refreshFleet();
  };

  const stopFleet = async (fleetName: string) => {
    // Same re-check as drainCell, for the same reason: authority is refreshed
    // on every sweep, and this one turns off a whole fleet rather than a cell.
    if (!capabilitiesForResource(snapshotRef.current.authority, fleetScope.orgId, fleetScope.projectId).manageFleet) {
      throw new ApiError("This session may not manage fleet capacity", 403);
    }
    await client.disableFleet(fleetName, scopeOperationAbortRef.current.signal);
    await refreshFleet();
    await refreshDashboard(desiredScopeRef.current);
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

  const operateWorkload = async (action: "cancel" | "checkpoint" | "retry" | "kick") => {
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

  const loadUsage = useCallback(
    (signal: AbortSignal) => client.getProjectUsage(fleetScope, signal),
    [client, fleetScope],
  );

  const loadPools = useCallback(
    (signal: AbortSignal) => client.listCapacityPools(fleetScope, signal),
    [client, fleetScope],
  );

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
  const loadQuota = useCallback(
    (signal: AbortSignal, options?: { refresh?: boolean }) =>
      withAuthorityRefresh(() => client.getTpuQuota(fleetScope, signal, options)),
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
  const loadCost = useCallback(
    (id: string, signal: AbortSignal) => withAuthorityRefresh(() => client.getWorkloadCost(id, explanationScope, signal)),
    [client, explanationScope.orgId, explanationScope.projectId, withAuthorityRefresh],
  );
  const loadCapacityEvents = useCallback(
    (signal: AbortSignal) => withAuthorityRefresh(() => client.listCapacityEvents(200, signal)),
    [client, withAuthorityRefresh],
  );
  const followedWorkloadId = selectedWorkload?.id;
  const followWorkloadLogs = useCallback(
    (handlers: LogFollowHandlers, signal: AbortSignal) => {
      if (!followedWorkloadId) return Promise.resolve();
      // Bound to the id captured when the callback was made, so a stream that
      // outlives a navigation cannot deliver one workload's output under
      // another's name. Changing the id changes the callback, which restarts
      // the follow rather than repointing a live one.
      return withAuthorityRefresh(() => client.followWorkloadLogs(followedWorkloadId, handlers, signal));
    },
    [client, followedWorkloadId, withAuthorityRefresh],
  );

  const browserIdentity = identityProvider();
  // A plane that authenticates its own page issues this browser no token, so
  // `currentIdentity()` is empty while the server has in fact resolved a
  // principal. Rendering that as no identity put "Authenticated user / OIDC
  // session" in the sidebar -- a name nobody has, a protocol this plane does
  // not run -- above a Sign out button for a session there is nothing to sign
  // out of. The authority response already carries the subject; use it.
  //
  // `development` is the honest mode: this is the loopback development path,
  // and it is also the mode that suppresses the sign-out control.
  const serverIdentity: AuthIdentity | undefined =
    !browserIdentity && authority?.subject
      ? {
          displayName: authority.displayName ?? authority.subject,
          detail: "",
          mode: "development",
          roles: [],
          scope: configuredScope(),
          capabilities,
        }
      : undefined;
  const resolvedIdentity = browserIdentity ?? serverIdentity;
  const identity: AuthIdentity | undefined = resolvedIdentity
    ? {
        ...resolvedIdentity,
        displayName:
          authority?.displayName ?? resolvedIdentity.displayName ?? authority?.subject ?? "Authenticated user",
        detail: selectedScope
          ? `${selectedScope.orgId}/${selectedScope.projectId}${selectedScope.roles.length ? ` · ${selectedScope.roles.join(", ")}` : ""}`
          : "Authority pending server verification",
        roles: selectedScope?.roles ?? [],
        scope: selectedScope ?? resolvedIdentity.scope,
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
    if (detailFailure?.workloadId === activeRoute.workloadId) {
      content = (
        <ErrorState
          message={detailFailure.error.message}
          onRetry={() => {
            setSelectedWorkload(null);
            updateDetailFailure(null);
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
          loadCost={loadCost}
          followLogs={followWorkloadLogs}
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
            onSubmit={openSubmit}
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
            onDrainCell={capabilities.manageFleet ? drainCell : undefined}
            onStopFleet={capabilities.manageFleet ? stopFleet : undefined}
            loadPools={loadPools}
          />
        );
        break;
      case "capacity":
        content = (
          <CapacityPage
            loadQuota={loadQuota}
            cells={data.cells}
            // Absent for a session that may not manage the fleet: cell history
            // is fleet-wide and answering it to a project reader would be a
            // cross-tenant read wearing a chart.
            loadCapacityEvents={capabilities.manageFleet ? loadCapacityEvents : undefined}
          />
        );
        break;
      case "billing":
        content = <BillingPage loadBilling={loadBilling} loadUsage={loadUsage} />;
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
        onSubmit={openSubmit}
        sectionAvailability={sectionAvailability}
        health={data?.health}
        live={canReadSelectedScope ? live : undefined}
        identity={identity}
        scopes={authority?.scopes ?? []}
        selectedScope={selectedScope ?? undefined}
        onScopeChange={selectScope}
        onLogout={identity?.mode === "development" ? undefined : () => void logout()}
      >
        {/* A scope commit remounts page-local filters, pending controls, and panels. */}
        <Fragment key={selectedScopeIdentity}>
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
        </Fragment>
      </Shell>
      {submitInteraction &&
        capabilities.submitWorkloads &&
        selectedScope &&
        scopeKey(selectedScope) === scopeKey(submitInteraction.scope) && (
          <SubmitWorkloadModal
            key={`${scopeKey(submitInteraction.scope)}:${submitInteraction.id}`}
            open
            project={submitInteraction.scope.projectId}
            shapes={availableShapes.shapes}
            loadingShapes={availableShapes.loading}
            shapesError={availableShapes.error}
            onClose={() => closeSubmitInteraction(submitInteraction.id)}
            onSubmit={(input) => submitWorkload(input, submitInteraction)}
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
