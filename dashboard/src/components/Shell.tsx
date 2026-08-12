// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of EasyDeL.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { useEffect, useRef, type ReactNode } from "react";
import {
  Activity,
  Boxes,
  CircleDollarSign,
  ClipboardList,
  Gauge,
  LayoutDashboard,
  LogOut,
  Menu,
  Plus,
  ServerCog,
  X,
} from "lucide-react";

import type { AuthIdentity } from "../auth/oidc";
import type { Route, Section, SectionAvailability } from "../routing";
import type { ControlPlaneHealth, EffectiveProjectScope } from "../types";

interface ShellProps {
  route: Route;
  mobileNavigationOpen: boolean;
  onMobileNavigationChange: (open: boolean) => void;
  onNavigate: (section: Section) => void;
  onSubmit: () => void;
  sectionAvailability: SectionAvailability;
  health?: ControlPlaneHealth;
  /** Whether the change stream is attached; undefined in demo mode. */
  live?: boolean;
  identity?: AuthIdentity;
  scopes: EffectiveProjectScope[];
  selectedScope?: EffectiveProjectScope;
  onScopeChange: (scope: EffectiveProjectScope) => void;
  onLogout?: () => void;
  children: ReactNode;
}

const navigation: Array<{
  section: Section;
  label: string;
  icon: typeof LayoutDashboard;
}> = [
  { section: "overview", label: "Overview", icon: LayoutDashboard },
  { section: "workloads", label: "Workloads", icon: ClipboardList },
  { section: "fleet", label: "Fleet", icon: ServerCog },
  { section: "capacity", label: "Quota & capacity", icon: Gauge },
  { section: "budgets", label: "Budgets & policies", icon: CircleDollarSign },
  { section: "audit", label: "Audit events", icon: Activity },
];

const pageMeta: Record<Section, { title: string; description: string }> = {
  overview: { title: "Operations overview", description: "Global scheduling and capacity state" },
  workloads: { title: "Workloads", description: "Queue, placement, and execution lifecycle" },
  fleet: { title: "Fleet", description: "Cells, topology, resources, and capacity leases" },
  capacity: {
    title: "Quota & capacity",
    description: "Where provider TPU quota is free right now, and where nobody could tell",
  },
  budgets: { title: "Budgets & policies", description: "Spend controls and scheduling governance" },
  audit: { title: "Audit events", description: "Durable control-plane decision history" },
};

export function Shell({
  route,
  mobileNavigationOpen,
  onMobileNavigationChange,
  onNavigate,
  onSubmit,
  sectionAvailability,
  health,
  live,
  identity,
  scopes,
  selectedScope,
  onScopeChange,
  onLogout,
  children,
}: ShellProps) {
  const drawerRef = useRef<HTMLElement>(null);
  const meta = pageMeta[route.section];
  const connected = health?.status === "ok";
  const initials = identity?.displayName
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("") || "ME";

  useEffect(() => {
    if (!mobileNavigationOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onMobileNavigationChange(false);
    };
    globalThis.addEventListener("keydown", onKeyDown);
    drawerRef.current?.querySelector<HTMLButtonElement>("button")?.focus();
    return () => globalThis.removeEventListener("keydown", onKeyDown);
  }, [mobileNavigationOpen, onMobileNavigationChange]);

  const chooseSection = (section: Section) => {
    onNavigate(section);
    onMobileNavigationChange(false);
  };

  return (
    <div className="app-shell">
      {mobileNavigationOpen && (
        <button
          className="drawer-scrim"
          type="button"
          aria-label="Close navigation"
          onClick={() => onMobileNavigationChange(false)}
        />
      )}
      <aside
        className={`sidebar ${mobileNavigationOpen ? "sidebar-open" : ""}`}
        ref={drawerRef}
        aria-label="Primary navigation"
      >
        <div className="brand-row">
          <span className="brand-mark" aria-hidden="true">
            <Boxes size={20} strokeWidth={2.1} />
          </span>
          <span className="brand-name">Eyvan</span>
          <button
            className="icon-button sidebar-close"
            type="button"
            aria-label="Close navigation"
            title="Close navigation"
            onClick={() => onMobileNavigationChange(false)}
          >
            <X size={18} />
          </button>
        </div>

        <div className="environment-switcher">
          <span
            className={`environment-dot ${identity?.mode === "demo" ? "environment-dot-demo" : ""}`}
            aria-hidden="true"
          />
          <label>
            <small>Project scope</small>
            <select
              aria-label="Project scope"
              value={selectedScope ? JSON.stringify([selectedScope.orgId, selectedScope.projectId]) : ""}
              disabled={scopes.length < 2}
              onChange={(event) => {
                const scope = scopes.find(
                  (candidate) => JSON.stringify([candidate.orgId, candidate.projectId]) === event.target.value,
                );
                if (scope) onScopeChange(scope);
              }}
            >
              {!selectedScope && <option value="">Authority pending</option>}
              {scopes.map((scope) => (
                <option
                  key={JSON.stringify([scope.orgId, scope.projectId])}
                  value={JSON.stringify([scope.orgId, scope.projectId])}
                >
                  {scope.displayName || `${scope.orgId}/${scope.projectId}`}
                  {!scope.enabled ? " (disabled - history only)" : ""}
                </option>
              ))}
            </select>
          </label>
        </div>

        <nav className="sidebar-nav">
          <span className="nav-label">Control plane</span>
          {navigation.filter(({ section }) => sectionAvailability[section] !== false).map(({ section, label, icon: Icon }) => (
            <button
              key={section}
              className={`nav-item ${route.section === section ? "nav-item-active" : ""}`}
              type="button"
              aria-current={route.section === section ? "page" : undefined}
              onClick={() => chooseSection(section)}
            >
              <Icon size={18} strokeWidth={1.9} aria-hidden="true" />
              <span>{label}</span>
            </button>
          ))}
        </nav>

        <div className="sidebar-footer">
          <div className="system-health">
            <span className={`health-indicator ${connected ? "" : "health-indicator-pending"}`} aria-hidden="true" />
            <span>
              <strong>{connected ? "Control plane connected" : health?.status === "demo" ? "Demo mode" : "Checking control plane"}</strong>
              <small>
                {/* A stream that stopped and a fleet where nothing is happening look
                    identical from the page. The first is a bug; the second is a Tuesday. */}
                {live ? "Live -- updating as changes commit" : health ? `${health.service} reported ${health.status}` : "Awaiting health response"}
              </small>
            </span>
          </div>
          {onLogout ? <button className="profile-button" type="button" aria-label={`Sign out ${identity?.displayName ?? "current user"}`} title="Sign out" onClick={onLogout}>
            <span className="avatar" aria-hidden="true">{initials}</span>
            <span>
              <strong>{identity?.displayName ?? "Authenticated user"}</strong>
              <small>{identity?.detail ?? "OIDC session"}</small>
            </span>
            <LogOut size={15} aria-hidden="true" />
          </button> : <div className="profile-button profile-static">
            <span className="avatar" aria-hidden="true">{initials}</span>
            <span>
              <strong>{identity?.displayName ?? "Unauthenticated"}</strong>
              <small>{identity?.detail ?? "No identity"}</small>
            </span>
          </div>}
        </div>
      </aside>

      <div className="main-column">
        <header className="topbar">
          <button
            className="icon-button mobile-menu"
            type="button"
            aria-label="Open navigation"
            title="Open navigation"
            onClick={() => onMobileNavigationChange(true)}
          >
            <Menu size={20} />
          </button>
          <div className="page-title">
            <h1>{"workloadId" in route ? "Workload detail" : meta.title}</h1>
            <p>{"workloadId" in route ? route.workloadId : meta.description}</p>
          </div>
          <div className="topbar-actions">
            {identity?.capabilities.submitWorkloads && (
              <button className="button button-primary" type="button" onClick={onSubmit}>
                <Plus size={17} aria-hidden="true" />
                <span className="button-label">Submit workload</span>
              </button>
            )}
          </div>
        </header>
        <main className="page-content">{children}</main>
      </div>
    </div>
  );
}
