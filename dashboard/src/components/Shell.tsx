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
import { useEffect, useRef, type ReactNode } from "react";
import {
  Activity,
  Boxes,
  CircleDollarSign,
  ClipboardList,
  LayoutDashboard,
  LogOut,
  Menu,
  Plus,
  ServerCog,
  X,
} from "lucide-react";

import type { AuthIdentity } from "../auth/oidc";
import type { Route, Section } from "../routing";
import type { ControlPlaneHealth } from "../types";

interface ShellProps {
  route: Route;
  mobileNavigationOpen: boolean;
  onMobileNavigationChange: (open: boolean) => void;
  onNavigate: (section: Section) => void;
  onSubmit: () => void;
  health?: ControlPlaneHealth;
  identity?: AuthIdentity;
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
  { section: "budgets", label: "Budgets & policies", icon: CircleDollarSign },
  { section: "audit", label: "Audit events", icon: Activity },
];

const pageMeta: Record<Section, { title: string; description: string }> = {
  overview: { title: "Operations overview", description: "Global scheduling and capacity state" },
  workloads: { title: "Workloads", description: "Queue, placement, and execution lifecycle" },
  fleet: { title: "Fleet", description: "Cells, topology, resources, and capacity leases" },
  budgets: { title: "Budgets & policies", description: "Spend controls and scheduling governance" },
  audit: { title: "Audit events", description: "Durable control-plane decision history" },
};

export function Shell({
  route,
  mobileNavigationOpen,
  onMobileNavigationChange,
  onNavigate,
  onSubmit,
  health,
  identity,
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
          <span className={`environment-dot ${identity?.mode === "demo" ? "environment-dot-demo" : ""}`} aria-hidden="true" />
          <span>
            <small>Data source</small>
            <strong>{identity?.mode === "demo" ? "Explicit demo" : "Live API"}</strong>
          </span>
        </div>

        <nav className="sidebar-nav">
          <span className="nav-label">Control plane</span>
          {navigation.map(({ section, label, icon: Icon }) => (
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
              <small>{health ? `${health.service} reported ${health.status}` : "Awaiting health response"}</small>
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
            <button className="button button-primary" type="button" onClick={onSubmit}>
              <Plus size={17} aria-hidden="true" />
              <span className="button-label">Submit workload</span>
            </button>
          </div>
        </header>
        <main className="page-content">{children}</main>
      </div>
    </div>
  );
}
