// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import {
  AlertTriangle,
  ArrowUp,
  Check,
  ChevronDown,
  CircleStop,
  Inbox,
  MessageSquarePlus,
  Search,
  Sparkles,
  Wrench,
  X,
} from "lucide-react";
import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
} from "react";

import type { AuthScope } from "../auth/oidc";
import type {
  AssistantDraft,
  AssistantMessage,
  AssistantProvider,
  AssistantProviderList,
  AssistantTurn,
  TeamRequest,
} from "../types";
import { AssistantMarkdown } from "./AssistantMarkdown";
import { DOCK_MAX_WIDTH, DOCK_MIN_WIDTH } from "./assistantDockState";

/** Methods the dock calls -- always as `client.method(...)`: the real class needs its `this`. */
export interface AssistantDockClient {
  listAssistantProviders(signal?: AbortSignal): Promise<AssistantProviderList>;
  assistantChat(
    messages: AssistantMessage[],
    selection?: { provider_id: string; model: string } | null,
    scope?: AuthScope,
    signal?: AbortSignal,
  ): Promise<AssistantTurn>;
  submitAssistantDraft(draft: AssistantDraft): Promise<{ id: string }>;
  listTeamRequests(signal?: AbortSignal): Promise<TeamRequest[]>;
  resolveTeamRequest(
    requestId: string,
    resolution: "approved" | "declined" | "withdrawn",
    note?: string,
  ): Promise<TeamRequest>;
}

export interface AssistantDockProps {
  client: AssistantDockClient;
  /** False keeps the dock mounted (an answer in flight survives) but hidden. */
  open: boolean;
  width: number;
  onWidthChange: (width: number) => void;
  onClose: () => void;
  /** Fleet admins configure providers and resolve everyone's requests. */
  canAdminister: boolean;
  /** Requests exist only on a team-mode plane. */
  teamMode: boolean;
  onOpenSettings?: () => void;
  onOpenWorkload?: (workloadId: string) => void;
  /** Bumped when Settings changes the providers, so the picker re-reads them. */
  providersRevision?: number;
}

/** The conversation survives page navigation and reloads, but not the browser tab. */
export const CONVERSATION_KEY = "eyvan.assistant.conversation";
/** The last provider/model choice survives across tabs. */
export const SELECTION_KEY = "eyvan.assistant.selection";

export const STARTERS = [
  "How much TPU do I need to SFT an 8B model?",
  "Why is my job pending?",
  "What's my quota?",
  "Which fleets can I use right now?",
];

type DraftStatus = "pending" | "confirming" | "submitting" | "submitted" | "dismissed";

export type ChatItem =
  | { kind: "user"; id: string; content: string }
  | {
      kind: "assistant";
      id: string;
      content: string;
      tools: Array<{ tool: string; ok: boolean }>;
      model?: string;
    }
  | { kind: "draft"; id: string; draft: AssistantDraft; status: DraftStatus; workloadId?: string; error?: string }
  | { kind: "request"; id: string; request: TeamRequest };

type Selection = { provider_id: string; model: string };

let sequence = 0;
const nextId = () => `${Date.now().toString(36)}-${(sequence += 1)}`;
const message = (error: unknown, fallback: string) => (error instanceof Error ? error.message : fallback);
const defaultModel = (provider: AssistantProvider) =>
  provider.models.includes(provider.default_model) ? provider.default_model : (provider.models[0] ?? "");

function loadConversation(): ChatItem[] {
  try {
    const parsed: unknown = JSON.parse(globalThis.sessionStorage?.getItem(CONVERSATION_KEY) ?? "[]");
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (item): item is ChatItem =>
          typeof item === "object" &&
          item !== null &&
          typeof (item as ChatItem).id === "string" &&
          ["user", "assistant", "draft", "request"].includes((item as ChatItem).kind),
      )
      .map((item) =>
        // A submission that was mid-flight when the page went away is shown
        // as undecided again; the user sees its state rather than a spinner.
        item.kind === "draft" && (item.status === "submitting" || item.status === "confirming")
          ? { ...item, status: "pending" }
          : item,
      );
  } catch {
    return [];
  }
}

function saveConversation(items: ChatItem[]): void {
  try {
    if (items.length) globalThis.sessionStorage?.setItem(CONVERSATION_KEY, JSON.stringify(items));
    else globalThis.sessionStorage?.removeItem(CONVERSATION_KEY);
  } catch {
    // A conversation that cannot be remembered still works in this page.
  }
}

function loadSelection(): Selection | null {
  try {
    const parsed = JSON.parse(globalThis.localStorage?.getItem(SELECTION_KEY) ?? "null") as Selection | null;
    return parsed && typeof parsed.provider_id === "string" && typeof parsed.model === "string" ? parsed : null;
  } catch {
    return null;
  }
}

function saveSelection(selection: Selection): void {
  try {
    globalThis.localStorage?.setItem(SELECTION_KEY, JSON.stringify(selection));
  } catch {
    // Remembering the model is a convenience.
  }
}

const toMessages = (items: ChatItem[]): AssistantMessage[] =>
  items.flatMap((item) =>
    item.kind === "user" || item.kind === "assistant" ? [{ role: item.kind, content: item.content }] : [],
  );

interface PickerOption {
  provider: AssistantProvider;
  model: string;
  label: string;
}

function ModelPicker({
  providers,
  current,
  onChoose,
}: {
  providers: AssistantProvider[];
  current: { provider: AssistantProvider; model: string };
  onChoose: (selection: Selection) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const rootRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const listId = useId();

  const options = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return providers
      .flatMap((provider) =>
        provider.models.map((model) => ({ provider, model, label: `${provider.name} · ${model}` })),
      )
      .filter((option) => !needle || option.label.toLowerCase().includes(needle));
  }, [providers, query]);
  const groups = useMemo(() => {
    const byProvider = new Map<
      string,
      { provider: AssistantProvider; options: Array<PickerOption & { index: number }> }
    >();
    options.forEach((option, index) => {
      const group = byProvider.get(option.provider.id) ?? { provider: option.provider, options: [] };
      group.options.push({ ...option, index });
      byProvider.set(option.provider.id, group);
    });
    return [...byProvider.values()];
  }, [options]);

  useEffect(() => {
    if (!open) return;
    searchRef.current?.focus();
    const onPointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    globalThis.addEventListener("pointerdown", onPointerDown);
    return () => globalThis.removeEventListener("pointerdown", onPointerDown);
  }, [open]);

  const close = () => {
    setOpen(false);
    setQuery("");
    buttonRef.current?.focus();
  };
  const choose = (option: PickerOption) => {
    onChoose({ provider_id: option.provider.id, model: option.model });
    close();
  };
  const onKeyDown = (event: ReactKeyboardEvent) => {
    if (event.key === "Escape") {
      // The popover owns this Escape; the dock must not close with it.
      event.preventDefault();
      event.stopPropagation();
      close();
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      setActive((value) => Math.min(options.length - 1, value + 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActive((value) => Math.max(0, value - 1));
    } else if (event.key === "Enter") {
      event.preventDefault();
      const option = options[active];
      if (option) choose(option);
    }
  };
  const label = `${current.provider.name} · ${current.model}`;

  return (
    <div className="dock-picker" ref={rootRef}>
      <button
        ref={buttonRef}
        className="dock-picker-button"
        type="button"
        aria-label="Assistant model"
        aria-haspopup="listbox"
        aria-expanded={open}
        title={label}
        onClick={() => {
          setActive(
            Math.max(
              0,
              options.findIndex((option) => option.label === label),
            ),
          );
          setOpen((value) => !value);
        }}
      >
        <span className="dock-picker-provider">{current.provider.name}</span>
        <span className="dock-picker-sep" aria-hidden="true">
          ·
        </span>
        <span className="dock-picker-model">{current.model}</span>
        <ChevronDown size={12} aria-hidden="true" />
      </button>
      {open && (
        <div className="dock-picker-popover" onKeyDown={onKeyDown}>
          <label className="dock-picker-search">
            <Search size={13} aria-hidden="true" />
            <input
              ref={searchRef}
              role="combobox"
              aria-label="Search models"
              aria-controls={listId}
              aria-expanded="true"
              aria-activedescendant={options[active] ? `${listId}-${active}` : undefined}
              placeholder="Search provider or model"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setActive(0);
              }}
            />
          </label>
          <div className="dock-picker-list" role="listbox" id={listId} aria-label="Assistant models">
            {groups.map((group) => (
              <div key={group.provider.id} role="group" aria-label={group.provider.name}>
                <div className="dock-picker-group" aria-hidden="true">
                  {group.provider.name}
                </div>
                {group.options.map((option) => {
                  const selected = option.provider.id === current.provider.id && option.model === current.model;
                  return (
                    <div
                      key={option.label}
                      id={`${listId}-${option.index}`}
                      role="option"
                      aria-selected={selected}
                      aria-label={option.label}
                      className={`dock-picker-option ${option.index === active ? "dock-picker-option-active" : ""}`}
                      onMouseDown={(event) => event.preventDefault()}
                      onMouseEnter={() => setActive(option.index)}
                      onClick={() => choose(option)}
                    >
                      <span>{option.model}</span>
                      {selected && <Check size={12} aria-hidden="true" />}
                    </div>
                  );
                })}
              </div>
            ))}
            {options.length === 0 && <div className="dock-picker-empty">No models match.</div>}
          </div>
        </div>
      )}
    </div>
  );
}

function DraftCard({
  item,
  onRequestSubmit,
  onCancel,
  onConfirm,
  onDismiss,
  onOpenWorkload,
}: {
  item: Extract<ChatItem, { kind: "draft" }>;
  onRequestSubmit: () => void;
  onCancel: () => void;
  onConfirm: () => void;
  onDismiss: () => void;
  onOpenWorkload?: (workloadId: string) => void;
}) {
  const { draft } = item;
  const where = `${draft.org_id}/${draft.project_id}`;
  return (
    <div
      className={`dock-card dock-card-${item.status}`}
      data-testid="assistant-draft"
      role="group"
      aria-label={`Drafted workload ${draft.spec.name}`}
    >
      <div className="dock-card-title">
        <strong>{draft.spec.name}</strong>
        <span className="dock-card-meta">draft · {where}</span>
      </div>
      <details className="dock-card-spec">
        <summary>Specification</summary>
        <pre>{JSON.stringify(draft.spec, null, 2)}</pre>
      </details>
      {item.error && (
        <p className="dock-card-error" role="alert">
          {item.error}
        </p>
      )}
      <div className="dock-card-actions">
        {item.status === "pending" && (
          <>
            <button className="button button-primary" type="button" onClick={onRequestSubmit}>
              Submit
            </button>
            <button className="button button-secondary" type="button" onClick={onDismiss}>
              Dismiss
            </button>
          </>
        )}
        {item.status === "confirming" && (
          <>
            <span className="dock-card-confirm">Run it in {where} under your identity?</span>
            <button className="button button-primary" type="button" onClick={onConfirm}>
              Confirm submit
            </button>
            <button className="button button-secondary" type="button" onClick={onCancel}>
              Cancel
            </button>
          </>
        )}
        {item.status === "submitting" && <span role="status">Submitting…</span>}
        {item.status === "submitted" && (
          <>
            <span role="status">Submitted {item.workloadId}</span>
            {onOpenWorkload && item.workloadId && (
              <button
                className="button button-secondary"
                type="button"
                onClick={() => onOpenWorkload(item.workloadId as string)}
              >
                Open
              </button>
            )}
          </>
        )}
        {item.status === "dismissed" && <span className="dock-card-meta">Dismissed</span>}
      </div>
    </div>
  );
}

function RequestsPanel({
  client,
  canAdminister,
  revision,
}: {
  client: AssistantDockClient;
  canAdminister: boolean;
  revision: number;
}) {
  const [requests, setRequests] = useState<TeamRequest[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    Promise.resolve()
      .then(() => client.listTeamRequests(controller.signal))
      .then((value) => {
        if (!controller.signal.aborted) setRequests(value);
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(message(failure, "Could not read requests"));
      });
    return () => controller.abort();
  }, [client, revision, reload]);

  const resolve = async (id: string, resolution: "approved" | "declined" | "withdrawn") => {
    setError(null);
    try {
      await client.resolveTeamRequest(id, resolution);
      setReload((value) => value + 1);
    } catch (failure) {
      setError(message(failure, "The request was not updated"));
    }
  };

  return (
    <div className="dock-requests" role="tabpanel" aria-label="Requests">
      {error && (
        <div className="dock-alert" role="alert">
          <AlertTriangle size={14} aria-hidden="true" />
          {error}
        </div>
      )}
      <p className="dock-hint">
        {canAdminister
          ? "Capacity and approval requests to the fleet admins."
          : "Your capacity and approval requests. Ask the assistant to file one."}
      </p>
      {requests === null && !error && <p className="dock-hint">Reading requests…</p>}
      {requests?.length === 0 && (
        <div className="dock-empty">
          <Inbox size={18} aria-hidden="true" />
          <span>No requests.</span>
        </div>
      )}
      {requests?.map((item) => (
        <div key={item.id} className="dock-card" role="group" aria-label={`Request ${item.summary}`}>
          <div className="dock-card-title">
            <strong>{item.summary}</strong>
            <span className={`dock-state dock-state-${item.state}`}>{item.state}</span>
          </div>
          <span className="dock-card-meta">
            {item.kind} · from {item.subject}
            {typeof item.details?.held_workload_id === "string" ? ` · holds ${item.details.held_workload_id}` : ""}
          </span>
          {item.state === "open" && (
            <div className="dock-card-actions">
              {canAdminister ? (
                <>
                  <button
                    className="button button-primary"
                    type="button"
                    onClick={() => void resolve(item.id, "approved")}
                  >
                    Approve
                  </button>
                  <button
                    className="button button-secondary"
                    type="button"
                    onClick={() => void resolve(item.id, "declined")}
                  >
                    Decline
                  </button>
                </>
              ) : (
                <button
                  className="button button-secondary"
                  type="button"
                  onClick={() => void resolve(item.id, "withdrawn")}
                >
                  Withdraw
                </button>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

export function AssistantDock({
  client,
  open,
  width,
  onWidthChange,
  onClose,
  canAdminister,
  teamMode,
  onOpenSettings,
  onOpenWorkload,
  providersRevision = 0,
}: AssistantDockProps) {
  const [items, setItems] = useState<ChatItem[]>(loadConversation);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [providers, setProviders] = useState<AssistantProvider[] | null>(null);
  const [providersError, setProvidersError] = useState<string | null>(null);
  const [selection, setSelection] = useState<Selection | null>(loadSelection);
  const [tab, setTab] = useState<"chat" | "requests">("chat");
  const [requestsRevision, setRequestsRevision] = useState(0);
  const abortRef = useRef<AbortController | null>(null);
  const logRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const dockRef = useRef<HTMLElement>(null);

  useEffect(() => saveConversation(items), [items]);

  // Read when the dock opens rather than on every page load: a closed dock
  // costs the plane nothing.
  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    setProvidersError(null);
    Promise.resolve()
      .then(() => client.listAssistantProviders(controller.signal))
      .then((value) => {
        if (!controller.signal.aborted) setProviders(value.providers);
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) {
          setProviders([]);
          setProvidersError(message(failure, "Could not read assistant providers"));
        }
      });
    return () => controller.abort();
  }, [client, open, providersRevision]);

  // An answer that is no longer wanted is not paid for.
  useEffect(() => () => abortRef.current?.abort(), []);

  const available = useMemo(
    () => (providers ?? []).filter((provider) => provider.enabled !== false && provider.models.length > 0),
    [providers],
  );
  const current = useMemo(() => {
    const chosen = available.find((provider) => provider.id === selection?.provider_id);
    if (chosen && selection && chosen.models.includes(selection.model))
      return { provider: chosen, model: selection.model };
    const first = chosen ?? available[0];
    return first ? { provider: first, model: defaultModel(first) } : null;
  }, [available, selection]);

  const close = useCallback(() => {
    onClose();
    globalThis.document?.getElementById("assistant-dock-toggle")?.focus();
  }, [onClose]);

  // A dock remembered open must not steal focus from the page on load; one the
  // user just opened takes it.
  const mountedOpenRef = useRef(open);
  useEffect(() => {
    if (!open) {
      mountedOpenRef.current = false;
      return;
    }
    if (!mountedOpenRef.current) (textareaRef.current ?? dockRef.current)?.focus();
    mountedOpenRef.current = false;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || event.defaultPrevented) return;
      // A modal on top owns Escape.
      if (globalThis.document?.querySelector('[aria-modal="true"]')) return;
      close();
    };
    globalThis.addEventListener("keydown", onKeyDown);
    return () => globalThis.removeEventListener("keydown", onKeyDown);
  }, [open, close]);

  useEffect(() => {
    const log = logRef.current;
    if (log) log.scrollTop = log.scrollHeight;
  }, [items, busy]);

  useEffect(() => {
    const area = textareaRef.current;
    if (!area) return;
    area.style.height = "auto";
    area.style.height = `${Math.min(area.scrollHeight, 200)}px`;
  }, [input]);

  const send = async (raw: string) => {
    const text = raw.trim();
    if (!text || busy || !current) return;
    const next: ChatItem[] = [...items, { kind: "user", id: nextId(), content: text }];
    setItems(next);
    setInput("");
    setBusy(true);
    setError(null);
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const turn = await client.assistantChat(
        toMessages(next),
        { provider_id: current.provider.id, model: current.model },
        undefined,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      const added: ChatItem[] = [
        {
          kind: "assistant",
          id: nextId(),
          content: turn.reply || "(no answer)",
          tools: turn.tool_calls ?? [],
          model: turn.model ?? current.model,
        },
        ...(turn.drafts ?? []).map((draft): ChatItem => ({ kind: "draft", id: nextId(), draft, status: "pending" })),
        ...(turn.requests ?? []).map((request): ChatItem => ({ kind: "request", id: nextId(), request })),
      ];
      setItems((existing) => [...existing, ...added]);
      if (turn.requests?.length) setRequestsRevision((value) => value + 1);
    } catch (failure) {
      if (!controller.signal.aborted) setError(message(failure, "The assistant could not answer"));
    } finally {
      if (abortRef.current === controller) {
        abortRef.current = null;
        setBusy(false);
      }
    }
  };

  const stop = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setBusy(false);
  };

  const newChat = () => {
    stop();
    setItems([]);
    setError(null);
    setInput("");
    setTab("chat");
    textareaRef.current?.focus();
  };

  const updateDraft = (id: string, patch: Partial<Extract<ChatItem, { kind: "draft" }>>) =>
    setItems((existing) =>
      existing.map((item) => (item.id === id && item.kind === "draft" ? { ...item, ...patch } : item)),
    );

  const confirmDraft = async (item: Extract<ChatItem, { kind: "draft" }>) => {
    updateDraft(item.id, { status: "submitting", error: undefined });
    try {
      const created = await client.submitAssistantDraft(item.draft);
      updateDraft(item.id, { status: "submitted", workloadId: created.id });
    } catch (failure) {
      updateDraft(item.id, { status: "pending", error: message(failure, "The workload was not submitted") });
    }
  };

  const chooseModel = (choice: Selection) => {
    setSelection(choice);
    saveSelection(choice);
  };

  const startResize = (event: ReactPointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = width;
    const move = (moved: PointerEvent) => onWidthChange(startWidth + (startX - moved.clientX));
    const end = () => {
      globalThis.removeEventListener("pointermove", move);
      globalThis.removeEventListener("pointerup", end);
      globalThis.document?.body.classList.remove("dock-resizing");
    };
    globalThis.addEventListener("pointermove", move);
    globalThis.addEventListener("pointerup", end);
    globalThis.document?.body.classList.add("dock-resizing");
  };

  const loading = providers === null;
  const unconfigured = !loading && available.length === 0;

  return (
    <aside
      className="assistant-dock"
      id="assistant-dock"
      aria-label="Assistant panel"
      hidden={!open}
      ref={dockRef}
      tabIndex={-1}
    >
      <div
        className="dock-resize"
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize assistant panel"
        aria-valuemin={DOCK_MIN_WIDTH}
        aria-valuemax={DOCK_MAX_WIDTH}
        aria-valuenow={width}
        tabIndex={0}
        onPointerDown={startResize}
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") onWidthChange(width + 16);
          else if (event.key === "ArrowRight") onWidthChange(width - 16);
        }}
      />
      <header className="dock-header">
        <span className="dock-title">
          <Sparkles size={14} aria-hidden="true" />
          <span>Assistant</span>
        </span>
        {current && <ModelPicker providers={available} current={current} onChoose={chooseModel} />}
        <span className="dock-header-actions">
          <button className="icon-button" type="button" aria-label="New chat" title="New chat" onClick={newChat}>
            <MessageSquarePlus size={15} aria-hidden="true" />
          </button>
          <button
            className="icon-button"
            type="button"
            aria-label="Close assistant"
            title="Close (Esc)"
            onClick={close}
          >
            <X size={15} aria-hidden="true" />
          </button>
        </span>
      </header>
      {teamMode && (
        <div className="tabs dock-tabs" role="tablist" aria-label="Assistant views">
          <button
            type="button"
            role="tab"
            aria-selected={tab === "chat"}
            className={tab === "chat" ? "tab-active" : ""}
            onClick={() => setTab("chat")}
          >
            Chat
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === "requests"}
            className={tab === "requests" ? "tab-active" : ""}
            onClick={() => setTab("requests")}
          >
            Requests
          </button>
        </div>
      )}

      {tab === "requests" && teamMode ? (
        <RequestsPanel client={client} canAdminister={canAdminister} revision={requestsRevision} />
      ) : (
        <>
          <div className="dock-log" role="log" aria-label="Conversation" ref={logRef}>
            {loading && <p className="dock-hint">Checking whether this plane runs an assistant…</p>}
            {unconfigured && (
              <div className="dock-empty dock-empty-setup">
                <Sparkles size={20} aria-hidden="true" />
                <strong>No assistant provider is configured yet.</strong>
                {providersError && <span className="dock-hint">{providersError}</span>}
                {canAdminister ? (
                  onOpenSettings && (
                    <button className="button button-primary" type="button" onClick={onOpenSettings}>
                      Set up a provider
                    </button>
                  )
                ) : (
                  <span>Ask a fleet admin to add one.</span>
                )}
              </div>
            )}
            {current && items.length === 0 && (
              <div className="dock-welcome">
                <Sparkles size={20} aria-hidden="true" />
                <strong>Ask about this plane</strong>
                <span className="dock-hint">
                  Answers come from this plane&apos;s data with your permissions. It drafts jobs; nothing runs until you
                  confirm.
                </span>
                <div className="dock-starters" role="group" aria-label="Suggestions">
                  {STARTERS.map((starter) => (
                    <button key={starter} className="dock-starter" type="button" onClick={() => void send(starter)}>
                      {starter}
                    </button>
                  ))}
                </div>
              </div>
            )}
            {items.map((item) => {
              switch (item.kind) {
                case "user":
                  return (
                    <div key={item.id} className="dock-message dock-message-user">
                      <div className="dock-bubble">{item.content}</div>
                    </div>
                  );
                case "assistant":
                  return (
                    <div key={item.id} className="dock-message dock-message-assistant">
                      <AssistantMarkdown text={item.content} />
                      {item.tools.length > 0 && (
                        <div className="dock-tools" aria-label="Tools used">
                          {item.tools.map((call, index) => (
                            <span
                              key={`${call.tool}-${index}`}
                              className={`dock-tool ${call.ok ? "" : "dock-tool-failed"}`}
                              title={call.ok ? `${call.tool} answered` : `${call.tool} failed`}
                            >
                              <Wrench size={10} aria-hidden="true" />
                              used {call.tool} {call.ok ? "✓" : "✗"}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                case "draft":
                  return (
                    <DraftCard
                      key={item.id}
                      item={item}
                      onRequestSubmit={() => updateDraft(item.id, { status: "confirming", error: undefined })}
                      onCancel={() => updateDraft(item.id, { status: "pending" })}
                      onConfirm={() => void confirmDraft(item)}
                      onDismiss={() => updateDraft(item.id, { status: "dismissed" })}
                      onOpenWorkload={onOpenWorkload}
                    />
                  );
                default:
                  return (
                    <div key={item.id} className="dock-note" data-testid="assistant-request">
                      <Inbox size={12} aria-hidden="true" />
                      <span>
                        Filed a {item.request.kind} request: {item.request.summary} ({item.request.state})
                      </span>
                    </div>
                  );
              }
            })}
            {busy && (
              <div className="dock-typing" role="status" aria-label="Assistant is thinking">
                <span />
                <span />
                <span />
              </div>
            )}
          </div>
          {error && (
            <div className="dock-alert" role="alert">
              <AlertTriangle size={14} aria-hidden="true" />
              <span>{error}</span>
            </div>
          )}
          {current && (
            <form
              className="dock-composer"
              aria-label="Ask the assistant"
              onSubmit={(event) => {
                event.preventDefault();
                void send(input);
              }}
            >
              <textarea
                ref={textareaRef}
                aria-label="Message the assistant"
                rows={1}
                placeholder="Ask about fleets, quota, sizing…"
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
                    event.preventDefault();
                    void send(input);
                  }
                }}
              />
              {busy ? (
                <button
                  className="dock-send dock-stop"
                  type="button"
                  aria-label="Stop generating"
                  title="Stop"
                  onClick={stop}
                >
                  <CircleStop size={15} aria-hidden="true" />
                </button>
              ) : (
                <button
                  className="dock-send"
                  type="submit"
                  aria-label="Send message"
                  title="Send (Enter)"
                  disabled={!input.trim()}
                >
                  <ArrowUp size={15} aria-hidden="true" />
                </button>
              )}
            </form>
          )}
        </>
      )}
    </aside>
  );
}
