// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Check, LoaderCircle, Send, X } from "lucide-react";
import { useCallback, useEffect, useState, type FormEvent } from "react";

import type {
  AssistantDraft,
  AssistantMessage,
  AssistantProvider,
  AssistantProviderInput,
  AssistantProviderList,
  AssistantProviderUpdate,
  AssistantStatus,
  AssistantTurn,
  TeamRequest,
} from "../types";

export interface AssistantClient {
  getAssistantStatus: (signal?: AbortSignal) => Promise<AssistantStatus>;
  listAssistantProviders: (signal?: AbortSignal) => Promise<AssistantProviderList>;
  createAssistantProvider?: (input: AssistantProviderInput) => Promise<AssistantProvider>;
  updateAssistantProvider?: (id: string, changes: AssistantProviderUpdate) => Promise<AssistantProvider>;
  deleteAssistantProvider?: (id: string) => Promise<{ deleted: string }>;
  assistantChat: (
    messages: AssistantMessage[],
    selection?: { provider_id: string; model: string } | null,
  ) => Promise<AssistantTurn>;
  submitAssistantDraft: (draft: AssistantDraft) => Promise<{ id: string }>;
  listTeamRequests: (signal?: AbortSignal) => Promise<TeamRequest[]>;
  resolveTeamRequest: (
    requestId: string,
    resolution: "approved" | "declined" | "withdrawn",
    note?: string,
  ) => Promise<TeamRequest>;
}

interface AssistantPageProps {
  client: AssistantClient;
  /** Fleet admins see everyone's requests and resolve them. */
  canAdminister: boolean;
  /** Requests exist only on a team-mode plane. */
  teamMode: boolean;
}

type DraftState = { draft: AssistantDraft; status: "pending" | "submitting" | "submitted" | "dismissed"; id?: string };

type ProviderForm = {
  name: string;
  baseUrl: string;
  apiKey: string;
  models: string;
  defaultModel: string;
  maxSteps: string;
  enabled: boolean;
  clearApiKey: boolean;
};

const emptyProviderForm = (): ProviderForm => ({
  name: "",
  baseUrl: "",
  apiKey: "",
  models: "",
  defaultModel: "",
  maxSteps: "8",
  enabled: true,
  clearApiKey: false,
});

const message = (error: unknown, fallback: string) => (error instanceof Error ? error.message : fallback);
const splitModels = (models: string) => [
  ...new Set(
    models
      .split(",")
      .map((model) => model.trim())
      .filter(Boolean),
  ),
];
const defaultModel = (provider: AssistantProvider) =>
  provider.models.includes(provider.default_model) ? provider.default_model : (provider.models[0] ?? "");

export function AssistantPage({ client, canAdminister, teamMode }: AssistantPageProps) {
  const [status, setStatus] = useState<AssistantStatus | null>(null);
  const [history, setHistory] = useState<AssistantMessage[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drafts, setDrafts] = useState<DraftState[]>([]);
  const [requests, setRequests] = useState<TeamRequest[]>([]);
  const [revision, setRevision] = useState(0);
  const [providerList, setProviderList] = useState<AssistantProviderList | null>(null);
  const [providerId, setProviderId] = useState("");
  const [model, setModel] = useState("");
  const [providerRevision, setProviderRevision] = useState(0);
  const [editingProvider, setEditingProvider] = useState<AssistantProvider | null>(null);
  const [providerForm, setProviderForm] = useState<ProviderForm>(emptyProviderForm);
  const [providerBusy, setProviderBusy] = useState(false);

  const providers = providerList?.providers ?? [];
  const availableProviders = providers.filter((provider) => provider.enabled !== false);
  const selectedProvider = availableProviders.find((provider) => provider.id === providerId) ?? availableProviders[0];
  const selectedModel = selectedProvider
    ? selectedProvider.models.includes(model)
      ? model
      : defaultModel(selectedProvider)
    : "";
  const formModels = splitModels(providerForm.models);
  const formDefaultModel = formModels.includes(providerForm.defaultModel)
    ? providerForm.defaultModel
    : (formModels[0] ?? "");
  const canManageProviders = canAdminister && providerList?.can_manage === true;

  useEffect(() => {
    const controller = new AbortController();
    client
      .getAssistantStatus(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setStatus(value);
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(message(failure, "Could not read the assistant status"));
      });
    client
      .listAssistantProviders(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setProviderList(value);
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(message(failure, "Could not read assistant providers"));
      });
    return () => controller.abort();
  }, [client, providerRevision]);

  useEffect(() => {
    if (!teamMode) return;
    const controller = new AbortController();
    client
      .listTeamRequests(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setRequests(value);
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(message(failure, "Could not read requests"));
      });
    return () => controller.abort();
  }, [client, teamMode, revision]);

  const reloadRequests = useCallback(() => setRevision((value) => value + 1), []);

  const send = async (event: FormEvent) => {
    event.preventDefault();
    const text = input.trim();
    if (!text) return;
    const next: AssistantMessage[] = [...history, { role: "user", content: text }];
    setHistory(next);
    setInput("");
    setBusy(true);
    setError(null);
    try {
      const turn = await client.assistantChat(
        next,
        selectedProvider ? { provider_id: selectedProvider.id, model: selectedModel } : null,
      );
      setHistory([...next, { role: "assistant", content: turn.reply || "(no answer)" }]);
      if (turn.drafts.length) {
        setDrafts((current) => [...current, ...turn.drafts.map((draft) => ({ draft, status: "pending" as const }))]);
      }
      if (turn.requests.length) reloadRequests();
    } catch (failure) {
      setError(message(failure, "The assistant could not answer"));
    } finally {
      setBusy(false);
    }
  };

  const confirmDraft = async (index: number) => {
    const target = drafts[index];
    setDrafts((current) => current.map((item, i) => (i === index ? { ...item, status: "submitting" } : item)));
    try {
      const created = await client.submitAssistantDraft(target.draft);
      setDrafts((current) =>
        current.map((item, i) => (i === index ? { ...item, status: "submitted", id: created.id } : item)),
      );
    } catch (failure) {
      setError(message(failure, "The workload was not submitted"));
      setDrafts((current) => current.map((item, i) => (i === index ? { ...item, status: "pending" } : item)));
    }
  };

  const dismissDraft = (index: number) =>
    setDrafts((current) => current.map((item, i) => (i === index ? { ...item, status: "dismissed" } : item)));

  const resolve = async (id: string, resolution: "approved" | "declined" | "withdrawn") => {
    setError(null);
    try {
      await client.resolveTeamRequest(id, resolution);
      reloadRequests();
    } catch (failure) {
      setError(message(failure, "The request was not updated"));
    }
  };

  const resetProviderForm = () => {
    setEditingProvider(null);
    setProviderForm(emptyProviderForm());
  };

  const editProvider = (provider: AssistantProvider) => {
    setEditingProvider(provider);
    setProviderForm({
      name: provider.name,
      baseUrl: provider.base_url ?? "",
      apiKey: "",
      models: provider.models.join(", "),
      defaultModel: defaultModel(provider),
      maxSteps: String(provider.max_steps ?? 8),
      enabled: provider.enabled !== false,
      clearApiKey: false,
    });
  };

  const changeProvider = async (operation: () => Promise<unknown>) => {
    if (!canManageProviders || providerBusy) return;
    setProviderBusy(true);
    setError(null);
    try {
      await operation();
      resetProviderForm();
      setProviderRevision((value) => value + 1);
    } catch (failure) {
      setError(message(failure, "The assistant provider was not updated"));
    } finally {
      setProviderBusy(false);
    }
  };

  const saveProvider = (event: FormEvent) => {
    event.preventDefault();
    const input: AssistantProviderInput = {
      name: providerForm.name.trim(),
      base_url: providerForm.baseUrl.trim(),
      models: formModels,
      default_model: formDefaultModel,
      max_steps: Number(providerForm.maxSteps),
      enabled: providerForm.enabled,
      ...(providerForm.apiKey ? { api_key: providerForm.apiKey } : {}),
    };
    if (editingProvider) {
      void changeProvider(async () => {
        if (!client.updateAssistantProvider) throw new Error("Provider editing is unavailable");
        await client.updateAssistantProvider(editingProvider.id, {
          ...input,
          ...(providerForm.clearApiKey ? { clear_api_key: true } : {}),
        });
      });
    } else {
      void changeProvider(async () => {
        if (!client.createAssistantProvider) throw new Error("Provider creation is unavailable");
        await client.createAssistantProvider(input);
      });
    }
  };

  const toggleProvider = (provider: AssistantProvider) =>
    changeProvider(async () => {
      if (!client.updateAssistantProvider) throw new Error("Provider editing is unavailable");
      await client.updateAssistantProvider(provider.id, { enabled: provider.enabled === false });
    });

  const deleteProvider = (provider: AssistantProvider) => {
    if (!window.confirm(`Delete assistant provider “${provider.name}”?`)) return;
    void changeProvider(async () => {
      if (!client.deleteAssistantProvider) throw new Error("Provider deletion is unavailable");
      await client.deleteAssistantProvider(provider.id);
    });
  };

  return (
    <div className="page-stack">
      {error && (
        <div className="inline-error" role="alert">
          <AlertTriangle size={16} aria-hidden="true" />
          {error}
        </div>
      )}

      <section className="panel data-panel" aria-label="Assistant chat">
        <div className="panel-header">
          <div>
            <h2>Fleet assistant</h2>
            <p>
              {status === null || providerList === null
                ? "Checking whether this plane runs an assistant…"
                : availableProviders.length > 0
                  ? `Answers from this plane's real data with your own permissions (model ${selectedModel}). It drafts jobs; nothing runs until you confirm.`
                  : "No assistant provider is configured yet."}
            </p>
            {providerList !== null && availableProviders.length === 0 && (
              <p>{canAdminister ? "Add one under Assistant providers below." : "Ask a fleet admin to add one."}</p>
            )}
          </div>
        </div>
        {availableProviders.length > 0 && (
          <div className="form-grid form-grid-two">
            <label className="field">
              <span>Provider</span>
              <select
                value={selectedProvider?.id ?? ""}
                disabled={busy}
                onChange={(event) => {
                  const provider = availableProviders.find((item) => item.id === event.target.value);
                  setProviderId(event.target.value);
                  setModel(provider ? defaultModel(provider) : "");
                }}
              >
                {availableProviders.map((provider) => (
                  <option key={provider.id} value={provider.id}>
                    {provider.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>Model</span>
              <select value={selectedModel} onChange={(event) => setModel(event.target.value)} disabled={busy}>
                {selectedProvider?.models.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
          </div>
        )}
        {status?.enabled && selectedProvider && (
          <>
            <div className="assistant-log" aria-label="Conversation" role="log">
              {history.length === 0 && (
                <p className="panel-note">Try: “How big a TPU do I need for Llama 3 8B SFT at 4k context?”</p>
              )}
              {history.map((item, index) => (
                <div key={index} className={`assistant-message assistant-message-${item.role}`}>
                  <strong>{item.role === "user" ? "You" : "Assistant"}</strong>
                  <p>{item.content}</p>
                </div>
              ))}
              {busy && (
                <p className="panel-note" role="status">
                  <LoaderCircle className="spin" size={14} /> Thinking…
                </p>
              )}
            </div>
            <form className="form-grid form-grid-two" onSubmit={(event) => void send(event)} aria-label="Ask">
              <label className="field">
                <span>Message</span>
                <input value={input} onChange={(event) => setInput(event.target.value)} disabled={busy} />
              </label>
              <div className="field">
                <span>&nbsp;</span>
                <button className="button button-primary" type="submit" disabled={busy || !input.trim()}>
                  <Send size={16} aria-hidden="true" /> Send
                </button>
              </div>
            </form>
          </>
        )}
      </section>

      {canAdminister && (
        <section className="panel data-panel" aria-label="Assistant providers">
          <div className="panel-header">
            <div>
              <h2>Assistant providers</h2>
              <p>Manage the models available to the fleet. API keys stay on the server and are never shown again.</p>
            </div>
          </div>
          {providers.length > 0 && (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Base URL</th>
                    <th>Models</th>
                    <th>Default model</th>
                    <th>Enabled</th>
                    <th>API key</th>
                    <th aria-label="Actions" />
                  </tr>
                </thead>
                <tbody>
                  {providers.map((provider) => (
                    <tr key={provider.id}>
                      <td>{provider.name}</td>
                      <td>{provider.base_url ?? "—"}</td>
                      <td>{provider.models.join(", ")}</td>
                      <td>{provider.default_model}</td>
                      <td>{provider.enabled ? "Enabled" : "Disabled"}</td>
                      <td>{provider.has_api_key ? "key set" : "no key"}</td>
                      <td>
                        <button
                          className="button button-secondary"
                          type="button"
                          disabled={providerBusy || !canManageProviders || !client.updateAssistantProvider}
                          onClick={() => editProvider(provider)}
                        >
                          Edit
                        </button>{" "}
                        <button
                          className="button button-secondary"
                          type="button"
                          disabled={providerBusy || !canManageProviders || !client.updateAssistantProvider}
                          onClick={() => void toggleProvider(provider)}
                        >
                          {provider.enabled === false ? "Enable" : "Disable"}
                        </button>{" "}
                        <button
                          className="button button-secondary"
                          type="button"
                          disabled={providerBusy || !canManageProviders || !client.deleteAssistantProvider}
                          onClick={() => deleteProvider(provider)}
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <h3>{editingProvider ? `Edit ${editingProvider.name}` : "Add provider"}</h3>
          <form
            className="form-grid form-grid-two"
            aria-label={editingProvider ? `Edit ${editingProvider.name}` : "Add provider"}
            onSubmit={saveProvider}
          >
            <label className="field">
              <span>Name</span>
              <input
                required
                value={providerForm.name}
                disabled={providerBusy || !canManageProviders}
                onChange={(event) => setProviderForm({ ...providerForm, name: event.target.value })}
              />
            </label>
            <label className="field">
              <span>Base URL</span>
              <input
                required
                type="url"
                placeholder="https://api.example.com/v1"
                value={providerForm.baseUrl}
                disabled={providerBusy || !canManageProviders}
                onChange={(event) => setProviderForm({ ...providerForm, baseUrl: event.target.value })}
              />
            </label>
            <label className="field">
              <span>API key</span>
              <input
                type="password"
                autoComplete="off"
                placeholder={editingProvider ? "Stored key is kept when left empty" : "Optional API key"}
                value={providerForm.apiKey}
                disabled={providerBusy || !canManageProviders || providerForm.clearApiKey}
                onChange={(event) => setProviderForm({ ...providerForm, apiKey: event.target.value })}
              />
            </label>
            <label className="field">
              <span>Models (comma separated)</span>
              <input
                required
                value={providerForm.models}
                disabled={providerBusy || !canManageProviders}
                onChange={(event) => setProviderForm({ ...providerForm, models: event.target.value })}
              />
            </label>
            <label className="field">
              <span>Default model</span>
              <select
                required
                value={formDefaultModel}
                disabled={providerBusy || !canManageProviders || formModels.length === 0}
                onChange={(event) => setProviderForm({ ...providerForm, defaultModel: event.target.value })}
              >
                {formModels.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span>Max steps</span>
              <input
                required
                type="number"
                min={1}
                max={20}
                step={1}
                value={providerForm.maxSteps}
                disabled={providerBusy || !canManageProviders}
                onChange={(event) => setProviderForm({ ...providerForm, maxSteps: event.target.value })}
              />
            </label>
            <label className="checkbox-field">
              <input
                type="checkbox"
                checked={providerForm.enabled}
                disabled={providerBusy || !canManageProviders}
                onChange={(event) => setProviderForm({ ...providerForm, enabled: event.target.checked })}
              />
              <span>Enabled</span>
            </label>
            {editingProvider && (
              <label className="checkbox-field">
                <input
                  type="checkbox"
                  checked={providerForm.clearApiKey}
                  disabled={providerBusy || !canManageProviders}
                  onChange={(event) =>
                    setProviderForm({ ...providerForm, clearApiKey: event.target.checked, apiKey: "" })
                  }
                />
                <span>Remove stored key</span>
              </label>
            )}
            <div className="field-wide">
              <button
                className="button button-primary"
                type="submit"
                disabled={
                  providerBusy ||
                  !canManageProviders ||
                  formModels.length === 0 ||
                  (editingProvider ? !client.updateAssistantProvider : !client.createAssistantProvider)
                }
              >
                {providerBusy ? "Saving…" : editingProvider ? "Save provider" : "Add provider"}
              </button>{" "}
              {editingProvider && (
                <button
                  className="button button-secondary"
                  type="button"
                  disabled={providerBusy}
                  onClick={resetProviderForm}
                >
                  Cancel edit
                </button>
              )}
            </div>
          </form>
        </section>
      )}

      {drafts.length > 0 && (
        <section className="panel data-panel" aria-label="Drafted workloads">
          <div className="panel-header">
            <div>
              <h2>Drafted workloads</h2>
              <p>Prepared by the assistant. Each runs only if you submit it.</p>
            </div>
          </div>
          {drafts.map((item, index) => (
            <div key={index} className="panel-note" data-testid="assistant-draft">
              <strong>{item.draft.spec.name}</strong> in {item.draft.org_id}/{item.draft.project_id}
              <pre className="monospace">{JSON.stringify(item.draft.spec, null, 2)}</pre>
              {item.status === "submitted" ? (
                <span role="status">Submitted {item.id}</span>
              ) : item.status === "dismissed" ? (
                <span>Dismissed</span>
              ) : (
                <>
                  <button
                    className="button button-primary"
                    type="button"
                    disabled={item.status === "submitting"}
                    onClick={() => void confirmDraft(index)}
                  >
                    <Check size={14} aria-hidden="true" /> Submit this workload
                  </button>{" "}
                  <button className="button button-secondary" type="button" onClick={() => dismissDraft(index)}>
                    <X size={14} aria-hidden="true" /> Dismiss
                  </button>
                </>
              )}
            </div>
          ))}
        </section>
      )}

      {teamMode && (
        <section className="panel data-panel" aria-label="Requests">
          <div className="panel-header">
            <div>
              <h2>{canAdminister ? "Requests to the fleet admins" : "My requests"}</h2>
              <p>Capacity and approval requests. Ask the assistant to file one, or use `eyvan requests file`.</p>
            </div>
          </div>
          {requests.length === 0 ? (
            <p className="panel-note">No requests.</p>
          ) : (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Kind</th>
                    <th>From</th>
                    <th>Summary</th>
                    <th>State</th>
                    <th aria-label="Actions" />
                  </tr>
                </thead>
                <tbody>
                  {requests.map((item) => (
                    <tr key={item.id}>
                      <td>{item.kind}</td>
                      <td className="monospace">{item.subject}</td>
                      <td>
                        {item.summary}
                        {typeof item.details?.held_workload_id === "string" && (
                          <div className="panel-note" data-testid={`held-${item.id}`}>
                            Holds workload <span className="monospace">{item.details.held_workload_id}</span>
                            {typeof item.details.tpu_units === "number" ? ` (${item.details.tpu_units} TPU units)` : ""}
                            {item.state === "open"
                              ? "; approving lets it run, declining cancels it"
                              : item.state === "approved"
                                ? "; released to the scheduler"
                                : "; cancelled"}
                          </div>
                        )}
                      </td>
                      <td>{item.state}</td>
                      <td>
                        {item.state === "open" && canAdminister && (
                          <>
                            <button
                              className="button button-primary"
                              type="button"
                              onClick={() => void resolve(item.id, "approved")}
                            >
                              Approve
                            </button>{" "}
                            <button
                              className="button button-secondary"
                              type="button"
                              onClick={() => void resolve(item.id, "declined")}
                            >
                              Decline
                            </button>
                          </>
                        )}
                        {item.state === "open" && !canAdminister && (
                          <button
                            className="button button-secondary"
                            type="button"
                            onClick={() => void resolve(item.id, "withdrawn")}
                          >
                            Withdraw
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}
    </div>
  );
}
