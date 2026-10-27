// Copyright (C) 2026 Erfan Zare Chavoshi
// This file is part of Eyvan.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import { AlertTriangle, Check, LoaderCircle, Send, X } from "lucide-react";
import { useCallback, useEffect, useState, type FormEvent } from "react";

import type { AssistantDraft, AssistantMessage, AssistantStatus, AssistantTurn, TeamRequest } from "../types";

export interface AssistantClient {
  getAssistantStatus: (signal?: AbortSignal) => Promise<AssistantStatus>;
  assistantChat: (messages: AssistantMessage[]) => Promise<AssistantTurn>;
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

const message = (error: unknown, fallback: string) => (error instanceof Error ? error.message : fallback);

export function AssistantPage({ client, canAdminister, teamMode }: AssistantPageProps) {
  const [status, setStatus] = useState<AssistantStatus | null>(null);
  const [history, setHistory] = useState<AssistantMessage[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drafts, setDrafts] = useState<DraftState[]>([]);
  const [requests, setRequests] = useState<TeamRequest[]>([]);
  const [revision, setRevision] = useState(0);

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
    return () => controller.abort();
  }, [client]);

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
      const turn = await client.assistantChat(next);
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
              {status === null
                ? "Checking whether this plane runs an assistant…"
                : status.enabled
                  ? `Answers from this plane's real data with your own permissions (model ${status.model}). It drafts jobs; nothing runs until you confirm.`
                  : "This plane has no assistant configured. Its operator sets EYVAN_ASSISTANT_URL and EYVAN_ASSISTANT_MODEL on the server."}
            </p>
          </div>
        </div>
        {status?.enabled && (
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
                      <td>{item.summary}</td>
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
