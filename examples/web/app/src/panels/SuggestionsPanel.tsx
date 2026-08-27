// The agent-suggestion review queue — the propose-and-review path.
//
// An agent (or anyone) proposes an edit; the working tree is untouched until a
// reviewer accepts it. On accept, origofs lands the edit **attributed to the
// original author** while recording the approver, and refuses a stale base
// (409) — so review is safe against concurrent edits.

import { useCallback, useEffect, useState } from "react";
import { useSession } from "../session";
import { OrigoFSError } from "../lib/origofsClient";
import { relativeTime } from "../lib/time";
import { ActorChip } from "../components/ActorBadge";
import { DiffText } from "./DiffText";
import type { Suggestion } from "../lib/types";

export function SuggestionsPanel({ onApplied }: { onApplied: () => void }) {
  const { client, token, resolveActor, actorName } = useSession();
  const [items, setItems] = useState<Suggestion[]>([]);
  const [selected, setSelected] = useState<Suggestion | null>(null);
  const [diff, setDiff] = useState<string>("");
  const [notice, setNotice] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const [nonce, setNonce] = useState(0);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    let live = true;
    client
      .listSuggestions()
      .then((s) => live && setItems(s))
      .catch((e) => live && setNotice({ kind: "err", text: e instanceof Error ? e.message : String(e) }));
    return () => {
      live = false;
    };
  }, [client, nonce]);

  const open = useCallback(
    async (s: Suggestion) => {
      setSelected(s);
      setDiff("");
      try {
        setDiff(await client.suggestionDiff(s.id));
      } catch (e) {
        setDiff(`(could not load diff: ${e instanceof Error ? e.message : String(e)})`);
      }
    },
    [client],
  );

  const act = useCallback(
    async (s: Suggestion, action: "accept" | "reject") => {
      setNotice(null);
      try {
        if (action === "accept") await client.acceptSuggestion(s.id);
        else await client.rejectSuggestion(s.id);
        setNotice({ kind: "ok", text: `suggestion #${s.id} ${action}ed` });
        refresh();
        if (action === "accept") onApplied();
      } catch (e) {
        const text =
          e instanceof OrigoFSError && e.status === 409
            ? `#${s.id} has a stale base — the file changed since it was proposed. Re-propose it.`
            : e instanceof Error
              ? e.message
              : String(e);
        setNotice({ kind: "err", text });
      }
    },
    [client, onApplied, refresh],
  );

  const pending = items.filter((s) => s.status === "pending");
  const resolved = items.filter((s) => s.status !== "pending");

  const row = (s: Suggestion) => {
    const actor = resolveActor(s.actor_id);
    return (
      <li key={s.id} className={selected?.id === s.id ? "selected" : ""} onClick={() => open(s)}>
        <span className={`sug-status ${s.status}`}>{s.status}</span>
        <span className="sug-path">{s.path}</span>
        <span className="sug-by">
          <ActorChip name={actor?.display_name ?? actorName(s.actor_id)} kind={actor?.kind ?? "agent"} />
        </span>
        <span className="sug-summary">{s.summary ?? (s.proposed_hash ? "" : "(deletion)")}</span>
        <span className="commit-meta">{relativeTime(s.created_ts)}</span>
      </li>
    );
  };

  return (
    <div className="panel suggestions-panel">
      <div className="panel-head">
        <h3>Suggestions</h3>
        <div className="row-actions">
          <span className="muted">{pending.length} pending</span>
          <button onClick={refresh}>Refresh</button>
        </div>
      </div>
      {notice && <div className={`notice ${notice.kind}`}>{notice.text}</div>}
      {items.length === 0 ? (
        <div className="empty">
          No suggestions. Sign in as an agent (e.g. <code>claude</code>) and use “Suggest…” in the
          editor to propose an edit for review.
        </div>
      ) : (
        <>
          <ul className="sug-list">
            {pending.map(row)}
            {resolved.map(row)}
          </ul>
          {selected && (
            <div className="diff-pane">
              <div className="diff-head">
                <span>
                  #{selected.id} · {selected.path}
                  {selected.resolved_by != null && (
                    <span className="muted"> · resolved by {actorName(selected.resolved_by)}</span>
                  )}
                </span>
                {selected.status === "pending" && (
                  <span className="row-actions">
                    <button className="primary" disabled={!token} onClick={() => act(selected, "accept")}>
                      Accept
                    </button>
                    <button disabled={!token} onClick={() => act(selected, "reject")}>
                      Reject
                    </button>
                    {!token && <span className="hint">sign in to review</span>}
                  </span>
                )}
              </div>
              <DiffText text={diff} />
            </div>
          )}
        </>
      )}
    </div>
  );
}
