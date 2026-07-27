// Inline review of a pending agent suggestion — rendered natively **in a Plate
// editor**. The proposal is a Plate value (unchanged text deserialized from
// Markdown; each change a `suggestion_change` element with word-level
// insert/delete marks). You Keep or Discard each change in place, then Apply.
//
// Keep-all applies through origofs's native accept (atomic, credits the agent). A
// partial keep is reconstructed server-side and written *as the agent*, so the
// agent stays credited for its lines — the reviewer only ever chooses which
// changes, never authors them.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Plate, PlateContent, usePlateEditor } from "platejs/react";
import { BasicBlocksPlugin, BasicMarksPlugin } from "@platejs/basic-nodes/react";
import { useSession } from "../session";
import { OrigoFSError } from "../lib/origofsClient";
import { ActorChip } from "../components/ActorBadge";
import { DiffText } from "../panels/DiffText";
import { buildSuggestionValue } from "./buildSuggestionValue";
import { SuggestChangePlugin, SuggestDeletePlugin, SuggestInsertPlugin } from "./suggestionPlugins";
import type { SuggestionDetail } from "../lib/types";

const reviewPlugins = [
  BasicBlocksPlugin,
  BasicMarksPlugin,
  SuggestInsertPlugin,
  SuggestDeletePlugin,
  SuggestChangePlugin,
];

const EMPTY = [{ type: "p", children: [{ text: "" }] }];

export function ReviewOverlay({
  id,
  onDone,
  onCancel,
}: {
  id: number;
  onDone: (msg: string) => void;
  onCancel: () => void;
}) {
  const { client, token } = useSession();
  const [detail, setDetail] = useState<SuggestionDetail | null>(null);
  const [decisions, setDecisions] = useState<Record<number, boolean>>({});
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const editor = usePlateEditor({ plugins: reviewPlugins, value: EMPTY });

  // Toggling a change updates React state; an effect mirrors it to the plugin so
  // the in-editor ✓/✗ re-render.
  const toggle = useCallback((hunk: number, keep: boolean) => {
    setDecisions((d) => ({ ...d, [hunk]: keep }));
  }, []);

  useEffect(() => {
    let live = true;
    setError(null);
    client
      .suggestionDetail(id)
      .then((d) => {
        if (!live) return;
        setDetail(d);
        const init: Record<number, boolean> = {};
        for (let i = 0; i < (d.hunks ?? 0); i++) init[i] = true;
        setDecisions(init);
      })
      .catch((e) => live && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, [client, id]);

  // Build the Plate value + wire the toggle callback once the proposal loads.
  useEffect(() => {
    if (!detail?.segments) return;
    editor.tf.setValue(buildSuggestionValue(detail.segments));
    editor.setOption(SuggestChangePlugin, "onToggle", toggle);
  }, [editor, detail, toggle]);

  // Push the current decisions into the plugin (drives the in-editor ✓/✗).
  useEffect(() => {
    editor.setOption(SuggestChangePlugin, "decisions", decisions);
  }, [editor, decisions]);

  const total = detail?.hunks ?? 0;
  const keptIdx = useMemo(
    () => Object.entries(decisions).filter(([, v]) => v).map(([k]) => Number(k)),
    [decisions],
  );
  const setAll = useCallback(
    (v: boolean) => {
      const next: Record<number, boolean> = {};
      for (let i = 0; i < total; i++) next[i] = v;
      setDecisions(next);
    },
    [total],
  );

  const apply = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await client.applySuggestion(id, keptIdx);
      onDone(
        res.mode === "accept"
          ? `kept all ${res.total} changes — accepted (credited to the agent)`
          : res.kept === 0
            ? "discarded — the proposal was rejected"
            : `kept ${res.kept}/${res.total} changes — written as the agent`,
      );
    } catch (e) {
      setError(
        e instanceof OrigoFSError && e.status === 409
          ? "The document changed since this was proposed (stale base). Ask the agent to re-propose."
          : e instanceof Error
            ? e.message
            : String(e),
      );
    } finally {
      setBusy(false);
    }
  }, [client, id, keptIdx, onDone]);

  if (error && !detail) return <div className="notice err">{error}</div>;
  if (!detail) return <div className="empty">Loading proposal…</div>;

  // Not stashed (proposed straight to /fs): show the unified diff read-only.
  if (detail.segments === null) {
    return (
      <div className="review">
        <div className="review-bar">
          <span>
            Proposal #{id} by <ActorChip name={detail.actor_name} kind={detail.actor_kind} /> —
            read-only (review it in the Suggestions tab)
          </span>
          <button onClick={onCancel}>Close</button>
        </div>
        <div className="review-body">
          <DiffText text={detail.unified ?? ""} />
        </div>
      </div>
    );
  }

  const kept = keptIdx.length;
  return (
    <div className="review">
      <div className="review-bar">
        <span className="review-title">
          Reviewing <ActorChip name={detail.actor_name} kind={detail.actor_kind} />
          {detail.summary ? <span className="muted"> — {detail.summary}</span> : null}
        </span>
        <span className="review-count">
          {kept}/{total} {total === 1 ? "change" : "changes"} kept
        </span>
        <span className="row-actions">
          <button onClick={() => setAll(true)}>Keep all</button>
          <button onClick={() => setAll(false)}>Discard all</button>
          <button className="primary" disabled={busy || !token} onClick={apply} title={token ? "" : "sign in to apply"}>
            Apply
          </button>
          <button disabled={busy} onClick={onCancel}>
            Cancel
          </button>
          {!token && <span className="hint">sign in to apply</span>}
        </span>
      </div>
      {error && <div className="notice err">{error}</div>}
      <div className="suggestion-doc">
        <Plate editor={editor}>
          <PlateContent className="plate-content" readOnly />
        </Plate>
      </div>
    </div>
  );
}
