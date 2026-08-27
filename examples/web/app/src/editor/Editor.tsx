// The authoring surface: a PlateJS Markdown editor bound to a document in origofs.
//
// - Loads by deserializing the stored Markdown into Plate nodes.
// - Saves by serializing back to Markdown and doing an **attributed** write
//   (write_as) — so origofs records blame + an audit op crediting the signed-in
//   principal. A client can't forge that: the token is resolved server-side.
// - "Suggest" queues the same content as a proposal instead.
// - Inline attribution is native: the AttributionPlugin *decorates* authored
//   text with its author's color through Plate's own leaf pipeline (no DOM
//   overlays). The caret's line author is shown natively via useEditorSelection.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Plate, PlateContent, useEditorSelection, usePlateEditor } from "platejs/react";
import { deserializeMd, serializeMd } from "@platejs/markdown";
import { editorPlugins } from "./plugins";
import { AttributionPlugin } from "./attributionPlugin";
import { blockAuthorship, blockLineSpans, type BlockAuthorship } from "../lib/blame";
import { actorColor, kindGlyph } from "../lib/colors";
import { useSession } from "../session";
import { OrigoFSError } from "../lib/origofsClient";
import type { BlameRange } from "../lib/types";

// Native current-line attribution: reads the caret's block from the editor
// selection (Plate context) and names its author — no DOM measurement.
function ActiveAuthor({ authorship }: { authorship: BlockAuthorship[] }) {
  const selection = useEditorSelection();
  const idx = selection?.anchor?.path?.[0];
  const info = idx != null ? authorship[idx] : undefined;
  if (!info?.primary) return null;
  const c = actorColor(info.primary.id, info.primary.kind);
  const lines =
    info.lineStart === info.lineEnd ? `line ${info.lineStart}` : `lines ${info.lineStart}–${info.lineEnd}`;
  return (
    <div className="active-author">
      <span style={{ color: c.fg }}>
        {kindGlyph(info.primary.kind)} {info.primary.display_name}
        {info.mixed ? " (+ others)" : ""}
      </span>
      <span className="muted"> · {lines}</span>
    </div>
  );
}

export function EditorTab({
  path,
  initialText,
  blame,
  onSaved,
}: {
  path: string;
  initialText: string;
  blame: BlameRange[];
  onSaved: () => void;
}) {
  const { client, token } = useSession();

  const editor = usePlateEditor({
    plugins: [...editorPlugins, AttributionPlugin],
    value: (ed) => deserializeMd(ed, initialText || ""),
  });

  const [nodes, setNodes] = useState<unknown[]>(() => editor.children as unknown[]);
  const [status, setStatus] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [showAttribution, setShowAttribution] = useState(true);

  const authorship = useMemo<BlockAuthorship[]>(
    () => blockAuthorship(blockLineSpans(editor, nodes), blame),
    [editor, nodes, blame],
  );

  // Push per-block authorship into the plugin and refresh the decorations.
  useEffect(() => {
    editor.setOption(AttributionPlugin, "spans", authorship);
    editor.setOption(AttributionPlugin, "enabled", showAttribution);
    editor.api.redecorate();
  }, [editor, authorship, showAttribution]);

  const onChange = useCallback(() => {
    setNodes([...(editor.children as unknown[])]);
  }, [editor]);

  const save = useCallback(async () => {
    setBusy(true);
    setStatus(null);
    try {
      const md = serializeMd(editor);
      const res = await client.writeDoc(path, md);
      if (res.proposed !== undefined) {
        // This actor is propose-only: the write was routed into the review queue.
        setStatus({
          kind: "ok",
          text: `you're propose-only — queued as suggestion #${res.proposed} for review, not applied`,
        });
      } else {
        setStatus({ kind: "ok", text: `saved ${res.written} bytes — attributed to you` });
      }
      onSaved();
    } catch (e) {
      setStatus({ kind: "err", text: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  }, [client, editor, path, onSaved]);

  const suggest = useCallback(async () => {
    const summary = window.prompt("Summary for this suggestion:", "propose an edit");
    if (summary === null) return;
    setBusy(true);
    setStatus(null);
    try {
      const md = serializeMd(editor);
      const id = await client.suggest(path, md, summary || undefined);
      setStatus({ kind: "ok", text: `suggestion #${id} queued — not applied until a reviewer accepts it` });
    } catch (e) {
      const text = e instanceof OrigoFSError ? e.message : e instanceof Error ? e.message : String(e);
      setStatus({ kind: "err", text });
    } finally {
      setBusy(false);
    }
  }, [client, editor, path]);

  return (
    <div className="editor-tab">
      <div className="toolbar">
        <button className="primary" disabled={!token || busy} onClick={save} title="Attributed write to the working tree (write_as)">
          Save
        </button>
        <button disabled={!token || busy} onClick={suggest} title="Queue a suggestion — the working tree is untouched until a reviewer accepts">
          Suggest…
        </button>
        <label className="toggle">
          <input type="checkbox" checked={showAttribution} onChange={(e) => setShowAttribution(e.target.checked)} />
          inline attribution
        </label>
        <span className="spacer" />
        {!token && <span className="hint">sign in to edit</span>}
        {status && <span className={`status ${status.kind}`}>{status.text}</span>}
      </div>
      <div className="editor-surface">
        <Plate editor={editor} onChange={onChange}>
          {showAttribution && <ActiveAuthor authorship={authorship} />}
          <PlateContent className="plate-content" placeholder="Write Markdown…" spellCheck />
        </Plate>
      </div>
    </div>
  );
}
