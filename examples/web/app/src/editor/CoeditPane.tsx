// Live co-editing pane (roadmap M8). A plain-text / Markdown editor bound to the
// document's shared `Y.Text` over origofs's y-sync WebSocket: open the same doc in
// two browsers and watch edits converge, each character attributed server-side to
// whoever is signed in. When the last editor leaves, the server checkpoints the
// CRDT into the byte-range blame index — so live edits land in blame like any other
// write (switch to the Blame view after everyone disconnects to see it).
//
// PlateJS binds to a Yjs *XML fragment*; origofs models a doc as a flat `Y.Text`,
// so the live surface here is a plain Markdown textarea rather than the rich
// PlateJS editor. The two are complementary: edit live here, review authorship in
// the PlateJS blame view.

import { useCallback, useEffect, useRef, useState } from "react";

import { connectCoedit, replaceYText, type CoeditConnection } from "../lib/coedit";
import { useSession } from "../session";

// Origin tag marking edits this pane makes locally (so the provider forwards them).
const LOCAL_EDIT = { local: true };

export function CoeditPane({ path }: { path: string }) {
  const { token, me } = useSession();
  const taRef = useRef<HTMLTextAreaElement>(null);
  const connRef = useRef<CoeditConnection | null>(null);
  const [synced, setSynced] = useState(false);
  const [peers, setPeers] = useState<string[]>([]);

  useEffect(() => {
    if (!token) return;
    const conn = connectCoedit("", path, token);
    connRef.current = conn;
    conn.awareness.setLocalStateField("user", { name: me?.display_name ?? "someone" });

    const ta = taRef.current;
    if (ta) ta.value = conn.text.toString();

    // Remote edits → refresh the textarea, keeping the caret roughly in place.
    const onText = () => {
      const el = taRef.current;
      if (!el) return;
      const next = conn.text.toString();
      if (el.value === next) return; // our own echo, or nothing changed
      const s = Math.min(el.selectionStart, next.length);
      const e = Math.min(el.selectionEnd, next.length);
      el.value = next;
      el.setSelectionRange(s, e);
    };
    conn.text.observe(onText);

    const offSync = conn.onSync(setSynced);
    setSynced(conn.isSynced());

    const onAware = () => {
      const names: string[] = [];
      conn.awareness.getStates().forEach((st, clientId) => {
        if (clientId === conn.doc.clientID) return;
        const name = (st as { user?: { name?: string } })?.user?.name;
        if (name) names.push(name);
      });
      setPeers(names);
    };
    conn.awareness.on("change", onAware);
    onAware();

    return () => {
      conn.text.unobserve(onText);
      conn.awareness.off("change", onAware);
      offSync();
      conn.destroy();
      connRef.current = null;
    };
  }, [path, token, me?.display_name]);

  // Local typing → apply the change to the shared text (which forwards it, and the
  // server attributes it to us).
  const onInput = useCallback(() => {
    const el = taRef.current;
    const conn = connRef.current;
    if (!el || !conn) return;
    replaceYText(conn.text, el.value, LOCAL_EDIT);
  }, []);

  return (
    <div className="coedit-pane">
      <div className="coedit-status">
        <span className={synced ? "dot dot-on" : "dot dot-off"} />
        {synced ? "live" : "connecting…"}
        {peers.length > 0 && (
          <span className="coedit-peers"> · also here: {peers.join(", ")}</span>
        )}
        <span className="coedit-hint">
          {" "}
          · edits sync live and are attributed to you; blame updates when everyone
          leaves the room
        </span>
      </div>
      <textarea
        ref={taRef}
        className="coedit-textarea"
        spellCheck={false}
        onInput={onInput}
        placeholder="Type here — open this doc in another window to co-edit."
      />
    </div>
  );
}
