// A minimal Yjs y-sync WebSocket client for origofs live co-editing.
//
// origofs exposes a document as a *flat text* CRDT (a Yjs `Y.Text` named
// "content") and speaks the standard Yjs sync protocol over the co-editing
// WebSocket at `/fs/coedit/{path}`. This is a trimmed y-websocket provider — just
// enough to sync one doc and relay awareness — so we control the URL (nested
// paths, `?token=`) and depend only on `yjs` + `y-protocols`.
//
// The server is the authority on attribution: every update we send is credited to
// the *authenticated* actor (the token), not to anything the bytes claim. When the
// last client leaves, the server checkpoints the CRDT into the byte-range blame
// index — so live edits show up in blame exactly like ordinary writes.

import * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";

const MSG_SYNC = 0;
const MSG_AWARENESS = 1;

export interface CoeditConnection {
  doc: Y.Doc;
  /** The shared plain-text CRDT ("content"). Bind an editor to this. */
  text: Y.Text;
  awareness: awarenessProtocol.Awareness;
  /** True once the initial server→client sync (SyncStep2) has been applied. */
  isSynced: () => boolean;
  /** Subscribe to sync-state changes; returns an unsubscribe. */
  onSync: (cb: (synced: boolean) => void) => () => void;
  destroy: () => void;
}

/** Turn an http(s) origin into the ws(s) origin for the WebSocket. */
function wsOrigin(base: string): string {
  const url = new URL(base || window.location.origin, window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.origin;
}

export function connectCoedit(base: string, path: string, token: string): CoeditConnection {
  const doc = new Y.Doc();
  const text = doc.getText("content");
  const awareness = new awarenessProtocol.Awareness(doc);
  // A stable per-connection origin so we can tell server-applied updates (which we
  // must not echo back) from local edits (which we must send).
  const wsTag = {};

  const abs = path.startsWith("/") ? path : `/${path}`;
  const url = `${wsOrigin(base)}/fs/coedit${abs}?token=${encodeURIComponent(token)}`;
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";

  let synced = false;
  const syncCbs = new Set<(s: boolean) => void>();
  const setSynced = (s: boolean) => {
    if (s !== synced) {
      synced = s;
      syncCbs.forEach((cb) => cb(s));
    }
  };

  const send = (data: Uint8Array) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  };

  ws.onopen = () => {
    // Greet the server with SyncStep1 (our state vector) so it sends what we lack;
    // the server also greets us the same way.
    const enc = encoding.createEncoder();
    encoding.writeVarUint(enc, MSG_SYNC);
    syncProtocol.writeSyncStep1(enc, doc);
    send(encoding.toUint8Array(enc));
    // Announce our awareness (who we are), if set.
    if (awareness.getLocalState() !== null) {
      const aenc = encoding.createEncoder();
      encoding.writeVarUint(aenc, MSG_AWARENESS);
      encoding.writeVarUint8Array(
        aenc,
        awarenessProtocol.encodeAwarenessUpdate(awareness, [doc.clientID]),
      );
      send(encoding.toUint8Array(aenc));
    }
  };

  ws.onmessage = (ev: MessageEvent) => {
    const dec = decoding.createDecoder(new Uint8Array(ev.data as ArrayBuffer));
    const msgType = decoding.readVarUint(dec);
    if (msgType === MSG_SYNC) {
      const enc = encoding.createEncoder();
      encoding.writeVarUint(enc, MSG_SYNC);
      // Applies incoming sync content to `doc` tagged with `wsTag`, and writes any
      // reply (e.g. our SyncStep2 answering the server's SyncStep1) into `enc`.
      const readType = syncProtocol.readSyncMessage(dec, enc, doc, wsTag);
      if (encoding.length(enc) > 1) send(encoding.toUint8Array(enc));
      if (readType === syncProtocol.messageYjsSyncStep2) setSynced(true);
    } else if (msgType === MSG_AWARENESS) {
      awarenessProtocol.applyAwarenessUpdate(awareness, decoding.readVarUint8Array(dec), wsTag);
    }
  };

  ws.onclose = () => setSynced(false);

  // Local edits (origin != wsTag) go to the server; server-applied updates don't.
  const onDocUpdate = (update: Uint8Array, origin: unknown) => {
    if (origin === wsTag) return;
    const enc = encoding.createEncoder();
    encoding.writeVarUint(enc, MSG_SYNC);
    syncProtocol.writeUpdate(enc, update);
    send(encoding.toUint8Array(enc));
  };
  doc.on("update", onDocUpdate);

  const onAwareness = (
    changes: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ) => {
    if (origin === wsTag) return; // relayed change; don't echo
    const changed = changes.added.concat(changes.updated, changes.removed);
    const enc = encoding.createEncoder();
    encoding.writeVarUint(enc, MSG_AWARENESS);
    encoding.writeVarUint8Array(enc, awarenessProtocol.encodeAwarenessUpdate(awareness, changed));
    send(encoding.toUint8Array(enc));
  };
  awareness.on("update", onAwareness);

  return {
    doc,
    text,
    awareness,
    isSynced: () => synced,
    onSync: (cb) => {
      syncCbs.add(cb);
      return () => syncCbs.delete(cb);
    },
    destroy: () => {
      doc.off("update", onDocUpdate);
      awareness.off("update", onAwareness);
      awarenessProtocol.removeAwarenessStates(awareness, [doc.clientID], "destroy");
      try {
        ws.close();
      } catch {
        /* already closing */
      }
      doc.destroy();
    },
  };
}

/**
 * Replace a `Y.Text`'s content with `next`, editing only the changed middle (so
 * concurrent edits elsewhere survive). Runs under `origin` so the caller can tell
 * its own edits apart from remote ones.
 */
export function replaceYText(ytext: Y.Text, next: string, origin: unknown): void {
  const old = ytext.toString();
  if (old === next) return;
  let start = 0;
  const min = Math.min(old.length, next.length);
  while (start < min && old[start] === next[start]) start++;
  let endOld = old.length;
  let endNew = next.length;
  while (endOld > start && endNew > start && old[endOld - 1] === next[endNew - 1]) {
    endOld--;
    endNew--;
  }
  const yDoc = ytext.doc;
  const apply = () => {
    if (endOld > start) ytext.delete(start, endOld - start);
    if (endNew > start) ytext.insert(start, next.slice(start, endNew));
  };
  if (yDoc) yDoc.transact(apply, origin);
  else apply();
}
