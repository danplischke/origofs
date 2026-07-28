// Lineage: the commit history (origofs's versioning DAG) plus, for the current
// document, the unified diff a selected commit introduced against its parent.

import { useCallback, useEffect, useState } from "react";
import { useSession } from "../session";
import { relativeTime, shortHash } from "../lib/time";
import { DiffText } from "./DiffText";
import type { Commit } from "../lib/types";

export function HistoryPanel({ path, revision }: { path: string; revision: number }) {
  const { client } = useSession();
  const [commits, setCommits] = useState<Commit[]>([]);
  const [selected, setSelected] = useState<Commit | null>(null);
  const [diff, setDiff] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    client
      .log()
      .then((cs) => live && setCommits(cs))
      .catch((e) => live && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, [client, revision]);

  const openDiff = useCallback(
    async (c: Commit) => {
      setSelected(c);
      setDiff("");
      setError(null);
      const parent = c.parents[0];
      if (!parent) {
        setDiff(`(initial commit — ${path} introduced here)`);
        return;
      }
      try {
        const text = await client.diffFile(parent, c.hash, path);
        setDiff(text || `(no change to ${path} in this commit)`);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    },
    [client, path],
  );

  return (
    <div className="panel history-panel">
      <div className="panel-head">
        <h3>History</h3>
        <span className="muted">{commits.length} commits · diff is for {path}</span>
      </div>
      {error && <div className="notice err">{error}</div>}
      {commits.length === 0 ? (
        <div className="empty">No commits yet. Use “Commit snapshot” in the header to record one.</div>
      ) : (
        <ol className="commit-list">
          {commits.map((c) => (
            <li
              key={c.hash}
              className={selected?.hash === c.hash ? "selected" : ""}
              onClick={() => openDiff(c)}
            >
              <code className="hash">{shortHash(c.hash)}</code>
              <span className="commit-msg">{c.message}</span>
              <span className="commit-meta">
                {c.author} · {relativeTime(c.timestamp)}
              </span>
            </li>
          ))}
        </ol>
      )}
      {selected && (
        <div className="diff-pane">
          <div className="diff-head">
            <code>{shortHash(selected.hash)}</code> — {selected.message}
          </div>
          <DiffText text={diff} />
        </div>
      )}
    </div>
  );
}
