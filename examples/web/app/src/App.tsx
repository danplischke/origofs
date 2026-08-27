import { useCallback, useState } from "react";
import { useSession } from "./session";
import { useDocument } from "./doc/useDocument";
import { EditPane } from "./editor/EditPane";
import { CoeditPane } from "./editor/CoeditPane";
import { BlameView } from "./editor/BlameView";
import { HistoryPanel } from "./panels/HistoryPanel";
import { SuggestionsPanel } from "./panels/SuggestionsPanel";
import { ActivityFeed } from "./panels/ActivityFeed";
import { kindGlyph } from "./lib/colors";

type Tab = "edit" | "live" | "blame" | "history" | "suggestions";

function SignIn() {
  const { config, me, token, signIn, signOut } = useSession();
  if (token && me) {
    return (
      <div className="signin">
        <span className="me">
          {kindGlyph(me.kind ?? "human")} {me.display_name ?? "signed in"}
          {me.kind === "agent" && me.model ? <span className="muted"> · {me.model}</span> : null}
        </span>
        <button onClick={signOut}>Sign out</button>
      </div>
    );
  }
  return (
    <div className="signin">
      <select defaultValue="" onChange={(e) => e.target.value && signIn(e.target.value)}>
        <option value="" disabled>
          Sign in as…
        </option>
        {config?.tokens.map((t) => (
          <option key={t.token} value={t.token}>
            {kindGlyph(t.kind)} {t.name}
            {t.kind === "agent" ? " (agent)" : ""}
          </option>
        ))}
      </select>
    </div>
  );
}

export function App() {
  const { client, token } = useSession();
  const [path, setPath] = useState("/README.md");
  const [pathDraft, setPathDraft] = useState(path);
  const [tab, setTab] = useState<Tab>("edit");
  const [revision, setRevision] = useState(0);
  const { doc, loading, error, reload } = useDocument(path);

  const bump = useCallback(() => setRevision((r) => r + 1), []);
  const afterWrite = useCallback(() => {
    reload();
    bump();
  }, [reload, bump]);

  const openPath = useCallback(() => {
    let p = pathDraft.trim();
    if (!p) return;
    if (!p.startsWith("/")) p = "/" + p;
    setPath(p);
    setPathDraft(p);
  }, [pathDraft]);

  const commit = useCallback(async () => {
    const message = window.prompt("Commit message:", "snapshot");
    if (!message) return;
    try {
      const hash = await client.commit(message);
      bump();
      window.alert(`committed ${hash.slice(0, 10)}`);
    } catch (e) {
      window.alert(e instanceof Error ? e.message : String(e));
    }
  }, [client, bump]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="brand">
          <span className="logo">origofs</span>
          <span className="tagline">attribution &amp; lineage</span>
        </div>
        <div className="doc-open">
          <input
            value={pathDraft}
            onChange={(e) => setPathDraft(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && openPath()}
            spellCheck={false}
            aria-label="document path"
          />
          <button onClick={openPath}>Open</button>
          <button disabled={!token} onClick={commit} title="Snapshot the working tree into a commit">
            Commit snapshot
          </button>
        </div>
        <SignIn />
      </header>

      <div className="body">
        <main className="main">
          <nav className="tabs">
            {(["edit", "live", "blame", "history", "suggestions"] as Tab[]).map((t) => (
              <button key={t} className={tab === t ? "active" : ""} onClick={() => setTab(t)}>
                {t}
              </button>
            ))}
            <span className="doc-name">{path}</span>
          </nav>

          <section className="tab-body">
            {error && <div className="notice err">{error}</div>}
            {/* Stale-while-revalidate: only block on the *first* load. Re-loading
                after a save keeps the editor mounted (no flash, no lost cursor)
                while blame refreshes underneath. */}
            {!doc ? (
              <div className="empty">{loading ? `Loading ${path}…` : `Could not load ${path}.`}</div>
            ) : tab === "edit" ? (
              <EditPane
                key={path}
                path={path}
                initialText={doc.text}
                blame={doc.blame}
                onChanged={afterWrite}
              />
            ) : tab === "live" ? (
              <CoeditPane key={path} path={path} />
            ) : tab === "blame" ? (
              <BlameView text={doc.text} blame={doc.blame} />
            ) : tab === "history" ? (
              <HistoryPanel path={path} revision={revision} />
            ) : (
              <SuggestionsPanel onApplied={afterWrite} />
            )}
          </section>
        </main>

        <aside className="sidebar">
          <ActivityFeed currentPath={path} />
        </aside>
      </div>
    </div>
  );
}
