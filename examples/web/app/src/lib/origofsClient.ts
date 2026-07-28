// A typed client over the origofs document server. Reads hit the origofs router (/fs)
// and the app layer (/api); writes carry the bearer token so the server can
// attribute them (the body never names an actor — attribution can't be forged).
//
// URLs are relative; the Vite dev server proxies /fs and /api to :8000.

import type {
  AppConfig,
  BlameRange,
  ChangeEvent,
  Commit,
  DiffEntry,
  DirectoryActor,
  DocLoad,
  Me,
  Presence,
  Suggestion,
  SuggestionDetail,
  SuggestionStatus,
} from "./types";

/** Encode an origofs path ("/dir/a.md") into a `{path:path}` URL suffix. */
function pathSuffix(path: string): string {
  return path
    .replace(/^\/+/, "")
    .split("/")
    .map(encodeURIComponent)
    .join("/");
}

export class OrigoFSError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "OrigoFSError";
  }
}

export class OrigoFSClient {
  private token: string | null = null;

  constructor(readonly base: string = "") {}

  /** Set (or clear) the bearer token used to attribute writes. */
  setToken(token: string | null): void {
    this.token = token;
  }

  private authHeaders(extra: Record<string, string> = {}): Record<string, string> {
    return this.token ? { ...extra, Authorization: `Bearer ${this.token}` } : extra;
  }

  private async json<T>(res: Response): Promise<T> {
    if (!res.ok) throw await this.error(res);
    return (await res.json()) as T;
  }

  private async error(res: Response): Promise<OrigoFSError> {
    let detail = res.statusText;
    try {
      const body = await res.json();
      if (body && typeof body.detail === "string") detail = body.detail;
    } catch {
      /* non-JSON error body */
    }
    return new OrigoFSError(detail, res.status);
  }

  // --- app layer (/api) -----------------------------------------------------

  config(): Promise<AppConfig> {
    return fetch(`${this.base}/api/config`).then((r) => this.json<AppConfig>(r));
  }

  me(): Promise<Me> {
    return fetch(`${this.base}/api/me`, { headers: this.authHeaders() }).then((r) =>
      this.json<Me>(r),
    );
  }

  actors(): Promise<DirectoryActor[]> {
    return fetch(`${this.base}/api/actors`).then((r) => this.json<DirectoryActor[]>(r));
  }

  /** Load a document's text and per-line blame in one round trip. */
  loadDoc(path: string): Promise<DocLoad> {
    return fetch(`${this.base}/api/doc/${pathSuffix(path)}`).then((r) => this.json<DocLoad>(r));
  }

  /** The URL for an SSE `EventSource` over the live change feed. */
  feedUrl(since = 0): string {
    return `${this.base}/api/feed?since=${since}`;
  }

  // --- files + attribution (/fs) --------------------------------------------

  /** Attributed write of the whole document (UTF-8). Creates parent dirs. */
  async writeDoc(path: string, text: string): Promise<{ path: string; written: number }> {
    const res = await fetch(`${this.base}/fs/files/${pathSuffix(path)}`, {
      method: "PUT",
      headers: this.authHeaders({ "Content-Type": "application/octet-stream" }),
      body: new Blob([text], { type: "application/octet-stream" }),
    });
    return this.json(res);
  }

  blame(path: string): Promise<BlameRange[]> {
    return fetch(`${this.base}/fs/blame/${pathSuffix(path)}`).then((r) =>
      this.json<BlameRange[]>(r),
    );
  }

  // --- versioning + lineage (/fs) -------------------------------------------

  async commit(message: string, author = "origofs-web"): Promise<string> {
    const res = await fetch(`${this.base}/fs/commit`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ message, author }),
    });
    const { hash } = await this.json<{ hash: string }>(res);
    return hash;
  }

  log(): Promise<Commit[]> {
    return fetch(`${this.base}/fs/log`).then((r) => this.json<Commit[]>(r));
  }

  status(): Promise<DiffEntry[]> {
    return fetch(`${this.base}/fs/status`).then((r) => this.json<DiffEntry[]>(r));
  }

  diffFile(from: string, to: string, path: string): Promise<string> {
    const q = new URLSearchParams({ from, to, path });
    return fetch(`${this.base}/fs/diff/file?${q}`).then((r) =>
      r.ok ? r.text() : this.error(r).then((e) => Promise.reject(e)),
    );
  }

  // --- agent-suggestion review queue (/fs) ----------------------------------

  listSuggestions(status?: SuggestionStatus, path?: string): Promise<Suggestion[]> {
    const q = new URLSearchParams();
    if (status) q.set("status", status);
    if (path) q.set("path", path);
    const qs = q.toString();
    return fetch(`${this.base}/fs/suggestions${qs ? `?${qs}` : ""}`).then((r) =>
      this.json<Suggestion[]>(r),
    );
  }

  suggestionDiff(id: number): Promise<string> {
    return fetch(`${this.base}/fs/suggestions/${id}/diff`).then((r) =>
      r.ok ? r.text() : this.error(r).then((e) => Promise.reject(e)),
    );
  }

  /**
   * Propose an edit (agent-authored, not applied until kept). Goes through the
   * origofs router; the inline review reads the proposed content back from origofs via
   * `suggestionDetail`, so nothing is stashed app-side.
   */
  async suggest(path: string, text: string, summary?: string): Promise<number> {
    const q = new URLSearchParams({ path });
    if (summary) q.set("summary", summary);
    const res = await fetch(`${this.base}/fs/suggestions?${q}`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/octet-stream" }),
      body: new Blob([text], { type: "application/octet-stream" }),
    });
    const { id } = await this.json<{ id: number }>(res);
    return id;
  }

  /** A pending suggestion rendered as an inline line diff (base vs proposed). */
  suggestionDetail(id: number): Promise<SuggestionDetail> {
    return fetch(`${this.base}/api/suggestion/${id}`).then((r) => this.json<SuggestionDetail>(r));
  }

  /**
   * Keep the chosen hunks. Keep-all → origofs's native accept (credits the agent);
   * a partial keep → the server writes the kept hunks as the agent. Either way
   * the agent stays credited, never the reviewer.
   */
  async applySuggestion(
    id: number,
    keep: number[],
  ): Promise<{ applied: boolean; mode: "accept" | "partial"; kept: number; total: number }> {
    const res = await fetch(`${this.base}/api/suggestion/${id}/apply`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ keep }),
    });
    return this.json(res);
  }

  async acceptSuggestion(id: number): Promise<void> {
    const res = await fetch(`${this.base}/fs/suggestions/${id}/accept`, {
      method: "POST",
      headers: this.authHeaders(),
    });
    if (!res.ok) throw await this.error(res);
  }

  async rejectSuggestion(id: number): Promise<void> {
    const res = await fetch(`${this.base}/fs/suggestions/${id}/reject`, {
      method: "POST",
      headers: this.authHeaders(),
    });
    if (!res.ok) throw await this.error(res);
  }

  // --- presence (/fs) -------------------------------------------------------

  presence(windowSecs = 60): Promise<Presence[]> {
    return fetch(`${this.base}/fs/presence?window=${windowSecs}`).then((r) =>
      this.json<Presence[]>(r),
    );
  }

  async touch(path?: string): Promise<void> {
    const res = await fetch(`${this.base}/fs/presence/touch`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ path: path ?? null }),
    });
    if (!res.ok) throw await this.error(res);
  }

  events(since = 0): Promise<ChangeEvent[]> {
    return fetch(`${this.base}/fs/events?since=${since}`).then((r) =>
      this.json<ChangeEvent[]>(r),
    );
  }
}
