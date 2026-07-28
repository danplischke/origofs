// TypeScript mirrors of the exact JSON shapes the origofs Python bindings emit
// (via origofs.fastapi.build_router at /fs) and the app server (/api/*). Keys and
// nullability match the bindings 1:1 — see crates/origofs-py/src/lib.rs.

export type ActorKind = "human" | "agent" | "system";

/** The actor embedded in every blame range (crates/origofs-py/src/lib.rs `actor_dict`). */
export interface Actor {
  id: number;
  kind: ActorKind;
  display_name: string;
  auth_subject: string | null;
  agent_model: string | null;
  agent_vendor: string | null;
  controller_actor_id: number | null;
  created_at: number;
}

/**
 * One attributed span. `byte_start`/`byte_end` are the exact `[start, end)` byte
 * range — the ground truth — so sub-line, character-level authorship from live
 * co-editing renders precisely (two authors on one line are two spans that share
 * a line number). `line_start`/`line_end` are the **1-based, inclusive** lines the
 * span touches, for line-oriented views; a single line has
 * `line_start === line_end`. `blame()` returns an empty array for an unattributed
 * file (a plain `write`, or empty content).
 */
export interface BlameRange {
  byte_start: number;
  byte_end: number;
  line_start: number;
  line_end: number;
  session: number | null;
  actor: Actor;
}

/** Combined document load from GET /api/doc/{path}. */
export interface DocLoad {
  path: string;
  exists: boolean;
  text: string;
  blame: BlameRange[];
}

export interface DirEntry {
  name: string;
  ino: number;
  kind: "file" | "dir" | "symlink";
}

export interface Inode {
  ino: number;
  kind: "file" | "dir" | "symlink";
  mode: number;
  nlink: number;
  size: number;
  content: string | null;
  mtime: number;
  ctime: number;
}

export interface Commit {
  hash: string;
  author: string;
  message: string;
  timestamp: number;
  parents: string[];
}

export type ChangeStatus = "added" | "modified" | "deleted";

export interface DiffEntry {
  path: string;
  status: ChangeStatus;
}

export type SuggestionStatus = "pending" | "accepted" | "rejected" | "superseded";

export interface Suggestion {
  id: number;
  actor_id: number;
  session_id: number | null;
  branch: string | null;
  path: string;
  base_hash: string | null;
  proposed_hash: string | null;
  summary: string | null;
  status: SuggestionStatus;
  created_ts: number;
  resolved_ts: number | null;
  resolved_by: number | null;
}

/** One segment of an inline line-diff (GET /api/suggestion/{id}). Changed
 * segments carry a `hunk` index the reviewer keeps or discards; equal segments
 * have `hunk === null` and identical `del`/`add`. */
export interface DiffSegment {
  tag: "equal" | "replace" | "delete" | "insert";
  del: string[];
  add: string[];
  hunk: number | null;
}

/** A pending suggestion rendered as an inline diff for review. */
export interface SuggestionDetail extends Suggestion {
  actor_name: string;
  actor_kind: ActorKind;
  base_text: string;
  segments: DiffSegment[] | null; // null → not stashed; use `unified` read-only
  hunks?: number;
  unified?: string;
}

/** A change-feed event (GET /fs/events and the SSE stream GET /api/feed). */
export interface ChangeEvent {
  seq: number;
  actor_id: number | null;
  session_id: number | null;
  kind: string; // write|mkdir|remove|rename|symlink|commit|lock|unlock|suggest|accept|reject
  path: string;
  detail: string | null;
  ts: number;
  branch: string | null;
}

export interface Presence {
  session_id: number;
  actor_id: number;
  display_name: string;
  kind: ActorKind;
  path: string | null;
  last_seen: number;
}

// --- app server (/api/*) ----------------------------------------------------

/** An entry in the app's actor directory (GET /api/actors). */
export interface DirectoryActor {
  id: number;
  display_name: string;
  kind: ActorKind;
  model: string | null;
}

/** The authenticated principal (GET /api/me). */
export interface Me {
  actor_id: number;
  session_id: number | null;
  display_name: string | null;
  kind: ActorKind | null;
  model: string | null;
}

/** A demo bearer token offered by the dev token-picker (GET /api/config). */
export interface DemoToken {
  token: string;
  name: string;
  kind: ActorKind;
  external_id: string;
}

export interface AppConfig {
  demo: boolean;
  tokens: DemoToken[];
}
