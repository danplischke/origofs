//! Dual-dialect schema migrations (`docs/DESIGN.md` §4b).
//!
//! One ordered list of migrations, each carrying the SQL for both dialects. Every
//! backend applies the steps it hasn't yet recorded in `schema_meta`, so SQLite
//! and Postgres stay in lockstep from a single source of truth.

/// One migration step, with the SQL for each supported engine.
pub struct Migration {
    pub version: i64,
    pub sqlite: &'static str,
    pub postgres: &'static str,
}

/// The ordered migration list. Append new steps; never edit an applied one.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sqlite: SQLITE_V1,
        postgres: POSTGRES_V1,
    },
    // V2 — versioning: refs (branches/tags/HEAD) and workspace config. The SQL is
    // identical across dialects (plain TEXT columns).
    Migration {
        version: 2,
        sqlite: V2,
        postgres: V2,
    },
    // V3 — merge: recorded conflicts and git-LFS-style exclusive file locks.
    Migration {
        version: 3,
        sqlite: V3,
        postgres: V3,
    },
    // V4 — attribution: actors, sessions, tool-call audit, the edit-op log, and
    // per-file line authorship. Dialects differ only in the identity columns.
    Migration {
        version: 4,
        sqlite: V4_SQLITE,
        postgres: V4_POSTGRES,
    },
    // V5 — live collaboration: an append-only change feed and per-session
    // presence. Dialects differ only in the identity columns.
    Migration {
        version: 5,
        sqlite: V5_SQLITE,
        postgres: V5_POSTGRES,
    },
    // V6 — branch-scoped change feed: tag each event with the branch it happened
    // on so a UI showing `main` can attribute/filter edits per branch. Plain
    // `ADD COLUMN` in both dialects.
    Migration {
        version: 6,
        sqlite: V6_SQLITE,
        postgres: V6_POSTGRES,
    },
    // V7 — agent-suggestion review queue: proposed edits held for human review.
    // Content lives in the CAS (base/proposed are content hashes); this table is
    // just the review record. Dialects differ only in the identity/int columns.
    Migration {
        version: 7,
        sqlite: V7_SQLITE,
        postgres: V7_POSTGRES,
    },
    // V8 — blame keyed by content hash (blob version) rather than by inode, so
    // per-line authorship survives checkout/merge (the inode is rebuilt, but the
    // content hash it points at carries the blame) and can never desync from the
    // content it describes. Identical SQL in both dialects (a plain TEXT key).
    Migration {
        version: 8,
        sqlite: V8,
        postgres: V8,
    },
    // V9 — one actor per external identity: a partial UNIQUE index on
    // `auth_subject` (ignoring NULLs), so an app can map its own user id to an
    // actor idempotently and `find_or_create_actor` is race-safe. Identical SQL in
    // both dialects (both support partial unique indexes).
    Migration {
        version: 9,
        sqlite: V9,
        postgres: V9,
    },
    // V10 — per-actor write policy: `0` = direct (may write straight to the tree),
    // `1` = propose (writes must go through the suggestion queue for review). An
    // actor property, not a kind — a bounded, actor-agnostic trust gate. Defaults
    // to direct, so existing actors keep writing directly. Plain `ADD COLUMN`.
    Migration {
        version: 10,
        sqlite: V10_SQLITE,
        postgres: V10_POSTGRES,
    },
    // V11 — multi-workspace in one store (`docs/MULTI_TENANCY.md`). A `workspace`
    // registry table (each workspace has its own root inode), a `workspace_id` tag
    // on `inode` so a per-workspace `truncate_tree`/checkout clears only its own
    // tree, and `workspace_id` folded into the primary keys of the namespace-keyed
    // tables (`ref`, `config`, `conflict`, `file_lock`) whose bare name/path keys
    // would otherwise collide across workspaces (every workspace has its own `HEAD`
    // and `refs/heads/main`). `dentry`/`symlink` need no column: inodes share one
    // global id sequence, so a workspace's subtree is reachable only from its own
    // root. Backfill maps every existing row to a `default` workspace (id 1, root
    // `INO_ROOT`), so the migration is non-breaking. (`blob_blame` is scoped later,
    // in V13.) SQLite rebuilds the four PK-changed tables (no `ALTER … PRIMARY KEY`);
    // Postgres alters them in place.
    Migration {
        version: 11,
        sqlite: V11_SQLITE,
        postgres: V11_POSTGRES,
    },
    // V12 — workspace-scope the per-location activity + attribution tables so they
    // isolate like the working tree does (`docs/MULTI_TENANCY.md` §8). Without this
    // the suggestion queue, change feed, op-log, and presence were store-wide: a
    // suggestion made in one workspace was visible — and acceptable into the wrong
    // tree — from another. A plain `workspace_id` tag on each (their surrogate-id/seq
    // PKs don't collide, so no table rebuild), backfilled to the `default` workspace.
    // `actor`/`session`/`tool_calls` stay store-wide (identity is tenant-wide).
    Migration {
        version: 12,
        sqlite: V12_SQLITE,
        postgres: V12_POSTGRES,
    },
    // V13 — workspace-scope `blob_blame`: re-key it on `(workspace_id, content_hash)`
    // instead of `content_hash` alone, so blame is per workspace and identical content
    // in two workspaces carries each workspace's own authorship (not a shared map).
    // Within a workspace this keeps V8's property — blame keyed by content survives
    // checkout/merge — because the workspace_id is stable there. A PK change, so the
    // same rebuild (SQLite) / alter (Postgres) shape as V11; backfilled to `default`.
    Migration {
        version: 13,
        sqlite: V13_SQLITE,
        postgres: V13_POSTGRES,
    },
];

/// The highest migration version this build knows about — the schema version a
/// freshly-opened (or migrated) workspace is brought up to. Compare against a
/// store's [`MetadataStore::schema_version`](crate::MetadataStore::schema_version)
/// to tell whether it needs migrating.
pub fn latest_schema_version() -> i64 {
    match MIGRATIONS.last() {
        Some(m) => m.version,
        None => 0,
    }
}

// V8 — per-blob-version blame (see the migration entry above).
const V8: &str = "
CREATE TABLE IF NOT EXISTS blob_blame(
    content_hash TEXT PRIMARY KEY,
    runs         TEXT NOT NULL
);
";

// V9 — at most one actor per external identity (see the migration entry above).
const V9: &str = "
CREATE UNIQUE INDEX IF NOT EXISTS idx_actor_auth_subject
    ON actor(auth_subject) WHERE auth_subject IS NOT NULL;
";

// V10 — the per-actor write policy column (0 = direct, 1 = propose). NOT NULL with
// a default so it applies to existing rows; the runner tolerates a re-applied
// SQLite ADD COLUMN, Postgres expresses idempotency directly.
const V10_SQLITE: &str = "ALTER TABLE actor ADD COLUMN write_policy INTEGER NOT NULL DEFAULT 0;";
const V10_POSTGRES: &str =
    "ALTER TABLE actor ADD COLUMN IF NOT EXISTS write_policy BIGINT NOT NULL DEFAULT 0;";

// V11 — multi-workspace in one store (see the migration entry above). SQLite has no
// `ALTER TABLE … ADD PRIMARY KEY`, so the four namespace-keyed tables are rebuilt
// (create-copy-drop-rename); the `ADD COLUMN`s ride the runner's duplicate-column
// tolerance on re-apply.
const V11_SQLITE: &str = "
CREATE TABLE IF NOT EXISTS workspace(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    root_ino   INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
INSERT OR IGNORE INTO workspace(id, name, root_ino, created_at) VALUES (1, 'default', 1, 0);

ALTER TABLE inode ADD COLUMN workspace_id INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_inode_workspace ON inode(workspace_id);

CREATE TABLE ref_v11(
    workspace_id INTEGER NOT NULL DEFAULT 1,
    name  TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(workspace_id, name)
);
INSERT INTO ref_v11(workspace_id, name, value) SELECT 1, name, value FROM ref;
DROP TABLE ref;
ALTER TABLE ref_v11 RENAME TO ref;

CREATE TABLE config_v11(
    workspace_id INTEGER NOT NULL DEFAULT 1,
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(workspace_id, key)
);
INSERT INTO config_v11(workspace_id, key, value) SELECT 1, key, value FROM config;
DROP TABLE config;
ALTER TABLE config_v11 RENAME TO config;

CREATE TABLE conflict_v11(
    workspace_id INTEGER NOT NULL DEFAULT 1,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY(workspace_id, path)
);
INSERT INTO conflict_v11(workspace_id, path, kind) SELECT 1, path, kind FROM conflict;
DROP TABLE conflict;
ALTER TABLE conflict_v11 RENAME TO conflict;

CREATE TABLE file_lock_v11(
    workspace_id INTEGER NOT NULL DEFAULT 1,
    path        TEXT NOT NULL,
    owner       TEXT NOT NULL,
    acquired_at BIGINT NOT NULL,
    PRIMARY KEY(workspace_id, path)
);
INSERT INTO file_lock_v11(workspace_id, path, owner, acquired_at)
    SELECT 1, path, owner, acquired_at FROM file_lock;
DROP TABLE file_lock;
ALTER TABLE file_lock_v11 RENAME TO file_lock;
";

// V11 — Postgres alters the four tables in place (`DROP CONSTRAINT … ADD PRIMARY
// KEY`). Explicitly inserting the `default` workspace at id 1 does not advance the
// identity sequence, so `setval` bumps it past 1.
const V11_POSTGRES: &str = "
CREATE TABLE IF NOT EXISTS workspace(
    id         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    root_ino   BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);
INSERT INTO workspace(id, name, root_ino, created_at) VALUES (1, 'default', 1, 0)
    ON CONFLICT DO NOTHING;
SELECT setval(pg_get_serial_sequence('workspace', 'id'),
              GREATEST((SELECT MAX(id) FROM workspace), 1));

ALTER TABLE inode ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_inode_workspace ON inode(workspace_id);

ALTER TABLE ref       ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE ref       DROP CONSTRAINT IF EXISTS ref_pkey;
ALTER TABLE ref       ADD PRIMARY KEY(workspace_id, name);

ALTER TABLE config    ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE config    DROP CONSTRAINT IF EXISTS config_pkey;
ALTER TABLE config    ADD PRIMARY KEY(workspace_id, key);

ALTER TABLE conflict  ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE conflict  DROP CONSTRAINT IF EXISTS conflict_pkey;
ALTER TABLE conflict  ADD PRIMARY KEY(workspace_id, path);

ALTER TABLE file_lock ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE file_lock DROP CONSTRAINT IF EXISTS file_lock_pkey;
ALTER TABLE file_lock ADD PRIMARY KEY(workspace_id, path);
";

// V12 — workspace-scope the activity/attribution tables (see the migration entry
// above). Plain `ADD COLUMN` (surrogate-id/seq PKs don't collide), so no rebuild;
// the `ADD COLUMN`s ride the runner's duplicate-column tolerance on re-apply.
const V12_SQLITE: &str = "
ALTER TABLE suggestion ADD COLUMN workspace_id INTEGER NOT NULL DEFAULT 1;
ALTER TABLE edit_op    ADD COLUMN workspace_id INTEGER NOT NULL DEFAULT 1;
ALTER TABLE fs_event   ADD COLUMN workspace_id INTEGER NOT NULL DEFAULT 1;
ALTER TABLE presence   ADD COLUMN workspace_id INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_suggestion_workspace ON suggestion(workspace_id);
CREATE INDEX IF NOT EXISTS idx_edit_op_workspace ON edit_op(workspace_id);
CREATE INDEX IF NOT EXISTS idx_fs_event_workspace ON fs_event(workspace_id);
CREATE INDEX IF NOT EXISTS idx_presence_workspace ON presence(workspace_id);
";

const V12_POSTGRES: &str = "
ALTER TABLE suggestion ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE edit_op    ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE fs_event   ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE presence   ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_suggestion_workspace ON suggestion(workspace_id);
CREATE INDEX IF NOT EXISTS idx_edit_op_workspace ON edit_op(workspace_id);
CREATE INDEX IF NOT EXISTS idx_fs_event_workspace ON fs_event(workspace_id);
CREATE INDEX IF NOT EXISTS idx_presence_workspace ON presence(workspace_id);
";

// V13 — per-workspace blame (see the migration entry above). SQLite rebuilds the
// table to change the PK (`content_hash` -> `(workspace_id, content_hash)`);
// Postgres alters it in place. Existing blame backfills to the `default` workspace.
const V13_SQLITE: &str = "
CREATE TABLE blob_blame_v13(
    workspace_id INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT NOT NULL,
    runs         TEXT NOT NULL,
    PRIMARY KEY(workspace_id, content_hash)
);
INSERT INTO blob_blame_v13(workspace_id, content_hash, runs)
    SELECT 1, content_hash, runs FROM blob_blame;
DROP TABLE blob_blame;
ALTER TABLE blob_blame_v13 RENAME TO blob_blame;
";

const V13_POSTGRES: &str = "
ALTER TABLE blob_blame ADD COLUMN IF NOT EXISTS workspace_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE blob_blame DROP CONSTRAINT IF EXISTS blob_blame_pkey;
ALTER TABLE blob_blame ADD PRIMARY KEY(workspace_id, content_hash);
";

// SQLite has no `ADD COLUMN IF NOT EXISTS`; the migration runner tolerates a
// re-applied ADD COLUMN (duplicate-column) so a re-run is idempotent. Postgres
// expresses idempotency directly.
const V6_SQLITE: &str = "ALTER TABLE fs_event ADD COLUMN branch TEXT;";
const V6_POSTGRES: &str = "ALTER TABLE fs_event ADD COLUMN IF NOT EXISTS branch TEXT;";

const V7_SQLITE: &str = "
CREATE TABLE IF NOT EXISTS suggestion(
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id      INTEGER NOT NULL,
    session_id    INTEGER,
    branch        TEXT,
    path          TEXT NOT NULL,
    base_hash     TEXT,
    proposed_hash TEXT,
    summary       TEXT,
    status        TEXT NOT NULL,
    created_ts    INTEGER NOT NULL,
    resolved_ts   INTEGER,
    resolved_by   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_suggestion_status ON suggestion(status);
CREATE INDEX IF NOT EXISTS idx_suggestion_path ON suggestion(path);
";

const V7_POSTGRES: &str = "
CREATE TABLE IF NOT EXISTS suggestion(
    id            BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    actor_id      BIGINT NOT NULL,
    session_id    BIGINT,
    branch        TEXT,
    path          TEXT NOT NULL,
    base_hash     TEXT,
    proposed_hash TEXT,
    summary       TEXT,
    status        TEXT NOT NULL,
    created_ts    BIGINT NOT NULL,
    resolved_ts   BIGINT,
    resolved_by   BIGINT
);
CREATE INDEX IF NOT EXISTS idx_suggestion_status ON suggestion(status);
CREATE INDEX IF NOT EXISTS idx_suggestion_path ON suggestion(path);
";

const V5_SQLITE: &str = "
CREATE TABLE IF NOT EXISTS fs_event(
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id   INTEGER,
    session_id INTEGER,
    kind       TEXT NOT NULL,
    path       TEXT NOT NULL,
    detail     TEXT,
    ts         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fs_event_seq ON fs_event(seq);
CREATE TABLE IF NOT EXISTS presence(
    session_id INTEGER PRIMARY KEY,
    actor_id   INTEGER NOT NULL,
    path       TEXT,
    last_seen  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_presence_last_seen ON presence(last_seen);
";

const V5_POSTGRES: &str = "
CREATE TABLE IF NOT EXISTS fs_event(
    seq        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    actor_id   BIGINT,
    session_id BIGINT,
    kind       TEXT NOT NULL,
    path       TEXT NOT NULL,
    detail     TEXT,
    ts         BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fs_event_seq ON fs_event(seq);
CREATE TABLE IF NOT EXISTS presence(
    session_id BIGINT PRIMARY KEY,
    actor_id   BIGINT NOT NULL,
    path       TEXT,
    last_seen  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_presence_last_seen ON presence(last_seen);
";

const V2: &str = "
CREATE TABLE IF NOT EXISTS ref(
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS config(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

const V3: &str = "
CREATE TABLE IF NOT EXISTS conflict(
    path TEXT PRIMARY KEY,
    kind TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_lock(
    path        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    acquired_at BIGINT NOT NULL
);
";

const V4_SQLITE: &str = "
CREATE TABLE IF NOT EXISTS actor(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    kind                TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    auth_subject        TEXT,
    agent_model         TEXT,
    agent_vendor        TEXT,
    controller_actor_id INTEGER,
    created_at          INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS session(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id   INTEGER NOT NULL,
    client     TEXT,
    started_at INTEGER NOT NULL,
    ended_at   INTEGER
);
CREATE TABLE IF NOT EXISTS tool_calls(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id   INTEGER,
    actor_id     INTEGER,
    name         TEXT NOT NULL,
    parameters   TEXT,
    result       TEXT,
    error        TEXT,
    started_at   INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    duration_ms  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS edit_op(
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id   INTEGER,
    actor_id     INTEGER NOT NULL,
    tool_call_id INTEGER,
    ino          INTEGER NOT NULL,
    path         TEXT NOT NULL,
    op           TEXT NOT NULL,
    byte_start   INTEGER NOT NULL,
    byte_len     INTEGER NOT NULL,
    pre_hash     TEXT,
    post_hash    TEXT,
    ts           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_edit_op_actor ON edit_op(actor_id);
CREATE INDEX IF NOT EXISTS idx_edit_op_session ON edit_op(session_id);
CREATE TABLE IF NOT EXISTS line_blame(
    ino  INTEGER PRIMARY KEY,
    runs TEXT NOT NULL
);
";

const V4_POSTGRES: &str = "
CREATE TABLE IF NOT EXISTS actor(
    id                  BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    kind                TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    auth_subject        TEXT,
    agent_model         TEXT,
    agent_vendor        TEXT,
    controller_actor_id BIGINT,
    created_at          BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS session(
    id         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    actor_id   BIGINT NOT NULL,
    client     TEXT,
    started_at BIGINT NOT NULL,
    ended_at   BIGINT
);
CREATE TABLE IF NOT EXISTS tool_calls(
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    session_id   BIGINT,
    actor_id     BIGINT,
    name         TEXT NOT NULL,
    parameters   TEXT,
    result       TEXT,
    error        TEXT,
    started_at   BIGINT NOT NULL,
    completed_at BIGINT NOT NULL,
    duration_ms  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS edit_op(
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    session_id   BIGINT,
    actor_id     BIGINT NOT NULL,
    tool_call_id BIGINT,
    ino          BIGINT NOT NULL,
    path         TEXT NOT NULL,
    op           TEXT NOT NULL,
    byte_start   BIGINT NOT NULL,
    byte_len     BIGINT NOT NULL,
    pre_hash     TEXT,
    post_hash    TEXT,
    ts           BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_edit_op_actor ON edit_op(actor_id);
CREATE INDEX IF NOT EXISTS idx_edit_op_session ON edit_op(session_id);
CREATE TABLE IF NOT EXISTS line_blame(
    ino  BIGINT PRIMARY KEY,
    runs TEXT NOT NULL
);
";

const SQLITE_V1: &str = "
CREATE TABLE IF NOT EXISTS inode(
    ino          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,
    mode         INTEGER NOT NULL,
    nlink        INTEGER NOT NULL DEFAULT 1,
    size         INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT,
    mtime        INTEGER NOT NULL,
    ctime        INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dentry(
    parent_ino INTEGER NOT NULL,
    name       TEXT    NOT NULL,
    ino        INTEGER NOT NULL,
    PRIMARY KEY(parent_ino, name)
);
CREATE INDEX IF NOT EXISTS idx_dentry_ino ON dentry(ino);
CREATE TABLE IF NOT EXISTS symlink(
    ino    INTEGER PRIMARY KEY,
    target TEXT NOT NULL
);
";

const POSTGRES_V1: &str = "
CREATE TABLE IF NOT EXISTS inode(
    ino          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    kind         TEXT   NOT NULL,
    mode         BIGINT NOT NULL,
    nlink        BIGINT NOT NULL DEFAULT 1,
    size         BIGINT NOT NULL DEFAULT 0,
    content_hash TEXT,
    mtime        BIGINT NOT NULL,
    ctime        BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS dentry(
    parent_ino BIGINT NOT NULL,
    name       TEXT   NOT NULL,
    ino        BIGINT NOT NULL,
    PRIMARY KEY(parent_ino, name)
);
CREATE INDEX IF NOT EXISTS idx_dentry_ino ON dentry(ino);
CREATE TABLE IF NOT EXISTS symlink(
    ino    BIGINT PRIMARY KEY,
    target TEXT NOT NULL
);
";

#[cfg(test)]
mod tests {
    use super::*;

    // A4 (issue #70): the migration list is the single source of truth for the
    // schema version, and `latest_schema_version()` trusts it to be sorted — it
    // reads `MIGRATIONS.last()`. Pin the forward-only, append-only invariants so an
    // authoring slip (a duplicate version, a gap, an out-of-order append, or an
    // empty step) fails loudly here instead of silently reporting the wrong schema
    // version or bricking an upgrade. The end-to-end "every populated store
    // upgrades to latest" path is covered by the V10→latest data-preservation tests
    // (`sqlite::tests::upgrade_preserves_data_and_backfills_default_workspace` and
    // `tests/postgres.rs`); this guards the list those runners iterate.
    #[test]
    fn migration_list_is_contiguous_sorted_and_nonempty() {
        assert!(!MIGRATIONS.is_empty(), "there must be at least one migration");

        for (i, m) in MIGRATIONS.iter().enumerate() {
            // Versions are exactly 1, 2, 3, … with no gaps, duplicates, or reordering.
            assert_eq!(
                m.version,
                (i + 1) as i64,
                "migration at index {i} has version {} — versions must be contiguous from 1",
                m.version
            );
            // Every step carries real SQL for both dialects (no accidental empty step).
            assert!(
                !m.sqlite.trim().is_empty(),
                "migration v{} has empty SQLite SQL",
                m.version
            );
            assert!(
                !m.postgres.trim().is_empty(),
                "migration v{} has empty Postgres SQL",
                m.version
            );
        }

        // `latest_schema_version()` reads `.last()`, so the list MUST be sorted for
        // it to be correct: assert it equals the true maximum, and — given the
        // contiguous-from-1 invariant above — the element count.
        let max = MIGRATIONS.iter().map(|m| m.version).max().unwrap();
        assert_eq!(
            latest_schema_version(),
            max,
            "latest_schema_version() must equal the highest migration version"
        );
        assert_eq!(
            latest_schema_version(),
            MIGRATIONS.len() as i64,
            "with contiguous 1..=N versions, latest must equal the migration count"
        );
    }
}
