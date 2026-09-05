//! A SQLite-backed [`MetadataStore`] — the M0 default and the SQLite half of the
//! pluggable-backend story (`docs/DESIGN.md` §4b).
//!
//! rusqlite is synchronous, so DB work runs under a mutex inside each `async`
//! method. **No `.await` occurs while the connection guard is held** — that is a
//! load-bearing invariant, not an incidental property: the guard is a *blocking*
//! `parking_lot` lock, so a suspension point underneath it lets another task
//! block a runtime worker on a lock only the suspended task can release (on a
//! `current_thread` runtime, a hard deadlock — see
//! `crate::engine::Fs::plan_materialize` and `tests/materialize.rs`). It also
//! keeps the futures `Send`.
//!
//! Every statement is therefore a synchronous blocking section, wrapped in
//! [`blocking_section`] so the multi-thread scheduler is told about it rather
//! than silently losing a worker to a WAL fsync.

use crate::attribution::{
    Actor, ActorInit, ActorKind, EditOp, EditOpInit, ToolCallInit, WritePolicy,
};
use crate::collab::{Event, EventInit, LiveDoc, Presence};
use crate::error::{OrigoFSError, Result};
use crate::metadata::{
    AclStore, AttributionStore, CollabStore, ConfigStore, LockStore, MetaTxn, MetadataStore,
    NamespaceStore, PortableStore, RefStore, StoreLifecycle, SuggestionStore, TrashStore,
    WorkspaceRegistry,
};
use crate::migrations::MIGRATIONS;
use crate::posixlock::{LockRequest, PosixLock};
use crate::suggest::{Suggestion, SuggestionInit, SuggestionKind, SuggestionStatus};
use crate::types::{DirEntry, FileKind, Hash, INO_ROOT, Ino, Inode, InodeInit};
use crate::util::{blocking_section, now_secs};
use async_trait::async_trait;
use parking_lot::{ArcMutexGuard, Mutex, MutexGuard, RawMutex};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::Path;
use std::sync::Arc;

const DIR_MODE: i64 = 0o040755;

/// How many inode numbers one batched `get_inodes` query binds. SQLite's
/// `SQLITE_MAX_VARIABLE_NUMBER` is 999 on builds before 3.32, so the IN-list is
/// chunked well under it rather than trusting the caller's slice length.
const INODE_BATCH: usize = 500;

/// The workspace every store is bound to until re-scoped with `with_workspace`;
/// its root is [`INO_ROOT`]. Backfilled by migration V11 (`docs/MULTI_TENANCY.md`).
const DEFAULT_WORKSPACE: i64 = 1;

/// How many read-only connections a file-backed store opens alongside its writer.
///
/// Small on purpose. Each is an open file descriptor and a page cache, and the
/// win is almost entirely in going from one to several — a read no longer waits
/// behind a write, or behind another read. Past the core count there is nothing
/// left to overlap, so it is capped rather than scaled.
///
/// Override with `ORIGOFS_SQLITE_READERS`; `0` restores the single-connection
/// behaviour, where every read serializes on the writer.
fn reader_count() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        if let Ok(v) = std::env::var("ORIGOFS_SQLITE_READERS")
            && let Ok(n) = v.parse::<usize>()
        {
            return n.min(64);
        }
        std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, 4))
            .unwrap_or(2)
    })
}

/// A metadata store backed by a single SQLite database.
pub struct SqliteMetadataStore {
    /// The one connection that may write.
    ///
    /// SQLite allows a single writer, so serializing writes on one connection is
    /// not a limitation this imposes — it is the engine's own rule, and holding
    /// the writer here is what lets a [`SqliteTxn`] pin it for the life of a
    /// transaction.
    conn: Arc<Mutex<Connection>>,
    /// Additional read-only connections to the same database file.
    ///
    /// Every read *and* write used to serialize on `conn`. WAL was enabled and
    /// then thrown away: its entire point is that readers do not block on the
    /// writer or on each other, and one `Mutex<Connection>` made the metadata
    /// store a global lock for the process. That is invisible for a solo CLI call
    /// and very visible under a mount, which issues concurrent requests, or a
    /// `readdir` racing a write.
    ///
    /// Empty for an in-memory database, where a second `Connection` would open a
    /// *different*, empty database rather than another view of this one — see
    /// [`Self::read`], which falls back to the writer.
    readers: Arc<Vec<Mutex<Connection>>>,
    /// Round-robin cursor into `readers`, so concurrent readers spread across the
    /// pool instead of all queueing on the first one.
    next_reader: Arc<std::sync::atomic::AtomicUsize>,
    /// The workspace this handle is bound to (default = 1). Workspace-scoped
    /// statements stamp/filter by it; [`SqliteMetadataStore::with_workspace`]
    /// rebinds a handle that shares this connection (`docs/MULTI_TENANCY.md`).
    workspace_id: i64,
}

impl SqliteMetadataStore {
    /// Open (creating if needed) a database file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        // Create the parent directory so a workspace path "just works", matching
        // LocalCasStore::open (SQLite itself won't create missing directories).
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path.as_ref())?;
        // `busy_timeout` so a second process/writer waits for the lock instead of
        // failing instantly with `SQLITE_BUSY` ("database is locked").
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )?;

        // WAL is set on the file, so every connection opened after this one
        // inherits it and reads a consistent snapshot without blocking the
        // writer.
        let mut readers = Vec::with_capacity(reader_count());
        for _ in 0..reader_count() {
            let r = Connection::open(path.as_ref())?;
            // `query_only` is a safety interlock, not a tuning knob: if a method
            // that mutates is ever routed to the read pool, SQLite refuses the
            // statement loudly instead of writing on a connection outside the
            // single-writer discipline. A misclassification becomes a failing
            // test rather than a corruption.
            r.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; \
                 PRAGMA query_only=ON;",
            )?;
            readers.push(Mutex::new(r));
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            readers: Arc::new(readers),
            next_reader: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            workspace_id: DEFAULT_WORKSPACE,
        })
    }

    /// Open a private in-memory database (handy for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            // No read pool: `open_in_memory` gives each connection its own
            // private database, so a second one would be an empty database
            // rather than another view of this one. `read()` falls back to the
            // writer, which is the pre-pool behaviour and correct here.
            readers: Arc::new(Vec::new()),
            next_reader: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            workspace_id: DEFAULT_WORKSPACE,
        })
    }

    /// A connection for a **read**.
    ///
    /// Picks a free reader if one is available, otherwise blocks on the
    /// round-robin choice rather than falling back to the writer — taking the
    /// writer for a read is what this pool exists to stop, and a reader that is
    /// briefly busy is still faster to wait for than a write in flight.
    ///
    /// With no pool (an in-memory database) this is the writer, which is the
    /// behaviour every read had before the pool existed.
    fn read(&self) -> MutexGuard<'_, Connection> {
        if self.readers.is_empty() {
            return self.lock();
        }
        let start = self
            .next_reader
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for i in 0..self.readers.len() {
            let idx = (start + i) % self.readers.len();
            if let Some(g) = self.readers[idx].try_lock() {
                return g;
            }
        }
        self.readers[start % self.readers.len()].lock()
    }

    /// The single writer connection.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        // `parking_lot::Mutex` does not poison: a panic while another operation
        // holds the lock simply releases it on unwind, so a single panicking
        // statement can't brick every subsequent metadata call for the life of
        // the process (M4). This also gives the owned, `Send` guard that a
        // [`SqliteTxn`] holds across `.await`s (C1).
        self.conn.lock()
    }
}

/// The inode columns every `SELECT` in this backend reads, in the order
/// [`build_inode`] expects them. One constant so a column added to the row (uid
/// and gid, in V17) cannot be picked up by some queries and missed by others.
const INODE_COLS: &str = "ino, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid";

/// Build an [`Inode`] from a raw row tuple.
#[allow(clippy::type_complexity)]
fn build_inode(
    row: (
        i64,
        String,
        i64,
        i64,
        i64,
        Option<String>,
        i64,
        i64,
        i64,
        i64,
    ),
) -> Result<Inode> {
    let (ino, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid) = row;
    let kind = FileKind::parse(&kind)
        .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind:?}")))?;
    let content = match content_hash {
        Some(s) => Some(
            Hash::from_hex(&s)
                .ok_or_else(|| OrigoFSError::Metadata(format!("bad content hash {s:?}")))?,
        ),
        None => None,
    };
    Ok(Inode {
        ino,
        kind,
        mode: mode as u32,
        nlink,
        size: size as u64,
        content,
        mtime,
        ctime,
        uid: uid as u32,
        gid: gid as u32,
    })
}

/// Columns every trash `SELECT` reads, in the order [`build_trash`] expects.
const TRASH_COLS: &str = "id, path, kind, mode, size, content_hash, symlink_target, \
                          uid, gid, actor_id, session_id, deleted_at";

type TrashRow = (
    i64,
    String,
    String,
    i64,
    i64,
    Option<String>,
    Option<String>,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    i64,
);

fn trash_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<TrashRow> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
    ))
}

fn build_trash(row: TrashRow) -> Result<crate::trash::TrashEntry> {
    let (
        id,
        path,
        kind,
        mode,
        size,
        content_hash,
        symlink_target,
        uid,
        gid,
        actor_id,
        session_id,
        deleted_at,
    ) = row;
    Ok(crate::trash::TrashEntry {
        id,
        path,
        kind: FileKind::parse(&kind)
            .ok_or_else(|| OrigoFSError::Metadata(format!("unknown trash kind {kind:?}")))?,
        mode: mode as u32,
        size: size as u64,
        content: content_hash.as_deref().and_then(Hash::from_hex),
        symlink_target,
        owner: crate::types::Owner::new(uid as u32, gid as u32),
        actor_id,
        session_id,
        deleted_at,
    })
}

/// Refuse a table name that is not in the dump allowlist (issue #117).
///
/// `export_table`/`import_table` interpolate the name into SQL, which is only safe
/// because of this check — it is the boundary that keeps a name-taking method from
/// being an arbitrary-SQL hole. Returns the `&'static str` from the allowlist
/// rather than the caller's string, so what reaches the SQL is a constant from this
/// binary and never caller-controlled bytes.
pub(crate) fn validated_dump_table(table: &str) -> Result<&'static str> {
    crate::portable::DUMP_TABLES
        .iter()
        .find(|t| **t == table)
        .copied()
        .ok_or_else(|| OrigoFSError::InvalidArgument(format!("{table:?} is not a dumpable table")))
}

/// Read column `i` of a row as a backend-neutral [`Cell`](crate::portable::Cell).
fn sqlite_cell(r: &rusqlite::Row<'_>, i: usize) -> rusqlite::Result<crate::portable::Cell> {
    use crate::portable::Cell;
    use rusqlite::types::ValueRef;
    Ok(match r.get_ref(i)? {
        ValueRef::Null => Cell::Null,
        ValueRef::Integer(v) => Cell::Int(v),
        // SQLite is dynamically typed, so a REAL can appear in a column every
        // other backend calls BIGINT. Carried as text rather than silently
        // truncated to an integer.
        ValueRef::Real(v) => Cell::Text(v.to_string()),
        ValueRef::Text(t) => Cell::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Cell::Bytes(b.to_vec()),
    })
}

fn cell_to_sqlite(c: &crate::portable::Cell) -> rusqlite::types::Value {
    use crate::portable::Cell;
    use rusqlite::types::Value;
    match c {
        Cell::Null => Value::Null,
        Cell::Int(i) => Value::Integer(*i),
        Cell::Text(s) => Value::Text(s.clone()),
        Cell::Bytes(b) => Value::Blob(b.clone()),
    }
}

/// True if a DDL error is SQLite's "duplicate column name" — i.e. an
/// `ADD COLUMN` migration re-applied to a table that already has the column.
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    e.to_string().contains("duplicate column name")
}

/// Split a migration's SQL into individual statements.
///
/// The runner tolerates a duplicate-column error so a re-applied `ADD COLUMN` is a
/// no-op (SQLite has no `IF NOT EXISTS` for it). That tolerance has to be
/// **per statement**: `execute_batch` stops at the first failing statement, so
/// swallowing the error there and stamping the version recorded a migration as
/// applied while everything after the duplicate column had silently not run. V11
/// is the dangerous shape — it adds a column *before* rebuilding four tables to
/// widen their primary keys — so the re-apply the tolerance exists for left `ref`
/// on its old `name`-only key while claiming V11 was done, and every later
/// `set_ref`/`cas_ref` (`ON CONFLICT(workspace_id, name)`) failed. Postgres, whose
/// statements each carry `IF NOT EXISTS`, self-heals the same state.
///
/// Splitting is on `;` outside single-quoted strings; toggling on each quote also
/// handles SQLite's doubled-quote escape (`''` toggles off then straight back on).
/// These are DDL migrations authored in this file — no dollar-quoting, no
/// semicolons in identifiers.
fn sql_statements(sql: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut in_quote = false;
    let mut start = 0;
    for (i, c) in sql.char_indices() {
        match c {
            '\'' => in_quote = !in_quote,
            ';' if !in_quote => {
                let stmt = sql[start..i].trim();
                if !stmt.is_empty() {
                    out.push(stmt);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let tail = sql[start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out
}

/// Clear only workspace `ws`'s working tree (checkout/merge/rebuild). `dentry` and
/// `symlink` carry no `workspace_id`, so they are cleared via inode ownership; the
/// workspace's own root inode is kept. Blame (keyed by content hash) is untouched.
fn truncate_workspace_tree(conn: &Connection, ws: i64) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM dentry WHERE parent_ino IN (SELECT ino FROM inode WHERE workspace_id = ?1)",
        params![ws],
    )?;
    conn.execute(
        "DELETE FROM symlink WHERE ino IN (SELECT ino FROM inode WHERE workspace_id = ?1)",
        params![ws],
    )?;
    // xattrs are keyed by inode, so a truncated tree takes them with it (#119).
    conn.execute(
        "DELETE FROM xattr WHERE ino IN (SELECT ino FROM inode WHERE workspace_id = ?1)",
        params![ws],
    )?;
    conn.execute(
        "DELETE FROM inode WHERE workspace_id = ?1
           AND ino <> (SELECT root_ino FROM workspace WHERE id = ?1)",
        params![ws],
    )?;
    Ok(())
}

/// Decode one `posix_lock` row. Shared by the plain listing and the transactional
/// apply so the two cannot drift in column order.
fn row_to_lock(r: &rusqlite::Row<'_>) -> rusqlite::Result<PosixLock> {
    Ok(PosixLock {
        owner: r.get(0)?,
        holder: r.get(1)?,
        pid: r.get(2)?,
        start: r.get(3)?,
        end: r.get(4)?,
        exclusive: r.get::<_, i64>(5)? != 0,
    })
}

#[async_trait]
impl StoreLifecycle for SqliteMetadataStore {
    async fn init(&self) -> Result<()> {
        blocking_section(move || {
            let mut conn = self.lock();
            let now = now_secs();
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);",
            )?;
            for m in MIGRATIONS {
                let applied = conn
                    .query_row(
                        "SELECT 1 FROM schema_meta WHERE version = ?1",
                        params![m.version],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if applied {
                    continue;
                }
                // Apply the DDL and record the version in ONE transaction, so a crash
                // can never leave a migration half-applied (its bookkeeping absent),
                // which would brick the next `init` on a non-idempotent step.
                let tx = conn.transaction()?;
                for stmt in sql_statements(m.sqlite) {
                    match tx.execute_batch(stmt) {
                        Ok(()) => {}
                        // Idempotency for a re-applied `ADD COLUMN` (SQLite lacks
                        // `IF NOT EXISTS`): the column is already present, so this
                        // statement's work is done — skip it and run the rest of the
                        // migration. Per statement, not per migration: see
                        // `sql_statements`.
                        Err(e) if is_duplicate_column(&e) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                tx.execute(
                    "INSERT INTO schema_meta(version, applied_at) VALUES (?1, ?2)",
                    params![m.version, now],
                )?;
                tx.commit()?;
            }
            conn.execute(
                "INSERT OR IGNORE INTO inode(ino, workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
                 VALUES (?1, ?2, 'dir', ?3, 1, 0, NULL, ?4, ?4)",
                params![INO_ROOT, DEFAULT_WORKSPACE, DIR_MODE, now],
            )?;
            Ok(())
        })
    }

    async fn schema_version(&self) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            match conn.query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_meta",
                [],
                |r| r.get::<_, i64>(0),
            ) {
                Ok(v) => Ok(v),
                // A store that was never initialized has no schema_meta table yet.
                Err(e) if e.to_string().contains("no such table") => Ok(0),
                Err(e) => Err(e.into()),
            }
        })
    }

    /// SQLite's **online backup API**, not a file copy.
    ///
    /// Copying `meta.db` while the workspace is live gives you a file that may
    /// be mid-transaction and whose `-wal` sidecar you probably didn't copy — it
    /// often restores, which is what makes it dangerous. The backup API walks the
    /// pages under the same locking the engine uses, so the result is a coherent
    /// database even with writers active.
    ///
    /// Runs to completion in one step: the alternative, stepping a few pages at a
    /// time, lets a concurrent writer restart the copy and can starve it
    /// indefinitely on a busy store. Holding the connection for the duration is
    /// the price of a snapshot that terminates.
    async fn backup_to(&self, dest: &std::path::Path) -> Result<String> {
        blocking_section(move || {
            if let Some(parent) = dest.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            // Refuse to clobber: a backup command that silently overwrites the
            // previous backup can destroy the only good copy.
            if dest.exists() {
                return Err(OrigoFSError::AlreadyExists(format!(
                    "{} already exists; choose another destination",
                    dest.display()
                )));
            }
            let conn = self.lock();
            let mut out = Connection::open(dest)?;
            let backup = rusqlite::backup::Backup::new(&conn, &mut out)?;
            // `-1` is SQLite's "copy every remaining page in this step". Passing a
            // huge positive count instead is a trap: `run_to_completion` asserts the
            // count is positive, and `usize::MAX as i32` wraps to -1, so it panics.
            match backup.step(-1)? {
                rusqlite::backup::StepResult::Done => {}
                other => {
                    return Err(OrigoFSError::Metadata(format!(
                        "sqlite backup did not complete: {other:?}"
                    )));
                }
            }
            drop(backup);
            let bytes = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
            Ok(format!(
                "sqlite online backup -> {} ({bytes} bytes)",
                dest.display()
            ))
        })
    }

    async fn begin(&self) -> Result<Box<dyn MetaTxn>> {
        // Hold the connection lock for the whole transaction — SQLite is
        // single-writer, so this both serializes writers and lets the txn issue
        // its statements without another operation interleaving on the shared
        // connection. `lock_arc` yields an owned, `Send` guard we can move into
        // the returned box and hold across `.await`s.
        //
        // Both the lock acquisition and `BEGIN IMMEDIATE` can wait on another
        // writer for up to `busy_timeout`, so this is the section most worth
        // handing off the worker for.
        blocking_section(move || {
            let guard = self.conn.lock_arc();
            // `BEGIN IMMEDIATE` takes the write lock now rather than lazily on
            // the first write, so a second writer waits (up to `busy_timeout`)
            // instead of failing partway through.
            guard.execute_batch("BEGIN IMMEDIATE")?;
            Ok(Box::new(SqliteTxn {
                guard: Some(guard),
                workspace_id: self.workspace_id,
            }) as Box<dyn MetaTxn>)
        })
    }
}

#[async_trait]
impl NamespaceStore for SqliteMetadataStore {
    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>> {
        blocking_section(move || {
            let conn = self.read();
            let row = conn
                .query_row(
                    &format!("SELECT {INODE_COLS} FROM inode WHERE ino = ?1"),
                    params![ino],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, i64>(4)?,
                            r.get::<_, Option<String>>(5)?,
                            r.get::<_, i64>(6)?,
                            r.get::<_, i64>(7)?,
                            r.get::<_, i64>(8)?,
                            r.get::<_, i64>(9)?,
                        ))
                    },
                )
                .optional()?;
            match row {
                Some(t) => Ok(Some(build_inode(t)?)),
                None => Ok(None),
            }
        })
    }

    async fn get_inodes(&self, inos: &[Ino]) -> Result<Vec<Inode>> {
        blocking_section(move || {
            if inos.is_empty() {
                return Ok(Vec::new());
            }
            let conn = self.read();
            let mut out = Vec::with_capacity(inos.len());
            // SQLite caps bound parameters per statement (999 on older builds), so the
            // IN-list is chunked rather than assuming the caller kept it small.
            for chunk in inos.chunks(INODE_BATCH) {
                let placeholders = (1..=chunk.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut stmt = conn.prepare(&format!(
                    "SELECT {INODE_COLS} FROM inode WHERE ino IN ({placeholders})"
                ))?;
                let binds: Vec<&dyn rusqlite::ToSql> =
                    chunk.iter().map(|i| i as &dyn rusqlite::ToSql).collect();
                let rows = stmt.query_map(binds.as_slice(), |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, Option<String>>(5)?,
                        r.get::<_, i64>(6)?,
                        r.get::<_, i64>(7)?,
                        r.get::<_, i64>(8)?,
                        r.get::<_, i64>(9)?,
                    ))
                })?;
                for row in rows {
                    out.push(build_inode(row?)?);
                }
            }
            Ok(out)
        })
    }

    async fn create_inode(&self, init: InodeInit) -> Result<Ino> {
        blocking_section(move || {
            let conn = self.lock();
            let now = now_secs();
            conn.execute(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid)
                 VALUES (?1, ?2, ?3, 1, 0, NULL, ?4, ?4, ?5, ?6)",
                params![
                    self.workspace_id,
                    init.kind.as_str(),
                    init.mode as i64,
                    now,
                    init.owner.uid as i64,
                    init.owner.gid as i64
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "UPDATE inode SET content_hash = ?1, size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?4",
                params![content.map(|h| h.to_hex()), size as i64, now_secs(), ino],
            )?;
            // A zero-row update means the inode was unlinked while the content was
            // being written, and reporting that as success loses the write
            // silently. Both `MetaTxn::set_content` implementations already checked
            // this and so did the Postgres store; this one returned `Ok(())`
            // unconditionally, so the same race was an error on one backend and an
            // acknowledged-but-lost write on the other.
            if n == 0 {
                return Err(OrigoFSError::NotFound(format!(
                    "inode {ino} was removed before its content could be written"
                )));
            }
            Ok(())
        })
    }

    async fn set_nlink(&self, ino: Ino, nlink: i64) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "UPDATE inode SET nlink = ?1 WHERE ino = ?2",
                params![nlink, ino],
            )?;
            Ok(())
        })
    }

    async fn set_mode(&self, ino: Ino, mode: u32) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            // Mask in only the permission bits: the format bits are the inode's
            // kind, not a caller's to rewrite. `& 0o7777` keeps setuid/setgid/sticky.
            conn.execute(
                "UPDATE inode SET mode = (mode & ~4095) | ?1, ctime = ?2 WHERE ino = ?3",
                params![(mode & 0o7777) as i64, now_secs(), ino],
            )?;
            Ok(())
        })
    }

    async fn set_owner(&self, ino: Ino, uid: Option<u32>, gid: Option<u32>) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            // COALESCE so a `None` half leaves the stored value alone, which is what
            // chown(2)'s -1 sentinel means.
            conn.execute(
                "UPDATE inode SET uid = COALESCE(?1, uid), gid = COALESCE(?2, gid), \
                 ctime = ?3 WHERE ino = ?4",
                params![
                    uid.map(|v| v as i64),
                    gid.map(|v| v as i64),
                    now_secs(),
                    ino
                ],
            )?;
            Ok(())
        })
    }

    async fn delete_inode(&self, ino: Ino) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute("DELETE FROM symlink WHERE ino = ?1", params![ino])?;
            // xattrs are keyed by inode, so they die with it (issue #119).
            conn.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            conn.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
            Ok(())
        })
    }

    async fn lookup(&self, parent: Ino, name: &str) -> Result<Option<Ino>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT ino FROM dentry WHERE parent_ino = ?1 AND name = ?2",
                params![parent, name],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn add_dentry(&self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            match conn.execute(
                "INSERT INTO dentry(parent_ino, name, ino) VALUES (?1, ?2, ?3)",
                params![parent, name, ino],
            ) {
                Ok(_) => Ok(()),
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    Err(OrigoFSError::AlreadyExists(name.to_string()))
                }
                Err(e) => Err(e.into()),
            }
        })
    }

    async fn remove_dentry(&self, parent: Ino, name: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "DELETE FROM dentry WHERE parent_ino = ?1 AND name = ?2",
                params![parent, name],
            )?;
            Ok(())
        })
    }

    async fn list_dir(&self, parent: Ino) -> Result<Vec<DirEntry>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT d.name, d.ino, i.kind
                 FROM dentry d JOIN inode i ON i.ino = d.ino
                 WHERE d.parent_ino = ?1
                 ORDER BY d.name",
            )?;
            let rows = stmt.query_map(params![parent], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (name, ino, kind) = row?;
                let kind = FileKind::parse(&kind).ok_or_else(|| {
                    OrigoFSError::Metadata(format!("unknown inode kind {kind:?}"))
                })?;
                out.push(DirEntry { name, ino, kind });
            }
            Ok(out)
        })
    }

    async fn list_dir_page(
        &self,
        parent: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        blocking_section(move || {
            let conn = self.read();
            let limit = limit as i64;
            // Two statements rather than one with `(?2 IS NULL OR d.name > ?2)`: the
            // OR would defeat the `(parent_ino, name)` primary-key index and turn the
            // page into a full scan of the directory, which is the whole point of the
            // method. Both forms below are a bounded range scan on that index.
            let mut stmt = conn.prepare(match after_name {
                Some(_) => {
                    "SELECT d.name, d.ino, i.kind
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent_ino = ?1 AND d.name > ?2
                     ORDER BY d.name LIMIT ?3"
                }
                None => {
                    "SELECT d.name, d.ino, i.kind
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent_ino = ?1
                     ORDER BY d.name LIMIT ?2"
                }
            })?;
            let row = |r: &rusqlite::Row<'_>| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            };
            let raw: Vec<_> = match after_name {
                Some(a) => stmt
                    .query_map(params![parent, a, limit], row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
                None => stmt
                    .query_map(params![parent, limit], row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            };
            let mut out = Vec::with_capacity(raw.len());
            for (name, ino, kind) in raw {
                let kind = FileKind::parse(&kind).ok_or_else(|| {
                    OrigoFSError::Metadata(format!("unknown inode kind {kind:?}"))
                })?;
                out.push(DirEntry { name, ino, kind });
            }
            Ok(out)
        })
    }

    async fn dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT name FROM dentry WHERE parent_ino = ?1 AND ino = ?2
                 ORDER BY name LIMIT 1",
                params![parent, ino],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn parent_of(&self, ino: Ino) -> Result<Option<Ino>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT parent_ino FROM dentry WHERE ino = ?1 LIMIT 1",
                params![ino],
                |r| r.get::<_, Ino>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn child_count(&self, parent: Ino) -> Result<usize> {
        blocking_section(move || {
            let conn = self.read();
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM dentry WHERE parent_ino = ?1",
                params![parent],
                |r| r.get(0),
            )?;
            Ok(n as usize)
        })
    }

    async fn workspace_usage(&self) -> Result<(u64, u64)> {
        blocking_section(move || {
            let conn = self.read();
            let (n, b): (i64, i64) = conn.query_row(
                "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM inode WHERE workspace_id = ?1",
                params![self.workspace_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok((n.max(0) as u64, b.max(0) as u64))
        })
    }

    async fn subtree_usage(&self, ino: Ino) -> Result<(u64, u64)> {
        blocking_section(move || {
            let conn = self.read();
            // `UNION` (not `UNION ALL`) dedups inode ids, so an inode reachable by
            // several names -- a hard link -- is counted once, as `du` does.
            let (n, b): (i64, i64) = conn.query_row(
                "WITH RECURSIVE sub(ino) AS (
                     SELECT ?1
                     UNION
                     SELECT d.ino FROM dentry d JOIN sub ON d.parent_ino = sub.ino
                 )
                 SELECT COUNT(*), COALESCE(SUM(i.size), 0)
                 FROM inode i JOIN sub ON i.ino = sub.ino",
                params![ino],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok((n.max(0) as u64, b.max(0) as u64))
        })
    }

    async fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>> {
        blocking_section(move || {
            let conn = self.read();
            Ok(conn
                .query_row(
                    "SELECT value FROM xattr WHERE ino = ?1 AND name = ?2",
                    params![ino, name],
                    |r| r.get::<_, Vec<u8>>(0),
                )
                .optional()?)
        })
    }

    async fn set_xattr(&self, ino: Ino, name: &str, value: &[u8]) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO xattr(ino, name, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(ino, name) DO UPDATE SET value = excluded.value",
                params![ino, name, value],
            )?;
            Ok(())
        })
    }

    async fn remove_xattr(&self, ino: Ino, name: &str) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM xattr WHERE ino = ?1 AND name = ?2",
                params![ino, name],
            )?;
            Ok(n > 0)
        })
    }

    async fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare("SELECT name FROM xattr WHERE ino = ?1 ORDER BY name")?;
            let rows = stmt.query_map(params![ino], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r?);
            }
            Ok(out)
        })
    }

    async fn set_symlink(&self, ino: Ino, target: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO symlink(ino, target) VALUES (?1, ?2)
                 ON CONFLICT(ino) DO UPDATE SET target = excluded.target",
                params![ino, target],
            )?;
            Ok(())
        })
    }

    async fn get_symlink(&self, ino: Ino) -> Result<Option<String>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT target FROM symlink WHERE ino = ?1",
                params![ino],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn truncate_tree(&self) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            truncate_workspace_tree(&conn, self.workspace_id)?;
            Ok(())
        })
    }
}

#[async_trait]
impl RefStore for SqliteMetadataStore {
    async fn get_ref(&self, name: &str) -> Result<Option<String>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT value FROM ref WHERE workspace_id = ?1 AND name = ?2",
                params![self.workspace_id, name],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn set_ref(&self, name: &str, value: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO ref(workspace_id, name, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, name) DO UPDATE SET value = excluded.value",
                params![self.workspace_id, name, value],
            )?;
            Ok(())
        })
    }

    async fn cas_ref(&self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let changed = match expect {
                None => conn.execute(
                    "INSERT INTO ref(workspace_id, name, value) VALUES (?1, ?2, ?3)
                     ON CONFLICT(workspace_id, name) DO NOTHING",
                    params![self.workspace_id, name, new],
                )?,
                Some(v) => conn.execute(
                    "UPDATE ref SET value = ?1 WHERE workspace_id = ?2 AND name = ?3 AND value = ?4",
                    params![new, self.workspace_id, name, v],
                )?,
            };
            Ok(changed == 1)
        })
    }

    async fn delete_ref(&self, name: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "DELETE FROM ref WHERE workspace_id = ?1 AND name = ?2",
                params![self.workspace_id, name],
            )?;
            Ok(())
        })
    }

    async fn list_refs(&self) -> Result<Vec<(String, String)>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt =
                conn.prepare("SELECT name, value FROM ref WHERE workspace_id = ?1 ORDER BY name")?;
            let rows = stmt.query_map(params![self.workspace_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }
}

#[async_trait]
impl ConfigStore for SqliteMetadataStore {
    async fn get_config(&self, key: &str) -> Result<Option<String>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT value FROM config WHERE workspace_id = ?1 AND key = ?2",
                params![self.workspace_id, key],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn set_config(&self, key: &str, value: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO config(workspace_id, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, key) DO UPDATE SET value = excluded.value",
                params![self.workspace_id, key, value],
            )?;
            Ok(())
        })
    }

    async fn bump_counter(&self, key: &str) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            // One atomic upsert: create at 1, else increment the stored integer.
            //
            // The `WHERE` is what keeps this honest on a non-integer value.
            // SQLite's `CAST` never fails — it takes the numeric prefix and yields
            // 0 for text — so a key holding `"abc"` silently reset the counter to 1
            // and `"3.7"` became 4, while Postgres's `value::bigint` raised. The
            // round-trip comparison admits exactly the values this method itself
            // writes (a plain decimal integer) and rejects the rest, leaving the
            // row untouched and returning no row.
            let v: Option<i64> = conn
                .query_row(
                    "INSERT INTO config(workspace_id, key, value) VALUES (?1, ?2, '1')
                     ON CONFLICT(workspace_id, key) DO UPDATE SET value = CAST(value AS INTEGER) + 1
                       WHERE CAST(CAST(value AS INTEGER) AS TEXT) = value
                     RETURNING CAST(value AS INTEGER)",
                    params![self.workspace_id, key],
                    |r| r.get(0),
                )
                .optional()?;
            v.ok_or_else(|| {
                OrigoFSError::InvalidArgument(format!(
                    "config key {key:?} does not hold an integer, so it cannot be used as a counter"
                ))
            })
        })
    }
}

#[async_trait]
impl WorkspaceRegistry for SqliteMetadataStore {
    fn with_workspace(&self, workspace_id: i64) -> Arc<dyn MetadataStore> {
        Arc::new(SqliteMetadataStore {
            conn: self.conn.clone(),
            readers: self.readers.clone(),
            next_reader: self.next_reader.clone(),
            workspace_id,
        })
    }

    async fn create_workspace(&self, name: &str) -> Result<(i64, Ino)> {
        blocking_section(move || {
            let mut conn = self.lock();
            let now = now_secs();
            let tx = conn.transaction()?;
            // Reserve the row (fails on a duplicate name), give it its own root
            // directory inode, then point the row at that inode — all atomic.
            match tx.execute(
                "INSERT INTO workspace(name, root_ino, created_at) VALUES (?1, 0, ?2)",
                params![name, now],
            ) {
                Ok(_) => {}
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    return Err(OrigoFSError::AlreadyExists(format!("workspace {name}")));
                }
                Err(e) => return Err(e.into()),
            }
            let id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
                 VALUES (?1, 'dir', ?2, 1, 0, NULL, ?3, ?3)",
                params![id, DIR_MODE, now],
            )?;
            let root_ino = tx.last_insert_rowid();
            tx.execute(
                "UPDATE workspace SET root_ino = ?1 WHERE id = ?2",
                params![root_ino, id],
            )?;
            tx.commit()?;
            Ok((id, root_ino))
        })
    }

    async fn lookup_workspace(&self, name: &str) -> Result<Option<(i64, Ino)>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT id, root_ino FROM workspace WHERE name = ?1",
                params![name],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<(i64, String, Ino)>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare("SELECT id, name, root_ino FROM workspace ORDER BY id")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }
}

#[async_trait]
impl AclStore for SqliteMetadataStore {
    async fn set_acl(
        &self,
        actor_id: i64,
        path_prefix: &str,
        perms: u32,
        granted_at: i64,
        granted_by: Option<i64>,
    ) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO acl(workspace_id, actor_id, path_prefix, perms, granted_at, granted_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(workspace_id, actor_id, path_prefix)
                 DO UPDATE SET perms = excluded.perms,
                               granted_at = excluded.granted_at,
                               granted_by = excluded.granted_by",
                params![
                    self.workspace_id,
                    actor_id,
                    path_prefix,
                    perms as i64,
                    granted_at,
                    granted_by
                ],
            )?;
            Ok(())
        })
    }

    async fn remove_acl(&self, actor_id: i64, path_prefix: &str) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM acl WHERE workspace_id = ?1 AND actor_id = ?2 AND path_prefix = ?3",
                params![self.workspace_id, actor_id, path_prefix],
            )?;
            Ok(n > 0)
        })
    }

    async fn list_acl(&self, actor_id: Option<i64>) -> Result<Vec<crate::acl::AclGrant>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT actor_id, path_prefix, perms, granted_at, granted_by FROM acl
                 WHERE workspace_id = ?1 AND (?2 IS NULL OR actor_id = ?2)
                 ORDER BY LENGTH(path_prefix) DESC",
            )?;
            let rows = stmt.query_map(params![self.workspace_id, actor_id], |r| {
                Ok(crate::acl::AclGrant {
                    actor_id: r.get(0)?,
                    path_prefix: r.get(1)?,
                    perms: crate::acl::Perms::from_bits(r.get::<_, i64>(2)? as u32),
                    granted_at: r.get(3)?,
                    granted_by: r.get(4)?,
                })
            })?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r?);
            }
            Ok(out)
        })
    }
}

#[async_trait]
impl TrashStore for SqliteMetadataStore {
    async fn push_trash(&self, init: crate::trash::TrashInit) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO trash(workspace_id, path, kind, mode, size, content_hash,
                                   symlink_target, uid, gid, actor_id, session_id, deleted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    self.workspace_id,
                    init.path,
                    init.kind.as_str(),
                    init.mode as i64,
                    init.size as i64,
                    init.content.map(|h| h.to_hex()),
                    init.symlink_target,
                    init.owner.uid as i64,
                    init.owner.gid as i64,
                    init.actor_id,
                    init.session_id,
                    init.deleted_at,
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn get_trash(&self, id: i64) -> Result<Option<crate::trash::TrashEntry>> {
        blocking_section(move || {
            let conn = self.read();
            let row = conn
                .query_row(
                    &format!("SELECT {TRASH_COLS} FROM trash WHERE id = ?1 AND workspace_id = ?2"),
                    params![id, self.workspace_id],
                    trash_row,
                )
                .optional()?;
            row.map(build_trash).transpose()
        })
    }

    async fn list_trash(&self) -> Result<Vec<crate::trash::TrashEntry>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(&format!(
                "SELECT {TRASH_COLS} FROM trash WHERE workspace_id = ?1
                 ORDER BY deleted_at DESC, id DESC"
            ))?;
            let rows = stmt.query_map(params![self.workspace_id], trash_row)?;
            let mut out = Vec::new();
            for r in rows {
                out.push(build_trash(r?)?);
            }
            Ok(out)
        })
    }

    async fn delete_trash(&self, id: i64) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM trash WHERE id = ?1 AND workspace_id = ?2",
                params![id, self.workspace_id],
            )?;
            Ok(n > 0)
        })
    }

    async fn purge_trash_before(&self, cutoff: i64) -> Result<usize> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM trash WHERE workspace_id = ?1 AND deleted_at < ?2",
                params![self.workspace_id, cutoff],
            )?;
            Ok(n)
        })
    }

    async fn trash_content_hashes(&self) -> Result<Vec<Hash>> {
        blocking_section(move || {
            let conn = self.read();
            // Store-wide, not workspace-scoped: `gc` sweeps one shared content
            // store, so a workspace-scoped root would let it reclaim another
            // workspace's trashed content.
            let mut stmt =
                conn.prepare("SELECT content_hash FROM trash WHERE content_hash IS NOT NULL")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows {
                if let Some(h) = Hash::from_hex(&r?) {
                    out.push(h);
                }
            }
            Ok(out)
        })
    }
}

#[async_trait]
impl LockStore for SqliteMetadataStore {
    async fn set_conflict(&self, path: &str, kind: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO conflict(workspace_id, path, kind) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, path) DO UPDATE SET kind = excluded.kind",
                params![self.workspace_id, path, kind],
            )?;
            Ok(())
        })
    }

    async fn list_conflicts(&self) -> Result<Vec<(String, String)>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn
                .prepare("SELECT path, kind FROM conflict WHERE workspace_id = ?1 ORDER BY path")?;
            let rows = stmt.query_map(params![self.workspace_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn clear_conflicts(&self) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "DELETE FROM conflict WHERE workspace_id = ?1",
                params![self.workspace_id],
            )?;
            Ok(())
        })
    }

    async fn acquire_lock(&self, path: &str, owner: &str, at: i64) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let changed = conn.execute(
                "INSERT INTO file_lock(workspace_id, path, owner, acquired_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(workspace_id, path) DO NOTHING",
                params![self.workspace_id, path, owner, at],
            )?;
            Ok(changed == 1)
        })
    }

    async fn release_lock(&self, path: &str, owner: &str) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let changed = conn.execute(
                "DELETE FROM file_lock WHERE workspace_id = ?1 AND path = ?2 AND owner = ?3",
                params![self.workspace_id, path, owner],
            )?;
            Ok(changed == 1)
        })
    }

    async fn list_locks(&self) -> Result<Vec<(String, String, i64)>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT path, owner, acquired_at FROM file_lock WHERE workspace_id = ?1 ORDER BY path",
            )?;
            let rows = stmt.query_map(params![self.workspace_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn posix_locks(&self, ino: Ino, now: i64) -> Result<Vec<PosixLock>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT owner, holder, pid, start_off, end_off, exclusive FROM posix_lock
                 WHERE workspace_id = ?1 AND ino = ?2 AND expires_at > ?3 ORDER BY start_off",
            )?;
            let rows = stmt.query_map(params![self.workspace_id, ino, now], row_to_lock)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn apply_posix_lock(
        &self,
        ino: Ino,
        req: &LockRequest,
        expires_at: i64,
        now: i64,
    ) -> Result<Option<PosixLock>> {
        let req = req.clone();
        blocking_section(move || {
            let mut conn = self.lock();
            // `Immediate` takes the write lock before the SELECT. A deferred
            // transaction would read, decide, and only then discover another
            // process had written — with the decision already made from stale
            // rows, which is exactly the race this table exists to close.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // An expired lease is not a blocker, so drop those rows here rather
            // than needing a background reaper to make progress possible.
            tx.execute(
                "DELETE FROM posix_lock WHERE workspace_id = ?1 AND ino = ?2 AND expires_at <= ?3",
                params![self.workspace_id, ino, now],
            )?;
            let existing = {
                let mut stmt = tx.prepare(
                    "SELECT owner, holder, pid, start_off, end_off, exclusive FROM posix_lock
                     WHERE workspace_id = ?1 AND ino = ?2 ORDER BY start_off",
                )?;
                let rows = stmt.query_map(params![self.workspace_id, ino], row_to_lock)?;
                let mut v = Vec::new();
                for row in rows {
                    v.push(row?);
                }
                v
            };
            let res = crate::posixlock::resolve(&existing, &req);
            for (owner, start) in &res.delete {
                tx.execute(
                    "DELETE FROM posix_lock
                     WHERE workspace_id = ?1 AND ino = ?2 AND owner = ?3 AND start_off = ?4",
                    params![self.workspace_id, ino, owner, start],
                )?;
            }
            for l in &res.insert {
                tx.execute(
                    "INSERT INTO posix_lock(workspace_id, ino, owner, holder, pid, start_off,
                                            end_off, exclusive, acquired_at, expires_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        self.workspace_id,
                        ino,
                        l.owner,
                        l.holder,
                        l.pid,
                        l.start,
                        l.end,
                        i64::from(l.exclusive),
                        now,
                        expires_at
                    ],
                )?;
            }
            // Committed even when refused: the request wrote nothing, but the
            // expired rows it cleared should stay cleared.
            tx.commit()?;
            Ok(res.conflict)
        })
    }

    async fn release_posix_locks_for_holder(&self, holder: &str) -> Result<u64> {
        let holder = holder.to_string();
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM posix_lock WHERE workspace_id = ?1 AND holder = ?2",
                params![self.workspace_id, holder],
            )?;
            Ok(n as u64)
        })
    }

    async fn renew_posix_lease(&self, holder: &str, expires_at: i64) -> Result<u64> {
        let holder = holder.to_string();
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "UPDATE posix_lock SET expires_at = ?3 WHERE workspace_id = ?1 AND holder = ?2",
                params![self.workspace_id, holder, expires_at],
            )?;
            Ok(n as u64)
        })
    }

    async fn claim_undo_stack(
        &self,
        path: &str,
        root: &str,
        actor_id: i64,
        holder: &str,
        expires_at: i64,
        now: i64,
    ) -> Result<bool> {
        let (path, root, holder) = (path.to_string(), root.to_string(), holder.to_string());
        blocking_section(move || {
            let conn = self.lock();
            // One statement, so read-decide-write is atomic without an explicit
            // transaction: the upsert takes the row's lock, and the `WHERE` on the
            // conflict arm is what refuses a live claim held by somebody else. Two
            // workers racing here cannot both be told yes.
            let n = conn.execute(
                "INSERT INTO coedit_undo_claim \
                   (workspace_id, path, root, actor_id, holder, claimed_at, expires_at) \
                 VALUES (?1, ?2, ?7, ?3, ?4, ?6, ?5) \
                 ON CONFLICT(workspace_id, path, root, actor_id) DO UPDATE SET \
                   holder = excluded.holder, expires_at = excluded.expires_at \
                 WHERE coedit_undo_claim.holder = excluded.holder \
                    OR coedit_undo_claim.expires_at <= ?6",
                params![
                    self.workspace_id,
                    path,
                    actor_id,
                    holder,
                    expires_at,
                    now,
                    root
                ],
            )?;
            Ok(n > 0)
        })
    }

    async fn release_undo_stack(
        &self,
        path: &str,
        root: &str,
        actor_id: i64,
        holder: &str,
    ) -> Result<bool> {
        let (path, root, holder) = (path.to_string(), root.to_string(), holder.to_string());
        blocking_section(move || {
            let conn = self.lock();
            // Scoped to `holder`: a claim that has since been taken over by another
            // worker (this one's lease expired) is not ours to drop.
            let n = conn.execute(
                "DELETE FROM coedit_undo_claim \
                 WHERE workspace_id = ?1 AND path = ?2 AND root = ?5 \
                   AND actor_id = ?3 AND holder = ?4",
                params![self.workspace_id, path, actor_id, holder, root],
            )?;
            Ok(n > 0)
        })
    }

    async fn release_undo_claims_for_holder(&self, holder: &str) -> Result<u64> {
        let holder = holder.to_string();
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "DELETE FROM coedit_undo_claim WHERE workspace_id = ?1 AND holder = ?2",
                params![self.workspace_id, holder],
            )?;
            Ok(n as u64)
        })
    }

    async fn renew_undo_claims(&self, holder: &str, expires_at: i64) -> Result<u64> {
        let holder = holder.to_string();
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "UPDATE coedit_undo_claim SET expires_at = ?3 \
                 WHERE workspace_id = ?1 AND holder = ?2",
                params![self.workspace_id, holder, expires_at],
            )?;
            Ok(n as u64)
        })
    }
}

#[async_trait]
impl AttributionStore for SqliteMetadataStore {
    async fn create_actor(&self, init: ActorInit) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            let kind = init.kind.unwrap_or(ActorKind::System);
            conn.execute(
                "INSERT INTO actor(kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    kind.as_str(),
                    init.display_name,
                    init.auth_subject,
                    init.agent_model,
                    init.agent_vendor,
                    init.controller_actor_id,
                    now_secs()
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        blocking_section(move || {
            let conn = self.read();
            let row = conn
                .query_row(
                    "SELECT id, kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at, write_policy
                     FROM actor WHERE id = ?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<String>>(4)?,
                            r.get::<_, Option<String>>(5)?,
                            r.get::<_, Option<i64>>(6)?,
                            r.get::<_, i64>(7)?,
                            r.get::<_, i64>(8)?,
                        ))
                    },
                )
                .optional()?;
            match row {
                Some((
                    id,
                    kind,
                    display_name,
                    auth_subject,
                    agent_model,
                    agent_vendor,
                    controller,
                    created_at,
                    write_policy,
                )) => {
                    let kind = ActorKind::parse(&kind).ok_or_else(|| {
                        OrigoFSError::Metadata(format!("bad actor kind {kind:?}"))
                    })?;
                    Ok(Some(Actor {
                        id,
                        kind,
                        display_name,
                        auth_subject,
                        agent_model,
                        agent_vendor,
                        controller_actor_id: controller,
                        created_at,
                        write_policy: WritePolicy::from_i64(write_policy),
                    }))
                }
                None => Ok(None),
            }
        })
    }

    async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "UPDATE actor SET write_policy = ?1 WHERE id = ?2",
                params![policy.as_i64(), actor_id],
            )?;
            if n == 0 {
                return Err(OrigoFSError::NotFound(format!("actor #{actor_id}")));
            }
            Ok(())
        })
    }

    async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        // Resolve the id under the lock, then reuse get_actor for the row mapping.
        let id: Option<i64> = blocking_section(|| {
            let conn = self.read();
            conn.query_row(
                "SELECT id FROM actor WHERE auth_subject = ?1",
                params![subject],
                |r| r.get::<_, i64>(0),
            )
            .optional()
        })?;
        match id {
            Some(id) => self.get_actor(id).await,
            None => Ok(None),
        }
    }

    async fn list_actors(&self) -> Result<Vec<Actor>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT id, kind, display_name, auth_subject, agent_model, agent_vendor, controller_actor_id, created_at, write_policy
                 FROM actor ORDER BY id",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<i64>>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, i64>(8)?,
                ))
            })?;
            let mut actors = Vec::new();
            for row in rows {
                let (
                    id,
                    kind,
                    display_name,
                    auth_subject,
                    agent_model,
                    agent_vendor,
                    controller,
                    created_at,
                    write_policy,
                ) = row?;
                let kind = ActorKind::parse(&kind)
                    .ok_or_else(|| OrigoFSError::Metadata(format!("bad actor kind {kind:?}")))?;
                actors.push(Actor {
                    id,
                    kind,
                    display_name,
                    auth_subject,
                    agent_model,
                    agent_vendor,
                    controller_actor_id: controller,
                    created_at,
                    write_policy: WritePolicy::from_i64(write_policy),
                });
            }
            Ok(actors)
        })
    }

    async fn create_session(
        &self,
        actor_id: i64,
        client: Option<&str>,
        started_at: i64,
    ) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO session(actor_id, client, started_at, ended_at) VALUES (?1, ?2, ?3, NULL)",
                params![actor_id, client, started_at],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO tool_calls(session_id, actor_id, name, parameters, result, error, started_at, completed_at, duration_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    tc.session_id, tc.actor_id, tc.name, tc.parameters, tc.result, tc.error,
                    tc.started_at, tc.completed_at, tc.duration_ms
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn append_edit_op(&self, op: EditOpInit) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO edit_op(workspace_id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    self.workspace_id, op.session_id, op.actor_id, op.tool_call_id, op.ino, op.path, op.op,
                    op.byte_start, op.byte_len, op.pre_hash, op.post_hash, op.ts
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn list_edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts
                 FROM edit_op WHERE workspace_id = ?1 AND actor_id = ?2 AND (?3 IS NULL OR session_id = ?3) ORDER BY id",
            )?;
            let rows = stmt.query_map(params![self.workspace_id, actor_id, session_id], |r| {
                Ok(EditOp {
                    id: r.get(0)?,
                    session_id: r.get(1)?,
                    actor_id: r.get(2)?,
                    tool_call_id: r.get(3)?,
                    ino: r.get(4)?,
                    path: r.get(5)?,
                    op: r.get(6)?,
                    byte_start: r.get(7)?,
                    byte_len: r.get(8)?,
                    pre_hash: r.get(9)?,
                    post_hash: r.get(10)?,
                    ts: r.get(11)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn set_blob_blame(&self, content: &Hash, runs: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, content_hash) DO UPDATE SET runs = excluded.runs",
                params![self.workspace_id, content.to_hex(), runs],
            )?;
            Ok(())
        })
    }

    async fn get_blob_blame(&self, content: &Hash) -> Result<Option<String>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT runs FROM blob_blame WHERE workspace_id = ?1 AND content_hash = ?2",
                params![self.workspace_id, content.to_hex()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
    }
}

#[async_trait]
impl CollabStore for SqliteMetadataStore {
    async fn append_event(&self, ev: EventInit, ts: i64) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO fs_event(workspace_id, actor_id, session_id, kind, path, detail, ts, branch)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    self.workspace_id,
                    ev.actor_id,
                    ev.session_id,
                    ev.kind,
                    ev.path,
                    ev.detail,
                    ts,
                    ev.branch
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>> {
        // SQLite reads a negative LIMIT as unbounded; rejecting it here keeps the
        // two backends answering the same way. See the trait.
        crate::metadata::reject_negative_limit(limit)?;
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT seq, actor_id, session_id, kind, path, detail, ts, branch FROM fs_event
                 WHERE workspace_id = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![self.workspace_id, after_seq, limit], |r| {
                Ok(Event {
                    seq: r.get(0)?,
                    actor_id: r.get(1)?,
                    session_id: r.get(2)?,
                    kind: r.get(3)?,
                    path: r.get(4)?,
                    detail: r.get(5)?,
                    ts: r.get(6)?,
                    branch: r.get(7)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
        at: i64,
    ) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO presence(session_id, workspace_id, actor_id, path, last_seen) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(session_id) DO UPDATE SET
                     workspace_id = excluded.workspace_id, actor_id = excluded.actor_id,
                     path = excluded.path, last_seen = excluded.last_seen",
                params![session_id, self.workspace_id, actor_id, path, at],
            )?;
            Ok(())
        })
    }

    async fn active_presence(&self, since_ts: i64) -> Result<Vec<Presence>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT p.session_id, p.actor_id, a.display_name, a.kind, p.path, p.last_seen
                 FROM presence p JOIN actor a ON a.id = p.actor_id
                 WHERE p.workspace_id = ?1 AND p.last_seen >= ?2 ORDER BY p.last_seen DESC",
            )?;
            let rows = stmt.query_map(params![self.workspace_id, since_ts], |r| {
                let kind: String = r.get(3)?;
                Ok(Presence {
                    session_id: r.get(0)?,
                    actor_id: r.get(1)?,
                    display_name: r.get(2)?,
                    kind: ActorKind::parse(&kind).unwrap_or(ActorKind::System),
                    path: r.get(4)?,
                    last_seen: r.get(5)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn reap_presence(&self, older_than: i64) -> Result<u64> {
        blocking_section(move || {
            let conn = self.lock();
            // Scoped to this workspace: a store-wide reap would evict other workspaces'
            // presence (including live sessions) whenever one workspace uses a shorter
            // cutoff. `touch_presence`/`active_presence` are both workspace-scoped too.
            let n = conn.execute(
                "DELETE FROM presence WHERE workspace_id = ?1 AND last_seen < ?2",
                params![self.workspace_id, older_than],
            )?;
            Ok(n as u64)
        })
    }

    async fn set_live_doc(
        &self,
        path: &str,
        session_id: Option<i64>,
        actor_id: i64,
        content_hash: Option<&str>,
        at: i64,
        checkpointed_at: Option<i64>,
    ) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            // `since` is deliberately not in the DO UPDATE list: re-marking an
            // already-live path (a second joiner, a checkpoint) keeps when it first
            // went live.
            //
            // `checkpointed_at` uses COALESCE so a re-mark that is *not* a
            // checkpoint (excluded value NULL) keeps the previous stamp rather than
            // erasing it — the row must never claim a checkpoint that didn't
            // happen, nor forget one that did.
            conn.execute(
                "INSERT INTO live_doc(workspace_id, path, session_id, actor_id, content_hash, since, checkpointed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(workspace_id, path) DO UPDATE SET
                     session_id = excluded.session_id,
                     actor_id = excluded.actor_id,
                     content_hash = excluded.content_hash,
                     checkpointed_at = COALESCE(excluded.checkpointed_at, live_doc.checkpointed_at)",
                params![
                    self.workspace_id,
                    path,
                    session_id,
                    actor_id,
                    content_hash,
                    at,
                    checkpointed_at
                ],
            )?;
            Ok(())
        })
    }

    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT path, session_id, actor_id, content_hash, since, checkpointed_at
                 FROM live_doc WHERE workspace_id = ?1 AND path = ?2",
                params![self.workspace_id, path],
                row_to_live_doc,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn list_live_docs(&self) -> Result<Vec<LiveDoc>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT path, session_id, actor_id, content_hash, since, checkpointed_at
                 FROM live_doc WHERE workspace_id = ?1 ORDER BY path",
            )?;
            let rows = stmt.query_map(params![self.workspace_id], row_to_live_doc)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn clear_live_doc(&self, path: &str) -> Result<()> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "DELETE FROM live_doc WHERE workspace_id = ?1 AND path = ?2",
                params![self.workspace_id, path],
            )?;
            Ok(())
        })
    }
}

#[async_trait]
impl SuggestionStore for SqliteMetadataStore {
    async fn create_suggestion(&self, init: SuggestionInit, ts: i64) -> Result<i64> {
        blocking_section(move || {
            let conn = self.lock();
            conn.execute(
                "INSERT INTO suggestion(workspace_id, actor_id, session_id, branch, path, base_hash,
                     proposed_hash, summary, status, created_ts, kind)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    self.workspace_id,
                    init.actor_id,
                    init.session_id,
                    init.branch,
                    init.path,
                    init.base_hash,
                    init.proposed_hash,
                    init.summary,
                    SuggestionStatus::Pending.as_str(),
                    ts,
                    init.kind.as_str(),
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        blocking_section(move || {
            let conn = self.read();
            conn.query_row(
                "SELECT id, actor_id, session_id, branch, path, base_hash, proposed_hash,
                     summary, status, created_ts, resolved_ts, resolved_by, kind
                 FROM suggestion WHERE id = ?1 AND workspace_id = ?2",
                params![id, self.workspace_id],
                row_to_suggestion,
            )
            .optional()
            .map_err(Into::into)
        })
    }

    async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        blocking_section(move || {
            let conn = self.read();
            let mut stmt = conn.prepare(
                "SELECT id, actor_id, session_id, branch, path, base_hash, proposed_hash,
                     summary, status, created_ts, resolved_ts, resolved_by, kind
                 FROM suggestion
                 WHERE workspace_id = ?1 AND (?2 IS NULL OR status = ?2) AND (?3 IS NULL OR path = ?3)
                 ORDER BY id DESC",
            )?;
            let rows = stmt.query_map(
                params![self.workspace_id, status.map(|s| s.as_str()), path],
                row_to_suggestion,
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    async fn resolve_suggestion(
        &self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        blocking_section(move || {
            let conn = self.lock();
            let n = conn.execute(
                "UPDATE suggestion SET status = ?1, resolved_by = ?2, resolved_ts = ?3
                 WHERE id = ?4 AND workspace_id = ?5 AND status = 'pending'",
                params![status.as_str(), resolved_by, ts, id, self.workspace_id],
            )?;
            Ok(n == 1)
        })
    }
}

#[async_trait]
impl PortableStore for SqliteMetadataStore {
    async fn export_table(&self, table: &str) -> Result<Vec<crate::portable::Row>> {
        let table = validated_dump_table(table)?;
        blocking_section(move || {
            let conn = self.read();
            // Quoted: `ref` is a reserved word in both dialects, and the allowlist
            // above is what makes interpolating the name here safe at all.
            let mut stmt = conn.prepare(&format!("SELECT * FROM \"{table}\""))?;
            let cols: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(r) = rows.next()? {
                let mut cells = Vec::with_capacity(cols.len());
                for (i, name) in cols.iter().enumerate() {
                    cells.push((name.clone(), sqlite_cell(r, i)?));
                }
                out.push(crate::portable::Row(cells));
            }
            Ok(out)
        })
    }

    async fn import_table(&self, table: &str, rows: &[crate::portable::Row]) -> Result<()> {
        let table = validated_dump_table(table)?;
        if rows.is_empty() {
            return Ok(());
        }
        blocking_section(move || {
            let mut conn = self.lock();
            let tx = conn.transaction()?;
            for row in rows {
                let cols: Vec<&str> = row.0.iter().map(|(c, _)| c.as_str()).collect();
                let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{i}")).collect();
                let quoted: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
                let sql = format!(
                    "INSERT INTO \"{table}\"({}) VALUES ({})",
                    quoted.join(", "),
                    placeholders.join(", ")
                );
                let vals: Vec<rusqlite::types::Value> =
                    row.0.iter().map(|(_, v)| cell_to_sqlite(v)).collect();
                let refs: Vec<&dyn rusqlite::ToSql> =
                    vals.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
                tx.execute(&sql, refs.as_slice())?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    async fn reset_for_load(&self) -> Result<()> {
        blocking_section(move || {
            let mut conn = self.lock();
            let tx = conn.transaction()?;
            // Reverse dependency order, so nothing is orphaned mid-way even though
            // the schema declares no foreign keys to enforce it.
            for table in crate::portable::DUMP_TABLES.iter().rev() {
                tx.execute(&format!("DELETE FROM \"{table}\""), [])?;
            }
            tx.commit()?;
            Ok(())
        })
    }
}

/// A SQLite metadata transaction ([`MetadataStore::begin`]). Holds the shared
/// connection's lock for its whole lifetime (SQLite is single-writer) and runs
/// `BEGIN IMMEDIATE … COMMIT`. Dropped without [`commit`](MetaTxn::commit) — on
/// an error path or a panic — it rolls back, so a half-applied multi-step write
/// never reaches disk.
struct SqliteTxn {
    /// `Some` while the transaction is open; `commit`/`Drop` take it to close
    /// exactly once. An *owned* `Arc` guard so it is `Send` and can be held
    /// across the engine's `.await`s.
    guard: Option<ArcMutexGuard<RawMutex, Connection>>,
    /// The workspace this txn is scoped to (inherited from the store handle).
    workspace_id: i64,
}

impl SqliteTxn {
    #[expect(
        clippy::expect_used,
        reason = "`obj`/`guard` is `Some` for the whole life of the transaction: it is taken only by `commit`/`rollback`, which consume the `Box<Self>`, so no handle survives to observe the `None`. A panic here is a use-after-finish bug in this file, not a runtime condition a caller can hit."
    )]
    fn conn(&self) -> &Connection {
        self.guard.as_deref().expect("transaction already finished")
    }
}

#[async_trait]
impl MetaTxn for SqliteTxn {
    async fn create_inode(&mut self, init: InodeInit) -> Result<Ino> {
        blocking_section(move || {
            let ws = self.workspace_id;
            let conn = self.conn();
            conn.execute(
                "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime, uid, gid)
                 VALUES (?1, ?2, ?3, 1, 0, NULL, ?4, ?4, ?5, ?6)",
                params![
                    ws,
                    init.kind.as_str(),
                    init.mode as i64,
                    now_secs(),
                    init.owner.uid as i64,
                    init.owner.gid as i64
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn set_content(&mut self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        blocking_section(move || {
            let n = self.conn().execute(
                "UPDATE inode SET content_hash = ?1, size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?4",
                params![content.map(|h| h.to_hex()), size as i64, now_secs(), ino],
            )?;
            // Zero rows means the inode is gone. `write_reader` resolves the inode
            // *before* a stream that can run for minutes, so a path unlinked in the
            // meantime lands here — and discarding the count reported that write as
            // durable while the bytes went nowhere. Reachable from `vfs_write` and
            // `write_body` for the same reason.
            if n == 0 {
                return Err(OrigoFSError::NotFound(format!(
                    "inode {ino} was removed before its content could be written"
                )));
            }
            Ok(())
        })
    }

    async fn set_content_if(
        &mut self,
        ino: Ino,
        expected: Option<&Hash>,
        content: Option<Hash>,
        size: u64,
    ) -> Result<bool> {
        blocking_section(move || {
            // `IS` is SQLite's null-safe equality, so this matches a NULL (empty)
            // current content too.
            let n = self.conn().execute(
                "UPDATE inode SET content_hash = ?1, size = ?2, mtime = ?3, ctime = ?3
                 WHERE ino = ?4 AND content_hash IS ?5",
                params![
                    content.map(|h| h.to_hex()),
                    size as i64,
                    now_secs(),
                    ino,
                    expected.map(|h| h.to_hex())
                ],
            )?;
            Ok(n == 1)
        })
    }

    async fn set_nlink(&mut self, ino: Ino, nlink: i64) -> Result<()> {
        blocking_section(move || {
            self.conn().execute(
                "UPDATE inode SET nlink = ?1 WHERE ino = ?2",
                params![nlink, ino],
            )?;
            Ok(())
        })
    }

    async fn adjust_nlink(&mut self, ino: Ino, delta: i64) -> Result<i64> {
        blocking_section(move || {
            Ok(self.conn().query_row(
                "UPDATE inode SET nlink = nlink + ?1 WHERE ino = ?2 RETURNING nlink",
                params![delta, ino],
                |r| r.get::<_, i64>(0),
            )?)
        })
    }

    async fn delete_inode(&mut self, ino: Ino) -> Result<()> {
        blocking_section(move || {
            let conn = self.conn();
            conn.execute("DELETE FROM symlink WHERE ino = ?1", params![ino])?;
            // xattrs are keyed by inode, so they die with it (issue #119).
            conn.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            conn.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
            Ok(())
        })
    }

    async fn delete_inode_if_childless(&mut self, ino: Ino) -> Result<bool> {
        blocking_section(move || {
            let conn = self.conn();
            // Claim the row first, mirroring the Postgres implementation — see
            // there for why the ordering matters. SQLite serializes writers
            // outright, so here it only reports that the inode still exists.
            if conn.execute(
                "UPDATE inode SET nlink = nlink WHERE ino = ?1",
                params![ino],
            )? == 0
            {
                return Ok(false);
            }
            let n = conn.execute(
                "DELETE FROM inode WHERE ino = ?1
                   AND NOT EXISTS (SELECT 1 FROM dentry WHERE parent_ino = ?1)",
                params![ino],
            )?;
            if n == 1 {
                conn.execute("DELETE FROM symlink WHERE ino = ?1", params![ino])?;
                // xattrs are keyed by inode, so they die with it (issue #119).
                conn.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            }
            Ok(n == 1)
        })
    }

    async fn add_dentry(&mut self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
        blocking_section(move || {
            // Claim the parent first. A self-update rather than a read: it both
            // proves the directory still exists *inside* this transaction and
            // takes its row, so a concurrent `rmdir` cannot delete it out from
            // under this insert. See the trait method's docs.
            if self.conn().execute(
                "UPDATE inode SET nlink = nlink WHERE ino = ?1",
                params![parent],
            )? == 0
            {
                return Err(OrigoFSError::NotFound(format!(
                    "parent inode {parent} no longer exists"
                )));
            }
            match self.conn().execute(
                "INSERT INTO dentry(parent_ino, name, ino) VALUES (?1, ?2, ?3)",
                params![parent, name, ino],
            ) {
                Ok(_) => Ok(()),
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    Err(OrigoFSError::AlreadyExists(name.to_string()))
                }
                Err(e) => Err(e.into()),
            }
        })
    }

    async fn remove_dentry(&mut self, parent: Ino, name: &str) -> Result<()> {
        blocking_section(move || {
            self.conn().execute(
                "DELETE FROM dentry WHERE parent_ino = ?1 AND name = ?2",
                params![parent, name],
            )?;
            Ok(())
        })
    }

    async fn set_symlink(&mut self, ino: Ino, target: &str) -> Result<()> {
        blocking_section(move || {
            self.conn().execute(
                "INSERT INTO symlink(ino, target) VALUES (?1, ?2)
                 ON CONFLICT(ino) DO UPDATE SET target = excluded.target",
                params![ino, target],
            )?;
            Ok(())
        })
    }

    async fn set_blob_blame(&mut self, content: &Hash, runs: &str) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn().execute(
                "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, content_hash) DO UPDATE SET runs = excluded.runs",
                params![ws, content.to_hex(), runs],
            )?;
            Ok(())
        })
    }

    async fn append_edit_op(&mut self, op: EditOpInit) -> Result<i64> {
        blocking_section(move || {
            let ws = self.workspace_id;
            let conn = self.conn();
            conn.execute(
                "INSERT INTO edit_op(workspace_id, session_id, actor_id, tool_call_id, ino, path, op, byte_start, byte_len, pre_hash, post_hash, ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    ws, op.session_id, op.actor_id, op.tool_call_id, op.ino, op.path, op.op,
                    op.byte_start, op.byte_len, op.pre_hash, op.post_hash, op.ts
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn set_ref(&mut self, name: &str, value: &str) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn().execute(
                "INSERT INTO ref(workspace_id, name, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, name) DO UPDATE SET value = excluded.value",
                params![ws, name, value],
            )?;
            Ok(())
        })
    }

    async fn cas_ref(&mut self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
        blocking_section(move || {
            let ws = self.workspace_id;
            let conn = self.conn();
            let changed = match expect {
                None => conn.execute(
                    "INSERT INTO ref(workspace_id, name, value) VALUES (?1, ?2, ?3)
                     ON CONFLICT(workspace_id, name) DO NOTHING",
                    params![ws, name, new],
                )?,
                Some(v) => conn.execute(
                    "UPDATE ref SET value = ?1 WHERE workspace_id = ?2 AND name = ?3 AND value = ?4",
                    params![new, ws, name, v],
                )?,
            };
            Ok(changed == 1)
        })
    }

    async fn delete_ref(&mut self, name: &str) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn().execute(
                "DELETE FROM ref WHERE workspace_id = ?1 AND name = ?2",
                params![ws, name],
            )?;
            Ok(())
        })
    }

    async fn set_conflict(&mut self, path: &str, kind: &str) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn().execute(
                "INSERT INTO conflict(workspace_id, path, kind) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, path) DO UPDATE SET kind = excluded.kind",
                params![ws, path, kind],
            )?;
            Ok(())
        })
    }

    async fn clear_conflicts(&mut self) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn()
                .execute("DELETE FROM conflict WHERE workspace_id = ?1", params![ws])?;
            Ok(())
        })
    }

    async fn set_config(&mut self, key: &str, value: &str) -> Result<()> {
        blocking_section(move || {
            let ws = self.workspace_id;
            self.conn().execute(
                "INSERT INTO config(workspace_id, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(workspace_id, key) DO UPDATE SET value = excluded.value",
                params![ws, key, value],
            )?;
            Ok(())
        })
    }

    async fn append_event(&mut self, ev: EventInit, ts: i64) -> Result<i64> {
        blocking_section(move || {
            let ws = self.workspace_id;
            let conn = self.conn();
            conn.execute(
                "INSERT INTO fs_event(workspace_id, actor_id, session_id, kind, path, detail, ts, branch)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![ws, ev.actor_id, ev.session_id, ev.kind, ev.path, ev.detail, ts, ev.branch],
            )?;
            Ok(conn.last_insert_rowid())
        })
    }

    async fn resolve_suggestion(
        &mut self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        blocking_section(move || {
            let ws = self.workspace_id;
            let n = self.conn().execute(
                "UPDATE suggestion SET status = ?1, resolved_by = ?2, resolved_ts = ?3
                 WHERE id = ?4 AND workspace_id = ?5 AND status = 'pending'",
                params![status.as_str(), resolved_by, ts, id, ws],
            )?;
            Ok(n == 1)
        })
    }

    async fn truncate_tree(&mut self) -> Result<()> {
        blocking_section(move || {
            // Same as MetadataStore::truncate_tree, staged in this transaction.
            let ws = self.workspace_id;
            truncate_workspace_tree(self.conn(), ws)?;
            Ok(())
        })
    }

    #[expect(
        clippy::expect_used,
        reason = "`guard` is `Some` until `commit`/`rollback` consume the `Box<Self>`; \
                  a panic here is a use-after-finish bug in this file, not a runtime \
                  condition a caller can reach."
    )]
    async fn commit(mut self: Box<Self>) -> Result<()> {
        blocking_section(move || {
            let guard = self.guard.take().expect("transaction already finished");
            guard.execute_batch("COMMIT")?;
            Ok(())
        })
    }

    #[expect(
        clippy::expect_used,
        reason = "`guard` is `Some` until `commit`/`rollback` consume the `Box<Self>`; \
                  a panic here is a use-after-finish bug in this file, not a runtime \
                  condition a caller can reach."
    )]
    async fn rollback(mut self: Box<Self>) -> Result<()> {
        blocking_section(move || {
            let guard = self.guard.take().expect("transaction already finished");
            guard.execute_batch("ROLLBACK")?;
            // Taking the guard also releases the connection lock here rather than
            // in `Drop`, so a caller that rolls back and immediately re-reads is
            // not waiting on its own dropped transaction.
            Ok(())
        })
    }
}

impl Drop for SqliteTxn {
    fn drop(&mut self) {
        // Roll back unless `commit` already took the guard. Best-effort: if the
        // ROLLBACK itself fails (a dying connection), there is nothing further
        // to do — the transaction never committed, so nothing partial persists.
        if let Some(guard) = self.guard.take() {
            let _ = guard.execute_batch("ROLLBACK");
        }
    }
}

fn row_to_live_doc(r: &rusqlite::Row) -> rusqlite::Result<LiveDoc> {
    Ok(LiveDoc {
        path: r.get(0)?,
        session_id: r.get(1)?,
        actor_id: r.get(2)?,
        content_hash: r.get(3)?,
        since: r.get(4)?,
        checkpointed_at: r.get(5)?,
    })
}

fn row_to_suggestion(r: &rusqlite::Row) -> rusqlite::Result<Suggestion> {
    let status: String = r.get(8)?;
    let kind: String = r.get(12)?;
    Ok(Suggestion {
        id: r.get(0)?,
        actor_id: r.get(1)?,
        session_id: r.get(2)?,
        branch: r.get(3)?,
        path: r.get(4)?,
        base_hash: r.get(5)?,
        proposed_hash: r.get(6)?,
        summary: r.get(7)?,
        kind: SuggestionKind::parse(&kind).unwrap_or_default(),
        status: SuggestionStatus::parse(&status).unwrap_or(SuggestionStatus::Pending),
        created_ts: r.get(9)?,
        resolved_ts: r.get(10)?,
        resolved_by: r.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FileKind, InodeInit};

    // M4: a panic while another operation holds the lock must not brick the
    // store for the rest of the process. `parking_lot::Mutex` does not poison —
    // the lock is released on unwind — so the store keeps working with no
    // recovery dance (the property C1's owned guard also relies on).
    #[tokio::test]
    async fn a_panic_under_the_lock_does_not_brick_the_store() {
        let store = SqliteMetadataStore::open_in_memory().unwrap();
        store.init().await.unwrap();
        let conn = store.conn.clone();
        let _ = std::thread::spawn(move || {
            let _g = conn.lock();
            panic!("panic while holding the lock");
        })
        .join();
        // The mutex was released on unwind, not poisoned: the store still works.
        assert!(store.get_inode(1).await.unwrap().is_some());
    }

    // H8: `busy_timeout` is configured so a second writer waits instead of
    // failing instantly with SQLITE_BUSY.
    #[tokio::test]
    async fn busy_timeout_is_configured() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteMetadataStore::open(dir.path().join("m.db")).unwrap();
        let timeout: i64 = store
            .lock()
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(timeout, 5000);
    }

    // H9: a re-applied non-idempotent migration (V6's ADD COLUMN) must not brick
    // `init`. Simulate a crash that applied the DDL but not its bookkeeping.
    #[tokio::test]
    async fn init_recovers_from_a_reapplied_add_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init().await.unwrap();
        // Drop V6's bookkeeping so the runner re-applies its `ADD COLUMN`.
        store
            .lock()
            .execute("DELETE FROM schema_meta WHERE version = 6", [])
            .unwrap();

        // Must NOT fail with "duplicate column name: branch".
        store.init().await.unwrap();

        let has_branch: i64 = store
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('fs_event') WHERE name = 'branch'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(has_branch, 1);
        // and normal operations still work
        store
            .create_inode(InodeInit::new(FileKind::File, 0o100644))
            .await
            .unwrap();
    }

    // The multi-workspace migrations (V11 rebuilds ref/config/conflict/file_lock,
    // V13 rebuilds blob_blame; V11/V12 ADD COLUMN the rest) must PRESERVE existing
    // data on a real upgrade of a populated store — every row must survive and land
    // in the `default` workspace (id 1), never be dropped by a bad copy. Simulate a
    // store stopped at schema V10, fill its old-shape tables, then migrate.
    #[tokio::test]
    async fn upgrade_preserves_data_and_backfills_default_workspace() {
        let store = SqliteMetadataStore::open_in_memory().unwrap();
        {
            let conn = store.lock();
            // Bring a fresh DB to schema V10 by hand: apply each ≤V10 migration and
            // record it, exactly as the runner would, but without V11+.
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);",
            )
            .unwrap();
            for m in MIGRATIONS.iter().filter(|m| m.version <= 10) {
                conn.execute_batch(m.sqlite).unwrap();
                conn.execute(
                    "INSERT INTO schema_meta(version, applied_at) VALUES (?1, 0)",
                    params![m.version],
                )
                .unwrap();
            }
            // Old-shape rows (no `workspace_id` column yet). Includes the root inode
            // a real V10 store would already have, plus one of each table the
            // migrations touch — especially the ones V11/V13 rebuild.
            conn.execute_batch(
                "INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
                     VALUES (1, 'dir', 16877, 1, 0, NULL, 0, 0);
                 INSERT INTO inode(ino, kind, mode, nlink, size, content_hash, mtime, ctime)
                     VALUES (2, 'file', 33188, 1, 3, 'hash-x', 7, 7);
                 INSERT INTO ref(name, value) VALUES ('refs/heads/main', 'commit-abc');
                 INSERT INTO config(key, value) VALUES ('versioning', 'native');
                 INSERT INTO conflict(path, kind) VALUES ('/c.txt', 'text');
                 INSERT INTO file_lock(path, owner, acquired_at) VALUES ('/l.bin', 'bob', 42);
                 INSERT INTO blob_blame(content_hash, runs) VALUES ('hash-x', 'blame-runs');
                 INSERT INTO suggestion(actor_id, path, status, created_ts)
                     VALUES (5, '/s.txt', 'pending', 9);",
            )
            .unwrap();
        }
        assert_eq!(store.schema_version().await.unwrap(), 10);

        // Run the remaining migrations (V11–V13).
        store.init().await.unwrap();
        assert_eq!(
            store.schema_version().await.unwrap(),
            crate::latest_schema_version()
        );

        // Every row survived, now tagged into the default workspace (id 1).
        let conn = store.lock();
        let row = |sql: &str| -> (i64, String) {
            conn.query_row(sql, [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
        };
        // V11 rebuilds.
        assert_eq!(
            row("SELECT workspace_id, value FROM ref WHERE name='refs/heads/main'"),
            (1, "commit-abc".into())
        );
        assert_eq!(
            row("SELECT workspace_id, value FROM config WHERE key='versioning'"),
            (1, "native".into())
        );
        assert_eq!(
            row("SELECT workspace_id, kind FROM conflict WHERE path='/c.txt'"),
            (1, "text".into())
        );
        assert_eq!(
            row("SELECT workspace_id, owner FROM file_lock WHERE path='/l.bin'"),
            (1, "bob".into())
        );
        // V13 rebuild.
        assert_eq!(
            row("SELECT workspace_id, runs FROM blob_blame WHERE content_hash='hash-x'"),
            (1, "blame-runs".into())
        );
        // ADD-COLUMN tables backfill to 1 too.
        let ws_of = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(ws_of("SELECT workspace_id FROM inode WHERE ino=2"), 1);
        assert_eq!(
            ws_of("SELECT workspace_id FROM suggestion WHERE path='/s.txt'"),
            1
        );
        // And the default workspace registry row now exists.
        let wname: String = conn
            .query_row("SELECT name FROM workspace WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(wname, "default");
    }

    /// `blocking_section` must be callable from every context the store can be
    /// reached from. `block_in_place` panics on a `current_thread` runtime, and
    /// the flavor check is the only thing standing between that and a metadata
    /// call that aborts the process — so pin the contexts rather than trusting
    /// the guard to keep matching tokio's behaviour.
    #[test]
    fn blocking_section_is_safe_in_every_runtime_context() {
        let store = || SqliteMetadataStore::open_in_memory().unwrap();

        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        mt.block_on(async {
            let s = store();
            s.schema_version().await.unwrap();
            // Nested: a caller that already parked itself on the blocking pool.
            tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async { s.schema_version().await })
            })
            .await
            .unwrap()
            .unwrap();
        });

        let ct = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        ct.block_on(async { store().schema_version().await.unwrap() });

        // And with no runtime at all — an embedder driving the future by hand.
        futures::executor::block_on(async { store().schema_version().await.unwrap() });
    }
}
