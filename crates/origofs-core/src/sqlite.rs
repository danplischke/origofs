//! A SQLite-backed [`MetadataStore`] — the M0 default and the SQLite half of the
//! pluggable-backend story (`docs/DESIGN.md` §4b).
//!
//! rusqlite is synchronous, so DB work runs under a mutex inside each `async`
//! method. Because no `.await` occurs while the guard is held, the futures stay
//! `Send`. A production build would move DB work onto `spawn_blocking` (or use an
//! async driver like sqlx, which M2 introduces alongside Postgres).

use crate::attribution::{
    Actor, ActorInit, ActorKind, EditOp, EditOpInit, ToolCallInit, WritePolicy,
};
use crate::collab::{Event, EventInit, LiveDoc, Presence};
use crate::error::{OrigoFSError, Result};
use crate::metadata::{MetaTxn, MetadataStore};
use crate::migrations::MIGRATIONS;
use crate::suggest::{Suggestion, SuggestionInit, SuggestionKind, SuggestionStatus};
use crate::types::{DirEntry, FileKind, Hash, INO_ROOT, Ino, Inode, InodeInit};
use crate::util::now_secs;
use async_trait::async_trait;
use parking_lot::{ArcMutexGuard, Mutex, MutexGuard, RawMutex};
use rusqlite::{Connection, OptionalExtension, params};
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

/// A metadata store backed by a single SQLite database.
pub struct SqliteMetadataStore {
    conn: Arc<Mutex<Connection>>,
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
        let conn = Connection::open(path)?;
        // `busy_timeout` so a second process/writer waits for the lock instead of
        // failing instantly with `SQLITE_BUSY` ("database is locked").
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            workspace_id: DEFAULT_WORKSPACE,
        })
    }

    /// Open a private in-memory database (handy for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            workspace_id: DEFAULT_WORKSPACE,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // `parking_lot::Mutex` does not poison: a panic while another operation
        // holds the lock simply releases it on unwind, so a single panicking
        // statement can't brick every subsequent metadata call for the life of
        // the process (M4). This also gives the owned, `Send` guard that a
        // [`SqliteTxn`] holds across `.await`s (C1).
        self.conn.lock()
    }
}

/// Build an [`Inode`] from a raw row tuple.
#[allow(clippy::type_complexity)]
fn build_inode(row: (i64, String, i64, i64, i64, Option<String>, i64, i64)) -> Result<Inode> {
    let (ino, kind, mode, nlink, size, content_hash, mtime, ctime) = row;
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
    })
}

/// True if a DDL error is SQLite's "duplicate column name" — i.e. an
/// `ADD COLUMN` migration re-applied to a table that already has the column.
fn is_duplicate_column(e: &rusqlite::Error) -> bool {
    e.to_string().contains("duplicate column name")
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
    conn.execute(
        "DELETE FROM inode WHERE workspace_id = ?1
           AND ino <> (SELECT root_ino FROM workspace WHERE id = ?1)",
        params![ws],
    )?;
    Ok(())
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn init(&self) -> Result<()> {
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
            match tx.execute_batch(m.sqlite) {
                Ok(()) => {}
                // Idempotency for a re-applied `ADD COLUMN` (SQLite lacks
                // `IF NOT EXISTS`): the column is already present, so the schema
                // is correct — record the version and continue.
                Err(e) if is_duplicate_column(&e) => {}
                Err(e) => return Err(e.into()),
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
    }

    async fn schema_version(&self) -> Result<i64> {
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
    }

    async fn begin(&self) -> Result<Box<dyn MetaTxn>> {
        // Hold the connection lock for the whole transaction — SQLite is
        // single-writer, so this both serializes writers and lets the txn issue
        // its statements without another operation interleaving on the shared
        // connection. `lock_arc` yields an owned, `Send` guard we can move into
        // the returned box and hold across `.await`s.
        let guard = self.conn.lock_arc();
        // `BEGIN IMMEDIATE` takes the write lock now rather than lazily on the
        // first write, so a second writer waits (up to `busy_timeout`) instead
        // of failing partway through.
        guard.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Box::new(SqliteTxn {
            guard: Some(guard),
            workspace_id: self.workspace_id,
        }))
    }

    async fn get_inode(&self, ino: Ino) -> Result<Option<Inode>> {
        let conn = self.lock();
        let row = conn
            .query_row(
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime
                 FROM inode WHERE ino = ?1",
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
                    ))
                },
            )
            .optional()?;
        match row {
            Some(t) => Ok(Some(build_inode(t)?)),
            None => Ok(None),
        }
    }

    async fn get_inodes(&self, inos: &[Ino]) -> Result<Vec<Inode>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.lock();
        let mut out = Vec::with_capacity(inos.len());
        // SQLite caps bound parameters per statement (999 on older builds), so the
        // IN-list is chunked rather than assuming the caller kept it small.
        for chunk in inos.chunks(INODE_BATCH) {
            let placeholders = (1..=chunk.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt = conn.prepare(&format!(
                "SELECT ino, kind, mode, nlink, size, content_hash, mtime, ctime
                 FROM inode WHERE ino IN ({placeholders})"
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
                ))
            })?;
            for row in rows {
                out.push(build_inode(row?)?);
            }
        }
        Ok(out)
    }

    async fn create_inode(&self, init: InodeInit) -> Result<Ino> {
        let conn = self.lock();
        let now = now_secs();
        conn.execute(
            "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
             VALUES (?1, ?2, ?3, 1, 0, NULL, ?4, ?4)",
            params![self.workspace_id, init.kind.as_str(), init.mode as i64, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    async fn set_content(&self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE inode SET content_hash = ?1, size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?4",
            params![content.map(|h| h.to_hex()), size as i64, now_secs(), ino],
        )?;
        Ok(())
    }

    async fn set_nlink(&self, ino: Ino, nlink: i64) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE inode SET nlink = ?1 WHERE ino = ?2",
            params![nlink, ino],
        )?;
        Ok(())
    }

    async fn delete_inode(&self, ino: Ino) -> Result<()> {
        let conn = self.lock();
        conn.execute("DELETE FROM symlink WHERE ino = ?1", params![ino])?;
        conn.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
        Ok(())
    }

    async fn lookup(&self, parent: Ino, name: &str) -> Result<Option<Ino>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT ino FROM dentry WHERE parent_ino = ?1 AND name = ?2",
            params![parent, name],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn add_dentry(&self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
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
    }

    async fn remove_dentry(&self, parent: Ino, name: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM dentry WHERE parent_ino = ?1 AND name = ?2",
            params![parent, name],
        )?;
        Ok(())
    }

    async fn list_dir(&self, parent: Ino) -> Result<Vec<DirEntry>> {
        let conn = self.lock();
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
            let kind = FileKind::parse(&kind)
                .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind:?}")))?;
            out.push(DirEntry { name, ino, kind });
        }
        Ok(out)
    }

    async fn list_dir_page(
        &self,
        parent: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        let conn = self.lock();
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
            let kind = FileKind::parse(&kind)
                .ok_or_else(|| OrigoFSError::Metadata(format!("unknown inode kind {kind:?}")))?;
            out.push(DirEntry { name, ino, kind });
        }
        Ok(out)
    }

    async fn dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT name FROM dentry WHERE parent_ino = ?1 AND ino = ?2
             ORDER BY name LIMIT 1",
            params![parent, ino],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn child_count(&self, parent: Ino) -> Result<usize> {
        let conn = self.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dentry WHERE parent_ino = ?1",
            params![parent],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    async fn set_symlink(&self, ino: Ino, target: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO symlink(ino, target) VALUES (?1, ?2)
             ON CONFLICT(ino) DO UPDATE SET target = excluded.target",
            params![ino, target],
        )?;
        Ok(())
    }

    async fn get_symlink(&self, ino: Ino) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT target FROM symlink WHERE ino = ?1",
            params![ino],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn get_ref(&self, name: &str) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT value FROM ref WHERE workspace_id = ?1 AND name = ?2",
            params![self.workspace_id, name],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn set_ref(&self, name: &str, value: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO ref(workspace_id, name, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id, name) DO UPDATE SET value = excluded.value",
            params![self.workspace_id, name, value],
        )?;
        Ok(())
    }

    async fn cas_ref(&self, name: &str, expect: Option<&str>, new: &str) -> Result<bool> {
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
    }

    async fn delete_ref(&self, name: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM ref WHERE workspace_id = ?1 AND name = ?2",
            params![self.workspace_id, name],
        )?;
        Ok(())
    }

    async fn list_refs(&self) -> Result<Vec<(String, String)>> {
        let conn = self.lock();
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
    }

    async fn get_config(&self, key: &str) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT value FROM config WHERE workspace_id = ?1 AND key = ?2",
            params![self.workspace_id, key],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn set_config(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO config(workspace_id, key, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id, key) DO UPDATE SET value = excluded.value",
            params![self.workspace_id, key, value],
        )?;
        Ok(())
    }

    async fn bump_counter(&self, key: &str) -> Result<i64> {
        let conn = self.lock();
        // One atomic upsert: create at 1, else increment the stored integer.
        let v: i64 = conn.query_row(
            "INSERT INTO config(workspace_id, key, value) VALUES (?1, ?2, '1')
             ON CONFLICT(workspace_id, key) DO UPDATE SET value = CAST(value AS INTEGER) + 1
             RETURNING CAST(value AS INTEGER)",
            params![self.workspace_id, key],
            |r| r.get(0),
        )?;
        Ok(v)
    }

    fn with_workspace(&self, workspace_id: i64) -> Arc<dyn MetadataStore> {
        Arc::new(SqliteMetadataStore {
            conn: self.conn.clone(),
            workspace_id,
        })
    }

    async fn create_workspace(&self, name: &str) -> Result<(i64, Ino)> {
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
    }

    async fn lookup_workspace(&self, name: &str) -> Result<Option<(i64, Ino)>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT id, root_ino FROM workspace WHERE name = ?1",
            params![name],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn list_workspaces(&self) -> Result<Vec<(i64, String, Ino)>> {
        let conn = self.lock();
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
    }

    async fn truncate_tree(&self) -> Result<()> {
        let conn = self.lock();
        truncate_workspace_tree(&conn, self.workspace_id)?;
        Ok(())
    }

    async fn set_conflict(&self, path: &str, kind: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO conflict(workspace_id, path, kind) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id, path) DO UPDATE SET kind = excluded.kind",
            params![self.workspace_id, path, kind],
        )?;
        Ok(())
    }

    async fn list_conflicts(&self) -> Result<Vec<(String, String)>> {
        let conn = self.lock();
        let mut stmt =
            conn.prepare("SELECT path, kind FROM conflict WHERE workspace_id = ?1 ORDER BY path")?;
        let rows = stmt.query_map(params![self.workspace_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    async fn clear_conflicts(&self) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM conflict WHERE workspace_id = ?1",
            params![self.workspace_id],
        )?;
        Ok(())
    }

    async fn acquire_lock(&self, path: &str, owner: &str, at: i64) -> Result<bool> {
        let conn = self.lock();
        let changed = conn.execute(
            "INSERT INTO file_lock(workspace_id, path, owner, acquired_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(workspace_id, path) DO NOTHING",
            params![self.workspace_id, path, owner, at],
        )?;
        Ok(changed == 1)
    }

    async fn release_lock(&self, path: &str, owner: &str) -> Result<bool> {
        let conn = self.lock();
        let changed = conn.execute(
            "DELETE FROM file_lock WHERE workspace_id = ?1 AND path = ?2 AND owner = ?3",
            params![self.workspace_id, path, owner],
        )?;
        Ok(changed == 1)
    }

    async fn list_locks(&self) -> Result<Vec<(String, String, i64)>> {
        let conn = self.lock();
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
    }

    async fn create_actor(&self, init: ActorInit) -> Result<i64> {
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
    }

    async fn get_actor(&self, id: i64) -> Result<Option<Actor>> {
        let conn = self.lock();
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
                let kind = ActorKind::parse(&kind)
                    .ok_or_else(|| OrigoFSError::Metadata(format!("bad actor kind {kind:?}")))?;
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
    }

    async fn set_write_policy(&self, actor_id: i64, policy: WritePolicy) -> Result<()> {
        let conn = self.lock();
        let n = conn.execute(
            "UPDATE actor SET write_policy = ?1 WHERE id = ?2",
            params![policy.as_i64(), actor_id],
        )?;
        if n == 0 {
            return Err(OrigoFSError::NotFound(format!("actor #{actor_id}")));
        }
        Ok(())
    }

    async fn actor_by_subject(&self, subject: &str) -> Result<Option<Actor>> {
        // Resolve the id under the lock, then reuse get_actor for the row mapping.
        let id: Option<i64> = {
            let conn = self.lock();
            conn.query_row(
                "SELECT id FROM actor WHERE auth_subject = ?1",
                params![subject],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
        };
        match id {
            Some(id) => self.get_actor(id).await,
            None => Ok(None),
        }
    }

    async fn list_actors(&self) -> Result<Vec<Actor>> {
        let conn = self.lock();
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
    }

    async fn create_session(
        &self,
        actor_id: i64,
        client: Option<&str>,
        started_at: i64,
    ) -> Result<i64> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO session(actor_id, client, started_at, ended_at) VALUES (?1, ?2, ?3, NULL)",
            params![actor_id, client, started_at],
        )?;
        Ok(conn.last_insert_rowid())
    }

    async fn record_tool_call(&self, tc: ToolCallInit) -> Result<i64> {
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
    }

    async fn append_edit_op(&self, op: EditOpInit) -> Result<i64> {
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
    }

    async fn list_edit_ops(&self, actor_id: i64, session_id: Option<i64>) -> Result<Vec<EditOp>> {
        let conn = self.lock();
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
    }

    async fn set_blob_blame(&self, content: &Hash, runs: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id, content_hash) DO UPDATE SET runs = excluded.runs",
            params![self.workspace_id, content.to_hex(), runs],
        )?;
        Ok(())
    }

    async fn get_blob_blame(&self, content: &Hash) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT runs FROM blob_blame WHERE workspace_id = ?1 AND content_hash = ?2",
            params![self.workspace_id, content.to_hex()],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .map_err(Into::into)
    }

    async fn append_event(&self, ev: EventInit, ts: i64) -> Result<i64> {
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
    }

    async fn events_since(&self, after_seq: i64, limit: i64) -> Result<Vec<Event>> {
        let conn = self.lock();
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
    }

    async fn touch_presence(
        &self,
        session_id: i64,
        actor_id: i64,
        path: Option<&str>,
        at: i64,
    ) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO presence(session_id, workspace_id, actor_id, path, last_seen) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id) DO UPDATE SET
                 workspace_id = excluded.workspace_id, actor_id = excluded.actor_id,
                 path = excluded.path, last_seen = excluded.last_seen",
            params![session_id, self.workspace_id, actor_id, path, at],
        )?;
        Ok(())
    }

    async fn active_presence(&self, since_ts: i64) -> Result<Vec<Presence>> {
        let conn = self.lock();
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
    }

    async fn reap_presence(&self, older_than: i64) -> Result<u64> {
        let conn = self.lock();
        // Scoped to this workspace: a store-wide reap would evict other workspaces'
        // presence (including live sessions) whenever one workspace uses a shorter
        // cutoff. `touch_presence`/`active_presence` are both workspace-scoped too.
        let n = conn.execute(
            "DELETE FROM presence WHERE workspace_id = ?1 AND last_seen < ?2",
            params![self.workspace_id, older_than],
        )?;
        Ok(n as u64)
    }

    async fn set_live_doc(
        &self,
        path: &str,
        session_id: Option<i64>,
        actor_id: i64,
        content_hash: Option<&str>,
        at: i64,
    ) -> Result<()> {
        let conn = self.lock();
        // `since` is deliberately not in the DO UPDATE list: re-marking an
        // already-live path (a second joiner, a checkpoint) keeps when it first
        // went live.
        conn.execute(
            "INSERT INTO live_doc(workspace_id, path, session_id, actor_id, content_hash, since)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(workspace_id, path) DO UPDATE SET
                 session_id = excluded.session_id,
                 actor_id = excluded.actor_id,
                 content_hash = excluded.content_hash",
            params![
                self.workspace_id,
                path,
                session_id,
                actor_id,
                content_hash,
                at
            ],
        )?;
        Ok(())
    }

    async fn get_live_doc(&self, path: &str) -> Result<Option<LiveDoc>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT path, session_id, actor_id, content_hash, since
             FROM live_doc WHERE workspace_id = ?1 AND path = ?2",
            params![self.workspace_id, path],
            row_to_live_doc,
        )
        .optional()
        .map_err(Into::into)
    }

    async fn list_live_docs(&self) -> Result<Vec<LiveDoc>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT path, session_id, actor_id, content_hash, since
             FROM live_doc WHERE workspace_id = ?1 ORDER BY path",
        )?;
        let rows = stmt.query_map(params![self.workspace_id], row_to_live_doc)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    async fn clear_live_doc(&self, path: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "DELETE FROM live_doc WHERE workspace_id = ?1 AND path = ?2",
            params![self.workspace_id, path],
        )?;
        Ok(())
    }

    async fn create_suggestion(&self, init: SuggestionInit, ts: i64) -> Result<i64> {
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
    }

    async fn get_suggestion(&self, id: i64) -> Result<Option<Suggestion>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT id, actor_id, session_id, branch, path, base_hash, proposed_hash,
                 summary, status, created_ts, resolved_ts, resolved_by, kind
             FROM suggestion WHERE id = ?1 AND workspace_id = ?2",
            params![id, self.workspace_id],
            row_to_suggestion,
        )
        .optional()
        .map_err(Into::into)
    }

    async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        path: Option<&str>,
    ) -> Result<Vec<Suggestion>> {
        let conn = self.lock();
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
    }

    async fn resolve_suggestion(
        &self,
        id: i64,
        status: SuggestionStatus,
        resolved_by: Option<i64>,
        ts: i64,
    ) -> Result<bool> {
        let conn = self.lock();
        let n = conn.execute(
            "UPDATE suggestion SET status = ?1, resolved_by = ?2, resolved_ts = ?3
             WHERE id = ?4 AND workspace_id = ?5 AND status = 'pending'",
            params![status.as_str(), resolved_by, ts, id, self.workspace_id],
        )?;
        Ok(n == 1)
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
    fn conn(&self) -> &Connection {
        // Present until commit consumes the txn; callers never touch it after.
        self.guard.as_deref().expect("transaction already finished")
    }
}

#[async_trait]
impl MetaTxn for SqliteTxn {
    async fn create_inode(&mut self, init: InodeInit) -> Result<Ino> {
        let ws = self.workspace_id;
        let conn = self.conn();
        conn.execute(
            "INSERT INTO inode(workspace_id, kind, mode, nlink, size, content_hash, mtime, ctime)
             VALUES (?1, ?2, ?3, 1, 0, NULL, ?4, ?4)",
            params![ws, init.kind.as_str(), init.mode as i64, now_secs()],
        )?;
        Ok(conn.last_insert_rowid())
    }

    async fn set_content(&mut self, ino: Ino, content: Option<Hash>, size: u64) -> Result<()> {
        self.conn().execute(
            "UPDATE inode SET content_hash = ?1, size = ?2, mtime = ?3, ctime = ?3 WHERE ino = ?4",
            params![content.map(|h| h.to_hex()), size as i64, now_secs(), ino],
        )?;
        Ok(())
    }

    async fn set_content_if(
        &mut self,
        ino: Ino,
        expected: Option<&Hash>,
        content: Option<Hash>,
        size: u64,
    ) -> Result<bool> {
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
    }

    async fn set_nlink(&mut self, ino: Ino, nlink: i64) -> Result<()> {
        self.conn().execute(
            "UPDATE inode SET nlink = ?1 WHERE ino = ?2",
            params![nlink, ino],
        )?;
        Ok(())
    }

    async fn delete_inode(&mut self, ino: Ino) -> Result<()> {
        let conn = self.conn();
        conn.execute("DELETE FROM symlink WHERE ino = ?1", params![ino])?;
        conn.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
        Ok(())
    }

    async fn add_dentry(&mut self, parent: Ino, name: &str, ino: Ino) -> Result<()> {
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
    }

    async fn remove_dentry(&mut self, parent: Ino, name: &str) -> Result<()> {
        self.conn().execute(
            "DELETE FROM dentry WHERE parent_ino = ?1 AND name = ?2",
            params![parent, name],
        )?;
        Ok(())
    }

    async fn set_symlink(&mut self, ino: Ino, target: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO symlink(ino, target) VALUES (?1, ?2)
             ON CONFLICT(ino) DO UPDATE SET target = excluded.target",
            params![ino, target],
        )?;
        Ok(())
    }

    async fn set_blob_blame(&mut self, content: &Hash, runs: &str) -> Result<()> {
        let ws = self.workspace_id;
        self.conn().execute(
            "INSERT INTO blob_blame(workspace_id, content_hash, runs) VALUES (?1, ?2, ?3)
             ON CONFLICT(workspace_id, content_hash) DO UPDATE SET runs = excluded.runs",
            params![ws, content.to_hex(), runs],
        )?;
        Ok(())
    }

    async fn append_edit_op(&mut self, op: EditOpInit) -> Result<i64> {
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
    }

    async fn truncate_tree(&mut self) -> Result<()> {
        // Same as MetadataStore::truncate_tree, staged in this transaction.
        let ws = self.workspace_id;
        truncate_workspace_tree(self.conn(), ws)?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let guard = self.guard.take().expect("transaction already finished");
        guard.execute_batch("COMMIT")?;
        Ok(())
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
            .create_inode(InodeInit {
                kind: FileKind::File,
                mode: 0o100644,
            })
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
}
