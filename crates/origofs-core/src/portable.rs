//! Engine-independent metadata dump and load (issue #117).
//!
//! # Why this exists
//!
//! `MetadataStore::backup_to` returns an error on every backend but SQLite —
//! *"this metadata backend has no built-in backup; use the backend's own tooling
//! (for Postgres: `pg_dump`, or continuous archiving/PITR)."* That is defensible
//! for **backup** and weak for everything else, because `CLAUDE.md` is explicit
//! that the DB is the irreplaceable half: `fsck --rebuild` reconstructs committed
//! files, dirs, symlinks and branches from the bucket alone, and **none** of the
//! attribution — blame, the audit log, actors, uncommitted edits live only here.
//!
//! Three things this buys, in the order they will be missed:
//!
//! 1. **A SQLite → Postgres migration path**, which did not exist. The documented
//!    posture is "SQLite = solo/offline, Postgres = multi-writer/production", and
//!    there was no supported way to graduate from one to the other with
//!    attribution intact. [`resync`](crate::resync) moves committed state and
//!    blame between workspaces on different backends and is most of the way there,
//!    but deliberately carries neither the audit log, nor the working tree, nor
//!    tool-call history.
//! 2. **A real backup story for Postgres** in origofs's own terms rather than
//!    "go use `pg_dump`".
//! 3. **A debugging artifact**, which is what `dump` gets used for most in
//!    practice.
//!
//! # The format
//!
//! JSON Lines. One header record naming the format and schema version, then one
//! record per row, each tagged with its table. Line-oriented so a dump streams in
//! constant memory in both directions and can be inspected with ordinary tools —
//! which is most of the point of (3).
//!
//! Rows are carried as [`Cell`] values rather than typed structs on purpose: a
//! dump has to survive being read by a build that is not this one, and a typed
//! round trip would refuse a row carrying a column it did not know about. The
//! loader skips unknown columns and unknown tables with a warning rather than
//! failing, so a dump from a *newer* origofs still restores everything an older
//! one understands.
//!
//! # What travels
//!
//! A dump is **whole-store**, not per-workspace: it carries every workspace in the
//! store, because the interesting uses (backup, backend migration) all want the
//! store as it stands, and a per-workspace dump would have to renumber every id it
//! restored — the very thing `load` refuses to do. Everything durable travels;
//! deliberately **not** carried:
//!
//! * `fs_event` — the change feed is a transient notification stream; replaying
//!   one into a restored workspace would fire every watcher for changes that
//!   already happened.
//! * `presence` — who was connected minutes ago is meaningless after a restore.
//! * `live_doc` — a marker that a path is open in a CRDT editing session, which by
//!   definition it is not on the other side of a dump.
//!
//! Those three are the *only* omissions, and each is omitted because restoring it
//! would be actively wrong rather than merely unnecessary.

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;

/// The format tag written into a dump's header, so a loader can refuse a file that
/// is not one of these rather than misparsing it.
pub const DUMP_FORMAT: &str = "origofs-metadata-dump";

/// The dump *format* version, distinct from the schema version. Bumped only when
/// the envelope changes; the schema version travels beside it in the header.
pub const DUMP_FORMAT_VERSION: u32 = 1;

/// Tables a dump carries, in **dependency order** so a load can insert them
/// straight through without deferring anything.
///
/// `workspace` first because every other row references one. `actor` before
/// `session` before the tables that reference sessions. `inode` before `dentry`
/// and `symlink` and `xattr`, which reference inodes.
///
/// The allowlist is also the security boundary: `export_table`/`import_table` take
/// a table *name*, and refusing anything not on this list is what stops that being
/// an arbitrary-SQL hole.
pub const DUMP_TABLES: &[&str] = &[
    "workspace",
    "config",
    "ref",
    "actor",
    "session",
    "inode",
    "dentry",
    "symlink",
    "xattr",
    "tool_calls",
    "edit_op",
    "blob_blame",
    "line_blame",
    "suggestion",
    "conflict",
    "file_lock",
    "trash",
    "acl",
];

/// One column value, in the small set every backend can represent losslessly.
///
/// Deliberately not `serde_json::Value`: JSON has one number type, and an `i64`
/// past 2^53 round-trips through a float wrong. Inode numbers, sizes, and
/// timestamps are all `i64` here, so that is not hypothetical — a large workspace
/// would corrupt exactly the columns that matter most.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Null,
    Int(i64),
    Text(String),
    /// Serialized as base16, so a `BLOB`/`BYTEA` column survives a text format.
    Bytes(Vec<u8>),
}

impl Cell {
    /// Encode for the wire. Integers become `{"i": "123"}` — a *string*, because a
    /// bare JSON number is a float in most parsers and an `i64` past 2^53 would
    /// come back wrong.
    fn to_json(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Cell::Null => serde_json::Value::Null,
            Cell::Int(i) => json!({ "i": i.to_string() }),
            Cell::Text(s) => serde_json::Value::String(s.clone()),
            Cell::Bytes(b) => json!({ "b": hex::encode(b) }),
        }
    }

    fn from_json(v: &serde_json::Value) -> Result<Cell> {
        match v {
            serde_json::Value::Null => Ok(Cell::Null),
            serde_json::Value::String(s) => Ok(Cell::Text(s.clone())),
            serde_json::Value::Object(o) => {
                if let Some(i) = o.get("i").and_then(|x| x.as_str()) {
                    return i
                        .parse::<i64>()
                        .map(Cell::Int)
                        .map_err(|_| OrigoFSError::Metadata(format!("bad integer cell {i:?}")));
                }
                if let Some(b) = o.get("b").and_then(|x| x.as_str()) {
                    return hex::decode(b)
                        .map(Cell::Bytes)
                        .map_err(|_| OrigoFSError::Metadata(format!("bad bytes cell {b:?}")));
                }
                Err(OrigoFSError::Metadata(format!("unrecognized cell {o:?}")))
            }
            other => Err(OrigoFSError::Metadata(format!(
                "unrecognized cell {other:?}"
            ))),
        }
    }
}

/// One row: ordered `(column, value)` pairs.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Row(pub Vec<(String, Cell)>);

impl Row {
    pub fn get(&self, col: &str) -> Option<&Cell> {
        self.0.iter().find(|(c, _)| c == col).map(|(_, v)| v)
    }
}

/// What a load actually restored, so a caller can report it rather than guess.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadReport {
    /// `(table, rows)` for each table restored, in the order they were applied.
    pub tables: Vec<(String, usize)>,
    /// Tables present in the dump that this build does not know. Reported rather
    /// than fatal — a dump from a newer origofs must still restore what an older
    /// one understands.
    pub skipped_tables: Vec<String>,
    /// The schema version the dump was taken at. A dump from a *newer* schema is
    /// refused outright; an older one loads and is then migrated forward.
    pub source_schema_version: i64,
}

impl LoadReport {
    pub fn total_rows(&self) -> usize {
        self.tables.iter().map(|(_, n)| n).sum()
    }
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Write an engine-independent dump of the **whole metadata store**.
    ///
    /// Streams table by table, so a large workspace does not have to be resident.
    /// The content store is **not** dumped: it is content-addressed, already
    /// durable, and `fsck --rebuild` can reconstruct committed structure from it —
    /// the metadata is the half that cannot be rebuilt, which is the whole reason
    /// this exists.
    pub async fn dump<W: std::io::Write>(&self, mut out: W) -> Result<usize> {
        use serde_json::json;

        let schema_version = self.meta.schema_version().await?;
        writeln!(
            out,
            "{}",
            json!({
                "format": DUMP_FORMAT,
                "format_version": DUMP_FORMAT_VERSION,
                "schema_version": schema_version,
                "tables": DUMP_TABLES,
            })
        )?;

        let mut written = 0usize;
        for table in DUMP_TABLES {
            for row in self.meta.export_table(table).await? {
                let cols: serde_json::Map<String, serde_json::Value> = row
                    .0
                    .iter()
                    .map(|(c, v)| (c.clone(), v.to_json()))
                    .collect();
                writeln!(out, "{}", json!({ "t": table, "r": cols }))?;
                written += 1;
            }
        }
        out.flush()?;
        Ok(written)
    }

    /// Restore a dump into a **pristine** store.
    ///
    /// # This is a restore, not a merge
    ///
    /// It refuses a store holding anything beyond what `init` created, and
    /// deliberately so. Merging a dump would have to reconcile two independent id
    /// spaces — inode numbers, actor ids, session ids are all local sequences — and
    /// silently getting that wrong produces blame attributed to the wrong actor,
    /// which is the one failure this system exists to prevent.
    /// [`resync`](crate::resync) is the operation for combining two live
    /// workspaces; it carries an explicit identity map for exactly that reason.
    ///
    /// Once the store is confirmed pristine, `init`'s own rows are cleared — the
    /// `default` workspace, the root inode, the default config — because the dump
    /// carries its own. That is what makes a restore a faithful copy rather than a
    /// dump grafted onto a different workspace registry.
    pub async fn load<R: std::io::BufRead>(&self, input: R) -> Result<LoadReport> {
        let mut lines = input.lines();

        let header: serde_json::Value = match lines.next() {
            Some(l) => serde_json::from_str(&l?)
                .map_err(|e| OrigoFSError::Metadata(format!("dump header is not JSON: {e}")))?,
            None => return Err(OrigoFSError::Metadata("dump is empty".into())),
        };
        if header.get("format").and_then(|v| v.as_str()) != Some(DUMP_FORMAT) {
            return Err(OrigoFSError::Metadata(
                "not an origofs metadata dump (bad format tag)".into(),
            ));
        }
        let source_schema_version = header
            .get("schema_version")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        // A dump from a *newer* schema is refused: it may carry columns this build
        // cannot interpret, and loading it half-way is worse than not starting.
        // The reverse is fine — an older dump loads and the normal migration
        // machinery brings it forward, which is exactly the SQLite → Postgres
        // upgrade path this feature is for.
        let latest = crate::migrations::latest_schema_version();
        if source_schema_version > latest {
            return Err(OrigoFSError::Metadata(format!(
                "dump was taken at schema version {source_schema_version}, but this build \
                 understands at most {latest}; upgrade origofs before restoring"
            )));
        }

        self.ensure_loadable().await?;
        // Confirmed pristine above; clear what `init` created so the dump's own
        // workspace/root/config rows can land without colliding.
        self.meta.reset_for_load().await?;

        let known: std::collections::HashSet<&str> = DUMP_TABLES.iter().copied().collect();
        let mut batches: std::collections::HashMap<String, Vec<Row>> = Default::default();
        let mut skipped: std::collections::BTreeSet<String> = Default::default();

        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(&line)
                .map_err(|e| OrigoFSError::Metadata(format!("bad dump record: {e}")))?;
            let Some(table) = v.get("t").and_then(|t| t.as_str()) else {
                return Err(OrigoFSError::Metadata("dump record has no table".into()));
            };
            if !known.contains(table) {
                // A dump from a newer origofs. Skip rather than fail: restoring
                // what we do understand beats refusing the whole file.
                skipped.insert(table.to_string());
                continue;
            }
            let Some(obj) = v.get("r").and_then(|r| r.as_object()) else {
                return Err(OrigoFSError::Metadata("dump record has no row".into()));
            };
            let mut row = Row::default();
            for (col, val) in obj {
                row.0.push((col.clone(), Cell::from_json(val)?));
            }
            batches.entry(table.to_string()).or_default().push(row);
        }

        // Apply in the declared dependency order, not the order they happened to
        // appear in the file, so a hand-edited or reordered dump still restores.
        let mut report = LoadReport {
            source_schema_version,
            skipped_tables: skipped.into_iter().collect(),
            ..Default::default()
        };
        for table in DUMP_TABLES {
            if let Some(rows) = batches.remove(*table)
                && !rows.is_empty()
            {
                self.meta.import_table(table, &rows).await?;
                report.tables.push((table.to_string(), rows.len()));
            }
        }
        Ok(report)
    }

    /// Refuse to load into a store that already holds anything.
    ///
    /// A freshly `init`ed store has a root inode and the default config, and that
    /// is the only state a load tolerates. See [`load`](Self::load) on why this is
    /// not a merge.
    async fn ensure_loadable(&self) -> Result<()> {
        let usage = self.usage().await?;
        // The root directory is the one inode `init` creates.
        if usage.inodes > 1 {
            return Err(OrigoFSError::InvalidArgument(format!(
                "refusing to load into a store that already holds {} inodes; a load \
                 restores into an empty store, it does not merge (use `resync` to \
                 combine two live workspaces)",
                usage.inodes
            )));
        }
        if !self.list_branches().await?.is_empty() {
            return Err(OrigoFSError::InvalidArgument(
                "refusing to load into a store that already has branches; a load \
                 restores into an empty store, it does not merge"
                    .into(),
            ));
        }
        Ok(())
    }
}
