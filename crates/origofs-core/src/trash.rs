//! Trash: a recoverable delete for uncommitted work (issue #115).
//!
//! # Why this exists
//!
//! A committed file can be read back out of history. An **uncommitted** one could
//! not be recovered at all. GC's grace period looks like it might help and does
//! not: it protects in-flight writes from the sweep, which is a correctness guard
//! for the durability barrier, not a user-facing undo.
//!
//! That gap matters more here than it would for an ordinary filesystem, because
//! the users are agents. An agent that shells out to `rm -rf` on a bad path is a
//! routine failure mode rather than an exotic one, and "you should have committed
//! first" is not an answer when the actor that failed to commit is the same one
//! that deleted the tree.
//!
//! # Why it fits origofs's grain
//!
//! A trash entry carries **the actor and session that deleted it**, so a restore
//! is an attributed operation and the deletion itself is already in the op-log
//! beside it. That is something JuiceFS's `.trash` directory cannot express, and
//! it is why this is closer to origofs's existing model than to a port of theirs.
//!
//! # Off by default, and why
//!
//! Retention is per-workspace config and defaults to **disabled**. Turning it on
//! by default would silently change *when space is reclaimed* for every existing
//! deployment: a workspace with churn would suddenly hold a retention window's
//! worth of deleted content, and the first anyone would learn of it is a storage
//! bill or a full disk. Enabling it is one call, and the cost of the opt-in is
//! that it must be chosen — which is the right way round for a change that only
//! ever *adds* retained bytes.
//!
//! # Interaction with GC
//!
//! A trashed body is a **GC root** for as long as its entry is retained
//! (`gc.rs`, root 5). Without that the sweep reclaims the chunks and a restore
//! finds an entry pointing at content that no longer exists. Purging expired
//! entries is folded into the `gc` pass rather than run from a background thread,
//! so there is one maintenance path rather than two schedules to reason about.

use crate::content::ContentStore;
use crate::engine::Fs;
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::{FileKind, Hash, Owner};

/// Config key holding the workspace's trash retention, in seconds.
/// Absent or `0` means trash is disabled and deletes are immediate.
pub(crate) const TRASH_RETENTION: &str = "trash.retention_secs";

/// A convenient retention for a workspace that wants trash on: seven days.
///
/// Long enough to survive a weekend, short enough that the retained bytes are a
/// bounded multiple of the workspace's delete rate.
pub const DEFAULT_TRASH_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

/// One deleted entry, with everything needed to put it back.
#[derive(Clone, Debug)]
pub struct TrashEntry {
    pub id: i64,
    /// The path it was deleted from, and where a restore puts it back.
    pub path: String,
    pub kind: FileKind,
    pub mode: u32,
    pub size: u64,
    /// Manifest address of the body. `None` for a directory, an empty file, or a
    /// symlink.
    pub content: Option<Hash>,
    /// A symlink's target, so restoring one does not need the content store.
    pub symlink_target: Option<String>,
    pub owner: Owner,
    /// Who deleted it. `None` for an unattributed delete (the internal machinery,
    /// or a surface with no actor context such as a mount).
    pub actor_id: Option<i64>,
    pub session_id: Option<i64>,
    pub deleted_at: i64,
}

/// The fields a store needs to record a deletion.
#[derive(Clone, Debug)]
pub struct TrashInit {
    pub path: String,
    pub kind: FileKind,
    pub mode: u32,
    pub size: u64,
    pub content: Option<Hash>,
    pub symlink_target: Option<String>,
    pub owner: Owner,
    pub actor_id: Option<i64>,
    pub session_id: Option<i64>,
    pub deleted_at: i64,
}

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// The workspace's trash retention in seconds; `None` when trash is disabled.
    ///
    /// Disabled is the default — see the module docs on why enabling it by default
    /// would silently change when space is reclaimed for every existing workspace.
    pub async fn trash_retention(&self) -> Result<Option<i64>> {
        Ok(self
            .meta
            .get_config(TRASH_RETENTION)
            .await?
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|n| *n > 0))
    }

    /// Enable trash with `secs` of retention, or disable it with `None`/`0`.
    ///
    /// Disabling does **not** purge what is already there: existing entries stay
    /// restorable until they are purged explicitly. Silently dropping recoverable
    /// data as a side effect of a config change would be the opposite of what this
    /// feature is for.
    pub async fn set_trash_retention(&self, secs: Option<i64>) -> Result<()> {
        let v = secs.filter(|n| *n > 0).unwrap_or(0);
        self.meta.set_config(TRASH_RETENTION, &v.to_string()).await
    }

    /// Everything currently in the trash, newest deletion first.
    pub async fn list_trash(&self) -> Result<Vec<TrashEntry>> {
        self.meta.list_trash().await
    }

    /// Capture `path` into the trash before it is removed, if trash is enabled.
    ///
    /// Returns the new entry's id, or `None` when trash is off — in which case the
    /// caller deletes as it always did.
    ///
    /// Directories are captured as a **single entry for the directory itself**, not
    /// recursively: `rmdir` only ever removes an empty one, and the recursive
    /// deletes in this engine walk children individually, so each child gets its own
    /// entry on the way past. That keeps a restore able to put back exactly one
    /// thing at a time rather than needing to replay a subtree in order.
    pub(crate) async fn trash_capture(
        &self,
        path: &str,
        ctx: Option<crate::WriteCtx>,
    ) -> Result<Option<i64>> {
        if self.trash_retention().await?.is_none() {
            return Ok(None);
        }
        let inode = self.stat(path).await?;
        let symlink_target = match inode.kind {
            FileKind::Symlink => self.meta.get_symlink(inode.ino).await?,
            _ => None,
        };
        let id = self
            .meta
            .push_trash(TrashInit {
                path: path.to_string(),
                kind: inode.kind,
                mode: inode.mode,
                size: inode.size,
                content: inode.content,
                symlink_target,
                owner: inode.owner(),
                actor_id: ctx.map(|c| c.actor),
                session_id: ctx.and_then(|c| c.session),
                deleted_at: self.now_secs(),
            })
            .await?;
        Ok(Some(id))
    }

    /// [`trash_capture`](Self::trash_capture) for the inode-oriented surfaces,
    /// which have `(parent, name)` rather than a path (issue #115).
    ///
    /// The path is reconstructed by walking up from `parent`, because a trash entry
    /// is only useful if it knows where to put the file back — and the mount layer
    /// addresses everything by inode number, so nothing on the call path already
    /// knows it. If the walk fails the delete still proceeds: refusing to delete
    /// because trash could not name the path would be a worse failure than a
    /// missing trash entry.
    pub(crate) async fn trash_capture_inode(
        &self,
        inode: &crate::types::Inode,
        parent: crate::types::Ino,
        name: &str,
        ctx: Option<crate::WriteCtx>,
    ) -> Result<Option<i64>> {
        if self.trash_retention().await?.is_none() {
            return Ok(None);
        }
        let Some(dir) = self.vfs_path_of(parent).await? else {
            return Ok(None);
        };
        let path = if dir == "/" {
            format!("/{name}")
        } else {
            format!("{dir}/{name}")
        };
        let symlink_target = match inode.kind {
            FileKind::Symlink => self.meta.get_symlink(inode.ino).await?,
            _ => None,
        };
        let id = self
            .meta
            .push_trash(TrashInit {
                path,
                kind: inode.kind,
                mode: inode.mode,
                size: inode.size,
                content: inode.content,
                symlink_target,
                owner: inode.owner(),
                actor_id: ctx.map(|c| c.actor),
                session_id: ctx.and_then(|c| c.session),
                deleted_at: self.now_secs(),
            })
            .await?;
        Ok(Some(id))
    }

    /// Put a trashed entry back at its original path and remove it from the trash.
    ///
    /// Refuses if something already occupies the path — restoring over a live file
    /// would trade one lost file for another, which is not a trade an undo gets to
    /// make on the user's behalf.
    ///
    /// The restore is **attributed to the caller**, not to whoever deleted it: this
    /// is a new act by a new actor, and the op-log should say so. The original
    /// deleter is still on the trash entry, which is what makes "who deleted this
    /// and who brought it back" answerable.
    pub async fn restore_trash(&self, id: i64, ctx: crate::WriteCtx) -> Result<String> {
        let entry = self
            .meta
            .get_trash(id)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("trash entry #{id}")))?;

        if self.stat(&entry.path).await.is_ok() {
            return Err(OrigoFSError::AlreadyExists(format!(
                "{} already exists; restoring would overwrite it",
                entry.path
            )));
        }

        match entry.kind {
            FileKind::Dir => {
                self.mkdir_as(ctx, &entry.path).await?;
            }
            FileKind::Symlink => {
                let target = entry.symlink_target.clone().ok_or_else(|| {
                    OrigoFSError::Metadata(format!("trash entry #{id} is a symlink with no target"))
                })?;
                self.symlink_as(ctx, &target, &entry.path).await?;
            }
            FileKind::File => {
                // Read the body back through the content store. The manifest is
                // still there because a retained trash entry is a GC root — that
                // is the whole reason for root 5 in `gc.rs`.
                let body = match entry.content {
                    Some(h) => self.read_body(&h).await?,
                    None => Vec::new(),
                };
                self.write_as(ctx, &entry.path, &body).await?;
            }
        }

        // Restore the mode, which `write_as`/`mkdir_as` set to their defaults.
        if let Ok(restored) = self.stat(&entry.path).await {
            self.meta.set_mode(restored.ino, entry.mode).await?;
            self.meta
                .set_owner(restored.ino, Some(entry.owner.uid), Some(entry.owner.gid))
                .await?;
        }

        self.meta.delete_trash(id).await?;
        Ok(entry.path)
    }

    /// Permanently drop one trash entry. Its content becomes ordinary garbage for
    /// the next `gc` to reclaim.
    pub async fn purge_trash(&self, id: i64) -> Result<bool> {
        self.meta.delete_trash(id).await
    }

    /// Permanently drop every trash entry, whatever its age.
    pub async fn empty_trash(&self) -> Result<usize> {
        let entries = self.meta.list_trash().await?;
        let mut n = 0;
        for e in &entries {
            if self.meta.delete_trash(e.id).await? {
                n += 1;
            }
        }
        Ok(n)
    }
}
