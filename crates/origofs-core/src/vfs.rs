//! Inode-oriented operations for the FUSE/NFS access layer (`docs/DESIGN.md`
//! §4e). FUSE addresses everything by inode number, so these mirror the
//! path-based [`Fs`] methods but take `(parent_ino, name)` / `ino` directly.
//!
//! Reads assemble only the covering chunks; writes are read-modify-write of the
//! whole file for now (correct, but a production build would update chunks
//! incrementally).

use crate::content::ContentStore;
use crate::engine::{Fs, validate_component};
use crate::error::{OrigoFSError, Result};
use crate::metadata::MetadataStore;
use crate::types::{DirEntry, DirEntryAttr, DirPage, FileKind, Ino, Inode, InodeInit, Owner};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use std::collections::HashMap;

const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const SYMLINK_MODE: u32 = 0o120777;

impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// Look up `name` in directory `parent`, returning its inode.
    pub async fn vfs_lookup(&self, parent: Ino, name: &str) -> Result<Option<Inode>> {
        match self.meta.lookup(parent, name).await? {
            Some(ino) => self.meta.get_inode(ino).await,
            None => Ok(None),
        }
    }

    /// Inode attributes.
    pub async fn vfs_getattr(&self, ino: Ino) -> Result<Inode> {
        self.meta
            .get_inode(ino)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(format!("ino {ino}")))
    }

    /// Directory entries — the whole directory, in name order.
    ///
    /// Prefer [`vfs_readdir_page`](Self::vfs_readdir_page) on a mount surface: a
    /// large directory is materialized in full here, once per `readdir` call.
    pub async fn vfs_readdir(&self, ino: Ino) -> Result<Vec<DirEntry>> {
        self.meta.list_dir(ino).await
    }

    /// One keyset page of directory `ino`: up to `limit` entries whose name sorts
    /// strictly after `after_name` (from the start when `None`), in name order.
    ///
    /// A thin pass-through to [`MetadataStore::list_dir_page`], so the store does
    /// the paging as one indexed range scan instead of the surface slicing a full
    /// listing in memory (M16). Resume by passing the last returned name back as
    /// `after_name`. Use this when only names/kinds are needed; use
    /// [`vfs_readdir_page_with_attrs`](Self::vfs_readdir_page_with_attrs) when the
    /// reply also carries attributes.
    pub async fn vfs_readdir_page(
        &self,
        ino: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        self.meta.list_dir_page(ino, after_name, limit).await
    }

    /// One keyset page of directory `ino` with every entry's inode attributes
    /// attached, fetched in a **single batched** inode query (M16).
    ///
    /// This is the N+1-free form of `readdir` + `getattr`: two store round-trips
    /// per page regardless of the page size, versus one `getattr` per entry.
    ///
    /// An entry whose inode disappeared between the two queries (a concurrent
    /// unlink) is dropped from [`DirPage::entries`], but
    /// [`DirPage::next_after`] still advances past it, so a resumed scan can
    /// never stall on it.
    pub async fn vfs_readdir_page_with_attrs(
        &self,
        ino: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<DirPage> {
        let page = self.meta.list_dir_page(ino, after_name, limit).await?;
        let end = page.len() < limit;
        let next_after = page.last().map(|e| e.name.clone());
        let inos: Vec<Ino> = page.iter().map(|e| e.ino).collect();
        let attrs = self.meta.get_inodes(&inos).await?;
        // `get_inodes` returns rows in an unspecified order (and may omit or
        // coalesce inos), so join on the inode number rather than by position.
        let by_ino: HashMap<Ino, Inode> = attrs.into_iter().map(|i| (i.ino, i)).collect();
        let entries = page
            .into_iter()
            .filter_map(|entry| {
                let inode = by_ino.get(&entry.ino)?.clone();
                Some(DirEntryAttr { entry, inode })
            })
            .collect();
        Ok(DirPage {
            entries,
            next_after,
            end,
        })
    }

    /// The name `ino` is linked under in directory `parent`, or `None` if it is
    /// not a child of `parent`.
    ///
    /// The inverse of [`vfs_lookup`](Self::vfs_lookup). A surface whose `readdir`
    /// resume cookie is an inode number (NFSv3) uses this to translate that cookie
    /// into the name cursor [`vfs_readdir_page`](Self::vfs_readdir_page) pages by.
    pub async fn vfs_dentry_name(&self, parent: Ino, ino: Ino) -> Result<Option<String>> {
        self.meta.dentry_name(parent, ino).await
    }

    /// The absolute path of `ino`, or `None` if it is not reachable from the root.
    ///
    /// The mount surfaces address everything by inode number, so nothing on those
    /// call paths already knows a path — but a trash entry is only useful if it
    /// knows where to put the file back (issue #115), and an error message naming
    /// an inode number helps nobody.
    ///
    /// Walks up via `parent_of`/`dentry_name`, both single indexed lookups, so the
    /// cost is the depth rather than the size of the tree. Bounded, so a cycle
    /// already present in the store surfaces as `None` instead of hanging — the
    /// same guard `ensure_not_own_descendant` uses.
    pub async fn vfs_path_of(&self, ino: Ino) -> Result<Option<String>> {
        /// Deeper than any real tree; only a pre-existing cycle reaches it.
        const MAX_DEPTH: usize = 4096;

        if ino == crate::types::INO_ROOT {
            return Ok(Some("/".to_string()));
        }
        let mut parts: Vec<String> = Vec::new();
        let mut cur = ino;
        for _ in 0..MAX_DEPTH {
            let Some(parent) = self.meta.parent_of(cur).await? else {
                // No dentry: unreachable from the root (an orphan, or the root of
                // another workspace).
                return Ok(None);
            };
            let Some(name) = self.meta.dentry_name(parent, cur).await? else {
                return Ok(None);
            };
            parts.push(name);
            if parent == crate::types::INO_ROOT {
                parts.reverse();
                return Ok(Some(format!("/{}", parts.join("/"))));
            }
            cur = parent;
        }
        Ok(None)
    }

    /// Read up to `size` bytes at `offset`, fetching only the covering chunks.
    pub async fn vfs_read(&self, ino: Ino, offset: u64, size: u32) -> Result<Bytes> {
        let inode = self.vfs_getattr(ino).await?;
        let Some(mhash) = inode.content else {
            return Ok(Bytes::new());
        };
        let manifest = self.load_manifest(&mhash).await?;
        let end = offset.saturating_add(size as u64).min(manifest.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let mut buf = BytesMut::with_capacity((end - offset) as usize);
        // Bounded-concurrency, ordered fetch (issue #113). The kernel issues a
        // mount read as many modest requests, each covering a handful of chunks;
        // fetching those serially made every one of them cost a round trip per
        // chunk. `read_range_stream` yields in byte order, so appending as parts
        // arrive still reconstructs the range correctly.
        let mut parts = self.read_range_stream(manifest, offset, end - offset);
        while let Some(part) = parts.next().await {
            buf.extend_from_slice(&part?);
        }
        Ok(buf.freeze())
    }

    /// How many times a `vfs_*` read-modify-write re-reads and retries after
    /// losing a compare-and-set race.
    ///
    /// These paths must produce a single value from `read → modify → write`, and
    /// doing that unconditionally is a lost update: two writes to *different*
    /// offsets of one file would each rewrite the whole body, and the second would
    /// erase the first. That is not an exotic case here — this is the FUSE/NFS
    /// surface, where concurrent writers to one file are the norm.
    ///
    /// So the store is updated conditionally on the content the body was read from
    /// (the same `set_content_if` guard the attributed write path uses), and a lost
    /// race re-reads and reapplies. Retrying rather than returning a conflict is
    /// what the surface requires: a POSIX `write(2)` has no way to tell the kernel
    /// "re-read and try again", so resolving it here is the only place it can
    /// happen. Bounded, so genuine livelock surfaces as an error instead of
    /// spinning forever.
    const VFS_CAS_ATTEMPTS: usize = 16;

    /// Write `data` at `offset` (extending the file as needed). Returns bytes written.
    pub async fn vfs_write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32> {
        for _ in 0..Self::VFS_CAS_ATTEMPTS {
            match self.vfs_write_attempt(ino, offset, data).await? {
                Some(n) => return Ok(n),
                None => continue, // lost the CAS: someone else wrote; re-read and redo
            }
        }
        Err(OrigoFSError::Conflict(format!(
            "ino {ino}: write at offset {offset} lost {} compare-and-set races in a \
             row; the file is under sustained concurrent modification",
            Self::VFS_CAS_ATTEMPTS
        )))
    }

    /// One read-modify-write attempt. `Ok(None)` means the file changed underneath
    /// us and the caller should retry.
    async fn vfs_write_attempt(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<Option<u32>> {
        let inode = self.vfs_getattr(ino).await?;
        let pre = inode.content;
        let mut bytes = match inode.content {
            Some(h) => self.read_body(&h).await?,
            None => Vec::new(),
        };
        // This path rewrites the whole file in memory (read-modify-write), so the
        // only real ceiling is what can actually be allocated — there is no fixed
        // file-size limit. A hostile offset/size (e.g. near u64::MAX) must still
        // fail cleanly rather than overflow or abort the process: reject an
        // overflowing end, one that can't be addressed, or one we can't reserve.
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
            OrigoFSError::TooLarge(format!("write end overflows u64 (ino {ino})"))
        })?;
        let end = usize::try_from(end)
            .map_err(|_| OrigoFSError::TooLarge(format!("write past {end} bytes (ino {ino})")))?;
        if bytes.len() < end {
            let extra = end - bytes.len();
            bytes.try_reserve(extra).map_err(|_| {
                OrigoFSError::TooLarge(format!("cannot allocate {end} bytes (ino {ino})"))
            })?;
            bytes.resize(end, 0);
        }
        bytes[offset as usize..end].copy_from_slice(data);
        // Refuse before storing — see `write_attempt` (issue #116).
        self.check_quota_for_ino(ino, bytes.len() as u64).await?;
        let (mhash, size) = self.store_body(&bytes).await?;
        // Conditional on the version this body was read from, so a concurrent
        // write to another offset is not silently erased. The orphaned chunks a
        // lost race leaves behind are ordinary unreferenced content; gc reclaims
        // them.
        let mut tx = self.meta.begin().await?;
        let won = tx.set_content_if(ino, pre.as_ref(), mhash, size).await?;
        if !won {
            return Ok(None); // dropping `tx` rolls back
        }
        tx.commit().await?;
        Ok(Some(data.len() as u32))
    }

    /// Truncate/extend a file to `size` bytes.
    pub async fn vfs_truncate(&self, ino: Ino, size: u64) -> Result<()> {
        for _ in 0..Self::VFS_CAS_ATTEMPTS {
            if self.vfs_truncate_attempt(ino, size).await? {
                return Ok(());
            }
        }
        Err(OrigoFSError::Conflict(format!(
            "ino {ino}: truncate to {size} lost {} compare-and-set races in a row; \
             the file is under sustained concurrent modification",
            Self::VFS_CAS_ATTEMPTS
        )))
    }

    /// One truncate attempt; `Ok(false)` means retry. See [`Self::vfs_write_attempt`].
    async fn vfs_truncate_attempt(&self, ino: Ino, size: u64) -> Result<bool> {
        let inode = self.vfs_getattr(ino).await?;
        let pre = inode.content;
        // No fixed ceiling: growing a file materializes it in memory, so bound
        // only by what can actually be addressed and allocated — a hostile size
        // (e.g. u64::MAX) fails as TooLarge instead of aborting the process.
        let target = usize::try_from(size)
            .map_err(|_| OrigoFSError::TooLarge(format!("truncate to {size} bytes (ino {ino})")))?;
        let mut bytes = match inode.content {
            Some(h) => self.read_body(&h).await?,
            None => Vec::new(),
        };
        if target > bytes.len() {
            let extra = target - bytes.len();
            bytes.try_reserve(extra).map_err(|_| {
                OrigoFSError::TooLarge(format!("cannot allocate {size} bytes (ino {ino})"))
            })?;
        }
        bytes.resize(target, 0);
        // Only a *growing* truncate can breach a quota, but the check is uniform:
        // `check_quota_for_ino` compares against the current size, so shrinking
        // yields a zero delta and always passes (issue #116).
        self.check_quota_for_ino(ino, bytes.len() as u64).await?;
        let (mhash, sz) = self.store_body(&bytes).await?;
        // See `vfs_write_attempt`: conditional, so a concurrent write is not lost.
        let mut tx = self.meta.begin().await?;
        let won = tx.set_content_if(ino, pre.as_ref(), mhash, sz).await?;
        if !won {
            return Ok(false); // dropping `tx` rolls back
        }
        tx.commit().await?;
        Ok(true)
    }

    /// Create a regular file under `parent`, owned by `owner` (issue #122).
    ///
    /// The mounts pass the uid/gid of the process that issued the `create`, so a
    /// file made through a mount belongs to whoever made it rather than to root.
    /// Pass [`Owner::ROOT`] from anything without a requesting process.
    pub async fn vfs_create(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        owner: Owner,
    ) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        // Inode + dentry commit together, so a failed link can't orphan the
        // inode (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit::owned_by(
                FileKind::File,
                S_IFREG | (mode & 0o7777),
                owner,
            ))
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Create a directory under `parent`, owned by `owner`. See
    /// [`vfs_create`](Self::vfs_create).
    pub async fn vfs_mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        owner: Owner,
    ) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit::owned_by(
                FileKind::Dir,
                S_IFDIR | (mode & 0o7777),
                owner,
            ))
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Remove a file under `parent`.
    ///
    /// Captures into the trash first when the workspace has retention enabled
    /// (issue #115). A mount has no actor context — a deliberate bypass per
    /// `CLAUDE.md` — so the entry records no actor; that is strictly better than
    /// not capturing it, since `rm` through a mount is exactly the failure mode
    /// trash exists for.
    pub async fn vfs_unlink(&self, parent: Ino, name: &str) -> Result<()> {
        let ino = self
            .meta
            .lookup(parent, name)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(name.to_string()))?;
        let inode = self.vfs_getattr(ino).await?;
        if inode.kind == FileKind::Dir {
            return Err(OrigoFSError::IsADirectory(name.to_string()));
        }
        self.trash_capture_inode(&inode, parent, name, None).await?;
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        // The database decrements; see `MetaTxn::adjust_nlink` for why the
        // `nlink` read above must not be turned into an absolute write.
        if tx.adjust_nlink(ino, -1).await? <= 0 {
            tx.delete_inode(ino).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Hard-link the existing inode `ino` as `(newparent, newname)` (issue #119).
    ///
    /// The increment side of `nlink`, which the schema anticipated and nothing ever
    /// built: the column, the type, and the whole decrement path (`vfs_unlink`,
    /// `vfs_rename`, and the two path-API removes) already existed, and
    /// `adjust_nlink` had only ever been called with `-1`. That made this the
    /// cheapest of #119's POSIX holes to close, and it is not an exotic one —
    /// `git` uses hard links, and several editors save via `rename`+`link`.
    ///
    /// Directories are refused (`EPERM`), as POSIX requires: a directory hard link
    /// would let the dentry graph form a cycle no longer reachable from the root,
    /// which nothing here — `gc`, commit, the recursive walks — is written to
    /// survive.
    ///
    /// The dentry and the `nlink` bump commit together, so a failed link cannot
    /// leave a count that no name backs (a leak that would keep the inode alive
    /// forever) or a name whose count was never raised (a premature delete on the
    /// next unlink).
    pub async fn vfs_link(&self, ino: Ino, newparent: Ino, newname: &str) -> Result<Inode> {
        validate_component(newname)?;
        let inode = self.vfs_getattr(ino).await?;
        if inode.kind == FileKind::Dir {
            // POSIX: EPERM, not EISDIR — the operation is forbidden for this type,
            // rather than the caller having named a directory where a file was
            // wanted.
            return Err(OrigoFSError::Denied(format!(
                "hard links to directories are not allowed (ino {ino})"
            )));
        }
        if self.meta.lookup(newparent, newname).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(newname.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        tx.add_dentry(newparent, newname, ino).await?;
        tx.adjust_nlink(ino, 1).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Remove an empty directory under `parent`.
    pub async fn vfs_rmdir(&self, parent: Ino, name: &str) -> Result<()> {
        let ino = self
            .meta
            .lookup(parent, name)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(name.to_string()))?;
        let inode = self.vfs_getattr(ino).await?;
        if inode.kind != FileKind::Dir {
            return Err(OrigoFSError::NotADirectory(name.to_string()));
        }
        // Early answer for the common case; the conditional delete below is the
        // binding check — this read happens before the transaction opens.
        if self.meta.child_count(ino).await? > 0 {
            return Err(OrigoFSError::DirectoryNotEmpty(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        if !tx.delete_inode_if_childless(ino).await? {
            return Err(OrigoFSError::DirectoryNotEmpty(name.to_string()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Rename/move `(parent, name)` to `(newparent, newname)`.
    pub async fn vfs_rename(
        &self,
        parent: Ino,
        name: &str,
        newparent: Ino,
        newname: &str,
    ) -> Result<()> {
        // Validate only the newly-introduced destination name; the source must
        // already exist (so it is already well-formed, and a pre-existing odd
        // entry stays renamable/removable).
        validate_component(newname)?;
        let sino = self
            .meta
            .lookup(parent, name)
            .await?
            .ok_or_else(|| OrigoFSError::NotFound(name.to_string()))?;
        // `mv /mnt/a /mnt/a/b/a2` through a mount is the same self-into-descendant
        // cycle the path API guards, and the inode surface can reach it too.
        self.ensure_not_own_descendant(sino, newparent).await?;
        // Resolve the destination's state before the txn; the mutations below
        // commit together so a crash can't leave the source unlinked with the
        // destination half-replaced (C1).
        let overwrite = match self.meta.lookup(newparent, newname).await? {
            Some(dino) if dino == sino => return Ok(()),
            Some(dino) => {
                let dinode = self.vfs_getattr(dino).await?;
                if dinode.kind == FileKind::Dir && self.meta.child_count(dino).await? > 0 {
                    return Err(OrigoFSError::DirectoryNotEmpty(newname.to_string()));
                }
                Some((dino, dinode))
            }
            None => None,
        };

        let mut tx = self.meta.begin().await?;
        if let Some((dino, dinode)) = overwrite {
            tx.remove_dentry(newparent, newname).await?;
            match dinode.kind {
                // Conditional, for the same reason `rmdir` is: the emptiness
                // check above ran before this transaction opened.
                FileKind::Dir => {
                    if !tx.delete_inode_if_childless(dino).await? {
                        return Err(OrigoFSError::DirectoryNotEmpty(format!("inode {dino}")));
                    }
                }
                _ => {
                    if tx.adjust_nlink(dino, -1).await? <= 0 {
                        tx.delete_inode(dino).await?;
                    }
                }
            }
        }
        tx.remove_dentry(parent, name).await?;
        tx.add_dentry(newparent, newname, sino).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Create a symlink under `parent`, owned by `owner`. See
    /// [`vfs_create`](Self::vfs_create).
    pub async fn vfs_symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        owner: Owner,
    ) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit::owned_by(FileKind::Symlink, SYMLINK_MODE, owner))
            .await?;
        tx.set_symlink(ino, target).await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Change an inode's permission bits (issue #121).
    ///
    /// Only the low 12 bits are the caller's — the format bits (`S_IFREG`/
    /// `S_IFDIR`/`S_IFLNK`) are the inode's kind, and the store masks them in.
    ///
    /// Until this existed, both mounts accepted a mode change and discarded it,
    /// then reported success: FUSE's `setattr` bound `mode` with a leading
    /// underscore and replied with freshly-read (unchanged) attributes, and NFS
    /// documented the no-op in a comment. A script running `chmod +x build.sh` and
    /// checking the return code proceeded on a false premise. Worse, the mode a
    /// file happened to be *created* with was the mode it carried into committed
    /// tree objects (`TreeEntry.mode`) and out through `git clone origofs://…`,
    /// with no way to correct it.
    pub async fn vfs_chmod(&self, ino: Ino, mode: u32) -> Result<Inode> {
        // Confirm it exists first, so a chmod of a missing inode is NotFound
        // rather than a silently-zero-row UPDATE — the same silence this fixes.
        self.vfs_getattr(ino).await?;
        self.meta.set_mode(ino, mode).await?;
        self.vfs_getattr(ino).await
    }

    /// Change an inode's owning uid/gid (issue #122).
    ///
    /// Either half may be `None` to leave it alone — `chown(2)`'s `-1` sentinel,
    /// which is how `chgrp` reaches this and how both mounts forward a request
    /// carrying only one of the two.
    ///
    /// This is ownership, not authorization: it changes what the *kernel* evaluates
    /// its permission checks against on a mount, and nothing about what an actor may
    /// do. See `docs/PERMISSIONS.md` §2.
    pub async fn vfs_chown(&self, ino: Ino, uid: Option<u32>, gid: Option<u32>) -> Result<Inode> {
        self.vfs_getattr(ino).await?;
        self.meta.set_owner(ino, uid, gid).await?;
        self.vfs_getattr(ino).await
    }

    /// Read one extended attribute (issue #119).
    pub async fn vfs_getxattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>> {
        self.vfs_getattr(ino).await?;
        self.meta.get_xattr(ino, name).await
    }

    /// Set one extended attribute (issue #119).
    ///
    /// Refuses a value larger than [`MAX_XATTR_LEN`](crate::MAX_XATTR_LEN). That
    /// bound is not a preference: an xattr lives in the **metadata** store, and the
    /// rule this whole design rests on is that the metadata DB never holds large
    /// bytes. Without a cap here, `setfattr` is an unbounded, un-deduplicated,
    /// un-chunked write straight into the DB — a supported way to do exactly the
    /// thing the metadata/content split exists to prevent. The limit matches
    /// Linux's own per-value ceiling, so nothing that works on ext4 or XFS is
    /// refused here.
    pub async fn vfs_setxattr(&self, ino: Ino, name: &str, value: &[u8]) -> Result<()> {
        if name.is_empty() {
            return Err(OrigoFSError::InvalidArgument(
                "xattr name must not be empty".into(),
            ));
        }
        if value.len() > crate::MAX_XATTR_LEN {
            return Err(OrigoFSError::TooLarge(format!(
                "xattr {name:?} is {} bytes; the limit is {} — extended attributes \
                 live in the metadata store, which never holds large bytes",
                value.len(),
                crate::MAX_XATTR_LEN
            )));
        }
        self.vfs_getattr(ino).await?;
        self.meta.set_xattr(ino, name, value).await
    }

    /// Remove one extended attribute, reporting whether it was there (issue #119).
    ///
    /// The boolean is what lets a mount answer `ENODATA` for a name that was never
    /// set, rather than reporting success for a removal that removed nothing.
    pub async fn vfs_removexattr(&self, ino: Ino, name: &str) -> Result<bool> {
        self.vfs_getattr(ino).await?;
        self.meta.remove_xattr(ino, name).await
    }

    /// Every extended-attribute name on an inode, in name order (issue #119).
    pub async fn vfs_listxattr(&self, ino: Ino) -> Result<Vec<String>> {
        self.vfs_getattr(ino).await?;
        self.meta.list_xattrs(ino).await
    }

    /// Read a symlink target by inode.
    pub async fn vfs_readlink(&self, ino: Ino) -> Result<String> {
        self.meta
            .get_symlink(ino)
            .await?
            .ok_or_else(|| OrigoFSError::InvalidArgument(format!("ino {ino} is not a symlink")))
    }
}
