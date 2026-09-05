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
use crate::posixlock::{LockAnswer, LockKind, LockRequest};
use crate::types::{DirEntry, DirEntryAttr, DirPage, FileKind, Ino, Inode, InodeInit, Owner};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use std::collections::HashMap;

/// The `fallocate(2)` modes this filesystem can honour (issue #119).
///
/// Deliberately not `libc` flags: the engine states what it is being asked to do,
/// and each surface maps its own constants onto this — which is also where an
/// unsupported combination is refused rather than approximated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocateMode {
    /// Extend the file if the range runs past the end.
    Allocate,
    /// `FALLOC_FL_KEEP_SIZE` alone: reserve without changing the size — nothing to
    /// do in a store that does not reserve blocks.
    KeepSize,
    /// `FALLOC_FL_PUNCH_HOLE`: zero the range, size unchanged.
    PunchHole,
    /// `FALLOC_FL_ZERO_RANGE`: zero the range, extending if it runs past the end.
    ZeroRange,
}

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

    /// One attempt. `Ok(None)` means the file changed underneath us and the caller
    /// should retry.
    ///
    /// Since #111 this **splices** the written range into the existing manifest
    /// rather than reading, patching and re-chunking the whole body, so the cost is
    /// `O(bytes written)` rather than `O(file size)` — see
    /// [`Fs::splice_body`](crate::Fs::splice_body) for why splicing rather than the
    /// slice list the issue sketches.
    async fn vfs_write_attempt(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<Option<u32>> {
        let inode = self.vfs_getattr(ino).await?;
        let pre = inode.content;
        // A hostile offset/size (near u64::MAX) must fail cleanly rather than
        // overflow; `splice_body` rejects an overflowing end and a region it
        // cannot allocate. There is still no fixed file-size limit.
        let base = match inode.content {
            Some(h) => self.load_manifest(&h).await?,
            None => crate::chunk::Manifest::default(),
        };
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
            OrigoFSError::TooLarge(format!("write end overflows u64 (ino {ino})"))
        })?;
        // Refuse before storing — see `write_attempt` (issue #116).
        self.check_quota_for_ino(ino, base.size.max(end)).await?;
        let (mhash, size) = self.splice_body(&base, offset, data).await?;
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
        // Since #111 this resizes the manifest rather than materializing the body:
        // shrinking drops whole chunks past the new end and re-chunks only the one
        // straddling it, and growing appends a hole. Truncating a 1 GiB file to
        // zero used to read and re-chunk all of it first.
        let base = match inode.content {
            Some(h) => self.load_manifest(&h).await?,
            None => crate::chunk::Manifest::default(),
        };
        // Only a *growing* truncate can breach a quota, but the check is uniform:
        // `check_quota_for_ino` compares against the current size, so shrinking
        // yields a zero delta and always passes (issue #116).
        self.check_quota_for_ino(ino, size).await?;
        let (mhash, sz) = self.resize_body(&base, size).await?;
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

// --- the ACL-checked inode ops a mount calls (issue #141) --------------------
//
// The ops above take no actor, and for most of their history nothing could give
// them one: a mount had no identity, which `CLAUDE.md` documented as a
// deliberate bypass. That was survivable while the only authorization was a
// per-actor write policy — a mount was all-or-nothing anyway. Path-scoped ACLs
// (#123) made it false containment: an agent refused `WRITE` under `/src` over
// MCP or HTTP took the identical action through a mount, and no check ran.
//
// # Why the checks live here and not in the mount
//
// `CLAUDE.md`'s standing rule — enforce in the engine, never per surface. A
// guard the surface calls is a guard the next surface, or the next op on this
// one, can forget; a guard *inside* the method cannot be forgotten by anything
// that calls the method. What remains is a surface calling the unchecked op
// instead, which is a question about source text rather than about runtime
// behaviour, and `origofs-sdk/tests/mount_acl.rs` answers it the way `tests/mcp.rs`
// answers it for MCP tools.
//
// # `None` is an anonymous mount, and still bypasses
//
// Every one of these takes `Option<WriteCtx>`, not `WriteCtx`. `None` means a
// mount started without an identity, and behaves exactly as the mounts always
// have. That keeps `origofs mount` working unchanged for the single-user case it
// was built for, and it makes the bypass a visible argument at the call site
// rather than an absent one. `Some(ctx)` is a mount bound to an actor: every op
// is checked against the grants covering the path it touches.
//
// # What this does *not* do
//
// It does not attribute. A write through a mount still records no `edit_op` and
// no blame, exactly as before — the ACL question is "may this actor", and the
// attribution question is "what did they change", which for an offset write is
// a different and larger problem than authorizing it. Do not read a mount having
// an actor as its writes being attributed to one.
//
// # Denials still do not leak existence
//
// `#123`'s invariant 4. The `(parent, name)` guards resolve only the *parent*'s
// path and append the name, so no lookup of the target precedes the check —
// a create, unlink or rename is refused identically whether or not the name is
// there. The `ino` guards must resolve the inode to know its path, but an `ino`
// only ever reaches a mount through a `lookup` that this same layer already
// gated, so nothing is revealed that the caller had not already been allowed to
// learn.
impl<M: MetadataStore, C: ContentStore> Fs<M, C> {
    /// The absolute path of `ino`, for a guard that must name it in a check.
    ///
    /// An inode unreachable from the root has no path for a grant to cover, so it
    /// is refused rather than allowed: a prefix ACL cannot express permission for
    /// something outside the tree, and defaulting to "allow" would make an orphan
    /// the way around every grant.
    async fn guard_path_of(&self, ino: Ino) -> Result<String> {
        self.vfs_path_of(ino).await?.ok_or_else(|| {
            OrigoFSError::Denied(format!(
                "ino {ino} is not reachable from the workspace root"
            ))
        })
    }

    /// The path `name` would have inside directory `parent`.
    async fn guard_path_in(&self, parent: Ino, name: &str) -> Result<String> {
        let dir = self.guard_path_of(parent).await?;
        Ok(format!("{}/{name}", dir.trim_end_matches('/')))
    }

    /// Refuse a mount-initiated write at `ino` unless `ctx` may write there.
    async fn guard_write(&self, ctx: Option<crate::WriteCtx>, op: &str, ino: Ino) -> Result<()> {
        let Some(ctx) = ctx else { return Ok(()) };
        let path = self.guard_path_of(ino).await?;
        self.ensure_may_write_at(ctx, op, &path).await
    }

    /// Refuse a mount-initiated write at `parent`/`name` unless `ctx` may write there.
    async fn guard_write_in(
        &self,
        ctx: Option<crate::WriteCtx>,
        op: &str,
        parent: Ino,
        name: &str,
    ) -> Result<()> {
        let Some(ctx) = ctx else { return Ok(()) };
        let path = self.guard_path_in(parent, name).await?;
        self.ensure_may_write_at(ctx, op, &path).await
    }

    /// Refuse a mount-initiated read of `ino` unless `ctx` may read there.
    ///
    /// A no-op unless the workspace has `acl_enforce_reads` on, like every other
    /// read check.
    async fn guard_read(&self, ctx: Option<crate::WriteCtx>, op: &str, ino: Ino) -> Result<()> {
        let Some(ctx) = ctx else { return Ok(()) };
        if !self.acl_enforce_reads().await? {
            return Ok(());
        }
        let path = self.guard_path_of(ino).await?;
        self.ensure_may_read_at(ctx, op, &path).await
    }

    /// Whether `ctx` may read `parent`/`name`, for filtering a listing.
    async fn guard_may_read_in(
        &self,
        ctx: crate::WriteCtx,
        parent_path: &str,
        name: &str,
    ) -> Result<bool> {
        let child = format!("{}/{name}", parent_path.trim_end_matches('/'));
        Ok(self
            .effective_perms(ctx.actor, &child)
            .await?
            .contains(crate::acl::Perms::READ))
    }

    // --- mutations ---------------------------------------------------------

    /// [`vfs_write`](Self::vfs_write), checked.
    pub async fn vfs_write_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        offset: u64,
        data: &[u8],
    ) -> Result<u32> {
        self.guard_write(ctx, "write", ino).await?;
        self.vfs_write(ino, offset, data).await
    }

    /// [`vfs_truncate`](Self::vfs_truncate), checked.
    pub async fn vfs_truncate_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        size: u64,
    ) -> Result<()> {
        self.guard_write(ctx, "truncate", ino).await?;
        self.vfs_truncate(ino, size).await
    }

    /// [`vfs_create`](Self::vfs_create), checked.
    pub async fn vfs_create_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
        mode: u32,
        owner: Owner,
    ) -> Result<Inode> {
        self.guard_write_in(ctx, "create", parent, name).await?;
        self.vfs_create(parent, name, mode, owner).await
    }

    /// [`vfs_mkdir`](Self::vfs_mkdir), checked.
    pub async fn vfs_mkdir_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
        mode: u32,
        owner: Owner,
    ) -> Result<Inode> {
        self.guard_write_in(ctx, "create a directory at", parent, name)
            .await?;
        self.vfs_mkdir(parent, name, mode, owner).await
    }

    /// [`vfs_unlink`](Self::vfs_unlink), checked.
    pub async fn vfs_unlink_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
    ) -> Result<()> {
        self.guard_write_in(ctx, "remove", parent, name).await?;
        self.vfs_unlink(parent, name).await
    }

    /// [`vfs_rmdir`](Self::vfs_rmdir), checked.
    pub async fn vfs_rmdir_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
    ) -> Result<()> {
        self.guard_write_in(ctx, "remove the directory", parent, name)
            .await?;
        self.vfs_rmdir(parent, name).await
    }

    /// [`vfs_link`](Self::vfs_link), checked at the **new** name.
    ///
    /// The destination is where the change lands; the source inode is not
    /// modified, and its content is already reachable to anyone holding the
    /// inode.
    pub async fn vfs_link_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        newparent: Ino,
        newname: &str,
    ) -> Result<Inode> {
        self.guard_write_in(ctx, "hard-link into", newparent, newname)
            .await?;
        self.vfs_link(ino, newparent, newname).await
    }

    /// [`vfs_rename`](Self::vfs_rename), checked at **both** ends.
    ///
    /// Source *and* destination, per `#123`: checking only the source lets an
    /// actor move a file it controls into a tree it does not, and checking only
    /// the destination lets it move one out of a tree it may not touch.
    pub async fn vfs_rename_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
        newparent: Ino,
        newname: &str,
    ) -> Result<()> {
        self.guard_write_in(ctx, "rename out of", parent, name)
            .await?;
        self.guard_write_in(ctx, "rename into", newparent, newname)
            .await?;
        self.vfs_rename(parent, name, newparent, newname).await
    }

    /// [`vfs_symlink`](Self::vfs_symlink), checked.
    pub async fn vfs_symlink_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
        target: &str,
        owner: Owner,
    ) -> Result<Inode> {
        self.guard_write_in(ctx, "create a symlink at", parent, name)
            .await?;
        self.vfs_symlink(parent, name, target, owner).await
    }

    /// [`vfs_chmod`](Self::vfs_chmod), checked.
    pub async fn vfs_chmod_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        mode: u32,
    ) -> Result<Inode> {
        self.guard_write(ctx, "change the mode of", ino).await?;
        self.vfs_chmod(ino, mode).await
    }

    /// [`vfs_chown`](Self::vfs_chown), checked.
    pub async fn vfs_chown_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<Inode> {
        self.guard_write(ctx, "change the owner of", ino).await?;
        self.vfs_chown(ino, uid, gid).await
    }

    /// [`vfs_setxattr`](Self::vfs_setxattr), checked.
    pub async fn vfs_setxattr_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        name: &str,
        value: &[u8],
    ) -> Result<()> {
        self.guard_write(ctx, "set an extended attribute on", ino)
            .await?;
        self.vfs_setxattr(ino, name, value).await
    }

    /// [`vfs_removexattr`](Self::vfs_removexattr), checked.
    pub async fn vfs_removexattr_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        name: &str,
    ) -> Result<bool> {
        self.guard_write(ctx, "remove an extended attribute from", ino)
            .await?;
        self.vfs_removexattr(ino, name).await
    }

    // --- reads -------------------------------------------------------------

    /// [`vfs_lookup`](Self::vfs_lookup), checked at the child.
    ///
    /// Checked at `parent`/`name` rather than at `parent`, so a grant that opens
    /// one child of a directory does not open its siblings.
    pub async fn vfs_lookup_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        parent: Ino,
        name: &str,
    ) -> Result<Option<Inode>> {
        if let Some(ctx) = ctx
            && self.acl_enforce_reads().await?
        {
            let path = self.guard_path_in(parent, name).await?;
            self.ensure_may_read_at(ctx, "look up", &path).await?;
        }
        self.vfs_lookup(parent, name).await
    }

    /// [`vfs_getattr`](Self::vfs_getattr), checked.
    pub async fn vfs_getattr_as(&self, ctx: Option<crate::WriteCtx>, ino: Ino) -> Result<Inode> {
        self.guard_read(ctx, "stat", ino).await?;
        self.vfs_getattr(ino).await
    }

    /// [`vfs_read`](Self::vfs_read), checked.
    pub async fn vfs_read_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        offset: u64,
        size: u32,
    ) -> Result<Bytes> {
        self.guard_read(ctx, "read", ino).await?;
        self.vfs_read(ino, offset, size).await
    }

    /// [`vfs_readlink`](Self::vfs_readlink), checked.
    pub async fn vfs_readlink_as(&self, ctx: Option<crate::WriteCtx>, ino: Ino) -> Result<String> {
        self.guard_read(ctx, "read the symlink", ino).await?;
        self.vfs_readlink(ino).await
    }

    /// [`vfs_getxattr`](Self::vfs_getxattr), checked.
    pub async fn vfs_getxattr_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        name: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.guard_read(ctx, "read an extended attribute of", ino)
            .await?;
        self.vfs_getxattr(ino, name).await
    }

    /// [`vfs_listxattr`](Self::vfs_listxattr), checked.
    pub async fn vfs_listxattr_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
    ) -> Result<Vec<String>> {
        self.guard_read(ctx, "list the extended attributes of", ino)
            .await?;
        self.vfs_listxattr(ino).await
    }

    /// [`vfs_readdir_page`](Self::vfs_readdir_page), checked at the directory and
    /// **filtered per entry** — the same pair of rules
    /// [`ls_as`](Self::ls_as) takes, and for the same reason: a listing that names
    /// an entry `stat` would refuse is the existence oracle the refusal exists to
    /// prevent.
    ///
    /// # Why this pages internally
    ///
    /// The caller resumes from the last name it was handed, so returning a page
    /// that filtered to empty would read as end-of-directory and silently truncate
    /// the listing. Instead this keeps scanning until it has something visible or
    /// the directory is genuinely exhausted. Everything between the last visible
    /// entry and where the scan stopped is invisible and simply gets re-filtered
    /// on the next call — redundant, never wrong, and it always advances.
    pub async fn vfs_readdir_page_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirEntry>> {
        let Some(ctx) = ctx else {
            return self.vfs_readdir_page(ino, after_name, limit).await;
        };
        if !self.acl_enforce_reads().await? {
            return self.vfs_readdir_page(ino, after_name, limit).await;
        }
        let dir = self.guard_path_of(ino).await?;
        self.ensure_may_read_at(ctx, "list", &dir).await?;

        let mut cursor = after_name.map(|s| s.to_string());
        loop {
            let page = self.vfs_readdir_page(ino, cursor.as_deref(), limit).await?;
            let exhausted = page.len() < limit;
            let last = page.last().map(|e| e.name.clone());
            let mut visible = Vec::with_capacity(page.len());
            for entry in page {
                if self.guard_may_read_in(ctx, &dir, &entry.name).await? {
                    visible.push(entry);
                }
            }
            if !visible.is_empty() || exhausted {
                return Ok(visible);
            }
            // The whole page was invisible: keep scanning rather than reporting a
            // premature end. `last` is `Some` whenever the page was non-empty, and
            // an empty page sets `exhausted`, so this always advances.
            cursor = last;
        }
    }

    /// [`vfs_readdir_page_with_attrs`](Self::vfs_readdir_page_with_attrs), checked
    /// and filtered like [`vfs_readdir_page_as`](Self::vfs_readdir_page_as).
    ///
    /// This form carries its own cursor (`next_after`/`end`), so a page that
    /// filters to empty is not mistaken for the end and it needs no internal loop.
    pub async fn vfs_readdir_page_with_attrs_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<DirPage> {
        let page = self
            .vfs_readdir_page_with_attrs(ino, after_name, limit)
            .await?;
        let Some(ctx) = ctx else { return Ok(page) };
        if !self.acl_enforce_reads().await? {
            return Ok(page);
        }
        let dir = self.guard_path_of(ino).await?;
        self.ensure_may_read_at(ctx, "list", &dir).await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for e in page.entries {
            if self.guard_may_read_in(ctx, &dir, &e.entry.name).await? {
                entries.push(e);
            }
        }
        Ok(DirPage { entries, ..page })
    }

    // --- fallocate / copy_file_range (issue #119) ------------------------

    /// Copy `len` bytes from `src` at `src_off` to `dst` at `dst_off`, returning
    /// how many were actually copied — `copy_file_range(2)`.
    ///
    /// **The point is that no bytes move.** Content is chunked and
    /// content-addressed, so a range copy is a matter of pointing the destination
    /// manifest at chunks that already exist. Only the (at most two) chunks
    /// straddling the ends are read and re-stored, which makes copying a gigabyte
    /// cost about what copying a kilobyte costs. Without this the kernel falls back
    /// to a read/write loop, which on this write path is the expensive case.
    ///
    /// A short copy is normal rather than an error: the count is clamped at the
    /// source's end of file, exactly as the syscall specifies.
    pub async fn vfs_copy_range_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        src: Ino,
        src_off: u64,
        dst: Ino,
        dst_off: u64,
        len: u64,
    ) -> Result<u64> {
        self.guard_read(ctx, "copy from", src).await?;
        self.guard_write(ctx, "copy into", dst).await?;

        // Overlapping ranges within one file are undefined for the syscall and
        // refused by it. Refuse rather than guess: the surgery below reads the
        // source before writing, so an overlap would quietly copy pre-image bytes
        // and look like it had worked.
        if src == dst {
            let (s_end, d_end) = (src_off.saturating_add(len), dst_off.saturating_add(len));
            if src_off < d_end && dst_off < s_end {
                return Err(OrigoFSError::InvalidArgument(
                    "copy_file_range: source and destination ranges overlap in one file".into(),
                ));
            }
        }

        for _ in 0..Self::VFS_CAS_ATTEMPTS {
            let source = self.vfs_getattr(src).await?;
            let base = match source.content {
                Some(h) => self.load_manifest(&h).await?,
                None => crate::chunk::Manifest::default(),
            };
            let taken = self.slice_chunks(&base, src_off, len).await?;
            let copied: u64 = taken.iter().map(|c| c.len as u64).sum();
            if copied == 0 {
                return Ok(0);
            }

            let target = self.vfs_getattr(dst).await?;
            let pre = target.content;
            let into = match target.content {
                Some(h) => self.load_manifest(&h).await?,
                None => crate::chunk::Manifest::default(),
            };
            self.check_quota_for_ino(dst, into.size.max(dst_off.saturating_add(copied)))
                .await?;
            let (mhash, size) = self.replace_range(&into, dst_off, taken).await?;

            let mut tx = self.meta.begin().await?;
            if !tx.set_content_if(dst, pre.as_ref(), mhash, size).await? {
                continue; // lost the CAS: re-read and redo, as `vfs_write` does
            }
            tx.commit().await?;
            return Ok(copied);
        }
        Err(OrigoFSError::Conflict(format!(
            "ino {dst}: copy_file_range lost {} compare-and-set races in a row; the \
             file is under sustained concurrent modification",
            Self::VFS_CAS_ATTEMPTS
        )))
    }

    /// `fallocate(2)`, in the modes a content-addressed store can honour.
    ///
    /// There is nothing to preallocate here — blocks are not reserved, content
    /// exists once it is written — so the honest reading of each mode is its
    /// *observable* result rather than its allocation:
    ///
    /// * [`Allocate`](AllocateMode::Allocate) extends the file when the range runs
    ///   past the end and otherwise does nothing. It cannot reserve space, so it is
    ///   a size change and not a promise about later writes.
    /// * [`KeepSize`](AllocateMode::KeepSize) is a genuine no-op: it asks for
    ///   blocks without changing the size, and there are no blocks to ask for.
    /// * [`PunchHole`](AllocateMode::PunchHole) zeroes the range and keeps the
    ///   size; [`ZeroRange`](AllocateMode::ZeroRange) zeroes it and may extend.
    ///   Both write deduplicated zero chunks, so punching a hole *releases* space
    ///   once gc runs rather than consuming it.
    ///
    /// The modes that move data — `COLLAPSE_RANGE`, `INSERT_RANGE` — are absent,
    /// and the surface says so rather than handing back a near miss.
    pub async fn vfs_allocate_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        offset: u64,
        len: u64,
        mode: AllocateMode,
    ) -> Result<()> {
        self.guard_write(ctx, "allocate in", ino).await?;
        let end = offset.checked_add(len).ok_or_else(|| {
            OrigoFSError::TooLarge(format!("allocate end overflows u64 (ino {ino})"))
        })?;
        if len == 0 || mode == AllocateMode::KeepSize {
            return Ok(());
        }

        for _ in 0..Self::VFS_CAS_ATTEMPTS {
            let inode = self.vfs_getattr(ino).await?;
            let pre = inode.content;
            let base = match inode.content {
                Some(h) => self.load_manifest(&h).await?,
                None => crate::chunk::Manifest::default(),
            };

            let (mhash, size) = match mode {
                // Handled by the early return above; repeated as the same
                // no-op rather than a panic, so a future edit to that guard
                // degrades into "did nothing" instead of aborting a mount.
                AllocateMode::KeepSize => return Ok(()),
                AllocateMode::Allocate => {
                    if end <= base.size {
                        return Ok(());
                    }
                    self.check_quota_for_ino(ino, end).await?;
                    self.resize_body(&base, end).await?
                }
                AllocateMode::PunchHole | AllocateMode::ZeroRange => {
                    // Punching keeps the size, so it only zeroes what already
                    // exists; zeroing may extend, and anything past the old end is
                    // a hole either way.
                    let stop = if mode == AllocateMode::PunchHole {
                        end.min(base.size)
                    } else {
                        end
                    };
                    if offset >= stop {
                        return Ok(());
                    }
                    self.check_quota_for_ino(ino, base.size.max(stop)).await?;
                    let zeros = self.zero_chunks(stop - offset).await?;
                    self.replace_range(&base, offset, zeros).await?
                }
            };

            let mut tx = self.meta.begin().await?;
            if !tx.set_content_if(ino, pre.as_ref(), mhash, size).await? {
                continue;
            }
            tx.commit().await?;
            return Ok(());
        }
        Err(OrigoFSError::Conflict(format!(
            "ino {ino}: allocate lost {} compare-and-set races in a row; the file \
             is under sustained concurrent modification",
            Self::VFS_CAS_ATTEMPTS
        )))
    }

    // --- POSIX advisory locks (issue #119) -------------------------------

    /// Whether this workspace answers `fcntl` advisory locks itself.
    ///
    /// **Off by default, and the default is the point.** A FUSE mount that does
    /// not implement `setlk` still has working advisory locks — the kernel serves
    /// them locally, per mount — so every existing deployment has locking today.
    /// Answering `setlk` *takes that over*, which means a bug here breaks what
    /// currently works. Turning it on buys the one thing local locking cannot do:
    /// coordination between separate mounts. That is a trade an operator makes,
    /// not one an upgrade makes for them. Same reasoning as `acl_enforce_reads`
    /// and trash retention.
    pub async fn posix_locks_enabled(&self) -> Result<bool> {
        Ok(self
            .meta
            .get_config(crate::posixlock::ENABLED_KEY)
            .await?
            .as_deref()
            == Some("1"))
    }

    /// Turn cross-mount advisory locking on or off for this workspace.
    pub async fn set_posix_locks_enabled(&self, on: bool) -> Result<()> {
        self.meta
            .set_config(crate::posixlock::ENABLED_KEY, if on { "1" } else { "0" })
            .await
    }

    /// Test whether `req` would be granted on `ino` — `fcntl(F_GETLK)`.
    ///
    /// A read: it reports another process's lock, so it runs the read guard, which
    /// like every other read check is inert unless `acl_enforce_reads` is on.
    pub async fn vfs_getlk_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        req: &LockRequest,
    ) -> Result<LockAnswer> {
        if !self.posix_locks_enabled().await? {
            return Ok(LockAnswer::NotEnabled);
        }
        self.guard_read(ctx, "test a lock on", ino).await?;
        let held = self.meta.posix_locks(ino, self.now_secs()).await?;
        Ok(match crate::posixlock::test(&held, req) {
            Some(l) => LockAnswer::Held(l),
            None => LockAnswer::Free,
        })
    }

    /// Acquire, downgrade or release a lock on `ino` — `fcntl(F_SETLK)`.
    ///
    /// The authorization follows what the lock actually claims. An **exclusive**
    /// lock says "nobody else writes these bytes", which is a writer's claim, so it
    /// takes the write check. A **shared** lock says "nobody writes these bytes
    /// while I read them" — a reader's claim, so it takes the read guard.
    /// **Unlocking** is checked by nothing: it only ever removes ranges this owner
    /// already holds, and an actor whose grant was revoked mid-flight must still be
    /// able to let go. Refusing a release would strand the lock until its lease ran
    /// out, which is worse for everyone including the revoker.
    pub async fn vfs_setlk_as(
        &self,
        ctx: Option<crate::WriteCtx>,
        ino: Ino,
        req: &LockRequest,
    ) -> Result<LockAnswer> {
        if !self.posix_locks_enabled().await? {
            return Ok(LockAnswer::NotEnabled);
        }
        match req.kind {
            LockKind::Exclusive => self.guard_write(ctx, "lock", ino).await?,
            LockKind::Shared => self.guard_read(ctx, "lock for reading", ino).await?,
            LockKind::Unlock => {}
        }
        let now = self.now_secs();
        let conflict = self
            .meta
            .apply_posix_lock(ino, req, now + crate::posixlock::LEASE_SECS, now)
            .await?;
        Ok(match conflict {
            Some(l) => LockAnswer::Held(l),
            None => LockAnswer::Free,
        })
    }

    /// Every advisory lock currently held on `ino`, live leases only.
    ///
    /// Introspection, and the counterpart to [`Fs::locks`](Self::locks) for the
    /// LFS-style claims. `getlk` answers only "what blocks *me*", which cannot show
    /// a caller the locks it already holds or two readers sharing a range.
    /// Unchecked, exactly as `locks()` is: it reports the mount's own bookkeeping,
    /// and the paths are not in it.
    pub async fn posix_locks(&self, ino: Ino) -> Result<Vec<crate::posixlock::PosixLock>> {
        self.meta.posix_locks(ino, self.now_secs()).await
    }

    /// Drop every advisory lock a mount instance holds — a clean unmount.
    ///
    /// Not a `vfs_` op and not guarded: a mount releasing its own rows on the way
    /// down is cleanup, and making it refusable would leave the rows to expire.
    pub async fn release_posix_locks_for_holder(&self, holder: &str) -> Result<u64> {
        self.meta.release_posix_locks_for_holder(holder).await
    }

    /// Push out the lease on a mount instance's locks; called while it is alive.
    pub async fn renew_posix_lease(&self, holder: &str) -> Result<u64> {
        let until = self.now_secs() + crate::posixlock::LEASE_SECS;
        self.meta.renew_posix_lease(holder, until).await
    }
}
