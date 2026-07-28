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
use crate::types::{DirEntry, FileKind, Ino, Inode, InodeInit};
use bytes::{Bytes, BytesMut};

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

    /// Directory entries.
    pub async fn vfs_readdir(&self, ino: Ino) -> Result<Vec<DirEntry>> {
        self.meta.list_dir(ino).await
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
        let mut pos = 0u64;
        for c in &manifest.chunks {
            let cstart = pos;
            let cend = pos + c.len as u64;
            pos = cend;
            if cend <= offset {
                continue;
            }
            if cstart >= end {
                break;
            }
            let from = offset.max(cstart) - cstart;
            let to = end.min(cend) - cstart;
            buf.extend_from_slice(&self.content.get_range(&c.hash, from, to - from).await?);
        }
        Ok(buf.freeze())
    }

    /// Write `data` at `offset` (extending the file as needed). Returns bytes written.
    pub async fn vfs_write(&self, ino: Ino, offset: u64, data: &[u8]) -> Result<u32> {
        let inode = self.vfs_getattr(ino).await?;
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
        let (mhash, size) = self.store_body(&bytes).await?;
        self.meta.set_content(ino, mhash, size).await?;
        Ok(data.len() as u32)
    }

    /// Truncate/extend a file to `size` bytes.
    pub async fn vfs_truncate(&self, ino: Ino, size: u64) -> Result<()> {
        let inode = self.vfs_getattr(ino).await?;
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
        let (mhash, sz) = self.store_body(&bytes).await?;
        self.meta.set_content(ino, mhash, sz).await?;
        Ok(())
    }

    /// Create a regular file under `parent`.
    pub async fn vfs_create(&self, parent: Ino, name: &str, mode: u32) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        // Inode + dentry commit together, so a failed link can't orphan the
        // inode (C1/M6).
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit {
                kind: FileKind::File,
                mode: S_IFREG | (mode & 0o7777),
            })
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Create a directory under `parent`.
    pub async fn vfs_mkdir(&self, parent: Ino, name: &str, mode: u32) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit {
                kind: FileKind::Dir,
                mode: S_IFDIR | (mode & 0o7777),
            })
            .await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Remove a file under `parent`.
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
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        let nlink = inode.nlink - 1;
        if nlink <= 0 {
            tx.delete_inode(ino).await?;
        } else {
            tx.set_nlink(ino, nlink).await?;
        }
        tx.commit().await?;
        Ok(())
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
        if self.meta.child_count(ino).await? > 0 {
            return Err(OrigoFSError::DirectoryNotEmpty(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        tx.remove_dentry(parent, name).await?;
        tx.delete_inode(ino).await?;
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
                FileKind::Dir => tx.delete_inode(dino).await?,
                _ => {
                    let nlink = dinode.nlink - 1;
                    if nlink <= 0 {
                        tx.delete_inode(dino).await?;
                    } else {
                        tx.set_nlink(dino, nlink).await?;
                    }
                }
            }
        }
        tx.remove_dentry(parent, name).await?;
        tx.add_dentry(newparent, newname, sino).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Create a symlink under `parent`.
    pub async fn vfs_symlink(&self, parent: Ino, name: &str, target: &str) -> Result<Inode> {
        validate_component(name)?;
        if self.meta.lookup(parent, name).await?.is_some() {
            return Err(OrigoFSError::AlreadyExists(name.to_string()));
        }
        let mut tx = self.meta.begin().await?;
        let ino = tx
            .create_inode(InodeInit {
                kind: FileKind::Symlink,
                mode: SYMLINK_MODE,
            })
            .await?;
        tx.set_symlink(ino, target).await?;
        tx.add_dentry(parent, name, ino).await?;
        tx.commit().await?;
        self.vfs_getattr(ino).await
    }

    /// Read a symlink target by inode.
    pub async fn vfs_readlink(&self, ino: Ino) -> Result<String> {
        self.meta
            .get_symlink(ino)
            .await?
            .ok_or_else(|| OrigoFSError::InvalidArgument(format!("ino {ino} is not a symlink")))
    }
}
