//! NFSv3 surface (`nfs` feature) — serve a workspace over NFSv3 (`docs/DESIGN.md` §6, M7 surfaces).
//!
//! A [`nfsserve`] adapter that maps NFSv3 operations onto origofs's inode-oriented
//! `vfs_*` methods — the very same ones the FUSE mount uses, so the filesystem
//! semantics are shared and already exercised. NFS is fileid-oriented, which
//! lines up directly: an NFS `fileid3` *is* an origofs inode number.
//!
//! ```no_run
//! # async fn run(ws: origofs_sdk::Workspace) -> std::io::Result<()> {
//! origofs_sdk::nfs::serve(ws, "127.0.0.1:11111").await
//! # }
//! ```
//!
//! A client then mounts it with, e.g.
//! `mount -t nfs -o vers=3,tcp,port=11111,mountport=11111,nolock host:/ /mnt`
//! (needs the OS NFS client / `nfs-utils`).

use crate::{FileKind, Inode, OrigoFSError, Workspace};
use async_trait::async_trait;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfsstring, nfstime3, sattr3, set_mode3,
    set_size3, specdata3,
};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry as NfsDirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};

/// The origofs root inode, which is also the NFS root fileid.
const ROOT: fileid3 = 1;

/// An NFSv3 view of a workspace.
pub struct OrigoFSNfs {
    ws: Workspace,
}

impl OrigoFSNfs {
    pub fn new(ws: Workspace) -> Self {
        Self { ws }
    }
}

/// Bind an NFSv3 server for `ws` at `addr` (e.g. `127.0.0.1:11111`) and serve
/// until the process exits.
///
/// # Security
///
/// NFSv3 has no meaningful authentication — anyone who can reach `addr` gets full
/// access to the workspace, and its writes are unattributed. Bind a **loopback**
/// address (the default) and reach it over an SSH tunnel / VPN, or otherwise keep
/// it behind a network boundary; never expose it on an untrusted network.
pub async fn serve(ws: Workspace, addr: &str) -> std::io::Result<()> {
    let listener = NFSTcpListener::bind(addr, OrigoFSNfs::new(ws)).await?;
    listener.handle_forever().await
}

/// Map an origofs error to the closest NFSv3 status.
fn stat(e: OrigoFSError) -> nfsstat3 {
    match e {
        OrigoFSError::NotFound(_) | OrigoFSError::ContentMissing(_) => nfsstat3::NFS3ERR_NOENT,
        OrigoFSError::IsADirectory(_) => nfsstat3::NFS3ERR_ISDIR,
        OrigoFSError::NotADirectory(_) => nfsstat3::NFS3ERR_NOTDIR,
        OrigoFSError::AlreadyExists(_) => nfsstat3::NFS3ERR_EXIST,
        OrigoFSError::DirectoryNotEmpty(_) => nfsstat3::NFS3ERR_NOTEMPTY,
        OrigoFSError::InvalidArgument(_) | OrigoFSError::InvalidPath(_) => nfsstat3::NFS3ERR_INVAL,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

fn name(f: &filename3) -> Result<&str, nfsstat3> {
    std::str::from_utf8(&f.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

fn ftype(kind: FileKind) -> ftype3 {
    match kind {
        FileKind::File => ftype3::NF3REG,
        FileKind::Dir => ftype3::NF3DIR,
        FileKind::Symlink => ftype3::NF3LNK,
    }
}

/// Build NFS attributes from an origofs inode.
fn attr(inode: &Inode) -> fattr3 {
    let t = nfstime3 {
        seconds: inode.mtime.max(0) as u32,
        nseconds: 0,
    };
    fattr3 {
        ftype: ftype(inode.kind),
        mode: inode.mode & 0o7777,
        nlink: inode.nlink.max(1) as u32,
        uid: 0,
        gid: 0,
        size: inode.size,
        used: inode.size,
        rdev: specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid: 0,
        fileid: inode.ino as fileid3,
        atime: t,
        mtime: t,
        ctime: nfstime3 {
            seconds: inode.ctime.max(0) as u32,
            nseconds: 0,
        },
    }
}

#[async_trait]
impl NFSFileSystem for OrigoFSNfs {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    fn root_dir(&self) -> fileid3 {
        ROOT
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        match self
            .ws
            .fs()
            .vfs_lookup(dirid as i64, name(filename)?)
            .await
            .map_err(stat)?
        {
            Some(inode) => Ok(inode.ino as fileid3),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(attr(
            &self.ws.fs().vfs_getattr(id as i64).await.map_err(stat)?,
        ))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        if let set_size3::size(sz) = setattr.size {
            self.ws
                .fs()
                .vfs_truncate(id as i64, sz)
                .await
                .map_err(stat)?;
        }
        // origofs's minimal inode set doesn't persist uid/gid/atime/mtime; mode
        // changes aren't yet surfaced by vfs_*, so those set-attrs are accepted
        // but no-op. Size (truncate) is the one that matters for NFS clients.
        Ok(attr(
            &self.ws.fs().vfs_getattr(id as i64).await.map_err(stat)?,
        ))
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let size = self
            .ws
            .fs()
            .vfs_getattr(id as i64)
            .await
            .map_err(stat)?
            .size;
        let bytes = self
            .ws
            .fs()
            .vfs_read(id as i64, offset, count)
            .await
            .map_err(stat)?;
        let eof = offset.saturating_add(bytes.len() as u64) >= size;
        Ok((bytes.to_vec(), eof))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        self.ws
            .fs()
            .vfs_write(id as i64, offset, data)
            .await
            .map_err(stat)?;
        Ok(attr(
            &self.ws.fs().vfs_getattr(id as i64).await.map_err(stat)?,
        ))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr_in: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let mode = match attr_in.mode {
            set_mode3::mode(m) => m,
            _ => 0o644,
        };
        let inode = self
            .ws
            .fs()
            .vfs_create(dirid as i64, name(filename)?, mode)
            .await
            .map_err(stat)?;
        if let set_size3::size(sz) = attr_in.size {
            self.ws
                .fs()
                .vfs_truncate(inode.ino, sz)
                .await
                .map_err(stat)?;
        }
        let inode = self.ws.fs().vfs_getattr(inode.ino).await.map_err(stat)?;
        Ok((inode.ino as fileid3, attr(&inode)))
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let inode = self
            .ws
            .fs()
            .vfs_create(dirid as i64, name(filename)?, 0o644)
            .await
            .map_err(stat)?;
        Ok(inode.ino as fileid3)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let inode = self
            .ws
            .fs()
            .vfs_mkdir(dirid as i64, name(dirname)?, 0o755)
            .await
            .map_err(stat)?;
        Ok((inode.ino as fileid3, attr(&inode)))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let n = name(filename)?;
        match self
            .ws
            .fs()
            .vfs_lookup(dirid as i64, n)
            .await
            .map_err(stat)?
        {
            Some(inode) if inode.kind == FileKind::Dir => {
                self.ws.fs().vfs_rmdir(dirid as i64, n).await.map_err(stat)
            }
            Some(_) => self.ws.fs().vfs_unlink(dirid as i64, n).await.map_err(stat),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        self.ws
            .fs()
            .vfs_rename(
                from_dirid as i64,
                name(from_filename)?,
                to_dirid as i64,
                name(to_filename)?,
            )
            .await
            .map_err(stat)
    }

    /// Read a directory as one store-side keyset page with batched attributes
    /// (M16) — no full listing in memory, and no `getattr` per entry.
    ///
    /// # The cookie contract, and how it maps onto keyset pages
    ///
    /// NFSv3 resumes with the **fileid of the last entry returned**, so the cookie
    /// is unchanged and an unknown one is still `NFS3ERR_BAD_COOKIE`. The store
    /// pages by *name*, so the cookie is translated back into its name with a
    /// single indexed dentry lookup ([`Fs::vfs_dentry_name`]) and used as the
    /// keyset cursor. That translation is exact for any cookie this server ever
    /// emitted, so resumption needs no server-side session state — the property
    /// NFS's stateless readdir wants.
    ///
    /// Two deliberate consequences:
    ///
    /// * Entries now come back in **name** order rather than the inode order the
    ///   old in-memory sort imposed. NFSv3 mandates no particular order, only that
    ///   it be stable enough for the cookie to mean something — and a name order
    ///   is both stable and what the underlying index already provides.
    /// * `end` is reported when the store returns a short page. A directory whose
    ///   remaining entries exactly fill `max_entries` therefore costs one extra
    ///   (empty) `READDIR` to confirm EOF, instead of being called at the boundary.
    ///   That is inherent to paging without a count, and clients handle it.
    ///
    /// [`Fs::vfs_dentry_name`]: origofs_core::vfs
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let dirid = dirid as i64;
        let after = if start_after == 0 {
            None
        } else {
            match self
                .ws
                .fs()
                .vfs_dentry_name(dirid, start_after as i64)
                .await
                .map_err(stat)?
            {
                Some(n) => Some(n),
                None => return Err(nfsstat3::NFS3ERR_BAD_COOKIE),
            }
        };

        // A zero-entry request still has to answer "is there more?", so probe with
        // a single row rather than asking the store for a zero-sized page.
        let page = self
            .ws
            .fs()
            .vfs_readdir_page_with_attrs(dirid, after.as_deref(), max_entries.max(1))
            .await
            .map_err(stat)?;
        let entries = if max_entries == 0 {
            Vec::new()
        } else {
            page.entries
                .iter()
                .map(|e| NfsDirEntry {
                    fileid: e.entry.ino as fileid3,
                    name: nfsstring::from(e.entry.name.as_bytes()),
                    attr: attr(&e.inode),
                })
                .collect()
        };
        Ok(ReadDirResult {
            entries,
            end: page.end,
        })
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let target = std::str::from_utf8(&symlink.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        let inode = self
            .ws
            .fs()
            .vfs_symlink(dirid as i64, name(linkname)?, target)
            .await
            .map_err(stat)?;
        Ok((inode.ino as fileid3, attr(&inode)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let target = self.ws.fs().vfs_readlink(id as i64).await.map_err(stat)?;
        Ok(nfsstring::from(target.into_bytes()))
    }
}
