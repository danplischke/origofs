//! FUSE surface (`fuse` feature) — mount an origofs workspace as a POSIX
//! filesystem via FUSE (`docs/DESIGN.md` §4e).
//!
//! A [`fuser::Filesystem`] adapter over the inode-oriented [`Fs::vfs_*`] methods.
//! origofs is async and FUSE callbacks are synchronous, so each callback drives the
//! op on an owned Tokio runtime via `block_on` (the callback runs on the FUSE
//! session thread, never inside another runtime).
//!
//! Mounting uses the `mount()` syscall directly (no `fusermount` needed) and so
//! requires root/`CAP_SYS_ADMIN`; [`mountable`] probes for that.

use crate::{FileKind, Inode, OrigoFSError, Workspace};
use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request, TimeOrNow, WriteFlags,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_secs(1);

/// Entries pulled from the store per round-trip while filling one `readdir`
/// reply. A FUSE reply buffer is a few KiB — on the order of a hundred short
/// names — so one page normally fills it outright, and a huge directory is never
/// materialized in memory (M16).
const READDIR_PAGE: usize = 128;

/// Cap on the offset→cursor map below. The map is a pure accelerator (a miss
/// falls back to a correct re-scan), so it is cleared wholesale when it grows
/// past this rather than carrying LRU bookkeeping.
const READDIR_CURSOR_CAP: usize = 4096;

/// A FUSE filesystem backed by an origofs [`Workspace`].
pub struct OrigoFSFuse {
    ws: Workspace,
    rt: Runtime,
    /// `(dir ino, FUSE offset) → name of the entry that offset sits after`.
    ///
    /// FUSE resumes a `readdir` by a dense numeric offset, but the store pages by
    /// *name* (a keyset scan — `Fs::vfs_readdir_page`). This remembers the
    /// translation for the offsets this mount actually handed out, which is every
    /// offset a sequentially-reading client will come back with.
    cursors: Mutex<HashMap<(i64, u64), String>>,
}

impl OrigoFSFuse {
    pub fn new(ws: Workspace) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            ws,
            rt,
            cursors: Mutex::new(HashMap::new()),
        })
    }

    fn blk<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    /// Record that FUSE offset `offset` in directory `ino` sits just after `name`.
    fn remember_cursor(&self, ino: i64, offset: u64, name: &str) {
        let mut map = self.cursors.lock().unwrap_or_else(PoisonError::into_inner);
        if map.len() >= READDIR_CURSOR_CAP {
            map.clear();
        }
        map.insert((ino, offset), name.to_string());
    }

    /// Translate a FUSE `readdir` offset into the store's keyset cursor: the name
    /// of the last entry already returned, or `None` to start from the beginning.
    ///
    /// `offset` counts `.` and `..` as the first two entries (see [`Filesystem::readdir`]
    /// below), so `offset - 2` real entries have been consumed. The common case —
    /// a client resuming from an offset this mount just handed out — is a map hit
    /// and costs nothing. A cold offset (a `seekdir` to an arbitrary position, or
    /// an offset evicted by the cap) falls back to walking pages to that index:
    /// still correct, and no worse than the full listing every call used to do.
    fn resume_cursor(&self, ino: i64, offset: u64) -> Result<Option<String>, OrigoFSError> {
        let consumed = offset.saturating_sub(2);
        if consumed == 0 {
            return Ok(None);
        }
        let cached = self
            .cursors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(ino, offset))
            .cloned();
        if cached.is_some() {
            return Ok(cached);
        }
        let mut cursor: Option<String> = None;
        let mut skipped: u64 = 0;
        loop {
            let page = self.blk(self.ws.fs().vfs_readdir_page(
                ino,
                cursor.as_deref(),
                READDIR_PAGE,
            ))?;
            if page.is_empty() {
                // Past the end of the directory: the last name we saw is still the
                // right cursor — the next page from it is empty, i.e. EOF.
                return Ok(cursor);
            }
            let take = ((consumed - skipped) as usize).min(page.len());
            skipped += take as u64;
            cursor = Some(page[take - 1].name.clone());
            if skipped == consumed || page.len() < READDIR_PAGE {
                return Ok(cursor);
            }
        }
    }
}

/// Whether a FUSE mount is possible here (root + an openable `/dev/fuse`).
pub fn mountable() -> bool {
    let is_root = unsafe { libc::geteuid() == 0 };
    is_root
        && std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/fuse")
            .is_ok()
}

#[allow(clippy::field_reassign_with_default)] // Config is #[non_exhaustive]
fn config() -> Config {
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("origofs".to_string()),
        MountOption::DefaultPermissions,
    ];
    config
}

/// Mount `ws` at `mountpoint`, blocking until unmounted. Run this off any async
/// runtime (e.g. a dedicated thread), since it drives its own runtime.
pub fn mount(ws: Workspace, mountpoint: &Path) -> std::io::Result<()> {
    let fs = OrigoFSFuse::new(ws)?;
    fuser::mount2(fs, mountpoint, &config())
}

/// Mount in the background; the returned session unmounts on drop.
pub fn spawn(ws: Workspace, mountpoint: &Path) -> std::io::Result<BackgroundSession> {
    let fs = OrigoFSFuse::new(ws)?;
    fuser::spawn_mount2(fs, mountpoint, &config())
}

fn errno(e: &OrigoFSError) -> Errno {
    match e {
        OrigoFSError::NotFound(_) => Errno::ENOENT,
        OrigoFSError::AlreadyExists(_) => Errno::EEXIST,
        OrigoFSError::DirectoryNotEmpty(_) => Errno::ENOTEMPTY,
        OrigoFSError::IsADirectory(_) => Errno::EISDIR,
        OrigoFSError::NotADirectory(_) => Errno::ENOTDIR,
        OrigoFSError::InvalidArgument(_) | OrigoFSError::InvalidPath(_) => Errno::EINVAL,
        _ => Errno::EIO,
    }
}

fn ftype(k: FileKind) -> FileType {
    match k {
        FileKind::Dir => FileType::Directory,
        FileKind::File => FileType::RegularFile,
        FileKind::Symlink => FileType::Symlink,
    }
}

fn to_attr(i: &Inode) -> FileAttr {
    let t = UNIX_EPOCH + Duration::from_secs(i.mtime.max(0) as u64);
    FileAttr {
        ino: INodeNo(i.ino as u64),
        size: i.size,
        blocks: i.size.div_ceil(512),
        atime: t,
        mtime: t,
        ctime: t,
        crtime: t,
        kind: ftype(i.kind),
        perm: (i.mode & 0o7777) as u16,
        nlink: i.nlink.max(1) as u32,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

impl Filesystem for OrigoFSFuse {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_lookup(parent.0 as i64, &name)) {
            Ok(Some(i)) => reply.entry(&TTL, &to_attr(&i), Generation(0)),
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.blk(self.ws.fs().vfs_getattr(ino.0 as i64)) {
            Ok(i) => reply.attr(&TTL, &to_attr(&i)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ino = ino.0 as i64;
        if let Some(sz) = size
            && let Err(e) = self.blk(self.ws.fs().vfs_truncate(ino, sz))
        {
            reply.error(errno(&e));
            return;
        }
        match self.blk(self.ws.fs().vfs_getattr(ino)) {
            Ok(i) => reply.attr(&TTL, &to_attr(&i)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.blk(self.ws.fs().vfs_readlink(ino.0 as i64)) {
            Ok(t) => reply.data(t.as_bytes()),
            Err(e) => reply.error(errno(&e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.blk(self.ws.fs().vfs_read(ino.0 as i64, offset, size)) {
            Ok(b) => reply.data(&b),
            Err(e) => reply.error(errno(&e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.blk(self.ws.fs().vfs_write(ino.0 as i64, offset, data)) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Read a directory, pulling keyset pages from the store instead of listing
    /// the whole directory and slicing it in memory (M16).
    ///
    /// # The offset contract, and how it maps onto keyset pages
    ///
    /// The cookie handed to the kernel is unchanged, so a `readdir` interrupted by
    /// the old code resumes identically under the new: offsets are dense, `1` means
    /// "after `.`", `2` means "after `..`", and `2 + k` means "after the k-th real
    /// entry in name order". An offset is always the position *after* the entry it
    /// was emitted with, which is what makes a resumed read continue rather than
    /// repeat.
    ///
    /// The store, though, pages by *name* — `WHERE name > cursor` — because a
    /// name cursor is the only one that cannot skip or duplicate an entry when the
    /// directory is modified mid-scan. Bridging a numeric offset to a name cursor
    /// is the one thing pure keyset paging cannot do by itself, so this keeps a
    /// small `offset → name` map ([`Self::resume_cursor`]) populated from the
    /// offsets it emits. A sequential reader — every real client, and the only
    /// pattern the kernel's `readdir` cache produces — always hits it. A cold
    /// offset (`seekdir` to an arbitrary position) falls back to walking pages up
    /// to that index: **correctness first**, and still no full in-memory listing.
    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino.0 as i64;
        let self_ino = INodeNo(ino as u64);
        let mut next = offset;
        if next == 0 {
            if reply.add(self_ino, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
            next = 1;
        }
        if next == 1 {
            if reply.add(self_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
            next = 2;
        }

        let mut cursor = match self.resume_cursor(ino, next) {
            Ok(c) => c,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        'fill: loop {
            let page = match self.blk(self.ws.fs().vfs_readdir_page(
                ino,
                cursor.as_deref(),
                READDIR_PAGE,
            )) {
                Ok(p) => p,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            let short = page.len() < READDIR_PAGE;
            for e in page {
                let off = next + 1;
                // `add` returns true when the buffer is full — the entry was *not*
                // added, so neither the offset nor the cursor may advance past it.
                if reply.add(INodeNo(e.ino as u64), off, ftype(e.kind), &e.name) {
                    break 'fill;
                }
                next = off;
                self.remember_cursor(ino, off, &e.name);
                cursor = Some(e.name);
            }
            if short {
                break;
            }
        }
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_create(parent.0 as i64, &name, mode)) {
            Ok(i) => reply.created(
                &TTL,
                &to_attr(&i),
                Generation(0),
                FileHandle(0),
                FopenFlags::empty(),
            ),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_mkdir(parent.0 as i64, &name, mode)) {
            Ok(i) => reply.entry(&TTL, &to_attr(&i), Generation(0)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_unlink(parent.0 as i64, &name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_rmdir(parent.0 as i64, &name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let name = name.to_string_lossy().to_string();
        let newname = newname.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_rename(
            parent.0 as i64,
            &name,
            newparent.0 as i64,
            &newname,
        )) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let name = link_name.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_symlink(parent.0 as i64, &name, &target)) {
            Ok(i) => reply.entry(&TTL, &to_attr(&i), Generation(0)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }
}
