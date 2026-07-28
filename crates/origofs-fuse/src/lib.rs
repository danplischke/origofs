//! origofs-fuse — mount an origofs workspace as a POSIX filesystem via FUSE
//! (`docs/DESIGN.md` §4e).
//!
//! A [`fuser::Filesystem`] adapter over the inode-oriented [`Fs::vfs_*`] methods.
//! origofs is async and FUSE callbacks are synchronous, so each callback drives the
//! op on an owned Tokio runtime via `block_on` (the callback runs on the FUSE
//! session thread, never inside another runtime).
//!
//! Mounting uses the `mount()` syscall directly (no `fusermount` needed) and so
//! requires root/`CAP_SYS_ADMIN`; [`mountable`] probes for that.

use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request, TimeOrNow, WriteFlags,
};
use origofs_sdk::{FileKind, Inode, OrigoFSError, Workspace};
use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_secs(1);

/// A FUSE filesystem backed by an origofs [`Workspace`].
pub struct OrigoFSFuse {
    ws: Workspace,
    rt: Runtime,
}

impl OrigoFSFuse {
    pub fn new(ws: Workspace) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self { ws, rt })
    }

    fn blk<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
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
        if let Some(sz) = size {
            if let Err(e) = self.blk(self.ws.fs().vfs_truncate(ino, sz)) {
                reply.error(errno(&e));
                return;
            }
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

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let ino = ino.0 as i64;
        let entries = match self.blk(self.ws.fs().vfs_readdir(ino)) {
            Ok(v) => v,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        let mut all: Vec<(u64, FileType, String)> = vec![
            (ino as u64, FileType::Directory, ".".to_string()),
            (ino as u64, FileType::Directory, "..".to_string()),
        ];
        for e in entries {
            all.push((e.ino as u64, ftype(e.kind), e.name));
        }
        for (i, (child, kind, name)) in all.into_iter().enumerate().skip(offset as usize) {
            // The next offset is i+1 so a resumed readdir continues correctly.
            if reply.add(INodeNo(child), (i + 1) as u64, kind, &name) {
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
