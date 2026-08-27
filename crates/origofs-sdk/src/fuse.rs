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
//!
//! A mount is not only a server: it also *listens*. [`mount`] and [`spawn`] start
//! a [`Watcher`] that tails the workspace's change feed and pushes kernel cache
//! invalidations for whatever another writer touched — without it the kernel
//! serves its own stale caches (see [`TTL`] for exactly why a timeout is not
//! enough). Constructing an [`OrigoFSFuse`] and handing it to `fuser` yourself
//! skips that; use [`spawn`] unless you have a reason not to.

use crate::{Event, FileKind, Inode, OrigoFSError, Owner, Workspace};
use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, Notifier, OpenFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request, TimeOrNow,
    WriteFlags,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

/// How long the kernel may trust a cached entry/attr reply before asking again.
///
/// # Why a TTL is not enough on its own
///
/// A TTL only bounds how long the kernel keeps *metadata* — and it only bounds
/// it *downwards in the FUSE server's favour*: within the window the kernel
/// answers `stat`/`lookup` from its own caches without ever asking us, so a
/// change made by **another writer** (a second process, the HTTP API, an agent
/// over MCP, a Postgres peer) is simply invisible until the window lapses.
///
/// Worse, the page cache is not on a timer at all. Once a file is open the
/// kernel serves `read`s straight from cached pages; nothing re-validates them
/// until the file is re-opened or the size/mtime changes in a way the kernel
/// happens to notice. A remote write that keeps a file the same length can
/// therefore be served as *stale bytes indefinitely* — a correctness bug, not a
/// freshness one.
///
/// The fix is the other direction of the FUSE protocol: the server tells the
/// kernel to drop what it cached ([`Notifier::inval_inode`]). [`Watcher`] does
/// that from the workspace's change feed, so page-cache staleness is repaired on
/// the spot and this TTL is left governing only what it can safely govern —
/// cached *names*, where one second of staleness is the pre-existing, bounded
/// behaviour (see [`invalidate`] for why dentries are deliberately left to it).
/// Shrinking the TTL instead would cost a round-trip per `stat` and still not
/// touch the page cache.
const TTL: Duration = Duration::from_secs(1);

/// How often the invalidation [`Watcher`] re-reads the change feed when the
/// workspace cannot push to it (SQLite — Postgres uses `LISTEN/NOTIFY` and is
/// woken instead of polled).
///
/// This is the bound on how stale a mounted view can be, so it wants to be
/// comfortably *inside* [`TTL`]: a remote change should reach the kernel before
/// the kernel would have re-validated on its own, otherwise the notifier adds
/// nothing over just waiting the TTL out. The cost is one indexed range query
/// per interval per mount against a local SQLite file — the backend the docs
/// already scope to solo/offline use.
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long the push feed may block in `recv` before the watcher re-checks its
/// stop flag. Purely a shutdown-responsiveness bound: no query is issued when it
/// lapses, so it costs a wakeup and nothing else.
#[cfg(feature = "postgres")]
const SUBSCRIBE_WAKE: Duration = Duration::from_millis(250);

/// How long an unmount waits for the change-feed watcher to confirm it is gone
/// before giving up on it. See [`Watcher::shutdown`] for why this is bounded.
const WATCHER_JOIN_TIMEOUT: Duration = Duration::from_millis(500);

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
    /// Kernel-cache invalidation driven by the workspace change feed.
    ///
    /// Deliberately a field of the *filesystem* rather than of the session
    /// handle: the FUSE session owns the `Filesystem` and drops it when the
    /// mount ends, so tying the watcher's lifetime here makes teardown
    /// automatic — an unmount (however it happens: dropping the
    /// [`BackgroundSession`], `umount(8)`, the session loop failing) drops this,
    /// and [`Watcher::drop`] stops the watcher. Nothing outlives the mount.
    ///
    /// It is behind an `Arc` only so [`spawn`] can hand it the session's
    /// [`Notifier`], which does not exist until after the filesystem has been
    /// moved into the session.
    watcher: Arc<Watcher>,
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
            watcher: Arc::new(Watcher::new()),
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

// --- kernel cache invalidation ---------------------------------------------

/// The background change-feed consumer that keeps the kernel's caches honest,
/// together with the levers that stop it.
///
/// See [`TTL`] for *why* this exists. Lifetime: owned by [`OrigoFSFuse`], so it
/// is stopped exactly when the mount ends — no watcher, and no Postgres `LISTEN`
/// connection behind it, survives an unmount.
///
/// # Why its own thread instead of a task on the mount's runtime
///
/// A kernel notification is a **blocking** write to `/dev/fuse` that the kernel
/// services synchronously, so it can park for as long as the kernel needs — and
/// it parks in uninterruptible `D` state, where nothing can cancel or kill it.
/// Running that on the mount's own Tokio runtime makes it part of the mount's
/// teardown path, because dropping a `Runtime` joins its workers: a watcher task
/// caught mid-notification would block the session thread that is dropping the
/// filesystem, which is the very thread that has to answer outstanding requests.
/// That is a cycle, and a `D`-state thread makes it survive `SIGKILL`.
///
/// Its own thread and its own single-threaded runtime break that by
/// construction: nothing the session does on the way down is ever queued behind
/// an in-flight notification. (This is necessary, not sufficient — see
/// [`invalidate`] for the notification this mount still refuses to send.)
struct Watcher {
    /// Cooperative stop. The loop checks it between feed batches, and the waits
    /// in between are bounded ([`WATCH_POLL_INTERVAL`], [`SUBSCRIBE_WAKE`]) so it
    /// is seen promptly.
    stop: Arc<AtomicBool>,
    /// Signalled by the watcher thread just before it returns, so [`Drop`] can
    /// wait for a real exit rather than assume one.
    done: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl Watcher {
    fn new() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            done: Mutex::new(None),
        }
    }

    /// Begin consuming `ws`'s change feed, invalidating through `notifier`.
    fn start(&self, ws: Workspace, notifier: Notifier) {
        let stop = Arc::clone(&self.stop);
        let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let spawned = std::thread::Builder::new()
            .name("origofs-fuse-notify".to_string())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt.block_on(watch_loop(ws, notifier, stop)),
                    Err(e) => tracing::warn!(error = %e, "fuse: no runtime for the change feed"),
                }
                let _ = tx.send(());
            });
        match spawned {
            Ok(_) => *self.done.lock().unwrap_or_else(PoisonError::into_inner) = Some(rx),
            // A mount that cannot spawn its watcher still serves; it just can't
            // hear about other writers.
            Err(e) => tracing::warn!(error = %e, "fuse: could not start the change-feed watcher"),
        }
    }

    /// Ask the watcher to stop and wait — briefly — for it to actually be gone.
    fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        let done = self
            .done
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(done) = done {
            // Bounded on purpose. The watcher normally exits within one
            // [`WATCH_POLL_INTERVAL`], but it may be parked in an uninterruptible
            // kernel notification (see the type docs) that only completes once
            // this thread has finished tearing the session down — so waiting for
            // it unconditionally would be the deadlock we just designed away.
            if done.recv_timeout(WATCHER_JOIN_TIMEOUT).is_err() {
                tracing::debug!("fuse: change-feed watcher still winding down at unmount");
            }
        }
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Consume the change feed forever, translating each event into the kernel
/// invalidations it implies.
///
/// Backend choice mirrors the rest of the codebase: Postgres gets the **push**
/// feed (`subscribe`, `LISTEN/NOTIFY`), everything else polls `watch` every
/// [`WATCH_POLL_INTERVAL`]. A push feed that fails or ends (dropped connection)
/// degrades to polling rather than going silent — a mount serving stale bytes is
/// the bug we are here to prevent.
///
/// Nothing in here is fatal: a failed lookup or a rejected notification is
/// logged and skipped. The mount stays up.
async fn watch_loop(ws: Workspace, notifier: Notifier, stop: Arc<AtomicBool>) {
    // Start from the tail. Replaying history would be a burst of invalidations
    // for a cache that is empty at mount time — pure cost. The narrow race (an
    // event landing during this catch-up) is likewise harmless for the same
    // reason: there is nothing cached yet to go stale.
    let mut cursor = tail_cursor(&ws).await;

    // The push feed is Postgres `LISTEN`/`NOTIFY`. Without that backend compiled
    // in there is no feed to prefer, and the poll loop below is the whole story —
    // which is exactly what a SQLite mount already does at runtime.
    #[cfg(feature = "postgres")]
    if ws.is_postgres() {
        match ws.subscribe(cursor, None).await {
            Ok(mut sub) => {
                while !stop.load(Ordering::Relaxed) {
                    // Bounded so an unmount is noticed even on a silent feed.
                    match tokio::time::timeout(SUBSCRIBE_WAKE, sub.recv()).await {
                        Err(_elapsed) => continue,
                        // `recv` only yields empty once the connection has closed.
                        Ok(Ok(batch)) if batch.is_empty() => break,
                        Ok(Ok(batch)) => {
                            apply_batch(&ws, &notifier, batch, &mut cursor, &stop).await;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(error = %e, "fuse: change-feed subscription failed");
                            break;
                        }
                    }
                }
                if stop.load(Ordering::Relaxed) {
                    return;
                }
            }
            Err(e) => tracing::debug!(error = %e, "fuse: no push feed, polling instead"),
        }
    }

    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(WATCH_POLL_INTERVAL).await;
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match ws.watch(cursor).await {
            Ok(batch) => apply_batch(&ws, &notifier, batch, &mut cursor, &stop).await,
            Err(e) => tracing::debug!(error = %e, "fuse: change-feed poll failed"),
        }
    }
}

/// Walk the feed to its end without acting on it, returning the newest `seq`.
async fn tail_cursor(ws: &Workspace) -> i64 {
    let mut cursor = 0;
    loop {
        match ws.watch(cursor).await {
            Ok(batch) => match batch.last() {
                Some(last) => cursor = last.seq,
                None => return cursor,
            },
            Err(e) => {
                tracing::debug!(error = %e, "fuse: could not read change feed cursor");
                return cursor;
            }
        }
    }
}

async fn apply_batch(
    ws: &Workspace,
    notifier: &Notifier,
    batch: Vec<Event>,
    cursor: &mut i64,
    stop: &AtomicBool,
) {
    for ev in batch {
        *cursor = (*cursor).max(ev.seq);
        // Re-checked per event, not just per batch: a long batch must not keep
        // pushing notifications at a mount that is already coming down.
        if stop.load(Ordering::Relaxed) {
            return;
        }
        invalidate(ws, notifier, &ev).await;
    }
}

/// Map one feed event onto the kernel caches it invalidates.
///
/// # Self-inflicted invalidation
///
/// There is none to suppress: the change feed is emitted at the `Workspace` API
/// boundary, while this mount writes through the inode-oriented `Fs::vfs_*`
/// methods, which emit nothing. So a write *through the mount* never produces an
/// event and never round-trips back as an invalidation. Should that ever change,
/// the honest default is to keep invalidating — `Event` carries the actor and
/// session that caused it, but a mount is not a single actor (every uid sharing
/// it writes through the same session), so filtering on it would risk dropping a
/// genuinely-remote invalidation to save a redundant one. Correctness over the
/// round-trip.
///
/// # Why only `inval_inode`, never `inval_entry`
///
/// `FUSE_NOTIFY_INVAL_INODE` is handled by the kernel without taking any inode
/// lock, so it can never wait on a request this mount has not answered yet.
/// `FUSE_NOTIFY_INVAL_ENTRY` — the one that would forget a cached *dentry* — is
/// the opposite: it takes the parent directory's `i_rwsem` exclusively, so it
/// parks in uninterruptible `D` state behind any syscall on the mount that holds
/// that lock while waiting for us.
///
/// That is not theoretical. An earlier revision of this file did issue
/// `inval_entry`, and under concurrent mount traffic it wedged the whole process
/// roughly one run in eight: a watcher thread stuck in `fuse_reverse_inval_entry`,
/// a caller stuck in `request_wait_answer` holding the lock it wanted, and a
/// session thread that never got to answer — a cycle that survives `SIGKILL`,
/// because a `D`-state thread cannot be killed, and leaves the mount behind.
/// Moving the watcher onto its own thread (see [`Watcher`]) removed one arm of
/// it but not the hang; dropping `inval_entry` did, over 20 consecutive runs of
/// the same stress loop.
///
/// So namespace events invalidate the parent directory's *attributes* instead.
/// The consequence is honest and bounded: a name the kernel has already resolved
/// keeps resolving for up to [`TTL`] after a remote create/delete/rename — which
/// is exactly the freshness the mount had before this change, and one second,
/// not forever. The unbounded failure the change feed exists to fix — a mounted
/// reader served *stale file bytes* indefinitely out of the page cache — is
/// fixed, by the safe notification.
///
/// Doing better needs `inval_entry` to be impossible to have in flight while the
/// session is torn down, i.e. a mount guard that stops the watcher *before* the
/// unmount rather than after. That is an API change to `spawn`'s return type and
/// so to `origofs-py`'s `Mount`; it is deliberately left as follow-up.
async fn invalidate(ws: &Workspace, notifier: &Notifier, ev: &Event) {
    match ev.kind.as_str() {
        // Content changed: the page cache for this inode is the stale thing, and
        // it is the only staleness here that no timeout ever repairs.
        "write" => {
            invalidate_data(ws, notifier, &ev.path).await;
            invalidate_dir(ws, notifier, &ev.path).await;
        }
        // Namespace changes: refresh the directory's own attributes so a reader
        // sees the new mtime/size (see the note above about dentries).
        "remove" | "symlink" | "mkdir" => invalidate_dir(ws, notifier, &ev.path).await,
        "rename" => {
            invalidate_dir(ws, notifier, &ev.path).await;
            // `detail` carries the destination (see `Workspace::rename`).
            if let Some(to) = &ev.detail {
                invalidate_dir(ws, notifier, to).await;
                invalidate_data(ws, notifier, to).await;
            }
        }
        // `commit`/`lock`/`unlock`/`suggest` don't touch working-tree bytes, so
        // there is nothing the kernel is caching that they invalidate.
        _ => {}
    }
}

/// Drop the kernel's cached data (and attributes) for `path`'s inode.
async fn invalidate_data(ws: &Workspace, notifier: &Notifier, path: &str) {
    let ino = match ws.fs().stat(path).await {
        Ok(inode) => inode.ino,
        // Raced with a delete; there is no longer an inode to invalidate, and
        // the path stops resolving on its own once the entry TTL lapses.
        Err(e) => {
            tracing::debug!(path, error = %e, "fuse: no inode to invalidate");
            return;
        }
    };
    // `offset 0, len 0` is FUSE's "the whole file"; every INVAL_INODE also
    // invalidates the inode's cached attributes.
    if let Err(e) = notifier.inval_inode(INodeNo(ino as u64), 0, 0) {
        // ENOENT (kernel already dropped it) is swallowed by fuser itself;
        // anything else is still not worth tearing a mount down for.
        tracing::debug!(path, error = %e, "fuse: inval_inode rejected");
    }
}

/// Refresh the attributes of the directory `path` lives in, so its mtime/size
/// reflect the change instead of being served from a cache for up to [`TTL`].
async fn invalidate_dir(ws: &Workspace, notifier: &Notifier, path: &str) {
    let Some((parent_path, _name)) = split_parent(path) else {
        return; // the root's own attributes are not interesting on their own
    };
    let parent = match ws.fs().stat(parent_path).await {
        Ok(inode) => INodeNo(inode.ino as u64),
        Err(e) => {
            tracing::debug!(path, error = %e, "fuse: no parent to invalidate");
            return;
        }
    };
    // A *negative* offset means "attributes only" — leave the directory's own
    // pages alone, and take no inode lock.
    if let Err(e) = notifier.inval_inode(parent, -1, 0) {
        tracing::debug!(path, error = %e, "fuse: parent inval_inode rejected");
    }
}

/// Split an absolute path into `(parent dir, basename)`; `None` for the root.
fn split_parent(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_end_matches('/');
    let cut = trimmed.rfind('/')?;
    let name = &trimmed[cut + 1..];
    if name.is_empty() {
        return None;
    }
    Some((if cut == 0 { "/" } else { &trimmed[..cut] }, name))
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
///
/// Implemented on top of [`spawn`] rather than `fuser::mount2` because the
/// kernel [`Notifier`] is only reachable from a session handle, and a mount
/// without cache invalidation serves stale bytes after a remote write (see
/// [`TTL`]). Joining the session thread is what makes this block until the
/// filesystem is unmounted; the mount handle stays alive across the join (it is
/// among the fields left behind by the partial move), so the join can't unmount
/// itself out from under the thread it is waiting on.
pub fn mount(ws: Workspace, mountpoint: &Path) -> std::io::Result<()> {
    let session = spawn(ws, mountpoint)?;
    session
        .guard
        .join()
        .map_err(|_| std::io::Error::other("FUSE session thread panicked"))?
}

/// Mount in the background; the returned session unmounts on drop.
///
/// Also starts the [`Watcher`] that invalidates kernel caches from the change
/// feed. It is owned by the filesystem, which the session owns, so unmounting —
/// by dropping the returned handle or otherwise — stops it.
pub fn spawn(ws: Workspace, mountpoint: &Path) -> std::io::Result<BackgroundSession> {
    let fs = OrigoFSFuse::new(ws.clone())?;
    // Grabbed before `fs` is handed to the session, which is what gives us the
    // notifier to hand back to it.
    let watcher = Arc::clone(&fs.watcher);
    let session = fuser::spawn_mount2(fs, mountpoint, &config())?;
    watcher.start(ws, session.notifier());
    Ok(session)
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

/// The ownership to stamp on something this request creates: the uid/gid of the
/// process that issued the call (issue #122).
///
/// A file made through a mount belongs to whoever made it. Before ownership
/// existed every inode was created root-owned, which is the state this replaces.
fn caller_owner(req: &Request) -> Owner {
    Owner::new(req.uid(), req.gid())
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
        // Real ownership since #122. These were hardcoded to 0, which made every
        // inode report as root-owned — self-consistent while `fuse_mountable()`
        // requires root, but exactly why `allow_other` and non-root mounts could
        // not work: a non-root caller is evaluated against uid 0 in the *other*
        // class and loses write access to the whole tree.
        uid: i.uid,
        gid: i.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

impl Filesystem for OrigoFSFuse {
    /// The session calls this once its event loop has stopped. Stopping the
    /// watcher here rather than waiting for [`Drop`] shortens the window in which
    /// a notification can be issued at a mount that can no longer answer the
    /// syscall that notification has to wait behind (see [`Watcher`]).
    fn destroy(&mut self) {
        self.watcher.shutdown();
    }

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
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
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
        // Mode and ownership used to be bound as `_mode`/`_uid`/`_gid` and dropped,
        // after which this replied with freshly-read (unchanged) attributes — so a
        // `chmod` reported success and moved nothing (#121, #122). Apply them.
        if let Some(m) = mode
            && let Err(e) = self.blk(self.ws.fs().vfs_chmod(ino, m))
        {
            reply.error(errno(&e));
            return;
        }
        // One call for both halves: `chown` and `chgrp` each send only their own,
        // and `vfs_chown` treats `None` as chown(2)'s -1 ("leave alone").
        if (uid.is_some() || gid.is_some())
            && let Err(e) = self.blk(self.ws.fs().vfs_chown(ino, uid, gid))
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
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_string_lossy().to_string();
        match self.blk(
            self.ws
                .fs()
                .vfs_create(parent.0 as i64, &name, mode, caller_owner(req)),
        ) {
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
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();
        match self.blk(
            self.ws
                .fs()
                .vfs_mkdir(parent.0 as i64, &name, mode, caller_owner(req)),
        ) {
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
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let name = link_name.to_string_lossy().to_string();
        let target = target.to_string_lossy().to_string();
        match self.blk(
            self.ws
                .fs()
                .vfs_symlink(parent.0 as i64, &name, &target, caller_owner(req)),
        ) {
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
