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
//!
//! Writes are **buffered per open file handle** ([`DirtyBuffer`], issue #112) and
//! written out at `flush`/`fsync`/`release`, or sooner if a handle fills
//! [`HANDLE_BUFFER_CAP`]. Everything reached *through the mount* still reads its
//! own writes — see [`Filesystem::read`] — but bytes only become visible to the
//! workspace's other surfaces (the HTTP API, an agent over MCP, a Postgres peer)
//! once they are flushed. That is the ordinary bargain of a write-back cache, and
//! it buys a large constant factor off a write path that rewrites the whole file
//! per request (see [`HANDLE_BUFFER_CAP`] for the arithmetic).

use crate::{Event, FileKind, Inode, OrigoFSError, Owner, Workspace, WriteCtx};
use fuser::{
    BackgroundSession, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr, FileHandle,
    FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner, MountOption, Notifier,
    OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use origofs_core::posixlock::{LOCK_EOF, LockAnswer, LockKind, LockRequest};
use origofs_core::vfs::AllocateMode;
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

/// How many dirty bytes one open file handle may hold before `Filesystem::write` stops
/// buffering and flushes on the spot (issue #112).
///
/// The kernel hands a FUSE server one request per `write(2)`, splitting a large
/// one at whatever the mount negotiated — classically 128 KiB, and at most 1 MiB
/// on Linux, which caps a request at 256 pages however large a `max_write` the
/// server advertises. `Fs::vfs_write` answers each of those with a *whole-file*
/// read-modify-write: read a file of size `n` back, patch the request's slice of
/// it, re-chunk and re-store all `n` bytes. So the cost of writing a file is the
/// number of requests times its size.
///
/// Buffering divides the first factor. `CAP` bytes of coalesced writes become
/// one read-modify-write instead of `CAP / request_size` of them — 4× against a
/// 1 MiB request, 32× against a 128 KiB one, and three orders of magnitude for
/// an application that writes in 4 KiB pieces (an append loop, a log, an editor
/// saving line by line), which is where the pathology actually bites. It does
/// not divide the second factor: each flush still rewrites the whole file, so
/// this is a large constant, not a change of complexity. Issue #111's slices are
/// what attack the per-write cost; neither subsumes the other.
///
/// The number is a memory/round-trip trade made per *open handle*, so the worst
/// case is this times the number of files open for writing at once. 4 MiB
/// matches the block size JuiceFS coalesces to and is small enough that a few
/// hundred concurrent writers still fit in a normal process.
const HANDLE_BUFFER_CAP: usize = 4 * 1024 * 1024;

/// How long a blocked `F_SETLKW` waits before giving up.
///
/// A wait needs an end because nothing can cancel it: `fuser` exposes no
/// `FUSE_INTERRUPT` hook at this layer, so if the waiting process is killed the
/// reply is still owed. Generous enough to cover the lock handoffs real programs
/// do, short enough that a forgotten waiter frees its slot the same minute.
const BLOCKING_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// How often a blocked waiter re-tries. There is no cross-process wakeup to wait
/// on — the holder may be on another machine — so this polls. Short enough to feel
/// immediate, long enough not to hammer the store while a lock is held.
const BLOCKING_LOCK_POLL: Duration = Duration::from_millis(50);

/// Translate a FUSE lock range onto a stored one.
///
/// FUSE ranges are `u64` and inclusive, with `u64::MAX` for end-of-file; stored
/// ranges are `i64` for the database's sake. Both ends saturate rather than wrap,
/// so an open-ended lock stays open-ended instead of becoming a negative offset.
fn lock_range(start: u64, end: u64) -> (i64, i64) {
    let s = start.min(i64::MAX as u64) as i64;
    let e = if end >= i64::MAX as u64 {
        LOCK_EOF
    } else {
        end as i64
    };
    (s, e)
}

/// `F_RDLCK`/`F_WRLCK`/`F_UNLCK` onto the engine's kinds; `None` for anything else,
/// which the kernel should never send and which becomes `EINVAL` rather than a guess.
///
/// The conversions look pointless here and are not: these constants are `c_int` on
/// Linux but `c_short` on macOS, and this module is `cfg(unix)` rather than
/// `cfg(linux)`, so it does build against macFUSE. Clippy only ever sees the Linux
/// width, where the widening is a no-op — hence the allow rather than a "fix" that
/// would stop compiling on the other platform.
/// The `F_*LCK` constants widened to what `ReplyLock` takes. See [`lock_kind`] for
/// why the conversion is not redundant.
#[allow(clippy::useless_conversion)]
fn read_type() -> i32 {
    libc::F_RDLCK.into()
}

#[allow(clippy::useless_conversion)]
fn write_type() -> i32 {
    libc::F_WRLCK.into()
}

#[allow(clippy::useless_conversion)]
fn unlock_type() -> i32 {
    libc::F_UNLCK.into()
}

#[allow(clippy::useless_conversion)]
fn lock_kind(typ: i32) -> Option<LockKind> {
    let (rd, wr, un): (i32, i32, i32) = (
        libc::F_RDLCK.into(),
        libc::F_WRLCK.into(),
        libc::F_UNLCK.into(),
    );
    match typ {
        t if t == rd => Some(LockKind::Shared),
        t if t == wr => Some(LockKind::Exclusive),
        t if t == un => Some(LockKind::Unlock),
        _ => None,
    }
}

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
    /// Open file handles and the dirty bytes each is holding (issue #112).
    ///
    /// Managed exactly like [`Self::cursors`]: a plain `Mutex` behind the shared
    /// `&self` every [`Filesystem`] callback gets, never held across an `await`
    /// or a `block_on` (see `OrigoFSFuse::handles_of` for the lock order that
    /// keeps that true).
    handles: Mutex<HandleTable>,
    /// The actor this mount was started for, or `None` for an anonymous mount
    /// (issue #141).
    ///
    /// Held for the life of the mount and passed to every engine call, so the
    /// path-scoped ACLs apply to what comes through the kernel exactly as they do
    /// to MCP and HTTP. `None` preserves the historical behaviour — no identity,
    /// no check — for `origofs mount`'s single-user case.
    ///
    /// It is deliberately *not* captured per open file handle. A handle outlives
    /// the call that opened it, and buffered writes flush from `release`, so a
    /// per-handle actor would be an actor read long after the process that
    /// supplied it is gone. The mount's identity is a property of the mount.
    ctx: Option<WriteCtx>,
    /// Identity of *this mount instance* for POSIX advisory locks (issue #119).
    ///
    /// The lock table is shared between mounts, so rows need to say which mount
    /// put them there: a clean unmount deletes this holder's rows outright, and a
    /// crashed one is cleaned up when its lease expires. It also namespaces the
    /// kernel's lock owner, which is only unique within one kernel — two mounts on
    /// two machines can hand out the same owner id for unrelated files.
    holder: String,
}

impl OrigoFSFuse {
    pub fn new(ws: Workspace) -> std::io::Result<Self> {
        Self::new_as(ws, None)
    }

    /// [`new`](Self::new) for a mount bound to an actor (issue #141).
    pub fn new_as(ws: Workspace, ctx: Option<WriteCtx>) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let holder = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let me = Self {
            ws,
            watcher: Arc::new(Watcher::new()),
            rt,
            cursors: Mutex::new(HashMap::new()),
            handles: Mutex::new(HandleTable::default()),
            ctx,
            holder,
        };
        me.spawn_lease_renewer();
        Ok(me)
    }

    /// Keep this mount's advisory-lock leases alive while it runs.
    ///
    /// A durable lock table cannot be tidied by a process that has died, so rows
    /// expire unless renewed. Renewing only when a lock op happens would not do:
    /// a process that takes a lock and then works for five minutes would have it
    /// expire underneath it and another mount could take the range.
    ///
    /// Read once, at mount time. Turning the workspace switch on does not reach a
    /// mount that is already running — remount to pick it up — which is stated
    /// here because the alternative is polling the setting forever on every mount
    /// that will never use it.
    fn spawn_lease_renewer(&self) {
        let enabled = self
            .blk(self.ws.fs().posix_locks_enabled())
            .unwrap_or(false);
        if !enabled {
            return;
        }
        let ws = self.ws.clone();
        let holder = self.holder.clone();
        // Spawned on the mount's own runtime, which is dropped with the mount, so
        // the task cannot outlive the filesystem it renews for.
        self.rt.spawn(async move {
            let every = std::time::Duration::from_secs(
                (origofs_core::posixlock::LEASE_SECS as u64 / 3).max(1),
            );
            loop {
                tokio::time::sleep(every).await;
                if ws.fs().renew_posix_lease(&holder).await.is_err() {
                    // A failed renewal is not fatal: the lease still has time on
                    // it and the next tick tries again. Losing the store entirely
                    // is reported by the operations the user actually issued.
                    continue;
                }
            }
        });
    }

    /// Build a lock request from what the kernel handed us.
    ///
    /// The owner is the kernel's lock owner — the open file description, not the
    /// process, which is what POSIX says ownership follows — prefixed with this
    /// mount so two mounts cannot collide on the same number.
    fn lock_request(
        &self,
        lock_owner: LockOwner,
        pid: u32,
        start: i64,
        end: i64,
        kind: LockKind,
    ) -> LockRequest {
        LockRequest {
            owner: format!("{}:{}", self.holder, lock_owner),
            holder: self.holder.clone(),
            pid: i64::from(pid),
            start,
            end,
            kind,
        }
    }

    /// Wait for a blocked `F_SETLKW` **off the session thread**, then reply.
    ///
    /// This is the part that cannot be done inline. `fuser`'s dispatch loop is
    /// single-threaded unless configured otherwise (`Config::n_threads` defaults
    /// to 1, and this mount does not raise it), so sleeping inside the callback
    /// would freeze every other operation on the mountpoint for the duration —
    /// a blocking lock on one file would stall reads of every other. Moving the
    /// reply onto the mount's runtime keeps the session thread free, which is
    /// also what lets the wait be generous rather than a token retry.
    ///
    /// Bounded rather than indefinite: `fuser` surfaces no `FUSE_INTERRUPT` hook
    /// here, so a wait with no deadline could not be cancelled when the waiting
    /// process is killed, and the reply would be owed forever. On timeout the
    /// caller gets `EAGAIN`, which is a lie POSIX does not sanction for
    /// `F_SETLKW` but is the honest end of a wait nobody can cancel.
    fn wait_for_lock(&self, ino: i64, req: LockRequest, reply: ReplyEmpty) {
        let ws = self.ws.clone();
        let ctx = self.ctx;
        self.rt.spawn(async move {
            let deadline = std::time::Instant::now() + BLOCKING_LOCK_TIMEOUT;
            loop {
                tokio::time::sleep(BLOCKING_LOCK_POLL).await;
                match ws.fs().vfs_setlk_as(ctx, ino, &req).await {
                    Ok(LockAnswer::Free) => return reply.ok(),
                    Ok(LockAnswer::NotEnabled) => return reply.error(Errno::ENOSYS),
                    Err(e) => return reply.error(errno(&e)),
                    Ok(LockAnswer::Held(_)) => {}
                }
                if std::time::Instant::now() >= deadline {
                    return reply.error(Errno::EAGAIN);
                }
            }
        });
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
            let page = self.blk(self.ws.fs().vfs_readdir_page_as(
                self.ctx,
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

// --- per-handle write buffering (issue #112) --------------------------------

/// One contiguous run of dirty bytes held for an open file handle.
struct Run {
    offset: u64,
    data: Vec<u8>,
}

impl Run {
    fn end(&self) -> u64 {
        // Never overflows: `DirtyBuffer::write_at` refuses a range whose end does
        // not fit in a u64 before a run can ever be built from it.
        self.offset + self.data.len() as u64
    }
}

/// The dirty bytes one open file handle is holding, as a set of disjoint,
/// coalesced, ascending byte runs.
///
/// This is the whole of the buffering logic, deliberately split out from the
/// [`Filesystem`] impl so it can be tested without a mount (a FUSE mount needs
/// root and `/dev/fuse`, which CI does not always have — see
/// `tests/fuse_buffering.rs`).
///
/// # Why runs, and not a single byte vector
///
/// A flush must patch **only the bytes the caller actually wrote**. Holding a
/// materialized image of the file instead — read it, patch it, write it whole —
/// would take a snapshot at open time and hand it back at close time, silently
/// erasing everything another writer did to the untouched parts in between. That
/// is precisely the lost update `Fs::vfs_write`'s compare-and-set loop exists to
/// prevent, and a whole-file image would defeat it *while still winning the CAS*,
/// because the write really would be derived from the version it read. Recording
/// ranges keeps each flush a genuine patch, so the CAS still means what it says.
///
/// Runs also bound memory by bytes *written* rather than by file size: two writes
/// a gigabyte apart merge into nothing, they stay two small runs, and no gap is
/// ever materialized.
#[derive(Default)]
pub struct DirtyBuffer {
    /// Sorted by offset, pairwise disjoint, and never merely adjacent — two runs
    /// that touch are always coalesced into one.
    runs: Vec<Run>,
    /// Sum of `runs[..].data.len()`, kept incrementally so the cap check in
    /// `write` is O(1).
    len: usize,
}

impl DirtyBuffer {
    /// Record `data` as written at `offset`, coalescing with anything already
    /// buffered that it overlaps or touches. A later write to the same byte wins.
    ///
    /// Fails with [`OrigoFSError::TooLarge`] if the end offset does not fit in a
    /// `u64` — a hostile offset must be refused here rather than wrap and corrupt
    /// the run ordering (`Fs::vfs_write_attempt` makes the same check for the
    /// unbuffered path).
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), OrigoFSError> {
        let end = offset.checked_add(data.len() as u64).ok_or_else(|| {
            OrigoFSError::TooLarge(format!("write at offset {offset} overflows u64"))
        })?;
        if data.is_empty() {
            return Ok(());
        }
        // Every run that overlaps *or abuts* the new range is absorbed, so a
        // sequential writer's 128 KiB pages collapse into one run rather than
        // accumulating thousands of adjacent ones.
        let first = self.runs.partition_point(|r| r.end() < offset);
        let last = self.runs.partition_point(|r| r.offset <= end);
        let merged: Vec<Run> = self.runs.splice(first..last, std::iter::empty()).collect();

        let start = merged.first().map_or(offset, |r| r.offset.min(offset));
        let stop = merged.iter().map(Run::end).max().unwrap_or(end).max(end);
        let mut buf = vec![0u8; (stop - start) as usize];
        for r in &merged {
            let at = (r.offset - start) as usize;
            buf[at..at + r.data.len()].copy_from_slice(&r.data);
            self.len -= r.data.len();
        }
        // New bytes last: this write is the most recent one for its range.
        let at = (offset - start) as usize;
        buf[at..at + data.len()].copy_from_slice(data);

        self.len += buf.len();
        self.runs.insert(
            first,
            Run {
                offset: start,
                data: buf,
            },
        );
        Ok(())
    }

    /// Total dirty bytes held.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether anything is buffered.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// One past the highest dirty byte, or `None` when nothing is buffered.
    ///
    /// This is what lets `getattr` report a size that includes bytes not yet
    /// flushed; see `OrigoFSFuse::attr_of`.
    pub fn end(&self) -> Option<u64> {
        self.runs.last().map(Run::end)
    }

    /// Whether any buffered byte falls inside `[offset, offset + size)`.
    ///
    /// The read path's flush predicate — see [`Filesystem::read`] for why an
    /// overlap is the exact condition under which a read would otherwise be
    /// served stale bytes.
    ///
    /// Both intervals are half-open, so a read that stops exactly where a run
    /// starts (or starts exactly where one ends) does *not* overlap it — a
    /// zero-length read overlaps nothing at all.
    pub fn overlaps(&self, offset: u64, size: u32) -> bool {
        if size == 0 {
            return false;
        }
        let end = offset.saturating_add(size as u64);
        self.runs.iter().any(|r| r.offset < end && r.end() > offset)
    }

    /// The buffered runs as `(offset, bytes)`, ascending and disjoint.
    pub fn runs(&self) -> impl Iterator<Item = (u64, &[u8])> {
        self.runs.iter().map(|r| (r.offset, r.data.as_slice()))
    }

    /// Remove and return every run, leaving the buffer empty.
    fn take(&mut self) -> Vec<Run> {
        self.len = 0;
        std::mem::take(&mut self.runs)
    }

    /// Put back runs a failed flush never got to write.
    ///
    /// `runs` must be a suffix of what `take` returned and the buffer must
    /// still be empty — which the flush path guarantees, because it holds the
    /// handle's lock across the whole flush, so no new write can have landed.
    fn restore(&mut self, runs: Vec<Run>) {
        debug_assert!(self.runs.is_empty());
        self.len = runs.iter().map(|r| r.data.len()).sum();
        self.runs = runs;
    }
}

/// One open file handle: which inode it refers to, and what it has yet to flush.
///
/// # Where actor context would go, and why it is not here
///
/// A handle is the natural place to capture the actor and session behind a write
/// — resolve them once at `open`, hold them for the handle's lifetime, and flush
/// through the *attributed* write path so a mounted edit landed in the blame
/// trail like any other. This deliberately does **not** do that. The mounts have
/// no actor context at all today (a kernel mount has no caller identity origofs
/// can trust; `CLAUDE.md` records the bypass), and inventing one here would put
/// fabricated names in the attribution log, which is worse than an honest gap.
///
/// Buffering does not make that gap worse — an unattributed buffered write is
/// exactly as unattributed as the unbuffered write it replaces — and it should
/// not be read as making it better either. It only means that *if* the mounts
/// ever gain identity, this struct is where it belongs.
struct OpenFile {
    ino: i64,
    /// Guarded independently of [`HandleTable`], and held across the `block_on`
    /// of a flush: that is what makes a flush atomic with respect to further
    /// writes on the same handle, and what lets a failed flush put its unwritten
    /// runs back knowing nothing has landed on top of them.
    buf: Mutex<DirtyBuffer>,
}

/// Every handle this mount has handed out, and the reverse index a read needs.
#[derive(Default)]
struct HandleTable {
    /// Last handle id allocated. Ids start at 1 — never 0, which is what `fuser`
    /// hands out by default for the `opendir` this file leaves unimplemented, so
    /// a directory handle can never collide with a file handle here.
    next: u64,
    open: HashMap<u64, Arc<OpenFile>>,
    /// `ino → the handles open on it`. A read arrives addressed by inode and has
    /// to find dirty bytes held by *any* handle on that inode, including one
    /// another process opened, so the reverse index is not optional.
    by_ino: HashMap<i64, Vec<u64>>,
}

impl OrigoFSFuse {
    /// Allocate a handle for `ino`.
    fn open_handle(&self, ino: i64) -> FileHandle {
        let mut t = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        t.next += 1;
        let id = t.next;
        t.open.insert(
            id,
            Arc::new(OpenFile {
                ino,
                buf: Mutex::new(DirtyBuffer::default()),
            }),
        );
        t.by_ino.entry(ino).or_default().push(id);
        FileHandle(id)
    }

    /// The handle `fh` refers to, or `None` if this mount never handed it out.
    ///
    /// A miss is not an error: a write-back from the page cache or an `mmap`
    /// carries a `fh` the kernel explicitly documents as possibly unrelated to
    /// the one `open` returned, and the callers below fall back to the direct,
    /// unbuffered path for exactly that case.
    fn handle(&self, fh: FileHandle) -> Option<Arc<OpenFile>> {
        self.handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open
            .get(&fh.0)
            .cloned()
    }

    /// Forget `fh`, returning it so `release` can flush what it still holds.
    fn close_handle(&self, fh: FileHandle) -> Option<Arc<OpenFile>> {
        let mut t = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        let h = t.open.remove(&fh.0)?;
        if let Some(ids) = t.by_ino.get_mut(&h.ino) {
            ids.retain(|id| *id != fh.0);
            if ids.is_empty() {
                t.by_ino.remove(&h.ino);
            }
        }
        Some(h)
    }

    /// Every handle currently open on `ino`.
    ///
    /// **Lock order.** This returns owned `Arc`s and drops the table lock before
    /// returning, so a caller only ever holds *one* `OpenFile::buf` lock at a
    /// time and never holds the table lock while it does. That is what keeps a
    /// flush (which parks on `buf` across a `block_on`) from blocking an
    /// unrelated `lookup` or `open`, and rules out a lock cycle between the two.
    fn handles_of(&self, ino: i64) -> Vec<Arc<OpenFile>> {
        let t = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
        match t.by_ino.get(&ino) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| t.open.get(id).cloned())
                .collect(),
            None => Vec::new(),
        }
    }

    /// Whether this mount has any open file handle at all — the cheap guard that
    /// keeps the buffering machinery off the path of a mount nobody is writing to.
    fn any_open(&self) -> bool {
        !self
            .handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open
            .is_empty()
    }

    /// Write out every run in `buf` and leave it empty.
    ///
    /// # Why this does not defeat `VFS_CAS_ATTEMPTS`
    ///
    /// Each run goes out as its own `vfs_write(ino, offset, bytes)` — the same
    /// bounded compare-and-set read-modify-write an unbuffered mount already used,
    /// on a range covering only bytes this handle really wrote (see
    /// [`DirtyBuffer`]). So the lost-update guard is untouched: two handles
    /// flushing disjoint ranges of one file still resolve by whoever loses the CAS
    /// re-reading and reapplying, and two handles flushing *overlapping* ranges
    /// still resolve last-writer-wins per byte, exactly as two racing `write(2)`s
    /// on any filesystem do.
    ///
    /// If anything, buffering *relieves* that guard rather than straining it: the
    /// contention it bounds is per read-modify-write, and coalescing a run of
    /// kernel requests into one flush replaces that many chances to lose a race
    /// with one. What changes is the granularity of the interleaving, not its
    /// correctness.
    ///
    /// An error stops the flush and puts the unwritten runs back, so nothing is
    /// dropped on the floor and a later `flush`/`fsync`/`release` retries them.
    /// The already-written prefix is not rolled back — a partial `write(2)` is a
    /// state POSIX allows, and re-writing bytes that landed would be the more
    /// surprising outcome.
    fn flush_buf(&self, ino: i64, buf: &mut DirtyBuffer) -> Result<(), OrigoFSError> {
        let runs = buf.take();
        for (i, run) in runs.iter().enumerate() {
            if let Err(e) = self.blk(
                self.ws
                    .fs()
                    .vfs_write_as(self.ctx, ino, run.offset, &run.data),
            ) {
                buf.restore(runs.into_iter().skip(i).collect());
                return Err(e);
            }
        }
        Ok(())
    }

    /// Flush one handle. Idempotent: a handle with nothing buffered is a no-op,
    /// which is what makes `flush` correct when the kernel calls it once per
    /// `close(2)` of a dup'd descriptor.
    fn flush_handle(&self, h: &OpenFile) -> Result<(), OrigoFSError> {
        let mut buf = h.buf.lock().unwrap_or_else(PoisonError::into_inner);
        if buf.is_empty() {
            return Ok(());
        }
        self.flush_buf(h.ino, &mut buf)
    }

    /// Flush every handle open on `ino`.
    ///
    /// The first error stops the sweep and is returned; the remaining handles keep
    /// their buffers, so a retry (or their own `release`) still writes them.
    fn flush_ino(&self, ino: i64) -> Result<(), OrigoFSError> {
        for h in self.handles_of(ino) {
            self.flush_handle(&h)?;
        }
        Ok(())
    }

    /// Flush the handles on `ino` holding bytes inside `[offset, offset + size)`.
    fn flush_overlapping(&self, ino: i64, offset: u64, size: u32) -> Result<(), OrigoFSError> {
        for h in self.handles_of(ino) {
            let mut buf = h.buf.lock().unwrap_or_else(PoisonError::into_inner);
            if buf.overlaps(offset, size) {
                self.flush_buf(ino, &mut buf)?;
            }
        }
        Ok(())
    }

    /// The highest buffered byte offset across every handle on `ino`.
    fn dirty_end(&self, ino: i64) -> Option<u64> {
        self.handles_of(ino)
            .iter()
            .filter_map(|h| h.buf.lock().unwrap_or_else(PoisonError::into_inner).end())
            .max()
    }

    /// [`to_attr`], with the file's size widened to include bytes still sitting in
    /// a handle's buffer.
    ///
    /// Without this, a `write` followed by a `stat` would report the pre-write
    /// size, and — worse — the kernel would clamp subsequent reads to it and never
    /// ask us for the tail at all. With it, a read past the stored end reaches
    /// [`Filesystem::read`], which flushes the overlapping buffer and answers from
    /// the store.
    fn attr_of(&self, i: &Inode) -> FileAttr {
        let mut a = to_attr(i);
        if i.kind == FileKind::File
            && let Some(end) = self.dirty_end(i.ino)
            && end > a.size
        {
            a.size = end;
            a.blocks = end.div_ceil(512);
        }
        a
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
    ///
    /// Idempotent: the second call finds `done` already taken and returns at once,
    /// which is what lets [`Mount::unmount`] and [`Drop`] both call it.
    pub(crate) fn shutdown(&self) {
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
/// The prerequisite #75 named for revisiting this — "a mount guard that stops the
/// watcher *before* the unmount rather than after" — now exists: [`Mount`], which
/// is why `spawn` returns it instead of a bare `BackgroundSession`. With that in
/// place `inval_entry` is available behind `ORIGOFS_FUSE_INVAL_ENTRY=1` and stays
/// **off by default**; see [`inval_entry_enabled`] for what the evidence does and
/// does not yet support.
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
    let Some((parent_path, name)) = split_parent(path) else {
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
    // Opt-in dentry invalidation (issue #75). Off by default — see
    // `inval_entry_enabled` for exactly what is and is not known about it.
    if inval_entry_enabled()
        && let Err(e) = notifier.inval_entry(parent, name.as_ref())
    {
        tracing::debug!(path, error = %e, "fuse: inval_entry rejected");
    }
}

/// Whether to issue `FUSE_NOTIFY_INVAL_ENTRY` alongside the attribute
/// invalidation, via `ORIGOFS_FUSE_INVAL_ENTRY=1` (issue #75).
///
/// **Default off, and the reason is evidence rather than caution.** An earlier
/// revision issued it unconditionally and wedged the whole process roughly one run
/// in eight — a watcher thread parked in `fuse_reverse_inval_entry`, a caller in
/// `request_wait_answer` holding the lock it wanted, and a session thread that
/// never got to answer. A `D`-state thread cannot be killed, so the process
/// survives `SIGKILL` and leaves the mount behind.
///
/// #75 named the prerequisite for revisiting it: "a mount guard that stops the
/// watcher *before* the unmount rather than after". [`Mount`] is that guard, so the
/// prerequisite is now met, and `tests/fuse_teardown.rs` exercises 20 teardowns
/// under concurrent kernel and remote traffic per run.
///
/// What is known: with the guard in place and this enabled, 40 such cycles ran
/// clean on this kernel. What is **not** known: the original failure was
/// probabilistic and the interaction is kernel-version-dependent, so 40 cycles in
/// one environment is suggestive, not proof. Flipping the default deserves a
/// wider matrix than a single container, and the cost of being wrong — an
/// unkillable process — is high enough that "probably fine" is not the bar.
///
/// So the knob exists to *gather* that evidence, not to hide a decision. Until it
/// is gathered, the default keeps the honest, bounded behaviour: a name the kernel
/// has already resolved keeps resolving for up to [`TTL`] after a remote
/// create/delete/rename. One second, not forever.
fn inval_entry_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("ORIGOFS_FUSE_INVAL_ENTRY").is_ok_and(|v| v == "1" || v == "true")
    })
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
    mount_as(ws, mountpoint, None)
}

/// [`mount`], for a mount bound to an actor (issue #141).
pub fn mount_as(ws: Workspace, mountpoint: &Path, ctx: Option<WriteCtx>) -> std::io::Result<()> {
    spawn_as(ws, mountpoint, ctx)?.join()
}

/// A live background mount, which unmounts when dropped (issue #75).
///
/// # Why this exists rather than returning `BackgroundSession` directly
///
/// **Teardown order.** The change-feed watcher issues kernel notifications, and a
/// notification can only be safely in flight while the session is still able to
/// answer requests. Previously the watcher was owned by the filesystem, which the
/// session owned, so it was stopped *by* the unmount — that is, after it. The
/// window between "session torn down" and "watcher noticed" is exactly where a
/// notification has nobody left to answer it.
///
/// That ordering is the prerequisite #75 names for issuing
/// `FUSE_NOTIFY_INVAL_ENTRY` at all, which takes the parent directory's `i_rwsem`
/// exclusively and therefore parks in uninterruptible `D` state if the mount
/// cannot answer — a state that survives `SIGKILL`. See [`invalidate`] for the
/// full history.
///
/// So this guard stops the watcher **first**, waits for it, and only then drops
/// the session. `unmount` does it explicitly; `Drop` does it for anyone who does
/// not call it.
pub struct Mount {
    watcher: Arc<Watcher>,
    /// `Option` only so [`Drop`] can take it and control when the unmount happens
    /// relative to stopping the watcher.
    session: Option<BackgroundSession>,
}

impl Mount {
    /// Unmount, stopping the change-feed watcher before the session goes.
    ///
    /// Idempotent, and the same thing [`Drop`] does — call it when you want the
    /// unmount to happen at a known point, or to keep the handle alive afterwards.
    pub fn unmount(&mut self) {
        // Order is the whole point: no notification can be in flight once this
        // returns, so tearing the session down cannot strand one.
        self.watcher.shutdown();
        drop(self.session.take());
    }

    /// Block until the session ends (an external `umount(8)`, or a session
    /// failure). Consumes the handle, since the mount is over when this returns.
    pub fn join(mut self) -> std::io::Result<()> {
        let session = self.session.take();
        let r = match session {
            Some(s) => s
                .guard
                .join()
                .map_err(|_| std::io::Error::other("FUSE session thread panicked"))?,
            None => Ok(()),
        };
        self.watcher.shutdown();
        r
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        self.unmount();
    }
}

/// Mount in the background; the returned [`Mount`] unmounts on drop.
///
/// Also starts the [`Watcher`] that invalidates kernel caches from the change
/// feed. The returned guard stops it **before** the unmount — see [`Mount`] for
/// why that ordering is load-bearing rather than tidy.
pub fn spawn(ws: Workspace, mountpoint: &Path) -> std::io::Result<Mount> {
    spawn_as(ws, mountpoint, None)
}

/// [`spawn`], for a mount bound to an actor (issue #141).
///
/// Every operation the kernel sends is then checked against the grants covering
/// the path it touches, so a mount stops being the way around the ACLs that
/// govern every other surface. Passing `None` is the anonymous mount [`spawn`]
/// gives you.
///
/// This authorizes; it does not attribute. Writes through the mount still record
/// no `edit_op` and no blame.
pub fn spawn_as(ws: Workspace, mountpoint: &Path, ctx: Option<WriteCtx>) -> std::io::Result<Mount> {
    let fs = OrigoFSFuse::new_as(ws.clone(), ctx)?;
    // Grabbed before `fs` is handed to the session, which is what gives us the
    // notifier to hand back to it.
    let watcher = Arc::clone(&fs.watcher);
    let session = fuser::spawn_mount2(fs, mountpoint, &config())?;
    watcher.start(ws, session.notifier());
    Ok(Mount {
        watcher,
        session: Some(session),
    })
}

/// Map the kernel's `FALLOC_FL_*` bits onto what the engine can honour.
///
/// `None` means "this filesystem does not do that", which the caller turns into
/// `EOPNOTSUPP` — the answer the syscall documents for a mode a filesystem does
/// not support. `COLLAPSE_RANGE` and `INSERT_RANGE` shift every byte after the
/// range, and approximating either would silently corrupt a file, so they are
/// refused rather than attempted.
#[cfg(target_os = "linux")]
fn allocate_mode(mode: i32) -> Option<AllocateMode> {
    const KEEP_SIZE: i32 = libc::FALLOC_FL_KEEP_SIZE;
    const PUNCH_HOLE: i32 = libc::FALLOC_FL_PUNCH_HOLE;
    const ZERO_RANGE: i32 = libc::FALLOC_FL_ZERO_RANGE;
    match mode {
        0 => Some(AllocateMode::Allocate),
        m if m == KEEP_SIZE => Some(AllocateMode::KeepSize),
        // Punching requires `KEEP_SIZE`; the kernel rejects it without, so accept
        // either spelling rather than second-guessing what arrived.
        m if m == PUNCH_HOLE || m == PUNCH_HOLE | KEEP_SIZE => Some(AllocateMode::PunchHole),
        m if m == ZERO_RANGE || m == ZERO_RANGE | KEEP_SIZE => Some(AllocateMode::ZeroRange),
        _ => None,
    }
}

/// Non-Linux mounts see only the portable modes; the rest are Linux extensions.
#[cfg(not(target_os = "linux"))]
fn allocate_mode(mode: i32) -> Option<AllocateMode> {
    match mode {
        0 => Some(AllocateMode::Allocate),
        _ => None,
    }
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
/// The longest single path component a mount reports supporting, for `statfs`.
///
/// 255 bytes is what every common Linux filesystem reports and what tools expect;
/// origofs itself imposes no component-length limit beyond `validate_component`.
const MAX_NAME_LEN: u32 = 255;

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
    /// `fcntl(F_GETLK)` — report what would block this range, if anything.
    fn getlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: ReplyLock,
    ) {
        let Some(kind) = lock_kind(typ) else {
            return reply.error(Errno::EINVAL);
        };
        let (s, e) = lock_range(start, end);
        let req = self.lock_request(lock_owner, pid, s, e, kind);
        match self.blk(self.ws.fs().vfs_getlk_as(self.ctx, ino.0 as i64, &req)) {
            // `ENOSYS` is not a failure here: it is how the kernel is told to go
            // back to handling advisory locks locally, which is what every mount
            // did before this feature and what one with the switch off still does.
            Ok(LockAnswer::NotEnabled) => reply.error(Errno::ENOSYS),
            Ok(LockAnswer::Free) => reply.locked(start, end, unlock_type(), pid),
            Ok(LockAnswer::Held(l)) => reply.locked(
                l.start as u64,
                l.end as u64,
                if l.exclusive {
                    write_type()
                } else {
                    read_type()
                },
                l.pid as u32,
            ),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// `fcntl(F_SETLK)` / `F_SETLKW` — take, downgrade or release a range.
    fn setlk(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        let Some(kind) = lock_kind(typ) else {
            return reply.error(Errno::EINVAL);
        };
        let (s, e) = lock_range(start, end);
        let req = self.lock_request(lock_owner, pid, s, e, kind);
        let ino = ino.0 as i64;
        match self.blk(self.ws.fs().vfs_setlk_as(self.ctx, ino, &req)) {
            Ok(LockAnswer::NotEnabled) => reply.error(Errno::ENOSYS),
            Ok(LockAnswer::Free) => reply.ok(),
            // `F_SETLK` says "fail rather than wait", and `EAGAIN` is what that
            // failure is spelled as.
            Ok(LockAnswer::Held(_)) if !sleep => reply.error(Errno::EAGAIN),
            Ok(LockAnswer::Held(_)) => self.wait_for_lock(ino, req, reply),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn destroy(&mut self) {
        // Drop this mount's advisory locks. They are rows in a shared table, so
        // leaving them would block other mounts until the lease ran out — the
        // lease is the safety net for a crash, not the ordinary path.
        if let Err(e) = self.blk(
            self.ws
                .fs()
                .release_posix_locks_for_holder(&self.holder.clone()),
        ) {
            tracing::warn!(error = %e, "fuse: could not release advisory locks at unmount");
        }
        // Last chance for anything still buffered: after this the session is gone
        // and nobody will call `release`. Nothing here can be reported to a
        // caller, so a failure is logged rather than swallowed silently.
        let handles: Vec<Arc<OpenFile>> = {
            let mut t = self.handles.lock().unwrap_or_else(PoisonError::into_inner);
            t.by_ino.clear();
            t.open.drain().map(|(_, h)| h).collect()
        };
        for h in handles {
            if let Err(e) = self.flush_handle(&h) {
                tracing::warn!(ino = h.ino, error = %e, "fuse: buffered writes lost at unmount");
            }
        }
        self.watcher.shutdown();
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_lookup_as(self.ctx, parent.0 as i64, &name)) {
            Ok(Some(i)) => reply.entry(&TTL, &self.attr_of(&i), Generation(0)),
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.blk(self.ws.fs().vfs_getattr_as(self.ctx, ino.0 as i64)) {
            Ok(i) => reply.attr(&TTL, &self.attr_of(&i)),
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
        if let Some(sz) = size {
            // Buffered writes have to land *before* the truncate, or a `write`
            // followed by an `ftruncate` on the same descriptor would flush at
            // close and resurrect the bytes the truncate was supposed to drop.
            if let Err(e) = self.flush_ino(ino) {
                reply.error(errno(&e));
                return;
            }
            if let Err(e) = self.blk(self.ws.fs().vfs_truncate_as(self.ctx, ino, sz)) {
                reply.error(errno(&e));
                return;
            }
        }
        // Mode and ownership used to be bound as `_mode`/`_uid`/`_gid` and dropped,
        // after which this replied with freshly-read (unchanged) attributes — so a
        // `chmod` reported success and moved nothing (#121, #122). Apply them.
        if let Some(m) = mode
            && let Err(e) = self.blk(self.ws.fs().vfs_chmod_as(self.ctx, ino, m))
        {
            reply.error(errno(&e));
            return;
        }
        // One call for both halves: `chown` and `chgrp` each send only their own,
        // and `vfs_chown` treats `None` as chown(2)'s -1 ("leave alone").
        if (uid.is_some() || gid.is_some())
            && let Err(e) = self.blk(self.ws.fs().vfs_chown_as(self.ctx, ino, uid, gid))
        {
            reply.error(errno(&e));
            return;
        }
        match self.blk(self.ws.fs().vfs_getattr_as(self.ctx, ino)) {
            Ok(i) => reply.attr(&TTL, &self.attr_of(&i)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.blk(self.ws.fs().vfs_readlink_as(self.ctx, ino.0 as i64)) {
            Ok(t) => reply.data(t.as_bytes()),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Open a file, allocating the handle the buffering below hangs off (issue
    /// #112).
    ///
    /// Until this existed there was no `open` at all, so `fuser` answered with the
    /// default `FileHandle(0)` and this filesystem had nowhere to keep per-handle
    /// state — which is why every write request the kernel issued became a separate
    /// whole-file read-modify-write. See `OpenFile` for what a handle holds, and
    /// for the actor context it deliberately does *not*.
    ///
    /// No `getattr` is issued to validate `ino`: the kernel only reaches `open`
    /// through a successful `lookup`, and a file unlinked in between is one POSIX
    /// requires to stay openable through the reference the caller already has.
    /// Allocating a handle is pure in-memory bookkeeping, so `open` stays free.
    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(self.open_handle(ino.0 as i64), FopenFlags::empty());
    }

    /// Read, first flushing any buffered write that overlaps the requested range.
    ///
    /// # Read-your-own-writes
    ///
    /// This is the trap buffering introduces: a process that writes and then reads
    /// back without closing must not be served the pre-write bytes. Rather than
    /// overlay the dirty runs onto the reply — which would need the buffer and the
    /// stored body stitched together on a hot path, and would still leave `getattr`
    /// to fix — the read simply *flushes first*, so the store is authoritative by
    /// the time it is asked. It considers every handle on the inode, not just the one
    /// the read came in on, because a second descriptor (or a second process)
    /// holding dirty bytes for the same file would otherwise be just as invisible.
    ///
    /// Overlap is the exact condition, and it is worth saying why a read *below*
    /// the dirty region needs no flush even when the buffer has extended the file
    /// past the stored end. The store answers such a read short; FUSE specifies
    /// that a short reply is zero-filled up to the requested size, and
    /// `OrigoFSFuse::attr_of` has already told the kernel the larger size — so
    /// the hole reads back as the zeroes it is.
    ///
    /// The cost is that an alternating read/write workload degrades to a flush per
    /// read, i.e. to precisely the behaviour this mount had before buffering
    /// existed. Sequential writes — the case the issue is about — never pay it.
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
        let ino = ino.0 as i64;
        if self.any_open()
            && let Err(e) = self.flush_overlapping(ino, offset, size)
        {
            reply.error(errno(&e));
            return;
        }
        match self.blk(self.ws.fs().vfs_read_as(self.ctx, ino, offset, size)) {
            Ok(b) => reply.data(&b),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Write into the handle's buffer, flushing only when it fills (issue #112).
    ///
    /// Accumulating here is the whole point: `Fs::vfs_write` rewrites the entire
    /// file per request, so answering each of the kernel's requests directly made
    /// the cost of writing a file its size times the number of writes it took.
    /// Contiguous requests coalesce into one run ([`DirtyBuffer`]) and go out as a
    /// single read-modify-write once `HANDLE_BUFFER_CAP` is reached — see
    /// `OrigoFSFuse::flush_buf` for why that does not weaken the compare-and-set
    /// guard `vfs_write` uses against concurrent writers.
    ///
    /// The cap is what keeps a handle that is never flushed — a process killed
    /// mid-write — from growing without bound. Losing whatever is still buffered
    /// at that point is inherent to buffering and is the same bargain every
    /// page-cached filesystem makes; `flush`, `fsync` and `release` are the points
    /// at which it is paid off.
    ///
    /// Replying `written` before the bytes reach the store means a store error is
    /// reported at `flush`/`fsync`/`close` instead of at `write(2)`. That is the
    /// deferral POSIX explicitly allows, and it is why none of those three
    /// swallows an error. When a *cap* flush fails the error is reported here
    /// instead, and the buffer keeps every byte — including this call's — so the
    /// failing `write(2)` may still turn out to have landed once a later flush
    /// succeeds. Retaining the data is the lesser surprise: the alternative is
    /// discarding bytes the caller has no way to know were discarded.
    ///
    /// An unrecognized `fh` falls through to the direct path: the kernel documents
    /// a page-cache write-back as carrying a `fh` that need not match `open`'s, and
    /// serving that from a handle we cannot identify would be a guess. It flushes
    /// the inode first so the direct write, being the later operation, lands last.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let ino = ino.0 as i64;
        if let Some(h) = self.handle(fh).filter(|h| h.ino == ino) {
            let mut buf = h.buf.lock().unwrap_or_else(PoisonError::into_inner);
            if let Err(e) = buf.write_at(offset, data) {
                reply.error(errno(&e));
                return;
            }
            if buf.len() >= HANDLE_BUFFER_CAP
                && let Err(e) = self.flush_buf(ino, &mut buf)
            {
                reply.error(errno(&e));
                return;
            }
            reply.written(data.len() as u32);
            return;
        }
        if self.any_open()
            && let Err(e) = self.flush_ino(ino)
        {
            reply.error(errno(&e));
            return;
        }
        match self.blk(self.ws.fs().vfs_write_as(self.ctx, ino, offset, data)) {
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
            let page = match self.blk(self.ws.fs().vfs_readdir_page_as(
                self.ctx,
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

    /// Create and open in one step.
    ///
    /// The handle returned is a real one, allocated exactly as `Filesystem::open`
    /// would: this used to hand back `FileHandle(0)`, which meant the descriptor a
    /// freshly-created file was written through — the overwhelmingly common way a
    /// file gets written on a mount — had no state to buffer into.
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
        match self.blk(self.ws.fs().vfs_create_as(
            self.ctx,
            parent.0 as i64,
            &name,
            mode,
            caller_owner(req),
        )) {
            Ok(i) => {
                let fh = self.open_handle(i.ino);
                reply.created(&TTL, &to_attr(&i), Generation(0), fh, FopenFlags::empty());
            }
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
        match self.blk(self.ws.fs().vfs_mkdir_as(
            self.ctx,
            parent.0 as i64,
            &name,
            mode,
            caller_owner(req),
        )) {
            Ok(i) => reply.entry(&TTL, &to_attr(&i), Generation(0)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Unlink, first landing anything buffered for the file being removed.
    ///
    /// Otherwise a `rm` of a file some descriptor still holds dirty bytes for
    /// would leave that descriptor's `release` writing into an inode the store has
    /// already deleted — an `ENOENT` reported nowhere useful, and a confusing one.
    /// Flushing first makes the removal the last word. The extra lookup is skipped
    /// entirely unless this mount has a file open at all.
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        if self.any_open() {
            match self.blk(self.ws.fs().vfs_lookup_as(self.ctx, parent.0 as i64, &name)) {
                Ok(Some(i)) => {
                    if let Err(e) = self.flush_ino(i.ino) {
                        reply.error(errno(&e));
                        return;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            }
        }
        match self.blk(self.ws.fs().vfs_unlink_as(self.ctx, parent.0 as i64, &name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_rmdir_as(self.ctx, parent.0 as i64, &name)) {
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
        match self.blk(self.ws.fs().vfs_rename_as(
            self.ctx,
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
        match self.blk(self.ws.fs().vfs_symlink_as(
            self.ctx,
            parent.0 as i64,
            &name,
            &target,
            caller_owner(req),
        )) {
            Ok(i) => reply.entry(&TTL, &to_attr(&i), Generation(0)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Hard link (issue #119). `git` uses these, and several editors save via
    /// `rename`+`link`; without it both got a confusing failure rather than a
    /// clean one.
    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let newname = newname.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_link_as(
            self.ctx,
            ino.0 as i64,
            newparent.0 as i64,
            &newname,
        )) {
            Ok(i) => reply.entry(&TTL, &self.attr_of(&i), Generation(0)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// `statfs` (issues #116, #119) — what `df` reads, and what some installers
    /// refuse to run without. See `Fs::statfs` for what the totals mean when no
    /// quota is set.
    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        match self.blk(self.ws.fs().statfs()) {
            Ok(s) => reply.statfs(
                s.total_blocks,
                s.free_blocks,
                // bavail (space available to an unprivileged user) is the same as
                // bfree here: origofs reserves nothing for root.
                s.free_blocks,
                s.total_inodes,
                s.free_inodes,
                s.block_size,
                MAX_NAME_LEN,
                s.block_size,
            ),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Set an extended attribute (issue #119).
    ///
    /// `position` is a macOS resource-fork offset; a non-zero one asks for a
    /// partial write into a value, which is not supported rather than silently
    /// treated as a whole-value write.
    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        _flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if position != 0 {
            reply.error(Errno::EINVAL);
            return;
        }
        let name = name.to_string_lossy().to_string();
        match self.blk(
            self.ws
                .fs()
                .vfs_setxattr_as(self.ctx, ino.0 as i64, &name, value),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Get an extended attribute (issue #119).
    ///
    /// The two-call protocol: `size == 0` asks only for the length, and a `size`
    /// too small for the value is `ERANGE` — a caller sizes its buffer from the
    /// first form and would otherwise silently get a truncated value.
    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let name = name.to_string_lossy().to_string();
        match self.blk(self.ws.fs().vfs_getxattr_as(self.ctx, ino.0 as i64, &name)) {
            Ok(Some(v)) => {
                if size == 0 {
                    reply.size(v.len() as u32);
                } else if (v.len() as u32) > size {
                    reply.error(Errno::ERANGE);
                } else {
                    reply.data(&v);
                }
            }
            // "no such attribute" is ENODATA, distinct from "no such file".
            Ok(None) => reply.error(Errno::ENODATA),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// List extended attribute names (issue #119). The reply is the
    /// NUL-separated, NUL-terminated form `listxattr(2)` specifies.
    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.blk(self.ws.fs().vfs_listxattr_as(self.ctx, ino.0 as i64)) {
            Ok(names) => {
                let mut buf = Vec::new();
                for n in names {
                    buf.extend_from_slice(n.as_bytes());
                    buf.push(0);
                }
                if size == 0 {
                    reply.size(buf.len() as u32);
                } else if (buf.len() as u32) > size {
                    reply.error(Errno::ERANGE);
                } else {
                    reply.data(&buf);
                }
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// Remove an extended attribute (issue #119). Removing a name that was never
    /// set is `ENODATA`, not success.
    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        match self.blk(
            self.ws
                .fs()
                .vfs_removexattr_as(self.ctx, ino.0 as i64, &name),
        ) {
            Ok(true) => reply.ok(),
            Ok(false) => reply.error(Errno::ENODATA),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// `close(2)` — write the handle's buffer out.
    ///
    /// The kernel calls this once per `close` of a descriptor, so a `dup`'d fd
    /// produces several `flush`es for one `open`. That is why the flush is
    /// idempotent rather than "write and clear": the first call empties the
    /// buffer, every later one finds nothing and succeeds. It is also why the
    /// handle is *not* forgotten here — `release` is the one-per-`open` callback,
    /// and dropping the state at the first `close` would leave a still-valid
    /// descriptor writing into a handle this mount no longer knows.
    ///
    /// Errors are reported. This is the callback whose whole documented purpose is
    /// to give a filesystem somewhere to return deferred write errors, and with
    /// buffering there are now deferred write errors to return.
    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.handle(fh) {
            // An unknown handle has nothing buffered by construction.
            None => reply.ok(),
            Some(h) => match self.flush_handle(&h) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            },
        }
    }

    /// `fsync(2)` — the caller is asking for durability, so the buffer goes out.
    ///
    /// `datasync` is ignored: origofs stores an inode's bytes and its metadata in
    /// the same transaction, so there is no metadata-only half to skip.
    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Fall back to the inode when the handle is unknown (an `fsync` after an
        // `mmap` write-back can carry one), so the data still lands.
        let flushed = match self.handle(fh) {
            Some(h) => self.flush_handle(&h),
            None => self.flush_ino(ino.0 as i64),
        };
        match flushed {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// The last reference to an open file is gone: flush and forget the handle.
    ///
    /// Exactly one `release` follows each `open`, so this is the point at which
    /// per-handle state must be dropped — and the last point at which its buffer
    /// can still be written. The handle is removed whether or not the flush
    /// succeeds, since keeping it would leak a buffer nobody can ever reach again;
    /// a failure is both returned and logged, because `fuser` documents that a
    /// `release` error never reaches the `close(2)` that triggered it.
    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Some(h) = self.close_handle(fh) else {
            reply.ok();
            return;
        };
        match self.flush_handle(&h) {
            Ok(()) => reply.ok(),
            Err(e) => {
                tracing::warn!(ino = h.ino, error = %e, "fuse: flush at release failed");
                reply.error(errno(&e));
            }
        }
    }

    /// `fallocate(2)`. Unsupported modes are refused rather than approximated.
    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let Some(mode) = allocate_mode(mode) else {
            return reply.error(Errno::EOPNOTSUPP);
        };
        // Buffered writes for this inode have to land first: the engine works from
        // the stored body, so a pending buffer would be written back afterwards and
        // silently undo the hole (issue #112's buffering, this operation's problem).
        if let Err(e) = self.flush_ino(ino.0 as i64) {
            return reply.error(errno(&e));
        }
        match self.blk(
            self.ws
                .fs()
                .vfs_allocate_as(self.ctx, ino.0 as i64, offset, length, mode),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    /// `copy_file_range(2)` — served by referencing the source's chunks rather
    /// than copying bytes, which is the whole reason to implement it.
    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        _fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        _fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        // Both sides must be on disk before the manifests are read and rewritten.
        for ino in [ino_in, ino_out] {
            if let Err(e) = self.flush_ino(ino.0 as i64) {
                return reply.error(errno(&e));
            }
        }
        match self.blk(self.ws.fs().vfs_copy_range_as(
            self.ctx,
            ino_in.0 as i64,
            offset_in,
            ino_out.0 as i64,
            offset_out,
            len,
        )) {
            // A short copy is normal: the kernel re-issues from where this stopped.
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(errno(&e)),
        }
    }
}
