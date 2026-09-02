//! POSIX advisory byte-range locks — the `fcntl(F_SETLK)` family (issue #119).
//!
//! # Not the `lock` next door
//!
//! origofs already had a `lock` before this one, and they are different objects
//! that share a word. [`Fs::lock`](crate::Fs::lock) is a durable, named,
//! git-LFS-style claim on a *path*: a person takes it so nobody else edits a
//! binary, and it outlives every process involved. This one is
//! per-open-file-description, byte-ranged, addressed by inode, and dies with the
//! process holding it. Neither can be expressed in the other's table, so they do
//! not share one.
//!
//! # Why any of this is stored at all
//!
//! A FUSE filesystem that does not implement `setlk` still gets working advisory
//! locks — the kernel serves them locally, per mount. So an in-process table
//! would reimplement what already works. The only thing missing, and the only
//! reason to answer `setlk`, is coordination *between* mounts: two processes, two
//! machines, one workspace. That has to live where both can see it, which is the
//! metadata store.
//!
//! # This module is the semantics, and nothing else
//!
//! [`resolve`] is pure: existing locks in, a decision out. It touches no
//! database, which is deliberate — POSIX range semantics are where this gets
//! subtle (splitting a lock somebody re-locks the middle of, downgrading half of
//! an exclusive range, an unlock that punches a hole) and that is worth testing
//! directly rather than through two backends. Each backend's job is only to run
//! it inside a transaction.

/// End-of-file in a lock range. POSIX spells an open-ended lock `l_len == 0`;
/// stored ranges are closed, so the open end becomes this.
pub const LOCK_EOF: i64 = i64::MAX;

/// What a caller is asking for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockKind {
    /// `F_RDLCK` — any number may overlap, none may overlap an exclusive one.
    Shared,
    /// `F_WRLCK` — excludes every other owner over the range.
    Exclusive,
    /// `F_UNLCK` — drop whatever this owner holds over the range.
    Unlock,
}

/// One held range. `end` is **inclusive**; [`LOCK_EOF`] means to end-of-file.
///
/// `owner` is the kernel's lock owner, which is the open file description rather
/// than the process — the two differ after `fork` and for threads, and POSIX is
/// explicit that ownership follows the description. `holder` is the mount
/// instance, carried so a clean unmount can drop its rows in one statement and a
/// crashed one can be reaped by lease expiry. `pid` is reported back by `getlk`
/// and is not part of identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PosixLock {
    pub owner: String,
    pub holder: String,
    pub pid: i64,
    pub start: i64,
    pub end: i64,
    pub exclusive: bool,
}

/// How long a mount's locks survive without a renewal.
///
/// A durable table cannot be cleaned up by a process that has died, so rows carry
/// a lease and a live mount pushes it out. Long enough that renewal is cheap,
/// short enough that a `kill -9` does not wedge a byte range for a working day.
pub const LEASE_SECS: i64 = 60;

/// Workspace config key for the opt-in switch.
pub const ENABLED_KEY: &str = "posix.locks_enabled";

/// The answer to a lock question, including "this workspace does not answer".
///
/// `NotEnabled` is a real outcome rather than an error: a mount turns it into
/// `ENOSYS`, which is how the kernel is told to go back to handling advisory locks
/// locally. Spelling it as a failure would make the off state look like a fault.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockAnswer {
    /// Advisory locking is off for this workspace; fall back.
    NotEnabled,
    /// `getlk`: nothing blocks the range. `setlk`: the request was applied.
    Free,
    /// `getlk`: this lock blocks it. `setlk`: this lock refused it, nothing changed.
    Held(PosixLock),
}

/// A `setlk`/`getlk` request.
#[derive(Clone, Debug)]
pub struct LockRequest {
    pub owner: String,
    pub holder: String,
    pub pid: i64,
    pub start: i64,
    pub end: i64,
    pub kind: LockKind,
}

/// The decision: either a blocker, or an exact edit to apply.
///
/// `delete` addresses rows by `(owner, start)` because that is their key — one
/// owner's ranges on one inode never overlap, so `start` identifies a row.
#[derive(Clone, Debug, Default)]
pub struct Resolution {
    /// The lock that refuses the request. `Some` means nothing is written.
    pub conflict: Option<PosixLock>,
    pub delete: Vec<(String, i64)>,
    pub insert: Vec<PosixLock>,
}

fn overlaps(l: &PosixLock, start: i64, end: i64) -> bool {
    l.start <= end && start <= l.end
}

/// Ranges that touch without overlapping, guarding the `+ 1` at [`LOCK_EOF`].
fn adjacent(l: &PosixLock, start: i64, end: i64) -> bool {
    (l.end != LOCK_EOF && l.end + 1 == start) || (end != LOCK_EOF && end + 1 == l.start)
}

/// Decide what `req` does to `existing` (every lock currently held on the inode).
///
/// Callers must pass *live* locks only — an expired lease is not a blocker, and
/// filtering it here would need a clock this function deliberately does not have.
pub fn resolve(existing: &[PosixLock], req: &LockRequest) -> Resolution {
    let (s, e) = (req.start, req.end);

    // A conflict is another owner's overlapping lock where either side wants
    // exclusivity. Two shared readers over the same bytes are the whole point of
    // `F_RDLCK`, so they are not a conflict. Unlocking never conflicts: it only
    // ever removes this owner's own ranges.
    if req.kind != LockKind::Unlock {
        for l in existing {
            if l.owner != req.owner
                && overlaps(l, s, e)
                && (l.exclusive || req.kind == LockKind::Exclusive)
            {
                return Resolution {
                    conflict: Some(l.clone()),
                    ..Default::default()
                };
            }
        }
    }

    let want_exclusive = req.kind == LockKind::Exclusive;
    let mine: Vec<&PosixLock> = existing.iter().filter(|l| l.owner == req.owner).collect();

    // Absorb this owner's same-type ranges that touch the request, so repeatedly
    // locking consecutive ranges leaves one row rather than thousands. Run to a
    // fixpoint: absorbing one range can bring the next into contact.
    let (mut ns, mut ne) = (s, e);
    if req.kind != LockKind::Unlock {
        loop {
            let mut grew = false;
            for l in &mine {
                if l.exclusive == want_exclusive
                    && (overlaps(l, ns, ne) || adjacent(l, ns, ne))
                    && (l.start < ns || l.end > ne)
                {
                    ns = ns.min(l.start);
                    ne = ne.max(l.end);
                    grew = true;
                }
            }
            if !grew {
                break;
            }
        }
    }

    let mut delete = Vec::new();
    let mut insert = Vec::new();

    for l in &mine {
        let absorbed =
            req.kind != LockKind::Unlock && l.exclusive == want_exclusive && overlaps(l, ns, ne);
        let replaced = overlaps(l, s, e);
        if !absorbed && !replaced {
            continue;
        }
        delete.push((l.owner.clone(), l.start));
        if absorbed {
            // Its bytes are inside the range about to be written; nothing survives.
            continue;
        }
        // A different-type range of this owner's: the request replaces it over the
        // overlap, and the ends outside stay exactly as they were. This is the
        // splitting case — locking the middle of your own range leaves two.
        if l.start < s {
            insert.push(PosixLock {
                end: s - 1,
                ..(*l).clone()
            });
        }
        if l.end > e {
            insert.push(PosixLock {
                start: e + 1,
                ..(*l).clone()
            });
        }
    }

    if req.kind != LockKind::Unlock {
        insert.push(PosixLock {
            owner: req.owner.clone(),
            holder: req.holder.clone(),
            pid: req.pid,
            start: ns,
            end: ne,
            exclusive: want_exclusive,
        });
    }

    Resolution {
        conflict: None,
        delete,
        insert,
    }
}

/// The lock that would block `req`, or `None` if it would be granted — `getlk`.
pub fn test(existing: &[PosixLock], req: &LockRequest) -> Option<PosixLock> {
    resolve(existing, req).conflict
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock(owner: &str, start: i64, end: i64, exclusive: bool) -> PosixLock {
        PosixLock {
            owner: owner.into(),
            holder: "h".into(),
            pid: 1,
            start,
            end,
            exclusive,
        }
    }

    fn req(owner: &str, start: i64, end: i64, kind: LockKind) -> LockRequest {
        LockRequest {
            owner: owner.into(),
            holder: "h".into(),
            pid: 1,
            start,
            end,
            kind,
        }
    }

    /// The conflict matrix, as a table so a missing case reads as a gap.
    #[test]
    fn conflict_rules() {
        use LockKind::{Exclusive, Shared};
        // (held type, requested type, same owner, expect conflict)
        let cases = [
            (false, Shared, false, false),   // reader vs reader: fine
            (false, Exclusive, false, true), // writer vs reader: blocked
            (true, Shared, false, true),     // reader vs writer: blocked
            (true, Exclusive, false, true),  // writer vs writer: blocked
            (true, Exclusive, true, false),  // your own lock never blocks you
            (true, Shared, true, false),
        ];
        for (held_excl, want, same_owner, expect) in cases {
            let held = vec![lock("a", 0, 99, held_excl)];
            let who = if same_owner { "a" } else { "b" };
            let got = test(&held, &req(who, 50, 149, want)).is_some();
            assert_eq!(
                got, expect,
                "held_excl={held_excl} want={want:?} same_owner={same_owner}"
            );
        }
    }

    #[test]
    fn non_overlapping_ranges_do_not_conflict() {
        let held = vec![lock("a", 0, 9, true)];
        assert!(test(&held, &req("b", 10, 19, LockKind::Exclusive)).is_none());
        // Touching at the boundary is still not overlapping.
        assert!(test(&held, &req("b", 9, 19, LockKind::Exclusive)).is_some());
    }

    #[test]
    fn locking_the_middle_of_your_own_range_splits_it() {
        let held = vec![lock("a", 0, 99, false)];
        let r = resolve(&held, &req("a", 40, 59, LockKind::Exclusive));
        assert!(r.conflict.is_none());
        assert_eq!(r.delete, vec![("a".to_string(), 0)]);
        let mut got: Vec<(i64, i64, bool)> = r
            .insert
            .iter()
            .map(|l| (l.start, l.end, l.exclusive))
            .collect();
        got.sort();
        assert_eq!(got, vec![(0, 39, false), (40, 59, true), (60, 99, false)]);
    }

    #[test]
    fn unlocking_the_middle_punches_a_hole() {
        let held = vec![lock("a", 0, 99, true)];
        let r = resolve(&held, &req("a", 40, 59, LockKind::Unlock));
        assert_eq!(r.delete, vec![("a".to_string(), 0)]);
        let mut got: Vec<(i64, i64)> = r.insert.iter().map(|l| (l.start, l.end)).collect();
        got.sort();
        assert_eq!(got, vec![(0, 39), (60, 99)]);
    }

    #[test]
    fn adjacent_same_type_ranges_coalesce() {
        // Without this, a process locking record after record grows a row per record.
        let held = vec![lock("a", 0, 9, true), lock("a", 20, 29, true)];
        let r = resolve(&held, &req("a", 10, 19, LockKind::Exclusive));
        assert_eq!(
            r.insert.len(),
            1,
            "should collapse to one range: {:?}",
            r.insert
        );
        assert_eq!((r.insert[0].start, r.insert[0].end), (0, 29));
        assert_eq!(r.delete.len(), 2);
    }

    #[test]
    fn an_open_ended_range_does_not_overflow() {
        let held = vec![lock("a", 100, LOCK_EOF, true)];
        // Adjacency against EOF must not compute LOCK_EOF + 1.
        let r = resolve(&held, &req("a", 0, 99, LockKind::Exclusive));
        assert!(r.conflict.is_none());
        assert_eq!(r.insert.len(), 1);
        assert_eq!((r.insert[0].start, r.insert[0].end), (0, LOCK_EOF));
        // And a zero-start unlock must not compute `s - 1`.
        let held = vec![lock("a", 0, LOCK_EOF, true)];
        let r = resolve(&held, &req("a", 0, 49, LockKind::Unlock));
        assert_eq!(r.insert.len(), 1);
        assert_eq!((r.insert[0].start, r.insert[0].end), (50, LOCK_EOF));
    }

    #[test]
    fn a_refused_request_writes_nothing() {
        let held = vec![lock("a", 0, 99, true)];
        let r = resolve(&held, &req("b", 0, 99, LockKind::Exclusive));
        assert!(r.conflict.is_some());
        assert!(r.delete.is_empty() && r.insert.is_empty());
    }

    #[test]
    fn unlocking_leaves_other_owners_alone() {
        let held = vec![lock("a", 0, 99, false), lock("b", 0, 99, false)];
        let r = resolve(&held, &req("a", 0, 99, LockKind::Unlock));
        assert_eq!(r.delete, vec![("a".to_string(), 0)]);
        assert!(r.insert.is_empty());
    }
}
