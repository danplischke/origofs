#![no_main]
//! POSIX advisory lock resolution under arbitrary op sequences (issue #119).
//!
//! Unlike the other targets here this is not a `&[u8] -> Result<_>` decoder —
//! `posixlock::resolve` parses nothing and cannot fail. It earns a target for a
//! different reason: it is pure, total, and maintains an invariant the *database
//! schema depends on*. `(workspace, ino, owner, start_off)` is the primary key of
//! `posix_lock`, which is only a key while one owner's ranges never overlap. If
//! the resolver ever emits two overlapping ranges for one owner, the failure is a
//! constraint violation on a `setlk` somebody's editor issued — a crash in a
//! filesystem, arrived at from arithmetic no example test happened to try.
//!
//! The bytes are read as a *sequence of requests* rather than one, because the
//! interesting states are reached rather than constructed: a split, then a
//! coalesce over it, then an unlock through the middle. Generating lock sets
//! directly would mostly produce states the resolver can never emit.
//!
//! `tests/property.rs` asserts the same invariants on every CI run — this target
//! is the deeper search, and CI only `cargo check`s this crate.

use libfuzzer_sys::fuzz_target;
use origofs_core::posixlock::{self, LOCK_EOF, LockKind, LockRequest, PosixLock};

/// Six bytes per request: owner, kind, and two 16-bit offsets. Cramped on purpose
/// — a handful of owners over a small range makes overlap and adjacency the
/// common case instead of a coincidence.
fn requests(data: &[u8]) -> Vec<LockRequest> {
    data.chunks_exact(6)
        .map(|c| {
            let kind = match c[1] % 3 {
                0 => LockKind::Shared,
                1 => LockKind::Exclusive,
                _ => LockKind::Unlock,
            };
            let a = i64::from(u16::from_le_bytes([c[2], c[3]]));
            let b = i64::from(u16::from_le_bytes([c[4], c[5]]));
            let (start, mut end) = if a <= b { (a, b) } else { (b, a) };
            // Reach the open-ended range regularly: it is where `end + 1` would
            // overflow if the adjacency check ever stopped guarding it.
            if c[1] & 0x40 != 0 {
                end = LOCK_EOF;
            }
            LockRequest {
                owner: format!("owner-{}", c[0] % 4),
                holder: "fuzz".to_string(),
                pid: 1,
                start,
                end,
                kind,
            }
        })
        .collect()
}

fuzz_target!(|data: &[u8]| run(data));

fn run(data: &[u8]) {
    let mut state: Vec<PosixLock> = Vec::new();
    for req in requests(data) {
        let before = state.clone();
        let res = posixlock::resolve(&state, &req);

        if res.conflict.is_some() {
            assert!(
                res.delete.is_empty() && res.insert.is_empty(),
                "a refused request proposed writes: {res:?}"
            );
        }
        posixlock::apply(&mut state, &res);

        if let Err(e) = posixlock::check_state(&state) {
            panic!("invariant broken by {req:?}: {e}\nbefore: {before:?}\nafter: {state:?}");
        }

        // Somebody else's request never disturbs an owner's ranges.
        for owner in 0..4 {
            let name = format!("owner-{owner}");
            if name == req.owner {
                continue;
            }
            let was: Vec<&PosixLock> = before.iter().filter(|l| l.owner == name).collect();
            let now: Vec<&PosixLock> = state.iter().filter(|l| l.owner == name).collect();
            assert_eq!(was, now, "request for {} disturbed {name}", req.owner);
        }
    }

    // Whatever route the state took, releasing everything must empty it — a split
    // that strands a fragment shows up here and nowhere else.
    for owner in 0..4 {
        let r = LockRequest {
            owner: format!("owner-{owner}"),
            holder: "fuzz".to_string(),
            pid: 1,
            start: 0,
            end: LOCK_EOF,
            kind: LockKind::Unlock,
        };
        let res = posixlock::resolve(&state, &r);
        posixlock::apply(&mut state, &res);
    }
    assert!(state.is_empty(), "fragments survived a full release: {state:?}");
}
