//! Per-handle write buffering for the FUSE mount (issue #112).
//!
//! Two halves, deliberately. The coalescing buffer itself is a plain struct with
//! no kernel in it, so the bulk of this file tests it directly and runs
//! everywhere. The mount-level tests below need root and `/dev/fuse` and
//! self-skip without them, exactly like `fuse_mount.rs` — which is why the
//! buffer was split out in the first place: the interesting logic must not be
//! only reachable through a mount CI cannot make.
#![cfg(all(unix, feature = "fuse"))]

use origofs_sdk::Workspace;
use origofs_sdk::fuse::{DirtyBuffer, mountable, spawn};
use std::os::unix::fs::FileExt;
use std::time::Duration;

/// The buffered runs as owned `(offset, bytes)` pairs, for comparison.
fn runs(b: &DirtyBuffer) -> Vec<(u64, Vec<u8>)> {
    b.runs().map(|(o, d)| (o, d.to_vec())).collect()
}

// --- the coalescing buffer --------------------------------------------------

#[test]
fn empty_buffer_is_empty() {
    let b = DirtyBuffer::default();
    assert!(b.is_empty());
    assert_eq!(b.len(), 0);
    assert_eq!(b.end(), None);
    assert!(!b.overlaps(0, 4096));
    assert!(runs(&b).is_empty());
}

/// The case the whole feature exists for: the kernel's sequential stream of
/// fixed-size writes must collapse into **one** run, or nothing has been saved.
#[test]
fn sequential_writes_coalesce_into_one_run() {
    let mut b = DirtyBuffer::default();
    let page = vec![7u8; 4096];
    for i in 0..64u64 {
        b.write_at(i * 4096, &page).unwrap();
    }
    assert_eq!(runs(&b).len(), 1, "sequential pages did not coalesce");
    assert_eq!(b.len(), 64 * 4096);
    assert_eq!(b.end(), Some(64 * 4096));
    assert_eq!(runs(&b)[0].0, 0);
    assert!(runs(&b)[0].1.iter().all(|byte| *byte == 7));
}

/// Out-of-order but contiguous writes coalesce too — a run is defined by the
/// bytes it covers, not by the order they arrived in.
#[test]
fn descending_writes_coalesce() {
    let mut b = DirtyBuffer::default();
    for i in (0..8u64).rev() {
        b.write_at(i * 10, &[i as u8; 10]).unwrap();
    }
    assert_eq!(runs(&b).len(), 1);
    assert_eq!(runs(&b)[0].0, 0);
    assert_eq!(b.len(), 80);
}

/// A write that exactly abuts an existing run joins it; one that leaves even a
/// single byte of gap does not.
#[test]
fn adjacent_merges_but_a_gap_does_not() {
    let mut b = DirtyBuffer::default();
    b.write_at(0, b"aaaa").unwrap();
    b.write_at(4, b"bbbb").unwrap();
    assert_eq!(runs(&b), vec![(0, b"aaaabbbb".to_vec())]);

    let mut b = DirtyBuffer::default();
    b.write_at(0, b"aaaa").unwrap();
    b.write_at(5, b"bbbb").unwrap();
    assert_eq!(
        runs(&b),
        vec![(0, b"aaaa".to_vec()), (5, b"bbbb".to_vec())],
        "a one-byte gap must stay a gap"
    );
    assert_eq!(b.len(), 8);
}

/// The later write wins on every byte it covers, whether it lands inside an
/// existing run, straddles its edge, or swallows it whole.
#[test]
fn a_later_write_wins_the_overlap() {
    let mut b = DirtyBuffer::default();
    b.write_at(0, b"aaaaaaaa").unwrap();
    b.write_at(2, b"BB").unwrap();
    assert_eq!(runs(&b), vec![(0, b"aaBBaaaa".to_vec())]);

    b.write_at(6, b"CCCC").unwrap();
    assert_eq!(runs(&b), vec![(0, b"aaBBaaCCCC".to_vec())]);

    b.write_at(0, b"ZZZZZZZZZZZZ").unwrap();
    assert_eq!(runs(&b), vec![(0, b"ZZZZZZZZZZZZ".to_vec())]);
    assert_eq!(b.len(), 12);
}

/// A write that spans the hole between two runs absorbs both into one.
#[test]
fn a_bridging_write_absorbs_both_neighbours() {
    let mut b = DirtyBuffer::default();
    b.write_at(0, b"aa").unwrap();
    b.write_at(10, b"bb").unwrap();
    b.write_at(20, b"cc").unwrap();
    assert_eq!(runs(&b).len(), 3);

    b.write_at(1, &[b'X'; 20]).unwrap();
    // [0,21) from the bridge plus the 'c' at 21, and the run at 20 is swallowed.
    assert_eq!(runs(&b).len(), 1);
    let (off, data) = runs(&b).remove(0);
    assert_eq!(off, 0);
    assert_eq!(data.len(), 22);
    assert_eq!(&data[..1], b"a");
    assert_eq!(&data[1..21], &[b'X'; 20]);
    assert_eq!(&data[21..], b"c");
    assert_eq!(b.len(), 22);
}

/// **The regression that matters most.** Memory (and, at flush time, the bytes
/// handed to `vfs_write`) must be bounded by what was *written*, never by the
/// span between writes — a buffer that materialized the gap would both blow up
/// on a sparse file and turn each flush into a whole-file replacement that
/// erases a concurrent writer's untouched bytes.
#[test]
fn far_apart_writes_never_materialize_the_gap() {
    let mut b = DirtyBuffer::default();
    b.write_at(0, b"lo").unwrap();
    b.write_at(1 << 40, b"hi").unwrap();
    assert_eq!(b.len(), 4, "the terabyte in between must not be allocated");
    assert_eq!(
        runs(&b),
        vec![(0, b"lo".to_vec()), (1 << 40, b"hi".to_vec())]
    );
    assert_eq!(b.end(), Some((1 << 40) + 2));
}

/// Runs stay in ascending offset order however they were inserted — the flush
/// path relies on it, and so does `end()`.
#[test]
fn runs_stay_sorted() {
    let mut b = DirtyBuffer::default();
    for off in [500u64, 100, 900, 300, 700] {
        b.write_at(off, b"xx").unwrap();
    }
    let offsets: Vec<u64> = runs(&b).into_iter().map(|(o, _)| o).collect();
    assert_eq!(offsets, vec![100, 300, 500, 700, 900]);
    assert_eq!(b.end(), Some(902));
}

/// `overlaps` is the read path's flush predicate, so its edges are load-bearing:
/// touching-but-not-overlapping must be false, or every read would flush.
#[test]
fn overlaps_is_half_open_on_both_ends() {
    let mut b = DirtyBuffer::default();
    b.write_at(100, b"abcdefghij").unwrap(); // [100, 110)

    assert!(!b.overlaps(0, 100), "a read ending exactly at the run");
    assert!(b.overlaps(0, 101));
    assert!(b.overlaps(99, 2));
    assert!(b.overlaps(105, 1));
    assert!(b.overlaps(109, 1));
    assert!(
        !b.overlaps(110, 4096),
        "a read starting exactly after the run"
    );
    assert!(!b.overlaps(200, 10));
    // A zero-length read never overlaps anything.
    assert!(!b.overlaps(105, 0));
    // A hostile size must saturate rather than wrap into a false negative.
    assert!(b.overlaps(0, u32::MAX));
}

/// An end offset that does not fit in a `u64` is refused rather than wrapped —
/// the same guard `Fs::vfs_write_attempt` makes for the unbuffered path.
#[test]
fn an_overflowing_offset_is_refused() {
    let mut b = DirtyBuffer::default();
    assert!(b.write_at(u64::MAX, b"ab").is_err());
    assert!(b.is_empty(), "a refused write must leave nothing behind");
    // The largest in-range write is accepted.
    b.write_at(u64::MAX - 2, b"ab").unwrap();
    assert_eq!(b.end(), Some(u64::MAX));
}

/// A zero-length write is a no-op, not an empty run.
#[test]
fn empty_writes_are_dropped() {
    let mut b = DirtyBuffer::default();
    b.write_at(0, b"").unwrap();
    assert!(b.is_empty());
    b.write_at(4, b"x").unwrap();
    b.write_at(4, b"").unwrap();
    assert_eq!(runs(&b), vec![(4, b"x".to_vec())]);
}

/// `len` tracks the runs incrementally (the cap check in `write` depends on it),
/// so it must survive an arbitrary mix of merges, overwrites and inserts.
#[test]
fn len_matches_the_runs_after_arbitrary_writes() {
    let mut b = DirtyBuffer::default();
    // Deterministic pseudo-random offsets; no dev-dependency needed.
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    for i in 0..500u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let off = (state >> 33) % 4096;
        let len = 1 + (i % 37) as usize;
        b.write_at(off, &vec![i as u8; len]).unwrap();
        let expect: usize = runs(&b).iter().map(|(_, d)| d.len()).sum();
        assert_eq!(b.len(), expect, "len drifted after write {i}");
        // And the invariants the flush path assumes hold throughout.
        let rs = runs(&b);
        for w in rs.windows(2) {
            assert!(
                w[0].0 + (w[0].1.len() as u64) < w[1].0,
                "runs must stay disjoint and non-adjacent"
            );
        }
    }
}

// --- through a real mount ---------------------------------------------------

/// Writing a file larger than the per-handle cap must produce exactly the bytes
/// written: the cap-triggered mid-write flush and the final flush at `release`
/// have to stitch together seamlessly.
#[test]
fn fuse_buffered_write_survives_the_cap() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap()
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    // Comfortably past HANDLE_BUFFER_CAP (4 MiB), with a byte pattern that would
    // show any mis-ordered or duplicated run.
    let body: Vec<u8> = (0..10u64 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(mnt.join("big.bin"), &body).unwrap();

    assert_eq!(std::fs::read(mnt.join("big.bin")).unwrap(), body);
    // And out-of-band, through the workspace API: the bytes really did land.
    assert_eq!(
        rt.block_on(ws.read("/big.bin")).unwrap().as_ref(),
        &body[..]
    );

    drop(session);
}

/// Read-your-own-writes across descriptors, and the size a `stat` reports while
/// bytes are still buffered.
///
/// The second descriptor is the point: it is a *different* handle with a
/// *different* buffer, so serving its read out of the store alone would hand
/// back the pre-write bytes. `fsync` is exercised separately, without a close,
/// because that is the path a caller uses when it wants durability and cannot
/// give up its descriptor.
#[test]
fn fuse_reads_see_unflushed_writes() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/rw.txt", b"old").await.unwrap();
        ws
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    let path = mnt.join("rw.txt");
    let w = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    w.write_at(b"NEWCONTENT", 0).unwrap();

    // The grown size must be visible while the bytes are still buffered, or the
    // kernel would clamp every read to the old length and never ask for the tail.
    assert_eq!(
        w.metadata().unwrap().len(),
        10,
        "stat reported a size that ignored the handle's buffer"
    );

    // Still open, never explicitly flushed: a second descriptor is a *different*
    // handle with its own buffer, so this is served out of the store — and must
    // therefore have forced the first handle's buffer out on the way.
    let r = std::fs::File::open(&path).unwrap();
    let mut got = vec![0u8; 10];
    r.read_at(&mut got, 0).unwrap();
    assert_eq!(&got, b"NEWCONTENT", "a second fd was served stale bytes");

    // fsync without closing: the bytes reach the rest of the workspace while the
    // descriptor that wrote them is still open.
    w.write_at(b"TAIL", 10).unwrap();
    w.sync_all().unwrap();
    assert_eq!(
        rt.block_on(ws.read("/rw.txt")).unwrap().as_ref(),
        b"NEWCONTENTTAIL",
        "fsync did not write the handle's buffer out"
    );

    drop(r);
    drop(w);
    drop(session);
}

/// Two descriptors buffering disjoint ranges of one file must both survive.
///
/// This is the lost-update hazard `Fs::vfs_write`'s compare-and-set loop guards,
/// and the reason a handle buffers *ranges* rather than a whole-file image: each
/// flush is still a patch, so whichever one loses the CAS re-reads the other's
/// bytes and reapplies its own on top.
#[test]
fn fuse_concurrent_handles_do_not_lose_each_others_writes() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        let ws = Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap();
        ws.write("/shared.bin", &vec![0u8; 8192]).await.unwrap();
        ws
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    let path = mnt.join("shared.bin");
    let a = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let b = std::fs::OpenOptions::new().write(true).open(&path).unwrap();

    // Interleaved so neither handle's buffer is written before the other's exists.
    for i in 0..4u64 {
        a.write_at(&[b'A'; 1024], i * 1024).unwrap();
        b.write_at(&[b'B'; 1024], 4096 + i * 1024).unwrap();
    }
    drop(a); // flushes [0, 4096)
    drop(b); // flushes [4096, 8192)

    let got = rt.block_on(ws.read("/shared.bin")).unwrap();
    assert_eq!(got.len(), 8192);
    assert!(
        got[..4096].iter().all(|c| *c == b'A'),
        "the first handle's writes were lost"
    );
    assert!(
        got[4096..].iter().all(|c| *c == b'B'),
        "the second handle's writes were lost"
    );

    drop(session);
}

/// A truncate must not be undone by a buffer flushed after it.
#[test]
fn fuse_truncate_lands_after_buffered_writes() {
    if !mountable() {
        eprintln!("skipping: FUSE mount unavailable (need root + /dev/fuse)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mnt = dir.path().join("mnt");
    std::fs::create_dir_all(&mnt).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let ws = rt.block_on(async {
        Workspace::open_local(dir.path().join("meta.db"), dir.path().join("cas"))
            .await
            .unwrap()
    });

    let session = spawn(ws.clone(), &mnt).unwrap();
    std::thread::sleep(Duration::from_millis(300));

    let path = mnt.join("trunc.txt");
    let f = std::fs::File::create(&path).unwrap();
    f.write_at(b"abcdefghij", 0).unwrap();
    f.set_len(4).unwrap();
    drop(f);

    assert_eq!(
        rt.block_on(ws.read("/trunc.txt")).unwrap().as_ref(),
        b"abcd"
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"abcd");

    drop(session);
}
