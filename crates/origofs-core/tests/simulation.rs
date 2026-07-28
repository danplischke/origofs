//! Deterministic simulation testing (DST) — a first, trait-seam step.
//!
//! The real [`Fs`] engine runs against a *simulated, fault-injecting* content
//! store under an *injected clock*, driven by a *seeded* op sequence. Because
//! every input is derived from the seed (ops, fault schedule, crash point, and —
//! via the [`Clock`] seam — timestamps and thus commit hashes), a single `u64`
//! reproduces an entire run exactly. On failure the test prints the seed.
//!
//! What it proves today:
//! - **The C3/C4 durability barrier.** origofs makes content durable (`flush`) before
//!   the metadata that references it commits. So after a power-loss crash (which
//!   drops un-flushed writes), *no committed metadata reference may dangle*. The
//!   invariant is checked by re-reading the working tree and running `gc()`, whose
//!   mark phase loads every reachable object (refs → commits → trees → manifests →
//!   chunks + the live working tree) — a lost object surfaces as an error.
//! - **Determinism.** The same seed yields byte-identical state, including commit
//!   hashes (which embed the injected clock's timestamps — the clock seam is what
//!   makes that reproducible).
//! - **The checker isn't vacuous.** A negative control (a store whose `flush`
//!   never makes writes durable — a *broken* barrier) is reliably caught.
//!
//! Honest scope: this is the trait-seam tier. It exercises origofs's *own* ordering
//! and logic, not SQLite's internal crash-safety, and it crashes at *op
//! boundaries* rather than intercepting individual `await`s. Mid-`await` crash
//! injection and a deterministic scheduler (madsim-style) are the natural next
//! steps toward full DST; see the PR description.

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::{
    Clock, ContentStore, Fs, Hash, OrigoFSError, Result, SqliteMetadataStore, WriteCtx,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// --- seeded PRNG (SplitMix64) -----------------------------------------------

/// A tiny deterministic PRNG so the whole run is a pure function of the seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A value in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xff) as u8).collect()
    }
}

// --- injected deterministic clock -------------------------------------------

/// A clock that advances one second per read from a seed-derived epoch. Same
/// seed → same sequence of timestamps → same commit hashes.
struct SimClock {
    t: AtomicI64,
}

impl SimClock {
    fn new(start: i64) -> Self {
        SimClock {
            t: AtomicI64::new(start),
        }
    }
}

impl Clock for SimClock {
    fn now_secs(&self) -> i64 {
        self.t.fetch_add(1, Ordering::Relaxed)
    }
}

// --- fault-injecting content store ------------------------------------------

/// A content store that models durability explicitly: a `put` lands in a
/// `buffered` tier that a running process can read but that a **crash** drops;
/// `flush` promotes buffered → `durable` (surviving a crash). Faults are
/// injectable and seed-scheduled.
///
/// `promote_on_flush = false` models a *broken barrier* (a store that never makes
/// writes durable) — the negative control that proves the invariant has teeth.
struct FaultyContentStore {
    durable: Mutex<HashMap<Hash, Bytes>>,
    buffered: Mutex<HashMap<Hash, Bytes>>,
    promote_on_flush: bool,
    flush_calls: AtomicU64,
    fail_flush_at: HashSet<u64>,
}

impl FaultyContentStore {
    fn new(promote_on_flush: bool, fail_flush_at: HashSet<u64>) -> Self {
        FaultyContentStore {
            durable: Mutex::new(HashMap::new()),
            buffered: Mutex::new(HashMap::new()),
            promote_on_flush,
            flush_calls: AtomicU64::new(0),
            fail_flush_at,
        }
    }

    /// Power loss: everything not yet flushed to durable storage is gone.
    fn crash(&self) {
        self.buffered.lock().unwrap().clear();
    }

    fn store(&self, key: Hash, bytes: &[u8]) {
        // Idempotent, content-addressed: don't shadow a durable copy.
        if self.durable.lock().unwrap().contains_key(&key) {
            return;
        }
        self.buffered
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| Bytes::copy_from_slice(bytes));
    }
}

#[async_trait]
impl ContentStore for FaultyContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let h = Hash::of(bytes);
        self.store(h, bytes);
        Ok(h)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.store(*key, bytes);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        if let Some(b) = self.buffered.lock().unwrap().get(hash) {
            return Ok(b.clone());
        }
        self.durable
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .ok_or_else(|| OrigoFSError::ContentMissing(hash.to_hex()))
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        if self.buffered.lock().unwrap().contains_key(hash) {
            return Ok(true);
        }
        Ok(self.durable.lock().unwrap().contains_key(hash))
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let mut seen: HashSet<Hash> = self.durable.lock().unwrap().keys().copied().collect();
        seen.extend(self.buffered.lock().unwrap().keys().copied());
        Ok(seen.into_iter().collect())
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let mut freed = 0u64;
        if let Some(b) = self.buffered.lock().unwrap().remove(hash) {
            freed = b.len() as u64;
        }
        if let Some(b) = self.durable.lock().unwrap().remove(hash) {
            freed = b.len() as u64;
        }
        Ok(freed)
    }

    async fn flush(&self) -> Result<()> {
        let idx = self.flush_calls.fetch_add(1, Ordering::Relaxed);
        if self.fail_flush_at.contains(&idx) {
            return Err(OrigoFSError::Content(format!(
                "injected flush fault #{idx}"
            )));
        }
        if self.promote_on_flush {
            // Move without holding both locks at once.
            let drained: Vec<(Hash, Bytes)> = self.buffered.lock().unwrap().drain().collect();
            let mut dur = self.durable.lock().unwrap();
            for (h, b) in drained {
                dur.insert(h, b);
            }
        }
        Ok(())
    }
}

// --- the simulation ---------------------------------------------------------

type SimFs = Fs<SqliteMetadataStore, Arc<FaultyContentStore>>;

/// A small, fixed path space so writes overwrite each other (churn → orphaned
/// chunks → a real reachability/GC surface).
const PATHS: &[&str] = &["/a.md", "/b.md", "/c.md", "/f.md", "/g.md"];

/// The deterministic observable state: each file's content hash, and the branch
/// head commit. Both must be identical for two runs of the same seed.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    tree: BTreeMap<String, String>,
    head: Option<String>,
}

async fn snapshot(fs: &SimFs) -> Snapshot {
    let mut tree = BTreeMap::new();
    for &p in PATHS {
        if let Ok(inode) = fs.stat(p).await {
            let v = inode
                .content
                .map(|h| h.to_hex())
                .unwrap_or_else(|| "<empty>".to_string());
            tree.insert(p.to_string(), v);
        }
    }
    let head = fs.head_commit().await.ok().flatten().map(|h| h.to_hex());
    Snapshot { tree, head }
}

/// Run one seeded simulation. `promote_on_flush` false = broken-barrier control;
/// `flush_faults` injects seed-scheduled flush errors; `crash` drops un-flushed
/// writes at a seeded op boundary.
async fn run_sim(
    seed: u64,
    promote_on_flush: bool,
    flush_faults: bool,
    crash: bool,
) -> (Arc<FaultyContentStore>, SimFs) {
    let mut rng = Rng::new(seed);
    let n_ops = 8 + rng.below(20) as usize;

    // Seed-schedule the flush faults up front (indices of flush calls that error).
    let mut fail_flush_at = HashSet::new();
    if flush_faults {
        for i in 0..(n_ops as u64 * 2 + 4) {
            if rng.below(100) < 15 {
                fail_flush_at.insert(i);
            }
        }
    }

    let store = Arc::new(FaultyContentStore::new(promote_on_flush, fail_flush_at));
    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
    let fs = Fs::with_clock(
        SqliteMetadataStore::open_in_memory().unwrap(),
        store.clone(),
        clock,
    );
    fs.init().await.unwrap();
    let actor = fs.create_human("sim", None).await.unwrap();
    let ctx = WriteCtx::actor(actor);

    // Crash after at least one op so there is something to lose.
    let crash_at = if crash {
        Some(1 + rng.below(n_ops as u64) as usize)
    } else {
        None
    };

    for op_i in 0..n_ops {
        if Some(op_i) == crash_at {
            store.crash();
        }
        match rng.below(10) {
            0..=6 => {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let len = if rng.below(4) == 0 {
                    260_000 + rng.below(300_000) as usize // multi-chunk
                } else {
                    1 + rng.below(4096) as usize
                };
                let data = rng.bytes(len);
                // Tolerate injected write failures — that's the point.
                let _ = fs.write_as(ctx, path, &data).await;
            }
            7..=8 => {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let _ = fs.remove(path).await;
            }
            _ => {
                // Commit a snapshot; its timestamp comes from the injected clock.
                let _ = fs.commit("sim", "snapshot").await;
            }
        }
    }
    (store, fs)
}

/// The C3/C4 invariant: every content object referenced by committed metadata is
/// durable. Re-reading the working tree exercises manifest+chunk durability;
/// `gc()`'s mark phase walks the full reachable set (incl. the commit DAG) and
/// loads each object, so a lost reference surfaces as an error here.
async fn check_barrier(fs: &SimFs) -> Result<()> {
    for &p in PATHS {
        match fs.read(p).await {
            Ok(_) | Err(OrigoFSError::NotFound(_)) => {}
            Err(e) => return Err(e), // ContentMissing => a dangling reference
        }
    }
    fs.gc().await?;
    Ok(())
}

// --- tests ------------------------------------------------------------------

/// Sweep seeds: with the real (faithful) store, the durability barrier holds
/// under injected flush faults + a crash, for every seed.
#[tokio::test]
async fn durability_barrier_holds_across_seeds() {
    for seed in 0..64u64 {
        let (_store, fs) = run_sim(seed, true, true, true).await;
        if let Err(e) = check_barrier(&fs).await {
            panic!("durability barrier violated at seed {seed}: {e}");
        }
    }
}

/// Negative control: a store whose `flush` never makes writes durable is a broken
/// barrier — a crash after a committed write leaves a dangling reference, which
/// the checker must catch. Proves the invariant above isn't vacuously true.
#[tokio::test]
async fn broken_barrier_is_detected() {
    let mut caught = 0;
    for seed in 0..24u64 {
        let (_store, fs) = run_sim(seed, false, false, true).await;
        if check_barrier(&fs).await.is_err() {
            caught += 1;
        }
    }
    assert!(
        caught > 0,
        "the barrier checker never fired on a broken store — it is vacuous"
    );
}

/// The same seed reproduces byte-identical state, including the head commit hash
/// (which embeds the injected clock's timestamp — this is what the Clock seam
/// buys us; on the wall clock the two runs' commit hashes would diverge).
#[tokio::test]
async fn same_seed_is_reproducible() {
    for seed in [1u64, 7, 42, 100, 1234] {
        let (_s1, fs1) = run_sim(seed, true, false, false).await;
        let (_s2, fs2) = run_sim(seed, true, false, false).await;
        let a = snapshot(&fs1).await;
        let b = snapshot(&fs2).await;
        assert_eq!(a.tree, b.tree, "working tree diverged for seed {seed}");
        assert_eq!(
            a.head, b.head,
            "head commit hash diverged for seed {seed} — clock seam not deterministic?"
        );
    }
}

/// The fault model itself has teeth: an un-flushed `put` is lost on crash, a
/// flushed one survives, and a broken store (no promote) loses even flushed data.
#[tokio::test]
async fn faulty_store_crash_semantics() {
    // Faithful store: flush makes durable.
    let faithful = FaultyContentStore::new(true, HashSet::new());
    let survives = faithful.put(b"flushed").await.unwrap();
    faithful.flush().await.unwrap();
    let lost = faithful.put(b"buffered").await.unwrap(); // never flushed
    faithful.crash();
    assert!(
        faithful.has(&survives).await.unwrap(),
        "flushed must survive"
    );
    assert!(
        !faithful.has(&lost).await.unwrap(),
        "un-flushed must be lost"
    );

    // Broken store: flush does not make durable, so a crash loses it.
    let broken = FaultyContentStore::new(false, HashSet::new());
    let h = broken.put(b"x").await.unwrap();
    broken.flush().await.unwrap();
    broken.crash();
    assert!(
        !broken.has(&h).await.unwrap(),
        "broken store must lose even flushed data"
    );
}

// --- more invariants over the same harness ----------------------------------

/// Deterministic multi-line UTF-8 text, so writes land on the *line-based* blame
/// path (not the binary/file-level one).
fn text_blob(rng: &mut Rng, tag: char, lines: usize) -> Vec<u8> {
    let mut s = String::new();
    for k in 0..lines {
        s.push_str(&format!("{tag}-{k}-{}\n", rng.below(1000)));
    }
    s.into_bytes()
}

/// Attribution integrity: `blame` is a materialized view of the append-only
/// edit-op log, so it must never credit an actor who did not write the file, must
/// only name registered actors, and must vanish when an *unattributed* write
/// replaces the content (H7). Driven by seeded multi-actor writes with the
/// ground truth tracked alongside.
#[tokio::test]
async fn blame_is_consistent_with_who_wrote_each_file() {
    for seed in 0..48u64 {
        let mut rng = Rng::new(seed);
        let store = Arc::new(FaultyContentStore::new(true, HashSet::new()));
        let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
        let fs = Fs::with_clock(SqliteMetadataStore::open_in_memory().unwrap(), store, clock);
        fs.init().await.unwrap();

        let n_actors = 2 + rng.below(2) as usize;
        let mut actors = Vec::new();
        for i in 0..n_actors {
            actors.push(fs.create_human(&format!("a{i}"), None).await.unwrap());
        }

        // Ground truth: actors that attributed-wrote each path, and paths whose
        // most recent write was unattributed (must clear blame).
        let mut wrote: HashMap<&str, HashSet<i64>> = HashMap::new();
        let mut last_unattributed: HashSet<&str> = HashSet::new();

        for _ in 0..(6 + rng.below(20)) {
            let path = PATHS[rng.below(PATHS.len() as u64) as usize];
            if rng.below(10) < 8 {
                let actor = actors[rng.below(n_actors as u64) as usize];
                let tag = (b'A' + (actor % 26) as u8) as char;
                let lines = 1 + rng.below(6) as usize;
                let data = text_blob(&mut rng, tag, lines);
                if fs
                    .write_as(WriteCtx::actor(actor), path, &data)
                    .await
                    .is_ok()
                {
                    wrote.entry(path).or_default().insert(actor);
                    last_unattributed.remove(path);
                }
            } else {
                // A plain, unattributed write — must leave the file with no blame.
                let lines = 1 + rng.below(4) as usize;
                let data = text_blob(&mut rng, 'Z', lines);
                if fs.write(path, &data).await.is_ok() {
                    last_unattributed.insert(path);
                }
            }
        }

        let registered: HashSet<i64> = fs
            .list_actors()
            .await
            .unwrap()
            .iter()
            .map(|a| a.id)
            .collect();

        for &path in PATHS {
            if fs.stat(path).await.is_err() {
                continue;
            }
            let blame = fs.blame(path).await.unwrap();
            if last_unattributed.contains(path) {
                assert!(
                    blame.is_empty(),
                    "seed {seed}: unattributed write must clear blame for {path}"
                );
                continue;
            }
            let writers = wrote.get(path).cloned().unwrap_or_default();
            assert!(
                !blame.is_empty(),
                "seed {seed}: attributed file {path} must carry blame"
            );
            for r in &blame {
                assert!(
                    registered.contains(&r.actor.id),
                    "seed {seed}: blame on {path} credits unregistered actor {}",
                    r.actor.id
                );
                assert!(
                    writers.contains(&r.actor.id),
                    "seed {seed}: blame on {path} credits actor {} who never wrote it",
                    r.actor.id
                );
            }
        }
    }
}

/// GC is a mark-and-sweep, so it must be **safe** (never drop a reachable
/// object), **complete** (never leave an unreachable one), and **idempotent** (a
/// second pass finds nothing). Churn — overwrites, removes, commits — leaves
/// orphaned chunks for it to reclaim.
#[tokio::test]
async fn gc_is_safe_complete_and_idempotent() {
    for seed in 0..48u64 {
        // Faithful store, no faults, no crash: a clean run to collect over.
        let (store, fs) = run_sim(seed, true, false, false).await;

        // The live working tree, captured before collection.
        let mut live = BTreeMap::new();
        for &p in PATHS {
            if let Ok(bytes) = fs.read(p).await {
                live.insert(p, bytes.to_vec());
            }
        }

        let first = fs.gc().await.unwrap();

        // Complete: everything still stored is reachable (nothing unreachable
        // survived, nothing reachable was double-counted).
        assert_eq!(
            store.list().await.unwrap().len(),
            first.reachable,
            "seed {seed}: post-gc object count != reachable set"
        );

        // Safe: every live file still reads back, byte-for-byte.
        for (p, want) in &live {
            let got = fs.read(p).await.expect("gc dropped a reachable file");
            assert_eq!(&got[..], &want[..], "seed {seed}: gc corrupted {p}");
        }

        // Idempotent: a second pass has nothing left to delete.
        let second = fs.gc().await.unwrap();
        assert_eq!(
            second.deleted, 0,
            "seed {seed}: a second gc deleted {} objects (non-idempotent)",
            second.deleted
        );
    }
}

/// The content store is the backup: a workspace's committed state must rebuild
/// from the object graph alone (origofs `fsck --rebuild`). Build history, then point a
/// FRESH metadata DB at the same content store and rebuild — the recovered tree
/// and branch names must match what was committed.
#[tokio::test]
async fn rebuild_round_trips_committed_state_from_content() {
    for seed in 0..24u64 {
        let mut rng = Rng::new(seed);
        let store = Arc::new(FaultyContentStore::new(true, HashSet::new()));

        // Author some history: rounds of writes, each sealed by a commit (so the
        // working tree ends exactly at the last commit), then maybe a branch.
        let clock1: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
        let fs1 = Fs::with_clock(
            SqliteMetadataStore::open_in_memory().unwrap(),
            store.clone(),
            clock1,
        );
        fs1.init().await.unwrap();
        for _ in 0..(1 + rng.below(4)) {
            for _ in 0..(1 + rng.below(4)) {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let lines = 1 + rng.below(5) as usize;
                let data = text_blob(&mut rng, 'X', lines);
                fs1.write(path, &data).await.unwrap();
            }
            fs1.commit("sim", "round").await.unwrap();
        }
        if rng.below(2) == 0 {
            fs1.create_branch("feature").await.unwrap();
        }

        // Snapshot the committed state (== working tree, since we committed last).
        let mut committed = BTreeMap::new();
        for &p in PATHS {
            if let Ok(bytes) = fs1.read(p).await {
                committed.insert(p.to_string(), bytes.to_vec());
            }
        }
        let mut branches1: Vec<String> = fs1
            .list_branches()
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        branches1.sort();

        // Catastrophe: the metadata DB is gone. A fresh, empty DB over the SAME
        // content store rebuilds from the object graph + ref mirror alone.
        let clock2: Arc<dyn Clock> = Arc::new(SimClock::new(2_000_000 + seed as i64));
        let fs2 = Fs::with_clock(
            SqliteMetadataStore::open_in_memory().unwrap(),
            store.clone(),
            clock2,
        );
        fs2.init().await.unwrap();
        let report = fs2.rebuild_from_content().await.unwrap();
        assert!(
            report.used_mirror,
            "seed {seed}: committed via the engine, so the ref mirror should exist"
        );

        let mut recovered = BTreeMap::new();
        for &p in PATHS {
            if let Ok(bytes) = fs2.read(p).await {
                recovered.insert(p.to_string(), bytes.to_vec());
            }
        }
        assert_eq!(
            recovered, committed,
            "seed {seed}: rebuilt working tree != committed state"
        );

        let mut branches2: Vec<String> = fs2
            .list_branches()
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        branches2.sort();
        assert_eq!(
            branches2, branches1,
            "seed {seed}: recovered branch names differ"
        );
    }
}
