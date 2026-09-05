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
//! - **The barrier holds under a *mid-operation* crash.** Beyond crashing at op
//!   boundaries, the process can die at an arbitrary content-store call — *inside*
//!   a `write_as` or `commit` — and the barrier still holds. A per-seed sweep
//!   crashes at every `put`/`put_keyed`/`flush` a run makes, proving the
//!   flush-before-commit ordering at call granularity (e.g. a crash mid-`commit`
//!   never leaves a ref swapped to a body whose bytes weren't yet durable).
//! - **Determinism.** The same seed yields byte-identical state, including commit
//!   hashes (which embed the injected clock's timestamps — the clock seam is what
//!   makes that reproducible).
//! - **The checkers aren't vacuous.** Negative controls (a store whose `flush`
//!   never makes writes durable — a *broken* barrier) are reliably caught, at both
//!   op-boundary and mid-operation crash granularity.
//!
//! Honest scope: this is the trait-seam tier. It exercises origofs's *own* ordering
//! and logic (at the `ContentStore` seam, down to individual `put`/`flush` calls),
//! not SQLite's internal crash-safety. The remaining step toward full DST is a
//! *deterministic scheduler* for concurrent interleavings — deliberately not
//! adopted here: origofs's mutating ops hold a `parking_lot` guard across `.await`
//! (SQLite `MetaTxn`, C1), which a single-threaded cooperative sim (madsim-style)
//! would deadlock, and its genuine multi-writer backend is Postgres, which such a
//! sim can't drive. Concurrent-interleaving coverage therefore lives in the
//! real-runtime stress tiers (`concurrency.rs`, `postgres.rs`) instead.

//! GC assertions here use `gc_with_grace(0)`: the simulation quiesces the store
//! before collecting, and disabling the age gate keeps these checks about
//! *reachability* — which is what they are testing — rather than about how
//! recently an object happened to be written.

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::{
    Clock, ContentStore, Fs, Hash, MetadataStore, OrigoFSError, Result, SqliteMetadataStore,
    WorkspaceRegistry, WriteCtx,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
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
    // --- mid-operation crash injection (finer than the op-boundary `crash()`) --
    // The process can die *inside* an engine op, at an arbitrary content-store
    // call. `op_seq` counts durability-relevant calls (`put`/`put_keyed`/`flush`)
    // since the last `arm_crash_at`; when it reaches `crash_at_op`, buffered
    // (non-durable) writes vanish and the call fails, unwinding the caller before
    // any metadata commit could reference the lost bytes. `u64::MAX` disarms it.
    op_seq: AtomicU64,
    crash_at_op: AtomicU64,
    crashed: AtomicBool,
}

impl FaultyContentStore {
    fn new(promote_on_flush: bool, fail_flush_at: HashSet<u64>) -> Self {
        FaultyContentStore {
            durable: Mutex::new(HashMap::new()),
            buffered: Mutex::new(HashMap::new()),
            promote_on_flush,
            flush_calls: AtomicU64::new(0),
            fail_flush_at,
            op_seq: AtomicU64::new(0),
            crash_at_op: AtomicU64::new(u64::MAX),
            crashed: AtomicBool::new(false),
        }
    }

    /// Power loss: everything not yet flushed to durable storage is gone.
    fn crash(&self) {
        self.buffered.lock().unwrap().clear();
    }

    /// Arm a mid-operation crash at content-call index `op` (counted from now),
    /// resetting the counter and the fired flag. `u64::MAX` leaves it disarmed —
    /// used to *count* the content calls a run makes, so a sweep can crash at each.
    fn arm_crash_at(&self, op: u64) {
        self.op_seq.store(0, Ordering::Relaxed);
        self.crashed.store(false, Ordering::Relaxed);
        self.crash_at_op.store(op, Ordering::Relaxed);
    }

    /// Content-call index reached since the last `arm_crash_at` — i.e. the number
    /// of durability-relevant calls a disarmed run made.
    fn op_count(&self) -> u64 {
        self.op_seq.load(Ordering::Relaxed)
    }

    /// Whether the armed mid-operation crash has fired (the process is "dead").
    fn crashed_mid_op(&self) -> bool {
        self.crashed.load(Ordering::Relaxed)
    }

    /// Tick a durability-relevant content call; if it is the armed crash index,
    /// drop buffered writes (power loss) and fail the call so the caller unwinds
    /// before committing metadata that would reference the now-lost bytes.
    fn durability_tick(&self, op: &str) -> Result<()> {
        let idx = self.op_seq.fetch_add(1, Ordering::Relaxed);
        if idx == self.crash_at_op.load(Ordering::Relaxed)
            && !self.crashed.swap(true, Ordering::Relaxed)
        {
            self.buffered.lock().unwrap().clear();
            return Err(OrigoFSError::Content(format!(
                "injected mid-operation crash at content call #{idx} ({op})"
            )));
        }
        Ok(())
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
        self.durability_tick("put")?;
        let h = Hash::of(bytes);
        self.store(h, bytes);
        Ok(h)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.durability_tick("put_keyed")?;
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

    // Ages drive GC's grace period. The trait's default reports "unknown", which
    // GC treats as not-safe-to-sweep — correct as a fail-safe for a backend that
    // genuinely cannot date its objects, but here it would silently turn every
    // collection into a no-op and make the reclamation assertions meaningless.
    // This store is a fault injector over an in-memory map, and the simulation
    // quiesces before collecting, so every object is reportable and old enough.
    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|h| (h, Some(u64::MAX)))
            .collect())
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
        self.durability_tick("flush")?;
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

// A trait-object metadata handle so one alias covers both the `default` workspace
// and the `with_workspace`-scoped handles the multi-workspace tests build.
type SimFs = Fs<Arc<dyn MetadataStore>, Arc<FaultyContentStore>>;

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
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::with_clock(meta, store.clone(), clock);
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

/// Like [`run_sim`], but with **mid-operation** crash injection: the process dies
/// at content-store call index `crash_at_op` (counted over `put`/`put_keyed`/
/// `flush` *after* setup), which can land *inside* a `write_as` or `commit` — not
/// only at an op boundary. `u64::MAX` disarms it (used to count a run's content
/// calls so the sweep can crash at each). The op loop stops once the crash fires,
/// because the process is gone. The RNG drives only op selection, so the op
/// sequence up to the crash is identical for every `crash_at_op` — a fair sweep.
async fn run_sim_armed(
    seed: u64,
    crash_at_op: u64,
    promote_on_flush: bool,
) -> (Arc<FaultyContentStore>, SimFs) {
    let mut rng = Rng::new(seed);
    let n_ops = 8 + rng.below(20) as usize;

    let store = Arc::new(FaultyContentStore::new(promote_on_flush, HashSet::new()));
    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::with_clock(meta, store.clone(), clock);
    fs.init().await.unwrap();
    let actor = fs.create_human("sim", None).await.unwrap();
    let ctx = WriteCtx::actor(actor);

    // Arm only after setup, so init / actor-creation content calls are never the
    // crash target and the crash-index space is the driven ops alone.
    store.arm_crash_at(crash_at_op);

    for _ in 0..n_ops {
        if store.crashed_mid_op() {
            break; // the process died mid-op
        }
        match rng.below(10) {
            0..=6 => {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let len = if rng.below(4) == 0 {
                    // Multi-chunk (chunks are 16–256 KiB), so a crash can land
                    // *between* a write's chunk puts — kept modest so each of the
                    // many replays in the sweep stays cheap.
                    40_000 + rng.below(120_000) as usize
                } else {
                    1 + rng.below(4096) as usize
                };
                let data = rng.bytes(len);
                let _ = fs.write_as(ctx, path, &data).await;
            }
            7..=8 => {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let _ = fs.remove(path).await;
            }
            _ => {
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
    fs.gc_with_grace(0).await?;
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

/// Crash points for a mid-op sweep: every content call when there are at most
/// `cap` of them, otherwise `cap` points spread evenly across `[0, total)` — so a
/// run with many-chunk writes stays bounded while still probing the full span.
fn sweep_points(total: u64, cap: u64) -> Vec<u64> {
    if total <= cap {
        (0..total).collect()
    } else {
        (0..cap).map(|i| i * total / cap).collect()
    }
}

/// A1 (issue #70) — **mid-operation crash.** The C3/C4 durability barrier must
/// hold even when the process dies *inside* an engine op — at an arbitrary
/// content-store call, not only at an op boundary (the gap this file's header
/// names as the next DST step).
/// For each seed we count the content calls a full run makes, then crash across
/// them (every call, or an evenly-spread sample when a run makes many) and assert
/// no *committed* metadata reference dangles. This proves the flush-before-commit
/// ordering holds at call granularity, not just between ops — e.g. a crash
/// mid-`commit` must never leave a ref swapped to a body whose bytes weren't yet
/// made durable.
#[tokio::test]
async fn durability_barrier_holds_under_mid_operation_crash() {
    for seed in 0..24u64 {
        // A disarmed dry run counts this seed's durability-relevant content calls.
        let total = {
            let (store, _fs) = run_sim_armed(seed, u64::MAX, true).await;
            store.op_count()
        };
        // Crash at each content call, but bound the sweep so a run with
        // many-chunk writes stays CI-friendly while still spanning [0, total).
        for crash_at in sweep_points(total, 50) {
            let (_store, fs) = run_sim_armed(seed, crash_at, true).await;
            if let Err(e) = check_barrier(&fs).await {
                panic!(
                    "seed {seed}: durability barrier violated when crashing at content call \
                     #{crash_at} of {total}: {e}"
                );
            }
        }
    }
}

/// A1 (issue #70) — negative control for the mid-op checker: a broken barrier
/// (writes never made durable) loses *everything* on a crash, so a mid-op crash
/// after a committed reference must leave a dangling body the checker catches —
/// proving the sweep above isn't vacuously green.
#[tokio::test]
async fn mid_operation_crash_checker_has_teeth() {
    let mut caught = 0;
    for seed in 0..24u64 {
        let total = {
            let (store, _fs) = run_sim_armed(seed, u64::MAX, false).await;
            store.op_count()
        };
        for crash_at in 0..total {
            let (_store, fs) = run_sim_armed(seed, crash_at, false).await;
            if check_barrier(&fs).await.is_err() {
                caught += 1;
                break;
            }
        }
    }
    assert!(
        caught > 0,
        "the mid-op crash checker never fired on a broken store — it is vacuous"
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

        let first = fs.gc_with_grace(0).await.unwrap();

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
        let second = fs.gc_with_grace(0).await.unwrap();
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

// --- multi-workspace invariants (docs/MULTI_TENANCY.md) ----------------------
//
// The single-workspace simulation above is blind to the highest-risk part of the
// multi-workspace change: GC marks reachability across a store's workspaces, so a
// bug there would silently sweep another workspace's live content. These drive
// seeded churn spread over several workspaces sharing one store and check GC
// safety/completeness, cross-workspace non-interference, and determinism.

/// Build `n` workspaces sharing one store (`default` at index 0, then `ws1..`),
/// each its own engine handle over a `with_workspace`-scoped metadata view.
async fn build_workspaces(
    base: &Arc<dyn MetadataStore>,
    store: &Arc<FaultyContentStore>,
    clock: &Arc<dyn Clock>,
    n: usize,
) -> Vec<SimFs> {
    let root_fs = Fs::with_clock(base.clone(), store.clone(), clock.clone());
    root_fs.init().await.unwrap();
    let mut wss = vec![root_fs.clone()];
    for i in 1..n {
        let (id, root) = base.create_workspace(&format!("ws{i}")).await.unwrap();
        let fs = root_fs.rebind(base.with_workspace(id), root);
        fs.init().await.unwrap();
        wss.push(fs);
    }
    wss
}

/// Snapshot every workspace, in order (tree + head; what content-recovery must
/// reproduce). Used by the rebuild + determinism tests.
async fn all_snapshots(wss: &[SimFs]) -> Vec<Snapshot> {
    let mut out = Vec::new();
    for fs in wss {
        out.push(snapshot(fs).await);
    }
    out
}

/// The *full* observable per-workspace state a cross-workspace op must never
/// disturb — not just files+head, but every ref/branch, every lock, every recorded
/// conflict, and the versioning mode. `Snapshot` (tree+head) is deliberately narrow
/// because it also has to round-trip through content on rebuild; non-interference
/// has no such constraint, so it checks *everything* a leak could touch.
#[derive(Debug, PartialEq, Eq)]
struct FullState {
    tree: BTreeMap<String, String>,
    refs: BTreeMap<String, String>,
    locks: BTreeMap<String, String>,
    conflicts: BTreeMap<String, String>,
    versioning: String,
}

async fn full_state(fs: &SimFs) -> FullState {
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
    let refs = fs
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, h)| (n, h.to_hex()))
        .collect();
    let locks = fs
        .locks()
        .await
        .unwrap()
        .into_iter()
        .map(|(p, o, _)| (p, o))
        .collect();
    let conflicts = fs.conflicts().await.unwrap().into_iter().collect();
    let versioning = format!("{:?}", fs.versioning_mode().await.unwrap());
    FullState {
        tree,
        refs,
        locks,
        conflicts,
        versioning,
    }
}

/// The full state of every workspace except `skip` — the per-op non-interference
/// check: an op on `skip` must leave all the others byte-identical.
async fn full_states_except(wss: &[SimFs], skip: usize) -> Vec<FullState> {
    let mut out = Vec::new();
    for (i, fs) in wss.iter().enumerate() {
        if i != skip {
            out.push(full_state(fs).await);
        }
    }
    out
}

/// The full state of every workspace, in order.
async fn all_full_states(wss: &[SimFs]) -> Vec<FullState> {
    let mut out = Vec::new();
    for fs in wss {
        out.push(full_state(fs).await);
    }
    out
}

/// **Cross-workspace non-interference + GC safety.** Each op (write / remove /
/// commit / branch / checkout / lock / unlock) runs on a random workspace and must
/// leave *every other* workspace's **full state** identical — not just its files,
/// but its branches, its locks, its conflicts, and its versioning mode. A store-wide
/// `truncate` from a checkout, a leaked ref write, or an unscoped lock would all
/// surface here. Then one `gc()` over the shared content must keep every workspace
/// readable (the data-loss path), leave every workspace's full state untouched, and
/// be complete + idempotent.
#[tokio::test]
async fn gc_and_isolation_hold_across_workspaces() {
    for seed in 0..32u64 {
        let mut rng = Rng::new(seed);
        let store = Arc::new(FaultyContentStore::new(true, HashSet::new()));
        let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
        let base: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let n_ws = 2 + rng.below(3) as usize; // 2..=4
        let wss = build_workspaces(&base, &store, &clock, n_ws).await;
        let mut branch_seq = 0u64;

        for _ in 0..(16 + rng.below(30)) {
            let w = rng.below(n_ws as u64) as usize;
            // Lock owners are tagged with the workspace index, so a lock leaking into
            // another workspace's `locks()` is not just visible but attributable.
            let owner = format!("owner{w}");
            let before = full_states_except(&wss, w).await;
            match rng.below(16) {
                0..=6 => {
                    let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                    let len = 1 + rng.below(4096) as usize;
                    let data = rng.bytes(len);
                    wss[w].write(path, &data).await.unwrap();
                }
                7 => {
                    let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                    let _ = wss[w].remove(path).await;
                }
                8..=9 => {
                    let _ = wss[w].commit("sim", "snap").await;
                }
                10 => {
                    // Branch at the current head (needs a commit).
                    if wss[w].head_commit().await.ok().flatten().is_some() {
                        branch_seq += 1;
                        let _ = wss[w].create_branch(&format!("b{branch_seq}")).await;
                    }
                }
                11..=12 => {
                    // Check out a random existing branch of w — exercises the scoped
                    // truncate/materialize, which must not touch other workspaces.
                    let branches = wss[w].list_branches().await.unwrap();
                    if !branches.is_empty() {
                        let b = branches[rng.below(branches.len() as u64) as usize]
                            .0
                            .clone();
                        let _ = wss[w].checkout(&b).await;
                    }
                }
                13..=14 => {
                    // Lock a random path (per-workspace lock space, V11).
                    let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                    let _ = wss[w].lock(path, &owner).await;
                }
                _ => {
                    // Release a lock this workspace may hold.
                    let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                    let _ = wss[w].unlock(path, &owner).await;
                }
            }
            let after = full_states_except(&wss, w).await;
            assert_eq!(
                before, after,
                "seed {seed}: an op on ws{w} changed another workspace's full state"
            );
        }

        // GC safety: it may not change any workspace's full observable state, and
        // every file that still stats must still read (its content wasn't swept).
        let pre = all_full_states(&wss).await;
        let stats = wss[0].gc_with_grace(0).await.unwrap();
        let post = all_full_states(&wss).await;
        assert_eq!(pre, post, "seed {seed}: gc changed a workspace's state");
        for (w, fs) in wss.iter().enumerate() {
            for &p in PATHS {
                if fs.stat(p).await.is_ok() {
                    assert!(
                        fs.read(p).await.is_ok(),
                        "seed {seed} ws{w}: gc dropped content for {p}"
                    );
                }
            }
        }
        // Complete + idempotent over the shared store.
        assert_eq!(
            store.list().await.unwrap().len(),
            stats.reachable,
            "seed {seed}: post-gc object count != reachable set"
        );
        assert_eq!(
            wss[0].gc_with_grace(0).await.unwrap().deleted,
            0,
            "seed {seed}: a second gc pass was not idempotent"
        );
    }
}

/// **Durability barrier across workspaces.** Attributed churn spread over several
/// workspaces on a fault-injecting store, then a crash drops un-flushed writes: no
/// *committed* reference in *any* workspace may dangle. Reading every path surfaces
/// a lost committed body as `ContentMissing`, and one store-wide `gc()` mark loads
/// every workspace's reachable objects.
#[tokio::test]
async fn durability_barrier_holds_across_workspaces() {
    for seed in 0..32u64 {
        let mut rng = Rng::new(seed);
        let n_ops = 10 + rng.below(24) as usize;
        let mut fail_flush_at = HashSet::new();
        for i in 0..(n_ops as u64 * 2 + 4) {
            if rng.below(100) < 15 {
                fail_flush_at.insert(i);
            }
        }
        let store = Arc::new(FaultyContentStore::new(true, fail_flush_at));
        let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
        let base: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let n_ws = 2 + rng.below(3) as usize;
        let wss = build_workspaces(&base, &store, &clock, n_ws).await;
        let actor = wss[0].create_human("sim", None).await.unwrap();
        let ctx = WriteCtx::actor(actor);
        let crash_at: usize = 1 + rng.below(n_ops as u64) as usize;

        for op_i in 0..n_ops {
            if op_i == crash_at {
                store.crash();
            }
            let w = rng.below(n_ws as u64) as usize;
            let path = PATHS[rng.below(PATHS.len() as u64) as usize];
            match rng.below(10) {
                0..=6 => {
                    let len = 1 + rng.below(4096) as usize;
                    let data = rng.bytes(len);
                    let _ = wss[w].write_as(ctx, path, &data).await;
                }
                7..=8 => {
                    let _ = wss[w].remove(path).await;
                }
                _ => {
                    let _ = wss[w].commit("sim", "snap").await;
                }
            }
        }

        for (w, fs) in wss.iter().enumerate() {
            for &p in PATHS {
                match fs.read(p).await {
                    Ok(_) | Err(OrigoFSError::NotFound(_)) => {}
                    Err(e) => panic!("seed {seed} ws{w}: durability barrier violated on {p}: {e}"),
                }
            }
        }
        wss[0]
            .gc()
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: gc found a dangling reference: {e}"));
    }
}

/// Recovery restores **every** workspace from the content store alone, across
/// randomized multi-workspace histories: give each workspace committed history,
/// then a fresh DB over the same content rebuilds every workspace's tree.
#[tokio::test]
async fn rebuild_round_trips_all_workspaces() {
    for seed in 0..24u64 {
        let mut rng = Rng::new(seed);
        let store = Arc::new(FaultyContentStore::new(true, HashSet::new()));
        let clock1: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
        let base1: Arc<dyn MetadataStore> =
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let n_ws = 2 + rng.below(3) as usize;
        let wss = build_workspaces(&base1, &store, &clock1, n_ws).await;

        // Give every workspace at least one commit (so each has a recoverable
        // tagged mirror), then a few more seeded rounds; each round ends in a
        // commit, so a workspace's working tree == its last commit.
        for (i, fs) in wss.iter().enumerate() {
            let len = 1 + rng.below(512) as usize;
            let data = rng.bytes(len);
            fs.write(PATHS[i % PATHS.len()], &data).await.unwrap();
            fs.commit("sim", "init").await.unwrap();
        }
        for _ in 0..(2 + rng.below(5)) {
            let w = rng.below(n_ws as u64) as usize;
            for _ in 0..(1 + rng.below(3)) {
                let path = PATHS[rng.below(PATHS.len() as u64) as usize];
                let len = 1 + rng.below(2048) as usize;
                let data = rng.bytes(len);
                wss[w].write(path, &data).await.unwrap();
            }
            wss[w].commit("sim", "round").await.unwrap();
        }
        let committed = all_snapshots(&wss).await;

        // Catastrophe: a fresh DB over the SAME content store rebuilds every ws.
        let clock2: Arc<dyn Clock> = Arc::new(SimClock::new(2_000_000 + seed as i64));
        let base2: Arc<dyn MetadataStore> =
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let root2 = Fs::with_clock(base2.clone(), store.clone(), clock2);
        root2.init().await.unwrap();
        root2.rebuild_from_content().await.unwrap();

        for (i, want) in committed.iter().enumerate() {
            let fs = if i == 0 {
                root2.clone()
            } else {
                let (id, root) = base2
                    .lookup_workspace(&format!("ws{i}"))
                    .await
                    .unwrap()
                    .unwrap_or_else(|| panic!("seed {seed}: ws{i} not recovered"));
                root2.rebind(base2.with_workspace(id), root)
            };
            assert_eq!(
                &snapshot(&fs).await,
                want,
                "seed {seed}: workspace {i} not recovered to its committed state"
            );
        }
    }
}

/// Determinism holds with multiple workspaces: the same seed reproduces every
/// workspace's tree, including commit hashes (the clock seam).
#[tokio::test]
async fn multi_workspace_runs_are_reproducible() {
    for seed in [3u64, 11, 29, 77, 500] {
        let (_s1, w1) = run_multi_ws_sim(seed).await;
        let (_s2, w2) = run_multi_ws_sim(seed).await;
        assert_eq!(w1.len(), w2.len(), "seed {seed}: workspace count diverged");
        for (i, (a, b)) in w1.iter().zip(w2.iter()).enumerate() {
            assert_eq!(
                snapshot(a).await,
                snapshot(b).await,
                "seed {seed} ws{i}: state diverged between identical-seed runs"
            );
        }
    }
}

/// Drive seeded write/remove/commit churn across 2–4 workspaces sharing one store
/// (the determinism driver). Returns (store, handles).
async fn run_multi_ws_sim(seed: u64) -> (Arc<FaultyContentStore>, Vec<SimFs>) {
    let mut rng = Rng::new(seed);
    let store = Arc::new(FaultyContentStore::new(true, HashSet::new()));
    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(1_000_000 + seed as i64));
    let base: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let n_ws = 2 + rng.below(3) as usize; // 2..=4 workspaces
    let wss = build_workspaces(&base, &store, &clock, n_ws).await;

    for _ in 0..(12 + rng.below(30)) {
        let w = rng.below(n_ws as u64) as usize;
        let path = PATHS[rng.below(PATHS.len() as u64) as usize];
        match rng.below(10) {
            0..=6 => {
                let len = 1 + rng.below(4096) as usize;
                let data = rng.bytes(len);
                wss[w].write(path, &data).await.unwrap();
            }
            7..=8 => {
                let _ = wss[w].remove(path).await;
            }
            _ => {
                let _ = wss[w].commit("sim", "snap").await;
            }
        }
    }
    (store, wss)
}
