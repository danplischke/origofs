//! Deterministic simulation of the advisory-lock store against a reference model
//! (issue #119).
//!
//! `tests/property.rs` exercises `posixlock::resolve` on its own. That is the
//! semantics, but it is not the thing that runs: what runs is `resolve` inside a
//! transaction, with its decision turned into `DELETE`s addressed by
//! `(owner, start_off)` and `INSERT`s, and rows filtered by lease. Everything in
//! that sentence can be wrong while `resolve` is perfect — a delete that misses
//! its row, an insert that violates the primary key, an expiry filter off by one.
//!
//! So this drives the **real store** with a seeded op sequence and compares the
//! rows after every single step against the in-memory model. A `u64` reproduces a
//! whole run; failures print the seed and the diverging step.
//!
//! Honest scope: single-threaded, one workspace, SQLite. It proves the SQL
//! translation agrees with the model, not that concurrent transactions serialize
//! — that claim is `concurrency.rs`'s, and Postgres's variant of it is only
//! exercised where `ORIGOFS_PG_TEST_URL` is set.

use origofs_core::posixlock::{self, LOCK_EOF, LockKind, LockRequest, PosixLock};
use origofs_core::{Fs, LockStore, MemStore, MetadataStore, SqliteMetadataStore};
use std::sync::Arc;

/// Seeded xorshift: every input to a run is derived from one `u64`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Cramped ranges and few owners, so overlap and adjacency are the common case.
fn gen_request(rng: &mut Rng) -> LockRequest {
    let kind = match rng.below(3) {
        0 => LockKind::Shared,
        1 => LockKind::Exclusive,
        _ => LockKind::Unlock,
    };
    let a = rng.below(200) as i64;
    let b = rng.below(200) as i64;
    let (start, mut end) = if a <= b { (a, b) } else { (b, a) };
    if rng.below(8) == 0 {
        end = LOCK_EOF;
    }
    LockRequest {
        owner: format!("owner-{}", rng.below(4)),
        holder: format!("mount-{}", rng.below(2)),
        pid: 1,
        start,
        end,
        kind,
    }
}

fn normalized(mut locks: Vec<PosixLock>) -> Vec<(String, i64, i64, bool)> {
    locks.sort_by(|a, b| (&a.owner, a.start).cmp(&(&b.owner, b.start)));
    locks
        .into_iter()
        .map(|l| (l.owner, l.start, l.end, l.exclusive))
        .collect()
}

/// Run one seeded simulation. `break_model` makes the *model* wrong on purpose —
/// the negative control, proving the comparison can actually fail.
async fn run(seed: u64, ops: usize, break_model: bool) -> Result<(), String> {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta.clone(), Arc::new(MemStore::new()));
    fs.init().await.unwrap();
    fs.write("/f", b"x").await.unwrap();
    let ino = fs.stat("/f").await.unwrap().ino;

    let mut rng = Rng(seed | 1);
    let mut model: Vec<PosixLock> = Vec::new();
    // Fixed clock for the main phase: leases are exercised separately below, and
    // a lease expiring mid-run would make every step's comparison ambiguous.
    let (now, expires) = (1_000i64, 9_000i64);

    for step in 0..ops {
        let req = gen_request(&mut rng);
        let res = posixlock::resolve(&model, &req);
        let expected_conflict = res.conflict.is_some();

        if break_model {
            // Drop the split remainders: a plausible-looking model that silently
            // releases the ends of a range somebody re-locked the middle of.
            let mut r = res.clone();
            r.insert
                .retain(|l| l.owner == req.owner && l.start == req.start);
            posixlock::apply(&mut model, &r);
        } else {
            posixlock::apply(&mut model, &res);
        }

        let got = meta
            .apply_posix_lock(ino, &req, expires, now)
            .await
            .map_err(|e| format!("seed {seed} step {step}: store error {e}"))?;

        if got.is_some() != expected_conflict {
            return Err(format!(
                "seed {seed} step {step}: store {} but model {} for {req:?}",
                if got.is_some() { "refused" } else { "granted" },
                if expected_conflict {
                    "refused"
                } else {
                    "granted"
                },
            ));
        }

        let stored = normalized(meta.posix_locks(ino, now).await.unwrap());
        let modelled = normalized(model.clone());
        if stored != modelled {
            return Err(format!(
                "seed {seed} step {step}: diverged after {req:?}\n  store: {stored:?}\n  model: {modelled:?}"
            ));
        }
        posixlock::check_state(&model)
            .map_err(|e| format!("seed {seed} step {step}: model invariant broken: {e}"))?;
    }

    // The lease phase: everything written above was leased to `expires`, so a
    // clock past it must find the file unlocked — and the rows actually gone,
    // not merely filtered out of the answer.
    if !break_model && !model.is_empty() {
        let after = expires + 1;
        let probe = LockRequest {
            owner: "reaper".into(),
            holder: "mount-r".into(),
            pid: 1,
            start: 0,
            end: LOCK_EOF,
            kind: LockKind::Exclusive,
        };
        let conflict = meta
            .apply_posix_lock(ino, &probe, after + 100, after)
            .await
            .unwrap();
        if conflict.is_some() {
            return Err(format!("seed {seed}: an expired lease still blocked"));
        }
        let left = meta.posix_locks(ino, after).await.unwrap();
        if left.len() != 1 || left[0].owner != "reaper" {
            return Err(format!(
                "seed {seed}: expired rows survived the sweep: {:?}",
                normalized(left)
            ));
        }
    }
    Ok(())
}

/// The store agrees with the model, step for step, over many seeded runs.
#[tokio::test]
async fn the_store_matches_the_model_over_seeded_op_sequences() {
    for seed in 1..=64u64 {
        if let Err(e) = run(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15), 60, false).await {
            panic!("{e}");
        }
    }
}

/// The same seed twice is the same run — otherwise a reported seed is useless
/// for reproducing anything.
#[tokio::test]
async fn a_seed_reproduces_its_run() {
    let mut a = Rng(0xDEAD_BEEF);
    let mut b = Rng(0xDEAD_BEEF);
    let first: Vec<_> = (0..50)
        .map(|_| format!("{:?}", gen_request(&mut a)))
        .collect();
    let second: Vec<_> = (0..50)
        .map(|_| format!("{:?}", gen_request(&mut b)))
        .collect();
    assert_eq!(first, second, "the generator is not deterministic");
}

/// **Negative control.** A comparison that cannot fail proves nothing, so break
/// the model deliberately and require the simulation to notice.
///
/// The break is the one a real implementation would plausibly make: forgetting to
/// re-insert the remainders when a request splits an existing range, which
/// silently releases bytes the owner still believes it holds.
#[tokio::test]
async fn the_simulation_catches_a_broken_model() {
    let mut caught = 0;
    for seed in 1..=32u64 {
        if run(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15), 60, true)
            .await
            .is_err()
        {
            caught += 1;
        }
    }
    assert!(
        caught > 0,
        "a deliberately broken model went undetected across 32 seeds — the \
         comparison is vacuous"
    );
}
