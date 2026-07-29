//! A10 (issue #70): the clock seam's contract.
//!
//! `simulation.rs::same_seed_is_reproducible` already shows that the *same*
//! injected clock reproduces the *same* head commit hash. These pin the two
//! things it does not:
//!
//!   1. Commit identity is a **pure function of the injected timestamp** —
//!      identical ops under an identical timestamp hash identically (so wall time
//!      never leaks in), while a single different timestamp changes the commit
//!      hash (so the timestamp is genuinely bound into identity — the reason DST
//!      must inject the clock rather than read the wall clock).
//!   2. The engine tolerates **clock skew**: time running backwards between
//!      commits (NTP step-back, VM migration, a laggy node) records the reported
//!      timestamps verbatim yet keeps the commit DAG ordered by parent links, not
//!      by time, and leaves the data readable.

use origofs_core::{Clock, Fs, MemStore, SqliteMetadataStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// A clock whose value stays fixed until explicitly `set`, so a test controls the
/// exact timestamp every engine operation observes within a phase.
struct SettableClock {
    t: AtomicI64,
}

impl SettableClock {
    fn new(start: i64) -> Self {
        SettableClock {
            t: AtomicI64::new(start),
        }
    }
    fn set(&self, v: i64) {
        self.t.store(v, Ordering::Relaxed);
    }
}

impl Clock for SettableClock {
    fn now_secs(&self) -> i64 {
        self.t.load(Ordering::Relaxed)
    }
}

async fn fs_at(clock: Arc<dyn Clock>) -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::with_clock(meta, Arc::new(MemStore::new()), clock);
    fs.init().await.unwrap();
    fs
}

/// Identical content + author + message under an identical timestamp yields a
/// byte-identical commit hash (reproducibility, independent of wall time), and
/// changing only the timestamp changes the hash (time is bound into identity).
#[tokio::test]
async fn commit_hash_is_a_pure_function_of_the_injected_timestamp() {
    let mut hashes = Vec::new();
    // Two runs at the same instant, then one at a different instant — everything
    // else (content, author, message) held identical.
    for ts in [1_000_000i64, 1_000_000, 2_000_000] {
        let fs = fs_at(Arc::new(SettableClock::new(ts))).await;
        fs.write("/f.txt", b"identical body").await.unwrap();
        hashes.push(fs.commit("alice", "same message").await.unwrap());
    }
    assert_eq!(
        hashes[0], hashes[1],
        "same timestamp + identical tree/author/message must hash identically"
    );
    assert_ne!(
        hashes[0], hashes[2],
        "a different timestamp must change the commit hash — the timestamp is bound into identity"
    );
}

/// Time runs backwards across three commits. Each records the timestamp the clock
/// reported, but the DAG stays ordered by parent links (c1 <- c2 <- c3), head is
/// the structurally-latest commit, and the newest content still reads back — skew
/// never monotonizes timestamps silently nor corrupts state.
#[tokio::test]
async fn commits_survive_backwards_clock_skew_and_stay_dag_ordered() {
    let clock = Arc::new(SettableClock::new(5000));
    let fs = fs_at(clock.clone()).await;

    clock.set(5000);
    fs.write("/log.txt", b"one").await.unwrap();
    let c1 = fs.commit("a", "first").await.unwrap();

    clock.set(3000); // wall clock steps backwards
    fs.write("/log.txt", b"two").await.unwrap();
    let c2 = fs.commit("a", "second").await.unwrap();

    clock.set(1000); // and further back
    fs.write("/log.txt", b"three").await.unwrap();
    let c3 = fs.commit("a", "third").await.unwrap();

    // Three distinct, real commits; head is the structurally-latest one.
    assert_ne!(c1, c2);
    assert_ne!(c2, c3);
    assert_eq!(fs.head_commit().await.unwrap(), Some(c3));

    // The first-parent log is newest-first by DAG position — c3, c2, c1 — even
    // though timestamps *increase* down the chain.
    let log = fs.log().await.unwrap();
    let order: Vec<_> = log.iter().map(|i| i.hash).collect();
    assert_eq!(
        order,
        vec![c3, c2, c1],
        "log order must follow parent links, not timestamps"
    );

    // Parent links are structural, not temporal.
    assert_eq!(log[0].commit.parents, vec![c2]);
    assert_eq!(log[1].commit.parents, vec![c1]);
    assert!(log[2].commit.parents.is_empty());

    // Timestamps are recorded exactly as the (backwards-running) clock reported.
    assert_eq!(log[0].commit.timestamp, 1000);
    assert_eq!(log[1].commit.timestamp, 3000);
    assert_eq!(log[2].commit.timestamp, 5000);

    // Skew did not corrupt state: the latest content reads back.
    assert_eq!(&fs.read("/log.txt").await.unwrap()[..], b"three");
}
