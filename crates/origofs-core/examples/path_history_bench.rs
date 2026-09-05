//! Measures the cost of building per-file history ("which commits touched this
//! path, and what changed") three ways, over an in-memory store so the numbers
//! are origofs's own object-fetch and decode cost.
//!
//! The decisive metric is **`ContentStore::get` calls**, not wall time: on a
//! local CAS a get is a memcpy, but on S3/PackStore it is a network round-trip,
//! and the per-path history walk is a serial dependency chain.
//!
//!   cargo run --release -p origofs-core --example path_history_bench

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::{
    CommitInfo, ContentStore, Fs, Hash, MemStore, Result, SqliteMetadataStore, Tree, TreeKind,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Delegating store that counts `get`/`get_range`. Every method is forwarded
/// explicitly — several have default bodies that would silently degrade the
/// backend if omitted (the trap `content.rs` documents for `Arc`).
struct Counting {
    inner: MemStore,
    gets: AtomicU64,
    /// Microseconds to sleep per `get`, to model an object-store round-trip.
    delay_us: AtomicU64,
}

impl Counting {
    fn new() -> Self {
        Self {
            inner: MemStore::new(),
            gets: AtomicU64::new(0),
            delay_us: AtomicU64::new(0),
        }
    }
    fn take(&self) -> u64 {
        self.gets.swap(0, Ordering::Relaxed)
    }
}

#[async_trait]
impl ContentStore for Counting {
    async fn put(&self, b: &[u8]) -> Result<Hash> {
        self.inner.put(b).await
    }
    async fn put_keyed(&self, k: &Hash, b: &[u8]) -> Result<()> {
        self.inner.put_keyed(k, b).await
    }
    async fn replace_keyed(&self, k: &Hash, b: &[u8]) -> Result<()> {
        self.inner.replace_keyed(k, b).await
    }
    async fn put_meta(&self, n: &str, b: &[u8]) -> Result<()> {
        self.inner.put_meta(n, b).await
    }
    async fn get_meta(&self, n: &str) -> Result<Option<Bytes>> {
        self.inner.get_meta(n).await
    }
    async fn get(&self, h: &Hash) -> Result<Bytes> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let d = self.delay_us.load(Ordering::Relaxed);
        if d > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(d)).await;
        }
        self.inner.get(h).await
    }
    async fn get_range(&self, h: &Hash, o: u64, l: u64) -> Result<Bytes> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(h, o, l).await
    }
    async fn has(&self, h: &Hash) -> Result<bool> {
        self.inner.has(h).await
    }
    async fn list(&self) -> Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }
    async fn touch(&self, h: &Hash) -> Result<()> {
        self.inner.touch(h).await
    }
    async fn get_sidecar(&self, n: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_sidecar(n).await
    }
    async fn put_sidecar_if_absent(&self, n: &str, b: &[u8]) -> Result<Vec<u8>> {
        self.inner.put_sidecar_if_absent(n, b).await
    }
    async fn delete(&self, h: &Hash) -> Result<u64> {
        self.inner.delete(h).await
    }
    async fn size_of(&self, h: &Hash) -> Result<Option<u64>> {
        self.inner.size_of(h).await
    }
    async fn age_of(&self, h: &Hash) -> Result<Option<u64>> {
        self.inner.age_of(h).await
    }
    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }
    async fn repack(&self) -> Result<u64> {
        self.inner.repack().await
    }
    async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

// ---------------------------------------------------------------- strategy 2/3

/// Resolve one path against a commit's root tree by descending it, decoding one
/// `Tree` per component. O(depth) gets instead of O(files-in-tree).
async fn path_hash(store: &Arc<Counting>, root: Hash, parts: &[&str]) -> Result<Option<Hash>> {
    let mut cur = root;
    for (i, part) in parts.iter().enumerate() {
        let tree = Tree::decode(&store.get(&cur).await?)?;
        let Some(e) = tree.entries.iter().find(|e| &e.name == part) else {
            return Ok(None);
        };
        let last = i + 1 == parts.len();
        match (last, e.kind) {
            (true, _) => return Ok(Some(e.hash)),
            (false, TreeKind::Dir) => cur = e.hash,
            (false, _) => return Ok(None),
        }
    }
    Ok(None)
}

/// Same descent, memoizing decoded trees by hash. Sound without invalidation:
/// trees are immutable and content-addressed.
async fn path_hash_cached(
    store: &Arc<Counting>,
    cache: &mut HashMap<Hash, Arc<Tree>>,
    root: Hash,
    parts: &[&str],
    level_miss: &mut [u64],
) -> Result<Option<Hash>> {
    let mut cur = root;
    for (i, part) in parts.iter().enumerate() {
        let tree = match cache.get(&cur) {
            Some(t) => t.clone(),
            None => {
                level_miss[i] += 1;
                let t = Arc::new(Tree::decode(&store.get(&cur).await?)?);
                cache.insert(cur, t.clone());
                t
            }
        };
        let Some(e) = tree.entries.iter().find(|e| &e.name == part) else {
            return Ok(None);
        };
        let last = i + 1 == parts.len();
        match (last, e.kind) {
            (true, _) => return Ok(Some(e.hash)),
            (false, TreeKind::Dir) => cur = e.hash,
            (false, _) => return Ok(None),
        }
    }
    Ok(None)
}

// -------------------------------------------------------------------- workload

struct Shape {
    mods: usize,
    subs: usize,
    files: usize,
    commits: usize,
}

fn paths(s: &Shape) -> Vec<String> {
    let mut v = Vec::new();
    for m in 0..s.mods {
        for b in 0..s.subs {
            for f in 0..s.files {
                v.push(format!("/src/mod{m}/sub{b}/f{f}.rs"));
            }
        }
    }
    v
}

async fn build(
    s: &Shape,
) -> Result<(
    Fs<SqliteMetadataStore, Arc<Counting>>,
    Arc<Counting>,
    Vec<String>,
)> {
    let store = Arc::new(Counting::new());
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        store.clone(),
    );
    fs.init().await?;
    let all = paths(s);
    for m in 0..s.mods {
        for b in 0..s.subs {
            fs.mkdir_p(&format!("/src/mod{m}/sub{b}")).await?;
        }
    }
    for p in &all {
        fs.write(p, b"seed\n").await?;
    }
    fs.commit("bench", "seed").await?;
    // Each commit touches one file, chosen by a xorshift walk over the corpus,
    // so the target path is touched sparsely — the realistic `log <file>` case.
    let mut x: u64 = 0x9E3779B97F4A7C15;
    for i in 0..s.commits {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let p = if i % 10 == 0 {
            &all[all.len() / 3]
        } else {
            &all[(x as usize) % all.len()]
        };
        fs.write(p, format!("edit {i}\n").as_bytes()).await?;
        fs.commit("bench", &format!("c{i}")).await?;
    }
    Ok((fs, store, all))
}

// ------------------------------------------------------------------- strategies

/// What you can build today with the public API: `diff` per adjacent commit
/// pair. Each call runs `flatten_commit` on both sides.
async fn naive(
    fs: &Fs<SqliteMetadataStore, Arc<Counting>>,
    log: &[CommitInfo],
    target: &str,
) -> Result<usize> {
    let mut hits = 0;
    for w in log.windows(2) {
        let d = fs.diff(&w[1].hash.to_hex(), &w[0].hash.to_hex()).await?;
        if d.iter().any(|e| e.path == target) {
            hits += 1;
        }
    }
    Ok(hits)
}

async fn descent(store: &Arc<Counting>, log: &[CommitInfo], target: &str) -> Result<usize> {
    let parts: Vec<&str> = target.trim_start_matches('/').split('/').collect();
    let mut hs = Vec::with_capacity(log.len());
    for ci in log {
        hs.push(path_hash(store, ci.commit.tree, &parts).await?);
    }
    Ok(count_changes(&hs))
}

async fn descent_cached(store: &Arc<Counting>, log: &[CommitInfo], target: &str) -> Result<usize> {
    let parts: Vec<&str> = target.trim_start_matches('/').split('/').collect();
    let mut cache = HashMap::new();
    let mut hs = Vec::with_capacity(log.len());
    let mut miss = vec![0u64; parts.len()];
    for ci in log {
        hs.push(path_hash_cached(store, &mut cache, ci.commit.tree, &parts, &mut miss).await?);
    }
    let per_level: Vec<String> = miss
        .iter()
        .enumerate()
        .map(|(i, m)| format!("L{i}={m}/{}", log.len()))
        .collect();
    println!(
        "       cache misses by tree level (0 = root): {}",
        per_level.join("  ")
    );
    Ok(count_changes(&hs))
}

/// The descents for different commits are independent — only the DAG walk that
/// produced the commit list is serial. Issue them with bounded concurrency.
async fn descent_concurrent(
    store: &Arc<Counting>,
    log: &[CommitInfo],
    target: &str,
    limit: usize,
) -> Result<usize> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let parts: Vec<&str> = target.trim_start_matches('/').split('/').collect();
    let hs: Vec<Option<Hash>> = stream::iter(log.iter().map(|ci| {
        let parts = parts.clone();
        async move { path_hash(store, ci.commit.tree, &parts).await }
    }))
    .buffered(limit)
    .try_collect()
    .await?;
    Ok(count_changes(&hs))
}

/// `log` is newest-first, so entry i's parent is entry i+1. A commit touched the
/// path iff its path-hash differs from its parent's.
fn count_changes(hs: &[Option<Hash>]) -> usize {
    let mut n = 0;
    for i in 0..hs.len().saturating_sub(1) {
        if hs[i] != hs[i + 1] {
            n += 1;
        }
    }
    n
}

// ------------------------------------------------------------------------ main

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    for (label, s) in [
        (
            "500 files / 200 commits",
            Shape {
                mods: 10,
                subs: 5,
                files: 10,
                commits: 200,
            },
        ),
        (
            "2000 files / 200 commits",
            Shape {
                mods: 20,
                subs: 10,
                files: 10,
                commits: 200,
            },
        ),
        (
            "500 files / 800 commits",
            Shape {
                mods: 10,
                subs: 5,
                files: 10,
                commits: 800,
            },
        ),
    ] {
        let (fs, store, all) = build(&s).await?;
        let target = all[all.len() / 3].clone();
        println!("\n=== {label} — target {target} ===");

        store.take();
        let t = Instant::now();
        let log = fs.log().await?;
        let (log_gets, log_ms) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
        println!(
            "  DAG walk (shared floor, all strategies): {log_gets:>7} gets  {log_ms:>8.2} ms  ({} commits)",
            log.len()
        );

        let t = Instant::now();
        let a = naive(&fs, &log, &target).await?;
        let (g, ms) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
        println!(
            "  1. diff per pair (today's API):          {g:>7} gets  {ms:>8.2} ms  -> {a} revisions"
        );

        let t = Instant::now();
        let b = descent(&store, &log, &target).await?;
        let (g2, ms2) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
        println!(
            "  2. path descent:                         {g2:>7} gets  {ms2:>8.2} ms  -> {b} revisions"
        );

        let t = Instant::now();
        let c = descent_cached(&store, &log, &target).await?;
        let (g3, ms3) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
        println!(
            "  3. path descent + tree cache:            {g3:>7} gets  {ms3:>8.2} ms  -> {c} revisions"
        );

        assert_eq!(a, b, "descent disagrees with diff baseline");
        assert_eq!(b, c, "cache changed the answer");
        println!(
            "  speedup vs (1):  gets {:.0}x / {:.0}x    time {:.1}x / {:.1}x",
            g as f64 / g2.max(1) as f64,
            g as f64 / g3.max(1) as f64,
            ms / ms2.max(1e-9),
            ms / ms3.max(1e-9),
        );
    }

    // ---- depth sensitivity: descent is O(depth), so how deep is the path? ----
    println!("\n\n=== depth sensitivity (500 files, 200 commits) ===");
    for depth in [2usize, 4, 6, 8] {
        let store = Arc::new(Counting::new());
        let fs = Fs::new(
            SqliteMetadataStore::open_in_memory().unwrap(),
            store.clone(),
        );
        fs.init().await?;
        // A path `depth` components long, with 500 sibling files at the leaf dir.
        let dir: String = (0..depth - 1).map(|i| format!("/d{i}")).collect();
        fs.mkdir_p(&dir).await?;
        let all: Vec<String> = (0..500).map(|i| format!("{dir}/f{i}.rs")).collect();
        for p in &all {
            fs.write(p, b"seed\n").await?;
        }
        fs.commit("bench", "seed").await?;
        for i in 0..200 {
            fs.write(&all[i % all.len()], format!("e{i}\n").as_bytes())
                .await?;
            fs.commit("bench", &format!("c{i}")).await?;
        }
        let target = all[0].clone();
        let log = fs.log().await?;
        store.take();
        descent(&store, &log, &target).await?;
        let g2 = store.take();
        descent_cached(&store, &log, &target).await?;
        let g3 = store.take();
        println!(
            "  depth {depth}: descent {g2:>6} gets   cached {g3:>6} gets   cache saves {:.0}%",
            100.0 * (1.0 - g3 as f64 / g2 as f64)
        );
    }

    // ---- what it costs when a get is a network round-trip ----
    println!("\n\n=== simulated object store, 2ms per get (500 files, 200 commits) ===");
    let s = Shape {
        mods: 10,
        subs: 5,
        files: 10,
        commits: 200,
    };
    let (fs, store, all) = build(&s).await?;
    let target = all[all.len() / 3].clone();
    store.delay_us.store(2000, Ordering::Relaxed);
    store.take();
    let t = Instant::now();
    let log = fs.log().await?;
    let (g_log, ms_log) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
    println!(
        "  DAG walk (serial by construction): {g_log:>6} gets  {ms_log:>9.1} ms  <- irreducible floor"
    );

    let t = Instant::now();
    descent(&store, &log, &target).await?;
    let (g_ser, ms_ser) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
    println!("  descent, serial:              {g_ser:>6} gets  {ms_ser:>9.1} ms");

    for limit in [8usize, 32, 64] {
        let t = Instant::now();
        descent_concurrent(&store, &log, &target, limit).await?;
        let (g, msc) = (store.take(), t.elapsed().as_secs_f64() * 1e3);
        println!(
            "  descent, {limit:>2} concurrent:      {g:>6} gets  {msc:>9.1} ms   ({:.0}x)",
            ms_ser / msc.max(1e-9)
        );
    }
    store.delay_us.store(0, Ordering::Relaxed);
    Ok(())
}
