//! B2 (issue #70): property-based tests (`proptest`) for the pure, security-
//! sensitive cores — content-object (de)serialization, FastCDC chunking, and
//! three-way merge idempotence. These complement the example-based suites by
//! asserting invariants over *arbitrary* inputs:
//!
//! - **Roundtrip identity.** `decode(encode(x)) == x` for `Manifest`, `Tree`,
//!   and `Commit` over arbitrary values.
//! - **Size integrity.** A `Manifest` whose declared `size` disagrees with the
//!   sum of its chunk lengths is always rejected (the anti-OOM guard).
//! - **Exact tiling.** FastCDC boundaries tile the input contiguously with no
//!   gap/overlap, every non-final chunk is within `[MIN, MAX]`, and
//!   `concat(chunks) == input`.
//! - **Decoders never panic.** The untrusted-input object decoders return
//!   `Result` (never panic/abort) on arbitrary bytes — an always-on companion
//!   to the cargo-fuzz targets (B3).
//! - **Merge idempotence.** A fast-forward merge re-applied is a no-op, and a
//!   clean three-way merge of disjoint edits reproduces both sides exactly and
//!   is likewise a no-op when re-merged.

use origofs_core::chunk::chunk_bounds;
use origofs_core::{
    ChunkRef, Commit, Fs, Hash, MAX_CHUNK, MIN_CHUNK, Manifest, MemStore, MergeOutcome,
    SqliteMetadataStore, Tree, TreeEntry, TreeKind,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::sync::Arc;

// --- strategies for the pure content objects --------------------------------

/// An arbitrary (well-formed) 32-byte content hash. We derive it from a seed via
/// `Hash::of` so proptest inputs stay tiny while covering the hash space.
fn arb_hash() -> impl Strategy<Value = Hash> {
    any::<u64>().prop_map(|x| Hash::of(&x.to_le_bytes()))
}

fn arb_chunkref() -> impl Strategy<Value = ChunkRef> {
    (arb_hash(), any::<u32>()).prop_map(|(hash, len)| ChunkRef { hash, len })
}

/// A *consistent* manifest: `size == Σ chunk.len`, as the engine always builds.
fn arb_manifest() -> impl Strategy<Value = Manifest> {
    prop::collection::vec(arb_chunkref(), 0..64).prop_map(|chunks| {
        let size = chunks.iter().map(|c| c.len as u64).sum();
        Manifest { size, chunks }
    })
}

fn arb_treekind() -> impl Strategy<Value = TreeKind> {
    prop_oneof![
        Just(TreeKind::File),
        Just(TreeKind::Dir),
        Just(TreeKind::Symlink),
    ]
}

fn arb_tree() -> impl Strategy<Value = Tree> {
    // Names are arbitrary UTF-8 short enough to fit the u16 name-length field.
    prop::collection::vec((".{0,40}", any::<u32>(), arb_treekind(), arb_hash()), 0..16).prop_map(
        |v| Tree {
            entries: v
                .into_iter()
                .map(|(name, mode, kind, hash)| TreeEntry {
                    name,
                    mode,
                    kind,
                    hash,
                })
                .collect(),
        },
    )
}

fn arb_commit() -> impl Strategy<Value = Commit> {
    (
        arb_hash(),
        prop::collection::vec(arb_hash(), 0..8),
        ".{0,20}",
        ".{0,80}",
        any::<i64>(),
    )
        .prop_map(|(tree, parents, author, message, timestamp)| Commit {
            tree,
            parents,
            author,
            message,
            timestamp,
        })
}

/// Cheap, arbitrary-looking bytes of an exact length (keeps proptest inputs to a
/// `(seed, len)` pair instead of a giant `Vec<u8>` that is slow to shrink).
fn xorshift_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

proptest! {
    /// `decode(encode(m)) == m` for every consistent manifest.
    #[test]
    fn manifest_roundtrips(m in arb_manifest()) {
        let back = Manifest::decode(&m.encode()).expect("a consistent manifest must decode");
        prop_assert_eq!(back, m);
    }

    /// A manifest whose declared `size` disagrees with `Σ chunk.len` is rejected
    /// — the guard that stops a hostile `size` from driving an OOM pre-alloc.
    #[test]
    fn manifest_rejects_lying_size(
        chunks in prop::collection::vec(arb_chunkref(), 0..64),
        bogus in any::<u64>(),
    ) {
        let total: u64 = chunks.iter().map(|c| c.len as u64).sum();
        prop_assume!(bogus != total);
        let lying = Manifest { size: bogus, chunks };
        prop_assert!(Manifest::decode(&lying.encode()).is_err());
    }

    /// `decode(encode(t)) == t` for arbitrary trees.
    #[test]
    fn tree_roundtrips(t in arb_tree()) {
        let back = Tree::decode(&t.encode()).expect("a tree must decode");
        prop_assert_eq!(back, t);
    }

    /// `decode(encode(c)) == c` for arbitrary commits.
    #[test]
    fn commit_roundtrips(c in arb_commit()) {
        let back = Commit::decode(&c.encode()).expect("a commit must decode");
        prop_assert_eq!(back, c);
    }

    /// The untrusted-input decoders must return `Result` (never panic/abort) on
    /// arbitrary bytes — the always-on companion to the fuzz targets.
    #[test]
    fn content_decoders_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
        let _ = Manifest::decode(&bytes);
        let _ = Tree::decode(&bytes);
        let _ = Commit::decode(&bytes);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// FastCDC boundaries tile the input exactly: contiguous, gap-free, covering
    /// the whole body, every non-final chunk within `[MIN, MAX]`, and
    /// `concat(chunks) == input`.
    #[test]
    fn chunk_bounds_tile_exactly(seed in any::<u64>(), len in 0usize..(3 * MAX_CHUNK as usize)) {
        let data = xorshift_bytes(seed, len);
        let bounds = chunk_bounds(&data);

        if data.is_empty() {
            prop_assert!(bounds.is_empty(), "empty input yields no chunks");
        } else {
            prop_assert_eq!(bounds[0].0, 0, "first chunk must start at offset 0");
            let mut pos = 0usize;
            let mut reconstructed = Vec::with_capacity(data.len());
            for (i, &(off, clen)) in bounds.iter().enumerate() {
                prop_assert_eq!(off, pos, "chunks must tile contiguously");
                prop_assert!(clen > 0, "no zero-length chunk");
                prop_assert!(clen <= MAX_CHUNK as usize, "chunk exceeds MAX");
                if i + 1 != bounds.len() {
                    prop_assert!(clen >= MIN_CHUNK as usize, "non-final chunk below MIN");
                }
                reconstructed.extend_from_slice(&data[off..off + clen]);
                pos += clen;
            }
            prop_assert_eq!(pos, data.len(), "chunks must cover the whole input");
            prop_assert_eq!(reconstructed, data, "concat(chunks) must equal input");
        }
    }
}

// --- merge idempotence (async, over an in-memory engine) --------------------

async fn fresh_fs() -> Fs<SqliteMetadataStore, Arc<MemStore>> {
    let fs = Fs::new(
        SqliteMetadataStore::open_in_memory().unwrap(),
        Arc::new(MemStore::new()),
    );
    fs.init().await.unwrap();
    fs
}

/// A fast-forward merge advances HEAD to the branch tip, and re-merging the same
/// tip (or the shared base) is a no-op — for arbitrary edits on the branch.
#[test]
fn fast_forward_merge_is_idempotent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    proptest!(
        ProptestConfig::with_cases(32),
        |(writes in prop::collection::vec(("/[a-z]{1,8}", ".{0,64}"), 1..8usize))| {
            rt.block_on(async {
                let fs = fresh_fs().await;
                fs.write("/seed", b"0").await.unwrap();
                let base = fs.commit("a", "base").await.unwrap();

                fs.create_branch("dev").await.unwrap();
                fs.checkout("dev").await.unwrap();
                for (path, content) in &writes {
                    fs.write(path, content.as_bytes()).await.unwrap();
                }
                let tip = fs.commit("a", "dev work").await.unwrap();

                fs.checkout("main").await.unwrap();
                match fs.merge(tip, "a", "merge").await.unwrap() {
                    MergeOutcome::FastForward(h) => prop_assert_eq!(h, tip),
                    other => {
                        return Err(TestCaseError::fail(format!("expected fast-forward, got {other:?}")));
                    }
                }
                prop_assert_eq!(fs.head_commit().await.unwrap(), Some(tip));

                // Idempotent: re-merging the tip, and merging the shared base, are no-ops.
                prop_assert!(matches!(
                    fs.merge(tip, "a", "again").await.unwrap(),
                    MergeOutcome::AlreadyUpToDate
                ));
                prop_assert!(matches!(
                    fs.merge(base, "a", "base").await.unwrap(),
                    MergeOutcome::AlreadyUpToDate
                ));
                Ok::<(), TestCaseError>(())
            })?;
        }
    );
}

/// A clean three-way merge of *disjoint* edits (ours rewrites the `a` lines,
/// theirs rewrites the `b` lines) reproduces both sides exactly and records no
/// conflict; re-merging theirs is then a no-op. Each section is four lines —
/// `a{i}` (ours edits), `s{i}` gutter, `b{i}` (theirs edits), `g{i}` gutter — so
/// every changed line has an unchanged neighbour on *both* sides, including
/// across section boundaries, which is what keeps the diff3 hunks disjoint.
#[test]
fn disjoint_three_way_merge_is_clean_and_idempotent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    proptest!(
        ProptestConfig::with_cases(24),
        |(k in 1usize..8)| {
            rt.block_on(async {
                let lines = |f: &dyn Fn(usize) -> [String; 4]| -> Vec<String> {
                    (0..k).flat_map(f).collect()
                };
                let base = lines(&|i| [format!("a{i}"), format!("s{i}"), format!("b{i}"), format!("g{i}")]).join("\n");
                let ours = lines(&|i| [format!("A{i}"), format!("s{i}"), format!("b{i}"), format!("g{i}")]).join("\n");
                let theirs = lines(&|i| [format!("a{i}"), format!("s{i}"), format!("B{i}"), format!("g{i}")]).join("\n");
                let expected = lines(&|i| [format!("A{i}"), format!("s{i}"), format!("B{i}"), format!("g{i}")]);

                let fs = fresh_fs().await;
                fs.write("/f", base.as_bytes()).await.unwrap();
                fs.commit("a", "base").await.unwrap();

                // `theirs` branches from base; `main` (ours) diverges too.
                fs.create_branch("theirs").await.unwrap();
                fs.write("/f", ours.as_bytes()).await.unwrap();
                fs.commit("a", "ours").await.unwrap();

                fs.checkout("theirs").await.unwrap();
                fs.write("/f", theirs.as_bytes()).await.unwrap();
                let theirs_tip = fs.commit("a", "theirs").await.unwrap();

                fs.checkout("main").await.unwrap();
                match fs.merge(theirs_tip, "a", "merge").await.unwrap() {
                    MergeOutcome::Merged(_) => {}
                    other => {
                        return Err(TestCaseError::fail(format!("expected clean merge, got {other:?}")));
                    }
                }

                let merged = String::from_utf8(fs.read("/f").await.unwrap().to_vec()).unwrap();
                let got: Vec<&str> = merged.lines().collect();
                let want: Vec<&str> = expected.iter().map(String::as_str).collect();
                prop_assert_eq!(got, want);

                // The merge commit has theirs as a parent, so re-merging is a no-op.
                prop_assert!(matches!(
                    fs.merge(theirs_tip, "a", "again").await.unwrap(),
                    MergeOutcome::AlreadyUpToDate
                ));
                Ok::<(), TestCaseError>(())
            })?;
        }
    );
}
