//! `copy_file_range` and `fallocate` over a content-addressed store (issue #119).
//!
//! The correctness half is ordinary: bytes land where the syscalls say they land.
//! The half worth writing tests for is the *cost*. Both operations exist because
//! the kernel's fallback is a read/write loop, and an implementation that quietly
//! materializes the range is that loop with extra steps — it would pass every
//! byte-for-byte assertion here while being worthless. So the chunk-level tests
//! below assert what the manifest is made of, not just what it reads back as.

use async_trait::async_trait;
use bytes::Bytes;
use origofs_core::Result;
use origofs_core::vfs::AllocateMode;
use origofs_core::{
    ContentStore, Fs, Hash, MemStore, MetadataStore, OrigoFSError, SqliteMetadataStore,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A `MemStore` that tallies how many content bytes are *read back*.
///
/// Reads are the signal that matters here, and object counts are not. Content is
/// addressed by hash, so a read/write copy re-chunks the same bytes into the same
/// hashes and stores nothing new — it passes an object-count assertion while doing
/// exactly the work the operation exists to avoid. What it cannot hide is having
/// read the range.
struct CountingStore {
    inner: MemStore,
    read: AtomicU64,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: MemStore::new(),
            read: AtomicU64::new(0),
        }
    }
    fn bytes_read(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ContentStore for CountingStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        self.inner.put(bytes).await
    }
    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.inner.put_keyed(key, bytes).await
    }
    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let b = self.inner.get(hash).await?;
        self.read.fetch_add(b.len() as u64, Ordering::Relaxed);
        Ok(b)
    }
    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        let b = self.inner.get_range(hash, off, len).await?;
        self.read.fetch_add(b.len() as u64, Ordering::Relaxed);
        Ok(b)
    }
    async fn has(&self, hash: &Hash) -> Result<bool> {
        self.inner.has(hash).await
    }
    async fn list(&self) -> Result<Vec<Hash>> {
        self.inner.list().await
    }
    async fn delete(&self, hash: &Hash) -> Result<u64> {
        self.inner.delete(hash).await
    }
}

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<MemStore>>;

async fn fixture() -> (TestFs, Arc<MemStore>) {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let store = Arc::new(MemStore::new());
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

/// Deterministic bytes that actually chunk — a constant run would dedup into one
/// repeated chunk and hide whether ranges are being referenced or rebuilt.
fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut s = seed | 1;
    (0..len)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s & 0xff) as u8
        })
        .collect()
}

async fn ino<C: ContentStore + 'static>(
    fs: &Fs<Arc<dyn MetadataStore>, Arc<C>>,
    path: &str,
) -> i64 {
    fs.stat(path).await.unwrap().ino
}

async fn chunk_hashes<C: ContentStore + 'static>(
    fs: &Fs<Arc<dyn MetadataStore>, Arc<C>>,
    path: &str,
) -> Vec<String> {
    let (m, _) = fs.open_for_range(path).await.unwrap();
    m.map(|m| m.chunks.iter().map(|c| c.hash.to_hex()).collect())
        .unwrap_or_default()
}

// --- copy_file_range -------------------------------------------------------

/// Bytes land where they should, at alignments chosen to straddle chunk edges.
#[tokio::test]
async fn a_copy_reproduces_the_source_bytes() {
    let (fs, _) = fixture().await;
    let src = payload(600_000, 7);
    fs.write("/src.bin", &src).await.unwrap();
    fs.write("/dst.bin", &payload(600_000, 99)).await.unwrap();
    let (s, d) = (ino(&fs, "/src.bin").await, ino(&fs, "/dst.bin").await);

    for (src_off, dst_off, len) in [
        (0u64, 0u64, 1024u64),
        (1, 0, 4096), // unaligned source
        (0, 1, 4096), // unaligned destination
        (12_345, 54_321, 200_000),
        (599_000, 0, 1_000), // right up to the source end
    ] {
        let n = fs
            .vfs_copy_range_as(None, s, src_off, d, dst_off, len)
            .await
            .unwrap();
        assert_eq!(n, len, "short copy for {src_off}->{dst_off} len {len}");
        let got = fs.read_range("/dst.bin", dst_off, len).await.unwrap();
        assert_eq!(
            &got[..],
            &src[src_off as usize..(src_off + len) as usize],
            "bytes differ for {src_off}->{dst_off} len {len}"
        );
    }
}

/// **The reason this operation exists**, asserted by what it reads.
///
/// My first version of this test counted *stored objects* and was worthless: I
/// checked it by reimplementing the copy as the read/write loop it replaces, and
/// it passed. Content is addressed by hash, so re-chunking the same bytes yields
/// the same hashes and stores nothing new — an object count cannot tell a
/// reference copy from a full rebuild, and neither can "every destination chunk
/// came from the source".
///
/// Reads can. Copying a *whole* file straddles no chunk at either end, so a
/// reference copy touches none of the file's data — it reads only the manifest,
/// which it must, to know which chunks to point at. That residue is a couple of
/// kilobytes against megabytes of content, so the bound is stated as a fraction of
/// the range: any implementation that materializes what it copies blows through it
/// by three orders of magnitude.
#[tokio::test]
async fn a_copy_does_not_read_the_source_data() {
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let store = Arc::new(CountingStore::new());
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();

    let src = payload(4 * 1024 * 1024, 3);
    fs.write("/src.bin", &src).await.unwrap();
    fs.write("/dst.bin", b"seed").await.unwrap();
    let (s, d) = (ino(&fs, "/src.bin").await, ino(&fs, "/dst.bin").await);
    let chunks = chunk_hashes(&fs, "/src.bin").await.len();
    assert!(chunks > 8, "test needs a multi-chunk source, got {chunks}");

    let before = store.bytes_read();
    let n = fs
        .vfs_copy_range_as(None, s, 0, d, 0, src.len() as u64)
        .await
        .unwrap();
    assert_eq!(n, src.len() as u64);
    let read = store.bytes_read() - before;
    assert!(
        read * 100 < src.len() as u64,
        "a whole-file copy read {read} bytes of a {} byte file — more than the \
         manifest, so it is materializing the range rather than referencing it",
        src.len()
    );

    // And it really did copy, so "read nothing" is not "did nothing".
    assert_eq!(&fs.read("/dst.bin").await.unwrap()[..], &src[..]);

    // A partial range must still only read the chunks it straddles — at most two,
    // so well under the megabytes it is copying.
    let before = store.bytes_read();
    fs.vfs_copy_range_as(None, s, 1_000_001, d, 7, 2 * 1024 * 1024)
        .await
        .unwrap();
    let read = store.bytes_read() - before;
    assert!(
        read <= 2 * 256 * 1024,
        "an unaligned 2 MiB copy read {read} bytes; only the two straddling chunks \
         (at most 256 KiB each) should be materialized"
    );
}

/// The bytes of a whole-file copy are the source's own chunks, in order.
#[tokio::test]
async fn a_copy_references_the_source_chunks() {
    let (fs, _) = fixture().await;
    let src = payload(600_000, 3);
    fs.write("/src.bin", &src).await.unwrap();
    fs.write("/dst.bin", b"x").await.unwrap();
    let (s, d) = (ino(&fs, "/src.bin").await, ino(&fs, "/dst.bin").await);

    fs.vfs_copy_range_as(None, s, 0, d, 0, 600_000)
        .await
        .unwrap();

    let source: HashSet<String> = chunk_hashes(&fs, "/src.bin").await.into_iter().collect();
    let foreign: Vec<String> = chunk_hashes(&fs, "/dst.bin")
        .await
        .into_iter()
        .filter(|h| !source.contains(h))
        .collect();
    assert!(
        foreign.is_empty(),
        "destination holds {} chunk(s) the source never had: {foreign:?}",
        foreign.len()
    );
    assert_eq!(&fs.read("/dst.bin").await.unwrap()[..], &src[..]);
}

#[tokio::test]
async fn a_copy_is_clamped_at_the_source_end_of_file() {
    let (fs, _) = fixture().await;
    fs.write("/src.bin", &payload(1000, 5)).await.unwrap();
    fs.write("/dst.bin", b"").await.unwrap();
    let (s, d) = (ino(&fs, "/src.bin").await, ino(&fs, "/dst.bin").await);

    // Asking for more than exists is a short copy, not an error.
    assert_eq!(
        fs.vfs_copy_range_as(None, s, 900, d, 0, 500).await.unwrap(),
        100
    );
    // Starting past the end copies nothing at all.
    assert_eq!(
        fs.vfs_copy_range_as(None, s, 5000, d, 0, 10).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn an_overlapping_copy_within_one_file_is_refused() {
    let (fs, _) = fixture().await;
    fs.write("/f.bin", &payload(10_000, 11)).await.unwrap();
    let f = ino(&fs, "/f.bin").await;

    let err = fs
        .vfs_copy_range_as(None, f, 0, f, 100, 1000)
        .await
        .unwrap_err();
    assert!(
        matches!(err, OrigoFSError::InvalidArgument(_)),
        "expected InvalidArgument, got {err:?}"
    );
    // Non-overlapping ranges in one file are fine.
    assert_eq!(
        fs.vfs_copy_range_as(None, f, 0, f, 5_000, 1_000)
            .await
            .unwrap(),
        1_000
    );
}

/// A copy into the middle replaces exactly its range — the bytes around it, and
/// the file's length, are untouched. This is what `replace_range` is for; a naive
/// splice would shift the tail.
#[tokio::test]
async fn a_copy_into_the_middle_leaves_the_rest_intact() {
    let (fs, _) = fixture().await;
    let src = payload(50_000, 21);
    let dst = payload(50_000, 22);
    fs.write("/src.bin", &src).await.unwrap();
    fs.write("/dst.bin", &dst).await.unwrap();
    let (s, d) = (ino(&fs, "/src.bin").await, ino(&fs, "/dst.bin").await);

    fs.vfs_copy_range_as(None, s, 1_000, d, 20_000, 5_000)
        .await
        .unwrap();

    let got = fs.read("/dst.bin").await.unwrap();
    assert_eq!(got.len(), 50_000, "the file changed length");
    assert_eq!(
        &got[..20_000],
        &dst[..20_000],
        "bytes before the range moved"
    );
    assert_eq!(
        &got[20_000..25_000],
        &src[1_000..6_000],
        "the copied range is wrong"
    );
    assert_eq!(
        &got[25_000..],
        &dst[25_000..],
        "bytes after the range moved"
    );
}

// --- fallocate -------------------------------------------------------------

#[tokio::test]
async fn allocate_extends_and_keep_size_does_nothing() {
    let (fs, _) = fixture().await;
    fs.write("/f.bin", &payload(1000, 31)).await.unwrap();
    let f = ino(&fs, "/f.bin").await;

    // Within the file: nothing to do.
    fs.vfs_allocate_as(None, f, 0, 500, AllocateMode::Allocate)
        .await
        .unwrap();
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 1000);

    // Past the end: the file grows, and the new bytes read as zeroes.
    fs.vfs_allocate_as(None, f, 900, 1100, AllocateMode::Allocate)
        .await
        .unwrap();
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 2000);
    assert_eq!(
        &fs.read_range("/f.bin", 1000, 1000).await.unwrap()[..],
        &[0u8; 1000][..]
    );

    // `KEEP_SIZE` asks for blocks without changing the size. There are no blocks.
    fs.vfs_allocate_as(None, f, 0, 1_000_000, AllocateMode::KeepSize)
        .await
        .unwrap();
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 2000);
}

#[tokio::test]
async fn punching_a_hole_zeroes_the_range_and_keeps_the_size() {
    let (fs, _) = fixture().await;
    let body = payload(40_000, 41);
    fs.write("/f.bin", &body).await.unwrap();
    let f = ino(&fs, "/f.bin").await;

    fs.vfs_allocate_as(None, f, 10_000, 5_000, AllocateMode::PunchHole)
        .await
        .unwrap();

    let got = fs.read("/f.bin").await.unwrap();
    assert_eq!(got.len(), 40_000, "punching must not change the size");
    assert_eq!(&got[..10_000], &body[..10_000]);
    assert_eq!(
        &got[10_000..15_000],
        &[0u8; 5_000][..],
        "the range is not zeroed"
    );
    assert_eq!(&got[15_000..], &body[15_000..]);

    // Punching past the end cannot extend the file.
    fs.vfs_allocate_as(None, f, 39_000, 10_000, AllocateMode::PunchHole)
        .await
        .unwrap();
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 40_000);
}

/// Zeroes are stored once and referenced, so punching holes in many files costs
/// one object rather than one per file. That is what makes a hole cheap here.
///
/// Asserted by identity rather than by counting objects. A threshold would have to
/// budget for the per-file manifest and the two edge chunks each punch splits, and
/// a bound loose enough to cover those is loose enough to pass while every file
/// stored its own zeroes. The five files hold unrelated random bytes, so the only
/// chunk they can possibly share is the hole.
#[tokio::test]
async fn holes_share_their_zero_chunks() {
    let (fs, _) = fixture().await;
    for i in 0..5 {
        fs.write(&format!("/f{i}.bin"), &payload(40_000, 50 + i))
            .await
            .unwrap();
    }
    for i in 0..5 {
        let f = ino(&fs, &format!("/f{i}.bin")).await;
        fs.vfs_allocate_as(None, f, 1_000, 20_000, AllocateMode::PunchHole)
            .await
            .unwrap();
    }

    let mut shared: Option<HashSet<String>> = None;
    for i in 0..5 {
        let here: HashSet<String> = chunk_hashes(&fs, &format!("/f{i}.bin"))
            .await
            .into_iter()
            .collect();
        shared = Some(match shared {
            None => here,
            Some(acc) => acc.intersection(&here).cloned().collect(),
        });
    }
    let shared = shared.unwrap();
    let zero = Hash::of(&vec![0u8; 20_000]).to_hex();
    assert!(
        shared.contains(&zero),
        "each file stored its own zeroes instead of sharing one chunk; common \
         chunks were {shared:?}"
    );
}

#[tokio::test]
async fn zero_range_zeroes_and_may_extend() {
    let (fs, _) = fixture().await;
    let body = payload(10_000, 61);
    fs.write("/f.bin", &body).await.unwrap();
    let f = ino(&fs, "/f.bin").await;

    // Unlike punching, this one is allowed to grow the file.
    fs.vfs_allocate_as(None, f, 9_000, 3_000, AllocateMode::ZeroRange)
        .await
        .unwrap();
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 12_000);

    let got = fs.read("/f.bin").await.unwrap();
    assert_eq!(
        &got[..9_000],
        &body[..9_000],
        "bytes before the range moved"
    );
    assert_eq!(&got[9_000..], &[0u8; 3_000][..], "the range is not zeroed");
}

#[tokio::test]
async fn a_zero_length_request_is_a_no_op() {
    let (fs, _) = fixture().await;
    fs.write("/f.bin", &payload(100, 71)).await.unwrap();
    let f = ino(&fs, "/f.bin").await;
    for mode in [
        AllocateMode::Allocate,
        AllocateMode::PunchHole,
        AllocateMode::ZeroRange,
        AllocateMode::KeepSize,
    ] {
        fs.vfs_allocate_as(None, f, 10, 0, mode).await.unwrap();
    }
    assert_eq!(fs.stat("/f.bin").await.unwrap().size, 100);
}
