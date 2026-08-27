//! The write path is `O(bytes written)`, not `O(file size)` (issue #111).
//!
//! `vfs_write` used to read the entire body, patch the range, re-run FastCDC over
//! the whole buffer, and re-upload every changed chunk. `docs/LIMITS.md` documented
//! the consequence — *"Rewriting a 1 GiB file through a mount is quadratic in
//! allocation and hashing"* — because the kernel issues a bulk write as many modest
//! requests, so a sequential rewrite of an N-byte file cost `O(N^2/request)`.
//!
//! # What these tests are actually guarding
//!
//! Correctness first, and by a wide margin. A splice that is fast and wrong is far
//! worse than a rewrite that is slow and right, and the failure mode is silent: the
//! manifest still decodes, the file still reads, only the bytes are wrong. So most
//! of what follows compares the spliced result against the bytes a whole-file
//! rewrite would have produced, across the boundary cases where splice arithmetic
//! goes wrong — first chunk, last chunk, exactly on a boundary, spanning many
//! chunks, past EOF, into a hole.
//!
//! Then the cost claim itself, which is the point of the change.

use origofs_core::{
    ContentStore, Fs, INO_ROOT, MemStore, MetadataStore, Owner, SqliteMetadataStore,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;

/// A store that counts the bytes it is asked to store, so "how much did this write
/// actually touch" is observable rather than inferred from wall-clock time.
struct Counting {
    inner: MemStore,
    put_bytes: AtomicUsize,
    get_bytes: AtomicUsize,
}

impl Counting {
    fn new() -> Self {
        Self {
            inner: MemStore::new(),
            put_bytes: AtomicUsize::new(0),
            get_bytes: AtomicUsize::new(0),
        }
    }
    fn reset(&self) {
        self.put_bytes.store(0, Ordering::SeqCst);
        self.get_bytes.store(0, Ordering::SeqCst);
    }
    fn put_bytes(&self) -> usize {
        self.put_bytes.load(Ordering::SeqCst)
    }
    fn get_bytes(&self) -> usize {
        self.get_bytes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ContentStore for Counting {
    async fn put(&self, bytes: &[u8]) -> Result<origofs_core::Hash, origofs_core::OrigoFSError> {
        self.put_bytes.fetch_add(bytes.len(), Ordering::SeqCst);
        self.inner.put(bytes).await
    }
    async fn put_keyed(
        &self,
        key: &origofs_core::Hash,
        bytes: &[u8],
    ) -> Result<(), origofs_core::OrigoFSError> {
        self.put_bytes.fetch_add(bytes.len(), Ordering::SeqCst);
        self.inner.put_keyed(key, bytes).await
    }
    async fn get(&self, hash: &origofs_core::Hash) -> Result<Bytes, origofs_core::OrigoFSError> {
        let b = self.inner.get(hash).await?;
        self.get_bytes.fetch_add(b.len(), Ordering::SeqCst);
        Ok(b)
    }
    async fn get_range(
        &self,
        hash: &origofs_core::Hash,
        off: u64,
        len: u64,
    ) -> Result<Bytes, origofs_core::OrigoFSError> {
        let b = self.inner.get_range(hash, off, len).await?;
        self.get_bytes.fetch_add(b.len(), Ordering::SeqCst);
        Ok(b)
    }
    async fn has(&self, hash: &origofs_core::Hash) -> Result<bool, origofs_core::OrigoFSError> {
        self.inner.has(hash).await
    }
    async fn delete(&self, hash: &origofs_core::Hash) -> Result<u64, origofs_core::OrigoFSError> {
        self.inner.delete(hash).await
    }
    async fn list(&self) -> Result<Vec<origofs_core::Hash>, origofs_core::OrigoFSError> {
        self.inner.list().await
    }
}

type TestFs = Fs<Arc<dyn MetadataStore>, Arc<Counting>>;

async fn fixture() -> (TestFs, Arc<Counting>) {
    let store = Arc::new(Counting::new());
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

/// Incompressible, so chunk boundaries are content-defined and nothing
/// deduplicates by accident — the case that produces the most chunks and the one
/// where a splice bug is most likely to show.
fn media(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = vec![0u8; len];
    for b in out.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    out
}

/// Write `body`, then apply `(offset, patch)` through the mount path, and assert
/// the file reads back exactly as an in-memory patch of the same bytes would.
///
/// This is the whole correctness argument in one helper: the reference is a plain
/// `Vec` splice, which cannot be wrong.
async fn assert_splice_matches(body: &[u8], offset: u64, patch: &[u8]) {
    let (fs, _store) = fixture().await;
    fs.vfs_create(INO_ROOT, "f.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/f.bin").await.unwrap().ino;
    if !body.is_empty() {
        fs.vfs_write(ino, 0, body).await.unwrap();
    }

    fs.vfs_write(ino, offset, patch).await.unwrap();

    // The reference: the same edit applied to a plain buffer.
    let end = offset as usize + patch.len();
    let mut want = body.to_vec();
    if want.len() < end {
        want.resize(end, 0);
    }
    want[offset as usize..end].copy_from_slice(patch);

    let got = fs.read("/f.bin").await.unwrap();
    assert_eq!(
        got.len(),
        want.len(),
        "size after splicing at {offset} (+{} bytes) into a {}-byte file",
        patch.len(),
        body.len()
    );
    assert!(
        got[..] == want[..],
        "bytes differ after splicing at {offset} (+{} bytes) into a {}-byte file",
        patch.len(),
        body.len()
    );
    // And the mount read path agrees with the path API.
    let via_mount = fs.vfs_read(ino, 0, want.len() as u32).await.unwrap();
    assert!(via_mount[..] == want[..], "vfs_read disagrees with read");
}

// --- correctness across the boundary cases -----------------------------------

/// A write in the middle of a multi-chunk file — the ordinary case.
#[tokio::test]
async fn a_middle_write_is_exact() {
    let body = media(1 << 20, 7);
    assert_splice_matches(&body, 500_000, &media(4096, 99)).await;
}

/// The first bytes of the file, where `lo` saturates at chunk 0.
#[tokio::test]
async fn a_write_at_the_start_is_exact() {
    let body = media(1 << 20, 7);
    assert_splice_matches(&body, 0, &media(4096, 99)).await;
}

/// The last bytes, where the widened window clamps at the final chunk.
#[tokio::test]
async fn a_write_at_the_end_is_exact() {
    let body = media(1 << 20, 7);
    let at = body.len() as u64 - 4096;
    assert_splice_matches(&body, at, &media(4096, 99)).await;
}

/// A write that extends the file past its current end.
#[tokio::test]
async fn a_write_past_the_end_extends_the_file() {
    let body = media(200_000, 7);
    assert_splice_matches(&body, body.len() as u64, &media(50_000, 99)).await;
}

/// A write starting well beyond EOF leaves a **hole**, which must read back as
/// zeroes rather than as whatever was in the allocation.
#[tokio::test]
async fn a_write_beyond_the_end_leaves_a_zero_hole() {
    let body = media(100_000, 7);
    assert_splice_matches(&body, 500_000, &media(1000, 99)).await;

    // Explicitly: the gap is zeroes, not garbage.
    let (fs, _s) = fixture().await;
    fs.vfs_create(INO_ROOT, "h.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/h.bin").await.unwrap().ino;
    fs.vfs_write(ino, 0, b"start").await.unwrap();
    fs.vfs_write(ino, 100_000, b"end").await.unwrap();
    let got = fs.read("/h.bin").await.unwrap();
    assert_eq!(got.len(), 100_003);
    assert!(
        got[5..100_000].iter().all(|b| *b == 0),
        "a hole must read back as zeroes"
    );
}

/// A write into an empty file, where there are no chunks to anchor on.
#[tokio::test]
async fn a_write_into_an_empty_file_is_exact() {
    assert_splice_matches(&[], 0, &media(300_000, 99)).await;
    assert_splice_matches(&[], 10_000, &media(1000, 99)).await;
}

/// A write spanning many chunks at once.
#[tokio::test]
async fn a_write_spanning_many_chunks_is_exact() {
    let body = media(2 << 20, 7);
    assert_splice_matches(&body, 100_000, &media(900_000, 99)).await;
}

/// A single byte — the smallest write, and the one where the widened window is
/// almost all of the cost.
#[tokio::test]
async fn a_single_byte_write_is_exact() {
    let body = media(1 << 20, 7);
    assert_splice_matches(&body, 777_777, &[0xAB]).await;
}

/// Many sequential writes, the shape a bulk copy through a mount actually takes.
/// This is the case the old path made quadratic.
#[tokio::test]
async fn many_sequential_writes_reconstruct_the_file() {
    let (fs, _store) = fixture().await;
    fs.vfs_create(INO_ROOT, "seq.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/seq.bin").await.unwrap().ino;

    let body = media(1 << 20, 11);
    // 128 KiB at a time, as the kernel issues them.
    for (i, part) in body.chunks(128 * 1024).enumerate() {
        fs.vfs_write(ino, (i * 128 * 1024) as u64, part)
            .await
            .unwrap();
    }
    assert_eq!(
        &fs.read("/seq.bin").await.unwrap()[..],
        &body[..],
        "a sequential rewrite must reproduce the file exactly"
    );
}

/// Overlapping and out-of-order writes, since nothing guarantees a mount delivers
/// them in order.
#[tokio::test]
async fn overlapping_and_out_of_order_writes_are_exact() {
    let (fs, _store) = fixture().await;
    fs.vfs_create(INO_ROOT, "o.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/o.bin").await.unwrap().ino;

    let mut want = media(400_000, 3);
    fs.vfs_write(ino, 0, &want).await.unwrap();

    for (off, seed) in [(300_000u64, 21), (50_000, 22), (299_000, 23), (0, 24)] {
        let patch = media(4000, seed);
        fs.vfs_write(ino, off, &patch).await.unwrap();
        want[off as usize..off as usize + patch.len()].copy_from_slice(&patch);
    }
    assert_eq!(&fs.read("/o.bin").await.unwrap()[..], &want[..]);
}

// --- truncate ----------------------------------------------------------------

/// Shrinking keeps the surviving prefix exactly, including when the new end falls
/// inside a chunk rather than on a boundary.
#[tokio::test]
async fn truncate_shrinks_exactly() {
    let body = media(1 << 20, 5);
    for target in [0u64, 1, 12_345, 500_000, (1 << 20) - 1] {
        let (fs, _s) = fixture().await;
        fs.vfs_create(INO_ROOT, "t.bin", 0o644, Owner::ROOT)
            .await
            .unwrap();
        let ino = fs.stat("/t.bin").await.unwrap().ino;
        fs.vfs_write(ino, 0, &body).await.unwrap();

        fs.vfs_truncate(ino, target).await.unwrap();
        let got = fs.read("/t.bin").await.unwrap();
        assert_eq!(got.len() as u64, target, "truncate to {target}");
        assert!(
            got[..] == body[..target as usize],
            "truncate to {target} changed the surviving prefix"
        );
    }
}

/// Growing by truncate zero-fills, and the original bytes are untouched.
#[tokio::test]
async fn truncate_grows_with_zeroes() {
    let (fs, _s) = fixture().await;
    fs.vfs_create(INO_ROOT, "g.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/g.bin").await.unwrap().ino;
    let body = media(100_000, 5);
    fs.vfs_write(ino, 0, &body).await.unwrap();

    fs.vfs_truncate(ino, 250_000).await.unwrap();
    let got = fs.read("/g.bin").await.unwrap();
    assert_eq!(got.len(), 250_000);
    assert!(got[..100_000] == body[..], "the original bytes moved");
    assert!(
        got[100_000..].iter().all(|b| *b == 0),
        "the grown region must be zeroes"
    );
}

// --- the cost claim ----------------------------------------------------------

/// **The point of the change.** A small write to a large file touches an amount of
/// data proportional to the write, not to the file.
///
/// Asserted on bytes moved through the content store rather than on wall-clock
/// time, so it is a statement about the algorithm and not about the runner.
#[tokio::test]
async fn a_small_write_to_a_large_file_touches_little_data() {
    let (fs, store) = fixture().await;
    fs.vfs_create(INO_ROOT, "big.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/big.bin").await.unwrap().ino;

    const SIZE: usize = 4 << 20; // 4 MiB
    fs.vfs_write(ino, 0, &media(SIZE, 13)).await.unwrap();

    store.reset();
    fs.vfs_write(ino, SIZE as u64 / 2, &media(4096, 77))
        .await
        .unwrap();

    let touched = store.put_bytes() + store.get_bytes();
    // The old path read and re-wrote the whole 4 MiB, i.e. ~8 MiB touched. The new
    // one reads and rewrites the widened window: a handful of chunks either side,
    // bounded by MAX_CHUNK. Half the file is a generous ceiling that still fails
    // decisively against a whole-file rewrite.
    assert!(
        touched < SIZE / 2,
        "a 4 KiB write to a {SIZE}-byte file touched {touched} bytes; the write \
         path is still proportional to the file"
    );
    println!("4 KiB write into {SIZE} bytes touched {touched} bytes");
}

/// Truncating a large file to nothing does not first read and re-chunk all of it.
#[tokio::test]
async fn truncating_to_zero_does_not_read_the_file() {
    let (fs, store) = fixture().await;
    fs.vfs_create(INO_ROOT, "big.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/big.bin").await.unwrap().ino;
    const SIZE: usize = 2 << 20;
    fs.vfs_write(ino, 0, &media(SIZE, 13)).await.unwrap();

    store.reset();
    fs.vfs_truncate(ino, 0).await.unwrap();

    let touched = store.put_bytes() + store.get_bytes();
    assert!(
        touched < 64 * 1024,
        "truncating {SIZE} bytes to zero touched {touched} bytes; it should touch \
         essentially nothing"
    );
    assert_eq!(fs.read("/big.bin").await.unwrap().len(), 0);
}

/// A sequential rewrite is **linear**, not quadratic: doubling the file must not
/// quadruple the work.
///
/// This is the property `docs/LIMITS.md` warned about, measured directly.
#[tokio::test]
async fn a_sequential_rewrite_is_linear_not_quadratic() {
    async fn cost(size: usize) -> usize {
        let (fs, store) = fixture().await;
        fs.vfs_create(INO_ROOT, "s.bin", 0o644, Owner::ROOT)
            .await
            .unwrap();
        let ino = fs.stat("/s.bin").await.unwrap().ino;
        let body = media(size, 31);
        store.reset();
        for (i, part) in body.chunks(128 * 1024).enumerate() {
            fs.vfs_write(ino, (i * 128 * 1024) as u64, part)
                .await
                .unwrap();
        }
        store.put_bytes() + store.get_bytes()
    }

    let small = cost(1 << 20).await;
    let large = cost(4 << 20).await;

    // 4x the file. Linear would be ~4x the work; the old quadratic path was ~16x.
    let ratio = large as f64 / small as f64;
    println!("1 MiB: {small} bytes, 4 MiB: {large} bytes (ratio {ratio:.1}x)");
    assert!(
        ratio < 8.0,
        "quadrupling the file multiplied the work by {ratio:.1}x; a quadratic write \
         path gives ~16x, a linear one ~4x"
    );
}

/// Splicing preserves deduplication: rewriting a region with the *same* bytes it
/// already held must not mint new chunks for the untouched remainder.
#[tokio::test]
async fn splicing_preserves_deduplication() {
    let (fs, store) = fixture().await;
    fs.vfs_create(INO_ROOT, "d.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/d.bin").await.unwrap().ino;
    let body = media(1 << 20, 17);
    fs.vfs_write(ino, 0, &body).await.unwrap();
    let objects_before = store.inner.list().await.unwrap().len();

    // Rewrite a slice with exactly the bytes already there.
    fs.vfs_write(ino, 400_000, &body[400_000..404_096])
        .await
        .unwrap();

    let objects_after = store.inner.list().await.unwrap().len();
    let minted = objects_after - objects_before;
    assert!(
        minted <= 4,
        "rewriting identical bytes minted {minted} new objects; the untouched \
         remainder should have deduplicated"
    );
    assert_eq!(&fs.read("/d.bin").await.unwrap()[..], &body[..]);
}

// --- zero-length writes ------------------------------------------------------

/// A zero-length write has no effect and, above all, does not panic.
///
/// `write(2)` says a zero-count write to a regular file "may return zero and have
/// no other results". The splice path did not treat it as its own case: no chunk
/// satisfies `cend > offset && cstart < end` when `end == offset`, so an empty
/// write *inside* the file fell through to the past-EOF branch and anchored on the
/// file's tail. `region_start` then sat past `offset`, and `offset - region_start`
/// underflowed — a subtract-overflow panic in debug, an out-of-bounds slice index
/// in release.
///
/// Offset 0 of any file with three or more chunks was enough, and `nfs.rs` hands a
/// zero-count NFSv3 WRITE straight through to `vfs_write`, so a client reached it.
#[tokio::test]
async fn a_zero_length_write_is_a_no_op_everywhere() {
    let (fs, _s) = fixture().await;
    fs.vfs_create(INO_ROOT, "z.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/z.bin").await.unwrap().ino;
    let body = media(1 << 20, 23);
    fs.vfs_write(ino, 0, &body).await.unwrap();

    // Every interesting position: the front (the original repro), mid-chunk,
    // offsets that land on or near chunk boundaries, exactly EOF, and past EOF —
    // which must *not* extend the file the way a non-empty write past EOF would.
    // Which offsets are true boundaries depends on the content, and correctness
    // here must not depend on knowing them.
    let offsets = [0u64, 1, 4096, 65_536, 262_144, 700_000]
        .into_iter()
        .chain([body.len() as u64, body.len() as u64 + 4096]);
    for off in offsets {
        let n = fs.vfs_write(ino, off, &[]).await.unwrap();
        assert_eq!(n, 0, "a zero-length write at {off} reported bytes written");
        let got = fs.read("/z.bin").await.unwrap();
        assert_eq!(
            got.len(),
            body.len(),
            "a zero-length write at {off} changed the size"
        );
        assert!(
            got[..] == body[..],
            "a zero-length write at {off} changed bytes"
        );
    }
}

/// The same, on a file with no body at all — the degenerate manifest.
#[tokio::test]
async fn a_zero_length_write_to_an_empty_file_is_a_no_op() {
    let (fs, _s) = fixture().await;
    fs.vfs_create(INO_ROOT, "e.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/e.bin").await.unwrap().ino;
    assert_eq!(fs.vfs_write(ino, 0, &[]).await.unwrap(), 0);
    assert_eq!(fs.vfs_write(ino, 4096, &[]).await.unwrap(), 0);
    assert_eq!(fs.vfs_getattr(ino).await.unwrap().size, 0);
    assert!(fs.read("/e.bin").await.unwrap().is_empty());
}

// --- holes are not materialized ----------------------------------------------

/// Growing a file is a manifest edit, not a write of the hole.
///
/// Growth used to be `splice_body` of a one-byte write at the new end, which
/// materialized the entire gap first: allocating it, zeroing it, and running
/// FastCDC over all of it. Measured against the content store, so this is a
/// statement about the algorithm rather than about the runner.
#[tokio::test]
async fn growing_a_file_does_not_materialize_the_hole() {
    let (fs, store) = fixture().await;
    fs.vfs_create(INO_ROOT, "h.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/h.bin").await.unwrap().ino;
    let body = media(50_000, 29);
    fs.vfs_write(ino, 0, &body).await.unwrap();

    store.reset();
    let grown = 256 << 20; // 256 MiB
    fs.vfs_truncate(ino, grown).await.unwrap();

    // A hole of any size stores at most two distinct objects (one full zero chunk
    // that every whole chunk of the run shares, plus a short remainder) and the
    // manifest. Anything proportional to the hole means it was materialized.
    let put = store.put_bytes();
    assert!(
        put < 8 << 20,
        "growing to {grown} bytes stored {put} bytes; the hole was materialized"
    );
    assert!(
        store.get_bytes() < 8 << 20,
        "growing re-read {} bytes of the body",
        store.get_bytes()
    );
    assert_eq!(fs.vfs_getattr(ino).await.unwrap().size, grown);

    // And it still reads back correctly at both ends of the hole.
    let head = fs.vfs_read(ino, 0, 50_000).await.unwrap();
    assert!(head[..] == body[..], "the original bytes moved");
    let tail = fs.vfs_read(ino, grown - 4096, 4096).await.unwrap();
    assert_eq!(tail.len(), 4096);
    assert!(tail.iter().all(|b| *b == 0), "the hole must read as zeroes");
    let seam = fs.vfs_read(ino, 49_990, 20).await.unwrap();
    assert!(seam[..10] == body[49_990..], "the seam lost bytes");
    assert!(seam[10..].iter().all(|b| *b == 0), "the seam is not zeroed");
}

/// The same for a write that *starts* past EOF, which leaves a hole behind it.
#[tokio::test]
async fn a_write_far_past_eof_does_not_materialize_the_gap() {
    let (fs, store) = fixture().await;
    fs.vfs_create(INO_ROOT, "p.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/p.bin").await.unwrap().ino;
    let body = media(50_000, 31);
    fs.vfs_write(ino, 0, &body).await.unwrap();

    store.reset();
    let at = 256u64 << 20;
    let patch = media(10, 37);
    fs.vfs_write(ino, at, &patch).await.unwrap();

    let put = store.put_bytes();
    assert!(
        put < 8 << 20,
        "writing 10 bytes at offset {at} stored {put} bytes; the gap was materialized"
    );
    assert_eq!(fs.vfs_getattr(ino).await.unwrap().size, at + 10);
    assert!(fs.vfs_read(ino, 0, 50_000).await.unwrap()[..] == body[..]);
    assert!(fs.vfs_read(ino, at, 10).await.unwrap()[..] == patch[..]);
    assert!(
        fs.vfs_read(ino, at - 4096, 4096)
            .await
            .unwrap()
            .iter()
            .all(|b| *b == 0),
        "the gap must read as zeroes"
    );
}

/// A hole is ordinary content once written into: the bytes land, and the zeroes
/// around them survive.
#[tokio::test]
async fn writing_into_a_hole_is_exact() {
    let (fs, _s) = fixture().await;
    fs.vfs_create(INO_ROOT, "w.bin", 0o644, Owner::ROOT)
        .await
        .unwrap();
    let ino = fs.stat("/w.bin").await.unwrap().ino;
    fs.vfs_truncate(ino, 4 << 20).await.unwrap();

    let patch = media(5000, 41);
    fs.vfs_write(ino, 2 << 20, &patch).await.unwrap();

    let mut want = vec![0u8; 4 << 20];
    want[2 << 20..(2 << 20) + 5000].copy_from_slice(&patch);
    let got = fs.read("/w.bin").await.unwrap();
    assert_eq!(got.len(), want.len());
    assert!(got[..] == want[..], "writing into a hole was not exact");
}
