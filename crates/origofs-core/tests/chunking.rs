//! Chunking + manifest behavior: dedup on edit, ranged reads across chunk
//! boundaries, streaming writes, and the engine running over the object-store
//! backend end to end.

use origofs_core::chunk::{MAX_CHUNK, MIN_CHUNK, chunk_bounds};
use origofs_core::{
    ChunkRef, Fs, Hash, Manifest, MemStore, ObjectContentStore, SqliteMetadataStore,
};
use std::sync::Arc;

/// Deterministic pseudo-random bytes (xorshift64) — enough entropy for CDC to
/// find multiple content-defined boundaries.
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
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

#[test]
fn manifest_roundtrip() {
    let m = Manifest {
        size: 40,
        chunks: vec![
            ChunkRef {
                hash: Hash::of(b"a"),
                len: 16,
            },
            ChunkRef {
                hash: Hash::of(b"bc"),
                len: 24,
            },
        ],
    };
    let decoded = Manifest::decode(&m.encode()).unwrap();
    assert_eq!(decoded, m);
    // identical content => identical manifest bytes (stable serialization)
    assert_eq!(m.encode(), decoded.encode());
    // garbage is rejected
    assert!(Manifest::decode(b"not a manifest").is_err());
    assert!(Manifest::decode(&[]).is_err());
}

async fn mem_fs() -> (Fs<SqliteMetadataStore, Arc<MemStore>>, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store.clone());
    fs.init().await.unwrap();
    (fs, store)
}

#[tokio::test]
async fn large_file_roundtrip_and_multiple_chunks() {
    let (fs, _store) = mem_fs().await;
    let data = pseudo_random(1_000_000, 42);
    assert!(
        chunk_bounds(&data).len() >= 8,
        "1MB should split into many chunks"
    );

    fs.write("/big", &data).await.unwrap();
    let got = fs.read("/big").await.unwrap();
    assert_eq!(&got[..], &data[..]);
    assert_eq!(fs.stat("/big").await.unwrap().size, data.len() as u64);
}

#[tokio::test]
async fn edit_rewrites_only_touched_chunks() {
    let (fs, store) = mem_fs().await;
    let data = pseudo_random(1_000_000, 7);
    let n_chunks = chunk_bounds(&data).len();

    fs.write("/a", &data).await.unwrap();
    let after_first = store.len(); // chunks + 1 manifest

    // Localized edit near the start; CDC keeps later boundaries stable.
    let mut edited = data.clone();
    for b in edited.iter_mut().take(32) {
        *b ^= 0xFF;
    }
    fs.write("/b", &edited).await.unwrap();
    let after_edit = store.len();

    let new_objects = after_edit - after_first;
    assert!(n_chunks >= 8);
    assert!(
        new_objects <= 5,
        "a localized edit stored {new_objects} new objects across {n_chunks} chunks; expected only a few"
    );
    assert_eq!(&fs.read("/b").await.unwrap()[..], &edited[..]);
    assert_eq!(&fs.read("/a").await.unwrap()[..], &data[..]);
}

#[tokio::test]
async fn identical_content_fully_dedups() {
    let (fs, store) = mem_fs().await;
    let data = pseudo_random(500_000, 99);
    fs.write("/x", &data).await.unwrap();
    let n = store.len();
    // Same bytes at a different path add no new objects (chunks + manifest reused).
    fs.write("/y", &data).await.unwrap();
    assert_eq!(store.len(), n);
}

#[tokio::test]
async fn ranged_reads_across_boundaries() {
    let (fs, _store) = mem_fs().await;
    let data = pseudo_random(400_000, 5);
    fs.write("/f", &data).await.unwrap();

    let cases = [
        (0u64, 10u64),
        (65_530, 20),
        (100_000, 150_000),
        (399_990, 100),
    ];
    for (off, len) in cases {
        let got = fs.read_range("/f", off, len).await.unwrap();
        let start = (off as usize).min(data.len());
        let end = start.saturating_add(len as usize).min(data.len());
        assert_eq!(&got[..], &data[start..end], "range {off}+{len}");
    }
}

#[tokio::test]
async fn streaming_write_matches_in_memory_write() {
    let (fs, store) = mem_fs().await;
    let data = pseudo_random(800_000, 123);

    fs.write("/mem", &data).await.unwrap();
    let after_mem = store.len();

    // StreamCDC must find the same boundaries as the in-memory chunker, so
    // streaming the identical bytes adds no new objects.
    fs.write_reader("/stream", std::io::Cursor::new(data.clone()))
        .await
        .unwrap();
    assert_eq!(&fs.read("/stream").await.unwrap()[..], &data[..]);
    assert_eq!(
        store.len(),
        after_mem,
        "streaming chunker should agree with the in-memory chunker"
    );
}

#[tokio::test]
async fn engine_over_object_store_backend() {
    // The same ObjectContentStore adapter used for S3, in-memory here.
    let store = ObjectContentStore::in_memory();
    let meta = SqliteMetadataStore::open_in_memory().unwrap();
    let fs = Fs::new(meta, store);
    fs.init().await.unwrap();

    fs.mkdir_p("/d").await.unwrap();
    let data = pseudo_random(300_000, 314);
    fs.write("/d/big", &data).await.unwrap();
    assert_eq!(&fs.read("/d/big").await.unwrap()[..], &data[..]);
    assert_eq!(
        &fs.read_range("/d/big", 12_345, 50_000).await.unwrap()[..],
        &data[12_345..62_345]
    );

    fs.write("/d/small", b"hi").await.unwrap();
    assert_eq!(&fs.read("/d/small").await.unwrap()[..], b"hi");
}

// A7 (issue #70): the chunker must tile the input *exactly* — contiguous, no gaps
// or overlaps, covering every byte — across boundary sizes (empty, 1 byte, and
// around MIN/AVG/MAX chunk sizes). And every chunk obeys the size bounds: at most
// MAX_CHUNK, and (except the final chunk) at least MIN_CHUNK. This is the core
// correctness property behind `concat(chunks) == input`.
#[test]
fn chunk_bounds_cover_and_bound_all_sizes() {
    let min = MIN_CHUNK as usize;
    let max = MAX_CHUNK as usize;
    let sizes = [
        0,
        1,
        2,
        min - 1,
        min,
        min + 1,
        64 * 1024, // ~AVG chunk size
        max - 1,
        max,
        max + 1,
        2 * max,
        3 * max + 123,
        1_000_000,
    ];
    for &n in &sizes {
        let data = pseudo_random(n, 0x00C0_FFEE ^ n as u64);
        let bounds = chunk_bounds(&data);

        if n == 0 {
            assert!(bounds.is_empty(), "empty input must yield no chunks");
            continue;
        }

        // Contiguous tiling from offset 0, covering exactly `n` bytes.
        let mut pos = 0usize;
        for (i, &(off, len)) in bounds.iter().enumerate() {
            assert_eq!(
                off, pos,
                "size {n}: chunk {i} must start where the previous chunk ended"
            );
            assert!(len > 0, "size {n}: chunk {i} is zero-length");
            assert!(
                len <= max,
                "size {n}: chunk {i} len {len} exceeds MAX {max}"
            );
            let is_last = i + 1 == bounds.len();
            if !is_last {
                assert!(
                    len >= min,
                    "size {n}: non-final chunk {i} len {len} below MIN {min}"
                );
            }
            pos += len;
        }
        assert_eq!(pos, n, "size {n}: chunks must cover the whole input");

        // Reassembly is byte-identical: concat(chunks) == input.
        let mut reassembled = Vec::with_capacity(n);
        for &(off, len) in &bounds {
            reassembled.extend_from_slice(&data[off..off + len]);
        }
        assert_eq!(
            reassembled, data,
            "size {n}: concatenating the chunks must reproduce the input"
        );

        // A file at or below MIN can't be cut before MIN — it's a single chunk.
        if n <= min {
            assert_eq!(bounds.len(), 1, "size {n}: <= MIN must be a single chunk");
        }
    }
}

// A7 (issue #70): the engine round-trips files at chunk-size boundaries exactly,
// including the degenerate empty and single-byte files, and reports the right
// size for each.
#[tokio::test]
async fn engine_roundtrips_boundary_sizes() {
    let (fs, _store) = mem_fs().await;
    let min = MIN_CHUNK as usize;
    let max = MAX_CHUNK as usize;
    let sizes = [
        0usize,
        1,
        min - 1,
        min,
        min + 1,
        max - 1,
        max,
        max + 1,
        2 * max,
    ];

    for &n in &sizes {
        let data = pseudo_random(n, 0x0000_A5A5 ^ n as u64);
        let path = format!("/f{n}");
        fs.write(&path, &data).await.unwrap();
        assert_eq!(
            &fs.read(&path).await.unwrap()[..],
            &data[..],
            "roundtrip at size {n}"
        );
        assert_eq!(
            fs.stat(&path).await.unwrap().size,
            n as u64,
            "stat size at {n}"
        );
    }
}

// A7 (issue #70): ranged reads with out-of-bounds or zero-length windows return
// an empty slice rather than erroring or over-reading, and a huge `len` saturates
// to EOF instead of overflowing.
#[tokio::test]
async fn ranged_read_out_of_bounds_is_empty() {
    let (fs, _store) = mem_fs().await;
    let data = pseudo_random(300_000, 77);
    fs.write("/f", &data).await.unwrap();
    let size = data.len() as u64;

    // zero length
    assert!(fs.read_range("/f", 100, 0).await.unwrap().is_empty());
    // offset exactly at EOF
    assert!(fs.read_range("/f", size, 10).await.unwrap().is_empty());
    // offset past EOF
    assert!(
        fs.read_range("/f", size + 999, 10)
            .await
            .unwrap()
            .is_empty()
    );
    // len saturates to EOF (no overflow), returning just the tail
    let tail = fs.read_range("/f", size - 10, u64::MAX).await.unwrap();
    assert_eq!(&tail[..], &data[data.len() - 10..]);

    // empty file: any range is empty.
    fs.write("/empty", b"").await.unwrap();
    assert!(fs.read_range("/empty", 0, 100).await.unwrap().is_empty());
}
