//! Chunking + manifest behavior: dedup on edit, ranged reads across chunk
//! boundaries, streaming writes, and the engine running over the object-store
//! backend end to end.

use origofs_core::chunk::chunk_bounds;
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
