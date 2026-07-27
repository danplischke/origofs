//! Pack layer: many small chunks collapse into few large objects, reads are
//! ranged into a pack, the index survives a reopen, and repack reclaims the
//! space of deleted chunks — plus end-to-end through the engine.

use origofs_core::{ContentStore, Fs, Hash, MemStore, PackStore, SqliteMetadataStore};
use std::sync::Arc;

/// (pack_store, data backend, index backend), target = `target` bytes.
fn packed(target: usize) -> (PackStore, Arc<MemStore>, Arc<MemStore>) {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());
    let store = PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        target,
    );
    (store, data, index)
}

fn blob(len: usize, seed: u64) -> Vec<u8> {
    // Distinct seeds must yield distinct bytes; xorshift only needs non-zero state.
    let mut x = if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    };
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

#[tokio::test]
async fn many_chunks_collapse_into_few_packs() {
    let (store, data, index) = packed(4096);

    let mut hashes = Vec::new();
    for i in 0..100u64 {
        hashes.push(store.put(&blob(200, i)).await.unwrap());
    }
    store.flush().await.unwrap();

    // 100 chunks × 200 B into 4 KiB packs ≈ 5 objects, nowhere near 100.
    assert!(data.len() <= 8, "packs: {}", data.len());
    assert!(data.len() < 100);
    assert_eq!(index.len(), 100, "one index entry per chunk");

    // every chunk still reads back exactly
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(&store.get(h).await.unwrap()[..], &blob(200, i as u64)[..]);
    }
}

#[tokio::test]
async fn unflushed_reads_and_dedup() {
    let (store, data, _index) = packed(1 << 20);
    let h = store.put(b"hello pack").await.unwrap();
    assert_eq!(h, Hash::of(b"hello pack"));

    // readable before sealing (served from the open buffer), and no pack yet
    assert_eq!(&store.get(&h).await.unwrap()[..], b"hello pack");
    assert!(store.has(&h).await.unwrap());
    assert!(store.list().await.unwrap().contains(&h));
    assert_eq!(data.len(), 0, "nothing sealed yet");

    // storing identical bytes is a no-op
    store.put(b"hello pack").await.unwrap();

    store.flush().await.unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(&store.get(&h).await.unwrap()[..], b"hello pack");
}

#[tokio::test]
async fn ranged_read_into_a_pack() {
    let (store, _data, _index) = packed(1 << 20);
    let body = blob(1000, 9);
    let h = store.put(&body).await.unwrap();
    store.flush().await.unwrap();
    assert_eq!(
        &store.get_range(&h, 10, 5).await.unwrap()[..],
        &body[10..15]
    );
    assert_eq!(
        &store.get_range(&h, 995, 50).await.unwrap()[..],
        &body[995..1000]
    );
}

#[tokio::test]
async fn index_survives_a_reopen() {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());

    let hashes = {
        let store = PackStore::new(
            data.clone() as Arc<dyn ContentStore>,
            index.clone() as Arc<dyn ContentStore>,
        );
        let mut hs = Vec::new();
        for i in 0..20u64 {
            hs.push(store.put(&blob(500, i)).await.unwrap());
        }
        store.flush().await.unwrap();
        hs
    };

    // A brand-new PackStore over the same backends (no in-memory buffer) still
    // resolves every chunk through the persisted index + packs.
    let reopened = PackStore::new(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
    );
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(
            &reopened.get(h).await.unwrap()[..],
            &blob(500, i as u64)[..]
        );
    }
}

#[tokio::test]
async fn repack_reclaims_deleted_chunks() {
    // target 1500 with 1000-byte chunks => two chunks per pack.
    let (store, data, _index) = packed(1500);
    let mut h = Vec::new();
    for i in 0..10u64 {
        h.push(store.put(&blob(1000, i)).await.unwrap());
    }
    store.flush().await.unwrap();
    let packs_before = data.len();
    assert_eq!(packs_before, 5);

    // pack0 = {h0,h1} fully dead; pack2 = {h4,h5} partially dead.
    store.delete(&h[0]).await.unwrap();
    store.delete(&h[1]).await.unwrap();
    store.delete(&h[4]).await.unwrap();

    let reclaimed = store.repack().await.unwrap();
    assert!(reclaimed > 0, "dead pack bytes were reclaimed");
    assert!(data.len() < packs_before, "fewer pack objects after repack");

    // deleted chunks are gone; everything else still reads.
    for i in [0usize, 1, 4] {
        assert!(store.get(&h[i]).await.is_err());
    }
    for i in [2usize, 3, 5, 6, 7, 8, 9] {
        assert_eq!(
            &store.get(&h[i]).await.unwrap()[..],
            &blob(1000, i as u64)[..]
        );
    }
}

#[tokio::test]
async fn engine_writes_land_in_packs() {
    let data = Arc::new(MemStore::new());
    let index = Arc::new(MemStore::new());
    let store = Arc::new(PackStore::new(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
    ));
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), store);
    fs.init().await.unwrap();

    let big = blob(2 * 1024 * 1024, 42); // many FastCDC chunks
    fs.write("/big.bin", &big).await.unwrap();
    fs.commit("packer", "snapshot").await.unwrap(); // flushes the open pack

    // The workspace round-trips...
    assert_eq!(&fs.read("/big.bin").await.unwrap()[..], &big[..]);
    // ...and the many logical objects (chunks + manifest + tree + commit, plus
    // the ref-mirror snapshot the commit seals for recovery) live in far fewer
    // physical pack objects. (Per-commit mirror packs are later coalesced by
    // `repack`.)
    assert!(
        data.len() < index.len(),
        "packs {} should be far fewer than objects {}",
        data.len(),
        index.len()
    );
    assert!(index.len() > 8, "the big file produced many chunks");
    assert!(
        data.len() <= 3,
        "they packed into a handful of objects: {}",
        data.len()
    );
}
