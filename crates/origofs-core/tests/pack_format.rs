//! Pack objects and pack index entries carry a tag + format version.
//!
//! The pack layout had no version marker at all: a change to the trailer or the
//! index entry would have been read as valid by an old binary and produce wrong
//! offsets. `VerifyingStore` would eventually catch it as `Corrupt` (chunks are
//! content-addressed), which is safe but tells the operator exactly the wrong
//! story. These tests pin the framing.

use origofs_core::{ContentStore, Hash, MemStore, PackStore};
use std::sync::Arc;

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

/// Hand-build a pack — `body ‖ trailer ‖ trailer_len(u32) ‖ ORGP ‖ version` — so a
/// test can plant one at a version the engine would never write.
fn pack_bytes(chunks: &[&[u8]], version: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    for c in chunks {
        buf.extend_from_slice(c);
    }
    let body_len = buf.len();
    for c in chunks {
        buf.extend_from_slice(Hash::of(c).as_bytes());
        buf.extend_from_slice(&(c.len() as u32).to_le_bytes());
    }
    let tlen = (buf.len() - body_len) as u32;
    buf.extend_from_slice(&tlen.to_le_bytes());
    buf.extend_from_slice(b"ORGP");
    buf.push(version);
    buf
}

/// Hand-build an index entry at an arbitrary version, likewise.
fn index_entry(pack: Hash, offset: u32, len: u32, version: u8) -> Vec<u8> {
    let mut e = Vec::with_capacity(45);
    e.extend_from_slice(b"ORGI");
    e.push(version);
    e.extend_from_slice(pack.as_bytes());
    e.extend_from_slice(&offset.to_le_bytes());
    e.extend_from_slice(&len.to_le_bytes());
    e
}

#[tokio::test]
async fn sealed_packs_and_index_entries_are_tagged() {
    let (store, data, index) = packed(64);
    let chunk = store.put(b"the quick brown fox").await.unwrap();
    store.flush().await.unwrap();

    let pack = data.list().await.unwrap();
    assert_eq!(pack.len(), 1);
    let bytes = data.get(&pack[0]).await.unwrap();
    assert_eq!(
        &bytes[bytes.len() - 5..],
        b"ORGP\x01",
        "a sealed pack must end with its tag + format version"
    );

    let entry = index.get(&chunk).await.unwrap();
    assert_eq!(&entry[..5], b"ORGI\x01", "index entry header");
    assert_eq!(entry.len(), 45, "5-byte header + the 40-byte body");
}

/// The read path is a ranged GET at the recorded offset, so the footer must not
/// shift any chunk. Round-trip through a sealed pack.
#[tokio::test]
async fn tagging_the_footer_does_not_move_chunks() {
    let (store, _data, _index) = packed(64);
    let mut hashes = Vec::new();
    for i in 0..8u8 {
        hashes.push(store.put(&[i; 40]).await.unwrap());
    }
    store.flush().await.unwrap();

    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(&store.get(h).await.unwrap()[..], &[i as u8; 40][..]);
    }
}

/// `repack` is the only thing that parses a trailer, so exercise it end to end.
#[tokio::test]
async fn repack_reads_the_trailer_and_rewrites_survivors() {
    let (store, data, index) = packed(1 << 20);
    let a = b"first chunk".as_slice();
    let b = b"second chunk".as_slice();

    let pack = data.put(&pack_bytes(&[a, b], 1)).await.unwrap();
    index
        .put_keyed(&Hash::of(a), &index_entry(pack, 0, a.len() as u32, 1))
        .await
        .unwrap();
    index
        .put_keyed(
            &Hash::of(b),
            &index_entry(pack, a.len() as u32, b.len() as u32, 1),
        )
        .await
        .unwrap();

    assert_eq!(&store.get(&Hash::of(a)).await.unwrap()[..], a);
    assert_eq!(&store.get(&Hash::of(b)).await.unwrap()[..], b);

    store.delete(&Hash::of(a)).await.unwrap();
    store.repack().await.unwrap();

    assert_eq!(&store.get(&Hash::of(b)).await.unwrap()[..], b);
    let survivor = data.list().await.unwrap();
    assert_eq!(
        survivor.len(),
        1,
        "the old pack is gone, one fresh pack left"
    );
    let bytes = data.get(&survivor[0]).await.unwrap();
    assert_eq!(&bytes[bytes.len() - 5..], b"ORGP\x01");
}

/// A pack from a future origofs is refused, not parsed into wrong offsets.
#[tokio::test]
async fn a_too_new_pack_footer_is_refused() {
    let (store, data, index) = packed(1 << 20);
    let a = b"chunk in a pack from the future".as_slice();

    let pack = data.put(&pack_bytes(&[a], 2)).await.unwrap();
    index
        .put_keyed(&Hash::of(a), &index_entry(pack, 0, a.len() as u32, 1))
        .await
        .unwrap();

    // Only `repack` parses the trailer; a plain read is offset-driven and fine.
    let err = match store.repack().await {
        Err(e) => e,
        Ok(_) => panic!("repack accepted a pack it cannot parse"),
    };
    assert!(err.is_unsupported_version(), "got {err}");
    assert!(err.to_string().contains("pack"), "{err}");
}

/// Likewise for an index entry.
#[tokio::test]
async fn a_too_new_index_entry_is_refused() {
    let (store, data, index) = packed(1 << 20);
    let a = b"a chunk".as_slice();
    let pack = data.put(&pack_bytes(&[a], 1)).await.unwrap();
    index
        .put_keyed(&Hash::of(a), &index_entry(pack, 0, a.len() as u32, 2))
        .await
        .unwrap();

    let err = match store.get(&Hash::of(a)).await {
        Err(e) => e,
        Ok(_) => panic!("read accepted an index entry it cannot parse"),
    };
    assert!(err.is_unsupported_version(), "got {err}");
    assert!(err.to_string().contains("pack index entry"), "{err}");
}

/// An untagged or wrong-length entry is plain malformed — a short read must never
/// masquerade as "too new", and an entry without the header is not a valid entry.
#[tokio::test]
async fn an_untagged_or_short_index_entry_is_malformed() {
    let a = b"a chunk".as_slice();
    // 40 zero bytes: the body with no header at all. 45 with a wrong tag. 41: short.
    for entry in [vec![0u8; 40], vec![0u8; 45], vec![0u8; 41]] {
        let (store, _data, index) = packed(1 << 20);
        index.put_keyed(&Hash::of(a), &entry).await.unwrap();

        let err = match store.get(&Hash::of(a)).await {
            Err(e) => e,
            Ok(_) => panic!("read accepted a {}-byte index entry", entry.len()),
        };
        assert!(
            !err.is_unsupported_version(),
            "{}-byte entry is not a version problem: {err}",
            entry.len()
        );
        assert_eq!(err.code(), "content_error");
    }
}
