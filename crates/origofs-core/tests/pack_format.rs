//! Pack objects and pack index entries carry a tag + format version, and packs
//! written before they did still read.
//!
//! The pack layout had no version marker at all: a change to the trailer or the
//! index entry would have been read as valid by an old binary and produce wrong
//! offsets. `VerifyingStore` would eventually catch it as `Corrupt` (chunks are
//! content-addressed), which is safe but tells the operator exactly the wrong
//! story. These tests pin the framing and the legacy fallback.

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

/// Build a pack the way the code did before the footer was versioned:
/// `body ‖ trailer ‖ trailer_len(u32)`, with no tag.
fn legacy_pack(chunks: &[&[u8]]) -> Vec<u8> {
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
    buf
}

/// A pre-versioning index entry: the bare 40-byte body, no `ORGI` header.
fn legacy_index_entry(pack: Hash, offset: u32, len: u32) -> Vec<u8> {
    let mut e = Vec::with_capacity(40);
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

/// Packs and index entries written before this change must keep working — the
/// whole point of a version byte is that it doesn't orphan what came before.
#[tokio::test]
async fn legacy_packs_and_index_entries_still_read_and_repack() {
    let (store, data, index) = packed(1 << 20);
    let a = b"first legacy chunk".as_slice();
    let b = b"second legacy chunk".as_slice();

    let pack = data.put(&legacy_pack(&[a, b])).await.unwrap();
    index
        .put_keyed(&Hash::of(a), &legacy_index_entry(pack, 0, a.len() as u32))
        .await
        .unwrap();
    index
        .put_keyed(
            &Hash::of(b),
            &legacy_index_entry(pack, a.len() as u32, b.len() as u32),
        )
        .await
        .unwrap();

    // Reads resolve through the untagged index entry into the untagged pack.
    assert_eq!(&store.get(&Hash::of(a)).await.unwrap()[..], a);
    assert_eq!(&store.get(&Hash::of(b)).await.unwrap()[..], b);

    // `repack` parses the legacy trailer to learn the pack's membership; dropping
    // one chunk must rewrite the survivor rather than choke on the old framing.
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
    assert_eq!(
        &bytes[bytes.len() - 5..],
        b"ORGP\x01",
        "the rewritten pack is written in the current format"
    );
}

/// A pack from a future origofs is refused, not misread as a legacy footer.
#[tokio::test]
async fn a_too_new_pack_footer_is_refused() {
    let (store, data, index) = packed(1 << 20);
    let a = b"chunk in a pack from the future".as_slice();

    let mut pack_bytes = legacy_pack(&[a]);
    pack_bytes.extend_from_slice(b"ORGP\x02");
    let pack = data.put(&pack_bytes).await.unwrap();
    index
        .put_keyed(&Hash::of(a), &legacy_index_entry(pack, 0, a.len() as u32))
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

/// Likewise for an index entry — and a truncated one stays plain malformed, so a
/// short read never masquerades as a version problem.
#[tokio::test]
async fn a_too_new_index_entry_is_refused() {
    let (store, data, index) = packed(1 << 20);
    let a = b"a chunk".as_slice();
    let pack = data.put(&legacy_pack(&[a])).await.unwrap();

    let mut entry = b"ORGI\x02".to_vec();
    entry.extend_from_slice(&legacy_index_entry(pack, 0, a.len() as u32));
    index.put_keyed(&Hash::of(a), &entry).await.unwrap();

    let err = match store.get(&Hash::of(a)).await {
        Err(e) => e,
        Ok(_) => panic!("read accepted an index entry it cannot parse"),
    };
    assert!(err.is_unsupported_version(), "got {err}");
    assert!(err.to_string().contains("pack index entry"), "{err}");
}

/// A 41-byte entry is neither a legacy 40-byte body nor a valid tagged one, so it
/// stays plain malformed — a short read must never masquerade as "too new".
#[tokio::test]
async fn a_malformed_index_entry_is_not_reported_as_too_new() {
    let (store, _data, index) = packed(1 << 20);
    let a = b"a chunk".as_slice();
    index.put_keyed(&Hash::of(a), &[0u8; 41]).await.unwrap();

    let err = match store.get(&Hash::of(a)).await {
        Err(e) => e,
        Ok(_) => panic!("read accepted a malformed index entry"),
    };
    assert!(
        !err.is_unsupported_version(),
        "truncation is not a version problem: {err}"
    );
    assert_eq!(err.code(), "content_error");
}
