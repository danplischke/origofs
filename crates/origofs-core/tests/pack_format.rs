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

/// Hand-build an index entry at an arbitrary version, likewise. v1 entries end
/// after the body; v2 appends the addressing flag (0 = content-addressed).
fn index_entry(pack: Hash, offset: u32, len: u32, version: u8) -> Vec<u8> {
    let mut e = Vec::with_capacity(46);
    e.extend_from_slice(b"ORGI");
    e.push(version);
    e.extend_from_slice(pack.as_bytes());
    e.extend_from_slice(&offset.to_le_bytes());
    e.extend_from_slice(&len.to_le_bytes());
    if version >= 2 {
        e.push(0);
    }
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
    assert_eq!(&entry[..5], b"ORGI\x02", "index entry header");
    assert_eq!(entry.len(), 46, "5-byte header + the 41-byte body");
    assert_eq!(
        entry[45], 0,
        "a chunk written through `put` is content-addressed"
    );
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
        .put_keyed(&Hash::of(a), &index_entry(pack, 0, a.len() as u32, 3))
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
    // 40 zero bytes: the body with no header at all. 46 with a wrong tag. 41:
    // a v2-length body behind no header.
    for entry in [vec![0u8; 40], vec![0u8; 46], vec![0u8; 41]] {
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

/// A **keyed** value — one a transforming layer stored under an address that is
/// deliberately not its hash, as `EncryptedStore` does with ciphertext — survives
/// a repack instead of being reported as corruption.
///
/// This is the encrypted+packed composition `origofs-cli` builds whenever
/// `ORIGOFS_ENCRYPTION_KEY` is set alongside `packed = true`. Repack used to
/// re-hash every survivor and refuse a mismatch, which is true of every chunk in
/// that stack, so the first partially-dead pack failed the whole operation with a
/// `Corrupt` error and its dead space was unreclaimable for good.
#[tokio::test]
async fn a_repack_keeps_values_whose_key_is_not_their_hash() {
    let (store, data, _index) = packed(1 << 20);
    let keep = Hash::of(b"address of the kept value");
    let drop = Hash::of(b"address of the dropped value");

    // Ciphertext-shaped: the bytes do not hash to the key they are stored under.
    store
        .put_keyed(&keep, b"opaque bytes for keep")
        .await
        .unwrap();
    store
        .put_keyed(&drop, b"opaque bytes for drop")
        .await
        .unwrap();
    store.flush().await.unwrap();
    assert_eq!(data.list().await.unwrap().len(), 1, "one sealed pack");

    // Drop one so the pack is partially dead — the branch that rewrites survivors.
    store.delete(&drop).await.unwrap();
    store.repack().await.unwrap();

    assert_eq!(
        &store.get(&keep).await.unwrap()[..],
        b"opaque bytes for keep",
        "the surviving keyed value must still read back"
    );
    assert!(
        store.get(&drop).await.is_err(),
        "the dropped value must be gone"
    );
}

/// The flag round-trips: a keyed value's index entry records that it is *not*
/// content-addressed, which is what tells a later repack to skip re-hashing it.
#[tokio::test]
async fn an_index_entry_records_how_its_value_is_addressed() {
    let (store, _data, index) = packed(1 << 20);
    let content = store.put(b"addressed by its own hash").await.unwrap();
    let keyed = Hash::of(b"some other address");
    store
        .put_keyed(&keyed, b"bytes that hash to something else")
        .await
        .unwrap();
    store.flush().await.unwrap();

    assert_eq!(
        index.get(&content).await.unwrap()[45],
        0,
        "content-addressed"
    );
    assert_eq!(index.get(&keyed).await.unwrap()[45], 1, "keyed");
}

/// A **v1** entry whose bytes do not hash to its key leaves the pack alone rather
/// than failing the whole repack.
///
/// v1 carries no addressing flag, so this is either corruption or a keyed value
/// written before the flag existed — indistinguishable. Skipping is the one action
/// that is safe under both readings: nothing is laundered into a fresh pack and no
/// evidence is deleted, which is what the integrity check exists for. It also
/// un-breaks legacy encrypted+packed stores, whose every entry looks like this.
#[tokio::test]
async fn a_v1_entry_that_cannot_be_verified_leaves_its_pack_alone() {
    let (store, data, index) = packed(1 << 20);
    // Two chunks in a pack, indexed under keys that are *not* their hashes.
    let a = b"opaque bytes one".as_slice();
    let b = b"opaque bytes two".as_slice();
    let key_a = Hash::of(b"address of a");
    let key_b = Hash::of(b"address of b");

    let mut buf = Vec::new();
    buf.extend_from_slice(a);
    buf.extend_from_slice(b);
    let body_len = buf.len();
    for (k, c) in [(key_a, a), (key_b, b)] {
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(&(c.len() as u32).to_le_bytes());
    }
    let tlen = (buf.len() - body_len) as u32;
    buf.extend_from_slice(&tlen.to_le_bytes());
    buf.extend_from_slice(b"ORGP");
    buf.push(1);
    let pack = data.put(&buf).await.unwrap();

    index
        .put_keyed(&key_a, &index_entry(pack, 0, a.len() as u32, 1))
        .await
        .unwrap();
    index
        .put_keyed(
            &key_b,
            &index_entry(pack, a.len() as u32, b.len() as u32, 1),
        )
        .await
        .unwrap();

    // Kill one so the pack is partially dead — the branch that verifies survivors.
    store.delete(&key_a).await.unwrap();
    store
        .repack()
        .await
        .expect("an unverifiable v1 entry must not fail the whole repack");

    assert!(
        data.has(&pack).await.unwrap(),
        "the pack must be left in place, not deleted"
    );
    assert_eq!(
        &store.get(&key_b).await.unwrap()[..],
        b,
        "the surviving value must still read back"
    );
}
