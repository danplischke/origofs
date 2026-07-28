//! Content integrity (#34 · M1): a bit-rotted / tampered chunk is never served
//! as authentic. `VerifyingStore` re-hashes on read at the content-addressed
//! boundary, and `PackStore::repack` re-hashes survivors before copying them, so
//! corruption surfaces as `OrigoFSError::Corrupt` instead of silently propagating.

use origofs_core::{
    ContentStore, EncryptedStore, LocalCasStore, OrigoFSError, PackStore, VerifyingStore,
};
use std::sync::Arc;

/// The on-disk object path for a hash in a `LocalCasStore` rooted at `root`.
fn object_path(root: &std::path::Path, hex: &str) -> std::path::PathBuf {
    root.join("objects").join(&hex[0..2]).join(&hex[2..])
}

/// A flipped byte in a stored object is served silently by the bare store, but
/// rejected as `Corrupt` once reads go through `VerifyingStore` — for whole reads
/// and ranged reads alike.
#[tokio::test]
async fn verifying_store_detects_a_flipped_byte() {
    let dir = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());
    let plain = b"the quick brown fox jumps over the lazy dog";
    let h = local.put(plain).await.unwrap();

    // Corrupt the object on disk (bit-rot / tamper).
    let obj = object_path(dir.path(), &h.to_hex());
    let mut raw = std::fs::read(&obj).unwrap();
    raw[0] ^= 0xff;
    std::fs::write(&obj, &raw).unwrap();

    // Fail-before: the bare store hands back the corrupted bytes as authentic.
    let served = local.get(&h).await.unwrap();
    assert_ne!(&served[..], &plain[..], "bare store served corrupted bytes");

    // Pass-after: wrapped in VerifyingStore, the read is rejected.
    let verified = VerifyingStore::new(local.clone());
    assert!(
        matches!(verified.get(&h).await, Err(OrigoFSError::Corrupt(_))),
        "whole read must be Corrupt"
    );
    assert!(
        matches!(
            verified.get_range(&h, 0, 4).await,
            Err(OrigoFSError::Corrupt(_))
        ),
        "ranged read must be Corrupt too"
    );
}

/// `repack` must not launder a corrupt chunk into a fresh pack (and then delete
/// the old copy): it re-hashes each survivor and refuses a mismatch.
#[tokio::test]
async fn repack_refuses_to_relaunder_a_corrupt_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let data = Arc::new(LocalCasStore::open(dir.path().join("data")).await.unwrap());
    let index = Arc::new(LocalCasStore::open(dir.path().join("index")).await.unwrap());
    let pack = PackStore::with_target(
        data.clone() as Arc<dyn ContentStore>,
        index.clone() as Arc<dyn ContentStore>,
        1 << 20, // large target: seal only on explicit flush
    );

    // Two chunks in one pack; keep one live and delete the other -> partially
    // dead, which drives repack's survivor-copy path.
    let keep = pack
        .put(b"surviving chunk AAAAAAAAAAAAAAAAAAAA")
        .await
        .unwrap();
    let dead = pack
        .put(b"dead chunk BBBBBBBBBBBBBBBBBBBBBBBBBB")
        .await
        .unwrap();
    pack.flush().await.unwrap();
    pack.delete(&dead).await.unwrap();

    // Corrupt the survivor's bytes inside the pack object (it was staged first,
    // so it lives at offset 0).
    let packs = data.list().await.unwrap();
    assert_eq!(packs.len(), 1, "exactly one sealed pack");
    let pobj = object_path(&dir.path().join("data"), &packs[0].to_hex());
    let mut raw = std::fs::read(&pobj).unwrap();
    raw[0] ^= 0xff;
    std::fs::write(&pobj, &raw).unwrap();

    assert!(
        matches!(pack.repack().await, Err(OrigoFSError::Corrupt(_))),
        "repack must refuse to copy a corrupt survivor"
    );
    let _ = keep; // silence unused
}

/// Sanity: verification passes honest reads through unchanged, whole and ranged.
#[tokio::test]
async fn verifying_store_passes_good_reads_through() {
    let dir = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());
    let v = VerifyingStore::new(local);
    let h = v.put(b"honest bytes").await.unwrap();
    assert_eq!(&v.get(&h).await.unwrap()[..], b"honest bytes");
    assert_eq!(&v.get_range(&h, 0, 6).await.unwrap()[..], b"honest");
    assert!(v.has(&h).await.unwrap());
}

/// Verification composes with encryption: the address is the *plaintext* hash, so
/// `VerifyingStore` over an `EncryptedStore` round-trips (it must not mistake the
/// decrypted plaintext, whose hash is the address, for corruption).
#[tokio::test]
async fn verifying_store_over_encryption_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ContentStore> = Arc::new(LocalCasStore::open(dir.path()).await.unwrap());
    let enc =
        Arc::new(EncryptedStore::from_passphrase(backend, "hunter2", b"test salt 16byte").unwrap());
    let v = VerifyingStore::new(enc);
    let h = v.put(b"secret plaintext").await.unwrap();
    assert_eq!(&v.get(&h).await.unwrap()[..], b"secret plaintext");
}
