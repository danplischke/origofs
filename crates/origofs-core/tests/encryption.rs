//! Encryption at rest: content is transparent to the engine, ciphertext on the
//! backend never contains the plaintext, dedup survives, GC still works, and the
//! wrong key fails loudly.

use origofs_core::{
    ContentStore, EncryptedStore, Fs, Hash, LocalCasStore, MemStore, SqliteMetadataStore,
};
use std::sync::Arc;

fn key(seed: u8) -> [u8; 32] {
    [seed; 32]
}

#[tokio::test]
async fn roundtrips_through_the_engine() {
    let backend = Arc::new(MemStore::new());
    let enc: Arc<dyn ContentStore> = Arc::new(EncryptedStore::new(backend.clone(), key(1)));
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), enc);
    fs.init().await.unwrap();

    fs.mkdir_p("/dir").await.unwrap();
    fs.write("/dir/a.txt", b"secret contents").await.unwrap();
    let big = vec![7u8; 300 * 1024]; // multi-chunk
    fs.write("/big.bin", &big).await.unwrap();

    assert_eq!(
        &fs.read("/dir/a.txt").await.unwrap()[..],
        b"secret contents"
    );
    assert_eq!(&fs.read("/big.bin").await.unwrap()[..], &big[..]);
    // Ranged reads decrypt correctly too.
    assert_eq!(
        &fs.read_range("/big.bin", 10, 5).await.unwrap()[..],
        &big[10..15]
    );
}

#[tokio::test]
async fn backend_holds_ciphertext_not_plaintext() {
    let backend = Arc::new(MemStore::new());
    let enc = EncryptedStore::new(backend.clone(), key(2));

    let plaintext = b"the quick brown fox jumps over the lazy dog";
    let hash = enc.put(plaintext).await.unwrap();
    // The address is the plaintext hash (transparent to the engine)...
    assert_eq!(hash, Hash::of(plaintext));

    // ...but the bytes actually stored are ciphertext: different, longer (AEAD
    // tag), and not containing the plaintext.
    let stored = backend.get(&hash).await.unwrap();
    assert_ne!(&stored[..], &plaintext[..]);
    assert!(stored.len() > plaintext.len());
    assert!(
        !stored.windows(plaintext.len()).any(|w| w == plaintext),
        "plaintext must not appear in the stored bytes"
    );

    // And it decrypts back.
    assert_eq!(&enc.get(&hash).await.unwrap()[..], &plaintext[..]);
}

#[tokio::test]
async fn dedup_is_preserved() {
    let backend = Arc::new(MemStore::new());
    let enc = EncryptedStore::new(backend.clone(), key(3));

    let h1 = enc.put(b"same bytes").await.unwrap();
    let h2 = enc.put(b"same bytes").await.unwrap();
    assert_eq!(h1, h2);
    assert_eq!(backend.len(), 1, "identical plaintext stored once");

    // Convergent: identical plaintext produces identical ciphertext.
    enc.put(b"other bytes").await.unwrap();
    assert_eq!(backend.len(), 2);
}

#[tokio::test]
async fn wrong_key_fails_loudly() {
    let backend = Arc::new(MemStore::new());
    let writer = EncryptedStore::new(backend.clone(), key(4));
    let hash = writer.put(b"classified").await.unwrap();

    // A reader with a different key must error, not return garbage.
    let reader = EncryptedStore::new(backend.clone(), key(5));
    let err = reader.get(&hash).await.unwrap_err();
    assert!(err.to_string().contains("decryption failed"), "got: {err}");

    // The correct key still works.
    let ok = EncryptedStore::new(backend, key(4));
    assert_eq!(&ok.get(&hash).await.unwrap()[..], b"classified");
}

// A2 (issue #70): reopening an encrypted workspace with the WRONG key must fail
// loudly on read *through the engine*, not just at the raw EncryptedStore layer.
// This is the realistic "reopened with a different ORIGOFS_ENCRYPTION_KEY" case:
// the metadata (the manifest hash on the inode) is intact, but the engine has to
// fetch the encrypted manifest — and then the chunks — from the content store,
// and decrypting them with the wrong key must surface an error, never plaintext
// garbage or a silent short read.
#[tokio::test]
async fn reopen_with_wrong_key_fails_through_the_engine() {
    let backend = Arc::new(MemStore::new());
    let meta = Arc::new(SqliteMetadataStore::open_in_memory().unwrap());

    // Author a workspace with key A. A multi-chunk body guarantees real chunk
    // bytes (not just the manifest) flow through the encrypted content store.
    let enc_a: Arc<dyn ContentStore> = Arc::new(EncryptedStore::new(backend.clone(), key(10)));
    let fs_a = Fs::new(meta.clone(), enc_a);
    fs_a.init().await.unwrap();
    let payload = vec![42u8; 300 * 1024];
    fs_a.write("/secret.bin", &payload).await.unwrap();
    assert_eq!(&fs_a.read("/secret.bin").await.unwrap()[..], &payload[..]);

    // Reopen the SAME metadata + SAME backend, but wrapped with key B. The inode
    // and its manifest hash resolve fine; decrypting the content must fail loudly.
    let enc_b: Arc<dyn ContentStore> = Arc::new(EncryptedStore::new(backend.clone(), key(11)));
    let fs_b = Fs::new(meta.clone(), enc_b);
    let err = fs_b.read("/secret.bin").await.unwrap_err();
    assert!(
        err.to_string().contains("decryption failed"),
        "wrong-key reopen must fail loudly with a decryption error, got: {err}"
    );
    // A ranged read (separate code path) must fail loudly too.
    assert!(
        fs_b.read_range("/secret.bin", 10, 5).await.is_err(),
        "wrong-key ranged read must also fail, not return garbage"
    );

    // The correct key reopens the same workspace and reads it back intact.
    let enc_ok: Arc<dyn ContentStore> = Arc::new(EncryptedStore::new(backend, key(10)));
    let fs_ok = Fs::new(meta, enc_ok);
    assert_eq!(&fs_ok.read("/secret.bin").await.unwrap()[..], &payload[..]);
}

#[tokio::test]
async fn gc_works_through_encryption() {
    let backend = Arc::new(MemStore::new());
    let enc: Arc<dyn ContentStore> = Arc::new(EncryptedStore::new(backend.clone(), key(6)));
    let fs = Fs::new(SqliteMetadataStore::open_in_memory().unwrap(), enc);
    fs.init().await.unwrap();

    fs.write("/a.bin", &vec![1u8; 200 * 1024]).await.unwrap();
    let before = backend.len();
    fs.write("/a.bin", &vec![2u8; 200 * 1024]).await.unwrap(); // orphan v1
    assert!(backend.len() > before);

    let stats = fs.gc_with_grace(0).await.unwrap();
    assert!(stats.deleted > 0);
    // Live body still decrypts after collection.
    assert_eq!(
        &fs.read("/a.bin").await.unwrap()[..],
        &vec![2u8; 200 * 1024][..]
    );
}

#[tokio::test]
async fn on_disk_local_store_is_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalCasStore::open(dir.path().join("cas")).await.unwrap());
    let enc = EncryptedStore::new(backend.clone(), key(7));

    let plaintext = b"on-disk secrets that must not leak to the filesystem";
    let hash = enc.put(plaintext).await.unwrap();

    // The raw file on disk is ciphertext.
    let raw = backend.get(&hash).await.unwrap();
    assert!(!raw.windows(plaintext.len()).any(|w| w == plaintext));
    assert_eq!(&enc.get(&hash).await.unwrap()[..], &plaintext[..]);
}

// SEC (security audit #19): EncryptedStore::put_keyed must refuse a
// non-content-addressed key. The nonce is derived from the key, so storing two
// distinct plaintexts under one key would reuse an AEAD (key, nonce) pair — this
// guard makes it impossible to wrap a mutable-value keyed store (e.g. a pack
// index, whose entry for a chunk changes on repack) in encryption unsafely.
#[tokio::test]
async fn put_keyed_rejects_a_non_content_addressed_key() {
    let backend = Arc::new(MemStore::new());
    let enc = EncryptedStore::new(backend, key(9));

    let bytes = b"index-entry-v1";
    // The content-addressed key (the hash of the bytes) is accepted...
    enc.put_keyed(&Hash::of(bytes), bytes).await.unwrap();
    // ...but a key that isn't the hash of the bytes is refused.
    let wrong = Hash::of(b"a-different-key");
    assert!(
        enc.put_keyed(&wrong, bytes).await.is_err(),
        "a non-content-addressed key must be rejected (nonce reuse)"
    );
}

// --- golden vectors: the stored bytes must not move under us ------------------

/// A store's ciphertext is produced by four things origofs does not own outright —
/// the Argon2id parameters, `argon2` itself, BLAKE3's keyed nonce derivation, and
/// XChaCha20-Poly1305 — plus one it does, the envelope framing. If any of them
/// changes, every object already in every encrypted store becomes undecryptable,
/// and the symptom is `Corrupt("wrong key or corrupt data")`: indistinguishable
/// from an operator typing the wrong passphrase, on data that is in fact intact.
///
/// That is not a failure anyone would diagnose as "the dependency moved", so it is
/// pinned here rather than discovered in a bucket. Encryption is convergent — the
/// nonce is derived, not random — so the bytes are fully deterministic.
///
/// **If this fails, do not update the constant.** Work out which input changed; if
/// it is a dependency, the fix is to pin the dependency, not to accept new bytes.
/// A deliberate scheme change gets a new envelope version and keeps decoding v1
/// (`origofs_core::format`).
#[tokio::test]
async fn passphrase_store_ciphertext_is_pinned() {
    const PASSPHRASE: &str = "correct horse battery staple";
    const SALT: &[u8] = b"origofs-fixture!";
    const PLAINTEXT: &[u8] = b"the quick brown fox";
    const CIPHERTEXT: &str =
        "4f524745019f6a5eb2c97cea9395002959bf6f03f762f8bfed00a3341f47b99f55ec1296a51f97e7";

    let backend = Arc::new(MemStore::new());
    let store =
        EncryptedStore::from_passphrase(backend.clone(), PASSPHRASE, SALT).expect("derive key");

    let hash = store.put(PLAINTEXT).await.unwrap();
    // The address is the plaintext hash, so it is fixed by BLAKE3 alone.
    assert_eq!(hash, Hash::of(PLAINTEXT));

    let stored = backend.get(&hash).await.unwrap();
    assert_eq!(
        hex(&stored),
        CIPHERTEXT,
        "the encrypted-at-rest byte layout changed — every existing encrypted \
         store would fail to decrypt, reporting a wrong passphrase"
    );
    assert_eq!(&store.get(&hash).await.unwrap()[..], PLAINTEXT);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
