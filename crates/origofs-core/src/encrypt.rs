//! Encryption at rest: a [`ContentStore`] wrapper that encrypts every object
//! before it reaches the backend (`docs/DESIGN.md` §7 hardening; roadmap M9).
//!
//! The engine addresses content by the BLAKE3 hash of the **plaintext** — chunk
//! hashes live in manifests and inodes — so encryption has to be transparent to
//! everything above it. [`EncryptedStore`] keeps the plaintext hash as the
//! address (`put` still returns `Hash::of(plaintext)`) and stores the
//! *ciphertext* under that key via [`ContentStore::put_keyed`]. Reads decrypt on
//! the way out. The metadata store, GC, and the object graph never see
//! ciphertext or need to change.
//!
//! **Cipher & nonce.** XChaCha20-Poly1305 (a 256-bit AEAD). The 192-bit nonce is
//! derived deterministically from the storage key (the plaintext hash) keyed by
//! the encryption key, so identical plaintext yields identical ciphertext and
//! **content dedup still works** — this is convergent encryption. The tradeoff
//! is inherent to dedup: it reveals when two stored objects are byte-identical,
//! which the shared content address already did. Distinct plaintexts get
//! distinct nonces (BLAKE3 is collision-resistant), so a (key, nonce) pair is
//! never reused across different messages.
//!
//! **Keys.** Provide a 32-byte key, or derive one from a passphrase via
//! [`EncryptedStore::from_passphrase`] — **Argon2id** (memory-hard) with a
//! per-store random salt, so a weak passphrase is expensive to brute-force
//! offline and the same passphrase never yields the same key across stores. The
//! salt is not secret but must persist; origofs-sdk keeps it alongside the content
//! store (so it survives a metadata-DB loss). Losing the key means the data is
//! unrecoverable; the wrong key fails loudly rather than returning garbage (the
//! AEAD tag won't verify).

use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use std::sync::Arc;

const NONCE_CONTEXT: &str = "origofs content nonce v1";

/// A content store that encrypts objects at rest over an inner store.
pub struct EncryptedStore {
    inner: Arc<dyn ContentStore>,
    cipher: XChaCha20Poly1305,
    key: [u8; 32],
}

impl EncryptedStore {
    /// Wrap `inner`, encrypting with a raw 32-byte key.
    pub fn new(inner: Arc<dyn ContentStore>, key: [u8; 32]) -> Self {
        let cipher = XChaCha20Poly1305::new((&key).into());
        Self { inner, cipher, key }
    }

    /// Wrap `inner`, deriving the 256-bit key from `passphrase` with **Argon2id**
    /// (memory-hard) and a caller-supplied `salt` (>= 8 bytes). The same
    /// `(passphrase, salt)` must be used on every open. The salt is not secret but
    /// must be persisted somewhere durable that travels with the store — origofs-sdk
    /// keeps it beside the content store so it survives a metadata-DB loss.
    pub fn from_passphrase(
        inner: Arc<dyn ContentStore>,
        passphrase: &str,
        salt: &[u8],
    ) -> Result<Self> {
        use argon2::{Algorithm, Argon2, Params, Version};
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default());
        let mut key = [0u8; 32];
        argon
            .hash_password_into(passphrase.as_bytes(), salt, &mut key)
            .map_err(|e| OrigoFSError::Content(format!("passphrase key derivation failed: {e}")))?;
        Ok(Self::new(inner, key))
    }

    /// Derive a 192-bit nonce from the storage key, keyed by the encryption key,
    /// so it is deterministic (dedup-preserving) yet unique per distinct object.
    fn nonce_for(&self, storage_key: &Hash) -> XNonce {
        let mut h = blake3::Hasher::new_derive_key(NONCE_CONTEXT);
        h.update(&self.key);
        h.update(storage_key.as_bytes());
        let mut nonce = [0u8; 24];
        nonce.copy_from_slice(&h.finalize().as_bytes()[..24]);
        XNonce::from(nonce)
    }

    fn encrypt(&self, storage_key: &Hash, plaintext: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .encrypt(&self.nonce_for(storage_key), plaintext)
            .map_err(|_| OrigoFSError::Content("encryption failed".into()))
    }

    fn decrypt(&self, storage_key: &Hash, ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.cipher
            .decrypt(&self.nonce_for(storage_key), ciphertext)
            .map_err(|_| {
                OrigoFSError::Content(format!(
                    "decryption failed for {storage_key} (wrong key or corrupt data)"
                ))
            })
    }
}

#[async_trait]
impl ContentStore for EncryptedStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes); // address stays the plaintext hash
        let ciphertext = self.encrypt(&hash, bytes)?;
        self.inner.put_keyed(&hash, &ciphertext).await?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        // The nonce is derived from `key` (so reads can re-derive it), which is
        // only safe when a key maps to exactly one plaintext — i.e. content
        // addressing, `key == Hash::of(bytes)`. Storing two different plaintexts
        // under the same key would reuse an (key, nonce) pair, breaking the AEAD.
        // Reject any non-content-addressed key so a mutable-value keyed store
        // (e.g. a `PackStore` index, whose entry for a chunk changes on repack)
        // can't be wrapped in encryption and silently made insecure.
        if key != &Hash::of(bytes) {
            return Err(OrigoFSError::Content(
                "EncryptedStore::put_keyed requires a content-addressed key \
                 (key == hash of bytes); wrapping a non-content-addressed keyed \
                 store in encryption would reuse an AEAD nonce"
                    .into(),
            ));
        }
        let ciphertext = self.encrypt(key, bytes)?;
        self.inner.put_keyed(key, &ciphertext).await
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        // Same nonce-reuse rule as `put_keyed`, and doubly so: *replacing* a value
        // is by definition storing a second plaintext under one key, which is
        // exactly the (key, nonce) reuse the derivation cannot survive. A mutable
        // keyed store (a `PackStore` index) must not be wrapped in encryption.
        if key != &Hash::of(bytes) {
            return Err(OrigoFSError::Content(
                "EncryptedStore::replace_keyed requires a content-addressed key \
                 (key must equal the hash of the plaintext)"
                    .into(),
            ));
        }
        self.put_keyed(key, bytes).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let ciphertext = self.inner.get(hash).await?;
        Ok(Bytes::from(self.decrypt(hash, &ciphertext)?))
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        // AEAD authenticates the whole object, so decrypt then slice. origofs objects
        // are chunk-sized (<= a few hundred KB), so this stays cheap.
        let full = self.get(hash).await?;
        let start = (off as usize).min(full.len());
        let end = start.saturating_add(len as usize).min(full.len());
        Ok(full.slice(start..end))
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        self.inner.has(hash).await
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        self.inner.list().await
    }

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        self.inner.list_with_age().await
    }

    /// Forwarded **unencrypted**, and necessarily so: the salt stored here is what
    /// derives this store's key, so it has to be readable before the key exists.
    /// It is not secret — Argon2id salts are public by design; they exist to make
    /// the same passphrase yield a different key in every store.
    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_sidecar(name).await
    }

    /// See [`get_sidecar`](Self::get_sidecar).
    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        self.inner.put_sidecar_if_absent(name, bytes).await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        self.inner.delete(hash).await
    }

    /// Forwarded, and it matters: without this the default no-op silently swallows
    /// the seal, so wrapping a [`PackStore`](crate::PackStore) in encryption meant
    /// its buffered chunks never became durable — metadata would reference content
    /// that only ever existed in one process's memory. The same reasoning applies
    /// to `repack`, which would otherwise never reclaim anything through an
    /// encrypted store.
    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// See [`flush`](Self::flush).
    async fn repack(&self) -> Result<u64> {
        self.inner.repack().await
    }

    async fn ping(&self) -> Result<()> {
        self.inner.ping().await
    }
}
