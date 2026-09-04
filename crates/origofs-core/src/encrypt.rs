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
//!
//! **Upgrading origofs must not make a store unreadable, so both halves of that
//! are pinned in data rather than in the binary.**
//!
//! 1. *The stored bytes are versioned.* Every object written is
//!    `ORGE | version | AEAD(...)` ([`crate::format::ENCRYPTED`]), so a future
//!    build can define a v2 scheme and still decrypt every v1 object. Objects
//!    written before the envelope existed carry no header and are read forever —
//!    see [`EncryptedStore::decrypt`].
//! 2. *The Argon2id cost is the store's, not the crate's.* The parameters were
//!    `argon2::Params::default()`, a constant a dependency owns and has already
//!    changed once. [`KdfParams::LEGACY`] pins the values every existing store was
//!    built with, and `origofs-sdk` records the parameters actually used beside the
//!    salt, so raising the default for new stores can never re-derive a different
//!    key for an old one. The failure this avoids is the nastiest kind: a changed
//!    key is indistinguishable from a wrong passphrase.

use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::format;
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
use std::sync::Arc;

const NONCE_CONTEXT: &str = "origofs content nonce v1";

/// The Argon2id cost parameters a store derives its key with.
///
/// Recorded per store (`origofs-sdk` writes them beside the salt) rather than read
/// from the `argon2` crate at run time. See the module docs, and
/// [`crate::format::KDF`] for why a dependency's `Params::default()` is not a
/// safe place to keep something every byte in a store depends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KdfParams {
    /// Memory cost, in KiB.
    pub m_cost: u32,
    /// Time cost — the number of passes.
    pub t_cost: u32,
    /// Degree of parallelism.
    pub p_cost: u32,
}

impl KdfParams {
    /// The parameters every origofs store created before [`KdfParams`] existed was
    /// built with: `argon2` 0.5's `Params::default()`, frozen here as origofs's own
    /// constants.
    ///
    /// **Never change these values.** They are not a policy choice about how hard
    /// a passphrase should be to crack — they are the only record of how existing
    /// keys were derived, and moving them by one makes every such store
    /// permanently undecryptable. Raise the cost for *new* stores by changing
    /// [`KdfParams::current`] instead; the parameters travel with the store.
    pub const LEGACY: Self = Self {
        m_cost: 19 * 1024,
        t_cost: 2,
        p_cost: 1,
    };

    /// What a store created by this build records and derives with.
    ///
    /// Equal to [`LEGACY`](Self::LEGACY) today. Raising it is safe — every store
    /// carries the parameters it was made with — but it makes opening a *new*
    /// store measurably slower, so it is a deliberate change and not a default to
    /// drift.
    pub const fn current() -> Self {
        Self::LEGACY
    }

    /// `ORGK | version | m_cost | t_cost | p_cost` (little-endian `u32`s).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format::KDF.header().to_vec();
        out.extend_from_slice(&self.m_cost.to_le_bytes());
        out.extend_from_slice(&self.t_cost.to_le_bytes());
        out.extend_from_slice(&self.p_cost.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        match format::KDF.version_of(bytes)? {
            1 => {
                let body = &bytes[format::HEADER_LEN..];
                if body.len() < 12 {
                    return Err(format::KDF.malformed());
                }
                let u32_at =
                    |i: usize| u32::from_le_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
                let params = Self {
                    m_cost: u32_at(0),
                    t_cost: u32_at(4),
                    p_cost: u32_at(8),
                };
                // A zero in any field is not a cost anyone chose; it is a
                // truncated or zeroed sidecar. Deriving from it would either fail
                // inside Argon2 or — worse, if a future Argon2 tolerated it —
                // silently produce a key nothing else agrees on.
                if params.m_cost == 0 || params.t_cost == 0 || params.p_cost == 0 {
                    return Err(format::KDF.malformed());
                }
                Ok(params)
            }
            v => Err(format::KDF.unsupported(v)),
        }
    }

    fn to_argon2(self) -> Result<argon2::Params> {
        argon2::Params::new(self.m_cost, self.t_cost, self.p_cost, None).map_err(|e| {
            OrigoFSError::Content(format!("invalid Argon2id parameters {self:?}: {e}"))
        })
    }
}

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
    ///
    /// Derives with [`KdfParams::LEGACY`], which is what every store made before
    /// the parameters were recorded used. A caller that knows the store's
    /// parameters — `origofs-sdk` reads them from the `kdf` sidecar — should use
    /// [`from_passphrase_with_params`](Self::from_passphrase_with_params).
    pub fn from_passphrase(
        inner: Arc<dyn ContentStore>,
        passphrase: &str,
        salt: &[u8],
    ) -> Result<Self> {
        Self::from_passphrase_with_params(inner, passphrase, salt, KdfParams::LEGACY)
    }

    /// [`from_passphrase`](Self::from_passphrase) with the store's own recorded
    /// Argon2id cost.
    ///
    /// `params` must be the parameters the store's existing objects were keyed
    /// with; a different value derives a different key and every read fails the
    /// AEAD tag, indistinguishably from a wrong passphrase. That is precisely why
    /// they are stored rather than defaulted — see [`KdfParams`].
    pub fn from_passphrase_with_params(
        inner: Arc<dyn ContentStore>,
        passphrase: &str,
        salt: &[u8],
        params: KdfParams,
    ) -> Result<Self> {
        use argon2::{Algorithm, Argon2, Version};
        let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params.to_argon2()?);
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

    /// Encrypt, and frame the result as `ORGE | version | AEAD(...)`.
    ///
    /// The 5-byte header is what makes the scheme evolvable: without it the
    /// cipher, the nonce derivation and the KDF were pinned by whichever binary
    /// happened to write the object, and nothing in the bucket recorded which. See
    /// [`crate::format::ENCRYPTED`].
    fn encrypt(&self, storage_key: &Hash, plaintext: &[u8]) -> Result<Vec<u8>> {
        let sealed = self
            .cipher
            .encrypt(&self.nonce_for(storage_key), plaintext)
            .map_err(|_| OrigoFSError::Content("encryption failed".into()))?;
        let mut out = Vec::with_capacity(format::HEADER_LEN + sealed.len());
        out.extend_from_slice(&format::ENCRYPTED.header());
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    /// One AEAD open attempt. Returns the plaintext or nothing; the caller decides
    /// what a failure *means*, because a first failure here is routine (it is how
    /// the two stored shapes are told apart) and only the last one is an error
    /// worth logging.
    fn open(&self, storage_key: &Hash, sealed: &[u8]) -> Option<Vec<u8>> {
        self.cipher
            .decrypt(&self.nonce_for(storage_key), sealed)
            .ok()
    }

    /// Decrypt an object in either stored shape.
    ///
    /// Two shapes exist and both must be readable forever: the versioned envelope
    /// this build writes (`ORGE | version | AEAD`), and the **bare AEAD output**
    /// written by every origofs before the envelope existed. Upgrading must not
    /// require rewriting a bucket, so the old shape is not deprecated — it is
    /// supported.
    ///
    /// Telling them apart starts with the header but does not end there.
    /// Ciphertext is indistinguishable from random, so roughly one legacy object
    /// in 2^32 begins with the bytes `ORGE` by coincidence, and one in 2^40 also
    /// carries a plausible version byte. Sniffing alone would therefore fail a
    /// perfectly good object — rarely, unreproducibly, and reported as corruption.
    ///
    /// So the header only chooses which interpretation to *try first*, and the
    /// **AEAD tag decides**. A misread envelope fails to authenticate and falls
    /// back to the legacy reading (and vice versa); a misread can never yield the
    /// wrong plaintext, only one extra open. The cost is one redundant AEAD
    /// attempt on a path that already failed, which is the error path.
    ///
    /// An envelope version this build is too old to decode is reported as
    /// [`OrigoFSError::UnsupportedVersion`] — "upgrade origofs" — but only after
    /// the legacy reading has been tried and failed, since that is the shape a
    /// coincidental tag actually has.
    fn decrypt(&self, storage_key: &Hash, stored: &[u8]) -> Result<Vec<u8>> {
        let mut too_new = None;
        if format::ENCRYPTED.tagged(stored) {
            match format::ENCRYPTED.version_of(stored) {
                Ok(1) => {
                    if let Some(plaintext) = self.open(storage_key, &stored[format::HEADER_LEN..]) {
                        return Ok(plaintext);
                    }
                }
                // Unreachable while `version_of` caps at `max_read_version`; this
                // is the arm a v2 envelope is added beside.
                Ok(_) => {}
                Err(e) => too_new = Some(e),
            }
        }
        if let Some(plaintext) = self.open(storage_key, stored) {
            return Ok(plaintext);
        }
        if let Some(e) = too_new {
            return Err(e);
        }
        Err(self.undecryptable(storage_key))
    }

    /// The error for an object no interpretation could authenticate.
    ///
    /// `Corrupt`, not a generic `Content` error. A failed AEAD tag is an integrity
    /// failure on a stored object — the same event `VerifyingStore` reports as
    /// `Corrupt` — and the encrypted recipes deliberately do not stack a
    /// `VerifyingStore` (the AEAD already authenticates), so this is the *only*
    /// place an encrypted stack can report one. Reporting it as a plain content
    /// error put it in a different `code()`/`class()` bucket than the identical
    /// failure on every other stack, so an operator filtering for corruption saw
    /// nothing.
    ///
    /// A wrong key produces the same tag failure, hence the message: the two are
    /// cryptographically indistinguishable here.
    fn undecryptable(&self, storage_key: &Hash) -> OrigoFSError {
        tracing::warn!(
            hash = %storage_key.to_hex(),
            "content failed authenticated decryption"
        );
        OrigoFSError::Corrupt(format!(
            "decryption failed for {storage_key} (wrong key or corrupt data)"
        ))
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

    /// Slots pass through **in plaintext**, by design.
    ///
    /// The nonce here is derived from a content address, and a slot has none — but
    /// more importantly the format descriptor has to be readable *without* the key,
    /// so "this store needs a newer origofs" and "you gave the wrong passphrase"
    /// stay distinguishable. It carries no user data, exactly like the Argon2id
    /// salt already stored beside the content store.
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_meta(name, bytes).await
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        self.inner.get_meta(name).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        let ciphertext = self.inner.get(hash).await?;
        Ok(Bytes::from(self.decrypt(hash, &ciphertext)?))
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        // AEAD authenticates the whole object, so a ranged read is not possible
        // without decrypting all of it: the tag covers the ciphertext, and a
        // partial decrypt cannot be authenticated. Decrypt, then slice.
        //
        // Cheap for *chunks*, which is the overwhelming majority of reads and are
        // capped at `MAX_CHUNK` (256 KiB). It is not cheap for the two object kinds
        // that are not chunks: a `PackStore` pack (4 MiB by default) and a manifest
        // (36 bytes per chunk, so ~59 MB for a 100 GiB file). A ranged read of
        // either decrypts the whole object.
        //
        // The comment here used to assert all objects were "chunk-sized (<= a few
        // hundred KB)", which was simply false for those two. Left as-is rather
        // than "fixed" because there is no fix that preserves authentication —
        // the honest answer is that encryption and packing compose with a real
        // cost, and `docs/LIMITS.md` says so.
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

    /// Forwarded, like `list_with_age`. A decorator that reports the inner
    /// store's ages must forward the refresh that keeps those ages honest,
    /// or a deduplicating write through this layer stays invisible to the
    /// sweep's grace period (`ContentStore::touch`).
    async fn touch(&self, hash: &Hash) -> Result<()> {
        self.inner.touch(hash).await
    }

    /// Forwarded alongside `list_with_age`/`touch`: all three have to agree on one
    /// clock, or the sweep's age gate reads a different one than it acts on.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        self.inner.age_of(hash).await
    }

    async fn delete_if_older_than(&self, hash: &Hash, min_age_secs: u64) -> Result<Option<u64>> {
        self.inner.delete_if_older_than(hash, min_age_secs).await
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
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::MemStore;

    fn store() -> (Arc<MemStore>, EncryptedStore) {
        let backend = Arc::new(MemStore::new());
        let enc = EncryptedStore::new(backend.clone(), [7u8; 32]);
        (backend, enc)
    }

    /// The envelope is what makes the scheme evolvable at all: without it nothing
    /// in the bucket says which cipher, nonce derivation or KDF produced an
    /// object, so no future build could change any of them.
    #[tokio::test]
    async fn stored_objects_carry_a_versioned_envelope() {
        let (backend, enc) = store();
        let hash = enc.put(b"hello origofs").await.unwrap();

        let stored = backend.get(&hash).await.unwrap();
        assert_eq!(&stored[..4], b"ORGE");
        assert_eq!(stored[4], 1, "envelope format version");
        // header + plaintext + the AEAD tag, and nothing else.
        assert_eq!(
            stored.len(),
            format::HEADER_LEN + b"hello origofs".len() + 16
        );
        assert_eq!(&enc.get(&hash).await.unwrap()[..], b"hello origofs");
    }

    /// Every object written before the envelope existed is bare AEAD output. An
    /// upgrade must not require rewriting a bucket, so these stay readable —
    /// forever, not for a deprecation window.
    #[tokio::test]
    async fn objects_written_before_the_envelope_still_decrypt() {
        let (backend, enc) = store();
        let plaintext = b"written by an older origofs";
        let hash = Hash::of(plaintext);

        // Exactly what the pre-envelope `encrypt` produced: no header.
        let legacy = enc
            .cipher
            .encrypt(&enc.nonce_for(&hash), &plaintext[..])
            .unwrap();
        assert_ne!(&legacy[..4], b"ORGE");
        backend.put_keyed(&hash, &legacy).await.unwrap();

        assert_eq!(&enc.get(&hash).await.unwrap()[..], plaintext);
    }

    /// An envelope version this build is too old to read is an upgrade problem,
    /// not a corruption report — the same distinction every other object kind
    /// makes, and the reason `UnsupportedVersion` exists.
    #[tokio::test]
    async fn an_envelope_from_a_newer_origofs_says_upgrade() {
        let (backend, enc) = store();
        let hash = Hash::of(b"whatever");

        let mut future = b"ORGE\x02".to_vec();
        future.extend_from_slice(&[0u8; 32]);
        backend.put_keyed(&hash, &future).await.unwrap();

        let e = enc.get(&hash).await.unwrap_err();
        assert_eq!(e.code(), "unsupported_version");
        assert!(e.is_unsupported_version());
    }

    /// Bytes that authenticate under no interpretation are still corruption, and
    /// must not be reported as "upgrade origofs".
    #[tokio::test]
    async fn undecryptable_bytes_are_still_corrupt() {
        let (backend, enc) = store();
        let hash = Hash::of(b"whatever");
        backend.put_keyed(&hash, &[0u8; 48]).await.unwrap();

        assert_eq!(enc.get(&hash).await.unwrap_err().code(), "corrupt");
    }

    /// The whole point of recording the parameters: a store keyed at one cost must
    /// not be re-derived at another. This is the failure mode a changed
    /// `Params::default()` would have caused silently, made explicit.
    #[tokio::test]
    async fn a_different_kdf_cost_derives_a_different_key() {
        let backend = Arc::new(MemStore::new());
        let a = EncryptedStore::from_passphrase_with_params(
            backend.clone(),
            "hunter2",
            b"a fixed 16-byte!",
            KdfParams::LEGACY,
        )
        .unwrap();
        let hash = a.put(b"payload").await.unwrap();

        let b = EncryptedStore::from_passphrase_with_params(
            backend.clone(),
            "hunter2",
            b"a fixed 16-byte!",
            KdfParams {
                t_cost: KdfParams::LEGACY.t_cost + 1,
                ..KdfParams::LEGACY
            },
        )
        .unwrap();
        // Indistinguishable from a wrong passphrase — which is exactly why the
        // parameters travel with the store rather than with the binary.
        assert_eq!(b.get(&hash).await.unwrap_err().code(), "corrupt");
    }

    #[test]
    fn kdf_params_round_trip_and_reject_nonsense() {
        let p = KdfParams::current();
        assert_eq!(KdfParams::decode(&p.encode()).unwrap(), p);

        // A zeroed or truncated sidecar is not a cost anyone chose.
        let mut zeroed = KdfParams::LEGACY;
        zeroed.t_cost = 0;
        assert_eq!(
            KdfParams::decode(&zeroed.encode()).unwrap_err().code(),
            "content_error"
        );
        assert_eq!(
            KdfParams::decode(&p.encode()[..10]).unwrap_err().code(),
            "content_error"
        );

        // Written by a newer origofs: "upgrade", not "corrupt".
        let mut future = p.encode();
        future[4] = 2;
        assert_eq!(
            KdfParams::decode(&future).unwrap_err().code(),
            "unsupported_version"
        );
    }

    /// A canary on the `argon2` crate, not a test of origofs.
    ///
    /// [`KdfParams::LEGACY`] is a frozen copy of `argon2` 0.5's `Params::default()`
    /// — the values every encrypted store created before the parameters were
    /// recorded was keyed with. **If this fails after an `argon2` upgrade, do not
    /// change `LEGACY` to match**: the crate moved its default, our pin is what
    /// keeps those stores readable, and the correct response is to delete this
    /// assertion (its job is done) and decide separately whether
    /// [`KdfParams::current`] should adopt the new cost for *new* stores.
    #[test]
    fn legacy_params_are_the_crate_default_this_build_was_pinned_against() {
        let d = argon2::Params::default();
        assert_eq!(
            (d.m_cost(), d.t_cost(), d.p_cost()),
            (
                KdfParams::LEGACY.m_cost,
                KdfParams::LEGACY.t_cost,
                KdfParams::LEGACY.p_cost
            ),
        );
        assert_eq!(d.output_len(), None, "32-byte output is the default");
    }
}
