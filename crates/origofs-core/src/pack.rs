//! Pack objects: batch many small chunks into few large objects to amortize
//! per-request cost on object storage (`docs/DESIGN.md` §4a).
//!
//! Content-defined chunking makes writes cheap (only changed chunks re-upload)
//! but produces *many small objects* — and S3/R2/GCS bill per request. A
//! [`PackStore`] wraps a **data** backend and an **index** backend and, instead
//! of writing each chunk as its own object, appends chunks into an in-memory
//! buffer that is sealed into one **pack** object once it reaches a target size.
//! A tiny index entry per chunk records where it landed — `(pack, offset, len)`
//! — so reads are a single ranged GET into the pack. Content addressing is
//! preserved: a chunk's address is still `BLAKE3(chunk)`.
//!
//! Deploy the index on a fast/cheap tier (a local dir) and the data on object
//! storage: small index local, big packed data remote — the layout restic, borg,
//! and git packfiles use.
//!
//! **One index per data store.** The index is what makes a chunk reachable, so two
//! `PackStore`s sharing a bucket with separate indexes do not see each other's
//! chunks — and, worse, each reads the other's packs as garbage, because a pack no
//! index entry points into is exactly how `repack` recognizes dead space. The
//! packed recipes in `origofs-sdk` state the constraint; [`PackStore::owns_data_store`]
//! enforces the destructive half of it, so a second index cannot delete the
//! first's packs even if a deployment gets it wrong.
//!
//! # What batching actually buys, and where it stops
//!
//! Batching is **within a write, not across writes**, and it is worth being precise
//! about that because the difference is large.
//!
//! Every write ends with a durability barrier: content must be durable before the
//! metadata that references it commits, so `Fs::store_body` and `Fs::write_reader`
//! call [`ContentStore::flush`] — which here means [`PackStore::seal`] — before
//! their transaction. So:
//!
//! * **One large file is the good case.** A 40 MiB body chunks into hundreds of
//!   pieces and seals into a handful of packs: hundreds of PUTs become a few. This
//!   is the case the layout is for, and it works.
//! * **Many small files is the floor.** Ten thousand 2 KiB files are ten thousand
//!   writes, each with its own barrier, so each seals its own pack — roughly one
//!   PUT per file, not one per 4 MiB. Packing is not helping there.
//!
//! Nothing can be batched across that boundary without weakening the barrier,
//! which is what makes a crash recoverable. If a bulk-import workload needs
//! cross-write batching, the way to get it is fewer, larger writes (stream one
//! archive through `write_reader` rather than writing each member), not a laxer
//! barrier.
//!
//! A pack is `chunk₀ ‖ chunk₁ ‖ … ‖ trailer ‖ trailer_len(u32) ‖ ORGP ‖ version`,
//! where the trailer lists `(chunk_hash, len)` in order so [`PackStore::repack`]
//! can see a pack's full membership and reclaim dead space (deleted chunks) by
//! rewriting the survivors and dropping the old pack.
//!
//! **Where the version sits, and why at the end.** Everything else origofs writes
//! is tagged at byte 0 ([`crate::format`]), but a pack *starts* with raw user
//! bytes: a chunk whose contents begin with `ORGP` would be indistinguishable
//! from a header, so byte 0 cannot carry a trustworthy tag here. The footer can —
//! it is always origofs's own framing, never user data. The read path never parses
//! either end: a chunk is a ranged GET at the offset its index entry records.

use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::format;
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Default target size for a sealed pack (4 MiB).
pub const DEFAULT_PACK_SIZE: usize = 4 * 1024 * 1024;

/// Sidecar name under which a pack index claims its data store — see
/// [`PackStore::owns_data_store`]. Written to both stores: in the index it is this
/// index's identity, in the data store it is the identity of the index that owns
/// it.
const PACK_OWNER_SIDECAR: &str = "packowner";

/// How the value stored under an index key relates to that key — i.e. whether
/// [`PackStore::do_repack`] may re-hash the bytes to check them.
///
/// `put_keyed` exists for transforming layers that own the addressing invariant
/// themselves: [`EncryptedStore`](crate::encrypt::EncryptedStore) stores
/// *ciphertext* under the *plaintext* hash so dedup survives encryption. A pack
/// index therefore holds two kinds of entry, and repack's integrity check applies
/// to exactly one of them. Assuming every entry was content-addressed made
/// `repack` fail with a spurious `Corrupt` on the first partially-dead pack of any
/// encrypted+packed store — the composition `origofs-cli` builds whenever
/// `ORIGOFS_ENCRYPTION_KEY` is set alongside `packed = true`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Addressing {
    /// `key == BLAKE3(value)`. Re-hashable, so a mismatch on repack is corruption.
    Content,
    /// A transforming layer owns the address. Not re-hashable here; its own
    /// integrity check (an AEAD tag) runs when the value is read back through it.
    Keyed,
    /// A v1 entry, written before the flag existed. A mismatch could be either of
    /// the above, so repack cannot call it corruption — see `do_repack`.
    Legacy,
}

impl Addressing {
    /// Classify by the one thing that actually decides it. Exact: ciphertext
    /// hashing to its plaintext key would be a BLAKE3 preimage.
    fn of(key: &Hash, bytes: &[u8]) -> Self {
        if key == &Hash::of(bytes) {
            Self::Content
        } else {
            Self::Keyed
        }
    }

    fn code(self) -> u8 {
        match self {
            // `Legacy` is a decode-only state: v1 entries have no flag byte, and
            // nothing writes v1 any more.
            Self::Content | Self::Legacy => 0,
            Self::Keyed => 1,
        }
    }
}

/// Where a chunk lives inside a pack.
#[derive(Clone, Copy)]
struct PackLoc {
    pack: Hash,
    offset: u32,
    len: u32,
    addressing: Addressing,
}

/// Body of a v1 index entry: `pack(32) ‖ offset(4) ‖ len(4)`.
const LOC_BODY_V1: usize = 40;
/// Body of a v2 entry: v1's body plus the addressing flag.
const LOC_BODY_V2: usize = LOC_BODY_V1 + 1;
/// A whole v1 entry: the body behind an `ORGI ‖ version` header.
const LOC_ENTRY_V1: usize = format::HEADER_LEN + LOC_BODY_V1;
/// A whole v2 entry.
const LOC_ENTRY_V2: usize = format::HEADER_LEN + LOC_BODY_V2;

impl PackLoc {
    /// `ORGI ‖ version ‖ pack(32) ‖ offset(4) ‖ len(4) ‖ addressing(1)`.
    fn encode(&self) -> [u8; LOC_ENTRY_V2] {
        let mut out = [0u8; LOC_ENTRY_V2];
        out[..format::HEADER_LEN].copy_from_slice(&format::PACK_INDEX.header());
        let body = &mut out[format::HEADER_LEN..];
        body[..32].copy_from_slice(self.pack.as_bytes());
        body[32..36].copy_from_slice(&self.offset.to_le_bytes());
        body[36..40].copy_from_slice(&self.len.to_le_bytes());
        body[40] = self.addressing.code();
        out
    }

    fn decode(b: &[u8]) -> Result<Self> {
        // v1 entries stay readable forever (format rule 2); they simply cannot say
        // which kind of value they point at.
        let (body, addressing) = match format::PACK_INDEX.version_of(b)? {
            1 if b.len() == LOC_ENTRY_V1 => (&b[format::HEADER_LEN..], Addressing::Legacy),
            2 if b.len() == LOC_ENTRY_V2 => {
                let body = &b[format::HEADER_LEN..];
                let addressing = match body[40] {
                    0 => Addressing::Content,
                    1 => Addressing::Keyed,
                    _ => return Err(format::PACK_INDEX.malformed()),
                };
                (body, addressing)
            }
            1 | 2 => return Err(format::PACK_INDEX.malformed()),
            v => return Err(format::PACK_INDEX.unsupported(v)),
        };
        let mut pack = [0u8; 32];
        pack.copy_from_slice(&body[..32]);
        Ok(PackLoc {
            pack: Hash::from_array(pack),
            offset: u32::from_le_bytes(body[32..36].try_into().unwrap()),
            len: u32::from_le_bytes(body[36..40].try_into().unwrap()),
            addressing,
        })
    }
}

/// A chunk buffered in the open pack, with the addressing its index entry will
/// record when the pack is sealed.
struct Staged {
    bytes: Bytes,
    addressing: Addressing,
}

/// The open, not-yet-sealed pack.
#[derive(Default)]
struct Pending {
    order: Vec<Hash>,
    resident: HashMap<Hash, Staged>,
    size: usize,
}

/// What a `stage` call should do when the key is already known.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StageMode {
    /// Skip the write if the chunk is already buffered or already indexed,
    /// refreshing its recency instead. The normal write path.
    Dedup,
    /// Write it even though the index already has an entry — the repack path,
    /// which is deliberately moving a chunk that is already indexed elsewhere.
    Force,
    /// Overwrite whatever is there. See [`ContentStore::replace_keyed`].
    Replace,
}

/// A content store that packs many chunks into few objects (see module docs).
pub struct PackStore {
    data: Arc<dyn ContentStore>,
    index: Arc<dyn ContentStore>,
    target: usize,
    pending: Mutex<Pending>,
    /// Serializes seals so two flushes can't race the same buffer.
    flush_lock: tokio::sync::Mutex<()>,
}

impl PackStore {
    /// Pack into `data`, recording the chunk index in `index`.
    pub fn new(data: Arc<dyn ContentStore>, index: Arc<dyn ContentStore>) -> Self {
        Self::with_target(data, index, DEFAULT_PACK_SIZE)
    }

    pub fn with_target(
        data: Arc<dyn ContentStore>,
        index: Arc<dyn ContentStore>,
        target: usize,
    ) -> Self {
        Self {
            data,
            index,
            target: target.max(1),
            pending: Mutex::new(Pending::default()),
            flush_lock: tokio::sync::Mutex::new(()),
        }
    }

    async fn stage(
        &self,
        key: Hash,
        bytes: &[u8],
        mode: StageMode,
        addressing: Addressing,
    ) -> Result<()> {
        if mode != StageMode::Replace {
            let p = self.pending.lock();
            if p.resident.contains_key(&key) {
                return Ok(());
            }
        }
        if mode == StageMode::Dedup && self.index.has(&key).await? {
            // Deduplicated onto a chunk that is already packed. Its liveness for
            // GC is its *index entry's* age (see `list_with_age`), so refresh
            // that entry — otherwise a sweep can reclaim the pack holding a chunk
            // this write is about to reference. The index refresh is itself
            // age-gated, so a hit on recently-written content costs nothing.
            self.index.touch(&key).await?;
            return Ok(());
        }
        let full = {
            let mut p = self.pending.lock();
            let staged = Staged {
                bytes: Bytes::copy_from_slice(bytes),
                addressing,
            };
            match p.resident.insert(key, staged) {
                // Replacing a buffered chunk: it keeps its place in `order`, and
                // only the size delta needs accounting.
                Some(old) => {
                    p.size = p.size.saturating_sub(old.bytes.len());
                }
                None => p.order.push(key),
            }
            p.size += bytes.len();
            p.size >= self.target
        };
        if full {
            self.seal().await?;
        }
        Ok(())
    }

    /// Seal the open pack into a data object + index entries.
    async fn seal(&self) -> Result<()> {
        let _guard = self.flush_lock.lock().await;

        let (order, chunks) = {
            let p = self.pending.lock();
            if p.order.is_empty() {
                return Ok(());
            }
            let order = p.order.clone();
            let chunks: Vec<(Bytes, Addressing)> = order
                .iter()
                .map(|h| {
                    let s = &p.resident[h];
                    (s.bytes.clone(), s.addressing)
                })
                .collect();
            (order, chunks)
        };

        // body ‖ trailer ‖ trailer_len ‖ ORGP ‖ version
        //
        // `PackLoc` addresses a chunk with a `u32` offset and `u32` length, and
        // these three casts used to be bare `as`. Past 4 GiB they wrapped
        // *silently*: the pack was written, an index entry with a nonsense offset
        // was committed, and a later `get` issued a ranged read at the wrong place
        // — returning wrong bytes or a short read, with nothing to indicate which.
        //
        // Reachable despite the 4 MiB default target, because `stage` inserts
        // before it seals: any single `put` larger than the target gets a pack of
        // its own. The manifest of a ~7.5 TiB file is such a put. `with_target`
        // also takes an unbounded size.
        //
        // The read side was already careful — `parse_trailer` uses `checked_add`
        // with a comment about hostile trailers. This is the same asymmetry as
        // `Manifest::encode`: guarded coming in, unguarded going out.
        let too_large = |what: &str, n: usize| {
            OrigoFSError::TooLarge(format!(
                "pack {what} is {n} bytes, past the u32 the pack index can address \
                 ({} max); lower the pack target or store this object unpacked",
                u32::MAX
            ))
        };
        let mut buf = Vec::new();
        let mut locs: Vec<(Hash, u32, u32, Addressing)> = Vec::with_capacity(order.len());
        for (h, (b, addressing)) in order.iter().zip(&chunks) {
            let offset = u32::try_from(buf.len()).map_err(|_| too_large("offset", buf.len()))?;
            let len = u32::try_from(b.len()).map_err(|_| too_large("chunk", b.len()))?;
            buf.extend_from_slice(b);
            locs.push((*h, offset, len, *addressing));
        }
        let body_len = buf.len();
        for (h, _, len, _) in &locs {
            buf.extend_from_slice(h.as_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
        }
        let trailer = buf.len() - body_len;
        let trailer_len = u32::try_from(trailer).map_err(|_| too_large("trailer", trailer))?;
        buf.extend_from_slice(&trailer_len.to_le_bytes());
        buf.extend_from_slice(&format::PACK.header());

        let pack = self.data.put(&buf).await?;
        for (h, offset, len, addressing) in &locs {
            let loc = PackLoc {
                pack,
                offset: *offset,
                len: *len,
                addressing: *addressing,
            };
            // `replace_keyed`, not `put_keyed`: on a repack this chunk already has
            // an index entry pointing into the *old* pack, and insert-if-absent
            // would silently leave it there.
            self.index.replace_keyed(h, &loc.encode()).await?;
        }

        // Drop the sealed chunks; keep anything appended during the seal.
        let mut p = self.pending.lock();
        for h in &order {
            if let Some(s) = p.resident.remove(h) {
                // Saturating: a bookkeeping slip must not panic while holding
                // the buffer lock.
                p.size = p.size.saturating_sub(s.bytes.len());
            }
        }
        let Pending {
            order, resident, ..
        } = &mut *p;
        order.retain(|h| resident.contains_key(h));
        Ok(())
    }

    async fn locate(&self, hash: &Hash) -> Result<Option<PackLoc>> {
        match self.index.get(hash).await {
            Ok(b) => Ok(Some(PackLoc::decode(&b)?)),
            Err(OrigoFSError::ContentMissing(_)) | Err(OrigoFSError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// This pack index's stable identity, created on first use and kept in the
    /// **index** store's sidecar namespace (invisible to `list`, so GC cannot
    /// sweep it).
    async fn index_identity(&self) -> Result<Option<Vec<u8>>> {
        // A token, not a secret: it only has to differ between two index
        // directories, and it is written once and then read back forever after. So
        // it is derived from the things that separate two live creators — wall
        // clock, process, and a per-call counter — rather than pulling a CSPRNG
        // into a crate that otherwise needs none.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut h = blake3::Hasher::new();
        h.update(&nanos.to_le_bytes());
        h.update(&std::process::id().to_le_bytes());
        h.update(
            &SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .to_le_bytes(),
        );
        let fresh = *h.finalize().as_bytes();

        match self
            .index
            .put_sidecar_if_absent(PACK_OWNER_SIDECAR, &fresh)
            .await
        {
            Ok(id) => Ok(Some(id)),
            // A custom index backend without sidecar support cannot answer the
            // ownership question either way; the caller falls back to the historical
            // assumption that it is the only writer.
            Err(_) => Ok(None),
        }
    }

    /// Whether this pack store is the **only** one addressing its data store, and
    /// so may delete a pack that its index no longer references.
    ///
    /// `repack` reclaims a pack by observing that no index entry points into it.
    /// That inference holds only if this index is the sole index over this data
    /// store. The packed recipes already require that ("single-writer-per-index"),
    /// but they frame it as a dedup-visibility caveat — two nodes simply not seeing
    /// each other's chunks — when the sharper consequence is here: a pack written
    /// by node B has no entry in node A's index, so A's repack reads it as garbage
    /// and deletes it from the shared bucket.
    ///
    /// So the constraint is checked rather than assumed. The index claims the data
    /// store by writing its identity into a create-if-absent sidecar; if the claim
    /// comes back as someone else's, this store is shared and pack deletion is off.
    /// An unclaimed store (every store that predates this) is claimed on the first
    /// repack, which is correct for the single-writer deployments that are the norm.
    async fn owns_data_store(&self) -> Result<bool> {
        let Some(id) = self.index_identity().await? else {
            return Ok(true);
        };
        match self
            .data
            .put_sidecar_if_absent(PACK_OWNER_SIDECAR, &id)
            .await
        {
            Ok(claim) => Ok(claim == id),
            // Same fallback as above: a backend that cannot answer keeps the
            // historical behaviour rather than silently disabling reclamation.
            Err(_) => Ok(true),
        }
    }

    async fn do_repack(&self) -> Result<u64> {
        self.seal().await?;
        let exclusive = self.owns_data_store().await?;

        // pack -> the chunks the index still points into it, and how each is
        // addressed (which decides whether repack may re-hash it).
        let mut live_by_pack: HashMap<Hash, HashMap<Hash, Addressing>> = HashMap::new();
        for chunk in self.index.list().await? {
            if let Some(loc) = self.locate(&chunk).await? {
                live_by_pack
                    .entry(loc.pack)
                    .or_default()
                    .insert(chunk, loc.addressing);
            }
        }

        let mut reclaimed = 0u64;
        for pack in self.data.list().await? {
            let bytes = self.data.get(&pack).await?;
            let members = parse_trailer(&bytes)?;
            let live = live_by_pack.remove(&pack).unwrap_or_default();

            if live.is_empty() {
                // Fully dead *according to this index*, which is the same thing as
                // fully dead only when no other index addresses these packs — see
                // `owns_data_store`. A pack another node wrote has no entry here
                // and is indistinguishable from garbage, so deleting on this
                // evidence alone is what destroys it.
                if !exclusive {
                    tracing::warn!(
                        pack = %pack.to_hex(),
                        members = members.len(),
                        "not reclaiming a pack with no live chunks: this data store is claimed \
                         by a different pack index, so the pack may belong to another node \
                         rather than being garbage"
                    );
                    continue;
                }
                reclaimed += self.data.delete(&pack).await?;
            } else if live.len() < members.len() {
                // Partially dead: move survivors into a fresh pack, then drop the
                // old one.
                //
                // The order is the whole safety argument, and it used to be
                // backwards: the old index pointer was cleared *before* the
                // survivor was staged, and staging only buffers in memory. A crash
                // (or any error from `stage`/`seal`) in that window left the chunk
                // with no index entry while its bytes still lived in the old pack —
                // and a chunk with no index entry is invisible to the *next*
                // repack, which then reads that pack as fully dead and deletes it.
                // Permanent loss, from the one operation whose job is to reclaim
                // space safely.
                //
                // Now: stage everything, seal (which writes the new pack and
                // atomically repoints each index entry at it), and only then delete
                // the old pack. A crash before the seal leaves the old pack and its
                // pointers untouched; a crash after it leaves the old pack
                // unreferenced, which the next repack reclaims. Every intermediate
                // state keeps each live chunk reachable.
                //
                // Verification runs over every survivor *before* any of them is
                // staged, so a pack that cannot be verified is left untouched
                // rather than half-moved.
                let mut survivors: Vec<(Hash, Bytes, Addressing)> = Vec::with_capacity(live.len());
                let mut unverifiable = false;
                for (h, offset, len) in &members {
                    let Some(&addressing) = live.get(h) else {
                        continue;
                    };
                    let slice = bytes.slice(*offset as usize..(*offset + *len) as usize);
                    // Verify-on-repack: never launder a corrupt chunk into a fresh
                    // pack and then delete the evidence (audit M1). This is only
                    // meaningful for a chunk whose key *is* its hash. A value stored
                    // through `put_keyed` by a transforming layer deliberately does
                    // not hash to its key — `EncryptedStore` keeps the plaintext
                    // hash as the address and stores ciphertext — so re-hashing it
                    // proves nothing, and treating the mismatch as corruption made
                    // `repack` fail outright on every encrypted+packed store.
                    if addressing != Addressing::Keyed {
                        let actual = Hash::of(&slice);
                        if actual != *h {
                            if addressing == Addressing::Legacy {
                                // A v1 entry: no flag, so this is either corruption
                                // or a keyed value written before the flag existed.
                                // Both are possible and they are indistinguishable,
                                // so do the one thing that is safe under either —
                                // leave the pack exactly as it is. Nothing is
                                // laundered and no evidence is deleted, which is
                                // what the check exists for; the cost is only that
                                // this pack's dead space waits for entries to be
                                // rewritten as v2.
                                tracing::warn!(
                                    pack = %pack.to_hex(),
                                    chunk = %h.to_hex(),
                                    "leaving a pack unrepacked: a v1 index entry's bytes do not \
                                     hash to its key, which is either corruption or a value \
                                     written by a transforming layer before the addressing flag \
                                     existed"
                                );
                                unverifiable = true;
                                break;
                            }
                            return Err(OrigoFSError::Corrupt(format!(
                                "pack {} chunk {} failed its integrity check during repack (got {})",
                                pack.to_hex(),
                                h.to_hex(),
                                actual.to_hex()
                            )));
                        }
                    }
                    survivors.push((*h, slice, addressing));
                }
                if unverifiable {
                    continue;
                }
                for (h, slice, addressing) in &survivors {
                    self.stage(*h, slice, StageMode::Force, *addressing).await?;
                }
                self.seal().await?;
                reclaimed += self.data.delete(&pack).await?;
            }
            // else fully live: leave it.
        }
        Ok(reclaimed)
    }
}

/// Parse a pack's trailer into `(chunk_hash, offset, len)` in stored order.
///
/// The footer is `trailer_len(u32) ‖ ORGP ‖ version` — see the module docs for why
/// the tag lives at the end.
fn parse_trailer(pack: &[u8]) -> Result<Vec<(Hash, u32, u32)>> {
    let bad = || OrigoFSError::Content("malformed pack trailer".into());
    const FOOTER: usize = 4 + format::HEADER_LEN;
    let len_at = pack.len().checked_sub(FOOTER).ok_or_else(bad)?;
    match format::PACK.version_of(&pack[len_at + 4..])? {
        1 => {}
        v => return Err(format::PACK.unsupported(v)),
    }
    let tlen = u32::from_le_bytes(pack[len_at..len_at + 4].try_into().unwrap()) as usize;
    let trailer_start = len_at.checked_sub(tlen).ok_or_else(bad)?;
    let trailer = &pack[trailer_start..len_at];
    if !tlen.is_multiple_of(36) {
        return Err(bad());
    }
    let mut out = Vec::with_capacity(tlen / 36);
    let mut offset = 0u32;
    let mut i = 0;
    while i < trailer.len() {
        let mut h = [0u8; 32];
        h.copy_from_slice(&trailer[i..i + 32]);
        let len = u32::from_le_bytes(trailer[i + 32..i + 36].try_into().unwrap());
        out.push((Hash::from_array(h), offset, len));
        // Checked: a tampered trailer with huge lengths would otherwise overflow
        // (panic in debug / wrap in release).
        offset = offset.checked_add(len).ok_or_else(bad)?;
        i += 36;
    }
    // The chunk bodies must exactly fill the region before the trailer. If they
    // don't, the (offset, len) pairs are inconsistent with the pack — reject it
    // rather than letting `repack` slice out of range and panic.
    if offset as usize != trailer_start {
        return Err(bad());
    }
    Ok(out)
}

#[async_trait]
impl ContentStore for PackStore {
    async fn put(&self, bytes: &[u8]) -> Result<Hash> {
        let hash = Hash::of(bytes);
        // Content-addressed by construction — we just computed the address.
        self.stage(hash, bytes, StageMode::Dedup, Addressing::Content)
            .await?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.stage(*key, bytes, StageMode::Dedup, Addressing::of(key, bytes))
            .await
    }

    /// A genuine replace, per the trait contract.
    ///
    /// This used to delegate to the deduplicating path, which returns early when
    /// the index already holds the key — i.e. it was insert-if-absent, silently
    /// dropping exactly the update the method exists to deliver. Harmless in the
    /// compositions shipped here (nothing puts a `PackStore` behind another one's
    /// index), but the contract this violates is the one written for that case.
    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.stage(*key, bytes, StageMode::Replace, Addressing::of(key, bytes))
            .await
    }

    /// Slots go to the **data** backend — that's the one that travels with the
    /// store and survives a lost index (which is rebuildable from pack trailers).
    async fn put_meta(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.data.put_meta(name, bytes).await
    }

    async fn get_meta(&self, name: &str) -> Result<Option<Bytes>> {
        self.data.get_meta(name).await
    }

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        {
            let p = self.pending.lock();
            if let Some(s) = p.resident.get(hash) {
                return Ok(s.bytes.clone());
            }
        }
        match self.locate(hash).await? {
            Some(loc) => {
                self.data
                    .get_range(&loc.pack, loc.offset as u64, loc.len as u64)
                    .await
            }
            None => Err(OrigoFSError::ContentMissing(hash.to_hex())),
        }
    }

    async fn get_range(&self, hash: &Hash, off: u64, len: u64) -> Result<Bytes> {
        {
            let p = self.pending.lock();
            if let Some(s) = p.resident.get(hash) {
                let start = (off as usize).min(s.bytes.len());
                let end = start.saturating_add(len as usize).min(s.bytes.len());
                return Ok(s.bytes.slice(start..end));
            }
        }
        match self.locate(hash).await? {
            Some(loc) => {
                let start = off.min(loc.len as u64);
                let take = len.min(loc.len as u64 - start);
                self.data
                    .get_range(&loc.pack, loc.offset as u64 + start, take)
                    .await
            }
            None => Err(OrigoFSError::ContentMissing(hash.to_hex())),
        }
    }

    async fn has(&self, hash: &Hash) -> Result<bool> {
        {
            let p = self.pending.lock();
            if p.resident.contains_key(hash) {
                return Ok(true);
            }
        }
        self.index.has(hash).await
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let mut out = self.index.list().await?;
        let p = self.pending.lock();
        for h in p.resident.keys() {
            out.push(*h);
        }
        Ok(out)
    }

    async fn get_sidecar(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.data.get_sidecar(name).await
    }

    async fn put_sidecar_if_absent(&self, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        self.data.put_sidecar_if_absent(name, bytes).await
    }

    async fn list_with_age(&self) -> Result<Vec<(Hash, Option<u64>)>> {
        // A chunk's age is its *index entry's* age: the index is what makes it
        // reachable, and a repack rewrites the entry when the chunk moves packs.
        let mut out = self.index.list_with_age().await?;
        // Still buffered, so written moments ago by definition — age 0 keeps them
        // inside any grace period.
        let p = self.pending.lock();
        for h in p.resident.keys() {
            out.push((*h, Some(0)));
        }
        Ok(out)
    }

    /// A chunk's recency is its index entry's, matching `list_with_age`. A chunk
    /// still buffered in the open pack already reports age 0, so there is nothing
    /// to refresh for it.
    async fn touch(&self, hash: &Hash) -> Result<()> {
        if self.pending.lock().resident.contains_key(hash) {
            return Ok(());
        }
        self.index.touch(hash).await
    }

    /// A chunk's age is its index entry's, matching `list_with_age`; a chunk still
    /// buffered in the open pack is age 0 there and must be here too, or the
    /// sweep's conditional delete would resolve it against the index (where it does
    /// not exist yet) and treat it as undatable.
    async fn age_of(&self, hash: &Hash) -> Result<Option<u64>> {
        if self.pending.lock().resident.contains_key(hash) {
            return Ok(Some(0));
        }
        self.index.age_of(hash).await
    }

    async fn ping(&self) -> Result<()> {
        // The data store is the (possibly remote) backend whose reachability
        // gates readiness; the index is a local sidecar.
        self.data.ping().await
    }

    /// Close both stores, without sealing the pending buffer.
    ///
    /// Unlike `ping`, which asks only the backend because it alone gates
    /// readiness, close has to reach *both*: the index is a real store holding
    /// real handles. Anything still buffered is discarded — see the trait's note
    /// on why flushing is the caller's call, and `Workspace::close`, which makes
    /// it for the workspace.
    async fn close(&self) -> Result<()> {
        let index = self.index.close().await;
        let data = self.data.close().await;
        data.and(index)
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let staged = {
            let mut p = self.pending.lock();
            if let Some(s) = p.resident.remove(hash) {
                p.order.retain(|h| h != hash);
                // Saturating: a bookkeeping slip must not panic while holding
                // the buffer lock.
                p.size = p.size.saturating_sub(s.bytes.len());
                Some(s.bytes.len() as u64)
            } else {
                None
            }
        };
        // Drop the index pointer; the pack bytes are reclaimed only by `repack`.
        self.index.delete(hash).await?;
        Ok(staged.unwrap_or(0))
    }

    async fn flush(&self) -> Result<()> {
        self.seal().await
    }

    async fn repack(&self) -> Result<u64> {
        self.do_repack().await
    }
}
