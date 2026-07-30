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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Default target size for a sealed pack (4 MiB).
pub const DEFAULT_PACK_SIZE: usize = 4 * 1024 * 1024;

/// Where a chunk lives inside a pack.
#[derive(Clone, Copy)]
struct PackLoc {
    pack: Hash,
    offset: u32,
    len: u32,
}

/// Body of an index entry: `pack(32) ‖ offset(4) ‖ len(4)`.
const LOC_BODY: usize = 40;
/// A whole entry: the body behind an `ORGI ‖ version` header.
const LOC_ENTRY: usize = format::HEADER_LEN + LOC_BODY;

impl PackLoc {
    /// `ORGI ‖ version ‖ pack(32) ‖ offset(4) ‖ len(4)`.
    fn encode(&self) -> [u8; LOC_ENTRY] {
        let mut out = [0u8; LOC_ENTRY];
        out[..format::HEADER_LEN].copy_from_slice(&format::PACK_INDEX.header());
        let body = &mut out[format::HEADER_LEN..];
        body[..32].copy_from_slice(self.pack.as_bytes());
        body[32..36].copy_from_slice(&self.offset.to_le_bytes());
        body[36..40].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    fn decode(b: &[u8]) -> Result<Self> {
        let body = match format::PACK_INDEX.version_of(b)? {
            1 if b.len() == LOC_ENTRY => &b[format::HEADER_LEN..],
            1 => return Err(format::PACK_INDEX.malformed()),
            v => return Err(format::PACK_INDEX.unsupported(v)),
        };
        let mut pack = [0u8; 32];
        pack.copy_from_slice(&body[..32]);
        Ok(PackLoc {
            pack: Hash::from_array(pack),
            offset: u32::from_le_bytes(body[32..36].try_into().unwrap()),
            len: u32::from_le_bytes(body[36..40].try_into().unwrap()),
        })
    }
}

/// The open, not-yet-sealed pack.
#[derive(Default)]
struct Pending {
    order: Vec<Hash>,
    resident: HashMap<Hash, Bytes>,
    size: usize,
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

    async fn stage(&self, key: Hash, bytes: &[u8], dedup: bool) -> Result<()> {
        {
            let p = self.pending.lock();
            if p.resident.contains_key(&key) {
                return Ok(());
            }
        }
        if dedup && self.index.has(&key).await? {
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
            if p.resident.contains_key(&key) {
                return Ok(());
            }
            p.resident.insert(key, Bytes::copy_from_slice(bytes));
            p.order.push(key);
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
            let chunks: Vec<Bytes> = order.iter().map(|h| p.resident[h].clone()).collect();
            (order, chunks)
        };

        // body ‖ trailer ‖ trailer_len ‖ ORGP ‖ version
        let mut buf = Vec::new();
        let mut locs: Vec<(Hash, u32, u32)> = Vec::with_capacity(order.len());
        for (h, b) in order.iter().zip(&chunks) {
            let offset = buf.len() as u32;
            buf.extend_from_slice(b);
            locs.push((*h, offset, b.len() as u32));
        }
        let body_len = buf.len();
        for (h, _, len) in &locs {
            buf.extend_from_slice(h.as_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
        }
        let trailer_len = (buf.len() - body_len) as u32;
        buf.extend_from_slice(&trailer_len.to_le_bytes());
        buf.extend_from_slice(&format::PACK.header());

        let pack = self.data.put(&buf).await?;
        for (h, offset, len) in &locs {
            let loc = PackLoc {
                pack,
                offset: *offset,
                len: *len,
            };
            // `replace_keyed`, not `put_keyed`: on a repack this chunk already has
            // an index entry pointing into the *old* pack, and insert-if-absent
            // would silently leave it there.
            self.index.replace_keyed(h, &loc.encode()).await?;
        }

        // Drop the sealed chunks; keep anything appended during the seal.
        let mut p = self.pending.lock();
        for h in &order {
            if let Some(b) = p.resident.remove(h) {
                // Saturating: a bookkeeping slip must not panic while holding
                // the buffer lock.
                p.size = p.size.saturating_sub(b.len());
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

    async fn do_repack(&self) -> Result<u64> {
        self.seal().await?;

        // pack -> the chunks the index still points into it.
        let mut live_by_pack: HashMap<Hash, HashSet<Hash>> = HashMap::new();
        for chunk in self.index.list().await? {
            if let Some(loc) = self.locate(&chunk).await? {
                live_by_pack.entry(loc.pack).or_default().insert(chunk);
            }
        }

        let mut reclaimed = 0u64;
        for pack in self.data.list().await? {
            let bytes = self.data.get(&pack).await?;
            let members = parse_trailer(&bytes)?;
            let live = live_by_pack.remove(&pack).unwrap_or_default();

            if live.is_empty() {
                reclaimed += self.data.delete(&pack).await?; // fully dead
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
                for (h, offset, len) in &members {
                    if live.contains(h) {
                        let slice = bytes.slice(*offset as usize..(*offset + *len) as usize);
                        // Verify-on-repack: never launder a corrupt chunk into a
                        // fresh pack and then delete the evidence. Pack chunks are
                        // content-addressed, so re-hash and refuse a mismatch
                        // (audit M1).
                        let actual = Hash::of(&slice);
                        if actual != *h {
                            return Err(OrigoFSError::Corrupt(format!(
                                "pack {} chunk {} failed its integrity check during repack (got {})",
                                pack.to_hex(),
                                h.to_hex(),
                                actual.to_hex()
                            )));
                        }
                        self.stage(*h, &slice, false).await?;
                    }
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
        self.stage(hash, bytes, true).await?;
        Ok(hash)
    }

    async fn put_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.stage(*key, bytes, true).await
    }

    async fn replace_keyed(&self, key: &Hash, bytes: &[u8]) -> Result<()> {
        self.stage(*key, bytes, true).await
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
            if let Some(b) = p.resident.get(hash) {
                return Ok(b.clone());
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
            if let Some(b) = p.resident.get(hash) {
                let start = (off as usize).min(b.len());
                let end = start.saturating_add(len as usize).min(b.len());
                return Ok(b.slice(start..end));
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

    async fn ping(&self) -> Result<()> {
        // The data store is the (possibly remote) backend whose reachability
        // gates readiness; the index is a local sidecar.
        self.data.ping().await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let staged = {
            let mut p = self.pending.lock();
            if let Some(b) = p.resident.remove(hash) {
                p.order.retain(|h| h != hash);
                // Saturating: a bookkeeping slip must not panic while holding
                // the buffer lock.
                p.size = p.size.saturating_sub(b.len());
                Some(b.len() as u64)
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
