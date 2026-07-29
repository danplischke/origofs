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
//! A pack is `chunk₀ ‖ chunk₁ ‖ … ‖ trailer ‖ trailer_len(u32)`, where the
//! trailer lists `(chunk_hash, len)` in order so [`PackStore::repack`] can see a
//! pack's full membership and reclaim dead space (deleted chunks) by rewriting
//! the survivors and dropping the old pack.

use crate::content::ContentStore;
use crate::error::{OrigoFSError, Result};
use crate::types::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Default target size for a sealed pack (4 MiB).
pub const DEFAULT_PACK_SIZE: usize = 4 * 1024 * 1024;

/// Where a chunk lives inside a pack.
#[derive(Clone, Copy)]
struct PackLoc {
    pack: Hash,
    offset: u32,
    len: u32,
}

impl PackLoc {
    /// `pack(32) ‖ offset(4) ‖ len(4)`.
    fn encode(&self) -> [u8; 40] {
        let mut out = [0u8; 40];
        out[..32].copy_from_slice(self.pack.as_bytes());
        out[32..36].copy_from_slice(&self.offset.to_le_bytes());
        out[36..40].copy_from_slice(&self.len.to_le_bytes());
        out
    }
    fn decode(b: &[u8]) -> Result<Self> {
        if b.len() != 40 {
            return Err(OrigoFSError::Content("malformed pack index entry".into()));
        }
        let mut pack = [0u8; 32];
        pack.copy_from_slice(&b[..32]);
        Ok(PackLoc {
            pack: Hash::from_array(pack),
            offset: u32::from_le_bytes(b[32..36].try_into().unwrap()),
            len: u32::from_le_bytes(b[36..40].try_into().unwrap()),
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
            let p = self.pending.lock().unwrap();
            if p.resident.contains_key(&key) {
                return Ok(());
            }
        }
        if dedup && self.index.has(&key).await? {
            return Ok(());
        }
        let full = {
            let mut p = self.pending.lock().unwrap();
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
            let p = self.pending.lock().unwrap();
            if p.order.is_empty() {
                return Ok(());
            }
            let order = p.order.clone();
            let chunks: Vec<Bytes> = order.iter().map(|h| p.resident[h].clone()).collect();
            (order, chunks)
        };

        // body ‖ trailer ‖ trailer_len
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

        let pack = self.data.put(&buf).await?;
        for (h, offset, len) in &locs {
            let loc = PackLoc {
                pack,
                offset: *offset,
                len: *len,
            };
            self.index.put_keyed(h, &loc.encode()).await?;
        }

        // Drop the sealed chunks; keep anything appended during the seal.
        let mut p = self.pending.lock().unwrap();
        for h in &order {
            if let Some(b) = p.resident.remove(h) {
                p.size -= b.len();
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
                // Partially dead: move survivors into fresh packs, drop the old.
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
                        self.index.delete(h).await?; // clear the old pointer
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
fn parse_trailer(pack: &[u8]) -> Result<Vec<(Hash, u32, u32)>> {
    let bad = || OrigoFSError::Content("malformed pack trailer".into());
    if pack.len() < 4 {
        return Err(bad());
    }
    let tlen = u32::from_le_bytes(pack[pack.len() - 4..].try_into().unwrap()) as usize;
    let trailer_start = pack.len().checked_sub(4 + tlen).ok_or_else(bad)?;
    let trailer = &pack[trailer_start..pack.len() - 4];
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

    async fn get(&self, hash: &Hash) -> Result<Bytes> {
        {
            let p = self.pending.lock().unwrap();
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
            let p = self.pending.lock().unwrap();
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
            let p = self.pending.lock().unwrap();
            if p.resident.contains_key(hash) {
                return Ok(true);
            }
        }
        self.index.has(hash).await
    }

    async fn list(&self) -> Result<Vec<Hash>> {
        let mut out = self.index.list().await?;
        let p = self.pending.lock().unwrap();
        for h in p.resident.keys() {
            out.push(*h);
        }
        Ok(out)
    }

    async fn ping(&self) -> Result<()> {
        // The data store is the (possibly remote) backend whose reachability
        // gates readiness; the index is a local sidecar.
        self.data.ping().await
    }

    async fn delete(&self, hash: &Hash) -> Result<u64> {
        let staged = {
            let mut p = self.pending.lock().unwrap();
            if let Some(b) = p.resident.remove(hash) {
                p.order.retain(|h| h != hash);
                p.size -= b.len();
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
